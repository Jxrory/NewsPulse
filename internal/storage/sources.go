package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"
)

func scanSourceState(sc interface {
	Scan(dest ...interface{}) error
}) (SourceState, error) {
	var st SourceState
	var etag, lm, errCode sql.NullString
	var checked, success, latest sql.NullInt64
	err := sc.Scan(&st.SourceID, &etag, &lm, &checked, &success, &latest,
		&st.ConsecutiveFailures, &st.NextFetchAt, &st.Health, &errCode, &st.UpdatedAt)
	if err != nil {
		return st, err
	}
	st.ETag = stringPtr(etag)
	st.LastModified = stringPtr(lm)
	st.LastCheckedAt = int64Ptr(checked)
	st.LastSuccessAt = int64Ptr(success)
	st.LatestPublishedAt = int64Ptr(latest)
	st.LastErrorCode = nullStr(errCode)
	return st, nil
}

func (s *Shard) GetSourceState(ctx context.Context, sourceID string) (*SourceState, error) {
	s.opMu.RLock()
	defer s.opMu.RUnlock()
	row := s.r.QueryRowContext(ctx, `SELECT source_id, etag, last_modified, last_checked_at,
		last_success_at, latest_published_at, consecutive_failures, next_fetch_at,
		health, last_error_code, updated_at FROM source_state WHERE source_id=?`, sourceID)
	st, err := scanSourceState(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &st, nil
}

func (s *Shard) ListSourceStates(ctx context.Context) ([]SourceState, error) {
	s.opMu.RLock()
	defer s.opMu.RUnlock()
	rows, err := s.r.QueryContext(ctx, `SELECT source_id, etag, last_modified, last_checked_at,
		last_success_at, latest_published_at, consecutive_failures, next_fetch_at,
		health, last_error_code, updated_at FROM source_state ORDER BY source_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SourceState
	for rows.Next() {
		st, err := scanSourceState(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

func (s *Shard) InsertFetchRun(ctx context.Context, r *FetchRun) error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if r.ID == "" {
		id, err := NewID()
		if err != nil {
			return err
		}
		r.ID = id
	}
	r.RunYear = s.Year
	_, err := s.w.ExecContext(ctx, `INSERT INTO fetch_runs(id,source_id,started_at,status,run_year) VALUES(?,?,?,?,?)`,
		r.ID, r.SourceID, r.StartedAt, r.Status, r.RunYear)
	return err
}

func (s *Shard) FinishFetchRun(ctx context.Context, r *FetchRun) error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	_, err := s.w.ExecContext(ctx, `UPDATE fetch_runs SET finished_at=?, status=?, http_status=?,
		response_bytes=?, parsed_count=?, inserted_count=?, updated_count=?, rejected_count=?,
		rejected_items_json=?, error_code=?, error_message_redacted=? WHERE id=?`,
		sqlNullInt64(r.FinishedAt), r.Status, sqlNullInt64(r.HTTPStatus), r.ResponseBytes,
		r.ParsedCount, r.InsertedCount, r.UpdatedCount, r.RejectedCount,
		r.RejectedItemsJSON, sqlNullString(r.ErrorCode), sqlNullString(r.ErrorMessageRedacted), r.ID)
	return err
}

// RecordFetchSuccess commits the checkpoint update only after article
// writes succeeded. Callers pass the final run statistics.
func (s *Shard) RecordFetchSuccess(ctx context.Context, r *FetchRun, st SourceState) error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := finishFetchRunTx(ctx, tx, r); err != nil {
		return err
	}
	if err := updateSourceStateTx(ctx, tx, st); err != nil {
		return err
	}
	return tx.Commit()
}

// RecordFetchFailure records a failed or partial round without advancing
// ETag/Last-Modified checkpoints.
func (s *Shard) RecordFetchFailure(ctx context.Context, r *FetchRun, st SourceState) error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := finishFetchRunTx(ctx, tx, r); err != nil {
		return err
	}
	if err := updateSourceStateTx(ctx, tx, st); err != nil {
		return err
	}
	return tx.Commit()
}

// RecordNotModified updates only the check time for a 304 response.
func (s *Shard) RecordNotModified(ctx context.Context, run *FetchRun, st SourceState) error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := finishFetchRunTx(ctx, tx, run); err != nil {
		return err
	}
	if err := updateSourceStateTx(ctx, tx, st); err != nil {
		return err
	}
	return tx.Commit()
}

func finishFetchRunTx(ctx context.Context, tx *sql.Tx, r *FetchRun) error {
	_, err := tx.ExecContext(ctx, `UPDATE fetch_runs SET finished_at=?, status=?, http_status=?,
		response_bytes=?, parsed_count=?, inserted_count=?, updated_count=?, rejected_count=?,
		rejected_items_json=?, error_code=?, error_message_redacted=? WHERE id=?`,
		sqlNullInt64(r.FinishedAt), r.Status, sqlNullInt64(r.HTTPStatus), r.ResponseBytes,
		r.ParsedCount, r.InsertedCount, r.UpdatedCount, r.RejectedCount,
		r.RejectedItemsJSON, sqlNullString(r.ErrorCode), sqlNullString(r.ErrorMessageRedacted), r.ID)
	return err
}

func updateSourceStateTx(ctx context.Context, tx *sql.Tx, st SourceState) error {
	_, err := tx.ExecContext(ctx, `UPDATE source_state SET etag=?, last_modified=?,
		last_checked_at=?, last_success_at=?, latest_published_at=?, consecutive_failures=?,
		next_fetch_at=?, health=?, last_error_code=?, updated_at=? WHERE source_id=?`,
		sqlNullString(derefStr(st.ETag)), sqlNullString(derefStr(st.LastModified)),
		sqlNullInt64(st.LastCheckedAt), sqlNullInt64(st.LastSuccessAt),
		sqlNullInt64(st.LatestPublishedAt), st.ConsecutiveFailures, st.NextFetchAt,
		st.Health, sqlNullString(st.LastErrorCode), st.UpdatedAt, st.SourceID)
	return err
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// DueSources returns enabled source ids whose state is due, based on the
// annual shard of now. Every configured source gets a state row at shard
// creation.
func (s *Shard) DueSources(ctx context.Context, now int64, enabled map[string]bool) ([]SourceState, error) {
	states, err := s.ListSourceStates(ctx)
	if err != nil {
		return nil, err
	}
	// Only return enabled sources; a source disabled in config keeps its state
	// but must not be fetched.
	// State rows for all configured sources are created when the shard opens.
	out := make([]SourceState, 0, len(states))
	for _, st := range states {
		if !enabled[st.SourceID] {
			continue
		}
		if st.NextFetchAt <= now {
			out = append(out, st)
		}
	}
	return out, nil
}

// LatestSourceStates returns the most recent state per source across all
// shards, for status reporting.
func (m *ShardManager) LatestSourceStates(ctx context.Context) ([]SourceState, map[string]int, error) {
	shards, err := m.AllShards()
	if err != nil {
		return nil, nil, err
	}
	latest := map[string]SourceState{}
	years := map[string]int{}
	for _, s := range shards {
		states, err := s.ListSourceStates(ctx)
		if err != nil {
			return nil, nil, err
		}
		for _, st := range states {
			if cur, ok := latest[st.SourceID]; !ok || st.UpdatedAt >= cur.UpdatedAt {
				latest[st.SourceID] = st
				years[st.SourceID] = s.Year
			}
		}
	}
	out := make([]SourceState, 0, len(latest))
	for _, st := range latest {
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SourceID < out[j].SourceID })
	return out, years, nil
}

func (m *ShardManager) SourceHealth(ctx context.Context) ([]SourceHealth, error) {
	states, _, err := m.LatestSourceStates(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]SourceHealth, 0, len(states))
	for _, st := range states {
		out = append(out, SourceHealth{
			SourceID: st.SourceID, Health: st.Health,
			LastSuccessAt: st.LastSuccessAt, LatestPublishedAt: st.LatestPublishedAt,
			LastErrorCode: st.LastErrorCode,
		})
	}
	return out, nil
}

// MarkStaleSources marks enabled sources without a recent successful check as
// degraded so they are visible in status and digest coverage notes.
func (m *ShardManager) MarkStaleSources(ctx context.Context, staleAfter time.Duration, now time.Time, enabled map[string]bool) error {
	states, years, err := m.LatestSourceStates(ctx)
	if err != nil {
		return err
	}
	for _, st := range states {
		if !enabled[st.SourceID] {
			continue
		}
		if st.LastSuccessAt == nil {
			continue
		}
		if time.Duration(now.Unix()-*st.LastSuccessAt) <= staleAfter {
			continue
		}
		sh, err := m.Open(years[st.SourceID])
		if err != nil {
			return err
		}
		st.Health = "stale"
		st.UpdatedAt = now.Unix()
		sh.opMu.Lock()
		_, err = sh.w.ExecContext(ctx, `UPDATE source_state SET health='stale', updated_at=? WHERE source_id=?`, now.Unix(), st.SourceID)
		sh.opMu.Unlock()
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("mark stale %s: %w", st.SourceID, err)
		}
	}
	return nil
}
