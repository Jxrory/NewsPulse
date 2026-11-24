package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
)

// DigestWithItems is the unit of persistent editorial output. Outbox payloads
// are fixed when the digest is created; rendering again uses the stored item
// snapshots rather than live article rows.
type DigestBundle struct {
	Digest Digest
	Items  []DigestItem
	Outbox *OutboxItem
}

// SaveDigest writes the digest, its items and (when auto-send is on) the
// outbox row in one transaction in one annual shard. Repeated attempts for an
// already-saved slot return the existing digest without duplicating rows.
func (s *Shard) SaveDigest(ctx context.Context, b *DigestBundle) (string, bool, error) {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback()

	var existingID string
	err = tx.QueryRowContext(ctx, `SELECT id FROM digests WHERE slot_key=? AND revision=?`,
		b.Digest.SlotKey, b.Digest.Revision).Scan(&existingID)
	if err == nil {
		return existingID, false, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", false, err
	}

	if b.Digest.ID == "" {
		id, err := NewID()
		if err != nil {
			return "", false, err
		}
		b.Digest.ID = id
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO digests(
		id, job_id, slot_key, revision, schedule_id, scheduled_at, window_start,
		window_end, cutoff_at, title, markdown, content_hash, generation_mode,
		model_metadata_json, source_health_json, quality_flags_json, created_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		b.Digest.ID, nullable(b.Digest.JobID), b.Digest.SlotKey, b.Digest.Revision,
		nullable(b.Digest.ScheduleID), b.Digest.ScheduledAt, b.Digest.WindowStart,
		b.Digest.WindowEnd, b.Digest.CutoffAt, b.Digest.Title, b.Digest.Markdown,
		b.Digest.ContentHash, b.Digest.GenerationMode, b.Digest.ModelMetadataJSON,
		marshalJSON(b.Digest.SourceHealth), marshalJSON(b.Digest.QualityFlags),
		b.Digest.CreatedAt)
	if err != nil {
		return "", false, err
	}
	for i := range b.Items {
		it := &b.Items[i]
		it.Ordinal = i + 1
		_, err = tx.ExecContext(ctx, `INSERT INTO digest_items(
			digest_id, ordinal, section, event_key, title_zh, facts_json,
			background, impact, importance, coverage, article_refs_json,
			evidence_snapshot_json, quality_flags_json)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			b.Digest.ID, it.Ordinal, it.Section, it.EventKey, it.TitleZh,
			marshalJSON(it.Facts), it.Background, it.Impact, it.Importance,
			it.Coverage, marshalJSON(it.ArticleRefs), marshalJSON(it.EvidenceSnapshot),
			marshalJSON(it.QualityFlags))
		if err != nil {
			return "", false, err
		}
	}
	if b.Outbox != nil {
		o := b.Outbox
		if o.ID == "" {
			id, err := NewID()
			if err != nil {
				return "", false, err
			}
			o.ID = id
		}
		o.DigestID = b.Digest.ID
		if _, err := insertOutboxTx(ctx, tx, o); err != nil {
			return "", false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return "", false, err
	}
	return b.Digest.ID, true, nil
}

func scanDigest(sc interface {
	Scan(dest ...interface{}) error
}) (Digest, error) {
	var d Digest
	var jobID, scheduleID, mm, flags, health sql.NullString
	err := sc.Scan(&d.ID, &jobID, &d.SlotKey, &d.Revision, &scheduleID,
		&d.ScheduledAt, &d.WindowStart, &d.WindowEnd, &d.CutoffAt, &d.Title,
		&d.Markdown, &d.ContentHash, &d.GenerationMode, &mm, &health, &flags, &d.CreatedAt)
	if err != nil {
		return d, err
	}
	d.JobID = nullStr(jobID)
	d.ScheduleID = nullStr(scheduleID)
	d.ModelMetadataJSON = mm.String
	d.SourceHealth = decodeJSON(health.String, []SourceHealth(nil))
	d.QualityFlags = decodeStringSlice(flags.String)
	return d, nil
}

const digestColumns = `id, job_id, slot_key, revision, schedule_id, scheduled_at,
 window_start, window_end, cutoff_at, title, markdown, content_hash,
 generation_mode, model_metadata_json, source_health_json, quality_flags_json,
 created_at`

func (s *Shard) GetDigest(ctx context.Context, id string) (*Digest, []DigestItem, error) {
	s.opMu.RLock()
	defer s.opMu.RUnlock()
	row := s.r.QueryRowContext(ctx, `SELECT `+digestColumns+` FROM digests WHERE id=?`, id)
	d, err := scanDigest(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	items, err := s.getDigestItemsLocked(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	d.OwnerYear = s.Year
	return &d, items, nil
}

func (s *Shard) getDigestItemsLocked(ctx context.Context, id string) ([]DigestItem, error) {
	rows, err := s.r.QueryContext(ctx, `SELECT ordinal, section, event_key, title_zh,
		facts_json, background, impact, importance, coverage, article_refs_json,
		evidence_snapshot_json, quality_flags_json FROM digest_items
		WHERE digest_id=? ORDER BY ordinal`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DigestItem
	for rows.Next() {
		var it DigestItem
		var facts, refs, evidence, flags string
		if err := rows.Scan(&it.Ordinal, &it.Section, &it.EventKey, &it.TitleZh,
			&facts, &it.Background, &it.Impact, &it.Importance, &it.Coverage,
			&refs, &evidence, &flags); err != nil {
			return nil, err
		}
		it.Facts = decodeJSON(facts, []Fact(nil))
		it.ArticleRefs = decodeJSON(refs, []ArticleRef(nil))
		it.EvidenceSnapshot = decodeJSON(evidence, []ArticleSnapshot(nil))
		it.QualityFlags = decodeStringSlice(flags)
		out = append(out, it)
	}
	return out, rows.Err()
}

func (m *ShardManager) GetDigest(ctx context.Context, year int, id string) (*Digest, []DigestItem, error) {
	s, err := m.Open(year)
	if err != nil {
		return nil, nil, err
	}
	return s.GetDigest(ctx, id)
}

func (s *Shard) GetDigestBySlot(ctx context.Context, slotKey string, revision int) (*Digest, error) {
	s.opMu.RLock()
	defer s.opMu.RUnlock()
	row := s.r.QueryRowContext(ctx, `SELECT `+digestColumns+` FROM digests WHERE slot_key=? AND revision=?`, slotKey, revision)
	d, err := scanDigest(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &d, nil
}

func (s *Shard) ListDigests(ctx context.Context, limit int, before *int64, beforeID string) ([]Digest, error) {
	s.opMu.RLock()
	defer s.opMu.RUnlock()
	q := `SELECT ` + digestColumns + ` FROM digests`
	var args []interface{}
	if before != nil {
		q += ` WHERE (scheduled_at < ? OR (scheduled_at = ? AND id < ?))`
		args = append(args, *before, *before, beforeID)
	}
	q += ` ORDER BY scheduled_at DESC, id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.r.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Digest
	for rows.Next() {
		d, err := scanDigest(rows)
		if err != nil {
			return nil, err
		}
		d.OwnerYear = s.Year
		out = append(out, d)
	}
	return out, rows.Err()
}

// LatestDigestForSchedule returns the most recent digest for repeat
// suppression and /status reporting.
func (m *ShardManager) LatestDigestForSchedule(ctx context.Context, scheduleID string) (*Digest, error) {
	shards, err := m.AllShards()
	if err != nil {
		return nil, err
	}
	var best *Digest
	for _, s := range shards {
		digests, err := s.ListDigests(ctx, 5, nil, "")
		if err != nil {
			return nil, err
		}
		for i := range digests {
			d := digests[i]
			if scheduleID != "" && d.ScheduleID != scheduleID && d.ScheduleID != "" {
				continue
			}
			if best == nil || d.ScheduledAt > best.ScheduledAt {
				dd := d
				best = &dd
			}
		}
	}
	return best, nil
}

func decodeDigestItem(raw string) (DigestItem, error) {
	var it DigestItem
	err := json.Unmarshal([]byte(raw), &it)
	return it, err
}

// PreviousDigestTime returns the scheduled_at of the most recent digest for
// this schedule before the given time.
func (m *ShardManager) PreviousDigestTime(ctx context.Context, scheduleID string, before int64) (int64, bool, error) {
	shards, err := m.AllShards()
	if err != nil {
		return 0, false, err
	}
	best := int64(-1)
	for _, s := range shards {
		var at int64
		q := `SELECT scheduled_at FROM digests WHERE scheduled_at < ?`
		args := []interface{}{before}
		if scheduleID != "" {
			q += ` AND schedule_id=?`
			args = append(args, scheduleID)
		}
		q += ` ORDER BY scheduled_at DESC LIMIT 1`
		if err := s.r.QueryRowContext(ctx, q, args...).Scan(&at); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return 0, false, err
		}
		if at > best {
			best = at
		}
	}
	if best < 0 {
		return 0, false, nil
	}
	return best, true, nil
}

// PreviousDigestRefs returns the article refs used by the most recent digest
// for this schedule before the given time, across all annual shards.
func (m *ShardManager) PreviousDigestRefs(ctx context.Context, scheduleID string, before int64) (map[string]struct{}, error) {
	shards, err := m.AllShards()
	if err != nil {
		return nil, err
	}
	var bestID string
	var bestYear int
	var bestAt int64 = -1
	for _, s := range shards {
		var id string
		var at int64
		q := `SELECT id, scheduled_at FROM digests WHERE scheduled_at < ?`
		args := []interface{}{before}
		if scheduleID != "" {
			q += ` AND schedule_id=?`
			args = append(args, scheduleID)
		}
		q += ` ORDER BY scheduled_at DESC LIMIT 1`
		err := s.r.QueryRowContext(ctx, q, args...).Scan(&id, &at)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if at > bestAt {
			bestAt = at
			bestID = id
			bestYear = s.Year
		}
	}
	if bestAt < 0 {
		return nil, nil
	}
	s, err := m.Open(bestYear)
	if err != nil {
		return nil, err
	}
	items, err := s.getDigestItemsLocked(ctx, bestID)
	if err != nil {
		return nil, err
	}
	refs := map[string]struct{}{}
	for _, it := range items {
		for _, ref := range it.ArticleRefs {
			refs[ref.ID] = struct{}{}
		}
	}
	return refs, nil
}

type DigestWithYear struct {
	Digest
}

func (m *ShardManager) ListDigestsAcrossShards(ctx context.Context, limit int, before *int64, beforeID string) ([]Digest, error) {
	shards, err := m.AllShards()
	if err != nil {
		return nil, err
	}
	var out []Digest
	for _, s := range shards {
		list, err := s.ListDigests(ctx, limit, before, beforeID)
		if err != nil {
			return nil, err
		}
		out = append(out, list...)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].ScheduledAt != out[j].ScheduledAt {
			return out[i].ScheduledAt > out[j].ScheduledAt
		}
		return out[i].ID > out[j].ID
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *ShardManager) LatestDigest(ctx context.Context) (*Digest, error) {
	list, err := m.ListDigestsAcrossShards(ctx, 1, nil, "")
	if err != nil || len(list) == 0 {
		return nil, err
	}
	return &list[0], nil
}
