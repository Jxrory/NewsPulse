package storage

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
)

func scanJob(sc interface {
	Scan(dest ...interface{}) error
}) (Job, error) {
	var j Job
	var scheduleID, cfg, cand, result, errCode, errMsg sql.NullString
	var ws, we, cutoff, deadline, next, lease, heartbeat sql.NullInt64
	err := sc.Scan(&j.ID, &j.JobKey, &j.Kind, &scheduleID, &j.ScheduledAt,
		&ws, &we, &cutoff, &deadline, &j.ExpiresAt, &j.Status, &j.AttemptCount,
		&next, &lease, &heartbeat, &cfg, &cand, &result, &errCode, &errMsg,
		&j.CreatedAt, &j.UpdatedAt)
	if err != nil {
		return j, err
	}
	j.ScheduleID = nullStr(scheduleID)
	j.WindowStart = int64Ptr(ws)
	j.WindowEnd = int64Ptr(we)
	j.CutoffAt = int64Ptr(cutoff)
	j.GenerationDeadline = int64Ptr(deadline)
	j.NextAttemptAt = int64Ptr(next)
	j.LeaseUntil = int64Ptr(lease)
	j.HeartbeatAt = int64Ptr(heartbeat)
	j.ConfigSnapshotJSON = cfg.String
	j.CandidateSnapshotJSON = cand.String
	j.ResultRef = nullStr(result)
	j.LastErrorCode = nullStr(errCode)
	j.LastErrorRedacted = nullStr(errMsg)
	return j, nil
}

const jobColumns = `id, job_key, kind, schedule_id, scheduled_at, window_start,
 window_end, cutoff_at, generation_deadline, expires_at, status, attempt_count,
 next_attempt_at, lease_until, heartbeat_at, config_snapshot_json,
 candidate_snapshot_json, result_ref, last_error_code, last_error_redacted,
 created_at, updated_at`

// InsertJobIfAbsent creates a job for a stable slot key. A duplicate key is
// not an error; the existing job is returned so repeated scheduler ticks do
// not enqueue duplicate work.
func (s *Shard) InsertJobIfAbsent(ctx context.Context, j *Job) (*Job, bool, error) {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	id, err := NewID()
	if err != nil {
		return nil, false, err
	}
	j.ID = id
	_, err = s.w.ExecContext(ctx, `INSERT INTO jobs(`+jobColumns+`)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		j.ID, j.JobKey, j.Kind, sqlNullString(j.ScheduleID), j.ScheduledAt,
		sqlNullInt64(j.WindowStart), sqlNullInt64(j.WindowEnd), sqlNullInt64(j.CutoffAt),
		sqlNullInt64(j.GenerationDeadline), j.ExpiresAt, j.Status, j.AttemptCount,
		sqlNullInt64(j.NextAttemptAt), sqlNullInt64(j.LeaseUntil), sqlNullInt64(j.HeartbeatAt),
		j.ConfigSnapshotJSON, nullable(j.CandidateSnapshotJSON), nullable(j.ResultRef),
		sqlNullString(j.LastErrorCode), sqlNullString(j.LastErrorRedacted),
		j.CreatedAt, j.UpdatedAt)
	if err != nil {
		if isUniqueErr(err) {
			existing, gerr := s.getJobByKeyLocked(ctx, j.JobKey)
			if gerr != nil {
				return nil, false, gerr
			}
			return existing, false, nil
		}
		return nil, false, err
	}
	return j, true, nil
}

func isUniqueErr(err error) bool {
	return err != nil && (errors.Is(err, sql.ErrNoRows) ||
		strings.Contains(err.Error(), "UNIQUE") || strings.Contains(err.Error(), "constraint failed"))
}

func (s *Shard) getJobByKeyLocked(ctx context.Context, key string) (*Job, error) {
	row := s.r.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM jobs WHERE job_key=?`, key)
	j, err := scanJob(row)
	if err != nil {
		return nil, err
	}
	return &j, nil
}

func (s *Shard) GetJob(ctx context.Context, id string) (*Job, error) {
	s.opMu.RLock()
	defer s.opMu.RUnlock()
	row := s.r.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id=?`, id)
	j, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &j, nil
}

// ClaimDueJobs atomically moves due jobs to running with a lease. Because one
// daemon owns the directory lock, no cross-process claim protocol is needed,
// but the DB transaction still guards crashes and duplicate ticks.
func (s *Shard) ClaimDueJobs(ctx context.Context, now int64, lease time.Duration, limit int) ([]Job, error) {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT `+jobColumns+` FROM jobs
		WHERE status IN ('pending','retry_wait')
		AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
		AND expires_at > ?
		ORDER BY scheduled_at ASC LIMIT ?`, now, now, limit)
	if err != nil {
		return nil, err
	}
	var jobs []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		jobs = append(jobs, j)
	}
	rows.Close()
	for i := range jobs {
		jobs[i].Status = JobRunning
		jobs[i].AttemptCount++
		jobs[i].LeaseUntil = ptrInt64(now + int64(lease.Seconds()))
		_, err := tx.ExecContext(ctx, `UPDATE jobs SET status='running', attempt_count=?,
			lease_until=?, heartbeat_at=?, updated_at=? WHERE id=?`,
			jobs[i].AttemptCount, jobs[i].LeaseUntil, now, now, jobs[i].ID)
		if err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return jobs, nil
}

func ptrInt64(v int64) *int64 { return &v }

func (s *Shard) UpdateJobStatus(ctx context.Context, id, status, resultRef, errCode, errMsg string, nextAttemptAt *int64, updatedAt int64) error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	_, err := s.w.ExecContext(ctx, `UPDATE jobs SET status=?, result_ref=?, last_error_code=?,
		last_error_redacted=?, next_attempt_at=?, lease_until=NULL, updated_at=? WHERE id=?`,
		status, nullable(resultRef), nullable(errCode), nullable(errMsg), sqlNullInt64(nextAttemptAt), updatedAt, id)
	return err
}

func (s *Shard) SetJobCandidates(ctx context.Context, id, snapshotJSON string, updatedAt int64) error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	_, err := s.w.ExecContext(ctx, `UPDATE jobs SET candidate_snapshot_json=?, updated_at=? WHERE id=?`,
		snapshotJSON, updatedAt, id)
	return err
}

func (s *Shard) ExtendJobLease(ctx context.Context, id string, until int64) error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	_, err := s.w.ExecContext(ctx, `UPDATE jobs SET lease_until=?, heartbeat_at=? WHERE id=?`, until, timeNow(), id)
	return err
}

// RecoverExpiredLeases re-queues jobs whose worker died mid-run.
func (s *Shard) RecoverExpiredLeases(ctx context.Context, now int64) (int, error) {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	res, err := s.w.ExecContext(ctx, `UPDATE jobs SET status='retry_wait', next_attempt_at=?,
		lease_until=NULL, last_error_code='lease_expired', updated_at=?
		WHERE status='running' AND lease_until IS NOT NULL AND lease_until < ?`, now, now, now)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (s *Shard) ExpireOldJobs(ctx context.Context, now int64) (int, error) {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	res, err := s.w.ExecContext(ctx, `UPDATE jobs SET status='expired', updated_at=?
		WHERE status IN ('pending','retry_wait') AND expires_at <= ?`, now, now)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (m *ShardManager) ClaimDueJobs(ctx context.Context, now int64, lease time.Duration, limitPerShard int) ([]JobWithYear, error) {
	shards, err := m.AllShards()
	if err != nil {
		return nil, err
	}
	var out []JobWithYear
	for _, s := range shards {
		jobs, err := s.ClaimDueJobs(ctx, now, lease, limitPerShard)
		if err != nil {
			return nil, err
		}
		for _, j := range jobs {
			out = append(out, JobWithYear{Job: j, OwnerYear: s.Year})
		}
	}
	return out, nil
}

type JobWithYear struct {
	Job
	OwnerYear int
}

func (m *ShardManager) RecoverJobLeases(ctx context.Context, now int64) (int, error) {
	shards, err := m.AllShards()
	if err != nil {
		return 0, err
	}
	total := 0
	for _, s := range shards {
		n, err := s.RecoverExpiredLeases(ctx, now)
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

func (m *ShardManager) ExpireOldJobs(ctx context.Context, now int64) (int, error) {
	shards, err := m.AllShards()
	if err != nil {
		return 0, err
	}
	total := 0
	for _, s := range shards {
		n, err := s.ExpireOldJobs(ctx, now)
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

func (s *Shard) GetJobByKey(ctx context.Context, key string) (*Job, error) {
	s.opMu.RLock()
	defer s.opMu.RUnlock()
	return s.getJobByKeyLocked(ctx, key)
}

func (s *Shard) RecentJobs(ctx context.Context, limit int) ([]Job, error) {
	s.opMu.RLock()
	defer s.opMu.RUnlock()
	rows, err := s.r.QueryContext(ctx, `SELECT `+jobColumns+` FROM jobs ORDER BY scheduled_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

var timeNow = func() int64 { return nowUnix() }
var _ = time.Duration(0)
var _ = strings.Contains
