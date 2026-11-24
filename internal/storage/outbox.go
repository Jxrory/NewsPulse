package storage

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

func insertOutboxTx(ctx context.Context, tx *sql.Tx, o *OutboxItem) (bool, error) {
	if o.ID == "" {
		id, err := NewID()
		if err != nil {
			return false, err
		}
		o.ID = id
	}
	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO notification_outbox(
		id, digest_id, dedup_key, provider, recipient_alias, part_no,
		resend_revision, resend_reason, payload_json, payload_hash, status,
		send_attempt_count, next_attempt_at, expires_at, created_at, updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		o.ID, o.DigestID, o.DedupKey, o.Provider, o.RecipientAlias, o.PartNo,
		o.ResendRevision, nullable(o.ResendReason), o.PayloadJSON, o.PayloadHash,
		o.Status, o.SendAttemptCount, sqlNullInt64(o.NextAttemptAt), o.ExpiresAt,
		o.CreatedAt, o.UpdatedAt)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (s *Shard) EnqueueOutbox(ctx context.Context, o *OutboxItem) (string, bool, error) {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback()
	created, err := insertOutboxTx(ctx, tx, o)
	if err != nil {
		return "", false, err
	}
	if !created {
		var id string
		if err := tx.QueryRowContext(ctx, `SELECT id FROM notification_outbox WHERE dedup_key=? OR (digest_id=? AND provider=? AND recipient_alias=? AND part_no=? AND resend_revision=?)`,
			o.DedupKey, o.DigestID, o.Provider, o.RecipientAlias, o.PartNo, o.ResendRevision).Scan(&id); err != nil {
			return "", false, err
		}
		return id, false, tx.Commit()
	}
	return o.ID, true, tx.Commit()
}

type OutboxFilter struct {
	DigestID string
	Status   string
	Limit    int
}

const outboxColumns = `id, digest_id, dedup_key, provider, recipient_alias, part_no,
 resend_revision, resend_reason, payload_json, payload_hash, status,
 send_attempt_count, next_attempt_at, lease_until, expires_at, sent_at,
 last_error_code, created_at, updated_at`

func scanOutbox(sc interface {
	Scan(dest ...interface{}) error
}) (OutboxItem, error) {
	var o OutboxItem
	var reason, resp, errCode sql.NullString
	var next, lease, sent sql.NullInt64
	err := sc.Scan(&o.ID, &o.DigestID, &o.DedupKey, &o.Provider, &o.RecipientAlias,
		&o.PartNo, &o.ResendRevision, &reason, &o.PayloadJSON, &o.PayloadHash,
		&o.Status, &o.SendAttemptCount, &next, &lease, &o.ExpiresAt, &sent,
		&errCode, &o.CreatedAt, &o.UpdatedAt)
	if err != nil {
		return o, err
	}
	o.ResendReason = nullStr(reason)
	o.NextAttemptAt = int64Ptr(next)
	o.LeaseUntil = int64Ptr(lease)
	o.SentAt = int64Ptr(sent)
	o.LastErrorCode = nullStr(errCode)
	_ = resp
	return o, nil
}

func (s *Shard) ListOutbox(ctx context.Context, f OutboxFilter) ([]OutboxItem, error) {
	s.opMu.RLock()
	defer s.opMu.RUnlock()
	q := `SELECT ` + outboxColumns + ` FROM notification_outbox WHERE 1=1`
	var args []interface{}
	if f.DigestID != "" {
		q += ` AND digest_id=?`
		args = append(args, f.DigestID)
	}
	if f.Status != "" {
		q += ` AND status=?`
		args = append(args, f.Status)
	}
	q += ` ORDER BY created_at DESC, id DESC LIMIT ?`
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	args = append(args, limit)
	rows, err := s.r.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OutboxItem
	for rows.Next() {
		o, err := scanOutbox(rows)
		if err != nil {
			return nil, err
		}
		o.OwnerYear = s.Year
		out = append(out, o)
	}
	return out, rows.Err()
}

// ClaimDueOutbox selects due notifications, moves them to sending and creates
// an attempt row. The HTTP call happens outside this transaction.
func (s *Shard) ClaimDueOutbox(ctx context.Context, now int64, lease time.Duration, limit int) ([]OutboxItem, error) {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT `+outboxColumns+` FROM notification_outbox
		WHERE status IN ('pending','retry_wait') AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
		ORDER BY created_at ASC LIMIT ?`, now, limit)
	if err != nil {
		return nil, err
	}
	var items []OutboxItem
	for rows.Next() {
		o, err := scanOutbox(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		items = append(items, o)
	}
	rows.Close()
	for i := range items {
		o := &items[i]
		o.Status = OutboxSending
		o.SendAttemptCount++
		o.LeaseUntil = ptrInt64(now + int64(lease.Seconds()))
		_, err := tx.ExecContext(ctx, `UPDATE notification_outbox SET status='sending',
			send_attempt_count=?, lease_until=?, updated_at=? WHERE id=?`,
			o.SendAttemptCount, o.LeaseUntil, now, o.ID)
		if err != nil {
			return nil, err
		}
		o.AttemptID, _ = NewID()
		_, err = tx.ExecContext(ctx, `INSERT INTO notification_attempts(id,outbox_id,started_at,result) VALUES(?,?,?,'sending')`,
			o.AttemptID, o.ID, now)
		if err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return items, nil
}

type OutboxResult struct {
	Status        string
	HTTPStatus    *int64
	ProviderCode  string
	ErrorCode     string
	ResponseJSON  string
	NextAttemptAt *int64
	Now           int64
}

// FinishOutboxAttempt records the outcome and clamps terminal states. An
// explicit provider success is required before marking sent.
func (s *Shard) FinishOutboxAttempt(ctx context.Context, o *OutboxItem, attemptID string, r OutboxResult) error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if r.Status == OutboxSent {
		_, err = tx.ExecContext(ctx, `UPDATE notification_outbox SET status='sent',
			provider_response_redacted_json=?, sent_at=?, lease_until=NULL,
			next_attempt_at=NULL, updated_at=? WHERE id=?`,
			nullable(r.ResponseJSON), r.Now, r.Now, o.ID)
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE notification_outbox SET status=?,
			provider_response_redacted_json=?, last_error_code=?,
			next_attempt_at=?, lease_until=NULL, updated_at=? WHERE id=?`,
			r.Status, nullable(r.ResponseJSON), nullable(r.ErrorCode),
			sqlNullInt64(r.NextAttemptAt), r.Now, o.ID)
	}
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE notification_attempts SET finished_at=?,
		http_status=?, provider_code=?, result=?, error_code=?, response_redacted_json=?
		WHERE id=?`,
		r.Now, sqlNullInt64(r.HTTPStatus), nullable(r.ProviderCode), r.Status,
		nullable(r.ErrorCode), nullable(r.ResponseJSON), attemptID)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// RecoverSendingOnRestart converts stale in-flight rows to send_unknown.
// We must never assume a request that may have reached the provider did not.
func (s *Shard) RecoverSendingOnRestart(ctx context.Context, now int64) (int, error) {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id FROM notification_outbox
		WHERE status='sending' AND (lease_until IS NULL OR lease_until < ?)`, now)
	if err != nil {
		return 0, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	for _, id := range ids {
		_, err = tx.ExecContext(ctx, `UPDATE notification_outbox SET status='send_unknown',
			last_error_code='lease_expired', lease_until=NULL, updated_at=? WHERE id=?`, now, id)
		if err != nil {
			return 0, err
		}
		_, err = tx.ExecContext(ctx, `UPDATE notification_attempts SET finished_at=?,
			result='send_unknown', error_code='lease_expired' WHERE outbox_id=? AND result='sending'`, now, id)
		if err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return len(ids), nil
}

func (s *Shard) ExpireOldOutbox(ctx context.Context, now int64) (int, error) {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	res, err := s.w.ExecContext(ctx, `UPDATE notification_outbox SET status='expired', updated_at=?
		WHERE status IN ('pending','retry_wait') AND expires_at <= ?`, now, now)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

type OutboxStatusCounts struct {
	Pending     int64 `json:"pending"`
	Sending     int64 `json:"sending"`
	Sent        int64 `json:"sent"`
	RetryWait   int64 `json:"retry_wait"`
	Failed      int64 `json:"failed"`
	SendUnknown int64 `json:"send_unknown"`
	Expired     int64 `json:"expired"`
}

func (s *Shard) OutboxCounts(ctx context.Context) (OutboxStatusCounts, error) {
	s.opMu.RLock()
	defer s.opMu.RUnlock()
	rows, err := s.r.QueryContext(ctx, `SELECT status, count(*) FROM notification_outbox GROUP BY status`)
	if err != nil {
		return OutboxStatusCounts{}, err
	}
	defer rows.Close()
	var c OutboxStatusCounts
	for rows.Next() {
		var st string
		var n int64
		if err := rows.Scan(&st, &n); err != nil {
			return c, err
		}
		switch st {
		case OutboxPending:
			c.Pending += n
		case OutboxSending:
			c.Sending += n
		case OutboxSent:
			c.Sent += n
		case OutboxRetryWait:
			c.RetryWait += n
		case OutboxFailed:
			c.Failed += n
		case OutboxSendUnknown:
			c.SendUnknown += n
		case OutboxExpired:
			c.Expired += n
		}
	}
	return c, rows.Err()
}

func (m *ShardManager) OutboxCounts(ctx context.Context) (OutboxStatusCounts, error) {
	shards, err := m.AllShards()
	if err != nil {
		return OutboxStatusCounts{}, err
	}
	var total OutboxStatusCounts
	for _, s := range shards {
		c, err := s.OutboxCounts(ctx)
		if err != nil {
			return total, err
		}
		total.Pending += c.Pending
		total.Sending += c.Sending
		total.Sent += c.Sent
		total.RetryWait += c.RetryWait
		total.Failed += c.Failed
		total.SendUnknown += c.SendUnknown
		total.Expired += c.Expired
	}
	return total, nil
}

// NotificationAttemptsSince counts requests (including failures) for the
// project-side daily request budget.
func (m *ShardManager) NotificationAttemptsSince(ctx context.Context, since int64) (int64, error) {
	shards, err := m.AllShards()
	if err != nil {
		return 0, err
	}
	var total int64
	for _, s := range shards {
		var n int64
		err := s.r.QueryRowContext(ctx, `SELECT count(*) FROM notification_attempts WHERE started_at >= ?`, since).Scan(&n)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return total, err
		}
		total += n
	}
	return total, nil
}

func (m *ShardManager) ClaimDueOutbox(ctx context.Context, now int64, lease time.Duration, limitPerShard int) ([]OutboxItem, error) {
	shards, err := m.AllShards()
	if err != nil {
		return nil, err
	}
	var out []OutboxItem
	for _, s := range shards {
		items, err := s.ClaimDueOutbox(ctx, now, lease, limitPerShard)
		if err != nil {
			return nil, err
		}
		for i := range items {
			items[i].OwnerYear = s.Year
		}
		out = append(out, items...)
	}
	return out, nil
}

func (m *ShardManager) RecoverOutboxOnRestart(ctx context.Context, now int64) (int, error) {
	shards, err := m.AllShards()
	if err != nil {
		return 0, err
	}
	total := 0
	for _, s := range shards {
		n, err := s.RecoverSendingOnRestart(ctx, now)
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

func (m *ShardManager) ExpireOldOutbox(ctx context.Context, now int64) (int, error) {
	shards, err := m.AllShards()
	if err != nil {
		return 0, err
	}
	total := 0
	for _, s := range shards {
		n, err := s.ExpireOldOutbox(ctx, now)
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

func (s *Shard) GetOutbox(ctx context.Context, id string) (*OutboxItem, error) {
	s.opMu.RLock()
	defer s.opMu.RUnlock()
	row := s.r.QueryRowContext(ctx, `SELECT `+outboxColumns+` FROM notification_outbox WHERE id=?`, id)
	o, err := scanOutbox(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &o, nil
}

func (s *Shard) OutboxForDigest(ctx context.Context, digestID string) ([]OutboxItem, error) {
	return s.ListOutbox(ctx, OutboxFilter{DigestID: digestID, Limit: 1000})
}
