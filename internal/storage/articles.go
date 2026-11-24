package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

var ErrAliasConflict = errors.New("alias already points to a different article")

type ArticleBundle struct {
	Article      Article
	Aliases      []Alias
	Observation  Observation
	SnapshotJSON string
}

func (s *Shard) findAliases(ctx context.Context, hashes []string) ([]AliasHit, error) {
	s.opMu.RLock()
	defer s.opMu.RUnlock()
	if len(hashes) == 0 {
		return nil, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(hashes)), ",")
	args := make([]interface{}, len(hashes))
	for i, h := range hashes {
		args[i] = h
	}
	rows, err := s.r.QueryContext(ctx,
		`SELECT alias_hash, alias_kind, alias_value, article_id FROM article_aliases WHERE alias_hash IN (`+placeholders+`)`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AliasHit
	for rows.Next() {
		var h AliasHit
		h.Year = s.Year
		if err := rows.Scan(&h.AliasHash, &h.AliasKind, &h.AliasValue, &h.ArticleID); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

func scanArticle(sc interface {
	Scan(dest ...interface{}) error
}) (Article, error) {
	var a Article
	var published, updated sql.NullInt64
	var pubRaw, url sql.NullString
	var flags string
	err := sc.Scan(&a.ID, &a.IdentityVersion, &url, &a.Title, &a.NormalizedTitle,
		&a.SummaryText, &a.ContentText, &a.ContentHash, &a.Language, &a.Section,
		&a.PublisherID, &a.PublisherGroup, &pubRaw, &published, &updated,
		&a.FirstSeenAt, &a.LastSeenAt, &a.DateQuality, &flags)
	if err != nil {
		return a, err
	}
	a.CanonicalURL = nullStr(url)
	a.PublishedAtRaw = nullStr(pubRaw)
	a.PublishedAt = int64Ptr(published)
	a.UpdatedAt = int64Ptr(updated)
	a.QualityFlags = decodeStringSlice(flags)
	return a, nil
}

const articleColumns = `id, identity_version, canonical_url, title, normalized_title,
 summary_text, content_text, content_hash, language, section, publisher_id,
 publisher_group, published_at_raw, published_at, updated_at, first_seen_at,
 last_seen_at, date_quality, quality_flags_json`

func (s *Shard) GetArticle(ctx context.Context, id string) (*Article, error) {
	s.opMu.RLock()
	defer s.opMu.RUnlock()
	row := s.r.QueryRowContext(ctx, `SELECT `+articleColumns+` FROM articles WHERE id=?`, id)
	a, err := scanArticle(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func (s *Shard) InsertArticleBundle(ctx context.Context, b ArticleBundle) error {
	if b.Article.ID == "" {
		return errors.New("article id is required")
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := insertArticleRow(ctx, tx, b.Article); err != nil {
		return err
	}
	for _, al := range b.Aliases {
		if _, err := tx.ExecContext(ctx, `INSERT INTO article_aliases(alias_hash,alias_kind,alias_value,article_id,created_at) VALUES(?,?,?,?,?)`,
			al.Hash, al.Kind, al.Value, b.Article.ID, b.Article.FirstSeenAt); err != nil {
			if strings.Contains(err.Error(), "UNIQUE") {
				return ErrAliasConflict
			}
			return err
		}
	}
	if err := upsertObservation(ctx, tx, &b.Observation); err != nil {
		return err
	}
	if b.SnapshotJSON != "" {
		if err := insertRevision(ctx, tx, b.Article.ID, b.Article.FirstSeenAt, b.Article.ContentHash, b.SnapshotJSON); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func insertArticleRow(ctx context.Context, tx *sql.Tx, a Article) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO articles(`+articleColumns+`)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		a.ID, a.IdentityVersion, sqlNullString(a.CanonicalURL), a.Title, a.NormalizedTitle,
		a.SummaryText, a.ContentText, a.ContentHash, a.Language, a.Section,
		a.PublisherID, a.PublisherGroup, sqlNullString(a.PublishedAtRaw),
		sqlNullInt64(a.PublishedAt), sqlNullInt64(a.UpdatedAt), a.FirstSeenAt,
		a.LastSeenAt, a.DateQuality, marshalJSON(a.QualityFlags))
	return err
}

func (s *Shard) UpdateArticleBundle(ctx context.Context, b ArticleBundle) error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `UPDATE articles SET
		canonical_url=?, title=?, normalized_title=?, summary_text=?, content_text=?,
		content_hash=?, language=?, section=?, published_at_raw=?, published_at=?,
		updated_at=?, last_seen_at=?, date_quality=?, quality_flags_json=?
		WHERE id=?`,
		sqlNullString(b.Article.CanonicalURL), b.Article.Title, b.Article.NormalizedTitle,
		b.Article.SummaryText, b.Article.ContentText, b.Article.ContentHash,
		b.Article.Language, b.Article.Section, sqlNullString(b.Article.PublishedAtRaw),
		sqlNullInt64(b.Article.PublishedAt), sqlNullInt64(b.Article.UpdatedAt),
		b.Article.LastSeenAt, b.Article.DateQuality, marshalJSON(b.Article.QualityFlags),
		b.Article.ID)
	if err != nil {
		return err
	}
	for _, al := range b.Aliases {
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO article_aliases(alias_hash,alias_kind,alias_value,article_id,created_at) VALUES(?,?,?,?,?)`,
			al.Hash, al.Kind, al.Value, b.Article.ID, b.Article.FirstSeenAt); err != nil {
			return err
		}
		var owner string
		if err := tx.QueryRowContext(ctx, `SELECT article_id FROM article_aliases WHERE alias_hash=?`, al.Hash).Scan(&owner); err != nil {
			return err
		}
		if owner != b.Article.ID {
			return ErrAliasConflict
		}
	}
	if err := upsertObservation(ctx, tx, &b.Observation); err != nil {
		return err
	}
	if b.SnapshotJSON != "" {
		if err := insertRevision(ctx, tx, b.Article.ID, b.Article.LastSeenAt, b.Article.ContentHash, b.SnapshotJSON); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func upsertObservation(ctx context.Context, tx *sql.Tx, o *Observation) error {
	if o.ID == "" {
		id, err := NewID()
		if err != nil {
			return err
		}
		o.ID = id
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO article_observations(
		id, article_id, source_id, source_item_key, guid_raw, url_raw, title_raw,
		description_raw, content_raw, published_at_raw, publisher_claim_raw,
		raw_item_json, raw_item_hash, first_seen_at, last_seen_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(source_id, source_item_key) DO UPDATE SET
		article_id=excluded.article_id, guid_raw=excluded.guid_raw, url_raw=excluded.url_raw,
		title_raw=excluded.title_raw, description_raw=excluded.description_raw,
		content_raw=excluded.content_raw, published_at_raw=excluded.published_at_raw,
		publisher_claim_raw=excluded.publisher_claim_raw,
		raw_item_json=excluded.raw_item_json, raw_item_hash=excluded.raw_item_hash,
		last_seen_at=excluded.last_seen_at`,
		o.ID, o.ArticleID, o.SourceID, o.SourceItemKey, sqlNullString(o.GUIDRaw),
		sqlNullString(o.URLRaw), o.TitleRaw, sqlNullString(o.DescriptionRaw),
		sqlNullString(o.ContentRaw), sqlNullString(o.PublishedAtRaw),
		sqlNullString(o.PublisherClaimRaw), o.RawItemJSON, o.RawItemHash,
		o.FirstSeenAt, o.LastSeenAt)
	return err
}

func insertRevision(ctx context.Context, tx *sql.Tx, articleID string, observedAt int64, hash, snapshot string) error {
	id, err := NewID()
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO article_revisions(id,article_id,observed_at,content_hash,snapshot_json) VALUES(?,?,?,?,?)`,
		id, articleID, observedAt, hash, snapshot)
	return err
}

type ArticleFilter struct {
	WindowStart     *int64
	WindowEnd       *int64
	FirstSeenBefore *int64
	FirstSeenAfter  *int64
	Section         string
	PublisherGroup  string
	SourceID        string
	OnlyPublished   bool
	OnlyUndated     bool
	Limit           int
	AfterPublished  *int64
	AfterID         string
	OrderBy         string
	PublisherID     string
}

func (f ArticleFilter) orderColumn() string {
	if f.OrderBy == "first_seen" {
		return "first_seen_at"
	}
	return "published_at"
}

func (s *Shard) QueryArticles(ctx context.Context, f ArticleFilter) ([]Article, error) {
	s.opMu.RLock()
	defer s.opMu.RUnlock()
	var where []string
	var args []interface{}
	if f.OnlyPublished {
		where = append(where, "published_at IS NOT NULL")
	}
	if f.WindowStart != nil {
		where = append(where, "published_at >= ?")
		args = append(args, *f.WindowStart)
	}
	if f.WindowEnd != nil {
		where = append(where, "published_at < ?")
		args = append(args, *f.WindowEnd)
	}
	if f.OnlyUndated {
		where = append(where, "published_at IS NULL")
	}
	if f.FirstSeenBefore != nil {
		where = append(where, "first_seen_at <= ?")
		args = append(args, *f.FirstSeenBefore)
	}
	if f.FirstSeenAfter != nil {
		where = append(where, "first_seen_at >= ?")
		args = append(args, *f.FirstSeenAfter)
	}
	if f.Section != "" {
		where = append(where, "section = ?")
		args = append(args, f.Section)
	}
	if f.PublisherGroup != "" {
		where = append(where, "publisher_group = ?")
		args = append(args, f.PublisherGroup)
	}
	if f.PublisherID != "" {
		where = append(where, "publisher_id = ?")
		args = append(args, f.PublisherID)
	}
	if f.SourceID != "" {
		where = append(where, "EXISTS(SELECT 1 FROM article_observations o WHERE o.article_id=articles.id AND o.source_id=?)")
		args = append(args, f.SourceID)
	}
	order := f.orderColumn()
	if f.AfterPublished != nil {
		where = append(where, "("+order+" < ? OR ("+order+" = ? AND id < ?))")
		args = append(args, *f.AfterPublished, *f.AfterPublished, f.AfterID)
	}
	q := `SELECT ` + articleColumns + ` FROM articles`
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY " + order + " DESC, id DESC"
	limit := f.Limit
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	q += fmt.Sprintf(" LIMIT %d", limit)

	rows, err := s.r.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]Article, 0, limit)
	for rows.Next() {
		a, err := scanArticle(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// QueryArticlesAcrossShards merges per-shard results and applies one global
// ordering, then trims to the requested limit.
func (m *ShardManager) QueryArticlesAcrossShards(ctx context.Context, f ArticleFilter) ([]ArticleWithYear, error) {
	shards, err := m.AllShards()
	if err != nil {
		return nil, err
	}
	var all []ArticleWithYear
	for _, s := range shards {
		items, err := s.QueryArticles(ctx, f)
		if err != nil {
			return nil, err
		}
		for _, a := range items {
			all = append(all, ArticleWithYear{Article: a, OwnerYear: s.Year})
		}
	}
	order := f.orderColumn()
	sort.SliceStable(all, func(i, j int) bool {
		vi, vj := keyOf(all[i], order), keyOf(all[j], order)
		if vi != vj {
			return vi > vj
		}
		return all[i].ID > all[j].ID
	})
	limit := f.Limit
	if limit <= 0 {
		limit = 200
	}
	if len(all) > limit {
		all = all[:limit]
	}
	return all, nil
}

func keyOf(a ArticleWithYear, column string) int64 {
	if column == "first_seen" {
		return a.FirstSeenAt
	}
	if a.PublishedAt == nil {
		return -1
	}
	return *a.PublishedAt
}

// RecentArticleRefs returns lightweight refs for repeat suppression checks.
func (s *Shard) ArticleRefsForSlot(ctx context.Context, slotKey string) (map[string]struct{}, error) {
	s.opMu.RLock()
	defer s.opMu.RUnlock()
	rows, err := s.r.QueryContext(ctx, `SELECT d.slot_key, i.article_refs_json FROM digest_items i JOIN digests d ON d.id=i.digest_id WHERE d.slot_key=?`, slotKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]struct{}{}
	for rows.Next() {
		var slot, refs string
		if err := rows.Scan(&slot, &refs); err != nil {
			return nil, err
		}
		var parsed []ArticleRef
		_ = json.Unmarshal([]byte(refs), &parsed)
		for _, r := range parsed {
			out[r.ID] = struct{}{}
		}
	}
	return out, rows.Err()
}

var _ = time.Now
