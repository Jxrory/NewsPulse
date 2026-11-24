package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/jxrory/newspulse/internal/config"
	"github.com/jxrory/newspulse/internal/logging"
)

// Shard is one annual business database. The write pool has a single
// connection; opMu additionally serializes writes in-process and protects
// against eviction while an operation is running.
type Shard struct {
	Year int
	Path string

	w        *sql.DB
	r        *sql.DB
	opMu     sync.RWMutex
	lastUsed time.Time
	useMu    sync.Mutex
}

func (s *Shard) W() *sql.DB { return s.w }
func (s *Shard) R() *sql.DB { return s.r }

func (s *Shard) touch() {
	s.useMu.Lock()
	s.lastUsed = time.Now()
	s.useMu.Unlock()
}

func (s *Shard) close() error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	var errs []error
	if s.w != nil {
		errs = append(errs, s.w.Close())
	}
	if s.r != nil {
		errs = append(errs, s.r.Close())
	}
	return errors.Join(errs...)
}

// ShardManager owns annual files, the single-writer gate and the cross-shard
// alias index lookup. It creates missing annual shards on first write and
// never silently substitutes an unreadable archive file.
type ShardManager struct {
	cfg       *config.Config
	dataDir   string
	annualLoc *time.Location

	mu     sync.Mutex
	shards map[int]*Shard

	writeGate sync.RWMutex
	identity  sync.Mutex
}

func NewShardManager(cfg *config.Config) (*ShardManager, error) {
	loc, err := time.LoadLocation(cfg.App.AnnualTimezone)
	if err != nil {
		return nil, fmt.Errorf("annual timezone: %w", err)
	}
	m := &ShardManager{
		cfg:       cfg,
		dataDir:   cfg.App.DataDir,
		annualLoc: loc,
		shards:    make(map[int]*Shard),
	}
	if err := os.MkdirAll(m.dbDir(), 0o700); err != nil {
		return nil, fmt.Errorf("create db dir: %w", err)
	}
	return m, nil
}

func (m *ShardManager) dbDir() string { return filepath.Join(m.dataDir, "db") }

func (m *ShardManager) AnnualLocation() *time.Location { return m.annualLoc }

func (m *ShardManager) YearOf(t time.Time) int { return t.In(m.annualLoc).Year() }

func (m *ShardManager) PathForYear(year int) string {
	return filepath.Join(m.dbDir(), fmt.Sprintf(m.cfg.Storage.FilenamePattern, year))
}

func (m *ShardManager) BeginWrite() func() {
	m.writeGate.RLock()
	return m.writeGate.RUnlock
}

// WithWriteLock runs fn while new writes are paused. Used for consistent
// backups; reads remain available.
func (m *ShardManager) WithWriteLock(fn func() error) error {
	m.writeGate.Lock()
	defer m.writeGate.Unlock()
	return fn()
}

func (m *ShardManager) IdentityLock() func() {
	m.identity.Lock()
	return m.identity.Unlock
}

func (m *ShardManager) yearRegexp() (*regexp.Regexp, error) {
	pat := m.cfg.Storage.FilenamePattern
	if !strings.Contains(pat, "%04d") {
		return nil, errors.New("filename_pattern must contain %04d")
	}
	esc := regexp.QuoteMeta(pat)
	esc = strings.Replace(esc, regexp.QuoteMeta("%04d"), `([0-9]{4})`, 1)
	return regexp.Compile("^" + esc + "$")
}

// ShardYears returns strictly-validated annual files currently present.
func (m *ShardManager) ShardYears() ([]int, error) {
	entries, err := os.ReadDir(m.dbDir())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	re, err := m.yearRegexp()
	if err != nil {
		return nil, err
	}
	var years []int
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		mt := re.FindStringSubmatch(e.Name())
		if mt == nil {
			continue
		}
		y, _ := strconv.Atoi(mt[1])
		if y < 1970 || y > 9999 {
			continue
		}
		years = append(years, y)
	}
	sort.Ints(years)
	return years, nil
}

func (m *ShardManager) dsn(path string, readOnly bool) string {
	q := url.Values{}
	q.Set("_busy_timeout", strconv.FormatInt(m.cfg.Storage.BusyTimeout.D().Milliseconds(), 10))
	q.Set("_journal_mode", m.cfg.Storage.JournalMode)
	q.Set("_synchronous", m.cfg.Storage.Synchronous)
	q.Set("_foreign_keys", "on")
	if readOnly {
		q.Set("mode", "ro")
	} else {
		q.Set("mode", "rwc")
	}
	u := url.URL{Scheme: "file", Path: path, RawQuery: q.Encode()}
	return u.String()
}

func (m *ShardManager) openPools(path string) (*sql.DB, *sql.DB, error) {
	w, err := sql.Open("sqlite3", m.dsn(path, false))
	if err != nil {
		return nil, nil, err
	}
	w.SetMaxOpenConns(1)
	w.SetMaxIdleConns(1)
	w.SetConnMaxLifetime(0)
	if err := w.Ping(); err != nil {
		w.Close()
		return nil, nil, err
	}
	readConns := m.cfg.Storage.ReadConnectionsPerShard
	if readConns < 1 {
		readConns = 1
	}
	r, err := sql.Open("sqlite3", m.dsn(path, true))
	if err != nil {
		w.Close()
		return nil, nil, err
	}
	r.SetMaxOpenConns(readConns)
	r.SetMaxIdleConns(readConns)
	if err := r.Ping(); err != nil {
		r.Close()
		w.Close()
		return nil, nil, err
	}
	return w, r, nil
}

// Open opens (and fully migrates) an existing annual shard. Missing years are
// not created here; use OpenForWrite.
func (m *ShardManager) Open(year int) (*Shard, error) {
	if year < 1970 || year > 9999 {
		return nil, fmt.Errorf("invalid shard year %d", year)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.openLocked(year)
}

func (m *ShardManager) openLocked(year int) (*Shard, error) {
	if s, ok := m.shards[year]; ok {
		s.touch()
		return s, nil
	}
	path := m.PathForYear(year)
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("shard %d unavailable: %w", year, err)
	}
	w, r, err := m.openPools(path)
	if err != nil {
		return nil, fmt.Errorf("open shard %d: %w", year, err)
	}
	s := &Shard{Year: year, Path: path, w: w, r: r}
	if err := ApplyMigrations(s); err != nil {
		s.close()
		return nil, fmt.Errorf("migrate shard %d: %w", year, err)
	}
	if err := m.checkAnnualTimezone(s); err != nil {
		s.close()
		return nil, err
	}
	if err := m.ensureSourcesOn(s, time.Now()); err != nil {
		s.close()
		return nil, err
	}
	m.shards[year] = s
	m.evictLocked()
	return s, nil
}

func (m *ShardManager) checkAnnualTimezone(s *Shard) error {
	var v string
	err := s.r.QueryRow(`SELECT value FROM app_meta WHERE key='annual_timezone'`).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read annual timezone metadata: %w", err)
	}
	if v != m.cfg.App.AnnualTimezone {
		return fmt.Errorf("shard %d was created with annual_timezone %q, config now uses %q; refusing to mix shards (requires data migration)", s.Year, v, m.cfg.App.AnnualTimezone)
	}
	return nil
}

// OpenForWrite opens or creates the annual shard that owns the given capture
// time. Creation is migration-checked before use.
func (m *ShardManager) OpenForWrite(observedAt time.Time) (*Shard, error) {
	year := m.YearOf(observedAt)
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.shards[year]; ok {
		s.touch()
		return s, nil
	}
	path := m.PathForYear(year)
	_, statErr := os.Stat(path)
	isNew := errors.Is(statErr, os.ErrNotExist)
	if statErr != nil && !isNew {
		return nil, fmt.Errorf("shard %d unavailable: %w", year, statErr)
	}
	w, r, err := m.openPools(path)
	if err != nil {
		return nil, fmt.Errorf("open/create shard %d: %w", year, err)
	}
	s := &Shard{Year: year, Path: path, w: w, r: r}
	if err := ApplyMigrations(s); err != nil {
		s.close()
		return nil, fmt.Errorf("migrate new shard %d: %w", year, err)
	}
	var check string
	if err := s.r.QueryRow(`PRAGMA quick_check`).Scan(&check); err != nil || check != "ok" {
		s.close()
		return nil, fmt.Errorf("shard %d failed quick_check: %v %s", year, err, check)
	}
	if err := m.ensureAnnualTimezone(s, isNew); err != nil {
		s.close()
		return nil, err
	}
	if err := m.ensureSourcesOn(s, observedAt); err != nil {
		s.close()
		return nil, err
	}
	m.shards[year] = s
	m.evictLocked()
	return s, nil
}

func (m *ShardManager) ensureAnnualTimezone(s *Shard, isNew bool) error {
	if isNew {
		_, err := s.w.Exec(`INSERT OR REPLACE INTO app_meta(key,value) VALUES('annual_timezone',?)`, m.cfg.App.AnnualTimezone)
		return err
	}
	return m.checkAnnualTimezone(s)
}

// ensureSourcesOn records the configured source snapshot and creates initial
// source_state rows. The new-year state deliberately forces a first fetch.
func (m *ShardManager) sourcesConfigHash() string {
	srcs := append([]config.SourceConfig(nil), m.cfg.Sources...)
	sort.Slice(srcs, func(i, j int) bool { return srcs[i].ID < srcs[j].ID })
	h := sha256.New()
	for _, src := range srcs {
		fmt.Fprintf(h, "%s|%s|%s|%s|%s|%s|%s|%s|%t\n", src.ID, src.URL, src.PublisherID,
			src.PublisherGroup, src.Language, src.SectionHint, src.TimezoneHint,
			src.PollInterval.D().String(), src.Enabled)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (m *ShardManager) ensureSourcesOn(s *Shard, now time.Time) error {
	wantHash := m.sourcesConfigHash()
	var haveHash string
	err := s.r.QueryRow(`SELECT value FROM app_meta WHERE key='sources_config_hash'`).Scan(&haveHash)
	if err == nil && haveHash == wantHash {
		return nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	tx, err := s.w.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, src := range m.cfg.Sources {
		cfgJSON := fmt.Sprintf(`{"id":%q,"url":%q,"publisher_id":%q,"publisher_group":%q,"language":%q,"section_hint":%q,"timezone_hint":%q,"poll_interval":%q,"enabled":%t}`,
			src.ID, src.URL, src.PublisherID, src.PublisherGroup, src.Language, src.SectionHint, src.TimezoneHint, src.PollInterval.D().String(), src.Enabled)
		if _, err := tx.Exec(`INSERT INTO sources(id,url,publisher_id,publisher_group,language,section_hint,timezone_hint,config_json,updated_at)
			VALUES(?,?,?,?,?,?,?,?,?)
			ON CONFLICT(id) DO UPDATE SET url=excluded.url, publisher_id=excluded.publisher_id,
			publisher_group=excluded.publisher_group, language=excluded.language,
			section_hint=excluded.section_hint, timezone_hint=excluded.timezone_hint,
			config_json=excluded.config_json, updated_at=excluded.updated_at`,
			src.ID, src.URL, src.PublisherID, src.PublisherGroup, src.Language, src.SectionHint, nullable(src.TimezoneHint), cfgJSON, now.Unix()); err != nil {
			return fmt.Errorf("upsert source %s: %w", src.ID, err)
		}
		if _, err := tx.Exec(`INSERT OR IGNORE INTO source_state(source_id,next_fetch_at,health,updated_at) VALUES(?,?,'unknown',?)`,
			src.ID, now.Unix(), now.Unix()); err != nil {
			return fmt.Errorf("init source_state %s: %w", src.ID, err)
		}
	}
	if _, err := tx.Exec(`INSERT OR REPLACE INTO app_meta(key,value) VALUES('sources_config_hash',?)`, wantHash); err != nil {
		return err
	}
	return tx.Commit()
}

func nullable(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func (m *ShardManager) evictLocked() {
	max := m.cfg.Storage.MaxOpenShards
	if max < 1 || len(m.shards) <= max {
		return
	}
	var oldest *Shard
	for _, s := range m.shards {
		if oldest == nil {
			oldest = s
			continue
		}
		s.useMu.Lock()
		if s.lastUsed.Before(oldest.lastUsed) {
			oldest = s
		}
		s.useMu.Unlock()
	}
	if oldest != nil {
		if err := oldest.close(); err != nil {
			fmt.Fprintf(os.Stderr, "close shard %d: %s\n", oldest.Year, logging.ErrorString(err))
		}
		delete(m.shards, oldest.Year)
	}
}

func (m *ShardManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var errs []error
	for y, s := range m.shards {
		errs = append(errs, s.close())
		delete(m.shards, y)
	}
	return errors.Join(errs...)
}

// AliasHit records where an alias currently points.
type AliasHit struct {
	AliasHash  string
	AliasKind  string
	AliasValue string
	Year       int
	ArticleID  string
}

// LookupAliasesAllShards searches every existing annual shard. The identity
// lock must be held by the caller across lookup + write to keep the global
// alias space consistent.
func (m *ShardManager) LookupAliasesAllShards(ctx context.Context, hashes []string) ([]AliasHit, error) {
	if len(hashes) == 0 {
		return nil, nil
	}
	years, err := m.ShardYears()
	if err != nil {
		return nil, err
	}
	var hits []AliasHit
	for _, y := range years {
		s, err := m.Open(y)
		if err != nil {
			return nil, err
		}
		h, err := s.findAliases(ctx, hashes)
		if err != nil {
			return nil, err
		}
		for i := range h {
			h[i].Year = y
		}
		hits = append(hits, h...)
	}
	return hits, nil
}

// AllShards opens every existing annual shard for read-side aggregation.
func (m *ShardManager) AllShards() ([]*Shard, error) {
	years, err := m.ShardYears()
	if err != nil {
		return nil, err
	}
	out := make([]*Shard, 0, len(years))
	for _, y := range years {
		s, err := m.Open(y)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

// EnsureShardForScheduled opens the shard that owns a scheduled digest time.
func (m *ShardManager) EnsureShardForScheduled(t time.Time) (*Shard, error) {
	return m.OpenForWrite(t)
}

// Verify runs integrity checks across all annual shards.
func (m *ShardManager) Verify(ctx context.Context) ([]ShardVerifyResult, error) {
	shards, err := m.AllShards()
	if err != nil {
		return nil, err
	}
	var results []ShardVerifyResult
	for _, s := range shards {
		var check string
		err := s.r.QueryRowContext(ctx, `PRAGMA quick_check`).Scan(&check)
		res := ShardVerifyResult{Year: s.Year, Path: s.Path, QuickCheck: check}
		if err != nil {
			res.Error = err.Error()
		} else if check != "ok" {
			res.Error = "quick_check reported " + check
		}
		if err == nil {
			_ = s.r.QueryRowContext(ctx, `SELECT count(*) FROM articles`).Scan(&res.Articles)
			_ = s.r.QueryRowContext(ctx, `SELECT count(*) FROM digests`).Scan(&res.Digests)
			_ = s.r.QueryRowContext(ctx, `SELECT max(version) FROM schema_migrations`).Scan(&res.SchemaVersion)
		}
		results = append(results, res)
	}
	return results, nil
}

type ShardVerifyResult struct {
	Year          int    `json:"year"`
	Path          string `json:"path"`
	SchemaVersion int64  `json:"schema_version"`
	QuickCheck    string `json:"quick_check"`
	Articles      int64  `json:"articles"`
	Digests       int64  `json:"digests"`
	Error         string `json:"error,omitempty"`
}

// SQLiteVersion reports the embedded driver's runtime version.
func SQLiteVersion(s *Shard) string {
	var v string
	if err := s.r.QueryRow(`SELECT sqlite_version()`).Scan(&v); err != nil {
		return "unknown"
	}
	return v
}
