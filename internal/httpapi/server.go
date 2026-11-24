package httpapi

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jxrory/newspulse/internal/clock"
	"github.com/jxrory/newspulse/internal/config"
	"github.com/jxrory/newspulse/internal/logging"
	"github.com/jxrory/newspulse/internal/notify"
	"github.com/jxrory/newspulse/internal/scheduler"
	"github.com/jxrory/newspulse/internal/storage"
)

type Server struct {
	cfg   *config.Config
	mgr   *storage.ShardManager
	sched *scheduler.Service
	clk   clock.Clock
	log   *slog.Logger
}

func New(cfg *config.Config, mgr *storage.ShardManager, sched *scheduler.Service, clk clock.Clock, log *slog.Logger) *Server {
	if clk == nil {
		clk = clock.Real{}
	}
	if log == nil {
		log = slog.Default()
	}
	return &Server{cfg: cfg, mgr: mgr, sched: sched, clk: clk, log: log}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	auth := s.requireAuth
	mux.Handle("GET /api/v1/status", auth(http.HandlerFunc(s.handleStatus)))
	mux.Handle("GET /api/v1/articles", auth(http.HandlerFunc(s.handleArticles)))
	mux.Handle("GET /api/v1/digests", auth(http.HandlerFunc(s.handleDigests)))
	mux.Handle("GET /api/v1/digests/{year}/{id}", auth(http.HandlerFunc(s.handleDigestDetail)))
	mux.Handle("POST /api/v1/jobs/fetch", auth(http.HandlerFunc(s.handleFetchJob)))
	mux.Handle("POST /api/v1/jobs/digest", auth(http.HandlerFunc(s.handleDigestJob)))
	mux.Handle("POST /api/v1/digests/{year}/{id}/notify", auth(http.HandlerFunc(s.handleNotifyDigest)))
	return mux
}

func (s *Server) Start(ctx context.Context) error {
	addr := s.cfg.HTTP.Listen
	srv := &http.Server{
		Addr:              addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	s.log.Info("http api listening", "addr", addr)
	err := srv.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "service": "newspulse"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	now := s.clk.Now()
	ready := true
	problems := []string{}
	if _, err := s.mgr.OpenForWrite(now); err != nil {
		ready = false
		problems = append(problems, "storage: "+logging.ErrorString(err))
	}
	if s.cfg.AdminToken() == "" {
		ready = false
		problems = append(problems, "admin token environment variable is empty")
	}
	mode := "collection_only"
	if s.cfg.Schedule.Enabled {
		mode = "scheduled"
	}
	resp := map[string]interface{}{
		"ready": ready, "mode": mode, "problems": problems,
		"scheduled_notifications": s.cfg.Schedule.Enabled && s.cfg.Notification.Enabled,
		"time":                    now.In(s.cfg.DisplayLocation()).Format(time.RFC3339),
	}
	code := http.StatusOK
	if !ready {
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, resp)
}

func (s *Server) requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expected := s.cfg.AdminToken()
		if expected == "" {
			writeAPIError(w, http.StatusServiceUnavailable, "admin token is not configured")
			return
		}
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") {
			writeAPIError(w, http.StatusUnauthorized, "missing bearer token")
			return
		}
		got := strings.TrimPrefix(auth, "Bearer ")
		if subtle.ConstantTimeCompare([]byte(got), []byte(expected)) != 1 {
			writeAPIError(w, http.StatusUnauthorized, "invalid bearer token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	now := s.clk.Now()
	health, err := s.mgr.SourceHealth(ctx)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, logging.ErrorString(err))
		return
	}
	latest, err := s.mgr.LatestDigest(ctx)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, logging.ErrorString(err))
		return
	}
	if latest != nil {
		copy := *latest
		copy.Markdown = ""
		copy.Items = nil
		latest = &copy
	}
	counts, err := s.mgr.OutboxCounts(ctx)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, logging.ErrorString(err))
		return
	}
	loc := s.cfg.DisplayLocation()
	t := now.In(loc)
	dayStart := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc).UTC().Unix()
	dayEnd := dayStart + 86400
	spend, err := s.mgr.DailyLLMSpend(ctx, dayStart, dayEnd)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, logging.ErrorString(err))
		return
	}
	years, _ := s.mgr.ShardYears()
	resp := map[string]interface{}{
		"time":                 t.Format(time.RFC3339),
		"schedule_enabled":     s.cfg.Schedule.Enabled,
		"notification_enabled": s.cfg.Notification.Enabled,
		"mode":                 modeString(s.cfg),
		"sources":              health,
		"latest_digest":        latest,
		"outbox":               counts,
		"llm_usage_today":      spend,
		"llm_budget": map[string]interface{}{
			"daily_max_requests":        s.cfg.LLM.DailyMaxRequests,
			"daily_max_reserved_tokens": s.cfg.LLM.DailyMaxReservedTokens,
		},
		"annual_shards": years,
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleArticles(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := parseLimit(q.Get("limit"), 50, 500)
	var from, to *int64
	if v := q.Get("from"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid from (RFC3339 required)")
			return
		}
		u := t.Unix()
		from = &u
	}
	if v := q.Get("to"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid to (RFC3339 required)")
			return
		}
		u := t.Unix()
		to = &u
	}
	f := storage.ArticleFilter{
		OnlyPublished:  true,
		Limit:          limit + 1,
		Section:        q.Get("section"),
		PublisherID:    q.Get("publisher_id"),
		PublisherGroup: q.Get("publisher_group"),
		SourceID:       q.Get("source_id"),
		WindowStart:    from,
		WindowEnd:      to,
	}
	if v := q.Get("before_published"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid before_published")
			return
		}
		f.AfterPublished = &n
		f.AfterID = q.Get("before_id")
	}
	items, err := s.mgr.QueryArticlesAcrossShards(r.Context(), f)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, logging.ErrorString(err))
		return
	}
	next := map[string]interface{}{}
	if len(items) > limit {
		items = items[:limit]
		last := items[len(items)-1]
		if last.PublishedAt != nil {
			next = map[string]interface{}{"before_published": *last.PublishedAt, "before_id": last.ID}
		}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"items": items, "next": next})
}

func (s *Server) handleDigests(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := parseLimit(q.Get("limit"), 20, 100)
	var before *int64
	if v := q.Get("before"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid before")
			return
		}
		before = &n
	}
	list, err := s.mgr.ListDigestsAcrossShards(r.Context(), limit+1, before, q.Get("before_id"))
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, logging.ErrorString(err))
		return
	}
	next := map[string]interface{}{}
	if len(list) > limit {
		last := list[limit-1]
		next = map[string]interface{}{"before": last.ScheduledAt, "before_id": last.ID}
	}
	for i := range list {
		list[i].Markdown = ""
		list[i].Items = nil
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"items": list, "next": next})
}

func (s *Server) handleDigestDetail(w http.ResponseWriter, r *http.Request) {
	year, err := strconv.Atoi(r.PathValue("year"))
	if err != nil || year < 1970 || year > 9999 {
		writeAPIError(w, http.StatusBadRequest, "invalid year")
		return
	}
	id := r.PathValue("id")
	d, items, err := s.mgr.GetDigest(r.Context(), year, id)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, logging.ErrorString(err))
		return
	}
	if d == nil {
		writeAPIError(w, http.StatusNotFound, "digest not found")
		return
	}
	if r.URL.Query().Get("format") == "markdown" || strings.Contains(r.Header.Get("Accept"), "text/markdown") {
		w.Header().Set("Content-Type", "text/markdown; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(d.Markdown))
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"digest": d, "items": items})
}

func (s *Server) handleFetchJob(w http.ResponseWriter, r *http.Request) {
	job, err := s.sched.CreateFetchJob(r.Context())
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, logging.ErrorString(err))
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]interface{}{"job_id": job.ID, "owner_year": job.OwnerYear, "status": job.Status})
}

func (s *Server) handleDigestJob(w http.ResponseWriter, r *http.Request) {
	var body struct {
		At    string `json:"at"`
		Fetch bool   `json:"fetch"`
	}
	if r.ContentLength > 0 {
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		if err := dec.Decode(&body); err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
	}
	at := s.clk.Now()
	if body.At != "" {
		t, err := time.Parse(time.RFC3339, body.At)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "at must be RFC3339")
			return
		}
		at = t
	}
	job, err := s.sched.CreateManualDigestJob(r.Context(), at, body.Fetch)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, logging.ErrorString(err))
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]interface{}{"job_id": job.ID, "owner_year": job.OwnerYear, "status": job.Status, "auto_notify": false})
}

func (s *Server) handleNotifyDigest(w http.ResponseWriter, r *http.Request) {
	if !s.cfg.Notification.Enabled {
		writeAPIError(w, http.StatusConflict, "notification.enabled is false")
		return
	}
	year, err := strconv.Atoi(r.PathValue("year"))
	if err != nil || year < 1970 || year > 9999 {
		writeAPIError(w, http.StatusBadRequest, "invalid year")
		return
	}
	id := r.PathValue("id")
	ctx := r.Context()
	d, items, err := s.mgr.GetDigest(ctx, year, id)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, logging.ErrorString(err))
		return
	}
	if d == nil {
		writeAPIError(w, http.StatusNotFound, "digest not found")
		return
	}
	shard, err := s.mgr.Open(year)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, logging.ErrorString(err))
		return
	}
	o, _, err := notify.BuildOutbox(s.cfg, *d, items, 0, "", s.clk.Now().Unix())
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, logging.ErrorString(err))
		return
	}
	releaseWrite := s.mgr.BeginWrite()
	outboxID, created, err := shard.EnqueueOutbox(ctx, o)
	releaseWrite()
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, logging.ErrorString(err))
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusAccepted
	}
	writeJSON(w, status, map[string]interface{}{"outbox_id": outboxID, "created": created, "dedup_key": o.DedupKey})
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func writeAPIError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func parseLimit(raw string, def, max int) int {
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return def
	}
	if n > max {
		return max
	}
	return n
}

func modeString(cfg *config.Config) string {
	if cfg.Schedule.Enabled {
		return "scheduled"
	}
	return "collection_only"
}
