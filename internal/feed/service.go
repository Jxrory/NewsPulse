package feed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/url"
	"sync"
	"time"

	"github.com/jxrory/newspulse/internal/clock"
	"github.com/jxrory/newspulse/internal/config"
	"github.com/jxrory/newspulse/internal/logging"
	"github.com/jxrory/newspulse/internal/storage"
)

type Service struct {
	cfg     *config.Config
	mgr     *storage.ShardManager
	fetcher *Fetcher
	clock   clock.Clock
	log     *slog.Logger
}

func NewService(cfg *config.Config, mgr *storage.ShardManager, clk clock.Clock, log *slog.Logger) *Service {
	if clk == nil {
		clk = clock.Real{}
	}
	if log == nil {
		log = slog.Default()
	}
	return &Service{cfg: cfg, mgr: mgr, fetcher: NewFetcher(cfg), clock: clk, log: log}
}

func (s *Service) FetcherClient() *Fetcher { return s.fetcher }

type Outcome struct {
	SourceID    string `json:"source_id"`
	Status      string `json:"status"`
	ErrorCode   string `json:"error_code,omitempty"`
	HTTPStatus  int    `json:"http_status,omitempty"`
	Parsed      int    `json:"parsed"`
	Inserted    int    `json:"inserted"`
	Updated     int    `json:"updated"`
	Rejected    int    `json:"rejected"`
	NextFetchAt int64  `json:"next_fetch_at"`
	DurationMS  int64  `json:"duration_ms"`
	Message     string `json:"message,omitempty"`
}

func (s *Service) FetchSource(ctx context.Context, src config.SourceConfig) (*Outcome, error) {
	started := s.clock.Now()
	wallStart := time.Now()
	release := s.mgr.BeginWrite()
	defer release()

	shard, err := s.mgr.OpenForWrite(started)
	if err != nil {
		return nil, err
	}
	state, err := shard.GetSourceState(ctx, src.ID)
	if err != nil {
		return nil, err
	}
	if state == nil {
		state = &storage.SourceState{SourceID: src.ID, NextFetchAt: started.Unix(), Health: "unknown"}
	}
	run := &storage.FetchRun{
		SourceID:  src.ID,
		StartedAt: started.Unix(),
		Status:    "running",
	}
	if err := shard.InsertFetchRun(ctx, run); err != nil {
		return nil, err
	}

	resp, ferr := s.fetcher.Fetch(ctx, src, deref(state.ETag), deref(state.LastModified))
	out := &Outcome{SourceID: src.ID}
	if ferr != nil {
		code := "fetch_error"
		var fe *FetchError
		var httpStatus int
		var retryAfter time.Duration
		if errors.As(ferr, &fe) {
			code = fe.Code
			httpStatus = fe.HTTPStatus
			retryAfter = fe.RetryAfter
		}
		finished := s.clock.Now()
		state.ConsecutiveFailures++
		state.LastCheckedAt = int64At(finished.Unix())
		if state.ConsecutiveFailures >= 3 {
			state.Health = "degraded"
		} else {
			state.Health = "warning"
		}
		state.LastErrorCode = code
		backoff := failureBackoff(state.ConsecutiveFailures)
		if retryAfter > 0 {
			backoff = retryAfter
		}
		if httpStatus == 401 || httpStatus == 403 || httpStatus == 404 {
			if backoff < time.Hour {
				backoff = time.Hour
			}
		}
		state.NextFetchAt = finished.Add(jitter(backoff, s.cfg.Fetch.JitterRatio)).Unix()
		state.UpdatedAt = finished.Unix()

		out.Status = "failed"
		out.ErrorCode = code
		out.HTTPStatus = httpStatus
		out.Message = logging.ErrorString(ferr)
		out.NextFetchAt = state.NextFetchAt
		out.DurationMS = time.Since(wallStart).Milliseconds()
		finishRun(run, s.clock.Now().Unix(), out.Status, httpStatus, code, ferr)
		if err := shard.RecordFetchFailure(ctx, run, *state); err != nil {
			return out, err
		}
		s.log.Warn("feed fetch failed", "feed_id", src.ID, "code", code, "http", httpStatus, "error", out.Message)
		return out, nil
	}

	if resp.NotModified {
		finished := s.clock.Now()
		state.LastCheckedAt = int64At(finished.Unix())
		state.LastSuccessAt = int64At(finished.Unix())
		state.ConsecutiveFailures = 0
		state.Health = "ok"
		state.LastErrorCode = ""
		if resp.ETag != "" {
			state.ETag = &resp.ETag
		}
		if resp.LastModified != "" {
			state.LastModified = &resp.LastModified
		}
		state.NextFetchAt = finished.Add(jitter(src.PollInterval.D(), s.cfg.Fetch.JitterRatio)).Unix()
		state.UpdatedAt = finished.Unix()
		run.Status = "not_modified"
		run.FinishedAt = int64At(finished.Unix())
		run.HTTPStatus = int64At(int64(resp.StatusCode))
		run.ResponseBytes = resp.Bytes
		if err := shard.RecordNotModified(ctx, run, *state); err != nil {
			return nil, err
		}
		out.Status = "not_modified"
		out.HTTPStatus = resp.StatusCode
		out.NextFetchAt = state.NextFetchAt
		out.DurationMS = time.Since(wallStart).Milliseconds()
		return out, nil
	}

	feedObj, perr := s.fetcher.Parse(resp.Body)
	if perr != nil {
		finished := s.clock.Now()
		state.ConsecutiveFailures++
		state.LastCheckedAt = int64At(finished.Unix())
		if state.ConsecutiveFailures >= 3 {
			state.Health = "degraded"
		} else {
			state.Health = "warning"
		}
		state.LastErrorCode = "parse_error"
		state.NextFetchAt = finished.Add(jitter(failureBackoff(state.ConsecutiveFailures), s.cfg.Fetch.JitterRatio)).Unix()
		state.UpdatedAt = finished.Unix()
		out.Status = "failed"
		out.ErrorCode = "parse_error"
		out.HTTPStatus = resp.StatusCode
		out.Message = logging.ErrorString(perr)
		out.NextFetchAt = state.NextFetchAt
		out.DurationMS = time.Since(wallStart).Milliseconds()
		finishRun(run, s.clock.Now().Unix(), "failed", resp.StatusCode, "parse_error", perr)
		if err := shard.RecordFetchFailure(ctx, run, *state); err != nil {
			return out, err
		}
		s.log.Warn("feed parse failed", "feed_id", src.ID, "error", out.Message)
		return out, nil
	}

	items, rejections := ParseItems(src, feedObj, started, s.cfg.Fetch.FutureTolerance.D())
	var ingest IngestResult
	func() {
		releaseIdentity := s.mgr.IdentityLock()
		defer releaseIdentity()
		ingest, err = s.IngestItems(ctx, src, items, started.Unix())
	}()
	if err != nil {
		finished := s.clock.Now()
		state.ConsecutiveFailures++
		state.LastCheckedAt = int64At(finished.Unix())
		state.Health = "degraded"
		state.LastErrorCode = "ingest_error"
		state.NextFetchAt = finished.Add(jitter(failureBackoff(state.ConsecutiveFailures), s.cfg.Fetch.JitterRatio)).Unix()
		state.UpdatedAt = finished.Unix()
		out.Status = "failed"
		out.ErrorCode = "ingest_error"
		out.Parsed = len(items)
		out.Inserted = ingest.Inserted
		out.Updated = ingest.Updated
		out.NextFetchAt = state.NextFetchAt
		out.DurationMS = time.Since(wallStart).Milliseconds()
		finishRun(run, s.clock.Now().Unix(), "failed", resp.StatusCode, "ingest_error", err)
		run.ParsedCount = len(items)
		run.InsertedCount = ingest.Inserted
		run.UpdatedCount = ingest.Updated
		if rerr := shard.RecordFetchFailure(ctx, run, *state); rerr != nil {
			return out, rerr
		}
		return out, err
	}

	rejections = append(rejections, ingest.Rejections...)
	status := "success"
	if len(rejections) > 0 {
		status = "partial"
	}
	rejJSON, _ := json.Marshal(rejections)
	finished := s.clock.Now()
	state.LastCheckedAt = int64At(finished.Unix())
	state.LastSuccessAt = int64At(finished.Unix())
	state.ConsecutiveFailures = 0
	state.Health = "ok"
	state.LastErrorCode = ""
	if resp.ETag != "" {
		state.ETag = &resp.ETag
	}
	if resp.LastModified != "" {
		state.LastModified = &resp.LastModified
	}
	if latest := latestPublished(items); latest != nil {
		state.LatestPublishedAt = latest
	}
	state.NextFetchAt = finished.Add(jitter(src.PollInterval.D(), s.cfg.Fetch.JitterRatio)).Unix()
	state.UpdatedAt = finished.Unix()

	run.FinishedAt = int64At(finished.Unix())
	run.HTTPStatus = int64At(int64(resp.StatusCode))
	run.ResponseBytes = resp.Bytes
	run.ParsedCount = len(items)
	run.InsertedCount = ingest.Inserted
	run.UpdatedCount = ingest.Updated
	run.RejectedCount = len(rejections)
	run.RejectedItemsJSON = string(rejJSON)
	run.Status = status
	if err := shard.RecordFetchSuccess(ctx, run, *state); err != nil {
		return out, err
	}

	out.Status = status
	out.HTTPStatus = resp.StatusCode
	out.Parsed = len(items)
	out.Inserted = ingest.Inserted
	out.Updated = ingest.Updated
	out.Rejected = len(rejections)
	out.NextFetchAt = state.NextFetchAt
	out.DurationMS = time.Since(wallStart).Milliseconds()
	s.log.Info("feed fetched", "feed_id", src.ID, "status", status,
		"parsed", out.Parsed, "inserted", out.Inserted, "updated", out.Updated,
		"rejected", out.Rejected, "bytes", resp.Bytes)
	return out, nil
}

func finishRun(run *storage.FetchRun, finishedUnix int64, status string, httpStatus int, code string, cause error) {
	run.Status = status
	run.FinishedAt = int64At(finishedUnix)
	if httpStatus != 0 {
		run.HTTPStatus = int64At(int64(httpStatus))
	}
	run.ErrorCode = code
	if cause != nil {
		run.ErrorMessageRedacted = logging.ErrorString(cause)
	}
}

func latestPublished(items []ParsedItem) *int64 {
	var max *int64
	for _, it := range items {
		p := it.Article.PublishedAt
		if p == nil || it.Article.DateQuality == storage.DateFormatFuture {
			continue
		}
		if max == nil || *p > *max {
			v := *p
			max = &v
		}
	}
	return max
}

func failureBackoff(consecutive int) time.Duration {
	switch {
	case consecutive <= 1:
		return time.Minute
	case consecutive == 2:
		return 5 * time.Minute
	case consecutive == 3:
		return 15 * time.Minute
	default:
		return time.Hour
	}
}

func jitter(d time.Duration, ratio float64) time.Duration {
	if d <= 0 || ratio <= 0 {
		return d
	}
	delta := float64(d) * ratio
	return d + time.Duration((rand.Float64()*2-1)*delta)
}

func int64At(v int64) *int64 { return &v }

func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// FetchDue fetches all enabled sources whose next_fetch_at has passed.
func (s *Service) FetchDue(ctx context.Context) ([]*Outcome, error) {
	now := s.clock.Now()
	shard, err := s.mgr.OpenForWrite(now)
	if err != nil {
		return nil, err
	}
	enabled := map[string]bool{}
	for _, src := range s.cfg.Sources {
		enabled[src.ID] = src.Enabled
	}
	states, err := shard.DueSources(ctx, now.Unix(), enabled)
	if err != nil {
		return nil, err
	}
	if len(states) == 0 {
		return nil, nil
	}
	maxConc := s.cfg.Fetch.MaxConcurrent
	sem := make(chan struct{}, maxConc)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var outcomes []*Outcome
	hostLocks := map[string]*sync.Mutex{}
	lockFor := func(host string) *sync.Mutex {
		mu.Lock()
		defer mu.Unlock()
		l, ok := hostLocks[host]
		if !ok {
			l = &sync.Mutex{}
			hostLocks[host] = l
		}
		return l
	}
	for _, src := range s.cfg.Sources {
		if !src.Enabled {
			continue
		}
		src := src
		due := false
		for _, st := range states {
			if st.SourceID == src.ID {
				due = true
				break
			}
		}
		if !due {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			host := hostOf(src.URL)
			hl := lockFor(host)
			sem <- struct{}{}
			defer func() { <-sem }()
			hl.Lock()
			defer hl.Unlock()
			out, err := s.FetchSource(ctx, src)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				s.log.Error("feed source failed internally", "feed_id", src.ID, "error", logging.ErrorString(err))
				outcomes = append(outcomes, &Outcome{SourceID: src.ID, Status: "failed", ErrorCode: "internal_error", Message: logging.ErrorString(err)})
				return
			}
			outcomes = append(outcomes, out)
		}()
	}
	wg.Wait()
	return outcomes, nil
}

func hostOf(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	return u.Hostname()
}

var _ = fmt.Sprintf

// FetchAll fetches every enabled source once, ignoring next_fetch_at. It is
// used by the one-shot CLI and the manual fetch job.
func (s *Service) FetchAll(ctx context.Context) ([]*Outcome, error) {
	var sources []config.SourceConfig
	for _, src := range s.cfg.Sources {
		if src.Enabled {
			sources = append(sources, src)
		}
	}
	maxConc := s.cfg.Fetch.MaxConcurrent
	sem := make(chan struct{}, maxConc)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var outcomes []*Outcome
	hostLocks := map[string]*sync.Mutex{}
	lockFor := func(host string) *sync.Mutex {
		mu.Lock()
		defer mu.Unlock()
		l, ok := hostLocks[host]
		if !ok {
			l = &sync.Mutex{}
			hostLocks[host] = l
		}
		return l
	}
	for _, src := range sources {
		src := src
		wg.Add(1)
		go func() {
			defer wg.Done()
			hl := lockFor(hostOf(src.URL))
			sem <- struct{}{}
			defer func() { <-sem }()
			hl.Lock()
			defer hl.Unlock()
			out, err := s.FetchSource(ctx, src)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				outcomes = append(outcomes, &Outcome{SourceID: src.ID, Status: "failed", ErrorCode: "internal_error", Message: logging.ErrorString(err)})
				return
			}
			outcomes = append(outcomes, out)
		}()
	}
	wg.Wait()
	return outcomes, nil
}
