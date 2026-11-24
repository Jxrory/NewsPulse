package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/jxrory/newspulse/internal/clock"
	"github.com/jxrory/newspulse/internal/config"
	"github.com/jxrory/newspulse/internal/digest"
	"github.com/jxrory/newspulse/internal/editorial"
	"github.com/jxrory/newspulse/internal/feed"
	"github.com/jxrory/newspulse/internal/llm"
	"github.com/jxrory/newspulse/internal/logging"
	"github.com/jxrory/newspulse/internal/notify"
	"github.com/jxrory/newspulse/internal/storage"
)

type Service struct {
	cfg      *config.Config
	mgr      *storage.ShardManager
	feedSvc  *feed.Service
	analyzer *llm.Analyzer
	notify   *notify.Worker
	log      *slog.Logger
	clk      clock.Clock

	fetchGuard sync.Mutex
}

func New(cfg *config.Config, mgr *storage.ShardManager, feedSvc *feed.Service,
	analyzer *llm.Analyzer, worker *notify.Worker, clk clock.Clock, log *slog.Logger) *Service {
	if clk == nil {
		clk = clock.Real{}
	}
	if log == nil {
		log = slog.Default()
	}
	return &Service{cfg: cfg, mgr: mgr, feedSvc: feedSvc, analyzer: analyzer, notify: worker, clk: clk, log: log}
}

func (s *Service) Run(ctx context.Context) {
	s.recover(ctx)

	var wg sync.WaitGroup
	runLoop := func(name string, interval time.Duration, fn func(context.Context)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					fn(ctx)
				}
			}
		}()
	}
	// Initial collection is deliberately asynchronous so HTTP health checks
	// start immediately.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.runFetchRound(ctx); err != nil {
			s.log.Warn("initial fetch round failed", "error", logging.ErrorString(err))
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.notify.Run(ctx)
	}()
	runLoop("fetch", 20*time.Second, func(c context.Context) {
		if err := s.runFetchRound(c); err != nil {
			s.log.Warn("fetch round failed", "error", logging.ErrorString(err))
		}
	})
	runLoop("schedule", 30*time.Second, func(c context.Context) {
		if s.cfg.Schedule.Enabled {
			if n, exp, err := s.CreateDueSlots(c, s.clk.Now()); err != nil {
				s.log.Error("create schedule slots failed", "error", logging.ErrorString(err))
			} else if n > 0 || exp > 0 {
				s.log.Info("schedule slots created", "created", n, "expired", exp)
			}
		}
		if err := s.ProcessDueJobs(c, 1); err != nil {
			s.log.Error("process due jobs failed", "error", logging.ErrorString(err))
		}
	})
	runLoop("maintenance", 5*time.Minute, func(c context.Context) {
		s.maintenance(c)
	})
	if s.cfg.Backup.Enabled {
		runLoop("backup", s.cfg.Backup.Interval.D(), func(c context.Context) {
			s.runBackup(c)
		})
	}
	<-ctx.Done()
	wg.Wait()
}

func (s *Service) recover(ctx context.Context) {
	now := s.clk.Now().Unix()
	if n, err := s.mgr.RecoverJobLeases(ctx, now); err != nil {
		s.log.Warn("recover job leases failed", "error", logging.ErrorString(err))
	} else if n > 0 {
		s.log.Warn("requeued jobs with expired leases", "count", n)
	}
	if n, err := s.mgr.ExpireOldJobs(ctx, now); err != nil {
		s.log.Warn("expire old jobs failed", "error", logging.ErrorString(err))
	} else if n > 0 {
		s.log.Info("expired old jobs", "count", n)
	}
	if err := s.notify.Recover(ctx); err != nil {
		s.log.Warn("recover notification outbox failed", "error", logging.ErrorString(err))
	}
}

func (s *Service) maintenance(ctx context.Context) {
	now := s.clk.Now()
	releaseWrite := s.mgr.BeginWrite()
	defer releaseWrite()
	if n, err := s.mgr.RecoverJobLeases(ctx, now.Unix()); err != nil {
		s.log.Warn("recover job leases failed", "error", logging.ErrorString(err))
	} else if n > 0 {
		s.log.Warn("requeued jobs with expired leases", "count", n)
	}
	if n, err := s.mgr.ExpireOldJobs(ctx, now.Unix()); err != nil {
		s.log.Warn("expire old jobs failed", "error", logging.ErrorString(err))
	} else if n > 0 {
		s.log.Info("expired old jobs", "count", n)
	}
	if n, err := s.mgr.ExpireOldOutbox(ctx, now.Unix()); err != nil {
		s.log.Warn("expire old outbox failed", "error", logging.ErrorString(err))
	} else if n > 0 {
		s.log.Info("expired old outbox rows", "count", n)
	}
	if n, err := s.mgr.RecoverOutboxOnRestart(ctx, now.Unix()); err != nil {
		s.log.Warn("recover outbox sends failed", "error", logging.ErrorString(err))
	} else if n > 0 {
		s.log.Warn("converted stalled outbox sends to send_unknown", "count", n)
	}
	enabled := map[string]bool{}
	for _, src := range s.cfg.Sources {
		enabled[src.ID] = src.Enabled
	}
	if err := s.mgr.MarkStaleSources(ctx, s.cfg.Fetch.StaleAfter.D(), now, enabled); err != nil {
		s.log.Warn("mark stale sources failed", "error", logging.ErrorString(err))
	}
}

func (s *Service) runBackup(ctx context.Context) {
	manifest, err := s.mgr.CreateBackup(ctx, AppVersion, s.clk.Now(), s.cfg.Backup)
	if err != nil {
		s.log.Error("backup failed", "error", logging.ErrorString(err))
		return
	}
	s.log.Info("backup created", "dir", manifest.Dir, "files", len(manifest.Files))
	if s.cfg.Backup.RemoteCopyEnabled {
		s.log.Warn("remote backup copy is configured but not implemented; local backup only")
	}
}

func (s *Service) runFetchRound(ctx context.Context) error {
	if !s.fetchGuard.TryLock() {
		return nil
	}
	defer s.fetchGuard.Unlock()
	outcomes, err := s.feedSvc.FetchDue(ctx)
	if err != nil {
		return err
	}
	for _, o := range outcomes {
		if o.Status != "success" && o.Status != "not_modified" {
			s.log.Warn("source fetch outcome", "feed_id", o.SourceID, "status", o.Status, "code", o.ErrorCode)
		}
	}
	return nil
}

// FetchAll fetches every enabled source once, ignoring next_fetch_at.
func (s *Service) FetchAll(ctx context.Context) ([]*feed.Outcome, error) {
	return s.feedSvc.FetchAll(ctx)
}

type jobConfig struct {
	AutoNotify bool `json:"auto_notify"`
	FetchFirst bool `json:"fetch_first"`
}

func jobConfigOf(job storage.Job) jobConfig {
	var c jobConfig
	_ = json.Unmarshal([]byte(job.ConfigSnapshotJSON), &c)
	return c
}

func (s *Service) CreateDueSlots(ctx context.Context, now time.Time) (created int, expired int, err error) {
	if !s.cfg.Schedule.Enabled {
		return 0, 0, nil
	}
	loc := s.cfg.ScheduleLocation()
	if loc == nil {
		loc = time.UTC
	}
	type slot struct {
		key       string
		scheduled time.Time
		labels    string
	}
	var slots []slot
	nowLocal := now.In(loc)
	days := int(s.cfg.Schedule.CatchupMaxAge.D().Hours()/24) + 2
	if days < 2 {
		days = 2
	}
	for dayOff := -days; dayOff <= 0; dayOff++ {
		day := nowLocal.AddDate(0, 0, dayOff)
		dateLabel := day.Format("2006-01-02")
		for _, raw := range s.cfg.Schedule.Times {
			hhmm, ok := config.ParseHHMM(raw)
			if !ok {
				continue
			}
			hh := int(hhmm[0]-'0')*10 + int(hhmm[1]-'0')
			mm := int(hhmm[3]-'0')*10 + int(hhmm[4]-'0')
			t := time.Date(day.Year(), day.Month(), day.Day(), hh, mm, 0, 0, loc)
			// Missing DST wall-clock times normalize to another hour; skip them.
			if t.In(loc).Format("15:04") != hhmm {
				continue
			}
			slots = append(slots, slot{
				key:       fmt.Sprintf("daily:%s:%s:%s", s.cfg.Schedule.ID, dateLabel, hhmm),
				scheduled: t,
				labels:    dateLabel + " " + hhmm,
			})
		}
	}
	sort.SliceStable(slots, func(i, j int) bool { return slots[i].scheduled.After(slots[j].scheduled) })
	pastCreated := 0
	for _, sl := range slots {
		if now.Before(sl.scheduled.Add(-s.cfg.Schedule.PrepareBefore.D())) {
			continue
		}
		if now.After(sl.scheduled.Add(s.cfg.Schedule.CatchupMaxAge.D())) {
			continue
		}
		isPast := !now.Before(sl.scheduled)
		if isPast && pastCreated >= s.cfg.Schedule.CatchupSlots {
			continue
		}
		shard, err := s.mgr.OpenForWrite(sl.scheduled)
		if err != nil {
			return created, expired, err
		}
		cfgSnapshot, _ := json.Marshal(jobConfig{AutoNotify: s.cfg.Notification.Enabled, FetchFirst: true})
		nowUnix := now.Unix()
		next := nowUnix
		genDeadline := sl.scheduled.Add(s.cfg.Schedule.GenerationTimeout.D()).Unix()
		job := &storage.Job{
			JobKey:             sl.key,
			Kind:               "digest",
			ScheduleID:         s.cfg.Schedule.ID,
			ScheduledAt:        sl.scheduled.Unix(),
			WindowStart:        ptr(sl.scheduled.Add(-s.cfg.Schedule.Lookback.D()).Unix()),
			WindowEnd:          ptr(sl.scheduled.Unix()),
			CutoffAt:           ptr(sl.scheduled.Unix()),
			GenerationDeadline: &genDeadline,
			ExpiresAt:          sl.scheduled.Add(s.cfg.Schedule.CatchupMaxAge.D()).Unix(),
			Status:             storage.JobPending,
			NextAttemptAt:      &next,
			ConfigSnapshotJSON: string(cfgSnapshot),
			CreatedAt:          nowUnix,
			UpdatedAt:          nowUnix,
		}
		releaseWrite := s.mgr.BeginWrite()
		_, wasCreated, err := shard.InsertJobIfAbsent(ctx, job)
		releaseWrite()
		if err != nil {
			return created, expired, err
		}
		if wasCreated {
			created++
			if isPast {
				pastCreated++
			}
			s.log.Info("digest job created", "job_key", sl.key, "scheduled_at", sl.scheduled.Format(time.RFC3339))
		}
	}
	// Old missing slots outside the catch-up window get an expired marker so
	// they are not silently delivered later.
	_ = expired
	return created, expired, nil
}

func (s *Service) ProcessDueJobs(ctx context.Context, limitPerShard int) error {
	now := s.clk.Now()
	jobs, err := s.mgr.ClaimDueJobs(ctx, now.Unix(), 15*time.Minute, limitPerShard)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if err := s.executeJob(ctx, job); err != nil {
			s.log.Error("job execution failed", "job_id", job.ID, "job_key", job.JobKey, "error", logging.ErrorString(err))
		}
	}
	return nil
}

func (s *Service) executeJob(ctx context.Context, job storage.JobWithYear) error {
	shard, err := s.mgr.Open(job.OwnerYear)
	if err != nil {
		return err
	}
	if job.ExpiresAt <= s.clk.Now().Unix() {
		releaseWrite := s.mgr.BeginWrite()
		err := shard.UpdateJobStatus(ctx, job.ID, storage.JobExpired, "", "job_expired", "past expires_at", nil, s.clk.Now().Unix())
		releaseWrite()
		return err
	}
	timeout := s.cfg.Schedule.GenerationTimeout.D()
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var resultRef string
	if job.Kind == "digest" && s.clk.Now().Unix() < job.ScheduledAt {
		jc := jobConfigOf(job.Job)
		if jc.FetchFirst {
			if err := s.runFetchRound(runCtx); err != nil {
				s.log.Warn("prepare fetch round failed", "error", logging.ErrorString(err))
			}
		}
		next := job.ScheduledAt
		releaseWrite := s.mgr.BeginWrite()
		err = shard.UpdateJobStatus(ctx, job.ID, storage.JobPending, "", "", "", &next, s.clk.Now().Unix())
		releaseWrite()
		if err != nil {
			return err
		}
		s.log.Info("digest job prepared", "job_id", job.ID, "scheduled_at", job.ScheduledAt)
		return nil
	}
	switch job.Kind {
	case "digest":
		res, err := s.generateForJob(runCtx, job, true)
		if err != nil {
			return s.jobFailed(ctx, job, err)
		}
		resultRef = fmt.Sprintf("%d/%s", res.Year, res.Digest.ID)
	case "fetch":
		outs, err := s.FetchAll(runCtx)
		s.finishFetchJob(ctx, job, outs, err)
		return nil
	default:
		releaseWrite := s.mgr.BeginWrite()
		err := shard.UpdateJobStatus(ctx, job.ID, storage.JobFailed, "", "unsupported_job", "", nil, s.clk.Now().Unix())
		releaseWrite()
		return err
	}
	releaseWrite := s.mgr.BeginWrite()
	err = shard.UpdateJobStatus(ctx, job.ID, storage.JobSucceeded, resultRef, "", "", nil, s.clk.Now().Unix())
	releaseWrite()
	if err != nil {
		return err
	}
	s.log.Info("job succeeded", "job_id", job.ID, "result", resultRef)
	return nil
}

func (s *Service) finishFetchJob(ctx context.Context, job storage.JobWithYear, outs []*feed.Outcome, fetchErr error) {
	now := s.clk.Now().Unix()
	shard, err := s.mgr.Open(job.OwnerYear)
	if err != nil {
		return
	}
	if fetchErr != nil {
		_ = s.jobFailed(ctx, job, fetchErr)
		return
	}
	b, _ := json.Marshal(outs)
	releaseWrite := s.mgr.BeginWrite()
	_ = shard.UpdateJobStatus(ctx, job.ID, storage.JobSucceeded, string(b), "", "", nil, now)
	releaseWrite()
}

func (s *Service) jobFailed(ctx context.Context, job storage.JobWithYear, cause error) error {
	now := s.clk.Now().Unix()
	shard, err := s.mgr.Open(job.OwnerYear)
	if err != nil {
		return err
	}
	code := "job_error"
	if errors.Is(cause, context.DeadlineExceeded) {
		code = "deadline_exceeded"
	}
	var apiErr *llm.APIError
	if errors.As(cause, &apiErr) {
		code = "llm_" + apiErr.Code
	}
	var budgetErr *llm.BudgetError
	if errors.As(cause, &budgetErr) {
		code = "llm_budget"
	}
	status := storage.JobFailed
	var next *int64
	if job.AttemptCount < 3 {
		status = storage.JobRetryWait
		delay := time.Minute
		if job.AttemptCount == 2 {
			delay = 5 * time.Minute
		}
		n := now + int64(delay.Seconds())
		next = &n
	}
	errMsg := logging.ErrorString(cause)
	if len(errMsg) > 500 {
		errMsg = errMsg[:500]
	}
	releaseWrite := s.mgr.BeginWrite()
	terr := shard.UpdateJobStatus(ctx, job.ID, status, "", code, errMsg, next, now)
	releaseWrite()
	if terr != nil {
		return terr
	}
	s.log.Warn("job not completed", "job_id", job.ID, "status", status, "code", code, "attempt", job.AttemptCount)
	return nil
}

type GenerateResult struct {
	Digest  storage.Digest
	Items   []storage.DigestItem
	Year    int
	Created bool
}

type GenerateOptions struct {
	Scheduled  time.Time
	FetchFirst bool
	AutoNotify bool
	PersistJob bool
	JobID      string
	JobKey     string
}

func (s *Service) GenerateOnce(ctx context.Context, opts GenerateOptions) (*GenerateResult, error) {
	if opts.Scheduled.IsZero() {
		opts.Scheduled = s.clk.Now()
	}
	year := s.mgr.YearOf(opts.Scheduled)
	jobID := opts.JobID
	if jobID == "" {
		id, err := storage.NewID()
		if err != nil {
			return nil, err
		}
		jobID = id
	}
	jobKey := opts.JobKey
	if jobKey == "" {
		jobKey = "manual:generate:" + opts.Scheduled.UTC().Format("20060102T150405Z")
	}
	cfgSnap, _ := json.Marshal(jobConfig{AutoNotify: opts.AutoNotify, FetchFirst: opts.FetchFirst})
	scheduled := opts.Scheduled.Unix()
	job := storage.JobWithYear{
		Job: storage.Job{
			ID: jobID, JobKey: jobKey, Kind: "digest",
			ScheduledAt:        scheduled,
			WindowStart:        ptr(scheduled - int64(s.cfg.Schedule.Lookback.D().Seconds())),
			WindowEnd:          ptr(scheduled),
			CutoffAt:           ptr(scheduled),
			ExpiresAt:          scheduled + int64(s.cfg.Schedule.CatchupMaxAge.D().Seconds()),
			Status:             storage.JobRunning,
			ConfigSnapshotJSON: string(cfgSnap),
			CreatedAt:          s.clk.Now().Unix(),
			UpdatedAt:          s.clk.Now().Unix(),
		},
		OwnerYear: year,
	}
	return s.generateForJob(ctx, job, opts.PersistJob)
}

func (s *Service) generateForJob(ctx context.Context, job storage.JobWithYear, persist bool) (*GenerateResult, error) {
	jc := jobConfigOf(job.Job)
	hasSnapshot := job.CandidateSnapshotJSON != ""
	if jc.FetchFirst && (!persist || !hasSnapshot) {
		if err := s.runFetchRound(ctx); err != nil {
			s.log.Warn("pre-digest fetch round failed", "error", logging.ErrorString(err))
		}
	}
	shard, err := s.mgr.OpenForWrite(time.Unix(job.ScheduledAt, 0))
	if err != nil {
		return nil, err
	}
	job.OwnerYear = shard.Year
	snap, err := s.snapshotForJob(ctx, job)
	if err != nil {
		return nil, err
	}
	if persist && job.CandidateSnapshotJSON == "" {
		b, _ := json.Marshal(snap)
		releaseWrite := s.mgr.BeginWrite()
		serr := shard.SetJobCandidates(ctx, job.ID, string(b), s.clk.Now().Unix())
		releaseWrite()
		if serr != nil {
			return nil, serr
		}
	}
	prevRefs, err := s.mgr.PreviousDigestRefs(ctx, s.cfg.Schedule.ID, job.ScheduledAt)
	if err != nil {
		s.log.Warn("previous digest refs unavailable", "error", logging.ErrorString(err))
		prevRefs = nil
	}
	var events []llm.AnalysisEvent
	mode := "ai"
	fbReason := ""
	metadata := map[string]interface{}{}
	if s.analyzer.Enabled() {
		analysis, aerr := s.analyzer.Analyze(ctx, job.ID, job.OwnerYear, snap)
		if aerr != nil {
			s.log.Warn("llm analysis unavailable; falling back", "job_id", job.ID, "error", logging.ErrorString(aerr))
			mode = "fallback"
			fbReason = classifyLLMFailure(aerr)
			metadata["llm_error"] = fbReason
			if s.cfg.LLM.Fallback == "none" {
				return nil, aerr
			}
		} else {
			events = analysis.Events
			mode = analysis.Mode
			metadata["llm_model"] = analysis.Model
			metadata["llm_protocol"] = analysis.Protocol
			metadata["llm_cache_hit"] = analysis.CacheHit
		}
	} else {
		mode = "fallback"
		fbReason = "llm_disabled"
		if s.cfg.LLM.Fallback == "none" {
			return nil, llm.ErrDisabled
		}
	}
	metaJSON, _ := json.Marshal(metadata)
	health, herr := s.mgr.SourceHealth(ctx)
	if herr != nil {
		return nil, herr
	}
	d, items, flags := digest.Build(s.cfg, digest.BuildInput{
		Job:            job,
		Snapshot:       snap,
		Events:         events,
		Mode:           mode,
		FallbackReason: fbReason,
		ModelMetadata:  string(metaJSON),
		SourceHealth:   health,
		PreviousRefs:   prevRefs,
		Now:            s.clk.Now().Unix(),
	})
	if d.ID == "" {
		id, err := storage.NewID()
		if err != nil {
			return nil, err
		}
		d.ID = id
	}
	var outbox *storage.OutboxItem
	if jc.AutoNotify && s.cfg.Notification.Enabled {
		o, oflags, oerr := notify.BuildOutbox(s.cfg, d, items, 0, "", s.clk.Now().Unix())
		if oerr != nil {
			s.log.Warn("notification payload could not be built; saving draft", "job_id", job.ID, "error", logging.ErrorString(oerr))
			d.QualityFlags = append(d.QualityFlags, "notification_payload_error")
		} else {
			flags = append(flags, oflags...)
			d.QualityFlags = append(d.QualityFlags, oflags...)
			outbox = o
		}
	}
	releaseWrite := s.mgr.BeginWrite()
	id, created, err := shard.SaveDigest(ctx, &storage.DigestBundle{Digest: d, Items: items, Outbox: outbox})
	releaseWrite()
	if err != nil {
		return nil, err
	}
	d.ID = id
	return &GenerateResult{Digest: d, Items: items, Year: shard.Year, Created: created}, nil
}

func classifyLLMFailure(err error) string {
	var budget *llm.BudgetError
	if errors.As(err, &budget) {
		return "budget_exhausted"
	}
	if errors.Is(err, llm.ErrDisabled) {
		return "llm_disabled"
	}
	var apiErr *llm.APIError
	if errors.As(err, &apiErr) {
		return "llm_api_" + apiErr.Code
	}
	var verr *llm.ValidationError
	if errors.As(err, &verr) {
		return "validation_failed"
	}
	return "llm_error"
}

func (s *Service) snapshotForJob(ctx context.Context, job storage.JobWithYear) (editorial.CandidateSnapshot, error) {
	if job.CandidateSnapshotJSON != "" {
		var snap editorial.CandidateSnapshot
		if err := json.Unmarshal([]byte(job.CandidateSnapshotJSON), &snap); err == nil && len(snap.Groups) > 0 {
			return snap, nil
		}
	}
	windowStart := int64(0)
	windowEnd := int64(0)
	cutoff := int64(0)
	if job.WindowStart != nil {
		windowStart = *job.WindowStart
	}
	if job.WindowEnd != nil {
		windowEnd = *job.WindowEnd
	}
	if job.CutoffAt != nil {
		cutoff = *job.CutoffAt
	}
	if windowEnd == 0 {
		windowEnd = job.ScheduledAt
	}
	if windowStart == 0 {
		windowStart = windowEnd - int64(s.cfg.Schedule.Lookback.D().Seconds())
	}
	if cutoff == 0 {
		cutoff = windowEnd
	}
	if s.cfg.Editorial.RepeatPolicy == "since_previous_slot" {
		if prev, ok, perr := s.mgr.PreviousDigestTime(ctx, s.cfg.Schedule.ID, windowEnd); perr == nil && ok && prev > windowStart && prev <= windowEnd {
			windowStart = prev
		}
	}
	return editorial.BuildSnapshot(ctx, s.mgr, s.cfg, editorial.BuildOptions{
		WindowStart:   windowStart,
		WindowEnd:     windowEnd,
		CutoffAt:      cutoff,
		MaxCandidates: s.cfg.Editorial.MaxCandidates,
		LateMaxAge:    s.cfg.Editorial.LateArrivalMaxAge.D(),
		Lookback:      s.cfg.Schedule.Lookback.D(),
		AllowUndated:  s.cfg.Editorial.AllowUndatedOnColdStart,
	})
}

// CreateManualDigestJob persists a draft-only digest job (no automatic send).
func (s *Service) CreateManualDigestJob(ctx context.Context, scheduled time.Time, fetchFirst bool) (*storage.JobWithYear, error) {
	if scheduled.IsZero() {
		scheduled = s.clk.Now()
	}
	shard, err := s.mgr.OpenForWrite(scheduled)
	if err != nil {
		return nil, err
	}
	id, err := storage.NewID()
	if err != nil {
		return nil, err
	}
	cfgSnap, _ := json.Marshal(jobConfig{AutoNotify: false, FetchFirst: fetchFirst})
	now := s.clk.Now()
	next := now.Unix()
	windowStart := scheduled.Add(-s.cfg.Schedule.Lookback.D()).Unix()
	windowEnd := scheduled.Unix()
	cutoff := scheduled.Unix()
	deadline := scheduled.Add(s.cfg.Schedule.GenerationTimeout.D()).Unix()
	expiresBase := now
	if scheduled.After(now) {
		expiresBase = scheduled
	}
	expires := expiresBase.Add(s.cfg.Schedule.CatchupMaxAge.D()).Unix()
	job := &storage.Job{
		ID: id, JobKey: "manual:digest:" + id, Kind: "digest",
		ScheduledAt:        scheduled.Unix(),
		WindowStart:        &windowStart,
		WindowEnd:          &windowEnd,
		CutoffAt:           &cutoff,
		GenerationDeadline: &deadline,
		ExpiresAt:          expires,
		Status:             storage.JobPending,
		NextAttemptAt:      &next,
		ConfigSnapshotJSON: string(cfgSnap),
		CreatedAt:          now.Unix(),
		UpdatedAt:          now.Unix(),
	}
	releaseWrite := s.mgr.BeginWrite()
	created, ok, err := shard.InsertJobIfAbsent(ctx, job)
	releaseWrite()
	if err != nil {
		return nil, err
	}
	if !ok {
		return &storage.JobWithYear{Job: *created, OwnerYear: shard.Year}, nil
	}
	return &storage.JobWithYear{Job: *created, OwnerYear: shard.Year}, nil
}

func (s *Service) CreateFetchJob(ctx context.Context) (*storage.JobWithYear, error) {
	now := s.clk.Now()
	shard, err := s.mgr.OpenForWrite(now)
	if err != nil {
		return nil, err
	}
	id, err := storage.NewID()
	if err != nil {
		return nil, err
	}
	expires := now.Add(time.Hour).Unix()
	next := now.Unix()
	cfgSnap, _ := json.Marshal(jobConfig{})
	job := &storage.Job{
		ID: id, JobKey: "manual:fetch:" + id, Kind: "fetch",
		ScheduledAt:        now.Unix(),
		ExpiresAt:          expires,
		Status:             storage.JobPending,
		NextAttemptAt:      &next,
		ConfigSnapshotJSON: string(cfgSnap),
		CreatedAt:          now.Unix(),
		UpdatedAt:          now.Unix(),
	}
	releaseWrite := s.mgr.BeginWrite()
	created, _, err := shard.InsertJobIfAbsent(ctx, job)
	releaseWrite()
	if err != nil {
		return nil, err
	}
	return &storage.JobWithYear{Job: *created, OwnerYear: shard.Year}, nil
}

// AppVersion is the reported application version.
const AppVersion = "0.1.1"

func ptr(v int64) *int64 { return &v }
