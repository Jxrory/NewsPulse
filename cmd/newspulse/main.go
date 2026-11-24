package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"
	_ "time/tzdata"

	"github.com/jxrory/newspulse/internal/clock"
	"github.com/jxrory/newspulse/internal/config"
	"github.com/jxrory/newspulse/internal/feed"
	"github.com/jxrory/newspulse/internal/httpapi"
	"github.com/jxrory/newspulse/internal/llm"
	"github.com/jxrory/newspulse/internal/lock"
	"github.com/jxrory/newspulse/internal/logging"
	"github.com/jxrory/newspulse/internal/notify"
	"github.com/jxrory/newspulse/internal/scheduler"
	"github.com/jxrory/newspulse/internal/storage"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]
	var err error
	switch cmd {
	case "serve":
		err = cmdServe(args)
	case "config":
		err = cmdConfig(args)
	case "sources":
		err = cmdSources(args)
	case "fetch":
		err = cmdFetch(args)
	case "digest":
		err = cmdDigest(args)
	case "notify":
		err = cmdNotify(args)
	case "db":
		err = cmdDB(args)
	case "backup":
		err = cmdBackup(args)
	case "healthcheck":
		err = cmdHealthcheck(args)
	case "version":
		fmt.Printf("NewsPulse %s (%s %s/%s)\n", scheduler.AppVersion, runtime.Version(), runtime.GOOS, runtime.GOARCH)
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", cmd)
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", logging.ErrorString(err))
		os.Exit(1)
	}
}

func usage() {
	fmt.Print(`NewsPulse - personal news digest service

Usage:
  newspulse serve --config /etc/newspulse/config.yaml
  newspulse config validate --config ...
  newspulse sources check --config ... [--all] [--json] [--links 2]
  newspulse fetch --once --config ...
  newspulse digest generate --config ... [--at RFC3339] [--dry-run]
  newspulse digest show --config ... [--format markdown|json] <year>/<id>
  newspulse notify enqueue --config ... --digest <year>/<id>
  newspulse notify status --config ... --digest <year>/<id>
  newspulse notify resend --config ... --digest <year>/<id> --reason <text>
  newspulse db migrate --config ... [--all]
  newspulse db verify --config ... [--all]
  newspulse backup create --config ...
  newspulse healthcheck [--url http://127.0.0.1:8080/healthz] [--timeout 3s]
  newspulse version

The example config disables scheduled pushes. Real Feishu/LLM credentials are
read only from environment variables; never pass secrets on the command line.
`)
}

func addCommon(fs *flag.FlagSet) *string {
	return fs.String("config", "", "path to config YAML (required)")
}

func loadCfg(path string) (*config.Config, error) {
	if path == "" {
		return nil, errors.New("--config is required")
	}
	return config.Load(path)
}

func acquireLock(cfg *config.Config) (*lock.DirLock, error) {
	return lock.Acquire(cfg.App.DataDir)
}

func newManager(cfg *config.Config) (*storage.ShardManager, error) {
	return storage.NewShardManager(cfg)
}

func cmdConfig(args []string) error {
	if len(args) == 0 {
		return errors.New("config subcommand required: validate")
	}
	switch args[0] {
	case "validate":
		fs := flag.NewFlagSet("config validate", flag.ContinueOnError)
		path := addCommon(fs)
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		cfg, err := loadCfg(*path)
		if err != nil {
			return err
		}
		fmt.Println("config OK")
		fmt.Printf("  data_dir: %s\n", cfg.App.DataDir)
		fmt.Printf("  annual_timezone: %s\n", cfg.App.AnnualTimezone)
		fmt.Printf("  enabled_sources: %d/%d\n", len(cfg.EnabledSources()), len(cfg.Sources))
		if !cfg.Schedule.Enabled {
			fmt.Println("  schedule: disabled (collection-only until a time is confirmed)")
		} else {
			fmt.Printf("  schedule: %s at %s (%s)\n", cfg.Schedule.ID, strings.Join(cfg.Schedule.Times, ", "), cfg.Schedule.Timezone)
		}
		if !cfg.Notification.Enabled {
			fmt.Println("  notification: disabled (digests are drafts only)")
		}
		return nil
	default:
		return fmt.Errorf("unknown config subcommand %q", args[0])
	}
}

func cmdSources(args []string) error {
	if len(args) == 0 || args[0] != "check" {
		return errors.New("usage: newspulse sources check --config ...")
	}
	fs := flag.NewFlagSet("sources check", flag.ContinueOnError)
	path := addCommon(fs)
	all := fs.Bool("all", false, "check disabled sources too")
	asJSON := fs.Bool("json", false, "output JSON")
	links := fs.Int("links", 2, "number of article links to probe per feed")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	cfg, err := loadCfg(*path)
	if err != nil {
		return err
	}
	clk := clock.Real{}
	svc := feed.NewService(cfg, nil, clk, slog.Default())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	var results []feed.CheckResult
	for _, src := range cfg.Sources {
		if !src.Enabled && !*all {
			continue
		}
		res := svc.CheckSource(ctx, src, *links)
		results = append(results, res)
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(results); err != nil {
			return err
		}
	} else {
		tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
		fmt.Fprintln(tw, "ID\tHTTP\tITEMS\tDATES\tRECENT24H\tFUTURE\tLATEST\tLINK PROBES\tERROR")
		bad := 0
		for _, r := range results {
			status := ""
			if r.Status != 0 {
				status = strconv.Itoa(r.Status)
			}
			dateRate := fmt.Sprintf("%d/%d", r.ParsedDates, r.Items)
			var probes []string
			for _, l := range r.SampleLinks {
				if l.Error != "" {
					probes = append(probes, "ERR")
				} else {
					probes = append(probes, strconv.Itoa(l.Status))
				}
			}
			if r.Error != "" {
				bad++
			}
			fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%d\t%d\t%s\t%s\t%s\n",
				r.ID, status, r.Items, dateRate, r.Recent24h, r.FutureDates,
				r.Latest, strings.Join(probes, ","), r.Error)
		}
		tw.Flush()
		if bad > 0 {
			return fmt.Errorf("%d source checks failed", bad)
		}
	}
	return nil
}

func cmdFetch(args []string) error {
	fs := flag.NewFlagSet("fetch", flag.ContinueOnError)
	path := addCommon(fs)
	once := fs.Bool("once", false, "fetch all enabled sources once")
	if err := fs.Parse(args); err != nil {
		return err
	}
	_ = once
	cfg, err := loadCfg(*path)
	if err != nil {
		return err
	}
	lk, err := acquireLock(cfg)
	if errors.Is(err, lock.ErrWriterHeld) {
		return submitFetchViaAPI(cfg)
	}
	if err != nil {
		return err
	}
	defer lk.Release()
	mgr, err := newManager(cfg)
	if err != nil {
		return err
	}
	defer mgr.Close()
	log := logging.New(cfg.App.LogLevel)
	svc := feed.NewService(cfg, mgr, clock.Real{}, log)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	outs, err := svc.FetchAll(ctx)
	if err != nil {
		return err
	}
	b, _ := json.MarshalIndent(outs, "", "  ")
	fmt.Println(string(b))
	return nil
}

func cmdDigest(args []string) error {
	if len(args) == 0 {
		return errors.New("digest subcommand required: generate|show")
	}
	switch args[0] {
	case "generate":
		fs := flag.NewFlagSet("digest generate", flag.ContinueOnError)
		path := addCommon(fs)
		at := fs.String("at", "", "scheduled time (RFC3339); default now")
		dryRun := fs.Bool("dry-run", true, "never enqueue notifications; tool always saves a draft")
		asJSON := fs.Bool("json", false, "print JSON instead of Markdown")
		noFetch := fs.Bool("no-fetch", false, "do not run a fetch round before generating")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		_ = dryRun
		cfg, err := loadCfg(*path)
		if err != nil {
			return err
		}
		lk, err := acquireLock(cfg)
		if errors.Is(err, lock.ErrWriterHeld) {
			return submitDigestViaAPI(cfg, *at, !*noFetch)
		}
		if err != nil {
			return err
		}
		defer lk.Release()
		mgr, err := newManager(cfg)
		if err != nil {
			return err
		}
		defer mgr.Close()
		log := logging.New(cfg.App.LogLevel)
		scheduled := time.Now()
		if *at != "" {
			t, err := time.Parse(time.RFC3339, *at)
			if err != nil {
				return errors.New("--at must be RFC3339")
			}
			scheduled = t
		}
		feedSvc := feed.NewService(cfg, mgr, clock.Real{}, log)
		analyzer := llm.NewAnalyzer(cfg, mgr, clock.Real{}, log)
		worker := notify.NewWorker(cfg, mgr, clock.Real{}, log)
		sched := scheduler.New(cfg, mgr, feedSvc, analyzer, worker, clock.Real{}, log)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
		defer cancel()
		res, err := sched.GenerateOnce(ctx, scheduler.GenerateOptions{
			Scheduled: scheduled, FetchFirst: !*noFetch, AutoNotify: false,
		})
		if err != nil {
			return err
		}
		if *asJSON {
			b, _ := json.MarshalIndent(map[string]interface{}{
				"year": res.Year, "digest_id": res.Digest.ID, "created": res.Created,
				"digest": res.Digest, "items": res.Items,
			}, "", "  ")
			fmt.Println(string(b))
			return nil
		}
		fmt.Println(res.Digest.Markdown)
		return nil
	case "show":
		fs := flag.NewFlagSet("digest show", flag.ContinueOnError)
		path := addCommon(fs)
		format := fs.String("format", "markdown", "markdown|json")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		ref := fs.Arg(0)
		year, id, err := parseDigestRef(ref)
		if err != nil {
			return err
		}
		cfg, err := loadCfg(*path)
		if err != nil {
			return err
		}
		mgr, err := newManager(cfg)
		if err != nil {
			return err
		}
		defer mgr.Close()
		d, items, err := mgr.GetDigest(context.Background(), year, id)
		if err != nil {
			return err
		}
		if d == nil {
			return fmt.Errorf("digest %d/%s not found", year, id)
		}
		if *format == "json" {
			b, _ := json.MarshalIndent(map[string]interface{}{"digest": d, "items": items}, "", "  ")
			fmt.Println(string(b))
			return nil
		}
		fmt.Println(d.Markdown)
		return nil
	default:
		return fmt.Errorf("unknown digest subcommand %q", args[0])
	}
}

func parseDigestRef(ref string) (int, string, error) {
	parts := strings.SplitN(ref, "/", 2)
	if len(parts) != 2 {
		return 0, "", errors.New("digest reference must be <year>/<id>")
	}
	year, err := strconv.Atoi(parts[0])
	if err != nil || year < 1970 || year > 9999 {
		return 0, "", errors.New("invalid digest year")
	}
	if parts[1] == "" {
		return 0, "", errors.New("digest id is required")
	}
	return year, parts[1], nil
}

func cmdNotify(args []string) error {
	if len(args) == 0 {
		return errors.New("notify subcommand required: enqueue|status|resend")
	}
	sub := args[0]
	fs := flag.NewFlagSet("notify "+sub, flag.ContinueOnError)
	path := addCommon(fs)
	digestRef := fs.String("digest", "", "digest reference <year>/<id>")
	reason := fs.String("reason", "", "reason for explicit resend (required for resend)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *digestRef == "" {
		*digestRef = fs.Arg(0)
	}
	cfg, err := loadCfg(*path)
	if err != nil {
		return err
	}
	year, id, err := parseDigestRef(*digestRef)
	if err != nil {
		return err
	}
	switch sub {
	case "status":
		mgr, err := newManager(cfg)
		if err != nil {
			return err
		}
		defer mgr.Close()
		shard, err := mgr.Open(year)
		if err != nil {
			return err
		}
		items, err := shard.OutboxForDigest(context.Background(), id)
		if err != nil {
			return err
		}
		if items == nil {
			items = []storage.OutboxItem{}
		}
		b, _ := json.MarshalIndent(items, "", "  ")
		fmt.Println(string(b))
		return nil
	case "enqueue", "resend":
		if !cfg.Notification.Enabled {
			return errors.New("notification.enabled is false; enable it in config before enqueueing")
		}
		lk, err := acquireLock(cfg)
		if errors.Is(err, lock.ErrWriterHeld) {
			if sub == "enqueue" {
				return submitNotifyViaAPI(cfg, year, id)
			}
			return errors.New("notify resend must be run while the daemon is stopped (no API endpoint for explicit resend)")
		}
		if err != nil {
			return err
		}
		defer lk.Release()
		mgr, err := newManager(cfg)
		if err != nil {
			return err
		}
		defer mgr.Close()
		d, items, err := mgr.GetDigest(context.Background(), year, id)
		if err != nil {
			return err
		}
		if d == nil {
			return fmt.Errorf("digest %d/%s not found", year, id)
		}
		shard, err := mgr.Open(year)
		if err != nil {
			return err
		}
		rev := 0
		if sub == "resend" {
			if strings.TrimSpace(*reason) == "" {
				return errors.New("--reason is required for notify resend")
			}
			existing, err := shard.OutboxForDigest(context.Background(), id)
			if err != nil {
				return err
			}
			for _, o := range existing {
				if o.ResendRevision > rev {
					rev = o.ResendRevision
				}
			}
			rev++
		}
		o, _, err := notify.BuildOutbox(cfg, *d, items, rev, *reason, time.Now().Unix())
		if err != nil {
			return err
		}
		outboxID, created, err := shard.EnqueueOutbox(context.Background(), o)
		if err != nil {
			return err
		}
		fmt.Printf("outbox_id=%s created=%t revision=%d status=%s\n", outboxID, created, rev, storage.OutboxPending)
		return nil
	default:
		return fmt.Errorf("unknown notify subcommand %q", sub)
	}
}

func cmdDB(args []string) error {
	if len(args) == 0 {
		return errors.New("db subcommand required: migrate|verify")
	}
	sub := args[0]
	fs := flag.NewFlagSet("db "+sub, flag.ContinueOnError)
	path := addCommon(fs)
	all := fs.Bool("all", false, "apply to all annual shards")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	_ = all
	cfg, err := loadCfg(*path)
	if err != nil {
		return err
	}
	switch sub {
	case "migrate":
		lk, err := acquireLock(cfg)
		if err != nil {
			return err
		}
		defer lk.Release()
		mgr, err := newManager(cfg)
		if err != nil {
			return err
		}
		defer mgr.Close()
		years, err := mgr.ShardYears()
		if err != nil {
			return err
		}
		for _, y := range years {
			if _, err := mgr.Open(y); err != nil {
				return err
			}
			fmt.Printf("migrated shard %d\n", y)
		}
		if len(years) == 0 {
			now := time.Now()
			shard, err := mgr.OpenForWrite(now)
			if err != nil {
				return err
			}
			fmt.Printf("created shard %d at %s\n", shard.Year, shard.Path)
		}
		return nil
	case "verify":
		mgr, err := newManager(cfg)
		if err != nil {
			return err
		}
		defer mgr.Close()
		results, err := mgr.Verify(context.Background())
		if err != nil {
			return err
		}
		b, _ := json.MarshalIndent(results, "", "  ")
		fmt.Println(string(b))
		for _, r := range results {
			if r.Error != "" {
				return fmt.Errorf("shard %d failed verification: %s", r.Year, r.Error)
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown db subcommand %q", sub)
	}
}

func cmdBackup(args []string) error {
	if len(args) == 0 || args[0] != "create" {
		return errors.New("usage: newspulse backup create --config ...")
	}
	fs := flag.NewFlagSet("backup create", flag.ContinueOnError)
	path := addCommon(fs)
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	cfg, err := loadCfg(*path)
	if err != nil {
		return err
	}
	lk, err := acquireLock(cfg)
	if err != nil {
		return err
	}
	defer lk.Release()
	mgr, err := newManager(cfg)
	if err != nil {
		return err
	}
	defer mgr.Close()
	manifest, err := mgr.CreateBackup(context.Background(), scheduler.AppVersion, time.Now(), cfg.Backup)
	if err != nil {
		return err
	}
	b, _ := json.MarshalIndent(manifest, "", "  ")
	fmt.Println(string(b))
	return nil
}

func apiBaseURL(cfg *config.Config) string {
	listen := cfg.HTTP.Listen
	if strings.HasPrefix(listen, "0.0.0.0:") {
		listen = "127.0.0.1:" + strings.TrimPrefix(listen, "0.0.0.0:")
	}
	if strings.HasPrefix(listen, "[::]:") {
		listen = "127.0.0.1:" + strings.TrimPrefix(listen, "[::]:")
	}
	return "http://" + listen
}

func submitAPI(cfg *config.Config, method, path string, body interface{}) (int, map[string]interface{}, error) {
	token := cfg.AdminToken()
	if token == "" {
		return 0, nil, errors.New("admin token environment variable is empty")
	}
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return 0, nil, err
		}
	}
	req, err := http.NewRequest(method, apiBaseURL(cfg)+path, &buf)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("daemon is not reachable at %s: %w", apiBaseURL(cfg), err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var out map[string]interface{}
	_ = json.Unmarshal(raw, &out)
	if resp.StatusCode >= 300 {
		msg := strings.TrimSpace(string(raw))
		if out != nil {
			if e, ok := out["error"].(string); ok {
				msg = e
			}
		}
		return resp.StatusCode, out, fmt.Errorf("api %s %s returned %d: %s", method, path, resp.StatusCode, msg)
	}
	return resp.StatusCode, out, nil
}

func submitFetchViaAPI(cfg *config.Config) error {
	code, out, err := submitAPI(cfg, http.MethodPost, "/api/v1/jobs/fetch", nil)
	if err != nil {
		return err
	}
	fmt.Printf("daemon is running; fetch job submitted via local API (http %d): %v\n", code, out)
	return nil
}

func submitDigestViaAPI(cfg *config.Config, at string, fetch bool) error {
	body := map[string]interface{}{"fetch": fetch}
	if at != "" {
		body["at"] = at
	}
	code, out, err := submitAPI(cfg, http.MethodPost, "/api/v1/jobs/digest", body)
	if err != nil {
		return err
	}
	fmt.Printf("daemon is running; draft digest job submitted via local API (http %d): %v\n", code, out)
	return nil
}

func submitNotifyViaAPI(cfg *config.Config, year int, id string) error {
	code, out, err := submitAPI(cfg, http.MethodPost, fmt.Sprintf("/api/v1/digests/%d/%s/notify", year, id), nil)
	if err != nil {
		return err
	}
	fmt.Printf("daemon is running; notification enqueue submitted via local API (http %d): %v\n", code, out)
	return nil
}

func cmdHealthcheck(args []string) error {
	fs := flag.NewFlagSet("healthcheck", flag.ContinueOnError)
	url := fs.String("url", "http://127.0.0.1:8080/healthz", "health endpoint URL")
	timeout := fs.Duration("timeout", 3*time.Second, "request timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, *url, nil)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: *timeout}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("health endpoint returned %d", resp.StatusCode)
	}
	return nil
}

func cmdServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	path := addCommon(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := loadCfg(*path)
	if err != nil {
		return err
	}
	lk, err := acquireLock(cfg)
	if err != nil {
		return err
	}
	defer lk.Release()
	log := logging.New(cfg.App.LogLevel)
	slog.SetDefault(log)
	mgr, err := newManager(cfg)
	if err != nil {
		return err
	}
	defer mgr.Close()
	shard, err := mgr.OpenForWrite(time.Now())
	if err != nil {
		return err
	}
	log.Info("newspulse starting", "version", scheduler.AppVersion,
		"go", runtime.Version(), "sqlite", storage.SQLiteVersion(shard),
		"data_dir", cfg.App.DataDir, "shard_year", shard.Year,
		"schedule_enabled", cfg.Schedule.Enabled,
		"notification_enabled", cfg.Notification.Enabled,
		"llm_enabled", cfg.LLM.Enabled)
	if cfg.Schedule.Enabled && len(cfg.Schedule.Times) == 0 {
		return errors.New("schedule.enabled is true but schedule.times is empty")
	}
	if cfg.Notification.Enabled && !cfg.Schedule.Enabled {
		log.Warn("notification is enabled but schedule is disabled; only manual API/CLI sends will occur")
	}
	feedSvc := feed.NewService(cfg, mgr, clock.Real{}, log)
	analyzer := llm.NewAnalyzer(cfg, mgr, clock.Real{}, log)
	worker := notify.NewWorker(cfg, mgr, clock.Real{}, log)
	sched := scheduler.New(cfg, mgr, feedSvc, analyzer, worker, clock.Real{}, log)
	apiServer := httpapi.New(cfg, mgr, sched, clock.Real{}, log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	schedDone := make(chan struct{})
	go func() { sched.Run(ctx); close(schedDone) }()
	apiErr := make(chan error, 1)
	go func() { apiErr <- apiServer.Start(ctx) }()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sig:
		log.Info("shutdown signal received")
	case err := <-apiErr:
		if err != nil {
			cancel()
			return err
		}
	}
	cancel()
	// Give background work a bounded window to stop; SQL is short and
	// external calls are context-bounded, but a hung provider must not block
	// process exit forever.
	select {
	case <-schedDone:
	case <-time.After(10 * time.Second):
		log.Warn("scheduler did not stop within deadline")
	}
	select {
	case <-apiErr:
	case <-time.After(5 * time.Second):
		log.Warn("http server did not stop within deadline")
	}
	return nil
}
