package notify

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"time"

	"github.com/jxrory/newspulse/internal/clock"
	"github.com/jxrory/newspulse/internal/config"
	"github.com/jxrory/newspulse/internal/logging"
	"github.com/jxrory/newspulse/internal/storage"
)

type Worker struct {
	cfg    *config.Config
	mgr    *storage.ShardManager
	clk    clock.Clock
	log    *slog.Logger
	client *http.Client

	mu            sync.Mutex
	lastSend      time.Time
	lastBudgetLog time.Time
}

func NewWorker(cfg *config.Config, mgr *storage.ShardManager, clk clock.Clock, log *slog.Logger) *Worker {
	if clk == nil {
		clk = clock.Real{}
	}
	if log == nil {
		log = slog.Default()
	}
	return &Worker{
		cfg: cfg, mgr: mgr, clk: clk, log: log,
		client: &http.Client{
			Timeout: cfg.Notification.Timeout.D(),
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return errors.New("webhook redirects are disabled")
			},
		},
	}
}

func (w *Worker) Recover(ctx context.Context) error {
	now := w.clk.Now().Unix()
	n, err := w.mgr.RecoverOutboxOnRestart(ctx, now)
	if err != nil {
		return err
	}
	if n > 0 {
		w.log.Warn("converted interrupted notification sends to send_unknown", "count", n)
	}
	m, err := w.mgr.ExpireOldOutbox(ctx, now)
	if err != nil {
		return err
	}
	if m > 0 {
		w.log.Info("expired old notification outbox rows", "count", m)
	}
	return nil
}

func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.Tick(ctx); err != nil {
				w.log.Error("notification worker tick failed", "error", logging.ErrorString(err))
			}
		}
	}
}

func (w *Worker) Tick(ctx context.Context) error {
	if !w.cfg.Notification.Enabled {
		return nil
	}
	now := w.clk.Now()
	start, end := dayBounds(w.cfg, now)
	_ = end
	attempts, err := w.mgr.NotificationAttemptsSince(ctx, start)
	if err != nil {
		return err
	}
	if attempts >= w.cfg.Notification.DailyRequestBudget {
		w.mu.Lock()
		if now.Sub(w.lastBudgetLog) > 10*time.Minute {
			w.lastBudgetLog = now
			w.log.Warn("notification daily request budget exhausted", "attempts", attempts, "budget", w.cfg.Notification.DailyRequestBudget)
		}
		w.mu.Unlock()
		return nil
	}
	releaseWrite := w.mgr.BeginWrite()
	items, err := w.mgr.ClaimDueOutbox(ctx, now.Unix(), 90*time.Second, 5)
	releaseWrite()
	if err != nil {
		return err
	}
	for _, item := range items {
		if err := w.sendOne(ctx, item); err != nil {
			w.log.Error("notification send failed", "outbox_id", item.ID, "error", logging.ErrorString(err))
		}
	}
	return nil
}

func (w *Worker) sendOne(ctx context.Context, item storage.OutboxItem) error {
	now := w.clk.Now().Unix()
	shard, err := w.mgr.Open(item.OwnerYear)
	if err != nil {
		return err
	}
	finish := func(out Outcome) error {
		status := out.Status
		var next *int64
		switch status {
		case storage.OutboxSent:
			// terminal
		case storage.OutboxRetryWait:
			if item.SendAttemptCount >= w.cfg.Notification.MaxSendAttempts {
				status = storage.OutboxFailed
				out.ErrorCode = "max_attempts_exceeded"
			} else {
				delay := sendBackoff(item.SendAttemptCount)
				if out.RetryAfter > 0 {
					delay = time.Duration(out.RetryAfter) * time.Second
					if delay < time.Second {
						delay = time.Second
					}
				}
				n := now + int64(delay.Seconds())
				next = &n
			}
		case storage.OutboxSendUnknown:
			if w.cfg.Notification.RetryUnknown && item.SendAttemptCount < w.cfg.Notification.MaxSendAttempts {
				status = storage.OutboxRetryWait
				n := now + int64(sendBackoff(item.SendAttemptCount).Seconds())
				next = &n
			}
		}
		out.Status = status
		releaseWrite := w.mgr.BeginWrite()
		ferr := shard.FinishOutboxAttempt(ctx, &item, item.AttemptID, storage.OutboxResult{
			Status: status, HTTPStatus: out.HTTPStatus, ProviderCode: out.ProviderCode,
			ErrorCode: out.ErrorCode, ResponseJSON: out.ResponseJSON,
			NextAttemptAt: next, Now: now,
		})
		releaseWrite()
		return ferr
	}

	if item.ExpiresAt <= now {
		return finish(Outcome{Status: storage.OutboxExpired, ErrorCode: "notification_expired"})
	}
	rawURL := w.cfg.FeishuWebhook()
	if err := config.ValidateFeishuWebhook(rawURL); err != nil {
		return finish(Outcome{Status: storage.OutboxFailed, ErrorCode: "invalid_webhook_config"})
	}
	secret := w.cfg.FeishuSecret()
	if w.cfg.Notification.RequireSignature && secret == "" {
		return finish(Outcome{Status: storage.OutboxFailed, ErrorCode: "missing_sign_secret"})
	}

	ts := w.clk.Now().Unix()
	sign := ""
	if w.cfg.Notification.RequireSignature {
		sign = Sign(ts, secret)
	}
	reqBytes, err := BuildFinalRequest(item.PayloadJSON, ts, sign)
	if err != nil {
		return finish(Outcome{Status: storage.OutboxFailed, ErrorCode: "bad_payload"})
	}
	if len(reqBytes) > w.cfg.Notification.MaxPayloadBytes {
		return finish(Outcome{Status: storage.OutboxFailed, ErrorCode: "payload_too_large"})
	}

	if err := w.waitInterval(ctx); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, bytes.NewReader(reqBytes))
	if err != nil {
		return finish(Outcome{Status: storage.OutboxFailed, ErrorCode: "bad_request"})
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "NewsPulse/0.1")
	var wrote bool
	var writeMu sync.Mutex
	trace := &httptrace.ClientTrace{
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			writeMu.Lock()
			wrote = true
			writeMu.Unlock()
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	resp, err := w.client.Do(req)
	w.mu.Lock()
	w.lastSend = w.clk.Now()
	w.mu.Unlock()
	if err != nil {
		writeMu.Lock()
		written := wrote
		writeMu.Unlock()
		if written {
			w.log.Warn("notification response unknown after write", "outbox_id", item.ID, "error", logging.ErrorString(err))
			return finish(Outcome{Status: storage.OutboxSendUnknown, ErrorCode: "request_sent_response_unknown"})
		}
		return finish(Outcome{Status: storage.OutboxRetryWait, ErrorCode: "connection_not_established", RetryAfter: 5})
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	out := ParseResponse(resp.StatusCode, raw)
	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		out.Status = storage.OutboxRetryWait
		out.ErrorCode = "http_429"
		out.RetryAfter = retryAfterSeconds(resp.Header.Get("Retry-After"))
	case resp.StatusCode >= 500:
		out.Status = storage.OutboxSendUnknown
		out.ErrorCode = fmt.Sprintf("http_%d", resp.StatusCode)
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		out.Status = storage.OutboxFailed
		if out.ErrorCode == "" {
			out.ErrorCode = fmt.Sprintf("http_%d", resp.StatusCode)
		}
	}
	if out.Status == storage.OutboxSent {
		if out.ProviderCode != "0" {
			out.Status = storage.OutboxSendUnknown
			out.ErrorCode = "missing_explicit_zero_code"
		}
	}
	w.log.Info("notification attempt completed", "outbox_id", item.ID, "status", out.Status,
		"http", resp.StatusCode, "provider_code", out.ProviderCode, "error", out.ErrorCode)
	return finish(out)
}

func (w *Worker) waitInterval(ctx context.Context) error {
	w.mu.Lock()
	interval := w.cfg.Notification.MinSendInterval.D()
	last := w.lastSend
	w.mu.Unlock()
	if last.IsZero() || interval <= 0 {
		return nil
	}
	next := last.Add(interval)
	if d := time.Until(next); d > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d):
		}
	}
	return nil
}

func sendBackoff(attempt int) time.Duration {
	switch attempt {
	case 1:
		return 5 * time.Second
	case 2:
		return 30 * time.Second
	default:
		return 120 * time.Second
	}
}

func retryAfterSeconds(raw string) int64 {
	if raw == "" {
		return 0
	}
	if t, err := http.ParseTime(raw); err == nil {
		d := time.Until(t)
		if d > 0 {
			return int64(d.Seconds())
		}
	}
	d, err := time.ParseDuration(strings.TrimSpace(raw) + "s")
	if err == nil {
		return int64(d.Seconds())
	}
	return 0
}

func dayBounds(cfg *config.Config, now time.Time) (int64, int64) {
	loc := cfg.DisplayLocation()
	if loc == nil {
		loc = time.UTC
	}
	t := now.In(loc)
	start := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc)
	return start.UTC().Unix(), start.Add(24 * time.Hour).UTC().Unix()
}
