package scheduler

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/jxrory/newspulse/internal/clock"
	"github.com/jxrory/newspulse/internal/config"
	"github.com/jxrory/newspulse/internal/storage"
)

func TestCreateDueSlotsIsIdempotent(t *testing.T) {
	cfg := config.Defaults()
	cfg.App.DataDir = t.TempDir()
	cfg.Schedule.Enabled = true
	cfg.Schedule.ID = "test-daily"
	cfg.Schedule.Timezone = "Asia/Shanghai"
	cfg.Schedule.Times = []string{"07:15"}
	cfg.Schedule.PrepareBefore = config.Duration(5 * time.Minute)
	cfg.Schedule.CatchupMaxAge = config.Duration(6 * time.Hour)
	cfg.Schedule.CatchupSlots = 1
	cfg.Notification.Enabled = false
	cfg.LLM.Enabled = false
	mgr, err := storage.NewShardManager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()
	loc, _ := time.LoadLocation("Asia/Shanghai")
	now := time.Date(2026, 11, 24, 7, 12, 0, 0, loc)
	svc := New(cfg, mgr, nil, nil, nil, clock.NewFake(now), slog.Default())
	created, _, err := svc.CreateDueSlots(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if created != 1 {
		t.Fatalf("created=%d", created)
	}
	created, _, err = svc.CreateDueSlots(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if created != 0 {
		t.Fatalf("duplicate slots created: %d", created)
	}
	shard, err := mgr.OpenForWrite(now)
	if err != nil {
		t.Fatal(err)
	}
	job, err := shard.GetJobByKey(context.Background(), "daily:test-daily:2026-11-24:07:15")
	if err != nil || job == nil {
		t.Fatalf("job missing: %v", err)
	}
	if job.Status != storage.JobPending {
		t.Fatalf("job status=%s", job.Status)
	}
}

func TestMissingDSTTimeIsSkipped(t *testing.T) {
	cfg := config.Defaults()
	cfg.App.DataDir = t.TempDir()
	cfg.Schedule.Enabled = true
	cfg.Schedule.ID = "dst"
	cfg.Schedule.Timezone = "America/New_York"
	cfg.Schedule.Times = []string{"02:30"}
	cfg.Schedule.PrepareBefore = config.Duration(5 * time.Minute)
	cfg.Schedule.CatchupMaxAge = config.Duration(6 * time.Hour)
	cfg.LLM.Enabled = false
	cfg.Notification.Enabled = false
	mgr, err := storage.NewShardManager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()
	loc, _ := time.LoadLocation("America/New_York")
	// 2026-03-08: 02:00 -> 03:00; 02:30 does not exist.
	now := time.Date(2026, 3, 8, 1, 50, 0, 0, loc)
	svc := New(cfg, mgr, nil, nil, nil, clock.NewFake(now), slog.Default())
	created, _, err := svc.CreateDueSlots(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if created != 0 {
		t.Fatalf("nonexistent DST wall time created a job")
	}
}
