package storage

import (
	"context"
	"testing"
	"time"

	"github.com/jxrory/newspulse/internal/config"
)

func testManager(t *testing.T) *ShardManager {
	t.Helper()
	cfg := config.Defaults()
	cfg.App.DataDir = t.TempDir()
	cfg.LLM.Enabled = false
	cfg.Notification.Enabled = false
	mgr, err := NewShardManager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { mgr.Close() })
	return mgr
}

func TestSaveDigestAtomicAndIdempotent(t *testing.T) {
	mgr := testManager(t)
	shard, err := mgr.OpenForWrite(time.Date(2026, 11, 24, 7, 15, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	d := Digest{
		ID: "d1", JobID: "j1", SlotKey: "daily:x:2026-11-24:0715", Revision: 1,
		ScheduledAt: 1000, WindowStart: 1000 - 86400, WindowEnd: 1000, CutoffAt: 1000,
		Title: "t", Markdown: "m", ContentHash: "h", GenerationMode: "fallback",
		ModelMetadataJSON: "{}", SourceHealth: []SourceHealth{}, QualityFlags: []string{},
		CreatedAt: 1000,
	}
	items := []DigestItem{{
		Section: SectionInternational, EventKey: "e1", TitleZh: "title",
		Facts:      []Fact{{Text: "fact", EvidenceRefs: []string{"a1"}}},
		Importance: 80, Coverage: "single_source",
		ArticleRefs: []ArticleRef{{Year: 2026, ID: "a1"}},
		EvidenceSnapshot: []ArticleSnapshot{{Year: 2026, ID: "a1", PublisherID: "p",
			URL: "https://example.com/a"}},
		QualityFlags: []string{},
	}}
	outbox := &OutboxItem{
		DigestID: "d1", DedupKey: "dedup", Provider: "feishu_webhook",
		RecipientAlias: "news-group", PartNo: 1, PayloadJSON: "{}", PayloadHash: "ph",
		Status: OutboxPending, ExpiresAt: 9999, CreatedAt: 1000, UpdatedAt: 1000,
	}
	id, created, err := shard.SaveDigest(context.Background(), &DigestBundle{Digest: d, Items: items, Outbox: outbox})
	if err != nil {
		t.Fatal(err)
	}
	if !created || id != "d1" {
		t.Fatalf("created=%v id=%s", created, id)
	}
	if _, _, err := shard.GetDigest(context.Background(), "d1"); err != nil {
		t.Fatal(err)
	}
	var outboxCount int
	if err := shard.R().QueryRow(`SELECT count(*) FROM notification_outbox`).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if outboxCount != 1 {
		t.Fatalf("outbox count=%d", outboxCount)
	}
	id2, created2, err := shard.SaveDigest(context.Background(), &DigestBundle{Digest: d, Items: items, Outbox: outbox})
	if err != nil {
		t.Fatal(err)
	}
	if created2 || id2 != "d1" {
		t.Fatalf("second save created=%v id=%s", created2, id2)
	}
	outboxCount = 0
	if err := shard.R().QueryRow(`SELECT count(*) FROM notification_outbox`).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if outboxCount != 1 {
		t.Fatalf("duplicate outbox after replay: %d", outboxCount)
	}
}

func TestOutboxRecoveryUnknown(t *testing.T) {
	mgr := testManager(t)
	shard, err := mgr.OpenForWrite(time.Date(2026, 11, 24, 7, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	d := Digest{ID: "d2", SlotKey: "s", Revision: 1, ScheduledAt: 1, WindowStart: 0,
		WindowEnd: 1, CutoffAt: 1, Title: "t", Markdown: "m", ContentHash: "h",
		GenerationMode: "fallback", ModelMetadataJSON: "{}", SourceHealth: []SourceHealth{},
		QualityFlags: []string{}, CreatedAt: 1}
	items := []DigestItem{{Section: SectionStatus, EventKey: "status", TitleZh: "s",
		Facts: []Fact{{Text: "x"}}, Coverage: "unknown", ArticleRefs: []ArticleRef{},
		EvidenceSnapshot: []ArticleSnapshot{}, QualityFlags: []string{}}}
	o := &OutboxItem{DigestID: "d2", DedupKey: "dd2", Provider: "feishu_webhook",
		RecipientAlias: "g", PartNo: 1, PayloadJSON: "{}", PayloadHash: "h",
		Status: OutboxPending, ExpiresAt: 9999, CreatedAt: 1, UpdatedAt: 1}
	if _, _, err := shard.SaveDigest(context.Background(), &DigestBundle{Digest: d, Items: items, Outbox: o}); err != nil {
		t.Fatal(err)
	}
	// Simulate a crashed worker: row left sending with expired lease.
	now := int64(100)
	if _, err := shard.w.Exec(`UPDATE notification_outbox SET status='sending', lease_until=? WHERE id=?`, now-1, o.ID); err != nil {
		t.Fatal(err)
	}
	n, err := shard.RecoverSendingOnRestart(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("recovered %d", n)
	}
	var status string
	if err := shard.R().QueryRow(`SELECT status FROM notification_outbox WHERE id=?`, o.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != OutboxSendUnknown {
		t.Fatalf("status=%s want send_unknown", status)
	}
}
