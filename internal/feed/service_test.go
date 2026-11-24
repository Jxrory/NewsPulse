package feed

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jxrory/newspulse/internal/clock"
	"github.com/jxrory/newspulse/internal/config"
	"github.com/jxrory/newspulse/internal/storage"
)

func feedConfig(t *testing.T, sources ...config.SourceConfig) *config.Config {
	t.Helper()
	c := config.Defaults()
	c.App.DataDir = t.TempDir()
	c.LLM.Enabled = false
	c.Notification.Enabled = false
	c.Schedule.Enabled = false
	c.Sources = sources
	return c
}

func source(id, url string) config.SourceConfig {
	return config.SourceConfig{
		ID: id, URL: url, PublisherID: "pub-" + id, PublisherGroup: "grp-" + id,
		Language: "en", SectionHint: "international",
		PollInterval: config.Duration(10 * time.Minute), Enabled: true,
	}
}

func rssFor(base string, title string, pub string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><title>Feed</title>
<item>
  <title>%s</title>
  <link>%s/article-a?utm_source=feed</link>
  <guid>guid-a</guid>
  <description><![CDATA[<p>Summary <b>one</b></p>]]></description>
  <pubDate>%s</pubDate>
</item>
</channel></rss>`, title, base, pub)
}

func TestFetchDedupAndYearRouting(t *testing.T) {
	baseTitle := "Calm headline"
	var rss []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Content-Type", "text/xml; charset=utf-8")
		if _, err := w.Write(rss); err != nil {
			t.Errorf("write feed: %v", err)
		}
	}))
	defer srv.Close()
	rss = []byte(rssFor(srv.URL, baseTitle, "Tue, 31 Dec 2026 10:00:00 +0000"))

	srcA := source("feed_a", srv.URL)
	cfg := feedConfig(t, srcA)
	mgr, err := storage.NewShardManager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	loc, _ := time.LoadLocation("Asia/Shanghai")
	clk := clock.NewFake(time.Date(2026, 12, 31, 23, 59, 0, 0, loc))
	svc := NewService(cfg, mgr, clk, slog.Default())
	ctx := context.Background()

	out, err := svc.FetchSource(ctx, srcA)
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != "success" || out.Inserted != 1 {
		t.Fatalf("first fetch outcome %+v", out)
	}

	// 304 must keep the article count and not fabricate inserts.
	out, err = svc.FetchSource(ctx, srcA)
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != "not_modified" || out.Inserted != 0 || out.Updated != 0 {
		t.Fatalf("304 outcome %+v", out)
	}
	articles := listArticles(t, mgr, 2026)
	if len(articles) != 1 {
		t.Fatalf("expected 1 article, got %d", len(articles))
	}
	if articles[0].FirstSeenAt != clk.Now().Unix() {
		t.Fatalf("first_seen_at not captured time: %d vs %d", articles[0].FirstSeenAt, clk.Now().Unix())
	}

	// New year: same URL re-seen must update the original 2026 shard, not
	// create a duplicate in the 2027 shard.
	clk.Set(time.Date(2027, 1, 1, 0, 1, 0, 0, loc))
	out, err = svc.FetchSource(ctx, srcA)
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != "success" || out.Inserted != 0 || out.Updated != 1 {
		t.Fatalf("new-year fetch outcome %+v", out)
	}
	if got := len(listArticles(t, mgr, 2026)); got != 1 {
		t.Fatalf("2026 articles=%d", got)
	}
	if got := len(listArticles(t, mgr, 2027)); got != 0 {
		t.Fatalf("article leaked into 2027: %d", got)
	}
	years, _ := mgr.ShardYears()
	if len(years) != 2 {
		t.Fatalf("expected two shards, got %v", years)
	}
}

func TestCrossFeedURLAliasDedup(t *testing.T) {
	rssXML := []byte(rssFor("", "Shared headline", "Tue, 24 Nov 2026 10:00:00 +0000"))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/xml; charset=utf-8")
		_, _ = w.Write(rssXML)
	}))
	defer srv.Close()
	rssXML = []byte(rssFor(srv.URL, "Shared headline", "Tue, 24 Nov 2026 10:00:00 +0000"))

	linkA := srv.URL + "/redirect?to=article-a"
	srcA := source("a", linkA)
	srcB := source("b", linkA)
	srcB.PublisherGroup = "other-group"
	cfg := feedConfig(t, srcA, srcB)
	mgr, err := storage.NewShardManager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()
	clk := clock.NewFake(time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC))
	svc := NewService(cfg, mgr, clk, slog.Default())
	if _, err := svc.FetchSource(context.Background(), srcA); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.FetchSource(context.Background(), srcB); err != nil {
		t.Fatal(err)
	}
	if got := len(listArticles(t, mgr, 2026)); got != 1 {
		t.Fatalf("cross-feed URL dedup failed: %d articles", got)
	}
	sh, _ := mgr.Open(2026)
	var count int
	if err := sh.R().QueryRow(`SELECT count(*) FROM article_observations`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("expected 2 observations, got %d", count)
	}
}

func listArticles(t *testing.T, mgr *storage.ShardManager, year int) []storage.Article {
	t.Helper()
	sh, err := mgr.Open(year)
	if err != nil {
		return nil
	}
	items, err := sh.QueryArticles(context.Background(), storage.ArticleFilter{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	return items
}
