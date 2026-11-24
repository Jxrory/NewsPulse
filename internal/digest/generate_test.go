package digest

import (
	"fmt"
	"strings"
	"testing"

	"github.com/jxrory/newspulse/internal/config"
	"github.com/jxrory/newspulse/internal/editorial"
	"github.com/jxrory/newspulse/internal/llm"
	"github.com/jxrory/newspulse/internal/storage"
)

func snapshotWithArticles() editorial.CandidateSnapshot {
	art := func(year int, id, section, group, title, lang string) editorial.SnapshotArticle {
		ts := int64(1000)
		return editorial.SnapshotArticle{
			Year: year, ID: id, Title: title, Summary: "摘要 " + title, URL: "https://example.org/" + id,
			PublisherID: group + "-pub", PublisherGroup: group, Language: lang,
			Section: section, PublishedAt: &ts, DateQuality: "parsed", FirstSeenAt: 1000, Score: 80,
		}
	}
	return editorial.CandidateSnapshot{
		GeneratedAt: 1000, WindowStart: 0, WindowEnd: 1000, CutoffAt: 1000,
		Groups: []editorial.SnapshotGroup{
			{Key: "candidate_group_1", Section: editorial.SectionInternational,
				Articles: []editorial.SnapshotArticle{art(2026, "a1", editorial.SectionInternational, "g1", "国际标题一", "zh")}},
			{Key: "candidate_group_2", Section: editorial.SectionDomestic,
				Articles: []editorial.SnapshotArticle{art(2026, "a2", editorial.SectionDomestic, "g2", "国内标题一", "zh")}},
		},
	}
}

func testJob() storage.JobWithYear {
	ws := int64(0)
	we := int64(1000)
	cut := int64(1000)
	return storage.JobWithYear{
		Job: storage.Job{
			ID: "job1", JobKey: "manual:generate:x", ScheduledAt: 1000,
			WindowStart: &ws, WindowEnd: &we, CutoffAt: &cut,
		},
		OwnerYear: 2026,
	}
}

func TestBuildFallback(t *testing.T) {
	cfg := config.Defaults()
	cfg.Schedule.Enabled = false
	cfg.LLM.Enabled = false
	d, items, flags := Build(cfg, BuildInput{
		Job: testJob(), Snapshot: snapshotWithArticles(), Mode: "fallback",
		SourceHealth: []storage.SourceHealth{{SourceID: "s1", Health: "ok"}},
		Now:          1000,
	})
	if d.GenerationMode != "fallback" {
		t.Fatalf("mode=%s", d.GenerationMode)
	}
	if len(items) != 3 {
		t.Fatalf("expected 2 editorial + 1 status, got %d", len(items))
	}
	if !strings.Contains(d.Markdown, "## 国际新闻") || !strings.Contains(d.Markdown, "## 中国国内新闻") {
		t.Fatalf("markdown missing sections:\n%s", d.Markdown)
	}
	if !strings.Contains(strings.Join(flags, ","), "rss_summary_only") {
		t.Fatalf("flags=%v", flags)
	}
	if d.ID != "job1" {
		t.Fatalf("digest id must be set before rendering footer: %q", d.ID)
	}
	if !strings.Contains(d.Markdown, "简报 ID：2026/job1") {
		t.Fatalf("digest footer missing ID: %s", d.Markdown)
	}
}

func TestRepeatSuppression(t *testing.T) {
	cfg := config.Defaults()
	snap := snapshotWithArticles()
	prev := map[string]struct{}{"a1": {}, "a2": {}}
	var events []llm.AnalysisEvent
	for _, g := range snap.Groups {
		a := g.Articles[0]
		events = append(events, llm.AnalysisEvent{
			EventKey: g.Key, Section: g.Section, TitleZh: a.Title,
			Facts:       []storage.Fact{{Text: "事实", EvidenceRefs: []string{a.ID}}},
			ArticleRefs: []storage.ArticleRef{{Year: a.Year, ID: a.ID}}, Importance: 80,
		})
	}
	d, items, flags := Build(cfg, BuildInput{
		Job: testJob(), Snapshot: snap, Events: events, Mode: "ai",
		PreviousRefs: prev, Now: 1000,
	})
	if len(items) != 1 || items[0].Section != storage.SectionStatus {
		t.Fatalf("expected status-only digest, got %d items", len(items))
	}
	if !strings.Contains(strings.Join(flags, ","), "repeat_events_suppressed") {
		t.Fatalf("flags=%v", flags)
	}
	if d.Title != "新闻采集状态 · "+"1970-01-01 00:16" {
		t.Logf("title=%q (display timezone dependent)", d.Title)
	}
}

func TestPublisherCapRelaxedWhenBelowMin(t *testing.T) {
	cfg := config.Defaults()
	cfg.Editorial.MinPerSection = 3
	cfg.Editorial.TargetPerSection = 3
	cfg.Editorial.MaxPerSection = 4
	cfg.Editorial.MaxPublisherShareInternational = 0.5
	var groups []editorial.SnapshotGroup
	var events []llm.AnalysisEvent
	for i := 0; i < 4; i++ {
		id := fmt.Sprintf("i%d", i)
		ts := int64(1000)
		groups = append(groups, editorial.SnapshotGroup{
			Key: fmt.Sprintf("g%d", i), Section: editorial.SectionInternational,
			Articles: []editorial.SnapshotArticle{{
				Year: 2026, ID: id, Title: "title", Summary: "s", URL: "https://e.org/" + id,
				PublisherGroup: "same", PublisherID: "p", Language: "zh", Section: editorial.SectionInternational,
				PublishedAt: &ts, DateQuality: "parsed", FirstSeenAt: 1000, Score: 80,
			}},
		})
		events = append(events, llm.AnalysisEvent{
			EventKey: fmt.Sprintf("e%d", i), Section: editorial.SectionInternational,
			TitleZh: "标题", Facts: []storage.Fact{{Text: "事实", EvidenceRefs: []string{id}}},
			ArticleRefs: []storage.ArticleRef{{Year: 2026, ID: id}}, Importance: 80 - i,
		})
	}
	snap := editorial.CandidateSnapshot{WindowStart: 0, WindowEnd: 1000, CutoffAt: 1000, Groups: groups}
	d, items, flags := Build(cfg, BuildInput{
		Job: testJob(), Snapshot: snap, Events: events, Mode: "ai", Now: 1000,
	})
	_ = d
	if len(items) != 4 {
		t.Fatalf("expected 3 editorial + status after relaxation, got %d", len(items))
	}
	if !strings.Contains(strings.Join(flags, ","), "publisher_cap_relaxed") {
		t.Fatalf("expected cap relaxed flag, got %v", flags)
	}
	// First pass caps one group at 2 (floor(4*0.5)), then relaxation adds one
	// more to reach the minimum of 3.
	if countFlags(items, "publisher_cap_relaxed") < 1 {
		t.Fatalf("expected at least one item flag")
	}
}

func countFlags(items []storage.DigestItem, flag string) int {
	n := 0
	for _, it := range items {
		for _, f := range it.QualityFlags {
			if f == flag {
				n++
			}
		}
	}
	return n
}

func TestCoverageFlagsAndStatusFacts(t *testing.T) {
	cfg := config.Defaults()
	cfg.LLM.Enabled = false
	ts := int64(1000)
	snap := editorial.CandidateSnapshot{
		GeneratedAt: 1000, WindowStart: 0, WindowEnd: 1000, CutoffAt: 1000,
		Groups: []editorial.SnapshotGroup{{
			Key: "candidate_group_dom", Section: editorial.SectionDomestic,
			Articles: []editorial.SnapshotArticle{
				{Year: 2026, ID: "d1", Title: "国内一", Summary: "摘要", URL: "https://e.org/d1",
					PublisherID: "p1", PublisherGroup: "group-a", Section: editorial.SectionDomestic,
					PublishedAt: &ts, DateQuality: "parsed", FirstSeenAt: 1000, Score: 80},
				{Year: 2026, ID: "d2", Title: "国内二", Summary: "摘要", URL: "https://e.org/d2",
					PublisherID: "p2", PublisherGroup: "group-b", Section: editorial.SectionDomestic,
					PublishedAt: &ts, DateQuality: "parsed", FirstSeenAt: 1000, Score: 70},
			},
		}},
	}
	d, items, flags := Build(cfg, BuildInput{
		Job: testJob(), Snapshot: snap, Mode: "fallback", Now: 1000,
	})
	if !strings.Contains(strings.Join(flags, ","), "independence_unverified") {
		t.Fatalf("expected independence_unverified flag, got %v", flags)
	}
	if strings.Contains(strings.Join(flags, ","), "single_group_domestic_coverage") {
		t.Fatalf("two publisher groups must not raise single_group flag: %v", flags)
	}
	status := items[len(items)-1]
	if status.Section != storage.SectionStatus || !strings.Contains(factText(status), "独立性关系未逐条核实") {
		t.Fatalf("status facts missing independence fact: %+v", status.Facts)
	}
	if !strings.Contains(d.Markdown, "独立性未核实") {
		t.Fatalf("markdown missing independence label:\n%s", d.Markdown)
	}

	singleGroup := editorial.CandidateSnapshot{
		GeneratedAt: 1000, WindowStart: 0, WindowEnd: 1000, CutoffAt: 1000,
		Groups: []editorial.SnapshotGroup{{
			Key: "candidate_group_dom_single", Section: editorial.SectionDomestic,
			Articles: []editorial.SnapshotArticle{
				{Year: 2026, ID: "s1", Title: "国内一", Summary: "摘要", URL: "https://e.org/s1",
					PublisherID: "p1", PublisherGroup: "group-a", Section: editorial.SectionDomestic,
					PublishedAt: &ts, DateQuality: "parsed", FirstSeenAt: 1000, Score: 80},
			},
		}},
	}
	_, singleItems, singleFlags := Build(cfg, BuildInput{
		Job: testJob(), Snapshot: singleGroup, Mode: "fallback", Now: 1000,
	})
	if !strings.Contains(strings.Join(singleFlags, ","), "single_group_domestic_coverage") {
		t.Fatalf("expected single_group flag, got %v", singleFlags)
	}
	if !strings.Contains(factText(singleItems[len(singleItems)-1]), "同一媒体集团") {
		t.Fatalf("status facts missing single-group fact: %+v", singleItems[len(singleItems)-1].Facts)
	}
}

func factText(item storage.DigestItem) string {
	var parts []string
	for _, f := range item.Facts {
		parts = append(parts, f.Text)
	}
	return strings.Join(parts, "\n")
}
