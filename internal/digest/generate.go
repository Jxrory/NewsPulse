package digest

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jxrory/newspulse/internal/config"
	"github.com/jxrory/newspulse/internal/editorial"
	"github.com/jxrory/newspulse/internal/llm"
	"github.com/jxrory/newspulse/internal/storage"
)

type BuildInput struct {
	Job            storage.JobWithYear
	Snapshot       editorial.CandidateSnapshot
	Events         []llm.AnalysisEvent
	Mode           string
	FallbackReason string
	ModelMetadata  string
	SourceHealth   []storage.SourceHealth
	PreviousRefs   map[string]struct{}
	Now            int64
}

// Build assembles the final digest from validated events or, when the model
// was unavailable, from deterministic RSS summaries. Selection is rule-based:
// coverage caps, repeat suppression and per-section limits are enforced here,
// never by the model.
func Build(cfg *config.Config, in BuildInput) (storage.Digest, []storage.DigestItem, []string) {
	snap := in.Snapshot
	flags := []string{}
	mode := in.Mode
	if mode == "" {
		mode = "ai"
	}
	events := in.Events
	fallback := false
	if mode == "fallback" || len(events) == 0 {
		fallback = true
		mode = "fallback"
		events = fallbackEvents(snap)
		flags = append(flags, "ai_degraded", "rss_summary_only")
		if in.FallbackReason != "" {
			flags = append(flags, "degrade_reason_"+in.FallbackReason)
		}
	}
	items, repeatSuppressed, capRelaxed := selectItems(cfg, snap, events, in.PreviousRefs)
	if repeatSuppressed > 0 {
		flags = append(flags, "repeat_events_suppressed")
	}
	if capRelaxed {
		flags = append(flags, "publisher_cap_relaxed")
	}

	for _, it := range items {
		if it.Coverage == "multiple_sources" {
			flags = append(flags, "independence_unverified")
			break
		}
	}
	intl, dom := countSections(items)
	if dom > 0 && domesticGroupCount(items) <= 1 {
		flags = append(flags, "single_group_domestic_coverage")
	}
	if intl < cfg.Editorial.MinPerSection {
		flags = append(flags, "below_min_international", "insufficient_international_sources")
	}
	if dom < cfg.Editorial.MinPerSection {
		flags = append(flags, "below_min_domestic", "insufficient_domestic_sources")
	}
	if intl == 0 {
		flags = append(flags, "no_international_items")
	}
	if dom == 0 {
		flags = append(flags, "no_domestic_items")
	}
	degraded := degradedSources(in.SourceHealth)
	if len(degraded) > 0 {
		flags = append(flags, "source_degraded")
	}
	translationMissing := false
	for _, it := range items {
		for _, f := range it.QualityFlags {
			if f == "translation_unavailable" {
				translationMissing = true
			}
		}
	}
	if translationMissing {
		flags = append(flags, "translation_unavailable")
	}
	if snap.LateCount > 0 {
		flags = append(flags, "late_arrivals_included_or_considered")
	}
	if snap.UndatedCount > 0 {
		flags = append(flags, "undated_candidates_limited")
	}
	flags = dedupe(flags)

	statusFlags := append([]string{}, flags...)
	statusItem := buildStatusItem(snap, items, in.SourceHealth, degraded, fallback, repeatSuppressed, statusFlags)
	items = append(items, statusItem)

	windowStart := int64(0)
	windowEnd := int64(0)
	cutoff := int64(0)
	scheduleID := ""
	if in.Job.WindowStart != nil {
		windowStart = *in.Job.WindowStart
	}
	if in.Job.WindowEnd != nil {
		windowEnd = *in.Job.WindowEnd
	}
	if in.Job.CutoffAt != nil {
		cutoff = *in.Job.CutoffAt
	}
	if windowStart == 0 {
		windowStart = snap.WindowStart
	}
	if windowEnd == 0 {
		windowEnd = snap.WindowEnd
	}
	if cutoff == 0 {
		cutoff = snap.CutoffAt
	}
	loc := cfg.DisplayLocation()
	if loc == nil {
		loc = time.UTC
	}
	scheduled := time.Unix(in.Job.ScheduledAt, 0).In(loc)
	title := "每日新闻 · " + scheduled.Format("2006-01-02 15:04")
	if len(items) == 1 {
		title = "新闻采集状态 · " + scheduled.Format("2006-01-02 15:04")
	}
	if in.Job.ScheduleID != "" {
		scheduleID = in.Job.ScheduleID
	}
	d := storage.Digest{
		ID:                in.Job.ID,
		JobID:             in.Job.ID,
		SlotKey:           in.Job.JobKey,
		Revision:          1,
		ScheduleID:        scheduleID,
		ScheduledAt:       in.Job.ScheduledAt,
		WindowStart:       windowStart,
		WindowEnd:         windowEnd,
		CutoffAt:          cutoff,
		Title:             title,
		GenerationMode:    mode,
		ModelMetadataJSON: in.ModelMetadata,
		SourceHealth:      in.SourceHealth,
		QualityFlags:      flags,
		CreatedAt:         in.Now,
		OwnerYear:         in.Job.OwnerYear,
	}
	if d.ModelMetadataJSON == "" {
		d.ModelMetadataJSON = "{}"
	}
	d.Markdown = RenderMarkdown(cfg, d, items)
	sum := sha256.Sum256([]byte(d.Title + "\n" + d.Markdown))
	d.ContentHash = hex.EncodeToString(sum[:])
	if d.CreatedAt == 0 {
		d.CreatedAt = time.Now().Unix()
	}
	return d, items, flags
}

func selectItems(cfg *config.Config, snap editorial.CandidateSnapshot, events []llm.AnalysisEvent,
	previousRefs map[string]struct{}) ([]storage.DigestItem, int, bool) {
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].Importance != events[j].Importance {
			return events[i].Importance > events[j].Importance
		}
		return events[i].EventKey < events[j].EventKey
	})
	bySection := map[string][]llm.AnalysisEvent{}
	for _, ev := range events {
		s := ev.Section
		if s != editorial.SectionDomestic {
			s = editorial.SectionInternational
		}
		bySection[s] = append(bySection[s], ev)
	}
	snapArticles := snap.ArticleByRef()
	suppressRepeat := cfg.Editorial.RepeatPolicy == "suppress_unchanged_events"
	var out []storage.DigestItem
	repeatSuppressed := 0
	capRelaxed := false
	for _, section := range []string{editorial.SectionInternational, editorial.SectionDomestic} {
		evs := bySection[section]
		allowedPerGroup := 0
		if section == editorial.SectionInternational {
			allowedPerGroup = int(float64(cfg.Editorial.MaxPerSection) * cfg.Editorial.MaxPublisherShareInternational)
			if allowedPerGroup < 1 {
				allowedPerGroup = 1
			}
		}
		groupCounts := map[string]int{}
		var skippedByCap []llm.AnalysisEvent
		selected := 0
		for _, ev := range evs {
			if selected >= cfg.Editorial.MaxPerSection {
				break
			}
			if suppressRepeat && allRefsSuppressed(ev.ArticleRefs, previousRefs) {
				repeatSuppressed++
				continue
			}
			primary := primaryGroup(ev.ArticleRefs, snapArticles)
			if allowedPerGroup > 0 && groupCounts[primary] >= allowedPerGroup {
				skippedByCap = append(skippedByCap, ev)
				continue
			}
			it := buildItem(ev, snapArticles)
			groupCounts[primary]++
			selected++
			out = append(out, it)
		}
		if selected < cfg.Editorial.MinPerSection && len(skippedByCap) > 0 {
			for _, ev := range skippedByCap {
				if selected >= cfg.Editorial.TargetPerSection {
					break
				}
				it := buildItem(ev, snapArticles)
				it.QualityFlags = dedupe(append(it.QualityFlags, "publisher_cap_relaxed"))
				selected++
				out = append(out, it)
				capRelaxed = true
			}
		}
	}
	return out, repeatSuppressed, capRelaxed
}

func buildItem(ev llm.AnalysisEvent, snapArticles map[string]editorial.SnapshotArticle) storage.DigestItem {
	refs := make([]storage.ArticleRef, 0, len(ev.ArticleRefs))
	evidence := make([]storage.ArticleSnapshot, 0, len(ev.ArticleRefs))
	flags := append([]string{}, ev.QualityFlags...)
	groups := map[string]bool{}
	allNonChinese := true
	hasLate, hasUndated := false, false
	for _, ref := range ev.ArticleRefs {
		refs = append(refs, ref)
		a, ok := snapArticles[editorial.RefKey(ref.Year, ref.ID)]
		if !ok {
			continue
		}
		groups[a.PublisherGroup] = true
		if a.Language == "zh" {
			allNonChinese = false
		}
		if a.Late {
			hasLate = true
		}
		if a.Undated {
			hasUndated = true
		}
		evidence = append(evidence, storage.ArticleSnapshot{
			Year: a.Year, ID: a.ID, Title: a.Title, URL: a.URL,
			PublisherID: a.PublisherID, PublisherGroup: a.PublisherGroup,
			PublishedAt: a.PublishedAt, Summary: truncateRunes(a.Summary, 200),
		})
	}
	if hasLate {
		flags = append(flags, "late_arrival")
	}
	if hasUndated {
		flags = append(flags, "undated")
	}
	if allNonChinese && len(evidence) > 0 {
		flags = append(flags, "translation_unavailable")
	}
	coverage := editorial.PublisherCoverage(groups)
	if coverage == "multiple_sources" {
		flags = append(flags, "independence_unverified")
	}
	it := storage.DigestItem{
		Section:          ev.Section,
		EventKey:         ev.EventKey,
		TitleZh:          truncateRunes(ev.TitleZh, 200),
		Facts:            ev.Facts,
		Background:       truncateRunes(ev.Background, 250),
		Impact:           truncateRunes(ev.Impact, 250),
		Importance:       clamp(ev.Importance, 0, 100),
		Coverage:         coverage,
		ArticleRefs:      refs,
		EvidenceSnapshot: evidence,
		QualityFlags:     dedupe(flags),
	}
	for _, w := range ev.Warnings {
		if strings.Contains(strings.ToLower(w), "summary") && !contains(it.QualityFlags, "summary_only") {
			it.QualityFlags = append(it.QualityFlags, "summary_only")
		}
	}
	return it
}

func fallbackEvents(snap editorial.CandidateSnapshot) []llm.AnalysisEvent {
	var out []llm.AnalysisEvent
	for _, g := range snap.Groups {
		if len(g.Articles) == 0 {
			continue
		}
		rep := g.Articles[0]
		factText := rep.Summary
		if strings.TrimSpace(factText) == "" {
			factText = rep.Title
		}
		factText = truncateRunes(factText, 150)
		flags := []string{"rss_summary_only"}
		if rep.Late {
			flags = append(flags, "late_arrival")
		}
		if rep.Undated {
			flags = append(flags, "undated")
		}
		if rep.Language != "zh" {
			flags = append(flags, "translation_unavailable")
		}
		ev := llm.AnalysisEvent{
			EventKey:     g.Key,
			Section:      g.Section,
			TitleZh:      rep.Title,
			Facts:        []storage.Fact{{Text: factText, EvidenceRefs: []string{rep.ID}}},
			Importance:   clamp(int(rep.Score), 0, 100),
			Warnings:     []string{"summary_only"},
			QualityFlags: flags,
		}
		for _, a := range g.Articles {
			ev.ArticleRefs = append(ev.ArticleRefs, storage.ArticleRef{Year: a.Year, ID: a.ID})
		}
		out = append(out, ev)
	}
	return out
}

func buildStatusItem(snap editorial.CandidateSnapshot, items []storage.DigestItem,
	health []storage.SourceHealth, degraded []string, fallback bool, repeatSuppressed int, flags []string) storage.DigestItem {
	intl, dom := countSections(items)
	facts := []storage.Fact{}
	if len(items) == 0 {
		facts = append(facts, storage.Fact{Text: "本轮没有可用的精选新闻；这不代表当天没有新闻。"})
	} else {
		facts = append(facts, storage.Fact{Text: fmt.Sprintf("国际新闻 %d 条，中国国内新闻 %d 条。", intl, dom)})
	}
	if fallback {
		facts = append(facts, storage.Fact{Text: "AI 分析不可用，本简报使用 RSS 原始摘要降级生成。"})
	}
	if contains(flags, "single_group_domestic_coverage") {
		facts = append(facts, storage.Fact{Text: "国内新闻目前主要来自同一媒体集团，来源独立性有限。"})
	}
	if contains(flags, "independence_unverified") {
		facts = append(facts, storage.Fact{Text: "多家报道的转载/独立性关系未逐条核实。"})
	}
	if len(degraded) > 0 {
		ids := append([]string{}, degraded...)
		sort.Strings(ids)
		facts = append(facts, storage.Fact{Text: "本轮来源异常或不可用：" + strings.Join(ids, "、") + "。"})
	}
	if repeatSuppressed > 0 {
		facts = append(facts, storage.Fact{Text: fmt.Sprintf("已抑制 %d 条与上一时段完全重复的事件。", repeatSuppressed)})
	}
	if snap.LateCount > 0 {
		facts = append(facts, storage.Fact{Text: "候选包含生成前才发现的延迟收录或较旧报道，已单独标注。"})
	}
	if snap.UndatedCount > 0 {
		facts = append(facts, storage.Fact{Text: "部分候选未提供可靠报道时间，未当作过去 24 小时已确认报道。"})
	}
	facts = append(facts, storage.Fact{Text: "内容根据 RSS 摘要整理，可能不完整；请点击来源链接阅读原文。"})
	title := "来源覆盖与本轮状态"
	if len(items) == 0 {
		title = "采集异常状态"
	}
	return storage.DigestItem{
		Section:          editorial.SectionStatus,
		EventKey:         "status_summary",
		TitleZh:          title,
		Facts:            facts,
		Coverage:         "unknown",
		ArticleRefs:      []storage.ArticleRef{},
		EvidenceSnapshot: []storage.ArticleSnapshot{},
		QualityFlags:     flags,
	}
}

func RenderMarkdown(cfg *config.Config, d storage.Digest, items []storage.DigestItem) string {
	loc := cfg.DisplayLocation()
	if loc == nil {
		loc = time.UTC
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# %s\n\n", d.Title)
	fmt.Fprintf(&b, "> 窗口：%s — %s（%s）\n",
		time.Unix(d.WindowStart, 0).In(loc).Format("2006-01-02 15:04"),
		time.Unix(d.WindowEnd, 0).In(loc).Format("2006-01-02 15:04"),
		cfg.App.DisplayTimezone)
	fmt.Fprintf(&b, "> 根据 RSS 摘要整理，详见原文。生成模式：%s。\n\n", modeLabel(d.GenerationMode))
	writeSection(&b, editorial.SectionInternational, items)
	writeSection(&b, editorial.SectionDomestic, items)
	writeSection(&b, editorial.SectionStatus, items)
	if len(d.SourceHealth) > 0 {
		b.WriteString("## 来源状态\n\n")
		for _, h := range d.SourceHealth {
			status := h.Health
			if status == "" {
				status = "unknown"
			}
			line := "- " + h.SourceID + "：" + status
			if h.LastSuccessAt != nil {
				line += "，最近成功 " + time.Unix(*h.LastSuccessAt, 0).In(loc).Format("2006-01-02 15:04")
			}
			if h.LastErrorCode != "" {
				line += "，错误 " + h.LastErrorCode
			}
			b.WriteString(line + "\n")
		}
		b.WriteString("\n")
	}
	if len(d.QualityFlags) > 0 {
		b.WriteString("## 质量标记\n\n")
		b.WriteString("- " + strings.Join(d.QualityFlags, "、") + "\n\n")
	}
	fmt.Fprintf(&b, "简报 ID：%d/%s\n", d.OwnerYear, d.ID)
	return b.String()
}

func writeSection(b *strings.Builder, section string, items []storage.DigestItem) {
	var sectionItems []storage.DigestItem
	for _, it := range items {
		if it.Section == section {
			sectionItems = append(sectionItems, it)
		}
	}
	if len(sectionItems) == 0 {
		return
	}
	fmt.Fprintf(b, "## %s\n\n", editorial.SectionLabel(section))
	n := 0
	for _, it := range sectionItems {
		n++
		if section == editorial.SectionStatus {
			fmt.Fprintf(b, "- **%s**\n", it.TitleZh)
			for _, f := range it.Facts {
				fmt.Fprintf(b, "  - %s\n", f.Text)
			}
			if it.Background != "" {
				fmt.Fprintf(b, "  - 背景：%s\n", it.Background)
			}
			if it.Impact != "" {
				fmt.Fprintf(b, "  - 影响：%s\n", it.Impact)
			}
			b.WriteString("\n")
			continue
		}
		fmt.Fprintf(b, "%d. **%s**\n", n, it.TitleZh)
		for _, f := range it.Facts {
			fmt.Fprintf(b, "   %s\n", f.Text)
		}
		if it.Background != "" {
			fmt.Fprintf(b, "   背景：%s\n", it.Background)
		}
		if it.Impact != "" {
			fmt.Fprintf(b, "   影响：%s\n", it.Impact)
		}
		if len(it.EvidenceSnapshot) > 0 {
			links := make([]string, 0, len(it.EvidenceSnapshot))
			for _, a := range it.EvidenceSnapshot {
				if a.URL == "" {
					continue
				}
				links = append(links, fmt.Sprintf("[%s](%s)", a.PublisherID, a.URL))
			}
			if len(links) > 0 {
				fmt.Fprintf(b, "   来源：%s", strings.Join(links, "、"))
				switch it.Coverage {
				case "multiple_sources":
					b.WriteString("（多家报道，独立性未核实）")
				case "single_source":
					b.WriteString("（单一来源）")
				}
				b.WriteString("\n")
			}
		}
		if len(it.QualityFlags) > 0 {
			fmt.Fprintf(b, "   标记：%s\n", strings.Join(it.QualityFlags, "、"))
		}
		b.WriteString("\n")
	}
}

func modeLabel(mode string) string {
	switch mode {
	case "ai":
		return "AI 分析"
	case "ai_cache":
		return "AI 分析（缓存）"
	case "fallback":
		return "规则降级"
	default:
		return mode
	}
}

func domesticGroupCount(items []storage.DigestItem) int {
	groups := map[string]bool{}
	for _, it := range items {
		if it.Section != editorial.SectionDomestic {
			continue
		}
		for _, a := range it.EvidenceSnapshot {
			groups[a.PublisherGroup] = true
		}
	}
	return len(groups)
}

func countSections(items []storage.DigestItem) (int, int) {
	var intl, dom int
	for _, it := range items {
		switch it.Section {
		case editorial.SectionInternational:
			intl++
		case editorial.SectionDomestic:
			dom++
		}
	}
	return intl, dom
}

func primaryGroup(refs []storage.ArticleRef, snap map[string]editorial.SnapshotArticle) string {
	counts := map[string]int{}
	for _, r := range refs {
		if a, ok := snap[editorial.RefKey(r.Year, r.ID)]; ok {
			counts[a.PublisherGroup]++
		}
	}
	best := ""
	for g, n := range counts {
		if n > counts[best] || best == "" {
			best = g
		}
	}
	return best
}

func allRefsSuppressed(refs []storage.ArticleRef, prev map[string]struct{}) bool {
	if len(prev) == 0 || len(refs) == 0 {
		return false
	}
	for _, r := range refs {
		if _, ok := prev[r.ID]; !ok {
			return false
		}
	}
	return true
}

func degradedSources(health []storage.SourceHealth) []string {
	var out []string
	for _, h := range health {
		switch h.Health {
		case "ok", "unknown", "":
			continue
		}
		out = append(out, h.SourceID)
	}
	return out
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func truncateRunes(s string, max int) string {
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	r := []rune(s)
	return string(r[:max])
}

func dedupe(in []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, v := range in {
		if v == "" || seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	return out
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}
