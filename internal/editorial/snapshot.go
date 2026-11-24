package editorial

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/jxrory/newspulse/internal/config"
	"github.com/jxrory/newspulse/internal/normalize"
	"github.com/jxrory/newspulse/internal/storage"
)

type SnapshotArticle struct {
	Year           int      `json:"year"`
	ID             string   `json:"id"`
	Title          string   `json:"title"`
	Summary        string   `json:"summary"`
	URL            string   `json:"url"`
	PublisherID    string   `json:"publisher_id"`
	PublisherGroup string   `json:"publisher_group"`
	Language       string   `json:"language"`
	Section        string   `json:"section"`
	PublishedAt    *int64   `json:"published_at,omitempty"`
	PublishedAtRaw string   `json:"published_at_raw,omitempty"`
	DateQuality    string   `json:"date_quality"`
	FirstSeenAt    int64    `json:"first_seen_at"`
	QualityFlags   []string `json:"quality_flags"`
	Late           bool     `json:"late,omitempty"`
	Undated        bool     `json:"undated,omitempty"`
	Score          float64  `json:"score"`
}

type SnapshotGroup struct {
	Key      string            `json:"key"`
	Section  string            `json:"section"`
	Articles []SnapshotArticle `json:"articles"`
}

type CandidateSnapshot struct {
	GeneratedAt  int64           `json:"generated_at"`
	WindowStart  int64           `json:"window_start"`
	WindowEnd    int64           `json:"window_end"`
	CutoffAt     int64           `json:"cutoff_at"`
	Groups       []SnapshotGroup `json:"groups"`
	ArticleCount int             `json:"article_count"`
	LateCount    int             `json:"late_count"`
	UndatedCount int             `json:"undated_count"`
	Notes        []string        `json:"notes"`
}

func (s CandidateSnapshot) ArticleByRef() map[string]SnapshotArticle {
	out := map[string]SnapshotArticle{}
	for _, g := range s.Groups {
		for _, a := range g.Articles {
			out[refKey(a.Year, a.ID)] = a
		}
	}
	return out
}

func (s CandidateSnapshot) RefMap() map[string]storage.ArticleRef {
	out := map[string]storage.ArticleRef{}
	for _, g := range s.Groups {
		for _, a := range g.Articles {
			out[refKey(a.Year, a.ID)] = storage.ArticleRef{Year: a.Year, ID: a.ID}
		}
	}
	return out
}

func refKey(year int, id string) string {
	return fmt.Sprintf("%d/%s", year, id)
}

// RefKey exposes the stable snapshot reference key used by validators.
func RefKey(year int, id string) string { return refKey(year, id) }

type BuildOptions struct {
	WindowStart   int64
	WindowEnd     int64
	CutoffAt      int64
	MaxCandidates int
	LateMaxAge    time.Duration
	Lookback      time.Duration
	AllowUndated  bool
}

// BuildSnapshot queries all annual shards, ranks candidates deterministically,
// and groups near-duplicate same-language reports. Cross-language event
// confirmation is left to the model, but each group is only a hint.
func BuildSnapshot(ctx context.Context, mgr *storage.ShardManager, cfg *config.Config, opts BuildOptions) (CandidateSnapshot, error) {
	snap := CandidateSnapshot{
		GeneratedAt: time.Now().Unix(),
		WindowStart: opts.WindowStart,
		WindowEnd:   opts.WindowEnd,
		CutoffAt:    opts.CutoffAt,
	}
	if opts.MaxCandidates <= 0 {
		opts.MaxCandidates = 120
	}
	if opts.Lookback <= 0 {
		opts.Lookback = 24 * time.Hour
	}
	if snap.GeneratedAt > opts.CutoffAt {
		snap.GeneratedAt = opts.CutoffAt
	}
	perSection := opts.MaxCandidates / 2
	if perSection < 10 {
		perSection = 10
	}
	var raw []candidate
	for _, section := range []string{SectionInternational, SectionDomestic} {
		f := storage.ArticleFilter{
			OnlyPublished:   true,
			WindowStart:     ptr(opts.WindowStart),
			WindowEnd:       ptr(opts.WindowEnd),
			FirstSeenBefore: ptr(opts.CutoffAt),
			Section:         section,
			Limit:           perSection,
		}
		list, err := mgr.QueryArticlesAcrossShards(ctx, f)
		if err != nil {
			return snap, err
		}
		for _, a := range list {
			c := makeCandidate(a, opts.WindowStart, opts.WindowEnd, section, false, false)
			raw = append(raw, c)
		}
	}
	lateStart := opts.WindowStart - int64(opts.LateMaxAge.Seconds())
	if lateStart < opts.WindowStart && opts.LateMaxAge > 0 {
		f := storage.ArticleFilter{
			OnlyPublished:   true,
			WindowStart:     ptr(lateStart),
			WindowEnd:       ptr(opts.WindowStart),
			FirstSeenAfter:  ptr(opts.WindowStart),
			FirstSeenBefore: ptr(opts.CutoffAt),
			Limit:           opts.MaxCandidates / 4,
		}
		list, err := mgr.QueryArticlesAcrossShards(ctx, f)
		if err != nil {
			return snap, err
		}
		for _, a := range list {
			raw = append(raw, makeCandidate(a, opts.WindowStart, opts.WindowEnd, a.Section, true, false))
			snap.LateCount++
		}
	}
	if opts.AllowUndated {
		f := storage.ArticleFilter{
			OnlyUndated:     true,
			FirstSeenBefore: ptr(opts.CutoffAt),
			FirstSeenAfter:  ptr(opts.WindowStart - int64(opts.Lookback.Seconds())),
			Limit:           10,
		}
		list, err := mgr.QueryArticlesAcrossShards(ctx, f)
		if err != nil {
			return snap, err
		}
		for _, a := range list {
			raw = append(raw, makeCandidate(a, opts.WindowStart, opts.WindowEnd, a.Section, false, true))
			snap.UndatedCount++
		}
	} else {
		snap.Notes = append(snap.Notes, "undated_candidates_excluded")
	}

	raw = excludeFuture(raw)
	sort.SliceStable(raw, func(i, j int) bool {
		if raw[i].score != raw[j].score {
			return raw[i].score > raw[j].score
		}
		return raw[i].a.ID > raw[j].a.ID
	})
	groups := groupCandidates(raw, opts.WindowStart)
	snap.Groups = groups
	for _, g := range groups {
		snap.ArticleCount += len(g.Articles)
	}
	return snap, nil
}

type candidate struct {
	a        SnapshotArticle
	score    float64
	titleKey string
	section  string
	late     bool
	undated  bool
}

func makeCandidate(aw storage.ArticleWithYear, windowStart, windowEnd int64, section string, late, undated bool) candidate {
	if section == "" || section == "unknown" {
		section = aw.Section
	}
	if section != SectionDomestic {
		section = SectionInternational
	}
	sa := SnapshotArticle{
		Year: aw.OwnerYear, ID: aw.ID, Title: aw.Title, Summary: aw.SummaryText,
		URL: aw.CanonicalURL, PublisherID: aw.PublisherID, PublisherGroup: aw.PublisherGroup,
		Language: aw.Language, Section: section, PublishedAt: aw.PublishedAt,
		PublishedAtRaw: aw.PublishedAtRaw, DateQuality: aw.DateQuality,
		FirstSeenAt: aw.FirstSeenAt, QualityFlags: aw.QualityFlags,
		Late: late, Undated: undated,
	}
	score := 0.0
	if aw.PublishedAt != nil {
		span := float64(windowEnd - windowStart)
		if span <= 0 {
			span = 86400
		}
		age := float64(windowEnd - *aw.PublishedAt)
		score = 100 * (1 - age/span)
		if score < 0 {
			score = 0
		}
		if score > 100 {
			score = 100
		}
	} else {
		score = 20
	}
	if late {
		score -= 25
	}
	if undated {
		score -= 60
	}
	sa.Score = score
	return candidate{a: sa, score: score, titleKey: normalize.TitleComparisonKey(aw.Title), section: section, late: late, undated: undated}
}

func excludeFuture(cs []candidate) []candidate {
	out := cs[:0]
	for _, c := range cs {
		if c.a.DateQuality == storage.DateFormatFuture {
			continue
		}
		hasFuture := false
		for _, f := range c.a.QualityFlags {
			if f == "future_date" {
				hasFuture = true
				break
			}
		}
		if hasFuture {
			continue
		}
		out = append(out, c)
	}
	return out
}

// groupCandidates uses a non-transitive greedy rule: a candidate joins only
// the first representative it matches, preventing A≈B≈C chains from merging
// A and C.
func groupCandidates(cs []candidate, windowStart int64) []SnapshotGroup {
	var groups []SnapshotGroup
	var reps []candidate
	const threshold = 0.65
	for _, c := range cs {
		placed := false
		for i := range reps {
			r := reps[i]
			if r.section != c.section || r.a.Language != c.a.Language || c.undated || r.undated {
				continue
			}
			if r.a.PublishedAt != nil && c.a.PublishedAt != nil {
				d := *r.a.PublishedAt - *c.a.PublishedAt
				if d < 0 {
					d = -d
				}
				if time.Duration(d)*time.Second > 48*time.Hour {
					continue
				}
			}
			if normalize.BigramJaccard(r.titleKey, c.titleKey) >= threshold {
				groups[i].Articles = append(groups[i].Articles, c.a)
				placed = true
				break
			}
		}
		if !placed {
			g := SnapshotGroup{Key: groupKey(len(groups) + 1), Section: c.section, Articles: []SnapshotArticle{c.a}}
			groups = append(groups, g)
			reps = append(reps, c)
		}
	}
	return groups
}

func groupKey(n int) string {
	return "candidate_group_" + itoa(n)
}

func itoa(n int) string { return fmt.Sprintf("%d", n) }

func ptr(v int64) *int64 { return &v }

// PublisherCoverage is computed from source metadata, never from model
// self-reporting.
func PublisherCoverage(groups map[string]bool) string {
	switch {
	case len(groups) >= 2:
		return "multiple_sources"
	case len(groups) == 1:
		return "single_source"
	default:
		return "unknown"
	}
}
