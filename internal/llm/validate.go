package llm

import (
	"encoding/json"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/jxrory/newspulse/internal/editorial"
	"github.com/jxrory/newspulse/internal/storage"
)

type analysisEnvelope struct {
	SchemaVersion int             `json:"schema_version"`
	Events        []AnalysisEvent `json:"events"`
}

// Validate enforces the output contract against the exact candidate references
// sent to the model. Model-reported coverage is discarded; the server computes
// source independence from metadata.
func Validate(raw string, allowed map[string]editorial.SnapshotArticle) ([]AnalysisEvent, error) {
	raw = stripCodeFences(raw)
	var env analysisEnvelope
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		return nil, &ValidationError{Problems: []string{"invalid JSON: " + err.Error()}}
	}
	var problems []string
	if env.SchemaVersion != SchemaVersion {
		problems = append(problems, "schema_version must be 1")
	}
	if len(env.Events) == 0 {
		problems = append(problems, "events must not be empty")
	}
	if len(env.Events) > 60 {
		problems = append(problems, "too many events")
	}
	usedArticles := map[string]bool{}
	seenKeys := map[string]bool{}
	for i := range env.Events {
		ev := &env.Events[i]
		prefix := "events[" + itoa64(int64(i)) + "]: "
		if ev.EventKey == "" {
			problems = append(problems, prefix+"event_key is required")
		} else if seenKeys[ev.EventKey] {
			problems = append(problems, prefix+"duplicate event_key")
		}
		seenKeys[ev.EventKey] = true
		if ev.Section != "international" && ev.Section != "domestic" {
			problems = append(problems, prefix+"invalid section")
		}
		ev.TitleZh = strings.TrimSpace(ev.TitleZh)
		if ev.TitleZh == "" || utf8.RuneCountInString(ev.TitleZh) > 160 {
			problems = append(problems, prefix+"title_zh must be 1-160 characters")
		}
		if len(ev.ArticleRefs) == 0 {
			problems = append(problems, prefix+"article_refs must not be empty")
		}
		refIDs := map[string]bool{}
		for _, ref := range ev.ArticleRefs {
			if !allowedArticle(allowed, ref.Year, ref.ID) {
				problems = append(problems, prefix+"article ref not in input snapshot")
				continue
			}
			if refIDs[ref.ID] {
				problems = append(problems, prefix+"duplicate article ref")
			}
			refIDs[ref.ID] = true
		}
		if len(ev.Facts) == 0 {
			problems = append(problems, prefix+"facts must not be empty")
		}
		totalRunes := 0
		for j := range ev.Facts {
			f := &ev.Facts[j]
			f.Text = strings.TrimSpace(f.Text)
			n := utf8.RuneCountInString(f.Text)
			totalRunes += n
			if n == 0 || n > 250 {
				problems = append(problems, prefix+"fact text must be 1-250 characters")
			}
			if len(f.EvidenceRefs) == 0 {
				problems = append(problems, prefix+"fact evidence_refs must not be empty")
			}
			for _, er := range f.EvidenceRefs {
				if !refIDs[er] {
					problems = append(problems, prefix+"fact evidence_ref not in this event's article_refs")
					break
				}
			}
		}
		if totalRunes > 450 {
			problems = append(problems, prefix+"facts are too long")
		}
		ev.Background = strings.TrimSpace(ev.Background)
		ev.Impact = strings.TrimSpace(ev.Impact)
		if utf8.RuneCountInString(ev.Background) > 250 {
			problems = append(problems, prefix+"background exceeds 250 characters")
		}
		if utf8.RuneCountInString(ev.Impact) > 250 {
			problems = append(problems, prefix+"impact exceeds 250 characters")
		}
		if ev.Importance < 0 || ev.Importance > 100 {
			problems = append(problems, prefix+"importance must be 0-100")
		}
		if len(ev.Warnings) > 5 {
			problems = append(problems, prefix+"too many warnings")
		}
		for _, ref := range ev.ArticleRefs {
			if allowedArticle(allowed, ref.Year, ref.ID) {
				if usedArticles[ref.ID] {
					problems = append(problems, prefix+"article reused across events without justification")
					break
				}
			}
		}
		for _, ref := range ev.ArticleRefs {
			usedArticles[ref.ID] = true
		}
		// Server-side flags replace model claims.
		groups := map[string]bool{}
		for _, ref := range ev.ArticleRefs {
			a, ok := findAllowed(allowed, ref.Year, ref.ID)
			if ok {
				groups[a.PublisherGroup] = true
			}
		}
		ev.Coverage = editorial.PublisherCoverage(groups)
		ev.QualityFlags = eventFlags(ev, groups)
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return nil, &ValidationError{Problems: problems}
	}
	return env.Events, nil
}

func eventFlags(ev *AnalysisEvent, groups map[string]bool) []string {
	flags := []string{}
	if len(ev.ArticleRefs) >= 2 && len(groups) == 1 {
		flags = append(flags, "single_group_coverage")
	}
	hasSummaryOnly := false
	for _, w := range ev.Warnings {
		if strings.Contains(strings.ToLower(w), "summary") {
			hasSummaryOnly = true
		}
	}
	if hasSummaryOnly {
		flags = append(flags, "summary_only")
	}
	return flags
}

func allowedArticle(allowed map[string]editorial.SnapshotArticle, year int, id string) bool {
	_, ok := allowed[editorial.RefKey(year, id)]
	return ok
}

func findAllowed(allowed map[string]editorial.SnapshotArticle, year int, id string) (editorial.SnapshotArticle, bool) {
	a, ok := allowed[editorial.RefKey(year, id)]
	return a, ok
}

func stripCodeFences(raw string) string {
	s := strings.TrimSpace(raw)
	if strings.HasPrefix(s, "```") {
		s = strings.TrimPrefix(s, "```")
		s = strings.TrimPrefix(s, "json")
		if i := strings.LastIndex(s, "```"); i >= 0 {
			s = s[:i]
		}
	}
	return strings.TrimSpace(s)
}

var _ = storage.ArticleRef{}
