package llm

import (
	"strings"
	"testing"

	"github.com/jxrory/newspulse/internal/editorial"
)

func allowedSnapshot() map[string]editorial.SnapshotArticle {
	a1 := editorial.SnapshotArticle{Year: 2026, ID: "a1", Title: "one", PublisherGroup: "g1", Language: "en"}
	a2 := editorial.SnapshotArticle{Year: 2026, ID: "a2", Title: "two", PublisherGroup: "g2", Language: "zh"}
	return map[string]editorial.SnapshotArticle{
		editorial.RefKey(2026, "a1"): a1,
		editorial.RefKey(2026, "a2"): a2,
	}
}

func TestValidateValidAndCoverage(t *testing.T) {
	raw := `{"schema_version":1,"events":[{"event_key":"e1","section":"international","article_refs":[{"year":2026,"id":"a1"},{"year":2026,"id":"a2"}],"title_zh":"标题","facts":[{"text":"事实","evidence_refs":["a1"]}],"background":"","impact":"","importance":80,"warnings":[]}]}`
	events, err := Validate(raw, allowedSnapshot())
	if err != nil {
		t.Fatalf("valid analysis rejected: %v", err)
	}
	if len(events) != 1 || events[0].Coverage != "multiple_sources" {
		t.Fatalf("coverage not computed server-side: %+v", events)
	}
}

func TestValidateRejectsBadRefsAndReuse(t *testing.T) {
	badRef := `{"schema_version":1,"events":[{"event_key":"e1","section":"international","article_refs":[{"year":2026,"id":"nope"}],"title_zh":"标题","facts":[{"text":"事实","evidence_refs":["x"]}],"importance":50}]}`
	_, err := Validate(badRef, allowedSnapshot())
	if err == nil || !strings.Contains(err.Error(), "article ref") {
		t.Fatalf("unknown ref accepted: %v", err)
	}

	reuse := `{"schema_version":1,"events":[
{"event_key":"e1","section":"international","article_refs":[{"year":2026,"id":"a1"}],"title_zh":"标题一","facts":[{"text":"事实","evidence_refs":["a1"]}],"importance":50},
{"event_key":"e2","section":"domestic","article_refs":[{"year":2026,"id":"a1"}],"title_zh":"标题二","facts":[{"text":"事实","evidence_refs":["a1"]}],"importance":50}]}`
	_, err = Validate(reuse, allowedSnapshot())
	if err == nil || !strings.Contains(err.Error(), "reused") {
		t.Fatalf("article reuse accepted: %v", err)
	}

	badEvidence := `{"schema_version":1,"events":[{"event_key":"e1","section":"international","article_refs":[{"year":2026,"id":"a1"}],"title_zh":"标题","facts":[{"text":"事实","evidence_refs":["a2"]}],"importance":50}]}`
	_, err = Validate(badEvidence, allowedSnapshot())
	if err == nil || !strings.Contains(err.Error(), "evidence_ref") {
		t.Fatalf("external evidence accepted: %v", err)
	}
}

func TestValidateEmpty(t *testing.T) {
	_, err := Validate(`{"schema_version":1,"events":[]}`, allowedSnapshot())
	if err == nil {
		t.Fatalf("empty events must trigger repair/fallback")
	}
}
