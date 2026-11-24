package llm

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jxrory/newspulse/internal/clock"
	"github.com/jxrory/newspulse/internal/config"
	"github.com/jxrory/newspulse/internal/editorial"
	"github.com/jxrory/newspulse/internal/storage"
	"log/slog"
)

func llmAnalyzerSetup(t *testing.T, handler http.HandlerFunc) (*Analyzer, *storage.ShardManager, *clock.Fake) {
	t.Helper()
	cfg := config.Defaults()
	cfg.App.DataDir = t.TempDir()
	cfg.LLM.Enabled = true
	cfg.LLM.BaseURLEnv = "TEST_LLM_BASE_URL"
	cfg.LLM.APIKeyEnv = "TEST_LLM_API_KEY"
	cfg.LLM.ModelEnv = "TEST_LLM_MODEL"
	cfg.LLM.ResponseMode = "json_object"
	cfg.LLM.DailyMaxRequests = 10
	cfg.LLM.DailyMaxReservedTokens = 1_000_000
	cfg.Notification.Enabled = false
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	t.Setenv("TEST_LLM_BASE_URL", srv.URL)
	t.Setenv("TEST_LLM_API_KEY", "test-key")
	t.Setenv("TEST_LLM_MODEL", "test-model")
	mgr, err := storage.NewShardManager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { mgr.Close() })
	clk := clock.NewFake(time.Date(2026, 11, 24, 7, 15, 0, 0, time.UTC))
	if _, err := mgr.OpenForWrite(clk.Now()); err != nil {
		t.Fatal(err)
	}
	return NewAnalyzer(cfg, mgr, clk, slog.New(slog.NewJSONHandler(loggingWriter{t}, nil))), mgr, clk
}

type loggingWriter struct{ t *testing.T }

func (w loggingWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimSpace(string(p)))
	return len(p), nil
}

func testSnapshot(id string, summary string) editorial.CandidateSnapshot {
	ts := int64(1000)
	a := editorial.SnapshotArticle{
		Year: 2026, ID: id, Title: "Headline", Summary: summary,
		URL: "https://example.org/" + id, PublisherID: "bbc", PublisherGroup: "bbc",
		Language: "en", Section: editorial.SectionInternational,
		PublishedAt: &ts, DateQuality: "parsed", FirstSeenAt: 1000, Score: 90,
	}
	return editorial.CandidateSnapshot{
		GeneratedAt: 1000, WindowStart: 0, WindowEnd: 1000, CutoffAt: 1000,
		Groups: []editorial.SnapshotGroup{{
			Key: "candidate_group_1", Section: editorial.SectionInternational,
			Articles: []editorial.SnapshotArticle{a},
		}},
	}
}

func chatBody(content string, promptTokens, completionTokens int) []byte {
	resp := map[string]interface{}{
		"choices": []interface{}{map[string]interface{}{
			"message":       map[string]interface{}{"content": content},
			"finish_reason": "stop",
		}},
		"usage": map[string]interface{}{
			"prompt_tokens": promptTokens, "completion_tokens": completionTokens,
			"total_tokens": promptTokens + completionTokens,
		},
	}
	b, _ := json.Marshal(resp)
	return b
}

func TestAnalyzeHTTPCacheAndBudget(t *testing.T) {
	var requests int32
	var sawAuth bool
	var sawJSONMode bool
	validContent := `{"schema_version":1,"events":[{"event_key":"e1","section":"international","article_refs":[{"year":2026,"id":"a1"}],"title_zh":"标题","facts":[{"text":"事实内容","evidence_refs":["a1"]}],"background":"","impact":"","importance":80,"warnings":[]}]}`
	handler := func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		if strings.HasPrefix(r.Header.Get("Authorization"), "Bearer test-key") {
			sawAuth = true
		}
		var body struct {
			ResponseFormat struct {
				Type string `json:"type"`
			} `json:"response_format"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.ResponseFormat.Type == "json_object" {
			sawJSONMode = true
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(chatBody(validContent, 50, 30))
	}
	an, mgr, clk := llmAnalyzerSetup(t, handler)
	ctx := context.Background()
	snap := testSnapshot("a1", "summary")
	res, err := an.Analyze(ctx, "job-1", 2026, snap)
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	if len(res.Events) != 1 || res.Events[0].Section != "international" {
		t.Fatalf("events=%+v", res.Events)
	}
	if !sawAuth || !sawJSONMode {
		t.Fatalf("auth=%v jsonmode=%v", sawAuth, sawJSONMode)
	}
	if atomic.LoadInt32(&requests) != 1 {
		t.Fatalf("requests=%d", requests)
	}

	res2, err := an.Analyze(ctx, "job-1", 2026, snap)
	if err != nil {
		t.Fatalf("cached analyze: %v", err)
	}
	if !res2.CacheHit || atomic.LoadInt32(&requests) != 1 {
		t.Fatalf("cache hit=%v requests=%d", res2.CacheHit, requests)
	}

	// A different input bypasses cache, but the configured daily request
	// budget now blocks the paid call.
	an.cfg.LLM.DailyMaxRequests = 1
	_, err = an.Analyze(ctx, "job-2", 2026, testSnapshot("a2", "other summary"))
	var budget *BudgetError
	if !errors.As(err, &budget) {
		t.Fatalf("expected budget error, got %v", err)
	}

	shard, err := mgr.Open(2026)
	if err != nil {
		t.Fatal(err)
	}
	var calls int
	if err := shard.R().QueryRow(`SELECT count(*) FROM llm_calls`).Scan(&calls); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("llm_calls=%d want 1 (cache/budget must not reserve)", calls)
	}
	var cacheCount int
	if err := shard.R().QueryRow(`SELECT count(*) FROM analysis_cache`).Scan(&cacheCount); err != nil {
		t.Fatal(err)
	}
	if cacheCount != 1 {
		t.Fatalf("analysis_cache=%d", cacheCount)
	}
	_ = clk
}

func TestAnalyzeFallsBackToSecondaryProvider(t *testing.T) {
	oldDelay := llmRetryBaseDelay
	llmRetryBaseDelay = time.Millisecond
	defer func() { llmRetryBaseDelay = oldDelay }()

	validContent := `{"schema_version":1,"events":[{"event_key":"e1","section":"international","article_refs":[{"year":2026,"id":"a1"}],"title_zh":"标题","facts":[{"text":"事实内容","evidence_refs":["a1"]}],"background":"","impact":"","importance":80,"warnings":[]}]}`
	var primaryRequests, fallbackRequests int32
	primary := func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&primaryRequests, 1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`[{"error":{"code":503,"message":"high demand","status":"UNAVAILABLE"}}]`))
	}
	fallback := func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&fallbackRequests, 1)
		if r.Header.Get("Authorization") != "Bearer fallback-key" {
			t.Errorf("fallback auth=%q", r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(chatBody(validContent, 50, 30))
	}
	an, mgr, _ := llmAnalyzerSetup(t, primary)
	fbSrv := httptest.NewServer(http.HandlerFunc(fallback))
	t.Cleanup(fbSrv.Close)
	t.Setenv("TEST_FB_BASE_URL", fbSrv.URL)
	t.Setenv("TEST_FB_API_KEY", "fallback-key")
	t.Setenv("TEST_FB_MODEL", "fallback-model")
	an.cfg.LLM.FallbackProviders = []config.LLMCredential{{
		Name: "secondary", Adapter: "chat_completions",
		BaseURLEnv: "TEST_FB_BASE_URL", APIKeyEnv: "TEST_FB_API_KEY",
		ModelEnv: "TEST_FB_MODEL", ResponseMode: "json_object",
	}}

	res, err := an.Analyze(context.Background(), "job-fallback", 2026, testSnapshot("a1", "summary"))
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	if len(res.Events) != 1 || res.Mode != "ai" || res.Model != "fallback-model" {
		t.Fatalf("result=%+v", res)
	}
	if got := atomic.LoadInt32(&primaryRequests); got != int32(llmTransientAttempts) {
		t.Fatalf("primary requests=%d want %d", got, llmTransientAttempts)
	}
	if got := atomic.LoadInt32(&fallbackRequests); got != 1 {
		t.Fatalf("fallback requests=%d want 1", got)
	}
	shard, err := mgr.Open(2026)
	if err != nil {
		t.Fatal(err)
	}
	var succeeded int
	if err := shard.R().QueryRow(`SELECT count(*) FROM llm_calls WHERE status='succeeded' AND model='fallback-model'`).Scan(&succeeded); err != nil {
		t.Fatal(err)
	}
	if succeeded != 1 {
		t.Fatalf("succeeded fallback calls=%d", succeeded)
	}
}

func TestAnalyzeFallbackSkipsTransientRetryOnClientError(t *testing.T) {
	validContent := `{"schema_version":1,"events":[{"event_key":"e1","section":"international","article_refs":[{"year":2026,"id":"a1"}],"title_zh":"标题","facts":[{"text":"事实内容","evidence_refs":["a1"]}],"background":"","impact":"","importance":80,"warnings":[]}]}`
	var primaryRequests int32
	primary := func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&primaryRequests, 1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"message":"bad request"}}`))
	}
	fb := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(chatBody(validContent, 50, 30))
	}
	an, _, _ := llmAnalyzerSetup(t, primary)
	fbSrv := httptest.NewServer(http.HandlerFunc(fb))
	t.Cleanup(fbSrv.Close)
	t.Setenv("TEST_FB_BASE_URL", fbSrv.URL)
	t.Setenv("TEST_FB_API_KEY", "fallback-key")
	t.Setenv("TEST_FB_MODEL", "fallback-model")
	an.cfg.LLM.FallbackProviders = []config.LLMCredential{{
		Name: "secondary", Adapter: "chat_completions",
		BaseURLEnv: "TEST_FB_BASE_URL", APIKeyEnv: "TEST_FB_API_KEY",
		ModelEnv: "TEST_FB_MODEL", ResponseMode: "json_object",
	}}
	res, err := an.Analyze(context.Background(), "job-fallback-400", 2026, testSnapshot("a1", "summary"))
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	if res.Model != "fallback-model" {
		t.Fatalf("model=%q", res.Model)
	}
	if got := atomic.LoadInt32(&primaryRequests); got != 1 {
		t.Fatalf("primary requests=%d want 1", got)
	}
}

func TestAnalyzeRetriesTransientAPIErrors(t *testing.T) {
	oldDelay := llmRetryBaseDelay
	llmRetryBaseDelay = 10 * time.Millisecond
	defer func() { llmRetryBaseDelay = oldDelay }()

	var requests int32
	validContent := `{"schema_version":1,"events":[{"event_key":"e1","section":"international","article_refs":[{"year":2026,"id":"a1"}],"title_zh":"标题","facts":[{"text":"事实内容","evidence_refs":["a1"]}],"background":"","impact":"","importance":80,"warnings":[]}]}`
	handler := func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&requests, 1)
		w.Header().Set("Content-Type", "application/json")
		if n <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`[{"error":{"code":503,"message":"high demand","status":"UNAVAILABLE"}}]`))
			return
		}
		_, _ = w.Write(chatBody(validContent, 50, 30))
	}
	an, mgr, _ := llmAnalyzerSetup(t, handler)
	res, err := an.Analyze(context.Background(), "job-retry", 2026, testSnapshot("a1", "summary"))
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	if len(res.Events) != 1 || res.Mode != "ai" {
		t.Fatalf("result=%+v", res)
	}
	if got := atomic.LoadInt32(&requests); got != 3 {
		t.Fatalf("requests=%d want 3", got)
	}
	shard, err := mgr.Open(2026)
	if err != nil {
		t.Fatal(err)
	}
	var failed, succeeded int
	if err := shard.R().QueryRow(`SELECT count(*) FROM llm_calls WHERE status='failed'`).Scan(&failed); err != nil {
		t.Fatal(err)
	}
	if err := shard.R().QueryRow(`SELECT count(*) FROM llm_calls WHERE status='succeeded'`).Scan(&succeeded); err != nil {
		t.Fatal(err)
	}
	if failed != 2 || succeeded != 1 {
		t.Fatalf("failed=%d succeeded=%d", failed, succeeded)
	}
}

func TestRetryableLLMErrorOnlyTransient(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{&APIError{Code: "http_503", HTTPStatus: 503}, true},
		{&APIError{Code: "http_429", HTTPStatus: 429}, true},
		{&APIError{Code: "timeout"}, true},
		{&APIError{Code: "http_400", HTTPStatus: 400}, false},
		{&APIError{Code: "empty_output"}, false},
		{errors.New("plain"), false},
	}
	for _, c := range cases {
		if got := retryableLLMError(c.err); got != c.want {
			t.Fatalf("retryableLLMError(%v)=%v want %v", c.err, got, c.want)
		}
	}
}
