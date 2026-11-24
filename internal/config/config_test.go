package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func baseConfig(t *testing.T) *Config {
	t.Helper()
	c := Defaults()
	c.App.DataDir = t.TempDir()
	c.LLM.Enabled = false
	c.Notification.Enabled = false
	c.Schedule.Enabled = false
	c.Sources = []SourceConfig{{
		ID: "src", URL: "https://example.com/rss", PublisherID: "p",
		PublisherGroup: "g", Language: "en", SectionHint: "international",
		PollInterval: Duration(10 * 60 * 1e9),
		Enabled:      true,
	}}
	return c
}

func TestStrictUnknownFields(t *testing.T) {
	c := baseConfig(t)
	c.App.DataDir = t.TempDir()
	path := filepath.Join(t.TempDir(), "config.yaml")
	yaml := `
app:
  data_dir: /tmp/x
  display_timezone: Asia/Shanghai
  annual_timezone: Asia/Shanghai
  log_level: info
http:
  listen: 127.0.0.1:8080
  admin_token_env: NEWSPULSE_ADMIN_TOKEN
storage:
  filename_pattern: news-%04d.sqlite
  journal_mode: WAL
  synchronous: FULL
  busy_timeout: 5s
  read_connections_per_shard: 2
  max_open_shards: 8
fetch:
  max_concurrent: 4
  max_per_host: 1
  request_timeout: 20s
  connect_timeout: 5s
  max_response_bytes: 4194304
  max_redirects: 5
  user_agent: NewsPulse/0.1
  jitter_ratio: 0.1
  stale_after: 24h
  future_tolerance: 15m
schedule:
  enabled: false
  id: daily-news
  timezone: Asia/Shanghai
  times: []
  lookback: 24h
  prepare_before: 5m
  generation_timeout: 5m
  catchup_max_age: 6h
  catchup_slots: 1
  notification_ttl: 6h
editorial:
  min_per_section: 5
  target_per_section: 6
  max_per_section: 8
  max_candidates: 120
  max_publisher_share_international: 0.5
  late_arrival_max_age: 48h
  allow_undated_on_cold_start: false
  include_full_article_fetch: false
  repeat_policy: suppress_unchanged_events
llm:
  enabled: false
  daily_max_requests: 20
  daily_max_reserved_tokens: 200000
notification:
  enabled: false
backup:
  enabled: false
unknown_field: true
sources:
  - id: src
    url: https://example.com/rss
    publisher_id: p
    publisher_group: g
    language: en
    section_hint: international
    poll_interval: 10m
    enabled: true
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "unknown_field") {
		t.Fatalf("expected unknown field error, got %v", err)
	}
}

func TestScheduleValidation(t *testing.T) {
	c := baseConfig(t)
	c.Schedule.Times = []string{"07:15", "07:15"}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("expected duplicate time error, got %v", err)
	}
	c.Schedule.Times = []string{"7:15"}
	if err := c.Validate(); err == nil {
		t.Fatalf("expected invalid HH:MM error")
	}
	c.Schedule.Times = []string{"07:15", "19:30"}
	if err := c.Validate(); err != nil {
		t.Fatalf("valid times rejected: %v", err)
	}
}

func TestNotificationAndLLMValidation(t *testing.T) {
	c := baseConfig(t)
	c.Notification.Enabled = true
	c.Notification.SplitLongMessages = true
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "split_long_messages") {
		t.Fatalf("expected split unsupported error, got %v", err)
	}

	c = baseConfig(t)
	c.LLM.Enabled = true
	c.LLM.DailyMaxRequests = 1
	c.LLM.MaxInputTokens = 1000
	c.LLM.MaxOutputTokens = 100
	c.LLM.MaxConcurrent = 5
	os.Setenv("LLM_BASE_URL", "https://api.example.com/v1")
	os.Setenv("LLM_API_KEY", "secret")
	os.Setenv("LLM_MODEL", "test-model")
	defer os.Unsetenv("LLM_BASE_URL")
	defer os.Unsetenv("LLM_API_KEY")
	defer os.Unsetenv("LLM_MODEL")
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "max_concurrent") {
		t.Fatalf("expected max_concurrent error, got %v", err)
	}
}

func TestFeishuWebhookValidation(t *testing.T) {
	good := "https://open.feishu.cn/open-apis/bot/v2/hook/abc-123"
	if err := ValidateFeishuWebhook(good); err != nil {
		t.Fatalf("good webhook rejected: %v", err)
	}
	for _, bad := range []string{
		"http://open.feishu.cn/open-apis/bot/v2/hook/abc",
		"https://evil.example.com/open-apis/bot/v2/hook/abc",
		"https://open.feishu.cn/other/abc",
		"https://open.feishu.cn/open-apis/bot/v2/hook/abc?x=1",
	} {
		if err := ValidateFeishuWebhook(bad); err == nil {
			t.Errorf("bad webhook accepted: %s", bad)
		}
	}
}
