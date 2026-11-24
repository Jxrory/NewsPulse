package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"
	_ "time/tzdata"

	"gopkg.in/yaml.v3"
)

type Duration time.Duration

func (d Duration) D() time.Duration { return time.Duration(d) }

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	var raw string
	if err := node.Decode(&raw); err != nil {
		return fmt.Errorf("duration must be a string like \"20s\": %w", err)
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", raw, err)
	}
	*d = Duration(v)
	return nil
}

func (d Duration) MarshalYAML() (interface{}, error) {
	return time.Duration(d).String(), nil
}

type AppConfig struct {
	DataDir         string `yaml:"data_dir"`
	DisplayTimezone string `yaml:"display_timezone"`
	AnnualTimezone  string `yaml:"annual_timezone"`
	LogLevel        string `yaml:"log_level"`
	AdminTokenEnv   string `yaml:"admin_token_env"`
}

type HTTPConfig struct {
	Listen        string `yaml:"listen"`
	AdminTokenEnv string `yaml:"admin_token_env"`
}

type StorageConfig struct {
	FilenamePattern         string   `yaml:"filename_pattern"`
	JournalMode             string   `yaml:"journal_mode"`
	Synchronous             string   `yaml:"synchronous"`
	BusyTimeout             Duration `yaml:"busy_timeout"`
	ReadConnectionsPerShard int      `yaml:"read_connections_per_shard"`
	MaxOpenShards           int      `yaml:"max_open_shards"`
}

type FetchConfig struct {
	MaxConcurrent    int      `yaml:"max_concurrent"`
	MaxPerHost       int      `yaml:"max_per_host"`
	RequestTimeout   Duration `yaml:"request_timeout"`
	ConnectTimeout   Duration `yaml:"connect_timeout"`
	MaxResponseBytes int64    `yaml:"max_response_bytes"`
	MaxRedirects     int      `yaml:"max_redirects"`
	UserAgent        string   `yaml:"user_agent"`
	JitterRatio      float64  `yaml:"jitter_ratio"`
	StaleAfter       Duration `yaml:"stale_after"`
	FutureTolerance  Duration `yaml:"future_tolerance"`
}

type ScheduleConfig struct {
	Enabled           bool     `yaml:"enabled"`
	ID                string   `yaml:"id"`
	Timezone          string   `yaml:"timezone"`
	Times             []string `yaml:"times"`
	Lookback          Duration `yaml:"lookback"`
	PrepareBefore     Duration `yaml:"prepare_before"`
	GenerationTimeout Duration `yaml:"generation_timeout"`
	CatchupMaxAge     Duration `yaml:"catchup_max_age"`
	CatchupSlots      int      `yaml:"catchup_slots"`
	NotificationTTL   Duration `yaml:"notification_ttl"`
}

type EditorialConfig struct {
	MinPerSection                  int      `yaml:"min_per_section"`
	TargetPerSection               int      `yaml:"target_per_section"`
	MaxPerSection                  int      `yaml:"max_per_section"`
	MaxCandidates                  int      `yaml:"max_candidates"`
	MaxPublisherShareInternational float64  `yaml:"max_publisher_share_international"`
	LateArrivalMaxAge              Duration `yaml:"late_arrival_max_age"`
	AllowUndatedOnColdStart        bool     `yaml:"allow_undated_on_cold_start"`
	IncludeFullArticleFetch        bool     `yaml:"include_full_article_fetch"`
	RepeatPolicy                   string   `yaml:"repeat_policy"`
}

type LLMCredential struct {
	Name         string `yaml:"name"`
	Adapter      string `yaml:"adapter"`
	BaseURLEnv   string `yaml:"base_url_env"`
	APIKeyEnv    string `yaml:"api_key_env"`
	ModelEnv     string `yaml:"model_env"`
	ResponseMode string `yaml:"response_mode"`
}

type LLMConfig struct {
	Enabled                  bool            `yaml:"enabled"`
	Adapter                  string          `yaml:"adapter"`
	BaseURLEnv               string          `yaml:"base_url_env"`
	APIKeyEnv                string          `yaml:"api_key_env"`
	ModelEnv                 string          `yaml:"model_env"`
	ResponseMode             string          `yaml:"response_mode"`
	RequestTimeout           Duration        `yaml:"request_timeout"`
	MaxConcurrent            int             `yaml:"max_concurrent"`
	MaxInputTokens           int             `yaml:"max_input_tokens"`
	MaxOutputTokens          int             `yaml:"max_output_tokens"`
	MaxRepairAttempts        int             `yaml:"max_repair_attempts"`
	PromptVersion            string          `yaml:"prompt_version"`
	DailyMaxRequests         int             `yaml:"daily_max_requests"`
	DailyMaxReservedTokens   int             `yaml:"daily_max_reserved_tokens"`
	Fallback                 string          `yaml:"fallback"`
	InputPriceMicrosPerMTok  int64           `yaml:"input_price_micros_per_mtok"`
	OutputPriceMicrosPerMTok int64           `yaml:"output_price_micros_per_mtok"`
	Currency                 string          `yaml:"currency"`
	FallbackProviders        []LLMCredential `yaml:"fallback_providers"`
}

type NotificationConfig struct {
	Enabled            bool     `yaml:"enabled"`
	Provider           string   `yaml:"provider"`
	RecipientAlias     string   `yaml:"recipient_alias"`
	WebhookURLEnv      string   `yaml:"webhook_url_env"`
	SignSecretEnv      string   `yaml:"sign_secret_env"`
	RequireSignature   bool     `yaml:"require_signature"`
	MessageType        string   `yaml:"message_type"`
	RequiredKeyword    string   `yaml:"required_keyword"`
	Timeout            Duration `yaml:"timeout"`
	FollowRedirects    bool     `yaml:"follow_redirects"`
	MaxPayloadBytes    int      `yaml:"max_payload_bytes"`
	MinSendInterval    Duration `yaml:"min_send_interval"`
	MaxSendAttempts    int      `yaml:"max_send_attempts"`
	DailyRequestBudget int64    `yaml:"daily_request_budget"`
	RetryUnknown       bool     `yaml:"retry_unknown"`
	SplitLongMessages  bool     `yaml:"split_long_messages"`
}

type BackupConfig struct {
	Enabled            bool     `yaml:"enabled"`
	Interval           Duration `yaml:"interval"`
	PauseWritesTimeout Duration `yaml:"pause_writes_timeout"`
	KeepDaily          int      `yaml:"keep_daily"`
	KeepWeekly         int      `yaml:"keep_weekly"`
	KeepMonthly        int      `yaml:"keep_monthly"`
	RemoteCopyEnabled  bool     `yaml:"remote_copy_enabled"`
}

type SourceConfig struct {
	ID             string   `yaml:"id"`
	URL            string   `yaml:"url"`
	PublisherID    string   `yaml:"publisher_id"`
	PublisherGroup string   `yaml:"publisher_group"`
	Language       string   `yaml:"language"`
	SectionHint    string   `yaml:"section_hint"`
	TimezoneHint   string   `yaml:"timezone_hint"`
	PollInterval   Duration `yaml:"poll_interval"`
	Enabled        bool     `yaml:"enabled"`
	SourceType     string   `yaml:"source_type"`
}

type Config struct {
	App          AppConfig          `yaml:"app"`
	HTTP         HTTPConfig         `yaml:"http"`
	Storage      StorageConfig      `yaml:"storage"`
	Fetch        FetchConfig        `yaml:"fetch"`
	Schedule     ScheduleConfig     `yaml:"schedule"`
	Editorial    EditorialConfig    `yaml:"editorial"`
	LLM          LLMConfig          `yaml:"llm"`
	Notification NotificationConfig `yaml:"notification"`
	Backup       BackupConfig       `yaml:"backup"`
	Sources      []SourceConfig     `yaml:"sources"`
}

func Defaults() *Config {
	return &Config{
		App: AppConfig{
			DataDir:         "/var/lib/newspulse",
			DisplayTimezone: "Asia/Shanghai",
			AnnualTimezone:  "Asia/Shanghai",
			LogLevel:        "info",
		},
		HTTP: HTTPConfig{
			Listen:        "127.0.0.1:8080",
			AdminTokenEnv: "NEWSPULSE_ADMIN_TOKEN",
		},
		Storage: StorageConfig{
			FilenamePattern:         "news-%04d.sqlite",
			JournalMode:             "WAL",
			Synchronous:             "FULL",
			BusyTimeout:             Duration(5 * time.Second),
			ReadConnectionsPerShard: 2,
			MaxOpenShards:           8,
		},
		Fetch: FetchConfig{
			MaxConcurrent:    4,
			MaxPerHost:       1,
			RequestTimeout:   Duration(20 * time.Second),
			ConnectTimeout:   Duration(5 * time.Second),
			MaxResponseBytes: 4 << 20,
			MaxRedirects:     5,
			UserAgent:        "NewsPulse/0.1",
			JitterRatio:      0.1,
			StaleAfter:       Duration(24 * time.Hour),
			FutureTolerance:  Duration(15 * time.Minute),
		},
		Schedule: ScheduleConfig{
			Enabled:           false,
			ID:                "daily-news",
			Timezone:          "Asia/Shanghai",
			Times:             []string{},
			Lookback:          Duration(24 * time.Hour),
			PrepareBefore:     Duration(5 * time.Minute),
			GenerationTimeout: Duration(5 * time.Minute),
			CatchupMaxAge:     Duration(6 * time.Hour),
			CatchupSlots:      1,
			NotificationTTL:   Duration(6 * time.Hour),
		},
		Editorial: EditorialConfig{
			MinPerSection:                  5,
			TargetPerSection:               6,
			MaxPerSection:                  8,
			MaxCandidates:                  120,
			MaxPublisherShareInternational: 0.5,
			LateArrivalMaxAge:              Duration(48 * time.Hour),
			AllowUndatedOnColdStart:        false,
			RepeatPolicy:                   "suppress_unchanged_events",
		},
		LLM: LLMConfig{
			Enabled:                true,
			Adapter:                "chat_completions",
			BaseURLEnv:             "LLM_BASE_URL",
			APIKeyEnv:              "LLM_API_KEY",
			ModelEnv:               "LLM_MODEL",
			ResponseMode:           "json_object",
			RequestTimeout:         Duration(90 * time.Second),
			MaxConcurrent:          1,
			MaxInputTokens:         24000,
			MaxOutputTokens:        6000,
			MaxRepairAttempts:      1,
			PromptVersion:          "news-zh-v1",
			DailyMaxRequests:       20,
			DailyMaxReservedTokens: 200000,
			Fallback:               "rss_summary",
			Currency:               "CNY",
		},
		Notification: NotificationConfig{
			Enabled:            false,
			Provider:           "feishu_webhook",
			RecipientAlias:     "news-group",
			WebhookURLEnv:      "FEISHU_WEBHOOK_URL",
			SignSecretEnv:      "FEISHU_SIGN_SECRET",
			RequireSignature:   true,
			MessageType:        "post",
			Timeout:            Duration(20 * time.Second),
			FollowRedirects:    false,
			MaxPayloadBytes:    18000,
			MinSendInterval:    Duration(time.Second),
			MaxSendAttempts:    3,
			DailyRequestBudget: 20,
			RetryUnknown:       false,
			SplitLongMessages:  false,
		},
		Backup: BackupConfig{
			Enabled:            true,
			Interval:           Duration(24 * time.Hour),
			PauseWritesTimeout: Duration(60 * time.Second),
			KeepDaily:          14,
			KeepWeekly:         8,
			KeepMonthly:        12,
		},
	}
}

func Load(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	cfg := Defaults()
	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) AnnualLocation() *time.Location {
	loc, _ := time.LoadLocation(c.App.AnnualTimezone)
	return loc
}
func (c *Config) DisplayLocation() *time.Location {
	loc, _ := time.LoadLocation(c.App.DisplayTimezone)
	return loc
}
func (c *Config) ScheduleLocation() *time.Location {
	loc, _ := time.LoadLocation(c.Schedule.Timezone)
	return loc
}

func (c *Config) Validate() error {
	var problems []string
	add := func(format string, args ...interface{}) { problems = append(problems, fmt.Sprintf(format, args...)) }

	if strings.TrimSpace(c.App.DataDir) == "" {
		add("app.data_dir is required")
	}
	if c.App.AnnualTimezone == "" {
		add("app.annual_timezone is required (IANA name)")
	} else if _, err := time.LoadLocation(c.App.AnnualTimezone); err != nil {
		add("app.annual_timezone invalid: %v", err)
	}
	if c.App.DisplayTimezone == "" {
		add("app.display_timezone is required (IANA name)")
	} else if _, err := time.LoadLocation(c.App.DisplayTimezone); err != nil {
		add("app.display_timezone invalid: %v", err)
	}
	switch c.App.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		add("app.log_level must be debug|info|warn|error")
	}

	if c.Storage.FilenamePattern == "" {
		add("storage.filename_pattern is required")
	} else if !strings.Contains(c.Storage.FilenamePattern, "%04d") {
		add("storage.filename_pattern must contain %%04d")
	}
	if c.Storage.JournalMode != "WAL" {
		add("storage.journal_mode must be WAL")
	}
	if c.Storage.Synchronous != "FULL" && c.Storage.Synchronous != "NORMAL" {
		add("storage.synchronous must be FULL or NORMAL")
	}
	if c.Storage.BusyTimeout.D() <= 0 {
		add("storage.busy_timeout must be > 0")
	}
	if c.Storage.ReadConnectionsPerShard < 0 {
		add("storage.read_connections_per_shard must be >= 0")
	}
	if c.Storage.MaxOpenShards < 1 {
		add("storage.max_open_shards must be >= 1")
	}

	if c.Fetch.MaxConcurrent < 1 || c.Fetch.MaxConcurrent > 8 {
		add("fetch.max_concurrent must be between 1 and 8")
	}
	if c.Fetch.MaxPerHost < 1 {
		add("fetch.max_per_host must be >= 1")
	}
	if c.Fetch.RequestTimeout.D() <= 0 {
		add("fetch.request_timeout must be > 0")
	}
	if c.Fetch.ConnectTimeout.D() <= 0 {
		add("fetch.connect_timeout must be > 0")
	}
	if c.Fetch.MaxResponseBytes < 64<<10 {
		add("fetch.max_response_bytes must be >= 64KiB")
	}
	if c.Fetch.MaxRedirects < 1 {
		add("fetch.max_redirects must be >= 1")
	}
	if c.Fetch.UserAgent == "" {
		add("fetch.user_agent is required")
	}
	if c.Fetch.JitterRatio < 0 || c.Fetch.JitterRatio > 1 {
		add("fetch.jitter_ratio must be within [0,1]")
	}
	if c.Fetch.StaleAfter.D() <= 0 {
		add("fetch.stale_after must be > 0")
	}
	if c.Fetch.FutureTolerance.D() <= 0 {
		add("fetch.future_tolerance must be > 0")
	}

	if c.Schedule.Timezone == "" {
		add("schedule.timezone is required (IANA name)")
	} else if _, err := time.LoadLocation(c.Schedule.Timezone); err != nil {
		add("schedule.timezone invalid: %v", err)
	}
	if c.Schedule.Lookback.D() <= 0 {
		add("schedule.lookback must be > 0")
	}
	if c.Schedule.PrepareBefore.D() < 0 {
		add("schedule.prepare_before must be >= 0")
	}
	if c.Schedule.GenerationTimeout.D() <= 0 {
		add("schedule.generation_timeout must be > 0")
	}
	if c.Schedule.CatchupMaxAge.D() < 0 {
		add("schedule.catchup_max_age must be >= 0")
	}
	if c.Schedule.NotificationTTL.D() <= 0 {
		add("schedule.notification_ttl must be > 0")
	}
	if c.Schedule.ID == "" {
		add("schedule.id is required")
	}
	if c.Schedule.Enabled && len(c.Schedule.Times) == 0 {
		add("schedule.enabled is true but schedule.times is empty")
	}
	seenTimes := map[string]bool{}
	for _, t := range c.Schedule.Times {
		parsed, ok := ParseHHMM(t)
		if !ok {
			add("schedule.times %q is not HH:MM", t)
			continue
		}
		if seenTimes[parsed] {
			add("schedule.times contains duplicate %q", t)
		}
		seenTimes[parsed] = true
	}

	if c.Editorial.MinPerSection < 1 {
		add("editorial.min_per_section must be >= 1")
	}
	if c.Editorial.TargetPerSection < c.Editorial.MinPerSection || c.Editorial.TargetPerSection > c.Editorial.MaxPerSection {
		add("editorial.target_per_section must be between min and max")
	}
	if c.Editorial.MaxPerSection > 20 {
		add("editorial.max_per_section must be <= 20")
	}
	if c.Editorial.MaxCandidates < c.Editorial.MaxPerSection {
		add("editorial.max_candidates must be >= max_per_section")
	}
	if c.Editorial.MaxPublisherShareInternational <= 0 || c.Editorial.MaxPublisherShareInternational > 1 {
		add("editorial.max_publisher_share_international must be within (0,1]")
	}
	if c.Editorial.LateArrivalMaxAge.D() < 0 {
		add("editorial.late_arrival_max_age must be >= 0")
	}
	if c.Editorial.IncludeFullArticleFetch {
		add("editorial.include_full_article_fetch is not implemented in v1; RSS text only")
	}
	switch c.Editorial.RepeatPolicy {
	case "suppress_unchanged_events", "always", "since_previous_slot":
	default:
		add("editorial.repeat_policy must be suppress_unchanged_events|always|since_previous_slot")
	}

	if c.LLM.Enabled {
		if c.LLM.Adapter != "chat_completions" {
			add("llm.adapter must be chat_completions")
		}
		if c.LLM.ResponseMode != "json_object" && c.LLM.ResponseMode != "json_schema" && c.LLM.ResponseMode != "text" {
			add("llm.response_mode must be json_object|json_schema|text")
		}
		if c.LLM.RequestTimeout.D() <= 0 {
			add("llm.request_timeout must be > 0")
		}
		if c.LLM.MaxConcurrent < 1 || c.LLM.MaxConcurrent > 2 {
			add("llm.max_concurrent must be 1 or 2")
		}
		if c.LLM.MaxInputTokens <= 0 || c.LLM.MaxOutputTokens <= 0 {
			add("llm token limits must be > 0; budgets must be deliberate")
		}
		if c.LLM.DailyMaxRequests <= 0 || c.LLM.DailyMaxReservedTokens <= 0 {
			add("llm daily budgets must be > 0; no unlimited default")
		}
		if c.LLM.InputPriceMicrosPerMTok < 0 || c.LLM.OutputPriceMicrosPerMTok < 0 {
			add("llm prices must be >= 0 (they are estimates only)")
		}
		if c.LLM.MaxRepairAttempts < 0 || c.LLM.MaxRepairAttempts > 2 {
			add("llm.max_repair_attempts must be within [0,2]")
		}
		if c.LLM.PromptVersion == "" {
			add("llm.prompt_version is required")
		}
		switch c.LLM.Fallback {
		case "rss_summary", "none":
		default:
			add("llm.fallback must be rss_summary|none")
		}
		for _, def := range []struct{ name, env string }{
			{"llm.base_url_env", c.LLM.BaseURLEnv},
			{"llm.api_key_env", c.LLM.APIKeyEnv},
			{"llm.model_env", c.LLM.ModelEnv},
		} {
			if def.env == "" {
				add("%s is required", def.name)
			} else if os.Getenv(def.env) == "" {
				add("%s references unset environment variable %s", def.name, def.env)
			}
		}
		names := map[string]bool{"primary": true}
		for i, f := range c.LLM.FallbackProviders {
			prefix := fmt.Sprintf("llm.fallback_providers[%d]", i)
			if f.Name == "" {
				add("%s.name is required", prefix)
			} else if names[f.Name] {
				add("%s.name duplicates another provider", prefix)
			} else {
				names[f.Name] = true
			}
			if f.Adapter != "chat_completions" {
				add("%s.adapter must be chat_completions", prefix)
			}
			switch f.ResponseMode {
			case "json_object", "json_schema", "text":
			default:
				add("%s.response_mode must be json_object|json_schema|text", prefix)
			}
			for _, def := range []struct {
				name string
				env  string
			}{
				{"base_url_env", f.BaseURLEnv},
				{"api_key_env", f.APIKeyEnv},
				{"model_env", f.ModelEnv},
			} {
				if def.env == "" {
					add("%s.%s is required", prefix, def.name)
				} else if os.Getenv(def.env) == "" {
					add("%s.%s references unset environment variable %s", prefix, def.name, def.env)
				}
			}
		}
	}

	if c.Notification.Enabled {
		if c.Notification.Provider != "feishu_webhook" {
			add("notification.provider must be feishu_webhook")
		}
		if c.Notification.RecipientAlias == "" {
			add("notification.recipient_alias is required")
		}
		if c.Notification.MessageType != "post" {
			add("notification.message_type must be post")
		}
		if c.Notification.WebhookURLEnv == "" {
			add("notification.webhook_url_env is required")
		} else {
			raw := os.Getenv(c.Notification.WebhookURLEnv)
			if raw == "" {
				add("notification.webhook_url_env references unset environment variable %s", c.Notification.WebhookURLEnv)
			} else if err := ValidateFeishuWebhook(raw); err != nil {
				add("notification.webhook_url_env: %v", err)
			}
		}
		if c.Notification.RequireSignature {
			if c.Notification.SignSecretEnv == "" {
				add("notification.sign_secret_env is required when signature is enabled")
			} else if os.Getenv(c.Notification.SignSecretEnv) == "" {
				add("notification.sign_secret_env references unset environment variable %s", c.Notification.SignSecretEnv)
			}
		}
		if c.Notification.Timeout.D() <= 0 {
			add("notification.timeout must be > 0")
		}
		if c.Notification.MaxPayloadBytes < 1000 || c.Notification.MaxPayloadBytes > 20000 {
			add("notification.max_payload_bytes must be within [1000,20000]")
		}
		if c.Notification.MinSendInterval.D() < time.Second {
			add("notification.min_send_interval must be >= 1s")
		}
		if c.Notification.MaxSendAttempts < 1 || c.Notification.MaxSendAttempts > 10 {
			add("notification.max_send_attempts must be within [1,10]")
		}
		if c.Notification.DailyRequestBudget <= 0 {
			add("notification.daily_request_budget must be > 0")
		}
		if c.Notification.FollowRedirects {
			add("notification.follow_redirects must be false (do not forward secrets)")
		}
		if c.Notification.SplitLongMessages {
			add("notification.split_long_messages is not implemented in v1; leave false")
		}
	}

	if c.HTTP.Listen == "" {
		add("http.listen is required")
	}
	if c.HTTP.AdminTokenEnv == "" {
		add("http.admin_token_env is required")
	}

	if len(c.Sources) == 0 {
		add("sources: at least one source is required")
	}
	seenIDs := map[string]bool{}
	enabled := 0
	for i := range c.Sources {
		sc := &c.Sources[i]
		prefix := fmt.Sprintf("sources[%d](%s)", i, sc.ID)
		if sc.ID == "" {
			add("%s: id is required", prefix)
		} else if seenIDs[sc.ID] {
			add("%s: duplicate id", prefix)
		}
		seenIDs[sc.ID] = true
		if u, err := url.Parse(sc.URL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			add("%s: url must be an absolute http(s) URL", prefix)
		}
		if sc.PublisherID == "" || sc.PublisherGroup == "" {
			add("%s: publisher_id and publisher_group are required", prefix)
		}
		if sc.Language == "" {
			add("%s: language is required", prefix)
		}
		switch sc.SectionHint {
		case "international", "domestic", "mixed":
		default:
			add("%s: section_hint must be international|domestic|mixed", prefix)
		}
		if sc.TimezoneHint != "" {
			if _, err := time.LoadLocation(sc.TimezoneHint); err != nil {
				add("%s: timezone_hint invalid: %v", prefix, err)
			}
		}
		if sc.PollInterval.D() < time.Minute {
			add("%s: poll_interval must be >= 1m", prefix)
		}
		if sc.Enabled {
			enabled++
		}
	}
	if enabled == 0 {
		add("sources: at least one enabled source is required")
	}

	if c.Backup.Enabled {
		if c.Backup.Interval.D() < time.Hour {
			add("backup.interval must be >= 1h")
		}
		if c.Backup.PauseWritesTimeout.D() <= 0 {
			add("backup.pause_writes_timeout must be > 0")
		}
	}

	if len(problems) > 0 {
		return errors.New("config validation failed:\n  - " + strings.Join(problems, "\n  - "))
	}
	return nil
}

// ParseHHMM parses "HH:MM" and returns a normalized "HH:MM". It rejects
// out-of-range values so schedule configuration mistakes surface at startup.
func ParseHHMM(s string) (string, bool) {
	if len(s) != 5 || s[2] != ':' {
		return "", false
	}
	hh, err1 := parseInt2(s[0:2])
	mm, err2 := parseInt2(s[3:5])
	if err1 != nil || err2 != nil || hh > 23 || mm > 59 {
		return "", false
	}
	return fmt.Sprintf("%02d:%02d", hh, mm), true
}

func parseInt2(s string) (int, error) {
	if len(s) != 2 || s[0] < '0' || s[0] > '9' || s[1] < '0' || s[1] > '9' {
		return 0, fmt.Errorf("not a two digit number")
	}
	return int(s[0]-'0')*10 + int(s[1]-'0'), nil
}

func ValidateFeishuWebhook(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return errors.New("invalid URL")
	}
	if u.Scheme != "https" {
		return errors.New("webhook must use HTTPS")
	}
	if u.Host != "open.feishu.cn" {
		return errors.New("webhook host must be open.feishu.cn")
	}
	if !strings.HasPrefix(u.Path, "/open-apis/bot/v2/hook/") {
		return errors.New("webhook path must start with /open-apis/bot/v2/hook/")
	}
	if strings.TrimPrefix(u.Path, "/open-apis/bot/v2/hook/") == "" {
		return errors.New("webhook token is missing")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return errors.New("webhook must not contain query or fragment")
	}
	return nil
}

func (c *Config) EnabledSources() []SourceConfig {
	out := make([]SourceConfig, 0, len(c.Sources))
	for _, s := range c.Sources {
		if s.Enabled {
			out = append(out, s)
		}
	}
	return out
}

func (c *Config) SourceByID(id string) (SourceConfig, bool) {
	for _, s := range c.Sources {
		if s.ID == id {
			return s, true
		}
	}
	return SourceConfig{}, false
}

func (c *Config) LLMBaseURL() string     { return os.Getenv(c.LLM.BaseURLEnv) }
func (c *Config) LLMAPIKey() string      { return os.Getenv(c.LLM.APIKeyEnv) }
func (c *Config) LLMModel() string       { return os.Getenv(c.LLM.ModelEnv) }
func (c *LLMCredential) BaseURL() string { return os.Getenv(c.BaseURLEnv) }
func (c *LLMCredential) APIKey() string  { return os.Getenv(c.APIKeyEnv) }
func (c *LLMCredential) Model() string   { return os.Getenv(c.ModelEnv) }
func (c *Config) FeishuWebhook() string  { return os.Getenv(c.Notification.WebhookURLEnv) }
func (c *Config) FeishuSecret() string   { return os.Getenv(c.Notification.SignSecretEnv) }
func (c *Config) AdminToken() string     { return os.Getenv(c.HTTP.AdminTokenEnv) }
