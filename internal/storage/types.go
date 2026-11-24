package storage

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"time"
)

const (
	DateFormatParsed      = "parsed"
	DateFormatAssumedTZ   = "assumed_timezone"
	DateFormatMissing     = "missing"
	DateFormatFuture      = "future_date"
	DateFormatUnparseable = "unparseable"
)

type Article struct {
	ID              string   `json:"id"`
	IdentityVersion int      `json:"identity_version"`
	CanonicalURL    string   `json:"canonical_url"`
	Title           string   `json:"title"`
	NormalizedTitle string   `json:"normalized_title"`
	SummaryText     string   `json:"summary_text"`
	ContentText     string   `json:"content_text"`
	ContentHash     string   `json:"content_hash"`
	Language        string   `json:"language"`
	Section         string   `json:"section"`
	PublisherID     string   `json:"publisher_id"`
	PublisherGroup  string   `json:"publisher_group"`
	PublishedAtRaw  string   `json:"published_at_raw,omitempty"`
	PublishedAt     *int64   `json:"published_at,omitempty"`
	UpdatedAt       *int64   `json:"updated_at,omitempty"`
	FirstSeenAt     int64    `json:"first_seen_at"`
	LastSeenAt      int64    `json:"last_seen_at"`
	DateQuality     string   `json:"date_quality"`
	QualityFlags    []string `json:"quality_flags"`
}

type ArticleWithYear struct {
	Article
	OwnerYear int `json:"owner_year"`
}

type Alias struct {
	Hash  string
	Kind  string
	Value string
}

type Observation struct {
	ID                string
	ArticleID         string
	SourceID          string
	SourceItemKey     string
	GUIDRaw           string
	URLRaw            string
	TitleRaw          string
	DescriptionRaw    string
	ContentRaw        string
	PublishedAtRaw    string
	PublisherClaimRaw string
	RawItemJSON       string
	RawItemHash       string
	FirstSeenAt       int64
	LastSeenAt        int64
}

type SourceState struct {
	SourceID            string
	ETag                *string
	LastModified        *string
	LastCheckedAt       *int64
	LastSuccessAt       *int64
	LatestPublishedAt   *int64
	ConsecutiveFailures int
	NextFetchAt         int64
	Health              string
	LastErrorCode       string
	UpdatedAt           int64
}

type FetchRun struct {
	ID                   string
	SourceID             string
	RunYear              int
	StartedAt            int64
	FinishedAt           *int64
	Status               string
	HTTPStatus           *int64
	ResponseBytes        int64
	ParsedCount          int
	InsertedCount        int
	UpdatedCount         int
	RejectedCount        int
	RejectedItemsJSON    string
	ErrorCode            string
	ErrorMessageRedacted string
}

type Job struct {
	ID                    string
	JobKey                string
	Kind                  string
	ScheduleID            string
	ScheduledAt           int64
	WindowStart           *int64
	WindowEnd             *int64
	CutoffAt              *int64
	GenerationDeadline    *int64
	ExpiresAt             int64
	Status                string
	AttemptCount          int
	NextAttemptAt         *int64
	LeaseUntil            *int64
	HeartbeatAt           *int64
	ConfigSnapshotJSON    string
	CandidateSnapshotJSON string
	ResultRef             string
	LastErrorCode         string
	LastErrorRedacted     string
	CreatedAt             int64
	UpdatedAt             int64
}

const (
	JobPending   = "pending"
	JobRunning   = "running"
	JobRetryWait = "retry_wait"
	JobSucceeded = "succeeded"
	JobFailed    = "failed"
	JobExpired   = "expired"
)

type Fact struct {
	Text         string   `json:"text"`
	EvidenceRefs []string `json:"evidence_refs"`
}

type ArticleRef struct {
	Year int    `json:"year"`
	ID   string `json:"id"`
}

type ArticleSnapshot struct {
	Year           int    `json:"year"`
	ID             string `json:"id"`
	Title          string `json:"title"`
	URL            string `json:"url"`
	PublisherID    string `json:"publisher_id"`
	PublisherGroup string `json:"publisher_group"`
	PublishedAt    *int64 `json:"published_at,omitempty"`
	Summary        string `json:"summary,omitempty"`
}

type DigestItem struct {
	Ordinal          int               `json:"ordinal"`
	Section          string            `json:"section"`
	EventKey         string            `json:"event_key"`
	TitleZh          string            `json:"title_zh"`
	Facts            []Fact            `json:"facts"`
	Background       string            `json:"background"`
	Impact           string            `json:"impact"`
	Importance       int               `json:"importance"`
	Coverage         string            `json:"coverage"`
	ArticleRefs      []ArticleRef      `json:"article_refs"`
	EvidenceSnapshot []ArticleSnapshot `json:"evidence_snapshot"`
	QualityFlags     []string          `json:"quality_flags"`
}

type SourceHealth struct {
	SourceID          string `json:"source_id"`
	Health            string `json:"health"`
	LastSuccessAt     *int64 `json:"last_success_at,omitempty"`
	LatestPublishedAt *int64 `json:"latest_published_at,omitempty"`
	LastErrorCode     string `json:"last_error_code,omitempty"`
}

type Digest struct {
	ID                string         `json:"id"`
	JobID             string         `json:"job_id,omitempty"`
	SlotKey           string         `json:"slot_key"`
	Revision          int            `json:"revision"`
	ScheduleID        string         `json:"schedule_id,omitempty"`
	ScheduledAt       int64          `json:"scheduled_at"`
	WindowStart       int64          `json:"window_start"`
	WindowEnd         int64          `json:"window_end"`
	CutoffAt          int64          `json:"cutoff_at"`
	Title             string         `json:"title"`
	Markdown          string         `json:"markdown"`
	ContentHash       string         `json:"content_hash"`
	GenerationMode    string         `json:"generation_mode"`
	ModelMetadataJSON string         `json:"model_metadata_json"`
	SourceHealth      []SourceHealth `json:"source_health"`
	QualityFlags      []string       `json:"quality_flags"`
	CreatedAt         int64          `json:"created_at"`
	Items             []DigestItem   `json:"items,omitempty"`
	OwnerYear         int            `json:"owner_year,omitempty"`
}

const (
	SectionInternational = "international"
	SectionDomestic      = "domestic"
	SectionStatus        = "status"
)

const (
	OutboxPending     = "pending"
	OutboxSending     = "sending"
	OutboxSent        = "sent"
	OutboxRetryWait   = "retry_wait"
	OutboxFailed      = "failed"
	OutboxSendUnknown = "send_unknown"
	OutboxExpired     = "expired"
)

type OutboxItem struct {
	ID               string
	DigestID         string
	DedupKey         string
	Provider         string
	RecipientAlias   string
	PartNo           int
	ResendRevision   int
	ResendReason     string
	PayloadJSON      string
	PayloadHash      string
	Status           string
	SendAttemptCount int
	NextAttemptAt    *int64
	LeaseUntil       *int64
	ExpiresAt        int64
	SentAt           *int64
	LastErrorCode    string
	CreatedAt        int64
	UpdatedAt        int64
	OwnerYear        int
	AttemptID        string
}

type LLMCall struct {
	ID                   string
	JobID                string
	RequestedAt          int64
	FinishedAt           *int64
	Provider             string
	Model                string
	InputHash            string
	Status               string
	ReservedInputTokens  int
	ReservedOutputTokens int
	CostEstimateMicros   *int64
	Currency             string
	ErrorCode            string
}

type LLMUsageSummary struct {
	Requests     int64 `json:"requests"`
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
	CostMicros   int64 `json:"cost_micros"`
}

func NewID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func nullStr(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return ""
}

func sqlNullString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}

func sqlNullInt64(v *int64) sql.NullInt64 {
	if v == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: *v, Valid: true}
}

func int64Ptr(n sql.NullInt64) *int64 {
	if !n.Valid {
		return nil
	}
	v := n.Int64
	return &v
}

func stringPtr(n sql.NullString) *string {
	if !n.Valid {
		return nil
	}
	v := n.String
	return &v
}

func marshalJSON(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func decodeStringSlice(raw string) []string {
	var v []string
	if err := json.Unmarshal([]byte(raw), &v); err != nil || v == nil {
		return []string{}
	}
	return v
}

func decodeJSON[T any](raw string, fallback T) T {
	if raw == "" {
		return fallback
	}
	var v T
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return fallback
	}
	return v
}

func nowUnix() int64 { return time.Now().Unix() }
