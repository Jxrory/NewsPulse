package llm

import (
	"errors"
	"fmt"

	"github.com/jxrory/newspulse/internal/storage"
)

const SchemaVersion = 1

var (
	ErrDisabled  = errors.New("llm disabled")
	ErrBudget    = errors.New("llm daily budget exceeded")
	ErrCacheMiss = errors.New("analysis cache miss")
)

type AnalysisEvent struct {
	EventKey     string               `json:"event_key"`
	Section      string               `json:"section"`
	ArticleRefs  []storage.ArticleRef `json:"article_refs"`
	TitleZh      string               `json:"title_zh"`
	Facts        []storage.Fact       `json:"facts"`
	Background   string               `json:"background"`
	Impact       string               `json:"impact"`
	Importance   int                  `json:"importance"`
	Coverage     string               `json:"coverage,omitempty"`
	Warnings     []string             `json:"warnings,omitempty"`
	QualityFlags []string             `json:"quality_flags,omitempty"`
}

type Analysis struct {
	Events      []AnalysisEvent
	Mode        string
	Model       string
	Protocol    string
	UsageJSON   string
	CacheHit    bool
	RawResponse string
}

type BudgetError struct {
	Requests    int64
	MaxRequests int64
	Tokens      int64
	MaxTokens   int64
}

func (e *BudgetError) Error() string {
	return fmt.Sprintf("llm daily budget exceeded (requests %d/%d, reserved tokens %d/%d)",
		e.Requests, e.MaxRequests, e.Tokens, e.MaxTokens)
}

func (e *BudgetError) Unwrap() error { return ErrBudget }

type APIError struct {
	Code       string
	HTTPStatus int
	Message    string
}

func (e *APIError) Error() string {
	if e.HTTPStatus != 0 {
		return fmt.Sprintf("llm api %s (http %d): %s", e.Code, e.HTTPStatus, e.Message)
	}
	return fmt.Sprintf("llm api %s: %s", e.Code, e.Message)
}

type ValidationError struct {
	Problems []string
}

func (e *ValidationError) Error() string {
	msg := "analysis output validation failed"
	if len(e.Problems) > 0 {
		msg += ": " + e.Problems[0]
		if len(e.Problems) > 1 {
			msg += fmt.Sprintf(" (and %d more)", len(e.Problems)-1)
		}
	}
	return msg
}
