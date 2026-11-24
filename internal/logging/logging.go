package logging

import (
	"log/slog"
	"os"
	"regexp"
	"strings"
)

var (
	feishuHookRe  = regexp.MustCompile(`(/open-apis/bot/v2/hook/)[A-Za-z0-9_-]+`)
	querySecretRe = regexp.MustCompile(`(?i)(key|token|access_token|api_key|apikey|secret)=([^&\s]+)`)
	bearerRe      = regexp.MustCompile(`(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+`)
	authHeaderRe  = regexp.MustCompile(`(?i)(authorization:\s*)[^\s,]+`)
)

// New builds a JSON slog logger. Log records must never contain webhook tokens
// or API keys; all helpers below are used at call sites and the ReplaceAttr
// hook masks known-sensitive attribute names as a second line of defence.
func New(level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: lvl,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			switch strings.ToLower(a.Key) {
			case "sign", "secret", "api_key", "apikey", "authorization", "webhook_url", "webhook_token", "sendkey":
				return slog.String(a.Key, "[REDACTED]")
			}
			if a.Value.Kind() == slog.KindString {
				return slog.String(a.Key, Redact(a.Value.String()))
			}
			return a
		},
	})
	return slog.New(h)
}

// Redact scrubs webhook paths, query credentials, bearer tokens and auth
// headers from an arbitrary string before logging.
func Redact(s string) string {
	if s == "" {
		return s
	}
	s = feishuHookRe.ReplaceAllString(s, "${1}[REDACTED]")
	s = querySecretRe.ReplaceAllString(s, "${1}=[REDACTED]")
	s = bearerRe.ReplaceAllString(s, "${1}[REDACTED]")
	s = authHeaderRe.ReplaceAllString(s, "${1}[REDACTED]")
	return s
}

// ErrorString renders an error for logs without leaking embedded URLs.
func ErrorString(err error) string {
	if err == nil {
		return ""
	}
	return Redact(err.Error())
}
