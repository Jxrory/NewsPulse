package notify

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

// Sign implements Feishu custom-bot signing: HMAC-SHA256 with key
// "<timestamp>\n<secret>" over the empty message, then standard Base64.
func Sign(timestamp int64, secret string) string {
	key := strconv.FormatInt(timestamp, 10) + "\n" + secret
	mac := hmac.New(sha256.New, []byte(key))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

type Node struct {
	Tag  string `json:"tag"`
	Text string `json:"text,omitempty"`
	Href string `json:"href,omitempty"`
}

type PostBody struct {
	Title   string   `json:"title"`
	Content [][]Node `json:"content"`
}

type PostContent struct {
	Post struct {
		ZhCN PostBody `json:"zh_cn"`
	} `json:"post"`
}

type Payload struct {
	MsgType string      `json:"msg_type"`
	Content PostContent `json:"content"`
}

type envelope struct {
	Timestamp string      `json:"timestamp,omitempty"`
	Sign      string      `json:"sign,omitempty"`
	MsgType   string      `json:"msg_type"`
	Content   PostContent `json:"content"`
}

func BuildFinalRequest(payloadJSON string, timestamp int64, sign string) ([]byte, error) {
	var p Payload
	if err := json.Unmarshal([]byte(payloadJSON), &p); err != nil {
		return nil, fmt.Errorf("decode outbox payload: %w", err)
	}
	env := envelope{MsgType: p.MsgType, Content: p.Content}
	if sign != "" {
		env.Timestamp = strconv.FormatInt(timestamp, 10)
		env.Sign = sign
	}
	return json.Marshal(env)
}

// FinalRequestSize measures the real serialized request including JSON
// escaping, timestamp and signature. Sign length is fixed by Base64.
func FinalRequestSize(payloadJSON string) (int, error) {
	req, err := BuildFinalRequest(payloadJSON, 1760000000, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	if err != nil {
		return 0, err
	}
	return len(req), nil
}

type Response struct {
	Code       *int   `json:"code"`
	Msg        string `json:"msg"`
	StatusCode *int   `json:"StatusCode"`
}

type Outcome struct {
	Status       string
	HTTPStatus   *int64
	ProviderCode string
	ErrorCode    string
	ResponseJSON string
	RetryAfter   int64
}

func ParseResponse(httpStatus int, raw []byte) Outcome {
	var resp Response
	parseErr := json.Unmarshal(raw, &resp)
	out := Outcome{HTTPStatus: intPtr64(int64(httpStatus))}
	if parseErr != nil {
		out.Status = "send_unknown"
		out.ErrorCode = "unparseable_response"
		out.ResponseJSON = redactedBody(raw)
		return out
	}
	if resp.Code != nil {
		out.ProviderCode = strconv.Itoa(*resp.Code)
	} else if resp.StatusCode != nil {
		out.ProviderCode = strconv.Itoa(*resp.StatusCode)
	}
	out.ResponseJSON = bodyJSON(resp)
	switch {
	case resp.Code != nil && *resp.Code == 0:
		out.Status = "sent"
		return out
	case resp.Code == nil:
		out.Status = "send_unknown"
		out.ErrorCode = "missing_code"
		return out
	}
	msg := lower(resp.Msg)
	switch {
	case containsAny(msg, "rate", "limit", "too many", "频繁", "限流"):
		out.Status = "retry_wait"
		out.ErrorCode = "rate_limited"
	case containsAny(msg, "sign", "signature", "timestamp", "签名"):
		out.Status = "failed"
		out.ErrorCode = "signature_error"
	case containsAny(msg, "keyword", "关键词"):
		out.Status = "failed"
		out.ErrorCode = "keyword_error"
	case containsAny(msg, "ip", "whitelist", "白名单"):
		out.Status = "failed"
		out.ErrorCode = "ip_restricted"
	case containsAny(msg, "webhook", "token", "not exist", "invalid"):
		out.Status = "failed"
		out.ErrorCode = "invalid_webhook"
	default:
		out.Status = "failed"
		out.ErrorCode = "provider_error"
	}
	return out
}

func bodyJSON(r Response) string {
	b, _ := json.Marshal(map[string]interface{}{"code": r.Code, "msg": r.Msg})
	return string(b)
}

func redactedBody(raw []byte) string {
	msg := string(raw)
	if len(msg) > 200 {
		msg = msg[:200]
	}
	b, _ := json.Marshal(map[string]string{"unparsed": msg})
	return string(b)
}

func intPtr64(v int64) *int64 { return &v }

func lower(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}
	return string(b)
}

func containsAny(s string, parts ...string) bool {
	for _, p := range parts {
		if len(p) > 0 && indexOf(s, p) >= 0 {
			return true
		}
	}
	return false
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

var _ = errors.New
var _ = fmt.Sprintf
