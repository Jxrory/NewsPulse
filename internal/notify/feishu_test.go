package notify

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/jxrory/newspulse/internal/config"
	"github.com/jxrory/newspulse/internal/storage"
)

func TestSignVector(t *testing.T) {
	got := Sign(1700000000, "demo-secret")
	want := "mYHw2R2SOm8Rw/sne3lQdmz4sOfntKR+1P/8RKKTwmA="
	if got != want {
		t.Fatalf("sign=%q want %q", got, want)
	}
}

func TestParseResponse(t *testing.T) {
	cases := []struct {
		status int
		body   string
		want   string
	}{
		{200, `{"code":0,"msg":"success"}`, storage.OutboxSent},
		{200, `{"msg":"missing code"}`, storage.OutboxSendUnknown},
		{200, `not json`, storage.OutboxSendUnknown},
		{200, `{"code":19021,"msg":"sign match fail"}`, storage.OutboxFailed},
		{200, `{"code":9499,"msg":"too many requests"}`, storage.OutboxRetryWait},
	}
	for _, c := range cases {
		out := ParseResponse(c.status, []byte(c.body))
		if out.Status != c.want {
			t.Errorf("status=%d body=%s got=%s want=%s", c.status, c.body, out.Status, c.want)
		}
	}
}

func TestRenderPayloadTrimsAndMeasures(t *testing.T) {
	cfg := config.Defaults()
	cfg.Notification.MaxPayloadBytes = 2000
	cfg.Notification.RequiredKeyword = "日报"
	cfg.Notification.Enabled = true
	items := []storage.DigestItem{}
	for i := 0; i < 8; i++ {
		items = append(items, storage.DigestItem{
			Section: "international", EventKey: "e" + string(rune('a'+i)),
			TitleZh: strings.Repeat("标题", 5), Facts: []storage.Fact{{Text: strings.Repeat("事实", 60)}},
			Background: strings.Repeat("背景", 100), Impact: strings.Repeat("影响", 100),
			Importance: 90 - i, Coverage: "single_source",
			EvidenceSnapshot: []storage.ArticleSnapshot{{PublisherID: "bbc", URL: "https://example.org/news/" + string(rune('a'+i))}},
		})
	}
	items = append(items, storage.DigestItem{Section: "status", EventKey: "status_summary",
		TitleZh: "状态", Facts: []storage.Fact{{Text: strings.Repeat("状态", 100)}}})
	d := storage.Digest{ID: "d1", Title: "每日新闻", OwnerYear: 2026}
	payload, flags, err := RenderPayload(cfg, d, items)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	size, err := FinalRequestSize(payload)
	if err != nil {
		t.Fatal(err)
	}
	if size > cfg.Notification.MaxPayloadBytes {
		t.Fatalf("payload size %d exceeds %d", size, cfg.Notification.MaxPayloadBytes)
	}
	if len(flags) == 0 || flags[0] != "length_trimmed" {
		t.Fatalf("expected trim flag, got %v", flags)
	}
	var p Payload
	if err := json.Unmarshal([]byte(payload), &p); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(p.Content.Post.ZhCN.Title, "日报") {
		t.Fatalf("required keyword missing: %q", p.Content.Post.ZhCN.Title)
	}
	if p.MsgType != "post" {
		t.Fatalf("msg_type=%q", p.MsgType)
	}
}
