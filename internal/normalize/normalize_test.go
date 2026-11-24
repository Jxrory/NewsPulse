package normalize

import (
	"testing"
	"time"

	"github.com/jxrory/newspulse/internal/storage"
)

func TestNormalizeURL(t *testing.T) {
	cases := []struct{ in, want string }{
		{"https://Example.COM:443/Path?utm_source=x&id=3&fbclid=y#frag", "https://example.com/Path?id=3"},
		{"http://example.com:80/a?gclid=1", "http://example.com/a"},
		{"https://example.com/a?b=1", "https://example.com/a?b=1"},
		{"ftp://example.com/a", ""},
		{"", ""},
	}
	for _, c := range cases {
		if got := NormalizeURL(c.in); got != c.want {
			t.Errorf("NormalizeURL(%q)=%q want %q", c.in, got, c.want)
		}
	}
}

func TestParsePublished(t *testing.T) {
	shanghai, _ := time.LoadLocation("Asia/Shanghai")
	observed := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	tol := 15 * time.Minute

	v, q := ParsePublished("Tue, 24 Nov 2026 10:00:00 +0000", nil, observed, tol)
	if v == nil || q != storage.DateFormatParsed {
		t.Fatalf("zoned date: v=%v q=%s", v, q)
	}
	if got := time.Unix(*v, 0).UTC().Format(time.RFC3339); got != "2026-11-24T10:00:00Z" {
		t.Fatalf("zoned date value %s", got)
	}

	v, q = ParsePublished("2026-11-24 10:00:00", shanghai, observed, tol)
	if v == nil || q != storage.DateFormatAssumedTZ {
		t.Fatalf("assumed timezone: v=%v q=%s", v, q)
	}
	if got := time.Unix(*v, 0).UTC().Format(time.RFC3339); got != "2026-11-24T02:00:00Z" {
		t.Fatalf("assumed tz value %s", got)
	}

	v, q = ParsePublished("2026-11-24 10:00:00", nil, observed, tol)
	if v != nil || q != storage.DateFormatMissing {
		t.Fatalf("no zone/no hint must stay NULL: v=%v q=%s", v, q)
	}

	v, q = ParsePublished("Tue, 24 Nov 2026 15:00:00 +0000", nil, observed, tol)
	if v == nil || q != storage.DateFormatFuture {
		t.Fatalf("future date: v=%v q=%s", v, q)
	}
}

func TestHTMLAndIdentity(t *testing.T) {
	raw := `<p>Hello &amp; <b>world</b><script>bad()</script></p>`
	if got := HTMLToText(raw); got != "Hello & world" {
		t.Fatalf("HTMLToText=%q", got)
	}
	aliases, weak := AliasesFor("feed", "https://example.com/a", "", "title", "date", "summary")
	if weak || len(aliases) != 1 || aliases[0].Kind != "url" {
		t.Fatalf("unexpected aliases %+v weak=%v", aliases, weak)
	}
	aliases, weak = AliasesFor("feed", "", "", "title", "date", "summary")
	if !weak || len(aliases) != 1 || aliases[0].Kind != "fallback" {
		t.Fatalf("fallback expected: %+v weak=%v", aliases, weak)
	}
}

func TestBigramJaccard(t *testing.T) {
	if s := BigramJaccard("中国央行宣布降准", "中国央行宣布降准"); s != 1 {
		t.Fatalf("same title score %v", s)
	}
	if s := BigramJaccard("中国央行宣布降准", "美国国务院发布声明"); s > 0.2 {
		t.Fatalf("unrelated titles too similar: %v", s)
	}
}
