package normalize

import (
	"crypto/sha256"
	"encoding/hex"
	stdhtml "html"
	"regexp"
	"strings"
	"unicode/utf8"

	xhtml "golang.org/x/net/html"
)

var whitespaceRe = regexp.MustCompile(`[\s\p{Zs}]+`)

const (
	MaxTitleRunes  = 1024
	MaxDescription = 32 << 10
	MaxContent     = 256 << 10
	MaxRawItemJSON = 512 << 10
)

// HTMLToText extracts visible text, ignores scripts/styles/comments and
// decodes entities. Feed HTML is untrusted; nothing is executed.
func HTMLToText(raw string) string {
	if raw == "" {
		return ""
	}
	if !strings.ContainsAny(raw, "<&") {
		return CollapseWhitespace(stdhtml.UnescapeString(raw))
	}
	z := xhtml.NewTokenizer(strings.NewReader(raw))
	var b strings.Builder
	skip := 0
	for {
		tt := z.Next()
		switch tt {
		case xhtml.ErrorToken:
			return CollapseWhitespace(stdhtml.UnescapeString(b.String()))
		case xhtml.StartTagToken:
			t := z.Token()
			switch strings.ToLower(t.Data) {
			case "script", "style", "template":
				skip++
			case "p", "br", "div", "li", "h1", "h2", "h3", "h4", "h5", "h6", "tr":
				b.WriteByte(' ')
			}
		case xhtml.EndTagToken:
			t := z.Token()
			switch strings.ToLower(t.Data) {
			case "script", "style", "template":
				if skip > 0 {
					skip--
				}
			case "p", "div", "li":
				b.WriteByte(' ')
			}
		case xhtml.TextToken:
			if skip == 0 {
				b.WriteString(string(z.Text()))
			}
		}
	}
}

func CollapseWhitespace(s string) string {
	return strings.TrimSpace(whitespaceRe.ReplaceAllString(s, " "))
}

// NormalizeTitle keeps readable casing but collapses whitespace, strips HTML
// and truncates by runes so we never split a multi-byte character.
func NormalizeTitle(raw string) string {
	t := HTMLToText(raw)
	return truncateRunes(t, MaxTitleRunes)
}

// TitleComparisonKey is only used for candidate grouping, not identity.
func TitleComparisonKey(raw string) string {
	return strings.ToLower(NormalizeTitle(raw))
}

func CleanSummary(raw string) string {
	return truncateBytes(HTMLToText(raw), MaxDescription)
}

func CleanContent(raw string) string {
	return truncateBytes(HTMLToText(raw), MaxContent)
}

func truncateRunes(s string, max int) string {
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	r := []rune(s)
	return string(r[:max])
}

func truncateBytes(s string, max int) string {
	if len(s) <= max {
		return s
	}
	b := []byte(s)
	cut := max
	for cut > 0 && !utf8.RuneStart(b[cut]) {
		cut--
	}
	return string(b[:cut])
}

func TruncateRawJSON(raw string) (string, bool) {
	if len(raw) <= MaxRawItemJSON {
		return raw, false
	}
	return truncateBytes(raw, MaxRawItemJSON), true
}

// ContentHash detects substantive title/body changes. Capture timestamps and
// source listing churn are deliberately excluded.
func ContentHash(title, summary, content string) string {
	h := sha256.New()
	h.Write([]byte(TitleComparisonKey(title)))
	h.Write([]byte{0})
	h.Write([]byte(summary))
	h.Write([]byte{0})
	h.Write([]byte(content))
	return hex.EncodeToString(h.Sum(nil))
}

func SHA256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// BigramJaccard is a candidate hint for same-language event grouping. It is
// never used to merge articles automatically.
func BigramJaccard(a, b string) float64 {
	x := bigrams(a)
	y := bigrams(b)
	if len(x) == 0 || len(y) == 0 {
		return 0
	}
	inter := 0
	for k := range x {
		if _, ok := y[k]; ok {
			inter++
		}
	}
	union := len(x) + len(y) - inter
	if union == 0 {
		return 0
	}
	return float64(inter) / float64(union)
}

func bigrams(s string) map[string]struct{} {
	rs := []rune(strings.ToLower(CollapseWhitespace(s)))
	out := make(map[string]struct{}, len(rs))
	for i := 0; i+1 < len(rs); i++ {
		out[string(rs[i:i+2])] = struct{}{}
	}
	return out
}
