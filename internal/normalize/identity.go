package normalize

import (
	"strings"

	"github.com/jxrory/newspulse/internal/storage"
)

// AliasesFor returns deterministic identity aliases. URL is primary, a
// same-feed GUID is secondary, and a weak content fingerprint is only used
// when both are absent.
func AliasesFor(feedID, canonicalURL, guid, normalizedTitle, rawDate, cleanSummary string) ([]storage.Alias, bool) {
	var out []storage.Alias
	if canonicalURL != "" {
		out = append(out, newAlias("url", canonicalURL))
	}
	if guid != "" {
		out = append(out, newAlias("guid", feedID+"\x00"+guid))
	}
	if len(out) == 0 {
		preimage := normalizedTitle + "\x00" + rawDate + "\x00" + cleanSummary
		out = append(out, newAlias("fallback", feedID+"\x00"+preimage))
		return out, true
	}
	return out, false
}

func newAlias(kind, value string) storage.Alias {
	return storage.Alias{
		Hash:  SHA256Hex(kind + "\x00" + value),
		Kind:  kind,
		Value: value,
	}
}

// SourceItemKey is the stable per-feed observation key.
func SourceItemKey(feedID, guid, canonicalURL, weakPreimage string) string {
	switch {
	case guid != "":
		return guid
	case canonicalURL != "":
		return canonicalURL
	default:
		return SHA256Hex(weakPreimage)
	}
}

func IsLikelyAd(title string) bool {
	t := strings.ToLower(strings.TrimSpace(title))
	for _, p := range []string{"advertisement", "sponsored", "广告", "推广"} {
		if strings.HasPrefix(t, p) {
			return true
		}
	}
	return false
}
