package normalize

import (
	"net/url"
	"strings"
)

var trackingParams = map[string]bool{
	"fbclid": true,
	"gclid":  true,
	"dclid":  true,
}

// NormalizeURL lowercases scheme/host, strips default ports and fragments,
// and removes only well-known tracking parameters. Path case and unknown
// query parameters are preserved; HTTP is not forced to HTTPS.
func NormalizeURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return ""
	}
	u.Scheme = strings.ToLower(u.Scheme)
	if u.Scheme != "http" && u.Scheme != "https" {
		return ""
	}
	host := strings.ToLower(u.Host)
	switch {
	case strings.HasSuffix(host, ":80") && u.Scheme == "http":
		host = strings.TrimSuffix(host, ":80")
	case strings.HasSuffix(host, ":443") && u.Scheme == "https":
		host = strings.TrimSuffix(host, ":443")
	}
	u.Host = host
	u.Fragment = ""

	q := u.Query()
	changed := false
	for k := range q {
		lower := strings.ToLower(k)
		remove := trackingParams[lower] || strings.HasPrefix(lower, "utm_")
		if remove {
			q.Del(k)
			changed = true
		}
	}
	if changed {
		u.RawQuery = q.Encode()
	}
	return u.String()
}

// URLIdentityAlias returns the canonical URL used for the url alias.
func URLIdentityAlias(raw string) (string, bool) {
	n := NormalizeURL(raw)
	if n == "" {
		return "", false
	}
	return n, true
}

func HostOf(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Hostname())
}
