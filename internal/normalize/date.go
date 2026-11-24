package normalize

import (
	"regexp"
	"strings"
	"time"

	"github.com/jxrory/newspulse/internal/storage"
)

var zoneSuffixRe = regexp.MustCompile(`(?i)(Z|[+-][0-9]{2}:?[0-9]{2}|UTC|GMT)\s*$`)

var dateLayouts = []string{
	time.RFC1123Z,
	time.RFC1123,
	time.RFC822Z,
	time.RFC822,
	time.RFC3339Nano,
	time.RFC3339,
	"Mon, 2 Jan 2006 15:04:05 -0700",
	"Mon, 2 Jan 2006 15:04:05 MST",
	"2006-01-02T15:04:05Z0700",
	"2006-01-02 15:04:05 -0700",
	"2006-01-02 15:04:05Z07:00",
	"2006-01-02 15:04:05",
	"2006-01-02",
}

// ParsePublished interprets a feed date with an explicit policy:
//   - a timezone in the string is honoured;
//   - a missing timezone is only parsed when the source config declares a
//     timezone, and is marked assumed_timezone;
//   - otherwise the timestamp stays NULL rather than inventing a report time.
//
// A timestamp beyond observedAt+futureTolerance keeps its value but is
// flagged future_date so it cannot enter the main digest window.
func ParsePublished(raw string, tzHint *time.Location, observedAt time.Time, futureTolerance time.Duration) (*int64, string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, storage.DateFormatMissing
	}
	hasZone := zoneSuffixRe.MatchString(raw)
	layouts := dateLayouts
	if !hasZone {
		layouts = []string{
			"2006-01-02T15:04:05",
			"2006-01-02 15:04:05",
			"2006-01-02",
		}
	}
	for _, layout := range layouts {
		var t time.Time
		var err error
		if strings.HasSuffix(strings.ToUpper(raw), "GMT") {
			t, err = time.Parse(time.RFC1123Z, raw)
			if err != nil {
				t, err = time.Parse("Mon, 2 Jan 2006 15:04:05 -0700", raw)
			}
		} else {
			t, err = time.Parse(layout, raw)
		}
		if err != nil {
			continue
		}
		if !hasZone {
			if tzHint == nil {
				return nil, storage.DateFormatMissing
			}
			// time.Parse without a zone returns UTC; reinterpret the wall time
			// in the declared source timezone.
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), tzHint)
			unix := t.UTC().Unix()
			return flagFuture(&unix, observedAt, futureTolerance, storage.DateFormatAssumedTZ)
		}
		unix := t.UTC().Unix()
		return flagFuture(&unix, observedAt, futureTolerance, storage.DateFormatParsed)
	}
	return nil, storage.DateFormatUnparseable
}

func flagFuture(unix *int64, observedAt time.Time, tolerance time.Duration, quality string) (*int64, string) {
	if time.Unix(*unix, 0).After(observedAt.Add(tolerance)) {
		return unix, storage.DateFormatFuture
	}
	return unix, quality
}
