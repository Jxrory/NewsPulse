package feed

import (
	"encoding/json"
	"net/url"
	"strings"
	"time"

	"github.com/mmcdole/gofeed"

	"github.com/jxrory/newspulse/internal/config"
	"github.com/jxrory/newspulse/internal/editorial"
	"github.com/jxrory/newspulse/internal/normalize"
	"github.com/jxrory/newspulse/internal/storage"
)

type ParsedItem struct {
	Article      storage.Article
	Aliases      []storage.Alias
	WeakIdentity bool
	Observation  storage.Observation
	GUID         string
	RawURL       string
	RawTitle     string
}

type Rejection struct {
	Key    string `json:"key"`
	Reason string `json:"reason"`
}

func ParseItems(src config.SourceConfig, f *gofeed.Feed, observedAt time.Time, futureTolerance time.Duration) ([]ParsedItem, []Rejection) {
	var tz *time.Location
	if src.TimezoneHint != "" {
		tz, _ = time.LoadLocation(src.TimezoneHint)
	}
	now := observedAt
	var items []ParsedItem
	var rejected []Rejection
	for _, it := range f.Items {
		if it == nil {
			continue
		}
		rawTitle := it.Title
		title := normalize.NormalizeTitle(rawTitle)
		key := it.GUID
		if key == "" {
			key = it.Link
		}
		if title == "" {
			rejected = append(rejected, Rejection{Key: key, Reason: "missing_title"})
			continue
		}
		if normalize.IsLikelyAd(title) {
			rejected = append(rejected, Rejection{Key: key, Reason: "likely_advertisement"})
			continue
		}
		urlRaw := it.Link
		if urlRaw == "" && len(it.Links) > 0 {
			urlRaw = it.Links[0]
		}
		urlRaw = resolveRelative(src.URL, urlRaw)
		canonical, hasURL := normalize.URLIdentityAlias(urlRaw)
		if !hasURL && it.GUID != "" {
			if g, ok := normalize.URLIdentityAlias(it.GUID); ok {
				canonical = g
				hasURL = true
			}
		}
		guid := strings.TrimSpace(it.GUID)
		summary := normalize.CleanSummary(it.Description)
		content := normalize.CleanContent(it.Content)
		if content == "" {
			content = ""
		}
		pubRaw := strings.TrimSpace(it.Published)
		if pubRaw == "" {
			pubRaw = strings.TrimSpace(it.Updated)
		}
		published, quality := normalize.ParsePublished(pubRaw, tz, now, futureTolerance)
		if published == nil && it.PublishedParsed != nil && pubRaw != "" && hasZoneHint(pubRaw) {
			v := it.PublishedParsed.UTC().Unix()
			if time.Unix(v, 0).After(now.Add(futureTolerance)) {
				quality = storage.DateFormatFuture
			} else {
				quality = storage.DateFormatParsed
			}
			published = &v
		}
		var updated *int64
		if it.UpdatedParsed != nil {
			v := it.UpdatedParsed.UTC().Unix()
			updated = &v
		}
		flags := []string{}
		if quality == storage.DateFormatFuture {
			flags = append(flags, "future_date")
		}
		if quality == storage.DateFormatMissing || quality == storage.DateFormatUnparseable {
			flags = append(flags, "date_missing")
		}

		aliases, weak := normalize.AliasesFor(src.ID, canonical, guid, normalize.TitleComparisonKey(title), pubRaw, summary)
		sourceKey := normalize.SourceItemKey(src.ID, guid, canonical, normalize.TitleComparisonKey(title)+"\x00"+pubRaw+"\x00"+summary)
		hash := normalize.ContentHash(title, summary, content)

		artID, err := storage.NewID()
		if err != nil {
			rejected = append(rejected, Rejection{Key: key, Reason: "id_generation_failed"})
			continue
		}
		rawJSON, _ := json.Marshal(it)
		rawJSONStr, truncated := normalize.TruncateRawJSON(string(rawJSON))
		if truncated {
			flags = append(flags, "raw_item_truncated")
		}
		art := storage.Article{
			ID:              artID,
			IdentityVersion: 1,
			CanonicalURL:    canonical,
			Title:           title,
			NormalizedTitle: normalize.TitleComparisonKey(title),
			SummaryText:     summary,
			ContentText:     content,
			ContentHash:     hash,
			Language:        src.Language,
			Section:         editorial.Classify(src, title, summary),
			PublisherID:     src.PublisherID,
			PublisherGroup:  src.PublisherGroup,
			PublishedAtRaw:  pubRaw,
			PublishedAt:     published,
			UpdatedAt:       updated,
			FirstSeenAt:     now.Unix(),
			LastSeenAt:      now.Unix(),
			DateQuality:     quality,
			QualityFlags:    flags,
		}
		obs := storage.Observation{
			ArticleID:      artID,
			SourceID:       src.ID,
			SourceItemKey:  sourceKey,
			GUIDRaw:        guid,
			URLRaw:         urlRaw,
			TitleRaw:       rawTitle,
			DescriptionRaw: it.Description,
			ContentRaw:     it.Content,
			PublishedAtRaw: pubRaw,
			RawItemJSON:    rawJSONStr,
			RawItemHash:    normalize.SHA256Hex(rawJSONStr),
			FirstSeenAt:    now.Unix(),
			LastSeenAt:     now.Unix(),
		}
		items = append(items, ParsedItem{
			Article: art, Aliases: aliases, WeakIdentity: weak, Observation: obs,
			GUID: guid, RawURL: urlRaw, RawTitle: rawTitle,
		})
	}
	return items, rejected
}

func resolveRelative(base, raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	if u.IsAbs() {
		return raw
	}
	b, err := url.Parse(base)
	if err != nil {
		return raw
	}
	return b.ResolveReference(u).String()
}

func hasZoneHint(raw string) bool {
	u := strings.ToUpper(strings.TrimSpace(raw))
	return strings.HasSuffix(u, "Z") || strings.HasSuffix(u, "GMT") || strings.HasSuffix(u, "UTC") ||
		strings.Contains(u, "+") || strings.Contains(u, "-")
}
