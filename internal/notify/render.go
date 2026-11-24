package notify

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jxrory/newspulse/internal/config"
	"github.com/jxrory/newspulse/internal/editorial"
	"github.com/jxrory/newspulse/internal/normalize"
	"github.com/jxrory/newspulse/internal/storage"
)

var ErrPayloadTooLarge = errors.New("notification payload exceeds configured limit")

// RenderPayload builds the stable Feishu post business payload (no
// timestamp/sign). It measures the final serialized request and trims
// background, facts and low-priority items in a fixed order.
func RenderPayload(cfg *config.Config, d storage.Digest, items []storage.DigestItem) (string, []string, error) {
	canonical := orderItems(items)
	flags := []string{}
	for level := 0; level <= 3; level++ {
		payload, _, err := buildPost(cfg, d, canonical, level)
		if err != nil {
			return "", flags, err
		}
		size, err := FinalRequestSize(payload)
		if err != nil {
			return "", flags, err
		}
		if size <= cfg.Notification.MaxPayloadBytes {
			if level > 0 {
				flags = append(flags, "length_trimmed")
			}
			return payload, flags, nil
		}
		if level == 3 {
			break
		}
	}
	// Last resort: keep only top items per non-empty section.
	trimmed := dropLowPriority(canonical, 3)
	payload, _, err := buildPost(cfg, d, trimmed, 3)
	if err != nil {
		return "", flags, err
	}
	size, err := FinalRequestSize(payload)
	if err != nil {
		return "", flags, err
	}
	if size > cfg.Notification.MaxPayloadBytes {
		return "", flags, fmt.Errorf("%w: %d > %d", ErrPayloadTooLarge, size, cfg.Notification.MaxPayloadBytes)
	}
	flags = append(flags, "length_trimmed")
	return payload, flags, nil
}

func orderItems(items []storage.DigestItem) []storage.DigestItem {
	var intl, dom, status []storage.DigestItem
	for _, it := range items {
		switch it.Section {
		case editorial.SectionInternational:
			intl = append(intl, it)
		case editorial.SectionDomestic:
			dom = append(dom, it)
		default:
			status = append(status, it)
		}
	}
	sort.SliceStable(intl, func(i, j int) bool { return intl[i].Importance > intl[j].Importance })
	sort.SliceStable(dom, func(i, j int) bool { return dom[i].Importance > dom[j].Importance })
	out := append([]storage.DigestItem{}, intl...)
	out = append(out, dom...)
	out = append(out, status...)
	return out
}

func buildPost(cfg *config.Config, d storage.Digest, items []storage.DigestItem, level int) (string, int, error) {
	title := d.Title
	if kw := strings.TrimSpace(cfg.Notification.RequiredKeyword); kw != "" && !strings.Contains(title, kw) {
		title = "【" + kw + "】" + title
	}
	var content [][]Node
	intlN, domN := 0, 0
	for _, it := range items {
		if it.Section == editorial.SectionStatus {
			content = append(content, []Node{{Tag: "text", Text: "来源覆盖状态"}})
			if it.TitleZh != "" {
				content = append(content, []Node{{Tag: "text", Text: it.TitleZh}})
			}
			facts := it.Facts
			if level == 2 && len(facts) > 2 {
				facts = facts[:2]
			}
			if level >= 3 && len(facts) > 1 {
				facts = facts[:1]
			}
			for _, f := range facts {
				text := f.Text
				if level >= 2 {
					text = truncate(text, 120)
				}
				content = append(content, []Node{{Tag: "text", Text: text}})
			}
			continue
		}
		label := editorial.SectionLabel(it.Section)
		if it.Section == editorial.SectionInternational && intlN == 0 {
			content = append(content, []Node{{Tag: "text", Text: label}})
		}
		if it.Section == editorial.SectionDomestic && domN == 0 {
			content = append(content, []Node{{Tag: "text", Text: label}})
		}
		if it.Section == editorial.SectionInternational {
			intlN++
		} else if it.Section == editorial.SectionDomestic {
			domN++
		}
		line := fmt.Sprintf("%d. %s：%s", sectionIndex(it, items), it.TitleZh, factsText(it.Facts, level))
		content = append(content, []Node{{Tag: "text", Text: line}})
		if level == 0 {
			if it.Background != "" {
				content = append(content, []Node{{Tag: "text", Text: "背景：" + it.Background}})
			}
			if it.Impact != "" {
				content = append(content, []Node{{Tag: "text", Text: "影响：" + it.Impact}})
			}
		}
		linkRow := []Node{{Tag: "text", Text: "来源："}}
		links := 0
		for _, a := range it.EvidenceSnapshot {
			if !safeHTTPURL(a.URL) {
				continue
			}
			if links > 0 {
				linkRow = append(linkRow, Node{Tag: "text", Text: "、"})
			}
			linkRow = append(linkRow, Node{Tag: "a", Text: a.PublisherID, Href: a.URL})
			links++
		}
		if links > 0 {
			if it.Coverage == "multiple_sources" {
				linkRow = append(linkRow, Node{Tag: "text", Text: "（多家报道，独立性未核实）"})
			} else if it.Coverage == "single_source" {
				linkRow = append(linkRow, Node{Tag: "text", Text: "（单一来源）"})
			}
			content = append(content, linkRow)
		}
	}
	if level <= 2 {
		content = append(content, []Node{{Tag: "text", Text: "根据 RSS 摘要整理，详见原文。"}})
	} else {
		content = append(content, []Node{{Tag: "text", Text: "因长度限制已精简，完整简报保存在本地。"}})
	}
	if len(d.QualityFlags) > 0 && level <= 1 {
		content = append(content, []Node{{Tag: "text", Text: "质量标记：" + strings.Join(d.QualityFlags, "、")}})
	}
	var p Payload
	p.MsgType = "post"
	p.Content.Post.ZhCN = PostBody{Title: title, Content: content}
	raw, err := marshalPayload(p)
	if err != nil {
		return "", 0, err
	}
	return raw, len(content), nil
}

func factsText(facts []storage.Fact, level int) string {
	parts := make([]string, 0, len(facts))
	for i, f := range facts {
		if level >= 2 && i >= 2 {
			break
		}
		t := f.Text
		if level >= 2 {
			t = truncate(t, 100)
		}
		parts = append(parts, t)
	}
	return strings.Join(parts, "；")
}

func sectionIndex(it storage.DigestItem, all []storage.DigestItem) int {
	n := 0
	for _, x := range all {
		if x.Section == it.Section {
			n++
			if x.EventKey == it.EventKey {
				return n
			}
		}
	}
	return n
}

func safeHTTPURL(raw string) bool {
	if raw == "" {
		return false
	}
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	return (u.Scheme == "https" || u.Scheme == "http") && u.Host != ""
}

func truncate(s string, max int) string {
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	r := []rune(s)
	return string(r[:max])
}

func dropLowPriority(items []storage.DigestItem, keepTotal int) []storage.DigestItem {
	var intl, dom, status []storage.DigestItem
	for _, it := range items {
		switch it.Section {
		case editorial.SectionInternational:
			intl = append(intl, it)
		case editorial.SectionDomestic:
			dom = append(dom, it)
		default:
			status = append(status, it)
		}
	}
	remaining := keepTotal
	out := []storage.DigestItem{}
	if len(intl) > 0 && remaining > 1 {
		out = append(out, intl[0])
		intl = intl[1:]
		remaining--
	}
	if len(dom) > 0 && remaining > 1 {
		out = append(out, dom[0])
		dom = dom[1:]
		remaining--
	}
	all := append(append(intl, dom...), status...)
	sort.SliceStable(all, func(i, j int) bool { return all[i].Importance > all[j].Importance })
	for _, it := range all {
		if remaining <= 0 {
			break
		}
		out = append(out, it)
		remaining--
	}
	return out
}

// BuildOutbox renders the payload and creates a stable local outbox row.
func BuildOutbox(cfg *config.Config, d storage.Digest, items []storage.DigestItem,
	revision int, reason string, now int64) (*storage.OutboxItem, []string, error) {
	if !cfg.Notification.Enabled {
		return nil, nil, errors.New("notification.enabled is false")
	}
	payload, flags, err := RenderPayload(cfg, d, items)
	if err != nil {
		return nil, flags, err
	}
	ttl := cfg.Schedule.NotificationTTL.D()
	if ttl <= 0 {
		ttl = 6 * time.Hour
	}
	dedup := dedupKey(d.ID, cfg.Notification.RecipientAlias, cfg.Notification.Provider, 1, revision)
	o := &storage.OutboxItem{
		DigestID:       d.ID,
		DedupKey:       dedup,
		Provider:       cfg.Notification.Provider,
		RecipientAlias: cfg.Notification.RecipientAlias,
		PartNo:         1,
		ResendRevision: revision,
		ResendReason:   reason,
		PayloadJSON:    payload,
		PayloadHash:    normalize.SHA256Hex(payload),
		Status:         storage.OutboxPending,
		ExpiresAt:      now + int64(ttl.Seconds()),
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	return o, flags, nil
}

func dedupKey(digestID, alias, provider string, part, revision int) string {
	pre := fmt.Sprintf("%s|%s|%s|%d|%d", digestID, alias, provider, part, revision)
	sum := sha256.Sum256([]byte(pre))
	return hex.EncodeToString(sum[:])
}

func marshalPayload(p Payload) (string, error) {
	b, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
