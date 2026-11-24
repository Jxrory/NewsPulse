package feed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jxrory/newspulse/internal/config"
	"github.com/jxrory/newspulse/internal/storage"
)

type IngestResult struct {
	Inserted   int
	Updated    int
	Rejected   int
	Rejections []Rejection
}

type refKey struct {
	Year int
	ID   string
}

type aliasIndex map[string][]struct {
	ref   refKey
	value string
}

// IngestItems performs the deterministic identity flow:
// normalize -> alias lookup across all shards -> upsert in the owning shard.
// The caller must hold the manager identity lock.
func (s *Service) IngestItems(ctx context.Context, src config.SourceConfig, items []ParsedItem, observedAt int64) (IngestResult, error) {
	var res IngestResult
	if len(items) == 0 {
		return res, nil
	}
	hashes := make([]string, 0, len(items)*2)
	for _, it := range items {
		for _, a := range it.Aliases {
			hashes = append(hashes, a.Hash)
		}
	}
	hits, err := s.mgr.LookupAliasesAllShards(ctx, hashes)
	if err != nil {
		return res, fmt.Errorf("lookup aliases: %w", err)
	}
	index := aliasIndex{}
	for _, h := range hits {
		index[h.AliasHash] = append(index[h.AliasHash], struct {
			ref   refKey
			value string
		}{refKey{h.Year, h.ArticleID}, h.AliasValue})
	}

	// Sort by published time so older items are inserted first; this reduces
	// duplicate-in-batch churn when the feed lists the same URL twice.
	sort.SliceStable(items, func(i, j int) bool {
		a, b := items[i].Article.PublishedAt, items[j].Article.PublishedAt
		if a == nil {
			return false
		}
		if b == nil {
			return true
		}
		return *a < *b
	})

	for _, it := range items {
		refs := map[refKey]bool{}
		for _, a := range it.Aliases {
			for _, e := range index[a.Hash] {
				if e.value != a.Value {
					// A SHA-256 collision must not merge unrelated news.
					continue
				}
				refs[e.ref] = true
			}
		}
		switch len(refs) {
		case 0:
			shard, err := s.mgr.OpenForWrite(time.Unix(observedAt, 0))
			if err != nil {
				return res, err
			}
			bundle := storage.ArticleBundle{
				Article:      it.Article,
				Aliases:      it.Aliases,
				Observation:  it.Observation,
				SnapshotJSON: revisionSnapshot(it.Article, it.RawURL),
			}
			err = shard.InsertArticleBundle(ctx, bundle)
			if err != nil {
				if errors.Is(err, storage.ErrAliasConflict) {
					res.Rejected++
					res.Rejections = append(res.Rejections, Rejection{Key: it.Observation.SourceItemKey, Reason: "alias_conflict"})
					continue
				}
				return res, fmt.Errorf("insert article: %w", err)
			}
			res.Inserted++
			for _, a := range it.Aliases {
				index[a.Hash] = append(index[a.Hash], struct {
					ref   refKey
					value string
				}{refKey{shard.Year, it.Article.ID}, a.Value})
			}
		case 1:
			var owner refKey
			for k := range refs {
				owner = k
			}
			shard, err := s.mgr.Open(owner.Year)
			if err != nil {
				return res, err
			}
			existing, err := shard.GetArticle(ctx, owner.ID)
			if err != nil {
				return res, err
			}
			if existing == nil {
				return res, fmt.Errorf("alias references missing article %d/%s", owner.Year, owner.ID)
			}
			merged := mergeArticle(*existing, it.Article)
			bundle := storage.ArticleBundle{
				Article:     merged,
				Aliases:     it.Aliases,
				Observation: it.Observation,
			}
			bundle.Observation.ArticleID = existing.ID
			if existing.ContentHash != it.Article.ContentHash {
				bundle.SnapshotJSON = revisionSnapshot(merged, it.RawURL)
			}
			if err := shard.UpdateArticleBundle(ctx, bundle); err != nil {
				if errors.Is(err, storage.ErrAliasConflict) {
					res.Rejected++
					res.Rejections = append(res.Rejections, Rejection{Key: it.Observation.SourceItemKey, Reason: "alias_conflict"})
					continue
				}
				return res, fmt.Errorf("update article: %w", err)
			}
			res.Updated++
		default:
			res.Rejected++
			res.Rejections = append(res.Rejections, Rejection{Key: it.Observation.SourceItemKey, Reason: "identity_conflict"})
		}
	}
	return res, nil
}

func mergeArticle(old, incoming storage.Article) storage.Article {
	merged := old
	if incoming.CanonicalURL != "" {
		merged.CanonicalURL = incoming.CanonicalURL
	}
	merged.Title = incoming.Title
	merged.NormalizedTitle = incoming.NormalizedTitle
	merged.SummaryText = incoming.SummaryText
	merged.ContentText = incoming.ContentText
	merged.ContentHash = incoming.ContentHash
	merged.Language = incoming.Language
	merged.Section = incoming.Section
	merged.PublisherID = incoming.PublisherID
	merged.PublisherGroup = incoming.PublisherGroup
	merged.LastSeenAt = incoming.LastSeenAt
	if old.PublishedAt == nil && incoming.PublishedAt != nil {
		merged.PublishedAt = incoming.PublishedAt
		merged.PublishedAtRaw = incoming.PublishedAtRaw
		merged.DateQuality = incoming.DateQuality
	}
	if incoming.UpdatedAt != nil && (merged.UpdatedAt == nil || *incoming.UpdatedAt > *merged.UpdatedAt) {
		merged.UpdatedAt = incoming.UpdatedAt
	}
	merged.QualityFlags = mergeFlags(old.QualityFlags, incoming.QualityFlags)
	return merged
}

func mergeFlags(a, b []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(a)+len(b))
	for _, v := range append(append([]string{}, a...), b...) {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}

func revisionSnapshot(a storage.Article, rawURL string) string {
	snap := map[string]interface{}{
		"title":        a.Title,
		"summary":      a.SummaryText,
		"content":      a.ContentText,
		"content_hash": a.ContentHash,
		"url":          a.CanonicalURL,
		"language":     a.Language,
		"section":      a.Section,
	}
	b, err := json.Marshal(snap)
	if err != nil {
		return "{}"
	}
	return string(b)
}
