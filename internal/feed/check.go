package feed

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/jxrory/newspulse/internal/config"
	"github.com/jxrory/newspulse/internal/logging"
	"github.com/jxrory/newspulse/internal/normalize"
)

type LinkCheck struct {
	URL    string `json:"url"`
	Status int    `json:"status,omitempty"`
	Error  string `json:"error,omitempty"`
}

type CheckResult struct {
	ID             string      `json:"id"`
	URL            string      `json:"url"`
	Status         int         `json:"status"`
	FinalURL       string      `json:"final_url"`
	FeedType       string      `json:"feed_type"`
	Bytes          int64       `json:"bytes"`
	Items          int         `json:"items"`
	ParsedDates    int         `json:"parsed_dates"`
	DateRate       float64     `json:"date_rate"`
	Latest         string      `json:"latest,omitempty"`
	Recent24h      int         `json:"recent_24h"`
	FutureDates    int         `json:"future_dates"`
	SampleLinks    []LinkCheck `json:"sample_links,omitempty"`
	Error          string      `json:"error,omitempty"`
	DurationMillis int64       `json:"duration_ms"`
}

// CheckSource performs a real network check from this machine (or the VPS it
// runs on). It deliberately does not write any database state.
func (s *Service) CheckSource(ctx context.Context, src config.SourceConfig, probeLinks int) CheckResult {
	started := s.clock.Now()
	res := CheckResult{ID: src.ID, URL: src.URL}
	resp, err := s.fetcher.Fetch(ctx, src, "", "")
	res.DurationMillis = time.Since(started).Milliseconds()
	if err != nil {
		res.Error = logging.ErrorString(err)
		var fe *FetchError
		if errors.As(err, &fe) {
			res.Status = fe.HTTPStatus
		}
		return res
	}
	res.Status = resp.StatusCode
	res.FinalURL = resp.FinalURL
	res.Bytes = resp.Bytes
	if resp.NotModified {
		return res
	}
	feedObj, perr := s.fetcher.Parse(resp.Body)
	if perr != nil {
		res.Error = logging.ErrorString(perr)
		return res
	}
	res.FeedType = feedObj.FeedType
	var tz *time.Location
	if src.TimezoneHint != "" {
		tz, _ = time.LoadLocation(src.TimezoneHint)
	}
	now := s.clock.Now()
	var latest *time.Time
	cutoff := now.Add(-24 * time.Hour)
	probe := 0
	for _, it := range feedObj.Items {
		if it == nil {
			continue
		}
		res.Items++
		raw := it.Published
		if raw == "" {
			raw = it.Updated
		}
		unix, quality := normalize.ParsePublished(raw, tz, now, s.cfg.Fetch.FutureTolerance.D())
		if unix != nil {
			res.ParsedDates++
		}
		if quality == "future_date" {
			res.FutureDates++
		}
		if unix != nil {
			t := time.Unix(*unix, 0)
			if !t.Before(cutoff) && !t.After(now.Add(s.cfg.Fetch.FutureTolerance.D())) {
				res.Recent24h++
			}
			if latest == nil || t.After(*latest) {
				latest = &t
			}
		}
		if probe < probeLinks {
			link := it.Link
			if link == "" && len(it.Links) > 0 {
				link = it.Links[0]
			}
			if link != "" {
				status, lerr := s.fetcher.Probe(ctx, link)
				lc := LinkCheck{URL: link, Status: status}
				if lerr != nil {
					lc.Error = logging.ErrorString(lerr)
				}
				res.SampleLinks = append(res.SampleLinks, lc)
				probe++
			}
		}
	}
	if res.Items > 0 {
		res.DateRate = float64(res.ParsedDates) / float64(res.Items)
	}
	if latest != nil {
		res.Latest = latest.In(s.cfg.DisplayLocation()).Format(time.RFC3339)
	}
	res.DurationMillis = time.Since(started).Milliseconds()
	return res
}

func (f *Fetcher) Probe(ctx context.Context, rawURL string) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, rawURL, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("User-Agent", f.cfg.Fetch.UserAgent)
	resp, err := f.client.Do(req)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusForbidden {
		req, err = http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return resp.StatusCode, err
		}
		req.Header.Set("User-Agent", f.cfg.Fetch.UserAgent)
		resp2, err := f.client.Do(req)
		if err != nil {
			return resp.StatusCode, nil
		}
		resp2.Body.Close()
		return resp2.StatusCode, nil
	}
	return resp.StatusCode, nil
}
