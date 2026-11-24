package feed

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/jxrory/newspulse/internal/config"
	"github.com/jxrory/newspulse/internal/logging"
	"github.com/mmcdole/gofeed"
)

type Fetcher struct {
	cfg    *config.Config
	client *http.Client
	parser *gofeed.Parser
}

type FetchResponse struct {
	StatusCode   int
	NotModified  bool
	Body         []byte
	ETag         string
	LastModified string
	FinalURL     string
	Bytes        int64
	ContentType  string
	RetryAfter   time.Duration
}

type FetchError struct {
	Code       string
	HTTPStatus int
	RetryAfter time.Duration
	Message    string
}

func (e *FetchError) Error() string { return e.Message }

func NewFetcher(cfg *config.Config) *Fetcher {
	timeout := cfg.Fetch.RequestTimeout.D()
	connect := cfg.Fetch.ConnectTimeout.D()
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   connect,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout: connect,
		MaxIdleConns:        16,
		MaxIdleConnsPerHost: 2,
		IdleConnTimeout:     90 * time.Second,
	}
	maxRedirects := cfg.Fetch.MaxRedirects
	hc := &http.Client{
		Timeout:   timeout,
		Transport: tr,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= maxRedirects {
				return errors.New("too many redirects")
			}
			return nil
		},
	}
	p := gofeed.NewParser()
	return &Fetcher{cfg: cfg, client: hc, parser: p}
}

func (f *Fetcher) Fetch(ctx context.Context, src config.SourceConfig, etag, lastModified string) (*FetchResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, f.cfg.Fetch.RequestTimeout.D())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, src.URL, nil)
	if err != nil {
		return nil, &FetchError{Code: "bad_request", Message: logging.Redact(err.Error())}
	}
	req.Header.Set("User-Agent", f.cfg.Fetch.UserAgent)
	req.Header.Set("Accept", "application/rss+xml, application/atom+xml, application/xml, text/xml;q=0.9, */*;q=0.5")
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	if lastModified != "" {
		req.Header.Set("If-Modified-Since", lastModified)
	}
	resp, err := f.client.Do(req)
	if err != nil {
		code := "network_error"
		if errors.Is(err, context.DeadlineExceeded) {
			code = "timeout"
		}
		return nil, &FetchError{Code: code, Message: logging.ErrorString(err)}
	}
	defer resp.Body.Close()

	out := &FetchResponse{
		StatusCode:   resp.StatusCode,
		ETag:         resp.Header.Get("ETag"),
		LastModified: resp.Header.Get("Last-Modified"),
		FinalURL:     resp.Request.URL.String(),
		ContentType:  resp.Header.Get("Content-Type"),
	}
	if resp.StatusCode == http.StatusNotModified {
		out.NotModified = true
		return out, nil
	}
	if resp.StatusCode >= 300 {
		out.RetryAfter = parseRetryAfter(resp.Header.Get("Retry-After"))
		return out, &FetchError{
			Code:       fmt.Sprintf("http_%d", resp.StatusCode),
			HTTPStatus: resp.StatusCode,
			RetryAfter: out.RetryAfter,
			Message:    fmt.Sprintf("unexpected HTTP status %d", resp.StatusCode),
		}
	}
	limited := io.LimitReader(resp.Body, f.cfg.Fetch.MaxResponseBytes+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, &FetchError{Code: "body_read_error", Message: logging.ErrorString(err)}
	}
	out.Bytes = int64(len(body))
	if int64(len(body)) > f.cfg.Fetch.MaxResponseBytes {
		return nil, &FetchError{Code: "response_too_large", Message: "response exceeds configured limit"}
	}
	out.Body = body
	return out, nil
}

func parseRetryAfter(raw string) time.Duration {
	if raw == "" {
		return 0
	}
	if secs, err := time.ParseDuration(strings.TrimSpace(raw) + "s"); err == nil {
		return secs
	}
	if t, err := http.ParseTime(raw); err == nil {
		d := time.Until(t)
		if d > 0 {
			return d
		}
	}
	return 0
}

func (f *Fetcher) Parse(body []byte) (*gofeed.Feed, error) {
	feed, err := f.parser.Parse(strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	if feed == nil {
		return nil, errors.New("parser returned nil feed")
	}
	return feed, nil
}
