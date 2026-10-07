package activities

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"search-engine-scraper/internal/fetcher"
	"search-engine-scraper/internal/metrics"
	"search-engine-scraper/internal/robots"
)

func newTestActivities(mux *http.ServeMux) (*Activities, *httptest.Server) {
	srv := httptest.NewServer(mux)
	// httptest listens on 127.0.0.1, which the fetcher refuses by default.
	f := fetcher.New(5*time.Second, 0, fetcher.WithAllowPrivateNetworks(true))
	guard := robots.New(f, "testbot")
	return New(f, guard, nil, nil, nil), srv
}

func TestDiscoverSitemapURLs_FollowsSitemapIndexFromRobotsTxt(t *testing.T) {
	mux := http.NewServeMux()
	var baseURL string
	mux.HandleFunc("/robots.txt", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Sitemap: " + baseURL + "/sitemap-index.xml\n"))
	})
	mux.HandleFunc("/sitemap-index.xml", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.Write([]byte(`<sitemapindex>
			<sitemap><loc>` + baseURL + `/sitemap-1.xml</loc></sitemap>
			<sitemap><loc>` + baseURL + `/sitemap-2.xml</loc></sitemap>
		</sitemapindex>`))
	})
	mux.HandleFunc("/sitemap-1.xml", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.Write([]byte(`<urlset><url><loc>` + baseURL + `/a</loc></url></urlset>`))
	})
	mux.HandleFunc("/sitemap-2.xml", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.Write([]byte(`<urlset><url><loc>` + baseURL + `/b</loc></url></urlset>`))
	})

	a, srv := newTestActivities(mux)
	defer srv.Close()
	baseURL = srv.URL

	out, err := a.DiscoverSitemapURLs(context.Background(), DiscoverSitemapURLsInput{SeedURL: srv.URL + "/"})
	if err != nil {
		t.Fatalf("DiscoverSitemapURLs: %v", err)
	}

	got := append([]string(nil), out.URLs...)
	sort.Strings(got)
	want := []string{srv.URL + "/a", srv.URL + "/b"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("URLs = %v, want %v", got, want)
	}
}

func TestDiscoverSitemapURLs_FallsBackToDefaultSitemapPath(t *testing.T) {
	mux := http.NewServeMux()
	// No robots.txt at all (404) -- must fall back to the conventional
	// /sitemap.xml location.
	var baseURL string
	mux.HandleFunc("/sitemap.xml", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.Write([]byte(`<urlset><url><loc>` + baseURL + `/only</loc></url></urlset>`))
	})

	a, srv := newTestActivities(mux)
	defer srv.Close()
	baseURL = srv.URL

	out, err := a.DiscoverSitemapURLs(context.Background(), DiscoverSitemapURLsInput{SeedURL: srv.URL + "/"})
	if err != nil {
		t.Fatalf("DiscoverSitemapURLs: %v", err)
	}
	if len(out.URLs) != 1 || out.URLs[0] != srv.URL+"/only" {
		t.Errorf("URLs = %v, want [%s]", out.URLs, srv.URL+"/only")
	}
}

func TestDiscoverSitemapURLs_BadSeedURLYieldsEmptyResultNotError(t *testing.T) {
	a, srv := newTestActivities(http.NewServeMux())
	defer srv.Close()

	out, err := a.DiscoverSitemapURLs(context.Background(), DiscoverSitemapURLsInput{SeedURL: "not a url"})
	if err != nil {
		t.Fatalf("DiscoverSitemapURLs: %v, want nil error (best-effort)", err)
	}
	if len(out.URLs) != 0 {
		t.Errorf("URLs = %v, want none", out.URLs)
	}
}

func TestDiscoverSitemapURLs_NoSitemapAnywhereYieldsEmptyResultNotError(t *testing.T) {
	// Neither robots.txt nor /sitemap.xml exist -- everything 404s.
	a, srv := newTestActivities(http.NewServeMux())
	defer srv.Close()

	out, err := a.DiscoverSitemapURLs(context.Background(), DiscoverSitemapURLsInput{SeedURL: srv.URL + "/"})
	if err != nil {
		t.Fatalf("DiscoverSitemapURLs: %v, want nil error (best-effort)", err)
	}
	if len(out.URLs) != 0 {
		t.Errorf("URLs = %v, want none", out.URLs)
	}
}

func TestProcessPage_NoIndexNoFollow(t *testing.T) {
	tests := []struct {
		name         string
		body         string
		xRobotsTag   string
		wantNoIndex  bool
		wantNoFollow bool
	}{
		{
			name:        "header noindex, no meta tag",
			body:        `<html><body>page</body></html>`,
			xRobotsTag:  "noindex",
			wantNoIndex: true,
		},
		{
			name:        "bot-prefixed header directive",
			body:        `<html><body>page</body></html>`,
			xRobotsTag:  "googlebot: noindex",
			wantNoIndex: true,
		},
		{
			name:         "meta nofollow, no header",
			body:         `<html><head><meta name="robots" content="nofollow"></head><body>page</body></html>`,
			wantNoFollow: true,
		},
		{
			name:         "meta noindex combined with header nofollow",
			body:         `<html><head><meta name="robots" content="noindex"></head><body>page</body></html>`,
			xRobotsTag:   "nofollow",
			wantNoIndex:  true,
			wantNoFollow: true,
		},
		{
			name: "neither present",
			body: `<html><body>page</body></html>`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("/page", func(w http.ResponseWriter, r *http.Request) {
				if tt.xRobotsTag != "" {
					w.Header().Set("X-Robots-Tag", tt.xRobotsTag)
				}
				w.Header().Set("Content-Type", "text/html")
				w.Write([]byte(tt.body))
			})
			a, srv := newTestActivities(mux)
			defer srv.Close()

			out, err := a.ProcessPage(context.Background(), ProcessPageInput{URL: srv.URL + "/page"})
			if err != nil {
				t.Fatalf("ProcessPage: %v", err)
			}
			if out.NoIndex != tt.wantNoIndex {
				t.Errorf("NoIndex = %v, want %v", out.NoIndex, tt.wantNoIndex)
			}
			if out.NoFollow != tt.wantNoFollow {
				t.Errorf("NoFollow = %v, want %v", out.NoFollow, tt.wantNoFollow)
			}
		})
	}
}

func TestProcessPage_FollowsRedirectAndUpdatesNormalizedURL(t *testing.T) {
	mux := http.NewServeMux()
	var baseURL string
	mux.HandleFunc("/old", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, baseURL+"/new", http.StatusMovedPermanently)
	})
	mux.HandleFunc("/new", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(`<html><body>page</body></html>`))
	})
	a, srv := newTestActivities(mux)
	defer srv.Close()
	baseURL = srv.URL

	out, err := a.ProcessPage(context.Background(), ProcessPageInput{URL: srv.URL + "/old"})
	if err != nil {
		t.Fatalf("ProcessPage: %v", err)
	}

	if out.URL != srv.URL+"/old" {
		t.Errorf("URL = %q, want %q (the originally requested URL, unchanged)", out.URL, srv.URL+"/old")
	}
	if out.FinalURL != srv.URL+"/new" {
		t.Errorf("FinalURL = %q, want %q", out.FinalURL, srv.URL+"/new")
	}
	if out.NormalizedURL != srv.URL+"/new" {
		t.Errorf("NormalizedURL = %q, want %q (dedupe identity should follow the redirect)", out.NormalizedURL, srv.URL+"/new")
	}
}

func TestProcessPage_NoRedirect_FinalURLEmpty(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/page", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(`<html><body>page</body></html>`))
	})
	a, srv := newTestActivities(mux)
	defer srv.Close()

	out, err := a.ProcessPage(context.Background(), ProcessPageInput{URL: srv.URL + "/page"})
	if err != nil {
		t.Fatalf("ProcessPage: %v", err)
	}
	if out.FinalURL != "" {
		t.Errorf("FinalURL = %q, want empty (no redirect happened)", out.FinalURL)
	}
	if out.NormalizedURL != srv.URL+"/page" {
		t.Errorf("NormalizedURL = %q, want %q", out.NormalizedURL, srv.URL+"/page")
	}
}

func TestProcessPage_RecordsMetricsPerOutcome(t *testing.T) {
	metrics.PagesFetched.Reset()
	metrics.RateLimited.Reset()

	mux := http.NewServeMux()
	mux.HandleFunc("/ok", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(`<html><body>page</body></html>`))
	})
	mux.HandleFunc("/notfound", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	mux.HandleFunc("/boom", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	mux.HandleFunc("/slowdown", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	})
	mux.HandleFunc("/image.png", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.Write([]byte("not really an image"))
	})
	a, srv := newTestActivities(mux)
	defer srv.Close()

	paths := []string{"/ok", "/notfound", "/boom", "/slowdown", "/image.png"}
	for _, p := range paths {
		if _, err := a.ProcessPage(context.Background(), ProcessPageInput{URL: srv.URL + p}); err != nil {
			// 500/429 responses are returned as errors by design (so
			// Temporal retries them) -- that's expected here, not a test
			// failure; we're only checking that metrics were recorded.
			continue
		}
	}

	srvURL, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse server URL: %v", err)
	}
	host := srvURL.Hostname()

	cases := []struct {
		outcome string
		want    float64
	}{
		{metrics.OutcomeSuccess, 1},
		{metrics.OutcomeClientError, 1},
		{metrics.OutcomeServerError, 1},
		{metrics.OutcomeRateLimited, 1},
		{metrics.OutcomeSkippedNonHTML, 1},
	}
	for _, c := range cases {
		if got := testutil.ToFloat64(metrics.PagesFetched.WithLabelValues(c.outcome)); got != c.want {
			t.Errorf("PagesFetched[%s] = %v, want %v", c.outcome, got, c.want)
		}
	}
	if got := testutil.ToFloat64(metrics.RateLimited.WithLabelValues(host)); got != 1 {
		t.Errorf("RateLimited[%s] = %v, want 1", host, got)
	}
}

// fakeFreshnessChecker is a test double for storage.FreshnessChecker that
// records the `since` cutoff it was called with, so tests can verify
// CheckFreshness's MaxAge -> since conversion without depending on wall
// clock timing tolerances.
type fakeFreshnessChecker struct {
	fresh       map[string]bool
	calledSince time.Time
}

func (f *fakeFreshnessChecker) FreshURLs(ctx context.Context, normalizedURLs []string, since time.Time) (map[string]bool, error) {
	f.calledSince = since
	return f.fresh, nil
}

func TestCheckFreshness_DelegatesAndConvertsMaxAgeToSince(t *testing.T) {
	fake := &fakeFreshnessChecker{fresh: map[string]bool{"https://example.com/a": true}}
	a := New(nil, nil, nil, nil, fake)

	before := time.Now().Add(-time.Hour)
	out, err := a.CheckFreshness(context.Background(), CheckFreshnessInput{
		NormalizedURLs: []string{"https://example.com/a", "https://example.com/b"},
		MaxAge:         time.Hour,
	})
	after := time.Now().Add(-time.Hour)

	if err != nil {
		t.Fatalf("CheckFreshness: %v", err)
	}
	if !out.Fresh["https://example.com/a"] {
		t.Errorf("Fresh = %v, want a.example.com marked fresh", out.Fresh)
	}
	if fake.calledSince.Before(before) || fake.calledSince.After(after) {
		t.Errorf("since = %v, want between %v and %v (now - MaxAge)", fake.calledSince, before, after)
	}
}

func TestCheckFreshness_EmptyInputSkipsChecker(t *testing.T) {
	fake := &fakeFreshnessChecker{}
	a := New(nil, nil, nil, nil, fake)

	out, err := a.CheckFreshness(context.Background(), CheckFreshnessInput{MaxAge: time.Hour})
	if err != nil {
		t.Fatalf("CheckFreshness: %v", err)
	}
	if out.Fresh != nil {
		t.Errorf("Fresh = %v, want nil for empty input", out.Fresh)
	}
	if !fake.calledSince.IsZero() {
		t.Error("FreshURLs should not have been called for empty input")
	}
}
