package robots

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"search-engine-scraper/internal/fetcher"
)

func allowedPath(t *testing.T, body, agent, target string) bool {
	t.Helper()
	return parse(body, agent).allowed(target)
}

func TestLongestMatchWinsAndAllowWinsTies(t *testing.T) {
	body := "User-agent: *\nDisallow: /private\nAllow: /private/public\nDisallow: /tie\nAllow: /tie\n"
	cases := map[string]bool{
		"/private/secret":      false,
		"/private/public/page": true,
		"/open":                true,
		"/tie":                 true,
	}
	for target, want := range cases {
		if got := allowedPath(t, body, "mybot", target); got != want {
			t.Errorf("%s: allowed = %v, want %v", target, got, want)
		}
	}
}

func TestWildcardsAndEndAnchor(t *testing.T) {
	body := "User-agent: *\nDisallow: /*.pdf$\nDisallow: /search?*q=\n"
	cases := map[string]bool{
		"/docs/file.pdf":      false,
		"/docs/file.pdf?x=1":  true, // "$" anchors the end
		"/search?lang=en&q=x": false,
		"/search":             true,
	}
	for target, want := range cases {
		if got := allowedPath(t, body, "mybot", target); got != want {
			t.Errorf("%s: allowed = %v, want %v", target, got, want)
		}
	}
}

func TestSpecificAgentGroupReplacesWildcard(t *testing.T) {
	body := "User-agent: *\nDisallow: /\n\nUser-agent: MyBot\nDisallow: /admin\n"
	if !allowedPath(t, body, productToken("mybot/1.0 (+https://x)"), "/page") {
		t.Error("the mybot group (case-insensitive product token) should replace the * group")
	}
	if allowedPath(t, body, "mybot", "/admin/x") {
		t.Error("/admin should stay disallowed for mybot")
	}
	if allowedPath(t, body, "otherbot", "/page") {
		t.Error("other agents fall back to the * group")
	}
}

func TestGroupedUserAgentsShareRules(t *testing.T) {
	body := "User-agent: a\nUser-agent: b\nDisallow: /x\n"
	if allowedPath(t, body, "b", "/x") {
		t.Error("grouped agents should share rules")
	}
}

func TestPercentEncodingIsNormalized(t *testing.T) {
	body := "User-agent: *\nDisallow: /%e0%a4%b8\n"
	if allowedPath(t, body, "mybot", "/%E0%A4%B8/page") {
		t.Error("escapes should match regardless of hex case")
	}
}

func TestSitemapsAndCrawlDelay(t *testing.T) {
	body := "Sitemap: https://example.com/a.xml\nUser-agent: *\nCrawl-delay: 1.5\nDisallow: /p\nSitemap: https://example.com/b.xml\nCrawl-delay: nonsense\n"
	rs := parse(body, "mybot")
	if len(rs.sitemaps) != 2 {
		t.Fatalf("sitemaps = %v, want both, regardless of groups", rs.sitemaps)
	}
	if rs.crawlDelay != 1500*time.Millisecond {
		t.Errorf("crawl-delay = %v, want 1.5s (invalid values ignored)", rs.crawlDelay)
	}
	if rs := parse("User-agent: *\nDisallow: /p\n", "mybot"); rs.sitemaps != nil || rs.crawlDelay != 0 {
		t.Errorf("no Sitemap/Crawl-delay lines: got %v / %v", rs.sitemaps, rs.crawlDelay)
	}
}

func newGuardFor(t *testing.T, robotsBody string, status int) (*Guard, string, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/robots.txt" {
			http.NotFound(w, r)
			return
		}
		hits.Add(1)
		w.WriteHeader(status)
		_, _ = w.Write([]byte(robotsBody))
	}))
	t.Cleanup(srv.Close)
	// httptest listens on 127.0.0.1, which the fetcher refuses by default.
	f := fetcher.New(5*time.Second, 0, fetcher.WithAllowPrivateNetworks(true))
	return New(f, "testbot"), srv.URL, &hits
}

func TestGuardHonorsRulesAndCachesTheFile(t *testing.T) {
	g, base, hits := newGuardFor(t, "User-agent: *\nDisallow: /private\nSitemap: https://example.com/sm.xml\n", http.StatusOK)
	ctx := context.Background()
	for target, want := range map[string]bool{"/private/x": false, "/open": true, "/robots.txt": true} {
		got, err := g.Allowed(ctx, base+target)
		if err != nil || got != want {
			t.Errorf("%s: Allowed = %v, %v; want %v", target, got, err, want)
		}
	}
	sitemaps, err := g.Sitemaps(ctx, base+"/")
	if err != nil || len(sitemaps) != 1 || sitemaps[0] != "https://example.com/sm.xml" {
		t.Errorf("Sitemaps = %v, %v", sitemaps, err)
	}
	if n := hits.Load(); n != 1 {
		t.Errorf("robots.txt fetched %d times, want once (cached)", n)
	}
}

func TestGuardMissingRobotsTxtAllowsEverything(t *testing.T) {
	g, base, _ := newGuardFor(t, "", http.StatusNotFound)
	if ok, err := g.Allowed(context.Background(), base+"/anything"); err != nil || !ok {
		t.Errorf("a 404 robots.txt should allow all URLs, got %v, %v", ok, err)
	}
	if sm, err := g.Sitemaps(context.Background(), base+"/"); err != nil || sm != nil {
		t.Errorf("Sitemaps = %v, %v; want nil", sm, err)
	}
}

func TestGuardServerErrorDisallowsTheOriginForNow(t *testing.T) {
	for _, status := range []int{http.StatusServiceUnavailable, http.StatusTooManyRequests} {
		g, base, _ := newGuardFor(t, "", status)
		ok, err := g.Allowed(context.Background(), base+"/page")
		if ok || !errors.Is(err, ErrUnreachable) {
			t.Errorf("status %d: Allowed = %v, %v; want false, ErrUnreachable", status, ok, err)
		}
	}
}

func TestGuardRefusesPrivateOriginsByDefault(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(srv.Close)
	g := New(fetcher.New(time.Second, 0), "testbot")
	ok, err := g.Allowed(context.Background(), srv.URL+"/page")
	if ok || err == nil || !fetcher.IsPermanent(err) {
		t.Errorf("Allowed on 127.0.0.1 = %v, %v; want a permanent refusal", ok, err)
	}
}

func TestGuardInvalidURL(t *testing.T) {
	g := New(fetcher.New(time.Second, 0), "testbot")
	ok, err := g.Allowed(context.Background(), "http://[::1")
	if ok || !errors.Is(err, fetcher.ErrInvalidURL) {
		t.Errorf("Allowed = %v, %v; want false, ErrInvalidURL", ok, err)
	}
}
