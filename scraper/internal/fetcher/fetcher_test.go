package fetcher

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestGet_FollowsRedirectsAndRecordsFinalURLAndChain(t *testing.T) {
	mux := http.NewServeMux()
	var baseURL string
	mux.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, baseURL+"/middle", http.StatusMovedPermanently)
	})
	mux.HandleFunc("/middle", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, baseURL+"/end", http.StatusFound)
	})
	mux.HandleFunc("/end", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("done"))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	baseURL = srv.URL

	f := New(5*time.Second, 0, WithAllowPrivateNetworks(true))
	res, err := f.Get(context.Background(), srv.URL+"/start")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if res.FinalURL != srv.URL+"/end" {
		t.Errorf("FinalURL = %q, want %q", res.FinalURL, srv.URL+"/end")
	}
	wantChain := []string{srv.URL + "/middle", srv.URL + "/end"}
	if len(res.RedirectChain) != len(wantChain) {
		t.Fatalf("RedirectChain = %v, want %v", res.RedirectChain, wantChain)
	}
	for i := range wantChain {
		if res.RedirectChain[i] != wantChain[i] {
			t.Errorf("RedirectChain[%d] = %q, want %q", i, res.RedirectChain[i], wantChain[i])
		}
	}
}

func TestGet_NoRedirect_FinalURLMatchesRequestedAndChainEmpty(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	f := New(5*time.Second, 0, WithAllowPrivateNetworks(true))
	res, err := f.Get(context.Background(), srv.URL+"/page")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if res.FinalURL != srv.URL+"/page" {
		t.Errorf("FinalURL = %q, want %q", res.FinalURL, srv.URL+"/page")
	}
	if len(res.RedirectChain) != 0 {
		t.Errorf("RedirectChain = %v, want empty", res.RedirectChain)
	}
}

// TestGet_ConcurrentRedirectsDontCrossContaminate proves the per-Get()
// redirect-chain accumulator is genuinely request-scoped: many concurrent
// Get() calls to different redirect chains on the same *Fetcher (same
// underlying *http.Client, same shared CheckRedirect closure) must each
// see only their own hops, never another goroutine's.
func TestGet_ConcurrentRedirectsDontCrossContaminate(t *testing.T) {
	mux := http.NewServeMux()
	var baseURL string
	const n = 20
	// Build n independent 2-hop redirect chains: /startI -> /midI -> /endI.
	for i := 0; i < n; i++ {
		i := i
		mux.HandleFunc(pathFor(i, "start"), func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, baseURL+pathFor(i, "mid"), http.StatusMovedPermanently)
		})
		mux.HandleFunc(pathFor(i, "mid"), func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, baseURL+pathFor(i, "end"), http.StatusFound)
		})
		mux.HandleFunc(pathFor(i, "end"), func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte("done"))
		})
	}
	srv := httptest.NewServer(mux)
	defer srv.Close()
	baseURL = srv.URL

	f := New(5*time.Second, 0, WithAllowPrivateNetworks(true))
	var wg sync.WaitGroup
	errs := make([]error, n)
	chains := make([][]string, n)
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := f.Get(context.Background(), srv.URL+pathFor(i, "start"))
			if err != nil {
				errs[i] = err
				return
			}
			chains[i] = res.RedirectChain
		}()
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("chain %d: Get: %v", i, errs[i])
		}
		want := []string{srv.URL + pathFor(i, "mid"), srv.URL + pathFor(i, "end")}
		got := chains[i]
		if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
			t.Errorf("chain %d = %v, want %v (cross-contamination if this doesn't match)", i, got, want)
		}
	}
}

func pathFor(i int, stage string) string {
	return "/" + stage + "-" + strconv.Itoa(i)
}

// TestDNSCache_ReusesResultWithinTTL proves repeated lookups of the same
// host within the TTL window hit the cache instead of calling resolve
// again -- the whole point of caching DNS across a crawl's many
// same-host requests.
func TestDNSCache_ReusesResultWithinTTL(t *testing.T) {
	var calls int
	c := newDNSCache(time.Minute)
	c.resolve = func(ctx context.Context, host string) ([]string, error) {
		calls++
		return []string{"93.184.216.34"}, nil
	}

	for i := 0; i < 5; i++ {
		addrs, err := c.lookup(context.Background(), "example.com")
		if err != nil {
			t.Fatalf("lookup: %v", err)
		}
		if len(addrs) != 1 || addrs[0] != "93.184.216.34" {
			t.Fatalf("lookup = %v, want [93.184.216.34]", addrs)
		}
	}
	if calls != 1 {
		t.Errorf("resolve called %d times, want 1 (later lookups should hit cache)", calls)
	}
}

// TestDNSCache_ReResolvesAfterExpiry proves a stale entry is refreshed
// rather than reused forever, so a host's IP change eventually propagates.
func TestDNSCache_ReResolvesAfterExpiry(t *testing.T) {
	var calls int
	c := newDNSCache(0) // expires immediately
	c.resolve = func(ctx context.Context, host string) ([]string, error) {
		calls++
		return []string{"93.184.216.34"}, nil
	}

	if _, err := c.lookup(context.Background(), "example.com"); err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if _, err := c.lookup(context.Background(), "example.com"); err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if calls != 2 {
		t.Errorf("resolve called %d times, want 2 (expired entry should be re-resolved)", calls)
	}
}

// TestGet_UsesDNSCacheDialContext proves a Fetcher built by New still
// fetches successfully -- the DNS-caching DialContext falls back to plain
// dialing for an IP-literal host (as httptest servers are), so it must not
// break ordinary requests.
func TestGet_UsesDNSCacheDialContext(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	f := New(5*time.Second, 0, WithAllowPrivateNetworks(true))
	if _, err := f.Get(context.Background(), srv.URL); err != nil {
		t.Fatalf("Get: %v", err)
	}
}

// stubRoundTripper is a fake http.RoundTripper for testing
// http3FallbackTransport without real QUIC/TLS dialing.
type stubRoundTripper struct {
	resp   *http.Response
	err    error
	called *bool
}

func (s stubRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	if s.called != nil {
		*s.called = true
	}
	return s.resp, s.err
}

func fakeResponse() *http.Response {
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}
}

// TestHTTP3FallbackTransport_FallsBackWhenH3Fails proves an https request
// is retried on the fallback transport when the HTTP/3 attempt errors --
// the common case, since most sites don't speak HTTP/3 yet.
func TestHTTP3FallbackTransport_FallsBackWhenH3Fails(t *testing.T) {
	var h3Called, fallbackCalled bool
	rt := &http3FallbackTransport{
		h3:       stubRoundTripper{err: errors.New("no h3 support"), called: &h3Called},
		fallback: stubRoundTripper{resp: fakeResponse(), called: &fallbackCalled},
	}

	req, _ := http.NewRequest(http.MethodGet, "https://example.com", nil)
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if !h3Called || !fallbackCalled {
		t.Errorf("h3Called=%v fallbackCalled=%v, want both true", h3Called, fallbackCalled)
	}
}

// TestHTTP3FallbackTransport_UsesH3WhenItSucceeds proves the fallback
// transport is never invoked once HTTP/3 succeeds.
func TestHTTP3FallbackTransport_UsesH3WhenItSucceeds(t *testing.T) {
	var fallbackCalled bool
	rt := &http3FallbackTransport{
		h3:       stubRoundTripper{resp: fakeResponse()},
		fallback: stubRoundTripper{err: errors.New("should not be called"), called: &fallbackCalled},
	}

	req, _ := http.NewRequest(http.MethodGet, "https://example.com", nil)
	if _, err := rt.RoundTrip(req); err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	if fallbackCalled {
		t.Error("fallback transport was called even though h3 succeeded")
	}
}

// TestHTTP3FallbackTransport_SkipsH3ForPlainHTTP proves a plain-http
// request goes straight to the fallback transport -- QUIC/HTTP-3 is
// https-only, so there's no point attempting it.
func TestHTTP3FallbackTransport_SkipsH3ForPlainHTTP(t *testing.T) {
	var h3Called bool
	rt := &http3FallbackTransport{
		h3:       stubRoundTripper{err: errors.New("should not be called"), called: &h3Called},
		fallback: stubRoundTripper{resp: fakeResponse()},
	}

	req, _ := http.NewRequest(http.MethodGet, "http://example.com", nil)
	if _, err := rt.RoundTrip(req); err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	if h3Called {
		t.Error("h3 transport was called for a plain-http request")
	}
}

// TestGet_BandwidthLimit_ThrottlesAggregateRate proves a Fetcher built with
// a bandwidth cap makes fetching a body noticeably larger than one second's
// budget take a perceptible amount of wall-clock time -- otherwise a bug
// that ignores the limiter entirely would still pass.
func TestGet_BandwidthLimit_ThrottlesAggregateRate(t *testing.T) {
	const bandwidthBPS = 500          // bytes/sec: also the limiter's burst (see New)
	const bodySize = bandwidthBPS * 2 // 2 chunks: 1 free from burst, 1 must wait ~1s
	body := make([]byte, bodySize)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(body)
	}))
	defer srv.Close()

	f := New(5*time.Second, bandwidthBPS, WithAllowPrivateNetworks(true))

	start := time.Now()
	if _, err := f.Get(context.Background(), srv.URL); err != nil {
		t.Fatalf("Get: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed < 700*time.Millisecond {
		t.Errorf("fetching a %d-byte body at %d bytes/sec took %v, want throttling to take noticeably longer (~1s)", bodySize, bandwidthBPS, elapsed)
	}
}
