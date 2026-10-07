package fetcher

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"search-engine-scraper/internal/netguard"
)

func okServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte("<html>ok</html>"))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// By default the fetcher must refuse a server on 127.0.0.1 -- the httptest
// server stands in for any internal service (postgres, kafka, s3, the cloud
// metadata endpoint, ...) a crawled URL could point at.
func TestFetch_RefusesLoopbackByDefault(t *testing.T) {
	srv := okServer(t)
	f := New(5*time.Second, 0)

	_, err := f.Get(context.Background(), srv.URL+"/page")
	if !netguard.IsBlocked(err) {
		t.Fatalf("Get(%s) error = %v, want a netguard.BlockedError", srv.URL, err)
	}
	if !IsPermanent(err) {
		t.Errorf("IsPermanent(%v) = false, want a refused address to be permanent", err)
	}
}

// A hostname that resolves to loopback is refused at dial time, after DNS --
// the check the IP-literal shortcut can't make.
func TestFetch_RefusesHostnameResolvingToLoopback(t *testing.T) {
	srv := okServer(t)
	u, _ := url.Parse(srv.URL)
	f := New(5*time.Second, 0)

	_, err := f.Get(context.Background(), "http://localhost:"+u.Port()+"/page")
	if !netguard.IsBlocked(err) {
		t.Fatalf("Get(localhost) error = %v, want a netguard.BlockedError", err)
	}
}

func TestFetch_AllowPrivateNetworksOverride(t *testing.T) {
	srv := okServer(t)
	f := New(5*time.Second, 0, WithAllowPrivateNetworks(true))

	res, err := f.Get(context.Background(), srv.URL+"/page")
	if err != nil {
		t.Fatalf("Get with the override: %v", err)
	}
	if res.StatusCode != http.StatusOK || string(res.Body) != "<html>ok</html>" {
		t.Errorf("status=%d body=%q", res.StatusCode, res.Body)
	}
}

// A redirect is a new connection, so it is checked again: a public page that
// redirects to an internal address must not be followed. The "public" page
// is an httptest server reached as public.test through a dial override; the
// redirect target (localhost) goes through the real, guarded dialer.
func TestFetch_RedirectToRefusedAddressIsBlocked(t *testing.T) {
	var internalHits int
	internal := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		internalHits++
		w.Write([]byte("secret"))
	}))
	t.Cleanup(internal.Close)
	internalURL, _ := url.Parse(internal.URL)
	public := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://localhost:"+internalURL.Port()+"/secret", http.StatusFound)
	}))
	t.Cleanup(public.Close)
	publicAddr := strings.TrimPrefix(public.URL, "http://")

	f := New(5*time.Second, 0)
	tr := f.client.Transport.(*http.Transport)
	guarded := tr.DialContext
	tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		if strings.HasPrefix(addr, "public.test:") {
			return (&net.Dialer{}).DialContext(ctx, network, publicAddr)
		}
		return guarded(ctx, network, addr)
	}

	_, port, _ := net.SplitHostPort(publicAddr)
	_, err := f.Get(context.Background(), "http://public.test:"+port+"/start")
	if !netguard.IsBlocked(err) {
		t.Fatalf("redirect to localhost: error = %v, want a netguard.BlockedError", err)
	}
	if internalHits != 0 {
		t.Errorf("the internal server was reached %d time(s)", internalHits)
	}
}

// DNS rebinding: a name that resolved to a public address earlier but now
// answers with an internal one is refused, because the check runs on the
// address being dialed, not on an earlier lookup.
func TestDNSCacheDial_ChecksTheDialedAddress(t *testing.T) {
	c := newDNSCache(time.Minute)
	c.resolve = func(ctx context.Context, host string) ([]string, error) {
		return []string{"169.254.169.254"}, nil
	}
	dial := c.dialContext(&net.Dialer{Timeout: time.Second, Control: netguard.Policy{}.Control})
	_, err := dial(context.Background(), "tcp", "metadata.rebind.test:80")
	if !netguard.IsBlocked(err) {
		t.Fatalf("dial = %v, want a netguard.BlockedError for the metadata address", err)
	}
}

func TestDNSCacheDial_NotFoundIsReturnedWithoutFallback(t *testing.T) {
	c := newDNSCache(time.Minute)
	c.resolve = func(ctx context.Context, host string) ([]string, error) {
		return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
	}
	dial := c.dialContext(&net.Dialer{Timeout: time.Second})
	_, err := dial(context.Background(), "tcp", "gone.example:80")
	if !IsPermanent(err) {
		t.Fatalf("dial = %v, want a permanent not-found error", err)
	}
}

func TestQUICDialer_RefusesPrivateAddresses(t *testing.T) {
	d := &quicDialer{policy: netguard.Policy{}, resolver: net.DefaultResolver}
	_, err := d.dial(context.Background(), "127.0.0.1:443", &tls.Config{ServerName: "x"}, &quic.Config{})
	if !netguard.IsBlocked(err) {
		t.Fatalf("HTTP/3 dial to 127.0.0.1 = %v, want a netguard.BlockedError", err)
	}
}

func TestFetch_RejectsNonHTTPSchemes(t *testing.T) {
	f := New(time.Second, 0, WithAllowPrivateNetworks(true))
	for _, u := range []string{"file:///etc/passwd", "ftp://example.com/x", "gopher://example.com/", "javascript:alert(1)", "//no-scheme.example/"} {
		_, err := f.Get(context.Background(), u)
		if !errors.Is(err, ErrUnsupportedScheme) || !IsPermanent(err) {
			t.Errorf("Get(%q) = %v, want a permanent ErrUnsupportedScheme", u, err)
		}
	}
	if _, err := f.Get(context.Background(), "http://%zz"); !errors.Is(err, ErrInvalidURL) {
		t.Errorf("Get(unparsable) = %v, want ErrInvalidURL", err)
	}
}

func TestFetch_RedirectToNonHTTPSchemeIsRefused(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "ftp://example.com/file", http.StatusFound)
	}))
	t.Cleanup(srv.Close)
	f := New(5*time.Second, 0, WithAllowPrivateNetworks(true))
	if _, err := f.Get(context.Background(), srv.URL); !errors.Is(err, ErrUnsupportedScheme) {
		t.Fatalf("Get = %v, want ErrUnsupportedScheme", err)
	}
}

func TestFetch_RedirectLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, r.URL.Path+"x", http.StatusFound) // endless chain
	}))
	t.Cleanup(srv.Close)
	f := NewWithOptions(Options{Timeout: 5 * time.Second, MaxRedirects: 3, AllowPrivateNetworks: true})
	_, err := f.Get(context.Background(), srv.URL+"/r")
	if !errors.Is(err, ErrTooManyRedirects) || !IsPermanent(err) {
		t.Fatalf("Get = %v, want a permanent ErrTooManyRedirects", err)
	}
}

func TestFetch_TruncatesBodyPastLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(strings.Repeat("a", 100)))
	}))
	t.Cleanup(srv.Close)
	f := New(5*time.Second, 0, WithAllowPrivateNetworks(true))

	res, err := f.Fetch(context.Background(), Request{URL: srv.URL, MaxBytes: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Body) != 10 || !res.Truncated {
		t.Errorf("body=%d truncated=%v, want 10 bytes and Truncated", len(res.Body), res.Truncated)
	}
	res, err = f.Fetch(context.Background(), Request{URL: srv.URL, MaxBytes: 100})
	if err != nil || len(res.Body) != 100 || res.Truncated {
		t.Errorf("exactly-at-limit body: err=%v len=%d truncated=%v, want 100 bytes, not truncated", err, len(res.Body), res.Truncated)
	}
}

func TestStream_ReaderFailsPastLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(strings.Repeat("a", 100)))
	}))
	t.Cleanup(srv.Close)
	f := New(5*time.Second, 0, WithAllowPrivateNetworks(true))

	err := f.Stream(context.Background(), Request{URL: srv.URL, MaxBytes: 10}, func(_ *Result, body io.Reader) error {
		_, err := io.ReadAll(body)
		return err
	})
	if !errors.Is(err, ErrBodyTooLarge) {
		t.Fatalf("Stream = %v, want ErrBodyTooLarge", err)
	}
}

func TestSetHostDelay_SpacesRequestsToOneHost(t *testing.T) {
	srv := okServer(t)
	u, _ := url.Parse(srv.URL)
	f := New(5*time.Second, 0, WithAllowPrivateNetworks(true))
	f.SetHostDelay(u.Hostname(), 300*time.Millisecond)

	start := time.Now()
	for i := 0; i < 3; i++ {
		if _, err := f.Get(context.Background(), srv.URL); err != nil {
			t.Fatal(err)
		}
	}
	if elapsed := time.Since(start); elapsed < 600*time.Millisecond {
		t.Errorf("3 requests with a 300ms host delay took %v, want >= 600ms", elapsed)
	}
}

func TestIsPermanent(t *testing.T) {
	permanent := []error{
		fmt.Errorf("x: %w", ErrInvalidURL),
		&net.DNSError{Err: "no such host", Name: "gone.example", IsNotFound: true},
		&url.Error{Op: "Get", URL: "https://x", Err: &tls.CertificateVerificationError{Err: x509.UnknownAuthorityError{}}},
		x509.HostnameError{Host: "x"},
		&netguard.BlockedError{Addr: netip.MustParseAddr("10.0.0.1"), Reason: "private"},
	}
	for _, err := range permanent {
		if !IsPermanent(err) {
			t.Errorf("IsPermanent(%v) = false, want true", err)
		}
	}
	transient := []error{
		context.DeadlineExceeded,
		&net.DNSError{Err: "server misbehaving", Name: "x", IsTemporary: true},
		&net.OpError{Op: "dial", Err: errors.New("connection refused")},
		io.ErrUnexpectedEOF,
	}
	for _, err := range transient {
		if IsPermanent(err) {
			t.Errorf("IsPermanent(%v) = true, want false", err)
		}
	}
}
