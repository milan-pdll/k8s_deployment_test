// Package fetcher wraps an http.Client with the safety and politeness a
// crawler needs: it only connects to public addresses (checked at dial time,
// see internal/netguard), only speaks http/https, bounds redirects, response
// sizes and every phase of a request with timeouts, spaces requests to the
// same host, and optionally caps the process's aggregate download rate.
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
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"golang.org/x/time/rate"

	"search-engine-scraper/internal/netguard"
)

const (
	// MaxBodyBytes caps how much of a page response is read, so one huge
	// page (or a misbehaving server) can't blow up memory. Longer bodies are
	// truncated and reported through Result.Truncated.
	MaxBodyBytes = 5 << 20 // 5 MiB

	// DefaultUserAgent is the User-Agent header sent with every request.
	DefaultUserAgent = "search-engine-scraper/0.1 (+https://github.com/example/search-engine-scraper)"

	// DefaultMaxRedirects bounds how many redirects one fetch follows.
	DefaultMaxRedirects = 10

	// DefaultTimeout bounds one whole request (connect, TLS, headers, body).
	DefaultTimeout = 10 * time.Second

	defaultAccept = "text/html,application/xhtml+xml"

	// dnsCacheTTL is how long a resolved host's addresses are reused. A
	// crawl fetches many pages from the same host in a row, so caching
	// avoids a DNS round trip per page for a name that rarely changes
	// mid-crawl. The address policy is still checked on every dial.
	dnsCacheTTL = 5 * time.Minute

	// maxTrackedHosts bounds the per-host state (DNS cache entries,
	// politeness timers) kept by one long-running worker; past it, entries
	// idle for longer than hostStateIdle are dropped.
	maxTrackedHosts = 10000
	hostStateIdle   = 30 * time.Minute
)

var (
	// ErrInvalidURL is returned for a URL that can't be fetched at all.
	ErrInvalidURL = errors.New("invalid URL")
	// ErrUnsupportedScheme is returned for anything but http and https,
	// including on a redirect.
	ErrUnsupportedScheme = errors.New("unsupported URL scheme")
	// ErrTooManyRedirects is returned once a fetch exceeds its redirect limit.
	ErrTooManyRedirects = errors.New("too many redirects")
	// ErrBodyTooLarge is returned by a Stream body reader read past its limit.
	ErrBodyTooLarge = errors.New("response body exceeds the size limit")
)

// IsPermanent reports whether retrying the fetch that returned err cannot
// succeed: an invalid or non-http(s) URL, a refused (non-public)
// destination, a redirect loop, a host that does not exist, or a TLS
// certificate that does not verify. Everything else (timeouts, connection
// resets, temporary DNS failures) is worth retrying.
func IsPermanent(err error) bool {
	if err == nil {
		return false
	}
	switch {
	case errors.Is(err, ErrInvalidURL), errors.Is(err, ErrUnsupportedScheme),
		errors.Is(err, ErrTooManyRedirects), netguard.IsBlocked(err):
		return true
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
		return true
	}
	var verifyErr *tls.CertificateVerificationError
	var unknownAuthority x509.UnknownAuthorityError
	var hostnameErr x509.HostnameError
	var invalidCert x509.CertificateInvalidError
	return errors.As(err, &verifyErr) || errors.As(err, &unknownAuthority) ||
		errors.As(err, &hostnameErr) || errors.As(err, &invalidCert)
}

// CheckURL validates that rawURL is an absolute http(s) URL with a host.
func CheckURL(rawURL string) (*url.URL, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidURL, err)
	}
	if err := checkParsedURL(u); err != nil {
		return nil, err
	}
	return u, nil
}

func checkParsedURL(u *url.URL) error {
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("%w %q in %s", ErrUnsupportedScheme, u.Scheme, u.Redacted())
	}
	if u.Hostname() == "" {
		return fmt.Errorf("%w: %s has no host", ErrInvalidURL, u.Redacted())
	}
	return nil
}

// Options controls request safety and per-host politeness. Zero values
// select the defaults documented on each field.
type Options struct {
	// Timeout bounds one whole request: connect, TLS handshake, response
	// headers and reading the body (default DefaultTimeout).
	Timeout time.Duration
	// MaxBandwidthBPS caps the aggregate download rate in bytes/sec across
	// every concurrent request on this Fetcher (0 = unlimited).
	MaxBandwidthBPS int
	// DomainRequestsPerSecond and DomainBurst rate-limit requests per host
	// (0 = no rate limit).
	DomainRequestsPerSecond float64
	DomainBurst             int
	// PolitenessDelay is the minimum spacing between the starts of two
	// requests to the same host; a robots.txt Crawl-delay raises it for that
	// host (see SetHostDelay). 0 = no spacing.
	PolitenessDelay time.Duration
	// HTTP3 attempts HTTP/3 (QUIC) first on https requests, falling back to
	// HTTP/1.1 or HTTP/2.
	HTTP3 bool
	// UserAgent is sent with every request (default DefaultUserAgent).
	UserAgent string
	// MaxRedirects bounds the redirects one request follows (default
	// DefaultMaxRedirects).
	MaxRedirects int
	// AllowPrivateNetworks lets requests reach loopback, private, link-local
	// and other non-public addresses. Off by default: only tests and local
	// experiments against private hosts should turn it on.
	AllowPrivateNetworks bool
}

// Option adjusts Options for New.
type Option func(*Options)

// WithHTTP3 makes the Fetcher attempt HTTP/3 (QUIC) first on every https
// request, falling back to the normal TCP-based transport when the target
// doesn't speak HTTP/3 -- which is still most of the web, so this is opt-in.
func WithHTTP3() Option { return func(o *Options) { o.HTTP3 = true } }

// WithAllowPrivateNetworks lets the Fetcher connect to non-public addresses
// (see Options.AllowPrivateNetworks).
func WithAllowPrivateNetworks(allow bool) Option {
	return func(o *Options) { o.AllowPrivateNetworks = allow }
}

// WithUserAgent sets the User-Agent header.
func WithUserAgent(ua string) Option { return func(o *Options) { o.UserAgent = ua } }

// WithPolitenessDelay sets the default minimum spacing between requests to
// one host.
func WithPolitenessDelay(d time.Duration) Option {
	return func(o *Options) { o.PolitenessDelay = d }
}

// Fetcher performs polite, address-checked HTTP GETs. Safe for concurrent use.
type Fetcher struct {
	client    *http.Client
	userAgent string
	policy    netguard.Policy

	// bandwidth throttles total bytes/sec read across every request on this
	// Fetcher (nil when unlimited); bandwidthChunk is its burst size and the
	// most bytes charged in one WaitN call.
	bandwidth      *rate.Limiter
	bandwidthChunk int

	domainRequestsPerSecond rate.Limit
	domainBurst             int
	defaultPolitenessDelay  time.Duration
	domainsMu               sync.Mutex
	domains                 map[string]*domainState
}

type domainState struct {
	limiter *rate.Limiter
	mu      sync.Mutex
	next    time.Time
	delay   time.Duration
	used    time.Time // guarded by Fetcher.domainsMu
}

// redirectChainKey is the context key Get uses to give its CheckRedirect
// invocations somewhere to record each hop's target -- a pointer to a slice,
// since context values are immutable but the pointee isn't. Go's redirect
// loop copies the original request's context onto every redirect request, so
// each top-level call's chain only ever sees its own hops.
type redirectChainKey struct{}

// New builds a Fetcher with the given whole-request timeout and aggregate
// bandwidth cap (bytes/sec, 0 = unlimited).
func New(timeout time.Duration, maxBandwidthBPS int, opts ...Option) *Fetcher {
	o := Options{Timeout: timeout, MaxBandwidthBPS: maxBandwidthBPS}
	for _, opt := range opts {
		opt(&o)
	}
	return NewWithOptions(o)
}

// NewWithOptions builds a Fetcher from o.
func NewWithOptions(o Options) *Fetcher {
	if o.Timeout <= 0 {
		o.Timeout = DefaultTimeout
	}
	if o.MaxRedirects <= 0 {
		o.MaxRedirects = DefaultMaxRedirects
	}
	if o.UserAgent == "" {
		o.UserAgent = DefaultUserAgent
	}
	burst := o.DomainBurst
	if burst <= 0 {
		burst = 1
	}
	policy := netguard.Policy{AllowPrivate: o.AllowPrivateNetworks}

	dialer := &net.Dialer{
		Timeout:   o.Timeout,
		KeepAlive: 30 * time.Second,
		// The address check runs on the literal IP being connected to, for
		// every connection: DNS-cache hits, fresh lookups and redirects alike.
		Control: policy.Control,
	}
	transport := &http.Transport{
		// Never use HTTP(S)_PROXY: a proxy would connect on our behalf and
		// bypass the dial-time address check.
		Proxy:       nil,
		DialContext: newDNSCache(dnsCacheTTL).dialContext(dialer),
		// A custom DialContext turns HTTP/2 off unless asked for explicitly.
		ForceAttemptHTTP2:      true,
		TLSHandshakeTimeout:    o.Timeout,
		ResponseHeaderTimeout:  o.Timeout,
		ExpectContinueTimeout:  time.Second,
		IdleConnTimeout:        30 * time.Second,
		MaxIdleConns:           256,
		MaxIdleConnsPerHost:    4,
		MaxResponseHeaderBytes: 1 << 20,
	}
	var rt http.RoundTripper = transport
	if o.HTTP3 {
		qd := &quicDialer{policy: policy, resolver: net.DefaultResolver}
		rt = &http3FallbackTransport{
			h3:       &http3.Transport{Dial: qd.dial},
			fallback: transport,
		}
	}

	var limiter *rate.Limiter
	chunk := 0
	if o.MaxBandwidthBPS > 0 {
		chunk = o.MaxBandwidthBPS
		limiter = rate.NewLimiter(rate.Limit(o.MaxBandwidthBPS), chunk)
	}

	maxRedirects := o.MaxRedirects
	return &Fetcher{
		userAgent:               o.UserAgent,
		policy:                  policy,
		bandwidth:               limiter,
		bandwidthChunk:          chunk,
		domainRequestsPerSecond: rate.Limit(o.DomainRequestsPerSecond),
		domainBurst:             burst,
		defaultPolitenessDelay:  o.PolitenessDelay,
		domains:                 make(map[string]*domainState),
		client: &http.Client{
			Timeout:   o.Timeout,
			Transport: rt,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= maxRedirects {
					return fmt.Errorf("%w: stopped after %d", ErrTooManyRedirects, maxRedirects)
				}
				if err := checkParsedURL(req.URL); err != nil {
					return err
				}
				if chain, ok := req.Context().Value(redirectChainKey{}).(*[]string); ok {
					*chain = append(*chain, req.URL.String())
				}
				return nil
			},
		},
	}
}

// SetHostDelay sets the minimum spacing between requests to host (e.g. a
// robots.txt Crawl-delay). It never lowers the spacing below the Fetcher's
// default politeness delay. Existing rate-limit state is kept.
func (f *Fetcher) SetHostDelay(host string, delay time.Duration) {
	host = strings.ToLower(host)
	if host == "" {
		return
	}
	if delay < f.defaultPolitenessDelay {
		delay = f.defaultPolitenessDelay
	}
	state := f.domainState(host)
	state.mu.Lock()
	state.delay = delay
	state.mu.Unlock()
}

func (f *Fetcher) domainState(host string) *domainState {
	f.domainsMu.Lock()
	defer f.domainsMu.Unlock()
	now := time.Now()
	if state, ok := f.domains[host]; ok {
		state.used = now
		return state
	}
	if len(f.domains) >= maxTrackedHosts {
		for h, s := range f.domains {
			if now.Sub(s.used) > hostStateIdle {
				delete(f.domains, h)
			}
		}
	}
	state := &domainState{delay: f.defaultPolitenessDelay, used: now}
	if f.domainRequestsPerSecond > 0 {
		state.limiter = rate.NewLimiter(f.domainRequestsPerSecond, f.domainBurst)
	}
	f.domains[host] = state
	return state
}

// waitForDomain blocks until host may receive another request.
func (f *Fetcher) waitForDomain(ctx context.Context, host string) error {
	state := f.domainState(strings.ToLower(host))
	if state.limiter != nil {
		if err := state.limiter.Wait(ctx); err != nil {
			return fmt.Errorf("host rate limiter: %w", err)
		}
	}

	state.mu.Lock()
	now := time.Now()
	startAt := state.next
	if startAt.Before(now) {
		startAt = now
	}
	state.next = startAt.Add(state.delay)
	state.mu.Unlock()

	if wait := time.Until(startAt); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	return nil
}

// Request describes one fetch.
type Request struct {
	URL string
	// Accept is the Accept header (default "text/html,application/xhtml+xml").
	Accept string
	// MaxBytes bounds the body read (default MaxBodyBytes).
	MaxBytes int64
}

// Result is the outcome of fetching a single URL.
type Result struct {
	StatusCode  int
	ContentType string
	Header      http.Header // full response headers, e.g. for Retry-After
	// Body holds at most the request's MaxBytes (nil for Stream).
	Body []byte
	// Truncated reports that the body was longer than MaxBytes.
	Truncated bool
	Duration  time.Duration

	// FinalURL is the URL the response came from after any redirects --
	// equal to the requested URL if there were none.
	FinalURL string
	// RedirectChain lists each hop's target URL in order (the requested URL
	// is not included). Empty if the request wasn't redirected.
	RedirectChain []string
}

// Get fetches an HTML page (see Fetch).
func (f *Fetcher) Get(ctx context.Context, rawURL string) (*Result, error) {
	return f.Fetch(ctx, Request{URL: rawURL})
}

// Fetch performs req and returns the response with its body read into
// memory (at most req.MaxBytes; Result.Truncated reports a longer body).
// A non-2xx status is not an error.
func (f *Fetcher) Fetch(ctx context.Context, req Request) (*Result, error) {
	maxBytes := req.MaxBytes
	if maxBytes <= 0 {
		maxBytes = MaxBodyBytes
	}
	// Read one byte past the limit to tell a body of exactly maxBytes from a
	// longer one, without Stream's reader failing with ErrBodyTooLarge.
	req.MaxBytes = maxBytes + 1
	var res *Result
	err := f.Stream(ctx, req, func(r *Result, body io.Reader) error {
		data, err := io.ReadAll(io.LimitReader(body, maxBytes+1))
		if err != nil {
			return fmt.Errorf("read body: %w", err)
		}
		if int64(len(data)) > maxBytes {
			data = data[:maxBytes]
			r.Truncated = true
		}
		r.Body = data
		res = r
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Stream performs req and calls fn with the response (Body unset) and a
// reader over its body that fails with ErrBodyTooLarge past req.MaxBytes.
// The body is closed when fn returns. fn's error is returned as is.
func (f *Fetcher) Stream(ctx context.Context, req Request, fn func(*Result, io.Reader) error) error {
	start := time.Now()
	u, err := CheckURL(req.URL)
	if err != nil {
		return err
	}
	// A literal non-public IP is refused before waiting on politeness; a
	// hostname is checked when the connection is dialed.
	if ip, perr := netip.ParseAddr(u.Hostname()); perr == nil {
		if err := f.policy.Check(ip); err != nil {
			return err
		}
	}

	var chain []string
	ctx = context.WithValue(ctx, redirectChainKey{}, &chain)
	if err := f.waitForDomain(ctx, u.Hostname()); err != nil {
		return err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidURL, err)
	}
	httpReq.Header.Set("User-Agent", f.userAgent)
	accept := req.Accept
	if accept == "" {
		accept = defaultAccept
	}
	httpReq.Header.Set("Accept", accept)

	resp, err := f.client.Do(httpReq)
	if err != nil {
		// *url.Error already names the method and the (password-stripped) URL.
		return err
	}
	defer resp.Body.Close()

	maxBytes := req.MaxBytes
	if maxBytes <= 0 {
		maxBytes = MaxBodyBytes
	}
	var body io.Reader = &limitedReader{r: resp.Body, remaining: maxBytes}
	if f.bandwidth != nil {
		body = &throttledReader{ctx: ctx, r: body, limiter: f.bandwidth, chunk: f.bandwidthChunk}
	}

	finalURL := req.URL
	if resp.Request != nil && resp.Request.URL != nil {
		finalURL = resp.Request.URL.String()
	}
	res := &Result{
		StatusCode:    resp.StatusCode,
		ContentType:   resp.Header.Get("Content-Type"),
		Header:        resp.Header,
		FinalURL:      finalURL,
		RedirectChain: chain,
	}
	err = fn(res, body)
	res.Duration = time.Since(start)
	return err
}

// limitedReader is io.LimitReader that reports ErrBodyTooLarge instead of a
// silent EOF when the underlying body continues past the limit.
type limitedReader struct {
	r         io.Reader
	remaining int64
}

func (l *limitedReader) Read(p []byte) (int, error) {
	if l.remaining <= 0 {
		// Probe for one more byte to tell "exactly at the limit" from "over".
		var one [1]byte
		if n, err := l.r.Read(one[:]); n > 0 {
			return 0, ErrBodyTooLarge
		} else if err != nil {
			return 0, err
		}
		return 0, io.EOF
	}
	if int64(len(p)) > l.remaining {
		p = p[:l.remaining]
	}
	n, err := l.r.Read(p)
	l.remaining -= int64(n)
	return n, err
}

// throttledReader charges every read against the shared bandwidth limiter,
// in chunks no larger than its burst (WaitN fails for larger requests).
type throttledReader struct {
	ctx     context.Context
	r       io.Reader
	limiter *rate.Limiter
	chunk   int
}

func (t *throttledReader) Read(p []byte) (int, error) {
	if len(p) > t.chunk {
		p = p[:t.chunk]
	}
	n, err := t.r.Read(p)
	if n > 0 {
		if werr := t.limiter.WaitN(t.ctx, n); werr != nil {
			return n, fmt.Errorf("bandwidth limiter: %w", werr)
		}
	}
	return n, err
}

// dnsCacheEntry is one host's cached resolution.
type dnsCacheEntry struct {
	addrs  []string
	expiry time.Time
}

// dnsCache is a small TTL-based DNS cache shared across every request on one
// Fetcher. It only saves lookups: the address policy is enforced by the
// dialer's Control hook on whatever address is finally dialed.
type dnsCache struct {
	mu      sync.Mutex
	entries map[string]dnsCacheEntry
	ttl     time.Duration
	// resolve is a seam for tests; production code uses
	// net.DefaultResolver.LookupHost.
	resolve func(ctx context.Context, host string) ([]string, error)
}

func newDNSCache(ttl time.Duration) *dnsCache {
	return &dnsCache{
		entries: make(map[string]dnsCacheEntry),
		ttl:     ttl,
		resolve: net.DefaultResolver.LookupHost,
	}
}

// lookup returns host's IP addresses, reusing a cached, unexpired result.
func (c *dnsCache) lookup(ctx context.Context, host string) ([]string, error) {
	now := time.Now()
	c.mu.Lock()
	if e, ok := c.entries[host]; ok && now.Before(e.expiry) {
		c.mu.Unlock()
		return e.addrs, nil
	}
	c.mu.Unlock()

	addrs, err := c.resolve(ctx, host)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	if len(c.entries) >= maxTrackedHosts {
		for h, e := range c.entries {
			if now.After(e.expiry) {
				delete(c.entries, h)
			}
		}
	}
	c.entries[host] = dnsCacheEntry{addrs: addrs, expiry: now.Add(c.ttl)}
	c.mu.Unlock()
	return addrs, nil
}

// dialContext returns an http.Transport.DialContext that resolves the target
// host through this cache and dials the literal IPs. A host that does not
// exist fails immediately; any other lookup failure falls back to dialer's
// own resolution, so a cache problem never makes a reachable host
// unreachable. Every dial goes through dialer, whose Control hook enforces
// the address policy.
func (c *dnsCache) dialContext(dialer *net.Dialer) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil || net.ParseIP(host) != nil {
			return dialer.DialContext(ctx, network, addr)
		}

		addrs, err := c.lookup(ctx, host)
		if err != nil {
			var dnsErr *net.DNSError
			if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
				return nil, err
			}
			return dialer.DialContext(ctx, network, addr)
		}
		if len(addrs) == 0 {
			return dialer.DialContext(ctx, network, addr)
		}

		// Prefer reporting a real network failure over a refused address, so
		// a host with one blocked and one unreachable address is retried.
		var blockedErr, otherErr error
		for _, ip := range addrs {
			conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip, port))
			if err == nil {
				return conn, nil
			}
			if netguard.IsBlocked(err) {
				blockedErr = err
			} else {
				otherErr = err
			}
		}
		if otherErr != nil {
			return nil, otherErr
		}
		return nil, blockedErr
	}
}

// quicDialer dials HTTP/3 connections, applying the address policy to the
// resolved addresses before connecting (QUIC does not go through net.Dialer).
type quicDialer struct {
	policy   netguard.Policy
	resolver netguard.Resolver

	once sync.Once
	tr   *quic.Transport
	err  error
}

func (d *quicDialer) dial(ctx context.Context, addr string, tlsCfg *tls.Config, cfg *quic.Config) (*quic.Conn, error) {
	d.once.Do(func() {
		conn, err := net.ListenUDP("udp", nil)
		if err != nil {
			d.err = fmt.Errorf("open UDP socket for HTTP/3: %w", err)
			return
		}
		d.tr = &quic.Transport{Conn: conn}
	})
	if d.err != nil {
		return nil, d.err
	}
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidURL, err)
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return nil, fmt.Errorf("%w: bad port %q", ErrInvalidURL, portStr)
	}
	ips, err := d.policy.LookupAllowed(ctx, d.resolver, host)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for _, ip := range ips {
		udpAddr := net.UDPAddrFromAddrPort(netip.AddrPortFrom(ip, uint16(port)))
		conn, err := d.tr.DialEarly(ctx, udpAddr, tlsCfg, cfg)
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

// http3FallbackTransport tries HTTP/3 first on https requests and falls back
// to the standard transport when the server doesn't answer over QUIC. Safe
// to retry the same request: Fetcher always sends a nil body.
type http3FallbackTransport struct {
	h3       http.RoundTripper // *http3.Transport in production; a stub in tests
	fallback http.RoundTripper
}

func (t *http3FallbackTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme != "https" {
		return t.fallback.RoundTrip(req)
	}
	if resp, err := t.h3.RoundTrip(req); err == nil {
		return resp, nil
	}
	return t.fallback.RoundTrip(req)
}
