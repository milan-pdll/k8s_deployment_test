// Package robots implements robots.txt fetching, parsing, matching and
// per-origin caching following RFC 9309 (the Robots Exclusion Protocol):
//
//   - groups are selected by case-insensitive product-token match, falling
//     back to "*"; rules of every matching group are combined;
//   - Allow/Disallow paths support the "*" and "$" special characters and are
//     matched against the URL path plus query; the longest match wins and
//     Allow wins a tie; /robots.txt itself is always allowed;
//   - a 4xx response (other than 429) means no restrictions; a 429, a 5xx or
//     a network error means the file is unreachable and the whole origin is
//     disallowed for now (Allowed returns an error the caller retries);
//   - at least 500 KiB of the file is parsed (512 KiB; the rest is ignored);
//   - a fetched file is cached for at most 24 hours.
//
// Crawl-delay (not part of RFC 9309) is honored up to a cap, by spacing the
// fetcher's requests to the host.
package robots

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"search-engine-scraper/internal/fetcher"
)

const (
	// maxRobotsBytes is how much of a robots.txt file is parsed.
	maxRobotsBytes = 512 << 10
	// cacheTTL bounds how long a fetched robots.txt is used (RFC 9309 2.4).
	cacheTTL = 24 * time.Hour
	// unreachableTTL is how long an unreachable robots.txt (5xx, 429,
	// network error) is remembered before it is fetched again.
	unreachableTTL = 30 * time.Second
	// DefaultMaxCrawlDelay caps a site's Crawl-delay.
	DefaultMaxCrawlDelay = 10 * time.Second
	// maxCacheEntries triggers a sweep of expired entries.
	maxCacheEntries = 10000
)

// ErrUnreachable wraps the reason a robots.txt could not be retrieved
// (server error, 429 or network failure). RFC 9309 requires treating the
// origin as fully disallowed until the file can be fetched.
var ErrUnreachable = errors.New("robots.txt unreachable")

// rule is one Allow or Disallow line.
type rule struct {
	pattern string // percent-encoding normalized; may contain '*' and a trailing '$'
	allow   bool
}

type ruleSet struct {
	rules      []rule
	crawlDelay time.Duration
	// sitemaps are Sitemap: lines: they apply to the whole file, regardless
	// of user-agent groups.
	sitemaps []string
}

type entry struct {
	ready   chan struct{} // closed once rules/err are set
	rules   *ruleSet      // nil: no robots.txt (allow everything)
	err     error         // non-nil: unreachable or refused
	expires time.Time
}

// Guard checks and caches robots.txt rules per origin (scheme + host + port).
// Safe for concurrent use; concurrent lookups of the same origin share one
// fetch.
type Guard struct {
	fetcher       *fetcher.Fetcher
	userAgent     string
	maxCrawlDelay time.Duration
	now           func() time.Time

	mu    sync.Mutex
	cache map[string]*entry
}

// New builds a Guard that fetches robots.txt files with f and matches the
// user-agent product token userAgent (e.g. "search-engine-scraper").
func New(f *fetcher.Fetcher, userAgent string) *Guard {
	return &Guard{
		fetcher:       f,
		userAgent:     productToken(userAgent),
		maxCrawlDelay: DefaultMaxCrawlDelay,
		now:           time.Now,
		cache:         make(map[string]*entry),
	}
}

// Allowed reports whether rawURL may be fetched under its origin's
// robots.txt, fetching and caching the file on first use. An error means the
// question can't be answered right now: the robots.txt is unreachable
// (ErrUnreachable, retry later) or could not be fetched at all (e.g. the
// host's address is refused or does not exist).
func (g *Guard) Allowed(ctx context.Context, rawURL string) (bool, error) {
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		return false, fmt.Errorf("robots: %w: %q", fetcher.ErrInvalidURL, rawURL)
	}
	if u.EscapedPath() == "/robots.txt" && u.RawQuery == "" {
		return true, nil
	}
	rs, err := g.rulesFor(ctx, u)
	if err != nil {
		return false, err
	}
	if rs == nil {
		return true, nil
	}
	return rs.allowed(matchTarget(u)), nil
}

// Sitemaps returns the Sitemap: URLs declared in the robots.txt of rawURL's
// origin (nil when it declares none or has no robots.txt), or the reason the
// file could not be retrieved.
func (g *Guard) Sitemaps(ctx context.Context, rawURL string) ([]string, error) {
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("robots: %w: %q", fetcher.ErrInvalidURL, rawURL)
	}
	rs, err := g.rulesFor(ctx, u)
	if err != nil || rs == nil {
		return nil, err
	}
	return rs.sitemaps, nil
}

func (g *Guard) rulesFor(ctx context.Context, u *url.URL) (*ruleSet, error) {
	key := strings.ToLower(u.Scheme) + "://" + strings.ToLower(u.Host)
	now := g.now()

	g.mu.Lock()
	e, ok := g.cache[key]
	if !ok || (now.After(e.expires) && isReady(e)) {
		if len(g.cache) >= maxCacheEntries {
			for k, old := range g.cache {
				if isReady(old) && now.After(old.expires) {
					delete(g.cache, k)
				}
			}
		}
		e = &entry{ready: make(chan struct{})}
		g.cache[key] = e
		g.mu.Unlock()
		// In the background, so that this caller -- like every other waiter
		// below -- can give up when its own context ends.
		go g.fill(ctx, e, u)
	} else {
		g.mu.Unlock()
	}

	select {
	case <-e.ready:
		return e.rules, e.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func isReady(e *entry) bool {
	select {
	case <-e.ready:
		return true
	default:
		return false
	}
}

// fill fetches the robots.txt for u's origin into e. It uses a context that
// is not cancelled with the caller's, so the waiters sharing this fetch
// aren't failed by one caller giving up; the fetcher's own request timeout
// bounds it.
func (g *Guard) fill(ctx context.Context, e *entry, u *url.URL) {
	defer close(e.ready)
	robotsURL := (&url.URL{Scheme: u.Scheme, Host: u.Host, Path: "/robots.txt"}).String()
	res, err := g.fetcher.Fetch(context.WithoutCancel(ctx), fetcher.Request{
		URL:      robotsURL,
		Accept:   "text/plain,*/*;q=0.5",
		MaxBytes: maxRobotsBytes,
	})
	now := g.now()
	switch {
	case err != nil && fetcher.IsPermanent(err):
		// The origin itself can't be fetched (refused address, unknown
		// host, bad certificate): every page of it fails the same way.
		e.err, e.expires = err, now.Add(unreachableTTL)
	case err != nil:
		e.err, e.expires = fmt.Errorf("%w: %v", ErrUnreachable, err), now.Add(unreachableTTL)
	case res.StatusCode == http.StatusTooManyRequests || res.StatusCode >= 500:
		e.err, e.expires = fmt.Errorf("%w: %s returned %d", ErrUnreachable, robotsURL, res.StatusCode), now.Add(unreachableTTL)
	case res.StatusCode >= 200 && res.StatusCode < 300:
		e.rules, e.expires = parse(string(res.Body), g.userAgent), now.Add(cacheTTL)
	default:
		// 3xx left after following redirects, or 4xx: "unavailable", which
		// RFC 9309 treats as no restrictions.
		e.expires = now.Add(cacheTTL)
	}

	delay := time.Duration(0)
	if e.rules != nil {
		delay = min(e.rules.crawlDelay, g.maxCrawlDelay)
	}
	g.fetcher.SetHostDelay(u.Hostname(), delay)
}

// productToken lowercases a user-agent and strips any "/version" and
// comment, leaving the product token robots.txt groups are matched against.
func productToken(ua string) string {
	ua = strings.TrimSpace(ua)
	if i := strings.IndexAny(ua, "/ ("); i >= 0 {
		ua = ua[:i]
	}
	return strings.ToLower(ua)
}

// parse extracts the rules of every group matching userAgent (a product
// token), falling back to the "*" groups; Sitemap lines are collected from
// the whole file.
func parse(body, userAgent string) *ruleSet {
	type group struct {
		agents []string
		rules  []rule
		delay  time.Duration
	}
	var groups []*group
	var current *group
	inAgentLines := false
	var sitemaps []string

	scanner := bufio.NewScanner(strings.NewReader(body))
	scanner.Buffer(make([]byte, 0, 64<<10), maxRobotsBytes)
	for scanner.Scan() {
		line := scanner.Text()
		if i := strings.IndexByte(line, '#'); i >= 0 {
			line = line[:i]
		}
		key, val, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		key = strings.ToLower(strings.TrimSpace(key))
		val = strings.TrimSpace(val)

		// A run of user-agent lines opens a group; the group's member lines
		// (allow, disallow, crawl-delay) end the run. Sitemap and unknown
		// lines are not group members and leave the run alone.
		switch key {
		case "user-agent":
			if !inAgentLines || current == nil {
				current = &group{}
				groups = append(groups, current)
			}
			current.agents = append(current.agents, productToken(val))
			inAgentLines = true
		case "allow", "disallow":
			if current != nil && val != "" {
				current.rules = append(current.rules, rule{pattern: normalizePattern(val), allow: key == "allow"})
			}
			inAgentLines = false
		case "crawl-delay":
			if secs, err := strconv.ParseFloat(val, 64); err == nil && secs > 0 && current != nil {
				current.delay = max(current.delay, time.Duration(secs*float64(time.Second)))
			}
			inAgentLines = false
		case "sitemap":
			if val != "" {
				sitemaps = append(sitemaps, val)
			}
		}
	}

	rs := &ruleSet{sitemaps: sitemaps}
	collect := func(token string) bool {
		found := false
		for _, g := range groups {
			for _, a := range g.agents {
				if a == token {
					rs.rules = append(rs.rules, g.rules...)
					rs.crawlDelay = max(rs.crawlDelay, g.delay)
					found = true
					break
				}
			}
		}
		return found
	}
	if userAgent == "" || !collect(userAgent) {
		collect("*")
	}
	return rs
}

// allowed applies RFC 9309 2.2.2: the most specific (longest) matching rule
// wins; Allow wins a tie; no matching rule means allowed.
func (rs *ruleSet) allowed(target string) bool {
	bestLen, allowed := -1, true
	for _, r := range rs.rules {
		if !match(r.pattern, target) {
			continue
		}
		if n := len(r.pattern); n > bestLen || (n == bestLen && r.allow) {
			bestLen, allowed = n, r.allow
		}
	}
	return allowed
}

// matchTarget is the part of u robots.txt rules are matched against: the
// escaped path (at least "/") plus "?query" if any, percent-encoding
// normalized like the patterns.
func matchTarget(u *url.URL) string {
	p := u.EscapedPath()
	if p == "" {
		p = "/"
	}
	if u.RawQuery != "" {
		p += "?" + u.RawQuery
	}
	return normalizeEscapes(p)
}

// normalizePattern percent-encodes a rule path's non-ASCII bytes and
// normalizes its existing escapes, so that "/समाचार" in robots.txt matches the
// URL "/%E0%A4%B8...". A path that doesn't start with "/" or "*" gets a
// leading "/".
func normalizePattern(p string) string {
	if !strings.HasPrefix(p, "/") && !strings.HasPrefix(p, "*") {
		p = "/" + p
	}
	return normalizeEscapes(p)
}

// normalizeEscapes uppercases percent-escape hex digits and percent-encodes
// bytes outside printable ASCII.
func normalizeEscapes(s string) string {
	const hex = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '%' && i+2 < len(s) && isHex(s[i+1]) && isHex(s[i+2]):
			b.WriteByte('%')
			b.WriteByte(upperHex(s[i+1]))
			b.WriteByte(upperHex(s[i+2]))
			i += 2
		case c <= 0x20 || c >= 0x7f:
			b.WriteByte('%')
			b.WriteByte(hex[c>>4])
			b.WriteByte(hex[c&0x0f])
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

func isHex(c byte) bool {
	return ('0' <= c && c <= '9') || ('a' <= c && c <= 'f') || ('A' <= c && c <= 'F')
}

func upperHex(c byte) byte {
	if 'a' <= c && c <= 'f' {
		return c - 'a' + 'A'
	}
	return c
}

// match reports whether target matches a robots.txt path pattern: "*"
// matches any sequence of characters and a trailing "$" anchors the end;
// otherwise the pattern is a prefix match.
func match(pattern, target string) bool {
	anchored := strings.HasSuffix(pattern, "$")
	if anchored {
		pattern = pattern[:len(pattern)-1]
	}
	if !strings.Contains(pattern, "*") {
		if anchored {
			return target == pattern
		}
		return strings.HasPrefix(target, pattern)
	}
	if !anchored {
		pattern += "*" // prefix match: anything may follow
	}
	// Iterative wildcard match with single-star backtracking: O(len(p)*len(t))
	// worst case, linear for typical patterns.
	p, t := 0, 0
	star, mark := -1, 0
	for t < len(target) {
		switch {
		case p < len(pattern) && pattern[p] == '*':
			star, mark = p, t
			p++
		case p < len(pattern) && pattern[p] == target[t]:
			p++
			t++
		case star >= 0:
			p = star + 1
			mark++
			t = mark
		default:
			return false
		}
	}
	for p < len(pattern) && pattern[p] == '*' {
		p++
	}
	return p == len(pattern)
}
