// Package activities holds the Temporal Activities the crawl workflow
// calls: everything that touches the network, the filesystem, or any other
// non-deterministic/side-effecting resource. Activities are where Temporal's
// automatic retry, backoff, and timeout policies actually apply -- the
// workflow itself (internal/workflows) only orchestrates calls to these.
package activities

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.temporal.io/sdk/temporal"

	"search-engine-scraper/internal/fetcher"
	"search-engine-scraper/internal/metrics"
	"search-engine-scraper/internal/model"
	"search-engine-scraper/internal/normalize"
	"search-engine-scraper/internal/parser"
	"search-engine-scraper/internal/render"
	"search-engine-scraper/internal/robots"
	"search-engine-scraper/internal/sitemap"
	"search-engine-scraper/internal/storage"
)

// defaultRetryAfter is the backoff applied to a 429 when the response carries
// no (or an unparsable) Retry-After header.
const defaultRetryAfter = 30 * time.Second

// maxRetryAfter caps how long a single Retry-After-driven backoff can be, so
// a misbehaving/hostile server can't stall a workflow indefinitely.
const maxRetryAfter = 5 * time.Minute

// maxTextRunes bounds how much extracted text an activity result carries.
// Activity inputs/outputs are recorded in Temporal's workflow history, so
// this keeps history size (and gRPC payload size) predictable regardless of
// how large a scraped page's body is.
const maxTextRunes = 20000

// Activities bundles the dependencies every activity method needs. A single
// instance is created once per worker process and its methods are
// registered with the Temporal worker.
type Activities struct {
	Fetcher   *fetcher.Fetcher
	Robots    *robots.Guard
	Writer    storage.Writer
	Runs      storage.RunRecorder
	Freshness storage.FreshnessChecker
	// HTML saves each fetched page's raw HTML body, separately from the
	// parsed Document metadata Writer persists. Defaults to
	// storage.NoopHTMLWriter{} (raw HTML downloads disabled) unless the
	// caller overrides this field after New.
	HTML storage.HTMLWriter
	// Records saves each page's complete structured capture
	// (model.PageRecord). Defaults to a no-op.
	Records storage.RecordWriter
	// Sites publishes the "website crawl completed" event ETL starts from.
	// Defaults to a no-op (no Kafka brokers configured).
	Sites storage.SiteEventEmitter
	// Renderer loads pages in a headless browser so JavaScript-rendered
	// content is captured; RenderMode decides which pages it is used for.
	// Both default to off.
	Renderer   render.Renderer
	RenderMode render.Mode

	mu          sync.Mutex
	writtenHash map[string]bool // content-hash dedupe, guards WriteDocument against retry double-writes
}

// New builds an Activities bundle. runs/freshness may be
// storage.NoopRunRecorder{}/storage.NoopFreshnessChecker{} when no
// persistent backend (e.g. Postgres) is configured.
func New(f *fetcher.Fetcher, r *robots.Guard, w storage.Writer, runs storage.RunRecorder, freshness storage.FreshnessChecker) *Activities {
	return &Activities{
		Fetcher:     f,
		Robots:      r,
		Writer:      w,
		Runs:        runs,
		Freshness:   freshness,
		HTML:        storage.NoopHTMLWriter{},
		Records:     storage.NoopRecordWriter{},
		Sites:       storage.NoopSiteEventEmitter{},
		RenderMode:  render.ModeOff,
		writtenHash: make(map[string]bool),
	}
}

// ProcessPageInput is the argument to ProcessPage.
type ProcessPageInput struct {
	URL   string
	Depth int
}

// ProcessPageOutput is everything the workflow needs back from fetching and
// parsing one page: enough to enqueue new links and to build the ETL
// Document, without carrying the raw HTML body through workflow history.
type ProcessPageOutput struct {
	URL           string
	NormalizedURL string
	// FinalURL is where the response actually came from after following
	// any redirects (already folded into NormalizedURL when present, same
	// as CanonicalURL), or "" if the request wasn't redirected at all.
	FinalURL string
	// CanonicalURL is the page's declared <link rel="canonical"> target
	// (already folded into NormalizedURL when present), kept separately so
	// the workflow/Document can record it distinctly from the fetched URL.
	CanonicalURL    string
	Title           string
	MetaDescription string
	Text            string
	Headings        []model.Heading
	Links           []string
	AnchorTexts     []string
	JSONLD          []string
	Geo             *model.GeoPoint
	// Country is the page's best-guess origin country (ISO 3166-1 alpha-2),
	// or "" if undetected -- see internal/parser.DetectCountry.
	Country string
	// SimHash is a 64-bit near-duplicate fingerprint of Text (see
	// internal/simhash) -- 0 for pages that were never parsed (robots
	// disallowed, non-HTML, fetch error).
	SimHash     uint64
	StatusCode  int
	ContentType string
	ContentHash string
	FetchDurMs  int64
	Skipped     bool // true if robots.txt disallowed or content wasn't HTML
	SkipReason  string
	FetchError  string // set if the HTTP fetch itself failed (activity still returns nil error so the workflow can record it and move on, rather than retrying a page that 404s forever)
	// NoIndex/NoFollow combine <meta name="robots"> and the X-Robots-Tag
	// header (whichever declares it): NoIndex means the page asked not to
	// be stored, NoFollow means it asked not to have its links followed.
	// These are independent of Skipped/FetchError -- the page fetched and
	// parsed fine, it just opted out of one or both here.
	NoIndex  bool
	NoFollow bool

	// HTMLKey/RenderedHTMLKey/RecordKey locate this page's raw HTML,
	// browser-rendered HTML and full structured record in the object
	// store; Rendered reports whether a headless browser was used, and
	// RenderError why it was wanted but failed (the static HTML was used).
	HTMLKey         string
	RenderedHTMLKey string
	RecordKey       string
	Rendered        bool
	RenderError     string
}

// ProcessPage fetches url (after checking robots.txt and honoring any
// requested crawl-delay) and, for HTML responses, parses out the title,
// visible text, and outbound links.
//
// Transient failures (network errors, timeouts) are returned as a Go error
// so Temporal's activity RetryPolicy retries them with backoff. Permanent,
// page-level outcomes (404, robots disallow, non-HTML content) are returned
// as a normal result with Skipped/FetchError set, so the workflow records
// them once and moves on instead of retrying forever.
func (a *Activities) ProcessPage(ctx context.Context, in ProcessPageInput) (ProcessPageOutput, error) {
	out := ProcessPageOutput{
		URL:           in.URL,
		NormalizedURL: normalize.Canonical(in.URL),
	}

	// Crawl-delay is applied by the fetcher itself: the robots guard spaces its
	// requests to the host when it reads the file.
	allowed, err := a.Robots.Allowed(ctx, in.URL)
	if err != nil {
		if fetcher.IsPermanent(err) {
			// The origin can never be fetched (refused address, unknown host,
			// invalid URL): record it once instead of retrying.
			out.Skipped = true
			out.SkipReason = "robots.txt not retrievable: " + err.Error()
			metrics.PagesFetched.WithLabelValues(metrics.OutcomeNetworkError).Inc()
			return out, nil
		}
		// RFC 9309: an unreachable robots.txt disallows the origin for now --
		// let Temporal retry later.
		return out, fmt.Errorf("robots.txt for %s: %w", in.URL, err)
	}
	if !allowed {
		out.Skipped = true
		out.SkipReason = "disallowed by robots.txt"
		metrics.PagesFetched.WithLabelValues(metrics.OutcomeSkippedRobots).Inc()
		return out, nil
	}

	res, err := a.Fetcher.Get(ctx, in.URL)
	if err != nil {
		metrics.PagesFetched.WithLabelValues(metrics.OutcomeNetworkError).Inc()
		if fetcher.IsPermanent(err) {
			// Refused address, redirect loop, bad certificate...: retrying
			// can't help, so the workflow records the failure and moves on.
			out.FetchError = err.Error()
			return out, nil
		}
		// Network/timeout errors are transient -- let Temporal retry.
		return out, fmt.Errorf("fetch %s: %w", in.URL, err)
	}
	metrics.FetchDuration.Observe(res.Duration.Seconds())

	out.StatusCode = res.StatusCode
	out.ContentType = res.ContentType
	out.FetchDurMs = res.Duration.Milliseconds()
	sum := sha256.Sum256(res.Body)
	out.ContentHash = hex.EncodeToString(sum[:])

	if res.FinalURL != "" {
		if finalNormalized := normalize.Canonical(res.FinalURL); finalNormalized != out.NormalizedURL {
			// The response came from a different URL than requested (a
			// redirect chain, e.g. a 301 to a tracking-param-free or
			// https:// URL) -- record where it actually landed, and use
			// that as the dedupe identity instead of the originally
			// requested URL, so a redirected URL doesn't create a separate
			// document from the same content reached directly. A later
			// <link rel="canonical"> declaration (below) still takes
			// precedence over this if the page also declares one.
			out.FinalURL = finalNormalized
			out.NormalizedURL = finalNormalized
		}
	}

	if res.StatusCode >= 500 {
		// Server-side failure: worth retrying.
		metrics.PagesFetched.WithLabelValues(metrics.OutcomeServerError).Inc()
		return out, fmt.Errorf("fetch %s: server error %d", in.URL, res.StatusCode)
	}
	if res.StatusCode == 429 {
		// Rate limited: retryable, but honor the site's requested backoff
		// (or a conservative default) instead of the RetryPolicy's normal
		// exponential interval, so a throttling site actually gets slowed
		// down rather than hammered again in ~1s.
		metrics.PagesFetched.WithLabelValues(metrics.OutcomeRateLimited).Inc()
		metrics.RateLimited.WithLabelValues(normalize.Hostname(in.URL)).Inc()
		delay := parseRetryAfter(res.Header.Get("Retry-After"))
		return out, temporal.NewApplicationErrorWithOptions(
			fmt.Sprintf("fetch %s: rate limited (429)", in.URL),
			"RateLimited",
			temporal.ApplicationErrorOptions{NextRetryDelay: delay},
		)
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		out.FetchError = fmt.Sprintf("non-2xx status %d", res.StatusCode)
		metrics.PagesFetched.WithLabelValues(metrics.OutcomeClientError).Inc()
		return out, nil
	}
	if !strings.Contains(out.ContentType, "html") {
		out.Skipped = true
		out.SkipReason = "non-HTML content-type: " + out.ContentType
		metrics.PagesFetched.WithLabelValues(metrics.OutcomeSkippedNonHTML).Inc()
		return out, nil
	}

	rawBody := res.Body
	rawHash := out.ContentHash
	host := normalize.Hostname(out.NormalizedURL)

	// Best-effort: saving the raw document is a bonus alongside the parsed
	// Document this activity already returns, not a correctness requirement
	// of the crawl -- a storage error here shouldn't fail (and retry-refetch)
	// an otherwise-successful page.
	if err := a.HTML.SaveHTML(out.NormalizedURL, rawHash, rawBody); err == nil {
		out.HTMLKey = storage.S3HTMLObjectKey(out.NormalizedURL, rawHash)
	}

	body := rawBody
	parsed, err := parser.Parse(body, in.URL, out.ContentType)
	if err != nil {
		out.FetchError = "parse error: " + err.Error()
		metrics.PagesFetched.WithLabelValues(metrics.OutcomeParseError).Inc()
		return out, nil
	}

	// Client-side rendered pages serve an empty shell; load them in a
	// headless browser and use the resulting DOM instead.
	renderReason := ""
	switch a.RenderMode {
	case render.ModeAlways:
		renderReason = "always"
	case render.ModeAuto:
		renderReason = render.NeedsRender(string(rawBody), len([]rune(parsed.Text)))
	}
	if renderReason != "" && a.Renderer != nil {
		rendered, finalURL, rerr := a.Renderer.Render(ctx, in.URL)
		if rerr != nil {
			out.RenderError = rerr.Error()
		} else if rp, perr := parser.Parse([]byte(rendered), in.URL, "text/html; charset=utf-8"); perr != nil {
			out.RenderError = "parse rendered: " + perr.Error()
		} else {
			_ = finalURL
			body = []byte(rendered)
			parsed = rp
			out.Rendered = true
			sum := sha256.Sum256(body)
			out.ContentHash = hex.EncodeToString(sum[:])
			if err := a.HTML.SaveHTML(out.NormalizedURL, "rendered-"+out.ContentHash, body); err == nil {
				out.RenderedHTMLKey = storage.S3HTMLObjectKey(out.NormalizedURL, "rendered-"+out.ContentHash)
			}
		}
	}

	out.Title = parsed.Title
	out.MetaDescription = parsed.MetaDescription
	out.Text = truncateRunes(parsed.Text, maxTextRunes)
	out.Headings = parsed.Headings
	out.Links = parsed.Links
	out.AnchorTexts = parsed.AnchorTexts
	out.JSONLD = parsed.JSONLD
	out.Geo = parsed.Geo
	out.Country = parsed.Country
	out.SimHash = parsed.SimHash

	if parsed.CanonicalURL != "" && parsed.CanonicalURL != out.NormalizedURL {
		// The page is telling us which URL is authoritative for its
		// content -- use it as the dedupe identity instead of the fetched
		// URL, so tracking-param/AMP variants of the same page collapse to
		// one document rather than duplicating.
		out.CanonicalURL = parsed.CanonicalURL
		out.NormalizedURL = parsed.CanonicalURL
	}

	out.NoIndex = parsed.RobotsNoIndex
	out.NoFollow = parsed.RobotsNoFollow
	for _, v := range res.Header.Values("X-Robots-Tag") {
		noindex, nofollow := xRobotsTagDirectives(v)
		out.NoIndex = out.NoIndex || noindex
		out.NoFollow = out.NoFollow || nofollow
	}

	// Full structured capture, saved as its own object; the Document the
	// workflow writes only carries its key. Best-effort like the HTML.
	if rec, rerr := parser.BuildPageRecord(body, in.URL, "text/html; charset=utf-8", parsed); rerr == nil {
		rec.URL, rec.NormalizedURL, rec.FinalURL, rec.Host = in.URL, out.NormalizedURL, out.FinalURL, host
		if out.CanonicalURL != "" {
			rec.CanonicalURL = out.CanonicalURL
		}
		rec.Depth, rec.StatusCode, rec.ContentType, rec.ContentHash = in.Depth, out.StatusCode, out.ContentType, out.ContentHash
		rec.FetchedAt = time.Now().UTC()
		rec.HTMLKey, rec.RenderedHTMLKey, rec.Rendered, rec.RenderReason = out.HTMLKey, out.RenderedHTMLKey, out.Rendered, renderReason
		rec.NoIndex, rec.NoFollow = out.NoIndex, out.NoFollow
		if key, serr := a.Records.SaveRecord(rec); serr == nil {
			out.RecordKey = key
		}
	}

	// This is a fetch-level success metric: the HTTP request succeeded and
	// the HTML parsed cleanly. Whether the workflow goes on to actually
	// store it (it might not, for a NoIndex or near-duplicate page) is a
	// separate business decision layered on top, out of scope for this
	// activity-level metric.
	metrics.PagesFetched.WithLabelValues(metrics.OutcomeSuccess).Inc()

	return out, nil
}

// xRobotsTagDirectives parses one X-Robots-Tag header value. Per Google's
// spec it may optionally be scoped to a specific bot ("googlebot: noindex")
// rather than being a bare directive list ("noindex, nofollow") -- if the
// value looks like "name: directives" (a single token before the first
// colon, no comma/space in it), only the part after the colon is treated
// as directives; a genuinely bare list is parsed as-is even though some
// unrelated directives (e.g. "unavailable_after: <date>") also contain a
// colon, since neither of those tokens are ones ParseRobotsDirectives acts
// on anyway.
func xRobotsTagDirectives(v string) (noindex, nofollow bool) {
	if idx := strings.Index(v, ":"); idx >= 0 {
		prefix := strings.TrimSpace(v[:idx])
		if prefix != "" && !strings.ContainsAny(prefix, ", ") {
			v = v[idx+1:]
		}
	}
	return parser.ParseRobotsDirectives(v)
}

// WriteDocumentInput is the argument to WriteDocument.
type WriteDocumentInput struct {
	Doc model.Document
}

// WriteDocument persists one ETL record. It is idempotent per content hash
// within a worker process's lifetime: if Temporal retries this activity
// (e.g. the worker crashed right after a successful write but before
// Temporal recorded completion), a duplicate write for the same content is
// silently skipped instead of appending a second copy.
func (a *Activities) WriteDocument(ctx context.Context, in WriteDocumentInput) error {
	key := in.Doc.NormalizedURL + "|" + in.Doc.ContentHash

	a.mu.Lock()
	if a.writtenHash[key] {
		a.mu.Unlock()
		return nil
	}
	a.writtenHash[key] = true // also claims the key while the write is in flight
	a.mu.Unlock()

	if err := a.Writer.Write(&in.Doc); err != nil {
		// Release the claim so Temporal's retry writes again (e.g. S3 stored
		// the document but Kafka failed); keeping it would drop the document.
		a.mu.Lock()
		delete(a.writtenHash, key)
		a.mu.Unlock()
		return err
	}
	return nil
}

// maxSitemapURLs caps how many page URLs DiscoverSitemapURLs returns for
// one seed. Real sitemaps can list up to 50,000 URLs each; the crawl's own
// MaxPages budget would cap actual fetching regardless, but this keeps one
// activity result (and the Temporal history event it's recorded in) bounded.
const maxSitemapURLs = 50000

// maxChildSitemaps caps how many sitemaps DiscoverSitemapURLs will follow
// from a sitemapindex, so a pathological or malicious index (thousands of
// child sitemaps) can't turn one activity call into thousands of fetches.
const maxChildSitemaps = 50

// DiscoverSitemapURLsInput is the argument to DiscoverSitemapURLs.
type DiscoverSitemapURLsInput struct {
	// SeedURL is used only for its scheme+host; any path is ignored.
	SeedURL string
	// MaxURLs caps how many page URLs are returned for this seed (0 = the
	// package default, maxSitemapURLs). The workflow sets it to the
	// per-domain page cap: URLs beyond it would be dropped anyway, but
	// returning them bloats workflow history -- thousands of seeds each
	// returning up to maxSitemapURLs URLs can exceed Temporal's history size
	// limit.
	MaxURLs int
}

// DiscoverSitemapURLsOutput is the result of DiscoverSitemapURLs.
type DiscoverSitemapURLsOutput struct {
	URLs []string
}

// DiscoverSitemapURLs finds page URLs to seed the frontier with beyond
// link-following: it checks robots.txt for Sitemap: directives (falling
// back to the conventional /sitemap.xml location if none are declared),
// fetches each one, and recurses into any sitemapindex it finds (bounded by
// maxChildSitemaps) to collect the page URLs a plain urlset lists.
//
// This is a best-effort discovery step, not part of the crawl's correctness:
// any error here (a bad seed URL, no sitemap.xml, a malformed one) yields
// an empty result and a nil error rather than failing the crawl, since the
// crawl can always fall back to discovering pages by following links.
func (a *Activities) DiscoverSitemapURLs(ctx context.Context, in DiscoverSitemapURLsInput) (DiscoverSitemapURLsOutput, error) {
	var out DiscoverSitemapURLsOutput

	seed, err := url.Parse(in.SeedURL)
	if err != nil || seed.Scheme == "" || seed.Host == "" {
		return out, nil
	}

	sitemapURLs, err := a.Robots.Sitemaps(ctx, in.SeedURL)
	if err != nil {
		// No answer from robots.txt means the origin may not be crawled yet
		// (RFC 9309); the seed's own ProcessPage retries it.
		return out, nil
	}
	if len(sitemapURLs) == 0 {
		// Many sites serve /sitemap.xml without declaring it in robots.txt.
		sitemapURLs = []string{seed.Scheme + "://" + seed.Host + "/sitemap.xml"}
	}

	visited := make(map[string]bool)
	var pageURLs []string
	childrenFetched := 0
	limit := maxSitemapURLs
	if in.MaxURLs > 0 && in.MaxURLs < limit {
		limit = in.MaxURLs
	}

	var fetchSitemap func(sitemapURL string)
	fetchSitemap = func(sitemapURL string) {
		if visited[sitemapURL] || len(pageURLs) >= limit {
			return
		}
		visited[sitemapURL] = true

		res, err := a.Fetcher.Get(ctx, sitemapURL)
		if err != nil || res.StatusCode != http.StatusOK {
			return
		}
		urls, children, err := sitemap.ParseURLs(res.Body)
		if err != nil {
			return
		}
		for _, u := range urls {
			if len(pageURLs) >= limit {
				break
			}
			pageURLs = append(pageURLs, u)
		}
		for _, child := range children {
			if childrenFetched >= maxChildSitemaps {
				break
			}
			childrenFetched++
			fetchSitemap(child)
		}
	}

	for _, s := range sitemapURLs {
		fetchSitemap(s)
	}

	out.URLs = pageURLs
	return out, nil
}

// CheckFreshnessInput is the argument to CheckFreshness.
type CheckFreshnessInput struct {
	NormalizedURLs []string
	// MaxAge: a URL fetched more recently than this is "fresh" and should
	// be skipped rather than re-fetched.
	MaxAge time.Duration
}

// CheckFreshnessOutput is the result of CheckFreshness.
type CheckFreshnessOutput struct {
	// Fresh maps each of the input NormalizedURLs already crawled within
	// MaxAge to true. A URL absent from this map (not just false -- absent)
	// should be fetched: it's either stale or was never crawled before.
	Fresh map[string]bool
}

// CheckFreshness implements the revisit/freshness policy: it asks the
// configured storage.FreshnessChecker which of the given URLs already have
// a document on file recent enough not to bother re-fetching. Returns an
// empty result (fetch everything) when no persistent backend is configured
// (storage.NoopFreshnessChecker) -- the same behavior as before a revisit
// policy existed.
func (a *Activities) CheckFreshness(ctx context.Context, in CheckFreshnessInput) (CheckFreshnessOutput, error) {
	if len(in.NormalizedURLs) == 0 {
		return CheckFreshnessOutput{}, nil
	}
	fresh, err := a.Freshness.FreshURLs(ctx, in.NormalizedURLs, time.Now().Add(-in.MaxAge))
	if err != nil {
		return CheckFreshnessOutput{}, err
	}
	return CheckFreshnessOutput{Fresh: fresh}, nil
}

// StartCrawlRunInput is the argument to StartCrawlRun.
type StartCrawlRunInput struct {
	SeedCount int
	MaxDepth  int
	MaxPages  int
}

// StartCrawlRun records the start of a new crawl run and returns its ID,
// which the workflow carries through every Continue-As-New segment and uses
// for subsequent UpdateCrawlRunStats/FinishCrawlRun calls. Returns 0 when no
// run-tracking backend is configured (storage.NoopRunRecorder) -- the
// workflow treats runID == 0 as "skip run tracking for this crawl".
func (a *Activities) StartCrawlRun(ctx context.Context, in StartCrawlRunInput) (int64, error) {
	return a.Runs.StartRun(ctx, storage.StartRunInput{
		SeedCount: in.SeedCount,
		MaxDepth:  in.MaxDepth,
		MaxPages:  in.MaxPages,
	})
}

// UpdateCrawlRunStatsInput is the argument to UpdateCrawlRunStats.
type UpdateCrawlRunStatsInput struct {
	RunID        int64
	Fetched      int
	Succeeded    int
	Failed       int
	Skipped      int
	DomainCapped int
}

// UpdateCrawlRunStats records a still-running run's current progress, so
// another team can check a run's health while it's still going rather than
// only after it finishes.
func (a *Activities) UpdateCrawlRunStats(ctx context.Context, in UpdateCrawlRunStatsInput) error {
	return a.Runs.UpdateRunStats(ctx, in.RunID, storage.RunStats{
		Fetched:      in.Fetched,
		Succeeded:    in.Succeeded,
		Failed:       in.Failed,
		Skipped:      in.Skipped,
		DomainCapped: in.DomainCapped,
	})
}

// FinishCrawlRunInput is the argument to FinishCrawlRun.
type FinishCrawlRunInput struct {
	RunID        int64
	Status       string // "completed" or "failed"
	Fetched      int
	Succeeded    int
	Failed       int
	Skipped      int
	DomainCapped int
	Error        string
}

// FinishCrawlRun marks a run terminal with its final stats -- the
// validation summary another team checks before consuming this run's
// documents.
func (a *Activities) FinishCrawlRun(ctx context.Context, in FinishCrawlRunInput) error {
	return a.Runs.FinishRun(ctx, in.RunID, in.Status, storage.RunStats{
		Fetched:      in.Fetched,
		Succeeded:    in.Succeeded,
		Failed:       in.Failed,
		Skipped:      in.Skipped,
		DomainCapped: in.DomainCapped,
	}, in.Error)
}

// PublishSiteCrawledInput is the argument to PublishSiteCrawled.
type PublishSiteCrawledInput struct {
	RunID        int64
	WorkflowID   string
	Host         string
	Status       string // "completed" or "failed"
	Error        string
	PagesFetched int
	CompletedAt  time.Time
}

// PublishSiteCrawled tells ETL that one website's crawl has finished and
// every page of it is stored, so the site can be processed as a whole.
func (a *Activities) PublishSiteCrawled(ctx context.Context, in PublishSiteCrawledInput) error {
	return a.Sites.EmitSiteCrawled(ctx, in.RunID, in.WorkflowID, in.Host, in.Status, in.Error, in.PagesFetched, in.CompletedAt)
}

// parseRetryAfter interprets a Retry-After header, which per RFC 9110 is
// either a delay in seconds or an HTTP-date. Falls back to defaultRetryAfter
// when the header is absent or unparsable, and clamps to maxRetryAfter.
func parseRetryAfter(header string) time.Duration {
	delay := defaultRetryAfter
	if header != "" {
		if secs, err := strconv.Atoi(strings.TrimSpace(header)); err == nil {
			delay = time.Duration(secs) * time.Second
		} else if when, err := http.ParseTime(header); err == nil {
			delay = time.Until(when)
		}
	}
	if delay <= 0 {
		delay = defaultRetryAfter
	}
	if delay > maxRetryAfter {
		delay = maxRetryAfter
	}
	return delay
}

func truncateRunes(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max])
}
