// Package workflows contains the Temporal workflow that orchestrates a
// crawl. The workflow owns the frontier (queue + seen-set) as plain
// in-memory workflow state -- safe because workflow code executes as a
// single deterministic logical thread (Temporal replays it, it doesn't
// truly run it concurrently), so no locking is needed even though several
// ProcessPage activities are in flight at once.
//
// Failure handling is Temporal's job, not this file's: activity retries,
// backoff, and timeouts are configured once via ActivityOptions below, and
// Temporal persists workflow progress after every event, so a crashed or
// redeployed worker resumes an in-progress crawl (via replay) without
// losing the frontier or re-doing completed work.
package workflows

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"time"

	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"search-engine-scraper/internal/activities"
	"search-engine-scraper/internal/model"
	"search-engine-scraper/internal/normalize"
	"search-engine-scraper/internal/simhash"
)

// sitemapDiscoveryBatch bounds how many seeds' sitemap discovery activities
// run concurrently at workflow start.
const sitemapDiscoveryBatch = 100

// ShardForHost maps a hostname to one of shards task-queue shards using a
// stable hash, so every fetch for a given host lands on the same shard.
func ShardForHost(host string, shards int) int {
	if shards <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(strings.ToLower(host)))
	return int(h.Sum32() % uint32(shards))
}

// TaskQueueForShard is the task queue workers pinned to shard i poll for
// fetch activities.
func TaskQueueForShard(i int) string {
	return fmt.Sprintf("%s-shard-%d", TaskQueueName, i)
}

// TaskQueueForHost returns the queue a host's fetch activities run on:
// TaskQueueName itself when sharding is off (shards <= 1), otherwise the
// host's shard queue. All fetches for one host then run in one worker
// process, so that host's rate limit, Crawl-delay, robots.txt cache and DNS
// cache are enforced in a single place instead of once per replica.
func TaskQueueForHost(host string, shards int) string {
	if shards <= 1 {
		return TaskQueueName
	}
	return TaskQueueForShard(ShardForHost(host, shards))
}

const (
	// maxCarriedURLs is the largest frontier+seen set that is checkpointed
	// via Continue-As-New every pagesPerRun pages; beyond it the carried
	// payload would approach Temporal's 2MB payload limit.
	maxCarriedURLs = 6000
	// maxHistoryEvents forces a Continue-As-New well before Temporal's
	// 51200-event history limit.
	maxHistoryEvents = 45000
	// maxHistoryBytes does the same against Temporal's 50MB history size
	// limit, which large sitemap/page results can reach well before the
	// event-count limit.
	maxHistoryBytes = 30 << 20
)

// TaskQueueName is shared between the worker and the client that starts
// crawls -- both must point at the same Temporal task queue.
const TaskQueueName = "scraper-task-queue"

// pagesPerRun bounds how many pages one workflow execution processes before
// calling Continue-As-New. This keeps workflow history size (and therefore
// replay time and gRPC payload limits) bounded regardless of total crawl
// size -- the mechanism that lets this scale to large crawls.
const pagesPerRun = 50

// nearDupHammingThreshold: a page whose simhash fingerprint is within this
// many bits of one already written this crawl is treated as a
// near-duplicate (a timestamp/ad-slot/view-counter difference, not a
// substantively different page) and isn't written as a second document.
// 3 bits on a 64-bit fingerprint is a conservative choice -- see
// internal/simhash and docs/CRAWLER_ROADMAP.md for the tradeoffs.
const nearDupHammingThreshold = 3

// minTextLenForNearDupCheck guards against a degenerate case: a page with
// little or no text produces a low-signal (or, for truly empty text, an
// all-zero) fingerprint that would spuriously "match" every other
// low-text/empty-text page, incorrectly treating genuinely distinct pages
// (different image galleries, different download-only pages, ...) as
// near-duplicates of each other. Below this length, skip the check
// entirely rather than risk a false positive.
const minTextLenForNearDupCheck = 100

// act is a nil *Activities used only so workflow.ExecuteActivity can infer
// each activity's function signature via reflection; it is never called
// directly (Temporal dispatches activities by registered name to whichever
// worker picks up the task). This is the standard Temporal Go pattern for
// referencing activity methods with type safety.
var act *activities.Activities

// FrontierItem is one URL still queued to crawl.
type FrontierItem struct {
	URL      string
	Depth    int
	Category string // inherited from the seed this URL descended from
	// OriginHost is the host of the seed this branch descended from. Used
	// (when SameHostOnly is set) to keep each seed's own links on-site,
	// independently -- a multi-seed crawl with seeds on different hosts
	// still confines each seed's branch to its own domain instead of
	// comparing every discovered link back to a single global seed host.
	OriginHost string
	// Priority controls crawl order: among items currently startable (not
	// domain-capped, not waiting on a per-host concurrency slot), the
	// highest-Priority one is started next, ahead of the frontier's FIFO
	// order. Inherited unchanged from the seed a URL descended from (see
	// seeds.Seed.Priority / Seed.Priority below), so a seed's entire branch
	// keeps that seed's crawl priority as it's followed deeper.
	Priority int
}

// CrawlStats aggregates outcomes across the whole crawl (all Continue-As-New
// segments included).
type CrawlStats struct {
	Fetched   int
	Succeeded int
	Failed    int
	Skipped   int
	// DomainCapped counts URLs that were discovered but never fetched
	// because their host had already reached MaxPagesPerDomain.
	DomainCapped int
	// CountryFiltered counts pages that fetched and parsed fine but weren't
	// written because CountryFilter is set and the page's detected (or
	// undetected) country didn't match it.
	CountryFiltered int
}

// Seed is one starting URL, optionally tagged with a category (e.g. "news",
// "tech") so a single crawl can cover many unrelated sites/topics at once
// and downstream consumers can filter model.Document.Category.
type Seed struct {
	URL      string
	Category string
	// Priority sets this seed's (and everything discovered under it)
	// FrontierItem.Priority -- see that field for how it affects crawl
	// order.
	Priority int
}

// CrawlWorkflowInput starts a new crawl (when Frontier/Seen/PagesProcessed
// are zero-valued) or continues one across a Continue-As-New boundary.
type CrawlWorkflowInput struct {
	Seeds []Seed
	// MaxDepth is link-hops from a seed. MaxPages is the total page budget
	// across every seed/category combined for this whole crawl.
	MaxDepth    int
	MaxPages    int
	Concurrency int
	// SameHostOnly, when true, keeps each seed's discovered links confined
	// to that seed's own host. Set false for an open crawl that follows
	// outbound links anywhere they lead -- the broad, whole-web behavior a
	// real search engine crawler uses -- bounded only by MaxDepth/MaxPages.
	SameHostOnly bool
	// MaxPagesPerDomain caps how many pages any single host can contribute
	// to MaxPages. Zero means no per-domain cap (the whole MaxPages budget
	// is first-come-first-served, so a fast-responding site can crowd out
	// slower ones). Set this on multi-seed/open crawls so one domain can't
	// dominate the shared page budget.
	MaxPagesPerDomain int
	// MaxConcurrentPerHost caps how many fetches to any single host may be
	// in flight at once, independent of the global Concurrency cap. Zero
	// means no per-host cap -- Concurrency alone governs how many fetches
	// run at once, so one fast/large site can claim the whole concurrency
	// budget and starve fetches meant for other hosts. Set this (e.g. 1-2)
	// on multi-seed/open crawls so a single site can't do that.
	MaxConcurrentPerHost int
	// TaskQueueShards pins each host's fetch activities (ProcessPage,
	// DiscoverSitemapURLs) to one of this many shard task queues (see
	// TaskQueueForHost), so a single worker owns a given host. Zero or one
	// keeps every activity on TaskQueueName, served by any worker replica.
	TaskQueueShards int
	// DiscoveryPending/DiscoveryCursor carry sitemap discovery across a
	// Continue-As-New boundary: discovery runs concurrently with page
	// fetching, so a segment can end with seeds not yet discovered.
	// DiscoveryCursor is the index of the next seed to discover.
	DiscoveryPending bool
	DiscoveryCursor  int
	// ChildOfRun marks this crawl as one domain of a CrawlDomainsWorkflow:
	// CrawlRunID is the parent's run, which the parent starts, aggregates
	// and finishes, so this workflow must not start or finish it itself.
	ChildOfRun bool
	// RevisitAfter enables the revisit/freshness policy: a candidate URL
	// (seed, sitemap-discovered, or a discovered link) that already has a
	// document on file fetched more recently than this is skipped rather
	// than (re-)fetched. Zero disables the policy entirely -- no
	// CheckFreshness activity calls are made, and every candidate not
	// already `seen` this crawl is fetched, the original behavior. Only
	// takes effect with a persistent storage backend (Postgres); with
	// --storage=ndjson there's no history to check against
	// (storage.NoopFreshnessChecker reports nothing as fresh), so setting
	// this without Postgres is a no-op.
	RevisitAfter time.Duration

	// CountryFilter, when non-empty (an ISO 3166-1 alpha-2 code, e.g.
	// "NP"), restricts which fetched pages actually get written as
	// Documents to ones internal/parser.DetectCountry resolved to that
	// country -- everything else is still fetched, parsed, and has its
	// links followed exactly as normal (so a global/regional index page
	// still leads the crawl to in-country pages linked from it), it's just
	// not persisted. A page whose country couldn't be detected at all is
	// treated as a non-match and also not written, since "unknown" can't be
	// confirmed as the target country. Empty disables filtering entirely --
	// every fetched page is eligible for writing, the crawler's default,
	// whole-web behavior.
	CountryFilter string

	// Carried state; only set when continuing a crawl.
	Frontier       []FrontierItem
	Seen           []string
	PagesProcessed int
	Stats          CrawlStats
	DomainCounts   map[string]int
	// SimHashes carries every written document's near-duplicate
	// fingerprint forward across Continue-As-New segments, so the
	// near-duplicate check (see nearDupHammingThreshold) sees the whole
	// crawl's history, not just the current segment's. Known scaling
	// limitation: this grows unboundedly with crawl size (same tradeoff
	// Seen/DomainCounts already make) and the check is an O(n) scan per
	// page, so a very large MaxPages crawl pays an O(n^2) total cost --
	// acceptable for this crawler's typical scale, not for a
	// web-scale corpus (see docs/CRAWLER_ROADMAP.md).
	SimHashes []uint64
	// CrawlRunID identifies this crawl's row in the crawl_runs table (see
	// internal/storage.RunRecorder) -- 0 on the very first segment (before
	// StartCrawlRun has run), carried unchanged across every subsequent
	// Continue-As-New segment so they all update the same run record.
	CrawlRunID int64
}

// CrawlResult is the workflow's terminal output.
type CrawlResult struct {
	Stats CrawlStats
	// RunID is the crawl_runs row other teams can query for this run's
	// validation summary (0 if no run-tracking backend is configured).
	RunID int64
}

// hostCtx routes activities started with the returned context to host's
// shard queue; with sharding off it returns ctx unchanged.
func hostCtx(ctx workflow.Context, host string, shards int) workflow.Context {
	if shards <= 1 {
		return ctx
	}
	return workflow.WithTaskQueue(ctx, TaskQueueForHost(host, shards))
}

// CrawlWorkflow is the Temporal workflow entrypoint for a crawl.
func CrawlWorkflow(ctx workflow.Context, in CrawlWorkflowInput) (result CrawlResult, err error) {
	logger := workflow.GetLogger(ctx)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    5,
		},
	})

	concurrency := in.Concurrency
	if concurrency < 1 {
		concurrency = 1
	}

	seen := make(map[string]bool, len(in.Seen))
	var seenOrder []string // deterministic replacement for ranging over the `seen` map
	addSeen := func(canonicalURL string) {
		if !seen[canonicalURL] {
			seen[canonicalURL] = true
			seenOrder = append(seenOrder, canonicalURL)
		}
	}
	for _, u := range in.Seen {
		addSeen(u)
	}

	queue := append([]FrontierItem(nil), in.Frontier...)

	// frontierCandidate pairs a dedupe key with the FrontierItem it would
	// become, for the two-pass "collect candidates, batch-check freshness,
	// then enqueue" pattern used at every place URLs get added to the
	// frontier (seeds, sitemap-discovered URLs, discovered links).
	type frontierCandidate struct {
		key  string
		item FrontierItem
	}
	// enqueueFresh marks every candidate seen (regardless of outcome, so
	// none are reconsidered again this run) and enqueues the ones the
	// revisit/freshness policy doesn't consider fresh. A no-op freshness
	// check (RevisitAfter == 0, or the activity call itself failing) treats
	// nothing as fresh, so every candidate gets enqueued -- the same
	// behavior as before this policy existed.
	enqueueFresh := func(candidates []frontierCandidate) {
		if len(candidates) == 0 {
			return
		}
		keys := make([]string, len(candidates))
		for i, c := range candidates {
			keys[i] = c.key
		}
		fresh := checkFreshness(ctx, in.RevisitAfter, keys, logger)
		for _, c := range candidates {
			if seen[c.key] {
				// Duplicate key within this same batch (e.g. two links on
				// one page resolving to the same canonical URL) -- already
				// handled by an earlier candidate in this loop.
				continue
			}
			addSeen(c.key)
			if fresh[c.key] {
				continue
			}
			queue = append(queue, c.item)
		}
	}

	isFirstRun := in.PagesProcessed == 0 && len(in.Frontier) == 0 && len(in.Seen) == 0
	if isFirstRun {
		var seedCandidates []frontierCandidate
		for _, s := range in.Seeds {
			key := normalize.Canonical(s.URL)
			if seen[key] {
				continue
			}
			seedCandidates = append(seedCandidates, frontierCandidate{
				key: key,
				item: FrontierItem{
					URL: s.URL, Depth: 0, Category: s.Category, OriginHost: normalize.Hostname(s.URL), Priority: s.Priority,
				},
			})
		}
		enqueueFresh(seedCandidates)

	}

	// Sitemap discovery runs for every seed regardless of whether the seed
	// page itself was fresh -- a site's sitemap can list new or changed URLs
	// independent of whether its own front page changed. It runs
	// concurrently with page fetching (seeds are already queued above), a
	// bounded window of seeds at a time, so the first pages are fetched
	// immediately instead of after every seed's sitemap has been looked up.
	discoveryPending := isFirstRun || in.DiscoveryPending
	discCursor := in.DiscoveryCursor
	sitemapCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 60 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 2},
	})
	type discInFlight struct {
		future workflow.Future
		seed   Seed
	}
	var discActive []discInFlight

	stats := in.Stats
	budgetRemaining := in.MaxPages - in.PagesProcessed
	pagesThisRun := 0
	continuing := false // true once we've hit the per-run checkpoint and are draining in-flight work
	// willContinueAsNew is distinct from continuing: continuing can flip true
	// at the same moment the queue drains for good, in which case the
	// function still returns a true terminal result below rather than a
	// Continue-As-New error. The defer below needs to know which actually
	// happened to decide between an interim stats update and finalizing the
	// crawl_runs row.
	willContinueAsNew := false

	domainCounts := make(map[string]int, len(in.DomainCounts))
	for host, n := range in.DomainCounts {
		domainCounts[host] = n
	}

	simHashes := append([]uint64(nil), in.SimHashes...)

	runID := in.CrawlRunID
	if isFirstRun && !in.ChildOfRun {
		var startErr error
		if runID, startErr = startCrawlRun(ctx, in); startErr != nil {
			// Run-tracking is a validation aid, not load-bearing: don't fail
			// an otherwise-healthy crawl just because its health record
			// couldn't be created.
			logger.Warn("failed to start crawl run tracking", "error", startErr)
		}
	}
	result.RunID = runID

	// Best-effort: keep the crawl_runs row current. On a Continue-As-New
	// handoff this segment is being superseded (not finished), so only push
	// an interim progress update; the segment that actually terminates
	// finalizes the row. Skipped entirely when runID == 0 (no run-tracking
	// backend configured), so an NDJSON-only deployment doesn't pay for
	// activities that would just no-op.
	//
	// The segment that terminates also publishes one "site crawled" event per
	// website this crawl fetched (one for a CrawlDomainsWorkflow child): the
	// hand-off that starts ETL for that whole site.
	defer func() {
		// A cancelled crawl must still record its end and publish its sites.
		ctx, _ := workflow.NewDisconnectedContext(ctx)
		if willContinueAsNew {
			if in.ChildOfRun || runID == 0 {
				return
			}
			if updErr := updateCrawlRunStats(ctx, runID, stats); updErr != nil {
				logger.Warn("failed to update crawl run stats", "run_id", runID, "error", updErr)
			}
			return
		}
		status := "completed"
		errMsg := ""
		if err != nil {
			status = "failed"
			errMsg = err.Error()
		}
		if !in.ChildOfRun && runID != 0 {
			if finErr := finishCrawlRun(ctx, runID, status, stats, errMsg); finErr != nil {
				logger.Warn("failed to finish crawl run", "run_id", runID, "error", finErr)
			}
		}
		publishSitesCrawled(ctx, runID, domainCounts, status, errMsg, logger)
	}()

	type inFlight struct {
		future workflow.Future
		item   FrontierItem
	}
	var active []inFlight

	// activeHostCounts tracks fetches currently in flight per host (as
	// opposed to domainCounts, which is a running total across the whole
	// crawl for MaxPagesPerDomain). Incremented when a fetch for that host
	// starts below, decremented when it completes in the main loop.
	activeHostCounts := make(map[string]int)

	// startNext scans the whole queue (not just its front) to permanently
	// drop any item whose host has already hit MaxPagesPerDomain, and among
	// the remaining startable items -- excluding any whose host is already
	// at MaxConcurrentPerHost, left in the queue to retry once that host
	// frees a slot -- starts the one with the highest Priority. Ties break
	// by queue order (earliest-enqueued first), so same-priority items still
	// behave like the original FIFO frontier.
	startNext := func() bool {
		if budgetRemaining <= 0 {
			return false
		}
		bestIdx := -1
		for i := 0; i < len(queue); i++ {
			item := queue[i]
			host := normalize.Hostname(item.URL)

			if in.MaxPagesPerDomain > 0 && domainCounts[host] >= in.MaxPagesPerDomain {
				// bestIdx, if set, always points at an index < i here: this
				// forward scan only ever sets it from indices already
				// visited (0..i-1), never from i itself (a capped item
				// continues before reaching that assignment) or beyond.
				// Removing index i therefore never invalidates bestIdx, so
				// no compensating adjustment is needed.
				queue = append(queue[:i], queue[i+1:]...)
				stats.DomainCapped++
				i--
				continue
			}
			if in.MaxConcurrentPerHost > 0 && activeHostCounts[host] >= in.MaxConcurrentPerHost {
				continue
			}
			if bestIdx == -1 || item.Priority > queue[bestIdx].Priority {
				bestIdx = i
			}
		}
		if bestIdx == -1 {
			return false
		}

		item := queue[bestIdx]
		host := normalize.Hostname(item.URL)
		queue = append(queue[:bestIdx], queue[bestIdx+1:]...)
		budgetRemaining--
		domainCounts[host]++
		activeHostCounts[host]++
		fut := workflow.ExecuteActivity(hostCtx(ctx, host, in.TaskQueueShards), act.ProcessPage, activities.ProcessPageInput{
			URL: item.URL, Depth: item.Depth,
		})
		active = append(active, inFlight{future: fut, item: item})
		return true
	}
	fillActive := func() {
		for len(active) < concurrency && startNext() {
		}
	}

	// launchDiscovery keeps up to sitemapDiscoveryBatch seed discoveries in
	// flight. It stops launching once the crawl is checkpointing (continuing)
	// or no page budget is left to spend on what discovery would find.
	launchDiscovery := func() {
		for discoveryPending && !continuing && budgetRemaining > 0 &&
			discCursor < len(in.Seeds) && len(discActive) < sitemapDiscoveryBatch {
			sd := in.Seeds[discCursor]
			discCursor++
			discActive = append(discActive, discInFlight{
				future: workflow.ExecuteActivity(hostCtx(sitemapCtx, normalize.Hostname(sd.URL), in.TaskQueueShards),
					act.DiscoverSitemapURLs, activities.DiscoverSitemapURLsInput{SeedURL: sd.URL, MaxURLs: in.MaxPagesPerDomain}),
				seed: sd,
			})
		}
	}

	launchDiscovery()
	fillActive()
	for len(active) > 0 || len(discActive) > 0 {
		sel := workflow.NewSelector(ctx)
		doneIdx := -1
		discDoneIdx := -1
		var res activities.ProcessPageOutput
		var actErr error
		var discRes activities.DiscoverSitemapURLsOutput
		var discErr error

		for i := range active {
			idx := i
			sel.AddFuture(active[idx].future, func(f workflow.Future) {
				doneIdx = idx
				actErr = f.Get(ctx, &res)
			})
		}
		for i := range discActive {
			idx := i
			sel.AddFuture(discActive[idx].future, func(f workflow.Future) {
				discDoneIdx = idx
				discErr = f.Get(ctx, &discRes)
			})
		}
		sel.Select(ctx)

		if discDoneIdx >= 0 {
			sd := discActive[discDoneIdx].seed
			discActive = append(discActive[:discDoneIdx], discActive[discDoneIdx+1:]...)
			if discErr != nil {
				logger.Warn("sitemap discovery failed", "seed", sd.URL, "error", discErr)
			} else {
				originHost := normalize.Hostname(sd.URL)
				var sitemapCandidates []frontierCandidate
				for _, u := range discRes.URLs {
					key := normalize.Canonical(u)
					if seen[key] {
						continue
					}
					sitemapCandidates = append(sitemapCandidates, frontierCandidate{
						key:  key,
						item: FrontierItem{URL: u, Depth: 0, Category: sd.Category, OriginHost: originHost, Priority: sd.Priority},
					})
				}
				enqueueFresh(sitemapCandidates)
			}
			launchDiscovery()
			if !continuing {
				fillActive()
			}
			continue
		}

		doneItem := active[doneIdx].item
		active = append(active[:doneIdx], active[doneIdx+1:]...)
		activeHostCounts[normalize.Hostname(doneItem.URL)]--
		pagesThisRun++
		stats.Fetched++

		// parsedOK marks a page that was actually fetched and parsed --
		// distinct from Succeeded, since a NoIndex page falls into this
		// group too: it's eligible to have its links followed below even
		// though it isn't written as a Document.
		parsedOK := false

		switch {
		case actErr != nil:
			logger.Warn("page permanently failed after activity retries", "url", doneItem.URL, "error", actErr)
			stats.Failed++
		case res.Skipped:
			stats.Skipped++
		case res.FetchError != "":
			logger.Warn("page fetch/parse error", "url", doneItem.URL, "error", res.FetchError)
			stats.Failed++
		case res.NoIndex:
			// <meta name="robots"> or X-Robots-Tag declared noindex: the
			// page asked not to be stored. Counted under the same Skipped
			// stat as robots.txt-disallowed/non-HTML pages rather than a
			// separate counter -- all three mean "fetched but not indexed"
			// from a validation-summary point of view.
			parsedOK = true
			stats.Skipped++
		case len(res.Text) >= minTextLenForNearDupCheck && simhash.AnyWithin(simHashes, res.SimHash, nearDupHammingThreshold):
			// Near-duplicate of a page already written this crawl (differs
			// only by e.g. a timestamp/ad-slot/view-counter, not
			// substantively) -- exact content-hash dedup wouldn't have
			// caught this since the bytes genuinely differ. Same Skipped
			// stat as NoIndex/robots-disallowed/non-HTML for the same
			// reason: another "fetched but not indexed" outcome, not
			// worth a fourth crawl_runs counter.
			parsedOK = true
			stats.Skipped++
		case in.CountryFilter != "" && !strings.EqualFold(res.Country, in.CountryFilter):
			// Fetched and parsed fine, just not the country this crawl is
			// scoped to -- still eligible to have its links followed below
			// (parsedOK), just not written as a Document.
			parsedOK = true
			stats.CountryFiltered++
		default:
			parsedOK = true
			stats.Succeeded++
			simHashes = append(simHashes, res.SimHash)
			doc := model.Document{
				URL:             res.URL,
				NormalizedURL:   res.NormalizedURL,
				Host:            normalize.Hostname(res.URL),
				FinalURL:        res.FinalURL,
				CanonicalURL:    res.CanonicalURL,
				Category:        doneItem.Category,
				Title:           res.Title,
				MetaDescription: res.MetaDescription,
				Text:            res.Text,
				Headings:        res.Headings,
				Links:           res.Links,
				AnchorTexts:     res.AnchorTexts,
				JSONLD:          res.JSONLD,
				Geo:             res.Geo,
				Country:         res.Country,
				SimHash:         res.SimHash,
				CrawlRunID:      runID,
				Depth:           doneItem.Depth,
				StatusCode:      res.StatusCode,
				ContentType:     res.ContentType,
				ContentHash:     res.ContentHash,
				FetchDurMs:      res.FetchDurMs,
				FetchedAt:       workflow.Now(ctx).UTC(),
				HTMLKey:         res.HTMLKey,
				RenderedHTMLKey: res.RenderedHTMLKey,
				PageRecordKey:   res.RecordKey,
				Rendered:        res.Rendered,
			}
			if err := workflow.ExecuteActivity(ctx, act.WriteDocument, activities.WriteDocumentInput{Doc: doc}).Get(ctx, nil); err != nil {
				logger.Warn("write document failed after retries", "url", doc.URL, "error", err)
			}
		}

		if parsedOK {
			if res.CanonicalURL != "" {
				// This page declared its canonical target already has an
				// equivalent document on file (just written above under
				// that identity). Mark it seen so if some other page later
				// links directly to it, the frontier doesn't re-fetch a
				// duplicate of content already captured.
				addSeen(res.CanonicalURL)
			}
			if res.FinalURL != "" {
				// Same reasoning as CanonicalURL above, but for redirects:
				// the content is already on file under res.FinalURL's
				// identity, so mark it seen too.
				addSeen(res.FinalURL)
			}

			// NoFollow (meta robots or X-Robots-Tag) means don't follow
			// this page's links even though we may have indexed it --
			// independent of NoIndex, so a page can be indexed-but-not-
			// followed just as easily as the reverse.
			if !res.NoFollow && doneItem.Depth < in.MaxDepth {
				var linkCandidates []frontierCandidate
				for _, link := range res.Links {
					if in.SameHostOnly && doneItem.OriginHost != "" && normalize.Hostname(link) != doneItem.OriginHost {
						continue
					}
					key := normalize.Canonical(link)
					if seen[key] {
						continue
					}
					linkCandidates = append(linkCandidates, frontierCandidate{
						key: key,
						item: FrontierItem{
							URL: link, Depth: doneItem.Depth + 1, Category: doneItem.Category, OriginHost: doneItem.OriginHost, Priority: doneItem.Priority,
						},
					})
				}
				enqueueFresh(linkCandidates)
			}
		}

		// Checkpoint every pagesPerRun pages, but not while the carried
		// state (frontier + seen set) is too large to fit a Temporal
		// payload; a large seed list is instead checkpointed only when the
		// history nears its hard event limit.
		if (pagesThisRun >= pagesPerRun && len(queue)+len(seenOrder) <= maxCarriedURLs) ||
			workflow.GetInfo(ctx).GetCurrentHistoryLength() >= maxHistoryEvents ||
			workflow.GetInfo(ctx).GetCurrentHistorySize() >= maxHistoryBytes {
			continuing = true
		}
		if !continuing {
			launchDiscovery()
			fillActive()
		}
		// if continuing, we deliberately stop starting new activities and
		// let `active` drain so nothing is orphaned by Continue-As-New.
	}

	if continuing && budgetRemaining > 0 && (len(queue) > 0 || (discoveryPending && discCursor < len(in.Seeds))) {
		willContinueAsNew = true
		return CrawlResult{}, workflow.NewContinueAsNewError(ctx, CrawlWorkflow, CrawlWorkflowInput{
			Seeds:                in.Seeds,
			MaxDepth:             in.MaxDepth,
			MaxPages:             in.MaxPages,
			Concurrency:          in.Concurrency,
			SameHostOnly:         in.SameHostOnly,
			MaxPagesPerDomain:    in.MaxPagesPerDomain,
			MaxConcurrentPerHost: in.MaxConcurrentPerHost,
			TaskQueueShards:      in.TaskQueueShards,
			ChildOfRun:           in.ChildOfRun,
			DiscoveryPending:     discoveryPending,
			DiscoveryCursor:      discCursor,
			Frontier:             queue,
			Seen:                 seenOrder,
			PagesProcessed:       in.PagesProcessed + pagesThisRun,
			Stats:                stats,
			DomainCounts:         domainCounts,
			CrawlRunID:           runID,
			SimHashes:            simHashes,
			RevisitAfter:         in.RevisitAfter,
			CountryFilter:        in.CountryFilter,
		})
	}

	if stats.Fetched > 0 && stats.Failed == stats.Fetched {
		// Every single page this crawl fetched ended up Failed (fetch/parse
		// error or activity exhausted its retries) -- not a quiet-but-valid
		// run (which would show up as Skipped/CountryFiltered instead), but
		// a real, actionable failure (e.g. the target is entirely
		// unreachable, or the crawler is badly misconfigured). Without this,
		// the crawl_runs row for a run like this was indistinguishable from
		// a healthy one: CrawlWorkflow's only other return statement is the
		// Continue-As-New one above, so `err` was otherwise never set and
		// the finishCrawlRun defer's "failed" branch was dead code -- see
		// docs/TASK-SPLIT-search-engine-scraper.md Person 2 checklist item
		// 7.
		err = fmt.Errorf("crawl fetched %d page(s), all %d failed", stats.Fetched, stats.Failed)
		return CrawlResult{Stats: stats, RunID: runID}, err
	}

	return CrawlResult{Stats: stats, RunID: runID}, nil
}

// checkFreshness wraps the CheckFreshness activity for the revisit policy.
// Returns nil (treat nothing as fresh, i.e. fetch everything) when the
// policy is disabled (revisitAfter == 0) or the activity call fails --
// freshness checking is an optimization, not correctness-critical, so a
// failure should fall back to the safe-but-wasteful "just fetch it" rather
// than risk silently dropping pages the crawl should have visited.
func checkFreshness(ctx workflow.Context, revisitAfter time.Duration, candidates []string, logger sdklog.Logger) map[string]bool {
	if revisitAfter <= 0 || len(candidates) == 0 {
		return nil
	}
	var out activities.CheckFreshnessOutput
	err := workflow.ExecuteActivity(ctx, act.CheckFreshness, activities.CheckFreshnessInput{
		NormalizedURLs: candidates,
		MaxAge:         revisitAfter,
	}).Get(ctx, &out)
	if err != nil {
		logger.Warn("freshness check failed, fetching all candidates", "error", err)
		return nil
	}
	return out.Fresh
}

// publishSitesCrawled publishes a SiteCrawled event for every host the
// crawl fetched pages from, in sorted order (workflow code must be
// deterministic, and map iteration isn't). The activity retries for up to an
// hour: a lost event means ETL never processes that site. A failure after
// that is logged, not returned, so it can't mask the crawl's own outcome.
func publishSitesCrawled(ctx workflow.Context, runID int64, domainCounts map[string]int, status, errMsg string, logger sdklog.Logger) {
	hosts := make([]string, 0, len(domainCounts))
	for host, n := range domainCounts {
		if host != "" && n > 0 {
			hosts = append(hosts, host)
		}
	}
	sort.Strings(hosts)
	pubCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout:    30 * time.Second,
		ScheduleToCloseTimeout: time.Hour,
		RetryPolicy:            &temporal.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2, MaximumInterval: time.Minute},
	})
	workflowID := workflow.GetInfo(ctx).WorkflowExecution.ID
	for _, host := range hosts {
		err := workflow.ExecuteActivity(pubCtx, act.PublishSiteCrawled, activities.PublishSiteCrawledInput{
			RunID:        runID,
			WorkflowID:   workflowID,
			Host:         host,
			Status:       status,
			Error:        errMsg,
			PagesFetched: domainCounts[host],
			CompletedAt:  workflow.Now(ctx),
		}).Get(ctx, nil)
		if err != nil {
			logger.Error("failed to publish site-crawled event; ETL will not process this site", "run_id", runID, "host", host, "error", err)
		}
	}
}

// startCrawlRun, updateCrawlRunStats, and finishCrawlRun wrap the
// corresponding activities.Activities methods so the main workflow body
// isn't cluttered with ExecuteActivity/Get boilerplate for what is, from
// the crawl's perspective, a side-channel health record.

func startCrawlRun(ctx workflow.Context, in CrawlWorkflowInput) (int64, error) {
	var runID int64
	err := workflow.ExecuteActivity(ctx, act.StartCrawlRun, activities.StartCrawlRunInput{
		SeedCount: len(in.Seeds),
		MaxDepth:  in.MaxDepth,
		MaxPages:  in.MaxPages,
	}).Get(ctx, &runID)
	return runID, err
}

func updateCrawlRunStats(ctx workflow.Context, runID int64, stats CrawlStats) error {
	return workflow.ExecuteActivity(ctx, act.UpdateCrawlRunStats, activities.UpdateCrawlRunStatsInput{
		RunID:        runID,
		Fetched:      stats.Fetched,
		Succeeded:    stats.Succeeded,
		Failed:       stats.Failed,
		Skipped:      stats.Skipped,
		DomainCapped: stats.DomainCapped,
	}).Get(ctx, nil)
}

func finishCrawlRun(ctx workflow.Context, runID int64, status string, stats CrawlStats, errMsg string) error {
	return workflow.ExecuteActivity(ctx, act.FinishCrawlRun, activities.FinishCrawlRunInput{
		RunID:        runID,
		Status:       status,
		Fetched:      stats.Fetched,
		Succeeded:    stats.Succeeded,
		Failed:       stats.Failed,
		Skipped:      stats.Skipped,
		DomainCapped: stats.DomainCapped,
		Error:        errMsg,
	}).Get(ctx, nil)
}
