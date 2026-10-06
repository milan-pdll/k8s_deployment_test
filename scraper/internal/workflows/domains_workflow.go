package workflows

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"search-engine-scraper/internal/activities"
	"search-engine-scraper/internal/normalize"
)

const (
	// domainsPerRun bounds how many domains one CrawlDomainsWorkflow
	// execution starts before it checkpoints with Continue-As-New, keeping
	// its own history (a few events per child) far below Temporal's limit.
	domainsPerRun = 1500
	// defaultConcurrentDomains is how many domains are crawled at once when
	// the input doesn't say.
	defaultConcurrentDomains = 20
	// defaultDomainBatchSize is how many domains make one batch. A batch is
	// crawled to the end, and its site-crawled events are published, before
	// the next batch starts, so ETL receives events in groups of this size.
	defaultDomainBatchSize = 10
)

// CrawlDomainsInput starts (or continues) a whole-domain crawl: every seed's
// host is crawled completely -- all its sub-pages, not just the seed page --
// as its own child CrawlWorkflow, with its own frontier, budget and history.
type CrawlDomainsInput struct {
	Seeds []Seed
	// PerDomain is the template for each domain's crawl: MaxDepth, MaxPages
	// (the page budget for ONE domain), Concurrency, MaxConcurrentPerHost,
	// TaskQueueShards, CountryFilter and RevisitAfter are used; SameHostOnly
	// is forced on and Seeds/run-tracking fields are set per domain.
	PerDomain CrawlWorkflowInput
	// MaxConcurrentDomains is how many child domain crawls run at once.
	MaxConcurrentDomains int
	// DomainBatchSize is how many domains are crawled per batch (default
	// 10). The next batch starts only after every domain of the current one
	// has finished and published its event.
	DomainBatchSize int

	// Carried state across Continue-As-New.
	Cursor     int
	Stats      CrawlStats
	CrawlRunID int64
	Domains    DomainsStats
}

// DomainsStats counts domain-level outcomes.
type DomainsStats struct {
	Started, Completed, Failed int
}

// CrawlDomainsResult is the aggregate over every domain.
type CrawlDomainsResult struct {
	Stats   CrawlStats
	RunID   int64
	Domains DomainsStats
}

type domainGroup struct {
	Host  string
	Seeds []Seed
}

// groupSeedsByHost collects each host's seeds (in input order), ordered by
// the highest seed priority first, then first appearance -- so priority
// still decides which domains are crawled first.
func groupSeedsByHost(seeds []Seed) []domainGroup {
	idx := map[string]int{}
	var groups []domainGroup
	for _, s := range seeds {
		host := normalize.Hostname(s.URL)
		if host == "" {
			continue
		}
		i, ok := idx[host]
		if !ok {
			i = len(groups)
			idx[host] = i
			groups = append(groups, domainGroup{Host: host})
		}
		groups[i].Seeds = append(groups[i].Seeds, s)
	}
	prio := func(g domainGroup) int {
		best := g.Seeds[0].Priority
		for _, s := range g.Seeds {
			if s.Priority > best {
				best = s.Priority
			}
		}
		return best
	}
	sort.SliceStable(groups, func(a, b int) bool { return prio(groups[a]) > prio(groups[b]) })
	return groups
}

// CrawlDomainsWorkflow crawls every seed's domain end to end. It starts one
// child CrawlWorkflow per host (a bounded number at a time); each child
// follows links and sitemaps across its whole host up to the per-domain
// budget. Results are aggregated into one crawl run.
func CrawlDomainsWorkflow(ctx workflow.Context, in CrawlDomainsInput) (result CrawlDomainsResult, err error) {
	logger := workflow.GetLogger(ctx)

	actCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2, MaximumInterval: 30 * time.Second, MaximumAttempts: 5},
	})

	groups := groupSeedsByHost(in.Seeds)
	concurrent := in.MaxConcurrentDomains
	if concurrent < 1 {
		concurrent = defaultConcurrentDomains
	}

	batchSize := in.DomainBatchSize
	if batchSize < 1 {
		batchSize = defaultDomainBatchSize
	}
	if concurrent > batchSize {
		concurrent = batchSize
	}

	runID := in.CrawlRunID
	if in.Cursor == 0 && runID == 0 {
		var startErr error
		if runID, startErr = startCrawlRun(actCtx, CrawlWorkflowInput{Seeds: in.Seeds, MaxDepth: in.PerDomain.MaxDepth, MaxPages: in.PerDomain.MaxPages * len(groups)}); startErr != nil {
			logger.Warn("failed to start crawl run tracking", "error", startErr)
		}
	}

	stats := in.Stats
	domains := in.Domains
	cursor := in.Cursor
	startedThisRun := 0
	startedInBatch := 0
	willContinueAsNew := false

	defer func() {
		// A cancelled run must still record its end.
		actCtx, _ := workflow.NewDisconnectedContext(actCtx)
		result = CrawlDomainsResult{Stats: stats, RunID: runID, Domains: domains}
		if runID == 0 {
			return
		}
		if willContinueAsNew {
			_ = updateCrawlRunStats(actCtx, runID, stats)
			return
		}
		status, errMsg := "completed", ""
		if err != nil {
			status, errMsg = "failed", err.Error()
		}
		if finErr := finishCrawlRun(actCtx, runID, status, stats, errMsg); finErr != nil {
			logger.Warn("failed to finish crawl run", "run_id", runID, "error", finErr)
		}
	}()

	type running struct {
		future workflow.ChildWorkflowFuture
		host   string
	}
	var active []running

	start := func() {
		for len(active) < concurrent && startedInBatch < batchSize && cursor < len(groups) && startedThisRun < domainsPerRun {
			g := groups[cursor]
			cursor++
			startedThisRun++
			startedInBatch++
			domains.Started++

			child := in.PerDomain
			child.Seeds = g.Seeds
			child.SameHostOnly = true
			child.MaxPagesPerDomain = child.MaxPages
			child.CrawlRunID = runID
			child.ChildOfRun = true
			child.Frontier, child.Seen, child.PagesProcessed = nil, nil, 0

			cctx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
				WorkflowID:  fmt.Sprintf("%s-domain-%s", workflow.GetInfo(ctx).WorkflowExecution.ID, g.Host),
				TaskQueue:   TaskQueueName,
				RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 1},
			})
			active = append(active, running{future: workflow.ExecuteChildWorkflow(cctx, CrawlWorkflow, child), host: g.Host})
		}
	}

	// One batch at a time: start up to batchSize domains, wait for all of
	// them (each publishes its site event as it finishes), then the next.
	for {
		startedInBatch = 0
		start()
		if len(active) == 0 {
			break
		}
		for len(active) > 0 {
			sel := workflow.NewSelector(ctx)
			done := -1
			var res CrawlResult
			var cerr error
			for i := range active {
				idx := i
				sel.AddFuture(active[idx].future, func(f workflow.Future) {
					done = idx
					cerr = f.Get(ctx, &res)
				})
			}
			sel.Select(ctx)

			host := active[done].host
			active = append(active[:done], active[done+1:]...)
			if cerr != nil {
				domains.Failed++
				logger.Warn("domain crawl failed", "host", host, "error", cerr)
				// A killed or timed-out child never ran its own end-of-crawl
				// publish; send the failed event here so ETL still picks up
				// whatever pages it stored.
				var term *temporal.TerminatedError
				var tout *temporal.TimeoutError
				if errors.As(cerr, &term) || errors.As(cerr, &tout) {
					pubCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
						StartToCloseTimeout:    30 * time.Second,
						ScheduleToCloseTimeout: time.Hour,
						RetryPolicy:            &temporal.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2, MaximumInterval: time.Minute},
					})
					if perr := workflow.ExecuteActivity(pubCtx, act.PublishSiteCrawled, activities.PublishSiteCrawledInput{
						RunID:       runID,
						WorkflowID:  workflow.GetInfo(ctx).WorkflowExecution.ID,
						Host:        host,
						Status:      "failed",
						Error:       cerr.Error(),
						CompletedAt: workflow.Now(ctx),
					}).Get(ctx, nil); perr != nil {
						logger.Error("failed to publish site-crawled event for killed domain crawl", "host", host, "error", perr)
					}
				}
			} else {
				domains.Completed++
				stats.Fetched += res.Stats.Fetched
				stats.Succeeded += res.Stats.Succeeded
				stats.Failed += res.Stats.Failed
				stats.Skipped += res.Stats.Skipped
				stats.DomainCapped += res.Stats.DomainCapped
				stats.CountryFiltered += res.Stats.CountryFiltered
			}
			start()
		}
	}

	if cursor < len(groups) {
		willContinueAsNew = true
		return CrawlDomainsResult{}, workflow.NewContinueAsNewError(ctx, CrawlDomainsWorkflow, CrawlDomainsInput{
			Seeds: in.Seeds, PerDomain: in.PerDomain, MaxConcurrentDomains: in.MaxConcurrentDomains, DomainBatchSize: in.DomainBatchSize,
			Cursor: cursor, Stats: stats, CrawlRunID: runID, Domains: domains,
		})
	}
	return CrawlDomainsResult{Stats: stats, RunID: runID, Domains: domains}, nil
}

// childWasKilled reports whether a child workflow ended by termination or
// timeout, so its own deferred publish never ran.
func childWasKilled(err error) bool {
	var term *temporal.TerminatedError
	var tout *temporal.TimeoutError
	return errors.As(err, &term) || errors.As(err, &tout)
}
