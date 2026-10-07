// Command worker runs a Temporal worker that executes CrawlWorkflow and its
// activities. This is where actual concurrency and scale-out happen:
//   - within one process, up to --max-concurrent-activities activities run
//     as concurrent goroutines (the Temporal SDK's activity worker pool) --
//     it defaults to a large, CPU-count-scaled number since fetching is
//     I/O-bound, not CPU-bound (see maxConcurrentDefault below);
//   - horizontally, run this binary multiple times (same task queue,
//     same/different machines/pods) and Temporal load-balances activity and
//     workflow tasks across all of them automatically -- this is the knob
//     Docker/Kubernetes scale with (docker compose --scale, a Deployment's
//     replica count or HPA).
//
// Every flag can also be set via an environment variable of the same name
// (upper-cased, dashes to underscores, e.g. --s3-bucket ==
// S3_BUCKET), which is how the Docker/Kubernetes deployment configures
// this binary without a wrapper script.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	_ "go.uber.org/automaxprocs" // sets GOMAXPROCS from the container's cgroup CPU quota, not the host's core count

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"search-engine-scraper/internal/activities"
	"search-engine-scraper/internal/envflag"
	"search-engine-scraper/internal/fetcher"
	"search-engine-scraper/internal/render"
	"search-engine-scraper/internal/robots"
	"search-engine-scraper/internal/storage"
	"search-engine-scraper/internal/workflows"
)

func main() {
	var (
		hostPort       = envflag.String("temporal-address", "localhost:7233", "Temporal frontend address")
		namespace      = envflag.String("namespace", "default", "Temporal namespace")
		storageKind    = envflag.String("storage", "s3", "where to write crawled data: s3 (complete HTML + metadata objects, no database) or ndjson (local dev)")
		output         = envflag.String("output", "data/output/documents.ndjson", "NDJSON output path (used when --storage=ndjson)")
		s3Bucket       = envflag.String("s3-bucket", "", "S3 bucket crawled pages are uploaded to (required when --storage=s3)")
		s3Prefix       = envflag.String("s3-prefix", "", "optional key prefix inside the bucket, e.g. an environment name")
		s3Endpoint     = envflag.String("s3-endpoint", "", "S3-compatible endpoint URL for LocalStack/MinIO (empty = real AWS S3)")
		awsRegion      = envflag.String("aws-region", "", "AWS region (empty = resolve from AWS_REGION / shared config)")
		kafkaBrokers   = envflag.String("kafka-brokers", "", "comma-separated Kafka broker addresses, e.g. kafka-1:9092,kafka-2:9092; when set, one event per website whose crawl finished is published to --kafka-topic (requires --storage=s3)")
		kafkaTopic     = envflag.String("kafka-topic", "scraped_files_topic", "Kafka topic for the site-crawled events -- the hand-off point to the ETL pipeline")
		taskShards     = envflag.Int("task-queue-shards", 1, "total number of host-shard task queues the crawl uses (1 = off); must match the scraper client's --task-queue-shards")
		shardIndex     = envflag.Int("shard-index", -1, "which shard's fetch queue this worker polls (0..task-queue-shards-1); -1 = poll every shard queue")
		shardFromHost  = envflag.Bool("shard-from-hostname", false, "derive --shard-index from the trailing ordinal of the hostname (e.g. worker-2 -> 2, a Kubernetes StatefulSet pod), modulo --task-queue-shards")
		renderFlag     = envflag.String("render", "auto", "render pages in headless Chrome so JavaScript-built content is captured: off, auto (only pages that look like an empty JS app shell), or always")
		chromeURL      = envflag.String("chrome-url", "", "DevTools websocket of a headless Chrome, e.g. ws://chrome:9222 (required unless --render=off)")
		renderConc     = envflag.Int("render-concurrency", 4, "max pages this worker renders in Chrome at once")
		renderTimeout  = envflag.Duration("render-timeout", 30*time.Second, "per-page render timeout")
		requestTimeout = envflag.Duration("timeout", 10*time.Second, "per-request HTTP timeout")
		maxConcurrent  = envflag.Int("max-concurrent-activities", maxConcurrentDefault(), "max activities this worker process executes concurrently (fetching is I/O-bound, so this can safely exceed core count)")
		userAgent      = envflag.String("user-agent", "search-engine-scraper", "User-Agent / robots.txt group name to identify as")
		metricsAddress = envflag.String("metrics-address", ":9090", "address to serve Prometheus metrics on (GET /metrics); empty disables it")
		maxBandwidth   = envflag.Int("max-bandwidth-bytes-per-sec", 0, "cap this worker process's aggregate download rate in bytes/sec across all concurrent fetches (0 = unlimited)")
		enableHTTP3    = envflag.Bool("http3", false, "attempt HTTP/3 (QUIC) first on https requests, falling back to HTTP/1.1 or HTTP/2 for sites that don't support it")
		saveHTMLDir    = envflag.String("save-html-dir", "", "directory to save each crawled page's raw HTML body to, named by content hash (empty = don't save raw HTML)")
	)
	flag.Parse()

	if *metricsAddress != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		go func() {
			// Best-effort: a metrics-port conflict shouldn't take down the
			// crawler itself, just observability into it.
			if err := http.ListenAndServe(*metricsAddress, mux); err != nil {
				log.Printf("metrics server stopped: %v", err)
			}
		}()
		log.Printf("metrics: http://%s/metrics", *metricsAddress)
	}

	var (
		primary   storage.Writer
		runs      storage.RunRecorder      = storage.NoopRunRecorder{}
		freshness storage.FreshnessChecker = storage.NoopFreshnessChecker{}
		htmlStore storage.HTMLWriter       = storage.NoopHTMLWriter{}
		records   storage.RecordWriter     = storage.NoopRecordWriter{}
		err       error
	)
	if *storageKind == "s3" {
		s3b, err := buildS3Backend(*s3Bucket, *s3Prefix, *s3Endpoint, *awsRegion)
		if err != nil {
			log.Fatalf("open s3 storage: %v", err)
		}
		primary, runs, freshness, htmlStore, records = s3b.writer, s3b.runs, s3b.freshness, s3b.html, s3b.records
	} else {
		primary, err = buildWriter(*storageKind, *output)
		if err != nil {
			log.Fatalf("open storage writer: %v", err)
		}
	}
	writer := primary
	defer writer.Close()

	// The ETL hand-off: one Kafka event per website whose crawl finished,
	// pointing at that site's documents in the bucket. Nothing is published
	// per page or per file.
	var sites storage.SiteEventEmitter = storage.NoopSiteEventEmitter{}
	if *kafkaBrokers != "" {
		if *storageKind != "s3" {
			log.Fatal("--kafka-brokers requires --storage=s3: the site-crawled event points ETL at the site's objects in the bucket")
		}
		emitter, err := storage.NewKafkaSiteEventEmitter(storage.ParseBrokers(*kafkaBrokers), *kafkaTopic, *s3Bucket, *s3Prefix)
		if err != nil {
			log.Fatalf("open kafka site-event emitter: %v", err)
		}
		defer func() {
			// Events are written synchronously; a Close error only means the
			// connection did not shut down cleanly.
			if err := emitter.Close(); err != nil {
				log.Printf("close kafka site-event emitter: %v", err)
			}
		}()
		sites = emitter
	}

	c, err := client.Dial(client.Options{HostPort: *hostPort, Namespace: *namespace})
	if err != nil {
		log.Fatalf("connect to temporal at %s: %v", *hostPort, err)
	}
	defer c.Close()

	var fetcherOpts []fetcher.Option
	if *enableHTTP3 {
		fetcherOpts = append(fetcherOpts, fetcher.WithHTTP3())
	}
	f := fetcher.New(*requestTimeout, *maxBandwidth, fetcherOpts...)
	guard := robots.New(f, *userAgent)
	act := activities.New(f, guard, writer, runs, freshness)
	act.HTML = htmlStore
	act.Records = records
	act.Sites = sites

	mode, merr := render.ParseMode(*renderFlag)
	if merr != nil {
		log.Fatal(merr)
	}
	if mode != render.ModeOff {
		if *chromeURL == "" {
			log.Fatal("--chrome-url (or CHROME_URL) is required unless --render=off")
		}
		ch := render.NewChrome(*chromeURL, *userAgent, *renderConc, *renderTimeout)
		defer ch.Close()
		act.Renderer, act.RenderMode = ch, mode
	}

	if *saveHTMLDir != "" {
		htmlWriter, err := storage.NewDirHTMLWriter(*saveHTMLDir)
		if err != nil {
			log.Fatalf("open html output dir: %v", err)
		}
		act.HTML = htmlWriter
	}

	w := worker.New(c, workflows.TaskQueueName, worker.Options{
		MaxConcurrentActivityExecutionSize: *maxConcurrent,
	})
	w.RegisterWorkflow(workflows.CrawlWorkflow)
	w.RegisterWorkflow(workflows.CrawlDomainsWorkflow)
	w.RegisterActivity(act.ProcessPage)
	w.RegisterActivity(act.WriteDocument)
	w.RegisterActivity(act.DiscoverSitemapURLs)
	w.RegisterActivity(act.CheckFreshness)
	w.RegisterActivity(act.StartCrawlRun)
	w.RegisterActivity(act.UpdateCrawlRunStats)
	w.RegisterActivity(act.FinishCrawlRun)
	w.RegisterActivity(act.PublishSiteCrawled)

	// Host-sharded fetching: besides the shared queue (workflow tasks and
	// storage/run activities), poll the shard queue(s) this worker owns for
	// ProcessPage/DiscoverSitemapURLs, so all fetches for one host run here.
	idx := *shardIndex
	if *shardFromHost {
		hn, _ := os.Hostname()
		idx = ordinalFromHostname(hn, *taskShards)
	}
	var shardWorkers []worker.Worker
	if *taskShards > 1 {
		for i := 0; i < *taskShards; i++ {
			if idx >= 0 && i != idx%*taskShards {
				continue
			}
			sw := worker.New(c, workflows.TaskQueueForShard(i), worker.Options{
				MaxConcurrentActivityExecutionSize: *maxConcurrent,
			})
			sw.RegisterActivity(act.ProcessPage)
			sw.RegisterActivity(act.DiscoverSitemapURLs)
			if err := sw.Start(); err != nil {
				log.Fatalf("start shard worker %d: %v", i, err)
			}
			defer sw.Stop()
			shardWorkers = append(shardWorkers, sw)
		}
		log.Printf("host sharding: %d shard queues total, polling %d of them (shard-index=%d)", *taskShards, len(shardWorkers), idx)
	}

	log.Printf("worker started: task_queue=%s temporal=%s storage=%s task_queue_shards=%d gomaxprocs=%d max_concurrent_activities=%d max_bandwidth_bytes_per_sec=%d http3=%v save_html_dir=%q",
		workflows.TaskQueueName, *hostPort, *storageKind, *taskShards, runtime.GOMAXPROCS(0), *maxConcurrent, *maxBandwidth, *enableHTTP3, *saveHTMLDir)

	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("worker stopped: %v", err)
	}
}

// maxConcurrentDefault scales with visible CPU count so a container given
// more CPU automatically fans out more concurrent fetches without a manual
// flag per deployment size. 100x is aggressive on purpose: ProcessPage
// activities spend almost all their time blocked on network I/O, so far
// more of them can be in flight than there are cores -- the real ceiling is
// the target sites' response times and robots.txt Crawl-delay, not CPU.
// ordinalFromHostname returns the trailing integer of hostname modulo
// shards, or -1 if the hostname doesn't end in one.
func ordinalFromHostname(hostname string, shards int) int {
	i := len(hostname)
	for i > 0 && hostname[i-1] >= '0' && hostname[i-1] <= '9' {
		i--
	}
	n, err := strconv.Atoi(hostname[i:])
	if err != nil || shards < 1 {
		return -1
	}
	return n % shards
}

func maxConcurrentDefault() int {
	return runtime.NumCPU() * 100
}

// s3Backend bundles everything --storage=s3 needs. Nothing here touches a
// database: the complete HTML of each page and its parsed metadata are
// objects in the bucket, and run/freshness state are manifest/HEAD lookups
// against the same bucket.
type s3Backend struct {
	writer    storage.Writer
	runs      storage.RunRecorder
	freshness storage.FreshnessChecker
	html      storage.HTMLWriter
	records   storage.RecordWriter
}

func buildS3Backend(bucket, prefix, endpoint, region string) (*s3Backend, error) {
	if bucket == "" {
		return nil, fmt.Errorf("--s3-bucket (or S3_BUCKET) is required when --storage=s3")
	}
	var loadOpts []func(*config.LoadOptions) error
	if region != "" {
		loadOpts = append(loadOpts, config.WithRegion(region))
	}
	cfg, err := config.LoadDefaultConfig(context.Background(), loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	writerOpts := []storage.S3WriterOption{storage.WithS3KeyPrefix(prefix)}
	runOpts := []storage.S3RunRecorderOption{storage.WithS3RunRecorderKeyPrefix(prefix)}
	freshOpts := []storage.S3FreshnessOption{storage.WithS3FreshnessKeyPrefix(prefix)}
	if endpoint != "" {
		writerOpts = append(writerOpts, storage.WithS3Endpoint(endpoint))
		runOpts = append(runOpts, storage.WithS3RunRecorderEndpoint(endpoint))
		freshOpts = append(freshOpts, storage.WithS3FreshnessEndpoint(endpoint))
	}

	w, err := storage.NewS3Writer(cfg, bucket, writerOpts...)
	if err != nil {
		return nil, err
	}
	h, err := storage.NewS3HTMLWriter(cfg, bucket, writerOpts...)
	if err != nil {
		return nil, err
	}
	rw, err := storage.NewS3RecordWriter(cfg, bucket, writerOpts...)
	if err != nil {
		return nil, err
	}
	r, err := storage.NewS3RunRecorder(cfg, bucket, runOpts...)
	if err != nil {
		return nil, err
	}
	fr, err := storage.NewS3FreshnessChecker(cfg, bucket, freshOpts...)
	if err != nil {
		return nil, err
	}
	return &s3Backend{writer: w, runs: r, freshness: fr, html: h, records: rw}, nil
}

func buildWriter(kind, output string) (storage.Writer, error) {
	switch kind {
	case "ndjson":
		return storage.NewNDJSONWriter(output)
	default:
		log.Fatalf("unknown --storage %q: must be s3 or ndjson", kind)
		return nil, nil
	}
}
