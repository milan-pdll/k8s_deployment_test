# Scraper (Go)

The crawler of the PGS Search Engine: whole-website crawls of the seeded Nepali sites,
run as Temporal workflows, with pages written to S3 and one Kafka event per finished
website for the ETL. System context: [`../docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md).

| Binary | What |
| --- | --- |
| `cmd/worker` | Temporal worker (task queue `scraper-task-queue`, optionally sharded by host): `CrawlDomainsWorkflow` (all sites, `MaxConcurrentDomains` at a time) -> `CrawlWorkflow` per site (robots.txt, sitemaps, fetch, render, parse, store, dedup) -> site event |
| `cmd/api` | read-only documents API + Swagger UI over the bucket (`/docs`, `/healthz`) |
| `cmd/scraper` | starts a crawl from the command line (Airflow's `scraper_crawl_schedule` normally does) |

## Output contract

- **S3** (`internal/storage`, details in [`docs/SCHEMA.md`](docs/SCHEMA.md)):
  `<prefix>/<crawl_run_id>/<host>/<sha256(normalized_url)>.json` = one `model.Document`;
  raw HTML at `<prefix>/html/<host>/<content_hash>.html` (rendered captures too).
- **Kafka** `scraped_files_topic`, key = host, one `site_crawl_completed` event per website,
  published with `acks=all` only after every page of the site is stored
  ([`internal/storage/site_events.go`](internal/storage/site_events.go)):
  `event_type`, `schema_version` (1), `crawl_run_id`, `workflow_id`, `target_domain`,
  `status` (`completed` | `failed`), `error`, `pages_fetched`, `bucket`, `key_prefix`,
  `documents_prefix`, `completed_at`. Add fields freely; remove or retype one only with a
  new `schema_version`, after the ETL (`ETL/spark/site_event.py`) understands it.
- It never writes PostgreSQL in this deployment (the ETL saves each page's Document to
  Bronze); `internal/db` (sqlc) and `--storage` alternatives remain for other setups.

## Crawling rules and safety

- **robots.txt (RFC 9309)** (`internal/robots`): per-origin cache (24 h), longest match
  wins, `*`/`$` patterns, product-token group matching; 4xx = no restrictions; 5xx, 429 or a
  network error = the origin is disallowed for now and the activity retries; Crawl-delay is
  honored (capped at 10 s) by spacing the fetcher's requests to the host.
- **Public addresses only** (`internal/netguard`): every connection -- pages, robots.txt,
  sitemaps, redirects -- is checked at dial time on the resolved IP (loopback, RFC 1918,
  link-local/cloud metadata, CGNAT, multicast, reserved, NAT64/6to4-embedded private
  addresses are refused), so DNS rebinding cannot get around it; no HTTP proxy is used.
  Headless Chrome runs on its own Docker network (`crawl-egress`) for the same reason.
- **Bounded fetches** (`internal/fetcher`): http/https only, at most 10 redirects (each
  re-checked), 5 MiB body (decompressed), 1 MiB headers, connect/TLS/header/total timeouts,
  per-host politeness and rate limits, optional bandwidth cap. Permanent failures (refused
  address, unknown host, bad certificate, redirect loop) are recorded once, transient ones
  retried by Temporal.

## Configuration

Every flag of `cmd/worker` is also an environment variable of the same name in upper case
(`internal/envflag`), e.g. `TEMPORAL_ADDRESS`, `STORAGE` (`s3`), `S3_BUCKET`, `S3_ENDPOINT`,
`KAFKA_BROKERS`, `KAFKA_TOPIC`, `RENDER` (`off` | `auto` | `always`), `CHROME_URL`,
`TASK_QUEUE_SHARDS` / `SHARD_INDEX`, `MAX_CONCURRENT_ACTIVITIES`, `METRICS_ADDRESS`
(`:9090`, Prometheus metrics, [`docs/METRICS.md`](docs/METRICS.md)), `USER_AGENT`.
Docker compose sets them for the `scraper-worker*` services.

## Development

```bash
go build ./... && go vet ./... && go test -race ./...
gofmt -l .                                   # must print nothing
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0 run --config=.golangci.yml ./...
make sqlc                                    # after database/sql/scraper_schema.sql changed
```

Tests that run `httptest` servers build their fetcher with
`fetcher.WithAllowPrivateNetworks(true)` (127.0.0.1 is refused by default). More:
[`docs/GETTING_STARTED.md`](docs/GETTING_STARTED.md), [`docs/TESTING.md`](docs/TESTING.md),
[`docs/WHOLE_DOMAIN.md`](docs/WHOLE_DOMAIN.md), [`docs/SCALING.md`](docs/SCALING.md),
[`docs/RESILIENCE.md`](docs/RESILIENCE.md).
