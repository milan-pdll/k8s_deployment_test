# PGS Search Engine: architecture and operations

This is the reference for how the system works **as implemented**: services, data flow,
contracts between them, failure behavior, configuration, testing and what changes for a
production deployment. Each service's README covers its internals; `docker-compose.yml`
is the runnable definition of everything below.

## 1. Data flow

```text
 domains (PostgreSQL, seeded: 9.8k Nepali sites, local-government sites linked to their
   │      local body)
   ▼
 Airflow  scraper_crawl_schedule (every 30 min)
   │      one CrawlDomainsWorkflow on Temporal, fixed ID (never two crawls at once)
   ▼
 scraper-worker (Go, Temporal activities)                         chrome (headless,
   │  robots.txt (RFC 9309) -> fetch (public addresses only)  ◄──  crawl-egress network
   │  -> render JS pages -> parse -> store                         only)
   ▼
 S3 bucket crawled-pages                       Kafka scraped_files_topic
   <run>/<host>/<sha256(url)>.json  Document   one site_crawl_completed event per
   html/<host>/<content hash>.html  raw HTML   website, after all its pages are stored
   ▼                                              │
 Airflow  etl_ingestion_pipeline (every 2 min) ◄──┘ batches of ETL_BATCH_SIZE (100)
   │      events, or fewer once the oldest waited ETL_BATCH_MAX_WAIT_MINUTES (60);
   │      commits the offsets only after the batch's workflow succeeded
   ▼
 Temporal EtlBatchWorkflow  ->  etl-worker (Spark driver, LaBSE, PostgreSQL pool)
   │   per site:  marker? skip  ->  ClamAV + PostgreSQL reachable?  ->  list Documents
   ▼
 spark-worker executors, per page: read Document + HTML from S3 -> ClamAV (fail closed)
   │                               -> decode, extract main text, NFC, hashes, language
   ▼
 etl-worker: LaBSE embedding (768-d, pinned revision) -> pgs_db.etl.save_transformed
   │   PostgreSQL Bronze (crawl_runs, crawled_documents) + Silver (pages, page_embeddings,
   │   page_geo_tags from the domain's local body, dedup by content hash / SimHash)
   ▼
 search-indexer: claim UNPROCESSED canonical pages -> bulk index (_id = pages.id)
   │   into OpenSearch alias np_web_pages (-> np_web_pages_v1) -> mark PROCESSED
   ▼
 search-engine (gRPC :50051): normalize/expand query (lemma, place names, NLLB)
   │   -> BM25 on OpenSearch  +  LaBSE query vector on pgvector (page_embeddings)
   │   -> Reciprocal Rank Fusion -> LightGBM rerank -> page
   ▼
 api (FastAPI :8000) /api/v1/search, /api/v1/geo/*, /api/v1/auth/*, /api/v1/admin/*
   ▼
 ui (Next.js :3000, server-side calls to the API)  ◄──  nginx :80 (/api/v1/ -> api,
                                                           everything else -> ui)
```

PostgreSQL is the **system of record** for processed pages (Silver); OpenSearch is a
derived index the indexer can rebuild from it (`UPDATE pages SET processing_status =
'UNPROCESSED'` re-queues pages). S3 is the durable raw boundary between crawling and
processing. The scheduled `pgs_db_jobs_*` DAGs keep the Gold summaries (map counts,
domain stats, page scores) current and release claims of crashed workers.

## 2. Services

| Service | Image | Role | DB role |
| --- | --- | --- | --- |
| `postgres` | `database/Dockerfile` | PostgreSQL 16 + PostGIS + pgvector; databases `pgs`, `airflow`, `temporal`, `temporal_visibility` | owner `pgs` |
| `db-migrate` (one-shot) | `database/migrate.Dockerfile` | Alembic migrations (schema, extensions, roles, grants) + reference-data seeds | owner |
| `db-roles` (one-shot) | `database/Dockerfile` | sets role passwords; creates Airflow's and Temporal's roles/databases | owner |
| `kafka` | `apache/kafka:3.8.0` | `scraped_files_topic` | -- |
| `temporal`, `temporal-ui` | `temporalio/*` | durable workflows: crawls and ETL batches | `temporal` |
| `airflow-scheduler`, `airflow-webserver` | `ETL/Dockerfile` | scheduling only (LocalExecutor) | `airflow`; DAGs: `pgs_etl`, `pgs_jobs` |
| `clamav` | `clamav/clamav:1.4` | malware scanning daemon | -- |
| `spark-master`, `spark-worker` | `ETL/Dockerfile` | Spark standalone cluster (per-page work) | -- |
| `etl-worker` | `ETL/Dockerfile` | Temporal ETL worker = Spark driver, LaBSE, saves to PostgreSQL | `pgs_etl` |
| `opensearch` | `opensearchproject/opensearch:2.19.6` | lexical index `np_web_pages` | -- |
| `search-engine` (profile `search`) | `search-engine/Dockerfile` | gRPC SearchService | `pgs_search` |
| `search-indexer` (profile `search`) | `search-engine/Dockerfile` | Silver -> OpenSearch | `pgs_search` |
| `api` | `api/Dockerfile` | public REST API | `pgs_api` |
| `ui`, `nginx` (profile `ui`) | `ui/Dockerfile`, nginx | web UI and entry point | -- |
| `s3`, `s3-browser`, `chrome`, `scraper-worker`, `scraper-api` (profile `scraper`) | LocalStack, `scraper/Dockerfile` | crawling | -- |

Startup order is enforced with health checks and `depends_on` conditions (see the header
of `docker-compose.yml`); one-shot tasks are idempotent and re-run on every `up`.

## 3. Contracts

### 3.1 S3 layout (scraper -> ETL)

`scraper/internal/storage`: per crawl run and site, `<key_prefix>/<crawl_run_id>/<host>/
<sha256(normalized_url)>.json` holds one `model.Document` (`scraper/internal/model/
document.go`: `url`, `normalized_url` -- the dedupe identity, already the declared
canonical when a page has one --, `final_url`, `title`, `meta_description`,
`meta_keywords`, `open_graph`, `text`, `html_key`, `rendered_html_key`, `status_code`,
`content_type`, `content_hash`, `fetched_at`, ...). Raw HTML is content-addressed at
`<key_prefix>/html/<host>/<content_hash>.html` (identical bytes, identical key). The ETL
writes one marker per finished site at `<key_prefix>/_etl/<crawl_run_id>/<host>.json`.

### 3.2 Kafka event `site_crawl_completed`

Topic `scraped_files_topic`, key = the site's host (all events of a site in one partition),
value (JSON, `scraper/internal/storage/site_events.go`):

```json
{"event_type": "site_crawl_completed", "schema_version": 1, "crawl_run_id": 7,
 "workflow_id": "...", "target_domain": "ward.gov.np", "status": "completed",
 "error": "", "pages_fetched": 12, "bucket": "crawled-pages", "key_prefix": "",
 "documents_prefix": "7/ward.gov.np/", "completed_at": "2026-10-05T12:00:00Z"}
```

Published only after every page object of the site is in S3, with `acks=all`; a failed
publish fails the Temporal activity, which retries it. Delivery is **at-least-once**;
the ETL is idempotent. Consumers (`ETL/spark/site_event.py`) treat a missing
`schema_version` as 1, reject unknown versions, prefixes containing `..` or a leading
`/`, and non-positive run ids; an invalid message is logged and committed with its batch
so it cannot block the topic.

### 3.3 PostgreSQL (ETL -> Silver -> indexer)

The ETL calls `pgs_db.etl.save_transformed(record, bronze_document=<Document>)` once per
page, in its own transaction (`database/docs/bronze-silver-contract.md` has the field
mapping). Identity: a page is unique by `pages.canonical_url` (the Document's
`normalized_url`); re-crawling a URL updates the same row and bumps `version` only when
the content hash changed. Silver queues every save for the indexer
(`processing_status = UNPROCESSED`). Grants are least privilege per role
(`database/src/pgs_db/grants.py`); `database/tests/test_pipeline_contract.py` runs the
whole ETL -> indexer -> vector-search path under the real roles.

### 3.4 OpenSearch index

One index, defined only in `search-engine/src/pgs_search/indexing/mappings.py`:
`np_web_pages_v<INDEX_VERSION>` behind the alias `np_web_pages` (`OPENSEARCH_INDEX`),
`dynamic: strict`, standard analyzer without stopwords (Devanagari and Latin), fields
`document_id` (= `pages.id`), `title`, `description`, `searchable_text`, `summary`,
`source_url`, `domain`, `language`, `content_type`, `category`, `published_at`,
`keywords`, `content_length`, `geo` (primary location, codes + names), `geo_tags` (every
tag with its full code chain -- filters match any of them), `file_info`, `indexed_at`.
No vectors: they live in pgvector only. `index_manager.ensure_index` (run by the search
engine and the indexer on start) creates the index and alias, replaces an empty pre-alias
index, and refuses (never deletes) when a reindex is needed.

### 3.5 Embeddings

LaBSE (`sentence-transformers/LaBSE`, revision `836121a0533e5664b21c7aacc5d22951f2b8b25b`,
768-d, L2-normalized, cosine) for documents (ETL) and queries (search engine), from the
same `EMBEDDING_*` variables (`x-embedding-env` in compose). Both images pin the same
torch / transformers / sentence-transformers versions (`ETL/constraints.txt`,
`search-engine/constraints.txt`). Documents are embedded as the normalized mean of
256-token windows (first `EMBEDDING_MAX_CHUNKS` = 16), one vector per page.

### 3.6 gRPC (`search-engine/proto/search.proto`)

`SearchService.ExecuteSearch(SearchRequest) -> SearchResponse`; result items carry `id`
(= `pages.id`), title, url, domain, snippet, language, published_at and the primary
`geo`. Limits: query 1..512 characters, limit 1..100, `page * limit <= 500`. Errors are
status codes: `INVALID_ARGUMENT`, `UNAVAILABLE` (OpenSearch down), `DEADLINE_EXCEEDED`,
`INTERNAL`. `degraded = true` when an optional stage (dense retrieval, translation,
rerank) failed. The server also serves `grpc.health.v1` (`search.engine.v1.SearchService`).

### 3.7 REST (`api/README.md`)

`GET /api/v1/search`, `GET /api/v1/geo/hierarchy`, `GET /api/v1/geo/content-stats`,
`POST /api/v1/auth/login`, `GET /api/v1/auth/me`, `GET /api/v1/admin/summary`,
`GET /health/live`, `GET /health/ready`.

### 3.8 Traceability

```text
search result id = pages.id ──► pages.crawled_document_id ──► crawled_documents
   (minio_path = s3://bucket/html/<host>/<hash>.html, crawl_run_id, url, fetched_at)
   ──► crawl_runs ──► domains (──► local_bodies: the page's location)
pages ──► page_embeddings (model_name, content_hash), page_geo_tags (method, confidence)
```

## 4. Failure behavior

| Failure | What happens |
| --- | --- |
| ClamAV down | The site run fails before any work (`ScannerUnavailable`, PING check) or mid-run (a page "cannot scan" fails the Spark job); Temporal retries with backoff; nothing is processed unscanned. After ~1.5 h of retries the batch fails and Airflow retries it later. |
| PostgreSQL down | Same as above (`DependencyUnavailable`); the indexer backs off (up to 60 s) and retries; the API's `/health/ready` reports 503. |
| S3 down / object missing | Listing/reading errors retry the site; a single missing or corrupt object fails only that page (counted, logged to `error_logs`). |
| Kafka down | The scraper's publish activity fails and Temporal retries it; the DAG's poll task fails and runs again on the next tick. |
| Temporal down | DAG tasks fail and retry; workers reconnect; workflow state is durable. |
| Spark worker dies | Spark reschedules the task (`spark.task.maxFailures=2`), then the activity retries the site. |
| etl-worker dies | Activities heartbeat every 30 s; Temporal reschedules within 2 minutes (`heartbeat_timeout`). On SIGTERM running sites get 90 s, then are cancelled and retried. |
| Airflow restarts | Uncommitted offsets are polled again; the batch's workflow ID is its offset range, so a retry re-attaches to the running workflow instead of starting a second one. |
| Duplicate event / re-crawl | The site's marker skips a finished (run, site); otherwise Silver upserts by canonical URL. Indexing is by page id: re-indexing overwrites. |
| A site keeps failing (bad data) | After `ETL_MAX_SITE_ATTEMPTS` (5) with ClamAV/PostgreSQL/S3 healthy it is dead-lettered (`error_logs`, `error_type = SITE_DEAD_LETTERED`, with the event for replay) and the batch completes -- unless every site of the batch failed, which means a broken pipeline: then the batch fails and is kept. |
| Malware | The page is rejected, never saved; logged to `error_logs` (service `SECURITY`, `INFECTED`). |
| OpenSearch down | Search answers `UNAVAILABLE` (API 503); the indexer puts claimed pages back and backs off. |
| pgvector / LaBSE / NLLB / LightGBM failure | Search continues without that stage, `degraded = true`. |
| Chrome down | The worker keeps the static HTML (`--render=auto`). |

## 5. Configuration

All settings are environment variables, set in `docker-compose.yml` from `.env`
(template: `.env.example`, which documents each). Secrets use `${VAR:?...}` and fail
fast; non-secrets have defaults. Each service reads its settings once, typed:
`api/src/pgs_api/core/settings.py`, `search-engine/src/pgs_search/config.py`, `ETL/spark/embeddings.py`
and module constants in `ETL/spark/site_pipeline.py` / `security_scanner.py`, the Go
binaries' flags (`scraper/internal/envflag`: every flag is also an env var).

## 6. Security model

- Only nginx is published beyond 127.0.0.1. Inside the Docker network nothing is
  authenticated except PostgreSQL (one role per service, least privilege) -- local
  development only.
- The crawler connects only to public unicast addresses, checked at dial time on the
  resolved IP (DNS rebinding and redirects included), never through a proxy; responses
  are size-limited (5 MiB pages, 512 KiB robots.txt). Headless Chrome is on its own
  network (`crawl-egress`) and cannot reach internal services.
- Admin auth: `POST /api/v1/auth/login` checks `admin_users` (argon2 hashes) and returns
  an HMAC-SHA256-signed token (`API_AUTH_SECRET`); the UI keeps it in an httpOnly,
  SameSite=Lax cookie (Secure over https) and verifies it with the API on every request.
  Admin accounts: `docker compose run --rm db-migrate python scripts/create_admin.py`.
- The search engine loads its reranker from LightGBM's text format (no pickle); the
  Nepali lemmatizer is still a pickle shipped in the image (trusted build artifact).
- Containers run as non-root with `no-new-privileges` and all capabilities dropped where
  the image allows it; logs are rotated (10 MB x 3).

### Production deployment (what this compose file does not do)

Use `k8/` (single node) or adapt the compose file with: OpenSearch's security plugin and
TLS (`OPENSEARCH_SCHEME=https`, `OPENSEARCH_USERNAME/PASSWORD`), real S3 with scoped IAM
credentials (`S3_ENDPOINT=` empty; the ETL needs read on the crawl prefix and write on
`_etl/`), Kafka with SASL/TLS and a replication factor >= 3, `temporalio/server` instead
of `auto-setup`, TLS terminated in front of nginx, an `AIRFLOW_FERNET_KEY`, secrets from a
secret store, backups of PostgreSQL (the system of record) and OpenSearch snapshots (or a
re-index from Silver), and a metrics stack scraping the endpoints below.

## 7. Observability

- Health: every long-running service has a Docker healthcheck (`docker compose ps`);
  `GET /health/ready` (API) checks the schema revision, reference data, Gold freshness and
  the search engine's gRPC health.
- Metrics: the scraper exposes Prometheus metrics on `METRICS_ADDRESS` (`:9090`); the
  other services log structured lines (API: JSON with request ids; search engine: one line
  per search with stage statuses and latency; ETL: one summary per site).
- Failures needing attention land in `error_logs` (dead-lettered sites, page errors,
  malware) and show on the admin dashboard; Temporal UI (`localhost:8233`) shows every
  crawl and ETL workflow with its retries; Airflow UI (`localhost:8080`) the batches;
  Kafka lag: `kafka-consumer-groups.sh --describe --group etl-ingestion-pipeline`.

## 8. Testing

| Suite | Command |
| --- | --- |
| Go scraper | `cd scraper && go vet ./... && go test -race ./...` (+ `golangci-lint` v2.14 in CI) |
| Database (real PostgreSQL) | `cd database && DATABASE_URL=postgresql+psycopg://pgs:<pw>@localhost:5432/pgs python -m pytest` |
| API | `.venv/bin/python -m pytest api/tests` |
| Search engine | `cd search-engine && python -m pytest` (deps: `pip install -c search-engine/constraints.txt ...`) |
| ETL (in the ETL image) | `docker run --rm -v $PWD/ETL/spark:/work -w /work --entrypoint /opt/etl-venv/bin/python pgs-search-engine/etl:local -m unittest discover -p 'test_*.py'`; same for `ETL/temporal` (Temporal test server) |
| UI | `cd ui && npm run lint && npx tsc --noEmit && npm run build` |
| Lint / types | `ruff check .`, `pyright` (strict: `api/`, `database/src`) |
| Compose / k8 | `docker compose config --quiet`; `kubectl kustomize k8/ \| kubeconform -strict -kubernetes-version 1.30.0 -summary` |
| End to end | `tests/e2e/run_e2e.sh` (Temporal) or `tests/e2e/run_e2e.sh kafka` (through Kafka + Airflow) on a running stack |

## 9. Troubleshooting

- **Nothing reaches the index**: check Kafka lag (above), the `etl_ingestion_pipeline`
  runs in Airflow (a skipped run says how many events wait and for how long), the
  `EtlBatchWorkflow`s in Temporal UI, `error_logs`, and `SELECT processing_status,
  count(*) FROM pages GROUP BY 1` (UNPROCESSED rising = indexer down).
- **Search answers 503**: the `search` profile is not running or OpenSearch is down;
  `GET /health/ready` says which.
- **`IndexMigrationRequired` at search-engine start**: the alias points at another index
  version; reindex (`POST _reindex`) into `np_web_pages_v<N>` and move the alias.
- **Workers cannot reach a site**: the crawler refuses non-public addresses
  (`refusing to connect to ...`) -- expected for intranet hosts.
- **`set PGS_JOBS_DB_PASSWORD in .env`** (or another `set ... in .env`): copy the new
  variable from `.env.example`.
