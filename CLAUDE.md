# CLAUDE.md

Guidance for Claude Code when working in this repository. Read [README.md](README.md) for the
product overview and [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the data flow, the
contracts between services and the failure behavior. Each service's own README is the spec
for that service.

## Repository map

| Path | What | Language / runtime |
| --- | --- | --- |
| `database/` | `pgs-db`: SQLAlchemy models, Pydantic schemas, repositories, **Alembic migrations**, grants, seed scripts and data, `pgs_db.etl` (the ETL's save path) and `pgs_db.jobs` (maintenance). **The source of truth for the PostgreSQL schema.** | Python 3.11 |
| `api/` | FastAPI gateway, package `api/src/pgs_api` (`pgs_api.main:app`; `core/`, `auth/`, `db/`, `grpc/`, `health/`, `routers/`, `schemas/`); calls the search engine over gRPC | Python 3.11 |
| `search-engine/` | gRPC `SearchService` on :50051 (BM25 on OpenSearch + LaBSE/pgvector, NLLB translation, LightGBM rerank) and the search indexer (PostgreSQL Silver -> OpenSearch), package `src/pgs_search` | Python 3.11 |
| `ETL/` | `airflow/dags/` (`scraper_crawl_schedule`, `etl_ingestion_pipeline`, `pgs_db_jobs_*`), `temporal/` (`EtlBatchWorkflow`, activities, worker), `spark/` (per-site pipeline: `site_event.py`, `site_pipeline.py`, `transform.py`, `security_scanner.py` -> ClamAV, `embeddings.py` -> LaBSE) | Python 3.11, Airflow 2.10.4, PySpark 3.5.3 |
| `scraper/` | Go crawler: `cmd/worker` (Temporal worker -> S3 / Kafka), `cmd/api` (read-only API over S3), `cmd/scraper` (starts crawls) | Go 1.26 |
| `ui/` | Next.js 16 app (App Router, `src/`); calls the API server-side (`API_INTERNAL_URL`) | Node 24 |
| `nginx/` | `default.conf`: the web entry point on port 80; `/api/v1/` and `/health/` -> `api`, the rest -> `ui` | nginx 1.27 |
| `k8/` | Kubernetes manifests (Kustomize), grouped by resource kind; single-node, non-HA; Airflow on the KubernetesExecutor. See `k8/README.md`. | |
| `docs/` | `ARCHITECTURE.md` | |
| `tests/e2e/` | `run_e2e.sh`: S3 -> ETL -> PostgreSQL -> OpenSearch -> search API on a running stack | |

## Running the stack

Everything runs from the root [`docker-compose.yml`](docker-compose.yml). There are no
per-service compose files and no second Dockerfile per service; don't add any (merge into the
existing one, using build targets if a service ships several binaries). The dev machine is WSL
(Ubuntu) + Docker Desktop, so run `docker compose` inside WSL.

```bash
cp .env.example .env
docker compose up -d --build                         # default stack (incl. the ETL)
docker compose --profile scraper up -d --build       # + crawler (S3, Chrome, worker, API)
docker compose --profile search up -d --build        # + search engine and search indexer (heavy)
docker compose exec airflow-scheduler airflow dags trigger scraper_crawl_schedule   # crawl now
docker compose run --rm db-migrate python scripts/create_admin.py <user> --email <addr>
docker compose config --quiet                        # validate after editing compose
tests/e2e/run_e2e.sh                                 # end-to-end check (needs search + s3)
```

- **Profiles:** `search`, `scraper`, `scraper-sharded` (three host-sharded workers),
  `ui`, `tools`. A service targeted by `docker compose run` gets its profile
  automatically. Nothing in a profile starts with a plain `docker compose up`: set
  `COMPOSE_PROFILES` in `.env` (e.g. `scraper,search,ui`).
- **Startup order** comes from health checks plus `depends_on` conditions:
  `postgres` -> `db-migrate` (migrations + seed) -> `db-roles` (role passwords; Airflow's and
  Temporal's roles and databases) -> app services, `temporal` (healthy once its `default`
  namespace exists) and `airflow-scheduler` (migrates Airflow's database and creates the admin
  on start) -> `airflow-webserver`; `spark-master` -> `spark-worker`; `clamav` + `temporal` +
  `spark-master` -> `etl-worker`; `api` -> `ui` -> `nginx`. There is no init container per
  service: fold setup into the service (or into `db-migrate`/`db-roles`) rather than adding
  one. Never add `sleep`-based waits. Long-running services all have a healthcheck (Chrome's
  image has no HTTP client, so it has none).
- **Volumes and network are declared once, at the end of `docker-compose.yml`.** Every volume
  is a bind-type local volume (`driver_opts: {type: none, o: bind, device: ./data/<dir>}`), so
  all state is on the host under `./data/` (git-ignored except `data/.gitkeep`, which must
  stay: the `data-root` volume needs the directory). Services mount them by name with
  `volume: {nocopy: true}`, which defers the mount to container start, after the one-shot
  `data-dirs` has created the directory and given it the user the container runs as. A new
  service that keeps state gets a `data-<name>` volume, a line in `data-dirs` and a
  `depends_on` on it. Every service is on the bridge network `pgs-network`, except
  `chrome`, which is only on `crawl-egress` (shared with the scraper workers) so pages it
  renders cannot reach internal services. `docker compose down -v` removes the volume objects
  only; delete `./data/<dir>` to reset.
- **One-shot tasks** (`data-dirs`, `db-migrate`, `db-roles`) and the setup steps services run on
  start (Airflow's migration, the search index/alias creation) re-run on every `up` and must
  stay idempotent. Kafka creates `scraped_files_topic` on the scraper's first event
  (`KAFKA_AUTO_CREATE_TOPICS_ENABLE`); consumers must not auto-create topics.
- **Ports:** host ports bind to `127.0.0.1` and are all set in `.env` (Airflow owns 8080, so the
  scraper API is on 8082). The one exception is `nginx` (profile `ui`), the web entry point:
  `HTTP_PORT` (80) on all interfaces (`HTTP_BIND_ADDRESS`). nginx listens on 80 inside its
  container too, as a non-root user (the `net.ipv4.ip_unprivileged_port_start=0` sysctl).
  Inside the network, use service names (`postgres:5432`, `kafka:29092`, `opensearch:9200`,
  `clamav:3310`, `temporal:7233`, `s3:4566`, `search-engine:50051`, `api:8000`), never
  `localhost`. Kafka's `localhost:9092` listener exists only for host-side scripts.
- **Config:** all variables live in `.env` (template: `.env.example`, which documents each).
  Secrets use `${VAR:?...}` in compose, so they fail fast when missing; nothing secret goes in a
  Dockerfile. Shared settings are YAML anchors at the top of the compose file
  (`x-embedding-env`, `x-s3-env`, `x-opensearch-client-env`, `x-etl-processing-env`).
  Container logs are rotated (`x-logging`); heavy services have memory limits
  (`*_MEMORY_LIMIT`).

## Docker conventions

- **Service folders hold code and their one Dockerfile, no Docker infrastructure:** no compose
  files, container init scripts or `docker compose` wrapper targets inside `scraper/` (or any
  service). Container setup lives in the root `docker-compose.yml` (inline files go in its
  `configs:`, e.g. LocalStack's bucket hook `s3-create-bucket`) and in `k8/`.
- **One Dockerfile per application service:** `api/`, `ETL/`, `search-engine/`, `scraper/`, `ui/`.
  `scraper/Dockerfile` has one target per binary (`worker`, `api`, `cli`; compose sets
  `build.target`). `ETL/Dockerfile` is one image for Airflow, the ETL worker and the Spark
  cluster. `search-engine/Dockerfile` serves both `search-engine` and `search-indexer`.
  `database/` has two: `Dockerfile` is the PostgreSQL server image; `migrate.Dockerfile` is the
  migration/bootstrap image (`db-migrate`: migrate + seed). The role-password SQL
  (`database/sql/set-role-passwords.sql`) is baked into the PostgreSQL image and run with `psql`
  by compose's `db-roles` and the k8s `db-bootstrap` Job.
- **Build contexts:** `api`, `ETL` and `search-engine` build from the **repository root**,
  because they need `database/` as well. The root `.dockerignore` is an allow-list; re-include
  any new top-level directory one of them must copy. `ui`, `scraper` and `database` use their own
  directory and `.dockerignore`.
- **Dependencies are pinned:** the shared package is installed into images from `./database`
  with `-c database/constraints.txt`, never copied into other service directories; the search
  engine and the ETL processing environment have full pin sets
  (`search-engine/constraints.txt`, `ETL/constraints.txt`, regeneration commands in their
  headers) that must keep torch / transformers / sentence-transformers / tokenizers /
  huggingface-hub / numpy **identical** (documents and queries are embedded by the same code).
  Heavy dependency layers come before the `pgs-db` layer so a `database/` change rebuilds fast.
- **Images run as non-root, with numeric `USER`s.** Python images use 10001, `ui` uses 1000
  (node), ETL uses 50000 (airflow), scraper uses distroless nonroot. Base images are pinned
  (no `latest`); PyTorch is always the CPU build. Lint with `hadolint`.

## Kubernetes (`k8/`)

```bash
cp k8/secrets/secrets.env.example k8/secrets/secrets.env && kubectl apply -k k8/
kubectl kustomize k8/ | kubeconform -strict -kubernetes-version 1.30.0 -summary   # validate
```

- **Grouped by resource kind, one resource per file:** `namespaces/`, `configmaps/`,
  `secrets/`, `persistentvolumeclaims/`, `serviceaccounts/`, `roles/`, `rolebindings/`,
  `services/`, `statefulsets/`, `deployments/`, `jobs/`, `networkpolicies/`. A new resource
  goes in its kind's folder as `<name>.yaml` and is listed in `k8/kustomization.yaml`.
- Same services as compose (incl. `spark-master`, `spark-worker`, `etl-worker`,
  `search-indexer`), except that Airflow and Temporal keep their own PostgreSQL StatefulSets
  (`airflow-db`, `temporal-db`) instead of sharing the application server. The compose
  profiles are commented-out blocks at the end of `resources:` in `k8/kustomization.yaml`.
  One-shot tools are Jobs in `k8/jobs/on-demand/` (`generateName`, run with
  `kubectl create -f`); `k8/jobs/` itself holds the bootstrap Jobs.
- **Airflow runs on the KubernetesExecutor**: the scheduler launches one pod per task from the
  pod template in `k8/configmaps/airflow-pod-template.yaml` (container `base`, ETL image). That
  template is a string in a ConfigMap, so Kustomize doesn't rewrite its image or Secret name:
  the Secret keeps the fixed name `pgs-secrets` (`disableNameSuffixHash`). DAGs are baked into
  the ETL image (no bind mounts in k8s).
- No `depends_on`: ordering comes from init containers. Bootstrap Jobs use
  `ttlSecondsAfterFinished`, so every `kubectl apply -k` re-runs them; they must stay
  idempotent. Image tags are set once, in `images:` of `k8/kustomization.yaml`. Kubernetes
  1.30's bundled Kustomize is v5.0: avoid YAML anchors in these manifests.
- Shared ReadWriteOnce volumes (Airflow logs, model caches) assume a single node.

## Things that are easy to get wrong

- **Schema changes go only through Alembic migrations in `database/`.** No service creates
  tables, extensions or indexes at runtime (the search engine creates only its OpenSearch
  index). The Go models in `scraper/internal/db` are generated by sqlc (`scraper/sqlc.yaml`) from
  `database/sql/scraper_schema.sql`, which `database/scripts/export_scraper_schema.py`
  generates; after a change to `crawl_runs`, `crawled_documents`, `stored_files` or `domains`:
  model + migration, re-run the export script, then `make sqlc` in `scraper/`
  (`database/tests/test_scraper_schema_sync.py` checks it). A new table also needs the
  `updated_at` trigger, an entry in `pgs_db/grants.py`, and a bump of
  `pgs_db.health.EXPECTED_REVISION`; a privilege change is a migration that calls
  `grants.apply` (see `20261007_b8c9d0e1f2a3`). `database/tests/test_pipeline_contract.py`
  runs the ETL / indexer / vector-search calls under the real roles -- extend it when a
  service starts using a new table.
- **PostgreSQL Silver is the system of record for processed pages**; OpenSearch is a copy the
  search indexer rebuilds from it. The ETL saves through `pgs_db.etl.save_transformed` (Bronze
  + Silver in one transaction per page); nothing writes OpenSearch except
  `pgs_search.indexing.indexer`. Page identity is `pages.id`, unique per canonical URL; it is
  the OpenSearch `_id` and the search result id.
- **The OpenSearch index is defined once**, in `search-engine/src/pgs_search/indexing/
  mappings.py` (`dynamic: strict`, versioned index `np_web_pages_v<N>` behind the alias
  `np_web_pages`). A mapping change that existing documents can't take bumps `INDEX_VERSION`
  and needs a reindex; `ensure_index` refuses rather than deleting data. OpenSearch holds no
  vectors (dense retrieval is pgvector), so it is not tied to a k-NN engine.
- **Embeddings:** documents (ETL) and queries (search engine) must use the same model AND
  revision (`EMBEDDING_MODEL_NAME` / `EMBEDDING_MODEL_REVISION`, LaBSE 768-d). Changing the
  model means re-embedding Silver (`page_embeddings`) and registering the model in
  `embedding_models` through a migration.
- **The ETL image has two Python environments:** Airflow 2.10 requires SQLAlchemy < 2 and
  `pgs-db` requires SQLAlchemy 2. Airflow's own environment has only what DAGs import
  (`temporalio`, `kafka-python`); `/opt/etl-venv` has the processing stack (pgs-db, PySpark,
  torch, sentence-transformers, boto3, clamd, temporalio). The ETL worker, `spark-class` and
  the Spark executors use `/opt/etl-venv` (`SPARK_HOME` / `PYSPARK_PYTHON` point into it);
  DAGs that need pgs-db run `/opt/etl-venv/bin/python -m ...` (see `pgs_db_maintenance_dag.py`).
  Don't `pip install` pgs-db or the ML stack into Airflow's environment; DAG modules must only
  import the standard library from `ETL/spark` (`site_event.py`).
- **Airflow schedules, Temporal executes, Spark computes.** `scraper_crawl_schedule` (every
  30 min) starts one `CrawlDomainsWorkflow` (fixed ID, crawls never overlap);
  `etl_ingestion_pipeline` batches site events and starts one `EtlBatchWorkflow` per batch
  (ID = the offset range), committing offsets only after it succeeded. Workflow code changes
  must stay replay-compatible with in-flight runs (or use `workflow.patched`); the batch
  outcome rules (dead-letter vs fail the batch) are tested in `ETL/temporal/test_etl_workflows.py`.
- **Scraper -> ETL hand-off is one Kafka event per crawled website**, never per page: when a
  site's crawl finishes, the worker publishes `site_crawl_completed` (`schema_version` 1,
  acks=all) to `scraped_files_topic` naming `<s3-prefix>/<crawl_run_id>/<host>/`. Field changes
  are additive; a breaking change bumps `schema_version` and the consumers
  (`ETL/spark/site_event.py`) must learn it first.
- **ClamAV fails closed:** a page that cannot be scanned fails the site's run (retried);
  never turn a scanner error into a processed page.
- **The crawler refuses non-public addresses at dial time** (`scraper/internal/netguard`);
  tests that use `httptest` must build fetchers with `WithAllowPrivateNetworks(true)`.
  robots.txt follows RFC 9309 (`Allowed` returns an error when the file is unreachable).
- **The websites to crawl are database seed data:** `database/data/domains.json` (built by
  `database/scripts/build_domains.py`), seeded into `domains` by `db-migrate`
  (`scripts/seed_domains.py`, never overriding admin edits).
- **Services connect as their own DB role** (`pgs_api`, `pgs_etl`, `pgs_search`, `pgs_jobs`),
  never as the schema owner. Only `db-migrate`/`db-roles` use `POSTGRES_USER`. URL forms:
  Python uses `postgresql+psycopg://`, Go uses `postgres://...?sslmode=disable`.
- **OpenSearch is pinned to 2.19** (the version the stack is tested with; nothing in the
  mapping prevents 3.x anymore).
- **The search engine must run from its source layout** (`PYTHONPATH=/app/src`): it locates
  `models/lightgbm_reranker.txt` relative to its files. The API image copies only the search
  engine's generated gRPC stubs; regenerate them (search-engine/README.md) after changing
  `search.proto`, with append-only field numbers.
- **UI env vars are runtime, server-side** (`API_INTERNAL_URL`, `API_TIMEOUT_MS`,
  `SESSION_COOKIE_SECURE`, validated in `src/lib/env.ts`); nothing is inlined at build time.
  `next.config.ts` uses `output: "standalone"`. Before writing UI code, read `ui/AGENTS.md`:
  Next 16 has breaking changes (`src/proxy.ts` is the middleware), and its docs are in
  `ui/node_modules/next/dist/docs/`.

## Known gaps (as of 2026-10-07)

- `pyright` (strict) still reports ~150 pre-existing errors in `database/src`; `api/` is clean.
- The ETL processes HTML pages only (no PDFs/documents), geo-tags by the site's local body
  only, and logs malware instead of quarantining it (`quarantined_files` is unused).
- The LightGBM reranker is trained on a 33-row placeholder set.

## Checks

```bash
docker compose config --quiet                       # compose syntax and interpolation
cd scraper && go vet ./... && go test -race ./...
.venv/bin/ruff check . && .venv/bin/pyright         # Python lint/type check (root pyproject.toml)
.venv/bin/python -m pytest api/tests
cd database && DATABASE_URL=postgresql+psycopg://pgs:pgs@localhost:5432/pgs python -m pytest
cd search-engine && python -m pytest                # with its dependencies installed
docker run --rm -v "$PWD/ETL/spark:/work" -w /work --entrypoint /opt/etl-venv/bin/python \
    pgs-search-engine/etl:local -m unittest discover -p 'test_*.py'
cd ui && npm run lint && npx tsc --noEmit && npm run build
```
