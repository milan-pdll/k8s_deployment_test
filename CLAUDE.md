# CLAUDE.md

Guidance for Claude Code when working in this repository. Read [README.md](README.md) for the
product overview. Each service's own README is the spec for that service.

## Repository map

| Path | What | Language / runtime |
| --- | --- | --- |
| `database/` | `pgs-db`: SQLAlchemy models, Pydantic schemas, repositories, **Alembic migrations**, seed scripts and data. **The source of truth for the PostgreSQL schema.** | Python 3.11 |
| `api/` | FastAPI gateway (`api.main:app`); calls the search engine over gRPC (`api/grpc_client.py`) | Python 3.11 |
| `search-engine/` | gRPC `SearchService` on :50051 (BM25 on OpenSearch, pgvector, NLLB translation, LightGBM rerank) | Python 3.11 |
| `ETL/` | `airflow/` (DAGs; main one `etl_ingestion_pipeline`), `spark/` (`transform.py`, `security_scanner.py` → ClamAV), `kafka/` (`consumer.py`, `tools/`), `open_search/` (JSONL → index) | Python 3.11, Airflow 2.10.4, PySpark 3.5.3 |
| `scraper/` | Go crawler: `cmd/worker` (Temporal worker → S3 / Kafka), `cmd/api` (read-only API over S3), `cmd/scraper` (starts crawls) | Go 1.26 |
| `ui/` | Next.js 16 app (App Router, `src/`) | Node 24 |
| `nginx/` | `default.conf` for the `nginx` service: reverse proxy on port 80 in front of `ui` | nginx 1.27 |
| `k8/` | Kubernetes manifests (Kustomize), grouped by resource kind; single-node, non-HA; Airflow on the KubernetesExecutor. See `k8/README.md`. | |

## Running the stack

Everything runs from the root [`docker-compose.yml`](docker-compose.yml). There are no
per-service compose files and no second Dockerfile per service; don't add any (merge into the
existing one, using build targets if a service ships several binaries). The dev machine is WSL
(Ubuntu) + Docker Desktop, so run `docker compose` inside WSL.

```bash
cp .env.example .env
docker compose up -d --build                         # default stack
docker compose --profile scraper up -d --build       # + crawler (S3, Chrome, worker, API)
docker compose --profile search up -d                # + search engine (heavy)
docker compose exec airflow-scheduler airflow dags trigger scraper_crawl_schedule   # crawl now
docker compose run --rm opensearch-indexer           # index the ETL JSONL output
docker compose run --rm db-migrate python scripts/create_admin.py <user> --email <addr>
docker compose config --quiet                        # validate after editing compose
```

- **Profiles:** `search`, `scraper`, `scraper-sharded` (three host-sharded workers),
  `ui`, `tools`. A service targeted by `docker compose run` gets its profile
  automatically. Nothing in a profile starts with a plain `docker compose up`: set
  `COMPOSE_PROFILES` in `.env` (e.g. `scraper,ui`) to include the crawler, S3 + its browser UI
  (`localhost:8081`), the UI and nginx (`localhost`).
- **Startup order** comes from health checks plus `depends_on` conditions:
  `postgres` → `db-migrate` (migrations + seed) → `db-roles` (role passwords; Airflow's and
  Temporal's roles and databases) → app services, `temporal` (healthy once its `default`
  namespace exists) and `airflow-scheduler` (migrates Airflow's database and creates the admin
  on start) → `airflow-webserver`; `spark-master` → `spark-worker`, `etl-worker`. There is no
  init container per service: fold setup into the service (or into `db-migrate`/`db-roles`)
  rather than adding one. Never add `sleep`-based waits.
- **Volumes and network are declared once, at the end of `docker-compose.yml`.** Every volume
  is a bind-type local volume (`driver_opts: {type: none, o: bind, device: ./data/<dir>}`), so
  all state is on the host under `./data/` (git-ignored except `data/.gitkeep`, which must
  stay: the `data-root` volume needs the directory). Services mount them by name with
  `volume: {nocopy: true}`, which defers the mount to container start, after the one-shot
  `data-dirs` has created the directory and given it the user the container runs as. A new
  service that keeps state gets a `data-<name>` volume, a line in `data-dirs` and a
  `depends_on` on it. Every service is attached to the one bridge network, `pgs-network`.
  `docker compose down -v` removes the volume objects only; delete `./data/<dir>` to reset.
- **One-shot tasks** (`data-dirs`, `db-migrate`, `db-roles`) and the setup steps services run on start
  (Airflow's migration, the search engine's index creation) re-run on every `up` and must stay
  idempotent. Kafka creates `scraped_files_topic` on the scraper's first event
  (`KAFKA_AUTO_CREATE_TOPICS_ENABLE`); consumers must not auto-create topics.
- **Ports:** host ports bind to `127.0.0.1` and are all set in `.env` (Airflow owns 8080, so the
  scraper API is on 8082). The one exception is `nginx` (profile `ui`), the web entry point:
  `HTTP_PORT` (80) on all interfaces (`HTTP_BIND_ADDRESS`), so a domain can point at it. nginx
  listens on 80 inside its container too, as a non-root user (the container's
  `net.ipv4.ip_unprivileged_port_start=0` sysctl, not root or a capability). The UI is also
  published directly on `127.0.0.1:3000` (`UI_PORT`). Inside the network, use service names (`postgres:5432`,
  `kafka:29092`, `opensearch:9200`, `clamav:3310`, `temporal:7233`, `s3:4566`), never
  `localhost`. Kafka's `localhost:9092` listener exists only for host-side scripts.
- **Config:** all variables live in `.env` (template: `.env.example`). Secrets use
  `${VAR:?...}` in compose, so they fail fast when missing; nothing secret goes in a Dockerfile.

## Docker conventions

- **Service folders hold code and their one Dockerfile, no Docker infrastructure:** no compose
  files, container init scripts or `docker compose` wrapper targets inside `scraper/` (or any
  service). Container setup lives in the root `docker-compose.yml` (inline files go in its
  `configs:`, e.g. LocalStack's bucket hook `s3-create-bucket`) and in `k8/`.
- **One Dockerfile per application service:** `api/`, `ETL/`, `search-engine/`, `scraper/`, `ui/`.
  `scraper/Dockerfile` has one target per binary (`worker`, `api`, `cli`; compose sets
  `build.target`). `ETL/Dockerfile` is one image for Airflow, the consumer, the indexer and the
  Spark check. `database/` has two: `Dockerfile` is the PostgreSQL server image;
  `migrate.Dockerfile` is the migration/bootstrap image (`db-migrate`: migrate + seed). The
  role-password SQL (`database/sql/set-role-passwords.sql`) is baked into the PostgreSQL image
  and run with `psql` by compose's `db-roles` and the k8s `db-bootstrap` Job.
- **Build contexts:** `api`, `ETL` and `search-engine` build from the **repository root**,
  because they need `database/` as well. The root `.dockerignore` is an allow-list; re-include
  any new top-level directory one of them must copy. `ui`, `scraper` and `database` use their own
  directory and `.dockerignore`.
- **The shared package** is installed into images from `./database` with
  `-c database/constraints.txt`, never copied into other service directories. After changing
  `database/pyproject.toml` dependencies, regenerate `constraints.txt` (command in its header).
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
  `services/`, `statefulsets/`, `deployments/`, `jobs/`, `cronjobs/`. A new resource goes in its
  kind's folder as `<name>.yaml` and is listed in `k8/kustomization.yaml`.
- Same services as compose. The compose profiles are commented-out blocks at the end of
  `resources:` in `k8/kustomization.yaml` (uncomment to enable; Kustomize components can't
  reference files outside their own folder). One-shot tools are Jobs in `k8/jobs/on-demand/`
  (`generateName`, run with `kubectl create -f`); `k8/jobs/` itself holds the bootstrap Jobs.
- **Airflow runs on the KubernetesExecutor**: no Celery worker or Redis; the scheduler launches
  one pod per task from the pod template in `k8/configmaps/airflow-pod-template.yaml` (container `base`, ETL
  image). That template is a string in a ConfigMap, so Kustomize doesn't rewrite its image or
  Secret name: the Secret keeps the fixed name `pgs-secrets` (`disableNameSuffixHash`).
  DAGs are baked into the ETL image (no bind mounts in k8s).
- No `depends_on`: ordering comes from init containers (wait until the service can log in as its
  DB role and sees the seeded data; `airflow db check-migrations`; Temporal namespace / S3 bucket /
  Kafka topic checks). Bootstrap Jobs use `ttlSecondsAfterFinished`, so every
  `kubectl apply -k` re-runs them; they must stay idempotent.
- Image tags are set once, in `images:` of `k8/kustomization.yaml`. Kubernetes 1.30's bundled
  Kustomize is v5.0: avoid YAML anchors in these manifests.
- Shared ReadWriteOnce volumes (Airflow logs, ETL output, model cache) assume a single node.

## Things that are easy to get wrong

- **Schema changes go only through Alembic migrations in `database/`.** The Go scraper and the
  search engine never create tables or extensions, and the scraper has no migrations at all.
  The Go models in `scraper/internal/db` are generated by sqlc (`scraper/sqlc.yaml`) from
  `database/sql/scraper_schema.sql`, which `database/scripts/export_scraper_schema.py`
  generates from the pgs_db models of the tables the scraper uses (`crawl_runs`,
  `crawled_documents`, `stored_files`, `domains`); it is never executed. After a change to one
  of those tables: model + migration, re-run the export script, then `make sqlc` in
  `scraper/`. `database/tests/test_scraper_schema_sync.py` fails while the file is stale or the
  models disagree with the migrated database. Go writes follow
  `database/docs/scraper-db-contract.md`. A new table also needs the `updated_at`
  trigger, an entry in `pgs_db/grants.py`, and a bump of `pgs_db.health.EXPECTED_REVISION` (see
  `database/README.md` §7). Extensions (`vector`, `postgis`) are created by the migrations, not
  by the Postgres image. Airflow and Temporal keep their own databases (`airflow`,
  `temporal`, `temporal_visibility`) and roles in the same server, created by `db-roles`.
- **The websites to crawl are database seed data:** `database/data/domains.json` (built by
  `database/scripts/build_domains.py` from `scraper/configs/seeds.example.txt` plus every local
  body's website, each linked to its local body = its geolocation), seeded into `domains` by
  `db-migrate` (`scripts/seed_domains.py`, never overriding admin edits). The scraper does not
  seed anything; Airflow's `scraper_crawl_schedule` crawls what is in `domains`.
- **Services connect as their own DB role** (`pgs_api`, `pgs_etl`, `pgs_search`), never as the
  schema owner. Only `db-migrate`/`db-roles` use `POSTGRES_USER`. URL forms: Python uses
  `postgresql+psycopg://`, Go uses `postgres://...?sslmode=disable`.
- **Airflow 2.10 requires SQLAlchemy < 2; `pgs-db` requires SQLAlchemy 2**, so they can never
  share an environment. In the ETL image, `pgs_db` (plus the Kafka and OpenSearch clients) lives
  in `/opt/etl-venv`. Run DB-writing ETL code with `/opt/etl-venv/bin/python` (as
  `opensearch-indexer` does), or from a DAG via
  `@task.external_python(python="/opt/etl-venv/bin/python")`. Don't `pip install` pgs-db into
  Airflow's own environment. Packages DAG tasks import go into Airflow's environment in
  `ETL/Dockerfile`, with `apache-airflow==${AIRFLOW_VERSION}` in the same `pip install`.
- **Airflow schedules, Temporal executes, Spark computes** (compose): Airflow runs on the
  LocalExecutor (no Celery, no Redis) and stores its metadata in the shared `postgres` server
  (database and role `airflow`, created by `db-roles`; there is no `airflow-db`).
  `scraper_crawl_schedule` (every 30 min, `SCRAPER_CRAWL_SCHEDULE`) reads `domains` and starts
  one `CrawlDomainsWorkflow` on Temporal (fixed ID, so crawls never overlap);
  `etl_ingestion_pipeline` waits for `ETL_BATCH_SIZE` (10) site events, then starts one
  `EtlBatchWorkflow` on the `etl-task-queue`. `etl-worker` (`ETL/temporal/`) runs it as the
  Spark driver against the standalone cluster (`spark-master`, `spark-worker`, all on the ETL
  image, because executors need the driver's Python packages). Services that run the ETL image
  as `AIRFLOW_UID` go through the image's `/entrypoint`, which gives that UID a passwd entry and
  home (Airflow and Spark both fail without one). k8s still runs the old shape
  (KubernetesExecutor, no Spark cluster or ETL worker).
- **Scraper -> ETL hand-off is one Kafka event per crawled website**, never per page or file:
  when a site's crawl finishes, the worker publishes a `site_crawl_completed` event to
  `scraped_files_topic` (`scraper/internal/storage/site_events.go`) naming the site's documents
  prefix `<s3-prefix>/<crawl_run_id>/<host>/`. The `etl_ingestion_pipeline` DAG runs the whole
  PySpark pipeline (`ETL/spark/site_pipeline.py`: ClamAV scan, extraction, dedup, embedding,
  output) once per site, and commits the Kafka offsets only after the whole batch succeeded.
- **DAGs hardcode** `kafka:29092`, `/opt/airflow/spark_lib` and `/opt/airflow/data/...`. The
  compose bind mounts keep those paths; don't rename them. The intake scanner reaches ClamAV via
  `CLAMD_HOST`/`CLAMD_PORT` and fails closed (an unreachable ClamAV fails the site's run, which
  is retried), so `clamav` must be running for the DAG.
- **OpenSearch is pinned to 2.19:** the ETL index (`ETL/open_search/opensearch/create_index.py`)
  uses the `nmslib` k-NN engine, which OpenSearch 3.x refuses for new indexes. Moving to 3.x
  means switching that mapping to `faiss` or `lucene` first.
- **The search engine must run from its source layout** (`PYTHONPATH=/app/src`). It locates
  `models/` and `vector_search/` relative to its files, so don't pip-install it as a package.
- **UI env vars:** `NEXT_PUBLIC_*` values are inlined at **build** time (a build arg in compose),
  and the browser hits the API via the host URL. `next.config.ts` uses `output: "standalone"`
  for the image. Before writing UI code, read `ui/AGENTS.md`: Next 16 has breaking changes, and
  its docs are in `ui/node_modules/next/dist/docs/`.

## Known breakages (as of 2026-10-03; not Docker issues)

- `ui/` keeps stray `* copy.*` files (`package copy.json`, `next.config copy.ts`, ...). The UI's
  `src/lib/` was missing from this branch (the root `.gitignore` rule `lib/` hid it; there is now
  a `!ui/src/lib/` exception) and was restored, with the dependencies it needs, from
  `origin/ishantbranch`. `origin/ui-new-map` has a newer map UI (and `lib/geo.ts`) not merged here.
- Root `requirements.txt` lists `clamav==1.0.2`; the scanner imports `clamd` (`clamd==1.0.2`).

## Checks

```bash
docker compose config --quiet                      # compose syntax and interpolation
cd scraper && go vet ./... && go test ./...
cd ETL/spark && python -m unittest test_site_pipeline   # in the ETL image
cd database && DATABASE_URL=postgresql+psycopg://pgs:pgs@localhost:5432/pgs python -m pytest
ruff check . && pyright                            # Python lint/type check (root pyproject.toml)
```
