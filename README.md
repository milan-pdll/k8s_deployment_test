# PGS Search Engine

A real-time, geographically aware, cross-lingual (Nepali + English) search engine that indexes the Nepali web at large — not just news. It aims to crawl and index all publicly available websites of Nepal: news portals, all levels of government (federal/provincial ministries and departments down to local government and municipality sites), government corporations and institutions, universities and academic institutions, major corporations (banks, companies, cooperatives), and `.np`-domain sites generally. An NLP/geo-tagging pipeline resolves each page to a province/district/municipality where applicable, and a hybrid lexical + neural search index serves results through both a search bar and an interactive map of Nepal.

![Architecture Diagram](architecture-diagram.svg)

How it is built and operated -- data flow, contracts between the services, failure
behavior, configuration, security and testing: **[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)**.

## What it does

- **Crawls** the Nepali web — news portals, central/provincial/local government sites, government corporations and institutions, universities, banks and corporations, and other `.np`/Nepal-based websites (Go crawler on Temporal, robots.txt-compliant, JavaScript rendering with headless Chrome; ~9.8k seeded sites).
- **Processes** every crawled site on Spark: ClamAV malware scan, Devanagari/English text extraction, language detection, deduplication and LaBSE embeddings, saved to PostgreSQL.
- **Geo-tags** pages of local-government sites with their municipality/district/province (gazetteer of all 753 local bodies). Tagging pages by the places they mention is not implemented yet.
- **Indexes** content with a hybrid ranking pipeline: Okapi BM25 (OpenSearch) fused with dense multilingual embeddings (LaBSE on pgvector) by reciprocal rank fusion and reranked with LightGBM, so a query in Nepali or English finds pages in either language (single-language queries are also machine-translated for the lexical part).
- **Serves** results through a REST API (`/api/v1/search`, geo filters) and a Next.js UI with a search page, an interactive map of Nepal and an admin dashboard.

## Project layout

| Directory | Purpose |
| --- | --- |
| [`ui/`](ui/) | Next.js frontend — search bar and interactive Nepal map (Leaflet/MapLibre). |
| [`api/`](api/) | FastAPI gateway: search (over gRPC), geography, admin login and dashboard data. |
| [`database/`](database/) | `pgs-db` — a standalone, installable Python package (SQLAlchemy models + Pydantic schemas) shared across backend services. Framework-independent, not tied to FastAPI. |
| [`scraper/`](scraper/) | Go-based distributed crawler for Nepali websites (news, government, education, corporate, and other `.np`/Nepal-based sites). |
| [`ETL/`](ETL/) | Airflow DAGs, the Temporal ETL worker and the PySpark per-site pipeline (scan, extraction, embeddings, save to PostgreSQL). |
| [`search-engine/`](search-engine/) | gRPC search service (BM25 on OpenSearch + pgvector, translation, reranking) and the PostgreSQL -> OpenSearch indexer. |
| [`nginx/`](nginx/) | Reverse proxy: the stack's web entry point. |
| [`k8/`](k8/) | Kubernetes manifests (Kustomize) for the whole stack; see `k8/README.md`. |
| [`docs/`](docs/) | System architecture and operations. |
| [`tests/e2e/`](tests/e2e/) | End-to-end check of the data path on a running stack. |

## Getting started

### Run everything with Docker

The whole stack is defined in one file, [`docker-compose.yml`](docker-compose.yml), at the repo
root. You need Docker Desktop. On Windows, run these commands inside WSL, with *Settings →
Resources → WSL Integration* enabled for your distro.

```bash
cp .env.example .env          # once; set AIRFLOW_UID to `id -u`, change passwords as needed
docker compose up -d --build
docker compose ps             # wait until services are "healthy"
```

On every start, the stack migrates the database with Alembic and seeds the reference data
(`db-migrate`), then sets each service role's password and creates Airflow's and Temporal's
databases in the same PostgreSQL server (`db-roles`). Only then do the services that depend on
those start. The Kafka topic is created by its first event.

| Service | URL on the host | Notes |
| --- | --- | --- |
| API | http://localhost:8000 (docs: `/docs`) | `api/Dockerfile` |
| Airflow | http://localhost:8080 | ETL orchestration (`ETL/Dockerfile`); login from `.env` |
| PostgreSQL 16 + PostGIS + pgvector | `localhost:5432` | `database/Dockerfile`; schema owned by `pgs-db` migrations |
| OpenSearch 2.19 | http://localhost:9200 | security plugin disabled (local only); index `np_web_pages` |
| Temporal UI | http://localhost:8233 | crawl and ETL workflows |
| Spark master UI | http://localhost:8090 | the ETL's Spark cluster |
| Kafka 3.8 (KRaft) | `localhost:9092` | containers use `kafka:29092` |
| ClamAV | `localhost:3310` | malware scan for ETL intake; first start downloads signatures |

Optional parts are behind Compose profiles. Enable them with `--profile <name>`, or set
`COMPOSE_PROFILES` in `.env`:

| Profile | Adds | Notes |
| --- | --- | --- |
| `scraper` | Go crawler (`scraper/Dockerfile`): LocalStack S3 + browser (http://localhost:8081), headless Chrome, worker, documents API (http://localhost:8082/docs) | Airflow's `scraper_crawl_schedule` crawls every website in `domains` every 30 minutes; crawl now with `docker compose exec airflow-scheduler airflow dags trigger scraper_crawl_schedule` |
| `scraper-sharded` | the same, with three host-sharded workers | see `scraper/docs/SCALING.md` |
| `search` | gRPC search engine (`search-engine/Dockerfile`, :50051) + search indexer | needs ~5 GB RAM; downloads ~4.5 GB of models (NLLB, LaBSE) on first start |
| `ui` | Next.js UI (`ui/Dockerfile`) behind nginx on http://localhost (port 80, `HTTP_PORT`; nginx also routes `/api/v1/` to the API; any domain pointed at the machine works too, `nginx/default.conf`), and directly on http://localhost:3000 (`UI_PORT`) | admin login needs `API_AUTH_SECRET` and an account (`docker compose run --rm db-migrate python scripts/create_admin.py <user> --email <addr>`) |
| `tools` | OpenSearch Dashboards (http://localhost:5601) | |

All published ports bind to `127.0.0.1`. Containers reach each other by service name
(`postgres`, `kafka`, `opensearch`, `clamav`, `temporal`, `s3`, `search-engine`). `docker compose down` stops the stack;
all persistent state stays on the host under `./data/<dir>` (`postgres`, `opensearch`, `kafka`,
`clamav`, `localstack`, `etl-models`, `search-models`, `airflow-logs`; git-ignored), through the
volumes declared at the end of `docker-compose.yml`. Even `docker compose down -v` keeps those
files; delete a directory there (`sudo rm -rf data/postgres`; some are owned by the container's
user) to reset that service.

### Run on Kubernetes

[`k8/`](k8/) has Kustomize manifests for the same stack (single node, non-HA, e.g. Docker
Desktop's Kubernetes). Airflow uses the KubernetesExecutor there, so every DAG task runs in
its own pod. See [`k8/README.md`](k8/README.md):

```bash
docker compose build
cp k8/secrets/secrets.env.example k8/secrets/secrets.env
kubectl apply -k k8/
```

### Run services individually

Each service has its own setup docs in its directory. Quick summary:

- **UI**: `cd ui && npm install && API_INTERNAL_URL=http://localhost:8000 npm run dev`
- **API**: `source .venv/bin/activate && DATABASE_URL=... PYTHONPATH=api/src:search-engine/src uvicorn pgs_api.main:app --reload` (root-level `.venv`; see below)
- **Database package**: `pip install -e ./database` — installs `pgs-db`, importable from any Python project (`import pgs_db`)
- **Scraper**: `cd scraper && go run ./cmd/scraper`

### Python environment

A single virtual environment at the repo root (`.venv/`) is shared by the API and any other Python tooling. It has the `pgs-db` package installed in editable mode plus FastAPI/Uvicorn:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -c database/constraints.txt -e "./database[postgres,auth]"
pip install -r api/requirements.txt ruff pyright pytest httpx
```

### Linting, formatting & type checking

Python code is linted with Ruff everywhere (`ruff check .`); `api/` and `database/src/` are also kept strictly typed and consistently formatted using [Ruff](https://docs.astral.sh/ruff/) (linting + formatting, replacing Black/Flake8/isort) and [Pyright](https://microsoft.github.io/pyright/) (strict mode). Config lives in the root [`pyproject.toml`](pyproject.toml).

```bash
source .venv/bin/activate
pip install ruff pyright

ruff format .          # format
ruff check . --fix     # lint
pyright                # strict type check
```

## Scope of crawling

The crawl targets are not limited to news. The intent is to cover all publicly reachable Nepali websites, including:

- News portals (national and local)
- Government: federal ministries/departments, provincial governments, and local governments (municipalities/wards)
- Government corporations, institutions, and cooperatives
- Universities and other educational/academic institutions
- Major corporations: banks, companies, and other private-sector organizations
- General `.np`-domain and other Nepal-based websites not covered above

## Background

This project originates from a combined systems/DevOps + information-retrieval capstone: one team builds the bare-metal infrastructure (virtualization, distributed storage, queues, Kubernetes), and another builds the crawler, NLP, geo-spatial, and search stack on top of it — the split reflected in this repo's directory structure.
