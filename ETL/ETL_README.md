# ETL

ETL for a Nepali/English search engine. Airflow schedules, Temporal executes,
Spark computes:

- every 30 minutes Airflow starts a crawl of every website in the `domains`
  table on Temporal (the Go scraper worker runs it);
- each website whose crawl finishes sends one Kafka event;
- once 100 events have accumulated, Airflow hands the batch to Temporal, whose
  ETL worker is the Spark driver: per site, on the Spark cluster, security
  scan (rule checks + ClamAV), text extraction, dedup, geo-tagging, LaBSE
  embedding, and output as JSONL, which is indexed into OpenSearch.

The output still goes to JSONL instead of PostgreSQL; the target design is
described in `spark/README.md`.

## Layout

| Path | What it is |
| --- | --- |
| `airflow/` | DAGs (`dags/`) and the JSONL output (`data/processed/`). |
| `temporal/` | The ETL Temporal worker (`worker.py`): `EtlBatchWorkflow` (`etl_workflows.py`) and its `run_site_pipeline` activity (`etl_activities.py`); the Spark driver. |
| `spark/` | The per-site PySpark pipeline (`site_pipeline.py`), transform/dedup/embedding (`transform.py`) and intake security checks (`security_scanner.py`, rule-based + ClamAV). |
| `kafka/` | `requirements.txt` (kafka-python, for the DAG). `consumer.py`, `test_consumer.py` and `tools/` belong to the former per-document hand-off and are no longer used. |
| `open_search/` | Indexer that bulk-loads the JSONL output into the `pgs_documents` index. |
| `Dockerfile` | The one image every ETL container runs (see below). |

## Workflow

![Crawl and ETL pipeline: Airflow, Temporal, Go scraper, Kafka, Spark](etl_pipeline.svg)

The same flow as text:

```text
Airflow DAG scraper_crawl_schedule (every 30 min, SCRAPER_CRAWL_SCHEDULE)
  -> reads domains (not PAUSED) -> Temporal CrawlDomainsWorkflow "scheduled-crawl-domains"
     (fixed ID: a tick while the last crawl still runs is skipped)
  -> Go scraper-worker: one child crawl per website, pages to S3
  -> per finished website: Kafka scraped_files_topic, one site_crawl_completed event
       {crawl_run_id, target_domain, status, bucket, key_prefix,
        documents_prefix: "<key_prefix>/<crawl_run_id>/<host>/", ...}

Airflow DAG etl_ingestion_pipeline (every 2 min, one run at a time)
  poll_batch       waits for ETL_BATCH_SIZE (100) events; flushes a smaller batch
                   once its oldest event waited ETL_BATCH_MAX_WAIT_MINUTES (60)
  process_batch    starts Temporal EtlBatchWorkflow "etl-batch-p<partition>-<from>-<to>"
                   on etl-task-queue and waits for it
                     etl-worker (Spark driver): run_site_pipeline per site,
                     ETL_SITE_CONCURRENCY (4) at a time, retried per site
                       Spark executors (spark-worker x2): per page, read the HTML
                       from S3, rule checks + ClamAV, extract text
                       driver: dedup, LaBSE embedding, append JSONL
  commit_offsets   only after the whole batch succeeded
  -> airflow/data/processed/transformed_documents.jsonl
  -> opensearch-indexer -> OpenSearch index pgs_documents
```

A page that fails the intake checks is left out (and logged). If ClamAV, S3 or
Spark is unavailable, the site's activity is retried by Temporal; if it still
fails, the batch fails and nothing is committed, so the next DAG run starts the
same batch again. Sites of that batch that already reached the output are
skipped then (`run_site_pipeline` checks the output for that run and site).

Other DAGs: `airflow_healthcheck` (scheduler smoke test) and
`document_extraction_check` (parses `data/sample.html` / `data/sample.txt`).

## Run it

Everything runs from the repository-root `docker-compose.yml`, from the
repository root:

```bash
cp .env.example .env                                  # once; set AIRFLOW_UID to `id -u`
docker compose --profile scraper up -d --build        # Airflow, Temporal, Spark, Kafka, ClamAV, crawler, ...
docker compose exec airflow-scheduler airflow dags trigger scraper_crawl_schedule   # crawl now
# Airflow UI http://localhost:8080 (login from .env); Temporal UI http://localhost:8233;
# Spark master UI http://localhost:8090
docker compose run --rm opensearch-indexer            # index the JSONL output
```

| Container | What it does |
| --- | --- |
| `airflow-webserver`, `airflow-scheduler` | Airflow 2.10 with the LocalExecutor (no Celery, no Redis): the scheduler migrates Airflow's database on start and runs the DAGs' lightweight tasks itself |
| `temporal`, `temporal-ui` | Workflow engine for the crawl and the ETL batches; its databases are in the shared `postgres` server (created by `db-roles`) |
| `etl-worker` | Temporal worker for `EtlBatchWorkflow`; the Spark driver (one long-lived Spark application); dedup, embedding and JSONL output. One replica |
| `spark-master`, `spark-worker` | Spark 3.5 standalone cluster on the ETL image; `SPARK_WORKERS` (2) workers |
| `clamav` | ClamAV daemon the executors stream every page to (`CLAMD_HOST=clamav`); the first start downloads signatures (a few minutes) |
| `kafka` | Broker: `kafka:29092` from containers, `localhost:9092` from the host |
| `opensearch` | OpenSearch 2.19 (`http://localhost:9200`); 2.x because the index mapping uses the `nmslib` k-NN engine, which 3.x refuses for new indexes |
| `opensearch-indexer` | One-shot tool (`docker compose run --rm opensearch-indexer`) |

All of them run one image, `ETL/Dockerfile`: Airflow 2.10 + JDK + PySpark 3.5.3,
plus kafka-python, temporalio, pypdf, sentence-transformers (CPU-only PyTorch)
and clamd. `airflow/{dags,plugins,config,data}` and `spark/` are
bind-mounted into the Airflow containers, and `temporal/`, `spark/` and
`airflow/data` into `etl-worker`, so edits apply without a rebuild: DAGs within
a minute, the pipeline and the worker after `docker compose restart etl-worker`
(its long-lived Spark application ships `spark/*.py` to the executors when it
starts). The LaBSE model (~1.8 GB) downloads on the first embedding into
`./data/etl-models` (repository root).

The shared `pgs-db` package needs SQLAlchemy 2, which Airflow 2.10 cannot load,
so it lives in a separate interpreter, `/opt/etl-venv/bin/python` (with the Kafka
and OpenSearch clients). The indexer runs with it; an Airflow task can use it via
`@task.external_python(python="/opt/etl-venv/bin/python")`. ETL containers get
`DATABASE_URL` for the `pgs_etl` role.

Tests: `docker compose run --rm --no-deps --entrypoint bash -w /opt/airflow/spark_lib
etl-worker -c "python -m unittest test_site_pipeline"`.

## Known gaps

- **JSONL output:** the pipeline writes JSONL instead of PostgreSQL. The
  database write path is `pgs_db.etl.save_transformed(...)` as `pgs_etl`
  (see `database/README.md` §8).
- **One ETL worker:** the JSONL output and dedup live in the `etl-worker`
  process, so it runs as one replica (Spark scales the per-page work). Moving
  the output to PostgreSQL removes that limit.
