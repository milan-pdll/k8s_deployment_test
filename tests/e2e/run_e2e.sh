#!/usr/bin/env bash
# End-to-end check of the data path on a running stack:
#
#   synthetic site in S3 -> site_crawl_completed event -> ETL (Temporal + Spark + ClamAV +
#   LaBSE) -> PostgreSQL Bronze/Silver -> search-indexer -> OpenSearch -> search engine
#   (BM25 + pgvector) -> FastAPI /api/v1/search
#
#   tests/e2e/run_e2e.sh            # starts EtlBatchWorkflow on Temporal directly
#   tests/e2e/run_e2e.sh kafka      # publishes the event to Kafka; Airflow's
#                                   # etl_ingestion_pipeline must pick it up (run the
#                                   # scheduler with ETL_BATCH_SIZE=1 to not wait)
#
# Needs: postgres, kafka, temporal, clamav, opensearch, spark-master/-worker, etl-worker,
# s3, search-engine, search-indexer and api running (profiles search + the s3 service).
# Uses its own crawl_run_id, so it can run on a stack with real data.
set -euo pipefail
cd "$(dirname "$0")/../.."

MODE=${1:-temporal}
RUN_ID=${E2E_RUN_ID:-$((900000000 + $(date +%s) % 100000000))}
API=${E2E_API_URL:-http://localhost:${API_PORT:-8000}}
etl_python() {
  docker compose run --rm --no-deps -T --entrypoint /opt/etl-venv/bin/python \
    -v "$PWD/tests/e2e:/e2e:ro" etl-worker "$@"
}
sql() { docker compose exec -T postgres psql -U "${POSTGRES_USER:-pgs}" -d "${POSTGRES_DB:-pgs}" -Atc "$1"; }
wait_for() {  # wait_for <description> <seconds> <command...>
  local what=$1 seconds=$2; shift 2
  for _ in $(seq 1 "$((seconds / 5))"); do
    if "$@"; then return 0; fi
    sleep 5
  done
  echo "FAIL: timed out waiting for $what" >&2
  return 1
}

echo "== seeding a synthetic site (crawl run $RUN_ID) into S3"
EVENT=$(etl_python /e2e/seed_site.py "$RUN_ID")
echo "$EVENT"

if [ "$MODE" = kafka ]; then
  echo "== publishing the site event to Kafka"
  printf 'pokharamun.gov.np|%s\n' "$EVENT" | docker compose exec -T kafka \
    /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:29092 \
    --topic scraped_files_topic --property parse.key=true --property key.separator='|'
else
  echo "== running EtlBatchWorkflow on Temporal"
  etl_python /e2e/start_batch.py "$EVENT"
fi

pages_saved() {
  [ "$(sql "SELECT count(*) FROM pages p JOIN crawled_documents d ON d.id = p.crawled_document_id
            WHERE d.crawl_run_id = $RUN_ID")" -ge 2 ]
}
wait_for "the pages in PostgreSQL Silver" 600 pages_saved
echo "== Silver:"
sql "SELECT p.id, p.language, p.canonical_url, g.local_body_code, e.dimensions
     FROM pages p JOIN crawled_documents d ON d.id = p.crawled_document_id
     LEFT JOIN page_geo_tags g ON g.page_id = p.id
     LEFT JOIN page_embeddings e ON e.page_id = p.id
     WHERE d.crawl_run_id = $RUN_ID ORDER BY p.id"

pages_indexed() {
  [ "$(sql "SELECT count(*) FROM pages p JOIN crawled_documents d ON d.id = p.crawled_document_id
            WHERE d.crawl_run_id = $RUN_ID AND p.processing_status = 'PROCESSED'")" -ge 2 ]
}
wait_for "the search indexer" 300 pages_indexed

search() { curl -fsS --get "$API/api/v1/search" --data-urlencode "q=$1" --data-urlencode "limit=10"; }
expect_url() {  # expect_url <query> <url fragment>
  search "$1" | python3 tests/e2e/check_search.py "$2"
}
echo "== search through the API"
wait_for "a lexical match" 120 expect_url "e2e-marker-zebra" "/notice/e2e-budget"
# Cross-lingual: an English query finds the Nepali page through LaBSE (dense retrieval).
wait_for "a cross-lingual match" 60 expect_url "Pokhara annual budget road maintenance drinking water" "/notice/e2e-nepali"
echo "PASS: end-to-end (crawl run $RUN_ID)"
