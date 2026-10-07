# PGS Search Engine Core

The internal search service: a gRPC `SearchService` (`pgs_search.grpc.server`, :50051)
that the FastAPI gateway calls, and the search indexer (`pgs_search.indexing.indexer`)
that keeps OpenSearch a copy of PostgreSQL Silver. System context and contracts:
[`docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md).

## Search pipeline (`src/pgs_search/pipeline.py`)

```text
query ─► validate (1..512 chars, limit 1..100, page*limit <= 500)
      ─► normalize (NFC, whitespace, lowercase Latin), detect language (ne/en/mixed/unknown)
      ─► expand: English Porter stem / Nepali lemmatizer, EN<->NE place names,
         NLLB-200 translation of single-language queries (bounded, cached)   [query/]
      ─► BM25 on OpenSearch (title^3, description^2, keywords^2, searchable_text;
         fuzzy; geo/language/content-type filters; highlight snippet)        [required]
      ─► dense: LaBSE query vector -> pgvector page_embeddings via
         pgs_db SearchRepository.vector_search, same filters                 [optional]
      ─► Reciprocal Rank Fusion (k=60) of the two lists                      [ranking/fusion.py]
      ─► metadata for dense-only hits (one mget, no full text)
      ─► LightGBM rerank of the fused candidates                             [optional]
      ─► requested page
```

Every stage is bounded: each retriever returns at most
`min(max(page*limit, CANDIDATE_POOL), MAX_RESULT_WINDOW)` candidates (100..500). BM25
scores and cosine similarities are fused by rank, not by score. OpenSearch being down is
an error (`UNAVAILABLE`); a failure of an optional stage is logged and the response is
`degraded`. The gRPC deadline flows into the OpenSearch timeouts. Errors are gRPC status
codes (`INVALID_ARGUMENT`, `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `INTERNAL`); the server
also serves `grpc.health.v1` and refuses requests beyond `2 x SEARCH_GRPC_WORKERS` in
flight with `RESOURCE_EXHAUSTED`.

## Index (`src/pgs_search/indexing/`)

`mappings.py` is the only definition of the OpenSearch index: `np_web_pages_v1` behind
the alias `np_web_pages`, `dynamic: strict`, documents keyed by `pages.id`.
`index_manager.ensure_index` creates index + alias on start (the server and the indexer
both call it). `indexer.py` claims canonical pages the ETL saved
(`SearchRepository.claim_for_indexing`, SKIP LOCKED), bulk-indexes them and marks them
PROCESSED -- or FAILED with OpenSearch's reason -- and puts a batch back when OpenSearch
is unreachable. Vectors are not in OpenSearch: dense retrieval runs on pgvector.

## Models

| Model | Use | Pinned by |
| --- | --- | --- |
| `sentence-transformers/LaBSE` (768-d) | query vectors; must match the ETL's document vectors | `EMBEDDING_MODEL_NAME` / `EMBEDDING_MODEL_REVISION` (dimension checked at load) |
| `facebook/nllb-200-distilled-600M` | EN<->NE query translation (`TRANSLATION_ENABLED`) | `TRANSLATION_MODEL_REVISION` |
| `models/lightgbm_reranker.txt` | LightGBM LambdaRank over 9 features (`ranking/lightgbm_reranker.py`), loaded with `lightgbm.Booster` (no pickle) | `scripts/train_reranker.py` on `training_data/` (33 labelled rows: a placeholder model -- retrain on real relevance judgments before relying on it) |
| `query/nepali_lemma/nepali_hmm_pipeline.pkl` | Nepali lemmatizer (scikit-learn pipeline, pickled with scikit-learn 1.9.0) | shipped in the image |

Models download on first start into `HF_HOME` (a volume in compose).

## Configuration (`src/pgs_search/config.py`, environment only)

`OPENSEARCH_HOST/PORT/SCHEME/USERNAME/PASSWORD/INDEX`, `OPENSEARCH_TIMEOUT_SECONDS` (5),
`EMBEDDING_*`, `TRANSLATION_ENABLED` / `TRANSLATION_MODEL_*` /
`TRANSLATION_MAX_QUERY_CHARS` (200), `MAX_QUERY_CHARS` (512), `MAX_LIMIT` (100),
`MAX_RESULT_WINDOW` (500), `CANDIDATE_POOL` (100), `RERANK_ENABLED`,
`SEARCH_GRPC_HOST/PORT/WORKERS`, `INDEXER_BATCH_SIZE` (100), `INDEXER_IDLE_SECONDS`,
`INDEXER_STALE_AFTER_SECONDS` (900), `INDEXER_HEARTBEAT_FILE`; PostgreSQL through
`DATABASE_URL` (role `pgs_search`).

## Development

```bash
python3.11 -m venv .venv && . .venv/bin/activate
pip install --extra-index-url https://download.pytorch.org/whl/cpu "torch==2.14.1+cpu" \
    -c constraints.txt -c ../database/constraints.txt -e ".[dev]" -e "../database[postgres]"
python -m pytest -q                       # unit tests; the NLLB loader is mocked
PYTHONPATH=src python -m pgs_search.grpc.server
python scripts/test_grpc_client.py        # a sample query against localhost:50051
```

Regenerate the gRPC stubs after changing `proto/search.proto` (append-only field
numbers; the API image copies the stubs):

```bash
python -m grpc_tools.protoc -I proto --python_out=src/pgs_search/grpc/generated \
    --grpc_python_out=src/pgs_search/grpc/generated --pyi_out=src/pgs_search/grpc/generated \
    proto/search.proto
sed -i 's/^import search_pb2 as search__pb2/from . import search_pb2 as search__pb2/' \
    src/pgs_search/grpc/generated/search_pb2_grpc.py
```

## Known limitations

- One vector per page (mean of its first 16 windows); no passage-level retrieval yet.
- `total_hits` counts ranked candidates (at most 500), not every match in the index;
  dense retrieval always returns its nearest pages, so on a small corpus most queries
  "hit" most pages.
- The reranker is trained on a tiny placeholder set; `search_queries`/`search_clicks`/
  `relevance_judgments` in PostgreSQL are where real training data accumulates.
- Region browsing without a query (the map's "everything in district X") has no
  endpoint yet; search with a geo filter does.

---

# Original design specification (target, partly implemented)

## System Architecture & Technical Design Specification

## Bilingual Search Engine Core with Spatial Intelligence (Internal Service)

---

## 1. Executive Summary

This document details the architecture for the **Search Engine Core**, a highly scalable, bilingual search engine covering the Nepalese web ecosystem.

**Crucially, this Search Engine exposes no public APIs.** It operates entirely as an internal, backend microservice within a private virtual network. It receives queries and streams results exclusively via high-performance **gRPC** protocols to a dedicated API Gateway layer.

The core pipeline features **Geo-Spatial & Administrative Entity Intelligence**, enabling it to process both standard lexical queries and strict region-bounded administrative searches over its OpenSearch indices.

---

## 2. Core Search Engine Architecture (Internal Network)

```
┌────────────────────────────────────────────────────────────────────────┐
│                        SEARCH ENGINE INFRASTRUCTURE (Internal)         │
└────────────────────────────────────────────────────────────────────────┘

 [ Spark ETL: LangID + Geo-Tagging ] ──► [ OpenSearch Bulk Ingestion ]
                                                │
                                                ▼
┌────────────────────────────────────────────────────────────────────────┐
│ 1. MULTI-LINGUAL & GEO-INDEXED DOCUMENT STORE (OpenSearch)             │
│    - Indices: `np_web_pages`, `np_documents`, `np_entities`            │
│    - Dual-Analyzers for English/Nepali & nested geo-hierarchy filters  │
└────────────────────────────────────────────────────────────────────────┘
                                                ▲
                                                │
┌───────────────────────────────────────────────┴────────────────────────┐
│ 2. SEARCH ENGINE CORE SERVICE (Go / Python)                            │
│    - Query LangID & Devanagari Normalizer                              │
│    - Admin Code Filter Engine (`province`, `district`, `local_body`)   │
│    - Stage 1: BM25 candidate fetch (with optional hard Geo-Filtering)  │
│    - Stage 2: LightGBM Re-Ranker                                       │
│    - Exposes internal gRPC server (e.g., `SearchService`)              │
└───────────────────────────────────────────────┬────────────────────────┘
                                                │ 
                                                ▼ 
                                   [ Protobuf / gRPC Stream ] 
                                   (To External API Gateway)

```

---

## 3. Query Processing Subsystems

The Search Engine Core listens for incoming gRPC messages containing user intent, location filters, and pagination data. It processes two primary types of searches:

### 3.1 Flow A: Standard Free-Text Search

When a standard query is received via gRPC:

1. **Language Processing:** The core detects the language, applies stemming, and expands Devanagari/Romanized variants.
2. **BM25 Retrieval & LTR Ranking:** OpenSearch retrieves the top 500 matches across ALL regions. The LightGBM model ranks them by relevance, domain authority, and freshness.
3. **Serialization:** The results are packed into a Protobuf message and sent back to the API Gateway.

### 3.2 Flow B: Interactive Map & Region-Based Filtering

When a geo-filtered query is received via gRPC (e.g., bounded to `district_code=D39`):

1. **Filter Application:** The core constructs a strict Boolean OpenSearch query locking the search space to the specified administrative boundaries.
2. **Regional Ranking:** Candidates are fetched and ranked.
3. **Knowledge Assembly:** The core retrieves the specific administrative Knowledge Card (e.g., Official Portal links for Pokhara Municipality) and embeds it in the gRPC response payload.

---

---
