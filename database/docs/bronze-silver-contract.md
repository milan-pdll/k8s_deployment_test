# Bronze → Silver contract (for the Spark ETL)

How the Spark ETL reads Bronze and writes Silver. The Python reference implementation
is `pgs_db.repositories.SilverRepository`; `tests/test_silver.py` and
`tests/test_etl_support.py`, `tests/test_silver_complete.py` and `tests/test_quarantine.py`
prove every rule below against a real PostgreSQL, using
payloads in the `ETL/spark/README.md` §5.2 shape.

Schema version: Alembic revision `8aa170eaaf03`.

```text
crawled_documents          stored_files
(a fetched page)           (a PDF / image / doc in MinIO)
        │                        │
        │  N:1 -- every version of a URL lands on one page; exactly one of
        │         pages.crawled_document_id / pages.stored_file_id points at
        │         the newest source
        ▼                        ▼
      pages        (Silver, one row per canonical_url)
        │
 ├──────────────┬───────────────┬──────────────┬──────────────┬───────────────┐
 ▼              ▼               ▼              ▼              ▼               ▼
page_geo_tags  page_contacts  page_sources   page_media    page_entities   page_embeddings
                              (history)      (OCR text)    → entities      (pgvector)
```

**Fastest route for the ETL:** `pgs_db.etl.process_bronze_batch(Session, transform)`
runs the whole loop below; you write only `transform(row) -> payload`. See §4.

---

## 1. Source of truth, and what is still unsettled

Every Silver field traces to one of two places:

- **`ETL/spark/README.md` §5.2** — the ETL team's own documented output payload.
- **`search-engine/src/pgs_search/models/document.py`** — `SearchDocument` /
  `GeoLocation`, the search team's real consuming code.

The exceptions are the geo-tag provenance fields (`method`, `confidence`,
`mention_text`), which §5.2 does not have yet — see §9.

**The Spark transform exists and uses this contract** (2026-10-07):
`ETL/spark/site_pipeline.py` calls `save_transformed(..., bronze_document=<the scraper's
Document from S3>)` once per page, and `tests/test_pipeline_contract.py` checks that call
with the record the pipeline builds, as the `pgs_etl` role.

## 2. What is stored, and what is not

**`body_text` is stored.** Silver keeps the page's clean full text (§5.2
`searchable_text`) in `pages.body_text`, with its `word_count`. OpenSearch is built
*from* Silver, so re-indexing, re-ranking or re-tagging never needs Spark to run again
or Bronze to be re-parsed.

**Geography names are not stored.** `geo_location.*_name_en` / `*_name_ne` /
`municipality_type` would duplicate 837 gazetteer rows onto every page and drift.
Tags store codes; `GeoLocationOut.from_tag()` rebuilds the §5.2 block by joining.

---

## 3. Payload → column mapping

`save_page(payload, crawled_document_id=..., domain_id=..., content_hash=..., sim_hash=...)`.
Keyword arguments win over the payload for `content_hash` and `sim_hash`.

### `pages`

| Payload field (§5.2) | Column | Type | Rule |
|---|---|---|---|
| `source_url` (or `canonical_url`) | `canonical_url` | `TEXT` **UNIQUE** | optional; the page's identity. Omitted → the Bronze row's `normalized_url` |
| `searchable_text` (or `body_text`) | `body_text` | `TEXT` | required, non-blank |
| `word_count` | `word_count` | `INTEGER` | optional; else whitespace token count of `body_text` |
| `language_detected` (or `language`) | `language` | `Language` | `ne`→`NE`, `en`→`EN`, `mixed`→`MIXED`, anything else or absent→`OTHER`. Region subtags are ignored (`en-US`→`EN`). |
| `extracted_metadata.title` | `title` | `TEXT` | optional |
| `extracted_metadata.description` | `description` | `TEXT` | optional |
| `extracted_metadata.keywords` | `keywords` | `TEXT[]` | optional |
| `content_type` | `content_type` | `VARCHAR(32)` | default `web_page` |
| `published_at` | `published_at` | `TIMESTAMPTZ` | optional, RFC 3339; naive = UTC |
| `content_hash` / kwarg | `content_hash` | `VARCHAR(64)` | required (ETL §3 exact dedup) |
| `sim_hash` / kwarg | `sim_hash` | `BIGINT` | optional (ETL §3 fuzzy dedup) |
| — kwarg `crawled_document_id` | `crawled_document_id` | `BIGINT` | FK `crawled_documents.id`, `ON DELETE RESTRICT`, indexed |
| — kwarg `domain_id` | `domain_id` | `BIGINT` | optional FK `domains.id` |
| — | `duplicate_of_id` | `BIGINT` | set by `mark_duplicate_of`; NULL = canonical |
| — | `processing_status` / `processing_error` | | search-indexer state; reset to `UNPROCESSED` / NULL on every save |

`document_id` is gone. The public key for a page is `pages.id` internally and
`canonical_url` externally.

### `page_geo_tags` — from `geo_location` (one object, or a list)

| Payload field | Column | Type | Rule |
|---|---|---|---|
| `province_code` | `province_code` | `VARCHAR(4)` | FK `provinces.code` |
| `district_code` | `district_code` | `VARCHAR(4)` | FK `districts.code` |
| `municipality_id` (or `local_body_code`) | `local_body_code` | `VARCHAR(16)` | FK `local_bodies.code`, e.g. `MUN414` |
| `ward_number` | `ward_number` | `INTEGER` | optional, > 0 |
| `method` | `method` | `GeoTagMethod` | **required**: `GAZETTEER`, `NER`, `DOMAIN`, `GEO_META` (case-insensitive) |
| `confidence` | `confidence` | `DOUBLE PRECISION` | **required**, 0–1 inclusive |
| `mention_text` | `mention_text` | `TEXT` | optional; the span that produced the tag |

At least one of the three geo levels must be set (`ck_page_geo_tags_at_least_one_level`).
A block with none is **skipped**, not rejected — most pages are not about a place — and
is not asked for a `method` or `confidence`. A block that *does* resolve a level but
lacks `method`/`confidence` is **rejected**: there is no default, because an invented
confidence would silently skew ranking.

Unique on `(page_id, province_code, district_code, local_body_code, ward_number)` with
`NULLS NOT DISTINCT` (PostgreSQL 15+). Note §5.2's example puts Kaski at `D39`; the
seeded gazetteer has Kaski at **`D38`** (`D39` is Lamjung).

### `page_contacts` — from `extracted_metadata`

| Payload field | `type` | `value` |
|---|---|---|
| `contact_info.emails[]` | `EMAIL` | the address |
| `contact_info.phones[]` | `PHONE` | the number |
| `contact_info.social_links[]` or `social_links[]` | `SOCIAL` | the URL |

Unique on `(page_id, type, value)`; a value repeated on one page is stored once.

### Optional page fields

| Payload field | Column | Rule |
|---|---|---|
| `language_confidence` | `language_confidence` | 0–1, from the language detector |
| `category` | `category` | free text, e.g. `notice`, `news`, `tender` (max 64) |
| `author` (or `extracted_metadata.author`) | `author` | max 255 |
| `quality_flags` | `quality_flags` | list of strings, e.g. `["thin_content"]` |

Set by the database, never sent: `first_seen_at` (first save), `last_seen_at` (latest
save), `version` (1, then +1 each time `content_hash` changes).

### `page_sources` — written automatically

Every `save_page` call records its source row (`crawled_document_id` or
`stored_file_id`) with its `fetched_at` / `stored_at` and whether the content changed.
An older row that leaves the page untouched is still recorded. `page_history(page_id)`
reads it back, oldest first.

### `page_media` — from `media` (optional)

```json
"media": [
  {"url": "https://pokharamun.gov.np/images/budget.png", "media_type": "image",
   "alt_text": "Budget table", "extracted_text": "बजेट २०८०/८१ ..."}
]
```

`media_type` is `image`, `video` or `document` (any case). `extracted_text` is OCR or
parsed text, so a scanned notice is searchable. `stored_file_id` is optional: without
it, the newest stored file whose `document_url` equals `url` is linked. Unique on
`(page_id, url)`; a URL listed twice keeps its last entry.

### `entities` / `page_entities` — from `entities` (optional)

```json
"entities": [
  {"type": "person", "name_en": "Dhanraj Acharya", "name_ne": "धनराज आचार्य",
   "mention_count": 3, "salience": 0.8}
]
```

`type` is `person`, `organization`, `event` or `other` (places are geo tags, not
entities). One `entities` row per real-world entity, shared across pages, keyed by
`key` if sent, else `TYPE:casefolded name` (`PERSON:dhanraj acharya`). A known entity
keeps its names; a missing `name_en` / `name_ne` is filled in. The same entity twice
in one payload is merged (mention counts added, highest salience kept).

### `page_embeddings` — from `embeddings` (optional), or `replace_embeddings`

```json
"embeddings": {
  "model_name": "sentence-transformers/LaBSE", "model_version": "2",
  "chunks": [{"text": "first chunk ...", "vector": [0.013, -0.07, ...]}]
}
```

- **The model must be registered** in `embedding_models`, and every vector must have
  that model's size. Registered: `sentence-transformers/LaBSE` (768, **the default**:
  multilingual, what the ETL uses), `sentence-transformers/all-MiniLM-L6-v2` (384) and
  `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2` (384). The short name
  (`all-MiniLM-L6-v2`) also works. The database enforces it too: a composite foreign
  key on (`model_name`, `dimensions`), so a Go or Spark writer can't slip in a wrong size.
- **Documents and queries must use the same model.** Search embeds the query with the
  default model (`SilverRepository.embedding_model()`); moving the default is
  `register_embedding_model(name, dims, make_default=True)`, done together with a
  re-embed.
- One row per chunk; `chunk_index` defaults to the chunk's position.
- Writing one model's chunks replaces only that model's rows for the page.
- Each row records the page's `content_hash`. `pages_missing_embeddings(model)` lists
  pages with no vectors for their **current** text, so a separate embedding job can
  run instead of embedding inside the transform:

```python
for page in repo.pages_missing_embeddings("sentence-transformers/LaBSE", limit=500):
    chunks = [{"text": c, "vector": model.encode(c).tolist()} for c in split(page.body_text)]
    repo.replace_embeddings(page.id, "sentence-transformers/LaBSE", chunks)
```

- `nearest_chunks(vector, model_name, limit)` searches by cosine distance with that
  model's HNSW index, canonical pages only.

### Records from `ETL/spark/transform.py`, as they are

The pipeline (site event → Spark → record) saves each record with its Bronze source:

```python
from pgs_db.etl import save_transformed

# record: transform.transform_document(...) + crawl_run_id, fetched_at, category,
# keywords, published_at and the LaBSE embedding (ETL/spark/site_pipeline.py)
save_transformed(Session, record, geo_confidence=0.5, bronze_document=document)
```

- The Bronze row is found by the record's `object_key` (`stored_files.storage_path`
  or `crawled_documents.minio_path`), else by `source_url`. No Bronze row → `LookupError`.
- Top-level `title`, hex `simhash`, `content_sha256`, the LaBSE `embedding` and
  `geo_location` **place names** are all accepted. Names resolve through the gazetteer
  (`ReferenceRepository.codes_for_names`), inside the district when one is given.
- `resolve_geo` states no confidence, so the caller passes `geo_confidence`: how much
  it trusts its rules. A guessed default would skew ranking.
- A PDF keeps its own URL as its identity, not the page that linked it.

### When optional blocks are replaced

`geo_location` and contacts are re-derived on **every** save (absent = none).
`media`, `entities` and `embeddings` are replaced **only when their key is in the
payload**, so an OCR, NER or embedding job that writes separately is not undone by a
payload that never mentioned them. All blocks are validated before anything is written.

---

## 4. The claim → save → mark loop

### Ready-made: `pgs_db.etl`

```python
from pgs_db import make_session_factory
from pgs_db.etl import process_bronze_batch, process_stored_file_batch

Session = make_session_factory()

def transform_page(doc):            # doc: a claimed crawled_documents row
    return {"searchable_text": clean(doc.text), "language_detected": detect(doc.text), ...}

while (result := process_bronze_batch(Session, transform_page, limit=200)).claimed:
    log(result)                     # BatchResult(claimed, saved, duplicates, failed, errors)
process_stored_file_batch(Session, transform_pdf)   # stored_files: PDFs, images
```

For each claimed row it runs `transform` then `save_page` then marks the row PROCESSED,
all in one transaction. A transform or save that raises marks only that row
FAILED with `"ExceptionType: message"`. It also applies two ETL rules:

- **Stage 4 domain rule** (`domain_geo=True`): a payload with no `geo_location`, on a
  local body's own site, is tagged with that local body (`method: DOMAIN`).
- **Stage 3 dedup** (`dedup=True`): the page is folded into an **older** page with the
  same `content_hash`, or within 3 SimHash bits; a page whose content no longer
  matches is unfolded.

`content_hash` / `sim_hash` default to the Bronze row's (a stored file's `sha256`) unless
the payload sets them. The hand-written loop below is what it does.

### By hand

Bronze's `crawled_documents.processing_status` is the ETL's work queue:

```text
UNPROCESSED ──claim_bronze──▶ PROCESSING ──mark_bronze_processed──▶ PROCESSED
     ▲                            │
     └────── release_stale ───────┤
                                  └──mark_bronze_failed──▶ FAILED (+ processing_error)
```

```python
from datetime import timedelta

from pgs_db import SilverRepository, make_session_factory

Session = make_session_factory()  # expire_on_commit=False: claimed rows stay usable


def run_batch(transform, limit: int = 100) -> None:
    """transform(crawled_document) -> §5.2 payload dict. Raises on bad input."""
    # 1. Claim. Commit at once, so the claim is durable and the row locks are released.
    with Session() as s, s.begin():
        claimed = SilverRepository(s).claim_bronze(limit)

    for doc in claimed:
        try:
            # 2. Save + mark in ONE transaction: the page and the PROCESSED flag
            #    land together or not at all.
            with Session() as s, s.begin():
                repo = SilverRepository(s)
                repo.save_page(
                    transform(doc),
                    crawled_document_id=doc.id,
                    domain_id=doc.domain_id,
                    content_hash=doc.content_hash,
                    sim_hash=doc.sim_hash,
                )
                repo.mark_bronze_processed([doc.id])
        except Exception as exc:
            # 3. Park the row with the reason. It is not retried automatically.
            with Session() as s, s.begin():
                SilverRepository(s).mark_bronze_failed(doc.id, f"{type(exc).__name__}: {exc}")


def reap() -> None:
    """Run periodically: requeue claims whose worker died. Keep the window well
    above the slowest batch."""
    with Session() as s, s.begin():
        SilverRepository(s).release_stale(timedelta(minutes=30))
```

- **`claim_bronze(limit)`** takes the oldest `UNPROCESSED` rows (by `fetched_at`) with
  `SELECT … FOR UPDATE SKIP LOCKED` and flips them to `PROCESSING`. Any number of
  workers can run it concurrently; each gets disjoint rows, and none blocks on another
  (tested with two live connections, committed and uncommitted).
- **`mark_bronze_processed(ids)`** takes a list, so a batch writer can mark many rows
  in one statement; per-row marking inside the page's transaction, as above, is the
  safer default.
- **`mark_bronze_failed(id, error)`** records the reason in
  `crawled_documents.processing_error` (not `error`, which is the scraper's fetch
  error). FAILED rows stay parked until someone resets them to `UNPROCESSED`; the
  next claim clears the old error.
- **`release_stale(older_than)`** returns `PROCESSING` rows whose claim (`updated_at`)
  is older than the window to `UNPROCESSED`. A too-short window double-processes live
  rows — harmless, since `save_page` is idempotent, but wasted work.

### Stored files (PDFs, images, docs)

Files the scraper saved to MinIO (`stored_files`) have their own queue with the same
shape, so a PDF becomes a searchable page the same way a crawled page does:

```python
with Session() as s, s.begin():
    files = SilverRepository(s).claim_stored_files(limit)   # oldest stored_at first

for f in files:
    try:
        with Session() as s, s.begin():
            repo = SilverRepository(s)
            text = extract_text(f.storage_path)              # Spark: read from MinIO
            repo.save_page(
                {"searchable_text": text, "content_hash": sha256(text), ...},
                stored_file_id=f.id,
            )
            repo.mark_stored_files_processed([f.id])
    except Exception as exc:
        with Session() as s, s.begin():
            SilverRepository(s).mark_stored_file_failed(f.id, str(exc))
```

- The page's URL falls back to `stored_files.document_url` when the payload has none.
- `pages.stored_file_id` is set and `crawled_document_id` is NULL. The database
  enforces exactly one of the two (`ck_pages_one_source`).
- `mark_stored_file_failed` records the reason in `stored_files.processing_error`
  (e.g. an encrypted or image-only PDF).
- `release_stale` recovers stale claims on **both** queues.

Non-Python writers: the claim is one statement —

```sql
UPDATE crawled_documents SET processing_status = 'PROCESSING',
       processing_error = NULL, updated_at = now()
WHERE id IN (SELECT id FROM crawled_documents
             WHERE processing_status = 'UNPROCESSED'
             ORDER BY fetched_at, id LIMIT $1
             FOR UPDATE SKIP LOCKED)
RETURNING *;
```

---

### Infected payloads → `quarantined_files`

The ETL scans each claimed payload with ClamAV before parsing it. When the scan flags
it: move the object to the quarantine bucket, then record it.

```python
# With pgs_db.etl: raise from the transform, and the loop records it.
from pgs_db.etl import Infected

def transform_pdf(stored):
    data = minio.get(stored.storage_path)
    verdict = clamav.scan(data)
    if verdict.infected:
        path = move_to_quarantine(stored.storage_path)     # s3://quarantine-lake/...
        raise Infected(verdict.signature, path, scanner_version=verdict.version)
    return {"searchable_text": extract_text(data), ...}

# By hand, inside the row's transaction:
repo.quarantine(stored_file_id=f.id, threat_signature="Win.Trojan.Generic-998",
                quarantine_path="s3://quarantine-lake/update.exe", scanner_version="ClamAV 1.4.0")
```

`quarantine(...)` (exactly one of `crawled_document_id` / `stored_file_id`):

- copies URL, linking page, raw path, `sha256`, size, content type and domain from
  the Bronze row into `quarantined_files`;
- sets the Bronze row to **`QUARANTINED`** (`processing_error = "quarantined: <signature>"`),
  a status no claim ever picks up, so it is not retried like a FAILED row;
- **deletes a page already built from that row**, so search never links to the file;
- on a rescan of the same file (`document_url` + `sha256`) updates the one record, and
  puts it back to `QUARANTINED` if an admin had deleted it.

Records survive Bronze retention: both Bronze links are `ON DELETE SET NULL`.

The API reads it through `pgs_db.QuarantineRepository`: `list_quarantined(status, domain_id,
limit, offset)` for `GET /api/v1/admin/security/quarantine`, `summary()` for the dashboard's
`quarantine_store` block (`count`, `size_mb`, `latest_threat_detected`), and
`mark_deleted(id, deleted_by=...)` after the admin erases the object (the row stays,
as the audit record, with status `DELETED`).

## 5. Identity, recrawls and reprocessing

| Level | Key | Meaning |
|---|---|---|
| Bronze | `(normalized_url, content_hash)` | one fetched version of a URL |
| Silver | `canonical_url` **UNIQUE** | one page per URL — the upsert key |
| Silver | `crawled_document_id` / `stored_file_id` | the newest Bronze source the page reflects (exactly one set; not unique) |
| Silver | `duplicate_of_id` | mirrored content folded into a canonical page |

- **Recrawl with changed content** → Bronze inserts a new row (new `content_hash`) →
  `save_page` for the same `canonical_url` updates the existing page and repoints
  `crawled_document_id` at the new row.
- **"Newest" is the highest id in the same source table.** If an older Bronze row
  (or stored file) is processed after a newer one (retry, backfill), `save_page`
  leaves the page, its tags and its contacts untouched and returns
  `SaveResult(inserted=False)` for the existing page. Mark the old row processed as
  usual. When one URL was both crawled as a page and stored as a file, whichever is
  processed last wins, and the other link is cleared.
- **Running the ETL N times over the same row** yields one page, one set of tags, one
  set of contacts. `SaveResult.inserted` / `.duplicate` says which happened
  (`RETURNING id, (xmax = 0)`).
- **Children are replaced, not merged**: a corrected geo tag replaces the wrong one.
  `replace_children=False` keeps the existing ones.
- **Every save re-queues the page for indexing** (`processing_status = UNPROCESSED`),
  so changed content reaches OpenSearch.
- **Bronze rows behind a page cannot be deleted** (`ON DELETE RESTRICT`). Bronze
  retention must repoint or delete the page first.

Exact (SHA256) and fuzzy (SimHash) dedup across *different* URLs is the ETL's
decision (stage 3). The database provides the lookups and records the outcome:

```python
repo = SilverRepository(s)
saved = repo.save_page(payload, crawled_document_id=doc.id, ...)
original = repo.find_exact_duplicate(content_hash, exclude_page_id=saved.id)
if original is None and sim_hash is not None:
    near = repo.find_near_duplicates(sim_hash, exclude_page_id=saved.id)  # [(page, bits)]
    original = near[0][0] if near else None
if original is not None:
    repo.mark_duplicate_of(saved.id, original.id)
```

- **`find_exact_duplicate(content_hash)`** returns the oldest page with that hash
  that is not itself folded into another.
- **`find_near_duplicates(sim_hash, max_distance=3)`** returns canonical pages within
  3 differing SimHash bits (3/64 ≈ the ETL spec's ">95% similar"), closest first.
  Pass the signed int64 as stored. It scans every page with a `sim_hash`, which is
  fine at thousands of pages; at millions it needs band indexing first.
- **`mark_duplicate_of(page_id, canonical_page_id)`** folds the page and rejects a
  page that claims to be a duplicate of itself.

## 6. Errors and transactions

| Situation | Behaviour |
|---|---|
| Missing `searchable_text` or `content_hash` | `ValueError` before any write |
| Missing `source_url` and no Bronze row for `crawled_document_id` | `ValueError` before any write |
| Geo tag with a level but no `method` / `confidence` | `ValueError` before any SQL; no page is written |
| Unknown `method`, or `confidence` outside 0–1 | `ValueError` before any SQL (and a CHECK behind it) |
| Unrecognised `language_detected` | stored as `OTHER`, not an error |
| A stored `language` / `method` outside the enum (raw SQL) | CHECK violation (`ck_pages_language`, `ck_page_geo_tags_geo_tag_method`) |
| Unknown `province_code` / `district_code` / `municipality_id` | FK violation, raised — the ETL should `mark_bronze_failed` |
| Geo block with nothing resolved | skipped, not an error |
| Unknown `crawled_document_id` / `stored_file_id` | FK violation, raised |
| Both or neither of `crawled_document_id` / `stored_file_id` | `ValueError` before any SQL (and `ck_pages_one_source` behind it) |
| Missing `source_url` and no stored file for `stored_file_id` | `ValueError` before any write |
| Invalid `media`, `entities` or `embeddings` block (bad type, wrong vector size, NaN, ...) | `ValueError` before any write |
| Page deleted | tags, contacts, sources, media, entity links and embeddings cascade (entities themselves stay) |

Nothing in `SilverRepository` commits. The caller owns the transaction, so one page
plus its tags, contacts and Bronze status land as a single unit of work or not at all —
tested, including that a Silver rollback leaves Bronze untouched.

## 7. Enums

Stored as `VARCHAR` + `CHECK`, like every enum in `pgs_db.enums`.

| Enum | Values |
|---|---|
| `Language` | `NE`, `EN`, `MIXED`, `OTHER` |
| `GeoTagMethod` | `GAZETTEER` (name matched in text), `NER`, `DOMAIN` (site belongs to a local body), `GEO_META` (geo meta tags / structured data) |
| `ContactType` | `EMAIL`, `PHONE`, `SOCIAL` |
| `ProcessingStatus` | `UNPROCESSED`, `PROCESSING`, `PROCESSED`, `FAILED`, `QUARANTINED` (Bronze only) |
| `QuarantineStatus` | `QUARANTINED`, `DELETED` |

---

## 8. Search indexer

Silver is the indexer's source. It polls `pages` for `processing_status = UNPROCESSED`
and reports back with `mark_processed(page_id)` or `mark_processed(page_id, error=...)`.
Key OpenSearch documents on `pages.id` (or `canonical_url`); the old `document_id` no
longer exists.

## 9. Geo-tagging helpers (stage 4)

`pgs_db.ReferenceRepository` answers the two lookups stage 4 describes. Both return
codes in the shape the `geo_location` block expects.

- **Domain rules: `geo_for_domain(domain_id)`.** For a page on a local body's own site
  (`pokharamun.gov.np`), returns
  `{"province_code": "P4", "district_code": "D38", "municipality_id": "MUN414", "method": "DOMAIN", "confidence": 1.0}`,
  ready to pass as `geo_location`. Returns None for other sites.
- **`link_domains_to_local_bodies()`** fills `domains.local_body_id` by matching each
  domain against the 753 `local_bodies.website` hosts (`www.` ignored). It only fills
  unlinked rows, so manual links survive. Run it after new domains are registered;
  until it has run, `geo_for_domain` finds nothing.
- **Gazetteer matching: `gazetteer()`.** One flat list of all 837 places
  (`level`, `code`, `name_en`, `name_ne`, and the codes of every level above), for
  Spark to broadcast and match in text. Tags built from it use `method: "GAZETTEER"`
  and a confidence the ETL chooses.

## 10. Open questions for the ETL/Spark team

Resolved on 2026-10-07: 1 (the transform exists, see §1), 7 (the Kafka consumer is gone; the
ETL saves to Silver) and 8 (the search engine embeds queries with LaBSE). Still open: 2 (the
ETL sends no content geo tags yet; pages get their domain's location), 9 (infected pages are
rejected and logged, not written to `quarantined_files`).

1. **No Spark implementation exists to verify against.** Confirm this contract when the
   real transform lands.
2. **§5.2 has no `method` / `confidence` / `mention_text` on `geo_location`.** They are
   required here for any resolved tag. §5.2 needs updating, or every tagged page fails.
3. **§5.2 has no `word_count`.** Optional here: computed by whitespace split if absent.
   Send it if Spark tokenizes Nepali differently.
4. **`content_hash` is not in the §5.2 payload.** Pass it explicitly from the Bronze row
   (as the loop above does), or add it to the payload.
5. **`published_at` and `content_type` are in `SearchDocument` but not §5.2.** Both are
   optional here.
6. **`D38` vs `D39` for Kaski, `MUN414` vs `MUN75340` for Pokhara** — §5.2's example
   disagrees with the seeded gazetteer. The seeded values are authoritative; take them
   from `gazetteer()`.
7. **The Kafka consumer writes SQLite** (`ETL/kafka/consumer.py`), not PostgreSQL, and its
   `documents` table duplicates Bronze. Nothing currently bridges it to this layer.
8. **Embedding model: LaBSE (768) is the default** (migration `f6a7b8c9d0e1`), since
   `etl/workflow_saurav` embeds with it and it handles Nepali. The search branches
   embed queries with `all-MiniLM-L6-v2` (384), which is registered too, but a LaBSE
   document and a MiniLM query are not comparable: **the search team must embed
   queries with LaBSE** (or both switch together).
9. **Where the ClamAV scan runs.** `ETL/spark/README.md` Stage 2 puts it before MinIO and
   Kafka (scraper side), so infected files would never reach the ETL. `quarantined_files`
   is written by the ETL instead, scanning each payload as it claims it. Update Stage 2
   to match, or the scraper keeps scanning too and writes the same table (its rows would
   have no Bronze link).
