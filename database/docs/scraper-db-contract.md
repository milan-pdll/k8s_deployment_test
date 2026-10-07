# Bronze write contract (for the Go scraper and Spark)

> **The scraper is moving to S3-only storage** (`oxfordoli/api-s3-rework`). Nothing below is
> needed then: the `ingest` job (`pgs_db.ingest`) loads its bucket layout into these tables. This
> contract remains for any writer that talks to Postgres directly.

How a non-Python service writes the Bronze tables. The Python reference implementation is
`pgs_db.repositories.BronzeRepository`; every statement below is what it emits, and
`tests/test_bronze_repository.py` proves each one against a real PostgreSQL.

Schema version: Alembic revision `95e7b8aa6ec5`. No migration is needed to use this contract.

---

## 1. Conversions the writer must apply

These are the places where the scraper's JSON does not drop straight into a column. Getting any
of them wrong produces either a constraint violation or a silently wrong row.

| # | Scraper value | Write instead | Why |
|---|---|---|---|
| 1 | `CrawlRunID == 0` | `NULL` | `0` is not a `crawl_runs.id`; it violates the FK |
| 2 | `""` from `omitempty` (`Host`, `FinalURL`, `CanonicalURL`, `Error`, `Country`, …) | `NULL` | an empty string is not "absent" |
| 3 | `SimHash uint64` | `int64(simHash)` | Postgres has no unsigned type; bits are unchanged, so Hamming distance still works. Read back with `uint64(v)` |
| 4 | `Geo *GeoPoint` | `geo_lat`, `geo_lng` (both `NULL` when the pointer is nil) | flattened |
| 5 | `ContactInfo{Emails,Phones,Address}` | `emails`, `phones`, `address` | flattened |
| 6 | `StoredDocument.Size` | `size_bytes` | column is named differently |
| 7 | `CrawlStats.Duration` | *drop it* | no column; derive from `finished_at - started_at` |
| 8 | `FetchedAt` / `StoredAt` | RFC 3339 **with offset** | columns are `timestamptz`; a naive value is assumed UTC |

## 2. Columns that must always be sent

`NOT NULL` with **no server default** — omitting any of these fails the INSERT:

- `crawl_runs`: `status`, `started_at`, `fetched_count`, `succeeded_count`, `failed_count`,
  `skipped_count`, `unique_url_count`
- `crawled_documents`: `url`, `normalized_url`, `depth`, `content_hash`, `fetched_at`
- `stored_files`: `source_page_url`, `document_url`, `storage_path`, `sha256`, `size_bytes`,
  `stored_at`

`processing_status` defaults to `UNPROCESSED` on both document tables and may be omitted.

Status values are plain strings validated by `CHECK` constraints, never Postgres enums, so no
casts are needed. The permitted values live in `src/pgs_db/enums.py`. `crawl_runs.status` is
upper-cased by a trigger, so the Temporal scraper's `'running'` / `'completed'` / `'failed'`
are accepted as written.

The Temporal scraper (`person1/search-engine-scaffold`, `omprakash/discovery-frontier-workflow`)
sends `links` but not `internal_links` / `external_links`: that is fine, same-host `links` are
counted as the domain's discovered links. It sends no contacts and no stored files, so
municipality contacts and PDF/image search stay empty until it does.

---

## 3. Crawl lifecycle

### 3.1 Start a run

Returns the id that every document from this crawl carries as `crawl_run_id`.

```sql
INSERT INTO crawl_runs (category, temporal_workflow_id, status, started_at,
                        seed_count, max_depth, max_pages,        -- optional run config
                        fetched_count, succeeded_count, failed_count,
                        skipped_count, unique_url_count)
VALUES ($1, $2, 'RUNNING', $3, $4, $5, $6, 0, 0, 0, 0, 0)
RETURNING id;
```

These replace the scraper's former own `crawl_runs` (its migrations are gone; the Go side now
generates its models from `database/sql/scraper_schema.sql`, exported from the pgs_db models, and writes them with the statements of
this contract, `scraper/internal/db/queries.sql`): `fetched` → `fetched_count`,
`domain_capped` → `domain_capped_count`, `error` → `error`; its other columns map one to one.

### 3.2 Resolve the domain

`Document` carries `Host`, not a domain id. A miss is **not an error** — the crawler follows links
off seeded domains, and `crawled_documents.domain_id` is nullable so those pages still store.

```sql
SELECT id FROM domains WHERE domain = lower($1);
```

To auto-register unknown hosts instead (`register_unknown_domains` in the Python layer):

```sql
INSERT INTO domains (domain, category, status, priority, rate_limit_per_sec)
VALUES (lower($1), 'OTHER', 'PENDING', 'NORMAL', 1)
ON CONFLICT (domain) DO NOTHING
RETURNING id;
-- DO NOTHING returns no row when the domain already exists: re-run the SELECT above.
```

### 3.3 Save a document

Keyed on `(normalized_url, content_hash)`. Re-crawling unchanged content **updates** the row;
changed content inserts a new one, which is what makes the pair a content-version history.

```sql
INSERT INTO crawled_documents (
  crawl_run_id, domain_id, url, normalized_url, final_url, canonical_url, host, category,
  title, meta_description, meta_keywords, open_graph, text, headings, json_ld,
  emails, phones, address, social_links, links, anchor_texts,
  internal_links, external_links, image_links, video_links,
  geo_lat, geo_lng, country, sim_hash, depth, status_code, content_type,
  content_hash, fetched_at, fetch_duration_ms, error, minio_path, processing_status)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::TEXT[], $12::JSONB, $13, $14::JSONB,
        $15::TEXT[], $16::TEXT[], $17::TEXT[], $18, $19::TEXT[], $20::TEXT[], $21::TEXT[],
        $22::TEXT[], $23::TEXT[], $24::TEXT[], $25::TEXT[], $26, $27, $28, $29, $30, $31,
        $32, $33, $34, $35, $36, $37, $38)
ON CONFLICT (normalized_url, content_hash) DO UPDATE SET
  crawl_run_id = excluded.crawl_run_id,
  domain_id    = excluded.domain_id,
  url          = excluded.url,
  -- ... every column except the two conflict-target columns ...
  minio_path   = excluded.minio_path
RETURNING id, (xmax = 0) AS inserted;
```

`xmax = 0` is true only on a genuine insert, so one round trip tells you whether the page was new
or a duplicate. Array columns need the `::TEXT[]` casts; `open_graph` and `headings` need `::JSONB`.

**`anchor_texts` must be the same length as `links`** (same index = same link). The database
cannot enforce this; the ETL consumer rejects messages that violate it.

### 3.4 Save a stored file

```sql
INSERT INTO stored_files (crawled_document_id, source_page_url, document_url, storage_path,
                          content_type, sha256, size_bytes, stored_at, processing_status)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'UNPROCESSED')
ON CONFLICT (document_url, sha256) DO UPDATE SET
  crawled_document_id = excluded.crawled_document_id,
  source_page_url     = excluded.source_page_url,
  storage_path        = excluded.storage_path,
  content_type        = excluded.content_type,
  size_bytes          = excluded.size_bytes,
  stored_at           = excluded.stored_at
RETURNING id, (xmax = 0) AS inserted;
```

`StoredDocument` carries no document id. Pass the `id` returned by 3.3 for the page that linked
the file; pass `NULL` if it is not to hand.

### 3.5 Update counters, then finish

```sql
UPDATE crawl_runs SET fetched_count = $2, succeeded_count = $3, failed_count = $4,
                      skipped_count = $5, unique_url_count = $6, domain_capped_count = $7
WHERE id = $1;

UPDATE crawl_runs SET status = $2,          -- 'COMPLETED' or 'FAILED' (any case)
                      finished_at = $3,
                      error = $4             -- why the run failed; NULL when it completed
WHERE id = $1;
```

A crawl that dies must still be closed with `status = 'FAILED'`, otherwise it stays `RUNNING`
forever and the API cannot tell a live crawl from a dead one.

---

## 4. Freshness

Backs the scraper's `FreshnessChecker`. Both queries use the leading column of
`uq_crawled_documents_normalized_url_content_hash`, so no extra index is required.

```sql
-- Exact content already stored? A hit means the fetch can be skipped.
SELECT 1 FROM crawled_documents WHERE normalized_url = $1 AND content_hash = $2;

-- When was this URL last fetched?
SELECT fetched_at FROM crawled_documents
WHERE normalized_url = $1 ORDER BY fetched_at DESC LIMIT 1;
```

---

## 5. Error handling

| Situation | Behaviour |
|---|---|
| Duplicate page | `ON CONFLICT` updates; never raises. Check `inserted` to tell them apart |
| Unknown host | `domain_id` is `NULL`; the page still stores |
| Missing `crawl_runs` row | FK violation — always start the run before writing documents |
| Failed fetch | Store it: set `status_code` and `error`. `title`/`text` become `NULL` |
| Missing required field | Reject before the statement is issued, so a bad record cannot half-write |

> **Settled (2026-10-07).** `crawled_documents` accepts any `status_code`; the ETL
> (`ETL/spark/site_pipeline.py`) skips Documents of failed fetches (an `error`, or a status
> outside `200..299`) before they reach Bronze, so only fetched pages are stored.

---

## 6. Transactions

The Python layer never commits — the caller owns the transaction. Do the same in Go: group a
page's document write and its stored-file writes into one transaction so a crash cannot leave a
file row pointing at a document that was never committed.
