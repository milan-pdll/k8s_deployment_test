> **Status: target design, not the implementation.** What runs today is described in
> [`../ETL_README.md`](../ETL_README.md) and [`../../docs/ARCHITECTURE.md`](../../docs/ARCHITECTURE.md).
> Differences from this document: there are no Redis Bloom filters (URL dedup is the
> crawler's own, content dedup happens when Silver saves a page); raw pages are in S3
> (LocalStack locally), not MinIO; Kafka carries one `site_crawl_completed` event per
> website, not per file; ClamAV runs in the Spark executors, and nothing is moved to a
> quarantine bucket; Spark's output goes to PostgreSQL Silver through `pgs_db`, and a
> separate search indexer -- not Spark -- writes OpenSearch; geo-tagging uses the
> website's local body only.

# System Architecture & Technical Design Specification

## Distributed Web Scraping, Filtration, Security, Geo-Tagging & Spark/Kafka ETL Pipeline

---

## 1. Executive Summary

This specification details a fault-tolerant, scalable, and secure data pipeline designed to discover, fetch, filter, process, and geographically index raw web content and documents of Nepali websites domain.

The architecture enforces strict separation of concerns across 5 core stages, driven by a three-tiered orchestration model:

1. **Target Discovery & Crawl Orchestration (Temporal + Go Worker Fleet)**
2. **Pre-Fetch & Post-Fetch Filtration (Redis Bloom Filters + ClamAV Malware Isolation)**
3. **Decoupled Raw Storage & Event Transport (MinIO + Apache Kafka)**
4. **Distributed Computation, Deduplication & Geo-Tagging (Apache Spark Cluster)**
5. **Batch ETL Maintenance & Indexing Orchestration (Apache Airflow)**

---

## 2. High-Level System Architecture

```text
 [ Seed Discovery & Initial Admin Mapping ]
                     │
                     ▼
┌────────────────────────────────────────────────────────────────────────┐
│ 1. CRAWL ORCHESTRATION LAYER (Temporal + Go Worker Fleet)              │
│    - Dynamic Recursive Traversal & Domain Boundary Locking             │
│    - Domain Rate Throttling & Robots.txt Evaluation                    │
└────────────────────┬───────────────────────────────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────────────────────────┐
│ 2. PRE-FETCH FILTRATION LAYER (Redis Bloom Filter)                     │
│    - Fast Check: Has URL been crawled before?                          │
│    ├─► YES ──► [ DISCARD LINK ]                                        │
│    └─► NO  ──► [ PROCEED TO SCRAPE ]                                   │
└────────────────────┬───────────────────────────────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────────────────────────┐
│ 3. SECURITY & MALWARE SCANNER (ClamAV Stream Inspector)                │
│    - Inspect Downloaded PDFs, DOCX, Binaries & HTML                    │
│    ├─► INFECTED ──► [ ISOLATE TO QUARANTINE BUCKET & LOG ALERT ]       │
│    └─► CLEAN    ──► [ WRITE TO RAW MINIO BUCKET ]                      │
└────────────────────┬───────────────────────────────────────────────────┘
                     │
         ┌───────────┴───────────┐
         ▼                       ▼
┌──────────────────┐   ┌──────────────────────────────────┐
│ RAW DFS (MinIO)  │   │ EVENT BUS (Apache Kafka)         │
│ - Raw HTML       │   │ - Topic: scraped_files_topic     │
│ - Clean PDFs     │   │ - Partition Key: target_domain   │
│ - JSON Metadata  │   └────────────────┬─────────────────┘
└────────┬─────────┘                    │
         │                              │ (Stream Trigger)
         └───────────┐      ┌───────────┘
                     ▼      ▼
┌────────────────────────────────────────────────────────────────────────┐
│ 4. STREAMING ETL, GEO-TAGGING & CONTENT DEDUPLICATION (Apache Spark)   │
│    - In-Memory DOM Parsing & PDF Text Extraction                       │
│    - Content Deduplication (SHA256 Exact + SimHash Fuzzy Match)        │
│    - Admin Entity Resolution (Province, District, Municipality/Rural)  │
│    ├─► DUPLICATE CONTENT ──► [ DISCARD / LINK EXISTING RECORD ]        │
│    └─► UNIQUE CONTENT    ──► [ TRANSFORM, GEO-TAG & INDEX ]            │
└────────────────────┬───────────────────────────────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────────────────────────┐
│ 5. TARGET DATA STORES & BATCH MAINTENANCE (Apache Airflow)             │
│    - PostgreSQL (Structured Entities, Meta, Audit Logs)                │
│    - OpenSearch / Vector DB (Search Index & Text Embeddings)           │
│    - Airflow DAGs: Nightly deep-dedup, index optimization, cleanup     │
└────────────────────────────────────────────────────────────────────────┘

```

---

## 3. The Three-Tier Orchestration Model

To ensure reliability, resource efficiency, and proper task routing, orchestration is split into three distinct layers based on their specific strengths:

| Orchestrator Tier | Technology | Primary Responsibility |
| --- | --- | --- |
| **Resource & Container Orchestration** | **Kubernetes (K8s)** | Manages the physical infrastructure. Handles auto-scaling of Go scraper pods, Spark executor pods, and maintains high availability for Kafka, MinIO, and Redis clusters across nodes. |
| **Scraper Workflow Orchestration** | **Temporal** | Manages the real-time, event-driven, and long-running recursive crawl logic. It tracks rate limits per domain, handles Go worker crashes gracefully via its durable event history, and dynamically spawns sub-tasks as new internal URLs are discovered. |
| **ETL Batch & Maintenance Scheduler** | **Apache Airflow** | Manages the DAG-based, scheduled data engineering tasks. While Spark handles the real-time streaming ETL from Kafka, Airflow triggers heavy batch operations: nightly global deduplication across the data lake, OpenSearch index optimizations, weekly reporting, and quarantine bucket cleanup. |

---

## 4. Multi-Stage Filtration, Security & Geo-Tagging Pipeline

To prevent wasted processing power, database bloat, and security threats—and to ensure data is queryable via frontend map interfaces—data passes through the following checkpoints:

```text
[ URL ] ──► [ 1. URL Filter ] ──► [ Fetch ] ──► [ 2. Malware Scan ] ──► [ DFS ] ──► [ 3. Content Dedup ] ──► [ 4. Geo-Tagging ]

```

### Stage 1: URL-Level Pre-Fetch Deduplication (Redis Bloom Filter)

* **Goal:** Avoid downloading pages or files that have already been scraped.
* **Mechanism:**
* Before issuing an HTTP request, the Go scraper queries a **Distributed Redis Bloom Filter**.
* The Bloom Filter maintains a space-efficient bitset of normalized URL hashes (e.g., `SHA256(canonical_url)`).
* If the URL exists, the scraper skips fetching. If new, it is added to the filter and fetched.



### Stage 2: Security & Malware Inspection Layer

* **Goal:** Intercept malicious executables, macro-enabled documents, or infected payloads before they enter the data lake.
* **Mechanism:**
* All binary downloads (PDFs, DOCX, ZIPs) and HTML payloads are streamed through a **ClamAV Anti-Virus Engine** sidecar service.
* **Clean Payload:** Saved into the primary MinIO bucket (`s3://nepal-scraping-lake/raw/`).
* **Infected Payload:** Blocked and dumped into an isolated quarantine bucket (`s3://quarantine-lake/`). An alert is logged in PostgreSQL, and no Kafka message is emitted.



### Stage 3: Post-Fetch Content Deduplication (Spark Cluster)

* **Goal:** Eliminate duplicate pages, mirrored notices, or press releases published across multiple URLs or subdomains.
* **Mechanism:**
* **Exact Match (SHA256):** Spark computes a hash of the normalized text body. If it matches an existing record, the new page is marked as an alias.
* **Fuzzy Match (SimHash):** For news articles or notices with slight template variations, Spark calculates a **SimHash** distance score. Content with $>95\%$ similarity is grouped under a single canonical record.



### Stage 4: Geo-Spatial & Administrative Entity Resolution (Spark Cluster)

* **Goal:** Enable interactive map-based search and regional filtering by tagging every document explicitly down to the Municipality / Rural Municipality level.
* **Mechanism:**
* **Domain Rules:** Spark checks the source domain against the seed registry (e.g., mapping `pokharamun.gov.np` directly to Gandaki Province $\rightarrow$ Kaski District $\rightarrow$ Pokhara Metropolitan City).
* **NLP Gazetteer Extraction:** For generic domains (e.g., news sites), Spark scans the extracted text using a dictionary of all 7 Provinces, 77 Districts, and 753 Municipalities/Rural Municipalities.
* **Enrichment:** A structured `geo_location` object is appended to the data payload before it is inserted into PostgreSQL and OpenSearch, allowing frontend map clicks to strictly filter results by any of these three administrative tiers.



---

## 5. Self-Contained DFS Storage & Enriched Spark Payload Specification

The Go Scraper writes the initial raw payload to MinIO. Once the Spark ETL finishes processing, it generates the **Enriched Payload** (adding the extracted NLP data and precise Geo-Tags), which is then written to the primary databases.

### 5.1 Scraper Output (Written to MinIO)

This contains the raw data and security audit, ensuring the ETL can process it asynchronously.

```json
{
  "storage_metadata": {
    "website_name": "Pokhara Metropolitan City",
    "target_domain": "pokharamun.gov.np",
    "page_url": "https://pokharamun.gov.np/notice/detail/456",
    "scraped_at": "2026-09-16T22:30:00Z",
    "content_type": "application/pdf",
    "crawl_depth": 3
  },
  "security_audit": {
    "virus_scan_passed": true,
    "scanner_engine": "ClamAV",
    "scanned_at": "2026-09-16T22:30:02Z",
    "file_sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
  },
  "raw_payload": {
    "file_extension": "pdf",
    "content_encoding": "base64",
    "data": "JVBERi0xLj... (Base64 Encoded Binary Data)"
  }
}

```

### 5.2 Spark ETL Output (Upserted to OpenSearch & PostgreSQL)

Spark parses the MinIO file, extracts the text, checks for duplicates, and appends the **Administrative Geo-Tags** (Province, District, Municipality/Rural Municipality) and **Extracted Metadata** to power the map-based search engine.

```json
{
  "document_id": "doc_8831a2b",
  "source_url": "https://pokharamun.gov.np/notice/detail/456",
  "language_detected": "mixed",
  "extracted_metadata": {
    "title": "Local Governance Update Notice",
    "description": "Official notice regarding municipal administration.",
    "keywords": ["nepal", "local government", "gandaki", "pokhara"],
    "contact_info": {
      "emails": ["info@pokharamun.gov.np"],
      "phones": ["+977-61-521105"]
    }
  },
  "geo_location": {
    "province_code": "P4",
    "province_name_en": "Gandaki Province",
    "province_name_ne": "गण्डकी प्रदेश",
    "district_code": "D39",
    "district_name_en": "Kaski",
    "district_name_ne": "कास्की",
    "municipality_id": "MUN75340",
    "municipality_type": "Metropolitan City", 
    "municipality_name_en": "Pokhara",
    "municipality_name_ne": "पोखरा",
    "ward_number": null
  },
  "searchable_text": "Full extracted PDF text goes here for BM25 indexing..."
}

```