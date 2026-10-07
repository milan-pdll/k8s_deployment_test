# API gateway (FastAPI)

The public HTTP API of the PGS Search Engine (`pgs_api.main:app`, served by Uvicorn on :8000;
behind nginx at `/api/v1/` and `/health/`). It reads PostgreSQL as the `pgs_api` role
through the shared `pgs_db` package and calls the search engine over gRPC
(`search-engine/proto/search.proto`). Interactive docs: `http://localhost:8000/docs`.
How it fits in the system: [`docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md).

## Endpoints (implemented)

| Method | Path | Auth | Answer |
| --- | --- | --- | --- |
| GET | `/health/live` | -- | `{"status": "ok"}` while the process runs |
| GET | `/health/ready` | -- | 200/503 with checks: database (schema revision, reference data, Gold freshness) and search (gRPC health; "degraded" when the search engine is down -- not a readiness failure) |
| GET | `/api/v1/search?q=&page=1&limit=10&province_code=&district_code=&municipality_id=&ward_number=&language=auto&content_type=all` | -- | `{query, page, limit, total_hits, took_ms, query_language, degraded, results: [{id, title, url, domain, snippet, result_type, language, published_at, relevance_score, geo}]}`; 400 invalid parameters (`q` 1..512 characters, `limit` 1..100, `page * limit <= 500`), 503 search engine unavailable, 504 timeout, 502 search failed. The query is logged to `search_queries` best-effort. |
| GET | `/api/v1/geo/hierarchy` | -- | provinces -> districts -> local bodies (codes and names) |
| GET | `/api/v1/geo/content-stats?level=province\|district\|local_body&within=` | -- | indexed-content counts per region (map colouring) from the Gold summaries |
| POST | `/api/v1/auth/login` `{username \| email, password}` | -- | `{access_token, token_type: "bearer", expires_in, user}`; 401 bad credentials; 503 when `API_AUTH_SECRET` is not set |
| GET | `/api/v1/auth/me` | Bearer | the signed-in admin |
| GET | `/api/v1/admin/summary` | Bearer | dashboard numbers: domains, storage/queue, quarantine, errors in the last 24 h, search traffic |

Tokens are HMAC-SHA256 signed with `API_AUTH_SECRET` (>= 32 characters) and carry the
admin's id, role and expiry (`API_TOKEN_TTL_SECONDS`, default 8 h); verification is
constant-time and checks that the account is still active. Admin accounts live in
`admin_users` (argon2 hashes): `docker compose run --rm db-migrate python
scripts/create_admin.py <username> --email <address>`.

## Configuration (`src/pgs_api/core/settings.py`)

| Variable | Default | Purpose |
| --- | --- | --- |
| `DATABASE_URL` | required | `postgresql+psycopg://pgs_api:...@postgres:5432/pgs` |
| `SEARCH_GRPC_HOST` / `SEARCH_GRPC_PORT` | `localhost` / `50051` | the search engine |
| `SEARCH_GRPC_TLS_CA_FILE` | unset (plaintext) | PEM CA that signed the search engine's certificate; set = TLS (the engine needs `SEARCH_GRPC_TLS_CERT_FILE` / `SEARCH_GRPC_TLS_KEY_FILE`) |
| `SEARCH_TIMEOUT_SECONDS` | `5` (compose: 8) | gRPC deadline per search |
| `API_AUTH_SECRET` | unset | enables login/admin; unset -> those answer 503 |
| `API_TOKEN_TTL_SECONDS` | `28800` | token lifetime |
| `API_CORS_ORIGINS` | empty (no CORS) | comma-separated browser origins allowed cross-origin |
| `LOG_LEVEL` | `INFO` | JSON logs with a request id per request; query strings are not logged |

The configuration is validated at startup; an invalid value stops the process with a
message naming the variable (never its value).

## Layout

```text
api/
  src/pgs_api/
    main.py            Uvicorn entry point (pgs_api.main:app): settings -> logging -> app
    app.py             create_app: middleware, error handlers, routers
    core/              settings (typed, from the environment), errors, observability
                       (JSON logs, request ids), services (the dependency container)
    auth/              tokens (HMAC-signed admin tokens), dependencies (CurrentAdmin)
    db/                database (pgs_db repositories behind a small interface), search_log
    grpc/              client of the search engine (deadlines, status-code mapping, health)
    health/            readiness checks
    routers/           one module per endpoint group: search, geo, auth, admin, health
    schemas/           request/response models per area: search, geo, auth, admin, health
  tests/               pytest with fake database and search backends
  pyproject.toml       package metadata and the pytest configuration
  requirements.txt     pinned runtime dependencies (the image installs these + pgs-db)
  Dockerfile
```

## Development

```bash
.venv/bin/python -m pytest api/tests      # unit tests (fake gRPC stub and repositories)
.venv/bin/ruff check api && .venv/bin/pyright
PYTHONPATH=api/src:search-engine/src uvicorn pgs_api.main:app --reload   # with DATABASE_URL etc.
```

---

# Original specification (target design, mostly NOT implemented)

The rest of this file is the gateway's original design document. Only the endpoints in
the table above exist; the admin endpoints below for domains, storage, logs, quarantine,
cluster metrics, crawl control and file downloads are not implemented yet (the
repository methods most of them need exist in `pgs_db`, see `database/README.md` §3).

# API Gateway Specification & Service Contract

## FastAPI Gateway REST API & System Administration Manual

---

## 1. Executive Summary

This document specifies the complete REST API interface provided by the **FastAPI Gateway Service**. The API Gateway serves as the single public entry point for both the **User Frontend Application** (Search & Spatial Navigation) and the **Admin Infrastructure Dashboard** (System Health, Crawl Management, Metrics, Logs, and Security Audits).

All external communications use standard **HTTP/REST (JSON)** over TLS. Internally, the API Gateway translates requests into low-latency **gRPC Protobuf** calls to backend microservices (Search Core Engine, Temporal Orchestration Manager, Spark ETL Metrics Collector, MinIO DFS, and System Telemetry).

```
┌────────────────────────────────────────────────────────────────────────┐
│                         FRONTEND APPLICATIONS                          │
│     [ User UI: Search & Map ]        [ Admin UI: Monitoring Panel ]    │
└───────────────────┬────────────────────────────────┬───────────────────┘
                    │                                │
                    │ REST / HTTP (JSON)             │ REST / HTTP (JWT Auth)
                    ▼                                ▼
┌────────────────────────────────────────────────────────────────────────┐
│                      FASTAPI API GATEWAY SERVICE                       │
│    - OpenAPI / Swagger Docs        - Rate Limiting & Input Validation  │
│    - JWT Authentication & RBAC     - REST-to-gRPC Protocol Adapter     │
└───────────────────┬────────────────────────────────┬───────────────────┘
                    │                                │
                    │ Internal gRPC                  │ Internal gRPC
                    ▼                                ▼
┌───────────────────────────────┐  ┌─────────────────────────────────────┐
│ SEARCH ENGINE CORE SERVICE    │  │ SYSTEM MONITORING & CONTROL PLANE   │
│ - OpenSearch & BM25/LTR       │  │ - Temporal Workflows & Scrapers     │
│ - Geo-Spatial Entity Engine   │  │ - Spark Metrics & K8s Node Metrics  │
│ - Document / PDF Index        │  │ - MinIO DFS & ClamAV Quarantine     │
└───────────────────────────────┘  └─────────────────────────────────────┘

```

---

## 2. Authentication & Authorization Architecture

### 2.1 Access Control Scheme

The Gateway enforces **Role-Based Access Control (RBAC)** using **JSON Web Tokens (JWT)** signed with RSA-256 / HS256 algorithm.

* **User Section Endpoints:** Publicly accessible or protected by optional client tokens.
* **Admin Section Endpoints:** Strictly protected. Requests must include an `Authorization: Bearer <JWT_TOKEN>` header.
* **Roles:** `SUPER_ADMIN`, `SYSTEM_OPERATOR`, `AUDITOR`.

### 2.2 Auth Endpoints

#### 1. Admin Login

* **`POST /api/v1/auth/login`**
* **Request Body:**
```json
{
  "username": "admin_operator",
  "password": "SecurePassword123!"
}

```


* **Response `200 OK`:**
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6...",
  "token_type": "bearer",
  "expires_in": 28800,
  "user": {
    "username": "admin_operator",
    "role": "SUPER_ADMIN"
  }
}

```



#### 2. Refresh Token

* **`POST /api/v1/auth/refresh`**
* **Headers:** `Authorization: Bearer <REFRESH_TOKEN>`

---

## 3. SECTION A: User API Specifications

The User API supports standard Google-style text search, document discovery, and interactive map-based spatial filtering down to the Municipality / Rural Municipality level.

### 3.1 Unified Search Endpoint

* **`GET /api/v1/user/search`**
* **Description:** Unified endpoint for both free-text searches and map-based spatial filtering. Converts parameters into an internal gRPC `SearchRequest` to the Search Core.

#### Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `q` | `string` | No | Search query text (e.g., `"lok sewa syllabus"`, `"पोखरा बजेट"`). |
| `province_code` | `string` | No | Filter by Province Code (`P1` to `P7`). |
| `district_code` | `string` | No | Filter by District Code (`D01` to `D77`). |
| `municipality_id` | `string` | No | Filter by Municipality ID (e.g., `MUN75340`). |
| `ward_number` | `integer` | No | Filter by Ward Number (e.g., `1`). |
| `content_type` | `string` | No | Filter by type: `all`, `web_page`, `document` (PDF/DOCX). |
| `lang` | `string` | No | Force language analyzer: `auto`, `ne`, `en`. Default: `auto`. |
| `page` | `integer` | No | Page number for pagination (Default: `1`). |
| `limit` | `integer` | No | Page size (Default: `10`, Max: `100`). |

#### Example Request (Map Click on Pokhara Municipality)

`GET /api/v1/user/search?municipality_id=MUN75340&content_type=document&page=1`

#### Response `200 OK`

```json
{
  "status": "success",
  "search_metadata": {
    "query": "",
    "active_filters": {
      "province_name_en": "Gandaki Province",
      "province_name_ne": "गण्डकी प्रदेश",
      "district_name_en": "Kaski",
      "district_name_ne": "कास्की",
      "municipality_name_en": "Pokhara",
      "municipality_type": "Metropolitan City"
    },
    "total_hits": 184,
    "page": 1,
    "total_pages": 19,
    "execution_time_ms": 24
  },
  "regional_card": {
    "region_name_en": "Pokhara Metropolitan City",
    "region_name_ne": "पोखरा महानगरपालिका",
    "official_website": "https://pokharamun.gov.np",
    "contact": {
      "phone": "+977-61-521105",
      "email": "info@pokharamun.gov.np",
      "address": "New Road, Pokhara, Kaski"
    },
    "quick_links": [
      {
        "title": "Ward Directives",
        "url": "https://pokharamun.gov.np/wards"
      },
      {
        "title": "DAO Kaski",
        "url": "https://daokaski.moha.gov.np"
      }
    ]
  },
  "results": [
    {
      "id": "doc_998231",
      "result_type": "document",
      "title": "Pokhara_Annual_Budget_2080_81.pdf",
      "url": "https://pokharamun.gov.np/uploads/budget_2080.pdf",
      "domain": "pokharamun.gov.np",
      "snippet": "Official fiscal budget allocations for <b>Pokhara</b> Metropolitan City...",
      "published_date": "2026-07-15T00:00:00Z",
      "file_info": {
        "extension": "pdf",
        "size_bytes": 3250585,
        "formatted_size": "3.1 MB",
        "download_url": "/api/v1/user/documents/download/doc_998231"
      },
      "geo_tags": {
        "province": "Gandaki Province",
        "district": "Kaski",
        "municipality": "Pokhara Metropolitan City"
      }
    }
  ]
}

```

### 3.2 Geo-Boundaries & Administrative Map Data

* **`GET /api/v1/user/geo/hierarchy`**
* **Description:** Returns the administrative hierarchy tree (Provinces $\rightarrow$ Districts $\rightarrow$ Municipalities) for rendering the interactive SVG map in the frontend.

---

## 4. SECTION B: Admin Monitoring & Management API Specifications

All endpoints in Section B require a valid `SUPER_ADMIN` or `SYSTEM_OPERATOR` JWT bearer token.

---

### 4.1 System Overview & High-Level Health Dashboard

#### 1. System Metrics Summary

* **`GET /api/v1/admin/dashboard/summary`**
* **Description:** Provides a consolidated snapshot of the entire data pipeline state.

```json
{
  "timestamp": "2026-09-16T23:00:00Z",
  "system_status": "HEALTHY",
  "domains": {
    "total_registered": 12450,
    "active_crawling": 850,
    "completed": 11100,
    "failed_or_blocked": 500
  },
  "links": {
    "total_child_links_discovered": 14250000,
    "queued_for_crawl": 3200000,
    "filtered_by_bloom": 9800000
  },
  "storage": {
    "total_raw_files_disk": 4850000,
    "unprocessed_files": 125000,
    "processing_files": 15000,
    "processed_files": 4710000,
    "total_storage_used_bytes": 1485000000000,
    "formatted_storage": "1.48 TB"
  },
  "infrastructure": {
    "k8s_active_nodes": 12,
    "temporal_worker_pods": 48,
    "spark_executors": 16,
    "kafka_brokers": 3
  }
}

```

---

### 4.2 Infrastructure & Cluster Health Monitoring

#### 1. Node & Cluster Performance

* **`GET /api/v1/admin/monitor/nodes`**
* **Description:** Queries Kubernetes metric server & Prometheus to monitor CPU, RAM, Disk I/O across node pools.

```json
{
  "clusters": [
    {
      "node_name": "worker-node-01",
      "role": "Scraper Fleet",
      "status": "Ready",
      "cpu_usage_percent": 68.4,
      "ram_usage": {
        "used_gb": 48.2,
        "total_gb": 64.0,
        "percent": 75.31
      },
      "disk_io_read_mbps": 12.4,
      "disk_io_write_mbps": 45.1,
      "network_rx_mbps": 120.5,
      "network_tx_mbps": 15.2,
      "running_pods": 8
    },
    {
      "node_name": "spark-worker-03",
      "role": "Spark Executor Cluster",
      "status": "Ready",
      "cpu_usage_percent": 92.1,
      "ram_usage": {
        "used_gb": 112.0,
        "total_gb": 128.0,
        "percent": 87.5
      },
      "running_pods": 2
    }
  ]
}

```

#### 2. Service Sub-System Health Check

* **`GET /api/v1/admin/monitor/services`**
* **Description:** Pings internal microservices over gRPC and native APIs to return system status.

```json
{
  "services": {
    "temporal_orchestrator": { "status": "UP", "latency_ms": 3, "active_workflows": 850 },
    "kafka_event_bus": { "status": "UP", "lag": 4200, "active_consumers": 16 },
    "minio_dfs": { "status": "UP", "latency_ms": 12, "available_capacity_tb": 18.5 },
    "spark_streaming": { "status": "UP", "active_jobs": 4, "batch_processing_time_ms": 420 },
    "redis_bloom_filter": { "status": "UP", "memory_used_mb": 4200, "keys_count": 14250000 },
    "opensearch_cluster": { "status": "GREEN", "docs_count": 4710000, "index_size_gb": 320.5 },
    "clamav_scanner": { "status": "UP", "version": "ClamAV 1.4.0", "quarantined_count": 142 }
  }
}

```

---

### 4.3 Domain & Link Management APIs

#### 1. List Domains (Paginated & Filtered)

* **`GET /api/v1/admin/domains`**
* **Query Params:** `status` (`CRAWLING`, `FAILED`, `PENDING`, `COMPLETED`), `search_domain`, `page`, `limit`.

```json
{
  "total_count": 12450,
  "domains": [
    {
      "domain": "mofaga.gov.np",
      "category": "Government",
      "status": "CRAWLING",
      "discovered_child_links": 4520,
      "scraped_pages": 4100,
      "failed_pages": 12,
      "last_crawled_at": "2026-09-16T22:50:00Z",
      "rate_limit_per_sec": 5
    }
  ]
}

```

#### 2. Seed Domain Management

* **`POST /api/v1/admin/domains/add`**
* **Request Body:**
```json
{
  "domains": ["newcollege.edu.np", "districtnews.com.np"],
  "category": "Education",
  "priority": "HIGH"
}

```




* **`POST /api/v1/admin/domains/{domain_name}/action`**
* **Request Body:** `{"action": "PAUSE"}` (Options: `PAUSE`, `RESUME`, `RE_CRAWL`, `DELETE`)



---

### 4.4 Data Storage & Processing Queue Metrics

#### 1. Storage & DFS Data Pool Breakdown

* **`GET /api/v1/admin/storage/metrics`**

```json
{
  "unprocessed_queue": {
    "count": 125000,
    "size_gb": 42.5,
    "oldest_file_timestamp": "2026-09-16T21:40:00Z"
  },
  "processing_queue": {
    "count": 15000,
    "active_spark_executors": 16
  },
  "quarantine_store": {
    "count": 142,
    "size_mb": 520,
    "latest_threat_detected": "Win.Trojan.Generic-998"
  }
}

```

---

### 4.5 Logs, Errors & Malware Security Audits

#### 1. Centralized System Error Logs

* **`GET /api/v1/admin/logs/errors`**
* **Query Params:** `service` (`Scraper`, `ETL`, `Security`, `Search`), `severity` (`ERROR`, `FATAL`, `WARN`), `limit`.

```json
{
  "total": 2,
  "logs": [
    {
      "log_id": "err_99812",
      "timestamp": "2026-09-16T22:45:12Z",
      "service": "Scraper-Go-Worker-12",
      "severity": "ERROR",
      "domain": "failedsite.com.np",
      "url": "https://failedsite.com.np/data",
      "message": "HTTP 503 Service Unavailable - Rate Limit Exceeded by Remote Host"
    },
    {
      "log_id": "err_99815",
      "timestamp": "2026-09-16T22:48:30Z",
      "service": "ClamAV-Sidecar",
      "severity": "WARN",
      "domain": "unsecuresite.com.np",
      "url": "https://unsecuresite.com.np/files/update.exe",
      "message": "Malware detected: SCRIPT.Exploit.CVE-2023. File isolated to Quarantine DFS."
    }
  ]
}

```

#### 2. Quarantine & Security Audits

* **`GET /api/v1/admin/security/quarantine`**
* **Description:** Lists infected files blocked by ClamAV. Allows admins to inspect or permanently delete infected payloads.

---

## 5. Inter-Service gRPC Protocol Buffer Specification

FastAPI relies on internal gRPC stubs. Below is the complete Protobuf definition file (`search_admin.proto`) governing the communication between the API Gateway and the internal backend services.

```protobuf
syntax = "proto3";

package search.engine.v1;

option go_package = "github.com/search/proto/v1;enginev1";

// ==========================================
// USER SEARCH SERVICE
// ==========================================
service SearchService {
  rpc ExecuteSearch (SearchRequest) returns (SearchResponse);
  rpc GetGeoHierarchy (GeoHierarchyRequest) returns (GeoHierarchyResponse);
}

// ==========================================
// ADMIN CONTROL & MONITORING SERVICE
// ==========================================
service AdminMonitoringService {
  rpc GetSystemSummary (EmptyRequest) returns (SystemSummaryResponse);
  rpc GetNodeMetrics (EmptyRequest) returns (NodeMetricsResponse);
  rpc GetDomainStatus (DomainStatusRequest) returns (DomainStatusResponse);
  rpc ManageDomain (ManageDomainRequest) returns (ManageDomainResponse);
  rpc GetSecurityAudit (SecurityAuditRequest) returns (SecurityAuditResponse);
}

// --- SEARCH PROTOBUF MESSAGES ---
message SearchRequest {
  string query = 1;
  string province_code = 2;
  string district_code = 3;
  string municipality_id = 4;
  int32 ward_number = 5;
  string content_type = 6;
  string language = 7;
  int32 page = 8;
  int32 limit = 9;
}

message SearchResponse {
  int32 status_code = 1;
  int32 total_hits = 2;
  int64 execution_time_ms = 3;
  RegionalCard regional_card = 4;
  repeated SearchResultItem results = 5;
}

message RegionalCard {
  string region_name_en = 1;
  string region_name_ne = 2;
  string official_website = 3;
  string phone = 4;
  string email = 5;
  string address = 6;
}

message SearchResultItem {
  string id = 1;
  string result_type = 2;
  string title = 3;
  string url = 4;
  string domain = 5;
  string snippet = 6;
  string download_url = 7;
  int64 file_size_bytes = 8;
  double relevance_score = 9;
}

// --- ADMIN PROTOBUF MESSAGES ---
message EmptyRequest {}

message SystemSummaryResponse {
  int64 total_domains = 1;
  int64 active_crawls = 2;
  int64 total_child_links = 3;
  int64 queued_links = 4;
  int64 raw_files_unprocessed = 5;
  int64 raw_files_processing = 6;
  int64 raw_files_processed = 7;
  int64 total_storage_bytes = 8;
}

message NodeMetricsResponse {
  repeated NodeStatus nodes = 1;
}

message NodeStatus {
  string node_name = 1;
  string role = 2;
  double cpu_percent = 3;
  double ram_percent = 4;
  int32 running_pods = 5;
}

message DomainStatusRequest {
  int32 page = 1;
  int32 limit = 2;
  string filter_status = 3;
}

message DomainStatusResponse {
  int64 total_domains = 1;
  repeated DomainInfo domains = 2;
}

message DomainInfo {
  string domain_name = 1;
  string status = 2;
  int64 child_links_count = 3;
  int64 scraped_pages_count = 4;
  string last_crawled_timestamp = 5;
}

message ManageDomainRequest {
  string domain_name = 1;
  string action = 2; // PAUSE, RESUME, RE_CRAWL, DELETE
}

message ManageDomainResponse {
  bool success = 1;
  string message = 2;
}

message SecurityAuditRequest {
  int32 limit = 1;
}

message SecurityAuditResponse {
  int32 total_quarantined = 1;
  repeated SecurityThreat threats = 2;
}

message SecurityThreat {
  string timestamp = 1;
  string url = 2;
  string threat_name = 3;
  string quarantine_path = 4;
}

```