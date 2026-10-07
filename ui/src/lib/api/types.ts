// Shapes of the FastAPI gateway's responses (api/README.md). Shared by server code and
// client components, so nothing server-only belongs here.

export const MAX_QUERY_LENGTH = 512;
// The search engine pages through at most this many ranked results (page * limit).
export const MAX_RESULT_WINDOW = 500;

export interface GeoRef {
  province_code: string | null;
  province_name: string | null;
  district_code: string | null;
  district_name: string | null;
  municipality_id: string | null;
  municipality_name: string | null;
  ward_number: number | null;
}

export interface SearchResult {
  id: string;
  title: string;
  url: string;
  domain: string;
  snippet: string;
  result_type: string;
  language: string;
  published_at: string | null;
  relevance_score: number;
  geo: GeoRef | null;
}

export interface SearchResponse {
  query: string;
  page: number;
  limit: number;
  total_hits: number;
  took_ms: number;
  query_language: string;
  degraded: boolean;
  results: SearchResult[];
}

export interface AdminUser {
  username: string;
  email: string | null;
  role: string;
}

export interface LoginResponse {
  access_token: string;
  token_type: "bearer";
  expires_in: number;
  user: AdminUser;
}

export interface SearchTraffic {
  searches: number;
  queries_per_second: number;
  avg_latency_ms: number | null;
  p95_latency_ms: number | null;
  zero_result_rate: number;
  click_through_rate: number;
  since: string;
  until: string;
}

export interface AdminSummary {
  timestamp: string;
  domains: {
    total_registered: number;
    pending: number;
    active_crawling: number;
    paused: number;
    completed: number;
    failed_or_blocked: number;
  };
  links: { total_child_links_discovered: number };
  storage: {
    total_raw_files: number;
    unprocessed_files: number;
    processing_files: number;
    processed_files: number;
    failed_files: number;
    quarantined_files: number;
    unprocessed_size_bytes: number;
    oldest_unprocessed_at: string | null;
    total_storage_used_bytes: number;
  };
  quarantine: {
    count: number;
    size_bytes: number;
    size_mb: number;
    latest_threat_detected: string | null;
    latest_scanned_at: string | null;
  };
  errors_last_24h: { WARN: number; ERROR: number; FATAL: number };
  search_traffic: SearchTraffic;
}

export type CheckStatus = "ok" | "degraded" | "failing";

export interface ReadinessReport {
  status: CheckStatus;
  checks: {
    database: {
      status?: CheckStatus;
      revision?: string | null;
      expected_revision?: string;
      problems?: string[];
    };
    search: { status: "ok" | "degraded"; detail: string };
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function isSearchResponse(value: unknown): value is SearchResponse {
  return (
    isRecord(value) &&
    typeof value.total_hits === "number" &&
    typeof value.page === "number" &&
    Array.isArray(value.results) &&
    value.results.every((item) => isRecord(item) && typeof item.id === "string" && typeof item.url === "string")
  );
}

export function isAdminUser(value: unknown): value is AdminUser {
  return isRecord(value) && typeof value.username === "string" && typeof value.role === "string";
}

export function isLoginResponse(value: unknown): value is LoginResponse {
  return (
    isRecord(value) &&
    typeof value.access_token === "string" &&
    typeof value.expires_in === "number" &&
    isAdminUser(value.user)
  );
}

export function isAdminSummary(value: unknown): value is AdminSummary {
  return (
    isRecord(value) &&
    isRecord(value.domains) &&
    isRecord(value.storage) &&
    isRecord(value.quarantine) &&
    isRecord(value.errors_last_24h) &&
    isRecord(value.search_traffic)
  );
}

export function isReadinessReport(value: unknown): value is ReadinessReport {
  return (
    isRecord(value) &&
    typeof value.status === "string" &&
    isRecord(value.checks) &&
    isRecord(value.checks.database) &&
    isRecord(value.checks.search)
  );
}
