import { NextResponse, type NextRequest } from "next/server";
import { apiFetch } from "@/lib/api/server";
import { MAX_QUERY_LENGTH, isSearchResponse } from "@/lib/api/types";

const SUGGESTION_LIMIT = 6;

/** Live results for the search box: the top pages for what has been typed so far. */
export async function GET(request: NextRequest) {
  const q = (request.nextUrl.searchParams.get("q") ?? "").trim().slice(0, MAX_QUERY_LENGTH);
  if (!q) return NextResponse.json({ results: [] });

  const result = await apiFetch("/api/v1/search", isSearchResponse, {
    searchParams: { q, page: 1, limit: SUGGESTION_LIMIT },
  });
  if (!result.ok) return NextResponse.json({ results: [] }, { status: result.status === 0 ? 503 : result.status });

  const results = result.data.results.map(({ id, title, url, domain }) => ({ id, title, url, domain }));
  return NextResponse.json({ results });
}
