"use server";

import { apiFetch } from "@/lib/api/server";
import { isSearchResponse, type SearchResult } from "@/lib/api/types";

// The map popup's list of pages about a place. The browser can't reach the API
// directly (only the Next.js server can), so the popup calls this Server
// Function, which runs the same public search as the search page. It is
// reachable by anyone with a POST request, so it only ever does that public,
// read-only search, with a bounded query.

export type PlacePages =
  | { ok: true; results: SearchResult[]; total: number }
  | { ok: false; error: string };

const MAX_PLACE_LENGTH = 100;
const POPUP_RESULTS = 4;

export async function fetchPlacePages(place: unknown): Promise<PlacePages> {
  if (typeof place !== "string") return { ok: false, error: "Invalid place." };
  const q = place.trim().slice(0, MAX_PLACE_LENGTH);
  if (!q) return { ok: true, results: [], total: 0 };

  const result = await apiFetch("/api/v1/search", isSearchResponse, {
    searchParams: { q, page: 1, limit: POPUP_RESULTS },
  });
  if (!result.ok) return { ok: false, error: result.error };
  return { ok: true, results: result.data.results, total: result.data.total_hits };
}
