"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { ArrowRight, Newspaper } from "lucide-react";
import { fetchPlacePages, type PlacePages } from "@/components/map/placePages";

// A map region's popup: the top pages the search engine has about the place,
// from the real search API, plus a link to all of them on the search page.
// (There is no per-region news feed endpoint yet, so this uses search.)

const LIST_HEIGHT = 208; // px: fixed, so the popup doesn't jump when results arrive

function timeAgo(iso: string | null): string | null {
  if (!iso) return null;
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return null;
  const days = Math.floor((Date.now() - date.getTime()) / 86_400_000);
  if (days < 1) return "today";
  if (days < 30) return `${days} day${days === 1 ? "" : "s"} ago`;
  return date.toISOString().slice(0, 10);
}

export function NewsPopupContent({ place, listMaxHeight }: { place: string; listMaxHeight?: number }) {
  // The popup is re-created for each place, so loading starts fresh every time.
  const [pages, setPages] = useState<PlacePages | null>(null);

  useEffect(() => {
    let cancelled = false;
    fetchPlacePages(place)
      .then((result) => !cancelled && setPages(result))
      .catch(() => !cancelled && setPages({ ok: false, error: "Couldn't reach the server." }));
    return () => {
      cancelled = true;
    };
  }, [place]);

  const height = Math.min(LIST_HEIGHT, listMaxHeight ?? LIST_HEIGHT);
  const searchHref = `/search?${new URLSearchParams({ q: place })}`;

  return (
    <div className="w-64 max-w-[80vw]">
      <div className="mb-2 flex items-center gap-1.5 text-sm font-semibold text-slate-900 dark:text-slate-50">
        <Newspaper className="h-4 w-4 text-blue-600 dark:text-blue-400" aria-hidden />
        Pages about {place}
      </div>

      <div className="overflow-y-auto" style={{ height }} aria-live="polite" aria-busy={pages === null}>
        {pages === null ? (
          <ul className="space-y-3" aria-label="Loading pages">
            {Array.from({ length: 3 }).map((_, i) => (
              <li key={i} className="animate-pulse space-y-1.5">
                <div className="h-3.5 w-11/12 rounded bg-slate-200 dark:bg-slate-700" />
                <div className="h-2.5 w-1/2 rounded bg-slate-100 dark:bg-slate-800" />
                <div className="h-2.5 w-full rounded bg-slate-100 dark:bg-slate-800" />
              </li>
            ))}
          </ul>
        ) : !pages.ok ? (
          <p className="rounded-md bg-rose-50 px-2.5 py-2 text-xs text-rose-800 dark:bg-rose-500/10 dark:text-rose-200">
            Couldn&rsquo;t load pages right now. {pages.error}
          </p>
        ) : pages.results.length === 0 ? (
          <p className="rounded-md bg-slate-50 px-2.5 py-2 text-xs text-slate-600 dark:bg-slate-800 dark:text-slate-300">
            No pages about {place} yet. Check back after the next crawl.
          </p>
        ) : (
          <ul className="space-y-2">
            {pages.results.map((item) => {
              const when = timeAgo(item.published_at);
              return (
                <li key={item.id} className="border-b border-slate-100 pb-2 last:border-0 last:pb-0 dark:border-slate-700">
                  <a
                    href={item.url}
                    target="_blank"
                    rel="noopener noreferrer nofollow"
                    className="block text-sm font-medium leading-snug !text-slate-900 hover:!text-blue-700 hover:underline dark:!text-slate-100 dark:hover:!text-blue-300"
                  >
                    {item.title || item.url}
                  </a>
                  <p className="mt-0.5 truncate text-xs text-slate-500 dark:text-slate-400">
                    {item.domain}
                    {when && ` · ${when}`}
                  </p>
                  {item.snippet && (
                    <p className="mt-1 line-clamp-2 text-xs leading-relaxed text-slate-600 dark:text-slate-300">{item.snippet}</p>
                  )}
                </li>
              );
            })}
          </ul>
        )}
      </div>

      <Link
        href={searchHref}
        className="mt-2 flex items-center justify-between gap-2 rounded-md bg-blue-50 px-2.5 py-1.5 text-xs font-medium !text-blue-700 hover:bg-blue-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 dark:bg-blue-500/10 dark:!text-blue-300 dark:hover:bg-blue-500/20"
      >
        {pages?.ok && pages.total > pages.results.length
          ? `See all ${pages.total.toLocaleString("en-US")} results`
          : `Search everything about ${place}`}
        <ArrowRight className="h-3.5 w-3.5 shrink-0" aria-hidden />
      </Link>
    </div>
  );
}
