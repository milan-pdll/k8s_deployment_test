import Link from "next/link";
import { TopNav } from "@/components/layout/TopNav";
import { SearchBox } from "@/components/home/SearchBox";
import { SearchTabs } from "@/components/search/SearchTabs";
import { ResultItem } from "@/components/search/ResultItem";
import { PgsLogo } from "@/components/home/PgsLogo";
import { apiFetch } from "@/lib/api/server";
import { MAX_QUERY_LENGTH, MAX_RESULT_WINDOW, isSearchResponse } from "@/lib/api/types";

const PAGE_SIZE = 10;

function firstValue(value: string | string[] | undefined): string {
  return (Array.isArray(value) ? value[0] : value) ?? "";
}

export default async function SearchPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const params = await searchParams;
  const q = firstValue(params.q).trim().slice(0, MAX_QUERY_LENGTH);
  const requestedPage = Number.parseInt(firstValue(params.page), 10);
  const maxPage = Math.floor(MAX_RESULT_WINDOW / PAGE_SIZE);
  const page = Number.isFinite(requestedPage) ? Math.min(Math.max(requestedPage, 1), maxPage) : 1;

  const result = q
    ? await apiFetch("/api/v1/search", isSearchResponse, {
        searchParams: { q, page, limit: PAGE_SIZE },
      })
    : null;

  const pageHref = (target: number) => `/search?${new URLSearchParams({ q, page: String(target) })}`;

  return (
    <div className="min-h-screen bg-white dark:bg-slate-950">
      <TopNav />

      <div className="border-b border-slate-200 px-6 pb-4 dark:border-slate-800">
        <div className="flex flex-wrap items-center gap-6">
          <Link href="/">
            <PgsLogo size="small" />
          </Link>
          <div className="min-w-[280px] max-w-xl flex-1">
            <SearchBox autoFocus={false} initialValue={q} compact />
          </div>
        </div>
        <div className="mt-3">
          <SearchTabs />
        </div>
      </div>

      <main className="px-6 py-6">
        {!q ? (
          <p className="text-sm text-slate-500 dark:text-slate-400">Type a query above to search.</p>
        ) : !result?.ok ? (
          <p role="alert" className="max-w-2xl rounded-lg bg-rose-50 px-4 py-3 text-sm text-rose-700 dark:bg-rose-500/10 dark:text-rose-300">
            Search is unavailable right now: {result?.error}
          </p>
        ) : (
          <>
            <p className="mb-4 text-xs text-slate-500 dark:text-slate-400">
              {result.data.total_hits === 0
                ? <>No results for &ldquo;{q}&rdquo;.</>
                : <>{result.data.total_hits} result{result.data.total_hits === 1 ? "" : "s"} for &ldquo;{q}&rdquo; ({result.data.took_ms} ms)</>}
              {result.data.degraded ? " · some ranking signals were unavailable" : null}
            </p>
            {result.data.results.map((item) => (
              <ResultItem key={item.id} result={item} />
            ))}
            <nav className="mt-6 flex gap-4 text-sm" aria-label="Result pages">
              {page > 1 ? (
                <Link className="text-blue-700 hover:underline dark:text-blue-400" href={pageHref(page - 1)}>
                  Previous
                </Link>
              ) : null}
              {page * PAGE_SIZE < Math.min(result.data.total_hits, MAX_RESULT_WINDOW) ? (
                <Link className="text-blue-700 hover:underline dark:text-blue-400" href={pageHref(page + 1)}>
                  Next
                </Link>
              ) : null}
            </nav>
          </>
        )}
      </main>
    </div>
  );
}
