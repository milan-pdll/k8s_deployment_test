import { MapPin } from "lucide-react";
import type { SearchResult } from "@/lib/api/types";

function placeLabel(result: SearchResult): string | null {
  const geo = result.geo;
  if (!geo) return null;
  return [geo.municipality_name, geo.district_name, geo.province_name].filter(Boolean).join(", ") || null;
}

export function ResultItem({ result }: { result: SearchResult }) {
  const place = placeLabel(result);
  const published = result.published_at ? new Date(result.published_at) : null;
  return (
    <article className="mb-6 max-w-2xl">
      <p className="truncate text-xs text-slate-500 dark:text-slate-400">{result.url}</p>
      <a
        href={result.url}
        rel="noopener noreferrer nofollow"
        target="_blank"
        className="text-lg text-blue-700 hover:underline dark:text-blue-400"
      >
        {result.title || result.url}
      </a>
      <p className="mt-1 text-sm leading-relaxed text-slate-600 dark:text-slate-400">{result.snippet}</p>
      <p className="mt-1 flex flex-wrap items-center gap-3 text-xs text-slate-500 dark:text-slate-400">
        <span>{result.domain}</span>
        {place ? (
          <span className="flex items-center gap-1">
            <MapPin className="h-3 w-3" />
            {place}
          </span>
        ) : null}
        {published && !Number.isNaN(published.getTime()) ? (
          <span>{published.toISOString().slice(0, 10)}</span>
        ) : null}
      </p>
    </article>
  );
}
