import { ArrowRight, Newspaper } from "lucide-react";

// A map region's popup: a link to the real search results for the place. (There is no
// per-region feed endpoint yet; the search API's geo filters are the way in.)
export function NewsPopupContent({ place }: { place: string; listMaxHeight?: number }) {
  const href = `/search?${new URLSearchParams({ q: place })}`;
  return (
    <div className="w-64 max-w-[80vw]">
      <div className="flex items-center gap-1.5 text-sm font-semibold text-slate-900 dark:text-slate-50">
        <Newspaper className="h-4 w-4 text-blue-600 dark:text-blue-400" aria-hidden />
        {place}
      </div>
      <p className="mt-1 text-xs leading-relaxed text-slate-600 dark:text-slate-300">
        News, notices and documents the search engine has found about this place.
      </p>
      <a
        href={href}
        className="mt-3 flex items-center justify-between gap-2 rounded-md bg-blue-50 px-2.5 py-1.5 text-xs font-medium !text-blue-700 hover:bg-blue-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 dark:bg-blue-500/10 dark:!text-blue-300 dark:hover:bg-blue-500/20"
      >
        Search pages about {place}
        <ArrowRight className="h-3.5 w-3.5 shrink-0" aria-hidden />
      </a>
    </div>
  );
}
