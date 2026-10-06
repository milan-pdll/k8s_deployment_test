import Link from "next/link";
import { ArrowRight, Newspaper } from "lucide-react";
import { generateMockNews } from "@/lib/mock/news";

export function NewsPopupContent({ place, listMaxHeight }: { place: string; listMaxHeight?: number }) {
  const news = generateMockNews(place, 4);

  return (
    <div className="w-64 max-w-[80vw]">
      <div className="mb-2 flex items-center gap-1.5 text-sm font-semibold text-slate-900 dark:text-slate-50">
        <Newspaper className="h-4 w-4 text-blue-600 dark:text-blue-400" />
        Latest news · {place}
      </div>
      <ul
        className="max-h-64 space-y-2 overflow-y-auto"
        style={listMaxHeight ? { maxHeight: listMaxHeight } : undefined}
      >
        {news.map((item) => (
          <li key={item.id} className="border-b border-slate-100 pb-2 last:border-0 last:pb-0 dark:border-slate-700">
            <p className="text-sm font-medium leading-snug text-slate-900 dark:text-slate-100">{item.title}</p>
            <p className="mt-0.5 text-xs text-slate-500 dark:text-slate-400">
              {item.source} · {item.publishedAt}
            </p>
            <p className="mt-1 text-xs leading-relaxed text-slate-600 dark:text-slate-300">{item.snippet}</p>
          </li>
        ))}
      </ul>
      <Link
        href={`/search?q=${encodeURIComponent(place)}`}
        className="mt-2 flex items-center justify-between gap-2 rounded-md bg-blue-50 px-2.5 py-1.5 text-xs font-medium !text-blue-700 hover:bg-blue-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 dark:bg-blue-500/10 dark:!text-blue-300 dark:hover:bg-blue-500/20"
      >
        Search everything about {place}
        <ArrowRight className="h-3.5 w-3.5 shrink-0" aria-hidden />
      </Link>
    </div>
  );
}
