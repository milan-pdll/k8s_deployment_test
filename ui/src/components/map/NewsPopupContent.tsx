import { Search } from "lucide-react";

// A map region's popup: a link to the real search results for the place. (There is no
// per-region feed endpoint yet; the search API's geo filters are the way in.)
export function NewsPopupContent({ place }: { place: string; listMaxHeight?: number }) {
  const href = `/search?${new URLSearchParams({ q: place })}`;
  return (
    <div className="w-64 max-w-[80vw]">
      <a
        href={href}
        className="flex items-center gap-1.5 text-sm font-semibold text-blue-700 hover:underline"
      >
        <Search className="h-4 w-4" />
        Search pages about {place}
      </a>
    </div>
  );
}
