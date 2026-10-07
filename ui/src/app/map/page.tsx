import type { Metadata } from "next";
import { Suspense } from "react";
import { TopNav } from "@/components/layout/TopNav";
import { GeoExplorer } from "@/components/map/GeoExplorer";
import { DISTRICT_TO_PROVINCE, PROVINCE_NAMES, canonicalDistrictName, titleCase } from "@/lib/geo";

type MapPageProps = { searchParams: Promise<Record<string, string | string[] | undefined>> };

const DESCRIPTION =
  "Interactive map of Nepal's 7 provinces, 77 districts and 753 local governments. Click any place for local news and key facts.";

// Names the linked place in the browser tab and in shared links, e.g.
// "/map?focus=Kaski" → "Kaski district — Map of Nepal · PGS Search". Must give
// the same title as GeoExplorer, which keeps it up to date as people click.
function placeTitle(focus: string, district: string): string | null {
  if (!focus) return null;
  if (district) return `${focus}, ${district}`;
  const districtKey = canonicalDistrictName(focus);
  if (DISTRICT_TO_PROVINCE[districtKey]) return `${titleCase(districtKey)} district`;
  const province = Object.values(PROVINCE_NAMES).find(
    (p) => p.toLowerCase() === focus.toLowerCase() || p.replace(" Province", "").toLowerCase() === focus.toLowerCase()
  );
  return province ?? focus;
}

export async function generateMetadata({ searchParams }: MapPageProps): Promise<Metadata> {
  const params = await searchParams;
  const first = (value: string | string[] | undefined) => (Array.isArray(value) ? value[0] : value)?.trim() ?? "";
  const focus = first(params.focus).slice(0, 60);
  const district = first(params.district).slice(0, 60);
  const place = placeTitle(focus, district);
  return {
    title: place ? `${place} — Map of Nepal · PGS Search` : "Map of Nepal — PGS Search",
    description: DESCRIPTION,
  };
}

export default function MapPage() {
  return (
    <div className="flex h-screen flex-col bg-white font-sans dark:bg-slate-950">
      <div className="shrink-0 border-b border-slate-200 dark:border-slate-800">
        <TopNav />
      </div>
      <h1 className="sr-only">Map of Nepal by province, district and local government</h1>
      <div className="min-h-0 flex-1">
        {/* GeoExplorer reads ?focus= with useSearchParams, which needs a
            Suspense boundary if this page is ever statically rendered. */}
        <Suspense
          fallback={
            <div className="flex h-full items-center justify-center text-sm text-slate-500">Loading map…</div>
          }
        >
          <GeoExplorer />
        </Suspense>
      </div>
    </div>
  );
}
