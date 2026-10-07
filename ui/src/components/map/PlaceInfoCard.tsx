"use client";

import { useMemo } from "react";
import Link from "next/link";
import { Search } from "lucide-react";
import { cn } from "@/lib/cn";
import {
  getDistrictProvince,
  getProvinceColor,
  getProvinceName,
  titleCase,
  type DistrictCollection,
  type MunicipalityCollection,
  type ProvinceCollection,
} from "@/lib/geo";
import { areaKm2, findNeighbours, summariseLocalGovernments } from "@/components/map/placeStats";
import { useIsDark } from "@/components/map/useIsDark";

const FOCUS_RING = "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500";
const formatArea = (km2: number) => `≈ ${Math.round(km2).toLocaleString("en-US")} km²`;

// Key facts about the selected district (or province), all worked out from the
// boundary data, plus a way to search for it.
export function PlaceInfoCard({
  provinces,
  districts,
  municipalities,
  municipalitiesStatus,
  selectedProvince,
  selectedDistrict,
  onSelectDistrict,
}: {
  provinces: ProvinceCollection | null;
  districts: DistrictCollection | null;
  municipalities: MunicipalityCollection | null;
  municipalitiesStatus: "loading" | "ready" | "error";
  selectedProvince: string | null;
  selectedDistrict: string | null;
  onSelectDistrict: (name: string) => void;
}) {
  const dark = useIsDark();
  const province = selectedDistrict ? getDistrictProvince(selectedDistrict) ?? selectedProvince : selectedProvince;

  const facts = useMemo(() => {
    if (!districts) return null;
    if (selectedDistrict) {
      const feature = districts.features.find((f) => f.properties.DISTRICT === selectedDistrict);
      if (!feature) return null;
      const local = municipalities?.features.filter((f) => f.properties.DISTRICT === selectedDistrict);
      return {
        area: areaKm2([feature.geometry]),
        neighbours: findNeighbours(districts, selectedDistrict),
        local: local ? summariseLocalGovernments(local) : null,
        districtCount: null,
      };
    }
    if (selectedProvince) {
      const inProvince = districts.features.filter((f) => getDistrictProvince(f.properties.DISTRICT) === selectedProvince);
      const names = new Set(inProvince.map((f) => f.properties.DISTRICT));
      const shape = provinces?.features.find((f) => getProvinceName(f) === selectedProvince);
      const local = municipalities?.features.filter((f) => names.has(f.properties.DISTRICT));
      return {
        area: areaKm2(shape ? [shape.geometry] : inProvince.map((f) => f.geometry)),
        neighbours: null,
        local: local ? summariseLocalGovernments(local) : null,
        districtCount: inProvince.length,
      };
    }
    return null;
  }, [provinces, districts, municipalities, selectedDistrict, selectedProvince]);

  if (!facts || (!selectedDistrict && !selectedProvince)) return null;

  const name = selectedDistrict ? titleCase(selectedDistrict) : selectedProvince!;
  const searchQuery = selectedDistrict ? titleCase(selectedDistrict) : selectedProvince!;

  return (
    <section
      aria-label={`About ${name}`}
      className="m-3 overflow-hidden rounded-xl border border-slate-200 bg-white shadow-sm dark:border-slate-700 dark:bg-slate-900"
    >
      <div className="h-1.5" style={{ backgroundColor: getProvinceColor(province ?? undefined, dark) }} aria-hidden />
      <div className="p-3">
        <p className="text-[11px] font-medium uppercase tracking-wide text-slate-500 dark:text-slate-400">
          {selectedDistrict ? `District · ${province ?? ""}` : "Province"}
        </p>
        <p className="mt-0.5 text-lg font-semibold leading-tight text-slate-900 dark:text-slate-50">{name}</p>

        <dl className="mt-3 grid grid-cols-2 gap-2">
          <Fact label="Area" value={formatArea(facts.area)} title="Measured from the map's boundary data" />
          {facts.districtCount !== null ? (
            <Fact label="Districts" value={String(facts.districtCount)} />
          ) : (
            <Fact
              label="Local governments"
              value={facts.local ? String(facts.local.total) : municipalitiesStatus === "error" ? "—" : "…"}
            />
          )}
          {facts.districtCount !== null && (
            <Fact
              label="Local governments"
              value={facts.local ? String(facts.local.total) : municipalitiesStatus === "error" ? "—" : "…"}
              className="col-span-2"
            />
          )}
        </dl>

        {facts.local && facts.local.breakdown.length > 0 && (
          <p className="mt-2 text-xs leading-relaxed text-slate-600 dark:text-slate-400">
            {facts.local.breakdown.join(" · ")}
            {facts.local.protectedAreas > 0 &&
              ` · plus ${facts.local.protectedAreas} protected area${facts.local.protectedAreas === 1 ? "" : "s"}`}
          </p>
        )}

        {facts.neighbours && facts.neighbours.length > 0 && (
          <div className="mt-3">
            <p className="text-xs font-medium text-slate-700 dark:text-slate-300">
              Neighbouring districts <span className="font-normal text-slate-500">({facts.neighbours.length})</span>
            </p>
            <ul className="mt-1.5 flex flex-wrap gap-1">
              {facts.neighbours.map((n) => (
                <li key={n}>
                  <button
                    type="button"
                    onClick={() => onSelectDistrict(n)}
                    className={cn(
                      "inline-flex items-center gap-1.5 rounded-full border border-slate-200 px-2 py-0.5 text-xs text-slate-700 hover:border-slate-400 hover:bg-slate-50 dark:border-slate-700 dark:text-slate-300 dark:hover:bg-slate-800",
                      FOCUS_RING
                    )}
                  >
                    <span
                      className="h-2 w-2 rounded-full"
                      style={{ backgroundColor: getProvinceColor(getDistrictProvince(n), dark) }}
                      aria-hidden
                    />
                    {titleCase(n)}
                  </button>
                </li>
              ))}
            </ul>
          </div>
        )}

        <Link
          href={`/search?q=${encodeURIComponent(searchQuery)}`}
          className={cn(
            "mt-3 flex items-center justify-center gap-2 rounded-lg bg-blue-600 px-3 py-2 text-sm font-medium text-white hover:bg-blue-700",
            FOCUS_RING
          )}
        >
          <Search className="h-4 w-4" aria-hidden />
          Search everything about {selectedDistrict ? name : name.replace(" Province", "")}
        </Link>
      </div>
    </section>
  );
}

function Fact({ label, value, title, className }: { label: string; value: string; title?: string; className?: string }) {
  return (
    <div className={cn("rounded-lg bg-slate-50 px-2.5 py-1.5 dark:bg-slate-800/70", className)} title={title}>
      <dt className="text-[11px] text-slate-500 dark:text-slate-400">{label}</dt>
      <dd className="text-sm font-semibold tabular-nums text-slate-900 dark:text-slate-100">{value}</dd>
    </div>
  );
}
