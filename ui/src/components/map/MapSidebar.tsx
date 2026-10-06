"use client";

import { useMemo, useState } from "react";
import { ChevronRight, House, MapPin, Search, Trash2, X } from "lucide-react";
import { cn } from "@/lib/cn";
import type { GeoTag } from "@/lib/types";
import { PlaceInfoCard } from "@/components/map/PlaceInfoCard";
import { useIsDark } from "@/components/map/useIsDark";
import {
  DISTRICT_ALIASES,
  PROVINCE_NAMES,
  getDistrictProvince,
  getLevelLabel,
  getProvinceColor,
  titleCase,
  type DistrictCollection,
  type MunicipalityCollection,
  type ProvinceCollection,
} from "@/lib/geo";

interface SearchHit {
  type: "province" | "district" | "municipality";
  name: string;
  district?: string;
  level?: string;
  // The short name or older spelling that matched, when the official name didn't.
  alias?: string;
}

const FOCUS_RING = "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500";
const LIST_BUTTON = cn(
  "flex w-full items-center justify-between gap-2 rounded-md px-2 py-2 text-left text-sm text-slate-800 hover:bg-slate-100 dark:text-slate-200 dark:hover:bg-slate-800",
  FOCUS_RING
);

export function MapSidebar({
  provinces,
  districts,
  municipalities,
  selectedProvince,
  onSelectProvince,
  selectedDistrict,
  onSelectDistrict,
  onFocusMunicipality,
  taggingMode,
  onToggleTagging,
  pendingTag,
  onSavePendingTag,
  onCancelPendingTag,
  tags,
  onRemoveTag,
  loading,
  municipalitiesStatus,
  onRetryMunicipalities,
}: {
  provinces: ProvinceCollection | null;
  districts: DistrictCollection | null;
  municipalities: MunicipalityCollection | null;
  selectedProvince: string | null;
  onSelectProvince: (name: string) => void;
  selectedDistrict: string | null;
  onSelectDistrict: (name: string) => void;
  onFocusMunicipality: (name: string, district: string) => void;
  taggingMode: boolean;
  onToggleTagging: () => void;
  pendingTag: { lat: number; lng: number; district?: string } | null;
  onSavePendingTag: (label: string, note: string) => void;
  onCancelPendingTag: () => void;
  tags: GeoTag[];
  onRemoveTag: (id: string) => void;
  loading: boolean;
  municipalitiesStatus: "loading" | "ready" | "error";
  onRetryMunicipalities: () => void;
}) {
  const dark = useIsDark();
  const [query, setQuery] = useState("");
  const [labelDraft, setLabelDraft] = useState("");
  const [noteDraft, setNoteDraft] = useState("");

  const hits = useMemo<SearchHit[]>(() => {
    const q = query.trim().toLowerCase();
    if (!q) return [];

    const provinceHits: SearchHit[] = Object.values(PROVINCE_NAMES)
      .filter((name) => name.toLowerCase().includes(q))
      .map((name) => ({ type: "province", name }));

    const districtHits: SearchHit[] = districts
      ? districts.features.flatMap((f): SearchHit[] => {
          const name = f.properties.DISTRICT;
          if (name.toLowerCase().includes(q)) return [{ type: "district", name }];
          const alias = DISTRICT_ALIASES[name]?.find((a) => a.toLowerCase().includes(q));
          return alias ? [{ type: "district", name, alias }] : [];
        })
      : [];

    const seenMuni = new Set<string>();
    const muniHits: SearchHit[] = municipalities
      ? municipalities.features
          .filter((f) => f.properties.NAME.toLowerCase().includes(q))
          .filter((f) => {
            if (seenMuni.has(f.properties.N_ID)) return false;
            seenMuni.add(f.properties.N_ID);
            return true;
          })
          .slice(0, 20)
          .map((f) => ({
            type: "municipality",
            name: f.properties.NAME,
            district: f.properties.DISTRICT,
            level: f.properties.LEVEL,
          }))
      : [];

    return [...provinceHits, ...districtHits, ...muniHits].slice(0, 25);
  }, [query, districts, municipalities]);

  const districtCountByProvince = useMemo(() => {
    const counts: Record<string, number> = {};
    districts?.features.forEach((f) => {
      const province = getDistrictProvince(f.properties.DISTRICT);
      if (province) counts[province] = (counts[province] ?? 0) + 1;
    });
    return counts;
  }, [districts]);

  const provinceDistricts = useMemo(() => {
    if (!districts || !selectedProvince) return [];
    return districts.features
      .filter((f) => getDistrictProvince(f.properties.DISTRICT) === selectedProvince)
      .map((f) => f.properties.DISTRICT)
      .sort();
  }, [districts, selectedProvince]);

  const districtMunicipalities = useMemo(() => {
    if (!municipalities || !selectedDistrict) return [];
    const seen = new Set<string>();
    return municipalities.features
      .filter((f) => f.properties.DISTRICT.toLowerCase() === selectedDistrict.toLowerCase())
      .filter((f) => {
        if (seen.has(f.properties.N_ID)) return false;
        seen.add(f.properties.N_ID);
        return true;
      })
      .map((f) => f.properties)
      .sort((a, b) => a.NAME.localeCompare(b.NAME));
  }, [municipalities, selectedDistrict]);

  function resetPinDrafts() {
    setLabelDraft("");
    setNoteDraft("");
  }

  const hasQuery = query.trim().length > 0;

  return (
    <aside
      aria-label="Places and pins"
      className="flex min-h-0 w-full flex-1 flex-col overflow-hidden border-t border-slate-200 bg-white dark:border-slate-800 dark:bg-slate-900 sm:order-first sm:h-full sm:w-80 sm:flex-none sm:border-r sm:border-t-0"
    >
      {/* Search */}
      <div className="border-b border-slate-200 p-3 dark:border-slate-800">
        <label htmlFor="place-search" className="mb-1.5 block text-xs font-medium text-slate-600 dark:text-slate-400">
          Find a place
        </label>
        <div className="flex items-center gap-2 rounded-lg border border-slate-300 bg-white px-3 py-2 focus-within:border-blue-500 focus-within:ring-2 focus-within:ring-blue-500/30 dark:border-slate-700 dark:bg-slate-900">
          <Search className="h-4 w-4 shrink-0 text-slate-400" aria-hidden />
          <input
            id="place-search"
            type="search"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="e.g. Kaski, Pokhara, Bagmati"
            autoComplete="off"
            className="w-full bg-transparent text-sm text-slate-900 outline-none placeholder:text-slate-400 dark:text-slate-100 [&::-webkit-search-cancel-button]:hidden"
          />
          {hasQuery && (
            <button
              type="button"
              onClick={() => setQuery("")}
              aria-label="Clear search"
              className={cn("rounded text-slate-400 hover:text-slate-700 dark:hover:text-slate-200", FOCUS_RING)}
            >
              <X className="h-4 w-4" />
            </button>
          )}
        </div>

        {hasQuery && hits.length > 0 && (
          <ul className="mt-2 max-h-64 overflow-y-auto rounded-lg border border-slate-200 dark:border-slate-700">
            {hits.map((hit) => (
              <li key={`${hit.type}-${hit.name}-${hit.district ?? ""}`}>
                <button
                  type="button"
                  onClick={() => {
                    if (hit.type === "province") onSelectProvince(hit.name);
                    else if (hit.type === "district") onSelectDistrict(hit.name);
                    else onFocusMunicipality(hit.name, hit.district!);
                    setQuery("");
                  }}
                  className={cn(
                    "flex w-full items-center justify-between gap-2 px-3 py-2 text-left text-sm hover:bg-slate-50 dark:hover:bg-slate-800",
                    FOCUS_RING
                  )}
                >
                  <span className="truncate text-slate-900 dark:text-slate-100">
                    {hit.type === "district" ? titleCase(hit.name) : hit.name}
                    {hit.alias && (
                      <span className="ml-1 text-slate-500 dark:text-slate-400">({hit.alias})</span>
                    )}
                  </span>
                  <span className="shrink-0 text-xs text-slate-500 dark:text-slate-400">
                    {hit.type === "province"
                      ? "Province"
                      : hit.type === "district"
                        ? "District"
                        : `${getLevelLabel(hit.level)}, ${titleCase(hit.district ?? "")}`}
                  </span>
                </button>
              </li>
            ))}
          </ul>
        )}

        {hasQuery && hits.length === 0 && !loading && (
          <p className="mt-2 rounded-lg bg-slate-50 px-3 py-2 text-xs text-slate-600 dark:bg-slate-800 dark:text-slate-300">
            {municipalitiesStatus === "loading"
              ? `No provinces or districts match “${query.trim()}”. Towns and municipalities are still loading — results will appear in a moment.`
              : `No places match “${query.trim()}”. Check the spelling, or try a district name.`}
          </p>
        )}

        {hasQuery && hits.length > 0 && municipalitiesStatus === "loading" && (
          <p className="mt-1.5 text-xs text-slate-500 dark:text-slate-400">Still loading towns and municipalities…</p>
        )}
      </div>

      {/* Where you are */}
      {!loading && (selectedProvince || selectedDistrict) && (
        <nav
          aria-label="Current location"
          className="flex flex-wrap items-center gap-1 border-b border-slate-200 px-3 py-2 text-xs dark:border-slate-800"
        >
          <button
            type="button"
            onClick={() => onSelectProvince("")}
            className={cn(
              "inline-flex items-center gap-1 rounded px-1 py-0.5 font-medium text-blue-700 hover:underline dark:text-blue-400",
              FOCUS_RING
            )}
          >
            <House className="h-3.5 w-3.5" aria-hidden />
            All of Nepal
          </button>
          {selectedProvince && (
            <>
              <ChevronRight className="h-3 w-3 text-slate-400" aria-hidden />
              {selectedDistrict ? (
                <button
                  type="button"
                  onClick={() => onSelectDistrict("")}
                  className={cn(
                    "rounded px-1 py-0.5 font-medium text-blue-700 hover:underline dark:text-blue-400",
                    FOCUS_RING
                  )}
                >
                  {selectedProvince.replace(" Province", "")}
                </button>
              ) : (
                <span aria-current="location" className="px-1 font-semibold text-slate-900 dark:text-slate-100">
                  {selectedProvince.replace(" Province", "")}
                </span>
              )}
            </>
          )}
          {selectedDistrict && (
            <>
              <ChevronRight className="h-3 w-3 text-slate-400" aria-hidden />
              <span aria-current="location" className="px-1 font-semibold text-slate-900 dark:text-slate-100">
                {titleCase(selectedDistrict)}
              </span>
            </>
          )}
        </nav>
      )}

      <div className="flex-1 overflow-y-auto">
        {/* Unsaved pin — shown first because the user is in the middle of it */}
        {pendingTag && (
          <form
            onSubmit={(e) => {
              e.preventDefault();
              if (!labelDraft.trim()) return;
              onSavePendingTag(labelDraft.trim(), noteDraft.trim());
              resetPinDrafts();
            }}
            className="m-3 rounded-lg border border-amber-300 bg-amber-50 p-3 dark:border-amber-500/40 dark:bg-amber-500/10"
          >
            <p className="text-sm font-semibold text-amber-900 dark:text-amber-100">Name your pin</p>
            <p className="mt-0.5 text-xs text-amber-800 dark:text-amber-200">
              {pendingTag.district
                ? `In ${titleCase(pendingTag.district)} district`
                : `At ${pendingTag.lat.toFixed(3)}, ${pendingTag.lng.toFixed(3)}`}
            </p>
            <label htmlFor="pin-name" className="mt-3 block text-xs font-medium text-slate-700 dark:text-slate-300">
              Name
            </label>
            <input
              id="pin-name"
              autoFocus
              required
              value={labelDraft}
              onChange={(e) => setLabelDraft(e.target.value)}
              placeholder="e.g. Community library"
              className="mt-1 w-full rounded-md border border-slate-300 bg-white px-2.5 py-1.5 text-sm outline-none focus:border-blue-500 focus:ring-2 focus:ring-blue-500/30 dark:border-slate-600 dark:bg-slate-900"
            />
            <label htmlFor="pin-note" className="mt-2 block text-xs font-medium text-slate-700 dark:text-slate-300">
              Note <span className="font-normal text-slate-500">(optional)</span>
            </label>
            <textarea
              id="pin-note"
              value={noteDraft}
              onChange={(e) => setNoteDraft(e.target.value)}
              rows={2}
              className="mt-1 w-full resize-none rounded-md border border-slate-300 bg-white px-2.5 py-1.5 text-sm outline-none focus:border-blue-500 focus:ring-2 focus:ring-blue-500/30 dark:border-slate-600 dark:bg-slate-900"
            />
            <div className="mt-3 flex gap-2">
              <button
                type="submit"
                disabled={!labelDraft.trim()}
                className={cn(
                  "flex-1 rounded-md bg-blue-600 py-1.5 text-sm font-medium text-white hover:bg-blue-700 disabled:cursor-not-allowed disabled:opacity-40",
                  FOCUS_RING
                )}
              >
                Save pin
              </button>
              <button
                type="button"
                onClick={() => {
                  onCancelPendingTag();
                  resetPinDrafts();
                }}
                className={cn(
                  "rounded-md border border-slate-300 bg-white px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:bg-transparent dark:text-slate-200 dark:hover:bg-slate-800",
                  FOCUS_RING
                )}
              >
                Discard
              </button>
            </div>
          </form>
        )}

        {!loading && (
          <PlaceInfoCard
            provinces={provinces}
            districts={districts}
            municipalities={municipalities}
            municipalitiesStatus={municipalitiesStatus}
            selectedProvince={selectedProvince}
            selectedDistrict={selectedDistrict}
            onSelectDistrict={onSelectDistrict}
          />
        )}

        <div className="p-3">
          {loading && (
            <div className="space-y-2" aria-label="Loading places">
              {Array.from({ length: 7 }).map((_, i) => (
                <div key={i} className="h-8 animate-pulse rounded-md bg-slate-100 dark:bg-slate-800" />
              ))}
            </div>
          )}

          {!loading && !selectedProvince && (
            <section>
              <h2 className="text-sm font-semibold text-slate-900 dark:text-slate-100">Browse by province</h2>
              <p className="mb-2 mt-0.5 text-xs text-slate-500 dark:text-slate-400">
                Or click any district on the map.
              </p>
              <ul className="space-y-0.5">
                {Object.values(PROVINCE_NAMES).map((name) => (
                  <li key={name}>
                    <button type="button" onClick={() => onSelectProvince(name)} className={LIST_BUTTON}>
                      <span className="flex items-center gap-2">
                        <span
                          className="h-3 w-3 shrink-0 rounded-full"
                          style={{ backgroundColor: getProvinceColor(name, dark) }}
                          aria-hidden
                        />
                        {name}
                      </span>
                      <span className="flex items-center gap-1 text-xs text-slate-500 dark:text-slate-400">
                        {districtCountByProvince[name] ?? 0} districts
                        <ChevronRight className="h-3.5 w-3.5" aria-hidden />
                      </span>
                    </button>
                  </li>
                ))}
              </ul>
            </section>
          )}

          {!loading && selectedProvince && !selectedDistrict && (
            <section>
              <h2 className="text-sm font-semibold text-slate-900 dark:text-slate-100">
                Districts in {selectedProvince.replace(" Province", "")}
                <span className="ml-1 font-normal text-slate-500">({provinceDistricts.length})</span>
              </h2>
              <p className="mb-2 mt-0.5 text-xs text-slate-500 dark:text-slate-400">
                Pick one to zoom in and see its local governments.
              </p>
              <ul className="space-y-0.5">
                {provinceDistricts.map((name) => (
                  <li key={name}>
                    <button type="button" onClick={() => onSelectDistrict(name)} className={LIST_BUTTON}>
                      {titleCase(name)}
                      <ChevronRight className="h-3.5 w-3.5 text-slate-400" aria-hidden />
                    </button>
                  </li>
                ))}
              </ul>
            </section>
          )}

          {!loading && selectedDistrict && (
            <section>
              <h2 className="text-sm font-semibold text-slate-900 dark:text-slate-100">
                Local governments in {titleCase(selectedDistrict)}
                {municipalitiesStatus === "ready" && (
                  <span className="ml-1 font-normal text-slate-500">({districtMunicipalities.length})</span>
                )}
              </h2>
              <p className="mb-2 mt-0.5 text-xs text-slate-500 dark:text-slate-400">
                Cities, municipalities and rural municipalities. Pick one to zoom to it.
              </p>
              {municipalitiesStatus === "loading" && (
                <div className="space-y-2" aria-label="Loading local governments">
                  {Array.from({ length: 5 }).map((_, i) => (
                    <div key={i} className="h-8 animate-pulse rounded-md bg-slate-100 dark:bg-slate-800" />
                  ))}
                </div>
              )}
              {municipalitiesStatus === "error" && (
                <div className="rounded-lg bg-rose-50 px-3 py-2 text-xs text-rose-800 dark:bg-rose-500/10 dark:text-rose-200">
                  Couldn&rsquo;t load the local governments.{" "}
                  <button
                    type="button"
                    onClick={onRetryMunicipalities}
                    className={cn("font-medium underline", FOCUS_RING)}
                  >
                    Try again
                  </button>
                </div>
              )}
              <ul className="space-y-0.5">
                {districtMunicipalities.map((m) => (
                  <li key={m.N_ID}>
                    <button
                      type="button"
                      onClick={() => onFocusMunicipality(m.NAME, m.DISTRICT)}
                      className={LIST_BUTTON}
                    >
                      <span className="flex min-w-0 items-center gap-1.5">
                        <MapPin className="h-3.5 w-3.5 shrink-0 text-emerald-600" aria-hidden />
                        <span className="truncate">{m.NAME}</span>
                      </span>
                      <span className="shrink-0 text-xs text-slate-500 dark:text-slate-400" title={m.LEVEL}>
                        {getLevelLabel(m.LEVEL)}
                      </span>
                    </button>
                  </li>
                ))}
              </ul>
            </section>
          )}
        </div>

        {/* Pins */}
        {!loading && (
          <section className="border-t border-slate-200 p-3 dark:border-slate-800">
            <div className="flex items-center justify-between gap-2">
              <h2 className="text-sm font-semibold text-slate-900 dark:text-slate-100">
                My pins <span className="font-normal text-slate-500">({tags.length})</span>
              </h2>
              <button
                type="button"
                onClick={onToggleTagging}
                aria-pressed={taggingMode}
                className={cn(
                  "inline-flex items-center gap-1.5 rounded-md border px-2.5 py-1 text-xs font-medium transition-colors",
                  FOCUS_RING,
                  taggingMode
                    ? "border-amber-400 bg-amber-100 text-amber-900 dark:border-amber-500/50 dark:bg-amber-500/20 dark:text-amber-100"
                    : "border-slate-300 bg-white text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:bg-transparent dark:text-slate-200 dark:hover:bg-slate-800"
                )}
              >
                <MapPin className="h-3.5 w-3.5" aria-hidden />
                {taggingMode ? "Stop pinning" : "Drop a pin"}
              </button>
            </div>

            {tags.length === 0 ? (
              <p className="mt-2 text-xs leading-relaxed text-slate-500 dark:text-slate-400">
                Mark places you care about. Press &ldquo;Drop a pin&rdquo;, then click the map. Pins are saved on
                this device only.
              </p>
            ) : (
              <ul className="mt-2 space-y-1.5">
                {tags.map((tag) => (
                  <li
                    key={tag.id}
                    className="flex items-start justify-between gap-2 rounded-md border border-slate-200 px-2.5 py-2 dark:border-slate-700"
                  >
                    <div className="min-w-0">
                      <p className="truncate text-sm font-medium text-slate-900 dark:text-slate-100">{tag.label}</p>
                      <p className="truncate text-xs text-slate-500 dark:text-slate-400">
                        {[tag.district ? titleCase(tag.district) : null, tag.note].filter(Boolean).join(" · ") ||
                          "No note"}
                      </p>
                    </div>
                    <button
                      type="button"
                      onClick={() => onRemoveTag(tag.id)}
                      aria-label={`Delete pin ${tag.label}`}
                      title="Delete pin"
                      className={cn("shrink-0 rounded p-0.5 text-slate-400 hover:text-rose-600", FOCUS_RING)}
                    >
                      <Trash2 className="h-4 w-4" />
                    </button>
                  </li>
                ))}
              </ul>
            )}
          </section>
        )}
      </div>
    </aside>
  );
}
