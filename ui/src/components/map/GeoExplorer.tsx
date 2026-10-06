"use client";

import dynamic from "next/dynamic";
import { useSearchParams } from "next/navigation";
import { useEffect, useMemo, useRef, useState } from "react";
import "leaflet/dist/leaflet.css";
import { Info, MapPin, X } from "lucide-react";
import { MapSidebar } from "@/components/map/MapSidebar";
import type { FocusRequest } from "@/components/map/NepalMap";
import type { DistrictCollection, MunicipalityCollection, ProvinceCollection } from "@/lib/geo";
import {
  DISTRICT_ALIASES,
  PROVINCE_NAMES,
  getDistrictProvince,
  loadStoredTags,
  saveStoredTags,
  titleCase,
} from "@/lib/geo";
import { cn } from "@/lib/cn";
import type { GeoTag } from "@/lib/types";

const NepalMap = dynamic(() => import("@/components/map/NepalMap").then((m) => m.NepalMap), {
  ssr: false,
  loading: () => (
    <div className="flex h-full w-full items-center justify-center text-sm text-slate-500">
      Loading map…
    </div>
  ),
});

export interface PendingTag {
  lat: number;
  lng: number;
  district?: string;
}

function urlKey(focus: string, district: string): string {
  return `${focus.toLowerCase()}|${district.toLowerCase()}`;
}

function shortProvinceName(province: string): string {
  return province.replace(" Province", "");
}

export function GeoExplorer() {
  const [provinces, setProvinces] = useState<ProvinceCollection | null>(null);
  const [districts, setDistricts] = useState<DistrictCollection | null>(null);
  const [municipalities, setMunicipalities] = useState<MunicipalityCollection | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState(false);
  const [loadAttempt, setLoadAttempt] = useState(0);
  const [municipalitiesStatus, setMunicipalitiesStatus] = useState<"loading" | "ready" | "error">("loading");

  const [selectedProvince, setSelectedProvince] = useState<string | null>(null);
  const [selectedDistrict, setSelectedDistrict] = useState<string | null>(null);
  const [focusRequest, setFocusRequest] = useState<FocusRequest | null>(null);
  const focusSeq = useRef(0);

  const [taggingMode, setTaggingMode] = useState(false);
  const [pendingTag, setPendingTag] = useState<PendingTag | null>(null);
  const [tags, setTags] = useState<GeoTag[]>([]);

  useEffect(() => {
    // Reads localStorage, which isn't available during SSR.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setTags(loadStoredTags());
  }, []);

  useEffect(() => {
    let cancelled = false;
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setLoading(true);
    setLoadError(false);
    setMunicipalitiesStatus("loading");
    function getJson(url: string) {
      return fetch(url).then((r) => {
        if (!r.ok) throw new Error(`${url}: ${r.status}`);
        return r.json();
      });
    }

    // The map only needs provinces + districts to appear. Municipalities are the
    // largest file and only matter once someone picks a district or searches for
    // a town, so they load afterwards instead of holding up the first view.
    Promise.all([getJson("/data/nepal-provinces.geojson"), getJson("/data/nepal-districts.geojson")])
      .then(([p, d]: [ProvinceCollection, DistrictCollection]) => {
        if (cancelled) return;
        setProvinces(p);
        setDistricts(d);
        setLoading(false);

        getJson("/data/nepal-municipalities.geojson")
          .then((m: MunicipalityCollection) => {
            if (cancelled) return;
            // The source dataset has a handful of malformed entries (null name/id) — drop them.
            setMunicipalities({
              ...m,
              features: m.features.filter((f) => f.properties.NAME && f.properties.N_ID),
            });
            setMunicipalitiesStatus("ready");
          })
          .catch(() => {
            if (!cancelled) setMunicipalitiesStatus("error");
          });
      })
      .catch(() => {
        if (cancelled) return;
        setLoadError(true);
        setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [loadAttempt]);

  // Esc backs out of pin mode (or just the unsaved pin) — the standard "cancel" key.
  useEffect(() => {
    if (!taggingMode && !pendingTag) return;
    function onKeyDown(e: KeyboardEvent) {
      if (e.key !== "Escape") return;
      setPendingTag(null);
      setTaggingMode(false);
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [taggingMode, pendingTag]);

  // The address bar mirrors the selection: ?focus=Kaski, ?focus=Gandaki, or
  // ?focus=Madi&district=Kaski for a municipality (26 names exist in more than
  // one district). That makes every view linkable and lets the browser's Back
  // button step back through selections.
  const searchParams = useSearchParams();
  const focusParam = searchParams.get("focus")?.trim() ?? "";
  const districtParam = searchParams.get("district")?.trim() ?? "";
  // The URL key the current selection corresponds to. Set when we write the
  // URL ourselves, so the resulting searchParams change isn't re-applied.
  const urlKeyRef = useRef<string | null>(null);
  // True while applying a URL (initial link or Back): rewriting the URL then
  // (e.g. normalising "kaski" to "Kaski") must replace, not push, the entry.
  const applyingUrlRef = useRef(false);

  function writeUrl(focus: string | null, district?: string) {
    const params = new URLSearchParams(window.location.search);
    params.delete("focus");
    params.delete("district");
    if (focus) params.set("focus", focus);
    if (focus && district) params.set("district", district);
    const query = params.toString();
    const next = `${window.location.pathname}${query ? `?${query}` : ""}`;
    urlKeyRef.current = urlKey(focus ?? "", district ?? "");
    if (next === window.location.pathname + window.location.search) return;
    if (applyingUrlRef.current) window.history.replaceState(null, "", next);
    else window.history.pushState(null, "", next);
  }

  // Apply the URL: on first load (links from search results, which may name a
  // district, a province, or a city like "Pokhara") and on Back/Forward. If
  // nothing matches, say so instead of selecting something that doesn't exist.
  const [focusNotFound, setFocusNotFound] = useState<string | null>(null);
  useEffect(() => {
    if (!districts) return;
    const key = urlKey(focusParam, districtParam);
    if (urlKeyRef.current === key) return;

    applyingUrlRef.current = true;
    try {
      if (!focusParam) {
        urlKeyRef.current = key;
        if (selectedProvince || selectedDistrict) handleSelectProvince("");
        return;
      }
      const lower = focusParam.toLowerCase();
      const matchesDistrict = (name: string, text: string) =>
        name.toLowerCase() === text || DISTRICT_ALIASES[name]?.some((alias) => alias.toLowerCase() === text);

      if (!districtParam) {
        const province = PROVINCE_LIST.find(
          (p) => p.toLowerCase() === lower || shortProvinceName(p).toLowerCase() === lower
        );
        if (province) {
          urlKeyRef.current = key;
          handleSelectProvince(province);
          return;
        }
        const district = districts.features.map((f) => f.properties.DISTRICT).find((n) => matchesDistrict(n, lower));
        if (district) {
          urlKeyRef.current = key;
          handleSelectDistrict(district);
          return;
        }
      }

      // A municipality: wait for them to load (they arrive in the background).
      if (municipalitiesStatus === "loading") return;
      urlKeyRef.current = key;
      const districtLower = districtParam.toLowerCase();
      const candidates = (municipalities?.features ?? []).filter(
        (f) => !districtParam || matchesDistrict(f.properties.DISTRICT, districtLower)
      );
      const municipality =
        candidates.find((f) => f.properties.NAME.toLowerCase() === lower) ??
        candidates.find((f) => f.properties.NAME.toLowerCase().startsWith(lower));
      if (municipality) {
        handleFocusMunicipality(municipality.properties.NAME, municipality.properties.DISTRICT);
      } else {
        setFocusNotFound(focusParam);
      }
    } finally {
      applyingUrlRef.current = false;
    }
    // The handlers close over state that changes on every selection; urlKeyRef
    // keeps this to one run per distinct URL.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [focusParam, districtParam, districts, municipalities, municipalitiesStatus]);

  function handleSelectProvince(name: string) {
    setFocusNotFound(null);
    writeUrl(name ? shortProvinceName(name) : null);
    if (!name) {
      setSelectedProvince(null);
      setSelectedDistrict(null);
      focusSeq.current += 1;
      setFocusRequest({ seq: focusSeq.current, kind: "country", name: "" });
      return;
    }
    setSelectedProvince(name);
    setSelectedDistrict(null);
    focusSeq.current += 1;
    setFocusRequest({ seq: focusSeq.current, kind: "province", name });
  }

  function handleSelectDistrict(name: string) {
    setFocusNotFound(null);
    if (!name) {
      writeUrl(selectedProvince ? shortProvinceName(selectedProvince) : null);
      setSelectedDistrict(null);
      if (selectedProvince) {
        focusSeq.current += 1;
        setFocusRequest({ seq: focusSeq.current, kind: "province", name: selectedProvince });
      } else {
        focusSeq.current += 1;
        setFocusRequest({ seq: focusSeq.current, kind: "country", name: "" });
      }
      return;
    }
    writeUrl(titleCase(name));
    const province = getDistrictProvince(name);
    if (province) setSelectedProvince(province);
    setSelectedDistrict(name);
    focusSeq.current += 1;
    setFocusRequest({ seq: focusSeq.current, kind: "district", name });
  }

  function handleFocusMunicipality(name: string, district: string) {
    setFocusNotFound(null);
    writeUrl(name, titleCase(district));
    const province = getDistrictProvince(district);
    if (province) setSelectedProvince(province);
    setSelectedDistrict(district);
    focusSeq.current += 1;
    setFocusRequest({ seq: focusSeq.current, kind: "municipality", name, district });
  }

  // The browser tab names the place you're looking at. (The server sets the
  // first title; selections after that change the URL without a reload.)
  useEffect(() => {
    let place: string | null = null;
    if (focusParam && districtParam) place = `${focusParam}, ${districtParam}`;
    else if (selectedDistrict) place = `${titleCase(selectedDistrict)} district`;
    else if (selectedProvince) place = selectedProvince;
    document.title = place ? `${place} — Map of Nepal · PGS Search` : "Map of Nepal — PGS Search";
  }, [focusParam, districtParam, selectedDistrict, selectedProvince]);

  function handleMapClick(lat: number, lng: number, district?: string) {
    if (!taggingMode) return;
    setPendingTag({ lat, lng, district });
  }

  function handleSavePendingTag(label: string, note: string) {
    if (!pendingTag) return;
    const tag: GeoTag = {
      id: `tag-${Date.now()}`,
      label,
      note,
      lat: pendingTag.lat,
      lng: pendingTag.lng,
      district: pendingTag.district,
      createdAt: new Date().toISOString(),
    };
    const next = [tag, ...tags];
    setTags(next);
    saveStoredTags(next);
    setPendingTag(null);
    setTaggingMode(false);
  }

  function handleRemoveTag(id: string) {
    const next = tags.filter((t) => t.id !== id);
    setTags(next);
    saveStoredTags(next);
  }

  const mapProps = useMemo(
    () => ({
      provinces,
      districts,
      municipalities,
      selectedProvince,
      onSelectProvince: handleSelectProvince,
      selectedDistrict,
      onSelectDistrict: handleSelectDistrict,
      focusRequest,
      taggingMode,
      onMapClick: handleMapClick,
      pendingTag,
      tags,
      onRemoveTag: handleRemoveTag,
    }),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [provinces, districts, municipalities, selectedProvince, selectedDistrict, focusRequest, taggingMode, pendingTag, tags]
  );

  function cancelTagging() {
    setTaggingMode(false);
    setPendingTag(null);
  }

  return (
    // Phones: map on top (it's the point of the page), lists scroll underneath.
    // Wider screens: lists in a left sidebar, map fills the rest.
    <div className="flex h-full w-full flex-col sm:flex-row">
      <div className="flex h-[60vh] shrink-0 flex-col bg-slate-50 dark:bg-slate-950 sm:h-auto sm:min-w-0 sm:flex-1 sm:shrink">
        <MapContextBar
          selectedProvince={selectedProvince}
          selectedDistrict={selectedDistrict}
          taggingMode={taggingMode}
          pendingTag={pendingTag}
          loadError={loadError}
          onRetry={() => setLoadAttempt((n) => n + 1)}
          focusNotFound={focusNotFound}
          onDismissFocusNotFound={() => setFocusNotFound(null)}
          onSelectProvince={handleSelectProvince}
          onCancelTagging={cancelTagging}
        />
        <div className="relative min-h-0 flex-1">
          {!loadError && <NepalMap {...mapProps} />}
        </div>
      </div>
      <MapSidebar
        provinces={provinces}
        districts={districts}
        municipalities={municipalities}
        selectedProvince={selectedProvince}
        onSelectProvince={handleSelectProvince}
        selectedDistrict={selectedDistrict}
        onSelectDistrict={handleSelectDistrict}
        onFocusMunicipality={handleFocusMunicipality}
        taggingMode={taggingMode}
        onToggleTagging={() => {
          setTaggingMode((v) => !v);
          setPendingTag(null);
        }}
        pendingTag={pendingTag}
        onSavePendingTag={handleSavePendingTag}
        onCancelPendingTag={() => setPendingTag(null)}
        tags={tags}
        onRemoveTag={handleRemoveTag}
        loading={loading}
        municipalitiesStatus={municipalitiesStatus}
        onRetryMunicipalities={() => setLoadAttempt((n) => n + 1)}
      />
    </div>
  );
}

const PROVINCE_LIST = Object.values(PROVINCE_NAMES);

// One line telling people where they are and what they can do next (the color
// key, which doubles as a province filter, is in the map's corner). This is
// the page's main piece of guidance, so it changes with every state.
function MapContextBar({
  selectedProvince,
  selectedDistrict,
  taggingMode,
  pendingTag,
  loadError,
  onRetry,
  focusNotFound,
  onDismissFocusNotFound,
  onSelectProvince,
  onCancelTagging,
}: {
  selectedProvince: string | null;
  selectedDistrict: string | null;
  taggingMode: boolean;
  pendingTag: PendingTag | null;
  loadError: boolean;
  onRetry: () => void;
  focusNotFound: string | null;
  onDismissFocusNotFound: () => void;
  onSelectProvince: (name: string) => void;
  onCancelTagging: () => void;
}) {
  let tone: "info" | "pin" | "error" = "info";
  let title: string;
  let hint: React.ReactNode;
  let action: React.ReactNode = null;

  const clearButton = (
    <button
      type="button"
      onClick={() => onSelectProvince("")}
      className="inline-flex shrink-0 items-center gap-1 rounded-md border border-slate-300 bg-white px-2.5 py-1 text-xs font-medium text-slate-700 hover:bg-slate-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200 dark:hover:bg-slate-700"
    >
      <X className="h-3.5 w-3.5" />
      Show all of Nepal
    </button>
  );

  if (loadError) {
    tone = "error";
    title = "We couldn't load the map";
    hint = "Check your internet connection and try again.";
    action = (
      <button
        type="button"
        onClick={onRetry}
        className="shrink-0 rounded-md bg-rose-600 px-3 py-1 text-xs font-medium text-white hover:bg-rose-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-rose-400"
      >
        Try again
      </button>
    );
  } else if (taggingMode) {
    tone = "pin";
    if (pendingTag) {
      title = pendingTag.district ? `Pin placed in ${titleCase(pendingTag.district)}` : "Pin placed";
      hint = (
        <>
          Give it a name <span className="sm:hidden">below</span>
          <span className="hidden sm:inline">in the panel on the left</span> to save it, or click somewhere else to
          move it.
        </>
      );
    } else {
      title = "Click anywhere in Nepal to drop a pin";
      hint = "Mark a place you want to remember. Press Esc to cancel.";
    }
    action = (
      <button
        type="button"
        onClick={onCancelTagging}
        className="shrink-0 rounded-md border border-amber-300 bg-white px-2.5 py-1 text-xs font-medium text-amber-800 hover:bg-amber-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-amber-500 dark:border-amber-500/40 dark:bg-transparent dark:text-amber-200 dark:hover:bg-amber-500/10"
      >
        Cancel
      </button>
    );
  } else if (focusNotFound) {
    title = `We couldn’t find “${focusNotFound}” on the map`;
    hint = (
      <>
        Try a district or municipality name in &ldquo;Find a place&rdquo;
        <span className="sm:hidden"> below</span>
        <span className="hidden sm:inline"> on the left</span>, or click any district to read its local news.
      </>
    );
    action = (
      <button
        type="button"
        onClick={onDismissFocusNotFound}
        aria-label="Dismiss"
        className="shrink-0 rounded-md p-1 hover:bg-blue-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 dark:hover:bg-blue-500/20"
      >
        <X className="h-4 w-4" />
      </button>
    );
  } else if (selectedDistrict) {
    title = `${titleCase(selectedDistrict)} district`;
    hint = (
      <>
        In {getDistrictProvince(selectedDistrict) ?? selectedProvince}. Click any area inside it for local news, or pick
        one from the list.
      </>
    );
    action = clearButton;
  } else if (selectedProvince) {
    title = selectedProvince;
    hint = "Click a district to see its latest local news.";
    action = clearButton;
  } else {
    title = "Explore Nepal by district";
    hint = "Click any district to read its latest local news. Zoom in with + / − or your mouse wheel.";
  }

  return (
    <div className="shrink-0 space-y-2 border-b border-slate-200 bg-white px-3 py-2.5 dark:border-slate-800 dark:bg-slate-900 sm:px-4">
      <div
        role="status"
        aria-live="polite"
        className={cn(
          "flex items-start gap-2.5 rounded-lg px-3 py-2",
          tone === "info" && "bg-blue-50 text-blue-900 dark:bg-blue-500/10 dark:text-blue-100",
          tone === "pin" && "bg-amber-50 text-amber-900 dark:bg-amber-500/10 dark:text-amber-100",
          tone === "error" && "bg-rose-50 text-rose-900 dark:bg-rose-500/10 dark:text-rose-100"
        )}
      >
        {tone === "pin" ? (
          <MapPin className="mt-0.5 h-4 w-4 shrink-0" />
        ) : (
          <Info className="mt-0.5 h-4 w-4 shrink-0" />
        )}
        <div className="min-w-0 flex-1">
          <p className="text-sm font-semibold leading-snug">{title}</p>
          <p className="mt-0.5 text-xs leading-relaxed opacity-90">{hint}</p>
        </div>
        {action}
      </div>
    </div>
  );
}
