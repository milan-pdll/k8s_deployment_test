"use client";

import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { MapContainer, GeoJSON, Marker, Popup, useMap, useMapEvents } from "react-leaflet";
import L, { type Layer, type LeafletMouseEvent, type PathOptions } from "leaflet";
import type { Feature } from "geojson";
import { House, Minus, MousePointerClick, Plus } from "lucide-react";
import {
  getDistrictProvince,
  getLevelLabel,
  getProvinceColor,
  getProvinceName,
  titleCase,
  type DistrictFeature,
  type DistrictCollection,
  type MunicipalityCollection,
  type MunicipalityFeature,
  type ProvinceCollection,
  type ProvinceFeature,
} from "@/lib/geo";
import type { GeoTag } from "@/lib/types";
import { NewsPopupContent } from "@/components/map/NewsPopupContent";
import { MapLegend } from "@/components/map/MapLegend";
import { useIsDark } from "@/components/map/useIsDark";
import { MapBackgroundPicker, loadMapBackground, type MapBackground } from "@/components/map/MapBackgroundPicker";

const TAG_ICON = L.divIcon({
  className: "",
  html: `<div style="width:16px;height:16px;border-radius:9999px;background:#2563eb;border:2px solid white;box-shadow:0 0 0 2px #2563eb55"></div>`,
  iconSize: [16, 16],
  iconAnchor: [8, 8],
});

// Short map labels for districts too small to fit their full name. Only the
// label on the map is shortened; the hover card and sidebar show the full name.
const MAP_LABEL_ABBREVIATIONS: Record<string, string> = {
  KATHMANDU: "Ktm",
  LALITPUR: "Lal",
  BHAKTAPUR: "Bkt",
  KAVREPALANCHOK: "Kavre",
};

// The spot the user just clicked in pin mode, before they've named it.
const PENDING_ICON = L.divIcon({
  className: "",
  html: `<div style="width:18px;height:18px;border-radius:9999px;background:#f59e0b;border:3px solid white;box-shadow:0 0 0 3px #f59e0b66"></div>`,
  iconSize: [18, 18],
  iconAnchor: [9, 9],
});

// Nepal's approximate extent, used to fit the initial view. There's no tile
// basemap to worry about bleeding into India/China anymore (the GeoJSON is
// drawn on a blank canvas), so the view is free to zoom out as far as it
// needs to fit Nepal's full ~2:1 width. That full-country fit is also the
// zoom-out limit — past it there's only blank canvas.
const NEPAL_BOUNDS = L.latLngBounds([26.3, 80.0], [30.5, 88.3]);

// How far past Nepal the map may be panned. Wide enough that a district on
// the border (Darchula, Humla, Taplejung) can still be centred when zoomed in.
// Padded equally in projected (Mercator) space, so its centre is exactly the
// full-country view's centre: when the panel is bigger than these bounds,
// Leaflet centres on them, and any other centre would make the map drift.
const NEPAL_MAX_BOUNDS = padProjected(NEPAL_BOUNDS, 160_000);

function padProjected(bounds: L.LatLngBounds, metres: number): L.LatLngBounds {
  const crs = L.CRS.EPSG3857;
  const sw = crs.project(bounds.getSouthWest());
  const ne = crs.project(bounds.getNorthEast());
  return L.latLngBounds(
    crs.unproject(L.point(sw.x - metres, sw.y - metres)),
    crs.unproject(L.point(ne.x + metres, ne.y + metres))
  );
}

// Fly so that `bounds` fills the map (minus padding), landing on a view that
// lies inside NEPAL_MAX_BOUNDS, so a border district isn't left looking at
// empty space beyond the edge.
function flyToFit(map: L.Map, bounds: L.LatLngBounds, padding: number, duration: number) {
  const zoom = Math.min(map.getBoundsZoom(bounds, false, L.point(padding * 2, padding * 2)), map.getMaxZoom());
  const center = map.project(bounds.getSouthWest(), zoom).add(map.project(bounds.getNorthEast(), zoom)).divideBy(2);
  const limit = L.bounds(map.project(NEPAL_MAX_BOUNDS.getNorthWest(), zoom), map.project(NEPAL_MAX_BOUNDS.getSouthEast(), zoom));
  const half = map.getSize().divideBy(2);
  const clampAxis = (value: number, min: number, max: number, halfView: number) =>
    max - min <= halfView * 2 ? (min + max) / 2 : Math.min(Math.max(value, min + halfView), max - halfView);
  const target = L.point(
    clampAxis(center.x, limit.min!.x, limit.max!.x, half.x),
    clampAxis(center.y, limit.min!.y, limit.max!.y, half.y)
  );
  map.flyTo(map.unproject(target, zoom), zoom, { duration });
}
// Floor low enough that fitBounds can always show the whole country regardless
// of panel aspect ratio; ceiling generous enough for the municipality-level
// flyTo when focusing a search result.
const MIN_ZOOM = 5;
const MAX_ZOOM = 13;
// Breathing room around the country at the full view. Every "show all of
// Nepal" fit uses the same padding, so it lands exactly on the minimum zoom.
const FIT_PADDING: L.PointExpression = [20, 20];

export interface FocusRequest {
  seq: number;
  kind: "country" | "province" | "district" | "municipality";
  name: string;
  district?: string;
}

function BoundsController({ keepViewRef }: { keepViewRef: React.RefObject<boolean> }) {
  const map = useMap();

  useEffect(() => {
    let fittedSize: L.Point | null = null;

    function apply() {
      // While a place is selected, keep looking at it. Selecting one can itself
      // resize the panel (a longer hint above the map wraps onto a second line,
      // e.g. for Darchula), and refitting the country then would cancel the
      // flight to the place halfway. Only the zoom-out limit follows the size.
      if (fittedSize && keepViewRef.current) {
        map.invalidateSize({ pan: false });
        const size = map.getSize();
        if (size.equals(fittedSize) || size.x === 0 || size.y === 0) return;
        fittedSize = size;
        const fitZoom = map.getBoundsZoom(NEPAL_BOUNDS, false, L.point(FIT_PADDING).multiplyBy(2));
        map.setMinZoom(Math.min(fitZoom, map.getZoom()));
        return;
      }

      // Inside a flex layout the container can report a 0/stale, pre-layout size
      // on early ticks; invalidateSize() forces Leaflet to re-measure before we
      // fit. Re-fitting on every resize (sidebar toggle, window resize) keeps the
      // full country filling the panel instead of freezing at whatever size the
      // container happened to be first; it does reset any zoom the user applied.
      map.invalidateSize();

      const size = map.getSize();
      // ResizeObserver also fires once right after observe() with the size we
      // just fitted to. Refitting then would cancel a focus flyTo that started
      // on page load (e.g. /map?focus=Pokhara), so only refit on a real change.
      if (fittedSize && size.equals(fittedSize)) return;
      if (size.x > 0 && size.y > 0) {
        fittedSize = size;
        // Drop the floor first: a smaller panel needs a lower fit zoom than the
        // previous floor, and fitBounds would otherwise clamp to it and clip.
        map.setMinZoom(MIN_ZOOM);
        map.fitBounds(NEPAL_BOUNDS, { animate: false, padding: FIT_PADDING });
        map.setMinZoom(map.getZoom());
      }
    }

    apply();
    const container = map.getContainer();
    const observer = new ResizeObserver(apply);
    observer.observe(container);
    return () => observer.disconnect();
  }, [map, keepViewRef]);

  return null;
}

// At the full-country view there's nothing to pan to, so dragging only turns on
// once the user has zoomed in (via buttons, wheel/pinch, or a focus flyTo).
// Zooming back out from an off-center spot would otherwise land at the min zoom
// slightly shifted, clipping one edge of the country — so recenter there.
function PanWhenZoomed() {
  const map = useMap();

  // Dragging stops at NEPAL_MAX_BOUNDS (Leaflet's drag handler reads these
  // options). They're set here rather than as MapContainer props on purpose:
  // the maxBounds prop also re-centres the map after every move, and that
  // cancelled flights to border districts and fought the news popup's own
  // panning (the map shook back and forth on Darchula). Flights land inside
  // the bounds by themselves (flyToFit).
  useEffect(() => {
    L.Util.setOptions(map, { maxBounds: NEPAL_MAX_BOUNDS, maxBoundsViscosity: 1 });
  }, [map]);

  useMapEvents({
    zoomend() {
      if (map.getZoom() > map.getMinZoom() + 0.01) {
        map.dragging.enable();
        return;
      }
      map.dragging.disable();
      // Popups belong to the zoomed-in context and won't fit the full-country
      // box without clipping.
      map.closePopup();
      // fitBounds centers on the projected (Mercator) midpoint, not the lat/lng
      // one, so compare in pixel space to match the initial full-country view.
      const zoom = map.getZoom();
      const target = map
        .project(NEPAL_BOUNDS.getSouthWest(), zoom)
        .add(map.project(NEPAL_BOUNDS.getNorthEast(), zoom))
        .divideBy(2);
      if (map.project(map.getCenter(), zoom).distanceTo(target) > 1) {
        map.panTo(map.unproject(target, zoom));
      }
    },
  });
  return null;
}

// Rendered as a sibling overlay rather than inside the Leaflet container: clicks
// then never reach Leaflet's own handlers (district select, tagging), and we
// avoid L.DomEvent.disableClickPropagation, which would also swallow the native
// event before React's root listener sees it.
function ZoomControls({ map }: { map: L.Map }) {
  const [zoomState, setZoomState] = useState(() => ({
    zoom: map.getZoom(),
    min: map.getMinZoom(),
    max: map.getMaxZoom(),
  }));

  useEffect(() => {
    // The min zoom moves whenever the panel resizes (see BoundsController), so
    // re-read it alongside the zoom itself.
    function sync() {
      setZoomState({ zoom: map.getZoom(), min: map.getMinZoom(), max: map.getMaxZoom() });
    }
    sync();
    map.on("zoomend zoomlevelschange resize", sync);
    return () => {
      map.off("zoomend zoomlevelschange resize", sync);
    };
  }, [map]);

  const atMin = zoomState.zoom <= zoomState.min + 0.01;
  const atMax = zoomState.zoom >= zoomState.max - 0.01;
  const buttonClass =
    "flex h-8 w-8 items-center justify-center text-slate-700 transition-colors hover:bg-slate-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-blue-500 disabled:cursor-not-allowed disabled:text-slate-300 disabled:hover:bg-transparent dark:text-slate-200 dark:hover:bg-slate-800 dark:disabled:text-slate-600";

  return (
    <div data-map-overlay className="absolute right-3 top-3 z-[1000] flex flex-col divide-y divide-slate-200 overflow-hidden rounded-lg border border-slate-200 bg-white shadow-sm dark:divide-slate-700 dark:border-slate-700 dark:bg-slate-900">
      <button type="button" aria-label="Zoom in" title="Zoom in" disabled={atMax} onClick={() => map.zoomIn(1)} className={buttonClass}>
        <Plus className="h-4 w-4" />
      </button>
      <button type="button" aria-label="Zoom out" title="Zoom out" disabled={atMin} onClick={() => map.zoomOut(1)} className={buttonClass}>
        <Minus className="h-4 w-4" />
      </button>
      <button
        type="button"
        aria-label="Show all of Nepal"
        title="Show all of Nepal"
        disabled={atMin}
        onClick={() => {
          map.closePopup();
          map.flyToBounds(NEPAL_BOUNDS, { padding: FIT_PADDING, duration: 0.6 });
        }}
        className={buttonClass}
      >
        <House className="h-4 w-4" />
      </button>
    </div>
  );
}


function FocusHandler({
  focusRequest,
  provinces,
  districts,
  municipalities,
}: {
  focusRequest: FocusRequest | null;
  provinces: ProvinceCollection | null;
  districts: DistrictCollection | null;
  municipalities: MunicipalityCollection | null;
}) {
  const map = useMap();

  useEffect(() => {
    if (!focusRequest) return;
    // An open popup belongs to the place we're leaving, and its auto-panning
    // would knock the flight off course.
    map.closePopup();

    if (focusRequest.kind === "country") {
      map.flyToBounds(NEPAL_BOUNDS, { padding: FIT_PADDING, duration: 0.8 });
      return;
    }

    if (focusRequest.kind === "province" && provinces) {
      const feature = provinces.features.find(
        (f) => getProvinceName(f).toLowerCase() === focusRequest.name.toLowerCase()
      );
      if (feature) {
        flyToFit(map, L.geoJSON(feature).getBounds(), 30, 0.8);
      }
      return;
    }

    if (focusRequest.kind === "district" && districts) {
      const feature = districts.features.find(
        (f) => f.properties.DISTRICT.toLowerCase() === focusRequest.name.toLowerCase()
      );
      if (feature) {
        flyToFit(map, L.geoJSON(feature).getBounds(), 40, 0.8);
      }
      return;
    }

    if (focusRequest.kind === "municipality" && municipalities) {
      const feature = municipalities.features.find(
        (f) =>
          f.properties.NAME.toLowerCase() === focusRequest.name.toLowerCase() &&
          f.properties.DISTRICT.toLowerCase() === (focusRequest.district ?? "").toLowerCase()
      );
      if (feature) {
        flyToFit(map, L.geoJSON(feature).getBounds(), 60, 0.8);
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [focusRequest]);

  return null;
}

export function NepalMap({
  provinces,
  districts,
  municipalities,
  selectedProvince,
  selectedDistrict,
  onSelectProvince,
  onSelectDistrict,
  focusRequest,
  taggingMode,
  onMapClick,
  pendingTag,
  tags,
  onRemoveTag,
}: {
  provinces: ProvinceCollection | null;
  districts: DistrictCollection | null;
  municipalities: MunicipalityCollection | null;
  selectedProvince: string | null;
  /** Used by the map key, whose provinces double as a filter. */
  onSelectProvince: (name: string) => void;
  selectedDistrict: string | null;
  onSelectDistrict: (name: string) => void;
  focusRequest: FocusRequest | null;
  taggingMode: boolean;
  onMapClick: (lat: number, lng: number, district?: string) => void;
  pendingTag: { lat: number; lng: number } | null;
  tags: GeoTag[];
  onRemoveTag: (id: string) => void;
}) {
  const [newsTarget, setNewsTarget] = useState<NewsTarget | null>(null);
  // Stable values for the popup's props: react-leaflet closes and re-opens a
  // popup whenever `position` changes identity, and this component re-renders
  // on every hover, so an inline [lat, lng] made the popup flicker and re-pan
  // the map as the mouse moved.
  const newsPosition = useMemo<L.LatLngTuple | null>(
    () => (newsTarget ? [newsTarget.lat, newsTarget.lng] : null),
    [newsTarget]
  );
  const newsPopupHandlers = useMemo(() => ({ remove: () => setNewsTarget(null) }), []);
  const [map, setMap] = useState<L.Map | null>(null);

  // Selecting a district flies the map to it. Opening its news popup at the
  // same time breaks that for districts near the edge (Darchula, Humla,
  // Taplejung…): the popup doesn't fit, Leaflet pans to make room, and that pan
  // cancels the flight halfway. So wait until the map has landed, when the
  // district is centred and the popup fits.
  const pendingNewsRef = useRef<NewsTarget | null>(null);
  function openNewsAfterFlight(target: NewsTarget) {
    setNewsTarget(null);
    pendingNewsRef.current = target;
    const open = () => {
      // A newer click (another district) replaces this one.
      if (pendingNewsRef.current !== target) return;
      pendingNewsRef.current = null;
      setNewsTarget(target);
    };
    // zoomend, not moveend: a flight always ends with zoomend, while moveend
    // also fires mid-flight when the panel resizes (invalidateSize).
    map?.once("zoomend", open);
    // Re-clicking the selected district doesn't move the map, so no zoomend.
    window.setTimeout(open, 1500);
  }
  const [hovered, setHovered] = useState<HoverTarget | null>(null);
  // The map's own colors follow the chosen background: "auto" follows the
  // site's light/dark mode, "white" and "dark" force one look either way.
  const systemDark = useIsDark();
  const [background, setBackground] = useState<MapBackground>(loadMapBackground);
  const dark = background === "auto" ? systemDark : background === "dark";
  const palette = dark ? DARK_PALETTE : LIGHT_PALETTE;
  // Read by BoundsController on resize: keep the view while a place is selected.
  // A layout effect, so it's set before the resize that the selection itself
  // can cause is observed.
  const keepViewRef = useRef(false);
  useLayoutEffect(() => {
    keepViewRef.current = Boolean(selectedProvince || selectedDistrict);
  }, [selectedProvince, selectedDistrict]);
  const districtLayersRef = useRef(new Map<string, L.Polygon>());
  // Keyed by N_ID; only the first shape of a multi-part municipality gets a label.
  const municipalityLayersRef = useRef(new Map<string, L.Polygon>());

  // Place labels the way printed maps do: a name may spill past its own border
  // into open space. If its centered spot overlaps a name already placed, try a
  // few spots just around it (above, below, right, left); only if all are taken
  // is it hidden. Placement order decides who gets first pick: municipalities of
  // the selected district, then the selected district, then districts in the
  // selected province, then the rest. Hidden names reappear on zoom; the hover
  // card names every place.
  const updateLabelVisibility = useCallback(() => {
    if (!map) return;
    // Wait a frame so Leaflet has repositioned the tooltips for the new zoom.
    requestAnimationFrame(() => {
      const items: { el: HTMLElement; priority: number; area: number; forced: boolean }[] = [];
      function measure(layer: L.Polygon) {
        const bounds = layer.getBounds();
        const nw = map!.latLngToContainerPoint(bounds.getNorthWest());
        const se = map!.latLngToContainerPoint(bounds.getSouthEast());
        return (se.x - nw.x) * (se.y - nw.y);
      }

      municipalityLayersRef.current.forEach((layer) => {
        const el = layer.getTooltip()?.getElement();
        if (!el || !map.hasLayer(layer)) return;
        items.push({ el, priority: 3, area: measure(layer), forced: false });
      });
      // With its municipalities named on the map, the selected district's own
      // name (also in the context bar and breadcrumb) no longer has to win.
      const showingMunicipalities = items.length > 0;

      districtLayersRef.current.forEach((layer, name) => {
        const el = layer.getTooltip()?.getElement();
        if (!el || !map.hasLayer(layer)) return;
        const isSelected = name.toLowerCase() === selectedDistrict?.toLowerCase();
        const inSelectedProvince = Boolean(selectedProvince) && getDistrictProvince(name) === selectedProvince;
        items.push({
          el,
          priority: isSelected ? 2 : inSelectedProvince ? 1 : 0,
          area: measure(layer),
          forced: isSelected && !showingMunicipalities,
        });
      });
      // Scale-dependent order, as in printed atlases. At the full-country view
      // bigger districts pick first, so the overview names the major ones.
      // Once zoomed in there's room to spare, so smaller districts pick first:
      // a big district can nudge its name aside, a tiny one (Bhaktapur,
      // Lalitpur) has nowhere else to go.
      const zoomedIn = map.getZoom() > map.getMinZoom() + 0.5;
      items.sort((a, b) => b.priority - a.priority || (zoomedIn ? a.area - b.area : b.area - a.area));

      // Clear last time's nudges, then measure every label once at its centered
      // spot (rects are measurable even while visibility:hidden). Candidate
      // spots are then tested arithmetically — no re-layout per try.
      for (const item of items) item.el.style.margin = "0";
      const rects = items.map((item) => item.el.getBoundingClientRect());

      const GAP = 2;
      // A name half cut off by the map's edge is worse than a missing one, so
      // every spot must fit fully inside the map.
      const frame = map.getContainer().getBoundingClientRect();
      // The zoom buttons and credit line sit on top of the map; treat them as
      // already-taken space so no name ends up hidden underneath.
      const placed: { left: number; right: number; top: number; bottom: number }[] = Array.from(
        map.getContainer().parentElement?.querySelectorAll<HTMLElement>("[data-map-overlay]") ?? [],
        (el) => el.getBoundingClientRect()
      );
      items.forEach((item, i) => {
        const r = rects[i];
        const up = -(r.height + GAP);
        const down = r.height + GAP;
        const right = r.width / 2 + GAP;
        const left = -(r.width / 2 + GAP);
        const candidates: [number, number][] = [
          [0, 0],
          [0, up],
          [0, down],
          [right, 0],
          [left, 0],
          [right, up],
          [right, down],
          [left, up],
          [left, down],
        ];
        let chosen: [number, number] | null = null;
        for (const [dx, dy] of candidates) {
          const box = { left: r.left + dx, right: r.right + dx, top: r.top + dy, bottom: r.bottom + dy };
          const insideFrame =
            box.left >= frame.left + GAP &&
            box.right <= frame.right - GAP &&
            box.top >= frame.top + GAP &&
            box.bottom <= frame.bottom - GAP;
          if (!insideFrame) continue;
          const collides = placed.some(
            (p) => box.left < p.right + GAP && box.right > p.left - GAP && box.top < p.bottom + GAP && box.bottom > p.top - GAP
          );
          if (!collides || item.forced) {
            chosen = [dx, dy];
            placed.push(box);
            break;
          }
        }
        item.el.classList.toggle("district-label--placed", Boolean(chosen));
        if (chosen) item.el.style.margin = `${chosen[1]}px 0 0 ${chosen[0]}px`;
      });
    });
  }, [map, selectedDistrict, selectedProvince]);

  useEffect(() => {
    if (!map) return;
    updateLabelVisibility();
    map.on("zoomend", updateLabelVisibility);
    return () => {
      map.off("zoomend", updateLabelVisibility);
    };
    // municipalities: their labels need placing once the background load lands.
    // dark: switching colors redraws every shape, with fresh (hidden) labels.
  }, [map, updateLabelVisibility, districts, municipalities, selectedProvince, dark]);

  // Draw the selected district above its neighbours so its border and glow
  // aren't covered by theirs. Not once its municipalities are showing: they
  // are drawn on top of it and must stay there.
  const showingMunicipalities = Boolean(municipalities && selectedDistrict);
  useEffect(() => {
    if (!selectedDistrict || showingMunicipalities) return;
    districtLayersRef.current.get(selectedDistrict)?.bringToFront();
  }, [selectedDistrict, showingMunicipalities, districts, dark]);

  // onEachFeature only runs once per layer, so click handlers close over stale props.
  // Read tagging mode from a ref that's always current instead of the closed-over value.
  const taggingModeRef = useRef(taggingMode);
  const onMapClickRef = useRef(onMapClick);
  useEffect(() => {
    taggingModeRef.current = taggingMode;
    onMapClickRef.current = onMapClick;
  }, [taggingMode, onMapClick]);

  const filteredMunicipalities = useMemo(() => {
    if (!municipalities || !selectedDistrict) return null;
    return {
      ...municipalities,
      features: municipalities.features.filter(
        (f) => f.properties.DISTRICT.toLowerCase() === selectedDistrict.toLowerCase()
      ),
    };
  }, [municipalities, selectedDistrict]);

  // Every district renders at once, tinted by its province, like a printed
  // administrative map — selection just dims everything outside the active
  // province and picks out the active district's border.
  //
  // Memoized on the selection only: GeoJSON calls setStyle() on all 77
  // districts whenever this function's identity changes, so it must not change
  // on hover. Hover is applied to the one layer under the pointer instead.
  const districtStyle = useCallback(
    (feature?: Feature): PathOptions => {
      const name = (feature as DistrictFeature | undefined)?.properties.DISTRICT ?? "";
      const province = getDistrictProvince(name);
      const isSelectedDistrict = name.toLowerCase() === selectedDistrict?.toLowerCase();
      const inSelectedProvince = !selectedProvince || province === selectedProvince;
      // Three steps of emphasis, so the eye goes straight to the selection:
      // the selected district, the rest of its province, then everything else.
      let fillOpacity = 0.85;
      if (!inSelectedProvince) fillOpacity = palette.dimmedOpacity;
      else if (selectedDistrict && !isSelectedDistrict) fillOpacity = 0.55;
      if (isSelectedDistrict) fillOpacity = 1;
      return {
        color: isSelectedDistrict ? palette.selected : palette.border,
        weight: isSelectedDistrict ? 2.5 : 1,
        fillColor: getProvinceColor(province, dark),
        fillOpacity,
        // A soft glow around the selected district (see .map-selected-glow).
        className: isSelectedDistrict ? "map-selected-glow" : undefined,
      };
    },
    [selectedDistrict, selectedProvince, palette, dark]
  );
  // Layer event handlers are bound once per layer, so they read the current
  // style function through a ref rather than the one from when they were bound.
  const districtStyleRef = useRef(districtStyle);
  useEffect(() => {
    districtStyleRef.current = districtStyle;
  }, [districtStyle]);

  function onEachDistrict(feature: Feature, layer: Layer) {
    const props = (feature as DistrictFeature).properties;
    const path = layer as L.Polygon;
    districtLayersRef.current.set(props.DISTRICT, path);
    layer.bindTooltip(MAP_LABEL_ABBREVIATIONS[props.DISTRICT] ?? titleCase(props.DISTRICT), {
      permanent: true,
      direction: "center",
      className: "district-label",
      interactive: false,
    });
    layer.on("click", (e: LeafletMouseEvent) => {
      L.DomEvent.stopPropagation(e);
      // Pin mode: drop the pin where they clicked, without selecting the district
      // (selecting would fly the map away from the spot they just picked).
      if (taggingModeRef.current) {
        onMapClickRef.current(e.latlng.lat, e.latlng.lng, props.DISTRICT);
        return;
      }
      onSelectDistrict(props.DISTRICT);
      const center = (layer as L.Polygon).getBounds().getCenter();
      openNewsAfterFlight({ name: props.DISTRICT, lat: center.lat, lng: center.lng });
    });
    layer.on("mouseover", () => {
      const base = districtStyleRef.current(feature);
      path.setStyle({
        color: base.color === palette.border ? palette.hover : base.color,
        weight: 2.5,
        fillOpacity: Math.min(1, (base.fillOpacity ?? 0.85) + 0.3),
      });
      setHovered({ kind: "district", name: props.DISTRICT });
    });
    layer.on("mouseout", () => {
      path.setStyle(districtStyleRef.current(feature));
      setHovered((current) => (current?.kind === "district" && current.name === props.DISTRICT ? null : current));
    });
  }

  // Province outlines sit on top purely as a bolder boundary between
  // same-colored district clusters — no fill, no clicks of their own (the
  // district layer beneath already reports province selection on click).
  const provinceBoundaryStyle = useCallback(
    (feature?: Feature): PathOptions => {
      const name = feature ? getProvinceName(feature as ProvinceFeature) : undefined;
      const isSelected = name?.toLowerCase() === selectedProvince?.toLowerCase();
      return {
        fill: false,
        color: isSelected ? palette.selected : palette.provinceLine,
        weight: isSelected ? 3 : 1.75,
        opacity: isSelected ? 1 : palette.provinceLineOpacity,
        interactive: false,
      };
    },
    [selectedProvince, palette]
  );

  // Municipalities keep their district's province color, split by white
  // borders like the districts themselves (a translucent green overlay used to
  // turn e.g. Gandaki's orange a muddy olive).
  const municipalityStyle = useCallback((feature?: Feature): PathOptions => {
    const district = (feature as MunicipalityFeature | undefined)?.properties.DISTRICT ?? "";
    return {
      color: palette.border,
      weight: 1,
      fillColor: getProvinceColor(getDistrictProvince(district), dark),
      fillOpacity: 1,
    };
  }, [palette, dark]);

  // The selected district's blue outline, redrawn above the municipalities
  // (whose white borders would otherwise cover it).
  const selectedDistrictFeature = useMemo(
    () =>
      selectedDistrict
        ? districts?.features.find((f) => f.properties.DISTRICT.toLowerCase() === selectedDistrict.toLowerCase())
        : undefined,
    [districts, selectedDistrict]
  );

  // A multi-part municipality is several features with one N_ID; label only the
  // first. Leaflet hands onEachFeature these same feature objects.
  const labelledMunicipalityFeatures = useMemo(() => {
    const seen = new Set<string>();
    const firsts = new Set<Feature>();
    for (const f of filteredMunicipalities?.features ?? []) {
      if (seen.has(f.properties.N_ID)) continue;
      seen.add(f.properties.N_ID);
      firsts.add(f);
    }
    return firsts;
  }, [filteredMunicipalities]);

  function onEachMunicipality(feature: Feature, layer: Layer) {
    const props = (feature as MunicipalityFeature).properties;
    const path = layer as L.Polygon;
    if (labelledMunicipalityFeatures.has(feature)) {
      municipalityLayersRef.current.set(props.N_ID, path);
      path.bindTooltip(props.NAME, {
        permanent: true,
        direction: "center",
        className: "district-label municipality-label",
        interactive: false,
      });
    }
    layer.on("mouseover", () => {
      path.setStyle({ color: palette.hover, weight: 2 });
      setHovered({ kind: "municipality", name: props.NAME, district: props.DISTRICT, level: props.LEVEL });
    });
    layer.on("mouseout", () => {
      path.setStyle({ color: palette.border, weight: 1 });
      setHovered((current) => (current?.kind === "municipality" && current.name === props.NAME ? null : current));
    });
    layer.on("click", (e: LeafletMouseEvent) => {
      L.DomEvent.stopPropagation(e);
      if (taggingModeRef.current) {
        onMapClickRef.current(e.latlng.lat, e.latlng.lng, props.DISTRICT);
        return;
      }
      const center = (layer as L.Polygon).getBounds().getCenter();
      setNewsTarget({ name: props.NAME, lat: center.lat, lng: center.lng });
    });
  }

  return (
    // The map fills its whole panel on a soft backdrop, like a map app, rather
    // than sitting in a card with empty space around it. fitBounds centres the
    // country whatever the panel's shape.
    <div className="h-full w-full">
      <div
        className={`relative h-full w-full overflow-hidden ${BACKGROUND_CLASSES[background]} ${dark ? "map-canvas-dark" : ""} ${
          taggingMode
            ? "ring-2 ring-inset ring-amber-400 [&_.leaflet-container]:!cursor-crosshair [&_.leaflet-interactive]:!cursor-crosshair"
            : ""
        }`}
        role="region"
        aria-label="Map of Nepal by district"
        aria-describedby="nepal-map-help"
      >
      {/* The shapes themselves can't be reached by keyboard or read by a screen
          reader, so point to the side panel, which offers every place as a list. */}
      <p id="nepal-map-help" className="sr-only">
        Each district is colored by its province. To choose a province, district, or local government with a
        keyboard or screen reader, use &ldquo;Find a place&rdquo; or the lists in the side panel.
      </p>
      <MapContainer
        ref={setMap}
        center={[28.3949, 84.124]}
        zoom={MIN_ZOOM}
        minZoom={MIN_ZOOM}
        maxZoom={MAX_ZOOM}
        // Whole-number zoom snapping makes fitBounds round down to the next level
        // that fits, which can leave the country at roughly half the panel size.
        zoomSnap={0}
        zoomControl={false}
        dragging={false}
        scrollWheelZoom
        touchZoom
        doubleClickZoom={false}
        boxZoom={false}
        keyboard={false}
        attributionControl={false}
        className="h-full w-full"
      >
        <BoundsController keepViewRef={keepViewRef} />
        <PanWhenZoomed />

        {districts && (
          <GeoJSON
            key={`districts-${selectedProvince ?? "none"}-${selectedDistrict ?? "none"}-${dark}`}
            data={districts}
            style={districtStyle}
            onEachFeature={onEachDistrict}
          />
        )}

        {provinces && (
          <GeoJSON
            key={`province-outline-${selectedProvince ?? "none"}-${dark}`}
            data={provinces}
            style={provinceBoundaryStyle}
          />
        )}

        {filteredMunicipalities && (
          <GeoJSON
            key={`municipalities-${selectedDistrict}-${dark}`}
            data={filteredMunicipalities}
            style={municipalityStyle}
            onEachFeature={onEachMunicipality}
          />
        )}

        {filteredMunicipalities && selectedDistrictFeature && (
          <GeoJSON
            key={`selected-outline-${selectedDistrict}-${dark}`}
            data={selectedDistrictFeature}
            style={{ fill: false, color: palette.selected, weight: 3, interactive: false, className: "map-selected-glow" }}
          />
        )}

        {tags.map((tag) => (
          <Marker key={tag.id} position={[tag.lat, tag.lng]} icon={TAG_ICON}>
            <Popup>
              <div className="min-w-[160px] text-sm">
                <p className="font-semibold">{tag.label}</p>
                {tag.note && <p className="mt-1 text-slate-600 dark:text-slate-300">{tag.note}</p>}
                {tag.district && (
                  <p className="mt-1 text-xs text-slate-500 dark:text-slate-400">{titleCase(tag.district)} district</p>
                )}
                <button
                  type="button"
                  onClick={() => onRemoveTag(tag.id)}
                  className="mt-2 text-xs font-medium text-rose-600 hover:underline dark:text-rose-400"
                >
                  Delete pin
                </button>
              </div>
            </Popup>
          </Marker>
        ))}

        {pendingTag && (
          <Marker position={[pendingTag.lat, pendingTag.lng]} icon={PENDING_ICON} interactive={false} />
        )}

        {newsTarget && newsPosition && (
          <Popup
            key={`${newsTarget.name}-${newsTarget.lat}-${newsTarget.lng}`}
            position={newsPosition}
            eventHandlers={newsPopupHandlers}
          >
            {/* The popup opens over the centre of the map (where the flight
                landed), so cap the list to the space above centre (minus
                header, padding and tip). */}
            <NewsPopupContent
              place={titleCase(newsTarget.name)}
              listMaxHeight={map ? Math.max(90, map.getSize().y / 2 - 110) : undefined}
            />
          </Popup>
        )}

        <FocusHandler
          focusRequest={focusRequest}
          provinces={provinces}
          districts={districts}
          municipalities={municipalities}
        />
      </MapContainer>
      {map && <ZoomControls map={map} />}
      {map && <MapBackgroundPicker value={background} onChange={setBackground} />}
      {map && (
        <MapLegend
          dark={dark}
          selectedProvince={selectedProvince}
          onSelectProvince={onSelectProvince}
          showPins={tags.length > 0 || Boolean(pendingTag)}
          collapsed={Boolean(selectedProvince || selectedDistrict)}
          onResize={updateLabelVisibility}
        />
      )}
      {hovered && <HoverCard target={hovered} taggingMode={taggingMode} dark={dark} />}
      {/* Required by the boundary data's CC BY 4.0 license. */}
      <a
        href="https://localboundries.oknp.org/"
        data-map-overlay
        target="_blank"
        rel="noopener noreferrer"
        className="absolute bottom-1 right-2 z-[1000] rounded bg-white/80 px-1.5 py-0.5 text-[10px] text-slate-500 hover:text-slate-800 hover:underline dark:bg-slate-900/80 dark:text-slate-400 dark:hover:text-slate-100"
      >
        Boundaries: Open Knowledge Nepal, CC BY 4.0
      </a>
      </div>

      <style jsx global>{`
        .leaflet-container {
          background: transparent !important;
          font-family: inherit;
        }

        .map-selected-glow {
          filter: drop-shadow(0 0 3px rgba(37, 99, 235, 0.55)) drop-shadow(0 0 10px rgba(37, 99, 235, 0.35));
        }

        .district-label {
          background: transparent;
          border: none;
          box-shadow: none;
          padding: 0;
          margin: 0;
          font-size: 10px;
          font-weight: 600;
          line-height: 1;
          color: #0f172a;
          text-shadow:
            -1px -1px 0 #fff,
            1px -1px 0 #fff,
            -1px 1px 0 #fff,
            1px 1px 0 #fff,
            0 0 3px #fff;
          pointer-events: none;
          white-space: nowrap;
          /* Hidden until placement finds it a free spot, so freshly drawn
             labels never flash on top of each other. */
          visibility: hidden;
        }

        .district-label--placed {
          visibility: visible;
        }

        /* Lighter than district names, so the district still reads as the
           bigger unit when both are on screen. */
        .municipality-label {
          font-size: 9px;
          font-weight: 500;
          color: #334155;
        }

        .leaflet-popup-content-wrapper {
          border-radius: 12px;
        }

        /* Map labels and glow follow the map's background (set on the
           wrapper), not the page theme: a white map keeps dark labels even
           in dark mode. */
        .map-canvas-dark .district-label {
          color: #f1f5f9;
          text-shadow:
            -1px -1px 0 #0f172a,
            1px -1px 0 #0f172a,
            -1px 1px 0 #0f172a,
            1px 1px 0 #0f172a,
            0 0 3px #0f172a;
        }
        .map-canvas-dark .municipality-label {
          color: #cbd5e1;
        }
        .map-canvas-dark .map-selected-glow {
          filter: drop-shadow(0 0 3px rgba(96, 165, 250, 0.7)) drop-shadow(0 0 12px rgba(96, 165, 250, 0.4));
        }

        /* Popups are page UI, so they follow the page theme. */
        @media (prefers-color-scheme: dark) {
          .leaflet-popup-content-wrapper,
          .leaflet-popup-tip {
            background: #1e293b;
            color: #e2e8f0;
          }
          .leaflet-container a.leaflet-popup-close-button {
            color: #94a3b8;
          }
        }
      `}</style>
    </div>
  );
}

const BACKGROUND_CLASSES: Record<MapBackground, string> = {
  auto: "bg-[radial-gradient(ellipse_at_center,#f8fafc_0%,#e2e8f0_100%)] dark:bg-[radial-gradient(ellipse_at_center,#1e293b_0%,#0b1120_100%)]",
  white: "bg-white",
  dark: "bg-[radial-gradient(ellipse_at_center,#1e293b_0%,#0b1120_100%)]",
};

// Map colors that can't come from CSS classes (Leaflet sets them on the SVG).
const LIGHT_PALETTE = {
  border: "#ffffff",
  hover: "#0f172a",
  selected: "#1d4ed8",
  provinceLine: "#334155",
  provinceLineOpacity: 0.75,
  dimmedOpacity: 0.18,
};
const DARK_PALETTE = {
  border: "#0f172a",
  hover: "#f8fafc",
  selected: "#60a5fa",
  provinceLine: "#cbd5e1",
  provinceLineOpacity: 0.55,
  dimmedOpacity: 0.25,
};

type NewsTarget = { name: string; lat: number; lng: number };

type HoverTarget =
  | { kind: "district"; name: string }
  | { kind: "municipality"; name: string; district: string; level: string };

// Names the place under the pointer (even when its map label is hidden for
// space), shows where it belongs, and says what a click will do. Tucked in the
// top-right corner beside the zoom buttons, which Nepal's shape leaves empty
// (the map key has the bottom-left one). Hidden on touch
// screens: a tap fires mouseover but never mouseout, so it would stick.
function HoverCard({ target, taggingMode, dark }: { target: HoverTarget; taggingMode: boolean; dark: boolean }) {
  const district = target.kind === "district" ? target.name : target.district;
  const province = getDistrictProvince(district);
  return (
    <div className="pointer-events-none absolute right-14 top-3 z-[1000] hidden max-w-[60%] rounded-lg border border-slate-200 bg-white/95 px-3 py-2 shadow-md backdrop-blur dark:border-slate-700 dark:bg-slate-900/90 [@media(hover:hover)]:block">
      <p className="text-sm font-semibold text-slate-900 dark:text-slate-50">
        {target.kind === "district" ? `${titleCase(target.name)} district` : target.name}
      </p>
      {target.kind === "municipality" && (
        <p className="mt-0.5 text-xs text-slate-600 dark:text-slate-400">
          {getLevelLabel(target.level)} · {titleCase(target.district)} district
        </p>
      )}
      {target.kind === "district" && province && (
        <p className="mt-0.5 flex items-center gap-1.5 text-xs text-slate-600 dark:text-slate-400">
          <span className="h-2.5 w-2.5 shrink-0 rounded-full" style={{ backgroundColor: getProvinceColor(province, dark) }} />
          {province}
        </p>
      )}
      <p className="mt-1 flex items-center gap-1 text-xs font-medium text-blue-700 dark:text-blue-400">
        <MousePointerClick className="h-3.5 w-3.5" />
        {taggingMode ? "Click to drop your pin here" : "Click to see local news"}
      </p>
    </div>
  );
}
