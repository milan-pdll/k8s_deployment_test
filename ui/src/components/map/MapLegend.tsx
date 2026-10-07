"use client";

import { useState } from "react";
import { ChevronDown } from "lucide-react";
import { cn } from "@/lib/cn";
import { PROVINCE_NAMES, getProvinceColor } from "@/lib/geo";

const PROVINCES = Object.values(PROVINCE_NAMES);

// The map's key, in the bottom-left corner (Nepal's shape leaves it empty).
// Each province doubles as a filter. Starts open on wide screens and folded on
// phones, where the map has less room.
export function MapLegend({
  dark,
  selectedProvince,
  onSelectProvince,
  showPins,
  collapsed,
  onResize,
}: {
  dark: boolean;
  selectedProvince: string | null;
  onSelectProvince: (name: string) => void;
  showPins: boolean;
  /** Fold the key away while zoomed into a place, where it would cover the map. */
  collapsed: boolean;
  /** Called after opening or folding, so map labels can move out of the way. */
  onResize: () => void;
}) {
  const isWide = () => window.matchMedia("(min-width: 640px)").matches;
  const [open, setOpen] = useState(() => !collapsed && isWide());
  // Follow `collapsed` when it changes; in between, the reader can still open
  // or fold the key themselves.
  const [lastCollapsed, setLastCollapsed] = useState(collapsed);
  if (collapsed !== lastCollapsed) {
    setLastCollapsed(collapsed);
    setOpen(!collapsed && isWide());
  }

  return (
    <div
      data-map-overlay
      className={cn(
        "absolute bottom-3 left-3 z-[1000] max-w-[calc(100%-1.5rem)] rounded-lg border border-slate-200 bg-white/95 text-slate-700 shadow-sm backdrop-blur dark:border-slate-700 dark:bg-slate-900/90 dark:text-slate-200",
        // Folded, it's just a small button, clear of the credit line.
        open ? "w-60" : "w-auto"
      )}
    >
      <button
        type="button"
        onClick={() => {
          setOpen((v) => !v);
          requestAnimationFrame(onResize);
        }}
        aria-expanded={open}
        aria-controls="map-legend-body"
        className="flex w-full items-center justify-between gap-2 rounded-lg px-3 py-2 text-xs font-semibold focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500"
      >
        Map key
        <ChevronDown className={cn("h-3.5 w-3.5 transition-transform", open && "rotate-180")} aria-hidden />
      </button>

      {open && (
        <div id="map-legend-body" className="border-t border-slate-200 px-2 pb-2 pt-1.5 dark:border-slate-700">
          <p className="px-1 text-[11px] text-slate-500 dark:text-slate-400">Provinces · click to focus</p>
          <ul className="mt-1 grid grid-cols-2 gap-0.5">
            {PROVINCES.map((name) => {
              const active = selectedProvince === name;
              return (
                <li key={name}>
                  <button
                    type="button"
                    aria-pressed={active}
                    title={active ? "Show all of Nepal" : `Focus on ${name}`}
                    onClick={() => onSelectProvince(active ? "" : name)}
                    className={cn(
                      "flex w-full items-center gap-1.5 rounded px-1 py-0.5 text-left text-[11px] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500",
                      active
                        ? "bg-slate-900 font-semibold text-white dark:bg-slate-100 dark:text-slate-900"
                        : "hover:bg-slate-100 dark:hover:bg-slate-800"
                    )}
                  >
                    <span
                      className="h-2.5 w-2.5 shrink-0 rounded-sm ring-1 ring-black/10 dark:ring-white/15"
                      style={{ backgroundColor: getProvinceColor(name, dark) }}
                      aria-hidden
                    />
                    <span className="truncate">{name.replace(" Province", "")}</span>
                  </button>
                </li>
              );
            })}
          </ul>

          <ul className="mt-1.5 space-y-1 border-t border-slate-200 px-1 pt-1.5 text-[11px] text-slate-600 dark:border-slate-700 dark:text-slate-300">
            <li className="flex items-center gap-2">
              <span
                aria-hidden
                className="h-2.5 w-4 shrink-0 rounded-sm border-2 border-blue-600 shadow-[0_0_6px_rgba(37,99,235,0.6)] dark:border-blue-400"
              />
              Selected district
            </li>
            <li className="flex items-center gap-2">
              <span aria-hidden className="h-0.5 w-4 shrink-0 rounded bg-slate-600 dark:bg-slate-300" />
              Province border
            </li>
            {showPins && (
              <li className="flex items-center gap-2">
                <span aria-hidden className="ml-0.5 h-2.5 w-2.5 shrink-0 rounded-full border-2 border-white bg-blue-600 ring-1 ring-blue-600/40" />
                Your pins
              </li>
            )}
          </ul>
        </div>
      )}
    </div>
  );
}
