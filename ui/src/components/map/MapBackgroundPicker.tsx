"use client";

import { useEffect, useRef, useState } from "react";
import { Check, Layers } from "lucide-react";
import { cn } from "@/lib/cn";

export type MapBackground = "auto" | "white" | "dark";

const OPTIONS: { value: MapBackground; label: string; hint: string; swatch: string }[] = [
  {
    value: "auto",
    label: "Auto",
    hint: "Matches light or dark mode",
    swatch: "linear-gradient(135deg, #f1f5f9 50%, #1e293b 50%)",
  },
  { value: "white", label: "White", hint: "Plain white, like a printed map", swatch: "#ffffff" },
  { value: "dark", label: "Dark", hint: "Deep colors on a dark background", swatch: "#0f172a" },
];

const STORAGE_KEY = "pgs_map_background";

/** The reader's saved choice, or "auto". Saved per device only. */
export function loadMapBackground(): MapBackground {
  try {
    const saved = window.localStorage.getItem(STORAGE_KEY);
    return saved === "white" || saved === "dark" ? saved : "auto";
  } catch {
    return "auto";
  }
}

function saveMapBackground(value: MapBackground) {
  try {
    window.localStorage.setItem(STORAGE_KEY, value);
  } catch {
    // Private browsing or storage full: the choice just isn't remembered.
  }
}

// A button under the zoom controls that opens a small menu of map backgrounds.
export function MapBackgroundPicker({
  value,
  onChange,
}: {
  value: MapBackground;
  onChange: (value: MapBackground) => void;
}) {
  const [open, setOpen] = useState(false);
  const rootRef = useRef<HTMLDivElement>(null);
  const buttonRef = useRef<HTMLButtonElement>(null);

  // Close on a click outside or on Esc, like any menu.
  useEffect(() => {
    if (!open) return;
    function onPointerDown(e: PointerEvent) {
      if (!rootRef.current?.contains(e.target as Node)) setOpen(false);
    }
    function onKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape") {
        setOpen(false);
        buttonRef.current?.focus();
      }
    }
    document.addEventListener("pointerdown", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("pointerdown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [open]);

  return (
    // Sits just below the zoom buttons (top-3 + their 100px height + a gap).
    <div ref={rootRef} data-map-overlay className="absolute right-3 top-[7.5rem] z-[1000]">
      <button
        ref={buttonRef}
        type="button"
        onClick={() => setOpen((v) => !v)}
        aria-haspopup="true"
        aria-expanded={open}
        aria-label="Map background"
        title="Map background"
        className={cn(
          "flex h-8 w-8 items-center justify-center rounded-lg border bg-white shadow-sm transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 dark:bg-slate-900",
          open
            ? "border-blue-500 text-blue-600 dark:text-blue-400"
            : "border-slate-200 text-slate-700 hover:bg-slate-100 dark:border-slate-700 dark:text-slate-200 dark:hover:bg-slate-800"
        )}
      >
        <Layers className="h-4 w-4" aria-hidden />
      </button>

      {open && (
        <div
          role="radiogroup"
          aria-label="Map background"
          className="absolute right-full top-0 mr-2 w-56 rounded-lg border border-slate-200 bg-white p-1 shadow-lg dark:border-slate-700 dark:bg-slate-900"
        >
          <p className="px-2 pb-1 pt-1.5 text-[11px] font-semibold uppercase tracking-wide text-slate-500 dark:text-slate-400">
            Background
          </p>
          {OPTIONS.map((option) => {
            const selected = option.value === value;
            return (
              <button
                key={option.value}
                type="button"
                role="radio"
                aria-checked={selected}
                onClick={() => {
                  onChange(option.value);
                  saveMapBackground(option.value);
                  setOpen(false);
                }}
                className={cn(
                  "flex w-full items-center gap-2.5 rounded-md px-2 py-1.5 text-left focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500",
                  selected ? "bg-blue-50 dark:bg-blue-500/10" : "hover:bg-slate-100 dark:hover:bg-slate-800"
                )}
              >
                <span
                  aria-hidden
                  className="h-6 w-6 shrink-0 rounded-md border border-slate-300 dark:border-slate-600"
                  style={{ background: option.swatch }}
                />
                <span className="min-w-0 flex-1">
                  <span className="block text-sm font-medium text-slate-900 dark:text-slate-100">{option.label}</span>
                  <span className="block text-[11px] text-slate-500 dark:text-slate-400">{option.hint}</span>
                </span>
                {selected && <Check className="h-4 w-4 shrink-0 text-blue-600 dark:text-blue-400" aria-hidden />}
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}
