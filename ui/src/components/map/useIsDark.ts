import { useSyncExternalStore } from "react";

// Whether the page is in dark mode (the site follows the system setting). The
// map's colors are set from JavaScript, so it can't rely on CSS dark: classes.
const QUERY = "(prefers-color-scheme: dark)";

function subscribe(onChange: () => void) {
  const media = window.matchMedia(QUERY);
  media.addEventListener("change", onChange);
  return () => media.removeEventListener("change", onChange);
}

export function useIsDark(): boolean {
  return useSyncExternalStore(
    subscribe,
    () => window.matchMedia(QUERY).matches,
    () => false
  );
}
