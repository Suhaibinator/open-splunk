"use client";

import { useEffect, useSyncExternalStore } from "react";

import { createOpenSplunkApiClient, getSystemBootstrap, subscribeToSystemBootstrap } from "@/lib/api";
import type { SearchDataMode } from "@/lib/search/backend-data";
import {
  applyInstancePalette,
  currentThemePreference,
  followSystemTheme,
  PALETTE_STORAGE_KEY,
  subscribeToThemePreference,
  syncPalette,
  syncTheme,
} from "@/lib/theme-preference";

/**
 * Keeps `data-theme` and `data-palette` current after the boot script's
 * one-time read.
 *
 * Theme: a choice committed in another tab arrives as a `storage` event, and
 * while the preference is `system` the operating system's own switch is
 * followed live.
 *
 * Palette: the boot script painted whatever the cache held, so the mount
 * re-applies the cache (a first load has none and paints classic), follows
 * cache writes from other tabs, and -- against a backend only -- applies the
 * live value from every `/api/system/bootstrap` envelope the page resolves
 * (`subscribeToSystemBootstrap`). The product shell and every console already
 * fetch that envelope for their own catalog, so a backend page issues no
 * request for the palette alone; a page with no such loader mounts
 * `InstancePaletteFetch` to ask once. Every failure is swallowed: the demo
 * export never asks, and an offline backend keeps the cached or classic
 * palette rather than surfacing an error on every page.
 *
 * Mounted once from the root layout so every page, the sign-in screen
 * included, gets all of this without carrying the product shell.
 */
export function ThemeSync({ dataMode }: { dataMode: SearchDataMode }) {
  const preference = useSyncExternalStore(
    subscribeToThemePreference,
    currentThemePreference,
    () => "system" as const,
  );
  useEffect(() => {
    syncTheme();
    if (preference !== "system") return;
    return followSystemTheme();
  }, [preference]);
  useEffect(() => {
    syncPalette();
    const followCache = (event: StorageEvent) => {
      // A `null` key is `localStorage.clear()`, which drops the cache too.
      if (event.key === null || event.key === PALETTE_STORAGE_KEY) syncPalette();
    };
    window.addEventListener("storage", followCache);
    return () => window.removeEventListener("storage", followCache);
  }, []);
  useEffect(() => {
    if (dataMode !== "backend") return;
    // Unsubscribing on unmount is what drops an envelope that lands late.
    return subscribeToSystemBootstrap((bootstrap) => applyInstancePalette(bootstrap.palette));
  }, [dataMode]);
  return null;
}

/**
 * One in-flight palette request per API base URL in this document.
 *
 * React's StrictMode mounts, unmounts and mounts again before any response
 * can land, and a page that navigates away and back mounts the fetcher
 * anew: the first joins the request already running, the second asks again,
 * because the answer may have changed in between. The entry is dropped as
 * the request settles, whichever way.
 */
const paletteRequests = new Map<string, Promise<void>>();

/**
 * Asks `/api/system/bootstrap` once for a page that mounts no product shell
 * and no console: the sign-in screen. Bootstrap needs no bearer token, which
 * is what lets that page take the palette at all. The answer is not applied
 * here; it reaches `ThemeSync` the way every other loader's envelope does, so
 * one path paints the palette however it was fetched. A failure is left
 * silent for the same reason it is there: the cached or classic paint stands.
 *
 * Renders nothing. Mount it beside the page's content, never in the layout,
 * or every page would ask again on top of its own loader.
 */
export function InstancePaletteFetch({ apiBaseUrl, dataMode }: { apiBaseUrl: string; dataMode: SearchDataMode }) {
  useEffect(() => {
    if (dataMode !== "backend") return;
    if (paletteRequests.has(apiBaseUrl)) return;
    const client = createOpenSplunkApiClient({ baseUrl: apiBaseUrl });
    const request = getSystemBootstrap(client)
      .then(() => undefined, () => undefined)
      .finally(() => {
        if (paletteRequests.get(apiBaseUrl) === request) paletteRequests.delete(apiBaseUrl);
      });
    paletteRequests.set(apiBaseUrl, request);
  }, [apiBaseUrl, dataMode]);
  return null;
}
