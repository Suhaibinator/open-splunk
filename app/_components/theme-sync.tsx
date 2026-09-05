"use client";

import { useEffect, useSyncExternalStore } from "react";

import { createOpenSplunkApiClient, getSystemBootstrap } from "@/lib/api";
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
 * cache writes from other tabs, and -- against a backend only -- fetches the
 * live value from `/api/system/bootstrap` once and applies it. Every failure
 * is swallowed: the demo export never asks, and an offline backend keeps the
 * cached or classic palette rather than surfacing an error on every page.
 *
 * Mounted once from the root layout so every page, the sign-in screen
 * included, gets all of this without carrying the product shell. Bootstrap
 * needs no bearer token, which is what lets the sign-in page take the palette.
 */
export function ThemeSync({ apiBaseUrl, dataMode }: { apiBaseUrl: string; dataMode: SearchDataMode }) {
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
    let current = true;
    const client = createOpenSplunkApiClient({ baseUrl: apiBaseUrl });
    getSystemBootstrap(client).then((model) => {
      if (current) applyInstancePalette(model.palette);
    }).catch(() => {
      // Offline or unreachable: the cached (or classic) palette stands.
    });
    return () => {
      current = false;
    };
  }, [apiBaseUrl, dataMode]);
  return null;
}
