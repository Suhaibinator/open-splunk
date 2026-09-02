"use client";

import { useEffect, useSyncExternalStore } from "react";

import {
  currentThemePreference,
  followSystemTheme,
  subscribeToThemePreference,
  syncTheme,
} from "@/lib/theme-preference";

/**
 * Keeps `data-theme` current after the boot script's one-time read: a choice
 * committed in another tab arrives as a `storage` event, and while the
 * preference is `system` the operating system's own switch is followed live.
 * Mounted once from the root layout so every page, the sign-in screen
 * included, gets both without carrying the product shell.
 */
export function ThemeSync() {
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
  return null;
}
