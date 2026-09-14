"use client";

import { useCallback, useEffect, useMemo, useSyncExternalStore } from "react";

import {
  appCatalogKey,
  appCatalogStore,
  currentAdministratorSessionRevision,
  subscribeToAdministratorSessionRevision,
  type AppCatalogSnapshot,
} from "@/lib/api";

const DISABLED_CATALOG: AppCatalogSnapshot = Object.freeze({
  bootstrap: null,
  error: null,
  stale: false,
  state: "idle",
});

export interface UseAppCatalogResult extends AppCatalogSnapshot {
  refresh(): Promise<void>;
}

/** Shares one bootstrap-authoritative app catalog across every mounted surface. */
export function useAppCatalog(
  apiBaseUrl: string,
  preferredAppId: string | undefined,
  enabled = true,
): UseAppCatalogResult {
  const sessionRevision = useSyncExternalStore(
    subscribeToAdministratorSessionRevision,
    currentAdministratorSessionRevision,
    () => 0,
  );
  const key = useMemo(
    () => appCatalogKey(apiBaseUrl, preferredAppId, sessionRevision),
    [apiBaseUrl, preferredAppId, sessionRevision],
  );
  const subscribe = useCallback(
    (listener: () => void) => enabled ? appCatalogStore.subscribe(key, listener) : () => undefined,
    [enabled, key],
  );
  const getSnapshot = useCallback(
    () => enabled ? appCatalogStore.getSnapshot(key) : DISABLED_CATALOG,
    [enabled, key],
  );
  const snapshot = useSyncExternalStore(subscribe, getSnapshot, () => DISABLED_CATALOG);

  useEffect(() => {
    if (enabled) void appCatalogStore.load(key);
  }, [enabled, key]);

  return useMemo(() => ({
    ...snapshot,
    refresh: () => enabled ? appCatalogStore.refresh(key) : Promise.resolve(),
  }), [enabled, key, snapshot]);
}
