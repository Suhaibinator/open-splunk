import {
  subscribeToAdministratorSessionRevision,
} from "./administrator-session";
import { createOpenSplunkApiClient } from "./open-splunk-client";
import { getSystemBootstrap, type SystemBootstrapModel } from "./system-bootstrap";

export const APP_CATALOG_INVALIDATION_STORAGE_KEY = "open-splunk.app-catalog-invalidation";

export interface AppCatalogKey {
  readonly apiBaseUrl: string;
  readonly preferredAppId?: string;
  readonly sessionRevision: number;
}

export type AppCatalogState = "idle" | "loading" | "available" | "error";

export interface AppCatalogSnapshot {
  readonly bootstrap: SystemBootstrapModel | null;
  readonly error: string | null;
  readonly stale: boolean;
  readonly state: AppCatalogState;
}

type AppCatalogLoad = (key: AppCatalogKey, signal: AbortSignal) => Promise<SystemBootstrapModel>;
type AppCatalogStorage = Pick<Storage, "getItem" | "setItem">;
type AppCatalogEventTarget = Pick<Window, "addEventListener" | "removeEventListener">;

export interface CreateAppCatalogStoreOptions {
  readonly eventTarget?: AppCatalogEventTarget | null;
  readonly load?: AppCatalogLoad;
  readonly nonce?: () => string;
  readonly storage?: AppCatalogStorage | null;
}

export interface AppCatalogStore {
  clear(): void;
  dispose(): void;
  getSnapshot(key: AppCatalogKey): AppCatalogSnapshot;
  invalidate(apiBaseUrl: string): void;
  load(key: AppCatalogKey): Promise<void>;
  refresh(key: AppCatalogKey): Promise<void>;
  subscribe(key: AppCatalogKey, listener: () => void): () => void;
}

interface CatalogEntry {
  controller: AbortController | null;
  epoch: number;
  inFlight: Promise<void> | null;
  key: AppCatalogKey;
  listeners: Set<() => void>;
  snapshot: AppCatalogSnapshot;
}

function notify(entry: CatalogEntry): void {
  for (const listener of Array.from(entry.listeners)) {
    try {
      listener();
    } catch {
      // One consumer cannot prevent the remaining catalog views from updating.
    }
  }
}

const IDLE_SNAPSHOT: AppCatalogSnapshot = Object.freeze({
  bootstrap: null,
  error: null,
  stale: false,
  state: "idle",
});

function defaultEventTarget(): AppCatalogEventTarget | null {
  return typeof window === "undefined" ? null : window;
}

function defaultStorage(): AppCatalogStorage | null {
  if (typeof window === "undefined") return null;
  try {
    return window.localStorage;
  } catch {
    return null;
  }
}

function defaultNonce(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
}

function normalizedPreferredAppId(preferredAppId: string | undefined): string | undefined {
  const normalized = preferredAppId?.trim();
  return normalized ? normalized : undefined;
}

/** Canonicalizes equivalent API roots before they become cache partitions. */
export function normalizeAppCatalogApiBaseUrl(apiBaseUrl: string): string {
  const value = apiBaseUrl.trim();
  if (value.length === 0 || /^\/+$/u.test(value)) return "";
  if (value.startsWith("/")) return value.replace(/\/+$/u, "");
  try {
    const url = new URL(value);
    if (url.username || url.password || url.search || url.hash) return value.replace(/\/+$/u, "");
    const pathname = url.pathname.replace(/\/+$/u, "");
    return `${url.origin}${pathname}`;
  } catch {
    return value.replace(/\/+$/u, "");
  }
}

export function appCatalogKey(
  apiBaseUrl: string,
  preferredAppId: string | undefined,
  sessionRevision: number,
): AppCatalogKey {
  return Object.freeze({
    apiBaseUrl: normalizeAppCatalogApiBaseUrl(apiBaseUrl),
    preferredAppId: normalizedPreferredAppId(preferredAppId),
    sessionRevision,
  });
}

function serializedKey(key: AppCatalogKey): string {
  return JSON.stringify([
    normalizeAppCatalogApiBaseUrl(key.apiBaseUrl),
    normalizedPreferredAppId(key.preferredAppId) ?? null,
    key.sessionRevision,
  ]);
}

function errorMessage(error: unknown): string {
  return error instanceof Error && error.message.trim()
    ? error.message
    : "The backend app catalog could not be loaded.";
}

export function createAppCatalogStore(options: CreateAppCatalogStoreOptions = {}): AppCatalogStore {
  const entries = new Map<string, CatalogEntry>();
  const loadCatalog = options.load ?? (async (key, signal) => getSystemBootstrap(
    createOpenSplunkApiClient({ baseUrl: key.apiBaseUrl }),
    key.preferredAppId,
    { signal },
  ));
  const eventTarget = options.eventTarget === undefined ? defaultEventTarget() : options.eventTarget;
  const storage = options.storage === undefined ? defaultStorage() : options.storage;
  const nonce = options.nonce ?? defaultNonce;

  function entryFor(key: AppCatalogKey): CatalogEntry {
    const normalized = appCatalogKey(key.apiBaseUrl, key.preferredAppId, key.sessionRevision);
    const id = serializedKey(normalized);
    let entry = entries.get(id);
    if (entry !== undefined) return entry;
    entry = {
      controller: null,
      epoch: 0,
      inFlight: null,
      key: normalized,
      listeners: new Set(),
      snapshot: IDLE_SNAPSHOT,
    };
    entries.set(id, entry);
    return entry;
  }

  function begin(entry: CatalogEntry, replacePending: boolean): Promise<void> {
    if (!replacePending && entry.inFlight !== null) return entry.inFlight;
    if (!replacePending && entry.snapshot.state === "available" && !entry.snapshot.stale) {
      return Promise.resolve();
    }
    entry.epoch += 1;
    const requestEpoch = entry.epoch;
    entry.controller?.abort();
    const controller = new AbortController();
    entry.controller = controller;
    entry.snapshot = Object.freeze({
      bootstrap: entry.snapshot.bootstrap,
      error: null,
      stale: entry.snapshot.bootstrap !== null,
      state: "loading",
    });
    notify(entry);
    const request = loadCatalog(entry.key, controller.signal).then((bootstrap) => {
      if (controller.signal.aborted || requestEpoch !== entry.epoch) return;
      entry.snapshot = Object.freeze({
        bootstrap,
        error: null,
        stale: false,
        state: "available",
      });
      // The server-selected app is also the authoritative answer for its
      // canonical preference key. Reuse it when that key has never loaded,
      // so canonicalizing a fallback cannot open a second loading window.
      if (bootstrap.selectedAppId !== null && bootstrap.selectedAppId !== entry.key.preferredAppId) {
        const canonical = entryFor(appCatalogKey(entry.key.apiBaseUrl, bootstrap.selectedAppId, entry.key.sessionRevision));
        if (canonical.epoch === 0 && canonical.snapshot.state === "idle") {
          canonical.snapshot = entry.snapshot;
          notify(canonical);
        }
      }
      notify(entry);
    }, (error: unknown) => {
      if (controller.signal.aborted || requestEpoch !== entry.epoch) return;
      entry.snapshot = Object.freeze({
        bootstrap: entry.snapshot.bootstrap,
        error: errorMessage(error),
        stale: entry.snapshot.bootstrap !== null,
        state: "error",
      });
      notify(entry);
    }).finally(() => {
      if (requestEpoch !== entry.epoch) return;
      entry.controller = null;
      entry.inFlight = null;
    });
    entry.inFlight = request;
    return request;
  }

  function invalidateEntries(apiBaseUrl?: string): void {
    const normalizedBase = apiBaseUrl === undefined
      ? undefined
      : normalizeAppCatalogApiBaseUrl(apiBaseUrl);
    for (const entry of entries.values()) {
      if (normalizedBase !== undefined && entry.key.apiBaseUrl !== normalizedBase) continue;
      entry.epoch += 1;
      entry.controller?.abort();
      entry.controller = null;
      entry.inFlight = null;
      entry.snapshot = Object.freeze({
        bootstrap: entry.snapshot.bootstrap,
        error: null,
        stale: entry.snapshot.bootstrap !== null,
        state: entry.listeners.size > 0 ? "loading" : "idle",
      });
      notify(entry);
      if (entry.listeners.size > 0) void begin(entry, true);
    }
  }

  const onStorage = (event: StorageEvent): void => {
    if (event.key === APP_CATALOG_INVALIDATION_STORAGE_KEY) invalidateEntries();
  };
  let listeningForStorage = false;
  let subscriptionEpoch = 0;
  function synchronizeStorageSubscription(): void {
    const epoch = ++subscriptionEpoch;
    const active = [...entries.values()].some((entry) => entry.listeners.size > 0);
    if (active && !listeningForStorage) {
      eventTarget?.addEventListener("storage", onStorage);
      listeningForStorage = true;
    } else if (!active && listeningForStorage) {
      eventTarget?.removeEventListener("storage", onStorage);
      listeningForStorage = false;
    }
    if (!active) {
      // Keep a synchronous preference-key transition coalesced. Once no view
      // remains, the next mount must refetch changes made while unobserved.
      queueMicrotask(() => { if (epoch === subscriptionEpoch) invalidateEntries(); });
    }
  }

  return {
    clear() {
      for (const entry of entries.values()) {
        entry.epoch += 1;
        entry.controller?.abort();
        entry.controller = null;
        entry.inFlight = null;
        entry.snapshot = IDLE_SNAPSHOT;
        notify(entry);
      }
    },
    dispose() {
      subscriptionEpoch += 1;
      eventTarget?.removeEventListener("storage", onStorage);
      listeningForStorage = false;
      this.clear();
      entries.clear();
    },
    getSnapshot(key) {
      return entryFor(key).snapshot;
    },
    invalidate(apiBaseUrl) {
      invalidateEntries(apiBaseUrl);
      try {
        storage?.setItem(APP_CATALOG_INVALIDATION_STORAGE_KEY, nonce());
      } catch {
        // Same-document subscribers were already invalidated above.
      }
    },
    load(key) {
      return begin(entryFor(key), false);
    },
    refresh(key) {
      return begin(entryFor(key), true);
    },
    subscribe(key, listener) {
      const entry = entryFor(key);
      entry.listeners.add(listener);
      synchronizeStorageSubscription();
      return () => {
        entry.listeners.delete(listener);
        synchronizeStorageSubscription();
        if (entry.listeners.size !== 0 || entry.inFlight === null) return;
        entry.epoch += 1;
        entry.controller?.abort();
        entry.controller = null;
        entry.inFlight = null;
      };
    },
  };
}

export const appCatalogStore = createAppCatalogStore();

subscribeToAdministratorSessionRevision(() => appCatalogStore.clear());

export function invalidateAppCatalog(apiBaseUrl: string): void {
  appCatalogStore.invalidate(apiBaseUrl);
}
