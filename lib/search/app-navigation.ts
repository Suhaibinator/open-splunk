const BACKEND_APP_PARAMETER = "appId";
const BACKEND_APP_LOCATION_EVENT = "open-splunk:backend-app-location-change";

function normalizedBackendAppId(appId: string): string {
  const normalized = appId.trim();
  if (normalized.length === 0) throw new TypeError("A backend app ID is required.");
  return normalized;
}

/** Adds an app preference to an internal URL without discarding its other state. */
export function backendAppHref(href: string, appId: string): string {
  const url = new URL(href, "http://open-splunk.local");
  url.searchParams.set(BACKEND_APP_PARAMETER, normalizedBackendAppId(appId));
  return `${url.pathname}${url.search}${url.hash}`;
}

/** Builds a Search URL that asks system bootstrap to select a specific advertised app. */
export function backendAppSearchHref(appId: string): string {
  return backendAppHref("/search/events/", appId);
}

/** Reads the optional app selection handed to Search by another frontend surface. */
export function preferredBackendAppId(search: string): string | undefined {
  const value = new URLSearchParams(search).get(BACKEND_APP_PARAMETER)?.trim();
  return value === undefined || value.length === 0 ? undefined : value;
}

/** Returns the server-selected app only when it must replace an explicit URL preference. */
export function canonicalBackendAppId(
  preferredAppId: string | undefined,
  selectedAppId: string | null,
): string | undefined {
  return preferredAppId !== undefined
    && selectedAppId !== null
    && preferredAppId !== selectedAppId
    ? selectedAppId
    : undefined;
}

/** Reads the current browser app preference without making server rendering depend on `window`. */
export function currentBackendAppId(): string | undefined {
  return typeof window === "undefined"
    ? undefined
    : preferredBackendAppId(window.location.search);
}

/**
 * Observes both browser history navigation and same-document app commits.
 * `history.replaceState` does not emit `popstate`, so callers must use
 * `replaceBackendAppId` instead of writing history directly.
 */
export function subscribeToBackendAppId(listener: () => void): () => void {
  if (typeof window === "undefined") return () => undefined;
  window.addEventListener("popstate", listener);
  window.addEventListener(BACKEND_APP_LOCATION_EVENT, listener);
  return () => {
    window.removeEventListener("popstate", listener);
    window.removeEventListener(BACKEND_APP_LOCATION_EVENT, listener);
  };
}

/** Commits the selected app in place and notifies other mounted browser surfaces. */
export function replaceBackendAppId(appId: string): void {
  if (typeof window === "undefined") return;
  const currentHref = `${window.location.pathname}${window.location.search}${window.location.hash}`;
  const nextHref = backendAppHref(currentHref, appId);
  if (nextHref === currentHref) return;
  window.history.replaceState(window.history.state, "", nextHref);
  window.dispatchEvent(new Event(BACKEND_APP_LOCATION_EVENT));
}

/** Returns whether an observed URL app preference has changed, including removal. */
export function backendAppPreferenceNeedsSync(
  preferredAppId: string | undefined,
  observedAppId: string | undefined,
): boolean {
  return preferredAppId !== observedAppId;
}

export interface CurrentBackendAppRequestOptions {
  readonly currentAppId?: () => string | undefined;
  readonly subscribe?: (listener: () => void) => () => void;
}

/**
 * Runs a request against the current URL app preference. If browser navigation
 * changes that preference in flight, the stale request is aborted and its
 * success or failure is discarded before retrying the latest preference.
 */
export async function requestCurrentBackendApp<T>(
  request: (preferredAppId: string | undefined, signal: AbortSignal) => Promise<T>,
  options: CurrentBackendAppRequestOptions = {},
): Promise<{ preferredAppId: string | undefined; value: T }> {
  const currentAppId = options.currentAppId ?? currentBackendAppId;
  const subscribe = options.subscribe ?? subscribeToBackendAppId;
  for (;;) {
    const preferredAppId = currentAppId();
    const controller = new AbortController();
    let preferenceChanged = false;
    const unsubscribe = subscribe(() => {
      if (!backendAppPreferenceNeedsSync(currentAppId(), preferredAppId)) return;
      preferenceChanged = true;
      controller.abort(new DOMException("The backend app preference changed.", "AbortError"));
    });
    try {
      if (backendAppPreferenceNeedsSync(currentAppId(), preferredAppId)) {
        preferenceChanged = true;
        controller.abort(new DOMException("The backend app preference changed.", "AbortError"));
        continue;
      }
      // Each retry depends on the latest preference after the prior attempt settles.
      // eslint-disable-next-line no-await-in-loop
      const value = await request(preferredAppId, controller.signal);
      if (preferenceChanged || backendAppPreferenceNeedsSync(currentAppId(), preferredAppId)) continue;
      return { preferredAppId, value };
    } catch (error) {
      if (preferenceChanged || backendAppPreferenceNeedsSync(currentAppId(), preferredAppId)) continue;
      throw error;
    } finally {
      unsubscribe();
    }
  }
}
