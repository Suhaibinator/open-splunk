import assert from "node:assert/strict";
import test from "node:test";

import {
  backendAppHref,
  backendAppPreferenceNeedsSync,
  backendAppSearchHref,
  canonicalBackendAppId,
  currentBackendAppId,
  preferredBackendAppId,
  replaceBackendAppId,
  requestCurrentBackendApp,
  subscribeToBackendAppId,
} from "./app-navigation";

function installFakeWindow(initialHref: string) {
  const originalWindow = Object.getOwnPropertyDescriptor(globalThis, "window");
  const events = new EventTarget();
  const location = new URL(initialHref);
  const replacements: Array<{ state: unknown; href: string }> = [];
  const historyState = { navigation: "preserved" };
  const fakeWindow = {
    addEventListener(type: string, listener: EventListenerOrEventListenerObject) {
      events.addEventListener(type, listener);
    },
    dispatchEvent(event: Event) {
      return events.dispatchEvent(event);
    },
    history: {
      state: historyState,
      replaceState(state: unknown, _unused: string, href: string | URL | null) {
        const nextHref = String(href);
        replacements.push({ state, href: nextHref });
        const nextLocation = new URL(nextHref, location);
        location.pathname = nextLocation.pathname;
        location.search = nextLocation.search;
        location.hash = nextLocation.hash;
      },
    },
    location,
    removeEventListener(type: string, listener: EventListenerOrEventListenerObject) {
      events.removeEventListener(type, listener);
    },
  } as unknown as Window;
  Object.defineProperty(globalThis, "window", { configurable: true, value: fakeWindow });
  return {
    fakeWindow,
    historyState,
    replacements,
    restore() {
      if (originalWindow === undefined) delete (globalThis as { window?: Window }).window;
      else Object.defineProperty(globalThis, "window", originalWindow);
    },
  };
}

test("backend app links encode an exact bootstrap preference", () => {
  const href = backendAppSearchHref(" app/operations east ");
  assert.equal(href, "/search/events/?appId=app%2Foperations+east");
  assert.equal(preferredBackendAppId(new URL(href, "https://example.test").search), "app/operations east");
});

test("blank app selections are rejected or ignored", () => {
  assert.throws(() => backendAppSearchHref(" \t "), /app ID is required/);
  assert.equal(preferredBackendAppId("?appId=%20%20"), undefined);
  assert.equal(preferredBackendAppId("?q=index%3Dmain"), undefined);
});

test("backend app links preserve unrelated URL state and replace an existing preference", () => {
  assert.equal(
    backendAppHref("/analytics/?range=24h&appId=old#failures", " operations/east "),
    "/analytics/?range=24h&appId=operations%2Feast#failures",
  );
});

test("same-document app commits preserve history state and notify all location observers", () => {
  const browser = installFakeWindow("https://example.test/analytics/?range=24h#failures");
  try {
    let notifications = 0;
    const unsubscribe = subscribeToBackendAppId(() => {
      notifications += 1;
    });

    replaceBackendAppId("operations/east");
    assert.deepEqual(browser.replacements, [{
      state: browser.historyState,
      href: "/analytics/?range=24h&appId=operations%2Feast#failures",
    }]);
    assert.equal(currentBackendAppId(), "operations/east");
    assert.equal(notifications, 1);

    replaceBackendAppId("operations/east");
    assert.equal(browser.replacements.length, 1);
    assert.equal(notifications, 1);

    browser.fakeWindow.dispatchEvent(new Event("popstate"));
    assert.equal(notifications, 2);
    unsubscribe();
    browser.fakeWindow.dispatchEvent(new Event("popstate"));
    assert.equal(notifications, 2);
  } finally {
    browser.restore();
  }
});

test("Search detects changed URL preferences, including removal of appId", () => {
  assert.equal(backendAppPreferenceNeedsSync(undefined, undefined), false);
  assert.equal(backendAppPreferenceNeedsSync("default", "default"), false);
  assert.equal(backendAppPreferenceNeedsSync("operations", undefined), true);
  assert.equal(backendAppPreferenceNeedsSync(undefined, "operations"), true);
});

test("an explicit unavailable preference canonicalizes to the server-selected app", () => {
  assert.equal(canonicalBackendAppId("requested", "selected"), "selected");
  assert.equal(canonicalBackendAppId("selected", "selected"), undefined);
  assert.equal(canonicalBackendAppId(undefined, "selected"), undefined);
  assert.equal(canonicalBackendAppId("requested", null), undefined);
});

interface Deferred<T> {
  promise: Promise<T>;
  reject: (error: unknown) => void;
  resolve: (value: T) => void;
}

function deferred<T>(): Deferred<T> {
  let reject!: (error: unknown) => void;
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, reject, resolve };
}

async function nextMicrotask(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

for (const staleOutcome of ["resolve", "reject"] as const) {
  test(`initial bootstrap discards a stale ${staleOutcome} after the URL app changes`, async () => {
    let currentAppId: string | undefined = "app-a";
    const locationListeners = new Set<() => void>();
    const requests: Array<{
      appId: string | undefined;
      deferred: Deferred<string>;
      signal: AbortSignal;
    }> = [];
    const resultPromise = requestCurrentBackendApp(
      (appId, signal) => {
        const pending = deferred<string>();
        requests.push({ appId, deferred: pending, signal });
        return pending.promise;
      },
      {
        currentAppId: () => currentAppId,
        subscribe: (listener) => {
          locationListeners.add(listener);
          return () => locationListeners.delete(listener);
        },
      },
    );
    assert.equal(requests.length, 1);
    assert.equal(requests[0].appId, "app-a");

    currentAppId = "app-b";
    for (const listener of locationListeners) listener();
    assert.equal(requests[0].signal.aborted, true);
    if (staleOutcome === "resolve") requests[0].deferred.resolve("stale app-a");
    else requests[0].deferred.reject(new Error("stale app-a failure"));
    await nextMicrotask();

    assert.equal(requests.length, 2);
    assert.equal(requests[1].appId, "app-b");
    requests[1].deferred.resolve("current app-b");
    assert.deepEqual(await resultPromise, { preferredAppId: "app-b", value: "current app-b" });
  });
}
