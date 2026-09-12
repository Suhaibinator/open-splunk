import assert from "node:assert/strict";
import test from "node:test";

import { AppState } from "@/gen/ts/open_splunk/app";
import { GetSystemBootstrapResponse } from "@/gen/ts/open_splunk/system_api";
import {
  APP_CATALOG_INVALIDATION_STORAGE_KEY,
  createAppCatalogStore,
  type AppCatalogKey,
} from "./app-catalog";
import { adaptSystemBootstrap, type SystemBootstrapModel } from "./system-bootstrap";

function bootstrap(appIds: string[], selectedAppId = appIds[0]): SystemBootstrapModel {
  return adaptSystemBootstrap(GetSystemBootstrapResponse.fromPartial({
    apps: appIds.map((appId) => ({ appId, displayName: appId, state: AppState.APP_STATE_ACTIVE })),
    selectedAppId,
    serverTime: new Date("2026-09-12T00:00:00Z"),
  }));
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: Error) => void;
  const promise = new Promise<T>((accept, fail) => { resolve = accept; reject = fail; });
  return { promise, reject, resolve };
}

function harness(options: Parameters<typeof createAppCatalogStore>[0] = {}) {
  const requests: Array<{
    key: AppCatalogKey;
    signal: AbortSignal;
    response: ReturnType<typeof deferred<SystemBootstrapModel>>;
  }> = [];
  const store = createAppCatalogStore({
    eventTarget: null,
    storage: null,
    ...options,
    load(key, signal) {
      const response = deferred<SystemBootstrapModel>();
      requests.push({ key, response, signal });
      return response.promise;
    },
  });
  return { requests, store };
}

const key: AppCatalogKey = { apiBaseUrl: "https://backend.test", preferredAppId: "app-a", sessionRevision: 0 };
const settle = () => new Promise<void>((resolve) => setImmediate(resolve));

test("equivalent base URLs and trimmed preferences share one bootstrap request and snapshot", async () => {
  const { requests, store } = harness();
  try {
    const equivalent = { ...key, apiBaseUrl: "https://backend.test/", preferredAppId: " app-a " };
    const first = store.load(key);
    const second = store.load(equivalent);
    assert.equal(requests.length, 1);
    const result = bootstrap(["app-a", "app-b"]);
    requests[0].response.resolve(result);
    await Promise.all([first, second]);
    assert.equal(store.getSnapshot(key), store.getSnapshot(equivalent));
    assert.equal(store.getSnapshot(key).bootstrap, result);
    assert.equal(store.getSnapshot(key).stale, false);
  } finally { store.dispose(); }
});

test("backend, app preference, and authentication revision isolate catalog responses", async () => {
  const { requests, store } = harness();
  try {
    const keys = [key, { ...key, apiBaseUrl: "https://tenant-b.test" }, { ...key, preferredAppId: "app-b" }, { ...key, sessionRevision: 1 }];
    const loads = keys.map((entry) => store.load(entry));
    assert.equal(requests.length, keys.length);
    requests.forEach((request, index) => request.response.resolve(bootstrap([`private-${index}`])));
    await Promise.all(loads);
    keys.forEach((entry, index) => assert.equal(store.getSnapshot(entry).bootstrap?.selectedAppId, `private-${index}`));
  } finally { store.dispose(); }
});

test("catalog invalidation refreshes every mounted preference but leaves other backends alone", async () => {
  const { requests, store } = harness();
  const preferredB = { ...key, preferredAppId: "app-b" };
  const otherBackend = { ...key, apiBaseUrl: "https://other.test" };
  const unsubscribe = [key, preferredB, otherBackend].map((entry) => store.subscribe(entry, () => undefined));
  try {
    const initial = [key, preferredB, otherBackend].map((entry) => store.load(entry));
    requests.forEach((request) => request.response.resolve(bootstrap(["app-a", "app-b"])));
    await Promise.all(initial);
    store.invalidate(key.apiBaseUrl);
    await settle();
    assert.equal(requests.length, 5);
    assert.equal(store.getSnapshot(key).stale, true);
    assert.equal(store.getSnapshot(preferredB).stale, true);
    assert.equal(store.getSnapshot(otherBackend).stale, false);
    requests.slice(3).forEach((request) => request.response.resolve(bootstrap(["app-c"])));
    await settle();
    assert.deepEqual(store.getSnapshot(key).bootstrap?.apps.map((app) => app.appId), ["app-c"]);
    assert.equal(store.getSnapshot(preferredB).bootstrap?.selectedAppId, "app-c");
  } finally { unsubscribe.forEach((stop) => stop()); store.dispose(); }
});

test("a refresh failure preserves last good apps and server selection until retry succeeds", async () => {
  const { requests, store } = harness();
  try {
    const initial = store.load(key);
    const good = bootstrap(["app-a", "app-b"]);
    requests[0].response.resolve(good);
    await initial;
    const refresh = store.refresh(key);
    assert.equal(store.getSnapshot(key).bootstrap, good);
    requests[1].response.reject(new Error("Backend offline"));
    await refresh;
    const failed = store.getSnapshot(key);
    assert.equal(failed.bootstrap, good);
    assert.equal(failed.stale, true);
    assert.match(failed.error ?? "", /Backend offline/u);
    const retry = store.refresh(key);
    requests[2].response.resolve(bootstrap(["app-b"]));
    await retry;
    assert.equal(store.getSnapshot(key).bootstrap?.selectedAppId, "app-b");
    assert.equal(store.getSnapshot(key).error, null);
    assert.equal(store.getSnapshot(key).stale, false);
  } finally { store.dispose(); }
});

test("mutation invalidation supersedes a pre-mutation response even when fetch ignores abort", async () => {
  const { requests, store } = harness();
  const unsubscribe = store.subscribe(key, () => undefined);
  try {
    const initial = store.load(key);
    store.invalidate(key.apiBaseUrl);
    await settle();
    assert.equal(requests.length, 2);
    assert.equal(requests[0].signal.aborted, true);
    requests[1].response.resolve(bootstrap(["created-app"]));
    await settle();
    const current = store.getSnapshot(key);
    requests[0].response.resolve(bootstrap(["deleted-app"]));
    await initial;
    assert.equal(store.getSnapshot(key), current);
    assert.equal(current.bootstrap?.selectedAppId, "created-app");
  } finally { unsubscribe(); store.dispose(); }
});

test("a superseded failure cannot overwrite a newer successful catalog", async () => {
  const { requests, store } = harness();
  const unsubscribe = store.subscribe(key, () => undefined);
  try {
    const old = store.load(key);
    store.invalidate(key.apiBaseUrl);
    await settle();
    requests[1].response.resolve(bootstrap(["current-app"]));
    await settle();
    requests[0].response.reject(new Error("Outdated failure"));
    await old;
    assert.equal(store.getSnapshot(key).error, null);
    assert.equal(store.getSnapshot(key).stale, false);
    assert.equal(store.getSnapshot(key).bootstrap?.selectedAppId, "current-app");
  } finally { unsubscribe(); store.dispose(); }
});

function browserEvents() {
  const target = new EventTarget();
  return {
    target: target as unknown as Pick<Window, "addEventListener" | "removeEventListener">,
    storage(storageKey: string, value: string) {
      const event = Object.assign(new Event("storage"), { key: storageKey, newValue: value });
      target.dispatchEvent(event);
    },
  };
}

test("two documents exchange only a nonce and refetch their own authoritative bootstrap", async () => {
  const eventsA = browserEvents();
  const eventsB = browserEvents();
  const writes: Array<[string, string]> = [];
  const a = harness({ eventTarget: eventsA.target, nonce: () => "opaque-nonce", storage: {
    getItem: () => null,
    setItem(storageKey, value) { writes.push([storageKey, value]); eventsB.storage(storageKey, value); },
  } });
  const b = harness({ eventTarget: eventsB.target });
  const stops = [a.store.subscribe(key, () => undefined), b.store.subscribe(key, () => undefined)];
  try {
    const initial = [a.store.load(key), b.store.load(key)];
    a.requests[0].response.resolve(bootstrap(["tenant-a-secret"]));
    b.requests[0].response.resolve(bootstrap(["tenant-b-secret"]));
    await Promise.all(initial);
    a.store.invalidate(key.apiBaseUrl);
    await settle();
    assert.deepEqual(writes, [[APP_CATALOG_INVALIDATION_STORAGE_KEY, "opaque-nonce"]]);
    assert.equal(a.requests.length, 2);
    assert.equal(b.requests.length, 2);
    a.requests[1].response.resolve(bootstrap(["new-a"]));
    b.requests[1].response.resolve(bootstrap(["new-b"]));
    await settle();
    assert.equal(a.store.getSnapshot(key).bootstrap?.selectedAppId, "new-a");
    assert.equal(b.store.getSnapshot(key).bootstrap?.selectedAppId, "new-b");
    assert.equal(writes.length, 1, "receiving a storage event must not echo invalidation");
  } finally { stops.forEach((stop) => stop()); a.store.dispose(); b.store.dispose(); }
});

test("blocked local storage still refreshes mounted same-document catalogs", async () => {
  const { requests, store } = harness({ storage: {
    getItem() { throw new DOMException("blocked", "SecurityError"); },
    setItem() { throw new DOMException("blocked", "SecurityError"); },
  } });
  const unsubscribe = store.subscribe(key, () => undefined);
  try {
    const initial = store.load(key);
    requests[0].response.resolve(bootstrap(["app-a"]));
    await initial;
    assert.doesNotThrow(() => store.invalidate(key.apiBaseUrl));
    await settle();
    assert.equal(requests.length, 2);
    requests[1].response.resolve(bootstrap(["app-a", "created-app"]));
    await settle();
    assert.equal(store.getSnapshot(key).bootstrap?.apps.length, 2);
  } finally { unsubscribe(); store.dispose(); }
});

test("unrelated storage events are ignored and disposing unregisters cross-document listeners", async () => {
  const events = browserEvents();
  const { requests, store } = harness({ eventTarget: events.target });
  const unsubscribe = store.subscribe(key, () => undefined);
  const initial = store.load(key);
  requests[0].response.resolve(bootstrap(["app-a"]));
  await initial;
  events.storage("unrelated-setting", "anything");
  await settle();
  assert.equal(requests.length, 1);
  unsubscribe();
  store.dispose();
  events.storage(APP_CATALOG_INVALIDATION_STORAGE_KEY, "nonce-after-dispose");
  await settle();
  assert.equal(requests.length, 1);
});

test("unmounted subscribers are not notified by late responses", async () => {
  const { requests, store } = harness();
  let notifications = 0;
  const unsubscribe = store.subscribe(key, () => { notifications += 1; });
  const pending = store.load(key);
  unsubscribe();
  const before = notifications;
  requests[0].response.resolve(bootstrap(["app-a"]));
  await pending;
  assert.equal(notifications, before);
  store.dispose();
});

test("snapshots retain identity between committed state changes", async () => {
  const { requests, store } = harness();
  try {
    const empty = store.getSnapshot(key);
    assert.equal(store.getSnapshot(key), empty);
    let notifications = 0;
    const unsubscribe = store.subscribe(key, () => { notifications += 1; });
    const pending = store.load(key);
    const loading = store.getSnapshot(key);
    assert.equal(store.getSnapshot(key), loading);
    const joined = store.load(key);
    assert.equal(store.getSnapshot(key), loading);
    requests[0].response.resolve(bootstrap(["app-a"]));
    await Promise.all([pending, joined]);
    const available = store.getSnapshot(key);
    const committedNotifications = notifications;
    await store.load(key);
    assert.equal(store.getSnapshot(key), available);
    assert.equal(notifications, committedNotifications);
    unsubscribe();
    assert.equal(store.getSnapshot(key), available);
  } finally { store.dispose(); }
});

test("a previous session response arriving last cannot populate the current session catalog", async () => {
  const { requests, store } = harness();
  try {
    const previousSession = store.load(key);
    const newSessionKey = { ...key, sessionRevision: key.sessionRevision + 1 };
    assert.equal(store.getSnapshot(newSessionKey).bootstrap, null);
    const currentSession = store.load(newSessionKey);
    requests[1].response.resolve(bootstrap(["new-session-private-app"]));
    await currentSession;
    const current = store.getSnapshot(newSessionKey);
    requests[0].response.resolve(bootstrap(["previous-session-private-app"]));
    await previousSession;
    assert.equal(store.getSnapshot(newSessionKey), current);
    assert.equal(current.bootstrap?.selectedAppId, "new-session-private-app");
  } finally { store.dispose(); }
});

test("one broken subscriber cannot prevent other mounted catalogs from refreshing", async () => {
  const { requests, store } = harness();
  const secondKey = { ...key, preferredAppId: "app-b" };
  let throwOnNotification = false;
  const stops = [
    store.subscribe(key, () => { if (throwOnNotification) throw new Error("Unmounted observer failed"); }),
    store.subscribe(secondKey, () => undefined),
  ];
  try {
    const initial = [store.load(key), store.load(secondKey)];
    requests.forEach((request) => request.response.resolve(bootstrap(["app-a", "app-b"])));
    await Promise.all(initial);
    throwOnNotification = true;
    assert.doesNotThrow(() => store.invalidate(key.apiBaseUrl));
    await settle();
    assert.equal(requests.length, 4);
    requests.slice(2).forEach((request) => request.response.resolve(bootstrap(["app-c"])));
    await settle();
    assert.equal(store.getSnapshot(key).bootstrap?.selectedAppId, "app-c");
    assert.equal(store.getSnapshot(secondKey).bootstrap?.selectedAppId, "app-c");
  } finally { stops.forEach((stop) => stop()); store.dispose(); }
});

test("clearing a session aborts old loads and permits an immediate fresh load", async () => {
  const { requests, store } = harness();
  try {
    const previous = store.load(key);
    store.clear();
    assert.equal(requests[0].signal.aborted, true);
    assert.equal(store.getSnapshot(key).bootstrap, null);
    const fresh = store.load(key);
    assert.equal(requests.length, 2);
    requests[1].response.resolve(bootstrap(["fresh-app"]));
    await fresh;
    requests[0].response.resolve(bootstrap(["cleared-private-app"]));
    await previous;
    assert.equal(store.getSnapshot(key).bootstrap?.selectedAppId, "fresh-app");
  } finally { store.dispose(); }
});

test("server app ordering and fallback selection remain authoritative", async () => {
  const { requests, store } = harness();
  try {
    const pending = store.load(key);
    const result = bootstrap(["Zebra", "apple"], "apple");
    requests[0].response.resolve(result);
    await pending;
    assert.deepEqual(store.getSnapshot(key).bootstrap?.apps.map((app) => app.appId), ["Zebra", "apple"]);
    assert.equal(store.getSnapshot(key).bootstrap?.selectedAppId, "apple");
  } finally { store.dispose(); }
});


test("server fallback seeds an unused canonical app key without a second loading window", async () => {
  const { requests, store } = harness();
  try {
    const initial = { ...key, preferredAppId: undefined };
    const loading = store.load(initial);
    const response = bootstrap(["app-a"]);
    requests[0].response.resolve(response);
    await loading;
    assert.equal(store.getSnapshot(key).bootstrap, response);
    assert.equal(store.getSnapshot(key).state, "available");
    await store.load(key);
    assert.equal(requests.length, 1);
    assert.equal(store.getSnapshot(key).bootstrap?.receivedAt, response.receivedAt);
  } finally { store.dispose(); }
});

test("fallback cannot replace a canonical app request that already has newer authority", async () => {
  const { requests, store } = harness();
  try {
    const initial = { ...key, preferredAppId: undefined };
    const fallback = store.load(initial);
    const canonical = store.load(key);
    requests[0].response.resolve(bootstrap(["app-a"]));
    await fallback;
    assert.equal(store.getSnapshot(key).state, "loading");
    assert.equal(store.getSnapshot(key).bootstrap, null);
    const current = bootstrap(["app-a", "app-b"]);
    requests[1].response.resolve(current);
    await canonical;
    assert.equal(store.getSnapshot(key).bootstrap, current);
  } finally { store.dispose(); }
});
