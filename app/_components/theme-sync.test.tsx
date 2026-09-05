import assert from "node:assert/strict";
import process from "node:process";
import test from "node:test";

// Load-bearing order: the fake browser has to be on `globalThis` before
// `react-dom/client` is evaluated, because that module decides at load time
// whether it has a DOM. Module evaluation follows import order.
import { browser } from "@/lib/testing/fake-browser";
import { act } from "react";
import { createRoot } from "react-dom/client";

import { UiPalette } from "@/gen/ts/open_splunk/server_settings_api";
import { GetSystemBootstrapResponse } from "@/gen/ts/open_splunk/system_api";
import type { OpenSplunkApiClient } from "@/lib/api";
import * as clientModule from "@/lib/api/open-splunk-client";
import type { SearchDataMode } from "@/lib/search/backend-data";
import { DARK_SCHEME_QUERY, PALETTE_STORAGE_KEY, THEME_STORAGE_KEY } from "@/lib/theme-preference";

import { ThemeSync } from "./theme-sync";

/*
 * ThemeSync mounted for real, against a fake window whose storage, media
 * query and client factory the test steers. The demo export must never build
 * a client; a backend that is away must leave the cached palette standing
 * and say nothing; and a cross-tab write to one key must never repaint the
 * other axis.
 */

const html = browser.document.documentElement;
browser.computedStyle.set("--chrome-bar", () => `bar(${html.getAttribute("data-palette")}/${html.getAttribute("data-theme")})`);

/** Every attribute written on `<html>` since the last reset, in order. */
const attributeWrites: Array<[string, string]> = [];
const realSetAttribute = html.setAttribute.bind(html);
html.setAttribute = (name: string, value: unknown) => {
  attributeWrites.push([name, String(value)]);
  realSetAttribute(name, value);
};

/* -- The client factory, replaced on its module so `@/lib/api` re-exports the spy -- */

type Factory = typeof clientModule.createOpenSplunkApiClient;
const factoryCalls: Array<Parameters<Factory>[0]> = [];
let bootstrapAnswers: Array<() => Promise<GetSystemBootstrapResponse>> = [];
let bootstrapCalls = 0;
const realFactory = clientModule.createOpenSplunkApiClient;
const spyFactory: Factory = (options) => {
  factoryCalls.push(options);
  return {
    system: {
      bootstrap() {
        bootstrapCalls += 1;
        const next = bootstrapAnswers.shift();
        assert.ok(next, "unexpected bootstrap request");
        return next();
      },
    },
  } as unknown as OpenSplunkApiClient;
};
(clientModule as unknown as { createOpenSplunkApiClient: Factory }).createOpenSplunkApiClient = spyFactory;

/* -- Nothing may reach the network, the console, or the unhandled-rejection hook -- */

let fetches = 0;
const realFetch = globalThis.fetch;
globalThis.fetch = (() => {
  fetches += 1;
  return Promise.reject(new Error("the network must not be touched"));
}) as typeof fetch;

const consoleCalls: Array<[string, unknown[]]> = [];
const realConsole = { debug: console.debug, error: console.error, info: console.info, log: console.log, warn: console.warn };
for (const level of ["debug", "error", "info", "log", "warn"] as const) {
  console[level] = (...arguments_: unknown[]) => {
    consoleCalls.push([level, arguments_]);
  };
}
const unhandled: unknown[] = [];
const onUnhandled = (reason: unknown) => {
  unhandled.push(reason);
};
process.on("unhandledRejection", onUnhandled);

function bootstrap(uiPalette: UiPalette): GetSystemBootstrapResponse {
  return GetSystemBootstrapResponse.fromPartial({ serverTime: new Date("2026-09-05T00:00:00Z"), uiPalette });
}

/** A promise the test settles by hand, for answers that arrive late. */
function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((resolveIt, rejectIt) => {
    resolve = resolveIt;
    reject = rejectIt;
  });
  return { promise, reject, resolve };
}

/** Lets every pending promise chain in the component settle inside `act`. */
function settle(): Promise<void> {
  return act(async () => {
    await new Promise((resolve) => setImmediate(resolve));
  });
}

interface Mounted {
  unmount(): Promise<void>;
}

const mounts = new Set<Mounted>();

async function mount(dataMode: SearchDataMode, apiBaseUrl = ""): Promise<Mounted> {
  const container = browser.document.body.appendChild(browser.document.createElement("div"));
  const root = createRoot(container as unknown as Element);
  const mounted: Mounted = {
    async unmount() {
      mounts.delete(mounted);
      await act(async () => root.unmount());
      container.parentNode?.removeChild(container);
    },
  };
  mounts.add(mounted);
  await act(async () => {
    root.render(<ThemeSync apiBaseUrl={apiBaseUrl} dataMode={dataMode} />);
  });
  await settle();
  return mounted;
}

/** A `storage` event as another tab's write delivers it, inside `act` so React may re-render. */
function storageEvent(key: string | null, newValue: string | null): Promise<void> {
  return act(async () => {
    browser.dispatchWindowEvent("storage", { key, newValue, oldValue: null, storageArea: null });
  });
}

function palette(): string | null {
  return html.getAttribute("data-palette");
}

function theme(): string | null {
  return html.getAttribute("data-theme");
}

function writesTo(name: string, since = 0): string[] {
  return attributeWrites.slice(since).filter(([written]) => written === name).map(([, value]) => value);
}

/** The listeners still registered on `window` for `type`. */
function listeners(type: string): number {
  return browser.windowListeners.get(type)?.size ?? 0;
}

test.beforeEach(() => {
  attributeWrites.length = 0;
  bootstrapAnswers = [];
  bootstrapCalls = 0;
  consoleCalls.length = 0;
  factoryCalls.length = 0;
  fetches = 0;
  unhandled.length = 0;
  browser.media.changeListeners.clear();
  browser.media.prefersDark = false;
  browser.media.queries.length = 0;
  browser.storage.clear();
  browser.storageBlocked = false;
  browser.storageWrites.length = 0;
  html.removeAttribute("data-palette");
  html.removeAttribute("data-theme");
  browser.chromeMeta.setAttribute("content", "first-paint");
});

test.afterEach(async () => {
  await Promise.all(Array.from(mounts, (mounted) => mounted.unmount()));
  // Whatever the test did, nothing was said and nothing was left dangling.
  assert.deepEqual(consoleCalls, []);
  assert.deepEqual(unhandled, []);
  assert.equal(fetches, 0, "the network was touched");
  assert.equal(listeners("storage"), 0, "a storage listener outlived its mount");
  assert.equal(browser.media.changeListeners.size, 0, "a media listener outlived its mount");
});

test("the demo export paints the cache and the system theme without building a client", async () => {
  browser.storage.set(PALETTE_STORAGE_KEY, "ember");
  browser.media.prefersDark = true;
  await mount("demo");
  assert.equal(theme(), "dark");
  assert.equal(palette(), "ember");
  assert.equal(browser.chromeMeta.getAttribute("content"), "bar(ember/dark)");
  assert.deepEqual(factoryCalls, []);
  assert.equal(bootstrapCalls, 0);
  // A sync never writes: the cache is the server's, not this component's.
  assert.deepEqual(browser.storageWrites, []);
  // The sync reads the query once and the follow registers on it once; no
  // other query is ever asked for.
  assert.deepEqual(browser.media.queries, [DARK_SCHEME_QUERY, DARK_SCHEME_QUERY]);
  assert.equal(browser.media.changeListeners.size, 1);
});

test("the demo export with blocked storage still paints a theme and classic", async () => {
  browser.storageBlocked = true;
  await mount("demo");
  assert.equal(theme(), "light");
  assert.equal(palette(), "classic");
  assert.deepEqual(factoryCalls, []);
});

test("against a backend the palette is fetched once through a client built for the API base URL", async () => {
  browser.storage.set(PALETTE_STORAGE_KEY, "ocean");
  bootstrapAnswers.push(() => Promise.resolve(bootstrap(UiPalette.UI_PALETTE_TERMINAL)));
  await mount("backend", "https://example.test/base");
  assert.deepEqual(factoryCalls, [{ baseUrl: "https://example.test/base" }]);
  assert.equal(bootstrapCalls, 1);
  // The cache painted first, then the live value replaced it and was cached.
  assert.deepEqual(writesTo("data-palette"), ["ocean", "terminal"]);
  assert.equal(palette(), "terminal");
  assert.equal(browser.storage.get(PALETTE_STORAGE_KEY), "terminal");
  assert.deepEqual(browser.storageWrites, [[PALETTE_STORAGE_KEY, "terminal"]]);
  assert.equal(browser.chromeMeta.getAttribute("content"), "bar(terminal/light)");
});

const BOOTSTRAP_FAILURES: ReadonlyArray<[string, () => Promise<GetSystemBootstrapResponse>]> = [
  ["a network error", () => Promise.reject(new TypeError("Failed to fetch"))],
  ["a server error", () => Promise.reject(new Error("HTTP 503: Service Unavailable"))],
  ["a throw before the promise", () => {
    throw new Error("the transport refused the route");
  }],
  ["a rejection with no Error", () => Promise.reject("unreachable")],
  ["a rejection with undefined", () => Promise.reject(undefined)],
];

for (const [label, failure] of BOOTSTRAP_FAILURES) {
  test(`${label} from bootstrap leaves the attribute and the cache untouched and logs nothing`, async () => {
    browser.storage.set(PALETTE_STORAGE_KEY, "glass");
    bootstrapAnswers = [failure];
    await mount("backend");
    assert.equal(bootstrapCalls, 1);
    assert.equal(palette(), "glass");
    assert.deepEqual(writesTo("data-palette"), ["glass"]);
    assert.deepEqual(browser.storageWrites, []);
    assert.equal(browser.storage.get(PALETTE_STORAGE_KEY), "glass");
    assert.deepEqual(consoleCalls, []);
    assert.deepEqual(unhandled, []);
  });
}

test("a rejected bootstrap with blocked storage paints classic and still says nothing", async () => {
  browser.storageBlocked = true;
  bootstrapAnswers.push(() => Promise.reject(new Error("backend away")));
  await mount("backend");
  assert.equal(palette(), "classic");
  assert.deepEqual(browser.storageWrites, []);
});

test("a palette a newer server names overwrites a stale cache with classic", async () => {
  browser.storage.set(PALETTE_STORAGE_KEY, "ocean");
  bootstrapAnswers.push(() => Promise.resolve(bootstrap(99 as UiPalette)));
  const mounted = await mount("backend");
  assert.equal(palette(), "classic");
  assert.equal(browser.storage.get(PALETTE_STORAGE_KEY), "classic");
  assert.deepEqual(browser.storageWrites, [[PALETTE_STORAGE_KEY, "classic"]]);
  // And so does a server without a settings service (UNSPECIFIED).
  await mounted.unmount();
  browser.storage.set(PALETTE_STORAGE_KEY, "ember");
  bootstrapAnswers.push(() => Promise.resolve(bootstrap(UiPalette.UI_PALETTE_UNSPECIFIED)));
  await mount("backend");
  assert.equal(palette(), "classic");
  assert.equal(browser.storage.get(PALETTE_STORAGE_KEY), "classic");
});

test("a bootstrap answer that arrives after unmount is dropped", async () => {
  browser.storage.set(PALETTE_STORAGE_KEY, "glass");
  const late = deferred<GetSystemBootstrapResponse>();
  bootstrapAnswers.push(() => late.promise);
  const mounted = await mount("backend");
  assert.equal(bootstrapCalls, 1);
  await mounted.unmount();
  late.resolve(bootstrap(UiPalette.UI_PALETTE_EMBER));
  await settle();
  assert.equal(palette(), "glass");
  assert.deepEqual(browser.storageWrites, []);
  assert.equal(browser.storage.get(PALETTE_STORAGE_KEY), "glass");
});

test("a storage event for the theme key repaints the theme and leaves the palette alone, and vice versa", async () => {
  browser.storage.set(PALETTE_STORAGE_KEY, "ocean");
  await mount("demo");
  assert.equal(theme(), "light");
  assert.equal(palette(), "ocean");

  // Another tab pins dark.
  let since = attributeWrites.length;
  browser.storage.set(THEME_STORAGE_KEY, "dark");
  await storageEvent(THEME_STORAGE_KEY, "dark");
  assert.equal(theme(), "dark");
  assert.equal(palette(), "ocean");
  assert.deepEqual(writesTo("data-palette", since), [], "a theme write repainted the palette");
  assert.deepEqual(writesTo("data-theme", since), ["dark"]);
  assert.equal(browser.chromeMeta.getAttribute("content"), "bar(ocean/dark)");
  // Pinned: the operating system is no longer followed.
  assert.equal(browser.media.changeListeners.size, 0);

  // Another tab's ThemeSync corrected the palette cache.
  since = attributeWrites.length;
  browser.storage.set(PALETTE_STORAGE_KEY, "ember");
  await storageEvent(PALETTE_STORAGE_KEY, "ember");
  assert.equal(palette(), "ember");
  assert.equal(theme(), "dark");
  assert.deepEqual(writesTo("data-theme", since), [], "a palette write repainted the theme");
  assert.deepEqual(writesTo("data-palette", since), ["ember"]);
  assert.equal(browser.chromeMeta.getAttribute("content"), "bar(ember/dark)");

  // A key neither axis owns is ignored by both.
  since = attributeWrites.length;
  browser.storage.set("open-splunk.other", "x");
  await storageEvent("open-splunk.other", "x");
  assert.deepEqual(attributeWrites.slice(since), []);

  // A palette event whose cache value is hostile paints classic, not the value.
  since = attributeWrites.length;
  browser.storage.set(PALETTE_STORAGE_KEY, "</script>");
  await storageEvent(PALETTE_STORAGE_KEY, "</script>");
  assert.equal(palette(), "classic");
  assert.equal(theme(), "dark");
  assert.deepEqual(writesTo("data-theme", since), []);

  // `localStorage.clear()` elsewhere: both keys are gone, so both axes fall back.
  since = attributeWrites.length;
  browser.storage.clear();
  await storageEvent(null, null);
  assert.equal(palette(), "classic");
  assert.equal(theme(), "light");
  assert.equal(browser.media.changeListeners.size, 1, "system is followed again");

  // The cache is never written by any of this.
  assert.deepEqual(browser.storageWrites, []);
});

test("the operating-system switch is followed only while the preference is system", async () => {
  browser.storage.set(PALETTE_STORAGE_KEY, "graphite");
  await mount("demo");
  assert.equal(theme(), "light");
  let since = attributeWrites.length;
  browser.media.prefersDark = true;
  await act(async () => browser.media.dispatchChange());
  assert.equal(theme(), "dark");
  assert.equal(palette(), "graphite");
  assert.deepEqual(writesTo("data-palette", since), [], "an OS switch repainted the palette");

  browser.storage.set(THEME_STORAGE_KEY, "light");
  await storageEvent(THEME_STORAGE_KEY, "light");
  assert.equal(theme(), "light");
  assert.equal(browser.media.changeListeners.size, 0);
  since = attributeWrites.length;
  browser.media.prefersDark = true;
  await act(async () => browser.media.dispatchChange());
  assert.deepEqual(attributeWrites.slice(since), [], "a pinned theme followed the OS");
  assert.equal(theme(), "light");
});

test("a live bootstrap and a cross-tab theme write interleave without touching each other's key", async () => {
  browser.storage.set(PALETTE_STORAGE_KEY, "classic");
  const late = deferred<GetSystemBootstrapResponse>();
  bootstrapAnswers.push(() => late.promise);
  await mount("backend");
  browser.storage.set(THEME_STORAGE_KEY, "dark");
  await storageEvent(THEME_STORAGE_KEY, "dark");
  assert.equal(theme(), "dark");
  assert.equal(palette(), "classic");
  late.resolve(bootstrap(UiPalette.UI_PALETTE_OCEAN));
  await settle();
  assert.equal(palette(), "ocean");
  assert.equal(theme(), "dark");
  assert.deepEqual(browser.storageWrites, [[PALETTE_STORAGE_KEY, "ocean"]]);
  assert.equal(browser.storage.get(THEME_STORAGE_KEY), "dark");
});

test.after(() => {
  (clientModule as unknown as { createOpenSplunkApiClient: Factory }).createOpenSplunkApiClient = realFactory;
  globalThis.fetch = realFetch;
  Object.assign(console, realConsole);
  process.off("unhandledRejection", onUnhandled);
  browser.uninstall();
});
