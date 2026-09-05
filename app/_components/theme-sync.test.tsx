import assert from "node:assert/strict";
import process from "node:process";
import test from "node:test";

// Load-bearing order: the fake browser has to be on `globalThis` before
// `react-dom/client` is evaluated, because that module decides at load time
// whether it has a DOM. Module evaluation follows import order.
import { browser } from "@/lib/testing/fake-browser";
import { act, StrictMode, type ReactNode } from "react";
import { createRoot } from "react-dom/client";

import { UiPalette } from "@/gen/ts/open_splunk/server_settings_api";
import { GetSystemBootstrapResponse } from "@/gen/ts/open_splunk/system_api";
import type { OpenSplunkApiClient } from "@/lib/api";
import * as clientModule from "@/lib/api/open-splunk-client";
import { getSystemBootstrap } from "@/lib/api/system-bootstrap";
import type { SearchDataMode } from "@/lib/search/backend-data";
import { DARK_SCHEME_QUERY, PALETTE_STORAGE_KEY, THEME_STORAGE_KEY } from "@/lib/theme-preference";

import { ProductShell } from "./product-shell";
import { InstancePaletteFetch, ThemeSync } from "./theme-sync";

/*
 * ThemeSync mounted for real, against a fake window whose storage, media
 * query and client factory the test steers. The demo export must never build
 * a client; ThemeSync itself never asks bootstrap, it paints whatever
 * envelope the page's own loader resolves, so a page with the product shell
 * makes one bootstrap request in total and the sign-in page's
 * InstancePaletteFetch makes the one it has; a backend that is away must
 * leave the cached palette standing and say nothing; and a cross-tab write
 * to one key must never repaint the other axis.
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

interface MountOptions {
  /** Mount `InstancePaletteFetch` beside ThemeSync, as the sign-in page does, against this API base URL. */
  fetch?: string;
  /** Mount the product shell beside ThemeSync, as every product page does, against this API base URL. */
  shell?: string;
  /** Wrap the tree in StrictMode, so every effect mounts, unmounts and mounts again. */
  strict?: boolean;
}

async function mount(dataMode: SearchDataMode, options: MountOptions = {}): Promise<Mounted> {
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
  let tree: ReactNode = (
    <>
      <ThemeSync dataMode={dataMode} />
      {options.fetch === undefined ? null : <InstancePaletteFetch apiBaseUrl={options.fetch} dataMode={dataMode} />}
      {options.shell === undefined ? null : (
        <ProductShell activeSection="home" apiBaseUrl={options.shell} appName="Search & Reporting" dataMode={dataMode}>
          <p>Page content</p>
        </ProductShell>
      )}
    </>
  );
  if (options.strict) tree = <StrictMode>{tree}</StrictMode>;
  await act(async () => {
    root.render(tree);
  });
  await settle();
  return mounted;
}

/** A sign-in page: ThemeSync from the layout plus the page's own palette request. */
function mountSignIn(apiBaseUrl = ""): Promise<Mounted> {
  return mount("backend", { fetch: apiBaseUrl });
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
  // One at a time: two `act` scopes open at once is what React calls
  // overlapping, and it says so on the console this hook then inspects.
  await Array.from(mounts).reduce(
    (chain, mounted) => chain.then(() => mounted.unmount()),
    Promise.resolve(),
  );
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
  // The sign-in page's fetcher is mounted too: the demo export never asks.
  await mount("demo", { fetch: "https://example.test/base" });
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

test("ThemeSync alone asks nothing of a backend: the palette rides the page's own bootstrap", async () => {
  browser.storage.set(PALETTE_STORAGE_KEY, "ocean");
  await mount("backend");
  assert.deepEqual(factoryCalls, []);
  assert.equal(bootstrapCalls, 0);
  assert.equal(palette(), "ocean");
  assert.deepEqual(browser.storageWrites, []);

  // A loader elsewhere on the page resolves the envelope: that is the paint.
  bootstrapAnswers.push(() => Promise.resolve(bootstrap(UiPalette.UI_PALETTE_TERMINAL)));
  const loaded = await act(async () => getSystemBootstrap(spyFactory({ baseUrl: "" })));
  assert.equal(loaded.palette, "terminal");
  assert.deepEqual(writesTo("data-palette"), ["ocean", "terminal"]);
  assert.equal(palette(), "terminal");
  assert.equal(browser.storage.get(PALETTE_STORAGE_KEY), "terminal");
  assert.deepEqual(browser.storageWrites, [[PALETTE_STORAGE_KEY, "terminal"]]);
  assert.equal(browser.chromeMeta.getAttribute("content"), "bar(terminal/light)");

  // A later envelope from any loader -- a console's refresh -- keeps it live.
  bootstrapAnswers.push(() => Promise.resolve(bootstrap(UiPalette.UI_PALETTE_GLASS)));
  await act(async () => getSystemBootstrap(spyFactory({ baseUrl: "" }), "ops"));
  assert.equal(palette(), "glass");
  assert.equal(browser.storage.get(PALETTE_STORAGE_KEY), "glass");
  assert.equal(bootstrapCalls, 2);
});

test("a page with the product shell makes exactly one bootstrap request, and the palette rides it", async () => {
  browser.storage.set(PALETTE_STORAGE_KEY, "ocean");
  bootstrapAnswers.push(() => Promise.resolve(GetSystemBootstrapResponse.fromPartial({
    apps: [{ appId: "ops", slug: "ops", displayName: "Operations", defaultIndexNames: ["main"], state: 0 }],
    selectedAppId: "ops",
    serverTime: new Date("2026-09-05T00:00:00Z"),
    uiPalette: UiPalette.UI_PALETTE_TERMINAL,
  })));
  await mount("backend", { shell: "https://example.test/base" });
  // One client, the shell's, and one request on it.
  assert.deepEqual(factoryCalls, [{ baseUrl: "https://example.test/base" }]);
  assert.equal(bootstrapCalls, 1);
  // The shell took its catalog from the envelope ...
  assert.match(browser.document.body.textContent, /Operations/u);
  // ... and ThemeSync took the palette from the same one.
  assert.deepEqual(writesTo("data-palette"), ["ocean", "terminal"]);
  assert.equal(palette(), "terminal");
  assert.equal(browser.storage.get(PALETTE_STORAGE_KEY), "terminal");
  assert.deepEqual(browser.storageWrites, [[PALETTE_STORAGE_KEY, "terminal"]]);
});

test("a page whose shell request fails keeps the cached palette and says nothing", async () => {
  browser.storage.set(PALETTE_STORAGE_KEY, "graphite");
  bootstrapAnswers.push(() => Promise.reject(new Error("HTTP 503: Service Unavailable")));
  await mount("backend", { shell: "" });
  assert.equal(bootstrapCalls, 1);
  assert.equal(palette(), "graphite");
  assert.deepEqual(writesTo("data-palette"), ["graphite"]);
  assert.deepEqual(browser.storageWrites, []);
});

test("the sign-in page fetches the palette once through a client built for the API base URL", async () => {
  browser.storage.set(PALETTE_STORAGE_KEY, "ocean");
  bootstrapAnswers.push(() => Promise.resolve(bootstrap(UiPalette.UI_PALETTE_TERMINAL)));
  await mountSignIn("https://example.test/base");
  assert.deepEqual(factoryCalls, [{ baseUrl: "https://example.test/base" }]);
  assert.equal(bootstrapCalls, 1);
  // The cache painted first, then the live value replaced it and was cached.
  assert.deepEqual(writesTo("data-palette"), ["ocean", "terminal"]);
  assert.equal(palette(), "terminal");
  assert.equal(browser.storage.get(PALETTE_STORAGE_KEY), "terminal");
  assert.deepEqual(browser.storageWrites, [[PALETTE_STORAGE_KEY, "terminal"]]);
  assert.equal(browser.chromeMeta.getAttribute("content"), "bar(terminal/light)");
});

test("StrictMode's double mount of the sign-in page makes one request, not two", async () => {
  // What a plain mount registers on `window`, for the comparison below.
  bootstrapAnswers.push(() => Promise.resolve(bootstrap(UiPalette.UI_PALETTE_OCEAN)));
  const plain = await mountSignIn();
  const plainStorageListeners = listeners("storage");
  const plainMediaListeners = browser.media.changeListeners.size;
  await plain.unmount();
  bootstrapCalls = 0;
  browser.storageWrites.length = 0;
  attributeWrites.length = 0;

  const late = deferred<GetSystemBootstrapResponse>();
  bootstrapAnswers.push(() => late.promise);
  await mount("backend", { fetch: "", strict: true });
  assert.equal(bootstrapCalls, 1, "the remount asked again");
  late.resolve(bootstrap(UiPalette.UI_PALETTE_EMBER));
  await settle();
  assert.equal(palette(), "ember");
  assert.deepEqual(writesTo("data-palette").filter((value) => value === "ember"), ["ember"]);
  assert.deepEqual(browser.storageWrites, [[PALETTE_STORAGE_KEY, "ember"]]);
  // StrictMode's extra mount left no extra listener behind either.
  assert.equal(listeners("storage"), plainStorageListeners);
  assert.equal(browser.media.changeListeners.size, plainMediaListeners);
});

test("a sign-in page mounted again after its request settled asks again", async () => {
  bootstrapAnswers.push(() => Promise.resolve(bootstrap(UiPalette.UI_PALETTE_OCEAN)));
  const first = await mountSignIn();
  assert.equal(palette(), "ocean");
  await first.unmount();
  // The administrator changed it in between; the next visit must see that.
  bootstrapAnswers.push(() => Promise.resolve(bootstrap(UiPalette.UI_PALETTE_EMBER)));
  await mountSignIn();
  assert.equal(bootstrapCalls, 2);
  assert.equal(palette(), "ember");
});

test("two sign-in fetchers for different API base URLs each ask their own backend", async () => {
  bootstrapAnswers.push(() => Promise.resolve(bootstrap(UiPalette.UI_PALETTE_OCEAN)));
  bootstrapAnswers.push(() => Promise.resolve(bootstrap(UiPalette.UI_PALETTE_EMBER)));
  await mountSignIn("https://one.test");
  await mountSignIn("https://two.test");
  assert.deepEqual(factoryCalls, [{ baseUrl: "https://one.test" }, { baseUrl: "https://two.test" }]);
  assert.equal(bootstrapCalls, 2);
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
    await mountSignIn();
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
  await mountSignIn();
  assert.equal(palette(), "classic");
  assert.deepEqual(browser.storageWrites, []);
});

test("a palette a newer server names overwrites a stale cache with classic", async () => {
  browser.storage.set(PALETTE_STORAGE_KEY, "ocean");
  bootstrapAnswers.push(() => Promise.resolve(bootstrap(99 as UiPalette)));
  const mounted = await mountSignIn();
  assert.equal(palette(), "classic");
  assert.equal(browser.storage.get(PALETTE_STORAGE_KEY), "classic");
  assert.deepEqual(browser.storageWrites, [[PALETTE_STORAGE_KEY, "classic"]]);
  // And so does a server without a settings service (UNSPECIFIED).
  await mounted.unmount();
  browser.storage.set(PALETTE_STORAGE_KEY, "ember");
  bootstrapAnswers.push(() => Promise.resolve(bootstrap(UiPalette.UI_PALETTE_UNSPECIFIED)));
  await mountSignIn();
  assert.equal(palette(), "classic");
  assert.equal(browser.storage.get(PALETTE_STORAGE_KEY), "classic");
});

test("a bootstrap answer that arrives after ThemeSync unmounts is dropped", async () => {
  browser.storage.set(PALETTE_STORAGE_KEY, "glass");
  const late = deferred<GetSystemBootstrapResponse>();
  bootstrapAnswers.push(() => late.promise);
  const mounted = await mountSignIn();
  assert.equal(bootstrapCalls, 1);
  await mounted.unmount();
  late.resolve(bootstrap(UiPalette.UI_PALETTE_EMBER));
  await settle();
  assert.equal(palette(), "glass");
  assert.deepEqual(browser.storageWrites, []);
  assert.equal(browser.storage.get(PALETTE_STORAGE_KEY), "glass");
  // The settled request is forgotten, so the next sign-in mount asks afresh.
  bootstrapAnswers.push(() => Promise.resolve(bootstrap(UiPalette.UI_PALETTE_OCEAN)));
  await mountSignIn();
  assert.equal(bootstrapCalls, 2);
  assert.equal(palette(), "ocean");
});

test("a sign-in request that outlives its page still reaches the ThemeSync of the same document", async () => {
  // The user signs in while the palette request is in flight: the sign-in
  // page unmounts, the layout's ThemeSync stays. The live answer is still the
  // server's palette for this document, so it paints.
  browser.storage.set(PALETTE_STORAGE_KEY, "glass");
  const late = deferred<GetSystemBootstrapResponse>();
  bootstrapAnswers.push(() => late.promise);
  await mount("backend");
  const signIn = await mountSignIn();
  await signIn.unmount();
  late.resolve(bootstrap(UiPalette.UI_PALETTE_EMBER));
  await settle();
  assert.equal(palette(), "ember");
  assert.equal(browser.storage.get(PALETTE_STORAGE_KEY), "ember");
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
  await mountSignIn();
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
