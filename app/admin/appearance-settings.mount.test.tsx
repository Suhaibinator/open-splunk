import assert from "node:assert/strict";
import test from "node:test";

// Load-bearing order: the fake browser has to be on `globalThis` before
// `react-dom/client` is evaluated, because that module decides at load time
// whether it has a DOM. Module evaluation follows import order.
import { browser } from "@/lib/testing/fake-browser";
import { act } from "react";
import { createRoot } from "react-dom/client";

import {
  GetServerAppearanceResponse,
  UiPalette,
  UpdateServerAppearanceResponse,
  type UpdateServerAppearanceRequest,
} from "@/gen/ts/open_splunk/server_settings_api";
import { HttpError, type OpenSplunkApiClient } from "@/lib/api";
import type { Palette } from "@/lib/palettes";
import { fakeEvent, type FakeElement } from "@/lib/testing/fake-dom";
import { PALETTE_STORAGE_KEY } from "@/lib/theme-preference";

import { paletteOptionId } from "./appearance-form";
import { AppearanceSettings } from "./appearance-settings";

/*
 * The card mounted for real: effects run, handlers fire, and the document is
 * inspected after each step. The markup tests beside this file render
 * statically and so cannot see the behaviours the card exists for -- a
 * preview that paints this document and nothing else, a restore on unmount,
 * and one branch for a 409 and another for every other failure.
 */
browser.computedStyle.set("--chrome-bar", (html) => `bar(${html.getAttribute("data-palette")})`);

/**
 * Everything React logs while a card is mounted. React reports a misuse
 * (an update outside `act`, a controlled input without a handler) through
 * `console.error`, so each test asserts this stayed empty.
 */
const reactErrors: unknown[][] = [];
const consoleError = console.error;
console.error = (...arguments_: unknown[]) => {
  reactErrors.push(arguments_);
};

const PROTO: Record<Palette, UiPalette> = {
  classic: UiPalette.UI_PALETTE_CLASSIC,
  ember: UiPalette.UI_PALETTE_EMBER,
  glass: UiPalette.UI_PALETTE_GLASS,
  graphite: UiPalette.UI_PALETTE_GRAPHITE,
  ocean: UiPalette.UI_PALETTE_OCEAN,
  terminal: UiPalette.UI_PALETTE_TERMINAL,
};

function envelope(palette: Palette, version: bigint): GetServerAppearanceResponse {
  return GetServerAppearanceResponse.fromPartial({
    current: { palette: PROTO[palette], version },
    defaultPalette: UiPalette.UI_PALETTE_CLASSIC,
  });
}

interface FakeServer {
  client: OpenSplunkApiClient;
  /** Appended on every request, in order. */
  requests: Array<["get"] | ["update", UpdateServerAppearanceRequest]>;
  /** The next answers, consumed front to back; a rejected promise is an answer too. */
  gets: Array<() => Promise<GetServerAppearanceResponse>>;
  updates: Array<() => Promise<UpdateServerAppearanceResponse>>;
}

function fakeServer(): FakeServer {
  const server: FakeServer = { client: null as unknown as OpenSplunkApiClient, gets: [], requests: [], updates: [] };
  server.client = {
    serverSettings: {
      getAppearance() {
        server.requests.push(["get"]);
        const next = server.gets.shift();
        assert.ok(next, "unexpected getAppearance");
        return next();
      },
      updateAppearance(request: UpdateServerAppearanceRequest) {
        server.requests.push(["update", request]);
        const next = server.updates.shift();
        assert.ok(next, "unexpected updateAppearance");
        return next();
      },
    },
  } as unknown as OpenSplunkApiClient;
  return server;
}

interface Mounted {
  container: FakeElement;
  /** Every `onDirtyChange` call in order; an effect re-run reports `false` from its cleanup first. */
  dirty: boolean[];
  statuses: Array<[string, "success" | "warning"]>;
  choose(palette: Palette): Promise<void>;
  click(label: string): Promise<void>;
  radio(palette: Palette): FakeElement;
  button(label: string): FakeElement;
  submit(): Promise<void>;
  unmount(): Promise<void>;
}

/** Cards still mounted; a test that fails halfway must not leave its listeners for the next one. */
const mounts = new Set<Mounted>();

/** Lets every pending promise chain in the component settle inside `act`. */
function settle(): Promise<void> {
  return act(async () => {
    await new Promise((resolve) => setImmediate(resolve));
  });
}

async function mount(server: FakeServer): Promise<Mounted> {
  const container = browser.document.body.appendChild(browser.document.createElement("div"));
  const root = createRoot(container as unknown as Element);
  const mounted: Mounted = {
    button(label) {
      const button = container.querySelectorAll("button").find((candidate) => candidate.textContent === label);
      assert.ok(button, `no button labelled ${label}`);
      return button;
    },
    async choose(palette) {
      const input = mounted.radio(palette);
      await act(async () => {
        // A browser checks the radio before it dispatches the click.
        input.checked = true;
        input.dispatchEvent(fakeEvent("click"));
      });
    },
    async click(label) {
      const button = mounted.button(label);
      await act(async () => {
        button.dispatchEvent(fakeEvent("click"));
      });
    },
    container,
    dirty: [],
    radio(palette) {
      const input = container.querySelector(`#${paletteOptionId(palette)}`);
      assert.ok(input, `no radio for ${palette}`);
      return input;
    },
    statuses: [],
    async submit() {
      const form = container.querySelector("form");
      assert.ok(form, "no form mounted");
      await act(async () => {
        form.dispatchEvent(fakeEvent("submit"));
      });
      await settle();
    },
    async unmount() {
      mounts.delete(mounted);
      await act(async () => root.unmount());
      container.parentNode?.removeChild(container);
    },
  };
  mounts.add(mounted);
  await act(async () => {
    root.render(
      <AppearanceSettings
        client={server.client}
        onDirtyChange={(dirty) => mounted.dirty.push(dirty)}
        onStatus={(message, kind) => mounted.statuses.push([message, kind])}
      />,
    );
  });
  await settle();
  return mounted;
}

function paletteOnDocument(): string | null {
  return browser.document.documentElement.getAttribute("data-palette");
}

function checkedRadios(container: FakeElement): string[] {
  return container.querySelectorAll('input[type="radio"]').filter((input) => input.checked).map((input) => input.value);
}

test.afterEach(() => Promise.all(Array.from(mounts, (mounted) => mounted.unmount())));

test.beforeEach(() => {
  reactErrors.length = 0;
  browser.storage.clear();
  browser.storageWrites.length = 0;
  browser.document.documentElement.setAttribute("data-palette", "classic");
  browser.chromeMeta.setAttribute("content", "first-paint");
});

test("loading paints and caches the saved palette; a click previews on this document only", async () => {
  const server = fakeServer();
  server.gets.push(() => Promise.resolve(envelope("classic", 3n)));
  const mounted = await mount(server);
  assert.deepEqual(reactErrors, []);
  assert.deepEqual(server.requests, [["get"]]);
  assert.deepEqual(checkedRadios(mounted.container), ["classic"]);
  assert.equal(paletteOnDocument(), "classic");
  assert.deepEqual(browser.storageWrites, [[PALETTE_STORAGE_KEY, "classic"]]);
  assert.equal(browser.chromeMeta.getAttribute("content"), "bar(classic)");
  assert.deepEqual(mounted.dirty, [false]);
  assert.equal(browser.dispatchWindowEvent("beforeunload").defaultPrevented, false);

  await mounted.choose("terminal");
  assert.deepEqual(checkedRadios(mounted.container), ["terminal"]);
  assert.equal(paletteOnDocument(), "terminal");
  assert.equal(browser.chromeMeta.getAttribute("content"), "bar(terminal)");
  // The cache is what other tabs and the next boot read: a preview never
  // reaches it.
  assert.equal(browser.storage.get(PALETTE_STORAGE_KEY), "classic");
  assert.deepEqual(browser.storageWrites, [[PALETTE_STORAGE_KEY, "classic"]]);
  assert.equal(mounted.dirty.at(-1), true);
  assert.equal(mounted.button("Apply").disabled, false);
  // Leaving the page with a preview showing is guarded.
  assert.equal(browser.dispatchWindowEvent("beforeunload").defaultPrevented, true);

  await mounted.choose("ocean");
  assert.equal(paletteOnDocument(), "ocean");
  assert.deepEqual(browser.storageWrites, [[PALETTE_STORAGE_KEY, "classic"]]);

  await mounted.click("Reset to default");
  assert.deepEqual(checkedRadios(mounted.container), ["classic"]);
  assert.equal(paletteOnDocument(), "classic");
  assert.equal(mounted.dirty.at(-1), false);
  assert.equal(mounted.button("Apply").disabled, true);
  assert.equal(browser.dispatchWindowEvent("beforeunload").defaultPrevented, false);
  await mounted.unmount();
});

test("unmounting with a preview showing restores the saved palette", async () => {
  const server = fakeServer();
  server.gets.push(() => Promise.resolve(envelope("ocean", 7n)));
  const mounted = await mount(server);
  assert.equal(paletteOnDocument(), "ocean");
  await mounted.choose("graphite");
  assert.equal(paletteOnDocument(), "graphite");
  assert.equal(browser.chromeMeta.getAttribute("content"), "bar(graphite)");

  await mounted.unmount();
  assert.equal(paletteOnDocument(), "ocean");
  assert.equal(browser.chromeMeta.getAttribute("content"), "bar(ocean)");
  assert.equal(browser.storage.get(PALETTE_STORAGE_KEY), "ocean");
  assert.ok(browser.storageWrites.every(([, value]) => value === "ocean"), JSON.stringify(browser.storageWrites));
  // The console's dirty flag and the unload guard go with the card.
  assert.equal(mounted.dirty.at(-1), false);
  assert.equal(browser.dispatchWindowEvent("beforeunload").defaultPrevented, false);
  assert.deepEqual(reactErrors, []);
});

test("applying writes the choice under the loaded version and makes it the instance palette", async () => {
  const server = fakeServer();
  server.gets.push(() => Promise.resolve(envelope("classic", 3n)));
  server.updates.push(() => Promise.resolve(UpdateServerAppearanceResponse.fromPartial(envelope("ember", 4n))));
  const mounted = await mount(server);
  await mounted.choose("ember");
  await mounted.submit();

  assert.deepEqual(server.requests, [
    ["get"],
    ["update", { expectedVersion: 3n, palette: UiPalette.UI_PALETTE_EMBER }],
  ]);
  assert.equal(paletteOnDocument(), "ember");
  assert.equal(browser.storage.get(PALETTE_STORAGE_KEY), "ember");
  assert.deepEqual(browser.storageWrites, [[PALETTE_STORAGE_KEY, "classic"], [PALETTE_STORAGE_KEY, "ember"]]);
  assert.deepEqual(mounted.statuses, [["Palette set to Ember. Every user sees it on their next page load.", "success"]]);
  assert.deepEqual(checkedRadios(mounted.container), ["ember"]);
  assert.equal(mounted.button("Apply").disabled, true);
  assert.equal(mounted.dirty.at(-1), false);
  assert.equal(browser.dispatchWindowEvent("beforeunload").defaultPrevented, false);

  // The next apply names the version the update returned.
  server.updates.push(() => Promise.resolve(UpdateServerAppearanceResponse.fromPartial(envelope("classic", 5n))));
  await mounted.click("Reset to default");
  await mounted.submit();
  assert.deepEqual(server.requests.at(-1), ["update", { expectedVersion: 4n, palette: UiPalette.UI_PALETTE_CLASSIC }]);
  assert.equal(browser.storage.get(PALETTE_STORAGE_KEY), "classic");
  assert.deepEqual(reactErrors, []);
  await mounted.unmount();
});

test("a 409 takes the preview back, reloads the server's palette and warns", async () => {
  const server = fakeServer();
  server.gets.push(() => Promise.resolve(envelope("classic", 3n)));
  server.updates.push(() => Promise.reject(new HttpError({ message: "version conflict", status: 409, url: "/api/server/appearance" })));
  server.gets.push(() => Promise.resolve(envelope("glass", 4n)));
  const mounted = await mount(server);
  await mounted.choose("terminal");
  await mounted.submit();

  assert.deepEqual(server.requests.map(([kind]) => kind), ["get", "update", "get"]);
  assert.equal(paletteOnDocument(), "glass");
  assert.equal(browser.storage.get(PALETTE_STORAGE_KEY), "glass");
  // Restore first, then the reload: the preview is gone before the new
  // value arrives, and the cache never held terminal.
  assert.deepEqual(browser.storageWrites, [
    [PALETTE_STORAGE_KEY, "classic"],
    [PALETTE_STORAGE_KEY, "classic"],
    [PALETTE_STORAGE_KEY, "glass"],
  ]);
  assert.deepEqual(checkedRadios(mounted.container), ["glass"]);
  assert.equal(mounted.button("Apply").disabled, true);
  assert.deepEqual(mounted.statuses, [[
    "The palette changed on the server. The latest version was reloaded; review it before applying again.",
    "warning",
  ]]);

  // The reloaded version is the one the next apply must name.
  server.updates.push(() => Promise.resolve(UpdateServerAppearanceResponse.fromPartial(envelope("ocean", 5n))));
  await mounted.choose("ocean");
  await mounted.submit();
  assert.deepEqual(server.requests.at(-1), ["update", { expectedVersion: 4n, palette: UiPalette.UI_PALETTE_OCEAN }]);
  assert.deepEqual(reactErrors, []);
  await mounted.unmount();
});

test("a 409 whose reload also fails still takes the preview back and says so", async () => {
  const server = fakeServer();
  server.gets.push(() => Promise.resolve(envelope("classic", 3n)));
  server.updates.push(() => Promise.reject(new HttpError({ message: "version conflict", status: 409, url: "/api/server/appearance" })));
  server.gets.push(() => Promise.reject(new Error("backend away")));
  const mounted = await mount(server);
  await mounted.choose("ember");
  await mounted.submit();

  assert.equal(paletteOnDocument(), "classic");
  assert.equal(browser.storage.get(PALETTE_STORAGE_KEY), "classic");
  assert.deepEqual(mounted.statuses, [[
    "The palette changed on the server, and the latest version could not be reloaded.",
    "warning",
  ]]);
  assert.equal(mounted.container.querySelector("form"), null);
  assert.match(mounted.container.textContent, /Appearance could not be loaded/u);
  await mounted.unmount();
  assert.equal(paletteOnDocument(), "classic");
});

test("any other failure keeps the preview and the selection for another try", async () => {
  const server = fakeServer();
  server.gets.push(() => Promise.resolve(envelope("classic", 3n)));
  server.updates.push(() => Promise.reject(new HttpError({ message: "forbidden", status: 403, url: "/api/server/appearance" })));
  const mounted = await mount(server);
  await mounted.choose("terminal");
  await mounted.submit();

  assert.deepEqual(server.requests.map(([kind]) => kind), ["get", "update"]);
  assert.equal(paletteOnDocument(), "terminal");
  assert.equal(browser.storage.get(PALETTE_STORAGE_KEY), "classic");
  assert.deepEqual(checkedRadios(mounted.container), ["terminal"]);
  assert.equal(mounted.button("Apply").disabled, false);
  // The failure's own message is the toast; the fallback copy is for an
  // error that has none.
  assert.deepEqual(mounted.statuses, [["forbidden", "warning"]]);
  assert.equal(browser.dispatchWindowEvent("beforeunload").defaultPrevented, true);

  // Apply once more: the same version, the same choice.
  server.updates.push(() => Promise.resolve(UpdateServerAppearanceResponse.fromPartial(envelope("terminal", 4n))));
  await mounted.submit();
  assert.deepEqual(server.requests.at(-1), ["update", { expectedVersion: 3n, palette: UiPalette.UI_PALETTE_TERMINAL }]);
  assert.equal(browser.storage.get(PALETTE_STORAGE_KEY), "terminal");
  assert.deepEqual(reactErrors, []);
  await mounted.unmount();
});

test("a load failure paints nothing and unmounting restores nothing", async () => {
  const server = fakeServer();
  server.gets.push(() => Promise.reject(new Error("backend away")));
  browser.document.documentElement.setAttribute("data-palette", "ocean");
  const mounted = await mount(server);
  assert.match(mounted.container.textContent, /Appearance could not be loaded/u);
  assert.equal(paletteOnDocument(), "ocean");
  assert.deepEqual(browser.storageWrites, []);

  // Retry reaches the server again and adopts what it says.
  server.gets.push(() => Promise.resolve(envelope("graphite", 2n)));
  await mounted.click("Retry");
  await settle();
  assert.equal(paletteOnDocument(), "graphite");
  assert.deepEqual(checkedRadios(mounted.container), ["graphite"]);
  await mounted.unmount();
  assert.equal(paletteOnDocument(), "graphite");
  assert.deepEqual(reactErrors, []);
});

test.after(() => {
  console.error = consoleError;
  browser.uninstall();
});
