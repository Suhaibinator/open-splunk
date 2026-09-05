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
  GetServerSettingsResponse,
  UiPalette,
  type SearchLimits,
} from "@/gen/ts/open_splunk/server_settings_api";
import { GetSystemBootstrapResponse, ServerFeature } from "@/gen/ts/open_splunk/system_api";
import type { OpenSplunkApiClient } from "@/lib/api";
import { adaptSystemBootstrap } from "@/lib/api/system-bootstrap";
import type { Palette } from "@/lib/palettes";
import { fakeEvent, type FakeElement } from "@/lib/testing/fake-dom";

import { paletteOptionId } from "./appearance-form";
import { BackendServerSettings } from "./backend-admin-panels";

/*
 * The Server section mounted with both of its forms live. The console holds
 * one dirty flag for the section and asks it before discarding the view, so
 * what the section forwards has to be the OR of the two forms: true while
 * either has unapplied edits, false only once both are clean again.
 */

const reactErrors: unknown[][] = [];
const consoleError = console.error;
console.error = (...arguments_: unknown[]) => {
  reactErrors.push(arguments_);
};

const limits: SearchLimits = {
  maximumRuntime: { seconds: 120n, nanos: 0 },
  maximumMemoryBytes: 1n << 30n,
  maximumRowsToRead: 250_000_000n,
  maximumBytesToRead: 64n << 30n,
  maximumGroupedRows: 10_001n,
  maximumThreads: 4n,
  maximumResultRows: 10_000n,
  maximumResultBytes: 64n << 20n,
  maximumTotalResultBytes: 512n << 20n,
  maximumConcurrentSearches: 4,
  resultRetention: { seconds: 900n, nanos: 0 },
};
const minimums: SearchLimits = {
  ...limits,
  maximumRuntime: { seconds: 1n, nanos: 0 },
  maximumMemoryBytes: 1n << 20n,
  maximumRowsToRead: 1n,
  maximumBytesToRead: 1n << 20n,
  maximumGroupedRows: 1n,
  maximumThreads: 1n,
  maximumResultRows: 1n,
  maximumResultBytes: 1n << 20n,
  maximumTotalResultBytes: 1n << 20n,
  maximumConcurrentSearches: 1,
  resultRetention: { seconds: 60n, nanos: 0 },
};
const maximums: SearchLimits = {
  ...limits,
  maximumRuntime: { seconds: 3_600n, nanos: 0 },
  maximumMemoryBytes: 64n << 30n,
  maximumRowsToRead: 1_000_000_000n,
  maximumBytesToRead: 1n << 40n,
  maximumGroupedRows: 100_000_000n,
  maximumThreads: 64n,
  maximumResultRows: 1_000_000n,
  maximumResultBytes: 1n << 30n,
  maximumTotalResultBytes: 8n << 30n,
  maximumConcurrentSearches: 64,
  resultRetention: { seconds: 86_400n, nanos: 0 },
};

function fakeClient(): OpenSplunkApiClient {
  return {
    serverSettings: {
      get: () => Promise.resolve(GetServerSettingsResponse.fromPartial({
        current: { version: 1n, limits },
        defaults: limits,
        maximums,
        minimums,
      })),
      getAppearance: () => Promise.resolve(GetServerAppearanceResponse.fromPartial({
        current: { palette: UiPalette.UI_PALETTE_CLASSIC, version: 1n },
        defaultPalette: UiPalette.UI_PALETTE_CLASSIC,
      })),
    },
  } as unknown as OpenSplunkApiClient;
}

const editableBootstrap = adaptSystemBootstrap(GetSystemBootstrapResponse.fromPartial({
  features: [ServerFeature.SERVER_FEATURE_SERVER_SETTINGS_ADMIN],
  serverTime: new Date("2026-09-05T00:00:00Z"),
  uiPalette: UiPalette.UI_PALETTE_CLASSIC,
}));

function settle(): Promise<void> {
  return act(async () => {
    await new Promise((resolve) => setImmediate(resolve));
  });
}

interface Mounted {
  container: FakeElement;
  /** Every value forwarded to the console, in order. */
  forwarded: boolean[];
  unmount(): Promise<void>;
}

const mounts = new Set<Mounted>();

async function mount(): Promise<Mounted> {
  const container = browser.document.body.appendChild(browser.document.createElement("div"));
  const root = createRoot(container as unknown as Element);
  const forwarded: boolean[] = [];
  // A stable callback, as the console's `useState` setter is.
  const onDirtyChange = (dirty: boolean) => {
    forwarded.push(dirty);
  };
  const mounted: Mounted = {
    container,
    forwarded,
    async unmount() {
      mounts.delete(mounted);
      await act(async () => root.unmount());
      container.parentNode?.removeChild(container);
    },
  };
  mounts.add(mounted);
  await act(async () => {
    root.render(
      <BackendServerSettings
        bootstrap={editableBootstrap}
        client={fakeClient()}
        error={null}
        hecError={null}
        hecSnapshot={null}
        hecState="unavailable"
        onDirtyChange={onDirtyChange}
        onReload={() => {}}
        onStatus={() => {}}
      />,
    );
  });
  await settle();
  return mounted;
}

/** Types into a limits field the way a browser reports it: the property first, then the event. */
async function type(mounted: Mounted, id: string, value: string): Promise<void> {
  const input = mounted.container.querySelector(`#${id}`);
  assert.ok(input, `no input ${id}`);
  await act(async () => {
    input.value = value;
    input.dispatchEvent(fakeEvent("input"));
  });
}

async function choose(mounted: Mounted, palette: Palette): Promise<void> {
  const input = mounted.container.querySelector(`#${paletteOptionId(palette)}`);
  assert.ok(input, `no radio for ${palette}`);
  await act(async () => {
    input.checked = true;
    input.dispatchEvent(fakeEvent("click"));
  });
}

test.afterEach(() => Promise.all(Array.from(mounts, (mounted) => mounted.unmount())));

test.beforeEach(() => {
  reactErrors.length = 0;
  browser.storage.clear();
  browser.storageWrites.length = 0;
  browser.document.documentElement.setAttribute("data-palette", "classic");
});

test("the section forwards the OR of both forms and clears only once both are clean", async () => {
  const mounted = await mount();
  assert.deepEqual(reactErrors, []);
  assert.ok(mounted.container.querySelector("#search-limit-threads"), "the limits form did not mount");
  assert.ok(mounted.container.querySelector(`#${paletteOptionId("ocean")}`), "the appearance card did not mount");
  assert.equal(mounted.forwarded.at(-1), false);
  assert.ok(!mounted.forwarded.includes(true), "clean forms were reported dirty");

  // Limits alone.
  await type(mounted, "search-limit-threads", "8");
  assert.equal(mounted.forwarded.at(-1), true);

  // Both: nothing new is said, the flag is already up.
  let length = mounted.forwarded.length;
  await choose(mounted, "ocean");
  assert.equal(mounted.forwarded.length, length, "an already-dirty section re-announced itself");
  assert.equal(mounted.forwarded.at(-1), true);

  // Limits clean, appearance still dirty: the flag must not drop.
  length = mounted.forwarded.length;
  await type(mounted, "search-limit-threads", "4");
  assert.equal(mounted.forwarded.length, length, "cleaning one form while the other is dirty changed the flag");
  assert.equal(mounted.forwarded.at(-1), true);
  assert.equal(browser.dispatchWindowEvent("beforeunload").defaultPrevented, true);

  // Appearance clean too: now it drops. The effect's cleanup reports `false`
  // before the new value does, so the drop may be spelled twice, but nothing
  // in between ever says `true`.
  await choose(mounted, "classic");
  assert.equal(mounted.forwarded.at(-1), false);
  const drop = mounted.forwarded.slice(length);
  assert.ok(drop.length >= 1 && drop.every((value) => value === false), JSON.stringify(drop));
  assert.equal(browser.dispatchWindowEvent("beforeunload").defaultPrevented, false);

  // The other order: appearance first, then limits, then clean in the reverse order.
  await choose(mounted, "terminal");
  assert.equal(mounted.forwarded.at(-1), true);
  length = mounted.forwarded.length;
  await type(mounted, "search-limit-threads", "16");
  assert.equal(mounted.forwarded.length, length);
  await choose(mounted, "classic");
  assert.equal(mounted.forwarded.length, length, "cleaning the appearance while limits are dirty changed the flag");
  assert.equal(mounted.forwarded.at(-1), true);
  await type(mounted, "search-limit-threads", "4");
  assert.equal(mounted.forwarded.at(-1), false);

  // Unmounting a dirty section tells the console it is clean again.
  await choose(mounted, "glass");
  assert.equal(mounted.forwarded.at(-1), true);
  await mounted.unmount();
  assert.equal(mounted.forwarded.at(-1), false);
  assert.equal(browser.dispatchWindowEvent("beforeunload").defaultPrevented, false);
  assert.deepEqual(reactErrors, []);
});

test("an invalid limits value is dirty too, and the appearance preview does not clear it", async () => {
  const mounted = await mount();
  await type(mounted, "search-limit-threads", "not a number");
  assert.equal(mounted.forwarded.at(-1), true);
  const length = mounted.forwarded.length;
  await choose(mounted, "ember");
  await choose(mounted, "classic");
  assert.equal(mounted.forwarded.length, length);
  assert.equal(mounted.forwarded.at(-1), true);
  assert.deepEqual(reactErrors, []);
});

test.after(() => {
  console.error = consoleError;
  browser.uninstall();
});
