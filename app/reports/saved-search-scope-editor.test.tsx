import assert from "node:assert/strict";
import test from "node:test";

import { browser } from "@/lib/testing/fake-browser";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";

import { SharingScope } from "@/gen/ts/open_splunk/common";
import { SavedSearch } from "@/gen/ts/open_splunk/saved_search";
import type { UpdateSavedSearchRequest } from "@/gen/ts/open_splunk/saved_search_api";
import { SearchResultTab } from "@/gen/ts/open_splunk/search";
import { ServerFeature } from "@/gen/ts/open_splunk/system_api";
import {
  HttpError,
  OpenSplunkApiClient,
  type SystemBootstrapModel,
} from "@/lib/api";
import type { ProtobufRoute, ProtobufTransport } from "@/lib/api/protobuf-transport";
import { adaptSavedSearch, type ServerSavedSearch } from "@/lib/search/server-objects";
import { FakeElement, fakeEvent } from "@/lib/testing/fake-dom";

import { SavedSearchScopeEditor } from "./saved-search-scope-editor";

(globalThis as unknown as { HTMLElement: typeof FakeElement }).HTMLElement = FakeElement;
Object.defineProperties(FakeElement.prototype, {
  children: {
    get(this: FakeElement) {
      return this.childNodes.filter((child): child is FakeElement => child instanceof FakeElement);
    },
  },
  classList: {
    get(this: FakeElement) {
      return {
        contains: (className: string) => (this.getAttribute("class") ?? "").split(/\s+/u).includes(className),
      };
    },
  },
  parentElement: {
    get(this: FakeElement) {
      return this.parentNode instanceof FakeElement ? this.parentNode : null;
    },
  },
});
const fakeWindow = window as unknown as {
  cancelAnimationFrame(id: number): void;
  requestAnimationFrame(callback: FrameRequestCallback): number;
};
fakeWindow.requestAnimationFrame = () => 1;
fakeWindow.cancelAnimationFrame = () => {};
(FakeElement.prototype as FakeElement & { scrollIntoView(): void }).scrollIntoView = () => {};
(globalThis as typeof globalThis & {
  cancelAnimationFrame(id: number): void;
  requestAnimationFrame(callback: FrameRequestCallback): number;
}).requestAnimationFrame = () => 1;
(globalThis as typeof globalThis & {
  cancelAnimationFrame(id: number): void;
}).cancelAnimationFrame = () => {};

const reactErrors: unknown[][] = [];
const consoleError = console.error;
console.error = (...arguments_: unknown[]) => reactErrors.push(arguments_);

function bootstrap(): SystemBootstrapModel {
  return {
    apps: [],
    build: null,
    features: new Set([ServerFeature.SERVER_FEATURE_SAVED_SEARCHES]),
    indexes: [],
    limits: {
      defaultSearchTimeoutMs: 0,
      maximumExportBytes: 0n,
      maximumExportRows: 0n,
      maximumFieldSummaryValues: 0,
      maximumPageSize: 100,
      maximumPreviewRows: 0,
      maximumTimelineBuckets: 0,
      maximumWebsocketFrameBytes: 0n,
      maximumWebsocketSubscriptions: 0,
      searchResultRetentionMs: 0,
    },
    palette: "classic",
    searchWebsocketPath: null,
    selectedAppId: "search",
    serverTime: new Date("2026-09-12T00:00:00Z"),
  };
}

function saved(
  scope: SharingScope,
  version: bigint,
  ownerId = "current-user",
  appId: string | undefined = "search",
): SavedSearch {
  return SavedSearch.fromPartial({
    definition: {
      description: "Operational errors",
      name: "Errors",
      ownerId,
      schedule: {
        configVersion: 2n,
        cron: "*/5 * * * *",
        dispatchTtl: "2p",
        enabled: true,
        timezone: "UTC",
      },
      search: {
        appId,
        indexScope: ["main"],
        preferredResultTab: SearchResultTab.SEARCH_RESULT_TAB_EVENTS,
        selectedFields: ["host"],
        spl: "index=main | fields host",
        timeRange: { earliest: "-15m", latest: "now", timezone: "UTC" },
      },
      sharingScope: scope,
    },
    savedSearchId: "saved-1",
    version,
  });
}

interface FakeServer {
  ambiguousNext: boolean;
  client: OpenSplunkApiClient;
  current: SavedSearch;
  getFailure: unknown | null;
  gets: number;
  updateFailure: unknown | null;
  updates: UpdateSavedSearchRequest[];
}

function fakeServer(initial = saved(SharingScope.SHARING_SCOPE_PRIVATE, 1n)): FakeServer {
  const server: FakeServer = {
    ambiguousNext: false,
    client: null as unknown as OpenSplunkApiClient,
    current: initial,
    getFailure: null,
    gets: 0,
    updateFailure: null,
    updates: [],
  };
  server.client = new OpenSplunkApiClient({
    post(route: ProtobufRoute<unknown, unknown>, request: UpdateSavedSearchRequest | { savedSearchId: string }) {
      if (route.path.endsWith("/get")) {
        server.gets += 1;
        if (server.getFailure !== null) {
          const failure = server.getFailure;
          server.getFailure = null;
          return Promise.reject(failure);
        }
        return Promise.resolve({ savedSearch: server.current });
      }
      const update = request as UpdateSavedSearchRequest;
      server.updates.push(update);
      if (update.expectedVersion !== server.current.version) {
        return Promise.reject(new HttpError({
          message: "saved search version conflict",
          status: 409,
          url: route.path,
        }));
      }
      if (server.updateFailure !== null) {
        const failure = server.updateFailure;
        server.updateFailure = null;
        return Promise.reject(failure);
      }
      server.current = SavedSearch.fromPartial({
        ...server.current,
        definition: {
          ...server.current.definition,
          sharingScope: update.definition?.sharingScope,
        },
        version: server.current.version + 1n,
      });
      if (server.ambiguousNext) {
        server.ambiguousNext = false;
        return Promise.reject(new Error("connection closed before the response"));
      }
      return Promise.resolve({ savedSearch: server.current });
    },
  } as unknown as ProtobufTransport);
  return server;
}

interface MountedEditor {
  closes: number;
  container: FakeElement;
  notices: string[];
  root: Root;
  updated: ServerSavedSearch[];
}

const mounts = new Set<MountedEditor>();

function settle(): Promise<void> {
  return act(async () => {
    await new Promise((resolve) => setImmediate(resolve));
  });
}

async function mountEditor(server: FakeServer, baseline = adaptSavedSearch(server.current)): Promise<MountedEditor> {
  const container = browser.document.body.appendChild(browser.document.createElement("div"));
  const root = createRoot(container as unknown as Element);
  const mounted: MountedEditor = { closes: 0, container, notices: [], root, updated: [] };
  mounts.add(mounted);
  await act(async () => root.render(
    <SavedSearchScopeEditor
      bootstrap={bootstrap()}
      client={server.client}
      savedSearch={baseline}
      onClose={() => { mounted.closes += 1; }}
      onNotice={(message) => mounted.notices.push(message)}
      onUpdated={(updated) => mounted.updated.push(updated)}
    />,
  ));
  await settle();
  return mounted;
}

function button(container: FakeElement, label: string): FakeElement {
  const candidate = container.querySelectorAll("button").find((item) => item.textContent === label);
  assert.ok(candidate, `no button labelled ${label}`);
  return candidate;
}

async function choose(mounted: MountedEditor, label: string): Promise<void> {
  await act(async () => {
    mounted.container.querySelector("#reports-sharing-scope")?.dispatchEvent(fakeEvent("click"));
  });
  await act(async () => button(mounted.container, label).dispatchEvent(fakeEvent("click")));
}

async function submit(mounted: MountedEditor, twice = false): Promise<void> {
  const form = mounted.container.querySelector("#reports-edit-sharing-scope");
  assert.ok(form);
  await act(async () => {
    form.dispatchEvent(fakeEvent("submit"));
    if (twice) form.dispatchEvent(fakeEvent("submit"));
  });
  await settle();
}

test.beforeEach(() => {
  reactErrors.length = 0;
});

test.afterEach(async () => {
  const currentMounts = [...mounts];
  mounts.clear();
  await act(async () => {
    for (const mounted of currentMounts) mounted.root.unmount();
  });
  for (const mounted of currentMounts) {
    mounted.container.parentNode?.removeChild(mounted.container);
  }
  assert.deepEqual(reactErrors, []);
});

test.after(() => {
  console.error = consoleError;
});

test("two editors preserve the second proposal across conflict and require an explicit retry", async () => {
  const server = fakeServer();
  const original = adaptSavedSearch(server.current);
  const first = await mountEditor(server, original);
  const second = await mountEditor(server, original);

  await choose(first, "App");
  await submit(first, true);
  assert.equal(server.updates.length, 1, "the submit latch admits one request");
  assert.equal(server.current.definition?.sharingScope, SharingScope.SHARING_SCOPE_APP);

  await choose(second, "Global");
  await submit(second);
  assert.equal(server.updates.length, 2);
  assert.equal(server.gets, 1);
  assert.equal(server.current.definition?.sharingScope, SharingScope.SHARING_SCOPE_APP);
  assert.equal(button(second.container, "Submit again").disabled, false);
  assert.match(second.container.textContent, /proposed Global value is preserved/u);

  await submit(second);
  assert.equal(server.updates.length, 3);
  assert.equal(server.updates[2]?.expectedVersion, 2n);
  assert.deepEqual(server.updates[2]?.updateMask, ["sharing_scope"]);
  assert.equal(server.current.definition?.sharingScope, SharingScope.SHARING_SCOPE_GLOBAL);
  assert.equal(second.updated.at(-1)?.search.spl, original.search.spl);
  assert.equal(second.updated.at(-1)?.schedule?.cron, original.schedule?.cron);
});

test("an ambiguous response is reconciled by GET before success is shown", async () => {
  const server = fakeServer();
  server.ambiguousNext = true;
  const mounted = await mountEditor(server);
  await choose(mounted, "Global");
  await submit(mounted);

  assert.equal(server.updates.length, 1);
  assert.equal(server.gets, 1);
  assert.equal(mounted.updated.at(-1)?.sharingScope, SharingScope.SHARING_SCOPE_GLOBAL);
  assert.equal(mounted.closes, 1);
  assert.match(mounted.notices.at(-1) ?? "", /confirmed as Global after reconnecting/u);
});

test("an unchanged GET retains the proposal and waits for an explicit retry", async () => {
  const server = fakeServer();
  server.updateFailure = new Error("connection ended before acceptance was known");
  const mounted = await mountEditor(server);
  await choose(mounted, "Global");
  await submit(mounted);

  assert.equal(server.updates.length, 1);
  assert.equal(server.gets, 1);
  assert.equal(server.current.definition?.sharingScope, SharingScope.SHARING_SCOPE_PRIVATE);
  assert.equal(mounted.updated.length, 0);
  assert.equal(mounted.closes, 0);
  assert.match(mounted.container.textContent, /proposed Global value is preserved/u);
  assert.equal(button(mounted.container, "Submit again").disabled, false);

  await submit(mounted);
  assert.equal(server.updates.length, 2);
  assert.equal(server.current.definition?.sharingScope, SharingScope.SHARING_SCOPE_GLOBAL);
});

test("a conflict whose latest value matches the proposal still requires a second submit", async () => {
  const server = fakeServer();
  const mounted = await mountEditor(server);
  server.current = saved(SharingScope.SHARING_SCOPE_GLOBAL, 2n);
  await choose(mounted, "Global");
  await submit(mounted);

  assert.equal(server.updates.length, 1);
  assert.equal(server.gets, 1);
  assert.equal(mounted.updated.length, 0);
  assert.match(mounted.container.textContent, /submit once more to confirm/u);
  assert.equal(button(mounted.container, "Submit again").disabled, false);

  await submit(mounted);
  assert.equal(server.updates.length, 2);
  assert.equal(server.updates[1]?.expectedVersion, 2n);
  assert.equal(mounted.updated.at(-1)?.sharingScope, SharingScope.SHARING_SCOPE_GLOBAL);
});

test("a failed reconciliation GET and a definite 403 never report success or retry automatically", async () => {
  const ambiguous = fakeServer();
  ambiguous.updateFailure = new Error("connection ended");
  ambiguous.getFailure = new Error("backend unavailable");
  const first = await mountEditor(ambiguous);
  await choose(first, "Global");
  await submit(first);
  assert.equal(ambiguous.updates.length, 1);
  assert.equal(ambiguous.gets, 1);
  assert.equal(first.updated.length, 0);
  assert.equal(first.closes, 0);
  assert.match(first.container.textContent, /latest saved search could not be loaded/u);

  const forbidden = fakeServer();
  forbidden.updateFailure = new HttpError({
    message: "forbidden",
    status: 403,
    url: "/api/saved-searches/update",
  });
  const second = await mountEditor(forbidden);
  await choose(second, "Global");
  await submit(second);
  assert.equal(forbidden.updates.length, 1);
  assert.equal(forbidden.gets, 0);
  assert.equal(second.updated.length, 0);
  assert.equal(second.closes, 0);
  assert.match(second.container.textContent, /forbidden/u);
});

test("a conflict GET never adopts a saved search owned by another actor", async () => {
  const server = fakeServer();
  const mounted = await mountEditor(server);
  server.current = saved(SharingScope.SHARING_SCOPE_APP, 2n, "another-user");
  await choose(mounted, "Global");
  await submit(mounted);

  assert.equal(server.gets, 1);
  assert.equal(mounted.updated.length, 0);
  assert.equal(mounted.closes, 0);
  assert.match(mounted.container.textContent, /invalid saved-search conflict baseline/u);
});

test("an unknown scope is shown safely and cannot be overwritten", async () => {
  const server = fakeServer(saved(SharingScope.UNRECOGNIZED, 7n));
  const mounted = await mountEditor(server);
  assert.match(mounted.container.textContent, /unknown sharing value/u);
  assert.equal(mounted.container.querySelector("#reports-sharing-scope")?.disabled, true);
  assert.equal(button(mounted.container, "Save sharing").disabled, true);
  await submit(mounted);
  assert.equal(server.updates.length, 0);
});

test("switching client and record state clears a pending session and ignores its late reply", async () => {
  let resolveLate: ((value: unknown) => void) | undefined;
  const first = fakeServer();
  first.client = new OpenSplunkApiClient({
    post() {
      return new Promise((resolve) => { resolveLate = resolve; });
    },
  } as unknown as ProtobufTransport);
  const mounted = await mountEditor(first);
  await choose(mounted, "Global");
  const form = mounted.container.querySelector("#reports-edit-sharing-scope");
  assert.ok(form);
  await act(async () => form.dispatchEvent(fakeEvent("submit")));
  assert.equal(button(mounted.container, "Saving…").disabled, true);

  const second = fakeServer(saved(SharingScope.SHARING_SCOPE_APP, 9n));
  await act(async () => mounted.root.render(
    <SavedSearchScopeEditor
      bootstrap={bootstrap()}
      client={second.client}
      savedSearch={adaptSavedSearch(second.current)}
      onClose={() => { mounted.closes += 1; }}
      onNotice={(message) => mounted.notices.push(message)}
      onUpdated={(updated) => mounted.updated.push(updated)}
    />,
  ));
  await settle();
  assert.equal(mounted.container.querySelector("#reports-sharing-scope")?.textContent?.includes("App"), true);
  assert.equal(button(mounted.container, "Save sharing").disabled, true);

  resolveLate?.({ savedSearch: saved(SharingScope.SHARING_SCOPE_GLOBAL, 2n) });
  await settle();
  assert.equal(mounted.updated.length, 0);
  assert.equal(mounted.closes, 0);
  assert.equal(button(mounted.container, "Save sharing").disabled, true);
});
