import assert from "node:assert/strict";
import test from "node:test";

// React must observe the fake host before react-dom/client initializes.
import { browser } from "@/lib/testing/fake-browser";
import { act, type ReactNode } from "react";
import { createRoot } from "react-dom/client";
import { Dashboard } from "@/gen/ts/open_splunk/dashboard";
import { ResultPage, VisualizationSpec, VisualizationType } from "@/gen/ts/open_splunk/result";
import { SearchJob, SearchJobState } from "@/gen/ts/open_splunk/search";
import { GetSystemBootstrapResponse, ServerFeature } from "@/gen/ts/open_splunk/system_api";
import type { GetSearchResultsRequest, GetSearchResultsResponse } from "@/gen/ts/open_splunk/search_api";
import type { RunDashboardPanelRequest, RunDashboardPanelResponse } from "@/gen/ts/open_splunk/dashboard_api";
import type { OpenSplunkApiClient } from "@/lib/api/open-splunk-client";
import * as clientModule from "@/lib/api/open-splunk-client";
import * as navigationModule from "@/lib/search/app-navigation";
import { fakeEvent, type FakeElement } from "@/lib/testing/fake-dom";
import * as shellModule from "../_components/product-shell";
import * as visualizationModule from "./dashboard-visualization";
import { BackendDashboardManager } from "./backend-dashboard-manager";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => { resolve = done; });
  return { promise, resolve };
}

function storedDashboard(count = 1) {
  return Dashboard.fromPartial({
    dashboardId: "dashboard-1", version: 1n,
    definition: {
      name: "Managed panels", appId: "app-1", sharingScope: 1,
      panels: Array.from({ length: count }, (_, index) => ({
        panelId: `panel-${index}`, title: `Panel ${index}`, width: 12, height: 4,
        search: { spl: "index=main", appId: "app-1", indexScope: ["main"], timeRange: { earliest: "-24h", latest: "now" } },
      })),
    },
  });
}

function completedJob(id: string) {
  return SearchJob.fromPartial({ searchJobId: id, state: SearchJobState.SEARCH_JOB_STATE_COMPLETED });
}

function results(jobId: string, value: bigint, nextPageToken?: string): GetSearchResultsResponse {
  return {
    searchJobId: jobId,
    resultPage: ResultPage.fromPartial({
      schema: { schemaId: "panel-schema", revision: 1n, columns: [{ fieldName: "value" }] },
      rows: [{ rowId: `row-${value}`, ordinal: value, cells: [{ kind: { $case: "sint64Value", value } }] }],
      page: { nextPageToken }, snapshotComplete: true, snapshotRef: "fixed-snapshot",
    }),
  };
}

async function settle() {
  await act(async () => new Promise<void>((resolve) => setImmediate(resolve)));
}

function buttons(container: FakeElement, text: string) {
  return container.querySelectorAll("button").filter((button) => button.textContent.trim() === text);
}

async function click(button: FakeElement | undefined) {
  assert.ok(button);
  await act(async () => { button.dispatchEvent(fakeEvent("click")); });
  await settle();
}

async function mount(options: {
  count?: number;
  run: (request: RunDashboardPanelRequest) => Promise<RunDashboardPanelResponse>;
  results: (request: GetSearchResultsRequest) => Promise<GetSearchResultsResponse>;
}) {
  const stored = storedDashboard(options.count);
  const cancelIDs: string[] = [];
  const client = {
    system: { bootstrap: async () => GetSystemBootstrapResponse.fromPartial({
      apps: [{ appId: "app-1", displayName: "App", slug: "app", state: 1 }],
      selectedAppId: "app-1", serverTime: new Date("2026-09-12T00:00:00Z"),
      features: [ServerFeature.SERVER_FEATURE_DASHBOARDS],
    }) },
    dashboards: {
      list: async () => ({ dashboards: [stored] }),
      runPanel: options.run,
      update: async () => ({ dashboard: stored }),
    },
    search: {
      results: options.results,
      cancel: async ({ searchJobId }: { searchJobId: string }) => { cancelIDs.push(searchJobId); return {}; },
    },
  } as unknown as OpenSplunkApiClient;
  const clientMutable = clientModule as { createOpenSplunkApiClient: typeof clientModule.createOpenSplunkApiClient };
  const shellMutable = shellModule as { ProductShell: typeof shellModule.ProductShell };
  const navigationMutable = navigationModule as { replaceBackendAppId: typeof navigationModule.replaceBackendAppId };
  const visualMutable = visualizationModule as {
    DashboardVisualization: typeof visualizationModule.DashboardVisualization;
    DashboardVisualizationEditor: typeof visualizationModule.DashboardVisualizationEditor;
  };
  const originals = {
    client: clientMutable.createOpenSplunkApiClient,
    shell: shellMutable.ProductShell,
    navigation: navigationMutable.replaceBackendAppId,
    visualization: visualMutable.DashboardVisualization,
    editor: visualMutable.DashboardVisualizationEditor,
  };
  const savedGlobals = ["location", "addEventListener", "removeEventListener"].map((name) => [name, Object.getOwnPropertyDescriptor(globalThis, name)] as const);
  Object.defineProperty(globalThis, "location", { configurable: true, value: window.location });
  Object.defineProperty(globalThis, "addEventListener", { configurable: true, value: window.addEventListener.bind(window) });
  Object.defineProperty(globalThis, "removeEventListener", { configurable: true, value: window.removeEventListener.bind(window) });
  clientMutable.createOpenSplunkApiClient = () => client;
  shellMutable.ProductShell = ({ children }: { children: ReactNode }) => <div>{children}</div>;
  navigationMutable.replaceBackendAppId = () => undefined;
  // Real renderers have their own contracts; this suite isolates manager lifecycles.
  visualMutable.DashboardVisualization = ({ rows, onNextPage }) => <div>
    {rows.map((row) => <span key={row.rowId}>Value {String(row.cells[0]?.kind?.value)}</span>)}
    {onNextPage ? <button type="button" onClick={onNextPage}>Next test page</button> : null}
  </div>;
  visualMutable.DashboardVisualizationEditor = ({ onChange, onValidityChange }) => <div>
    <button type="button" onClick={() => onChange(VisualizationSpec.fromPartial({ type: VisualizationType.VISUALIZATION_TYPE_LINE, xField: "value", yFields: ["value"] }))}>Edit test visualization</button>
    <button type="button" onClick={() => onValidityChange?.(false)}>Invalidate test visualization</button>
    <button type="button" onClick={() => onValidityChange?.(true)}>Validate test visualization</button>
  </div>;
  const container = browser.document.body.appendChild(browser.document.createElement("div"));
  const root = createRoot(container as unknown as Element);
  await act(async () => { root.render(<BackendDashboardManager apiBaseUrl="https://dashboard-lifecycle.test" />); });
  await settle();
  return {
    cancelIDs, container,
    async close() {
      await act(async () => { root.unmount(); });
      container.parentNode?.removeChild(container);
      clientMutable.createOpenSplunkApiClient = originals.client;
      shellMutable.ProductShell = originals.shell;
      navigationMutable.replaceBackendAppId = originals.navigation;
      visualMutable.DashboardVisualization = originals.visualization;
      visualMutable.DashboardVisualizationEditor = originals.editor;
      for (const [name, descriptor] of savedGlobals) {
        if (descriptor) Object.defineProperty(globalThis, name, descriptor);
        else Reflect.deleteProperty(globalThis, name);
      }
    },
  };
}

test("four panel pipelines include pending result collection before admitting another job", async () => {
  const admissions: string[] = [];
  const pending = new Map<string, ReturnType<typeof deferred<GetSearchResultsResponse>>>();
  const view = await mount({
    count: 6,
    run: async ({ panelId }) => { admissions.push(panelId); return { searchJob: completedJob(`job-${panelId}`) }; },
    results: ({ searchJobId }) => {
      const operation = deferred<GetSearchResultsResponse>();
      pending.set(searchJobId, operation);
      return operation.promise;
    },
  });
  try {
    await act(async () => {
      for (const button of buttons(view.container, "Run")) button.dispatchEvent(fakeEvent("click"));
    });
    await settle();
    assert.equal(admissions.length, 4);
    pending.get("job-panel-0")!.resolve(results("job-panel-0", 11n));
    await settle();
    assert.equal(admissions.length, 5);
    assert.match(view.container.textContent, /Value 11/u);
  } finally { await view.close(); }
});

test("a replaced admission cancels its late job while only the latest result is shown", async () => {
  const old = deferred<RunDashboardPanelResponse>();
  let admissions = 0;
  const view = await mount({
    run: () => ++admissions === 1 ? old.promise : Promise.resolve({ searchJob: completedJob("new-job") }),
    results: async () => results("new-job", 29n),
  });
  try {
    await click(buttons(view.container, "Run")[0]);
    await click(buttons(view.container, "Run")[0]);
    assert.match(view.container.textContent, /Value 29/u);
    old.resolve({ searchJob: completedJob("old-job") });
    await settle();
    assert.deepEqual(view.cancelIDs, ["old-job"]);
    assert.match(view.container.textContent, /Value 29/u);
    assert.doesNotMatch(view.container.textContent, /old-job/u);
  } finally { old.resolve({ searchJob: completedJob("old-job") }); await view.close(); }
});

test("editing a panel aborts pending paging and keeps late rows out of the new configuration", async () => {
  const oldPage = deferred<GetSearchResultsResponse>();
  const view = await mount({
    run: async () => ({ searchJob: completedJob("table-job") }),
    results: async (request) => request.page?.pageToken ? oldPage.promise : results("table-job", 11n, "next-page"),
  });
  try {
    await click(buttons(view.container, "Run")[0]);
    await click(buttons(view.container, "Next test page")[0]);
    await click(buttons(view.container, "Edit test visualization")[0]);
    oldPage.resolve(results("table-job", 29n));
    await settle();
    assert.match(view.container.textContent, /Panel settings changed/u);
    assert.doesNotMatch(view.container.textContent, /Value 11|Value 29/u);
    assert.deepEqual(view.cancelIDs, [], "completed result jobs need no cancellation request");
  } finally { oldPage.resolve(results("table-job", 29n)); await view.close(); }
});


test("invalid local visualization input blocks saving and admission until corrected", async () => {
  let admissions = 0;
  const view = await mount({
    run: async () => { admissions += 1; return { searchJob: completedJob("valid-job") }; },
    results: async () => results("valid-job", 11n),
  });
  try {
    await click(buttons(view.container, "Edit test visualization")[0]);
    assert.equal(buttons(view.container, "Save")[0].disabled, false);
    await click(buttons(view.container, "Invalidate test visualization")[0]);
    assert.equal(buttons(view.container, "Save")[0].disabled, true);
    assert.equal(buttons(view.container, "Run")[0].disabled, true);
    await click(buttons(view.container, "Run")[0]);
    assert.equal(admissions, 0);
    await click(buttons(view.container, "Validate test visualization")[0]);
    assert.equal(buttons(view.container, "Save")[0].disabled, false);
    assert.equal(buttons(view.container, "Run")[0].disabled, false);
  } finally { await view.close(); }
});
