import { expect, test, type Page } from "@playwright/test";

import { AppState } from "../../gen/ts/open_splunk/app";
import { CreateAppResponse } from "../../gen/ts/open_splunk/app_api";
import { SharingScope } from "../../gen/ts/open_splunk/common";
import { Dashboard } from "../../gen/ts/open_splunk/dashboard";
import { CreateDashboardRequest, CreateDashboardResponse, DeleteDashboardResponse, ListDashboardsRequest, ListDashboardsResponse, RunDashboardPanelRequest, RunDashboardPanelResponse, UpdateDashboardRequest, UpdateDashboardResponse } from "../../gen/ts/open_splunk/dashboard_api";
import { ResultPage, ResultSetKind, VisualizationSpec, VisualizationStackMode, VisualizationType } from "../../gen/ts/open_splunk/result";
import { SearchJobState } from "../../gen/ts/open_splunk/search";
import { GetSearchResultsRequest, GetSearchResultsResponse } from "../../gen/ts/open_splunk/search_api";
import { ValueType } from "../../gen/ts/open_splunk/value";
import { GetSystemBootstrapRequest, GetSystemBootstrapResponse, ServerFeature } from "../../gen/ts/open_splunk/system_api";

const protobufHeaders = { "content-type": "application/x-protobuf" };

function app(appId: string, displayName: string) {
  return { appId, slug: displayName.toLowerCase(), displayName, defaultIndexNames: [], state: AppState.APP_STATE_ACTIVE };
}

async function mockBootstrap(page: Page, apps = [app("app-1", "GradeThis")], selectedAppId = apps[0]?.appId ?? "") {
  await page.route("**/api/system/bootstrap", (route) => route.fulfill({
    status: 200,
    headers: protobufHeaders,
    body: Buffer.from(GetSystemBootstrapResponse.encode(GetSystemBootstrapResponse.fromPartial({
      apps,
      selectedAppId,
      serverTime: new Date("2026-09-05T00:00:00Z"),
      features: [ServerFeature.SERVER_FEATURE_DASHBOARDS, ServerFeature.SERVER_FEATURE_APP_ADMIN],
    })).finish()),
  }));
}

async function mockDashboardList(page: Page, dashboards: Dashboard[]) {
  await page.route("**/api/dashboards/list", (route) => route.fulfill({
    status: 200,
    headers: protobufHeaders,
    body: Buffer.from(ListDashboardsResponse.encode({ dashboards }).finish()),
  }));
}

function dashboard(dashboardId: string, appId: string, name: string): Dashboard {
  return {
    dashboardId,
    version: 1n,
    definition: { name, appId, sharingScope: SharingScope.SHARING_SCOPE_PRIVATE, panels: [] },
    createdAt: new Date("2026-09-05T00:00:00Z"),
    updatedAt: new Date("2026-09-05T00:00:00Z"),
  };
}

test("creates the first dashboard from the single onboarding card", async ({ page }, testInfo) => {
  await mockBootstrap(page);
  await mockDashboardList(page, []);
  await page.route("**/api/dashboards/create", async (route) => {
    const wire = route.request().postDataBuffer();
    if (wire === null) throw new Error("dashboard create omitted its protobuf body");
    const request = CreateDashboardRequest.decode(wire);
    await route.fulfill({
      status: 200,
      headers: protobufHeaders,
      body: Buffer.from(CreateDashboardResponse.encode({ dashboard: dashboard("dash-1", "app-1", request.definition?.name ?? "") }).finish()),
    });
  });

  await page.goto("/dashboards/");
  await expect(page.getByRole("heading", { name: "Create your first dashboard" })).toBeVisible();
  await page.screenshot({ path: testInfo.outputPath("dashboard-empty.png"), fullPage: true });
  await expect(page.getByText("Select a dashboard")).toHaveCount(0);
  await page.getByLabel("Dashboard name").fill("Service overview");
  await page.getByRole("button", { name: "Create dashboard" }).click();
  await expect(page.getByRole("heading", { name: "Dashboard settings" })).toBeVisible();
  await expect(page.getByText("Service overview", { exact: true })).toBeVisible();
  await page.screenshot({ path: testInfo.outputPath("dashboard-editor.png"), fullPage: true });
});

test("creates the prerequisite app and continues into dashboard onboarding", async ({ page }, testInfo) => {
  let created = false;
  await page.route("**/api/system/bootstrap", (route) => route.fulfill({
    status: 200,
    headers: protobufHeaders,
    body: Buffer.from(GetSystemBootstrapResponse.encode(GetSystemBootstrapResponse.fromPartial({
      apps: created ? [app("app-new", "New App")] : [],
      selectedAppId: created ? "app-new" : "",
      serverTime: new Date("2026-09-05T00:00:00Z"),
      features: [ServerFeature.SERVER_FEATURE_DASHBOARDS, ServerFeature.SERVER_FEATURE_APP_ADMIN],
    })).finish()),
  }));
  await mockDashboardList(page, []);
  await page.route("**/api/apps/create", async (route) => {
    created = true;
    await route.fulfill({
      status: 200,
      headers: protobufHeaders,
      body: Buffer.from(CreateAppResponse.encode(CreateAppResponse.fromPartial({
        app: { appId: "app-new", state: AppState.APP_STATE_ACTIVE, version: 1n, definition: { slug: "new-app", displayName: "New App", defaultIndexNames: [] } },
      })).finish()),
    });
  });

  await page.goto("/dashboards/");
  await expect(page.getByRole("heading", { name: "Create an app workspace first" })).toBeVisible();
  await page.screenshot({ path: testInfo.outputPath("dashboard-no-app.png"), fullPage: true });
  await page.getByRole("button", { name: "Create app" }).click();
  await page.getByLabel("Slug").fill("new-app");
  await page.getByLabel("Display name").fill("New App");
  await page.getByRole("button", { name: "Create app" }).last().click();
  await expect(page.getByRole("heading", { name: "Create your first dashboard" })).toBeVisible();
  await expect(page).toHaveURL(/appId=app-new/u);
});

test("a failed dashboard load never claims the catalog is empty", async ({ page }) => {
  await mockBootstrap(page);
  await page.route("**/api/dashboards/list", (route) => route.fulfill({ status: 500, body: "failed" }));
  await page.goto("/dashboards/");
  await expect(page.locator(".operations-error-banner[role=alert]")).toBeVisible();
  await expect(page.getByRole("button", { name: "Retry" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Create your first dashboard" })).toHaveCount(0);
});

test("deleting the last dashboard returns to the creation card", async ({ page }) => {
  await mockBootstrap(page);
  await mockDashboardList(page, [dashboard("dash-1", "app-1", "Operations")]);
  await page.route("**/api/dashboards/delete", (route) => route.fulfill({
    status: 200,
    headers: protobufHeaders,
    body: Buffer.from(DeleteDashboardResponse.encode(DeleteDashboardResponse.fromPartial({})).finish()),
  }));
  await page.goto("/dashboards/");
  await expect(page.getByRole("button", { name: "Add panel", exact: true })).toBeDisabled();
  await expect(page.getByText("No searchable indexes are available. Add one before creating panels.")).toBeVisible();
  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("button", { name: "Delete", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Create your first dashboard" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Saved dashboards" })).toHaveCount(0);
});

test("unsupported dashboards show an explanation instead of perpetual loading", async ({ page }) => {
  await page.route("**/api/system/bootstrap", (route) => route.fulfill({
    status: 200,
    headers: protobufHeaders,
    body: Buffer.from(GetSystemBootstrapResponse.encode(GetSystemBootstrapResponse.fromPartial({
      serverTime: new Date("2026-09-05T00:00:00Z"),
      apps: [app("app-1", "GradeThis")],
      selectedAppId: "app-1",
      features: [],
    })).finish()),
  }));
  await page.goto("/dashboards/");
  await expect(page.getByRole("heading", { name: "Dashboards unavailable" })).toBeVisible();
  await expect(page.getByText("Loading dashboards…")).toHaveCount(0);
});

test("cancelling an app switch preserves the dirty dashboard and URL", async ({ page }) => {
  const apps = [app("app-1", "GradeThis"), app("app-2", "Security")];
  await mockBootstrap(page, apps, "app-1");
  await mockDashboardList(page, [dashboard("dash-1", "app-1", "Operations")]);
  await page.goto("/dashboards/?appId=app-1");
  await page.getByLabel("Name", { exact: true }).fill("Unsaved name");
  page.once("dialog", (dialog) => dialog.dismiss());
  await page.getByRole("button", { name: /App:/u }).click();
  await page.getByRole("menuitem", { name: "Select Security" }).click();
  await expect(page.getByLabel("Name", { exact: true })).toHaveValue("Unsaved name");
  await expect(page).toHaveURL(/appId=app-1/u);
});

test("a rejected app switch preserves the loaded dirty dashboard", async ({ page }) => {
  const apps = [app("app-1", "GradeThis"), app("app-2", "Security")];
  await mockBootstrap(page, apps, "app-1");
  await mockDashboardList(page, [dashboard("dash-1", "app-1", "Operations")]);
  await page.goto("/dashboards/?appId=app-1");
  await page.getByLabel("Name", { exact: true }).fill("Unsaved name");
  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("button", { name: /App:/u }).click();
  await page.getByRole("menuitem", { name: "Select Security" }).click();
  await expect(page.locator(".operations-error-banner[role=alert]")).toContainText("no longer available");
  await expect(page.getByLabel("Name", { exact: true })).toHaveValue("Unsaved name");
  await expect(page).toHaveURL(/appId=app-1/u);
});

test("app creation failure keeps the setup dialog actionable", async ({ page }) => {
  await mockBootstrap(page, [], "");
  await mockDashboardList(page, []);
  await page.route("**/api/apps/create", (route) => route.fulfill({ status: 500, body: "failed" }));
  await page.goto("/dashboards/");
  await page.getByRole("button", { name: "Create app" }).click();
  await page.getByLabel("Slug").fill("new-app");
  await page.getByLabel("Display name").fill("New App");
  await page.getByRole("button", { name: "Create app" }).last().click();
  await expect(page.getByRole("dialog")).toBeVisible();
  await expect(page.getByRole("dialog").getByRole("alert")).toContainText("could not be created");
  await expect(page.getByLabel("Display name")).toHaveValue("New App");
});

test("bootstrap failure offers Retry without showing an empty state", async ({ page }) => {
  let failing = true;
  await page.route("**/api/system/bootstrap", (route) => {
    if (failing) return route.fulfill({ status: 500, body: "failed" });
    return route.fulfill({
      status: 200,
      headers: protobufHeaders,
      body: Buffer.from(GetSystemBootstrapResponse.encode(GetSystemBootstrapResponse.fromPartial({
        apps: [app("app-1", "GradeThis")],
        selectedAppId: "app-1",
        serverTime: new Date("2026-09-05T00:00:00Z"),
        features: [ServerFeature.SERVER_FEATURE_DASHBOARDS],
      })).finish()),
    });
  });
  await mockDashboardList(page, []);
  await page.goto("/dashboards/");
  await expect(page.locator(".operations-error-banner[role=alert]")).toBeVisible();
  await expect(page.getByRole("heading", { name: "Create your first dashboard" })).toHaveCount(0);
  failing = false;
  await page.getByRole("button", { name: "Retry", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Create your first dashboard" })).toBeVisible();
});

test("no accessible apps without administration explains recovery", async ({ page }) => {
  await page.route("**/api/system/bootstrap", (route) => route.fulfill({
    status: 200,
    headers: protobufHeaders,
    body: Buffer.from(GetSystemBootstrapResponse.encode(GetSystemBootstrapResponse.fromPartial({
      apps: [],
      selectedAppId: "",
      serverTime: new Date("2026-09-05T00:00:00Z"),
      features: [ServerFeature.SERVER_FEATURE_DASHBOARDS],
    })).finish()),
  }));
  await page.goto("/dashboards/");
  await expect(page.getByRole("heading", { name: "Create an app workspace first" })).toBeVisible();
  await expect(page.getByText(/ask an administrator to give you access/u)).toBeVisible();
  await expect(page.getByText(/Create one here/u)).toHaveCount(0);
  await expect(page.getByRole("button", { name: "Check again" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Create app" })).toHaveCount(0);
});

test("rapid app selections commit only the latest successful catalog", async ({ page }) => {
  const apps = [app("app-1", "GradeThis"), app("app-2", "Security"), app("app-3", "Billing")];
  await page.route("**/api/system/bootstrap", async (route) => {
    const wire = route.request().postDataBuffer();
    const preferred = wire === null ? undefined : GetSystemBootstrapRequest.decode(wire).preferredAppId;
    if (preferred === "app-2") await new Promise((resolve) => setTimeout(resolve, 150));
    if (preferred === "app-3") await new Promise((resolve) => setTimeout(resolve, 10));
    const selectedAppId = preferred ?? "app-1";
    await route.fulfill({
      status: 200,
      headers: protobufHeaders,
      body: Buffer.from(GetSystemBootstrapResponse.encode(GetSystemBootstrapResponse.fromPartial({
        apps,
        selectedAppId,
        serverTime: new Date("2026-09-05T00:00:00Z"),
        features: [ServerFeature.SERVER_FEATURE_DASHBOARDS],
      })).finish()),
    });
  });
  await page.route("**/api/dashboards/list", async (route) => {
    const wire = route.request().postDataBuffer();
    if (wire === null) throw new Error("dashboard list omitted its protobuf body");
    const appId = ListDashboardsRequest.decode(wire).appIdFilter ?? "app-1";
    await route.fulfill({
      status: 200,
      headers: protobufHeaders,
      body: Buffer.from(ListDashboardsResponse.encode({ dashboards: [dashboard(`dash-${appId}`, appId, `${appId} dashboard`)] }).finish()),
    });
  });
  await page.goto("/dashboards/?appId=app-1");
  await page.getByRole("button", { name: /App:/u }).click();
  await page.getByRole("menuitem", { name: "Select Security" }).click();
  await page.getByRole("button", { name: /App:/u }).click();
  await page.getByRole("menuitem", { name: "Select Billing" }).click();
  await expect(page.getByRole("button", { name: /App: Billing/u })).toBeVisible();
  await expect(page.getByText("app-3 dashboard", { exact: true })).toBeVisible();
  await expect(page).toHaveURL(/appId=app-3/u);
});

for (const theme of ["light", "dark"] as const) {
  for (const width of [980, 760, 480, 390]) {
    test(`dashboard onboarding stays contained at ${width}px in ${theme} theme`, async ({ page }, testInfo) => {
      await mockBootstrap(page);
      await mockDashboardList(page, []);
      await page.addInitScript(([key, value]) => window.localStorage.setItem(key, value), ["open-splunk.theme", theme]);
      await page.setViewportSize({ width, height: 760 });
      await page.goto("/dashboards/");
      const card = page.locator(".operations-empty-dashboard");
      await expect(card).toBeVisible();
      const bounds = await card.boundingBox();
      expect(bounds).not.toBeNull();
      expect(bounds!.x).toBeGreaterThanOrEqual(0);
      expect(bounds!.x + bounds!.width).toBeLessThanOrEqual(width);
      await page.screenshot({ path: testInfo.outputPath(`dashboard-${theme}-${width}.png`), fullPage: true });
    });
  }
}

const visualizationCases = [
  ["Table", VisualizationType.VISUALIZATION_TYPE_TABLE],
  ["Line", VisualizationType.VISUALIZATION_TYPE_LINE],
  ["Area", VisualizationType.VISUALIZATION_TYPE_AREA],
  ["Column", VisualizationType.VISUALIZATION_TYPE_COLUMN],
  ["Bar", VisualizationType.VISUALIZATION_TYPE_BAR],
  ["Pie", VisualizationType.VISUALIZATION_TYPE_PIE],
  ["Single value", VisualizationType.VISUALIZATION_TYPE_SINGLE_VALUE],
  ["Scatter", VisualizationType.VISUALIZATION_TYPE_SCATTER],
] as const;

function visualizationDashboard(spec?: VisualizationSpec): Dashboard {
  return Dashboard.fromPartial({
    ...dashboard("dash-chart", "app-1", "Typed visualizations"),
    definition: {
      name: "Typed visualizations",
      appId: "app-1",
      sharingScope: SharingScope.SHARING_SCOPE_PRIVATE,
      panels: [{
        panelId: "panel-chart",
        title: "Latency panel",
        width: 12,
        height: 4,
        search: {
          spl: "index=main | table x latency",
          appId: "app-1",
          indexScope: ["main"],
          timeRange: { earliest: "-24h", latest: "now", timezone: "UTC" },
          selectedFields: ["latency", "x"],
          visualization: spec,
        },
      }],
    },
  });
}

function visualizationPage(values: bigint[], nextPageToken?: string, start = 0): ResultPage {
  return ResultPage.fromPartial({
    schema: {
      schemaId: "chart-schema",
      revision: 1n,
      resultKind: ResultSetKind.RESULT_SET_KIND_STATISTICS,
      columns: [
        { fieldName: "x", displayName: "Position", valueType: ValueType.VALUE_TYPE_SINT64 },
        { fieldName: "latency", displayName: "Latency", valueType: ValueType.VALUE_TYPE_SINT64, nullable: true },
      ],
    },
    snapshotComplete: true,
    rows: values.map((value, index) => ({
      rowId: `row-${start + index}`,
      ordinal: BigInt(start + index),
      cells: [
        { kind: { $case: "sint64Value", value: BigInt(start + index + 1) } },
        { kind: { $case: "sint64Value", value } },
      ],
    })),
    page: { nextPageToken },
  });
}

async function mockPanelResults(page: Page, pages: ResultPage[]) {
  const requests: GetSearchResultsRequest[] = [];
  await page.route("**/api/dashboards/panels/run", async (route) => {
    const body = route.request().postDataBuffer();
    if (body === null) throw new Error("panel run omitted its protobuf body");
    expect(RunDashboardPanelRequest.decode(body)).toEqual({ dashboardId: "dash-chart", panelId: "panel-chart" });
    await route.fulfill({
      status: 200,
      headers: protobufHeaders,
      body: Buffer.from(RunDashboardPanelResponse.encode(RunDashboardPanelResponse.fromPartial({
        searchJob: { searchJobId: "job-chart", state: SearchJobState.SEARCH_JOB_STATE_COMPLETED },
      })).finish()),
    });
  });
  await page.route("**/api/search/jobs/results", async (route) => {
    const body = route.request().postDataBuffer();
    if (body === null) throw new Error("results omitted its protobuf body");
    const request = GetSearchResultsRequest.decode(body);
    requests.push(request);
    expect(request.searchJobId).toBe("job-chart");
    expect(request.allowPartialResults).toBe(false);
    expect(request.columns).toEqual([]);
    const pageIndex = request.page?.pageToken === undefined ? 0 : Number(request.page.pageToken);
    expect(pageIndex).toBeLessThan(pages.length);
    await route.fulfill({
      status: 200,
      headers: protobufHeaders,
      body: Buffer.from(GetSearchResultsResponse.encode({ searchJobId: "job-chart", resultPage: pages[pageIndex] }).finish()),
    });
  });
  return requests;
}

for (const [label, type] of visualizationCases) {
  test(`runs a persisted ${label} visualization without changing its search`, async ({ page }) => {
    const spec = VisualizationSpec.fromPartial({
      type,
      title: `${label} exact values`,
      xField: type === VisualizationType.VISUALIZATION_TYPE_SINGLE_VALUE ? undefined : "x",
      yFields: ["latency"],
      stackMode: VisualizationStackMode.VISUALIZATION_STACK_MODE_NONE,
      showLegend: true,
      showDataLabels: true,
    });
    await mockBootstrap(page);
    await mockDashboardList(page, [visualizationDashboard(spec)]);
    await mockPanelResults(page, [visualizationPage([9007199254740993n])]);
    await page.goto("/dashboards/");
    await page.getByRole("button", { name: "Run", exact: true }).click();
    const panel = page.locator(".operations-live-panel");
    await expect(panel).toContainText("9007199254740993");
    await expect(panel.getByRole("alert")).toHaveCount(0);
    await expect(panel.getByRole("textbox", { name: "SPL", exact: true })).toHaveValue("index=main | table x latency");
    if (type !== VisualizationType.VISUALIZATION_TYPE_TABLE && type !== VisualizationType.VISUALIZATION_TYPE_SINGLE_VALUE) {
      await expect(panel.getByRole("img")).toBeVisible();
      await expect(panel).toContainText(/approximate/iu);
    }
  });
}

test("legacy table pages use the original job cursor and keep visualization absent on unrelated saves", async ({ page }) => {
  const stored = visualizationDashboard();
  await mockBootstrap(page);
  await mockDashboardList(page, [stored]);
  const requests = await mockPanelResults(page, [visualizationPage([11n], "1"), visualizationPage([29n], undefined, 1)]);
  let update: UpdateDashboardRequest | undefined;
  await page.route("**/api/dashboards/update", async (route) => {
    const body = route.request().postDataBuffer();
    if (body === null) throw new Error("dashboard update omitted its protobuf body");
    update = UpdateDashboardRequest.decode(body);
    await route.fulfill({
      status: 200,
      headers: protobufHeaders,
      body: Buffer.from(UpdateDashboardResponse.encode({ dashboard: { ...stored, definition: update.definition, version: 2n } }).finish()),
    });
  });
  await page.goto("/dashboards/");
  await page.getByRole("button", { name: "Run", exact: true }).click();
  await expect(page.locator(".operations-panel-result")).toContainText("11");
  await page.getByRole("button", { name: "Next results page" }).click();
  await expect(page.locator(".operations-panel-result")).toContainText("29");
  expect(requests.map((request) => request.page?.pageSize)).toEqual([20, 20]);
  expect(requests.map((request) => request.page?.pageToken)).toEqual([undefined, "1"]);
  await page.getByLabel("Name", { exact: true }).fill("Renamed dashboard");
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await expect.poll(() => update?.definition?.name).toBe("Renamed dashboard");
  expect(update?.definition?.panels[0]?.search).toEqual(stored.definition?.panels[0]?.search);
  expect(update?.definition?.panels[0]?.search?.visualization).toBeUndefined();
});

test("charts retain later cursor rows and reject incompatible typed values explicitly", async ({ page }) => {
  const spec = VisualizationSpec.fromPartial({
    type: VisualizationType.VISUALIZATION_TYPE_SCATTER,
    title: "Cursor scatter",
    xField: "x",
    yFields: ["latency"],
    showDataLabels: true,
  });
  await mockBootstrap(page);
  await mockDashboardList(page, [visualizationDashboard(spec)]);
  const firstPage = visualizationPage([11n], "1");
  const badPage = visualizationPage([23n], undefined, 1);
  for (const resultPage of [firstPage, badPage]) {
    resultPage.schema!.columns[1].valueType = ValueType.VALUE_TYPE_STRING;
  }
  firstPage.rows[0].cells[1] = { kind: { $case: "stringValue", value: "11" } };
  badPage.rows[0].cells[1] = { kind: { $case: "stringValue", value: "not-a-number" } };
  const requests = await mockPanelResults(page, [firstPage, badPage]);
  await page.goto("/dashboards/");
  await page.getByRole("button", { name: "Run", exact: true }).click();
  const panel = page.locator(".operations-live-panel");
  await expect(panel.getByRole("alert")).toBeVisible();
  await expect(panel.getByRole("img")).toHaveCount(0);
  expect(requests).toHaveLength(2);
  await panel.getByText("Inspect rows", { exact: true }).click();
  await expect(panel).toContainText("not-a-number");
  await expect(panel).toContainText("11");
});

test("visualization editor saves all presentation fields and ordered measures without rewriting search", async ({ page }) => {
  const originalSpec = VisualizationSpec.fromPartial({
    type: VisualizationType.VISUALIZATION_TYPE_LINE,
    title: "Original visual title",
    xField: "x",
    yFields: ["latency", "throughput"],
    seriesField: "region",
    stackMode: VisualizationStackMode.VISUALIZATION_STACK_MODE_STACKED_100_PERCENT,
    showLegend: true,
    showDataLabels: false,
    timeBucketWidth: { seconds: 1n, nanos: 1 },
  });
  const stored = visualizationDashboard(originalSpec);
  await mockBootstrap(page);
  await mockDashboardList(page, [stored]);
  let submitted: UpdateDashboardRequest | undefined;
  await page.route("**/api/dashboards/update", async (route) => {
    const body = route.request().postDataBuffer();
    if (body === null) throw new Error("dashboard update omitted its protobuf body");
    submitted = UpdateDashboardRequest.decode(body);
    await route.fulfill({
      status: 200,
      headers: protobufHeaders,
      body: Buffer.from(UpdateDashboardResponse.encode({ dashboard: { ...stored, definition: submitted.definition, version: 2n } }).finish()),
    });
  });
  await page.goto("/dashboards/");
  const panel = page.locator(".operations-live-panel");
  await expect(panel.getByLabel("Visualization title", { exact: true })).toHaveValue("Original visual title");
  await expect(panel.getByLabel("Time bucket width (seconds)", { exact: true })).toHaveValue("1.000000001");
  await panel.getByLabel("Visualization title", { exact: true }).fill("Renamed visual title");
  await panel.getByRole("button", { name: "Move throughput up", exact: true }).click();
  await panel.getByLabel("Show legend", { exact: true }).uncheck();
  await panel.getByLabel("Show data labels", { exact: true }).check();
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await expect.poll(() => submitted?.definition?.panels[0]?.search?.visualization?.title).toBe("Renamed visual title");
  const expectedSpec = { ...originalSpec, title: "Renamed visual title", yFields: ["throughput", "latency"], showLegend: false, showDataLabels: true };
  expect(submitted?.definition?.panels[0]?.search).toEqual({ ...stored.definition?.panels[0]?.search, visualization: expectedSpec });
});

test("a multi-page chart keeps later rows keyboard-inspectable within a narrow panel", async ({ page }, testInfo) => {
  const spec = VisualizationSpec.fromPartial({
    type: VisualizationType.VISUALIZATION_TYPE_LINE,
    title: "Every returned row",
    xField: "x",
    yFields: ["latency"],
  });
  await mockBootstrap(page);
  await mockDashboardList(page, [visualizationDashboard(spec)]);
  const requests = await mockPanelResults(page, [visualizationPage([11n], "1"), visualizationPage([9007199254740993n], undefined, 1)]);
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/dashboards/");
  await page.getByRole("button", { name: "Run", exact: true }).click();
  const panel = page.locator(".operations-live-panel");
  await expect(panel.getByRole("img")).toBeVisible();
  expect(requests).toHaveLength(2);
  const inspect = panel.getByRole("button", { name: /inspect chart values/u });
  await expect(inspect).toHaveCount(2);
  await inspect.first().focus();
  await page.keyboard.press("ArrowRight");
  await expect(inspect.nth(1)).toBeFocused();
  await expect(panel.getByRole("group", { name: /Chart values for/u })).toContainText("9007199254740993");
  const chartBounds = await panel.getByRole("img").boundingBox();
  const panelBounds = await panel.boundingBox();
  expect(chartBounds).not.toBeNull();
  expect(panelBounds).not.toBeNull();
  expect(chartBounds!.x).toBeGreaterThanOrEqual(panelBounds!.x);
  expect(chartBounds!.x + chartBounds!.width).toBeLessThanOrEqual(panelBounds!.x + panelBounds!.width + 1);
  expect(panelBounds!.x + panelBounds!.width).toBeLessThanOrEqual(391);
  await page.screenshot({ path: testInfo.outputPath("dashboard-chart-narrow.png"), fullPage: true });
});
