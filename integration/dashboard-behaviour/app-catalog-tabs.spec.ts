import { expect, test, type BrowserContext, type Page, type Route } from "@playwright/test";

import { AppState, AppWorkspace, type AppSelector } from "../../gen/ts/open_splunk/app";
import {
  CreateAppRequest,
  CreateAppResponse,
  DeleteAppRequest,
  DeleteAppResponse,
  GetAppRequest,
  GetAppResponse,
  ListAppsResponse,
  SetAppStateRequest,
  SetAppStateResponse,
  UpdateAppRequest,
  UpdateAppResponse,
} from "../../gen/ts/open_splunk/app_api";
import { IndexAccessState, IndexState } from "../../gen/ts/open_splunk/index";
import { ColumnSemanticType, ResultPage, ResultSchema, ResultSetKind } from "../../gen/ts/open_splunk/result";
import { SearchExecutionPhase, SearchJob, SearchJobOrigin, SearchJobState } from "../../gen/ts/open_splunk/search";
import {
  CreateSearchJobRequest,
  CreateSearchJobResponse,
  GetSearchJobRequest,
  GetSearchJobResponse,
  GetSearchResultsRequest,
  GetSearchResultsResponse,
  ValidateSearchResponse,
} from "../../gen/ts/open_splunk/search_api";
import { GetSystemBootstrapRequest, GetSystemBootstrapResponse, ServerFeature } from "../../gen/ts/open_splunk/system_api";
import { ValueType } from "../../gen/ts/open_splunk/value";
import { APP_CATALOG_INVALIDATION_STORAGE_KEY } from "../../lib/api/app-catalog";
import { PALETTE_STORAGE_KEY } from "../../lib/theme-preference";

const headers = { "content-type": "application/x-protobuf" };
const primaryAppId = "primary-app";
const createdAppId = "cross-tab-app";
const createdSlug = "cross-tab-app";
const initialQuery = "index=main | head 1";
const dirtyQuery = "index=main | where status=418";

type Realm = "admin" | "search";

interface StorageObservation {
  isTrusted: boolean;
  key: string | null;
  newValue: string | null;
}

function body(route: Route): Buffer {
  const value = route.request().postDataBuffer();
  if (value === null) throw new Error(`Missing protobuf request body for ${route.request().url()}`);
  return value;
}

function selectorAppId(selector: AppSelector | undefined): string {
  if (selector?.selector?.$case !== "appId") throw new Error("App request did not use an app ID selector.");
  return selector.selector.value;
}

function app(appId: string, slug: string, displayName: string, state = AppState.APP_STATE_ACTIVE, version = 1n): AppWorkspace {
  return AppWorkspace.fromPartial({
    appId,
    createdAt: new Date("2026-09-12T00:00:00Z"),
    definition: { slug, displayName, defaultIndexNames: ["main"] },
    state,
    updatedAt: new Date("2026-09-12T00:00:00Z"),
    version,
  });
}

function appName(value: AppWorkspace): string {
  return value.definition?.displayName || value.appId;
}

async function observeNativeStorage(context: BrowserContext) {
  await context.addInitScript((storageKey) => {
    const observedWindow = window as Window & { appCatalogStorageObservations?: StorageObservation[] };
    observedWindow.appCatalogStorageObservations = [];
    window.addEventListener("storage", (event) => {
      if (event.key !== storageKey) return;
      observedWindow.appCatalogStorageObservations?.push({
        isTrusted: event.isTrusted,
        key: event.key,
        newValue: event.newValue,
      });
    });
  }, APP_CATALOG_INVALIDATION_STORAGE_KEY);
}

async function expectCatalogEntry(page: Page, displayName: string, visible: boolean) {
  const trigger = page.getByRole("button", { name: /^App:/u });
  if (await trigger.getAttribute("aria-expanded") !== "true") await trigger.click();
  const entry = page.getByRole("menu").getByText(displayName, { exact: true });
  if (visible) await expect(entry).toBeVisible();
  else await expect(entry).toHaveCount(0);
  await trigger.click();
}

test("real app CRUD refreshes two tabs and selected-app fallback preserves dirty retained search state", async ({ context }) => {
  test.setTimeout(45_000);
  await observeNativeStorage(context);
  const adminPage = await context.newPage();
  const searchPage = await context.newPage();
  let apps = [
    app(primaryAppId, "primary", "Primary app"),
    app("reserve-app", "reserve", "Reserve app"),
  ];
  const bootstrapLog: Array<{ preferredAppId: string | undefined; realm: Realm; selectedAppId: string }> = [];
  const mutations: string[] = [];
  const searchCreates: CreateSearchJobRequest[] = [];
  const jobs = new Map<string, SearchJob>();
  const schema = ResultSchema.fromPartial({
    schemaId: "catalog-tab-events",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_EVENTS,
    columns: [
      { fieldName: "_time", valueType: ValueType.VALUE_TYPE_TIMESTAMP, semanticType: ColumnSemanticType.COLUMN_SEMANTIC_TYPE_EVENT_TIME },
      { fieldName: "_raw", valueType: ValueType.VALUE_TYPE_STRING },
      { fieldName: "index", valueType: ValueType.VALUE_TYPE_STRING },
    ],
  });

  const bootstrapCounts = (): Record<Realm, number> => ({
    admin: bootstrapLog.filter((entry) => entry.realm === "admin").length,
    search: bootstrapLog.filter((entry) => entry.realm === "search").length,
  });
  const waitForBothRefreshes = async (before: Record<Realm, number>) => {
    await expect.poll(() => bootstrapCounts().admin).toBeGreaterThan(before.admin);
    await expect.poll(() => bootstrapCounts().search).toBeGreaterThan(before.search);
  };

  await context.route("**/api/**", async (route) => {
    const pathname = new URL(route.request().url()).pathname;
    const requestPage = route.request().frame().page();
    const realm: Realm = requestPage === adminPage ? "admin" : requestPage === searchPage ? "search" : (() => { throw new Error("API request came from an unexpected page realm."); })();
    if (pathname === "/api/system/bootstrap") {
      const request = GetSystemBootstrapRequest.decode(route.request().postDataBuffer() ?? Buffer.alloc(0));
      const active = apps.filter((entry) => entry.state === AppState.APP_STATE_ACTIVE);
      const preferred = active.find((entry) => entry.appId === request.preferredAppId);
      const selected = preferred ?? active.find((entry) => entry.appId === primaryAppId) ?? active[0];
      if (selected === undefined) throw new Error("Fixture removed every active app.");
      bootstrapLog.push({ preferredAppId: request.preferredAppId, realm, selectedAppId: selected.appId });
      return route.fulfill({
        status: 200,
        headers,
        body: Buffer.from(GetSystemBootstrapResponse.encode(GetSystemBootstrapResponse.fromPartial({
          apps: active.map((entry) => ({
            appId: entry.appId,
            defaultIndexNames: entry.definition?.defaultIndexNames ?? [],
            displayName: entry.definition?.displayName,
            slug: entry.definition?.slug,
            state: entry.state,
          })),
          features: [ServerFeature.SERVER_FEATURE_APP_ADMIN, ServerFeature.SERVER_FEATURE_SEARCH],
          indexes: [{ indexId: "index-main", name: "main", state: IndexState.INDEX_STATE_ACTIVE, searchAccess: IndexAccessState.INDEX_ACCESS_STATE_ENABLED }],
          limits: { maximumPageSize: 100 },
          selectedAppId: selected.appId,
          serverTime: new Date("2026-09-12T01:00:00Z"),
        })).finish()),
      });
    }
    if (pathname === "/api/apps/list") {
      return route.fulfill({
        status: 200,
        headers,
        body: Buffer.from(ListAppsResponse.encode(ListAppsResponse.fromPartial({
          apps: apps.toSorted((left, right) => appName(left).localeCompare(appName(right))),
          page: { totalSize: BigInt(apps.length), totalSizeExact: true },
        })).finish()),
      });
    }
    if (pathname === "/api/apps/create") {
      const request = CreateAppRequest.decode(body(route));
      expect(request.definition).toMatchObject({ slug: createdSlug, displayName: "Cross tab app" });
      expect(request.clientRequestId).toMatch(/^[\x21-\x7e]{16,128}$/u);
      expect(apps.some((entry) => entry.appId === createdAppId)).toBe(false);
      const created = AppWorkspace.fromPartial({
        ...app(createdAppId, createdSlug, "Cross tab app"),
        definition: request.definition,
      });
      apps = [...apps, created];
      mutations.push("create");
      return route.fulfill({ status: 200, headers, body: Buffer.from(CreateAppResponse.encode(CreateAppResponse.fromPartial({ app: created })).finish()) });
    }
    if (pathname === "/api/apps/get") {
      const request = GetAppRequest.decode(body(route));
      const selected = apps.find((entry) => entry.appId === selectorAppId(request.selector));
      return route.fulfill({ status: 200, headers, body: Buffer.from(GetAppResponse.encode(GetAppResponse.fromPartial({ app: selected })).finish()) });
    }
    if (pathname === "/api/apps/update") {
      const request = UpdateAppRequest.decode(body(route));
      expect(selectorAppId(request.selector)).toBe(createdAppId);
      expect(request.expectedVersion).toBe(1n);
      expect(request.updateMask).toEqual(["display_name"]);
      expect(request.definition).toMatchObject({ slug: createdSlug, displayName: "Renamed across tabs" });
      const current = apps.find((entry) => entry.appId === createdAppId);
      if (current === undefined) throw new Error("Updated app is missing.");
      const updated = AppWorkspace.fromPartial({ ...current, definition: request.definition, version: 2n });
      apps = apps.map((entry) => entry.appId === createdAppId ? updated : entry);
      mutations.push("update");
      return route.fulfill({ status: 200, headers, body: Buffer.from(UpdateAppResponse.encode(UpdateAppResponse.fromPartial({ app: updated })).finish()) });
    }
    if (pathname === "/api/apps/state/set") {
      const request = SetAppStateRequest.decode(body(route));
      expect(selectorAppId(request.selector)).toBe(createdAppId);
      const current = apps.find((entry) => entry.appId === createdAppId);
      if (current === undefined) throw new Error("State target app is missing.");
      expect(request.expectedVersion).toBe(current.version);
      expect(request.state === AppState.APP_STATE_ACTIVE || request.state === AppState.APP_STATE_ARCHIVED).toBe(true);
      const updated = AppWorkspace.fromPartial({ ...current, state: request.state, version: current.version + 1n });
      apps = apps.map((entry) => entry.appId === createdAppId ? updated : entry);
      mutations.push(request.state === AppState.APP_STATE_ACTIVE ? "activate" : "archive");
      return route.fulfill({ status: 200, headers, body: Buffer.from(SetAppStateResponse.encode(SetAppStateResponse.fromPartial({ app: updated })).finish()) });
    }
    if (pathname === "/api/apps/delete") {
      const request = DeleteAppRequest.decode(body(route));
      expect(selectorAppId(request.selector)).toBe(createdAppId);
      expect(request.expectedVersion).toBe(5n);
      expect(request.confirmationSlug).toBe(createdSlug);
      apps = apps.filter((entry) => entry.appId !== createdAppId);
      mutations.push("delete");
      return route.fulfill({ status: 200, headers, body: Buffer.from(DeleteAppResponse.encode({ appId: createdAppId }).finish()) });
    }
    if (pathname === "/api/search/validate") {
      return route.fulfill({ status: 200, headers, body: Buffer.from(ValidateSearchResponse.encode(ValidateSearchResponse.fromPartial({ valid: true })).finish()) });
    }
    if (pathname === "/api/search/jobs/create") {
      const request = CreateSearchJobRequest.decode(body(route));
      searchCreates.push(request);
      const searchJobId = `catalog-job-${searchCreates.length}`;
      const job = SearchJob.fromPartial({
        definition: request.definition,
        effectiveIndexScope: ["main"],
        progress: { matchedEvents: 1n, percentComplete: 100, phase: SearchExecutionPhase.SEARCH_EXECUTION_PHASE_COMPLETE, producedRows: 1n, stateVersion: 1n },
        resultKind: ResultSetKind.RESULT_SET_KIND_EVENTS,
        resultSchema: schema,
        searchJobId,
        source: { origin: SearchJobOrigin.SEARCH_JOB_ORIGIN_AD_HOC },
        state: SearchJobState.SEARCH_JOB_STATE_COMPLETED,
        stateVersion: 1n,
      });
      jobs.set(searchJobId, job);
      return route.fulfill({ status: 200, headers, body: Buffer.from(CreateSearchJobResponse.encode(CreateSearchJobResponse.fromPartial({ searchJob: job })).finish()) });
    }
    if (pathname === "/api/search/jobs/get") {
      const request = GetSearchJobRequest.decode(body(route));
      return route.fulfill({ status: 200, headers, body: Buffer.from(GetSearchJobResponse.encode(GetSearchJobResponse.fromPartial({ searchJob: jobs.get(request.searchJobId) })).finish()) });
    }
    if (pathname === "/api/search/jobs/results") {
      const request = GetSearchResultsRequest.decode(body(route));
      const resultPage = ResultPage.fromPartial({
        page: { totalSize: 1n, totalSizeExact: true },
        rows: [{
          cells: [
            { kind: { $case: "timestampValue", value: new Date("2026-09-12T00:30:00Z") } },
            { kind: { $case: "stringValue", value: `retained ${request.searchJobId}` } },
            { kind: { $case: "stringValue", value: "main" } },
          ],
          ordinal: 0n,
          rowId: `${request.searchJobId}:0`,
        }],
        schema,
        snapshotComplete: true,
        snapshotRef: `snapshot-${request.searchJobId}`,
      });
      return route.fulfill({ status: 200, headers, body: Buffer.from(GetSearchResultsResponse.encode({ resultPage, searchJobId: request.searchJobId }).finish()) });
    }
    return route.fulfill({ status: 404, body: `Unsupported fixture endpoint: ${pathname}` });
  });

  await Promise.all([
    adminPage.goto(`/admin/?${new URLSearchParams({ section: "apps", appId: primaryAppId })}`),
    searchPage.goto(`/search/events/?${new URLSearchParams({ q: initialQuery, run: "0", appId: primaryAppId })}`),
  ]);
  await expect(adminPage.getByRole("heading", { name: "Apps", exact: true })).toBeVisible();
  await expect(searchPage.getByTestId("run-search")).toBeEnabled();
  const baselineStorage = await adminPage.evaluate(() => Object.fromEntries(Object.entries(window.localStorage)));
  expect(baselineStorage).toEqual({ [PALETTE_STORAGE_KEY]: "classic" });

  let before = bootstrapCounts();
  await adminPage.getByRole("button", { name: "Create app", exact: true }).click();
  const createDialog = adminPage.getByRole("dialog", { name: "Create app" });
  await createDialog.getByLabel("Slug").fill(createdSlug);
  await createDialog.getByLabel("Display name").fill("Cross tab app");
  await createDialog.getByRole("button", { name: "Create app", exact: true }).click();
  await waitForBothRefreshes(before);
  await expectCatalogEntry(adminPage, "Cross tab app", true);
  await expectCatalogEntry(searchPage, "Cross tab app", true);
  const nativeCreateEvents = await searchPage.evaluate(() => (window as Window & { appCatalogStorageObservations?: StorageObservation[] }).appCatalogStorageObservations ?? []);
  expect(nativeCreateEvents.some((event) => event.key === APP_CATALOG_INVALIDATION_STORAGE_KEY && event.isTrusted && Boolean(event.newValue))).toBe(true);

  before = bootstrapCounts();
  await adminPage.getByRole("button", { name: "Edit app Cross tab app" }).click();
  const editDialog = adminPage.getByRole("dialog", { name: "Edit Cross tab app" });
  await editDialog.getByLabel("Display name").fill("Renamed across tabs");
  await editDialog.getByRole("button", { name: "Save changes" }).click();
  await waitForBothRefreshes(before);
  await expectCatalogEntry(adminPage, "Renamed across tabs", true);
  await expectCatalogEntry(searchPage, "Renamed across tabs", true);
  await expectCatalogEntry(adminPage, "Cross tab app", false);
  await expectCatalogEntry(searchPage, "Cross tab app", false);

  before = bootstrapCounts();
  await adminPage.getByRole("button", { name: "Archive app Renamed across tabs" }).click();
  await waitForBothRefreshes(before);
  await expectCatalogEntry(adminPage, "Renamed across tabs", false);
  await expectCatalogEntry(searchPage, "Renamed across tabs", false);

  before = bootstrapCounts();
  await adminPage.getByRole("button", { name: "Activate app Renamed across tabs" }).click();
  await waitForBothRefreshes(before);
  await expectCatalogEntry(adminPage, "Renamed across tabs", true);
  await expectCatalogEntry(searchPage, "Renamed across tabs", true);

  const searchAppMenu = searchPage.getByRole("button", { name: /^App:/u });
  await searchAppMenu.click();
  await searchPage.getByRole("menu").getByText("Renamed across tabs", { exact: true }).click();
  await expect(searchPage).toHaveURL(new RegExp(`appId=${createdAppId}`, "u"));
  await searchPage.getByTestId("run-search").click();
  await expect(searchPage.getByTestId("event-list")).toContainText("retained catalog-job-1");
  expect(searchCreates).toHaveLength(1);
  expect(searchCreates[0].definition?.appId).toBe(createdAppId);
  await searchPage.getByTestId("search-input").fill(dirtyQuery);

  before = bootstrapCounts();
  await adminPage.getByRole("button", { name: "Archive app Renamed across tabs" }).click();
  await waitForBothRefreshes(before);
  await expect(searchPage).toHaveURL(new RegExp(`appId=${primaryAppId}`, "u"));
  await expect(searchPage.getByRole("button", { name: /App: Primary app/u })).toBeVisible();
  await expect(searchPage.getByTestId("search-input")).toHaveValue(dirtyQuery);
  await expect(searchPage.getByTestId("event-list")).toContainText("retained catalog-job-1");
  expect(searchCreates).toHaveLength(1);
  expect(bootstrapLog.some((entry) => entry.realm === "search" && entry.preferredAppId === createdAppId && entry.selectedAppId === primaryAppId)).toBe(true);

  before = bootstrapCounts();
  await adminPage.getByRole("button", { name: "Delete app Renamed across tabs" }).click();
  const deleteDialog = adminPage.getByRole("dialog", { name: "Delete Renamed across tabs" });
  await deleteDialog.getByLabel(/Type cross-tab-app to confirm/u).fill(createdSlug);
  await deleteDialog.getByRole("button", { name: "Delete permanently" }).click();
  await waitForBothRefreshes(before);
  await expect(adminPage.getByRole("button", { name: /app Renamed across tabs/u })).toHaveCount(0);
  await expectCatalogEntry(adminPage, "Renamed across tabs", false);
  await expectCatalogEntry(searchPage, "Renamed across tabs", false);
  await expect(searchPage.getByTestId("search-input")).toHaveValue(dirtyQuery);
  await expect(searchPage.getByTestId("event-list")).toContainText("retained catalog-job-1");
  expect(searchCreates).toHaveLength(1);

  expect(mutations).toEqual(["create", "update", "archive", "activate", "archive", "delete"]);
  expect(bootstrapLog.every((entry) => entry.preferredAppId === undefined || entry.preferredAppId === primaryAppId || entry.preferredAppId === createdAppId)).toBe(true);
  expect(bootstrapLog.some((entry) => entry.realm === "admin" && entry.preferredAppId === primaryAppId)).toBe(true);
  expect(bootstrapLog.some((entry) => entry.realm === "search" && entry.preferredAppId === primaryAppId)).toBe(true);
  expect(bootstrapLog.some((entry) => entry.realm === "search" && entry.preferredAppId === createdAppId && entry.selectedAppId === createdAppId)).toBe(true);
  const storage = await adminPage.evaluate(() => Object.fromEntries(Object.entries(window.localStorage)));
  expect(Object.keys(storage).toSorted()).toEqual([APP_CATALOG_INVALIDATION_STORAGE_KEY, PALETTE_STORAGE_KEY].toSorted());
  expect(storage[PALETTE_STORAGE_KEY]).toBe("classic");
  expect(storage[APP_CATALOG_INVALIDATION_STORAGE_KEY]).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u);
  expect(JSON.stringify(storage)).not.toMatch(/cross-tab-app|Cross tab app|Renamed across tabs|credential|token/iu);
});
