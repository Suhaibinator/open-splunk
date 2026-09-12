import { expect, test, type Page, type Route } from "@playwright/test";
import { AppState } from "../../gen/ts/open_splunk/app";
import { IndexAccessState, IndexState } from "../../gen/ts/open_splunk/index";
import { GetSystemBootstrapRequest, GetSystemBootstrapResponse, ServerFeature } from "../../gen/ts/open_splunk/system_api";
import { ColumnSemanticType, ResultPage, ResultSchema, ResultSetKind } from "../../gen/ts/open_splunk/result";
import { SearchExecutionPhase, SearchJob, SearchJobOrigin, SearchJobState } from "../../gen/ts/open_splunk/search";
import { CreateSearchJobRequest, CreateSearchJobResponse, GetSearchJobRequest, GetSearchJobResponse, GetSearchResultsRequest, GetSearchResultsResponse, ValidateSearchResponse } from "../../gen/ts/open_splunk/search_api";
import { PrepareNearbyContextRequest, PrepareNearbyContextResponse } from "../../gen/ts/open_splunk/nearby_api";
import { ValueType } from "../../gen/ts/open_splunk/value";
import { ListSearchPatternsRequest, ListSearchPatternsResponse, ListSearchPatternMembersRequest, ListSearchPatternMembersResponse, PatternSensitivity } from "../../gen/ts/open_splunk/patterns_api";
import { CreateExportJobRequest } from "../../gen/ts/open_splunk/export_api";

const headers = { "content-type": "application/x-protobuf" };
const oldQuery = "index=main";
const anchor = "2026-09-12T06:00:00.123456789Z";
const nearbyEarliest = "2026-09-12T05:55:00.123456789Z";
const nearbyLatest = "2026-09-12T06:05:00.123456789Z";

function gate() {
  let release!: () => void;
  const pending = new Promise<void>((resolve) => { release = resolve; });
  return { pending, release };
}

function body(route: Route): Buffer {
  const value = route.request().postDataBuffer();
  if (!value) throw new Error("Missing protobuf request");
  return value;
}

async function fixture(page: Page) {
  const creates: CreateSearchJobRequest[] = [];
  const gets: string[] = [];
  const prepares: PrepareNearbyContextRequest[] = [];
  const bootstrapRequests: GetSystemBootstrapRequest[] = [];
  const jobs = new Map<string, SearchJob>();
  const receipts = new Map<string, SearchJob>();
  const resultPages = new Map<string, ResultPage>();
  const patternRequests: ListSearchPatternsRequest[] = [];
  const memberRequests: ListSearchPatternMembersRequest[] = [];
  const exportRequests: CreateExportJobRequest[] = [];
  const state = {
    bootstrapGate: null as ReturnType<typeof gate> | null,
    prepareGate: null as ReturnType<typeof gate> | null,
    holdCanonical: false,
    heldBootstrap: false,
    exports: 0,
    selectedAppId: "app-1",
    dropCreateResponseOnce: false,
  };
  const schema = ResultSchema.fromPartial({
    schemaId: "review-events", revision: 1n, resultKind: ResultSetKind.RESULT_SET_KIND_EVENTS,
    columns: [
      { fieldName: "_time", valueType: ValueType.VALUE_TYPE_TIMESTAMP, semanticType: ColumnSemanticType.COLUMN_SEMANTIC_TYPE_EVENT_TIME },
      { fieldName: "_raw", valueType: ValueType.VALUE_TYPE_STRING },
      { fieldName: "host", valueType: ValueType.VALUE_TYPE_STRING },
      { fieldName: "source", valueType: ValueType.VALUE_TYPE_STRING },
      { fieldName: "index", valueType: ValueType.VALUE_TYPE_STRING },
    ],
  });
  await page.route("**/api/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    if (path === "/api/system/bootstrap") {
      const request = GetSystemBootstrapRequest.decode(route.request().postDataBuffer() ?? Buffer.alloc(0));
      bootstrapRequests.push(request);
      if (state.bootstrapGate && (!state.holdCanonical || request.preferredAppId === "app-1")) {
        state.heldBootstrap = true;
        await state.bootstrapGate.pending;
      }
      return route.fulfill({ status: 200, headers, body: Buffer.from(GetSystemBootstrapResponse.encode(GetSystemBootstrapResponse.fromPartial({
        apps: [{ appId: state.selectedAppId, slug: "review", displayName: state.selectedAppId === "app-1" ? "Review app" : "Replacement app", defaultIndexNames: ["main"], state: AppState.APP_STATE_ACTIVE }],
        indexes: [{ indexId: "index-1", name: "main", state: IndexState.INDEX_STATE_ACTIVE, searchAccess: IndexAccessState.INDEX_ACCESS_STATE_ENABLED }],
        selectedAppId: state.selectedAppId, serverTime: new Date("2026-09-12T07:00:00Z"),
        features: [ServerFeature.SERVER_FEATURE_SEARCH, ServerFeature.SERVER_FEATURE_EXPORT_CSV],
        limits: { maximumPageSize: 100, maximumExportRows: 1000n, maximumExportBytes: 1_000_000n },
      })).finish()) });
    }
    if (path === "/api/search/validate") return route.fulfill({ status: 200, headers, body: Buffer.from(ValidateSearchResponse.encode(ValidateSearchResponse.fromPartial({ valid: true })).finish()) });
    if (path === "/api/search/jobs/create") {
      const request = CreateSearchJobRequest.decode(body(route));
      creates.push(request);
      const previous = request.clientRequestId ? receipts.get(request.clientRequestId) : undefined;
      const id = previous?.searchJobId ?? `review-job-${jobs.size + 1}`;
      const job = previous ?? SearchJob.fromPartial({
        searchJobId: id, stateVersion: 1n, definition: request.definition,
        state: SearchJobState.SEARCH_JOB_STATE_COMPLETED,
        source: { origin: SearchJobOrigin.SEARCH_JOB_ORIGIN_AD_HOC },
        effectiveIndexScope: ["main"], resultKind: ResultSetKind.RESULT_SET_KIND_EVENTS, resultSchema: schema,
        progress: { phase: SearchExecutionPhase.SEARCH_EXECUTION_PHASE_COMPLETE, percentComplete: 100, matchedEvents: 1n, producedRows: 1n, stateVersion: 1n },
      });
      jobs.set(id, job);
      if (request.clientRequestId) receipts.set(request.clientRequestId, job);
      if (state.dropCreateResponseOnce) {
        state.dropCreateResponseOnce = false;
        return route.abort("failed");
      }
      return route.fulfill({ status: 200, headers, body: Buffer.from(CreateSearchJobResponse.encode({ searchJob: job, replayed: previous !== undefined }).finish()) });
    }
    if (path === "/api/search/jobs/get") {
      const id = GetSearchJobRequest.decode(body(route)).searchJobId;
      gets.push(id);
      return route.fulfill({ status: 200, headers, body: Buffer.from(GetSearchJobResponse.encode({ searchJob: jobs.get(id) }).finish()) });
    }
    if (path === "/api/search/jobs/results") {
      const id = GetSearchResultsRequest.decode(body(route)).searchJobId;
      const resultPage = ResultPage.fromPartial({
        schema, snapshotRef: `snapshot-${id}`, snapshotComplete: true,
        rows: [{ rowId: `${id}:0`, ordinal: 0n, cells: [
          { kind: { $case: "timestampValue", value: new Date(anchor) } },
          { kind: { $case: "stringValue", value: `retained ${id}` } },
          { kind: { $case: "stringValue", value: "literal*host" } },
          { kind: { $case: "stringValue", value: "source-path" } },
          { kind: { $case: "stringValue", value: "main" } },
        ] }], page: { totalSize: 1n, totalSizeExact: true },
      });
      resultPages.set(id, resultPage);
      return route.fulfill({ status: 200, headers, body: Buffer.from(GetSearchResultsResponse.encode({ searchJobId: id, resultPage }).finish()) });
    }
    if (path === "/api/search/jobs/nearby/prepare") {
      const request = PrepareNearbyContextRequest.decode(body(route));
      prepares.push(request);
      expect(jobs.has(request.searchJobId)).toBe(true);
      expect(request.snapshotRef).toBe(`snapshot-${request.searchJobId}`);
      expect(request.rowId).toBe(`${request.searchJobId}:0`);
      if (state.prepareGate) await state.prepareGate.pending;
      return route.fulfill({ status: 200, headers, body: Buffer.from(PrepareNearbyContextResponse.encode({
        searchJobId: request.searchJobId, rowId: request.rowId, anchorTime: anchor,
        earliest: nearbyEarliest, latest: nearbyLatest, index: "main", host: "literal*host", source: "source-path", clipped: false, fields: [],
      }).finish()) });
    }
    if (path === "/api/search/jobs/patterns/list") {
      const request = ListSearchPatternsRequest.decode(body(route));
      patternRequests.push(request);
      return route.fulfill({ status: 200, headers, body: Buffer.from(ListSearchPatternsResponse.encode(ListSearchPatternsResponse.fromPartial({
        snapshotRef: request.snapshotRef, algorithmVersion: "1", snapshotComplete: true,
        retainedEventCount: 1n, eligibleEventCount: 1n, excludedEventCount: 0n,
        patterns: [{ patternId: "exact-retained-group", signature: "literal * <int>", eventCount: 1n }],
        page: { totalSize: 1n, totalSizeExact: true },
      })).finish()) });
    }
    if (path === "/api/search/jobs/patterns/members") {
      const request = ListSearchPatternMembersRequest.decode(body(route));
      memberRequests.push(request);
      return route.fulfill({ status: 200, headers, body: Buffer.from(ListSearchPatternMembersResponse.encode({
        patternId: request.patternId, resultPage: resultPages.get(request.searchJobId),
      }).finish()) });
    }
    if (path === "/api/search/exports/create") {
      state.exports += 1;
      exportRequests.push(CreateExportJobRequest.decode(body(route)));
      return route.fulfill({ status: 503, body: "Temporary export admission failure" });
    }
    return route.fulfill({ status: 404, body: "Unsupported fixture endpoint" });
  });
  return { creates, gets, prepares, bootstrapRequests, patternRequests, memberRequests, exportRequests, state };
}

async function openCompleted(page: Page, model: Awaited<ReturnType<typeof fixture>>) {
  await page.goto(`/search/events/?${new URLSearchParams({ q: oldQuery, run: "0", appId: "app-1" })}`);
  await expect(page.getByTestId("run-search")).toBeEnabled();
  await page.getByTestId("run-search").click();
  await expect(page.getByTestId("event-list")).toContainText("retained review-job-1");
  expect(model.creates).toHaveLength(1);
}

test("initial canonical app bootstrap eventually runs the authorized default search once", async ({ page }) => {
  const model = await fixture(page);
  model.state.bootstrapGate = gate();
  model.state.holdCanonical = true;
  try {
    await page.goto("/search/events/");
    await expect.poll(() => model.state.heldBootstrap || model.creates.length > 0).toBe(true);
    model.state.bootstrapGate.release();
    await expect(page.getByTestId("event-list")).toContainText("retained review-job-1");
    expect(model.creates).toHaveLength(1);
  } finally { model.state.bootstrapGate.release(); }
});

test("Nearby keeps exact prepared bounds and Back restores the old job and dirty draft", async ({ page }) => {
  const model = await fixture(page);
  await openCompleted(page, model);
  const draft = "index=main | where status=599";
  await page.getByTestId("search-input").fill(draft);
  await page.getByTestId("time-range-button").click();
  const timePicker = page.getByTestId("time-picker-dialog");
  await timePicker.getByRole("button", { name: "Last 4 hours", exact: true }).click();
  await timePicker.getByRole("button", { name: "Apply", exact: true }).click();
  await page.getByTitle("Find nearby events", { exact: true }).click();
  await expect(page.getByTestId("event-list")).toContainText("retained review-job-2");
  expect(model.prepares).toEqual([{ searchJobId: "review-job-1", snapshotRef: "snapshot-review-job-1", rowId: "review-job-1:0" }]);
  expect(model.creates[1].definition?.timeRange).toMatchObject({ earliest: nearbyEarliest, latest: nearbyLatest });
  expect(model.creates[1].definition?.spl).toBe('index="main" | where \'index\' = "main" AND \'host\' = "literal*host" AND \'source\' = "source-path"');
  await page.goBack();
  await expect(page.getByTestId("event-list")).toContainText("retained review-job-1");
  await expect(page.getByTestId("search-input")).toHaveValue(draft);
  await expect(page.getByTestId("time-range-button")).toContainText("Last 4 hours");
  expect(model.creates).toHaveLength(2);
  expect(model.gets).toContain("review-job-1");
});

test("editing SPL while Nearby preparation is pending prevents a late search admission", async ({ page }) => {
  const model = await fixture(page);
  await openCompleted(page, model);
  model.state.prepareGate = gate();
  const settled = new Promise<void>((resolve) => {
    const finish = (request: import("@playwright/test").Request) => {
      if (!request.url().endsWith("/api/search/jobs/nearby/prepare")) return;
      page.off("requestfinished", finish);
      page.off("requestfailed", finish);
      resolve();
    };
    page.on("requestfinished", finish);
    page.on("requestfailed", finish);
  });
  try {
    await page.getByTitle("Find nearby events", { exact: true }).click();
    await expect.poll(() => model.prepares.length).toBe(1);
    const draft = "index=main | where status=418";
    await page.getByTestId("search-input").fill(draft);
    model.state.prepareGate.release();
    await settled;
    await page.evaluate(() => new Promise<void>((resolve) => requestAnimationFrame(() => resolve())));
    await expect(page.getByText("Preparing nearby event context…", { exact: true })).toHaveCount(0);
    await expect(page.getByTestId("search-input")).toHaveValue(draft);
    await expect(page.getByTestId("event-list")).toContainText("retained review-job-1");
    expect(model.creates).toHaveLength(1);
  } finally { model.state.prepareGate.release(); }
});

test("an open export dialog blocks new admission until the refreshed app catalog is accepted", async ({ page }) => {
  const model = await fixture(page);
  await openCompleted(page, model);
  await page.getByRole("button", { name: "Export", exact: true }).click();
  const create = page.getByRole("button", { name: "Create export", exact: true });
  await expect(create).toBeEnabled();
  model.state.bootstrapGate = gate();
  try {
    await page.evaluate(() => window.dispatchEvent(new StorageEvent("storage", {
      key: "open-splunk.app-catalog-invalidation", newValue: "review-refresh",
    })));
    await expect.poll(() => model.state.heldBootstrap).toBe(true);
    await expect(create).toBeDisabled();
    expect(model.state.exports).toBe(0);
    model.state.bootstrapGate.release();
    await expect(create).toBeEnabled();
    await expect(page.getByTestId("event-list")).toContainText("retained review-job-1");
    expect(model.creates).toHaveLength(1);
  } finally { model.state.bootstrapGate.release(); }
});

test("catalog fallback preserves the dirty editor and displayed retained job", async ({ page }) => {
  const model = await fixture(page);
  await openCompleted(page, model);
  const draft = "index=main | where status=409";
  await page.getByTestId("search-input").fill(draft);
  model.state.selectedAppId = "app-2";
  await page.evaluate(() => window.dispatchEvent(new StorageEvent("storage", {
    key: "open-splunk.app-catalog-invalidation", newValue: "review-fallback",
  })));
  await expect(page).toHaveURL(/appId=app-2/u);
  await expect(page.getByTestId("run-search")).toBeEnabled();
  await expect(page.getByTestId("search-input")).toHaveValue(draft);
  await expect(page.getByTestId("event-list")).toContainText("retained review-job-1");
  expect(model.creates).toHaveLength(1);
  expect(model.gets).toHaveLength(0);
});

test("a lost search response reuses its logical request key and a later accepted run gets a new key", async ({ page }) => {
  const model = await fixture(page);
  await page.goto(`/search/events/?${new URLSearchParams({ q: oldQuery, run: "0", appId: "app-1" })}`);
  const run = page.getByTestId("run-search");
  await expect(run).toBeEnabled();
  model.state.dropCreateResponseOnce = true;
  await run.click();
  await expect(page.getByRole("heading", { name: "Search failed", exact: true })).toBeVisible();
  expect(model.creates).toHaveLength(1);
  const originalKey = model.creates[0].clientRequestId;
  expect(originalKey).toMatch(/^[\x21-\x7e]{16,128}$/u);
  await run.click();
  await expect(page.getByTestId("event-list")).toContainText("retained review-job-1");
  expect(model.creates).toHaveLength(2);
  expect(model.creates[1].clientRequestId).toBe(originalKey);
  await run.click();
  await expect(page.getByTestId("event-list")).toContainText("retained review-job-2");
  expect(model.creates).toHaveLength(3);
  expect(model.creates[2].clientRequestId).not.toBe(originalKey);
});

test("Patterns opens exact retained members and exports the captured relation without a wildcard search", async ({ page }) => {
  const model = await fixture(page);
  await openCompleted(page, model);
  await page.getByTestId("result-tab-patterns").click();
  await page.getByRole("button", { name: "View events", exact: true }).click();
  await expect(page.getByRole("region", { name: "Exact pattern event filter" })).toContainText("literal * <int>");
  await expect(page.getByTestId("event-list")).toContainText("retained review-job-1");
  expect(model.memberRequests).toHaveLength(1);
  expect(model.memberRequests[0]).toMatchObject({ searchJobId: "review-job-1", snapshotRef: "snapshot-review-job-1", patternId: "exact-retained-group", columns: [] });
  await expect(page.getByTestId("search-input")).toHaveValue(oldQuery);
  expect(model.creates).toHaveLength(1);
  await page.getByRole("button", { name: "Export", exact: true }).click();
  await page.getByRole("button", { name: "Create export", exact: true }).click();
  await expect(page.getByRole("button", { name: "Retry export", exact: true })).toBeEnabled();
  expect(model.exportRequests[0].definition?.source).toEqual({ $case: "patternMembers", value: {
    searchJobId: "review-job-1", snapshotRef: "snapshot-review-job-1", sensitivity: PatternSensitivity.PATTERN_SENSITIVITY_BALANCED, patternId: "exact-retained-group",
  } });
  await page.getByRole("button", { name: "Retry export", exact: true }).click();
  await expect.poll(() => model.exportRequests.length).toBe(2);
  expect(model.exportRequests[1]).toEqual(model.exportRequests[0]);
  await page.getByRole("button", { name: "Cancel", exact: true }).click();
  await page.getByRole("button", { name: "Clear pattern", exact: true }).click();
  await expect(page.getByRole("region", { name: "Exact pattern event filter" })).toHaveCount(0);
  await expect(page.getByTestId("event-list")).toContainText("retained review-job-1");
  expect(model.creates).toHaveLength(1);
});
