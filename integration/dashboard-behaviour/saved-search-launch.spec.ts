import { expect, test, type Page, type Route } from "@playwright/test";
import { AppState } from "../../gen/ts/open_splunk/app";
import { SharingScope } from "../../gen/ts/open_splunk/common";
import { IndexAccessState, IndexState } from "../../gen/ts/open_splunk/index";
import { GetSystemBootstrapResponse, ServerFeature } from "../../gen/ts/open_splunk/system_api";
import { SavedSearch } from "../../gen/ts/open_splunk/saved_search";
import { GetSavedSearchResponse, ListSavedSearchesResponse, UpdateSavedSearchRequest, UpdateSavedSearchResponse } from "../../gen/ts/open_splunk/saved_search_api";
import { SearchHistoryEntry } from "../../gen/ts/open_splunk/history";
import { GetSearchHistoryEntryResponse, ListSearchHistoryResponse } from "../../gen/ts/open_splunk/history_api";
import { SearchExecutionPhase, SearchJob, SearchJobOrigin, SearchJobState } from "../../gen/ts/open_splunk/search";
import { CreateSearchJobRequest, CreateSearchJobResponse, ValidateSearchResponse } from "../../gen/ts/open_splunk/search_api";

const spl = " \nindex=main | table _time _raw\t";
const savedName = "Whitespace saved report";
const headers = { "content-type": "application/x-protobuf" };
const requestID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;

async function fixture(page: Page, delayed: boolean) {
  let release!: () => void;
  const pending = new Promise<void>((resolve) => { release = resolve; });
  const state = { gets: 0, holdCreate: false, creates: [] as CreateSearchJobRequest[], updates: [] as UpdateSavedSearchRequest[] };
  let saved = SavedSearch.fromPartial({
    savedSearchId: "saved-whitespace", version: 1,
    definition: { name: savedName, ownerId: "owner-1", sharingScope: SharingScope.SHARING_SCOPE_APP,
      search: { spl, appId: "app-1", indexScope: ["main"], timeRange: { earliest: "2026-09-12T05:00:00Z", latest: "2026-09-12T06:00:00Z", timezone: "UTC" } } },
  });
  const history = SearchHistoryEntry.fromPartial({ searchJobId: "history-whitespace", definition: saved.definition?.search,
    finalState: SearchJobState.SEARCH_JOB_STATE_CANCELED, createdAt: new Date("2026-09-12T06:00:00Z"), effectiveIndexScope: ["main"] });
  const fulfill = (route: Route, bytes: Uint8Array) => route.fulfill({ status: 200, headers, body: Buffer.from(bytes) });
  await page.route("**/api/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    if (path === "/api/system/bootstrap") return fulfill(route, GetSystemBootstrapResponse.encode(GetSystemBootstrapResponse.fromPartial({
      apps: [{ appId: "app-1", slug: "main", displayName: "Main", defaultIndexNames: ["main"], state: AppState.APP_STATE_ACTIVE }],
      indexes: [{ indexId: "index-1", name: "main", state: IndexState.INDEX_STATE_ACTIVE, searchAccess: IndexAccessState.INDEX_ACCESS_STATE_ENABLED }],
      selectedAppId: "app-1", serverTime: new Date("2026-09-12T07:00:00Z"), limits: { maximumPageSize: 100 },
      features: [ServerFeature.SERVER_FEATURE_SEARCH, ServerFeature.SERVER_FEATURE_SAVED_SEARCHES, ServerFeature.SERVER_FEATURE_SEARCH_HISTORY],
    })).finish());
    if (path === "/api/saved-searches/list") return fulfill(route, ListSavedSearchesResponse.encode(ListSavedSearchesResponse.fromPartial({ savedSearches: [saved] })).finish());
    if (path === "/api/search/history/list") return fulfill(route, ListSearchHistoryResponse.encode(ListSearchHistoryResponse.fromPartial({ historyEntries: [history] })).finish());
    if (path === "/api/saved-searches/get" || path === "/api/search/history/get") {
      state.gets += 1;
      if (delayed) await pending;
      return path === "/api/saved-searches/get"
        ? fulfill(route, GetSavedSearchResponse.encode({ savedSearch: saved }).finish())
        : fulfill(route, GetSearchHistoryEntryResponse.encode({ historyEntry: history }).finish());
    }
    if (path === "/api/search/validate") return fulfill(route, ValidateSearchResponse.encode(ValidateSearchResponse.fromPartial({ valid: true })).finish());
    if (path === "/api/search/jobs/create") {
      const request = CreateSearchJobRequest.decode(route.request().postDataBuffer()!);
      state.creates.push(request);
      if (state.holdCreate) await pending;
      const job = SearchJob.fromPartial({ searchJobId: `hydrated-job-${state.creates.length}`, stateVersion: 1n,
        definition: request.definition, source: request.source, effectiveIndexScope: ["main"],
        state: SearchJobState.SEARCH_JOB_STATE_CANCELED,
        progress: { phase: SearchExecutionPhase.SEARCH_EXECUTION_PHASE_COMPLETE, percentComplete: 100, stateVersion: 1n } });
      return fulfill(route, CreateSearchJobResponse.encode({ searchJob: job, replayed: false }).finish());
    }
    if (path === "/api/saved-searches/update") {
      const request = UpdateSavedSearchRequest.decode(route.request().postDataBuffer()!);
      state.updates.push(request);
      saved = SavedSearch.fromPartial({ ...saved, version: saved.version + 1n, definition: request.definition });
      return fulfill(route, UpdateSavedSearchResponse.encode({ savedSearch: saved }).finish());
    }
    return route.fulfill({ status: 404, body: `Unsupported fixture endpoint: ${path}` });
  });
  return { state, release, saved };
}

for (const delayed of [false, true]) {
  for (const run of ["0", "1"]) {
    test(`saved URL hydration retains identity with run=${run} and ${delayed ? "delayed" : "immediate"} response`, async ({ page }) => {
      const model = await fixture(page, delayed);
      try {
        await page.goto(`/search/events/?savedSearchId=saved-whitespace&appId=app-1&run=${run}`);
        if (delayed) {
          await expect.poll(() => model.state.gets).toBe(1);
          await expect(page.getByTestId("job-strip")).toContainText("Opening persisted search");
          await expect(page.getByTestId("run-search")).toBeDisabled();
          expect(model.state.creates).toHaveLength(0);
          model.release();
        }
        await expect(page.getByRole("heading", { name: savedName, exact: true })).toBeVisible();
        await expect(page.getByTestId("search-input")).toHaveValue(spl);
        if (run === "1") {
          await expect(page.getByTestId("job-strip")).toContainText("Canceled");
          expect(model.state.creates).toHaveLength(1);
          expect(model.state.creates[0].source).toEqual(CreateSearchJobRequest.fromPartial({ source: {
            origin: SearchJobOrigin.SEARCH_JOB_ORIGIN_SAVED_SEARCH, savedSearchId: "saved-whitespace",
          } }).source);
          expect(model.state.creates[0].definition?.spl).toBe(spl);
          expect(model.state.creates[0].definition?.timeRange).toEqual(model.saved.definition?.search?.timeRange);
          expect(model.state.creates[0].definition?.appId).toBe("app-1");
          expect(model.state.creates[0].clientRequestId).toMatch(requestID);
        } else expect(model.state.creates).toHaveLength(0);
        await page.getByRole("button", { name: /^Save As/u }).click();
        await page.getByRole("menuitem", { name: /Saved search/u }).click();
        const dialog = page.getByRole("dialog", { name: "Save search as", exact: true });
        await expect(dialog).toBeVisible();
        await expect(dialog.getByLabel("Sharing")).toContainText("Private");
        await dialog.getByRole("button", { name: "Cancel", exact: true }).click();
        await page.getByTestId("search-input").fill(`${spl.trim()} | head 1`);
        await page.locator(".search-action-save").click();
        await expect(page.getByTestId("toast")).toHaveText(`Saved “${savedName}”.`);
        expect(model.state.updates).toHaveLength(1);
        expect(model.state.updates[0].savedSearchId).toBe("saved-whitespace");
        expect(model.state.updates[0].definition?.sharingScope).toBe(SharingScope.SHARING_SCOPE_APP);
        expect(model.state.updates[0].updateMask).toEqual(["search"]);
      } finally { model.release(); }
    });
  }
}

test("delayed history URL draft opens without an active-job self-block", async ({ page }) => {
  const model = await fixture(page, true);
  try {
    await page.goto("/search/events/?historySearchId=history-whitespace&appId=app-1&run=0");
    await expect.poll(() => model.state.gets).toBe(1);
    await expect(page.getByTestId("job-strip")).toContainText("Opening persisted search");
    model.release();
    await expect(page.getByTestId("search-input")).toHaveValue(spl);
    await expect(page.getByTestId("toast")).toHaveText("Search restored without running.");
    await expect(page.getByTestId("run-search")).toBeEnabled();
    expect(model.state.creates).toHaveLength(0);
  } finally { model.release(); }
});


test("manual saved Open stays blocked while an ordinary admission is queued", async ({ page }) => {
  const model = await fixture(page, false);
  model.state.holdCreate = true;
  const draft = "index=main | head 2";
  try {
    await page.goto(`/search/events/?q=${encodeURIComponent(draft)}&appId=app-1&run=0`);
    await expect(page.getByTestId("run-search")).toBeEnabled();
    await page.getByTestId("run-search").click();
    await expect.poll(() => model.state.creates.length).toBe(1);
    await expect(page.getByTestId("job-strip")).toHaveAttribute("aria-busy", "true");
    await page.getByRole("button", { name: "Open", exact: true }).click();
    const library = page.getByRole("dialog", { name: "Open a saved search", exact: true });
    await library.locator(".saved-row-main").filter({ hasText: savedName }).click();
    await expect(page.getByTestId("toast")).toHaveText("Cancel the active search before opening a saved search.");
    await expect(library).toBeVisible();
    await expect(page.getByTestId("search-input")).toHaveValue(draft);
    await expect(page.getByRole("heading", { name: "New Search", exact: true })).toBeVisible();
    expect(model.state.creates).toHaveLength(1);
  } finally { model.release(); }
});
