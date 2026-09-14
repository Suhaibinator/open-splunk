import { once } from "node:events";

import { expect, test, type Download, type Locator, type Page, type Request } from "@playwright/test";

import { SharingScope } from "../gen/ts/open_splunk/common";
import { CreateExportJobRequest } from "../gen/ts/open_splunk/export_api";
import {
  CreateSavedSearchRequest,
  UpdateSavedSearchRequest,
} from "../gen/ts/open_splunk/saved_search_api";
import { SearchDefinition, SearchJobOrigin } from "../gen/ts/open_splunk/search";
import { CreateSearchJobRequest } from "../gen/ts/open_splunk/search_api";

const baseURL = loopbackOrigin(requiredEnvironment("OPEN_SPLUNK_FEATURE_COMPLETION_BASE_URL"));
const appID = requiredEnvironment("OPEN_SPLUNK_FEATURE_COMPLETION_APP_ID");
const savedSearchID = requiredEnvironment("OPEN_SPLUNK_FEATURE_COMPLETION_SAVED_SEARCH_ID");
const administratorToken = secretEnvironment("OPEN_SPLUNK_FEATURE_COMPLETION_ADMINISTRATOR_TOKEN");
const fixtureStart = requiredDate("OPEN_SPLUNK_FEATURE_COMPLETION_FIXTURE_START");
const bulkStart = requiredDate("OPEN_SPLUNK_FEATURE_COMPLETION_BULK_START");
const expectedBulkRows = positiveInteger("OPEN_SPLUNK_FEATURE_COMPLETION_EXPECTED_BULK_ROWS");
const browserExecutable = process.env.OPEN_SPLUNK_BROWSER_EXECUTABLE?.trim();
const timeout = 60_000;
const bulkQuery = "index=vertical-bulk";
const verticalQuery = "index=vertical | dedup event_id";
const expectedBulkPattern = "bulk export vertical-bulk-<int>";
const savedName = "Feature completion scope roundtrip";

interface BrowserObservation {
  externalRequests: string[];
  failedRequests: string[];
  pageErrors: string[];
  searchCreates: number;
  searchRequests: CreateSearchJobRequest[];
  exportCreates: CreateExportJobRequest[];
  savedCreates: CreateSavedSearchRequest[];
  savedUpdates: UpdateSavedSearchRequest[];
  savedUpdateSuccesses: number;
}

function requiredEnvironment(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function secretEnvironment(name: string): string {
  const value = process.env[name];
  if (!value || value.trim().length < 32) throw new Error(`${name} is missing or invalid`);
  return value.trim();
}

function loopbackOrigin(value: string): string {
  const parsed = new URL(value);
  if (
    (parsed.protocol !== "http:" && parsed.protocol !== "https:")
    || (parsed.hostname !== "127.0.0.1" && parsed.hostname !== "localhost")
    || parsed.username
    || parsed.password
    || parsed.pathname !== "/"
    || parsed.search
    || parsed.hash
  ) throw new Error("feature-completion base URL must be a loopback HTTP(S) origin");
  return parsed.origin;
}

function requiredDate(name: string): Date {
  const value = requiredEnvironment(name);
  const parsed = new Date(value);
  if (!Number.isFinite(parsed.valueOf())) throw new Error(`${name} must be an RFC3339 timestamp`);
  return parsed;
}

function positiveInteger(name: string): number {
  const value = requiredEnvironment(name);
  if (!/^[1-9][0-9]*$/u.test(value)) throw new Error(`${name} must be a positive integer`);
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) throw new Error(`${name} exceeds the safe integer range`);
  return parsed;
}

function featureURL(pathname: string, parameters: Record<string, string> = {}): string {
  const url = new URL(pathname, baseURL);
  url.search = new URLSearchParams({ ...parameters, appId: appID }).toString();
  return url.href;
}

function searchURL(query: string, earliest: Date, latest: Date): string {
  return featureURL("/search/events/", {
    q: query,
    earliest: earliest.toISOString(),
    latest: latest.toISOString(),
    timezone: "UTC",
    run: "0",
  });
}

function observeBrowser(page: Page): BrowserObservation {
  const observation: BrowserObservation = {
    externalRequests: [],
    failedRequests: [],
    pageErrors: [],
    searchCreates: 0,
    searchRequests: [],
    exportCreates: [],
    savedCreates: [],
    savedUpdates: [],
    savedUpdateSuccesses: 0,
  };
  page.on("pageerror", (error) => observation.pageErrors.push(error.message));
  page.on("request", (request) => observeRequest(request, observation));
  page.on("response", (response) => {
    const url = new URL(response.url());
    if (
      url.origin === baseURL
      && url.pathname === "/api/saved-searches/update"
      && response.status() >= 200
      && response.status() < 300
    ) observation.savedUpdateSuccesses += 1;
    if (url.origin === baseURL && url.pathname.startsWith("/api/") && response.status() >= 400) {
      observation.failedRequests.push(`${response.status()} ${url.pathname}`);
    }
  });
  return observation;
}

function observeRequest(request: Request, observation: BrowserObservation): void {
  const url = new URL(request.url());
  if ((url.protocol === "http:" || url.protocol === "https:") && url.origin !== baseURL) {
    observation.externalRequests.push(`${request.method()} ${url.origin}${url.pathname}`);
    return;
  }
  if (request.method() !== "POST") return;
  const body = request.postDataBuffer();
  if (url.pathname === "/api/search/jobs/create") observation.searchCreates += 1;
  if (body === null) return;
  if (url.pathname === "/api/search/jobs/create") {
    observation.searchRequests.push(CreateSearchJobRequest.decode(body));
  } else if (url.pathname === "/api/search/exports/create") {
    observation.exportCreates.push(CreateExportJobRequest.decode(body));
  } else if (url.pathname === "/api/saved-searches/create") {
    observation.savedCreates.push(CreateSavedSearchRequest.decode(body));
  } else if (url.pathname === "/api/saved-searches/update") {
    observation.savedUpdates.push(UpdateSavedSearchRequest.decode(body));
  }
}

async function chooseOption(control: Locator, name: string): Promise<void> {
  await control.click();
  const listboxID = await control.getAttribute("aria-controls");
  expect(listboxID).not.toBeNull();
  await control.page().locator(`[id="${listboxID}"]`).getByRole("option", { name, exact: true }).click();
}

async function waitForCompletedSearch(page: Page, rows: number, unit: "events" | "rows" = "events"): Promise<void> {
  const strip = page.getByTestId("job-strip");
  await expect(strip).toHaveAttribute("aria-busy", "false", { timeout });
  await expect(strip).toContainText("Completed", { timeout });
  await expect(strip).toContainText(`${rows.toLocaleString("en-US")} ${unit}`, { timeout });
}

async function openAndRunSearch(page: Page, query: string, earliest: Date, latest: Date, rows: number): Promise<void> {
  await page.goto(searchURL(query, earliest, latest), { waitUntil: "domcontentloaded", timeout });
  await expect(page.getByTestId("search-workspace")).toBeVisible({ timeout });
  await expect(page.getByText("Backend data", { exact: true })).toBeVisible({ timeout });
  await expect(page.getByTestId("search-input")).toHaveValue(query);
  await page.getByTestId("run-search").click();
  await waitForCompletedSearch(page, rows);
}

async function downloadContents(download: Download): Promise<string> {
  const stream = await download.createReadStream();
  if (stream === null) throw new Error("browser download did not expose a readable artifact");
  const chunks: Buffer[] = [];
  stream.on("data", (chunk: Buffer | string) => chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)));
  await once(stream, "end");
  return Buffer.concat(chunks).toString("utf8");
}

async function createJSONLinesExport(page: Page, expectedRows: number): Promise<string> {
  const dialog = page.getByRole("dialog", { name: /^Export /u });
  await expect(dialog).toBeVisible({ timeout });
  await dialog.getByLabel("Export as JSON Lines").check();
  const [created] = await Promise.all([
    page.waitForResponse((response) => {
      const url = new URL(response.url());
      return url.origin === baseURL && url.pathname === "/api/search/exports/create"
        && response.request().method() === "POST";
    }, { timeout }),
    dialog.getByRole("button", { name: "Create export" }).click(),
  ]);
  expect(created.status(), created.ok() ? "export admission" : await created.text()).toBe(200);
  await expect(dialog.getByTestId("export-dialog")).toHaveAttribute("aria-busy", "false", { timeout });
  expect(await dialog.getByRole("alert").allTextContents(), "export preparation errors").toEqual([]);
  await expect(dialog.locator(".workspace-dialog-export-summary")).toContainText(
    `Rows${expectedRows.toLocaleString("en-US")}`,
    { timeout },
  );
  const downloadButton = dialog.getByRole("button", { name: /^Download /u });
  const [download] = await Promise.all([
    page.waitForEvent("download", { timeout }),
    downloadButton.click(),
  ]);
  return downloadContents(download);
}

function jsonLines(contents: string): Record<string, unknown>[] {
  return contents.trimEnd().split("\n").filter(Boolean).map((line) => JSON.parse(line) as Record<string, unknown>);
}

function assertExactBulkMembers(rows: Record<string, unknown>[]): void {
  const members = new Map<string, string>();
  for (const row of rows) {
    const eventID = row["event_id"];
    const raw = row["_raw"];
    if (typeof eventID !== "string" || typeof raw !== "string") {
      throw new Error("pattern member export omitted a string event_id or _raw field");
    }
    if (members.has(eventID)) throw new Error(`pattern member export duplicated ${eventID}`);
    members.set(eventID, raw);
  }
  if (members.size !== expectedBulkRows) {
    throw new Error(`pattern member export contained ${members.size.toString()} unique IDs; want ${expectedBulkRows.toString()}`);
  }
  for (let sequence = 1; sequence <= expectedBulkRows; sequence += 1) {
    const eventID = `vertical-bulk-${sequence.toString().padStart(5, "0")}`;
    const expectedRaw = `bulk export ${eventID}`;
    if (members.get(eventID) !== expectedRaw) {
      throw new Error(`pattern member ${eventID} did not preserve its exact raw value`);
    }
  }
}

async function signInAndInspectHEC(page: Page): Promise<void> {
  await page.goto(featureURL("/signin/"), { waitUntil: "domcontentloaded", timeout });
  const tokenInput = page.getByLabel("Administrator bearer token");
  try {
    await tokenInput.fill(administratorToken);
    await page.getByRole("button", { name: "Open administrator session" }).click();
  } finally {
    if (await tokenInput.isVisible().catch(() => false)) await tokenInput.fill("");
  }
  await expect(page.getByRole("heading", { name: "Administration" })).toBeVisible({ timeout });
  const tokenPersisted = await page.evaluate((token) =>
    document.body.textContent?.includes(token) === true
    || Object.values(localStorage).some((value) => value.includes(token))
    || Object.values(sessionStorage).some((value) => value.includes(token)), administratorToken);
  if (tokenPersisted) throw new Error("administrator credential reached rendered text or browser storage");
  await page.locator(".admin-sidebar").getByRole("button", { name: /Server settings/u }).click();
  await expect(page.getByRole("heading", { name: "Server settings" })).toBeVisible({ timeout });
  await expect(page.getByText(
    /HTTP Event Collector is disabled|HTTP Event Collector operations|HEC operational snapshot/u,
  ).first()).toBeVisible({ timeout });
}

async function verifyPatterns(page: Page, observation: BrowserObservation): Promise<void> {
  await openAndRunSearch(
    page,
    bulkQuery,
    new Date(bulkStart.valueOf() - 60_000),
    new Date(bulkStart.valueOf() + 120_000),
    expectedBulkRows,
  );
  await page.getByTestId("result-tab-patterns").click();
  const panel = page.getByRole("tabpanel", { name: /Patterns/u });
  await expect(panel).toContainText(
    `${expectedBulkRows.toLocaleString("en-US")} eligible events of ${expectedBulkRows.toLocaleString("en-US")} retained rows; 0 excluded`,
    { timeout },
  );
  await expect(panel).toContainText("The retained snapshot is truncated.");
  const group = panel.locator(".pattern-table article").filter({ hasText: expectedBulkPattern });
  await expect(group).toHaveCount(1, { timeout });
  await expect(group).toContainText(`${expectedBulkRows.toLocaleString("en-US")} events`);
  await expect(group).toContainText("100.0%");

  await panel.getByRole("button", { name: "Export all patterns" }).click();
  const summaryContents = await createJSONLinesExport(page, 1);
  const summaryRows = jsonLines(summaryContents);
  expect(summaryRows).toHaveLength(1);
  expect(summaryRows[0]?.pattern).toBe(expectedBulkPattern);
  expect(Number(summaryRows[0]?.count)).toBe(expectedBulkRows);
  expect(Number(summaryRows[0]?.percent)).toBe(100);

  const createsBeforeMembers = observation.searchCreates;
  await group.getByRole("button", { name: "View events" }).click();
  await expect(page.getByTestId("result-tab-events")).toHaveAttribute("aria-selected", "true", { timeout });
  const exactFilter = page.getByRole("region", { name: "Exact pattern event filter" });
  await expect(exactFilter).toContainText(`Pattern: ${expectedBulkPattern}`);
  await expect(exactFilter).toContainText(`${expectedBulkRows.toLocaleString("en-US")} exact members`);
  await expect(page.getByTestId("search-input")).toHaveValue(bulkQuery);
  expect(observation.searchCreates, "pattern selection must not rerun SPL").toBe(createsBeforeMembers);

  await page.getByRole("button", { name: "Export", exact: true }).click();
  const memberContents = await createJSONLinesExport(page, expectedBulkRows);
  const memberRows = jsonLines(memberContents);
  expect(memberRows).toHaveLength(expectedBulkRows);
  assertExactBulkMembers(memberRows);

  expect(observation.exportCreates).toHaveLength(2);
  for (const request of observation.exportCreates) {
    expect(request.definition?.byteLimit, "workspace exports use the server default reservation").toBeUndefined();
  }
  const summarySource = observation.exportCreates[0]?.definition?.source;
  const memberSource = observation.exportCreates[1]?.definition?.source;
  expect(summarySource?.$case).toBe("patternSummary");
  expect(memberSource?.$case).toBe("patternMembers");
  if (summarySource?.$case !== "patternSummary" || memberSource?.$case !== "patternMembers") {
    throw new Error("pattern exports omitted their retained relation source");
  }
  expect(summarySource.value.searchJobId).toBe(memberSource.value.searchJobId);
  expect(summarySource.value.snapshotRef).toBe(memberSource.value.snapshotRef);
  expect(memberSource.value.patternId).not.toBe("");
}

async function verifyNearbyBack(page: Page, observation: BrowserObservation): Promise<void> {
  await openAndRunSearch(
    page,
    verticalQuery,
    fixtureStart,
    new Date(fixtureStart.valueOf() + 4_000),
    4,
  );
  const dirtyQuery = `${verticalQuery} | head 2`;
  await page.getByTestId("search-input").fill(dirtyQuery);
  await expect(page.getByText("Run to apply draft", { exact: true })).toBeVisible();
  const createsBeforeNearby = observation.searchCreates;
  await page.locator('.event-time[title="Find nearby events"]').first().click();
  const context = page.getByRole("region", { name: "Nearby event context" });
  await expect(context).toBeVisible({ timeout });
  const nearbyQuery = 'index="vertical" | where \'index\' = "vertical" AND \'host\' = "vertical-host" AND \'source\' = "app.log"';
  await expect(page.getByTestId("search-input")).toHaveValue(nearbyQuery, { timeout });
  const earliest = new Date(await context.getByLabel("Nearby earliest time").inputValue());
  const latest = new Date(await context.getByLabel("Nearby latest time").inputValue());
  expect(latest.valueOf() - earliest.valueOf()).toBe(600_000);
  await expect(page.getByTestId("job-strip")).toHaveAttribute("aria-busy", "false", { timeout });
  await expect(page.getByTestId("job-strip")).toContainText("Completed", { timeout });
  expect(observation.searchCreates).toBe(createsBeforeNearby + 1);

  await page.goBack({ waitUntil: "domcontentloaded", timeout });
  await expect(page.getByTestId("search-input")).toHaveValue(dirtyQuery, { timeout });
  await waitForCompletedSearch(page, 4);
  expect(observation.searchCreates, "Back must reopen the retained job").toBe(createsBeforeNearby + 1);
}

async function verifySavedSearchScope(page: Page, observation: BrowserObservation): Promise<void> {
  const searchesBefore = observation.searchCreates;
  await page.goto(featureURL("/search/events/", { savedSearchId: savedSearchID, run: "1" }), {
    waitUntil: "domcontentloaded",
    timeout,
  });
  await expect(page.getByTestId("search-workspace")).toBeVisible({ timeout });
  await waitForCompletedSearch(page, 4, "rows");
  await expect(page.getByRole("heading", { name: "SPL persisted lookup vertical", exact: true })).toBeVisible();
  const originalSearch = await page.getByTestId("search-input").inputValue();
  expect(observation.searchCreates).toBe(searchesBefore + 1);
  const savedRun = observation.searchRequests.at(-1);
  expect(savedRun?.source?.origin).toBe(SearchJobOrigin.SEARCH_JOB_ORIGIN_SAVED_SEARCH);
  expect(savedRun?.source?.savedSearchId).toBe(savedSearchID);
  expect(savedRun?.definition?.spl).toBe(originalSearch);
  expect(savedRun?.clientRequestId).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u);

  await page.getByRole("button", { name: /^Save As/u }).click();
  await page.getByRole("menuitem", { name: /Saved search/u }).click();
  const dialog = page.getByRole("dialog", { name: "Save search as" });
  await expect(dialog).toBeVisible();
  const sharing = dialog.getByLabel("Sharing");
  await expect(sharing).toContainText("Private");
  await dialog.getByLabel("Name").fill(savedName);
  await chooseOption(sharing, "Global");
  await dialog.getByRole("button", { name: "Save", exact: true }).click();
  await expect(page.getByTestId("toast")).toHaveText(`Saved “${savedName}”.`, { timeout });
  expect(observation.savedCreates).toHaveLength(1);
  const createdDefinition = observation.savedCreates[0]?.definition;
  expect(createdDefinition?.sharingScope).toBe(SharingScope.SHARING_SCOPE_GLOBAL);
  expect(createdDefinition?.name).toBe(savedName);
  expect(createdDefinition?.search?.appId).toBe(appID);
  expect(createdDefinition?.search?.spl).toBe(originalSearch);

  const quickSavedQuery = `${originalSearch.trim()} | head 1`;
  await page.getByTestId("search-input").fill(quickSavedQuery);
  const updatesBeforeQuickSave = observation.savedUpdateSuccesses;
  await page.locator(".search-action-save").click();
  await expect.poll(() => observation.savedUpdateSuccesses, { timeout }).toBe(updatesBeforeQuickSave + 1);
  await expect(page.getByTestId("toast")).toHaveText(`Saved “${savedName}”.`, { timeout });
  const quickSave = observation.savedUpdates.at(-1);
  expect(quickSave?.updateMask).toEqual(["search"]);
  expect(quickSave?.definition?.sharingScope).toBe(SharingScope.SHARING_SCOPE_GLOBAL);
  expect(quickSave?.definition?.search?.spl).toBe(quickSavedQuery);

  await page.goto(featureURL("/reports/saved-searches/"), { waitUntil: "domcontentloaded", timeout });
  await expect(page.getByRole("heading", { name: "Saved searches" })).toBeVisible({ timeout });
  const row = page.locator("tr").filter({ has: page.locator(`[aria-label="Saved search ${savedName}"]`) });
  await expect(row).toHaveCount(1, { timeout });
  await expect(row.locator('[data-label="Sharing"]')).toHaveText("Global");
  await row.getByRole("button", { name: `Edit sharing for ${savedName}` }).click();
  const scopeDialog = page.getByRole("dialog", { name: "Edit saved-search sharing" });
  await chooseOption(scopeDialog.getByLabel("Sharing"), "App");
  await scopeDialog.getByRole("button", { name: "Save sharing" }).click();
  await expect(row.locator('[data-label="Sharing"]')).toHaveText("App", { timeout });

  const scopeUpdate = observation.savedUpdates.at(-1);
  expect(scopeUpdate?.updateMask).toEqual(["sharing_scope"]);
  expect(scopeUpdate?.definition?.sharingScope).toBe(SharingScope.SHARING_SCOPE_APP);
  expect(scopeUpdate?.definition?.name).toBe(quickSave?.definition?.name);
  expect(scopeUpdate?.definition?.description).toBe(quickSave?.definition?.description);
  expect(scopeUpdate?.definition?.ownerId).toBe(quickSave?.definition?.ownerId);
  expect(scopeUpdate?.definition?.schedule).toBeUndefined();
  expect(SearchDefinition.encode(scopeUpdate!.definition!.search!).finish()).toEqual(
    SearchDefinition.encode(quickSave!.definition!.search!).finish(),
  );

  await row.getByRole("link", { name: `Open ${savedName} in Search` }).click();
  await expect(page.getByRole("heading", { name: savedName })).toBeVisible({ timeout });
  await expect(page.getByTestId("search-input")).toHaveValue(quickSavedQuery);
}

async function verifyBundledHelp(page: Page): Promise<void> {
  await page.goto(featureURL("/reports/saved-searches/"), { waitUntil: "domcontentloaded", timeout });
  await page.getByRole("button", { name: /^Help/u }).click();
  await page.getByRole("menuitem", { name: "Documentation", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Documentation" })).toBeVisible({ timeout });
  await expect(page.locator(".help-source-path")).toContainText("docs/README.md");
  await expect(page.locator(".help-revision")).toContainText(/^Docs revision .+ · content [a-f0-9]{12}$/u);
  expect(new URL(page.url()).searchParams.get("appId")).toBe(appID);

  await page.getByLabel("Search documentation").fill("saved search");
  const results = page.getByRole("region", { name: "Documentation search results" });
  await expect(results.getByRole("status")).toHaveText(/[1-9][0-9]* documentation results?\./u);
  const firstResult = results.getByRole("link").first();
  const href = await firstResult.getAttribute("href");
  expect(href).not.toBeNull();
  const local = new URL(href!, baseURL);
  expect(local.origin).toBe(baseURL);
  expect(local.pathname.startsWith("/help/")).toBe(true);
  expect(local.searchParams.get("appId")).toBe(appID);
  await firstResult.click();
  await expect(page.locator(".help-content h1")).toBeVisible({ timeout });
  expect(new URL(page.url()).pathname.startsWith("/help/")).toBe(true);

  await page.getByLabel("Search documentation").fill("Docker Compose recovery topology");
  const exampleResult = page.getByRole("region", { name: "Documentation search results" })
    .getByRole("link", { name: /Docker Compose recovery topology/u });
  await expect(exampleResult).toHaveCount(1);
  await exampleResult.click();
  await expect(page.getByRole("heading", { name: "Docker Compose recovery topology" })).toBeVisible({ timeout });
  await expect(page.getByRole("region", { name: "yaml code example" })).toContainText("services:");

  const splLink = page.locator(".help-navigation").getByRole("link", { name: "SPL", exact: true });
  await splLink.click();
  await expect(page.locator(".help-content h1")).toHaveText("SPL", { timeout });
  expect(new URL(page.url()).pathname).toBe("/help/spl/");
}

test.use({
  ignoreHTTPSErrors: true,
  launchOptions: browserExecutable ? { executablePath: browserExecutable } : {},
  screenshot: "only-on-failure",
  trace: "off",
});

test("compiled backend and embedded UI complete the advertised feature flow", async ({ page }) => {
  test.setTimeout(170_000);
  await page.setViewportSize({ width: 1_280, height: 900 });
  const observation = observeBrowser(page);

  await signInAndInspectHEC(page);
  await verifyPatterns(page, observation);
  await verifyNearbyBack(page, observation);
  await verifySavedSearchScope(page, observation);
  await verifyBundledHelp(page);

  expect(observation.pageErrors, "uncaught browser errors").toEqual([]);
  expect(observation.failedRequests, "failed same-origin API responses").toEqual([]);
  expect(observation.externalRequests, "external browser requests").toEqual([]);
  const tokenRendered = await page.evaluate((token) => document.body.textContent?.includes(token) === true, administratorToken);
  if (tokenRendered) throw new Error("administrator credential reached rendered browser text");
});
