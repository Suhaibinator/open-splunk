import {
  expect,
  test,
  type Locator,
  type Page,
  type Response,
  type WebSocket,
  type WebSocketRoute,
} from "@playwright/test";

import {
  CreateSearchJobResponse,
  GetSearchJobResponse,
  GetSearchResultsResponse,
} from "../gen/ts/open_splunk/v1/search_api";
import {
  ResynchronizationReason,
  SearchWebSocketCommand,
  SearchWebSocketEvent,
} from "../gen/ts/open_splunk/v1/search_ws";

const baseURL = requiredEnvironment("OPEN_SPLUNK_E2E_BASE_URL");
const searchSPL = requiredEnvironment("OPEN_SPLUNK_E2E_SPL");
const earliest = requiredEnvironment("OPEN_SPLUNK_E2E_EARLIEST");
const latest = requiredEnvironment("OPEN_SPLUNK_E2E_LATEST");
const expectedText = requiredEnvironment("OPEN_SPLUNK_E2E_EXPECTED_TEXT");
const expectedRows = parsePositiveInteger(requiredEnvironment("OPEN_SPLUNK_E2E_EXPECTED_ROWS"));
const browserExecutable = process.env.OPEN_SPLUNK_BROWSER_EXECUTABLE?.trim();
const recoveryControlURL = optionalLoopbackURL(process.env.OPEN_SPLUNK_E2E_RECOVERY_CONTROL_URL);
const recoveryControlToken = process.env.OPEN_SPLUNK_E2E_RECOVERY_CONTROL_TOKEN?.trim();
const recoveryInitialText = process.env.OPEN_SPLUNK_E2E_RECOVERY_INITIAL_TEXT?.trim();
const sequenceExpirationTest = process.env.OPEN_SPLUNK_E2E_SEQUENCE_EXPIRATION_TEST === "1";
const origin = validatedOrigin(baseURL);
const timeout = 45_000;

test.use({
  launchOptions: browserExecutable ? { executablePath: browserExecutable } : {},
  screenshot: "only-on-failure",
  trace: "retain-on-failure",
});

test("collector event is visible through the compiled backend UI", async ({ page }) => {
  test.setTimeout(60_000);
  const safety = observeBrowserSafety(page);
  const runSearch = await openSearchWorkspace(page);
  const { createResponsePromise, resultsResponsePromise } = waitForSearchResponses(page);
  const protocolObservation = observeSearchProtocol(page, origin, timeout);
  try {
    await runSearch.click();

    const createResponse = await createResponsePromise;
    assertProtobufResponse(createResponse);
    const browserSearchJobID = decodeCreateSearchJobID(await createResponse.body());
    const [resultsResponse] = await Promise.all([
      resultsResponsePromise,
      protocolObservation.waitForJob(browserSearchJobID),
    ]);
    assertProtobufResponse(resultsResponse);
    expect(decodeSearchResultsJobID(await resultsResponse.body())).toBe(browserSearchJobID);
  } finally {
    protocolObservation.dispose();
  }

  const jobStrip = page.getByTestId("job-strip");
  await expect(jobStrip).toHaveAttribute("aria-busy", "false", { timeout });
  await expect(jobStrip).toContainText("Completed", { timeout });
  await expect(jobStrip).toContainText(`${expectedRows} events`, { timeout });
  await expect(page.getByTestId("timeline")).toHaveAttribute("aria-label", "Event timeline", { timeout });

  const eventList = page.getByTestId("event-list");
  const finalRows = eventList.locator('[data-testid^="event-row-"]:not(.event-row--preview)');
  await expect(finalRows).toHaveCount(expectedRows, { timeout });
  await expect(eventList).toContainText(expectedText, { timeout });
  await expect(eventList.locator(".event-row--preview")).toHaveCount(0);

  const previewStatuses = await collectPreviewStatuses(page);
  expect(
    previewStatuses.some((status) => status === "live" || status === "finalizing"),
    `UI preview status transitions: ${JSON.stringify(previewStatuses)}`,
  ).toBe(true);
  expect(
    previewStatuses.filter((status) =>
      status === "paused" || status === "resyncing" || status === "finalization-error"),
    `UI preview status transitions: ${JSON.stringify(previewStatuses)}`,
  ).toEqual([]);
  await expect(page.locator("body")).not.toContainText(
    /Live job updates failed|Live job updates skipped a sequence|resynchronizing from the server/i,
  );
  assertBrowserSafety(safety);
});

test("live preview resumes from the exact retained WebSocket sequence", async ({ page }) => {
  test.setTimeout(60_000);
  const safety = observeBrowserSafety(page);
  const replay = await interceptOneRetainedReplay(page, origin, timeout);
  let releaseAuthoritativeRecovery!: () => void;
  const authoritativeRecoveryGate = new Promise<void>((resolve) => {
    releaseAuthoritativeRecovery = resolve;
  });
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/v1/search/jobs/get",
    async (route) => {
      await authoritativeRecoveryGate;
      await route.continue();
    },
  );
  const runSearch = await openSearchWorkspace(page);
  const { createResponsePromise, resultsResponsePromise } = waitForSearchResponses(page);

  try {
    await runSearch.click();
    const createResponse = await createResponsePromise;
    assertProtobufResponse(createResponse);
    const browserSearchJobID = decodeCreateSearchJobID(await createResponse.body());
    await replay.waitForCheckpoint(browserSearchJobID);
    await expect(page.getByTestId("backend-preview-status")).toHaveAttribute("data-status", "live", { timeout });
    await expect(page.locator(".event-row--preview").first()).toBeVisible({ timeout });
    await replay.disconnect();
    await replay.waitForTerminalReplay();
    await expect(page.getByTestId("backend-preview-status")).toHaveAttribute(
      "data-status",
      "finalizing",
      { timeout },
    );
    releaseAuthoritativeRecovery();

    const resultsResponse = await resultsResponsePromise;
    assertProtobufResponse(resultsResponse);
    expect(decodeSearchResultsJobID(await resultsResponse.body())).toBe(browserSearchJobID);
  } finally {
    releaseAuthoritativeRecovery();
    replay.dispose();
  }

  const jobStrip = page.getByTestId("job-strip");
  await expect(jobStrip).toHaveAttribute("aria-busy", "false", { timeout });
  await expect(jobStrip).toContainText("Completed", { timeout });
  await expect(jobStrip).toContainText(`${expectedRows} events`, { timeout });

  const eventList = page.getByTestId("event-list");
  const finalRows = eventList.locator('[data-testid^="event-row-"]:not(.event-row--preview)');
  await expect(finalRows).toHaveCount(expectedRows, { timeout });
  await expect(eventList).toContainText(expectedText, { timeout });
  await expect(eventList.locator(".event-row--preview")).toHaveCount(0);

  const previewStatuses = await collectPreviewStatuses(page);
  expect(previewStatuses, "UI preview status transitions").toContain("live");
  expect(previewStatuses, "UI preview status transitions").toContain("paused");
  expect(
    previewStatuses.filter((status) => status === "resyncing" || status === "finalization-error"),
    `UI preview status transitions: ${JSON.stringify(previewStatuses)}`,
  ).toEqual([]);
  expect(safety.createRequests(), "browser search create requests").toBe(1);
  assertBrowserSafety(safety);
});

test("live preview recovers from real sequence expiration", async ({ page }) => {
  test.skip(
    !sequenceExpirationTest
      || recoveryControlURL === undefined
      || !recoveryControlToken
      || !recoveryInitialText,
    "the deterministic sequence-expiration fixture is not enabled",
  );
  test.setTimeout(60_000);
  const safety = observeBrowserSafety(page);
  const expiration = await interceptSequenceExpiration(page, origin, timeout);
  let releaseStaleRecovery!: () => void;
  const staleRecoveryGate = new Promise<void>((resolve) => {
    releaseStaleRecovery = resolve;
  });
  let releaseFreshRecovery!: () => void;
  const freshRecoveryGate = new Promise<void>((resolve) => {
    releaseFreshRecovery = resolve;
  });
  let releaseDelayedWatchdog!: () => void;
  const delayedWatchdogGate = new Promise<void>((resolve) => {
    releaseDelayedWatchdog = resolve;
  });
  let releasePostWatchdogRecovery!: () => void;
  const postWatchdogRecoveryGate = new Promise<void>((resolve) => {
    releasePostWatchdogRecovery = resolve;
  });
  let authoritativeJobRequests = 0;
  let fulfilledAuthoritativeJobRequests = 0;
  let initialJobStateVersion: bigint | undefined;
  const authoritativeSnapshots: GetSearchJobResponse[] = [];
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/v1/search/jobs/get",
    async (route) => {
      authoritativeJobRequests += 1;
      const requestOrdinal = authoritativeJobRequests;
      if (requestOrdinal > 6) {
        await route.abort("blockedbyclient");
        return;
      }
      if (requestOrdinal === 1) await staleRecoveryGate;
      if (requestOrdinal === 2) await freshRecoveryGate;
      if (requestOrdinal === 4) await postWatchdogRecoveryGate;
      const upstream = await route.fetch();
      if (
        upstream.status() !== 200
        || upstream.headers()["content-type"] !== "application/x-protobuf"
      ) {
        throw new Error(`authoritative recovery GET ${requestOrdinal} was not protobuf success`);
      }
      const response = GetSearchJobResponse.decode(await upstream.body());
      if (response.searchJob === undefined) {
        throw new Error(`authoritative recovery GET ${requestOrdinal} returned no search job`);
      }
      if (requestOrdinal === 1) {
        if (initialJobStateVersion === undefined || initialJobStateVersion === 0n) {
          throw new Error("the created search job did not establish a positive state version");
        }
        response.searchJob.stateVersion = 0n;
      }
      authoritativeSnapshots.push(response);
      if (requestOrdinal === 3) await delayedWatchdogGate;
      await route.fulfill({
        response: upstream,
        body: Buffer.from(GetSearchJobResponse.encode(response).finish()),
      });
      fulfilledAuthoritativeJobRequests += 1;
    },
  );
  const runSearch = await openSearchWorkspace(page);
  const { createResponsePromise, resultsResponsePromise } = waitForSearchResponses(page);

  try {
    await runSearch.click();
    const createResponse = await createResponsePromise;
    assertProtobufResponse(createResponse);
    const createdJob = CreateSearchJobResponse.decode(await createResponse.body()).searchJob;
    if (createdJob === undefined || !createdJob.searchJobId.trim()) {
      throw new Error("CreateSearchJobResponse.search_job is empty");
    }
    const browserSearchJobID = createdJob.searchJobId;
    initialJobStateVersion = createdJob.stateVersion;
    expect(initialJobStateVersion, "created search job state version").toBeGreaterThan(0n);
    await expiration.waitForCheckpoint(browserSearchJobID);
    await expect(page.getByTestId("backend-preview-status")).toHaveAttribute(
      "data-status",
      "live",
      { timeout },
    );
    await expect(
      page.getByRole("table", { name: "Live preview search statistics" }),
    ).toContainText(recoveryInitialText!, { timeout });

    await sendBrowserRecoveryControl("progress");
    await expect.poll(() => expiration.withheldFrameCount(), { timeout }).toBe(1);
    await sendBrowserRecoveryControl("progress");
    await expect.poll(() => expiration.withheldFrameCount(), { timeout }).toBe(2);
    await expiration.disconnect();
    await expiration.waitForResynchronizations(1);

    await expect(page.getByTestId("backend-preview-status")).toHaveAttribute(
      "data-status",
      "resyncing",
      { timeout },
    );
    await expect(
      page.getByRole("table", { name: "Live preview search statistics" }),
    ).toHaveCount(0);
    await expect(page.getByText(recoveryInitialText!, { exact: true })).toHaveCount(0);
    expect(authoritativeJobRequests, "authoritative GETs before recovery release").toBe(1);

    await sendBrowserRecoveryControl("progress");
    await expect.poll(() => expiration.heldRecoveryFrameCount(), { timeout }).toBe(1);
    expect(authoritativeJobRequests, "authoritative GET while target update is queued").toBe(1);
    await expect(page.getByTestId("backend-preview-status")).toHaveAttribute(
      "data-status",
      "resyncing",
      { timeout },
    );
    await expect(
      page.getByRole("table", { name: "Live preview search statistics" }),
    ).toHaveCount(0);
    await expect(page.getByLabel("Job metrics")).toContainText("0 rows", { timeout });
    const statusCountBeforeStaleRecovery = (await snapshotPreviewStatuses(page)).length;

    releaseStaleRecovery();
    await expect.poll(() => authoritativeSnapshots.length, { timeout }).toBe(1);
    const staleJob = authoritativeSnapshots[0].searchJob;
    expect(staleJob?.stateVersion, "deliberately stale authoritative state version").toBe(0n);
    expect(staleJob?.progress?.scannedRows, "stale recovery scanned rows").toBe(3n);
    await expiration.waitForResynchronizations(2);
    await expect.poll(() => authoritativeJobRequests, { timeout }).toBe(2);
    expect(expiration.connectionCount(), "reconnect after stale authoritative snapshot").toBe(3);
    await expect(page.getByTestId("backend-preview-status")).toHaveAttribute(
      "data-status",
      "resyncing",
      { timeout },
    );
    await expect(
      page.getByRole("table", { name: "Live preview search statistics" }),
    ).toHaveCount(0);
    await expect(page.getByLabel("Job metrics")).toContainText("0 rows", { timeout });
    expect(
      (await snapshotPreviewStatuses(page)).slice(statusCountBeforeStaleRecovery),
      "preview statuses while the stale snapshot is rejected",
    ).not.toContain("waiting");

    await sendBrowserRecoveryControl("progress");
    await expect.poll(() => expiration.postRecoveryFrameCount(), { timeout }).toBe(1);
    await expect(page.getByLabel("Job metrics")).toContainText("0 rows", { timeout });
    expect(
      (await snapshotPreviewStatuses(page)).slice(statusCountBeforeStaleRecovery),
      "preview statuses while fresh authoritative recovery is blocked",
    ).not.toContain("waiting");

    releaseFreshRecovery();
    await expect.poll(() => authoritativeSnapshots.length, { timeout }).toBe(2);
    const recoveredJob = authoritativeSnapshots[1].searchJob;
    expect(recoveredJob?.searchJobId, "authoritative recovery job ID").toBe(browserSearchJobID);
    expect(recoveredJob?.stateVersion, "fresh authoritative state version")
      .toBeGreaterThan(createdJob.stateVersion);
    expect(recoveredJob?.progress?.scannedRows, "authoritative recovered scanned rows").toBe(4n);
    await expect(page.getByTestId("backend-preview-status")).toHaveAttribute(
      "data-status",
      "waiting",
      { timeout },
    );
    await expect(page.getByLabel("Job metrics")).toContainText("4 rows", { timeout });

    await expect.poll(() => authoritativeSnapshots.length, { timeout }).toBe(3);
    const delayedWatchdogJob = authoritativeSnapshots[2].searchJob;
    expect(delayedWatchdogJob?.progress?.scannedRows, "delayed watchdog scanned rows").toBe(4n);
    await sendBrowserRecoveryControl("progress");
    await expect.poll(() => expiration.postRecoveryFrameCount(), { timeout }).toBe(2);
    await expect(page.getByLabel("Job metrics")).toContainText("5 rows", { timeout });
    releaseDelayedWatchdog();
    await expect.poll(() => authoritativeJobRequests, { timeout }).toBe(4);
    await expect(page.getByLabel("Job metrics")).toContainText("5 rows", { timeout });
    releasePostWatchdogRecovery();
    await expect.poll(() => authoritativeSnapshots.length, { timeout }).toBe(4);
    expect(
      authoritativeSnapshots[3].searchJob?.progress?.scannedRows,
      "post-watchdog recovery scanned rows",
    ).toBe(5n);
    await expect.poll(() => fulfilledAuthoritativeJobRequests, { timeout }).toBeGreaterThanOrEqual(4);

    await sendBrowserRecoveryControl("append");
    await expect(page.getByTestId("backend-preview-status")).toHaveAttribute(
      "data-status",
      "live",
      { timeout },
    );
    await expect(
      page.getByRole("table", { name: "Live preview search statistics" }),
    ).toContainText(expectedText, { timeout });
    await sendBrowserRecoveryControl("complete");

    const resultsResponse = await resultsResponsePromise;
    assertProtobufResponse(resultsResponse);
    expect(decodeSearchResultsJobID(await resultsResponse.body())).toBe(browserSearchJobID);
  } finally {
    releaseStaleRecovery();
    releaseFreshRecovery();
    releaseDelayedWatchdog();
    releasePostWatchdogRecovery();
    expiration.dispose();
  }

  const jobStrip = page.getByTestId("job-strip");
  await expect(jobStrip).toHaveAttribute("aria-busy", "false", { timeout });
  await expect(jobStrip).toContainText("Completed", { timeout });
  await expect(jobStrip).toContainText("2 rows", { timeout });
  const finalTable = page.getByRole("table", { name: "Backend search statistics" });
  await expect(finalTable.locator("tbody tr")).toHaveCount(expectedRows, { timeout });
  await expect(finalTable).toContainText(expectedText, { timeout });
  await expect(
    page.getByRole("table", { name: "Live preview search statistics" }),
  ).toHaveCount(0);

  const previewStatuses = await collectPreviewStatuses(page);
  expect(previewStatuses, "UI preview status transitions").toContain("live");
  expect(previewStatuses, "UI preview status transitions").toContain("resyncing");
  expect(previewStatuses, "UI preview status transitions").toContain("waiting");
  expect(expiration.connectionCount(), "search WebSocket connections").toBe(3);
  expect(expiration.heldRecoveryFrameCount(), "target frames ignored during held recovery").toBe(1);
  expect(expiration.postRecoveryFrameCount(), "post-recovery target frames").toBeGreaterThan(0);
  expiration.assertHealthy();
  expect(safety.createRequests(), "browser search create requests").toBe(1);
  expect(authoritativeJobRequests, "bounded authoritative job GETs").toBeLessThanOrEqual(5);
  assertBrowserSafety(safety);
});

function requiredEnvironment(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function optionalLoopbackURL(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  if (!trimmed) return undefined;
  const parsed = new URL(trimmed);
  if (
    parsed.protocol !== "http:"
    || (parsed.hostname !== "127.0.0.1" && parsed.hostname !== "localhost")
    || parsed.username
    || parsed.password
    || parsed.pathname !== "/"
    || parsed.search
    || parsed.hash
  ) {
    throw new Error("the browser recovery control URL must contain only a loopback HTTP origin");
  }
  return parsed.origin;
}

function parsePositiveInteger(value: string): number {
  if (!/^[1-9][0-9]*$/.test(value)) throw new Error(`invalid positive integer ${JSON.stringify(value)}`);
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) throw new Error(`integer exceeds the safe range: ${value}`);
  return parsed;
}

function validatedOrigin(value: string): string {
  const parsed = new URL(value);
  if (parsed.protocol !== "http:") throw new Error("the browser vertical test requires an HTTP loopback URL");
  if (parsed.hostname !== "127.0.0.1" && parsed.hostname !== "localhost") {
    throw new Error("the browser vertical test only connects to a loopback server");
  }
  if (parsed.username || parsed.password || parsed.pathname !== "/" || parsed.search || parsed.hash) {
    throw new Error("the browser vertical base URL must contain only a loopback origin");
  }
  return parsed.origin;
}

function httpOriginForWebSocket(socketURL: URL): string {
  if (socketURL.protocol !== "ws:" && socketURL.protocol !== "wss:") {
    throw new Error(`unexpected WebSocket URL protocol: ${socketURL.protocol}`);
  }
  const httpURL = new URL(socketURL);
  httpURL.protocol = socketURL.protocol === "wss:" ? "https:" : "http:";
  return httpURL.origin;
}

function matchesAPIResponse(response: Response, expectedOrigin: string, pathname: string): boolean {
  const responseURL = new URL(response.url());
  return responseURL.origin === expectedOrigin
    && responseURL.pathname === pathname
    && response.request().method() === "POST";
}

function assertProtobufResponse(response: Response): void {
  expect(response.status(), `${response.url()} status`).toBe(200);
  expect(response.headers()["content-type"], `${response.url()} Content-Type`).toBe("application/x-protobuf");
}

interface BrowserSafetyObservation {
  pageErrors: BoundedRecorder;
  failedAPIRequests: BoundedRecorder;
  externalRequests: BoundedRecorder;
  externalWebSockets: BoundedRecorder;
  createRequests(): number;
}

function observeBrowserSafety(page: Page): BrowserSafetyObservation {
  const pageErrors = boundedRecorder();
  const failedAPIRequests = boundedRecorder();
  const externalRequests = boundedRecorder();
  const externalWebSockets = boundedRecorder();
  let createRequests = 0;
  page.on("pageerror", (error) => pageErrors.add(error.message));
  page.on("requestfailed", (request) => {
    const requestURL = new URL(request.url());
    if (requestURL.origin === origin && requestURL.pathname.startsWith("/api/")) {
      failedAPIRequests.add(`${request.method()} ${requestURL.pathname}: ${request.failure()?.errorText ?? "failed"}`);
    }
  });
  page.on("request", (request) => {
    const requestURL = new URL(request.url());
    if (
      requestURL.origin === origin
      && requestURL.pathname === "/api/v1/search/jobs/create"
      && request.method() === "POST"
    ) {
      createRequests += 1;
    }
    if ((requestURL.protocol === "http:" || requestURL.protocol === "https:") && requestURL.origin !== origin) {
      externalRequests.add(`${request.method()} ${requestURL.origin}${requestURL.pathname}`);
    }
  });
  page.on("websocket", (socket) => {
    const socketURL = new URL(socket.url());
    if (httpOriginForWebSocket(socketURL) !== origin) {
      externalWebSockets.add(socket.url());
    }
  });
  return {
    pageErrors,
    failedAPIRequests,
    externalRequests,
    externalWebSockets,
    createRequests: () => createRequests,
  };
}

function assertBrowserSafety(observation: BrowserSafetyObservation): void {
  expect(observation.pageErrors.snapshot(), "uncaught browser errors").toEqual([]);
  expect(observation.failedAPIRequests.snapshot(), "failed same-origin API requests").toEqual([]);
  expect(observation.externalRequests.snapshot(), "external browser resources").toEqual([]);
  expect(observation.externalWebSockets.snapshot(), "external browser WebSockets").toEqual([]);
}

async function openSearchWorkspace(page: Page): Promise<Locator> {
  const launchURL = new URL("/search/", origin);
  launchURL.search = new URLSearchParams({
    q: searchSPL,
    earliest,
    latest,
    timezone: "UTC",
    run: "0",
  }).toString();
  await page.goto(launchURL.href, { waitUntil: "domcontentloaded", timeout });
  await expect(page.getByTestId("search-workspace")).toBeVisible({ timeout });
  await expect(page.getByText("Backend data", { exact: true })).toBeVisible({ timeout });
  await expect(page.getByTestId("search-input")).toHaveValue(searchSPL);
  const runSearch = page.getByTestId("run-search");
  await expect(runSearch).toBeEnabled({ timeout });
  await installPreviewStatusRecorder(page);
  return runSearch;
}

interface SearchResponseWaiters {
  createResponsePromise: Promise<Response>;
  resultsResponsePromise: Promise<Response>;
}

function waitForSearchResponses(page: Page): SearchResponseWaiters {
  const createResponsePromise = page.waitForResponse(
    (response) => matchesAPIResponse(response, origin, "/api/v1/search/jobs/create"),
    { timeout },
  );
  const resultsResponsePromise = page.waitForResponse(
    (response) => matchesAPIResponse(response, origin, "/api/v1/search/jobs/results"),
    { timeout },
  );
  // Page teardown can reject either waiter before the normal await is reached.
  // Mark both handled immediately while retaining the original promises.
  void createResponsePromise.catch(() => undefined);
  void resultsResponsePromise.catch(() => undefined);
  return { createResponsePromise, resultsResponsePromise };
}

interface BoundedRecorder {
  add(value: string): void;
  snapshot(): string[];
}

function boundedRecorder(limit = 32): BoundedRecorder {
  const values: string[] = [];
  let overflow = 0;
  return {
    add(value) {
      if (values.length < limit) values.push(value);
      else overflow += 1;
    },
    snapshot() {
      return overflow === 0 ? [...values] : [...values, `... ${overflow} additional entries`];
    },
  };
}

interface PreviewRecorderState {
  observer: MutationObserver;
  statuses: string[];
}

type PreviewRecorderWindow = Window & {
  openSplunkE2EPreviewRecorder?: PreviewRecorderState;
};

async function installPreviewStatusRecorder(page: Page): Promise<void> {
  await page.evaluate(() => {
    const recorderWindow = window as PreviewRecorderWindow;
    recorderWindow.openSplunkE2EPreviewRecorder?.observer.disconnect();
    const statuses: string[] = [];
    const record = (): void => {
      const status = document.querySelector<HTMLElement>('[data-testid="backend-preview-status"]')
        ?.dataset.status;
      if (status && statuses.at(-1) !== status && statuses.length < 32) statuses.push(status);
    };
    const observer = new MutationObserver(record);
    observer.observe(document.body, {
      attributes: true,
      attributeFilter: ["data-status"],
      childList: true,
      subtree: true,
    });
    recorderWindow.openSplunkE2EPreviewRecorder = { observer, statuses };
    record();
  });
}

async function collectPreviewStatuses(page: Page): Promise<string[]> {
  return page.evaluate(() => {
    const recorderWindow = window as PreviewRecorderWindow;
    const recorder = recorderWindow.openSplunkE2EPreviewRecorder;
    recorder?.observer.disconnect();
    return recorder === undefined ? [] : [...recorder.statuses];
  });
}

async function snapshotPreviewStatuses(page: Page): Promise<string[]> {
  return page.evaluate(() => {
    const recorder = (window as PreviewRecorderWindow).openSplunkE2EPreviewRecorder;
    return recorder === undefined ? [] : [...recorder.statuses];
  });
}

interface ObservedSubscription {
  subscriptionID: string;
  searchJobID: string;
  afterSequence: bigint;
  includePreviews: boolean;
  previewRowLimit: number | undefined;
}

interface ObservedProgress {
  sequence: bigint;
  subscriptionID: string;
  searchJobID: string;
}

type FrameEvent = { payload: string | Buffer };

interface SocketListeners {
  sent(event: FrameEvent): void;
  received(event: FrameEvent): void;
  error(error: string): void;
  close(): void;
}

interface SearchProtocolObservation {
  waitForJob(searchJobID: string): Promise<void>;
  dispose(): void;
}

interface RetainedReplayObservation {
  waitForCheckpoint(searchJobID: string): Promise<void>;
  disconnect(): Promise<void>;
  waitForTerminalReplay(): Promise<void>;
  dispose(): void;
}

interface SequenceExpirationObservation {
  waitForCheckpoint(searchJobID: string): Promise<void>;
  withheldFrameCount(): number;
  disconnect(): Promise<void>;
  waitForResynchronizations(count: number): Promise<void>;
  heldRecoveryFrameCount(): number;
  connectionCount(): number;
  postRecoveryFrameCount(): number;
  assertHealthy(): void;
  dispose(): void;
}

interface ObservedSubscribeCommand {
  requestID: string;
  subscriptions: ObservedSubscription[];
}

async function sendBrowserRecoveryControl(
  action: "progress" | "append" | "complete",
): Promise<void> {
  if (recoveryControlURL === undefined || !recoveryControlToken) {
    throw new Error("the browser recovery control endpoint is unavailable");
  }
  const response = await fetch(new URL(`/${action}`, recoveryControlURL), {
    method: "POST",
    headers: {
      [browserRecoveryControlTokenHeader]: recoveryControlToken,
    },
  });
  const body = new Uint8Array(await response.arrayBuffer());
  if (body.byteLength > 1_024) {
    throw new Error(`browser recovery control ${action} response exceeded 1024 bytes`);
  }
  if (!response.ok) {
    throw new Error(
      `browser recovery control ${action} failed with status ${response.status}`,
    );
  }
  if (response.headers.get("content-type") !== "application/json") {
    throw new Error(`browser recovery control ${action} returned a non-JSON response`);
  }
}

const browserRecoveryControlTokenHeader = "X-Open-Splunk-Test-Token";

async function interceptSequenceExpiration(
  page: Page,
  expectedOrigin: string,
  timeoutMilliseconds: number,
): Promise<SequenceExpirationObservation> {
  let expectedJobID: string | undefined;
  let routedJobID: string | undefined;
  let subscriptionID: string | undefined;
  let checkpoint: bigint | undefined;
  let initialSubscription: ObservedSubscription | undefined;
  let latestTargetSequence: bigint | undefined;
  let firstServer: WebSocketRoute | undefined;
  const reconnectRequestIDs = new Map<number, string>();
  const acknowledgedConnections = new Set<number>();
  const resynchronizedConnections = new Set<number>();
  const lastConnectionSequences = new Map<number, bigint>();
  let matchingConnectionCount = 0;
  let clientFrameCount = 0;
  let serverFrameCount = 0;
  let heldRecoveryTargetFrames = 0;
  let recoveredTargetFrames = 0;
  let failure: Error | undefined;
  let checkpointSettled = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const withheldFrames = new Map<bigint, Buffer>();
  let resolveCheckpoint!: () => void;
  let rejectCheckpoint!: (reason: Error) => void;
  const checkpointReady = new Promise<void>((resolve, reject) => {
    resolveCheckpoint = resolve;
    rejectCheckpoint = reject;
  });
  void checkpointReady.catch(() => undefined);
  const resynchronizationWaiters = new Set<{
    count: number;
    resolve: () => void;
    reject: (reason: Error) => void;
  }>();

  const settleResynchronizationWaiters = (): void => {
    for (const waiter of resynchronizationWaiters) {
      if (resynchronizedConnections.size < waiter.count) continue;
      resynchronizationWaiters.delete(waiter);
      waiter.resolve();
    }
  };

  const fail = (error: unknown): void => {
    const normalized = normalizeError(error);
    failure ??= normalized;
    if (timer !== undefined) {
      clearTimeout(timer);
      timer = undefined;
    }
    if (!checkpointSettled) {
      checkpointSettled = true;
      rejectCheckpoint(normalized);
    }
    for (const waiter of resynchronizationWaiters) {
      waiter.reject(normalized);
    }
    resynchronizationWaiters.clear();
  };
  const routeMatchesSearchSocket = (url: URL): boolean =>
    httpOriginForWebSocket(url) === expectedOrigin && url.pathname === "/api/v1/search/ws";

  await page.routeWebSocket(routeMatchesSearchSocket, (client) => {
    matchingConnectionCount += 1;
    const connectionOrdinal = matchingConnectionCount;
    const server = client.connectToServer();
    if (connectionOrdinal === 1) firstServer = server;
    if (connectionOrdinal > 3) {
      fail(new Error(`search WebSocket opened ${connectionOrdinal} matching connections`));
      return;
    }

    client.onMessage((message) => {
      try {
        clientFrameCount += 1;
        if (clientFrameCount > 32) {
          throw new Error("search WebSocket sent more than 32 routed command frames");
        }
        if (typeof message === "string") throw new Error("search WebSocket sent a text frame");
        const subscribe = decodeSearchSubscribeCommand(message);
        if (subscribe !== undefined && subscribe.subscriptions.length !== 1) {
          throw new Error("search WebSocket sent a non-singleton subscribe command");
        }
        const subscription = subscribe?.subscriptions[0];
        if (subscription !== undefined) {
          if (connectionOrdinal === 1) {
            if (initialSubscription !== undefined) {
              throw new Error("initial WebSocket sent more than one subscribe command");
            }
            if (subscription.afterSequence !== 0n) {
              throw new Error(
                `initial subscription started after sequence ${subscription.afterSequence.toString()}`,
              );
            }
            initialSubscription = subscription;
            subscriptionID = subscription.subscriptionID;
            routedJobID = subscription.searchJobID;
          } else {
            if (
              checkpoint === undefined
              || subscriptionID === undefined
              || routedJobID === undefined
              || initialSubscription === undefined
              || subscribe === undefined
            ) {
              throw new Error("expiration reconnect preceded its browser checkpoint");
            }
            if (reconnectRequestIDs.has(connectionOrdinal)) {
              throw new Error(
                `expiration connection ${connectionOrdinal} sent more than one subscribe command`,
              );
            }
            if (
              subscription.subscriptionID !== subscriptionID
              || subscription.searchJobID !== routedJobID
              || subscription.afterSequence !== checkpoint
              || subscription.includePreviews !== initialSubscription.includePreviews
              || subscription.previewRowLimit !== initialSubscription.previewRowLimit
            ) {
              throw new Error(
                `expiration reconnect did not retain the original subscription after ${checkpoint.toString()}`,
              );
            }
            reconnectRequestIDs.set(connectionOrdinal, subscribe.requestID);
          }
        }
        server.send(message);
      } catch (error) {
        fail(error);
      }
    });

    server.onMessage((message) => {
      try {
        serverFrameCount += 1;
        if (serverFrameCount > 128) {
          throw new Error("search WebSocket received more than 128 routed event frames");
        }
        if (typeof message === "string") throw new Error("search WebSocket received a text frame");
        const frame = Buffer.from(message);
        const event = SearchWebSocketEvent.decode(frame);
        const target = event.target?.target;
        const matchesSubscription = event.sequence > 0n
          && subscriptionID !== undefined
          && routedJobID !== undefined
          && event.subscriptionId === subscriptionID
          && target?.$case === "searchJobId"
          && target.value === routedJobID;

        if (connectionOrdinal === 1) {
          if (matchesSubscription && checkpoint !== undefined) {
            const previous = Array.from(withheldFrames.keys()).at(-1) ?? checkpoint;
            if (event.sequence !== previous + 1n) {
              throw new Error(
                `withheld expiration sequence = ${event.sequence.toString()}, want ${(previous + 1n).toString()}`,
              );
            }
            const expectedScannedRows = BigInt(withheldFrames.size + 1);
            if (
              event.payload?.$case !== "searchProgress"
              || event.payload.value.scannedRows !== expectedScannedRows
            ) {
              throw new Error(
                `withheld expiration frame ${event.sequence.toString()} was not progress ${expectedScannedRows.toString()}`,
              );
            }
            withheldFrames.set(event.sequence, frame);
            latestTargetSequence = event.sequence;
            return;
          }
          client.send(message);
          if (
            matchesSubscription
            && event.payload?.$case === "resultPreview"
            && event.payload.value.rows.length > 0
          ) {
            checkpoint = event.sequence;
            latestTargetSequence = event.sequence;
            if (!checkpointSettled) {
              checkpointSettled = true;
              resolveCheckpoint();
            }
          }
          return;
        }

        const reconnectRequestID = reconnectRequestIDs.get(connectionOrdinal);
        const expectedLatestSequence = latestTargetSequence;
        if (event.payload?.$case === "subscriptionAcknowledged") {
          const acknowledgment = event.payload.value;
          if (
            acknowledgedConnections.has(connectionOrdinal)
            || reconnectRequestID === undefined
            || subscriptionID === undefined
            || routedJobID === undefined
            || expectedLatestSequence === undefined
            || event.sequence !== 0n
            || event.subscriptionId !== subscriptionID
            || acknowledgment.requestId !== reconnectRequestID
            || acknowledgment.subscriptionId !== subscriptionID
            || acknowledgment.target?.target?.$case !== "searchJobId"
            || acknowledgment.target.target.value !== routedJobID
            || acknowledgment.replayWillFollow
            || acknowledgment.earliestAvailableSequence !== expectedLatestSequence
            || acknowledgment.latestSequence !== expectedLatestSequence
          ) {
            throw new Error(
              `expiration connection ${connectionOrdinal} acknowledgment was invalid`,
            );
          }
          acknowledgedConnections.add(connectionOrdinal);
        } else if (event.payload?.$case === "resynchronizationRequired") {
          const required = event.payload.value;
          if (
            !acknowledgedConnections.has(connectionOrdinal)
            || resynchronizedConnections.has(connectionOrdinal)
            || subscriptionID === undefined
            || routedJobID === undefined
            || expectedLatestSequence === undefined
            || event.sequence !== 0n
            || event.subscriptionId !== subscriptionID
            || event.target?.target?.$case !== "searchJobId"
            || event.target.target.value !== routedJobID
            || required.subscriptionId !== subscriptionID
            || required.target?.target?.$case !== "searchJobId"
            || required.target.target.value !== routedJobID
            || required.reason
              !== ResynchronizationReason.RESYNCHRONIZATION_REASON_SEQUENCE_EXPIRED
            || required.earliestAvailableSequence !== expectedLatestSequence
            || required.latestSequence !== expectedLatestSequence
            || required.recoveryPath !== "/api/v1/search/jobs/get"
          ) {
            throw new Error(
              `expiration connection ${connectionOrdinal} resynchronization frame was invalid`,
            );
          }
          resynchronizedConnections.add(connectionOrdinal);
          lastConnectionSequences.set(connectionOrdinal, expectedLatestSequence);
        } else if (matchesSubscription) {
          const previousSequence = lastConnectionSequences.get(connectionOrdinal);
          if (
            !resynchronizedConnections.has(connectionOrdinal)
            || previousSequence === undefined
          ) {
            throw new Error(
              `expiration connection ${connectionOrdinal} delivered a target frame before resynchronization`,
            );
          }
          if (
            event.sequence !== previousSequence + 1n
            || latestTargetSequence === undefined
            || event.sequence !== latestTargetSequence + 1n
          ) {
            throw new Error(
              `expiration connection ${connectionOrdinal} sequence = ${event.sequence.toString()}, want ${(previousSequence + 1n).toString()}`,
            );
          }
          lastConnectionSequences.set(connectionOrdinal, event.sequence);
          latestTargetSequence = event.sequence;
          if (connectionOrdinal === 2) {
            if (
              event.payload?.$case !== "searchProgress"
              || event.payload.value.scannedRows !== 3n
            ) {
              throw new Error("the held-recovery target frame was not progress for three rows");
            }
            heldRecoveryTargetFrames += 1;
          }
          if (connectionOrdinal === 3) recoveredTargetFrames += 1;
        }
        client.send(message);
        if (event.payload?.$case === "resynchronizationRequired") {
          if (
            expectedJobID !== undefined
            && routedJobID !== expectedJobID
          ) {
            throw new Error(
              `routed WebSocket job ${routedJobID} did not match created job ${expectedJobID}`,
            );
          }
          settleResynchronizationWaiters();
        }
      } catch (error) {
        fail(error);
      }
    });
  });

  timer = setTimeout(
    () => fail(new Error("timed out waiting for real WebSocket sequence expiration")),
    timeoutMilliseconds,
  );
  return {
    waitForCheckpoint(searchJobID) {
      if (expectedJobID !== undefined) throw new Error("browser search job was already selected");
      expectedJobID = searchJobID;
      if (routedJobID !== undefined && routedJobID !== expectedJobID) {
        fail(
          new Error(
            `routed WebSocket job ${routedJobID} did not match created job ${expectedJobID}`,
          ),
        );
      }
      return checkpointReady;
    },
    withheldFrameCount() {
      return withheldFrames.size;
    },
    async disconnect() {
      if (
        firstServer === undefined
        || checkpoint === undefined
        || withheldFrames.size !== 2
        || Array.from(withheldFrames.keys())[0] !== checkpoint + 1n
        || Array.from(withheldFrames.keys())[1] !== checkpoint + 2n
      ) {
        throw new Error("sequence-expiration disruption did not capture exactly K+1 and K+2");
      }
      await firstServer.close({
        code: 4000,
        reason: "deterministic sequence-expiration checkpoint",
      });
    },
    waitForResynchronizations(count) {
      if (!Number.isSafeInteger(count) || count < 1 || count > 2) {
        return Promise.reject(new RangeError("resynchronization count must be between 1 and 2"));
      }
      if (failure !== undefined) return Promise.reject(failure);
      if (resynchronizedConnections.size >= count) return Promise.resolve();
      return new Promise<void>((resolve, reject) => {
        resynchronizationWaiters.add({ count, resolve, reject });
      });
    },
    heldRecoveryFrameCount() {
      return heldRecoveryTargetFrames;
    },
    connectionCount() {
      return matchingConnectionCount;
    },
    postRecoveryFrameCount() {
      return recoveredTargetFrames;
    },
    assertHealthy() {
      if (failure !== undefined) throw failure;
      if (
        reconnectRequestIDs.size !== 2
        || acknowledgedConnections.size !== 2
        || resynchronizedConnections.size !== 2
        || matchingConnectionCount !== 3
        || withheldFrames.size !== 2
        || heldRecoveryTargetFrames !== 1
        || recoveredTargetFrames < 1
      ) {
        throw new Error("sequence-expiration observation did not complete its exact contract");
      }
    },
    dispose() {
      if (timer !== undefined) clearTimeout(timer);
      if (!checkpointSettled) {
        checkpointSettled = true;
        resolveCheckpoint();
      }
      for (const waiter of resynchronizationWaiters) {
        waiter.resolve();
      }
      resynchronizationWaiters.clear();
    },
  };
}

async function interceptOneRetainedReplay(
  page: Page,
  expectedOrigin: string,
  timeoutMilliseconds: number,
): Promise<RetainedReplayObservation> {
  let expectedJobID: string | undefined;
  let routedJobID: string | undefined;
  let subscriptionID: string | undefined;
  let checkpoint: bigint | undefined;
  let withheldTerminalSequence: bigint | undefined;
  let reconnectRequestID: string | undefined;
  let reconnectAcknowledged = false;
  let initialSubscription: ObservedSubscription | undefined;
  const withheldFrames = new Map<bigint, Buffer>();
  let replayVerified = false;
  let terminalReplayed = false;
  let lastReplaySequence: bigint | undefined;
  let firstServer: WebSocketRoute | undefined;
  let connectionCount = 0;
  let clientFrameCount = 0;
  let serverFrameCount = 0;
  let checkpointSettled = false;
  let completionSettled = false;
  let settled = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let resolveCheckpoint!: () => void;
  let rejectCheckpoint!: (reason: Error) => void;
  let resolveCompletion!: () => void;
  let rejectCompletion!: (reason: Error) => void;
  const checkpointReady = new Promise<void>((resolve, reject) => {
    resolveCheckpoint = resolve;
    rejectCheckpoint = reject;
  });
  const completion = new Promise<void>((resolve, reject) => {
    resolveCompletion = resolve;
    rejectCompletion = reject;
  });
  void checkpointReady.catch(() => undefined);
  void completion.catch(() => undefined);

  const finish = (error?: Error): void => {
    if (settled) return;
    settled = true;
    if (timer !== undefined) clearTimeout(timer);
    if (!checkpointSettled) {
      checkpointSettled = true;
      if (error) rejectCheckpoint(error);
      else resolveCheckpoint();
    }
    if (!completionSettled) {
      completionSettled = true;
      if (error) rejectCompletion(error);
      else resolveCompletion();
    }
  };

  const checkCompletion = (): void => {
    if (!replayVerified || !terminalReplayed || !expectedJobID || !routedJobID) return;
    if (routedJobID !== expectedJobID) {
      finish(new Error(`routed WebSocket job ${routedJobID} did not match created job ${expectedJobID}`));
      return;
    }
    finish();
  };

  const fail = (error: unknown): void => finish(normalizeError(error));
  const routeMatchesSearchSocket = (url: URL): boolean =>
    httpOriginForWebSocket(url) === expectedOrigin && url.pathname === "/api/v1/search/ws";

  await page.routeWebSocket(routeMatchesSearchSocket, (client) => {
    connectionCount += 1;
    const connectionOrdinal = connectionCount;
    const server = client.connectToServer();
    if (connectionOrdinal === 1) firstServer = server;
    if (connectionOrdinal > 2) {
      fail(new Error(`search WebSocket opened ${connectionOrdinal} connections before retained replay completed`));
      return;
    }

    client.onMessage((message) => {
      try {
        clientFrameCount += 1;
        if (clientFrameCount > 64) throw new Error("search WebSocket sent more than 64 routed command frames");
        if (typeof message === "string") throw new Error("search WebSocket sent a text frame");
        const subscribe = decodeSearchSubscribeCommand(message);
        if (subscribe !== undefined && subscribe.subscriptions.length !== 1) {
          throw new Error("search WebSocket sent more than one subscription in a reconnect command");
        }
        const subscription = subscribe?.subscriptions[0];
        if (subscription !== undefined) {
          if (connectionOrdinal === 1) {
            if (initialSubscription !== undefined) {
              throw new Error("initial WebSocket sent more than one subscribe command");
            }
            if (subscription.afterSequence !== 0n) {
              throw new Error(`initial subscription started after sequence ${subscription.afterSequence.toString()}`);
            }
            initialSubscription = subscription;
            subscriptionID = subscription.subscriptionID;
            routedJobID = subscription.searchJobID;
          } else {
            if (
              checkpoint === undefined
              || subscriptionID === undefined
              || routedJobID === undefined
              || initialSubscription === undefined
              || subscribe === undefined
            ) {
              throw new Error("reconnect subscribed before the disruption checkpoint was established");
            }
            if (reconnectRequestID !== undefined) {
              throw new Error("reconnect WebSocket sent more than one subscribe command");
            }
            if (
              subscription.subscriptionID !== subscriptionID
              || subscription.searchJobID !== routedJobID
              || subscription.afterSequence !== checkpoint
              || subscription.includePreviews !== initialSubscription.includePreviews
              || subscription.previewRowLimit !== initialSubscription.previewRowLimit
            ) {
              throw new Error(
                `reconnect subscription = ${JSON.stringify({
                  subscriptionID: subscription.subscriptionID,
                  searchJobID: subscription.searchJobID,
                  afterSequence: subscription.afterSequence.toString(),
                  includePreviews: subscription.includePreviews,
                  previewRowLimit: subscription.previewRowLimit,
                })}; expected the original subscription after ${checkpoint.toString()}`,
              );
            }
            reconnectRequestID = subscribe.requestID;
            lastReplaySequence = checkpoint;
          }
        }
        server.send(message);
      } catch (error) {
        fail(error);
      }
    });

    server.onMessage((message) => {
      try {
        serverFrameCount += 1;
        if (serverFrameCount > 256) throw new Error("search WebSocket received more than 256 routed event frames");
        if (typeof message === "string") throw new Error("search WebSocket received a text frame");
        const frame = Buffer.from(message);
        const event = SearchWebSocketEvent.decode(frame);
        const target = event.target?.target;
        const matchesSubscription = event.sequence > 0n
          && subscriptionID !== undefined
          && routedJobID !== undefined
          && event.subscriptionId === subscriptionID
          && target?.$case === "searchJobId"
          && target.value === routedJobID;

        if (connectionOrdinal === 1) {
          if (matchesSubscription && checkpoint !== undefined) {
            const prior = withheldTerminalSequence
              ?? Array.from(withheldFrames.keys()).at(-1)
              ?? checkpoint;
            if (event.sequence !== prior + 1n) {
              throw new Error(
                `withheld sequence = ${event.sequence.toString()}, want ${(prior + 1n).toString()}`,
              );
            }
            withheldFrames.set(event.sequence, frame);
            if (event.payload?.$case === "searchTerminal") {
              withheldTerminalSequence = event.sequence;
              if (!checkpointSettled) {
                checkpointSettled = true;
                resolveCheckpoint();
              }
            }
            return;
          }
          client.send(message);
          if (
            matchesSubscription
            && event.payload?.$case === "resultPreview"
            && event.payload.value.rows.length > 0
          ) {
            checkpoint = event.sequence;
          }
          return;
        }

        if (event.payload?.$case === "resynchronizationRequired") {
          throw new Error(
            `retained replay unexpectedly required resynchronization: ${event.payload.value.reason}`,
          );
        }
        if (event.payload?.$case === "subscriptionAcknowledged") {
          const acknowledgment = event.payload.value;
          if (
            reconnectAcknowledged
            || reconnectRequestID === undefined
            || checkpoint === undefined
            || withheldTerminalSequence === undefined
            || acknowledgment.requestId !== reconnectRequestID
            || acknowledgment.subscriptionId !== subscriptionID
            || acknowledgment.target?.target?.$case !== "searchJobId"
            || acknowledgment.target.target.value !== routedJobID
            || !acknowledgment.replayWillFollow
            || acknowledgment.earliestAvailableSequence > checkpoint + 1n
            || acknowledgment.latestSequence !== withheldTerminalSequence
          ) {
            throw new Error("reconnect acknowledgment did not describe the withheld retained suffix");
          }
          reconnectAcknowledged = true;
        }
        if (matchesSubscription) {
          if (
            !reconnectAcknowledged
            || checkpoint === undefined
            || withheldTerminalSequence === undefined
            || withheldFrames.size === 0
          ) {
            throw new Error("reconnect delivered a target frame before the withheld replay was captured");
          }
          const previous = lastReplaySequence ?? checkpoint;
          if (event.sequence !== previous + 1n) {
            throw new Error(
              `replay sequence = ${event.sequence.toString()}, want ${(previous + 1n).toString()}`,
            );
          }
          const withheld = withheldFrames.get(event.sequence);
          if (withheld === undefined || !frame.equals(withheld)) {
            throw new Error(`retained replay frame ${event.sequence.toString()} was not byte-identical`);
          }
          lastReplaySequence = event.sequence;
          replayVerified = event.sequence === withheldTerminalSequence;
        }
        client.send(message);
        if (matchesSubscription && event.payload?.$case === "searchTerminal") {
          if (!replayVerified || event.sequence !== withheldTerminalSequence) {
            throw new Error("terminal arrived before the complete retained suffix was replayed");
          }
          terminalReplayed = true;
          checkCompletion();
        }
      } catch (error) {
        fail(error);
      }
    });
  });

  timer = setTimeout(
    () => finish(new Error("timed out waiting for a byte-identical retained WebSocket replay")),
    timeoutMilliseconds,
  );
  return {
    waitForCheckpoint(searchJobID) {
      if (expectedJobID !== undefined) throw new Error("browser search job was already selected");
      expectedJobID = searchJobID;
      if (routedJobID !== undefined && routedJobID !== expectedJobID) {
        fail(new Error(`routed WebSocket job ${routedJobID} did not match created job ${expectedJobID}`));
      }
      return checkpointReady;
    },
    async disconnect() {
      if (
        firstServer === undefined
        || checkpoint === undefined
        || withheldTerminalSequence === undefined
        || withheldFrames.size === 0
      ) {
        throw new Error("retained replay disruption was requested before its checkpoint was ready");
      }
      await firstServer.close({
        code: 4000,
        reason: "deterministic retained replay checkpoint",
      });
    },
    waitForTerminalReplay() {
      return completion;
    },
    dispose() {
      finish();
    },
  };
}

function observeSearchProtocol(
  page: Page,
  expectedOrigin: string,
  timeoutMilliseconds: number,
): SearchProtocolObservation {
  let expectedJobID: string | undefined;
  let settled = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let resolveCompletion!: () => void;
  let rejectCompletion!: (reason: Error) => void;
  const subscriptions: ObservedSubscription[] = [];
  const progressEvents: ObservedProgress[] = [];
  const socketListeners = new Map<WebSocket, SocketListeners>();
  const completion = new Promise<void>((resolve, reject) => {
    resolveCompletion = resolve;
    rejectCompletion = reject;
  });
  // A socket can fail before the create response selects its job. Mark the
  // rejection handled immediately; callers still observe it from completion.
  void completion.catch(() => undefined);

  const finish = (error?: Error): void => {
    if (settled) return;
    settled = true;
    if (timer !== undefined) clearTimeout(timer);
    page.off("websocket", observeSocket);
    for (const [socket, listeners] of socketListeners) {
      socket.off("framesent", listeners.sent);
      socket.off("framereceived", listeners.received);
      socket.off("socketerror", listeners.error);
      socket.off("close", listeners.close);
    }
    socketListeners.clear();
    if (error) rejectCompletion(error);
    else resolveCompletion();
  };

  const checkCompletion = (): void => {
    if (!expectedJobID) return;
    const subscription = subscriptions.find((candidate) => candidate.searchJobID === expectedJobID);
    if (!subscription) return;
    const progress = progressEvents.find((event) =>
      event.searchJobID === expectedJobID
      && event.subscriptionID === subscription.subscriptionID
      && event.sequence > 0n);
    if (progress) finish();
  };

  const observeSocket = (socket: WebSocket): void => {
    const socketURL = new URL(socket.url());
    if (httpOriginForWebSocket(socketURL) !== expectedOrigin || socketURL.pathname !== "/api/v1/search/ws") {
      return;
    }
    const listeners: SocketListeners = {
      sent: ({ payload }) => {
        try {
          if (typeof payload === "string") throw new Error("search WebSocket sent a text frame");
          subscriptions.push(...(decodeSearchSubscribeCommand(payload)?.subscriptions ?? []));
          if (subscriptions.length > 32) throw new Error("search WebSocket sent too many subscriptions");
          checkCompletion();
        } catch (error) {
          finish(normalizeError(error));
        }
      },
      received: ({ payload }) => {
        try {
          if (typeof payload === "string") throw new Error("search WebSocket received a text frame");
          const progress = decodeSearchProgress(payload);
          if (progress !== undefined) progressEvents.push(progress);
          if (progressEvents.length > 256) throw new Error("search WebSocket received too many progress events");
          checkCompletion();
        } catch (error) {
          finish(normalizeError(error));
        }
      },
      error: (error) => finish(new Error(`search WebSocket failed: ${error}`)),
      close: () => finish(new Error("search WebSocket closed before a sequenced search-progress event arrived")),
    };
    socketListeners.set(socket, listeners);
    socket.on("framesent", listeners.sent);
    socket.on("framereceived", listeners.received);
    socket.on("socketerror", listeners.error);
    socket.on("close", listeners.close);
  };

  timer = setTimeout(
    () => finish(new Error("timed out waiting for the browser's sequenced protobuf search-progress event")),
    timeoutMilliseconds,
  );
  page.on("websocket", observeSocket);
  return {
    waitForJob(searchJobID) {
      if (expectedJobID !== undefined) throw new Error("browser search job was already selected");
      expectedJobID = searchJobID;
      checkCompletion();
      return completion;
    },
    dispose() {
      finish();
    },
  };
}

function decodeCreateSearchJobID(payload: Uint8Array): string {
  const searchJobID = CreateSearchJobResponse.decode(payload).searchJob?.searchJobId.trim() ?? "";
  if (!searchJobID) throw new Error("CreateSearchJobResponse.search_job.search_job_id is empty");
  return searchJobID;
}

function decodeSearchResultsJobID(payload: Uint8Array): string {
  const searchJobID = GetSearchResultsResponse.decode(payload).searchJobId.trim();
  if (!searchJobID) throw new Error("GetSearchResultsResponse.search_job_id is empty");
  return searchJobID;
}

function decodeSearchSubscribeCommand(payload: Uint8Array): ObservedSubscribeCommand | undefined {
  const command = SearchWebSocketCommand.decode(payload);
  if (command.payload?.$case !== "subscribe") return undefined;
  if (!command.requestId) throw new Error("SearchWebSocketCommand.request_id is empty");
  return {
    requestID: command.requestId,
    subscriptions: command.payload.value.subscriptions.map((subscription) => {
      const target = subscription.target?.target;
      if (!subscription.subscriptionId) throw new Error("SearchSubscription.subscription_id is empty");
      if (target?.$case !== "searchJobId" || !target.value) {
        throw new Error("SearchSubscription.search_job_id is empty");
      }
      return {
        subscriptionID: subscription.subscriptionId,
        searchJobID: target.value,
        afterSequence: subscription.afterSequence,
        includePreviews: subscription.includePreviews,
        previewRowLimit: subscription.previewRowLimit,
      };
    }),
  };
}

function decodeSearchProgress(payload: Uint8Array): ObservedProgress | undefined {
  const event = SearchWebSocketEvent.decode(payload);
  if (event.payload?.$case !== "searchProgress") return undefined;
  const target = event.target?.target;
  if (!event.subscriptionId) throw new Error("SearchWebSocketEvent.subscription_id is empty");
  if (target?.$case !== "searchJobId" || !target.value) {
    throw new Error("search-progress event search_job_id is empty");
  }
  return {
    sequence: event.sequence,
    subscriptionID: event.subscriptionId,
    searchJobID: target.value,
  };
}

function normalizeError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
