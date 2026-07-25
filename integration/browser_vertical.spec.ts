import { expect, test, type Page, type Response, type WebSocket } from "@playwright/test";

import {
  CreateSearchJobResponse,
  GetSearchResultsResponse,
} from "../gen/ts/open_splunk/v1/search_api";
import {
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
const origin = validatedOrigin(baseURL);
const timeout = 45_000;

test.use({
  launchOptions: browserExecutable ? { executablePath: browserExecutable } : {},
  screenshot: "only-on-failure",
  trace: "retain-on-failure",
});

test("collector event is visible through the compiled backend UI", async ({ page }) => {
  test.setTimeout(60_000);
  const pageErrors = boundedRecorder();
  const failedAPIRequests = boundedRecorder();
  const externalRequests = boundedRecorder();
  const externalWebSockets = boundedRecorder();
  page.on("pageerror", (error) => pageErrors.add(error.message));
  page.on("requestfailed", (request) => {
    const requestURL = new URL(request.url());
    if (requestURL.origin === origin && requestURL.pathname.startsWith("/api/")) {
      failedAPIRequests.add(`${request.method()} ${requestURL.pathname}: ${request.failure()?.errorText ?? "failed"}`);
    }
  });
  page.on("request", (request) => {
    const requestURL = new URL(request.url());
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
  expect(pageErrors.snapshot(), "uncaught browser errors").toEqual([]);
  expect(failedAPIRequests.snapshot(), "failed same-origin API requests").toEqual([]);
  expect(externalRequests.snapshot(), "external browser resources").toEqual([]);
  expect(externalWebSockets.snapshot(), "external browser WebSockets").toEqual([]);
});

function requiredEnvironment(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
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

interface ObservedSubscription {
  subscriptionID: string;
  searchJobID: string;
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
          subscriptions.push(...decodeSearchSubscriptions(payload));
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

function decodeSearchSubscriptions(payload: Uint8Array): ObservedSubscription[] {
  const command = SearchWebSocketCommand.decode(payload);
  if (command.payload?.$case !== "subscribe") return [];
  return command.payload.value.subscriptions.map((subscription) => {
    const target = subscription.target?.target;
    if (!subscription.subscriptionId) throw new Error("SearchSubscription.subscription_id is empty");
    if (target?.$case !== "searchJobId" || !target.value) {
      throw new Error("SearchSubscription.search_job_id is empty");
    }
    return {
      subscriptionID: subscription.subscriptionId,
      searchJobID: target.value,
    };
  });
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
