import {
  expect,
  test,
  type Page,
  type Response,
  type WebSocket,
  type WebSocketRoute,
} from "@playwright/test";

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

test("live preview resumes from the exact retained WebSocket sequence", async ({ page }) => {
  test.setTimeout(60_000);
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

  const replay = await interceptOneRetainedReplay(page, origin, timeout);
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
  void createResponsePromise.catch(() => undefined);
  void resultsResponsePromise.catch(() => undefined);

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

    const resultsResponse = await resultsResponsePromise;
    assertProtobufResponse(resultsResponse);
    expect(decodeSearchResultsJobID(await resultsResponse.body())).toBe(browserSearchJobID);
  } finally {
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
  expect(createRequests, "browser search create requests").toBe(1);
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

interface RetainedReplayObservation {
  waitForCheckpoint(searchJobID: string): Promise<void>;
  disconnect(): Promise<void>;
  waitForTerminalReplay(): Promise<void>;
  dispose(): void;
}

interface RoutedSubscription {
  subscriptionID: string;
  searchJobID: string;
  afterSequence: bigint;
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
  let withheldFrame: Buffer | undefined;
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
        const subscriptions = decodeRoutedSubscriptions(message);
        if (subscriptions.length > 1) {
          throw new Error("search WebSocket sent more than one subscription in a reconnect command");
        }
        const subscription = subscriptions[0];
        if (subscription !== undefined) {
          if (connectionOrdinal === 1) {
            if (subscription.afterSequence !== 0n) {
              throw new Error(`initial subscription started after sequence ${subscription.afterSequence.toString()}`);
            }
            subscriptionID = subscription.subscriptionID;
            routedJobID = subscription.searchJobID;
          } else {
            if (checkpoint === undefined || subscriptionID === undefined || routedJobID === undefined) {
              throw new Error("reconnect subscribed before the disruption checkpoint was established");
            }
            if (
              subscription.subscriptionID !== subscriptionID
              || subscription.searchJobID !== routedJobID
              || subscription.afterSequence !== checkpoint
            ) {
              throw new Error(
                `reconnect subscription = ${JSON.stringify({
                  subscriptionID: subscription.subscriptionID,
                  searchJobID: subscription.searchJobID,
                  afterSequence: subscription.afterSequence.toString(),
                })}; expected the original subscription after ${checkpoint.toString()}`,
              );
            }
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
          if (withheldFrame !== undefined) return;
          if (
            matchesSubscription
            && checkpoint !== undefined
            && event.sequence === checkpoint + 1n
          ) {
            withheldFrame = frame;
            if (!checkpointSettled) {
              checkpointSettled = true;
              resolveCheckpoint();
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
            acknowledgment.subscriptionId === subscriptionID
            && acknowledgment.target?.target?.$case === "searchJobId"
            && acknowledgment.target.target.value === routedJobID
            && !acknowledgment.replayWillFollow
          ) {
            throw new Error("reconnect acknowledgment did not promise retained replay");
          }
        }
        if (matchesSubscription) {
          if (checkpoint === undefined || withheldFrame === undefined) {
            throw new Error("reconnect delivered a target frame before the withheld replay was captured");
          }
          const previous = lastReplaySequence ?? checkpoint;
          if (event.sequence !== previous + 1n) {
            throw new Error(
              `replay sequence = ${event.sequence.toString()}, want ${(previous + 1n).toString()}`,
            );
          }
          if (!replayVerified) {
            if (!frame.equals(withheldFrame)) {
              throw new Error("first retained replay frame was not byte-identical to the withheld server frame");
            }
            replayVerified = true;
          }
          lastReplaySequence = event.sequence;
        }
        client.send(message);
        if (matchesSubscription && event.payload?.$case === "searchTerminal") {
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
      if (firstServer === undefined || checkpoint === undefined || withheldFrame === undefined) {
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

function decodeRoutedSubscriptions(payload: Uint8Array): RoutedSubscription[] {
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
      afterSequence: subscription.afterSequence,
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
