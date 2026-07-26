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
  SearchExecutionPhase,
  SearchJobState,
  type SearchProgress,
} from "../gen/ts/open_splunk/v1/search";
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
const sequenceGapTest = process.env.OPEN_SPLUNK_E2E_SEQUENCE_GAP_TEST === "1";
const sequenceGapRESTTerminalTest =
  process.env.OPEN_SPLUNK_E2E_SEQUENCE_GAP_REST_TERMINAL_TEST === "1";
const sequenceGapRESTFirstProgressTest =
  process.env.OPEN_SPLUNK_E2E_SEQUENCE_GAP_REST_FIRST_PROGRESS_TEST === "1";
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

test("live preview recovers from real sequence expiration and a transient snapshot failure", async ({ page }) => {
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
  let releaseTransientRecoveryFailure!: () => void;
  const transientRecoveryFailureGate = new Promise<void>((resolve) => {
    releaseTransientRecoveryFailure = resolve;
  });
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
  let transientRecoveryFailures = 0;
  let initialJobStateVersion: bigint | undefined;
  const authoritativeSnapshots: GetSearchJobResponse[] = [];
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/v1/search/jobs/get",
    async (route) => {
      authoritativeJobRequests += 1;
      const requestOrdinal = authoritativeJobRequests;
      if (requestOrdinal > 7) {
        await route.abort("blockedbyclient");
        return;
      }
      if (requestOrdinal === 1) {
        await transientRecoveryFailureGate;
        transientRecoveryFailures += 1;
        await route.fulfill({
          status: 503,
          contentType: "application/json",
          body: JSON.stringify({ error: "transient authoritative recovery failure" }),
        });
        return;
      }
      if (requestOrdinal === 2) await staleRecoveryGate;
      if (requestOrdinal === 5) await postWatchdogRecoveryGate;
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
      if (requestOrdinal === 2) {
        if (initialJobStateVersion === undefined || initialJobStateVersion === 0n) {
          throw new Error("the created search job did not establish a positive state version");
        }
        response.searchJob.stateVersion = 0n;
        if (response.searchJob.progress !== undefined) {
          response.searchJob.progress.stateVersion = 0n;
        }
      }
      authoritativeSnapshots.push(response);
      if (requestOrdinal === 3) await freshRecoveryGate;
      if (requestOrdinal === 4) await delayedWatchdogGate;
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
    const statusCountBeforeTransientFailure = (await snapshotPreviewStatuses(page)).length;

    releaseTransientRecoveryFailure();
    await expiration.waitForResynchronizations(2);
    await expect.poll(() => authoritativeJobRequests, { timeout }).toBe(2);
    expect(transientRecoveryFailures, "transient authoritative GET failures").toBe(1);
    expect(authoritativeSnapshots, "snapshots after transient recovery failure").toEqual([]);
    expect(expiration.connectionCount(), "reconnect after transient authoritative failure").toBe(3);
    await expect(page.getByTestId("backend-preview-status")).toHaveAttribute(
      "data-status",
      "resyncing",
      { timeout },
    );
    await expect(page.getByLabel("Job metrics")).toContainText("0 rows", { timeout });
    expect(
      (await snapshotPreviewStatuses(page)).slice(statusCountBeforeTransientFailure),
      "preview statuses while the transient failure is retried",
    ).not.toContain("waiting");

    const statusCountBeforeStaleRecovery = (await snapshotPreviewStatuses(page)).length;
    releaseStaleRecovery();
    await expect.poll(
      () => authoritativeSnapshots.length,
      { timeout },
    ).toBeGreaterThanOrEqual(1);
    const staleJob = authoritativeSnapshots[0].searchJob;
    expect(staleJob?.stateVersion, "deliberately stale authoritative state version").toBe(0n);
    expect(staleJob?.progress?.scannedRows, "stale recovery scanned rows").toBe(3n);
    await expiration.waitForResynchronizations(3);
    await expect.poll(() => authoritativeJobRequests, { timeout }).toBe(3);
    expect(expiration.connectionCount(), "reconnect after stale authoritative snapshot").toBe(4);
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

    await expect.poll(() => authoritativeSnapshots.length, { timeout }).toBe(2);
    const recoveredJob = authoritativeSnapshots[1].searchJob;
    expect(recoveredJob?.searchJobId, "authoritative recovery job ID").toBe(browserSearchJobID);
    expect(recoveredJob?.stateVersion, "fresh authoritative state version")
      .toBeGreaterThan(createdJob.stateVersion);
    expect(
      recoveredJob?.progress?.scannedRows,
      "authoritative recovery rows captured before the queued live update",
    ).toBe(3n);

    await sendBrowserRecoveryControl("progress");
    await expect.poll(() => expiration.postRecoveryFrameCount(), { timeout }).toBe(1);
    await expect(page.getByLabel("Job metrics")).toContainText("0 rows", { timeout });
    expect(
      (await snapshotPreviewStatuses(page)).slice(statusCountBeforeStaleRecovery),
      "preview statuses while fresh authoritative recovery is blocked",
    ).not.toContain("waiting");

    releaseFreshRecovery();
    await expect.poll(() => fulfilledAuthoritativeJobRequests, { timeout }).toBeGreaterThanOrEqual(2);
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
    await expect.poll(() => authoritativeJobRequests, { timeout }).toBe(5);
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
    releaseTransientRecoveryFailure();
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
  expect(expiration.connectionCount(), "search WebSocket connections").toBe(4);
  expect(expiration.heldRecoveryFrameCount(), "target frames ignored during held recovery").toBe(1);
  expect(expiration.postRecoveryFrameCount(), "post-recovery target frames").toBeGreaterThan(0);
  expiration.assertHealthy();
  expect(safety.createRequests(), "browser search create requests").toBe(1);
  expect(transientRecoveryFailures, "transient authoritative GET failures").toBe(1);
  expect(authoritativeJobRequests, "bounded authoritative job GETs").toBeLessThanOrEqual(6);
  assertBrowserSafety(safety);
});

for (const sequenceGapScenario of [
  {
    title: "live preview recovers from a real sequence gap",
    enabled: sequenceGapTest,
    mode: "gap",
  },
  {
    title: "live preview recovers through REST-only completion after a real sequence gap",
    enabled: sequenceGapRESTTerminalTest,
    mode: "rest-terminal",
  },
  {
    title: "live progress preserves a REST-first snapshot across retained replay",
    enabled: sequenceGapRESTFirstProgressTest,
    mode: "rest-first-progress",
  },
] as const) {
test(sequenceGapScenario.title, async ({ page }) => {
  test.skip(
    !sequenceGapScenario.enabled
      || recoveryControlURL === undefined
      || !recoveryControlToken
      || !recoveryInitialText,
    "the deterministic sequence-gap fixture is not enabled",
  );
  test.setTimeout(60_000);
  const restOnlyTerminal = sequenceGapScenario.mode === "rest-terminal";
  const restFirstProgress = sequenceGapScenario.mode === "rest-first-progress";
  const safety = observeBrowserSafety(page);
  const gap = await interceptSequenceGap(
    page,
    origin,
    timeout,
    restOnlyTerminal,
  );
  let releaseAuthoritativeResultsResponse!: () => void;
  const authoritativeResultsResponseGate = new Promise<void>((resolve) => {
    releaseAuthoritativeResultsResponse = resolve;
  });
  let authoritativeResultsRequests = 0;
  const authoritativeResultSnapshots: GetSearchResultsResponse[] = [];
  if (restOnlyTerminal) {
    await page.route(
      (url) => url.origin === origin && url.pathname === "/api/v1/search/jobs/results",
      async (route) => {
        authoritativeResultsRequests += 1;
        if (authoritativeResultsRequests > 1) {
          await route.abort("blockedbyclient");
          return;
        }
        const upstream = await route.fetch();
        if (
          upstream.status() !== 200
          || upstream.headers()["content-type"] !== "application/x-protobuf"
        ) {
          throw new Error("sequence-gap authoritative results were not protobuf success");
        }
        const body = await upstream.body();
        const response = GetSearchResultsResponse.decode(body);
        authoritativeResultSnapshots.push(response);
        await authoritativeResultsResponseGate;
        await route.fulfill({
          response: upstream,
          body,
        });
      },
    );
  }
  let allowFirstAuthoritativeFetch!: () => void;
  const firstAuthoritativeFetchGate = new Promise<void>((resolve) => {
    allowFirstAuthoritativeFetch = resolve;
  });
  let allowSecondAuthoritativeFetch!: () => void;
  const secondAuthoritativeFetchGate = new Promise<void>((resolve) => {
    allowSecondAuthoritativeFetch = resolve;
  });
  let releaseFirstAuthoritativeResponse!: () => void;
  const firstAuthoritativeResponseGate = new Promise<void>((resolve) => {
    releaseFirstAuthoritativeResponse = resolve;
  });
  let releaseSecondAuthoritativeResponse!: () => void;
  const secondAuthoritativeResponseGate = new Promise<void>((resolve) => {
    releaseSecondAuthoritativeResponse = resolve;
  });
  let releaseThirdAuthoritativeResponse!: () => void;
  const thirdAuthoritativeResponseGate = new Promise<void>((resolve) => {
    releaseThirdAuthoritativeResponse = resolve;
  });
  let authoritativeJobRequests = 0;
  let fulfilledAuthoritativeJobRequests = 0;
  const authoritativeSnapshots: GetSearchJobResponse[] = [];
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/v1/search/jobs/get",
    async (route) => {
      authoritativeJobRequests += 1;
      const requestOrdinal = authoritativeJobRequests;
      if (requestOrdinal > 3) {
        await route.abort("blockedbyclient");
        return;
      }
      if (requestOrdinal === 1) await firstAuthoritativeFetchGate;
      if (requestOrdinal === 2) await secondAuthoritativeFetchGate;
      const upstream = await route.fetch();
      if (
        upstream.status() !== 200
        || upstream.headers()["content-type"] !== "application/x-protobuf"
      ) {
        throw new Error(`sequence-gap authoritative GET ${requestOrdinal} was not protobuf success`);
      }
      const response = GetSearchJobResponse.decode(await upstream.body());
      if (response.searchJob === undefined) {
        throw new Error(`sequence-gap authoritative GET ${requestOrdinal} returned no search job`);
      }
      if (restFirstProgress && requestOrdinal === 1) {
        const replayProgress = gap.replayProgress(2);
        const authoritativeProgress = response.searchJob.progress;
        if (
          authoritativeProgress === undefined
          || authoritativeProgress.stateVersion !== response.searchJob.stateVersion
          || authoritativeProgress.stateVersion !== replayProgress.stateVersion
          || authoritativeProgress.elapsed === undefined
          || authoritativeProgress.updatedAt === undefined
          || replayProgress.elapsed === undefined
          || replayProgress.updatedAt === undefined
        ) {
          throw new Error("REST-first progress snapshot did not match retained replay revision K+2");
        }
        // Exercise equal-version reconciliation against legitimate projection-time
        // drift without changing any stable execution counters.
        authoritativeProgress.elapsed = {
          seconds: replayProgress.elapsed.seconds + 1n,
          nanos: replayProgress.elapsed.nanos,
        };
        authoritativeProgress.updatedAt = new Date(replayProgress.updatedAt.getTime() + 1_000);
      }
      authoritativeSnapshots.push(response);
      if (requestOrdinal === 1) await firstAuthoritativeResponseGate;
      if (requestOrdinal === 2) await secondAuthoritativeResponseGate;
      if (requestOrdinal === 3) await thirdAuthoritativeResponseGate;
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
    await gap.waitForCheckpoint(browserSearchJobID);
    await expect(page.getByTestId("backend-preview-status")).toHaveAttribute(
      "data-status",
      "live",
      { timeout },
    );
    await expect(
      page.getByRole("table", { name: "Live preview search statistics" }),
    ).toContainText(recoveryInitialText!, { timeout });

    await sendBrowserRecoveryControl("progress");
    await expect.poll(() => gap.droppedFrameCount(), { timeout }).toBe(1);
    await expect(page.getByLabel("Job metrics")).toContainText("0 rows", { timeout });

    await sendBrowserRecoveryControl("progress");
    await gap.waitForReplayReady();
    allowFirstAuthoritativeFetch();
    await expect.poll(() => authoritativeSnapshots.length, { timeout }).toBe(1);
    const preReplaySnapshot = authoritativeSnapshots[0].searchJob;
    expect(preReplaySnapshot?.searchJobId, "pre-replay authoritative job ID").toBe(browserSearchJobID);
    expect(preReplaySnapshot?.progress?.scannedRows, "pre-replay authoritative rows").toBe(2n);
    expect(preReplaySnapshot?.progress?.stateVersion, "pre-replay progress revision").toBe(
      preReplaySnapshot?.stateVersion,
    );
    expect(preReplaySnapshot?.progress?.stateVersion, "pre-replay K+2 revision").toBe(
      gap.replayProgress(2).stateVersion,
    );
    expect(fulfilledAuthoritativeJobRequests, "authoritative responses before replay").toBe(0);
    expect(gap.connectionCount(), "sequence-gap WebSocket connections").toBe(2);
    await expect(page.getByTestId("backend-preview-status")).toHaveAttribute(
      "data-status",
      "resyncing",
      { timeout },
    );
    await expect(
      page.getByRole("table", { name: "Live preview search statistics" }),
    ).toHaveCount(0);
    await expect(page.getByText(recoveryInitialText!, { exact: true })).toHaveCount(0);
    await expect(page.getByLabel("Job metrics")).toContainText("0 rows", { timeout });
    await expect(page.locator("body")).toContainText(
      /Live job updates skipped a sequence; resynchronizing from the server/i,
      { timeout },
    );

    if (restFirstProgress) {
      const replayProgressK1 = gap.replayProgress(1);
      const replayProgressK2 = gap.replayProgress(2);
      expect(replayProgressK1.stateVersion, "retained progress revisions").toBeLessThan(
        replayProgressK2.stateVersion,
      );
      expect(preReplaySnapshot?.progress?.updatedAt?.getTime(), "REST projection-time drift").toBe(
        (replayProgressK2.updatedAt?.getTime() ?? 0) + 1_000,
      );
      expect(preReplaySnapshot?.progress?.elapsed?.seconds, "REST elapsed-time drift").toBe(
        (replayProgressK2.elapsed?.seconds ?? 0n) + 1n,
      );

      releaseFirstAuthoritativeResponse();
      await expect.poll(() => fulfilledAuthoritativeJobRequests, { timeout }).toBe(1);
      await expect(page.getByLabel("Job metrics")).toContainText("2 rows", { timeout });
      await expect(page.locator("body")).toContainText(
        /resynchronized from the server after a sequence gap/i,
        { timeout },
      );
      expect(authoritativeJobRequests, "REST-first authoritative job GETs").toBe(1);

      await beginJobMetricsObservation(page);
      const firstReplaySequence = gap.releaseNextReplayFrame();
      await gap.waitForReplayFrameReceived(firstReplaySequence);
      await waitForBrowserRender(page);
      await expect(page.getByLabel("Job metrics")).toContainText("2 rows", { timeout });
      const secondReplaySequence = gap.releaseNextReplayFrame();
      expect(secondReplaySequence, "contiguous replay sequence").toBe(firstReplaySequence + 1n);
      await gap.waitForReplayFrameReceived(secondReplaySequence);
      await waitForBrowserRender(page);
      await expect(page.getByLabel("Job metrics")).toContainText("2 rows", { timeout });
    } else {
      const firstReplaySequence = gap.releaseNextReplayFrame();
      await expect(page.getByLabel("Job metrics")).toContainText("1 rows", { timeout });
      expect(fulfilledAuthoritativeJobRequests, "responses after replay K+1").toBe(0);
      const secondReplaySequence = gap.releaseNextReplayFrame();
      expect(secondReplaySequence, "contiguous replay sequence").toBe(firstReplaySequence + 1n);
      await expect(page.getByLabel("Job metrics")).toContainText("2 rows", { timeout });
      expect(fulfilledAuthoritativeJobRequests, "responses after replay K+2").toBe(0);
    }

    await sendBrowserRecoveryControl("progress");
    await expect(page.getByLabel("Job metrics")).toContainText("3 rows", { timeout });
    if (restFirstProgress) {
      const observedMetrics = await finishJobMetricsObservation(page);
      expect(
        observedMetrics.some((text) => /^(?:≈ )?1 rows$/.test(text)),
        "stale replay K+1 never rendered, even transiently",
      ).toBe(false);
      expect(authoritativeJobRequests, "authoritative GETs through live K+3").toBe(1);
      await expect(page.locator("body")).not.toContainText(
        /Live search progress was inconsistent|legacy live progress update could not be ordered/i,
      );
    }
    expect(gap.connectionCount(), "connections after live K+3").toBe(2);
    await sendBrowserRecoveryControl("progress");
    await expect(page.getByLabel("Job metrics")).toContainText("4 rows", { timeout });
    await sendBrowserRecoveryControl("progress");
    await expect(page.getByLabel("Job metrics")).toContainText("5 rows", { timeout });

    // The original scenarios keep GET 1 at revision K+2 in flight until live
    // progress reaches K+5, proving the progress-revision fence rejects it.
    // The REST-first scenario applied GET 1 at K+2 and waits for the ordinary
    // active-job poll instead.
    releaseFirstAuthoritativeResponse();
    await expect.poll(() => authoritativeJobRequests, { timeout }).toBe(2);
    expect(fulfilledAuthoritativeJobRequests, "stale authoritative responses").toBe(1);
    await expect(page.getByLabel("Job metrics")).toContainText("5 rows", { timeout });
    await expect(page.getByTestId("backend-preview-status")).toHaveAttribute(
      "data-status",
      "resyncing",
      { timeout },
    );
    await expect(
      page.getByRole("table", { name: "Live preview search statistics" }),
    ).toHaveCount(0);
    await expect(page.getByText(recoveryInitialText!, { exact: true })).toHaveCount(0);

    if (!restOnlyTerminal) {
      // Capture GET 2 before completion so the subsequent terminal revision
      // deterministically makes this response stale and requires GET 3.
      allowSecondAuthoritativeFetch();
      await expect.poll(() => authoritativeSnapshots.length, { timeout }).toBe(2);
      const preTerminalSnapshot = authoritativeSnapshots[1].searchJob;
      expect(preTerminalSnapshot?.searchJobId, "in-flight pre-terminal job ID").toBe(browserSearchJobID);
      expect(preTerminalSnapshot?.state, "in-flight pre-terminal job state").toBe(
        SearchJobState.SEARCH_JOB_STATE_RUNNING,
      );
      expect(preTerminalSnapshot?.progress?.scannedRows, "in-flight pre-terminal rows").toBe(5n);
    }

    await sendBrowserRecoveryControl("complete");
    if (restOnlyTerminal) {
      await gap.waitForTerminalProjectionWithheld();
      expect(
        gap.withheldTerminalProjectionFrameCount(),
        "withheld terminal projection frames",
      ).toBe(3);
      await expect(page.getByTestId("backend-preview-status")).toHaveAttribute(
        "data-status",
        "resyncing",
        { timeout },
      );
      await expect(
        page.getByRole("table", { name: "Live preview search statistics" }),
      ).toHaveCount(0);
      await expect(
        page.getByRole("table", { name: "Backend search statistics" }),
      ).toHaveCount(0);
      await expect(page.getByText(recoveryInitialText!, { exact: true })).toHaveCount(0);
      await expect(page.getByTestId("job-strip")).toHaveAttribute(
        "aria-busy",
        "true",
        { timeout },
      );
      await expect(page.getByTestId("job-strip")).not.toContainText("Completed");
      expect(fulfilledAuthoritativeJobRequests, "responses before REST terminal").toBe(1);

      allowSecondAuthoritativeFetch();
      await expect.poll(() => authoritativeSnapshots.length, { timeout }).toBe(2);
      const restTerminalSnapshot = authoritativeSnapshots[1].searchJob;
      expect(restTerminalSnapshot?.searchJobId, "REST terminal job ID").toBe(browserSearchJobID);
      expect(restTerminalSnapshot?.state, "REST terminal job state").toBe(
        SearchJobState.SEARCH_JOB_STATE_COMPLETED,
      );
      expect(restTerminalSnapshot?.progress?.scannedRows, "REST terminal rows").toBe(5n);
      releaseSecondAuthoritativeResponse();

      await expect.poll(() => fulfilledAuthoritativeJobRequests, { timeout }).toBe(2);
      await expect.poll(() => authoritativeResultSnapshots.length, { timeout }).toBe(1);
      expect(
        authoritativeResultSnapshots[0].searchJobId,
        "gated authoritative results job ID",
      ).toBe(browserSearchJobID);
      expect(
        authoritativeResultSnapshots[0].resultPage?.rows.length,
        "gated authoritative result rows",
      ).toBe(expectedRows);

      await expectZeroRowBackendPreviewFinalizing(page);
      await expect(
        page.getByRole("table", { name: "Backend search statistics" }),
      ).toHaveCount(0);
      await expect(page.getByText(recoveryInitialText!, { exact: true })).toHaveCount(0);
      await expect(page.getByTestId("job-strip")).toHaveAttribute(
        "aria-busy",
        "true",
        { timeout },
      );
      await expect(page.getByTestId("job-strip")).toContainText("Finalizing", { timeout });
      releaseAuthoritativeResultsResponse();
    } else {
      await expectZeroRowBackendPreviewFinalizing(page);
      await expect(page.getByTestId("job-strip")).toContainText("Completed", { timeout });
      expect(fulfilledAuthoritativeJobRequests, "responses before terminal replay proof").toBe(1);

      const staleTerminalSnapshot = authoritativeSnapshots[1].searchJob;
      expect(staleTerminalSnapshot?.searchJobId, "in-flight terminal job ID").toBe(browserSearchJobID);
      expect(staleTerminalSnapshot?.state, "in-flight terminal job state").toBe(
        SearchJobState.SEARCH_JOB_STATE_RUNNING,
      );
      expect(staleTerminalSnapshot?.progress?.scannedRows, "in-flight terminal rows").toBe(5n);
      releaseSecondAuthoritativeResponse();

      await expect.poll(() => authoritativeJobRequests, { timeout }).toBe(3);
      await expect.poll(() => authoritativeSnapshots.length, { timeout }).toBe(3);
      const recoveredSnapshot = authoritativeSnapshots[2].searchJob;
      expect(recoveredSnapshot?.searchJobId, "post-gap authoritative job ID").toBe(browserSearchJobID);
      expect(recoveredSnapshot?.state, "post-gap authoritative job state").toBe(
        SearchJobState.SEARCH_JOB_STATE_COMPLETED,
      );
      expect(recoveredSnapshot?.progress?.scannedRows, "post-gap authoritative rows").toBe(5n);
      expect(fulfilledAuthoritativeJobRequests, "responses before final recovery").toBe(2);
      releaseThirdAuthoritativeResponse();
    }

    const resultsResponse = await resultsResponsePromise;
    assertProtobufResponse(resultsResponse);
    expect(decodeSearchResultsJobID(await resultsResponse.body())).toBe(browserSearchJobID);
    await gap.waitForTerminalClose();

    await expect.poll(() => fulfilledAuthoritativeJobRequests, { timeout }).toBe(
      restOnlyTerminal ? 2 : 3,
    );
    const jobStrip = page.getByTestId("job-strip");
    await expect(jobStrip).toHaveAttribute("aria-busy", "false", { timeout });
    await expect(jobStrip).toContainText("Completed", { timeout });
    await expect(jobStrip).toContainText(`${expectedRows} rows`, { timeout });
    const finalTable = page.getByRole("table", { name: "Backend search statistics" });
    await expect(finalTable.locator("tbody tr")).toHaveCount(expectedRows, { timeout });
    await expect(finalTable).toContainText(expectedText, { timeout });
    await expect(
      page.getByRole("table", { name: "Live preview search statistics" }),
    ).toHaveCount(0);
    await expect(page.getByTestId("backend-preview-status")).toHaveCount(0);
    if (!restFirstProgress) {
      await expect(page.locator("body")).toContainText(
        /resynchronized from the server after a sequence gap/i,
        { timeout },
      );
    }

    const previewStatuses = await collectPreviewStatuses(page);
    expect(previewStatuses, "UI preview status transitions").toContain("live");
    expect(previewStatuses, "UI preview status transitions").toContain("resyncing");
    expect(previewStatuses, "UI preview status transitions").toContain("finalizing");
    expect(previewStatuses, "UI preview status transitions").not.toContain("finalization-error");
    expect(gap.connectionCount(), "search WebSocket connections").toBe(2);
    expect(gap.liveFrameCount(), "post-replay live target frames").toBe(
      restOnlyTerminal ? 3 : 6,
    );
    gap.assertHealthy();
    expect(safety.createRequests(), "browser search create requests").toBe(1);
    expect(authoritativeJobRequests, "authoritative job GETs").toBe(
      restOnlyTerminal ? 2 : 3,
    );
    expect(authoritativeResultsRequests, "authoritative results GETs").toBe(
      restOnlyTerminal ? 1 : 0,
    );
    assertBrowserSafety(safety);
  } finally {
    await discardJobMetricsObservation(page).catch(() => undefined);
    allowFirstAuthoritativeFetch();
    allowSecondAuthoritativeFetch();
    releaseFirstAuthoritativeResponse();
    releaseSecondAuthoritativeResponse();
    releaseThirdAuthoritativeResponse();
    releaseAuthoritativeResultsResponse();
    gap.dispose();
  }
});
}

async function expectZeroRowBackendPreviewFinalizing(page: Page): Promise<void> {
  const status = page.getByTestId("backend-preview-status");
  await expect(status).toHaveAttribute("data-status", "finalizing", { timeout });
  await expect(status).toContainText("Loading the authoritative result snapshot.", { timeout });
  await expect(
    page.getByText("Search complete. Loading authoritative results.", { exact: true }),
  ).toHaveCount(1);
  await expect(
    page.getByRole("table", { name: "Live preview search statistics" }),
  ).toHaveCount(0);
}

async function beginJobMetricsObservation(page: Page): Promise<void> {
  await page.evaluate(() => {
    const target = document.querySelector('[aria-label="Job metrics"]');
    if (target === null) throw new Error("job metrics are unavailable for replay observation");
    const observed: string[] = [];
    const record = (): void => {
      const scannedRows = target.querySelector("strong")?.textContent;
      if (scannedRows === undefined) {
        throw new Error("scanned rows are unavailable for replay observation");
      }
      observed.push(scannedRows);
    };
    record();
    const observer = new MutationObserver(record);
    observer.observe(target, { childList: true, characterData: true, subtree: true });
    const observedWindow = window as JobMetricsObservationWindow;
    if (observedWindow.openSplunkJobMetricsObservation !== undefined) {
      observer.disconnect();
      throw new Error("job metrics replay observation is already active");
    }
    observedWindow.openSplunkJobMetricsObservation = { observed, observer };
  });
}

async function finishJobMetricsObservation(page: Page): Promise<string[]> {
  return stopJobMetricsObservation(page, true);
}

async function discardJobMetricsObservation(page: Page): Promise<void> {
  await stopJobMetricsObservation(page, false);
}

async function stopJobMetricsObservation(
  page: Page,
  required: boolean,
): Promise<string[]> {
  return page.evaluate((observationRequired) => {
    const observedWindow = window as JobMetricsObservationWindow;
    const observation = observedWindow.openSplunkJobMetricsObservation;
    if (observation === undefined) {
      if (observationRequired) {
        throw new Error("job metrics replay observation was not active");
      }
      return [];
    }
    observation.observer.disconnect();
    delete observedWindow.openSplunkJobMetricsObservation;
    return observation.observed;
  }, required);
}

async function waitForBrowserRender(page: Page): Promise<void> {
  await page.evaluate(
    () => new Promise<void>((resolve) => {
      setTimeout(() => {
        requestAnimationFrame(() => requestAnimationFrame(() => resolve()));
      }, 0);
    }),
  );
}

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

interface JobMetricsObservation {
  observed: string[];
  observer: MutationObserver;
}

type JobMetricsObservationWindow = Window & {
  openSplunkJobMetricsObservation?: JobMetricsObservation;
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

function assertRetainedSubscription(
  candidate: ObservedSubscription,
  initial: ObservedSubscription,
  expectedAfterSequence: bigint,
  context: string,
): void {
  if (
    candidate.subscriptionID === initial.subscriptionID
    && candidate.searchJobID === initial.searchJobID
    && candidate.afterSequence === expectedAfterSequence
    && candidate.includePreviews === initial.includePreviews
    && candidate.previewRowLimit === initial.previewRowLimit
  ) {
    return;
  }
  throw new Error(
    `${context} subscription = ${JSON.stringify({
      subscriptionID: candidate.subscriptionID,
      searchJobID: candidate.searchJobID,
      afterSequence: candidate.afterSequence.toString(),
      includePreviews: candidate.includePreviews,
      previewRowLimit: candidate.previewRowLimit,
    })}; expected the original subscription after ${expectedAfterSequence.toString()}`,
  );
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

interface SequenceGapObservation {
  waitForCheckpoint(searchJobID: string): Promise<void>;
  droppedFrameCount(): number;
  waitForReplayReady(): Promise<void>;
  replayProgress(index: 1 | 2): SearchProgress;
  releaseNextReplayFrame(): bigint;
  waitForReplayFrameReceived(sequence: bigint): Promise<void>;
  waitForTerminalProjectionWithheld(): Promise<void>;
  withheldTerminalProjectionFrameCount(): number;
  waitForTerminalClose(): Promise<void>;
  connectionCount(): number;
  liveFrameCount(): number;
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

async function interceptSequenceGap(
  page: Page,
  expectedOrigin: string,
  timeoutMilliseconds: number,
  withholdTerminalProjection = false,
): Promise<SequenceGapObservation> {
  let expectedJobID: string | undefined;
  let routedJobID: string | undefined;
  let subscriptionID: string | undefined;
  let checkpoint: bigint | undefined;
  let initialSubscription: ObservedSubscription | undefined;
  let reconnectRequestID: string | undefined;
  let reconnectAcknowledged = false;
  let secondClient: WebSocketRoute | undefined;
  let lastUpstreamSequence: bigint | undefined;
  let matchingConnectionCount = 0;
  let subscribeCommandCount = 0;
  let clientFrameCount = 0;
  let serverFrameCount = 0;
  let gapCloseCount = 0;
  let gapCloseEchoCount = 0;
  let terminalCloseCount = 0;
  let terminalCloseEchoCount = 0;
  let gapCloseForwardCompleted = false;
  let terminalCloseForwardCompleted = false;
  let replayReleaseCount = 0;
  let liveTargetFrameCount = 0;
  let withholdingTerminalProjection = false;
  let withheldTerminalProjectionFrameCount = 0;
  let withheldTerminalStateVersion: bigint | undefined;
  let gapStimulusSent = false;
  let disposed = false;
  let failure: Error | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const forwardedCloseConnections = new Set<number>();
  const originalFrames = new Map<bigint, Buffer>();
  const replayFrames = new Map<bigint, Buffer>();
  const connectionOneDeliveredSequences: bigint[] = [];
  const replayBrowserFrameReceipts = new Set<bigint>();
  const replayBrowserFrameWaiters = new Map<
    bigint,
    { resolve: () => void; reject: (reason: Error) => void }
  >();
  let matchingBrowserSocketCount = 0;
  let replayBrowserSocket: WebSocket | undefined;
  let replayBrowserFrameListener: ((event: FrameEvent) => void) | undefined;
  let checkpointSettled = false;
  let replayReadySettled = false;
  let gapCloseSettled = false;
  let terminalProjectionSettled = false;
  let terminalCloseSettled = false;
  let resolveCheckpoint!: () => void;
  let rejectCheckpoint!: (reason: Error) => void;
  let resolveReplayReady!: () => void;
  let rejectReplayReady!: (reason: Error) => void;
  let resolveGapClose!: () => void;
  let rejectGapClose!: (reason: Error) => void;
  let resolveTerminalProjection!: () => void;
  let rejectTerminalProjection!: (reason: Error) => void;
  let resolveTerminalClose!: () => void;
  let rejectTerminalClose!: (reason: Error) => void;
  const checkpointReady = new Promise<void>((resolve, reject) => {
    resolveCheckpoint = resolve;
    rejectCheckpoint = reject;
  });
  const replayReady = new Promise<void>((resolve, reject) => {
    resolveReplayReady = resolve;
    rejectReplayReady = reject;
  });
  const gapCloseReady = new Promise<void>((resolve, reject) => {
    resolveGapClose = resolve;
    rejectGapClose = reject;
  });
  const terminalProjectionReady = new Promise<void>((resolve, reject) => {
    resolveTerminalProjection = resolve;
    rejectTerminalProjection = reject;
  });
  const terminalCloseReady = new Promise<void>((resolve, reject) => {
    resolveTerminalClose = resolve;
    rejectTerminalClose = reject;
  });
  void checkpointReady.catch(() => undefined);
  void replayReady.catch(() => undefined);
  void gapCloseReady.catch(() => undefined);
  void terminalProjectionReady.catch(() => undefined);
  void terminalCloseReady.catch(() => undefined);

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
    if (!replayReadySettled) {
      replayReadySettled = true;
      rejectReplayReady(normalized);
    }
    if (!gapCloseSettled) {
      gapCloseSettled = true;
      rejectGapClose(normalized);
    }
    if (!terminalProjectionSettled) {
      terminalProjectionSettled = true;
      rejectTerminalProjection(normalized);
    }
    if (!terminalCloseSettled) {
      terminalCloseSettled = true;
      rejectTerminalClose(normalized);
    }
    for (const waiter of replayBrowserFrameWaiters.values()) waiter.reject(normalized);
    replayBrowserFrameWaiters.clear();
  };
  const routeMatchesSearchSocket = (url: URL): boolean =>
    httpOriginForWebSocket(url) === expectedOrigin && url.pathname === "/api/v1/search/ws";
  const observeBrowserSocket = (socket: WebSocket): void => {
    const socketURL = new URL(socket.url());
    if (!routeMatchesSearchSocket(socketURL)) return;
    matchingBrowserSocketCount += 1;
    if (matchingBrowserSocketCount !== 2) return;
    replayBrowserSocket = socket;
    replayBrowserFrameListener = ({ payload }) => {
      try {
        if (typeof payload === "string") {
          throw new Error("sequence-gap browser received a text replay frame");
        }
        const event = SearchWebSocketEvent.decode(payload);
        if (
          checkpoint === undefined
          || event.sequence < checkpoint + 1n
          || event.sequence > checkpoint + 2n
        ) {
          return;
        }
        replayBrowserFrameReceipts.add(event.sequence);
        replayBrowserFrameWaiters.get(event.sequence)?.resolve();
        replayBrowserFrameWaiters.delete(event.sequence);
      } catch (error) {
        fail(error);
      }
    };
    socket.on("framereceived", replayBrowserFrameListener);
  };
  page.on("websocket", observeBrowserSocket);

  await page.routeWebSocket(routeMatchesSearchSocket, (client) => {
    matchingConnectionCount += 1;
    const connectionOrdinal = matchingConnectionCount;
    const server = client.connectToServer();
    if (connectionOrdinal === 2) secondClient = client;
    if (connectionOrdinal > 2) {
      fail(new Error(`sequence-gap recovery opened ${connectionOrdinal} matching connections`));
      return;
    }

    client.onMessage((message) => {
      try {
        clientFrameCount += 1;
        if (clientFrameCount > 32) {
          throw new Error("sequence-gap WebSocket sent more than 32 routed command frames");
        }
        if (typeof message === "string") throw new Error("sequence-gap WebSocket sent a text frame");
        const subscribe = decodeSearchSubscribeCommand(message);
        if (subscribe !== undefined && subscribe.subscriptions.length !== 1) {
          throw new Error("sequence-gap WebSocket sent a non-singleton subscribe command");
        }
        const subscription = subscribe?.subscriptions[0];
        if (subscription !== undefined) {
          subscribeCommandCount += 1;
          if (connectionOrdinal === 1) {
            if (initialSubscription !== undefined || subscription.afterSequence !== 0n) {
              throw new Error("sequence-gap initial subscription was duplicated or resumed");
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
              || reconnectRequestID !== undefined
              || subscribe === undefined
            ) {
              throw new Error("sequence-gap reconnect preceded its exact checkpoint");
            }
            assertRetainedSubscription(
              subscription,
              initialSubscription,
              checkpoint,
              "sequence-gap reconnect",
            );
            reconnectRequestID = subscribe.requestID;
          }
        }
        server.send(message);
      } catch (error) {
        fail(error);
      }
    });

    client.onClose((code, reason) => {
      if (disposed) {
        if (!forwardedCloseConnections.has(connectionOrdinal)) {
          forwardedCloseConnections.add(connectionOrdinal);
          void server.close({ code, reason }).catch(() => undefined);
        }
        return;
      }
      try {
        if (
          connectionOrdinal === 2
          && code === 1000
          && (reason === undefined || reason === "" || reason === "Client disposed")
        ) {
          if (terminalCloseCount === 0) {
            terminalCloseCount = 1;
            forwardedCloseConnections.add(connectionOrdinal);
            void server.close({ code, reason })
              .then(() => {
                terminalCloseForwardCompleted = true;
                if (timer !== undefined) {
                  clearTimeout(timer);
                  timer = undefined;
                }
                if (!terminalCloseSettled) {
                  terminalCloseSettled = true;
                  resolveTerminalClose();
                }
              })
              .catch(fail);
            return;
          }
          if (terminalCloseEchoCount !== 0) {
            throw new Error("sequence-gap terminal close produced more than one forwarded echo");
          }
          terminalCloseEchoCount = 1;
          return;
        }
        if (connectionOrdinal === 1 && gapCloseCount === 1 && code === 4000) {
          if (gapCloseEchoCount !== 0) {
            throw new Error("sequence-gap close produced more than one forwarded echo");
          }
          gapCloseEchoCount += 1;
          return;
        }
        if (
          connectionOrdinal !== 1
          || gapCloseCount !== 0
          || !gapStimulusSent
          || code !== 4000
          || (
            reason !== undefined
            && reason !== ""
            && reason !== "Sequence gap; replay required"
          )
        ) {
          throw new Error(
            `unexpected sequence-gap client close on connection ${connectionOrdinal}: ${String(code)} ${JSON.stringify(reason)}`,
          );
        }
        gapCloseCount += 1;
        forwardedCloseConnections.add(connectionOrdinal);
        void server.close({ code, reason })
          .then(() => {
            gapCloseForwardCompleted = true;
            if (!gapCloseSettled) {
              gapCloseSettled = true;
              resolveGapClose();
            }
          })
          .catch(fail);
      } catch (error) {
        fail(error);
      }
    });

    server.onMessage((message) => {
      try {
        serverFrameCount += 1;
        if (serverFrameCount > 128) {
          throw new Error("sequence-gap WebSocket received more than 128 routed event frames");
        }
        if (typeof message === "string") throw new Error("sequence-gap WebSocket received a text frame");
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
            if (originalFrames.size >= 2) {
              throw new Error("sequence-gap connection published more than K+1 and K+2 before reconnect");
            }
            const expectedSequence = checkpoint + BigInt(originalFrames.size + 1);
            const expectedProgressRows = BigInt(originalFrames.size + 1);
            if (
              event.sequence !== expectedSequence
              || event.payload?.$case !== "searchProgress"
              || event.payload.value.scannedRows !== expectedProgressRows
              || event.payload.value.stateVersion <= 0n
            ) {
              throw new Error(
                `sequence-gap stimulus ${event.sequence.toString()} was not progress ${expectedProgressRows.toString()} at ${expectedSequence.toString()}`,
              );
            }
            if (originalFrames.size === 1) {
              const firstFrame = originalFrames.get(checkpoint + 1n);
              const firstEvent = firstFrame === undefined
                ? undefined
                : SearchWebSocketEvent.decode(firstFrame);
              if (
                firstEvent?.payload?.$case !== "searchProgress"
                || event.payload.value.stateVersion !== firstEvent.payload.value.stateVersion + 1n
              ) {
                throw new Error("sequence-gap progress revisions K+1 and K+2 were not contiguous");
              }
            }
            originalFrames.set(event.sequence, frame);
            if (originalFrames.size === 1) return;
            gapStimulusSent = true;
            connectionOneDeliveredSequences.push(event.sequence);
            client.send(message);
            return;
          }
          client.send(message);
          if (
            matchesSubscription
            && event.payload?.$case === "resultPreview"
            && event.payload.value.rows.length > 0
          ) {
            checkpoint = event.sequence;
            if (!checkpointSettled) {
              checkpointSettled = true;
              resolveCheckpoint();
            }
          }
          return;
        }

        if (event.payload?.$case === "resynchronizationRequired") {
          throw new Error(
            `sequence gap unexpectedly required authoritative resynchronization: ${event.payload.value.reason}`,
          );
        }
        if (event.payload?.$case === "protocolError") {
          throw new Error(`sequence-gap subscription received protocol error ${event.payload.value.code}`);
        }
        if (event.payload?.$case === "subscriptionAcknowledged") {
          const acknowledgment = event.payload.value;
          if (
            reconnectAcknowledged
            || reconnectRequestID === undefined
            || checkpoint === undefined
            || subscriptionID === undefined
            || routedJobID === undefined
            || event.sequence !== 0n
            || event.subscriptionId !== subscriptionID
            || acknowledgment.requestId !== reconnectRequestID
            || acknowledgment.subscriptionId !== subscriptionID
            || acknowledgment.target?.target?.$case !== "searchJobId"
            || acknowledgment.target.target.value !== routedJobID
            || !acknowledgment.replayWillFollow
            || acknowledgment.earliestAvailableSequence > checkpoint + 1n
            || acknowledgment.latestSequence !== checkpoint + 2n
          ) {
            throw new Error("sequence-gap reconnect acknowledgment was invalid");
          }
          reconnectAcknowledged = true;
          client.send(message);
          return;
        }
        if (matchesSubscription) {
          if (checkpoint === undefined || !reconnectAcknowledged) {
            throw new Error("sequence-gap reconnect delivered a target frame before acknowledgment");
          }
          if (replayFrames.size < 2) {
            const expectedSequence = checkpoint + BigInt(replayFrames.size + 1);
            const original = originalFrames.get(expectedSequence);
            if (
              event.sequence !== expectedSequence
              || original === undefined
              || !frame.equals(original)
            ) {
              throw new Error(
                `sequence-gap replay frame ${event.sequence.toString()} was not the byte-identical ${expectedSequence.toString()} frame`,
              );
            }
            replayFrames.set(event.sequence, frame);
            lastUpstreamSequence = event.sequence;
            if (replayFrames.size === 2 && !replayReadySettled) {
              replayReadySettled = true;
              resolveReplayReady();
            }
            return;
          }
          if (replayReleaseCount !== 2 || lastUpstreamSequence === undefined) {
            throw new Error("sequence-gap live target frame arrived before replay release completed");
          }
          if (event.sequence !== lastUpstreamSequence + 1n) {
            throw new Error(
              `sequence-gap live sequence = ${event.sequence.toString()}, want ${(lastUpstreamSequence + 1n).toString()}`,
            );
          }
          lastUpstreamSequence = event.sequence;
          if (
            withholdTerminalProjection
            && (
              withholdingTerminalProjection
              || (
                event.payload?.$case === "searchStateChanged"
                && event.payload.value.state === SearchJobState.SEARCH_JOB_STATE_COMPLETED
              )
            )
          ) {
            withholdingTerminalProjection = true;
            const expectedPayloads = [
              "searchStateChanged",
              "searchProgress",
              "searchTerminal",
            ] as const;
            const expectedPayload =
              expectedPayloads[withheldTerminalProjectionFrameCount];
            if (expectedPayload === undefined || event.payload?.$case !== expectedPayload) {
              throw new Error(
                `sequence-gap terminal projection frame ${withheldTerminalProjectionFrameCount + 1} was ${String(event.payload?.$case)}, want ${String(expectedPayload)}`,
              );
            }
            withheldTerminalProjectionFrameCount += 1;
            if (event.payload.$case === "searchStateChanged") {
              if (
                event.payload.value.searchJobId !== routedJobID
                || event.payload.value.state
                !== SearchJobState.SEARCH_JOB_STATE_COMPLETED
                || event.payload.value.stateVersion <= 0n
              ) {
                throw new Error("sequence-gap withheld completed state was invalid");
              }
              withheldTerminalStateVersion = event.payload.value.stateVersion;
            } else if (event.payload.$case === "searchProgress") {
              if (
                event.payload.value.phase
                !== SearchExecutionPhase.SEARCH_EXECUTION_PHASE_COMPLETE
                || event.payload.value.scannedRows !== 5n
                || event.payload.value.producedRows !== 1n
                || event.payload.value.stateVersion !== withheldTerminalStateVersion
              ) {
                throw new Error("sequence-gap withheld final progress was invalid");
              }
            } else if (event.payload.$case === "searchTerminal") {
              if (
                event.payload.value.searchJobId !== routedJobID
                || event.payload.value.state
                !== SearchJobState.SEARCH_JOB_STATE_COMPLETED
                || event.payload.value.stateVersion !== withheldTerminalStateVersion
                || event.payload.value.finalProgress?.phase
                !== SearchExecutionPhase.SEARCH_EXECUTION_PHASE_COMPLETE
                || event.payload.value.finalProgress.scannedRows !== 5n
                || event.payload.value.finalProgress.producedRows !== 1n
                || event.payload.value.finalProgress.stateVersion
                !== withheldTerminalStateVersion
              ) {
                throw new Error("sequence-gap withheld terminal event was invalid");
              }
              if (!terminalProjectionSettled) {
                terminalProjectionSettled = true;
                resolveTerminalProjection();
              }
            }
            return;
          }
          liveTargetFrameCount += 1;
        }
        client.send(message);
      } catch (error) {
        fail(error);
      }
    });
  });

  timer = setTimeout(
    () => fail(new Error("timed out waiting for deterministic sequence-gap replay")),
    timeoutMilliseconds,
  );
  return {
    waitForCheckpoint(searchJobID) {
      if (expectedJobID !== undefined) throw new Error("sequence-gap browser job was already selected");
      expectedJobID = searchJobID;
      if (routedJobID !== undefined && routedJobID !== expectedJobID) {
        fail(
          new Error(
            `sequence-gap routed job ${routedJobID} did not match created job ${expectedJobID}`,
          ),
        );
      }
      return checkpointReady;
    },
    droppedFrameCount() {
      return originalFrames.size;
    },
    waitForReplayReady() {
      if (failure !== undefined) return Promise.reject(failure);
      return Promise.all([gapCloseReady, replayReady]).then(() => undefined);
    },
    replayProgress(index) {
      if (failure !== undefined) throw failure;
      if (checkpoint === undefined || replayFrames.size !== 2) {
        throw new Error("sequence-gap replay is not ready for progress inspection");
      }
      const sequence = checkpoint + BigInt(index);
      const frame = replayFrames.get(sequence);
      if (frame === undefined) {
        throw new Error(`sequence-gap replay ${sequence.toString()} is missing`);
      }
      const event = SearchWebSocketEvent.decode(frame);
      if (event.payload?.$case !== "searchProgress") {
        throw new Error(`sequence-gap replay ${sequence.toString()} is not progress`);
      }
      return event.payload.value;
    },
    releaseNextReplayFrame() {
      if (failure !== undefined) throw failure;
      if (
        checkpoint === undefined
        || secondClient === undefined
        || replayFrames.size !== 2
        || replayReleaseCount >= 2
      ) {
        throw new Error("sequence-gap replay is not ready for staged release");
      }
      const sequence = checkpoint + BigInt(replayReleaseCount + 1);
      const frame = replayFrames.get(sequence);
      if (frame === undefined) throw new Error(`sequence-gap replay ${sequence.toString()} is missing`);
      replayReleaseCount += 1;
      secondClient.send(frame);
      return sequence;
    },
    waitForReplayFrameReceived(sequence) {
      if (failure !== undefined) return Promise.reject(failure);
      if (
        checkpoint === undefined
        || sequence < checkpoint + 1n
        || sequence > checkpoint + 2n
      ) {
        return Promise.reject(
          new Error(`sequence-gap replay receipt ${sequence.toString()} is outside K+1..K+2`),
        );
      }
      if (replayBrowserFrameReceipts.has(sequence)) return Promise.resolve();
      if (replayBrowserFrameWaiters.has(sequence)) {
        return Promise.reject(
          new Error(`sequence-gap replay receipt ${sequence.toString()} already has a waiter`),
        );
      }
      return new Promise<void>((resolve, reject) => {
        replayBrowserFrameWaiters.set(sequence, { resolve, reject });
      });
    },
    waitForTerminalProjectionWithheld() {
      if (!withholdTerminalProjection) {
        return Promise.reject(new Error("sequence-gap terminal projection is not being withheld"));
      }
      if (failure !== undefined) return Promise.reject(failure);
      return terminalProjectionReady;
    },
    withheldTerminalProjectionFrameCount() {
      return withheldTerminalProjectionFrameCount;
    },
    waitForTerminalClose() {
      if (failure !== undefined) return Promise.reject(failure);
      return terminalCloseReady;
    },
    connectionCount() {
      return matchingConnectionCount;
    },
    liveFrameCount() {
      return liveTargetFrameCount;
    },
    assertHealthy() {
      if (failure !== undefined) throw failure;
      if (
        expectedJobID === undefined
        || routedJobID !== expectedJobID
        || checkpoint === undefined
        || subscribeCommandCount !== 2
        || matchingConnectionCount !== 2
        || originalFrames.size !== 2
        || connectionOneDeliveredSequences.length !== 1
        || connectionOneDeliveredSequences[0] !== checkpoint + 2n
        || !gapStimulusSent
        || gapCloseCount !== 1
        || gapCloseEchoCount > 1
        || !gapCloseForwardCompleted
        || terminalCloseCount !== 1
        || terminalCloseEchoCount > 1
        || !terminalCloseForwardCompleted
        || !reconnectAcknowledged
        || replayFrames.size !== 2
        || replayReleaseCount !== 2
        || liveTargetFrameCount === 0
        || (
          withholdTerminalProjection
            ? withheldTerminalProjectionFrameCount !== 3
              || !terminalProjectionSettled
              || withheldTerminalStateVersion === undefined
            : withheldTerminalProjectionFrameCount !== 0
        )
      ) {
        throw new Error("sequence-gap observation did not complete its exact contract");
      }
    },
    dispose() {
      disposed = true;
      if (timer !== undefined) clearTimeout(timer);
      page.off("websocket", observeBrowserSocket);
      if (replayBrowserSocket !== undefined && replayBrowserFrameListener !== undefined) {
        replayBrowserSocket.off("framereceived", replayBrowserFrameListener);
      }
      for (const waiter of replayBrowserFrameWaiters.values()) waiter.resolve();
      replayBrowserFrameWaiters.clear();
      if (!checkpointSettled) {
        checkpointSettled = true;
        resolveCheckpoint();
      }
      if (!replayReadySettled) {
        replayReadySettled = true;
        resolveReplayReady();
      }
      if (!gapCloseSettled) {
        gapCloseSettled = true;
        resolveGapClose();
      }
      if (!terminalProjectionSettled) {
        terminalProjectionSettled = true;
        resolveTerminalProjection();
      }
      if (!terminalCloseSettled) {
        terminalCloseSettled = true;
        resolveTerminalClose();
      }
    },
  };
}

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
    if (connectionOrdinal > 4) {
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
            assertRetainedSubscription(
              subscription,
              initialSubscription,
              checkpoint,
              `expiration connection ${connectionOrdinal}`,
            );
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
          if (connectionOrdinal === 4) recoveredTargetFrames += 1;
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
      if (!Number.isSafeInteger(count) || count < 1 || count > 3) {
        return Promise.reject(new RangeError("resynchronization count must be between 1 and 3"));
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
        reconnectRequestIDs.size !== 3
        || acknowledgedConnections.size !== 3
        || resynchronizedConnections.size !== 3
        || matchingConnectionCount !== 4
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
            assertRetainedSubscription(
              subscription,
              initialSubscription,
              checkpoint,
              "retained replay reconnect",
            );
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
