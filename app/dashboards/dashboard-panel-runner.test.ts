import assert from "node:assert/strict";
import test from "node:test";

import {
  SearchJob,
  SearchJobState,
} from "@/gen/ts/open_splunk/search";

import {
  DashboardSearchJobWaitTimeoutError,
  dashboardPanelWaitTimeoutMs,
  waitForDashboardSearchJob,
} from "./dashboard-panel-runner";

function searchJob(searchJobId: string, state: SearchJobState): SearchJob {
  return SearchJob.fromPartial({ searchJobId, state });
}

test("dashboard panel wait timeout bounds advertised values and supplies a fallback", () => {
  assert.equal(dashboardPanelWaitTimeoutMs(30_000), 35_000);
  assert.equal(dashboardPanelWaitTimeoutMs(120_000), 132_000);
  assert.equal(dashboardPanelWaitTimeoutMs(1), 6_000);
  assert.equal(dashboardPanelWaitTimeoutMs(60 * 60 * 1_000), 930_000);
  assert.equal(dashboardPanelWaitTimeoutMs(0), 132_000);
  assert.equal(dashboardPanelWaitTimeoutMs(Number.NaN), 132_000);
  assert.equal(dashboardPanelWaitTimeoutMs(Number.POSITIVE_INFINITY), 132_000);
});

test("terminal initial dashboard jobs return without sleeping or polling", async () => {
  const initial = searchJob("job-terminal", SearchJobState.SEARCH_JOB_STATE_COMPLETED);
  let sleeps = 0;
  let requests = 0;
  const result = await waitForDashboardSearchJob(initial, {
    defaultSearchTimeoutMs: 30_000,
    signal: new AbortController().signal,
    now: () => 0,
    sleep: async () => { sleeps += 1; },
    getJob: async () => {
      requests += 1;
      return initial;
    },
  });
  assert.equal(result, initial);
  assert.equal(sleeps, 0);
  assert.equal(requests, 0);
});

test("dashboard polling is sequential and uses capped exponential backoff", async () => {
  const delays: number[] = [];
  const requestedIDs: string[] = [];
  const responses = [
    searchJob("job-backoff", SearchJobState.SEARCH_JOB_STATE_RUNNING),
    searchJob("job-backoff", SearchJobState.SEARCH_JOB_STATE_FINALIZING),
    searchJob("job-backoff", SearchJobState.SEARCH_JOB_STATE_RUNNING),
    searchJob("job-backoff", SearchJobState.SEARCH_JOB_STATE_RUNNING),
    searchJob("job-backoff", SearchJobState.SEARCH_JOB_STATE_COMPLETED),
  ];
  let activeRequests = 0;
  let maximumActiveRequests = 0;
  const result = await waitForDashboardSearchJob(
    searchJob("job-backoff", SearchJobState.SEARCH_JOB_STATE_QUEUED),
    {
      defaultSearchTimeoutMs: 120_000,
      signal: new AbortController().signal,
      now: () => 0,
      sleep: async (milliseconds) => { delays.push(milliseconds); },
      getJob: async (searchJobId) => {
        requestedIDs.push(searchJobId);
        activeRequests += 1;
        maximumActiveRequests = Math.max(maximumActiveRequests, activeRequests);
        const response = responses.shift();
        activeRequests -= 1;
        if (response === undefined) {
          throw new Error("The dashboard polling test ran out of responses.");
        }
        return response;
      },
    },
  );
  assert.equal(result.state, SearchJobState.SEARCH_JOB_STATE_COMPLETED);
  assert.deepEqual(delays, [500, 1_000, 2_000, 4_000, 5_000]);
  assert.deepEqual(requestedIDs, Array.from({ length: 5 }, () => "job-backoff"));
  assert.equal(maximumActiveRequests, 1);
});

test("dashboard polling rejects a different search job identity", async () => {
  await assert.rejects(
    waitForDashboardSearchJob(
      searchJob("job-authority", SearchJobState.SEARCH_JOB_STATE_RUNNING),
      {
        defaultSearchTimeoutMs: 30_000,
        signal: new AbortController().signal,
        now: () => 0,
        sleep: async () => undefined,
        getJob: async () => searchJob("job-substitution", SearchJobState.SEARCH_JOB_STATE_COMPLETED),
      },
    ),
    /different search job/,
  );
});

test("dashboard polling times out before issuing a post-deadline request", async () => {
  const timeoutMs = dashboardPanelWaitTimeoutMs(1);
  const delays: number[] = [];
  let requests = 0;
  const promise = waitForDashboardSearchJob(
    searchJob("job-timeout", SearchJobState.SEARCH_JOB_STATE_RUNNING),
    {
      defaultSearchTimeoutMs: 1,
      signal: new AbortController().signal,
      now: () => 0,
      sleep: async (milliseconds) => { delays.push(milliseconds); },
      getJob: async () => {
        requests += 1;
        return searchJob("job-timeout", SearchJobState.SEARCH_JOB_STATE_RUNNING);
      },
    },
  );
  await assert.rejects(promise, (error: unknown) => {
    assert.ok(error instanceof DashboardSearchJobWaitTimeoutError);
    assert.equal(error.code, "DASHBOARD_SEARCH_JOB_WAIT_TIMEOUT");
    assert.equal(error.searchJobId, "job-timeout");
    assert.equal(error.timeoutMs, timeoutMs);
    return true;
  });
  assert.deepEqual(delays, [500, 1_000, 2_000, 2_500]);
  assert.equal(requests, 3);
});

test("dashboard polling honors caller cancellation before doing work", async () => {
  const controller = new AbortController();
  const reason = new Error("test cancellation");
  controller.abort(reason);
  let requests = 0;
  let sleeps = 0;
  await assert.rejects(
    waitForDashboardSearchJob(
      searchJob("job-canceled", SearchJobState.SEARCH_JOB_STATE_RUNNING),
      {
        defaultSearchTimeoutMs: 30_000,
        signal: controller.signal,
        sleep: async () => { sleeps += 1; },
        getJob: async () => {
          requests += 1;
          return searchJob("job-canceled", SearchJobState.SEARCH_JOB_STATE_COMPLETED);
        },
      },
    ),
    reason,
  );
  assert.equal(sleeps, 0);
  assert.equal(requests, 0);
});

test("dashboard polling forwards cancellation to an in-flight request", async () => {
  const controller = new AbortController();
  const reason = new Error("cancel in-flight dashboard panel");
  let requestSignal: AbortSignal | undefined;
  let markRequestStarted: (() => void) | undefined;
  const requestStarted = new Promise<void>((resolve) => {
    markRequestStarted = resolve;
  });
  const promise = waitForDashboardSearchJob(
    searchJob("job-in-flight", SearchJobState.SEARCH_JOB_STATE_RUNNING),
    {
      defaultSearchTimeoutMs: 30_000,
      signal: controller.signal,
      now: () => 0,
      sleep: async () => undefined,
      getJob: (_searchJobId, signal) => {
        requestSignal = signal;
        markRequestStarted?.();
        return new Promise<SearchJob>(() => undefined);
      },
    },
  );
  await requestStarted;
  controller.abort(reason);
  await assert.rejects(promise, (error: unknown) => error === reason);
  assert.equal(requestSignal?.aborted, true);
  assert.equal(requestSignal?.reason, reason);
});

test("dashboard polling consumes request time before considering another request", async () => {
  const timeoutMs = dashboardPanelWaitTimeoutMs(1);
  let clock = 0;
  let requests = 0;
  const delays: number[] = [];
  await assert.rejects(
    waitForDashboardSearchJob(
      searchJob("job-slow-request", SearchJobState.SEARCH_JOB_STATE_RUNNING),
      {
        defaultSearchTimeoutMs: 1,
        signal: new AbortController().signal,
        now: () => clock,
        sleep: async (milliseconds) => { delays.push(milliseconds); },
        getJob: async () => {
          requests += 1;
          clock = timeoutMs - 500;
          return searchJob("job-slow-request", SearchJobState.SEARCH_JOB_STATE_RUNNING);
        },
      },
    ),
    DashboardSearchJobWaitTimeoutError,
  );
  assert.equal(requests, 1);
  assert.deepEqual(delays, [500, 500]);
});
