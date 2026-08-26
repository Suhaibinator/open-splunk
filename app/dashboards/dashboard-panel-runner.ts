import {
  type SearchJob,
  SearchJobState,
} from "@/gen/ts/open_splunk/search";

const INITIAL_POLL_DELAY_MS = 500;
const MAXIMUM_POLL_DELAY_MS = 5_000;
const FALLBACK_SEARCH_TIMEOUT_MS = 2 * 60 * 1_000;
const MINIMUM_SEARCH_TIMEOUT_MS = 1_000;
const MAXIMUM_SEARCH_TIMEOUT_MS = 15 * 60 * 1_000;
const MINIMUM_WAIT_GRACE_MS = 5_000;
const MAXIMUM_WAIT_GRACE_MS = 30_000;

const TERMINAL_SEARCH_JOB_STATES = new Set<SearchJobState>([
  SearchJobState.SEARCH_JOB_STATE_COMPLETED,
  SearchJobState.SEARCH_JOB_STATE_FAILED,
  SearchJobState.SEARCH_JOB_STATE_CANCELED,
  SearchJobState.SEARCH_JOB_STATE_EXPIRED,
]);

export type DashboardSearchJobGetter = (
  searchJobId: string,
  signal: AbortSignal,
) => Promise<SearchJob>;

export type DashboardPanelSleep = (
  milliseconds: number,
  signal: AbortSignal,
) => Promise<void>;

export interface DashboardSearchJobWaitOptions {
  readonly defaultSearchTimeoutMs: number;
  readonly signal: AbortSignal;
  readonly getJob: DashboardSearchJobGetter;
  readonly now?: () => number;
  readonly sleep?: DashboardPanelSleep;
}

export class DashboardSearchJobWaitTimeoutError extends Error {
  public readonly code = "DASHBOARD_SEARCH_JOB_WAIT_TIMEOUT";
  public readonly searchJobId: string;
  public readonly timeoutMs: number;

  public constructor(searchJobId: string, timeoutMs: number) {
    super(`Dashboard search job ${searchJobId} did not finish within ${timeoutMs}ms.`);
    this.name = "DashboardSearchJobWaitTimeoutError";
    this.searchJobId = searchJobId;
    this.timeoutMs = timeoutMs;
  }
}

/**
 * Gives a dashboard search enough time to reach the backend's advertised
 * execution timeout, plus a small bounded allowance for queueing/finalization.
 */
export function dashboardPanelWaitTimeoutMs(defaultSearchTimeoutMs: number): number {
  const advertised = Number.isFinite(defaultSearchTimeoutMs) && defaultSearchTimeoutMs > 0
    ? Math.min(
      MAXIMUM_SEARCH_TIMEOUT_MS,
      Math.max(MINIMUM_SEARCH_TIMEOUT_MS, Math.ceil(defaultSearchTimeoutMs)),
    )
    : FALLBACK_SEARCH_TIMEOUT_MS;
  const grace = Math.min(
    MAXIMUM_WAIT_GRACE_MS,
    Math.max(MINIMUM_WAIT_GRACE_MS, Math.ceil(advertised / 10)),
  );
  return advertised + grace;
}

/**
 * Polls one dashboard-created search job with bounded, sequential requests.
 * The logical sleep budget prevents a faulty injected sleep implementation
 * from turning the loop into unbounded work, while the monotonic clock also
 * accounts for time spent in backend requests.
 */
export async function waitForDashboardSearchJob(
  initialJob: SearchJob,
  options: DashboardSearchJobWaitOptions,
): Promise<SearchJob> {
  const searchJobId = validSearchJobID(initialJob);
  throwIfAborted(options.signal);
  if (TERMINAL_SEARCH_JOB_STATES.has(initialJob.state)) return initialJob;

  const now = options.now ?? monotonicNow;
  const sleep = options.sleep ?? abortableSleep;
  const waitTimeoutMs = dashboardPanelWaitTimeoutMs(options.defaultSearchTimeoutMs);
  const startedAt = readClock(now);
  const timeoutError = new DashboardSearchJobWaitTimeoutError(searchJobId, waitTimeoutMs);
  const boundedWait = createBoundedWaitSignal(options.signal, waitTimeoutMs, timeoutError);
  let logicalElapsedMs = 0;
  let pollDelayMs = INITIAL_POLL_DELAY_MS;
  let currentJob = initialJob;

  try {
    while (!TERMINAL_SEARCH_JOB_STATES.has(currentJob.state)) {
      throwIfAborted(boundedWait.signal);
      const elapsedBeforeSleep = effectiveElapsedMilliseconds(now, startedAt, logicalElapsedMs);
      const remainingBeforeSleep = waitTimeoutMs - elapsedBeforeSleep;
      if (remainingBeforeSleep <= 0) throw timeoutError;

      const sleepMilliseconds = Math.min(pollDelayMs, remainingBeforeSleep);
      // Polling must remain sequential so every delay and request consumes the
      // same bounded deadline and only one backend request can be in flight.
      // eslint-disable-next-line no-await-in-loop
      await awaitWithAbort(
        sleep(sleepMilliseconds, boundedWait.signal),
        boundedWait.signal,
      );
      logicalElapsedMs = elapsedBeforeSleep + sleepMilliseconds;
      throwIfAborted(boundedWait.signal);

      // Check the deadline after sleeping and immediately before issuing the
      // request. In particular, a final partial sleep consumes the remaining
      // budget without allowing one request to start after the deadline.
      if (effectiveElapsedMilliseconds(now, startedAt, logicalElapsedMs) >= waitTimeoutMs) {
        throw timeoutError;
      }

      // Each response determines whether another poll is necessary.
      // eslint-disable-next-line no-await-in-loop
      const nextJob = await awaitWithAbort(
        options.getJob(searchJobId, boundedWait.signal),
        boundedWait.signal,
      );
      throwIfAborted(boundedWait.signal);
      if (validSearchJobID(nextJob) !== searchJobId) {
        throw new TypeError("The dashboard poll returned a different search job.");
      }
      if (effectiveElapsedMilliseconds(now, startedAt, logicalElapsedMs) >= waitTimeoutMs) {
        throw timeoutError;
      }
      currentJob = nextJob;
      pollDelayMs = Math.min(pollDelayMs * 2, MAXIMUM_POLL_DELAY_MS);
    }
  } finally {
    boundedWait.dispose();
  }

  return currentJob;
}

interface BoundedWaitSignal {
  readonly signal: AbortSignal;
  readonly dispose: () => void;
}

function createBoundedWaitSignal(
  callerSignal: AbortSignal,
  timeoutMs: number,
  timeoutError: DashboardSearchJobWaitTimeoutError,
): BoundedWaitSignal {
  const controller = new AbortController();
  const relayCallerAbort = () => {
    controller.abort(
      callerSignal.reason
      ?? new DOMException("The dashboard search wait was aborted.", "AbortError"),
    );
  };
  callerSignal.addEventListener("abort", relayCallerAbort, { once: true });
  const timer = globalThis.setTimeout(() => controller.abort(timeoutError), timeoutMs);
  if (callerSignal.aborted) relayCallerAbort();

  return {
    signal: controller.signal,
    dispose: () => {
      globalThis.clearTimeout(timer);
      callerSignal.removeEventListener("abort", relayCallerAbort);
    },
  };
}

function awaitWithAbort<T>(operation: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) return Promise.reject(abortReason(signal));

  return new Promise<T>((resolve, reject) => {
    const onAbort = () => {
      signal.removeEventListener("abort", onAbort);
      reject(abortReason(signal));
    };
    signal.addEventListener("abort", onAbort, { once: true });
    operation.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", onAbort);
        reject(error);
      },
    );
  });
}

function validSearchJobID(job: SearchJob | null | undefined): string {
  const searchJobId = job?.searchJobId;
  if (
    typeof searchJobId !== "string"
    || searchJobId.length === 0
    || searchJobId.trim() !== searchJobId
  ) {
    throw new TypeError("The dashboard search job identity is invalid.");
  }
  return searchJobId;
}

function monotonicNow(): number {
  return globalThis.performance?.now() ?? Date.now();
}

function readClock(now: () => number): number {
  const value = now();
  if (!Number.isFinite(value)) {
    throw new TypeError("The dashboard polling clock returned an invalid value.");
  }
  return value;
}

function effectiveElapsedMilliseconds(
  now: () => number,
  startedAt: number,
  logicallyWaitedMs: number,
): number {
  const clockElapsed = readClock(now) - startedAt;
  if (clockElapsed < 0) {
    throw new TypeError("The dashboard polling clock moved backwards.");
  }
  return Math.max(clockElapsed, logicallyWaitedMs);
}

function throwIfAborted(signal: AbortSignal): void {
  if (!signal.aborted) return;
  throw abortReason(signal);
}

function abortReason(signal: AbortSignal): unknown {
  return signal.reason ?? new DOMException("The dashboard search wait was aborted.", "AbortError");
}

function abortableSleep(milliseconds: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(signal.reason ?? new DOMException("The dashboard search wait was aborted.", "AbortError"));
      return;
    }
    const onAbort = () => {
      globalThis.clearTimeout(timer);
      reject(signal.reason ?? new DOMException("The dashboard search wait was aborted.", "AbortError"));
    };
    const timer = globalThis.setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, milliseconds);
    signal.addEventListener("abort", onAbort, { once: true });
  });
}
