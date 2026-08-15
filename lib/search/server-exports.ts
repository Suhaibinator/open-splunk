import {
  CsvHeaderMode,
  ExportJobState,
  JsonIntegerEncoding,
  type ExportJob,
  type ExportProgress,
} from "@/gen/ts/open_splunk/v1/export";
import { ServerFeature } from "@/gen/ts/open_splunk/v1/system_api";
import type { OpenSplunkApiClient } from "@/lib/api/open-splunk-client";
import {
  SearchWebSocketClient,
  exportJobTarget,
} from "@/lib/api/search-websocket";
import {
  featureNotAdvertised,
  isAdvertisedFeatureRouteUnavailable,
  optionalRouteUnavailable,
  type OptionalFeatureResult,
} from "@/lib/api/optional-feature";
import {
  HttpError,
  type ProtobufRequestOptions,
} from "@/lib/api/protobuf-transport";
import {
  supportsServerFeature,
  type SystemBootstrapModel,
} from "@/lib/api/system-bootstrap";
import { canonicalBoundedServerText } from "@/lib/search/server-text";

export type ServerExportFormat = "csv" | "json-lines";

const MAXIMUM_UINT64 = (1n << 64n) - 1n;
const NANOSECONDS_PER_SECOND = 1_000_000_000n;
const MAXIMUM_COMPILER_VERSION_BYTES = 128;

export type ExportProgressRevisionState = {
  revision: bigint;
  epoch: bigint;
  progress: ExportProgress;
} | null;

export type ExportProgressSource =
  | { kind: "authoritative"; envelopeRevision: bigint }
  | { kind: "live" }
  | { kind: "terminal"; envelopeRevision: bigint };

export type ExportProgressDecision =
  | {
    kind: "apply";
    state: NonNullable<ExportProgressRevisionState>;
  }
  | {
    kind: "ignore";
    reason: "duplicate" | "lower";
    state: ExportProgressRevisionState;
  }
  | {
    kind: "recover";
    reason: "invalid-revision" | "invalid-watermark" | "unversioned" | "watermark-regression";
    state: ExportProgressRevisionState;
  };

interface AuthoritativeExportSnapshot {
  job: ExportJob;
  progressRevision: NonNullable<ExportProgressRevisionState>;
  compilerVersion: string;
}

function canonicalExportCompilerVersion(value: unknown): string {
  // An absent field on a pre-provenance wire decodes as the empty string.
  // Once observed, empty and non-empty identities are equally immutable.
  const compilerVersion = canonicalBoundedServerText(
    value,
    MAXIMUM_COMPILER_VERSION_BYTES,
    true,
  );
  if (compilerVersion === null) {
    throw new TypeError("The export snapshot has an invalid compiler compatibility identity.");
  }
  return compilerVersion;
}

function isVersionedExportRevision(revision: bigint): boolean {
  return typeof revision === "bigint"
    && revision > 0n
    && revision <= MAXIMUM_UINT64;
}

function cloneExportProgress(progress: ExportProgress): ExportProgress {
  return {
    ...progress,
    elapsed: progress.elapsed === undefined ? undefined : { ...progress.elapsed },
    updatedAt: progress.updatedAt === undefined
      ? undefined
      : new Date(progress.updatedAt.getTime()),
  };
}

function exportProgressElapsedNanoseconds(progress: ExportProgress): bigint | null {
  const elapsed = progress.elapsed;
  if (
    elapsed === undefined
    || elapsed.seconds < 0n
    || elapsed.nanos < 0
    || elapsed.nanos >= Number(NANOSECONDS_PER_SECOND)
  ) {
    return null;
  }
  return elapsed.seconds * NANOSECONDS_PER_SECOND + BigInt(elapsed.nanos);
}

function exportProgressWatermark(progress: ExportProgress): {
  updatedAt: number;
  elapsed: bigint;
} | null {
  const updatedAt = progress.updatedAt?.valueOf() ?? Number.NaN;
  const elapsed = exportProgressElapsedNanoseconds(progress);
  if (!Number.isFinite(updatedAt) || elapsed === null) return null;
  return { updatedAt, elapsed };
}

function optionalNumberOrder(left: number | undefined, right: number | undefined): -1 | 0 | 1 {
  if (Object.is(left, right)) return 0;
  if (left === undefined) return -1;
  if (right === undefined) return 1;
  if (!Number.isFinite(left) || !Number.isFinite(right)) return -1;
  return left < right ? -1 : 1;
}

/**
 * Orders versionless WebSocket export-progress frames by their server timestamp
 * and monotonic counters. REST/terminal envelopes also carry a state revision;
 * both domains must advance before a candidate may replace visible progress.
 */
export function reconcileExportProgress(
  current: ExportProgressRevisionState,
  progress: ExportProgress,
  source: ExportProgressSource,
): ExportProgressDecision {
  const envelopeRevision = source.kind === "live"
    ? current?.revision
    : source.envelopeRevision;
  if (
    source.kind !== "live"
    && (
      envelopeRevision === undefined
      || !isVersionedExportRevision(envelopeRevision)
    )
  ) {
    return { kind: "recover", reason: "invalid-revision", state: current };
  }
  if (source.kind === "live" && current === null) {
    return { kind: "recover", reason: "unversioned", state: current };
  }
  const candidateWatermark = exportProgressWatermark(progress);
  if (candidateWatermark === null) {
    return { kind: "recover", reason: "invalid-watermark", state: current };
  }
  if (current === null) {
    return {
      kind: "apply",
      state: {
        revision: envelopeRevision!,
        epoch: 1n,
        progress: cloneExportProgress(progress),
      },
    };
  }
  if (envelopeRevision! < current.revision) {
    return { kind: "ignore", reason: "lower", state: current };
  }

  const currentWatermark = exportProgressWatermark(current.progress);
  if (currentWatermark === null) {
    return { kind: "recover", reason: "invalid-watermark", state: current };
  }
  const percentOrder = optionalNumberOrder(
    progress.percentComplete,
    current.progress.percentComplete,
  );
  const regressed = candidateWatermark.updatedAt < currentWatermark.updatedAt
    || progress.rowsWritten < current.progress.rowsWritten
    || progress.bytesWritten < current.progress.bytesWritten
    || candidateWatermark.elapsed < currentWatermark.elapsed
    || percentOrder < 0;
  if (regressed) {
    return source.kind === "live"
      ? { kind: "ignore", reason: "lower", state: current }
      : { kind: "recover", reason: "watermark-regression", state: current };
  }

  const stable = candidateWatermark.updatedAt === currentWatermark.updatedAt
    && progress.rowsWritten === current.progress.rowsWritten
    && progress.bytesWritten === current.progress.bytesWritten
    && candidateWatermark.elapsed === currentWatermark.elapsed
    && percentOrder === 0;
  if (stable && envelopeRevision === current.revision) {
    return { kind: "ignore", reason: "duplicate", state: current };
  }
  return {
    kind: "apply",
    state: {
      revision: envelopeRevision!,
      epoch: current.epoch + 1n,
      progress: cloneExportProgress(progress),
    },
  };
}

function reconcileAuthoritativeExportSnapshot(
  current: AuthoritativeExportSnapshot | null,
  candidate: ExportJob,
  expectedExportJobId: string,
): AuthoritativeExportSnapshot {
  if (candidate.exportJobId !== expectedExportJobId) {
    throw new TypeError("The export snapshot belongs to a different export job.");
  }
  if (!isVersionedExportRevision(candidate.stateVersion)) {
    throw new TypeError("The export snapshot has an invalid state revision.");
  }
  if (candidate.progress === undefined) {
    throw new TypeError("The export snapshot omitted progress.");
  }
  const compilerVersion = canonicalExportCompilerVersion(candidate.compilerVersion);
  if (current !== null) {
    if (compilerVersion !== current.compilerVersion) {
      throw new Error("The export snapshot changed its compiler compatibility identity.");
    }
    if (candidate.stateVersion < current.job.stateVersion) {
      throw new Error("The authoritative export snapshot is older than the applied state.");
    }
    if (
      candidate.stateVersion === current.job.stateVersion
      && candidate.state !== current.job.state
    ) {
      throw new Error("The authoritative export state changed without advancing its revision.");
    }
  }
  const progressDecision = reconcileExportProgress(
    current?.progressRevision ?? null,
    candidate.progress,
    { kind: "authoritative", envelopeRevision: candidate.stateVersion },
  );
  if (progressDecision.kind === "recover") {
    throw new Error(`The authoritative export progress is inconsistent (${progressDecision.reason}).`);
  }
  if (progressDecision.kind === "ignore" && progressDecision.reason === "lower") {
    throw new Error("The authoritative export progress is older than the applied progress.");
  }
  const progressRevision = progressDecision.kind === "apply"
    ? progressDecision.state
    : current?.progressRevision;
  if (progressRevision === undefined) {
    throw new Error("The authoritative export snapshot could not establish progress.");
  }
  return {
    job: {
      ...candidate,
      compilerVersion,
      progress: progressRevision.progress,
    },
    progressRevision,
    compilerVersion,
  };
}

export interface CreateServerExportOptions extends ProtobufRequestOptions {
  searchJobId: string;
  format: ServerExportFormat;
  columns?: readonly string[];
  rowLimit?: bigint;
  byteLimit?: bigint;
  csvHeaderMode?: "field-names" | "display-names" | "none";
  jsonIntegerEncoding?: "safe-number" | "string";
  includeTypeMetadata?: boolean;
}

function csvHeaderMode(mode: CreateServerExportOptions["csvHeaderMode"]): CsvHeaderMode {
  if (mode === "display-names") return CsvHeaderMode.CSV_HEADER_MODE_DISPLAY_NAMES;
  if (mode === "none") return CsvHeaderMode.CSV_HEADER_MODE_NONE;
  return CsvHeaderMode.CSV_HEADER_MODE_FIELD_NAMES;
}

function jsonIntegerEncoding(
  encoding: CreateServerExportOptions["jsonIntegerEncoding"],
): JsonIntegerEncoding {
  return encoding === "string"
    ? JsonIntegerEncoding.JSON_INTEGER_ENCODING_STRING
    : JsonIntegerEncoding.JSON_INTEGER_ENCODING_NUMBER_WHEN_SAFE;
}

function formatFeature(format: ServerExportFormat): ServerFeature {
  return format === "csv"
    ? ServerFeature.SERVER_FEATURE_EXPORT_CSV
    : ServerFeature.SERVER_FEATURE_EXPORT_JSON_LINES;
}

function assertPositiveLimit(value: bigint | undefined, label: string): void {
  if (value !== undefined && value <= 0n) {
    throw new RangeError(`${label} must be positive when supplied.`);
  }
}

export async function createServerExport(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  options: CreateServerExportOptions,
): Promise<OptionalFeatureResult<ExportJob>> {
  if (!supportsServerFeature(bootstrap, formatFeature(options.format))) {
    return featureNotAdvertised;
  }
  const searchJobId = options.searchJobId.trim();
  if (searchJobId.length === 0) throw new TypeError("Search job ID is required.");
  assertPositiveLimit(options.rowLimit, "Export row limit");
  assertPositiveLimit(options.byteLimit, "Export byte limit");
  if (
    options.rowLimit !== undefined
    && bootstrap.limits.maximumExportRows > 0n
    && options.rowLimit > bootstrap.limits.maximumExportRows
  ) {
    throw new RangeError("Export row limit exceeds the server maximum.");
  }
  if (
    options.byteLimit !== undefined
    && bootstrap.limits.maximumExportBytes > 0n
    && options.byteLimit > bootstrap.limits.maximumExportBytes
  ) {
    throw new RangeError("Export byte limit exceeds the server maximum.");
  }
  const formatOptions = options.format === "csv"
    ? {
      $case: "csv" as const,
      value: { headerMode: csvHeaderMode(options.csvHeaderMode) },
    }
    : {
      $case: "jsonLines" as const,
      value: {
        integerEncoding: jsonIntegerEncoding(options.jsonIntegerEncoding),
        includeTypeMetadata: options.includeTypeMetadata ?? false,
      },
    };
  try {
    const response = await client.exports.create({
      definition: {
        searchJobId,
        columns: [...new Set(options.columns?.map((column) => column.trim()).filter(Boolean) ?? [])],
        rowLimit: options.rowLimit,
        byteLimit: options.byteLimit,
        formatOptions,
      },
      // The current handler explicitly rejects client-generated idempotency IDs.
      clientRequestId: undefined,
    }, options);
    if (response.exportJob === undefined) {
      throw new TypeError("The server returned an empty export job.");
    }
    return { status: "available", value: response.exportJob };
  } catch (error) {
    if (isAdvertisedFeatureRouteUnavailable(error)) return optionalRouteUnavailable;
    throw error;
  }
}

export function exportIsTerminal(job: ExportJob): boolean {
  return job.state === ExportJobState.EXPORT_JOB_STATE_COMPLETED
    || job.state === ExportJobState.EXPORT_JOB_STATE_FAILED
    || job.state === ExportJobState.EXPORT_JOB_STATE_CANCELED
    || job.state === ExportJobState.EXPORT_JOB_STATE_EXPIRED;
}

export function exportCanDownload(job: ExportJob): boolean {
  return job.state === ExportJobState.EXPORT_JOB_STATE_COMPLETED && job.artifact !== undefined;
}

export interface WaitForServerExportOptions extends ProtobufRequestOptions {
  onUpdate?: (job: ExportJob) => void;
  pollIntervalMs?: number;
  websocketBaseUrl?: string;
  websocketPath?: string | null;
  websocketRecoveryIntervalMs?: number;
}

function abortError(signal: AbortSignal): Error {
  if (signal.reason instanceof Error) return signal.reason;
  return new DOMException("The operation was aborted.", "AbortError");
}

async function wait(milliseconds: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) throw abortError(signal);
  await new Promise<void>((resolve, reject) => {
    const cleanup = () => signal?.removeEventListener("abort", onAbort);
    const timeout = globalThis.setTimeout(() => {
      cleanup();
      resolve();
    }, milliseconds);
    const onAbort = () => {
      globalThis.clearTimeout(timeout);
      cleanup();
      reject(signal ? abortError(signal) : new DOMException("The operation was aborted.", "AbortError"));
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

export async function waitForServerExport(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  exportJob: ExportJob,
  options: WaitForServerExportOptions = {},
): Promise<OptionalFeatureResult<ExportJob>> {
  const supported = supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_EXPORT_CSV)
    || supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_EXPORT_JSON_LINES);
  if (!supported) return featureNotAdvertised;
  if (exportJob.exportJobId.trim().length === 0) throw new TypeError("Export job ID is required.");
  if (options.signal?.aborted) throw abortError(options.signal);
  const initialSnapshot = reconcileAuthoritativeExportSnapshot(
    null,
    exportJob,
    exportJob.exportJobId,
  );
  if (exportIsTerminal(initialSnapshot.job)) {
    return { status: "available", value: initialSnapshot.job };
  }
  const websocketPath = options.websocketPath ?? bootstrap.searchWebsocketPath;
  if (websocketPath?.trim()) {
    const recoveryIntervalMs = options.websocketRecoveryIntervalMs ?? 10_000;
    if (!Number.isFinite(recoveryIntervalMs) || recoveryIntervalMs < 1_000) {
      throw new RangeError("Export WebSocket recovery interval must be at least 1000 milliseconds.");
    }
    try {
      const job = await waitForServerExportWebSocket(
        client,
        initialSnapshot.job,
        websocketPath,
        recoveryIntervalMs,
        options,
      );
      return { status: "available", value: job };
    } catch (error) {
      if (isAdvertisedFeatureRouteUnavailable(error)) return optionalRouteUnavailable;
      throw error;
    }
  }
  const pollIntervalMs = options.pollIntervalMs ?? 500;
  if (!Number.isFinite(pollIntervalMs) || pollIntervalMs < 100) {
    throw new RangeError("Export polling interval must be at least 100 milliseconds.");
  }
  let snapshot = initialSnapshot;
  try {
    while (!exportIsTerminal(snapshot.job)) {
      // Export state is sequential; each response determines whether another poll is needed.
      // eslint-disable-next-line no-await-in-loop
      await wait(pollIntervalMs, options.signal);
      // eslint-disable-next-line no-await-in-loop
      const response = await client.exports.get({
        exportJobId: initialSnapshot.job.exportJobId,
        issueDownloadGrant: false,
      }, options);
      if (response.exportJob === undefined) {
        throw new TypeError("The server returned an empty export job.");
      }
      snapshot = reconcileAuthoritativeExportSnapshot(
        snapshot,
        response.exportJob,
        initialSnapshot.job.exportJobId,
      );
      options.onUpdate?.(snapshot.job);
    }
    return { status: "available", value: snapshot.job };
  } catch (error) {
    if (isAdvertisedFeatureRouteUnavailable(error)) return optionalRouteUnavailable;
    throw error;
  }
}

async function getAuthoritativeExportJob(
  client: OpenSplunkApiClient,
  exportJobId: string,
  options: ProtobufRequestOptions,
): Promise<ExportJob> {
  const response = await client.exports.get({
    exportJobId,
    issueDownloadGrant: false,
  }, options);
  if (response.exportJob === undefined) {
    throw new TypeError("The server returned an empty export job.");
  }
  return response.exportJob;
}

function waitForServerExportWebSocket(
  client: OpenSplunkApiClient,
  initialJob: ExportJob,
  websocketPath: string,
  recoveryIntervalMs: number,
  options: WaitForServerExportOptions,
): Promise<ExportJob> {
  return new Promise<ExportJob>((resolve, reject) => {
    const signal = options.signal;
    const target = exportJobTarget(initialJob.exportJobId);
    const socket = new SearchWebSocketClient({
      path: websocketPath,
      baseUrl: options.websocketBaseUrl,
    });
    let job = initialJob;
    let progressRevision: ExportProgressRevisionState = null;
    const initialSnapshot = reconcileAuthoritativeExportSnapshot(
      null,
      initialJob,
      initialJob.exportJobId,
    );
    progressRevision = initialSnapshot.progressRevision;
    job = initialSnapshot.job;
    let compilerVersion = initialSnapshot.compilerVersion;
    let settled = false;
    let recoveryTimer: ReturnType<typeof globalThis.setTimeout> | undefined;
    let recoveryTimerImmediate = false;
    let recoveryCycle: Promise<ExportJob> | null = null;
    let immediateRecoveryPending = false;
    let subscriptionId: string | null = null;
    const cleanups: Array<() => void> = [];

    function publishAuthoritative(nextJob: ExportJob) {
      const reconciled = reconcileAuthoritativeExportSnapshot(
        {
          job,
          progressRevision: progressRevision!,
          compilerVersion,
        },
        nextJob,
        initialJob.exportJobId,
      );
      progressRevision = reconciled.progressRevision;
      job = reconciled.job;
      compilerVersion = reconciled.compilerVersion;
      options.onUpdate?.(job);
    }

    function cleanup() {
      if (recoveryTimer !== undefined) globalThis.clearTimeout(recoveryTimer);
      for (const dispose of cleanups) dispose();
      if (subscriptionId !== null) socket.unsubscribe(subscriptionId);
      socket.dispose();
    }

    function finish() {
      if (settled) return;
      settled = true;
      cleanup();
      resolve(job);
    }

    function fail(error: unknown) {
      if (settled) return;
      settled = true;
      cleanup();
      reject(error);
    }

    function scheduleRecovery(delay = recoveryIntervalMs) {
      if (settled || signal?.aborted) return;
      const immediate = delay === 0;
      if (recoveryCycle !== null) {
        if (immediate) immediateRecoveryPending = true;
        return;
      }
      if (recoveryTimer !== undefined) {
        if (!immediate || recoveryTimerImmediate) return;
        globalThis.clearTimeout(recoveryTimer);
      }
      recoveryTimerImmediate = immediate;
      recoveryTimer = globalThis.setTimeout(() => {
        recoveryTimer = undefined;
        recoveryTimerImmediate = false;
        void recoverFromRest().catch(() => undefined);
      }, delay);
    }

    function recoverFromRest(): Promise<ExportJob> {
      if (recoveryCycle !== null) return recoveryCycle;
      let scheduleNext = false;
      const requestedProgressEpoch = progressRevision?.epoch ?? 0n;
      const cycle = (async () => {
        try {
          const recovered = await getAuthoritativeExportJob(
            client,
            initialJob.exportJobId,
            options,
          );
          if (settled || signal?.aborted) {
            throw signal ? abortError(signal) : new DOMException("The operation was aborted.", "AbortError");
          }
          if ((progressRevision?.epoch ?? 0n) !== requestedProgressEpoch) {
            throw new Error("Live export progress advanced while the authoritative snapshot was loading.");
          }
          publishAuthoritative(recovered);
          if (exportIsTerminal(job)) finish();
          else scheduleNext = true;
          return recovered;
        } catch (error) {
          if (signal?.aborted) {
            fail(abortError(signal));
            throw error;
          }
          if (
            error instanceof HttpError
            && error.status >= 400
            && error.status < 500
            && error.status !== 408
            && error.status !== 429
          ) {
            fail(error);
            throw error;
          }
          scheduleNext = true;
          throw error;
        }
      })().finally(() => {
        if (recoveryCycle === cycle) recoveryCycle = null;
        if (settled || signal?.aborted) {
          immediateRecoveryPending = false;
          return;
        }
        if (immediateRecoveryPending) {
          immediateRecoveryPending = false;
          scheduleRecovery(0);
        } else if (scheduleNext) {
          scheduleRecovery();
        }
      });
      recoveryCycle = cycle;
      return cycle;
    }

    const abortListener = () => fail(signal ? abortError(signal) : new DOMException("The operation was aborted.", "AbortError"));
    signal?.addEventListener("abort", abortListener, { once: true });
    cleanups.push(() => signal?.removeEventListener("abort", abortListener));

    cleanups.push(socket.onEvent((event) => {
      const eventExportJobId = event.target?.target?.$case === "exportJobId"
        ? event.target.target.value
        : event.payload?.$case === "exportStateChanged"
          ? event.payload.value.exportJobId
          : event.payload?.$case === "exportTerminal"
            ? event.payload.value.exportJobId
            : null;
      if (settled || eventExportJobId !== initialJob.exportJobId) return;
      switch (event.payload?.$case) {
        case "exportProgress": {
          const decision = reconcileExportProgress(
            progressRevision,
            event.payload.value,
            { kind: "live" },
          );
          if (decision.kind === "apply") {
            progressRevision = decision.state;
            job = { ...job, progress: decision.state.progress };
            options.onUpdate?.(job);
          } else if (decision.kind === "recover") {
            scheduleRecovery(0);
          }
          scheduleRecovery();
          break;
        }
        case "exportStateChanged": {
          const change = event.payload.value;
          if (!isVersionedExportRevision(change.stateVersion)) {
            scheduleRecovery(0);
            break;
          }
          if (change.stateVersion > job.stateVersion) {
            job = { ...job, state: change.state, stateVersion: change.stateVersion };
            if (
              progressRevision !== null
              && progressRevision.revision < change.stateVersion
            ) {
              progressRevision = { ...progressRevision, revision: change.stateVersion };
            }
            options.onUpdate?.(job);
          } else if (
            change.stateVersion === job.stateVersion
            && change.state !== job.state
          ) {
            scheduleRecovery(0);
          }
          if (exportIsTerminal(job)) scheduleRecovery(0);
          else scheduleRecovery();
          break;
        }
        case "exportTerminal": {
          const terminal = event.payload.value;
          if (!isVersionedExportRevision(terminal.stateVersion)) {
            scheduleRecovery(0);
            break;
          }
          if (
            terminal.stateVersion === job.stateVersion
            && terminal.state !== job.state
          ) {
            scheduleRecovery(0);
            break;
          }
          if (terminal.stateVersion >= job.stateVersion) {
            let terminalProgress = progressRevision?.progress ?? job.progress;
            if (terminal.finalProgress === undefined) {
              scheduleRecovery(0);
              break;
            }
            const decision = reconcileExportProgress(
              progressRevision,
              terminal.finalProgress,
              { kind: "terminal", envelopeRevision: terminal.stateVersion },
            );
            if (
              decision.kind === "recover"
              || (decision.kind === "ignore" && decision.reason === "lower")
            ) {
              // Artifact and failure metadata share the terminal envelope with
              // final progress. If that progress cannot be ordered safely,
              // publish none of the frame and converge from REST as one unit.
              scheduleRecovery(0);
              break;
            }
            if (decision.kind === "apply") {
              progressRevision = decision.state;
              terminalProgress = decision.state.progress;
            }
            if (
              progressRevision !== null
              && progressRevision.revision < terminal.stateVersion
            ) {
              progressRevision = { ...progressRevision, revision: terminal.stateVersion };
            }
            job = {
              ...job,
              state: terminal.state,
              stateVersion: terminal.stateVersion,
              progress: terminalProgress,
              artifact: terminal.artifact,
              failure: terminal.failure,
            };
            options.onUpdate?.(job);
          }
          scheduleRecovery(0);
          break;
        }
      }
    }));
    cleanups.push(socket.onError(() => scheduleRecovery(0)));
    cleanups.push(socket.onProtocolError(() => scheduleRecovery(0)));
    cleanups.push(socket.onSequenceGap((gap) => {
      if (gap.target.target?.$case === "exportJobId"
        && gap.target.target.value === initialJob.exportJobId) scheduleRecovery(0);
    }));
    cleanups.push(socket.onResynchronizationRequired(async (notice) => {
      if (
        notice.target.target?.$case !== "exportJobId"
        || notice.target.target.value !== initialJob.exportJobId
        || settled
      ) return;
      await recoverFromRest();
      notice.acknowledge();
    }));
    subscriptionId = socket.subscribe(target, { includePreviews: false });
    scheduleRecovery(0);
  });
}

export async function cancelServerExport(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  exportJobId: string,
  options?: ProtobufRequestOptions,
): Promise<OptionalFeatureResult<ExportJob>> {
  const supported = supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_EXPORT_CSV)
    || supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_EXPORT_JSON_LINES);
  if (!supported) return featureNotAdvertised;
  const id = exportJobId.trim();
  if (id.length === 0) throw new TypeError("Export job ID is required.");
  try {
    const response = await client.exports.cancel({
      exportJobId: id,
      // The current handler rejects cancellation reasons.
      reason: undefined,
    }, options);
    if (response.exportJob === undefined) {
      throw new TypeError("The server returned an empty export job.");
    }
    return { status: "available", value: response.exportJob };
  } catch (error) {
    if (isAdvertisedFeatureRouteUnavailable(error)) return optionalRouteUnavailable;
    throw error;
  }
}

export interface DownloadedExportArtifact {
  blob: Blob;
  fileName: string;
  mediaType: string;
  sizeBytes: bigint;
  rowCount: bigint;
}

export interface DownloadServerExportOptions extends ProtobufRequestOptions {
  apiBaseUrl?: string;
  fetch?: typeof globalThis.fetch;
}

function servingOrigin(apiBaseUrl: string | undefined): URL {
  if (apiBaseUrl && typeof window !== "undefined") return new URL(apiBaseUrl, window.location.href);
  if (apiBaseUrl) return new URL(apiBaseUrl);
  if (typeof window === "undefined") {
    throw new Error("An API base URL is required outside a browser.");
  }
  return new URL(window.location.origin);
}

function validatedDownloadUrl(downloadPath: string, apiBaseUrl: string | undefined): string {
  const base = servingOrigin(apiBaseUrl);
  const url = new URL(downloadPath, base);
  if (
    url.origin !== base.origin
    || url.pathname !== "/api/v1/search/exports/download"
    || url.search.length > 0
    || url.hash.length > 0
    || url.username.length > 0
    || url.password.length > 0
  ) {
    throw new Error("The server returned an invalid export download path.");
  }
  return url.toString();
}

/**
 * Issues and immediately redeems a one-time download grant. The token is kept
 * inside this call and is never returned, persisted, or placed in a URL.
 */
export async function downloadServerExport(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  exportJobId: string,
  options: DownloadServerExportOptions = {},
): Promise<OptionalFeatureResult<DownloadedExportArtifact>> {
  const supported = supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_EXPORT_CSV)
    || supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_EXPORT_JSON_LINES);
  if (!supported) return featureNotAdvertised;
  const id = exportJobId.trim();
  if (id.length === 0) throw new TypeError("Export job ID is required.");
  let grantResponse;
  try {
    grantResponse = await client.exports.get({
      exportJobId: id,
      issueDownloadGrant: true,
    }, options);
  } catch (error) {
    if (isAdvertisedFeatureRouteUnavailable(error)) return optionalRouteUnavailable;
    throw error;
  }
  const job = grantResponse.exportJob;
  const grant = grantResponse.downloadGrant;
  const artifact = job?.artifact;
  if (
    job === undefined
    || !exportCanDownload(job)
    || artifact === undefined
    || grant === undefined
    || grant.expiresAt === undefined
  ) {
    throw new Error("The export artifact is not ready to download.");
  }
  const url = validatedDownloadUrl(grant.downloadPath, options.apiBaseUrl);
  const fetchImplementation = options.fetch ?? globalThis.fetch.bind(globalThis);
  const response = await fetchImplementation(url, {
    method: "GET",
    headers: { Authorization: `Bearer ${grant.downloadToken}` },
    cache: "no-store",
    credentials: "include",
    signal: options.signal,
  });
  if (!response.ok) {
    const responseBody = await response.text();
    throw new HttpError({
      status: response.status,
      statusText: response.statusText,
      message: responseBody || `Export download failed with HTTP ${response.status}.`,
      url,
      responseBody: responseBody || undefined,
    });
  }
  const blob = await response.blob();
  return {
    status: "available",
    value: {
      blob,
      fileName: artifact.fileName,
      mediaType: artifact.mediaType || response.headers.get("Content-Type") || "application/octet-stream",
      sizeBytes: artifact.sizeBytes,
      rowCount: artifact.rowCount,
    },
  };
}
