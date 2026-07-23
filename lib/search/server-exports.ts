import {
  CsvHeaderMode,
  ExportFormat,
  ExportJobState,
  JsonIntegerEncoding,
  type ExportJob,
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

export type ServerExportFormat = "csv" | "json-lines";

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
  if (exportIsTerminal(exportJob)) return { status: "available", value: exportJob };
  const websocketPath = options.websocketPath ?? bootstrap.searchWebsocketPath;
  if (websocketPath?.trim()) {
    const recoveryIntervalMs = options.websocketRecoveryIntervalMs ?? 10_000;
    if (!Number.isFinite(recoveryIntervalMs) || recoveryIntervalMs < 1_000) {
      throw new RangeError("Export WebSocket recovery interval must be at least 1000 milliseconds.");
    }
    try {
      const job = await waitForServerExportWebSocket(
        client,
        exportJob,
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
  let job = exportJob;
  try {
    while (!exportIsTerminal(job)) {
      // Export state is sequential; each response determines whether another poll is needed.
      // eslint-disable-next-line no-await-in-loop
      await wait(pollIntervalMs, options.signal);
      // eslint-disable-next-line no-await-in-loop
      const response = await client.exports.get({
        exportJobId: job.exportJobId,
        issueDownloadGrant: false,
      }, options);
      if (response.exportJob === undefined) {
        throw new TypeError("The server returned an empty export job.");
      }
      job = response.exportJob;
      options.onUpdate?.(job);
    }
    return { status: "available", value: job };
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
    let settled = false;
    let recoveryTimer: ReturnType<typeof globalThis.setTimeout> | undefined;
    let recoveryRequest: Promise<ExportJob> | null = null;
    let subscriptionId: string | null = null;
    const cleanups: Array<() => void> = [];

    function publish(nextJob: ExportJob) {
      job = nextJob;
      options.onUpdate?.(nextJob);
    }

    function cleanup() {
      if (recoveryTimer !== undefined) globalThis.clearTimeout(recoveryTimer);
      for (const dispose of cleanups) dispose();
      if (subscriptionId !== null) socket.unsubscribe(subscriptionId);
      socket.dispose();
    }

    function finish(nextJob: ExportJob) {
      if (settled) return;
      settled = true;
      publish(nextJob);
      cleanup();
      resolve(nextJob);
    }

    function fail(error: unknown) {
      if (settled) return;
      settled = true;
      cleanup();
      reject(error);
    }

    function scheduleRecovery(delay = recoveryIntervalMs) {
      if (settled || signal?.aborted) return;
      if (recoveryTimer !== undefined) globalThis.clearTimeout(recoveryTimer);
      recoveryTimer = globalThis.setTimeout(() => {
        recoveryTimer = undefined;
        void recoverFromRest();
      }, delay);
    }

    async function fetchAuthoritative(): Promise<ExportJob> {
      if (recoveryRequest !== null) return recoveryRequest;
      recoveryRequest = getAuthoritativeExportJob(client, initialJob.exportJobId, options)
        .finally(() => {
          recoveryRequest = null;
        });
      return recoveryRequest;
    }

    async function recoverFromRest(onRecovered?: () => void) {
      try {
        const recovered = await fetchAuthoritative();
        if (settled || signal?.aborted) return;
        publish(recovered);
        onRecovered?.();
        if (exportIsTerminal(recovered)) finish(recovered);
        else scheduleRecovery();
      } catch (error) {
        if (signal?.aborted) {
          fail(abortError(signal));
          return;
        }
        if (
          error instanceof HttpError
          && error.status >= 400
          && error.status < 500
          && error.status !== 408
          && error.status !== 429
        ) {
          fail(error);
          return;
        }
        scheduleRecovery();
      }
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
        case "exportProgress":
          publish({ ...job, progress: event.payload.value });
          scheduleRecovery();
          break;
        case "exportStateChanged": {
          const change = event.payload.value;
          if (change.stateVersion >= job.stateVersion) {
            publish({ ...job, state: change.state, stateVersion: change.stateVersion });
          }
          if (exportIsTerminal(job)) scheduleRecovery(0);
          else scheduleRecovery();
          break;
        }
        case "exportTerminal": {
          const terminal = event.payload.value;
          if (terminal.stateVersion >= job.stateVersion) {
            publish({
              ...job,
              state: terminal.state,
              stateVersion: terminal.stateVersion,
              progress: terminal.finalProgress ?? job.progress,
              artifact: terminal.artifact,
              failure: terminal.failure,
            });
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
      await recoverFromRest(() => notice.acknowledge());
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

export function exportFormatFromJob(job: ExportJob): ServerExportFormat | null {
  if (job.format === ExportFormat.EXPORT_FORMAT_CSV) return "csv";
  if (job.format === ExportFormat.EXPORT_FORMAT_JSON_LINES) return "json-lines";
  return null;
}
