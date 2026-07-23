import {
  SearchJobOrigin,
  SearchJobState,
  type SearchDefinition,
  type SearchJob,
  type SearchJobSource,
  type SearchProgress,
} from "@/gen/ts/open_splunk/v1/search";
import { ServerFeature } from "@/gen/ts/open_splunk/v1/system_api";
import type { OpenSplunkApiClient } from "@/lib/api/open-splunk-client";
import {
  featureNotAdvertised,
  isOptionalRouteUnavailable,
  optionalRouteUnavailable,
  type OptionalFeatureResult,
} from "@/lib/api/optional-feature";
import { recordNextPageToken } from "@/lib/api/pagination";
import type { ProtobufRequestOptions } from "@/lib/api/protobuf-transport";
import {
  supportsServerFeature,
  type SystemBootstrapModel,
} from "@/lib/api/system-bootstrap";

const LISTABLE_STATES = new Set<SearchJobState>([
  SearchJobState.SEARCH_JOB_STATE_QUEUED,
  SearchJobState.SEARCH_JOB_STATE_PARSING,
  SearchJobState.SEARCH_JOB_STATE_PLANNING,
  SearchJobState.SEARCH_JOB_STATE_RUNNING,
  SearchJobState.SEARCH_JOB_STATE_COMPLETED,
  SearchJobState.SEARCH_JOB_STATE_FAILED,
  SearchJobState.SEARCH_JOB_STATE_CANCELED,
  SearchJobState.SEARCH_JOB_STATE_EXPIRED,
]);

const CANCELABLE_STATES = new Set<SearchJobState>([
  SearchJobState.SEARCH_JOB_STATE_QUEUED,
  SearchJobState.SEARCH_JOB_STATE_PARSING,
  SearchJobState.SEARCH_JOB_STATE_PLANNING,
  SearchJobState.SEARCH_JOB_STATE_RUNNING,
]);

export interface ServerSearchJob {
  id: string;
  stateVersion: bigint;
  state: SearchJobState;
  definition: SearchDefinition;
  source: SearchJobSource | null;
  effectiveIndexScope: string[];
  progress: SearchProgress | null;
  warningCount: number;
  failureMessage: string | null;
  failureRetryable: boolean;
  createdAt: Date | null;
  startedAt: Date | null;
  finishedAt: Date | null;
  expiresAt: Date | null;
  resultsTruncated: boolean;
}

export interface ServerSearchJobPage {
  items: ServerSearchJob[];
  nextPageToken: string | null;
  totalSize: bigint | null;
  totalSizeExact: boolean;
  complete: boolean;
}

export interface ListServerSearchJobsOptions extends ProtobufRequestOptions {
  states?: readonly SearchJobState[];
  appId?: string;
  text?: string;
  pageSize?: number;
  pageToken?: string;
  includeTotalSize?: boolean;
  maximumPages?: number;
}

function validDate(value: Date | undefined): Date | null {
  return value !== undefined && !Number.isNaN(value.valueOf()) ? new Date(value) : null;
}

function requireDefinition(job: SearchJob): SearchDefinition {
  if (job.definition === undefined || job.definition.spl.trim().length === 0) {
    throw new TypeError(`Search job ${job.searchJobId || "response"} did not include a usable search definition.`);
  }
  return job.definition;
}

export function adaptServerSearchJob(job: SearchJob): ServerSearchJob {
  const id = job.searchJobId.trim();
  if (id.length === 0) throw new TypeError("The search job response did not include an ID.");
  if (!LISTABLE_STATES.has(job.state)) {
    throw new TypeError(`Search job ${id} returned an unsupported state.`);
  }
  if (job.source === undefined || job.progress === undefined) {
    throw new TypeError(`Search job ${id} did not include its source and progress projection.`);
  }
  const createdAt = validDate(job.createdAt);
  if (createdAt === null) throw new TypeError(`Search job ${id} did not include a valid creation time.`);
  return {
    id,
    stateVersion: job.stateVersion,
    state: job.state,
    definition: requireDefinition(job),
    source: job.source,
    effectiveIndexScope: [...job.effectiveIndexScope],
    progress: job.progress,
    warningCount: job.warnings.length,
    failureMessage: job.failure?.message.trim() || null,
    failureRetryable: job.failure?.retryable ?? false,
    createdAt,
    startedAt: validDate(job.startedAt),
    finishedAt: validDate(job.finishedAt),
    expiresAt: validDate(job.expiresAt),
    resultsTruncated: job.resultsTruncated,
  };
}

function normalizeStates(states: readonly SearchJobState[] | undefined): SearchJobState[] {
  const unique = [...new Set(states ?? [])];
  for (const state of unique) {
    if (!LISTABLE_STATES.has(state)) {
      throw new RangeError("Search job state filter contains a state unsupported by the list contract.");
    }
  }
  return unique.toSorted((left, right) => left - right);
}

function boundedText(
  value: string | undefined,
  maximumBytes: number,
  label: string,
  preserveEmpty = false,
): string | undefined {
  const trimmed = value?.trim();
  const normalized = trimmed === "" && !preserveEmpty ? undefined : trimmed;
  if (normalized !== undefined && new TextEncoder().encode(normalized).byteLength > maximumBytes) {
    throw new RangeError(`${label} cannot exceed ${maximumBytes.toLocaleString()} UTF-8 bytes.`);
  }
  return normalized;
}

export async function listServerSearchJobs(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  options: ListServerSearchJobsOptions = {},
): Promise<OptionalFeatureResult<ServerSearchJobPage>> {
  if (!supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_SEARCH)) {
    return featureNotAdvertised;
  }
  const states = normalizeStates(options.states);
  const appId = boundedText(options.appId, 255, "App filter", true);
  const text = boundedText(options.text, 1_024, "Search text filter");
  const requestedPageSize = options.pageSize ?? 15;
  const maximumPages = options.maximumPages ?? 1;
  const includeTotalSize = options.includeTotalSize ?? true;
  if (!Number.isSafeInteger(requestedPageSize) || requestedPageSize <= 0) throw new RangeError("Search job page size must be a positive integer.");
  if (!Number.isSafeInteger(maximumPages) || maximumPages <= 0) throw new RangeError("Maximum search job pages must be a positive integer.");
  const pageSize = Math.max(1, Math.min(
    requestedPageSize,
    bootstrap.limits.maximumPageSize || 15,
    15,
  ));

  let pageToken = boundedText(options.pageToken, 4_096, "Search job page token");
  const seenTokens = new Set<string>();
  if (pageToken !== undefined) seenTokens.add(pageToken);
  const seenJobIds = new Set<string>();
  const items: ServerSearchJob[] = [];
  let previousJob: ServerSearchJob | null = null;
  let totalSize: bigint | null = null;
  let totalSizeExact = false;

  try {
    for (let pageIndex = 0; pageIndex < maximumPages; pageIndex += 1) {
      // Opaque cursors make each request depend on the preceding response.
      // oxlint-disable-next-line no-await-in-loop
      const response = await client.search.list({
        page: {
          pageSize,
          pageToken,
          includeTotalSize,
        },
        stateFilters: states,
        appIdFilter: appId,
        textFilter: text,
      }, options);
      if (response.page === undefined) throw new TypeError("The server returned search jobs without page metadata.");
      if (includeTotalSize && (response.page.totalSize === undefined || !response.page.totalSizeExact)) {
        throw new TypeError("The server did not return the requested exact search job count.");
      }
      totalSize = response.page.totalSize ?? null;
      totalSizeExact = response.page.totalSizeExact;
      for (const rawJob of response.searchJobs) {
        const job = adaptServerSearchJob(rawJob);
        if (seenJobIds.has(job.id)) throw new TypeError(`The server repeated search job ${job.id} across pages.`);
        if (
          previousJob !== null
          && (
            previousJob.createdAt === null
            || job.createdAt === null
            || previousJob.createdAt.valueOf() < job.createdAt.valueOf()
            || (
              previousJob.createdAt.valueOf() === job.createdAt.valueOf()
              && previousJob.id <= job.id
            )
          )
        ) {
          throw new TypeError("The server returned search jobs outside the documented newest-first order.");
        }
        seenJobIds.add(job.id);
        items.push(job);
        previousJob = job;
      }
      pageToken = recordNextPageToken(seenTokens, response.page.nextPageToken, "Search jobs") ?? undefined;
      if (pageToken === undefined) break;
    }
    return {
      status: "available",
      value: {
        items,
        nextPageToken: pageToken ?? null,
        totalSize,
        totalSizeExact,
        complete: pageToken === undefined,
      },
    };
  } catch (error) {
    if (isOptionalRouteUnavailable(error)) return optionalRouteUnavailable;
    throw error;
  }
}

export async function cancelServerSearchJob(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  searchJobId: string,
  options?: ProtobufRequestOptions,
): Promise<OptionalFeatureResult<ServerSearchJob>> {
  if (!supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_SEARCH)) {
    return featureNotAdvertised;
  }
  const id = searchJobId.trim();
  if (id.length === 0) throw new TypeError("Search job ID is required.");
  const response = await client.search.cancel({
    searchJobId: id,
    reason: undefined,
  }, options);
  if (response.searchJob === undefined) throw new TypeError("The server returned an empty cancellation response.");
  return { status: "available", value: adaptServerSearchJob(response.searchJob) };
}

export async function rerunServerSearchJob(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  job: ServerSearchJob,
  options?: ProtobufRequestOptions,
): Promise<OptionalFeatureResult<ServerSearchJob>> {
  if (!supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_SEARCH)) {
    return featureNotAdvertised;
  }
  const response = await client.search.create({
    definition: job.definition,
    source: undefined,
    options: undefined,
    clientRequestId: undefined,
  }, options);
  if (response.searchJob === undefined) throw new TypeError("The server returned an empty rerun response.");
  return { status: "available", value: adaptServerSearchJob(response.searchJob) };
}

export function serverSearchJobCanCancel(job: ServerSearchJob): boolean {
  return CANCELABLE_STATES.has(job.state);
}

export function serverSearchJobOriginLabel(source: SearchJobSource | null): string {
  if (source?.origin === SearchJobOrigin.SEARCH_JOB_ORIGIN_SAVED_SEARCH) return "Saved search";
  if (source?.origin === SearchJobOrigin.SEARCH_JOB_ORIGIN_HISTORY_RERUN) return "History rerun";
  if (source?.origin === SearchJobOrigin.SEARCH_JOB_ORIGIN_DASHBOARD) return "Dashboard";
  if (source?.origin === SearchJobOrigin.SEARCH_JOB_ORIGIN_API) return "API";
  return "Ad hoc";
}
