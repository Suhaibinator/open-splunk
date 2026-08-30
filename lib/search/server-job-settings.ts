import {
  RetainedResultStatus,
  SearchJobRetentionClass,
  SearchJobVisibility,
  type SearchJob,
} from "@/gen/ts/open_splunk/search";
import { ServerFeature } from "@/gen/ts/open_splunk/system_api";
import { durationToMilliseconds, validDate } from "@/lib/api/duration";
import type { OpenSplunkApiClient } from "@/lib/api/open-splunk-client";
import type { ProtobufRequestOptions } from "@/lib/api/protobuf-transport";
import { supportsServerFeature, type SystemBootstrapModel } from "@/lib/api/system-bootstrap";
import { serverSearchJobOriginLabel } from "./server-jobs";

export type JobVisibility = "private" | "everyone";
export type JobRetentionClass =
  | "manual"
  | "shared"
  | "scheduled-report"
  | "scheduled-alert"
  | "triggered-webhook";
export type RetainedResultState = "pending" | "available" | "expired" | "missing" | "corrupt";

export interface RetainedJobReference {
  searchJobId: string | null;
  searchJobExpiresAt: Date | null;
  retainedResultState: RetainedResultState | null;
}

export interface ServerJobSettings {
  id: string;
  stateVersion: bigint;
  visibility: JobVisibility;
  retentionClass: JobRetentionClass;
  lifetimeMs: number;
  expiresAt: Date | null;
  lastAccessedAt: Date | null;
  retainedResultState: RetainedResultState;
  provenance: string;
}

function adaptVisibility(value: SearchJobVisibility): JobVisibility {
  switch (value) {
    case SearchJobVisibility.SEARCH_JOB_VISIBILITY_PRIVATE: return "private";
    case SearchJobVisibility.SEARCH_JOB_VISIBILITY_EVERYONE: return "everyone";
    default: throw new TypeError("The search job returned an unsupported visibility.");
  }
}

function adaptRetentionClass(value: SearchJobRetentionClass): JobRetentionClass {
  switch (value) {
    case SearchJobRetentionClass.SEARCH_JOB_RETENTION_CLASS_MANUAL: return "manual";
    case SearchJobRetentionClass.SEARCH_JOB_RETENTION_CLASS_SHARED: return "shared";
    case SearchJobRetentionClass.SEARCH_JOB_RETENTION_CLASS_SCHEDULED_REPORT: return "scheduled-report";
    case SearchJobRetentionClass.SEARCH_JOB_RETENTION_CLASS_SCHEDULED_ALERT: return "scheduled-alert";
    case SearchJobRetentionClass.SEARCH_JOB_RETENTION_CLASS_TRIGGERED_WEBHOOK: return "triggered-webhook";
    default: throw new TypeError("The search job returned an unsupported retention class.");
  }
}

export function adaptRetainedResultState(value: RetainedResultStatus): RetainedResultState {
  switch (value) {
    case RetainedResultStatus.RETAINED_RESULT_STATUS_PENDING: return "pending";
    case RetainedResultStatus.RETAINED_RESULT_STATUS_AVAILABLE: return "available";
    case RetainedResultStatus.RETAINED_RESULT_STATUS_EXPIRED: return "expired";
    case RetainedResultStatus.RETAINED_RESULT_STATUS_MISSING: return "missing";
    case RetainedResultStatus.RETAINED_RESULT_STATUS_CORRUPT: return "corrupt";
    default: throw new TypeError("The search job returned an unsupported retained-result status.");
  }
}

export function adaptRetainedJobReference(value: {
  retainedResultStatus: RetainedResultStatus;
  searchJobExpiresAt?: Date;
  searchJobId?: string;
}): RetainedJobReference {
  const searchJobId = value.searchJobId?.trim() || null;
  const searchJobExpiresAt = validDate(value.searchJobExpiresAt);
  const retainedResultState = value.retainedResultStatus === RetainedResultStatus.RETAINED_RESULT_STATUS_UNSPECIFIED
    ? null
    : adaptRetainedResultState(value.retainedResultStatus);
  const requiresExpiry = retainedResultState === "available" || retainedResultState === "expired";
  if (
    (searchJobId === null && (searchJobExpiresAt !== null || retainedResultState !== null))
    || (searchJobId !== null && retainedResultState === null)
    || (searchJobId !== null && requiresExpiry && searchJobExpiresAt === null)
  ) {
    throw new TypeError("The server returned incoherent retained-result metadata.");
  }
  return { retainedResultState, searchJobExpiresAt, searchJobId };
}

export function adaptServerJobSettings(job: SearchJob): ServerJobSettings {
  const id = job.searchJobId.trim();
  if (id.length === 0 || job.stateVersion <= 0n) {
    throw new TypeError("The search job settings response is missing its identity or revision.");
  }
  const lifetimeMs = durationToMilliseconds(job.retentionLifetime);
  if (lifetimeMs <= 0) throw new TypeError(`Search job ${id} returned an invalid retention lifetime.`);
  return {
    id,
    stateVersion: job.stateVersion,
    visibility: adaptVisibility(job.visibility),
    retentionClass: adaptRetentionClass(job.retentionClass),
    lifetimeMs,
    expiresAt: validDate(job.expiresAt),
    lastAccessedAt: validDate(job.lastAccessedAt),
    retainedResultState: adaptRetainedResultState(job.retainedResultStatus),
    provenance: serverSearchJobOriginLabel(job.source ?? null),
  };
}

function requireDurableJobs(bootstrap: SystemBootstrapModel): void {
  if (!supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_DURABLE_SEARCH_JOBS)) {
    throw new TypeError("The connected server does not advertise durable search jobs.");
  }
}

function requireJob(response: { searchJob?: SearchJob }, operation: string): SearchJob {
  if (response.searchJob === undefined) throw new TypeError(`The server returned an empty ${operation} response.`);
  return response.searchJob;
}

export async function getServerJobSettings(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  searchJobId: string,
  options?: ProtobufRequestOptions,
): Promise<{ job: SearchJob; settings: ServerJobSettings }> {
  requireDurableJobs(bootstrap);
  const id = searchJobId.trim();
  if (id.length === 0) throw new TypeError("Search job ID is required.");
  const job = requireJob(await client.search.getSettings({ searchJobId: id }, options), "job settings");
  return { job, settings: adaptServerJobSettings(job) };
}

export async function shareServerSearchJob(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  searchJobId: string,
  expectedStateVersion: bigint,
  options?: ProtobufRequestOptions,
): Promise<{ job: SearchJob; settings: ServerJobSettings }> {
  requireDurableJobs(bootstrap);
  const id = searchJobId.trim();
  if (id.length === 0 || expectedStateVersion <= 0n) throw new TypeError("Current search job settings are required before sharing.");
  const job = requireJob(await client.search.share({
    searchJobId: id,
    expectedStateVersion,
  }, options), "share job");
  return { job, settings: adaptServerJobSettings(job) };
}

export async function makeServerSearchJobPrivate(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  searchJobId: string,
  expectedStateVersion: bigint,
  options?: ProtobufRequestOptions,
): Promise<{ job: SearchJob; settings: ServerJobSettings }> {
  requireDurableJobs(bootstrap);
  const id = searchJobId.trim();
  if (id.length === 0 || expectedStateVersion <= 0n) throw new TypeError("Current search job settings are required before updating.");
  const job = requireJob(await client.search.updateSettings({
    searchJobId: id,
    expectedStateVersion,
    visibility: SearchJobVisibility.SEARCH_JOB_VISIBILITY_PRIVATE,
    retentionClass: SearchJobRetentionClass.SEARCH_JOB_RETENTION_CLASS_MANUAL,
  }, options), "job settings update");
  return { job, settings: adaptServerJobSettings(job) };
}
