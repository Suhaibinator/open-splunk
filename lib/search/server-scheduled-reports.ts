import {
  ScheduledSearchOutcome,
  type ScheduledSearchRun,
} from "@/gen/ts/open_splunk/saved_search";
import {
  ScheduleValidationCode,
  ScheduleValidationField,
  ScheduleValidationMode,
} from "@/gen/ts/open_splunk/schedule_api";
import { ServerFeature } from "@/gen/ts/open_splunk/system_api";
import type { OpenSplunkApiClient } from "@/lib/api/open-splunk-client";
import { collectCursorPages } from "@/lib/api/pagination";
import {
  featureNotAdvertised,
  isAdvertisedFeatureRouteUnavailable,
  optionalRouteUnavailable,
  type OptionalFeatureResult,
} from "@/lib/api/optional-feature";
import type { ProtobufRequestOptions } from "@/lib/api/protobuf-transport";
import {
  supportsServerFeature,
  type SystemBootstrapModel,
} from "@/lib/api/system-bootstrap";
import { validDate } from "@/lib/api/duration";
import { adaptRetainedJobReference, type RetainedJobReference, type RetainedResultState } from "./server-job-settings";
import { adaptSavedSearch, type ServerObjectPage, type ServerSavedSearch } from "./server-objects";

export interface ScheduledReportConfiguration {
  enabled: boolean;
  cron: string;
  timezone: string;
  dispatchTtl: string;
}

export interface ServerScheduledReportRun {
  id: string;
  savedSearchId: string;
  scheduledAt: Date;
  startedAt: Date | null;
  finishedAt: Date | null;
  outcome: ScheduledSearchOutcome;
  searchJobId: string | null;
  searchJobExpiresAt: Date | null;
  retainedResultState: RetainedResultState | null;
  skippedOccurrenceCount: number;
}

export interface ScheduledReportOutcomePresentation {
  label: string;
  tone: "success" | "warning" | "error" | "neutral" | "progress";
}

export function isIanaTimezone(value: string): boolean {
  const timezone = value.trim();
  if (timezone.length === 0 || timezone.length > 255) return false;
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: timezone }).format();
    return true;
  } catch {
    return false;
  }
}

export function scheduledReportResultExpired(expiresAt: Date | null, now = new Date()): boolean {
  return expiresAt !== null && expiresAt.valueOf() <= now.valueOf();
}

export type ScheduledReportRetainedResultPresentation =
  | "available"
  | "corrupt"
  | "expired"
  | "missing"
  | "pending";

export function scheduledReportRetainedResultPresentation(
  state: RetainedResultState,
  expiresAt: Date | null,
  now = new Date(),
): ScheduledReportRetainedResultPresentation {
  if (state === "pending") return "pending";
  if (state === "expired") return "expired";
  if (state === "missing") return "missing";
  if (state === "corrupt") return "corrupt";
  return scheduledReportResultExpired(expiresAt, now) ? "expired" : "available";
}

export interface ScheduledReportConfigurationErrors {
  cron?: string;
  timezone?: string;
  dispatchTtl?: string;
}

export function validateScheduledReportConfigurationFields(
  configuration: ScheduledReportConfiguration,
): ScheduledReportConfigurationErrors {
  const errors: ScheduledReportConfigurationErrors = {};
  if (configuration.cron.trim().length === 0) errors.cron = "Cron schedule is required.";
  if (configuration.timezone.trim().length === 0) errors.timezone = "Schedule timezone is required.";
  if (configuration.dispatchTtl.trim().length === 0) errors.dispatchTtl = "Result retention is required.";
  return errors;
}

export function validateScheduledReportConfiguration(configuration: ScheduledReportConfiguration): string | null {
  const errors = validateScheduledReportConfigurationFields(configuration);
  return errors.cron ?? errors.timezone ?? errors.dispatchTtl ?? null;
}

export interface ServerScheduleValidationInput {
  cron: string;
  dispatchTtl: string;
  mode: ScheduleValidationMode;
  timezone: string;
  webhookTtl?: string;
}

export interface ServerScheduleValidationErrors extends ScheduledReportConfigurationErrors {
  webhookTtl?: string;
}

function scheduleValidationFieldKey(
  field: ScheduleValidationField,
): keyof ServerScheduleValidationErrors {
  switch (field) {
    case ScheduleValidationField.SCHEDULE_VALIDATION_FIELD_CRON:
      return "cron";
    case ScheduleValidationField.SCHEDULE_VALIDATION_FIELD_TIMEZONE:
      return "timezone";
    case ScheduleValidationField.SCHEDULE_VALIDATION_FIELD_DISPATCH_TTL:
      return "dispatchTtl";
    case ScheduleValidationField.SCHEDULE_VALIDATION_FIELD_WEBHOOK_TTL:
      return "webhookTtl";
    case ScheduleValidationField.SCHEDULE_VALIDATION_FIELD_UNSPECIFIED:
    case ScheduleValidationField.UNRECOGNIZED:
    default:
      throw new TypeError("The server returned an unsupported schedule-validation field.");
  }
}

function assertScheduleValidationCode(code: ScheduleValidationCode): void {
  switch (code) {
    case ScheduleValidationCode.SCHEDULE_VALIDATION_CODE_REQUIRED:
    case ScheduleValidationCode.SCHEDULE_VALIDATION_CODE_INVALID:
    case ScheduleValidationCode.SCHEDULE_VALIDATION_CODE_TOO_LARGE:
      return;
    case ScheduleValidationCode.SCHEDULE_VALIDATION_CODE_UNSPECIFIED:
    case ScheduleValidationCode.UNRECOGNIZED:
    default:
      throw new TypeError("The server returned an unsupported schedule-validation code.");
  }
}

export async function validateServerSchedule(
  client: OpenSplunkApiClient,
  input: ServerScheduleValidationInput,
  options?: ProtobufRequestOptions,
): Promise<ServerScheduleValidationErrors> {
  if (![
    ScheduleValidationMode.SCHEDULE_VALIDATION_MODE_SCHEDULED_REPORT,
    ScheduleValidationMode.SCHEDULE_VALIDATION_MODE_WEBHOOK_ALERT,
  ].includes(input.mode)) {
    throw new TypeError("A supported schedule-validation mode is required.");
  }
  const response = await client.schedules.validate({
    mode: input.mode,
    cron: input.cron.trim(),
    timezone: input.timezone.trim(),
    dispatchTtl: input.dispatchTtl.trim(),
    webhookTtl: input.webhookTtl?.trim() ?? "",
  }, options);
  if (response.valid !== (response.violations.length === 0)) {
    throw new TypeError("The server returned an inconsistent schedule-validation response.");
  }
  const errors: ServerScheduleValidationErrors = {};
  for (const violation of response.violations) {
    assertScheduleValidationCode(violation.code);
    const key = scheduleValidationFieldKey(violation.field);
    if (errors[key] !== undefined || violation.message.trim().length === 0) {
      throw new TypeError("The server returned an invalid schedule-validation response.");
    }
    errors[key] = violation.message.trim();
  }
  return errors;
}

export function scheduledReportOutcomePresentation(outcome: ScheduledSearchOutcome): ScheduledReportOutcomePresentation {
  switch (outcome) {
    case ScheduledSearchOutcome.SCHEDULED_SEARCH_OUTCOME_RUNNING:
      return { label: "Running", tone: "progress" };
    case ScheduledSearchOutcome.SCHEDULED_SEARCH_OUTCOME_COMPLETED:
      return { label: "Succeeded", tone: "success" };
    case ScheduledSearchOutcome.SCHEDULED_SEARCH_OUTCOME_FAILED:
      return { label: "Failed", tone: "error" };
    case ScheduledSearchOutcome.SCHEDULED_SEARCH_OUTCOME_CANCELED:
      return { label: "Canceled", tone: "warning" };
    case ScheduledSearchOutcome.SCHEDULED_SEARCH_OUTCOME_SKIPPED_OVERLAP:
      return { label: "Skipped overlap", tone: "warning" };
    case ScheduledSearchOutcome.SCHEDULED_SEARCH_OUTCOME_INTERRUPTED:
      return { label: "Interrupted", tone: "error" };
    case ScheduledSearchOutcome.SCHEDULED_SEARCH_OUTCOME_UNSPECIFIED:
      return { label: "Not run", tone: "neutral" };
    case ScheduledSearchOutcome.UNRECOGNIZED:
    default:
      throw new TypeError("The scheduled-report outcome is unsupported.");
  }
}

export function adaptScheduledReportRun(run: ScheduledSearchRun): ServerScheduledReportRun {
  const scheduledAt = validDate(run.scheduledAt);
  let retainedJob: RetainedJobReference;
  try {
    retainedJob = adaptRetainedJobReference(run);
  } catch {
    throw new TypeError("The scheduled-report run response is incomplete or unsupported.");
  }
  if (
    run.scheduledSearchRunId.trim().length === 0
    || run.savedSearchId.trim().length === 0
    || scheduledAt === null
    || run.outcome === ScheduledSearchOutcome.SCHEDULED_SEARCH_OUTCOME_UNSPECIFIED
    || run.outcome === ScheduledSearchOutcome.UNRECOGNIZED
  ) {
    throw new TypeError("The scheduled-report run response is incomplete or unsupported.");
  }
  return {
    id: run.scheduledSearchRunId,
    savedSearchId: run.savedSearchId,
    scheduledAt,
    startedAt: validDate(run.startedAt),
    finishedAt: validDate(run.finishedAt),
    outcome: run.outcome,
    ...retainedJob,
    skippedOccurrenceCount: run.skippedOccurrenceCount,
  };
}

function unavailable(bootstrap: SystemBootstrapModel): boolean {
  return !supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_SCHEDULED_SEARCHES);
}

export async function setServerSavedSearchSchedule(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  savedSearch: ServerSavedSearch,
  configuration: ScheduledReportConfiguration,
  options?: ProtobufRequestOptions,
): Promise<OptionalFeatureResult<ServerSavedSearch>> {
  if (unavailable(bootstrap)) return featureNotAdvertised;
  const validationError = validateScheduledReportConfiguration(configuration);
  if (validationError !== null) throw new TypeError(validationError);
  if (savedSearch.version <= 0n) throw new RangeError("Expected saved-search version must be positive.");
  try {
    const response = await client.savedSearches.setSchedule({
      savedSearchId: savedSearch.id,
      expectedVersion: savedSearch.version,
      expectedScheduleVersion: savedSearch.schedule?.configVersion ?? 0n,
      schedule: {
        enabled: configuration.enabled,
        cron: configuration.cron.trim(),
        timezone: configuration.timezone.trim(),
        dispatchTtl: configuration.dispatchTtl.trim(),
        configVersion: savedSearch.schedule?.configVersion ?? 0n,
      },
    }, options);
    if (response.savedSearch === undefined) throw new TypeError("The server returned an empty saved search.");
    return { status: "available", value: adaptSavedSearch(response.savedSearch) };
  } catch (error) {
    if (isAdvertisedFeatureRouteUnavailable(error)) return optionalRouteUnavailable;
    throw error;
  }
}

export interface RunServerSavedSearchResult {
  runId: string;
  searchJobId: string;
}

export async function runServerSavedSearch(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  savedSearchId: string,
  options?: ProtobufRequestOptions,
): Promise<OptionalFeatureResult<RunServerSavedSearchResult>> {
  if (unavailable(bootstrap)) return featureNotAdvertised;
  const id = savedSearchId.trim();
  if (id.length === 0) throw new TypeError("Saved search ID is required.");
  try {
    const response = await client.savedSearches.run({ savedSearchId: id }, options);
    if (response.scheduledSearchRunId.trim().length === 0 || response.searchJobId.trim().length === 0) {
      throw new TypeError("The server returned an incomplete scheduled-report run.");
    }
    return {
      status: "available",
      value: { runId: response.scheduledSearchRunId, searchJobId: response.searchJobId },
    };
  } catch (error) {
    if (isAdvertisedFeatureRouteUnavailable(error)) return optionalRouteUnavailable;
    throw error;
  }
}

export interface ListServerScheduledReportRunsOptions extends ProtobufRequestOptions {
  pageSize?: number;
  pageToken?: string;
  maximumPages?: number;
}

export async function listServerScheduledReportRuns(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  savedSearchId: string,
  options: ListServerScheduledReportRunsOptions = {},
): Promise<OptionalFeatureResult<ServerObjectPage<ServerScheduledReportRun>>> {
  if (unavailable(bootstrap)) return featureNotAdvertised;
  const id = savedSearchId.trim();
  if (id.length === 0) throw new TypeError("Saved search ID is required.");
  const pageSize = options.pageSize ?? Math.min(25, bootstrap.limits.maximumPageSize || 25);
  const maximumPages = options.maximumPages ?? 4;
  if (!Number.isInteger(pageSize) || pageSize < 1) throw new RangeError("Page size must be positive.");
  if (!Number.isInteger(maximumPages) || maximumPages < 1) throw new RangeError("Maximum pages must be positive.");
  try {
    const collected = await collectCursorPages<ServerScheduledReportRun>({
      maximumPages,
      pageToken: options.pageToken?.trim() || undefined,
      label: "Scheduled report history",
      fetchPage: async ({ pageToken, includeTotalSize }) => {
        const response = await client.savedSearches.listRuns({
          savedSearchId: id,
          page: { pageSize, pageToken, includeTotalSize },
        }, options);
        return { items: response.runs.map(adaptScheduledReportRun), page: response.page };
      },
    });
    return { status: "available", value: collected };
  } catch (error) {
    if (isAdvertisedFeatureRouteUnavailable(error)) return optionalRouteUnavailable;
    throw error;
  }
}
