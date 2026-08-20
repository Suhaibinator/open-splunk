import { SearchJobState } from "@/gen/ts/open_splunk/search";

import { formatMediumDateTime } from "../_components/date-format";

export function formatActivityDate(value: Date | null): string {
  return formatMediumDateTime(value, "Not recorded");
}

const ACTIVITY_TIME = new Intl.DateTimeFormat(undefined, {
  hour: "numeric",
  minute: "2-digit",
  second: "2-digit",
});

export function formatActivityTime(value: Date | null): string {
  if (value === null || Number.isNaN(value.valueOf())) return "Not refreshed";
  return ACTIVITY_TIME.format(value);
}

export { formatDurationMilliseconds as formatActivityDuration } from "@/lib/api/duration";

export function formatActivityCount(value: bigint): string {
  return value.toLocaleString();
}

export function searchJobStateLabel(state: SearchJobState): string {
  if (state === SearchJobState.SEARCH_JOB_STATE_QUEUED) return "Queued";
  if (state === SearchJobState.SEARCH_JOB_STATE_PARSING) return "Parsing";
  if (state === SearchJobState.SEARCH_JOB_STATE_PLANNING) return "Planning";
  if (state === SearchJobState.SEARCH_JOB_STATE_RUNNING) return "Running";
  if (state === SearchJobState.SEARCH_JOB_STATE_FINALIZING) return "Finalizing";
  if (state === SearchJobState.SEARCH_JOB_STATE_COMPLETED) return "Completed";
  if (state === SearchJobState.SEARCH_JOB_STATE_FAILED) return "Failed";
  if (state === SearchJobState.SEARCH_JOB_STATE_CANCELED) return "Canceled";
  if (state === SearchJobState.SEARCH_JOB_STATE_EXPIRED) return "Expired";
  return "Unknown";
}

export function searchJobStateClass(state: SearchJobState): string {
  if (state === SearchJobState.SEARCH_JOB_STATE_COMPLETED) return "complete";
  if (
    state === SearchJobState.SEARCH_JOB_STATE_FAILED
    || state === SearchJobState.SEARCH_JOB_STATE_EXPIRED
  ) return "failed";
  if (state === SearchJobState.SEARCH_JOB_STATE_CANCELED) return "neutral";
  if (
    state === SearchJobState.SEARCH_JOB_STATE_QUEUED
    || state === SearchJobState.SEARCH_JOB_STATE_PARSING
    || state === SearchJobState.SEARCH_JOB_STATE_PLANNING
  ) return "warning";
  return "running";
}
