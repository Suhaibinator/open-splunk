import type { ReactNode } from "react";

import { SearchJobState } from "@/gen/ts/open_splunk/v1/search";

export function formatActivityDate(value: Date | null): string {
  if (value === null || Number.isNaN(value.valueOf())) return "Not recorded";
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(value);
}

export function formatActivityTime(value: Date | null): string {
  if (value === null || Number.isNaN(value.valueOf())) return "Not refreshed";
  return new Intl.DateTimeFormat(undefined, {
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
  }).format(value);
}

export function formatActivityDuration(milliseconds: number): string {
  if (!Number.isFinite(milliseconds) || milliseconds <= 0) return "0 ms";
  if (milliseconds < 1_000) return `${Math.round(milliseconds)} ms`;
  return `${(milliseconds / 1_000).toFixed(milliseconds < 10_000 ? 2 : 1)} s`;
}

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

interface ActivityStateProps {
  kind: "loading" | "unavailable" | "error" | "empty";
  title: string;
  message: string;
  action?: ReactNode;
}

export function ActivityState({ kind, title, message, action }: ActivityStateProps) {
  return (
    <div className={`backend-resource-state backend-resource-state--${kind}`} role={kind === "error" ? "alert" : "status"}>
      <span aria-hidden="true">{kind === "loading" ? "↻" : kind === "error" ? "!" : kind === "empty" ? "∅" : "i"}</span>
      <div><strong>{title}</strong><p>{message}</p></div>
      {action}
    </div>
  );
}
