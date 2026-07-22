import type { PointerEvent, ReactNode } from "react";

import { SearchJobState, type SearchJob } from "@/gen/ts/open_splunk/v1/search";
import { DEMO_EVENTS, type DemoEvent, type DemoScalar } from "@/lib/demo/search-data";

import type { JobPhase, ResultTab } from "./model";

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export function syntaxTokens(query: string): ReactNode[] {
  const tokenPattern = /(\b(?:index|host|source|sourcetype|level|status|trace_id|message|path)\b(?=\s*=)|\b(?:sort|stats|timechart|table|where|eval|fields|rename|head|top|rare|rex|spath|bin|transaction|join|map|subsearch)\b|\b(?:count|avg|sum|min|max|p95|dc|values|tonumber|replace)\b|\b(?:AND|OR|NOT)\b|"(?:\\.|[^"\\])*"|\|)/gi;
  const unsupported = new Set(["transaction", "join", "map", "subsearch"]);
  const parts = query.split(tokenPattern).filter((part) => part !== undefined && part.length > 0);

  let sourceOffset = 0;
  return parts.map((part) => {
    sourceOffset += part.length;
    const lower = part.toLowerCase();
    let className = "spl-plain";
    if (part === "|") className = "spl-pipe";
    else if (part.startsWith('"')) className = "spl-string";
    else if (["and", "or", "not"].includes(lower)) className = "spl-boolean";
    else if (unsupported.has(lower)) className = "spl-error-token";
    else if (/^(sort|stats|timechart|table|where|eval|fields|rename|head|top|rare|rex|spath|bin)$/i.test(part)) {
      className = "spl-command";
    } else if (/^(count|avg|sum|min|max|p95|dc|values|tonumber|replace)$/i.test(part)) {
      className = "spl-function";
    } else if (/^(index|host|source|sourcetype|level|status|trace_id|message|path)$/i.test(part)) {
      className = "spl-field";
    }
    return <span className={className} key={`${sourceOffset}-${part}`}>{part}</span>;
  });
}

export function eventCountForQuery(query: string): number {
  const lowered = query.toLowerCase();
  if (lowered.includes("trace_id=")) return 18;
  if (lowered.includes("connection refused")) return 391;
  if (lowered.includes("status>=500") || lowered.includes("status >= 500")) return 812;
  if (lowered.includes("level=error") && lowered.includes("level=warn")) return 3923;
  if (lowered.includes("level=error")) return 1432;
  if (lowered.includes("level=warn")) return 2491;
  return 12_846;
}

export function filteredDemoEvents(query: string): DemoEvent[] {
  const lowered = query.toLowerCase();
  let events = DEMO_EVENTS;
  if (lowered.includes("connection refused")) {
    events = events.filter((event) => String(event.fields.message).toLowerCase().includes("connection refused"));
  } else if (lowered.includes("status>=500") || lowered.includes("status >= 500")) {
    events = events.filter((event) => Number(event.fields.status ?? 0) >= 500);
  } else if (lowered.includes("level=error") && lowered.includes("level=warn")) {
    events = events.filter((event) => ["ERROR", "WARN"].includes(String(event.fields.level)));
  } else if (lowered.includes("level=error")) {
    events = events.filter((event) => event.fields.level === "ERROR");
  } else if (lowered.includes("level=warn")) {
    events = events.filter((event) => event.fields.level === "WARN");
  }
  return events;
}

export function resultTabForQuery(query: string): ResultTab {
  const lowered = query.toLowerCase();
  if (lowered.includes("timechart") || lowered.includes(" chart ")) return "visualization";
  if (lowered.includes("stats") || lowered.includes(" top ") || lowered.includes(" rare ")) return "statistics";
  return "events";
}

export function highlightedRaw(raw: string, query: string): ReactNode[] {
  const breakableRaw = raw.replaceAll(",", ",\u200b");
  const quoted = Array.from(query.matchAll(/"([^"\\]*(?:\\.[^"\\]*)*)"/g), (match) => match[1]).filter(Boolean);
  const fieldTerms = ["ERROR", "WARN", ...quoted].filter((term, index, terms) => terms.indexOf(term) === index).slice(0, 5);
  if (fieldTerms.length === 0) return [breakableRaw];
  const pattern = new RegExp(`(${fieldTerms.map(escapeRegExp).join("|")})`, "gi");
  let sourceOffset = 0;
  return breakableRaw.split(pattern).map((part) => {
    sourceOffset += part.length;
    return fieldTerms.some((term) => term.toLowerCase() === part.toLowerCase())
      ? <mark key={`${sourceOffset}-${part}`}>{part}</mark>
      : <span key={`${sourceOffset}-${part}`}>{part}</span>;
  });
}

export function queryForPattern(signature: string): string {
  const normalized = signature.replace(/\*+/g, "*").replaceAll('"', '\\"');
  const boundedPattern = normalized.replace(/^\*+|\*+$/g, "");
  return `index=gradethis\n| search _raw="*${boundedPattern}*"`;
}

export function formatFieldValue(value: DemoScalar): string {
  if (value === null) return "null";
  return typeof value === "boolean" ? (value ? "true" : "false") : String(value);
}

export function phaseLabel(phase: JobPhase): string {
  switch (phase) {
    case "queued": return "Queued";
    case "parsing": return "Parsing SPL";
    case "planning": return "Planning";
    case "running": return "Running";
    case "finalizing": return "Finalizing";
    case "completed": return "Completed";
    case "failed": return "Failed";
    case "canceled": return "Canceled";
  }
}

export function stateClass(phase: JobPhase): string {
  if (phase === "completed") return "state-success";
  if (phase === "failed") return "state-error";
  if (phase === "canceled") return "state-muted";
  return "state-running";
}

export function backendJobPhase(state: SearchJobState): JobPhase {
  switch (state) {
    case SearchJobState.SEARCH_JOB_STATE_QUEUED: return "queued";
    case SearchJobState.SEARCH_JOB_STATE_PARSING: return "parsing";
    case SearchJobState.SEARCH_JOB_STATE_PLANNING: return "planning";
    case SearchJobState.SEARCH_JOB_STATE_RUNNING: return "running";
    case SearchJobState.SEARCH_JOB_STATE_FINALIZING: return "finalizing";
    case SearchJobState.SEARCH_JOB_STATE_COMPLETED: return "completed";
    case SearchJobState.SEARCH_JOB_STATE_CANCELED: return "canceled";
    case SearchJobState.SEARCH_JOB_STATE_FAILED:
    case SearchJobState.SEARCH_JOB_STATE_EXPIRED:
      return "failed";
    default:
      return "queued";
  }
}

export function formatDuration(duration: { seconds: bigint; nanos: number } | undefined): string {
  if (duration === undefined) return "0.00 s";
  const seconds = Number(duration.seconds) + duration.nanos / 1_000_000_000;
  return seconds < 1 ? `${Math.max(0, Math.round(seconds * 1000))} ms` : `${seconds.toFixed(2)} s`;
}

export function formatBytes(value: bigint): string {
  const bytes = Number(value);
  if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const unit = Math.min(units.length - 1, Math.floor(Math.log(bytes) / Math.log(1024)));
  const scaled = bytes / 1024 ** unit;
  return `${scaled >= 10 || unit === 0 ? scaled.toFixed(0) : scaled.toFixed(1)} ${units[unit]}`;
}

export function jobEventCount(job: SearchJob): number {
  const count = job.progress?.matchedEvents ?? job.progress?.producedRows ?? 0n;
  return Math.min(Number.MAX_SAFE_INTEGER, Number(count));
}

export function timelineIndexFromPointer(event: PointerEvent<HTMLElement>): number | null {
  const target = (event.target as HTMLElement).closest<HTMLElement>("[data-timeline-index]");
  if (target === null) return null;
  const index = Number(target.dataset.timelineIndex);
  return Number.isFinite(index) ? index : null;
}

export function timelineBoundaryLabel(bucketIndex: number): string {
  return new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" })
    .format(new Date(Date.UTC(2026, 6, 21, 0, bucketIndex * 20)));
}
