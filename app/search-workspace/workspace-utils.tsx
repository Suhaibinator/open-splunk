import type { PointerEvent, ReactNode } from "react";

import { SearchJobState, type SearchJob } from "@/gen/ts/open_splunk/v1/search";
import { DEMO_EVENTS, type DemoEvent, type DemoScalar } from "@/lib/demo/search-data";
import {
  isSplOffsetInDoubleQuotedValue,
  isSupportedSplPipelineCommand,
  SPL_PIPELINE_COMMANDS,
  splitSplPipeline,
  UNSUPPORTED_SPL_PIPELINE_COMMANDS,
} from "@/lib/search/spl-syntax";

import { formatBinaryBytes } from "./formatters";
import type { JobPhase, ResultTab } from "./model";

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

const PIPELINE_COMMAND_PATTERN = [
  ...SPL_PIPELINE_COMMANDS.map((command) => command.name),
  ...UNSUPPORTED_SPL_PIPELINE_COMMANDS,
].map(escapeRegExp).join("|");
const SYNTAX_TOKEN_PATTERN = new RegExp(
  `(\\b(?:index|host|source|sourcetype|level|status|trace_id|message|path)\\b(?=\\s*=)|\\b(?:${PIPELINE_COMMAND_PATTERN})\\b|\\b(?:count|p95|sum|avg|tonumber|replace)\\b|\\b(?:AND|OR|NOT|AS|BY)\\b|"(?:\\\\.|[^"\\\\])*"|\\|)`,
  "gi",
);
const UNSUPPORTED_PIPELINE_COMMAND_SET = new Set<string>(UNSUPPORTED_SPL_PIPELINE_COMMANDS);

export function hasPipelineCommand(query: string, commands: string | readonly string[]): boolean {
  const allowed = new Set((typeof commands === "string" ? [commands] : commands).map((command) => command.toLowerCase()));
  return splitSplPipeline(query).slice(1).some((stage) => {
    const command = /^\s*([A-Za-z][A-Za-z0-9_-]*)\b/.exec(stage)?.[1]?.toLowerCase();
    return command !== undefined && allowed.has(command);
  });
}

interface DemoFieldPredicate {
  excluded: boolean;
  field: "level" | "trace_id";
  value: string;
}

function offsetIsOutsideQuotes(query: string, targetOffset: number): boolean {
  return !isSplOffsetInDoubleQuotedValue(query, targetOffset);
}

function demoFieldPredicates(query: string): DemoFieldPredicate[] {
  const predicates: DemoFieldPredicate[] = [];
  const pattern = /\b(NOT\s+)?(level|trace_id)\s*(!=|=)\s*(?:"((?:\\.|[^"\\])*)"|([^\s()|,=!<>"]+))/gi;
  for (const match of query.matchAll(pattern)) {
    const offset = match.index ?? 0;
    if (!offsetIsOutsideQuotes(query, offset)) continue;
    const field = match[2]?.toLowerCase();
    if (field !== "level" && field !== "trace_id") continue;
    const quotedValue = match[4];
    const value = (quotedValue === undefined ? match[5] ?? "" : decodeDoubleQuotedSpl(quotedValue)).toLowerCase();
    if (value.length === 0) continue;
    predicates.push({
      field,
      value,
      excluded: Boolean(match[1]) !== (match[3] === "!="),
    });
  }
  return predicates;
}

function decodeDoubleQuotedSpl(value: string): string {
  return value.replace(/\\(.)/gs, (_match, escaped: string) => {
    switch (escaped) {
      case '"': return '"';
      case "\\": return "\\";
      case "n": return "\n";
      case "r": return "\r";
      case "t": return "\t";
      default: return `\\${escaped}`;
    }
  });
}

function matchesDemoValue(actualValue: unknown, queryValue: string): boolean {
  const actual = String(actualValue ?? "").toLowerCase();
  if (!queryValue.includes("*")) return actual === queryValue;
  const wildcard = new RegExp(`^${queryValue.split("*").map(escapeRegExp).join(".*")}$`, "i");
  return wildcard.test(actual);
}

export function syntaxTokens(query: string): ReactNode[] {
  const parts = query.split(SYNTAX_TOKEN_PATTERN).filter((part) => part !== undefined && part.length > 0);

  let sourceOffset = 0;
  return parts.map((part) => {
    sourceOffset += part.length;
    const lower = part.toLowerCase();
    let className = "spl-plain";
    if (part === "|") className = "spl-pipe";
    else if (part.startsWith('"')) className = "spl-string";
    else if (["and", "or", "not", "as", "by"].includes(lower)) className = "spl-boolean";
    else if (UNSUPPORTED_PIPELINE_COMMAND_SET.has(lower)) className = "spl-error-token";
    else if (isSupportedSplPipelineCommand(lower)) {
      className = "spl-command";
    } else if (/^(count|p95|sum|avg|tonumber|replace)$/i.test(part)) {
      className = "spl-function";
    } else if (/^(index|host|source|sourcetype|level|status|trace_id|message|path)$/i.test(part)) {
      className = "spl-field";
    }
    return <span className={className} key={`${sourceOffset}-${part}`}>{part}</span>;
  });
}

export function eventCountForQuery(query: string): number {
  const lowered = query.toLowerCase();
  const predicates = demoFieldPredicates(query);
  const tracePredicates = predicates.filter((predicate) => predicate.field === "trace_id");
  const includedTraceIds = tracePredicates.filter((predicate) => !predicate.excluded).map((predicate) => predicate.value);
  const excludedTraceIds = tracePredicates.filter((predicate) => predicate.excluded).map((predicate) => predicate.value);
  if (includedTraceIds.length > 0) {
    if (excludedTraceIds.includes("*")) return 0;
    if (includedTraceIds.includes("*")) return excludedTraceIds.length > 0 ? 12_828 : 12_846;
    return includedTraceIds.some((traceId) => excludedTraceIds.every((excluded) => !matchesDemoValue(traceId, excluded))) ? 18 : 0;
  }
  if (excludedTraceIds.includes("*")) return 0;
  if (lowered.includes("connection refused")) return 391;
  if (lowered.includes("status>=500") || lowered.includes("status >= 500")) return 812;
  const levelCounts = new Map([
    ["info", 8_917],
    ["warn", 2_491],
    ["error", 1_432],
    ["debug", 6],
  ]);
  const levelPredicates = predicates.filter((predicate) => predicate.field === "level");
  const includedLevels = levelPredicates.filter((predicate) => !predicate.excluded);
  const excludedLevels = levelPredicates.filter((predicate) => predicate.excluded);
  if (levelPredicates.length > 0) {
    return [...levelCounts].reduce((total, [level, count]) => {
      const included = includedLevels.length === 0 || includedLevels.some((predicate) => matchesDemoValue(level, predicate.value));
      const excluded = excludedLevels.some((predicate) => matchesDemoValue(level, predicate.value));
      return total + (included && !excluded ? count : 0);
    }, 0);
  }
  return excludedTraceIds.length > 0 ? 12_828 : 12_846;
}

export function filteredDemoEvents(query: string): DemoEvent[] {
  const lowered = query.toLowerCase();
  let events = DEMO_EVENTS;
  if (lowered.includes("connection refused")) {
    events = events.filter((event) => String(event.fields.message).toLowerCase().includes("connection refused"));
  }
  if (lowered.includes("status>=500") || lowered.includes("status >= 500")) {
    events = events.filter((event) => Number(event.fields.status ?? 0) >= 500);
  }
  const predicates = demoFieldPredicates(query);
  for (const field of ["level", "trace_id"] as const) {
    const fieldPredicates = predicates.filter((predicate) => predicate.field === field);
    const included = fieldPredicates.filter((predicate) => !predicate.excluded);
    const excluded = fieldPredicates.filter((predicate) => predicate.excluded);
    if (included.length > 0) {
      events = events.filter((event) => included.some((predicate) => matchesDemoValue(event.fields[field], predicate.value)));
    }
    if (excluded.length > 0) {
      events = events.filter((event) => excluded.every((predicate) => !matchesDemoValue(event.fields[field], predicate.value)));
    }
  }
  return events;
}

export function resultTabForQuery(query: string): ResultTab {
  if (hasPipelineCommand(query, "timechart")) return "visualization";
  if (hasPipelineCommand(query, ["table", "stats", "top", "rare"])) return "statistics";
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

export function queryForPattern(baseQuery: string, signature: string): string {
  const normalized = signature.replace(/\*+/g, "*").replaceAll('"', '\\"');
  const boundedPattern = normalized.replace(/^\*+|\*+$/g, "");
  const sourceClause = splitSplPipeline(baseQuery)[0]?.trim() || "index=gradethis";
  return `${sourceClause}\n| search _raw="*${boundedPattern}*"`;
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
    case "expired": return "Expired";
  }
}

export function stateClass(phase: JobPhase): string {
  if (phase === "completed") return "state-success";
  if (phase === "failed") return "state-error";
  if (phase === "expired") return "state-muted";
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
    case SearchJobState.SEARCH_JOB_STATE_FAILED: return "failed";
    case SearchJobState.SEARCH_JOB_STATE_EXPIRED: return "expired";
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
  return formatBinaryBytes(value);
}

export function jobEventCount(job: SearchJob): number {
  const count = job.progress === undefined
    ? 0n
    : job.progress.matchedEvents || job.progress.producedRows;
  return Math.min(Number.MAX_SAFE_INTEGER, Number(count));
}

export function timelineIndexFromPointer(event: PointerEvent<HTMLElement>, bucketCount: number): number | null {
  if (bucketCount <= 0) return null;
  const bounds = event.currentTarget.getBoundingClientRect();
  if (bounds.width <= 0) return null;
  const ratio = Math.max(0, Math.min(1, (event.clientX - bounds.left) / bounds.width));
  return Math.min(bucketCount - 1, Math.floor(ratio * bucketCount));
}

export function timelineBoundaryLabel(bucketIndex: number): string {
  return new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" })
    .format(new Date(Date.UTC(2026, 6, 21, 0, bucketIndex * 20)));
}
