import type { PointerEvent, ReactNode } from "react";

import { SearchJobState } from "@/gen/ts/open_splunk/search";
import { DEMO_EVENTS, type DemoEvent, type DemoHistoryEntry, type DemoScalar } from "@/lib/demo/search-data";
import type { DiagnosticMarker } from "@/lib/search/spl-diagnostic-markers";
import { searchResultViewForQuery } from "@/lib/search/result-view-navigation";
import {
  isSplOffsetInQuotedValue,
  isSupportedSplPipelineCommand,
  scanSplStructure,
  SPL_FUNCTIONS,
  SPL_KEYWORDS,
  SPL_PIPELINE_COMMANDS,
  splitSplPipeline,
  UNSUPPORTED_SPL_PIPELINE_COMMANDS,
} from "@/lib/search/spl-syntax";

import type { StatusTone } from "../_components/status";
import type { JobPhase, ResultTab } from "./model";

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

const PIPELINE_COMMAND_PATTERN = [
  ...SPL_PIPELINE_COMMANDS.map((command) => command.name),
  ...UNSUPPORTED_SPL_PIPELINE_COMMANDS,
].map(escapeRegExp).join("|");
const SPL_PERCENTILE_FUNCTION_PATTERN = "(?:p|perc)0*(?:[1-9]|[1-9][0-9])";
const SPL_FUNCTION_PATTERN = [
  ...SPL_FUNCTIONS.map((definition) => {
    const name = escapeRegExp(definition.name);
    return definition.highlight_requires_call ? `${name}(?=\\s*\\()` : name;
  }),
  SPL_PERCENTILE_FUNCTION_PATTERN,
].join("|");
const SPL_FUNCTION_SET = new Set<string>(
  SPL_FUNCTIONS.map((definition) => definition.name),
);
const SPL_PERCENTILE_FUNCTION = new RegExp(`^${SPL_PERCENTILE_FUNCTION_PATTERN}$`, "i");
const SPL_HIGHLIGHTED_KEYWORDS = SPL_KEYWORDS
  .filter((definition) => definition.highlight)
  .map((definition) => definition.name);
const SPL_KEYWORD_PATTERN = SPL_HIGHLIGHTED_KEYWORDS.map(escapeRegExp).join("|");
const SPL_KEYWORD_SET = new Set<string>(
  SPL_HIGHLIGHTED_KEYWORDS.map((keyword) => keyword.toLowerCase()),
);
const SYNTAX_TOKEN_PATTERN = new RegExp(
  `(\\b(?:index|host|source|sourcetype|level|status|trace_id|message|path)\\b(?=\\s*=)|\\b(?:${PIPELINE_COMMAND_PATTERN})\\b|\\b(?:${SPL_FUNCTION_PATTERN})\\b|\\b(?:${SPL_KEYWORD_PATTERN})\\b|"(?:\\\\.|[^"\\\\])*"|==|!=|<=|>=|[+\\-*/%=<>]|\\|)`,
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

/** A deliberately small demo-only projection of `timechart ... by <field>`. */
export function demoTimechartSplitField(query: string): string | null {
  for (const stage of splitSplPipeline(query).slice(1)) {
    if (!/^\s*timechart\b/i.test(stage)) continue;
    const candidates = [...stage.matchAll(/\bby\s+([A-Za-z_][A-Za-z0-9_.-]*)\b/giu)];
    const match = candidates.toReversed().find((candidate) =>
      !isSplOffsetInQuotedValue(stage, candidate.index),
    );
    if (match?.[1] !== undefined) return match[1];
  }
  return null;
}

function demoHeadLimit(query: string): number | null {
  let effectiveLimit: number | null = null;
  for (const stage of splitSplPipeline(query).slice(1)) {
    const match = /^\s*head\s+([0-9]+)(?:\s|$)/i.exec(stage);
    if (match === null) continue;
    const limit = Number(match[1]);
    const safeLimit = Number.isSafeInteger(limit) ? limit : Number.MAX_SAFE_INTEGER;
    effectiveLimit = effectiveLimit === null ? safeLimit : Math.min(effectiveLimit, safeLimit);
  }
  return effectiveLimit;
}

interface DemoFieldPredicate {
  excluded: boolean;
  field: "level" | "trace_id";
  value: string;
}

function offsetIsOutsideQuotes(query: string, targetOffset: number): boolean {
  return !isSplOffsetInQuotedValue(query, targetOffset);
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

function nextNonWhitespaceIsLeftParenthesis(query: string, offset: number): boolean {
  for (let cursor = offset; cursor < query.length;) {
    const codePoint = query.codePointAt(cursor)!;
    const character = String.fromCodePoint(codePoint);
    if (!/\p{White_Space}/u.test(character)) return character === "(";
    cursor += codePoint > 0xffff ? 2 : 1;
  }
  return false;
}

const MARKER_SEVERITY_RANK: Record<DiagnosticMarker["severity"], number> = { error: 0, warning: 1, info: 2 };

/**
 * A token's text split where diagnostic markers begin or end, each slice
 * wrapped in a `<mark>` when a marker covers it. Without markers the token
 * renders exactly as before: one string child.
 */
function markedSlices(part: string, partOffset: number, markers: readonly DiagnosticMarker[]): ReactNode[] {
  const partEnd = partOffset + part.length;
  const boundaries = new Set<number>([partOffset, partEnd]);
  let covered = false;
  for (const marker of markers) {
    if (marker.end <= partOffset || marker.start >= partEnd) continue;
    covered = true;
    boundaries.add(Math.max(marker.start, partOffset));
    boundaries.add(Math.min(marker.end, partEnd));
  }
  if (!covered) return [part];
  const edges = Array.from(boundaries).toSorted((left, right) => left - right);
  const slices: ReactNode[] = [];
  for (let index = 0; index < edges.length - 1; index += 1) {
    const sliceStart = edges[index]!;
    const sliceEnd = edges[index + 1]!;
    const text = part.slice(sliceStart - partOffset, sliceEnd - partOffset);
    let severity: DiagnosticMarker["severity"] | null = null;
    for (const marker of markers) {
      if (marker.start > sliceStart || marker.end < sliceEnd) continue;
      if (severity === null || MARKER_SEVERITY_RANK[marker.severity] < MARKER_SEVERITY_RANK[severity]) {
        severity = marker.severity;
      }
    }
    slices.push(severity === null
      ? text
      : <mark className="spl-diagnostic" data-severity={severity} key={sliceStart}>{text}</mark>);
  }
  return slices;
}

export function syntaxTokens(query: string, markers: readonly DiagnosticMarker[] = []): ReactNode[] {
  const structure = scanSplStructure(query);
  const parts: string[] = [];
  let cursor = 0;
  for (const quoted of structure.quotes) {
    if (quoted.quote !== "'") continue;
    parts.push(...query.slice(cursor, quoted.offset)
      .split(SYNTAX_TOKEN_PATTERN)
      .filter((part) => part !== undefined && part.length > 0));
    parts.push(query.slice(quoted.offset, quoted.endOffset));
    cursor = quoted.endOffset;
  }
  parts.push(...query.slice(cursor)
    .split(SYNTAX_TOKEN_PATTERN)
    .filter((part) => part !== undefined && part.length > 0));

  let sourceOffset = 0;
  let scalarStageIndex = 0;
  return parts.map((part) => {
    const partOffset = sourceOffset;
    sourceOffset += part.length;
    while (
      scalarStageIndex < structure.scalarStageRanges.length
      && structure.scalarStageRanges[scalarStageIndex]!.endOffset <= partOffset
    ) {
      scalarStageIndex += 1;
    }
    const scalarStage = structure.scalarStageRanges[scalarStageIndex];
    const inScalarStage = scalarStage !== undefined
      && scalarStage.startOffset <= partOffset
      && partOffset < scalarStage.endOffset;
    const lower = part.toLowerCase();
    const followedByLeftParenthesis = nextNonWhitespaceIsLeftParenthesis(query, sourceOffset);
    let className = "spl-plain";
    if (part === "|") className = "spl-pipe";
    else if (part.startsWith('"')) className = "spl-string";
    else if (part.startsWith("'") && inScalarStage) className = "spl-field";
    else if (
      (/^(?:==|!=|<=|>=|[+\-*/%=<>])$/.test(part) || lower === "in")
      && inScalarStage
    ) className = "spl-operator";
    else if (SPL_KEYWORD_SET.has(lower)) className = "spl-boolean";
    else if (UNSUPPORTED_PIPELINE_COMMAND_SET.has(lower)) className = "spl-error-token";
    else if ((lower === "eval" && followedByLeftParenthesis) ||
      SPL_FUNCTION_SET.has(lower) ||
      SPL_PERCENTILE_FUNCTION.test(part)) {
      className = "spl-function";
    } else if (isSupportedSplPipelineCommand(lower)) {
      className = "spl-command";
    } else if (/^(index|host|source|sourcetype|level|status|trace_id|message|path)$/i.test(part)) {
      className = "spl-field";
    }
    return (
      <span className={className} key={`${sourceOffset}-${part}`}>
        {markedSlices(part, partOffset, markers)}
      </span>
    );
  });
}

function unboundedEventCountForQuery(query: string): number {
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

export function eventCountForQuery(query: string): number {
  const count = unboundedEventCountForQuery(query);
  const headLimit = demoHeadLimit(query);
  return headLimit === null ? count : Math.min(count, headLimit);
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
  const headLimit = demoHeadLimit(query);
  return headLimit === null ? events : events.slice(0, headLimit);
}

export function resultTabForQuery(query: string): ResultTab {
  return searchResultViewForQuery(query);
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

/** Preserve adapted nomv newlines without changing ordinary compact fields. */
export function eventFieldValueWhiteSpace(
  value: DemoScalar,
): "nowrap" | "pre-wrap" {
  return typeof value === "string" && /[\r\n]/u.test(value)
    ? "pre-wrap"
    : "nowrap";
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
    case "interrupted": return "Interrupted";
    case "expired": return "Expired";
  }
}

/**
 * The recorded outcome of a finished search, as the one job vocabulary spells it.
 *
 * Search history stores its state capitalised for display; routing it through
 * `JobPhase` keeps the job card and the running job reading their tone from the
 * same table instead of growing a second colour map for the same four words.
 */
export function historyPhase(state: DemoHistoryEntry["state"]): JobPhase {
  switch (state) {
    case "Canceled": return "canceled";
    case "Completed": return "completed";
    case "Expired": return "expired";
    case "Failed": return "failed";
    case "Interrupted": return "interrupted";
  }
}

export function stateTone(phase: JobPhase): StatusTone {
  if (phase === "completed") return "success";
  if (phase === "failed") return "error";
  if (phase === "expired") return "neutral";
  if (phase === "canceled") return "neutral";
  if (phase === "interrupted") return "neutral";
  return "running";
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
    case SearchJobState.SEARCH_JOB_STATE_INTERRUPTED: return "interrupted";
    default:
      throw new TypeError("The search job returned an unsupported lifecycle state.");
  }
}

export function formatDuration(duration: { seconds: bigint; nanos: number } | undefined): string {
  if (duration === undefined) return "0.00 s";
  const seconds = Number(duration.seconds) + duration.nanos / 1_000_000_000;
  return seconds < 1 ? `${Math.max(0, Math.round(seconds * 1000))} ms` : `${seconds.toFixed(2)} s`;
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
