import { type Diagnostic, DiagnosticSeverity } from "@/gen/ts/open_splunk/common";

import { sourceLocation, type SplDiagnostic, utf16OffsetsForUtf8ByteOffsets } from "./spl-editor";

export type EditorDiagnosticSeverity = "error" | "warning" | "info";

/** The marked span in UTF-16 offsets, with the one-based location the editor reports. */
export interface EditorDiagnosticRange {
  start: number;
  end: number;
  line: number;
  column: number;
}

/**
 * One diagnostic as the editor shows it, whether the local scanner or the
 * server produced it. `range` is null when the diagnostic names no source
 * (or an offset outside it), in which case it is listed but never marked.
 */
export interface EditorDiagnostic {
  code: string;
  severity: EditorDiagnosticSeverity;
  message: string;
  range: EditorDiagnosticRange | null;
  suggestions: string[];
}

/** A span to underline in the highlight mirror. */
export interface DiagnosticMarker {
  start: number;
  end: number;
  severity: EditorDiagnosticSeverity;
}

/**
 * A row of the problems list. A run's verdict outlives edits to the query:
 * its messages stay listed, tagged stale, while its markers leave the text
 * they no longer describe. A local diagnostic may carry a one-click fix.
 */
export interface EditorProblem {
  diagnostic: EditorDiagnostic;
  stale: boolean;
  fix: SplDiagnostic | null;
}

const SEVERITY_RANK: Record<EditorDiagnosticSeverity, number> = { error: 0, warning: 1, info: 2 };

const LOCAL_CODES = {
  empty: "SPL_EMPTY_QUERY",
  unsupported: "SPL_UNSUPPORTED_COMMAND",
  "unclosed-quote": "SPL_UNTERMINATED_STRING",
} as const;

function severityFromProto(severity: DiagnosticSeverity): EditorDiagnosticSeverity {
  switch (severity) {
    case DiagnosticSeverity.DIAGNOSTIC_SEVERITY_ERROR:
      return "error";
    case DiagnosticSeverity.DIAGNOSTIC_SEVERITY_WARNING:
      return "warning";
    default:
      return "info";
  }
}

function codePointLengthAt(source: string, offset: number): number {
  const codePoint = source.codePointAt(offset);
  return codePoint !== undefined && codePoint > 0xffff ? 2 : 1;
}

function codePointLengthBefore(source: string, offset: number): number {
  const unit = source.charCodeAt(offset - 1);
  return offset >= 2 && unit >= 0xdc00 && unit <= 0xdfff ? 2 : 1;
}

/**
 * A zero-width span has nothing to underline, so it grows to the code point
 * under it -- or, at the end of the source, to the one before it. An empty
 * source has neither.
 */
function editorRange(source: string, start: number, end: number): EditorDiagnosticRange | null {
  let markStart = Math.max(0, Math.min(start, source.length));
  let markEnd = Math.max(markStart, Math.min(end, source.length));
  if (markEnd === markStart) {
    if (markStart < source.length) markEnd = markStart + codePointLengthAt(source, markStart);
    else if (markStart > 0) markStart -= codePointLengthBefore(source, markStart);
    else return null;
  }
  return { start: markStart, end: markEnd, ...sourceLocation(source, markStart) };
}

function compareDiagnostics(left: EditorDiagnostic, right: EditorDiagnostic): number {
  const severity = SEVERITY_RANK[left.severity] - SEVERITY_RANK[right.severity];
  if (severity !== 0) return severity;
  if (left.range === null || right.range === null) {
    return left.range === right.range ? 0 : left.range === null ? 1 : -1;
  }
  return left.range.start - right.range.start;
}

/**
 * Converts the server's diagnostics for `source` into editor diagnostics,
 * translating every UTF-8 byte range in one pass. A range whose start lies
 * outside the source is dropped (the message stays); an end past the source
 * is clamped to it. Errors come before warnings, then earlier spans first.
 */
export function editorDiagnosticsFromProto(source: string, diagnostics: readonly Diagnostic[]): EditorDiagnostic[] {
  const byteLength = BigInt(new TextEncoder().encode(source).length);
  const byteOffsets: bigint[] = [];
  const rangeSlots = diagnostics.map((diagnostic) => {
    const start = diagnostic.sourceRange?.start?.byteOffset;
    const end = diagnostic.sourceRange?.end?.byteOffset;
    if (start === undefined || end === undefined || start < 0n || start > byteLength) return null;
    const slot = byteOffsets.length;
    byteOffsets.push(start, end < start ? start : end > byteLength ? byteLength : end);
    return slot;
  });
  const offsets = utf16OffsetsForUtf8ByteOffsets(source, byteOffsets);
  const converted = diagnostics.map((diagnostic, index): EditorDiagnostic => {
    const slot = rangeSlots[index] ?? null;
    return {
      code: diagnostic.code,
      severity: severityFromProto(diagnostic.severity),
      message: diagnostic.message,
      range: slot === null ? null : editorRange(source, offsets[slot]!, offsets[slot + 1]!),
      suggestions: diagnostic.suggestions,
    };
  });
  return converted.toSorted(compareDiagnostics);
}

/** The local scanner's verdict in the shape the server's takes, under the server's stable code. */
export function editorDiagnosticFromLocal(source: string, diagnostic: SplDiagnostic): EditorDiagnostic {
  const code = diagnostic.kind === "unclosed-quote" && diagnostic.quote === "'"
    ? "SPL_UNTERMINATED_FIELD_QUOTE"
    : LOCAL_CODES[diagnostic.kind];
  return {
    code,
    severity: "error",
    message: diagnostic.message,
    range: diagnostic.start === undefined || diagnostic.end === undefined
      ? null
      : editorRange(source, diagnostic.start, diagnostic.end),
    suggestions: [diagnostic.suggestion],
  };
}

/** The spans the highlight mirror underlines: current problems that name source. */
export function diagnosticMarkers(problems: readonly EditorProblem[]): DiagnosticMarker[] {
  const markers: DiagnosticMarker[] = [];
  for (const problem of problems) {
    if (problem.stale || problem.diagnostic.range === null) continue;
    markers.push({
      start: problem.diagnostic.range.start,
      end: problem.diagnostic.range.end,
      severity: problem.diagnostic.severity,
    });
  }
  return markers;
}

/** What the gutter shows for a line: its worst severity, and where its first marker starts. */
export interface MarkedLine {
  severity: EditorDiagnosticSeverity;
  start: number;
}

/** Every one-based line a marker touches, for the gutter dots. */
export function markedLines(source: string, markers: readonly DiagnosticMarker[]): Map<number, MarkedLine> {
  const lines = new Map<number, MarkedLine>();
  for (const marker of markers) {
    const first = sourceLocation(source, marker.start).line;
    const last = sourceLocation(source, Math.max(marker.start, marker.end - 1)).line;
    for (let line = first; line <= last; line += 1) {
      const current = lines.get(line);
      if (current === undefined) {
        lines.set(line, { severity: marker.severity, start: marker.start });
        continue;
      }
      lines.set(line, {
        severity: SEVERITY_RANK[marker.severity] < SEVERITY_RANK[current.severity] ? marker.severity : current.severity,
        start: Math.min(current.start, marker.start),
      });
    }
  }
  return lines;
}

function plural(count: number, noun: string): string {
  return `${count} ${noun}${count === 1 ? "" : "s"}`;
}

/** "2 errors, 1 warning" for the live region; empty when there is nothing to say. */
export function diagnosticSummary(problems: readonly EditorProblem[]): string {
  const counts: Record<EditorDiagnosticSeverity, number> = { error: 0, warning: 0, info: 0 };
  for (const problem of problems) counts[problem.diagnostic.severity] += 1;
  const parts: string[] = [];
  if (counts.error > 0) parts.push(plural(counts.error, "error"));
  if (counts.warning > 0) parts.push(plural(counts.warning, "warning"));
  if (counts.info > 0) parts.push(plural(counts.info, "note"));
  return parts.join(", ");
}
