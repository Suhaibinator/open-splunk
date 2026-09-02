import assert from "node:assert/strict";
import test from "node:test";

import { type Diagnostic, DiagnosticSeverity } from "@/gen/ts/open_splunk/common";

import {
  diagnosticMarkers,
  diagnosticSummary,
  type EditorProblem,
  editorDiagnosticFromLocal,
  editorDiagnosticsFromProto,
  markedLines,
} from "./spl-diagnostic-markers";
import { getQueryDiagnostic } from "./spl-editor";

function position(byteOffset: number) {
  return { byteOffset: BigInt(byteOffset), line: 1, column: 1 };
}

function protoDiagnostic(
  overrides: Partial<Diagnostic> & { start?: number; end?: number },
): Diagnostic {
  const { start, end, ...rest } = overrides;
  return {
    code: "SPL_UNSUPPORTED_COMMAND",
    severity: DiagnosticSeverity.DIAGNOSTIC_SEVERITY_ERROR,
    message: "Unsupported",
    suggestions: [],
    ...(start === undefined || end === undefined ? {} : { sourceRange: { start: position(start), end: position(end) } }),
    ...rest,
  };
}

function problem(diagnostic: Partial<EditorProblem["diagnostic"]>, stale = false): EditorProblem {
  return {
    diagnostic: { code: "X", severity: "error", message: "m", range: null, suggestions: [], ...diagnostic },
    stale,
    fix: null,
  };
}

test("server byte ranges land on UTF-16 offsets and one-based code-point columns", () => {
  const source = 'café="naïve" | foo';
  const [foo] = editorDiagnosticsFromProto(source, [
    // "café" is 5 bytes, the quoted value another 9, so `foo` starts at byte 17.
    protoDiagnostic({ start: 17, end: 20 }),
  ]);
  assert.deepEqual(foo?.range, { start: 15, end: 18, line: 1, column: 16 });
  assert.equal(source.slice(foo!.range!.start, foo!.range!.end), "foo");
});

test("zero-width server ranges grow to the code point they sit on, or the one before at the end", () => {
  const [inside] = editorDiagnosticsFromProto("a😀b", [protoDiagnostic({ start: 1, end: 1 })]);
  assert.deepEqual(inside?.range, { start: 1, end: 3, line: 1, column: 2 });
  const [atEnd] = editorDiagnosticsFromProto("a😀", [protoDiagnostic({ start: 5, end: 5 })]);
  assert.deepEqual(atEnd?.range, { start: 1, end: 3, line: 1, column: 2 });
  const [empty] = editorDiagnosticsFromProto("", [protoDiagnostic({ start: 0, end: 0 })]);
  assert.equal(empty?.range, null);
});

test("a range starting outside the source keeps its message but marks nothing; an end past it is clamped", () => {
  const [outside] = editorDiagnosticsFromProto("abc", [protoDiagnostic({ start: 7, end: 9, message: "Kept" })]);
  assert.equal(outside?.message, "Kept");
  assert.equal(outside?.range, null);
  const [clamped] = editorDiagnosticsFromProto("abc", [protoDiagnostic({ start: 1, end: 40 })]);
  assert.deepEqual(clamped?.range, { start: 1, end: 3, line: 1, column: 2 });
  const [inverted] = editorDiagnosticsFromProto("abc", [protoDiagnostic({ start: 2, end: 1 })]);
  assert.deepEqual(inverted?.range, { start: 2, end: 3, line: 1, column: 3 });
  const [missing] = editorDiagnosticsFromProto("abc", [protoDiagnostic({})]);
  assert.equal(missing?.range, null);
});

test("server diagnostics sort errors first, then ranged before unranged, then by position", () => {
  const source = "index=main | foo | bar";
  const ordered = editorDiagnosticsFromProto(source, [
    protoDiagnostic({ code: "W_LATE", severity: DiagnosticSeverity.DIAGNOSTIC_SEVERITY_WARNING, start: 19, end: 22 }),
    protoDiagnostic({ code: "E_NOWHERE" }),
    protoDiagnostic({ code: "I_EARLY", severity: DiagnosticSeverity.DIAGNOSTIC_SEVERITY_INFO, start: 0, end: 5 }),
    protoDiagnostic({ code: "E_LATE", start: 19, end: 22 }),
    protoDiagnostic({ code: "E_EARLY", start: 13, end: 16 }),
    protoDiagnostic({ code: "U", severity: DiagnosticSeverity.DIAGNOSTIC_SEVERITY_UNSPECIFIED, start: 0, end: 1 }),
  ]);
  assert.deepEqual(
    ordered.map((diagnostic) => `${diagnostic.severity}:${diagnostic.code}`),
    ["error:E_EARLY", "error:E_LATE", "error:E_NOWHERE", "warning:W_LATE", "info:I_EARLY", "info:U"],
  );
});

test("local scanner verdicts convert under the server's codes with their span", () => {
  const unsupported = editorDiagnosticFromLocal("index=main | transaction", getQueryDiagnostic("index=main | transaction")!);
  assert.equal(unsupported.code, "SPL_UNSUPPORTED_COMMAND");
  assert.equal(unsupported.severity, "error");
  assert.deepEqual(unsupported.range, { start: 13, end: 24, line: 1, column: 14 });
  assert.equal(unsupported.suggestions.length, 1);

  const unclosed = editorDiagnosticFromLocal('index=main "oops', getQueryDiagnostic('index=main "oops')!);
  assert.equal(unclosed.code, "SPL_UNTERMINATED_STRING");
  assert.deepEqual(unclosed.range, { start: 11, end: 16, line: 1, column: 12 });

  const field = "index=main | eval x='broken";
  const fieldQuote = editorDiagnosticFromLocal(field, getQueryDiagnostic(field)!);
  assert.equal(fieldQuote.code, "SPL_UNTERMINATED_FIELD_QUOTE");
  assert.equal(fieldQuote.range?.start, 20);

  const empty = editorDiagnosticFromLocal("", getQueryDiagnostic("")!);
  assert.equal(empty.code, "SPL_EMPTY_QUERY");
  assert.equal(empty.range, null);
});

test("only current problems with a span become markers", () => {
  const ranged = { start: 2, end: 4, line: 1, column: 3 };
  const markers = diagnosticMarkers([
    problem({ range: ranged }),
    problem({ range: ranged, severity: "warning" }, true),
    problem({ range: null }),
    problem({ range: { start: 0, end: 1, line: 1, column: 1 }, severity: "info" }),
  ]);
  assert.deepEqual(markers, [
    { start: 2, end: 4, severity: "error" },
    { start: 0, end: 1, severity: "info" },
  ]);
});

test("gutter lines carry the worst severity and the earliest start a marker gives them", () => {
  const source = "index=main\n| foo\n| bar\n| baz";
  const lines = markedLines(source, [
    { start: 13, end: 22, severity: "info" },
    { start: 19, end: 20, severity: "warning" },
    { start: 25, end: 28, severity: "error" },
  ]);
  assert.deepEqual([...lines.entries()], [
    [2, { severity: "info", start: 13 }],
    [3, { severity: "warning", start: 13 }],
    [4, { severity: "error", start: 25 }],
  ]);
  assert.deepEqual([...markedLines("ab\n", [{ start: 2, end: 3, severity: "error" }]).keys()], [1]);
});

test("the live summary counts every listed problem by severity", () => {
  assert.equal(diagnosticSummary([]), "");
  assert.equal(diagnosticSummary([problem({ severity: "info" })]), "1 note");
  assert.equal(
    diagnosticSummary([problem({}), problem({}, true), problem({ severity: "warning" }), problem({ severity: "info" })]),
    "2 errors, 1 warning, 1 note",
  );
});
