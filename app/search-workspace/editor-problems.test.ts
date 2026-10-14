import assert from "node:assert/strict";
import test from "node:test";

import { Diagnostic, DiagnosticSeverity } from "@/gen/ts/open_splunk/common";
import { getQueryDiagnostic } from "@/lib/search/spl-editor";

import { editorProblems, searchDiagnosticAuthorityKey, type BackendDiagnostics } from "./editor-problems";

const source = "index=main | futurecommand status";
const authority = searchDiagnosticAuthorityKey("", 1, "app-a");
const rejection = Diagnostic.fromPartial({
  code: "SPL_UNSUPPORTED_COMMAND",
  severity: DiagnosticSeverity.DIAGNOSTIC_SEVERITY_ERROR,
  message: 'unsupported command "futurecommand" at pipeline stage 1',
  sourceRange: { start: { byteOffset: 13n }, end: { byteOffset: 26n } },
  suggestions: ["Use a supported command."],
});
const verdict = (diagnostics: Diagnostic[] = []): BackendDiagnostics => ({ source, authorityKey: authority, diagnostics });
const problems = (backendVerdict: BackendDiagnostics | null, suggestionVerdict: BackendDiagnostics | null = null) =>
  editorProblems(source, getQueryDiagnostic(source), true, authority, backendVerdict, suggestionVerdict);

test("connected unknown commands report neutral information without an edit action", () => {
  const pending = problems(null);
  assert.equal(pending.length, 1);
  assert.equal(pending[0]!.diagnostic.code, "SPL_COMMAND_SUPPORT_UNKNOWN");
  assert.equal(pending[0]!.diagnostic.severity, "info");
  assert.match(pending[0]!.diagnostic.message, /server support is unknown/u);
  assert.doesNotMatch(pending[0]!.diagnostic.message, /newer/u);
  assert.equal(pending[0]!.fix, null);
  assert.equal(pending[0]!.stale, false);
});

test("an exact authoritative rejection replaces the unknown notice without rewriting it", () => {
  const rejected = problems(verdict([rejection]));
  assert.equal(rejected.length, 1);
  assert.equal(rejected[0]!.diagnostic.code, rejection.code);
  assert.equal(rejected[0]!.diagnostic.message, rejection.message);
  assert.equal(rejected[0]!.diagnostic.severity, "error");
  assert.deepEqual(rejected[0]!.diagnostic.suggestions, rejection.suggestions);
  assert.equal(rejected[0]!.diagnostic.range?.start, 13);
});

test("successful empty validation suppresses the notice; empty suggestions do not", () => {
  assert.deepEqual(problems(verdict()), []);
  assert.equal(problems(null, verdict()).length, 1);
});

test("source and server, session, and app authority must match before suppressing a notice", () => {
  const stale = [
    { ...verdict([rejection]), source: "index=main | anothercommand" },
    { ...verdict([rejection]), authorityKey: searchDiagnosticAuthorityKey("https://other.test", 1, "app-a") },
    { ...verdict([rejection]), authorityKey: searchDiagnosticAuthorityKey("", 2, "app-a") },
    { ...verdict([rejection]), authorityKey: searchDiagnosticAuthorityKey("", 1, "app-b") },
  ];
  for (const previous of stale) {
    const current = problems(previous, previous);
    assert.equal(current.length, 2);
    assert.equal(current[0]!.diagnostic.code, "SPL_COMMAND_SUPPORT_UNKNOWN");
    assert.equal(current[1]!.stale, true);
  }
  // Editing back to identical text may reuse a completed same-authority verdict.
  assert.deepEqual(problems(verdict()), []);
});

test("demo unsupported errors and local quote errors retain their fixes", () => {
  const local = getQueryDiagnostic(source)!;
  const demo = editorProblems(source, local, false, authority, null, null);
  assert.equal(demo[0]!.diagnostic.severity, "error");
  assert.equal(demo[0]!.fix?.actionLabel, "Remove stage");
  const unclosed = 'index=main | eval text="open';
  const quoted = editorProblems(unclosed, getQueryDiagnostic(unclosed), true, authority, verdict(), null);
  assert.equal(quoted[0]!.diagnostic.code, "SPL_UNTERMINATED_STRING");
  assert.equal(quoted[0]!.fix?.actionLabel, 'Add closing "');
});

test("conversion forms are accepted by the local catalog without becoming scalar functions", () => {
  for (const name of ["ctime", "mktime", "dur2sec", "num", "auto", "rmunit", "none"]) {
    assert.equal(getQueryDiagnostic(`index=main | convert ${name}(status)`), null);
  }
  assert.equal(getQueryDiagnostic('index=main | CONVERT timeformat="%Y|%m|%d" ctime(_time) AS rendered'), null);
});


test("a current suggestion replaces an identical stale message without duplicate UI keys", () => {
  const old = { ...verdict([rejection]), authorityKey: searchDiagnosticAuthorityKey("", 0, "app-a") };
  const current = problems(old, verdict([rejection]));
  assert.equal(current.length, 2);
  assert.equal(current[1]!.diagnostic.message, rejection.message);
  assert.equal(current[1]!.stale, false);
});
