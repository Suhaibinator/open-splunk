import type { Diagnostic } from "@/gen/ts/open_splunk/common";
import {
  editorDiagnosticFromLocal,
  editorDiagnosticsFromProto,
  type EditorProblem,
} from "@/lib/search/spl-diagnostic-markers";
import type { SplDiagnostic } from "@/lib/search/spl-editor";

/** A verdict belongs to one source and one server/session/app authority. */
export interface BackendDiagnostics {
  source: string;
  authorityKey: string;
  diagnostics: Diagnostic[];
}

export function searchDiagnosticAuthorityKey(baseUrl: string, sessionRevision: number, appId: string | undefined): string {
  return JSON.stringify([baseUrl, sessionRevision, appId ?? null]);
}

export function editorProblems(
  source: string,
  local: SplDiagnostic | null,
  backendEnabled: boolean,
  authorityKey: string,
  verdict: BackendDiagnostics | null,
  suggestions: BackendDiagnostics | null,
): EditorProblem[] {
  const problems: EditorProblem[] = [];
  const currentVerdict = verdict?.source === source && verdict.authorityKey === authorityKey;
  if (local !== null && !(backendEnabled && local.kind === "unsupported" && currentVerdict)) {
    const diagnostic = editorDiagnosticFromLocal(source, local);
    if (backendEnabled && local.kind === "unsupported") {
      diagnostic.code = "SPL_COMMAND_SUPPORT_UNKNOWN";
      diagnostic.severity = "info";
      diagnostic.message = `Command “${local.token}” is not in this editor's local command catalog; server support is unknown.`;
      diagnostic.suggestions = ["Run the search to let the connected server validate it."];
    }
    problems.push({
      diagnostic,
      stale: false,
      fix: (!backendEnabled || local.kind !== "unsupported") && local.actionLabel !== undefined ? local : null,
    });
  }
  if (backendEnabled && verdict !== null) {
    for (const diagnostic of editorDiagnosticsFromProto(verdict.source, verdict.diagnostics)) {
      problems.push({ diagnostic, stale: !currentVerdict, fix: null });
    }
  }
  // A tolerant completion pass is not an acceptance verdict, even when its
  // diagnostics are empty. It can only contribute its own current messages.
  if (backendEnabled && suggestions?.source === source && suggestions.authorityKey === authorityKey) {
    for (const diagnostic of editorDiagnosticsFromProto(source, suggestions.diagnostics)) {
      problems.push({ diagnostic, stale: false, fix: null });
    }
  }
  const unique = new Map<string, EditorProblem>();
  for (const problem of problems) {
    const { diagnostic } = problem;
    const key = JSON.stringify([diagnostic.code, diagnostic.message, diagnostic.range]);
    const previous = unique.get(key);
    if (previous === undefined || previous.stale && !problem.stale) unique.set(key, problem);
  }
  return [...unique.values()];
}
