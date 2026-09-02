import { type CompletionKind, completionContextAt, isCursorInQuotedValue } from "./spl-editor";

// Pure decisions behind the SPL editor's key handling. The workspace component
// owns the DOM, timers and React state; everything here takes plain values and
// returns plain values so the branches can be tested without a browser.

export interface EditorKeyInput {
  key: string;
  ctrlKey: boolean;
  metaKey: boolean;
}

export interface EditorKeyState {
  query: string;
  caret: number;
  completionOpen: boolean;
  completionCount: number;
}

export type EditorKeyIntent =
  | { kind: "run" }
  | { kind: "open-completions"; caret: number }
  | { kind: "close-completions" }
  | { kind: "move-completion"; delta: 1 | -1 }
  | { kind: "accept-completion" }
  | { kind: "ignore" };

/** Maps one editor key press onto the action the workspace should take. */
export function editorKeyIntent(input: EditorKeyInput, state: EditorKeyState): EditorKeyIntent {
  if ((input.metaKey || input.ctrlKey) && input.key === "Enter") return { kind: "run" };
  if (input.ctrlKey && input.key === " ") {
    return isCursorInQuotedValue(state.query, state.caret)
      ? { kind: "close-completions" }
      : { kind: "open-completions", caret: state.caret };
  }
  if (!state.completionOpen) return { kind: "ignore" };
  if (input.key === "ArrowDown") return { kind: "move-completion", delta: 1 };
  if (input.key === "ArrowUp") return { kind: "move-completion", delta: -1 };
  if ((input.key === "Enter" || input.key === "Tab") && state.completionCount > 0) {
    return { kind: "accept-completion" };
  }
  if (input.key === "Escape") return { kind: "close-completions" };
  return { kind: "ignore" };
}

/** Cycles the highlighted completion, wrapping at both ends of the list. */
export function nextCompletionIndex(current: number, delta: 1 | -1, count: number): number {
  if (count <= 0) return current;
  return (current + delta + count) % count;
}

export interface CompletionInsertion {
  insertion: string;
  /** Defaults to `command`, the only kind the popup offered before it grouped. */
  kind?: CompletionKind;
  replaceStart?: number;
  replaceEnd?: number;
}

export interface EditedQuery {
  query: string;
  caret: number;
}

/**
 * Splices a completion into the query. A server-supplied replacement range wins;
 * otherwise the fragment under the caret is replaced -- a command only when the
 * caret is in command position, anything else wherever it was typed -- and with
 * no fragment to replace a command starts a new pipeline stage on its own line.
 * Returns null when the caret sits inside a quoted value, where no completion
 * applies.
 */
export function insertCompletionIntoQuery(
  query: string,
  selectionStart: number,
  selectionEnd: number,
  completion: CompletionInsertion,
): EditedQuery | null {
  const { insertion } = completion;
  if (isCursorInQuotedValue(query, selectionStart)) return null;
  const context = completionContextAt(query, selectionStart);

  if (completion.replaceStart !== undefined && completion.replaceEnd !== undefined) {
    const replaceStart = Math.max(0, Math.min(query.length, completion.replaceStart));
    const replaceEnd = Math.max(replaceStart, Math.min(query.length, completion.replaceEnd));
    return {
      query: `${query.slice(0, replaceStart)}${insertion}${query.slice(replaceEnd)}`,
      caret: replaceStart + insertion.length,
    };
  }
  const kind = completion.kind ?? "command";
  if (context !== null && (kind !== "command" || context.stage === "command")) {
    return {
      query: `${query.slice(0, context.fragmentStart)}${insertion}${query.slice(Math.max(context.fragmentEnd, selectionEnd))}`,
      caret: context.fragmentStart + insertion.length,
    };
  }
  const before = query.slice(0, selectionStart);
  const after = query.slice(selectionEnd);
  if (kind !== "command") {
    return { query: `${before}${insertion}${after}`, caret: before.length + insertion.length };
  }
  const separator = before.length === 0 || before.endsWith("\n") ? "" : "\n";
  const inserted = `${separator}| ${insertion}`;
  return {
    query: `${before}${inserted}${after}`,
    caret: before.length + inserted.length,
  };
}

export type HistoryRecallDirection = "older" | "newer";

export interface HistoryRecall {
  /** Position in the recall list, or null while the draft is shown. */
  index: number | null;
  query: string;
}

/** Distinct, non-empty queries in the order the history lists them (newest first). */
export function recallableHistory(queries: readonly string[]): string[] {
  const seen = new Set<string>();
  const entries: string[] = [];
  for (const query of queries) {
    if (query.trim().length === 0 || seen.has(query)) continue;
    seen.add(query);
    entries.push(query);
  }
  return entries;
}

/**
 * Steps through recalled searches the way a shell steps through its history:
 * "older" walks away from the draft, "newer" walks back towards it, and the
 * draft itself is restored once the newest entry is passed. Returns null when
 * the step would leave the list, so the caller keeps the current text.
 */
export function recallHistory(
  entries: readonly string[],
  index: number | null,
  direction: HistoryRecallDirection,
  draft: string,
): HistoryRecall | null {
  if (direction === "older") {
    const next = index === null ? 0 : index + 1;
    const query = entries[next];
    return query === undefined ? null : { index: next, query };
  }
  if (index === null) return null;
  if (index === 0) return { index: null, query: draft };
  const query = entries[index - 1];
  return query === undefined ? null : { index: index - 1, query };
}
