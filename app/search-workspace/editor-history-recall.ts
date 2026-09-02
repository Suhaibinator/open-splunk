import type { HistoryRecallDirection } from "@/lib/search/spl-editor-interaction";

export type BoundaryLine = "first" | "last";

/**
 * True when the selection is collapsed and sits on the first (or last) line of
 * `value`. Arrow keys on any other line keep their native caret movement.
 */
export function caretOnBoundaryLine(
  value: string,
  selectionStart: number,
  selectionEnd: number,
  line: BoundaryLine,
): boolean {
  if (selectionStart !== selectionEnd) return false;
  if (line === "first") {
    const firstBreak = value.indexOf("\n");
    return firstBreak === -1 || selectionStart <= firstBreak;
  }
  const lastBreak = value.lastIndexOf("\n");
  return lastBreak === -1 || selectionStart > lastBreak;
}

export interface HistoryRecallKeyInput {
  altKey: boolean;
  ctrlKey: boolean;
  key: string;
  metaKey: boolean;
  shiftKey: boolean;
}

export interface HistoryRecallKeyState {
  completionOpen: boolean;
  selectionEnd: number;
  selectionStart: number;
  value: string;
}

/**
 * ArrowUp on the first line walks to older searches, ArrowDown on the last line
 * walks back towards the draft. The completion popup owns the arrows while it
 * is open, and modified arrows (selection, word jumps) are never recalls.
 */
export function historyRecallDirection(
  input: HistoryRecallKeyInput,
  state: HistoryRecallKeyState,
): HistoryRecallDirection | null {
  if (state.completionOpen || input.altKey || input.ctrlKey || input.metaKey || input.shiftKey) return null;
  if (input.key === "ArrowUp" && caretOnBoundaryLine(state.value, state.selectionStart, state.selectionEnd, "first")) {
    return "older";
  }
  if (input.key === "ArrowDown" && caretOnBoundaryLine(state.value, state.selectionStart, state.selectionEnd, "last")) {
    return "newer";
  }
  return null;
}

/** What the live region says after a recall step. */
export function historyRecallAnnouncement(index: number | null, total: number): string {
  return index === null ? "Restored draft" : `Recalled search ${index + 1} of ${total}`;
}
