import assert from "node:assert/strict";
import test from "node:test";

import {
  caretOnBoundaryLine,
  historyRecallAnnouncement,
  historyRecallDirection,
  type HistoryRecallKeyInput,
} from "./editor-history-recall";

const TWO_LINES = "index=main\n| stats count";

function key(name: string, modifiers: Partial<HistoryRecallKeyInput> = {}): HistoryRecallKeyInput {
  return { altKey: false, ctrlKey: false, key: name, metaKey: false, shiftKey: false, ...modifiers };
}

test("a single-line query is both its first and last line", () => {
  assert.equal(caretOnBoundaryLine("index=main", 3, 3, "first"), true);
  assert.equal(caretOnBoundaryLine("index=main", 3, 3, "last"), true);
  assert.equal(caretOnBoundaryLine("", 0, 0, "first"), true);
  assert.equal(caretOnBoundaryLine("", 0, 0, "last"), true);
});

test("the caret must sit on the boundary line itself, the line break included", () => {
  const firstBreak = TWO_LINES.indexOf("\n");
  assert.equal(caretOnBoundaryLine(TWO_LINES, 0, 0, "first"), true);
  assert.equal(caretOnBoundaryLine(TWO_LINES, firstBreak, firstBreak, "first"), true);
  assert.equal(caretOnBoundaryLine(TWO_LINES, firstBreak + 1, firstBreak + 1, "first"), false);
  assert.equal(caretOnBoundaryLine(TWO_LINES, firstBreak, firstBreak, "last"), false);
  assert.equal(caretOnBoundaryLine(TWO_LINES, firstBreak + 1, firstBreak + 1, "last"), true);
  assert.equal(caretOnBoundaryLine(TWO_LINES, TWO_LINES.length, TWO_LINES.length, "last"), true);
});

test("a range selection never counts as a boundary caret", () => {
  assert.equal(caretOnBoundaryLine("index=main", 0, 5, "first"), false);
  assert.equal(caretOnBoundaryLine("index=main", 0, 5, "last"), false);
});

test("ArrowUp on the first line recalls older, ArrowDown on the last line walks newer", () => {
  const firstLine = { completionOpen: false, selectionEnd: 4, selectionStart: 4, value: TWO_LINES };
  const lastLine = { ...firstLine, selectionEnd: TWO_LINES.length, selectionStart: TWO_LINES.length };
  assert.equal(historyRecallDirection(key("ArrowUp"), firstLine), "older");
  assert.equal(historyRecallDirection(key("ArrowDown"), firstLine), null);
  assert.equal(historyRecallDirection(key("ArrowUp"), lastLine), null);
  assert.equal(historyRecallDirection(key("ArrowDown"), lastLine), "newer");
  assert.equal(historyRecallDirection(key("ArrowLeft"), firstLine), null);
});

test("the completion popup and modified arrows keep their own meaning", () => {
  const state = { completionOpen: false, selectionEnd: 0, selectionStart: 0, value: "index=main" };
  assert.equal(historyRecallDirection(key("ArrowUp"), { ...state, completionOpen: true }), null);
  assert.equal(historyRecallDirection(key("ArrowUp", { shiftKey: true }), state), null);
  assert.equal(historyRecallDirection(key("ArrowUp", { altKey: true }), state), null);
  assert.equal(historyRecallDirection(key("ArrowDown", { ctrlKey: true }), state), null);
  assert.equal(historyRecallDirection(key("ArrowDown", { metaKey: true }), state), null);
});

test("the announcement counts recalls from one and names the restored draft", () => {
  assert.equal(historyRecallAnnouncement(1, 14), "Recalled search 2 of 14");
  assert.equal(historyRecallAnnouncement(0, 1), "Recalled search 1 of 1");
  assert.equal(historyRecallAnnouncement(null, 14), "Restored draft");
});
