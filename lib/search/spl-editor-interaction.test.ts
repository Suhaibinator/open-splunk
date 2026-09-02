import assert from "node:assert/strict";
import test from "node:test";

import {
  editorKeyIntent,
  insertCompletionIntoQuery,
  nextCompletionIndex,
  recallHistory,
  recallableHistory,
} from "./spl-editor-interaction";

const closedState = { query: "index=main", caret: 10, completionOpen: false, completionCount: 3 };
const openState = { ...closedState, completionOpen: true };

function key(name: string, modifiers: Partial<{ ctrlKey: boolean; metaKey: boolean }> = {}) {
  return { key: name, ctrlKey: false, metaKey: false, ...modifiers };
}

test("editor runs the search on Ctrl+Enter or Cmd+Enter whether or not completions are open", () => {
  assert.deepEqual(editorKeyIntent(key("Enter", { ctrlKey: true }), closedState), { kind: "run" });
  assert.deepEqual(editorKeyIntent(key("Enter", { metaKey: true }), openState), { kind: "run" });
  assert.deepEqual(editorKeyIntent(key("Enter"), closedState), { kind: "ignore" });
});

test("editor opens completions at the caret on Ctrl+Space unless the caret is inside a quoted value", () => {
  assert.deepEqual(
    editorKeyIntent(key(" ", { ctrlKey: true }), closedState),
    { kind: "open-completions", caret: 10 },
  );
  const quoted = `index=main | search message="serv`;
  assert.deepEqual(
    editorKeyIntent(key(" ", { ctrlKey: true }), { ...openState, query: quoted, caret: quoted.length }),
    { kind: "close-completions" },
  );
});

test("editor ignores navigation keys while the completion menu is closed", () => {
  for (const name of ["ArrowDown", "ArrowUp", "Tab", "Escape", "Enter"]) {
    assert.deepEqual(editorKeyIntent(key(name), closedState), { kind: "ignore" }, name);
  }
});

test("editor moves, accepts and dismisses completions while the menu is open", () => {
  assert.deepEqual(editorKeyIntent(key("ArrowDown"), openState), { kind: "move-completion", delta: 1 });
  assert.deepEqual(editorKeyIntent(key("ArrowUp"), openState), { kind: "move-completion", delta: -1 });
  assert.deepEqual(editorKeyIntent(key("Enter"), openState), { kind: "accept-completion" });
  assert.deepEqual(editorKeyIntent(key("Tab"), openState), { kind: "accept-completion" });
  assert.deepEqual(editorKeyIntent(key("Escape"), openState), { kind: "close-completions" });
  assert.deepEqual(editorKeyIntent(key("a"), openState), { kind: "ignore" });
});

test("editor lets Enter insert a newline when the open menu has nothing to accept", () => {
  const empty = { ...openState, completionCount: 0 };
  assert.deepEqual(editorKeyIntent(key("Enter"), empty), { kind: "ignore" });
  assert.deepEqual(editorKeyIntent(key("Tab"), empty), { kind: "ignore" });
  assert.deepEqual(editorKeyIntent(key("ArrowDown"), empty), { kind: "move-completion", delta: 1 });
});

test("completion index wraps in both directions and holds still on an empty list", () => {
  assert.equal(nextCompletionIndex(0, 1, 3), 1);
  assert.equal(nextCompletionIndex(2, 1, 3), 0);
  assert.equal(nextCompletionIndex(0, -1, 3), 2);
  assert.equal(nextCompletionIndex(1, -1, 3), 0);
  assert.equal(nextCompletionIndex(4, 1, 0), 4);
});

test("completion insertion prefers a server replacement range, clamped to the query", () => {
  const query = "index=main | sta";
  assert.deepEqual(
    insertCompletionIntoQuery(query, query.length, query.length, {
      insertion: "stats count",
      replaceStart: 13,
      replaceEnd: 16,
    }),
    { query: "index=main | stats count", caret: 24 },
  );
  assert.deepEqual(
    insertCompletionIntoQuery(query, query.length, query.length, {
      insertion: "stats count",
      replaceStart: -5,
      replaceEnd: 99,
    }),
    { query: "stats count", caret: 11 },
  );
  assert.deepEqual(
    insertCompletionIntoQuery(query, query.length, query.length, {
      insertion: "x",
      replaceStart: 10,
      replaceEnd: 4,
    }),
    { query: "index=mainx | sta", caret: 11 },
  );
});

test("completion insertion replaces the typed command fragment under the caret", () => {
  const query = "index=main\n| sta";
  assert.deepEqual(
    insertCompletionIntoQuery(query, query.length, query.length, { insertion: "stats count" }),
    { query: "index=main\n| stats count", caret: 24 },
  );
  // A selection reaching past the fragment is consumed along with it.
  const selected = "index=main | stX | head 5";
  assert.deepEqual(
    insertCompletionIntoQuery(selected, 15, 16, { insertion: "stats count" }),
    { query: "index=main | stats count | head 5", caret: 24 },
  );
});

test("completion insertion fills an empty query as the search head, not a pipeline stage", () => {
  assert.deepEqual(
    insertCompletionIntoQuery("", 0, 0, { insertion: "stats count" }),
    { query: "stats count", caret: 11 },
  );
});

test("completion insertion starts a new pipeline stage when the caret is not on a command", () => {
  assert.deepEqual(
    insertCompletionIntoQuery("index=main", 10, 10, { insertion: "stats count" }),
    { query: "index=main\n| stats count", caret: 24 },
  );
  assert.deepEqual(
    insertCompletionIntoQuery("index=main\n", 11, 11, { insertion: "stats count" }),
    { query: "index=main\n| stats count", caret: 24 },
  );
  assert.deepEqual(
    insertCompletionIntoQuery("index=main tail", 10, 15, { insertion: "head 5" }),
    { query: "index=main\n| head 5", caret: 19 },
  );
});

test("completion insertion replaces the field or value fragment under the caret", () => {
  assert.deepEqual(
    insertCompletionIntoQuery("index=main method=G", 19, 19, { insertion: "\"GET\"", kind: "value" }),
    { query: "index=main method=\"GET\"", caret: 23 },
  );
  assert.deepEqual(
    insertCompletionIntoQuery("index=ma", 8, 8, { insertion: "main", kind: "index" }),
    { query: "index=main", caret: 10 },
  );
  assert.deepEqual(
    insertCompletionIntoQuery("index=main | stats count by ho", 30, 30, { insertion: "host", kind: "field" }),
    { query: "index=main | stats count by host", caret: 32 },
  );
  assert.deepEqual(
    insertCompletionIntoQuery("inde", 4, 4, { insertion: "index", kind: "field" }),
    { query: "index", caret: 5 },
  );
});

test("a command completion outside command position still starts a new stage", () => {
  // The value fragment `main` is not a command being typed, so a command
  // offered there opens a stage instead of overwriting the value.
  assert.deepEqual(
    insertCompletionIntoQuery("index=main", 10, 10, { insertion: "stats count", kind: "command" }),
    { query: "index=main\n| stats count", caret: 24 },
  );
  assert.deepEqual(
    insertCompletionIntoQuery("index=main | stats count by ho", 30, 30, { insertion: "head 5", kind: "command" }),
    { query: "index=main | stats count by ho\n| head 5", caret: 39 },
  );
});

test("a non-command completion with nothing to replace is spliced at the caret", () => {
  assert.deepEqual(
    insertCompletionIntoQuery("index=main | head 5 ", 20, 20, { insertion: "host", kind: "field" }),
    { query: "index=main | head 5 host", caret: 24 },
  );
});

test("completion insertion refuses to edit inside a quoted value", () => {
  const query = `index=main | search message="serv`;
  assert.equal(insertCompletionIntoQuery(query, query.length, query.length, { insertion: "stats" }), null);
  assert.equal(insertCompletionIntoQuery(query, query.length, query.length, { insertion: "\"server\"", kind: "value" }), null);
});

test("recallable history drops blanks and repeats while keeping newest-first order", () => {
  assert.deepEqual(
    recallableHistory(["index=b", "  ", "index=a", "index=b", "", "index=c"]),
    ["index=b", "index=a", "index=c"],
  );
});

test("history recall walks older from the draft and back to it again", () => {
  const entries = ["newest", "middle", "oldest"];
  assert.deepEqual(recallHistory(entries, null, "older", "draft"), { index: 0, query: "newest" });
  assert.deepEqual(recallHistory(entries, 0, "older", "draft"), { index: 1, query: "middle" });
  assert.deepEqual(recallHistory(entries, 1, "older", "draft"), { index: 2, query: "oldest" });
  assert.equal(recallHistory(entries, 2, "older", "draft"), null);
  assert.deepEqual(recallHistory(entries, 2, "newer", "draft"), { index: 1, query: "middle" });
  assert.deepEqual(recallHistory(entries, 0, "newer", "draft"), { index: null, query: "draft" });
  assert.equal(recallHistory(entries, null, "newer", "draft"), null);
});

test("history recall does nothing without entries", () => {
  assert.equal(recallHistory([], null, "older", "draft"), null);
  assert.equal(recallHistory([], null, "newer", "draft"), null);
  assert.equal(recallHistory([], 3, "newer", "draft"), null);
});
