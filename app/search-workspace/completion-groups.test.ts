import assert from "node:assert/strict";
import test from "node:test";

import { SearchSuggestionKind } from "@/gen/ts/open_splunk/search_api";

import {
  COMPLETION_KIND_PRESENTATION,
  COMPLETION_KINDS,
  completionKindFromSuggestion,
  groupCompletions,
  localCompletionRelevance,
  orderCompletions,
  type RankedCompletion,
} from "./completion-groups";

function item(kind: RankedCompletion["kind"], label: string, relevance: number): RankedCompletion {
  return { kind, label, relevance };
}

test("orderCompletions sorts by kind, then relevance, then label", () => {
  const ordered = orderCompletions([
    item("keyword", "OR", 0.5),
    item("field", "status", 0.75),
    item("command", "stats", 0.5),
    item("command", "search", 1),
    item("index", "main", 0.5),
    item("field", "source", 0.75),
    item("value", "\"GET\"", 0.75),
    item("function", "sum", 0.5),
  ]);
  assert.deepEqual(
    ordered.map((entry) => `${entry.kind}:${entry.label}`),
    [
      "command:search", "command:stats",
      "function:sum",
      "field:source", "field:status",
      "value:\"GET\"",
      "keyword:OR",
      "index:main",
    ],
  );
});

test("orderCompletions keeps the incoming order for ties", () => {
  const ordered = orderCompletions([
    { ...item("field", "host", 0.5), detail: "first" },
    { ...item("field", "host", 0.5), detail: "second" },
  ]);
  assert.deepEqual(ordered.map((entry) => entry.detail), ["first", "second"]);
});

test("groupCompletions preserves the flat indexes the keyboard walks", () => {
  const ordered = orderCompletions([
    item("field", "status", 0.75),
    item("command", "stats", 0.5),
    item("field", "source", 0.75),
  ]);
  const groups = groupCompletions(ordered);
  assert.deepEqual(groups.map((group) => group.kind), ["command", "field"]);
  assert.deepEqual(groups[1].items.map((entry) => entry.index), [1, 2]);
  assert.deepEqual(groups[1].items.map((entry) => entry.item.label), ["source", "status"]);
  assert.deepEqual(groupCompletions([]), []);
});

test("every completion kind has a heading, a hint and an icon", () => {
  for (const kind of COMPLETION_KINDS) {
    const presentation = COMPLETION_KIND_PRESENTATION[kind];
    assert.ok(presentation.heading.length > 0, kind);
    assert.ok(presentation.hint.length > 0, kind);
    assert.ok(presentation.icon.length > 0, kind);
  }
});

test("completionKindFromSuggestion maps every server kind and buckets unknown ones", () => {
  assert.equal(completionKindFromSuggestion(SearchSuggestionKind.SEARCH_SUGGESTION_KIND_COMMAND), "command");
  assert.equal(completionKindFromSuggestion(SearchSuggestionKind.SEARCH_SUGGESTION_KIND_FUNCTION), "function");
  assert.equal(completionKindFromSuggestion(SearchSuggestionKind.SEARCH_SUGGESTION_KIND_FIELD), "field");
  assert.equal(completionKindFromSuggestion(SearchSuggestionKind.SEARCH_SUGGESTION_KIND_VALUE), "value");
  assert.equal(completionKindFromSuggestion(SearchSuggestionKind.SEARCH_SUGGESTION_KIND_KEYWORD), "keyword");
  assert.equal(completionKindFromSuggestion(SearchSuggestionKind.SEARCH_SUGGESTION_KIND_INDEX), "index");
  assert.equal(completionKindFromSuggestion(SearchSuggestionKind.SEARCH_SUGGESTION_KIND_UNSPECIFIED), "keyword");
  assert.equal(completionKindFromSuggestion(SearchSuggestionKind.UNRECOGNIZED), "keyword");
});

test("localCompletionRelevance follows the server's ladder", () => {
  assert.equal(localCompletionRelevance("stats", ""), 0.5);
  assert.equal(localCompletionRelevance("stats", "st"), 0.75);
  assert.equal(localCompletionRelevance("stats", "STATS"), 1);
  assert.equal(localCompletionRelevance("stats", "ev"), 0.5);
});
