import assert from "node:assert/strict";
import test from "node:test";

import { DEMO_FIELDS } from "@/lib/demo/search-data";
import { completionContextAt } from "@/lib/search/spl-editor";

import { extendsFragment, localCompletions, typeaheadOpens } from "./completion-candidates";
import { COMPLETIONS } from "./constants";

const manual = { commands: true, indexes: true };
const typing = { commands: false, indexes: true };

function labels(items: ReturnType<typeof localCompletions>): string[] {
  return items.map((item) => `${item.kind}:${item.label}`);
}

test("command position completes commands by prefix, with the relevance ladder", () => {
  const items = localCompletions(completionContextAt("index=main | st", 15), DEMO_FIELDS, typing);
  assert.ok(items.length > 0);
  assert.ok(items.every((item) => item.kind === "command" && item.label.startsWith("st")));
  assert.ok(items.every((item) => item.relevance === 0.75));
  assert.equal(
    localCompletions(completionContextAt("index=main | stats", 18), DEMO_FIELDS, typing)
      .find((item) => item.label === "stats")?.relevance,
    1,
  );
  assert.deepEqual(localCompletions(completionContextAt("index=main | ", 13), DEMO_FIELDS, typing), COMPLETIONS);
});

test("a bare term completes field names from the summary", () => {
  assert.deepEqual(
    labels(localCompletions(completionContextAt("index=main | stats count by s", 29), DEMO_FIELDS, typing)),
    ["field:source", "field:sourcetype", "field:status", "field:service"],
  );
  const [source] = localCompletions(completionContextAt("index=main | stats count by sou", 31), DEMO_FIELDS, typing);
  assert.equal(source.insertion, "source");
  assert.equal(source.detail, "string · 4 distinct values");
  assert.equal(source.relevance, 0.75);
});

test("the implicit search head completes field names too", () => {
  assert.deepEqual(labels(localCompletions(completionContextAt("inde", 4), DEMO_FIELDS, typing)), ["field:index"]);
});

test("a value fragment completes the values the summary saw, spelled as SPL", () => {
  const level = localCompletions(completionContextAt("index=main level=e", 18), DEMO_FIELDS, typing);
  assert.deepEqual(labels(level), ["value:\"ERROR\""]);
  assert.equal(level[0].insertion, "\"ERROR\"");
  assert.equal(level[0].detail, "1,432 events");
  assert.equal(level[0].relevance, 0.75);

  const status = localCompletions(completionContextAt("index=main status>=5", 20), DEMO_FIELDS, typing);
  assert.ok(status.length > 0);
  assert.ok(status.every((item) => item.kind === "value" && /^5\d\d$/u.test(item.label)));

  assert.deepEqual(localCompletions(completionContextAt("index=main nosuchfield=", 23), DEMO_FIELDS, typing), []);
});

test("index values are spelled bare, as index names, and left to a server when one is connected", () => {
  const context = completionContextAt("index=", 6);
  assert.deepEqual(labels(localCompletions(context, DEMO_FIELDS, typing)), ["index:gradethis"]);
  assert.deepEqual(localCompletions(context, DEMO_FIELDS, { commands: false, indexes: false }), []);
});

test("Ctrl+Space falls back to every command when nothing completes the fragment", () => {
  assert.deepEqual(localCompletions(completionContextAt("index=main", 10), DEMO_FIELDS, manual), COMPLETIONS);
  assert.deepEqual(localCompletions(completionContextAt("index=main | head 5 ", 20), DEMO_FIELDS, manual), COMPLETIONS);
  assert.deepEqual(localCompletions(completionContextAt("index=main | head 5 ", 20), DEMO_FIELDS, typing), []);
  // ...but not when something does.
  assert.deepEqual(labels(localCompletions(completionContextAt("index=", 6), DEMO_FIELDS, manual)), ["index:gradethis"]);
});

test("a list that only repeats the typed word has nothing to offer", () => {
  const exact = { kind: "value" as const, label: "\"ERROR\"", insertion: "\"ERROR\"", detail: "", relevance: 1 };
  const longer = { kind: "value" as const, label: "\"ERROR_X\"", insertion: "\"ERROR_X\"", detail: "", relevance: 0.75 };
  const unrelated = { kind: "keyword" as const, label: "OR", insertion: "OR", detail: "", relevance: 0.5 };
  assert.equal(extendsFragment([]), false);
  assert.equal(extendsFragment([exact]), false);
  assert.equal(extendsFragment([exact, longer]), true);
  assert.equal(extendsFragment([exact, unrelated]), true);
});

test("typing opens the popup for a stage, a spelled term with candidates, or a field comparison", () => {
  const demo = { server: false };
  assert.equal(typeaheadOpens(completionContextAt("index=main | ", 13), DEMO_FIELDS, demo), true);
  assert.equal(typeaheadOpens(completionContextAt("index=main | stats count by h", 29), DEMO_FIELDS, demo), true);
  assert.equal(typeaheadOpens(completionContextAt("index=main | stats count by ", 28), DEMO_FIELDS, demo), false);
  assert.equal(typeaheadOpens(completionContextAt("index=main | stats count by zz", 30), DEMO_FIELDS, demo), false);
  assert.equal(typeaheadOpens(completionContextAt("index=", 6), DEMO_FIELDS, demo), true);
  assert.equal(typeaheadOpens(completionContextAt("index=main nosuchfield=", 23), DEMO_FIELDS, demo), false);
  assert.equal(typeaheadOpens(completionContextAt("inde", 4), DEMO_FIELDS, demo), false);
  // A word spelled out in full closes the popup so the arrows recall history.
  assert.equal(typeaheadOpens(completionContextAt("index=main level=ERRO", 21), DEMO_FIELDS, demo), true);
  assert.equal(typeaheadOpens(completionContextAt("index=main level=ERROR", 22), DEMO_FIELDS, demo), false);
  assert.equal(typeaheadOpens(completionContextAt("index=main | stat", 17), DEMO_FIELDS, demo), true);
  assert.equal(typeaheadOpens(completionContextAt("index=main | stats", 18), DEMO_FIELDS, demo), false);
  assert.equal(typeaheadOpens(completionContextAt("index=main | stats count by host", 32), DEMO_FIELDS, demo), false);
  assert.equal(typeaheadOpens(null, DEMO_FIELDS, demo), false);
  // A server may know more than the summary, so a term or value asks it.
  assert.equal(typeaheadOpens(completionContextAt("index=main | stats count by zz", 30), DEMO_FIELDS, { server: true }), true);
  assert.equal(typeaheadOpens(completionContextAt("index=main nosuchfield=", 23), DEMO_FIELDS, { server: true }), true);
  assert.equal(typeaheadOpens(completionContextAt("index=main | stats count by ", 28), DEMO_FIELDS, { server: true }), false);
});
