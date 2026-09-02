import assert from "node:assert/strict";
import test from "node:test";

import {
  EXAMPLE_DRAFTS,
  type ExampleDraftTitle,
  backendDraftWithoutIndexSelector,
  exampleDraft,
  exampleDraftSpl,
} from "./example-drafts";
import { getQueryDiagnostic } from "./spl-editor";

test("backend example drafts omit fixture index selectors", () => {
  assert.equal(
    backendDraftWithoutIndexSelector("index=gradethis level=ERROR | stats count by service"),
    "level=ERROR | stats count by service",
  );
  assert.equal(
    backendDraftWithoutIndexSelector("  index=payments trace_id=*"),
    "trace_id=*",
  );
  assert.equal(
    backendDraftWithoutIndexSelector("index=gradethis | timechart span=5m count"),
    "* | timechart span=5m count",
  );
  assert.equal(backendDraftWithoutIndexSelector("index=gradethis"), "*");
});

test("backend example drafts never substitute unsupported wildcards", () => {
  const draft = backendDraftWithoutIndexSelector("index=gradethis duration_ms=* | stats count");
  assert.doesNotMatch(draft, /\bindex=\*/i);
  assert.match(draft, /^duration_ms=\*/);
});

test("queries without a leading index selector are preserved", () => {
  const spl = "level=WARN | stats count by service";
  assert.equal(backendDraftWithoutIndexSelector(spl), spl);
});

test("every example draft is a search the editor accepts as written and as the server sees it", () => {
  assert.ok(EXAMPLE_DRAFTS.length >= 4);
  const titles = new Set<string>();
  for (const example of EXAMPLE_DRAFTS) {
    assert.ok(!titles.has(example.title), `duplicate example title ${example.title}`);
    titles.add(example.title);
    assert.ok(example.description.length > 0, `${example.title} has no description`);
    assert.equal(getQueryDiagnostic(example.spl), null, `${example.title} is diagnosed as written`);
    assert.equal(getQueryDiagnostic(exampleDraftSpl(example, true)), null, `${example.title} is diagnosed once connected`);
  }
});

test("needsIndex says which examples are written against a preview index", () => {
  for (const example of EXAMPLE_DRAFTS) {
    const hasSelector = /^index=\S+/u.test(example.spl);
    assert.equal(hasSelector, example.needsIndex, `${example.title} needsIndex=${example.needsIndex}`);
    assert.doesNotMatch(exampleDraftSpl(example, true), /^index=/u, `${example.title} keeps its selector when connected`);
    assert.equal(exampleDraftSpl(example, false), example.spl);
  }
  assert.equal(
    exampleDraftSpl(exampleDraft("Production errors by service"), true),
    "level=ERROR | stats count by service",
  );
});

test("the home page's preview searches are looked up by title", () => {
  assert.equal(exampleDraft("Checkout trace investigation").spl, "index=payments trace_id=\"8e1c…\"");
  assert.throws(
    () => exampleDraft("Not an example" as ExampleDraftTitle),
    /Unknown example draft/u,
  );
});
