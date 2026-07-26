import assert from "node:assert/strict";
import test from "node:test";

import { backendDraftWithoutIndexSelector } from "./example-drafts";

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
