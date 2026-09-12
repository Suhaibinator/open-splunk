import assert from "node:assert/strict";
import test from "node:test";
import { patternExportSource } from "./pattern-export-source";
import type { PatternContext, PatternRow } from "./backend-patterns";

test("summary exports capture immutable source identity rather than a visible page", () => {
  const context: PatternContext = { searchJobId: "job", snapshotRef: "generation-7", sensitivity: "Balanced" };
  const source = patternExportSource(context);
  context.snapshotRef = "generation-8";
  context.sensitivity = "Broad";
  assert.deepEqual(source, { $case: "patternSummary", value: { searchJobId: "job", snapshotRef: "generation-7", sensitivity: 2 } });
  assert.ok(Object.isFrozen(source));
  assert.ok(Object.isFrozen(source.value));
});

test("member exports capture exact pattern identity and reject display-only demo signatures", () => {
  const context: PatternContext = { searchJobId: "job", snapshotRef: "generation-7", sensitivity: "Precise" };
  const pattern: PatternRow = { patternId: "opaque-group", signature: "literal *", count: 10, percent: 100 };
  const source = patternExportSource(context, pattern);
  pattern.patternId = "other-group";
  assert.deepEqual(source, { $case: "patternMembers", value: { searchJobId: "job", snapshotRef: "generation-7", sensitivity: 1, patternId: "opaque-group" } });
  assert.throws(() => patternExportSource(context, { signature: "literal *", count: 1, percent: 100 }), /exact retained group/u);
  assert.throws(() => patternExportSource({ ...context, snapshotRef: "" }), /retained result snapshot/u);
});
