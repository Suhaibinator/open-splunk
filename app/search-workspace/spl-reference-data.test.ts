import assert from "node:assert/strict";
import test from "node:test";

import {
  SPL_FUNCTIONS,
  SPL_KEYWORDS,
  SPL_PIPELINE_COMMANDS,
  UNSUPPORTED_SPL_PIPELINE_COMMANDS,
} from "@/lib/search/spl-syntax";

import {
  SPL_REFERENCE_SECTIONS,
  filterSplReference,
  referenceEntryMatches,
} from "./spl-reference-data";

const allEntries = SPL_REFERENCE_SECTIONS.flatMap((section) => section.entries);

test("every catalog entry appears in the reference exactly once", () => {
  const counts = new Map<string, number>();
  for (const entry of allEntries) counts.set(entry.id, (counts.get(entry.id) ?? 0) + 1);
  const duplicates = Array.from(counts).filter(([, count]) => count > 1).map(([id]) => id);
  assert.deepEqual(duplicates, []);

  const names = (section: string) => allEntries.filter((entry) => entry.section === section).map((entry) => entry.name);
  assert.deepEqual(names("commands"), SPL_PIPELINE_COMMANDS.map((command) => command.name));
  assert.deepEqual(
    names("aggregate-functions"),
    SPL_FUNCTIONS.filter((fn) => fn.class === "aggregate").map((fn) => fn.name),
  );
  assert.deepEqual(
    names("scalar-functions"),
    SPL_FUNCTIONS.filter((fn) => fn.class === "scalar").map((fn) => fn.name),
  );
  assert.deepEqual(names("keywords"), SPL_KEYWORDS.map((keyword) => keyword.name));
  assert.equal(
    allEntries.length,
    SPL_PIPELINE_COMMANDS.length + SPL_FUNCTIONS.length + SPL_KEYWORDS.length + UNSUPPORTED_SPL_PIPELINE_COMMANDS.length,
  );
});

test("commands carry the catalog's syntax and documentation", () => {
  for (const entry of allEntries.filter((candidate) => candidate.section === "commands")) {
    assert.ok(entry.syntax !== undefined && entry.syntax.length > 0, `${entry.name} has no syntax`);
    assert.ok(entry.documentation !== undefined && entry.documentation.length > 0, `${entry.name} has no documentation`);
    assert.equal(entry.supported, true);
    assert.notEqual(entry.insertion, null);
  }
});

test("unsupported commands are flagged, cannot be inserted, and name an alternative", () => {
  const unsupported = allEntries.filter((entry) => entry.section === "unsupported");
  assert.deepEqual(unsupported.map((entry) => entry.name), [...UNSUPPORTED_SPL_PIPELINE_COMMANDS]);
  for (const entry of unsupported) {
    assert.equal(entry.supported, false);
    assert.equal(entry.insertion, null);
    assert.match(entry.detail, /stats/u);
  }
  assert.match(unsupported.find((entry) => entry.name === "transaction")!.detail, /correlation field/u);
});

test("the filter matches on name and on detail, case-insensitively", () => {
  const byName = filterSplReference(SPL_REFERENCE_SECTIONS, "MVEXPAND");
  assert.deepEqual(byName.map((section) => section.id), ["commands"]);
  assert.deepEqual(byName[0]!.entries.map((entry) => entry.name), ["mvexpand"]);

  const byDetail = filterSplReference(SPL_REFERENCE_SECTIONS, "least frequent");
  assert.deepEqual(byDetail.flatMap((section) => section.entries.map((entry) => entry.name)), ["rare"]);

  const stats = allEntries.find((entry) => entry.id === "command-stats")!;
  assert.equal(referenceEntryMatches(stats, "  Aggregate "), true);
  assert.equal(referenceEntryMatches(stats, "zzz-no-such-text"), false);
});

test("an empty filter keeps every section and a miss keeps none", () => {
  assert.deepEqual(
    filterSplReference(SPL_REFERENCE_SECTIONS, "   ").map((section) => section.entries.length),
    SPL_REFERENCE_SECTIONS.map((section) => section.entries.length),
  );
  assert.deepEqual(filterSplReference(SPL_REFERENCE_SECTIONS, "no such entry anywhere"), []);
});
