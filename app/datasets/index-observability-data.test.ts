import assert from "node:assert/strict";
import test from "node:test";

import { ValueType } from "@/gen/ts/open_splunk/value";

import {
  fieldCountLabel,
  mergeIndexFieldPage,
  nextObservedIndexId,
  normalizeIndexObservationQuery,
  retainVisibleObservedIndexId,
} from "./index-observability-data";

const field = (fieldName: string) => ({
  fieldName,
  displayName: fieldName,
  valueType: ValueType.VALUE_TYPE_STRING,
  observedValueTypes: [ValueType.VALUE_TYPE_STRING],
  eventCount: 10n,
  nullCount: 0n,
  missingCount: 2n,
  distinctCount: undefined,
  distinctCountIsApproximate: false,
  selected: false,
  interesting: false,
});

test("mergeIndexFieldPage preserves one exact immutable snapshot", () => {
  const first = mergeIndexFieldPage(null, {
    fields: [field("host")],
    page: { nextPageToken: "cursor-2", totalSize: 2n, totalSizeExact: true },
  });
  assert.equal(fieldCountLabel(first), "1 of 2 fields loaded");

  const complete = mergeIndexFieldPage(first, {
    fields: [field("source")],
    page: { nextPageToken: undefined, totalSize: 2n, totalSizeExact: true },
  }, "cursor-2");
  assert.deepEqual(complete.fields.map((item) => item.fieldName), ["host", "source"]);
  assert.equal(fieldCountLabel(complete), "2 fields");
});

test("mergeIndexFieldPage rejects cursor and field overlap", () => {
  const first = mergeIndexFieldPage(null, {
    fields: [field("host")],
    page: { nextPageToken: "cursor-2", totalSize: undefined, totalSizeExact: false },
  });
  assert.throws(() => mergeIndexFieldPage(first, {
    fields: [field("source")],
    page: { nextPageToken: "cursor-2", totalSize: undefined, totalSizeExact: false },
  }, "cursor-2"), /repeated its page cursor/);
  assert.throws(() => mergeIndexFieldPage(first, {
    fields: [field("host")],
    page: { nextPageToken: undefined, totalSize: undefined, totalSizeExact: false },
  }, "cursor-2"), /repeated field host/);
});

test("normalizeIndexObservationQuery trims applied values and rejects unbounded ranges", () => {
  assert.deepEqual(normalizeIndexObservationQuery({
    earliest: "  -24h ",
    latest: " now ",
    nameFilter: " host ",
  }), { earliest: "-24h", latest: "now", nameFilter: "host" });
  assert.equal(normalizeIndexObservationQuery({
    earliest: "",
    latest: "now",
    nameFilter: "",
  }), null);
  assert.equal(normalizeIndexObservationQuery({
    earliest: "-24h",
    latest: "   ",
    nameFilter: "",
  }), null);
});

test("dataset profile selection toggles and clears when filtered from view", () => {
  assert.equal(nextObservedIndexId(null, "main"), "main");
  assert.equal(nextObservedIndexId("main", "main"), null);
  assert.equal(nextObservedIndexId("main", "audit"), "audit");
  assert.equal(retainVisibleObservedIndexId("main", ["main", "audit"]), "main");
  assert.equal(retainVisibleObservedIndexId("main", ["audit"]), null);
  assert.equal(retainVisibleObservedIndexId(null, ["main"]), null);
});
