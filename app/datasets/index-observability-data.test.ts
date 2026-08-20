import assert from "node:assert/strict";
import test from "node:test";

import { ValueType } from "@/gen/ts/open_splunk/value";

import {
  fieldCountLabel,
  formatStorageBytes,
  mergeIndexFieldPage,
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

test("formatStorageBytes uses binary storage units", () => {
  assert.equal(formatStorageBytes(0n), "0 B");
  assert.equal(formatStorageBytes(1_536n), "1.5 KiB");
  assert.equal(formatStorageBytes(1_073_741_824n), "1 GiB");
});
