import assert from "node:assert/strict";
import test from "node:test";

import type { SearchLimits } from "@/gen/ts/open_splunk/server_settings_api";

import { sameSearchLimits, searchLimitsFromForm, searchLimitsToForm } from "./search-limits-form";

const defaults: SearchLimits = {
  maximumRuntime: { seconds: 120n, nanos: 0 },
  maximumMemoryBytes: 1n << 30n,
  maximumRowsToRead: 250_000_000n,
  maximumBytesToRead: 64n << 30n,
  maximumGroupedRows: 10_001n,
  maximumThreads: 4n,
  maximumResultRows: 10_000n,
  maximumResultBytes: 64n << 20n,
  maximumTotalResultBytes: 512n << 20n,
  maximumConcurrentSearches: 4,
  resultRetention: { seconds: 900n, nanos: 0 },
};

test("search limit form formats display units and restores exact protobuf values", () => {
  const form = searchLimitsToForm(defaults);
  assert.deepEqual(form, {
    runtimeSeconds: "120",
    memoryMiB: "1024",
    rowsRead: "250000000",
    bytesReadMiB: "65536",
    groupedRows: "10001",
    threads: "4",
    resultRows: "10000",
    resultBytesMiB: "64",
    totalResultBytesMiB: "512",
    retentionMinutes: "15",
    concurrency: "4",
  });
  assert.deepEqual(searchLimitsFromForm(form), defaults);
});

test("search limit form rejects partial, fractional, zero, and uint32-overflow input", () => {
  const valid = searchLimitsToForm(defaults);
  for (const candidate of ["", "0", "1.5", " 4", "-1"]) {
    assert.equal(searchLimitsFromForm({ ...valid, threads: candidate }), null);
  }
  assert.equal(searchLimitsFromForm({ ...valid, concurrency: "4294967296" }), null);
});

test("unchanged display values preserve legal sub-unit protobuf precision", () => {
  const exact: SearchLimits = {
    ...defaults,
    maximumMemoryBytes: defaults.maximumMemoryBytes + 1n,
    maximumBytesToRead: defaults.maximumBytesToRead + 17n,
    maximumResultBytes: defaults.maximumResultBytes + 3n,
    maximumTotalResultBytes: defaults.maximumTotalResultBytes + 5n,
    maximumRuntime: { seconds: 120n, nanos: 1 },
    resultRetention: { seconds: 901n, nanos: 2 },
  };
  assert.deepEqual(searchLimitsFromForm(searchLimitsToForm(exact), exact), exact);
  assert.equal(sameSearchLimits(exact, defaults), false);
  assert.equal(sameSearchLimits(exact, { ...exact }), true);
});
