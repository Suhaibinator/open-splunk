import assert from "node:assert/strict";
import test from "node:test";

import type { SearchLimits } from "@/gen/ts/open_splunk/server_settings_api";

import {
  SEARCH_LIMIT_FIELDS,
  SEARCH_LIMIT_KEYS,
  sameSearchLimits,
  searchLimitErrors,
  searchLimitFieldHint,
  searchLimitsFromForm,
  searchLimitsToForm,
} from "./search-limits-form";

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

const minimums: SearchLimits = {
  maximumRuntime: { seconds: 1n, nanos: 0 },
  maximumMemoryBytes: 1n << 20n,
  maximumRowsToRead: 1n,
  maximumBytesToRead: 1n << 20n,
  maximumGroupedRows: 1n,
  maximumThreads: 1n,
  maximumResultRows: 1n,
  maximumResultBytes: 1n << 20n,
  maximumTotalResultBytes: 1n << 20n,
  maximumConcurrentSearches: 1,
  resultRetention: { seconds: 60n, nanos: 0 },
};

const maximums: SearchLimits = {
  maximumRuntime: { seconds: 3_600n, nanos: 0 },
  maximumMemoryBytes: 64n << 30n,
  maximumRowsToRead: 1_000_000_000n,
  maximumBytesToRead: 1n << 40n,
  maximumGroupedRows: 100_000_000n,
  maximumThreads: 64n,
  maximumResultRows: 1_000_000n,
  maximumResultBytes: 1n << 30n,
  maximumTotalResultBytes: 8n << 30n,
  maximumConcurrentSearches: 64,
  resultRetention: { seconds: 86_400n, nanos: 0 },
};

test("a byte field holds a quantity that names its own unit", () => {
  // The defect: four fields were labelled "…bytes" and held mebibytes, because
  // the form divided the protobuf value by a mebibyte and only the grey hint
  // underneath said so.
  const form = searchLimitsToForm(defaults);
  assert.deepEqual(form, {
    runtimeSeconds: "120",
    memory: "1 GiB",
    rowsRead: "250000000",
    bytesRead: "64 GiB",
    groupedRows: "10001",
    threads: "4",
    resultRows: "10000",
    resultBytes: "64 MiB",
    totalResultBytes: "512 MiB",
    retentionMinutes: "15",
    concurrency: "4",
  });
  assert.deepEqual(searchLimitsFromForm(form), defaults);
});

test("no byte field's label states a unit the value does not carry", () => {
  // A label that also named a scale would be the second place a scale is
  // stated, which is the shape the original defect had.
  for (const key of SEARCH_LIMIT_KEYS) {
    const field = SEARCH_LIMIT_FIELDS[key];
    if (field.kind !== "bytes") continue;
    assert.equal(field.unit, "", key);
    assert.doesNotMatch(field.label, /byte|MiB|MB|GiB/iu, key);
  }
});

test("a byte field accepts every notation the parser does, and reports what it read", () => {
  const form = { ...searchLimitsToForm(defaults), memory: "2048MB" };
  assert.equal(searchLimitErrors(form, minimums, maximums).memory, null);
  // `MB` is 1,000,000, as it is in the collector's configuration file; the echo
  // is what keeps that from being a silent reinterpretation of what was typed.
  assert.equal(searchLimitsFromForm(form)?.maximumMemoryBytes, 2_048_000_000n);
  assert.match(
    searchLimitFieldHint("memory", form, defaults, minimums, maximums),
    /^2,048,000,000 bytes\. /u,
  );
  assert.equal(
    searchLimitsFromForm({ ...form, memory: "2048MiB" })?.maximumMemoryBytes,
    2n << 30n,
  );
});

test("a byte field that is already canonical explains nothing extra", () => {
  const form = { ...searchLimitsToForm(defaults), memory: "2 GiB" };
  assert.equal(
    searchLimitFieldHint("memory", form, defaults, minimums, maximums),
    "1 MiB–64 GiB; default 1 GiB.",
  );
});

test("a range reads back in the notation the field is entered in", () => {
  const form = { ...searchLimitsToForm(defaults), memory: "1 KiB", threads: "999" };
  const errors = searchLimitErrors(form, minimums, maximums);
  assert.equal(errors.memory, "Enter 1 MiB–64 GiB.");
  assert.equal(errors.threads, "Enter 1–64 threads.");
});

test("an unparseable value is told what shape the field takes", () => {
  const form = { ...searchLimitsToForm(defaults), memory: "lots", threads: "1.5" };
  const errors = searchLimitErrors(form, minimums, maximums);
  assert.equal(errors.memory, "Enter a size such as 512 MiB, or a plain number of bytes.");
  assert.equal(errors.threads, "Enter a whole number greater than zero.");
});

test("a valid form reports no field in error", () => {
  const errors = searchLimitErrors(searchLimitsToForm(defaults), minimums, maximums);
  assert.deepEqual(SEARCH_LIMIT_KEYS.filter((key) => errors[key] !== null), []);
});

test("the per-job ceiling is reported on the total, which is the field that has to move", () => {
  const form = { ...searchLimitsToForm(defaults), resultBytes: "1 GiB", totalResultBytes: "512 MiB" };
  const errors = searchLimitErrors(form, minimums, maximums);
  assert.equal(errors.resultBytes, null);
  assert.equal(errors.totalResultBytes, "Enter at least the per-job limit of 1 GiB.");
});

test("a total that is out of range keeps its own range error rather than the cross-field one", () => {
  const form = { ...searchLimitsToForm(defaults), resultBytes: "1 GiB", totalResultBytes: "16 GiB" };
  assert.equal(searchLimitErrors(form, minimums, maximums).totalResultBytes, "Enter 1 MiB–8 GiB.");
});

test("an out-of-range per-job value does not put an impossible demand on the total", () => {
  // The cross-field rule compared against a per-job value that was itself over
  // its ceiling, so 16 GiB per job told the total -- untouched, valid, and
  // capped at 8 GiB -- to "enter at least 16 GiB", which it cannot do.
  const form = { ...searchLimitsToForm(defaults), resultBytes: "16 GiB" };
  const errors = searchLimitErrors(form, minimums, maximums);
  assert.equal(errors.resultBytes, "Enter 1 MiB–1 GiB.");
  assert.equal(errors.totalResultBytes, null);
});

test("search limit form rejects partial, fractional, zero, and uint32-overflow input", () => {
  const valid = searchLimitsToForm(defaults);
  for (const candidate of ["", "0", "1.5", " 4", "-1"]) {
    assert.equal(searchLimitsFromForm({ ...valid, threads: candidate }), null, candidate);
  }
  assert.equal(searchLimitsFromForm({ ...valid, concurrency: "4294967296" }), null);
});

test("a byte value that sits on no unit round-trips without an exact base", () => {
  // This is what replaced the `exactBase` restoration the four byte fields used
  // to need: the display no longer rounds, so there is nothing to restore.
  const exact: SearchLimits = {
    ...defaults,
    maximumMemoryBytes: defaults.maximumMemoryBytes + 1n,
    maximumBytesToRead: defaults.maximumBytesToRead + 17n,
    maximumResultBytes: defaults.maximumResultBytes + 3n,
    maximumTotalResultBytes: defaults.maximumTotalResultBytes + 5n,
  };
  const form = searchLimitsToForm(exact);
  assert.equal(form.memory, "1073741825 B");
  assert.deepEqual(searchLimitsFromForm(form), exact);
});

test("an untouched duration field keeps the sub-unit precision its display cannot show", () => {
  // Seconds cannot state nanoseconds and whole minutes cannot state 901 seconds,
  // so these two still need the exact base: opening the page and saving an
  // unrelated field must not quietly round either one.
  const exact: SearchLimits = {
    ...defaults,
    maximumRuntime: { seconds: 120n, nanos: 1 },
    resultRetention: { seconds: 901n, nanos: 2 },
  };
  assert.deepEqual(searchLimitsFromForm(searchLimitsToForm(exact), exact), exact);
  assert.equal(sameSearchLimits(exact, defaults), false);
  assert.equal(sameSearchLimits(exact, { ...exact }), true);
});

test("an edited duration field is taken at face value rather than restored", () => {
  const exact: SearchLimits = { ...defaults, resultRetention: { seconds: 901n, nanos: 2 } };
  const edited = { ...searchLimitsToForm(exact), retentionMinutes: "30" };
  assert.deepEqual(searchLimitsFromForm(edited, exact)?.resultRetention, { seconds: 1_800n, nanos: 0 });
});
