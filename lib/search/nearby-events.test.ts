import assert from "node:assert/strict";
import test from "node:test";
import { resolveAbsoluteTimeRange } from "./backend-data";

test("nearby admission retains nanoseconds and accepts submillisecond ranges", () => {
  const earliest = "2026-09-12T09:00:00.123456789Z";
  const latest = "2026-09-12T09:00:00.12345679Z";
  const result = resolveAbsoluteTimeRange(earliest, latest);
  assert.equal(result.earliest, earliest);
  assert.equal(result.latest, latest);
});

import {
  adaptNearbyContext,
  createNearbyDraft,
  createNearbyPreparationGate,
  nearbyBuilderAttached,
  nearbyFieldReference,
  nearbyScalar,
  nearbyScalarLiteral,
  nearbySearch,
  type NearbyContext,
} from "./nearby-events";
import { searchLaunchUrl, readSearchLaunchState } from "./search-launch-state";

const context: NearbyContext = {
  anchorTime: "2026-09-12T09:00:00.123456789Z",
  earliest: "2026-09-12T08:55:00.123456789Z",
  latest: "2026-09-12T09:05:00.123456789Z",
  index: "main",
  host: "edge*node",
  source: "C:\\logs\\a\n\"quoted\"",
  clipped: false,
  fields: [
    { field: "trace_id", scalar: { kind: "string", value: "trace-1" } },
    { field: "span_id", scalar: { kind: "number", value: "18446744073709551615" } },
    { field: "request_id", scalar: { kind: "string", value: "request-1" } },
    { field: "status", scalar: { kind: "number", value: "500" } },
  ],
};

test("nearby default scope uses literal where comparisons and keeps identifiers inactive", () => {
  const draft = createNearbyDraft(context);
  assert.deepEqual(draft.comparisons.map(({ field, enabled }) => [field, enabled]), [
    ["index", true], ["host", true], ["source", true], ["trace_id", false], ["span_id", false], ["request_id", false],
  ]);
  const search = nearbySearch(draft);
  assert.equal(search.query, 'index="main" | where \'index\' = "main" AND \'host\' = "edge*node" AND \'source\' = "C:\\\\logs\\\\a\\n\\"quoted\\""');
  assert.equal(search.timeRange.earliest, context.earliest);
  assert.equal(search.timeRange.latest, context.latest);
});

test("nearby edits batch into one detached search definition without mutating the origin", () => {
  const draft = createNearbyDraft(context);
  const before = structuredClone(draft);
  const edited = { ...draft, comparisons: draft.comparisons.map((item) => item.field === "span_id" ? { ...item, enabled: true } : item) };
  const search = nearbySearch(edited);
  assert.match(search.query, /'span_id' = 18446744073709551615$/u);
  assert.deepEqual(draft, before);
  assert.equal(nearbyBuilderAttached(edited, search.query), true);
  assert.equal(nearbyBuilderAttached(edited, `${search.query} | head 1`), false);
  assert.equal(nearbyBuilderAttached(edited, search.query.replace("AND", "OR")), false);
});

test("nearby scalars preserve uint64 and distinguish textual numbers, null and unsupported cells", () => {
  assert.deepEqual(nearbyScalar({ kind: { $case: "uint64Value", value: 18446744073709551615n } }), { kind: "number", value: "18446744073709551615" });
  assert.deepEqual(nearbyScalar({ kind: { $case: "stringValue", value: "18446744073709551615" } }), { kind: "string", value: "18446744073709551615" });
  assert.equal(nearbyScalar(undefined), null);
  assert.equal(nearbyScalar({ kind: { $case: "listValue", value: { values: [] } } }), null);
  assert.equal(nearbyScalar({ kind: { $case: "doubleValue", value: Number.POSITIVE_INFINITY } }), null);
  assert.equal(nearbyScalarLiteral({ kind: "number", value: "9007199254740993.000001" }), "9007199254740993.000001");
  assert.equal(nearbyScalarLiteral({ kind: "number", value: "18446744073709551616" }), "18446744073709551616.0");
  assert.equal(nearbyScalarLiteral({ kind: "string", value: "*?\"\\\n\r\t" }), '"*?\\"\\\\\\n\\r\\t"');
  for (const value of ["NaN", "Infinity", "1 OR true", "1e999", "", " 1", "01"]) {
    assert.throws(() => nearbyScalarLiteral({ kind: "number", value }), /supported finite number/u);
  }
  assert.throws(() => nearbyScalarLiteral({ kind: "boolean", value: "true OR true" }), /Boolean/u);
});

test("nearby scalar field identifiers safely quote punctuation, keywords and backslashes", () => {
  assert.equal(nearbyFieldReference("odd field'\\\\name|OR"), "'odd field\\'\\\\\\\\name|OR'");
  assert.equal(nearbyFieldReference("true"), "'true'");
  assert.equal(nearbyFieldReference("nested.path"), "'nested.path'");
  for (const field of ["", "x*", "a..b", "a\\b", "x\n", " x", "x.", "x".repeat(257), Array(18).fill("x").join(".")]) {
    assert.throws(() => nearbyFieldReference(field));
  }
  assert.throws(() => nearbyFieldReference("__os_internal"));
  assert.throws(() => nearbyFieldReference("\ud800"));
});

test("nearby admission never silently loses its exact index scope", () => {
  const draft = createNearbyDraft(context);
  assert.throws(() => nearbySearch({ ...draft, comparisons: [] }), /exact index/u);
  assert.throws(() => nearbySearch({ ...draft, comparisons: draft.comparisons.map((item) => item.field === "index" ? Object.assign({}, item, { enabled: false }) : item) }), /exact index/u);
  assert.throws(() => createNearbyDraft({ ...context, index: "" }), /original index/u);
  assert.throws(() => nearbySearch(createNearbyDraft({ ...context, index: "*" })), /exact index/u);
});

test("nanosecond ranges compare offsets, retain trailing zeros, and reject impossible dates", () => {
  const earliest = "2026-09-12T10:00:00.123456780+01:00";
  const latest = "2026-09-12T09:00:00.123456789Z";
  const result = resolveAbsoluteTimeRange(earliest, latest);
  assert.equal(result.earliest, earliest);
  assert.equal(result.latest, latest);
  assert.throws(() => resolveAbsoluteTimeRange(latest, earliest), /before/u);
  assert.throws(() => resolveAbsoluteTimeRange("2026-09-12T08:00:00.1234567891Z", latest), /Invalid/u);
  assert.throws(() => resolveAbsoluteTimeRange("2026-09-12T08:00:00.1234567891+01:00", latest), /Invalid/u);
  assert.throws(() => resolveAbsoluteTimeRange("2026-02-30T00:00:00Z", latest), /Invalid/u);
  assert.throws(() => resolveAbsoluteTimeRange("2026-09-12T09:00:00+24:00", latest), /Invalid/u);
});

test("nearby browser history state retains original nanosecond range text", () => {
  const range = nearbySearch(createNearbyDraft(context)).timeRange;
  const url = searchLaunchUrl(new URL("https://example.test/search?appId=ops"), "q", "new nearby", true, range);
  assert.equal(url.searchParams.get("earliest"), context.earliest);
  assert.equal(url.searchParams.get("latest"), context.latest);
  const retained = readSearchLaunchState({ q: "dirty draft", ...range, searchJobId: "original-job" });
  assert.equal(retained?.earliest, context.earliest);
  assert.equal(retained?.latest, context.latest);
  assert.equal(retained?.q, "dirty draft");
  assert.equal(retained?.searchJobId, "original-job");
});

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}

test("nearby preparation drops late results after navigation even when transport ignores abort", async () => {
  const gate = createNearbyPreparationGate();
  const load = deferred<string>();
  const applied: string[] = [];
  let signal: AbortSignal | undefined;
  const pending = gate.prepare((nextSignal) => { signal = nextSignal; return load.promise; }, (value) => applied.push(value));
  gate.invalidate();
  assert.equal(signal?.aborted, true);
  load.resolve("old");
  assert.equal(await pending, false);
  assert.deepEqual(applied, []);
});

test("new nearby click wins and stale failures cannot replace its results", async () => {
  const gate = createNearbyPreparationGate();
  const old = deferred<string>();
  const applied: string[] = [];
  const first = gate.prepare(() => old.promise, (value) => applied.push(value));
  assert.equal(await gate.prepare(async () => "new", (value) => applied.push(value)), true);
  old.reject(new Error("stale unavailable"));
  assert.equal(await first, false);
  assert.deepEqual(applied, ["new"]);
  await assert.rejects(gate.prepare(async () => { throw new Error("current unavailable"); }, () => undefined), /current unavailable/u);
});


test("nearby response must match selected retained row and exact clipped anchor interval", () => {
  const response = { ...context, searchJobId: "job", rowId: "job:2", fields: [] };
  assert.equal(adaptNearbyContext(response, "job", "job:2").anchorTime, context.anchorTime);
  assert.throws(() => adaptNearbyContext(response, "other", "job:2"), /unavailable/u);
  assert.throws(() => adaptNearbyContext(response, "job", "job:1"), /unavailable/u);
  assert.throws(() => adaptNearbyContext({ ...response, earliest: "2026-09-12T08:50:00.123456789Z" }, "job", "job:2"), /unavailable/u);
  assert.throws(() => adaptNearbyContext({ ...response, clipped: true }, "job", "job:2"), /unavailable/u);
  const clipped = { ...response, anchorTime: "1900-01-01T00:00:00.000000001Z", earliest: "1900-01-01T00:00:00Z", latest: "1900-01-01T00:05:00.000000001Z", clipped: true };
  assert.equal(adaptNearbyContext(clipped, "job", "job:2").clipped, true);
  assert.throws(() => adaptNearbyContext({ ...clipped, clipped: false }, "job", "job:2"), /unavailable/u);
  const upper = { ...response, anchorTime: "2262-01-01T00:00:00Z", earliest: "2261-12-31T23:55:00Z", latest: "2262-01-01T00:00:00Z", clipped: true };
  assert.equal(adaptNearbyContext(upper, "job", "job:2").latest, upper.latest);
});


test("nearby adapter rejects duplicate fields and excludes unsupported scalar paths", () => {
  const field = { fieldName: "trace_id", value: { kind: { $case: "stringValue" as const, value: "trace" } }, suggested: true };
  const response = { ...context, searchJobId: "job", rowId: "job:2", fields: [field] };
  assert.deepEqual(adaptNearbyContext(response, "job", "job:2").fields, [{ field: "trace_id", scalar: { kind: "string", value: "trace" } }]);
  assert.throws(() => adaptNearbyContext({ ...response, fields: [field, field] }, "job", "job:2"), /unavailable/u);
  assert.deepEqual(adaptNearbyContext({ ...response, fields: [{ ...field, fieldName: "wild*field" }, { ...field, fieldName: "empty", value: undefined }] }, "job", "job:2").fields, []);
});
