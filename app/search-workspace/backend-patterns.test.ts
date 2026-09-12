import assert from "node:assert/strict";
import test from "node:test";
import { ListSearchPatternsResponse, ListSearchPatternMembersResponse, PatternSensitivity } from "@/gen/ts/open_splunk/patterns_api";
import { ValueType } from "@/gen/ts/open_splunk/value";
import { BackendPatterns, type PatternClient, type PatternContext } from "./backend-patterns";

const context: PatternContext = { searchJobId: "job", snapshotRef: "snapshot-7", sensitivity: "Balanced" };

type ResponseOverrides = Partial<Omit<ListSearchPatternsResponse, "page">> & { page?: Partial<NonNullable<ListSearchPatternsResponse["page"]>> };

function groups(overrides: ResponseOverrides = {}) {
  return ListSearchPatternsResponse.fromPartial({
    snapshotRef: context.snapshotRef, algorithmVersion: "1", retainedEventCount: 5n,
    eligibleEventCount: 3n, excludedEventCount: 2n, retainedTruncated: true, snapshotComplete: true,
    patterns: [{ patternId: "pattern-1", signature: "request <int>", eventCount: 3n }],
    page: { totalSize: 1n, totalSizeExact: true }, ...overrides,
  });
}

function members(ordinals: bigint[], nextPageToken?: string) {
  return ListSearchPatternMembersResponse.fromPartial({
    patternId: "pattern-1", resultPage: {
      snapshotRef: context.snapshotRef, snapshotComplete: true,
      schema: { schemaId: "schema-7", columns: [{ fieldName: "_raw", valueType: ValueType.VALUE_TYPE_STRING }] },
      rows: ordinals.map((ordinal) => ({ rowId: `row-${ordinal}`, ordinal, cells: [{ kind: { $case: "stringValue" as const, value: `request ${ordinal}` } }] })),
      page: { totalSize: 3n, totalSizeExact: true, nextPageToken },
    },
  });
}

function client(overrides: Partial<PatternClient> = {}): PatternClient {
  return { patterns: async () => groups(), patternMembers: async () => members([2n, 9n, 14n]), ...overrides };
}

test("coverage uses only eligible retained strings and discloses excluded and truncated rows", async () => {
  const controller = new BackendPatterns(context, client());
  await controller.loadGroups();
  const state = controller.getSnapshot();
  assert.equal(state.error, null);
  assert.equal(state.rows[0]?.percent, 100);
  assert.deepEqual(state.coverage, { retainedRows: 5, eligibleRows: 3, excludedRows: 2, totalGroups: 1, retainedTruncated: true, snapshotComplete: true, algorithmVersion: "1" });
});

test("member paging sends immutable identity and preserves exact original rows", async () => {
  const calls: Parameters<PatternClient["patternMembers"]>[0][] = [];
  const controller = new BackendPatterns(context, client({ patternMembers: async (request) => {
    calls.push(request);
    return request.page?.pageToken ? members([14n]) : members([2n, 9n], "members-next");
  } }));
  await controller.loadGroups();
  await controller.selectPattern(controller.getSnapshot().rows[0]!, 2);
  const first = controller.getSnapshot().members!.page!.rows;
  await controller.loadMembers(2);
  const second = controller.getSnapshot().members!.page!.rows;
  assert.deepEqual([...first, ...second].map((row) => row.rowId), ["row-2", "row-9", "row-14"]);
  assert.deepEqual([...first, ...second].map((row) => row.ordinal), [2n, 9n, 14n]);
  assert.equal(calls[1]?.page?.pageToken, "members-next");
  assert.ok(calls.every((request) => request.snapshotRef === "snapshot-7" && request.searchJobId === "job"
    && request.patternId === "pattern-1" && request.sensitivity === PatternSensitivity.PATTERN_SENSITIVITY_BALANCED
    && request.columns.length === 0));
  controller.clearPattern();
  assert.equal(controller.getSnapshot().members, null);
  assert.equal(controller.getSnapshot().rows[0]?.count, 3);
});

test("clearing a filter suppresses an uncancelable late member response", async () => {
  const { promise: pending, resolve } = Promise.withResolvers<ListSearchPatternMembersResponse>();
  const controller = new BackendPatterns(context, client({ patternMembers: () => pending }));
  await controller.loadGroups();
  const selection = controller.selectPattern(controller.getSnapshot().rows[0]!);
  controller.clearPattern();
  resolve(members([2n, 9n, 14n]));
  await selection;
  assert.equal(controller.getSnapshot().members, null);
});

test("newer group request wins when an older transport ignores AbortSignal", async () => {
  const { promise: pending, resolve } = Promise.withResolvers<ListSearchPatternsResponse>();
  let calls = 0;
  const controller = new BackendPatterns(context, client({ patterns: () => ++calls === 1 ? pending : Promise.resolve(groups()) }));
  const old = controller.loadGroups();
  await controller.loadGroups();
  resolve(groups({ snapshotRef: "wrong-snapshot" }));
  await old;
  assert.equal(controller.getSnapshot().error, null);
  assert.equal(controller.getSnapshot().coverage?.retainedRows, 5);
});

test("group pages retain one denominator and reject repeated cursors atomically", async () => {
  let calls = 0;
  const controller = new BackendPatterns(context, client({ patterns: async () => groups({
    patterns: [{ patternId: `pattern-${++calls}`, signature: `request ${calls}`, eventCount: 1n }],
    page: { totalSize: 3n, totalSizeExact: true, nextPageToken: "repeated" },
  }) }), 1);
  await controller.loadGroups();
  await controller.loadGroups(2);
  assert.match(controller.getSnapshot().error!, /repeated page cursor/u);
  assert.equal(controller.getSnapshot().pageNumber, 1);
  assert.equal(controller.getSnapshot().rows[0]?.patternId, "pattern-1");
});

test("invalid retained counts and changed snapshot never publish rows", async () => {
  await Promise.all([groups({ excludedEventCount: 3n }), groups({ snapshotRef: "other" }), groups({ eligibleEventCount: 100_001n }), groups({ algorithmVersion: "2" })].map(async (response) => {
    const controller = new BackendPatterns(context, client({ patterns: async () => response }));
    await controller.loadGroups();
    assert.ok(controller.getSnapshot().error);
    assert.deepEqual(controller.getSnapshot().rows, []);
  }));
});

test("member metadata and ordinal integrity fail closed", async () => {
  await Promise.all([members([9n, 2n]), members([2n, 2n]), ListSearchPatternMembersResponse.fromPartial({ ...members([2n]), patternId: "other-pattern" })].map(async (response) => {
    const controller = new BackendPatterns(context, client({ patternMembers: async () => response }));
    await controller.loadGroups();
    await controller.selectPattern(controller.getSnapshot().rows[0]!);
    assert.ok(controller.getSnapshot().members?.error);
    assert.equal(controller.getSnapshot().members?.page, null);
  }));
});

test("each sensitivity and snapshot receives independent state and transport identity", async () => {
  await Promise.all((["Precise", "Balanced", "Broad"] as const).map(async (sensitivity) => {
    const requests: Parameters<PatternClient["patterns"]>[0][] = [];
    const controller = new BackendPatterns({ ...context, sensitivity }, client({ patterns: async (request) => { requests.push(request); return groups(); } }));
    assert.equal(controller.getSnapshot().coverage, null);
    await controller.loadGroups();
    assert.equal(requests[0]?.sensitivity, { Precise: 1, Balanced: 2, Broad: 3 }[sensitivity]);
    controller.dispose();
  }));
});

test("byte-short member pages advance by actual row count and reject cross-page repeats", async () => {
  const controller = new BackendPatterns(context, client({ patternMembers: async (request) => request.page?.pageToken
    ? members([2n, 14n]) : members([2n], "short-next") }));
  await controller.loadGroups();
  await controller.selectPattern(controller.getSnapshot().rows[0]!, 20);
  assert.equal(controller.getSnapshot().members?.pageStart, 1);
  await controller.loadMembers(2);
  assert.match(controller.getSnapshot().members?.error ?? "", /invalid retained row/u);
  assert.equal(controller.getSnapshot().members?.pageNumber, 1);
});

test("byte-short pages retain cumulative display offsets", async () => {
  const controller = new BackendPatterns(context, client({ patternMembers: async (request) => request.page?.pageToken
    ? members([9n, 14n]) : members([2n], "short-next") }));
  await controller.loadGroups();
  await controller.selectPattern(controller.getSnapshot().rows[0]!, 20);
  await controller.loadMembers(2);
  assert.equal(controller.getSnapshot().members?.error, null);
  assert.equal(controller.getSnapshot().members?.pageStart, 2);
  assert.equal(controller.getSnapshot().members?.hasNextPage, false);
});

test("terminal member pages cannot silently omit known members", async () => {
  await Promise.all([[], [2n], [2n, 9n]].map(async (ordinals) => {
    const controller = new BackendPatterns(context, client({ patternMembers: async () => members(ordinals) }));
    await controller.loadGroups();
    await controller.selectPattern(controller.getSnapshot().rows[0]!);
    assert.match(controller.getSnapshot().members?.error ?? "", /inconsistent page extent/u);
    assert.equal(controller.getSnapshot().members?.page, null);
  }));
});
