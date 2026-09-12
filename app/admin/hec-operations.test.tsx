import assert from "node:assert/strict";
import test from "node:test";
import { renderToStaticMarkup } from "react-dom/server";

import { GetHECOperationalSnapshotResponse } from "@/gen/ts/open_splunk/hec_admin_api";
import { GetSystemBootstrapResponse, ServerFeature } from "@/gen/ts/open_splunk/system_api";
import type { OpenSplunkApiClient } from "@/lib/api";
import { adaptSystemBootstrap } from "@/lib/api/system-bootstrap";

import { BackendServerSettings } from "./backend-admin-panels";

const UINT64_MAX = 18_446_744_073_709_551_615n;
const SHAPE_BOUNDS = [
  1n, 10n, 64n, 100n, 500n, 1_000n, 2_500n, 5_000n, 10_000n, 16_384n,
  32_768n, 50_000n, 65_536n,
];
const BYTE_BOUNDS = [
  1_024n, 4_096n, 16_384n, 65_536n, 262_144n, 1_048_576n, 4_194_304n,
  8_388_608n, 16_777_216n, 25_165_824n, 33_554_432n, 50_331_648n, 67_108_864n,
];
const LATENCY_BOUNDS = [
  1_000n, 10_000n, 50_000n, 200_000n, 1_000_000n, 5_000_000n, 30_000_000n,
];

const bootstrap = adaptSystemBootstrap(GetSystemBootstrapResponse.fromPartial({
  features: [ServerFeature.SERVER_FEATURE_HEC_INGESTION],
  serverTime: new Date("2026-09-12T12:00:00Z"),
}));

function histogram(upperBounds = SHAPE_BOUNDS, seed = 100n) {
  return {
    upperBounds,
    bucketCounts: Array.from({ length: 14 }, (_, index) => seed + BigInt(index)),
    count: seed + 20n,
    sum: seed + 21n,
    max: seed + 22n,
  };
}

function completeSnapshot() {
  return GetHECOperationalSnapshotResponse.fromPartial({
    observedAt: new Date("2026-09-12T12:00:00Z"),
    request: {
      requests: 0n,
      acceptedRequests: UINT64_MAX,
      events: 2n,
      uncompressedBytes: 3n,
      authenticationFailures: 4n,
      decodeFailures: 5n,
      eventPolicyFailures: 6n,
      rateLimitedRequests: 7n,
      stagingFailures: 8n,
      stagingDuration: { seconds: 0n, nanos: 0 },
      shutdownRejections: 9n,
    },
    durable: {
      queueAvailable: true,
      requestCapacityAvailable: true,
      pendingOutboxReservations: 10n,
      pendingOutboxBytes: 11n,
      oldestPendingOutboxAge: { seconds: 0n, nanos: 0 },
      retainedRequests: 12n,
      pendingMetadataBytes: 13n,
      pendingUngrouped: 14n,
      readyWriteGroups: 15n,
      ambiguousWriteGroups: 16n,
      liveWriteGroupLeases: 17n,
    },
    reconciliation: {
      available: true,
      successes: 18n,
      retries: 19n,
      ambiguities: 20n,
      stagedLogicalBatches: 21n,
      stagedLogicalRows: 22n,
      formedWriteGroups: 23n,
      physicalInsertSends: 24n,
      successfulWriteGroups: 25n,
      writeGroupMemberBatches: 26n,
      writeGroupRows: 27n,
      writeGroupDecodedBytes: 28n,
      writeGroupMonthlyPartitions: 29n,
      fillRowTarget: 30n,
      fillByteTarget: 31n,
      fillHardBoundary: 32n,
      fillLinger: 33n,
      fillDrain: 34n,
      fillRecovery: 35n,
      nativeWaiters: 0n,
      peakNativeWaiters: UINT64_MAX,
      nativeWaiterWakeups: 36n,
      nativeWaiterCancellations: 37n,
      nativeTerminalLookups: 38n,
      latencyUpperBoundsMicroseconds: LATENCY_BOUNDS,
      sealLatencyBuckets: [0n, 1n, 2n, 3n, 4n, 5n, 6n, 7n],
      sendLatencyBuckets: [7n, 6n, 5n, 4n, 3n, 2n, 1n, 0n],
      commitLatencyBuckets: [8n, 9n, 10n, 11n, 12n, 13n, 14n, 15n],
      memberBatchesPerGroup: histogram(SHAPE_BOUNDS, 100n),
      rowsPerGroup: histogram(SHAPE_BOUNDS, 200n),
      decodedBytesPerGroup: histogram(BYTE_BOUNDS, 300n),
      monthlyPartitionsPerGroup: histogram(SHAPE_BOUNDS, 400n),
      rowsPerPhysicalInsert: histogram(SHAPE_BOUNDS, 500n),
    },
    acknowledgments: { available: true },
    protocolFailures: [{ code: 4, count: 48n }],
  });
}

function renderSnapshot(snapshot = completeSnapshot()): string {
  return renderToStaticMarkup(
    <BackendServerSettings
      bootstrap={bootstrap}
      client={{} as OpenSplunkApiClient}
      error={null}
      hecError={null}
      hecSnapshot={snapshot}
      hecState="available"
      onDirtyChange={() => {}}
      onReload={() => {}}
      onStatus={() => {}}
    />,
  );
}

function definitionValue(markup: string, label: string): string {
  const escaped = label.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
  const match = new RegExp(`<dt>${escaped}</dt><dd>([^<]+)</dd>`, "u").exec(markup);
  assert.ok(match, `missing definition ${label}`);
  return match[1]!;
}

function labelledSection(markup: string, label: string): string {
  const escaped = label.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
  const match = new RegExp(`<section[^>]+aria-label="${escaped}"[^>]*>(.*?)</section>`, "u").exec(markup);
  assert.ok(match, `missing section ${label}`);
  return match[1]!;
}

function tableRow(markup: string, label: string): string[] {
  const escaped = label.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
  const match = new RegExp(`<tr><td>${escaped}</td>((?:<td[^>]*>[^<]+</td>)+)</tr>`, "u").exec(markup);
  assert.ok(match, `missing table row ${label}`);
  return Array.from(match[1]!.matchAll(/<td[^>]*>([^<]+)<\/td>/gu), (cell) => cell[1]!);
}

test("HEC operations renders every coalescing field with exact bigint and zero values", () => {
  const markup = renderSnapshot();
  const expectedDefinitions = new Map<string, string>([
    ["Requests", "0"],
    ["Accepted requests", "18,446,744,073,709,551,615"],
    ["Events", "2"],
    ["Uncompressed bytes", "3"],
    ["Authentication failures", "4"],
    ["Decode failures", "5"],
    ["Event-policy failures", "6"],
    ["Rate-limited requests", "7"],
    ["Staging failures", "8"],
    ["Staging duration", "0 seconds"],
    ["Shutdown rejections", "9"],
    ["Durable queue", "Available"],
    ["Request capacity", "Available"],
    ["Pending outbox reservations", "10"],
    ["Pending outbox bytes", "11"],
    ["Pending metadata bytes", "13"],
    ["Pending ungrouped requests", "14"],
    ["Ready write groups", "15"],
    ["Ambiguous write groups", "16"],
    ["Live write-group leases", "17"],
    ["Oldest pending age", "0 seconds"],
    ["Retained requests", "12"],
    ["Reconciliation", "Available"],
    ["Reconciliation successes", "18"],
    ["Reconciliation retries", "19"],
    ["Reconciliation ambiguities", "20"],
    ["Staged logical batches", "21"],
    ["Staged logical rows", "22"],
    ["Formed write groups", "23"],
    ["Physical insert sends", "24"],
    ["Successful write groups", "25"],
    ["Write-group member batches", "26"],
    ["Write-group rows", "27"],
    ["Write-group decoded bytes", "28"],
    ["Write-group monthly partitions", "29"],
    ["Row target", "30"],
    ["Byte target", "31"],
    ["Hard boundary", "32"],
    ["Linger", "33"],
    ["Drain", "34"],
    ["Recovery", "35"],
    ["Current native waiters", "0"],
    ["Peak native waiters", "18,446,744,073,709,551,615"],
    ["Native waiter wakeups", "36"],
    ["Native waiter cancellations", "37"],
    ["Native terminal lookups", "38"],
    ["ACK service", "Available"],
    ["Active ACK channels", "0"],
    ["Retained ACK channels", "0"],
    ["Pending ACK rows", "0"],
    ["Indexed ACK rows", "0"],
    ["Expired ACK rows", "0"],
    ["Terminal failed requests", "0"],
    ["ACK queries", "0"],
    ["ACK IDs queried", "0"],
    ["ACK query misses", "0"],
    ["Response code 4", "48"],
  ]);
  for (const [label, value] of expectedDefinitions) {
    assert.equal(definitionValue(markup, label), value, label);
  }
  for (const histogramName of [
    "Member batches per group",
    "Rows per group",
    "Decoded bytes per group",
    "Monthly partitions per group",
    "Rows per physical insert",
  ]) assert.match(markup, new RegExp(histogramName));

  assert.match(markup, /<section[^>]+aria-labelledby="hec-coalescing-title"/);
  assert.match(markup, /<caption[^>]*>Write-group seal, send, and commit latency buckets<\/caption>/);
  assert.equal((markup.match(/<caption/g) ?? []).length, 6);
  assert.equal((markup.match(/<tr/g) ?? []).length, 84, "fixed shapes bound rendered table rows");
  assert.match(markup, /0 through 1 batches/);
  assert.match(markup, /More than 1 through 10 batches/);
  assert.match(markup, /More than 65,536 batches/);
  assert.deepEqual(tableRow(markup, "0 through 1,000 microseconds"), ["0", "7", "8"]);
  assert.deepEqual(tableRow(markup, "More than 1,000 through 10,000 microseconds"), ["1", "6", "9"]);
  for (const [name, samples, sum, maximum, firstBucket] of [
    ["Member batches per group", "120", "121", "122", "100"],
    ["Rows per group", "220", "221", "222", "200"],
    ["Decoded bytes per group", "320", "321", "322", "300"],
    ["Monthly partitions per group", "420", "421", "422", "400"],
    ["Rows per physical insert", "520", "521", "522", "500"],
  ]) {
    const section = labelledSection(markup, name);
    assert.equal(definitionValue(section, "Samples"), samples, `${name} samples`);
    assert.equal(definitionValue(section, "Sum"), sum, `${name} sum`);
    assert.equal(definitionValue(section, "Maximum"), maximum, `${name} maximum`);
    assert.equal(tableRow(section, new RegExp("^Decoded", "u").test(name)
      ? "0 through 1,024 bytes"
      : new RegExp("^Member", "u").test(name)
        ? "0 through 1 batches"
        : new RegExp("Monthly", "u").test(name)
          ? "0 through 1 partitions"
          : "0 through 1 rows")[0], firstBucket, `${name} first bucket`);
  }
});

test("HEC operations rejects malformed fixed bounds and bucket shapes", () => {
  const wrongBounds = completeSnapshot();
  wrongBounds.reconciliation!.latencyUpperBoundsMicroseconds = [1n];
  wrongBounds.reconciliation!.memberBatchesPerGroup!.bucketCounts = Array.from(
    { length: 1_000 },
    () => 1n,
  );
  const markup = renderSnapshot(wrongBounds);
  assert.match(markup, /HEC operational distributions are unavailable/);
  assert.match(markup, /fixed latency bounds and histogram shapes/);
  assert.equal(markup.includes("1,000 buckets"), false);
  assert.equal(markup.includes("Member batches per group"), false);
});

test("HEC histogram display accepts a concurrent sample whose buckets do not sum to count", () => {
  const snapshot = completeSnapshot();
  snapshot.reconciliation!.rowsPerGroup!.count = 1n;
  snapshot.reconciliation!.rowsPerGroup!.bucketCounts = Array.from(
    { length: 14 },
    (_, index) => BigInt(index + 1),
  );
  const markup = renderSnapshot(snapshot);
  assert.equal(markup.includes("HEC operational distributions are unavailable"), false);
  assert.match(markup, /Rows per group/);
});

test("HEC operations preserves exact large durations and distinguishes absent metrics", () => {
  const snapshot = completeSnapshot();
  snapshot.request!.stagingDuration = { seconds: 315_576_000_000n, nanos: 123_456_789 };
  snapshot.durable!.queueAvailable = false;
  snapshot.durable!.requestCapacityAvailable = false;
  snapshot.reconciliation!.available = false;
  snapshot.acknowledgments!.available = false;
  const markup = renderSnapshot(snapshot);
  assert.equal(
    definitionValue(markup, "Staging duration"),
    "315,576,000,000.123456789 seconds",
  );
  assert.equal(definitionValue(markup, "Durable queue"), "Unavailable");
  assert.equal(definitionValue(markup, "Request capacity"), "Unavailable");
  assert.equal(definitionValue(markup, "Pending outbox reservations"), "10");
  assert.equal(definitionValue(markup, "Reconciliation"), "Unavailable");
  assert.equal(definitionValue(markup, "Reconciliation successes"), "18");
  assert.equal(definitionValue(markup, "ACK service"), "Unavailable");
  assert.equal(definitionValue(markup, "Active ACK channels"), "0");

  snapshot.acknowledgments = undefined;
  const absentMarkup = renderSnapshot(snapshot);
  assert.equal(definitionValue(absentMarkup, "ACK service"), "Not reported");
  assert.equal(definitionValue(absentMarkup, "Active ACK channels"), "Not reported");
});

test("HEC operations preserves uint64 maximums across every scalar family", () => {
  const snapshot = completeSnapshot();
  Object.assign(snapshot.request!, {
    requests: UINT64_MAX,
    events: UINT64_MAX,
    uncompressedBytes: UINT64_MAX,
    authenticationFailures: UINT64_MAX,
    decodeFailures: UINT64_MAX,
    eventPolicyFailures: UINT64_MAX,
    acceptedRequests: UINT64_MAX,
    rateLimitedRequests: UINT64_MAX,
    stagingFailures: UINT64_MAX,
    shutdownRejections: UINT64_MAX,
  });
  Object.assign(snapshot.durable!, {
    pendingOutboxReservations: UINT64_MAX,
    pendingOutboxBytes: UINT64_MAX,
    retainedRequests: UINT64_MAX,
    pendingMetadataBytes: UINT64_MAX,
    pendingUngrouped: UINT64_MAX,
    readyWriteGroups: UINT64_MAX,
    ambiguousWriteGroups: UINT64_MAX,
    liveWriteGroupLeases: UINT64_MAX,
  });
  const reconciliation = snapshot.reconciliation!;
  for (const field of [
    "successes",
    "retries",
    "ambiguities",
    "stagedLogicalBatches",
    "stagedLogicalRows",
    "formedWriteGroups",
    "physicalInsertSends",
    "successfulWriteGroups",
    "writeGroupMemberBatches",
    "writeGroupRows",
    "writeGroupDecodedBytes",
    "writeGroupMonthlyPartitions",
    "fillRowTarget",
    "fillByteTarget",
    "fillHardBoundary",
    "fillLinger",
    "fillDrain",
    "fillRecovery",
    "nativeWaiters",
    "peakNativeWaiters",
    "nativeWaiterWakeups",
    "nativeWaiterCancellations",
    "nativeTerminalLookups",
  ] as const) reconciliation[field] = UINT64_MAX;
  Object.assign(snapshot.acknowledgments!, {
    activeChannels: UINT64_MAX,
    retainedChannels: UINT64_MAX,
    pendingRows: UINT64_MAX,
    indexedRows: UINT64_MAX,
    expiredRows: UINT64_MAX,
    terminalFailedRequests: UINT64_MAX,
    queries: UINT64_MAX,
    idsQueried: UINT64_MAX,
    misses: UINT64_MAX,
  });
  reconciliation.sealLatencyBuckets.fill(UINT64_MAX);
  reconciliation.sendLatencyBuckets.fill(UINT64_MAX);
  reconciliation.commitLatencyBuckets.fill(UINT64_MAX);
  for (const fixed of [
    reconciliation.memberBatchesPerGroup!,
    reconciliation.rowsPerGroup!,
    reconciliation.decodedBytesPerGroup!,
    reconciliation.monthlyPartitionsPerGroup!,
    reconciliation.rowsPerPhysicalInsert!,
  ]) {
    fixed.bucketCounts.fill(UINT64_MAX);
    fixed.count = UINT64_MAX;
    fixed.sum = UINT64_MAX;
    fixed.max = UINT64_MAX;
  }
  snapshot.protocolFailures[0]!.count = UINT64_MAX;
  const markup = renderSnapshot(snapshot);
  assert.equal(markup.includes("HEC operational snapshot is unavailable"), false);
  assert.equal(markup.includes("HEC operational distributions are unavailable"), false);
  for (const label of [
    "Requests",
    "Pending metadata bytes",
    "Write-group decoded bytes",
    "Peak native waiters",
    "ACK query misses",
    "Response code 4",
  ]) assert.equal(definitionValue(markup, label), "18,446,744,073,709,551,615");
  assert.ok((markup.match(/18,446,744,073,709,551,615/g) ?? []).length >= 55);
});

test("HEC operations rejects adversarial fixed distributions without expanding them", () => {
  const mutations: Array<(snapshot: ReturnType<typeof completeSnapshot>) => void> = [
    (snapshot) => { snapshot.reconciliation!.latencyUpperBoundsMicroseconds[0] = 999n; },
    (snapshot) => { snapshot.reconciliation!.latencyUpperBoundsMicroseconds.reverse(); },
    (snapshot) => { snapshot.reconciliation!.sealLatencyBuckets[0] = -1n; },
    (snapshot) => { snapshot.reconciliation!.sendLatencyBuckets[0] = UINT64_MAX + 1n; },
    (snapshot) => { snapshot.reconciliation!.memberBatchesPerGroup = undefined; },
    (snapshot) => { snapshot.reconciliation!.rowsPerGroup!.upperBounds.reverse(); },
    (snapshot) => { snapshot.reconciliation!.decodedBytesPerGroup!.bucketCounts[0] = -1n; },
    (snapshot) => { snapshot.reconciliation!.monthlyPartitionsPerGroup!.count = UINT64_MAX + 1n; },
    (snapshot) => { snapshot.reconciliation!.rowsPerPhysicalInsert!.sum = -1n; },
    (snapshot) => { snapshot.reconciliation!.memberBatchesPerGroup!.max = UINT64_MAX + 1n; },
  ];
  for (const mutate of mutations) {
    const snapshot = completeSnapshot();
    mutate(snapshot);
    const markup = renderSnapshot(snapshot);
    assert.match(markup, /HEC operational distributions are unavailable/);
    assert.equal(markup.includes("Member batches per group"), false);
    assert.equal((markup.match(/<tr/g) ?? []).length, 0);
    assert.equal(definitionValue(markup, "Requests"), "0");
  }
});

test("HEC operations rejects invalid or unbounded protocol failure domains", () => {
  const mutations: Array<(snapshot: ReturnType<typeof completeSnapshot>) => void> = [
    (snapshot) => { snapshot.protocolFailures[0]!.code = 0; },
    (snapshot) => { snapshot.protocolFailures[0]!.code = 28; },
    (snapshot) => { snapshot.protocolFailures.push({ code: 4, count: 1n }); },
    (snapshot) => {
      snapshot.protocolFailures = Array.from({ length: 28 }, (_, index) => ({
        code: (index % 27) + 1,
        count: 0n,
      }));
    },
    (snapshot) => { snapshot.protocolFailures[0]!.count = -1n; },
  ];
  for (const mutate of mutations) {
    const snapshot = completeSnapshot();
    mutate(snapshot);
    const markup = renderSnapshot(snapshot);
    assert.match(markup, /HEC operational snapshot is unavailable/);
    assert.equal(markup.includes("Response code"), false);
  }
});
