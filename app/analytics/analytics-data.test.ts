import assert from "node:assert/strict";
import test from "node:test";

import type { SearchHistoryEntry } from "@/gen/ts/open_splunk/history";
import { SearchJobState } from "@/gen/ts/open_splunk/search";
import { ValueType } from "@/gen/ts/open_splunk/value";

import {
  adaptAnalyticsFields,
  adaptAnalyticsHistoryEntry,
  deriveAnalyticsWorkload,
  loadAnalyticsFields,
  loadAnalyticsHistory,
} from "./analytics-data";

const start = new Date("2026-08-25T00:00:00.000Z");
const end = new Date("2026-08-26T00:00:00.000Z");

function historyEntry(
  id: string,
  createdAt: string,
  values: Partial<SearchHistoryEntry> = {},
): SearchHistoryEntry {
  return {
    searchJobId: id,
    definition: {
      spl: `index=main id=${id}`,
      timeRange: { earliest: "-24h", latest: "now", timezone: "UTC" },
      appId: "search",
      indexScope: ["main"],
      preferredResultTab: 0,
      selectedFields: [],
      visualization: undefined,
    },
    source: undefined,
    effectiveIndexScope: ["main"],
    resolvedTimeRange: undefined,
    finalState: SearchJobState.SEARCH_JOB_STATE_COMPLETED,
    matchedEvents: 5n,
    scannedRows: 10n,
    scannedBytes: 100n,
    producedRows: 2n,
    duration: { seconds: 1n, nanos: 0 },
    warnings: [],
    failure: undefined,
    createdAt: new Date(createdAt),
    startedAt: undefined,
    finishedAt: new Date(createdAt),
    knowledgeSnapshot: undefined,
    ...values,
  };
}

function field(fieldName: string, eventCount: bigint, missingCount: bigint) {
  return {
    fieldName,
    displayName: fieldName,
    valueType: ValueType.VALUE_TYPE_STRING,
    observedValueTypes: [ValueType.VALUE_TYPE_STRING],
    eventCount,
    nullCount: 0n,
    missingCount,
    distinctCount: undefined,
    distinctCountIsApproximate: false,
    selected: false,
    interesting: true,
  };
}

test("history adaptation preserves absent measurements instead of inventing zero duration", () => {
  const adapted = adaptAnalyticsHistoryEntry(historyEntry("job-1", "2026-08-25T12:00:00.000Z", {
    duration: undefined,
    scannedRows: 0n,
  }));
  assert.equal(adapted.durationMs, null);
  assert.equal(adapted.scannedRows, 0n);
  assert.equal(adapted.spl, "index=main id=job-1");

  assert.throws(() => adaptAnalyticsHistoryEntry(historyEntry("job-bad", "2026-08-25T12:00:00.000Z", {
    effectiveIndexScope: ["main", "main"],
  })), /invalid effective index scope/);
});

test("workload aggregation derives exact counters, percentiles, failures, and empty trend buckets", () => {
  const records = [
    adaptAnalyticsHistoryEntry(historyEntry("job-3", "2026-08-25T18:00:00.000Z", {
      duration: { seconds: 3n, nanos: 0 }, scannedRows: 30n, scannedBytes: 300n,
    })),
    adaptAnalyticsHistoryEntry(historyEntry("job-2", "2026-08-25T12:00:00.000Z", {
      duration: undefined, scannedRows: 20n, scannedBytes: 200n,
      finalState: SearchJobState.SEARCH_JOB_STATE_FAILED,
      failure: { code: 9, message: "execution stopped", retryable: false, diagnostics: [] },
    })),
    adaptAnalyticsHistoryEntry(historyEntry("job-1", "2026-08-25T06:00:00.000Z", {
      duration: { seconds: 1n, nanos: 0 }, scannedRows: 10n, scannedBytes: 100n,
    })),
  ];
  const workload = deriveAnalyticsWorkload(records, { start, end, bucketCount: 4 });
  assert.equal(workload.searchCount, 3);
  assert.equal(workload.failedCount, 1);
  assert.equal(workload.scannedRows, 60n);
  assert.equal(workload.scannedBytes, 600n);
  assert.equal(workload.medianRuntimeMs, 2_000);
  assert.equal(workload.p95RuntimeMs, 3_000);
  assert.deepEqual(workload.trend.map((bucket) => bucket.p95RuntimeMs), [null, 1_000, null, 3_000]);
  assert.deepEqual(workload.slowest.map((entry) => entry.id), ["job-3", "job-1"]);
  assert.equal(workload.failures[0]?.failureMessage, "execution stopped");
});

test("history loading walks a bounded cursor chain and reports a partial sample", async () => {
  const requests: Array<{ pageToken?: string; includeTotalSize: boolean }> = [];
  const pages = [
    {
      historyEntries: [
        historyEntry("job-3", "2026-08-25T15:00:00.000Z"),
        historyEntry("job-2", "2026-08-25T14:00:00.000Z"),
      ],
      page: { nextPageToken: "page-2", totalSize: 4n, totalSizeExact: true },
    },
    {
      historyEntries: [historyEntry("job-1", "2026-08-25T13:00:00.000Z")],
      page: { nextPageToken: "page-3", totalSize: undefined, totalSizeExact: false },
    },
  ];
  const client = {
    history: {
      list: async (request: { page?: { pageToken?: string; includeTotalSize: boolean } }) => {
        requests.push({
          pageToken: request.page?.pageToken,
          includeTotalSize: request.page?.includeTotalSize ?? false,
        });
        return pages[requests.length - 1];
      },
    },
  };
  const snapshot = await loadAnalyticsHistory(client, {
    maximumPageSize: 2,
    maximumPages: 2,
    createdAfter: start,
    createdBefore: end,
  });
  assert.deepEqual(requests, [
    { pageToken: undefined, includeTotalSize: true },
    { pageToken: "page-2", includeTotalSize: false },
  ]);
  assert.equal(snapshot.complete, false);
  assert.equal(snapshot.nextPageToken, "page-3");
  assert.equal(snapshot.totalSize, 4n);
  assert.deepEqual(snapshot.entries.map((entry) => entry.id), ["job-3", "job-2", "job-1"]);
});

test("history loading rejects repeated page cursors", async () => {
  let calls = 0;
  const client = {
    history: {
      list: async () => {
        calls += 1;
        return {
          historyEntries: [historyEntry(`job-${3 - calls}`, `2026-08-25T${16 - calls}:00:00.000Z`)],
          page: { nextPageToken: "same", totalSize: 2n, totalSizeExact: true },
        };
      },
    },
  };
  await assert.rejects(loadAnalyticsHistory(client, {
    maximumPageSize: 10,
    maximumPages: 2,
    createdAfter: start,
    createdBefore: end,
  }), /repeated page cursor/);
});

test("history loading forwards cancellation to every backend request", async () => {
  const controller = new AbortController();
  controller.abort(new Error("test cancellation"));
  const client = {
    history: {
      list: async (_request: unknown, options?: { signal?: AbortSignal }) => {
        assert.equal(options?.signal, controller.signal);
        throw options?.signal?.reason;
      },
    },
  };
  await assert.rejects(loadAnalyticsHistory(client, {
    signal: controller.signal,
    maximumPageSize: 10,
    createdAfter: start,
    createdBefore: end,
  }), /test cancellation/);
});

test("field adaptation computes presence coverage and never claims unavailable cardinality", () => {
  const adapted = adaptAnalyticsFields([
    field("host", 8n, 2n),
    { ...field("source", 10n, 0n), distinctCount: 4n },
  ]);
  assert.equal(adapted.sampledEvents, 10n);
  assert.equal(adapted.fields[0]?.coverage, 80);
  assert.equal(adapted.fields[0]?.cardinality, null);
  assert.equal(adapted.fields[1]?.cardinality, 4n);
  assert.throws(() => adaptAnalyticsFields([
    field("host", 8n, 2n),
    field("source", 8n, 3n),
  ]), /different event samples/);
});

test("field loading is paginated, bounded, and preserves an incomplete snapshot", async () => {
  let calls = 0;
  const client = {
    indexes: {
      fields: async () => {
        calls += 1;
        return calls === 1
          ? { fields: [field("host", 8n, 2n)], page: { nextPageToken: "field-2", totalSize: 3n, totalSizeExact: true } }
          : { fields: [field("source", 10n, 0n)], page: { nextPageToken: "field-3", totalSize: undefined, totalSizeExact: false } };
      },
    },
  };
  const snapshot = await loadAnalyticsFields(client, {
    maximumPageSize: 1,
    maximumPages: 2,
    indexName: "main",
    earliest: "-24h",
    latest: "now",
  });
  assert.equal(snapshot.complete, false);
  assert.equal(snapshot.nextPageToken, "field-3");
  assert.equal(snapshot.totalSize, 3n);
  assert.deepEqual(snapshot.fields.map((item) => item.name), ["host", "source"]);
});
