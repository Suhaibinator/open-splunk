import assert from "node:assert/strict";
import test from "node:test";

import type { ResultRow } from "../../gen/ts/open_splunk/result";
import {
  MAXIMUM_CHART_BUCKETS,
  completeTimechartCoverage,
  describeTimechartCoverage,
  describeTimechartStatisticsPage,
  loadTimechartBuckets,
  type TimechartCoverage,
  type TimechartPage,
} from "./timechart-series";

function rows(start: number, count: number): ResultRow[] {
  return Array.from({ length: count }, (_, index) => ({
    rowId: `bucket-${start + index}`,
    ordinal: BigInt(start + index),
    cells: [],
  }));
}

function pagedResult(pageSize: number, totalRows: number): Map<string, TimechartPage> {
  const pages = new Map<string, TimechartPage>();
  for (let start = 0; start < totalRows; start += pageSize) {
    const count = Math.min(pageSize, totalRows - start);
    const token = start === 0 ? "" : `page-${start}`;
    const nextStart = start + count;
    pages.set(token, {
      rows: rows(start, count),
      nextPageToken: nextStart < totalRows ? `page-${nextStart}` : null,
    });
  }
  return pages;
}

function firstPage(pages: Map<string, TimechartPage>, totalRows: number, totalSizeExact = true) {
  const page = pages.get("");
  assert.ok(page !== undefined);
  return { ...page, totalSize: totalRows, totalSizeExact };
}

test("time-series pages concatenate in cursor order until the cursor ends", async () => {
  const pages = pagedResult(1_000, 2_017);
  const fetched: string[] = [];
  const progress: TimechartCoverage[] = [];

  const load = await loadTimechartBuckets({
    firstPage: firstPage(pages, 2_017),
    fetchPage: async (token) => {
      fetched.push(token);
      const page = pages.get(token);
      assert.ok(page !== undefined, `unexpected cursor ${token}`);
      return page;
    },
    onProgress: (partial) => progress.push(partial.coverage),
  });

  assert.deepEqual(fetched, ["page-1000", "page-2000"]);
  assert.equal(load.rows.length, 2_017);
  assert.deepEqual(load.rows.map((row) => row.ordinal).slice(998, 1_002), [998n, 999n, 1_000n, 1_001n]);
  assert.equal(load.rows.at(-1)?.rowId, "bucket-2016");
  assert.deepEqual(load.coverage, {
    status: "complete",
    plottedBuckets: 2_017,
    totalBuckets: 2_017,
    totalExact: true,
  });
  assert.deepEqual(progress.map((coverage) => [coverage.status, coverage.plottedBuckets]), [
    ["loading", 1_000],
    ["loading", 2_000],
  ]);
});

test("a result that fits on the first page completes without following a cursor", async () => {
  const pages = pagedResult(1_000, 672);
  const load = await loadTimechartBuckets({
    firstPage: firstPage(pages, 672),
    fetchPage: async () => {
      throw new Error("no cursor should be followed");
    },
  });
  assert.equal(load.rows.length, 672);
  assert.equal(load.coverage.status, "complete");
  assert.deepEqual(completeTimechartCoverage(672), load.coverage);
});

test("bucket loading stops at the cap and reports the truncation", async () => {
  const pages = pagedResult(1_000, 3_500);
  const fetched: string[] = [];
  const load = await loadTimechartBuckets({
    firstPage: firstPage(pages, 3_500),
    fetchPage: async (token) => {
      fetched.push(token);
      return pages.get(token) as TimechartPage;
    },
    maximumBuckets: 2_000,
  });
  assert.deepEqual(fetched, ["page-1000"]);
  assert.equal(load.rows.length, 2_000);
  assert.deepEqual(load.coverage, {
    status: "capped",
    plottedBuckets: 2_000,
    totalBuckets: 3_500,
    totalExact: true,
  });
  assert.equal(MAXIMUM_CHART_BUCKETS, 10_000);
  await assert.rejects(
    loadTimechartBuckets({ firstPage: firstPage(pages, 3_500), fetchPage: async () => pages.get("") as TimechartPage, maximumBuckets: 0 }),
    RangeError,
  );
});

test("a failing page keeps the buckets loaded before it and surfaces the error", async () => {
  const pages = pagedResult(1_000, 3_000);
  const failure = new Error("boom");
  const load = await loadTimechartBuckets({
    firstPage: firstPage(pages, 3_000, false),
    fetchPage: async (token) => {
      if (token === "page-2000") throw failure;
      return pages.get(token) as TimechartPage;
    },
  });
  assert.equal(load.rows.length, 2_000);
  assert.equal(load.error, failure);
  assert.deepEqual(load.coverage, {
    status: "failed",
    plottedBuckets: 2_000,
    totalBuckets: 3_000,
    totalExact: false,
  });
});

test("repeated cursors and empty continued pages stop the walk instead of looping", async () => {
  const repeating = await loadTimechartBuckets({
    firstPage: { rows: rows(0, 2), nextPageToken: "again", totalSize: null, totalSizeExact: false },
    fetchPage: async () => ({ rows: rows(2, 2), nextPageToken: "again" }),
  });
  assert.equal(repeating.rows.length, 4);
  assert.equal(repeating.coverage.status, "failed");
  assert.match(String((repeating.error as Error).message), /repeated a page cursor/);

  const empty = await loadTimechartBuckets({
    firstPage: { rows: rows(0, 2), nextPageToken: "next", totalSize: null, totalSizeExact: false },
    fetchPage: async () => ({ rows: [], nextPageToken: "beyond" }),
  });
  assert.equal(empty.rows.length, 2);
  assert.equal(empty.coverage.status, "failed");
  assert.match(String((empty.error as Error).message), /empty page/);
});

test("an aborted walk rejects rather than reporting partial coverage", async () => {
  const pages = pagedResult(1_000, 3_000);
  const controller = new AbortController();
  await assert.rejects(
    loadTimechartBuckets({
      firstPage: firstPage(pages, 3_000),
      fetchPage: async (token) => {
        controller.abort();
        return pages.get(token) as TimechartPage;
      },
      signal: controller.signal,
    }),
    (error: unknown) => error instanceof DOMException && error.name === "AbortError",
  );
  const preAborted = new AbortController();
  preAborted.abort();
  await assert.rejects(
    loadTimechartBuckets({
      firstPage: firstPage(pages, 3_000),
      fetchPage: async (token) => pages.get(token) as TimechartPage,
      signal: preAborted.signal,
    }),
    (error: unknown) => error instanceof DOMException && error.name === "AbortError",
  );
});

test("coverage copy states which buckets are plotted", () => {
  assert.equal(
    describeTimechartCoverage(completeTimechartCoverage(672), "Aug 31, 12:10 AM"),
    "Timechart across the submitted search range.",
  );
  assert.equal(
    describeTimechartCoverage(
      { status: "loading", plottedBuckets: 1_000, totalBuckets: 2_017, totalExact: true },
      "Aug 27, 11:25 AM",
    ),
    "Showing the first 1,000 of 2,017 buckets (through Aug 27, 11:25 AM). Loading the remaining 1,017 buckets…",
  );
  assert.equal(
    describeTimechartCoverage(
      { status: "loading", plottedBuckets: 1_000, totalBuckets: 2_000, totalExact: false },
      null,
    ),
    "Showing the first 1,000 of at least 2,000 buckets. Loading the remaining buckets…",
  );
  assert.equal(
    describeTimechartCoverage(
      { status: "capped", plottedBuckets: 10_000, totalBuckets: 20_161, totalExact: true },
      "Sep 3, 4:00 PM",
    ),
    "Showing the first 10,000 of 20,161 buckets (through Sep 3, 4:00 PM). The chart stops at 10,000 buckets; widen the timechart span to plot the full range.",
  );
  assert.equal(
    describeTimechartCoverage(
      { status: "failed", plottedBuckets: 2_000, totalBuckets: null, totalExact: false },
      "Aug 28, 7:30 AM",
    ),
    "Showing the first 2,000 buckets (through Aug 28, 7:30 AM). The remaining buckets could not be loaded; run the search again to retry.",
  );
  assert.equal(
    describeTimechartStatisticsPage(2, completeTimechartCoverage(2_017)),
    "Statistics show server page 2 of the timechart buckets; the visualization plots every bucket.",
  );
  assert.equal(
    describeTimechartStatisticsPage(1, { status: "loading", plottedBuckets: 1_000, totalBuckets: 2_017, totalExact: true }),
    "Statistics show server page 1 of the timechart buckets; the visualization plots the buckets loaded so far.",
  );
  assert.equal(
    describeTimechartStatisticsPage(3, null),
    "Statistics show server page 3 of the timechart buckets; the visualization plots the buckets loaded so far.",
  );
});
