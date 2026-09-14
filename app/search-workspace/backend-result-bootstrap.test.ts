import assert from "node:assert/strict";
import test from "node:test";

import {
  ColumnSemanticType,
  ResultSetKind,
  type ResultRow,
  type ResultSchema,
} from "@/gen/ts/open_splunk/result";
import { ValueType } from "@/gen/ts/open_splunk/value";
import type { SearchJob } from "@/gen/ts/open_splunk/search";
import type { OpenSplunkApiClient } from "@/lib/api/open-splunk-client";
import type { TimelinePoint } from "@/lib/demo/search-data";
import { adaptSearchResults } from "@/lib/search/backend-data";

import {
  adaptAndApplyBackendResultPage,
  seedBackendChartPoints,
} from "./backend-result-bootstrap";
import { BackendResultPages } from "./backend-result-pages";
import { loadTimechartBuckets } from "./timechart-series";

const schema: ResultSchema = {
  schemaId: "timechart-v1",
  revision: 1n,
  resultKind: ResultSetKind.RESULT_SET_KIND_TIME_SERIES,
  columns: [
    {
      fieldName: "_time",
      displayName: "_time",
      valueType: ValueType.VALUE_TYPE_TIMESTAMP,
      semanticType: ColumnSemanticType.COLUMN_SEMANTIC_TYPE_EVENT_TIME,
      nullable: false,
      multivalue: false,
      hiddenByDefault: false,
      statsSparkline: false,
    },
    {
      fieldName: "api",
      displayName: "api",
      valueType: ValueType.VALUE_TYPE_DOUBLE,
      semanticType: ColumnSemanticType.COLUMN_SEMANTIC_TYPE_METRIC,
      nullable: false,
      multivalue: false,
      hiddenByDefault: false,
      statsSparkline: false,
    },
  ],
};

function row(index: number): ResultRow {
  const timestamp = new Date(Date.UTC(2026, 0, 1, 0, 0, index));
  const earliest = timestamp.toISOString().replace(".000Z", "Z");
  const latest = new Date(timestamp.valueOf() + 1_000).toISOString().replace(".000Z", "Z");
  return {
    rowId: `bucket-${index}`,
    ordinal: BigInt(index),
    cells: [
      { kind: { $case: "timestampValue", value: timestamp } },
      { kind: { $case: "doubleValue", value: index + 1 } },
    ],
    timeBucket: { earliest, latest },
  };
}

test("paged timechart bootstrap adapts the first page once and shares its immutable points", async () => {
  const requests: Array<string | undefined> = [];
  const client = {
    search: {
      results: async (request: { searchJobId: string; page: { pageToken?: string } }) => {
        requests.push(request.page.pageToken);
        const first = request.page.pageToken === undefined;
        return {
          searchJobId: request.searchJobId,
          resultPage: {
            schema,
            rows: first ? [row(0), row(1)] : [row(2)],
            page: first
              ? { nextPageToken: "page-2", totalSize: 3n, totalSizeExact: true }
              : undefined,
            snapshotComplete: true,
          },
        };
      },
    },
  } as unknown as OpenSplunkApiClient;
  const job = { searchJobId: "job-1", resultSchema: schema } as SearchJob;
  const pages = new BackendResultPages();
  pages.resetForJob(2);
  let adaptationCount = 0;
  const adapt: typeof adaptSearchResults = (resultSchema, rows) => {
    adaptationCount += 1;
    return adaptSearchResults(resultSchema, rows);
  };
  const statisticsPagePoints: TimelinePoint[][] = [];
  let firstPagePoints: readonly TimelinePoint[] = [];

  const firstPage = await pages.fetch({
    client,
    job,
    pageNumber: 1,
    pageSize: 2,
    signal: new AbortController().signal,
    isCurrent: () => true,
    apply: true,
    onApply: (page) => {
      firstPagePoints = adaptAndApplyBackendResultPage(page, (_appliedPage, adapted) => {
        statisticsPagePoints.push(adapted.timeline);
      }, adapt);
    },
    onNotice: () => {},
  });
  const chartPoints = seedBackendChartPoints(firstPagePoints);

  await loadTimechartBuckets({
    firstPage: {
      rows: firstPage.rows,
      nextPageToken: firstPage.nextPageToken ?? null,
      totalSize: firstPage.totalSize ?? null,
      totalSizeExact: firstPage.totalSizeExact,
    },
    fetchPage: async (pageToken) => {
      const page = await pages.request({
        client,
        job,
        pageSize: 2,
        pageToken,
        includeTotalSize: false,
        signal: new AbortController().signal,
        isCurrent: () => true,
      });
      return { rows: page.rows, nextPageToken: page.rawNextPageToken ?? null };
    },
    onProgress: (batch) => {
      if (batch.rows.length > 0) chartPoints.push(...adapt(schema, batch.rows).timeline);
    },
    retainRows: false,
  });

  assert.equal(adaptationCount, 2, "each of the two server pages is adapted exactly once");
  assert.deepEqual(requests, [undefined, "page-2"]);
  assert.equal(statisticsPagePoints.length, 1);
  assert.notStrictEqual(chartPoints, statisticsPagePoints[0]);
  assert.strictEqual(chartPoints[0], statisticsPagePoints[0][0]);
  assert.strictEqual(chartPoints[0].series, statisticsPagePoints[0][0].series);
  assert.deepEqual(statisticsPagePoints[0].map((point) => point.id), ["bucket-0", "bucket-1"]);
  assert.deepEqual(chartPoints.map((point) => point.id), ["bucket-0", "bucket-1", "bucket-2"]);

  let cachedFirstPagePoints: readonly TimelinePoint[] = [];
  await pages.fetch({
    client,
    job,
    pageNumber: 1,
    pageSize: 2,
    signal: new AbortController().signal,
    isCurrent: () => true,
    apply: true,
    onApply: (page) => {
      cachedFirstPagePoints = adaptAndApplyBackendResultPage(page, (_appliedPage, adapted) => {
        statisticsPagePoints.push(adapted.timeline);
      }, adapt);
    },
    onNotice: () => {},
  });
  const cachedChartPoints = seedBackendChartPoints(cachedFirstPagePoints);

  assert.equal(adaptationCount, 3, "the cached first page is also adapted only once per application");
  assert.deepEqual(requests, [undefined, "page-2"], "cached bootstrap avoids another server request");
  assert.notStrictEqual(cachedChartPoints, statisticsPagePoints[1]);
  assert.strictEqual(cachedChartPoints[0], statisticsPagePoints[1][0]);
  assert.strictEqual(cachedChartPoints[0].series, statisticsPagePoints[1][0].series);

  await assert.rejects(
    pages.fetch({
      client,
      job,
      pageNumber: 1,
      pageSize: 2,
      signal: new AbortController().signal,
      isCurrent: () => false,
      apply: true,
      onApply: (page) => {
        adaptAndApplyBackendResultPage(page, () => {}, adapt);
      },
      onNotice: () => {},
    }),
    (error: unknown) => error instanceof DOMException && error.name === "AbortError",
  );
  assert.equal(adaptationCount, 3, "a superseded cached bootstrap is not adapted or published");
});
