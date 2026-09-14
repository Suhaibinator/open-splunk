import assert from "node:assert/strict";
import test from "node:test";

import {
  ResultPage,
  ResultSchema,
  type ResultRow,
} from "@/gen/ts/open_splunk/result";
import { SearchJob } from "@/gen/ts/open_splunk/search";
import type { GetSearchResultsResponse } from "@/gen/ts/open_splunk/search_api";
import type { OpenSplunkApiClient } from "@/lib/api/open-splunk-client";

import {
  acquireDashboardPanelPipeline,
  DASHBOARD_CHART_MAXIMUM_ROWS,
  DashboardResultLoader,
} from "./dashboard-result-loader";

const schema = ResultSchema.fromPartial({
  schemaId: "dashboard-results-v1",
  revision: 1n,
  columns: [{ fieldName: "value", displayName: "Value" }],
});
const job = SearchJob.fromPartial({ searchJobId: "job-dashboard", resultSchema: schema });

function row(index: number): ResultRow {
  return {
    rowId: `row-${index}`,
    ordinal: BigInt(index),
    cells: [{ kind: { $case: "sint64Value", value: BigInt(index) } }],
    timeBucket: undefined,
  };
}

interface ObservedRequest {
  page?: { pageSize?: number; pageToken?: string };
  searchJobId: string;
}

function clientFor(
  handler: (request: ObservedRequest, call: number) => GetSearchResultsResponse,
): { client: OpenSplunkApiClient; requests: ObservedRequest[] } {
  const requests: ObservedRequest[] = [];
  const client = {
    search: {
      results: async (request: ObservedRequest) => {
        requests.push(request);
        return handler(request, requests.length);
      },
    },
  } as unknown as OpenSplunkApiClient;
  return { client, requests };
}

function response(options: {
  nextPageToken?: string;
  responseJobId?: string;
  rows?: ResultRow[];
  schemaOverride?: typeof schema;
  snapshotComplete?: boolean;
  snapshotRef?: string;
} = {}): GetSearchResultsResponse {
  return {
    searchJobId: options.responseJobId ?? job.searchJobId,
    resultPage: ResultPage.fromPartial({
      page: { nextPageToken: options.nextPageToken },
      rows: options.rows ?? [row(1)],
      schema: options.schemaOverride ?? schema,
      snapshotComplete: options.snapshotComplete ?? true,
      snapshotRef: options.snapshotRef ?? "snapshot-1",
    }),
  };
}

function loader(client: OpenSplunkApiClient, controller = new AbortController(), current = () => true) {
  return new DashboardResultLoader({ client, isCurrent: current, job, signal: controller.signal });
}

test("table paging uses 20-row cursors from one job and keeps previous pages local", async () => {
  const { client, requests } = clientFor((_request, call) => response({
    nextPageToken: call === 1 ? "page-2" : undefined,
    rows: [row(call)],
  }));
  const pages = loader(client);
  const first = await pages.firstTablePage();
  const second = await pages.nextTablePage();
  assert.equal(pages.previousTablePage(), first);
  assert.equal(second.pageNumber, 2);
  assert.equal(second.rowStart, 2);
  assert.deepEqual(requests.map((request) => request.searchJobId), [job.searchJobId, job.searchJobId]);
  assert.deepEqual(requests.map((request) => request.page), [
    { pageSize: 20, pageToken: undefined, includeTotalSize: true },
    { pageSize: 20, pageToken: "page-2", includeTotalSize: false },
  ]);
});

test("chart collection retains every cursor row and discloses incomplete snapshots", async () => {
  const { client, requests } = clientFor((_request, call) => response({
    nextPageToken: call === 1 ? "page-2" : undefined,
    rows: [row(call)],
    snapshotComplete: call === 1,
  }));
  const result = await loader(client).collectChartRows();
  assert.deepEqual(result.rows.map((item) => item.rowId), ["row-1", "row-2"]);
  assert.equal(result.complete, false);
  assert.equal(result.snapshotComplete, false);
  assert.equal(result.capped, false);
  assert.deepEqual(requests.map((request) => request.page?.pageSize), [1_000, 1_000]);
});

test("chart collection stops at 10000 rows and retains an explicit continuation", async () => {
  const { client, requests } = clientFor((_request, call) => response({
    nextPageToken: `page-${call + 1}`,
    rows: Array.from({ length: 1_000 }, (_, index) => row((call - 1) * 1_000 + index)),
  }));
  const result = await loader(client).collectChartRows();
  assert.equal(result.rows.length, DASHBOARD_CHART_MAXIMUM_ROWS);
  assert.equal(result.capped, true);
  assert.equal(result.complete, false);
  assert.equal(requests.length, 10);
});

test("paging rejects repeated cursors, schema changes, and mixed snapshot identity modes", async () => {
  const cases = [
    clientFor((_request, call) => response({ nextPageToken: "repeat", rows: [row(call)] })).client,
    clientFor((_request, call) => response({
      nextPageToken: call === 1 ? "next" : undefined,
      schemaOverride: call === 1 ? schema : ResultSchema.fromPartial({ ...schema, schemaId: "changed" }),
    })).client,
    clientFor((_request, call) => response({
      nextPageToken: call === 1 ? "next" : undefined,
      snapshotRef: call === 1 ? "" : "snapshot-new",
    })).client,
    clientFor((_request, call) => response({
      nextPageToken: call === 1 ? "next" : undefined,
      snapshotRef: call === 1 ? "snapshot-1" : "snapshot-2",
    })).client,
  ];
  await Promise.all(cases.map((candidate) => assert.rejects(loader(candidate).collectChartRows())));
});

test("legacy pages are accepted only when every page omits snapshot identity", async () => {
  const { client } = clientFor((_request, call) => response({
    nextPageToken: call === 1 ? "next" : undefined,
    rows: [row(call)],
    snapshotRef: "",
  }));
  const result = await loader(client).collectChartRows();
  assert.equal(result.rows.length, 2);
  assert.equal(result.snapshotRef, undefined);
});

test("stale and canceled loaders cannot publish or start a request", async () => {
  const controller = new AbortController();
  const { client, requests } = clientFor(() => response());
  controller.abort(new Error("canceled"));
  await assert.rejects(loader(client, controller).firstTablePage(), /canceled/u);
  await assert.rejects(loader(client, new AbortController(), () => false).firstTablePage(), /superseded/u);
  assert.equal(requests.length, 0);
});

test("dashboard pipelines admit four jobs and remove a canceled queued waiter", async () => {
  const controllers = Array.from({ length: 6 }, () => new AbortController());
  const admissions = controllers.map((controller) => acquireDashboardPanelPipeline(controller.signal));
  const firstFour = await Promise.all(admissions.slice(0, 4));
  let sixthAdmitted = false;
  void admissions[5].then((release) => {
    sixthAdmitted = true;
    release();
  });
  controllers[4].abort(new Error("queued cancellation"));
  await assert.rejects(admissions[4], /queued cancellation/u);
  assert.equal(sixthAdmitted, false);
  firstFour[0]();
  await admissions[5];
  assert.equal(sixthAdmitted, true);
  for (const release of firstFour.slice(1)) release();
});
