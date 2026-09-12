import assert from "node:assert/strict";
import test from "node:test";

import { ResultPage, ResultRow, ResultSchema } from "@/gen/ts/open_splunk/result";
import { SearchJob } from "@/gen/ts/open_splunk/search";
import type { GetSearchResultsRequest, GetSearchResultsResponse } from "@/gen/ts/open_splunk/search_api";
import type { OpenSplunkApiClient } from "@/lib/api/open-splunk-client";
import { DashboardResultLoader } from "./dashboard-result-loader";

const schema = ResultSchema.fromPartial({ schemaId: "stable-schema", revision: 1n, columns: [{ fieldName: "count" }] });
const job = SearchJob.fromPartial({ searchJobId: "stable-job", resultSchema: schema });

function response(index: number, nextPageToken?: string, empty = false): GetSearchResultsResponse {
  return {
    searchJobId: job.searchJobId,
    resultPage: ResultPage.fromPartial({
      schema,
      rows: empty ? [] : [ResultRow.fromPartial({ rowId: `row-${index}`, ordinal: BigInt(index), cells: [{ kind: { $case: "sint64Value", value: BigInt(index) } }] })],
      page: { nextPageToken },
      snapshotComplete: true,
      snapshotRef: "retained-snapshot",
    }),
  };
}

function loader(results: (request: GetSearchResultsRequest) => Promise<GetSearchResultsResponse>, controller = new AbortController()) {
  return new DashboardResultLoader({
    client: { search: { results } } as unknown as OpenSplunkApiClient,
    job,
    signal: controller.signal,
    isCurrent: () => true,
  });
}

test("table back and forward navigation reuses the existing cursor chain", async () => {
  const requests: Array<string | undefined> = [];
  const pages = loader(async (request) => {
    requests.push(request.page?.pageToken);
    return request.page?.pageToken === undefined ? response(1, "page-2") : response(2, "page-3");
  });
  const first = await pages.firstTablePage();
  const second = await pages.nextTablePage();
  assert.equal(pages.previousTablePage(), first);
  assert.equal(await pages.nextTablePage(), second);
  assert.deepEqual(requests, [undefined, "page-2"]);
});

test("a cursor page that makes no row progress fails before another request", async () => {
  let requests = 0;
  const pages = loader(async () => {
    requests += 1;
    if (requests > 1) throw new Error("unexpected second request");
    return response(1, "fresh-cursor", true);
  });
  await assert.rejects(pages.collectChartRows(), /empty|progress/iu);
  assert.equal(requests, 1);
});

test("overlapping cursor pages cannot duplicate a retained row in a chart", async () => {
  let requests = 0;
  const pages = loader(async () => {
    requests += 1;
    return response(1, requests === 1 ? "page-2" : undefined);
  });
  await assert.rejects(pages.collectChartRows(), /duplicate|repeat|overlap/iu);
});

test("canceling a pending page settles before an abort-ignoring transport returns", async () => {
  const controller = new AbortController();
  let resolve!: (value: GetSearchResultsResponse) => void;
  const network = new Promise<GetSearchResultsResponse>((done) => { resolve = done; });
  const pages = loader(() => network, controller);
  let settled = false;
  const pending = pages.firstTablePage();
  const observed = pending.catch((error: unknown) => { settled = true; return error; });
  controller.abort(new Error("stale panel"));
  await new Promise<void>((done) => setImmediate(done));
  try {
    assert.equal(settled, true, "cancellation must release the panel pipeline before transport completion");
    assert.match(String(await observed), /stale panel/u);
  } finally {
    resolve(response(1));
    await observed;
  }
});
