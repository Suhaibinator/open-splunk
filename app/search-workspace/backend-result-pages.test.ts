import assert from "node:assert/strict";
import test from "node:test";

import { ResultSetKind, type ResultRow, type ResultSchema } from "@/gen/ts/open_splunk/result";
import type { SearchJob } from "@/gen/ts/open_splunk/search";
import type { OpenSplunkApiClient } from "@/lib/api/open-splunk-client";

import { BackendResultPages } from "./backend-result-pages";

const schema: ResultSchema = {
  schemaId: "events-v1",
  revision: 1n,
  resultKind: ResultSetKind.RESULT_SET_KIND_EVENTS,
  columns: [{
    fieldName: "message",
    displayName: "message",
    valueType: 0,
    semanticType: 0,
    nullable: false,
    multivalue: false,
    hiddenByDefault: false,
    statsSparkline: false,
  }],
};

function row(id: string, ordinal: bigint): ResultRow {
  return {
    rowId: id,
    ordinal,
    cells: [{ kind: { $case: "stringValue", value: id } }],
  };
}

interface StubResultsRequest {
  searchJobId: string;
  page: {
    pageSize: number;
    pageToken?: string;
    includeTotalSize: boolean;
  };
}

interface StubResultPage {
  schema?: ResultSchema;
  rows: ResultRow[];
  nextPageToken?: string;
}

function stubClient(
  response: (request: StubResultsRequest) => StubResultPage,
): OpenSplunkApiClient {
  return {
    search: {
      results: async (request: StubResultsRequest) => {
        const page = response(request);
        return {
          searchJobId: request.searchJobId,
          resultPage: {
            schema: page.schema ?? schema,
            rows: page.rows,
            page: { nextPageToken: page.nextPageToken },
            snapshotComplete: true,
          },
        };
      },
    },
  } as unknown as OpenSplunkApiClient;
}

function requestPage(
  pages: BackendResultPages,
  client: OpenSplunkApiClient,
  pageNumber: number,
  pageSize: number,
  notices: string[] = [],
  apply = false,
  job: SearchJob = { searchJobId: "job-1", resultSchema: schema } as SearchJob,
) {
  return pages.fetch({
    client,
    job,
    pageNumber,
    pageSize,
    signal: new AbortController().signal,
    isCurrent: () => true,
    apply,
    onApply: () => {},
    onNotice: (message) => notices.push(message),
  });
}

async function requestPagesInOrder(
  pageNumbers: number[],
  request: (pageNumber: number) => Promise<unknown>,
): Promise<void> {
  const [pageNumber, ...remaining] = pageNumbers;
  if (pageNumber === undefined) return;
  await request(pageNumber);
  await requestPagesInOrder(remaining, request);
}

test("result pages retain opaque cursor starts and reuse cached pages", async () => {
  const requests: unknown[] = [];
  const client = {
    search: {
      results: async (request: unknown) => {
        requests.push(request);
        return {
          searchJobId: "job-1",
          resultPage: {
            schema,
            rows: [row("one", 1n), row("two", 2n)],
            page: {
              nextPageToken: "cursor-2",
              totalSize: 3n,
              totalSizeExact: true,
            },
            snapshotComplete: true,
          },
        };
      },
    },
  } as unknown as OpenSplunkApiClient;
  const job = { searchJobId: "job-1", resultSchema: schema } as SearchJob;
  const pages = new BackendResultPages();
  const applied: string[][] = [];
  const notices: string[] = [];
  pages.resetForJob(2);

  const fetchPage = () => pages.fetch({
    client,
    job,
    pageNumber: 1,
    pageSize: 2,
    signal: new AbortController().signal,
    isCurrent: () => true,
    apply: true,
    onApply: (page) => applied.push(page.rows.map((item) => item.rowId)),
    onNotice: (message) => notices.push(message),
  });

  await fetchPage();
  await fetchPage();

  assert.equal(requests.length, 1);
  assert.deepEqual(applied, [["one", "two"], ["one", "two"]]);
  assert.equal(pages.canOpen(2, 2), true);
  assert.equal(pages.pageStart(2, 1), 1);
  assert.equal(notices.length, 0);
});

test("result pages reject a response for another search before publishing it", async () => {
  const client = {
    search: {
      results: async () => ({ searchJobId: "other-job", resultPage: undefined }),
    },
  } as unknown as OpenSplunkApiClient;
  const pages = new BackendResultPages();
  pages.resetForJob(20);

  await assert.rejects(
    pages.request({
      client,
      job: { searchJobId: "job-1" } as SearchJob,
      pageSize: 20,
      pageToken: undefined,
      includeTotalSize: true,
      signal: new AbortController().signal,
      isCurrent: () => true,
    }),
    /different search job/,
  );
});

test("changing page size keeps the retained snapshot schema fence", async () => {
  const changedSchema: ResultSchema = {
    ...schema,
    columns: schema.columns.map((column) => ({ ...column, displayName: "Message" })),
  };
  let requestCount = 0;
  const client = {
    search: {
      results: async () => ({
        searchJobId: "job-1",
        resultPage: {
          schema: requestCount++ === 0 ? schema : changedSchema,
          rows: [],
          page: undefined,
          snapshotComplete: true,
        },
      }),
    },
  } as unknown as OpenSplunkApiClient;
  const pages = new BackendResultPages();
  const job = { searchJobId: "job-1" } as SearchJob;
  pages.resetForJob(20);
  const request = (pageSize: number) => pages.request({
    client,
    job,
    pageSize,
    pageToken: undefined,
    includeTotalSize: true,
    signal: new AbortController().signal,
    isCurrent: () => true,
  });

  await request(20);
  pages.resetForPageSize(50);

  await assert.rejects(request(50), /mutated without changing its identity or revision/);
});

test("a changed cursor prunes every cached downstream result page", async () => {
  let pageOneRequests = 0;
  let requestCount = 0;
  const notices: string[] = [];
  const client = stubClient((request) => {
    requestCount += 1;
    const pageNumber = request.page.pageToken === undefined
      ? 1
      : Number(request.page.pageToken.replace("cursor-", ""));
    const nextPageToken = pageNumber === 1 && pageOneRequests++ > 0
      ? "replacement-cursor"
      : pageNumber < 9
        ? `cursor-${pageNumber + 1}`
        : undefined;
    return { rows: [row(`row-${pageNumber}`, BigInt(pageNumber))], nextPageToken };
  });
  const pages = new BackendResultPages();
  pages.resetForJob(1);

  await requestPagesInOrder(
    Array.from({ length: 9 }, (_, index) => index + 1),
    (pageNumber) => requestPage(pages, client, pageNumber, 1, notices),
  );
  assert.equal(pages.pageStart(1, 1), null);
  assert.equal(pages.canOpen(1, 9), true);

  const refreshed = await requestPage(pages, client, 1, 1, notices);

  assert.equal(requestCount, 10);
  assert.equal(refreshed.nextPageToken, undefined);
  assert.equal(pages.canOpen(1, 2), false);
  assert.equal(pages.canOpen(1, 9), false);
  assert.equal(pages.pageStart(1, 2), null);
  assert.deepEqual(notices, [
    "The retained result cursor changed while revisiting a page. Further paging was stopped.",
  ]);
});

test("a repeated cursor publishes the current page but stops the cursor chain", async () => {
  const notices: string[] = [];
  const client = stubClient((request) => ({
    rows: [row(request.page.pageToken === undefined ? "one" : "two", request.page.pageToken === undefined ? 1n : 2n)],
    nextPageToken: " cursor-2 ",
  }));
  const pages = new BackendResultPages();
  pages.resetForJob(1);

  await requestPage(pages, client, 1, 1, notices);
  const second = await requestPage(pages, client, 2, 1, notices);

  assert.deepEqual(second.rows.map((item) => item.rowId), ["two"]);
  assert.equal(second.nextPageToken, undefined);
  assert.equal(pages.pageStart(1, 2), 2);
  assert.equal(pages.canOpen(1, 3), false);
  assert.deepEqual(notices, [
    "Search results returned a repeated page cursor. Further paging was stopped.",
  ]);
});

test("page starts advance by the rows actually returned from each cursor page", async () => {
  const requestedTokens: Array<string | undefined> = [];
  const client = stubClient((request) => {
    requestedTokens.push(request.page.pageToken);
    if (request.page.pageToken === undefined) {
      return { rows: [row("one", 1n), row("two", 2n)], nextPageToken: "cursor-2" };
    }
    if (request.page.pageToken === "cursor-2") {
      return { rows: [row("three", 3n)], nextPageToken: "cursor-3" };
    }
    return { rows: [] };
  });
  const pages = new BackendResultPages();
  pages.resetForJob(4);

  await requestPage(pages, client, 1, 4);
  assert.equal(pages.pageStart(4, 1), 1);
  assert.equal(pages.pageStart(4, 2), null);
  await requestPage(pages, client, 2, 4);
  assert.equal(pages.pageStart(4, 2), 3);
  assert.equal(pages.pageStart(4, 3), null);
  await requestPage(pages, client, 3, 4);

  assert.equal(pages.pageStart(4, 3), 4);
  assert.deepEqual(requestedTokens, [undefined, "cursor-2", "cursor-3"]);
});

test("LRU eviction retains the displayed page and removes the oldest background page", async () => {
  const requestedTokens: Array<string | undefined> = [];
  const client = stubClient((request) => {
    requestedTokens.push(request.page.pageToken);
    const pageNumber = request.page.pageToken === undefined
      ? 1
      : Number(request.page.pageToken.replace("cursor-", ""));
    return {
      rows: [row(`row-${pageNumber}`, BigInt(pageNumber))],
      nextPageToken: `cursor-${pageNumber + 1}`,
    };
  });
  const pages = new BackendResultPages();
  pages.resetForJob(1);

  await requestPage(pages, client, 1, 1, [], true);
  await requestPagesInOrder(
    Array.from({ length: 8 }, (_, index) => index + 2),
    (pageNumber) => requestPage(pages, client, pageNumber, 1),
  );

  assert.equal(pages.pageStart(1, 1), 1);
  assert.equal(pages.pageStart(1, 2), null);
  await requestPage(pages, client, 1, 1);
  assert.equal(requestedTokens.length, 9);
  await requestPage(pages, client, 2, 1);
  assert.equal(requestedTokens.length, 10);
  assert.equal(requestedTokens.at(-1), "cursor-2");
});

test("resetting for a new job isolates old cursors, pages, starts, and schema", async () => {
  const replacementSchema: ResultSchema = {
    ...schema,
    schemaId: "events-v2",
    revision: 2n,
  };
  const client = stubClient((request) => ({
    schema: request.searchJobId === "job-1" ? schema : replacementSchema,
    rows: [row(request.searchJobId, 1n)],
    nextPageToken: request.searchJobId === "job-1" ? "old-cursor" : undefined,
  }));
  const pages = new BackendResultPages();
  const firstJob = { searchJobId: "job-1", resultSchema: schema } as SearchJob;
  const secondJob = { searchJobId: "job-2", resultSchema: replacementSchema } as SearchJob;
  pages.resetForJob(2);
  await requestPage(pages, client, 1, 2, [], false, firstJob);
  assert.equal(pages.canOpen(2, 2), true);

  pages.resetForJob(3);

  assert.deepEqual([...pages.pageTokenKeys()], ["3:1"]);
  assert.equal(pages.canOpen(2, 1), false);
  assert.equal(pages.canOpen(2, 2), false);
  assert.equal(pages.pageStart(2, 1), null);
  const next = await requestPage(pages, client, 1, 3, [], false, secondJob);
  assert.equal(next.schema.schemaId, "events-v2");
  assert.equal(pages.pageStart(3, 1), 1);
});
