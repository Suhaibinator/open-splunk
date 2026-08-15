import assert from "node:assert/strict";
import test from "node:test";

import {
  collectCursorPages,
  RepeatedPageCursorError,
  type CursorPage,
} from "./pagination";

interface RecordedRequest {
  pageToken: string | undefined;
  includeTotalSize: boolean;
}

function scriptedServer(pages: readonly CursorPage<string>[]) {
  const requests: RecordedRequest[] = [];
  return {
    requests,
    fetchPage: (request: RecordedRequest): Promise<CursorPage<string>> => {
      requests.push({ ...request });
      const page = pages[requests.length - 1];
      if (page === undefined) {
        throw new Error(`the collector requested page ${requests.length} beyond the script`);
      }
      return Promise.resolve(page);
    },
  };
}

test("a fresh chain requests the exact total once and keeps only the first page's total", async () => {
  const server = scriptedServer([
    { items: ["a"], page: { nextPageToken: "c1", totalSize: 9n, totalSizeExact: true } },
    { items: ["b"], page: { nextPageToken: "c2", totalSize: 99n, totalSizeExact: false } },
    { items: ["c"], page: { totalSize: 999n, totalSizeExact: false } },
  ]);
  const collected = await collectCursorPages<string>({
    maximumPages: 8,
    label: "Saved searches",
    fetchPage: server.fetchPage,
  });

  assert.deepEqual(collected, {
    items: ["a", "b", "c"],
    nextPageToken: null,
    totalSize: 9n,
    totalSizeExact: true,
    complete: true,
  });
  assert.deepEqual(server.requests, [
    { pageToken: undefined, includeTotalSize: true },
    { pageToken: "c1", includeTotalSize: false },
    { pageToken: "c2", includeTotalSize: false },
  ]);
});

test("a resumed chain never asks for a total and refuses to replay its own cursor", async () => {
  const resumed = scriptedServer([
    { items: ["x"], page: { totalSize: 42n, totalSizeExact: true } },
  ]);
  const collected = await collectCursorPages<string>({
    maximumPages: 4,
    pageToken: "cursor-a",
    label: "Search history",
    fetchPage: resumed.fetchPage,
  });
  assert.deepEqual(resumed.requests, [{ pageToken: "cursor-a", includeTotalSize: false }]);
  // The server may still volunteer a total; the collector reports what page 0 sent.
  assert.equal(collected.totalSize, 42n);
  assert.equal(collected.complete, true);

  const replay = scriptedServer([{ items: [], page: { nextPageToken: "cursor-a" } }]);
  await assert.rejects(
    collectCursorPages<string>({
      maximumPages: 4,
      pageToken: "cursor-a",
      label: "Search history",
      fetchPage: replay.fetchPage,
    }),
    (error: unknown) => error instanceof RepeatedPageCursorError
      && /Search history returned a repeated page cursor\./.test(error.message),
  );
});

test("an empty first-page token is a resumed chain that suppresses the total request", async () => {
  const server = scriptedServer([{ items: ["only"], page: { nextPageToken: "" } }]);
  const collected = await collectCursorPages<string>({
    maximumPages: 4,
    pageToken: "",
    label: "The field catalog",
    fetchPage: server.fetchPage,
  });
  assert.deepEqual(server.requests, [{ pageToken: "", includeTotalSize: false }]);
  assert.deepEqual(collected, {
    items: ["only"],
    nextPageToken: null,
    totalSize: null,
    totalSizeExact: false,
    complete: true,
  });
});

test("blank, whitespace, and absent server cursors all terminate the walk", async () => {
  for (const nextPageToken of [undefined, "", "   ", "\t\n"]) {
    const server = scriptedServer([{ items: ["end"], page: { nextPageToken } }]);
    // eslint-disable-next-line no-await-in-loop
    const collected = await collectCursorPages<string>({
      maximumPages: 4,
      label: "Saved searches",
      fetchPage: server.fetchPage,
    });
    assert.equal(collected.complete, true, JSON.stringify(nextPageToken));
    assert.equal(collected.nextPageToken, null, JSON.stringify(nextPageToken));
    assert.equal(server.requests.length, 1, JSON.stringify(nextPageToken));
  }
});

test("a repeated server cursor aborts the walk even when only whitespace differs", async () => {
  const server = scriptedServer([
    { items: ["a"], page: { nextPageToken: "c1" } },
    { items: ["b"], page: { nextPageToken: " c1\n" } },
  ]);
  await assert.rejects(
    collectCursorPages<string>({
      maximumPages: 64,
      label: "The field catalog",
      fetchPage: server.fetchPage,
    }),
    RepeatedPageCursorError,
  );
  assert.equal(server.requests.length, 2, "the loop stops at the duplicate, not at the ceiling");
});

test("the page ceiling reports a resume cursor and an incomplete chain", async () => {
  const server = scriptedServer([
    { items: ["a"], page: { nextPageToken: "c1", totalSize: 7n } },
    { items: ["b"], page: { nextPageToken: "c2" } },
    { items: ["never"], page: {} },
  ]);
  const collected = await collectCursorPages<string>({
    maximumPages: 2,
    label: "Saved searches",
    fetchPage: server.fetchPage,
  });
  assert.deepEqual(collected, {
    items: ["a", "b"],
    nextPageToken: "c2",
    totalSize: 7n,
    totalSizeExact: false,
    complete: false,
  });
  assert.equal(server.requests.length, 2);
});

test("a zero page ceiling issues no request and echoes the caller's cursor", async () => {
  const idle = scriptedServer([]);
  assert.deepEqual(await collectCursorPages<string>({
    maximumPages: 0,
    pageToken: "cursor-z",
    label: "Saved searches",
    fetchPage: idle.fetchPage,
  }), {
    items: [],
    nextPageToken: "cursor-z",
    totalSize: null,
    totalSizeExact: false,
    complete: false,
  });
  assert.equal(idle.requests.length, 0);

  const fresh = scriptedServer([]);
  const collected = await collectCursorPages<string>({
    maximumPages: 0,
    label: "Saved searches",
    fetchPage: fresh.fetchPage,
  });
  assert.equal(collected.nextPageToken, null);
  assert.equal(collected.complete, false);
});

test("a mid-chain transport failure propagates without a partial collection", async () => {
  const failure = new Error("transport exploded");
  let calls = 0;
  await assert.rejects(collectCursorPages<string>({
    maximumPages: 8,
    label: "Saved searches",
    fetchPage: () => {
      calls += 1;
      if (calls === 1) return Promise.resolve({ items: ["a"], page: { nextPageToken: "c1" } });
      return Promise.reject(failure);
    },
  }), (error: unknown) => error === failure);
  assert.equal(calls, 2);
});
