import assert from "node:assert/strict";
import test from "node:test";

import { ExportJobState } from "@/gen/ts/open_splunk/export";

import { buildExportListRequest } from "./backend-export-jobs";

test("export-list requests let the route choose its page size", () => {
  const initial = buildExportListRequest("all", "");
  assert.equal(initial.page?.pageSize, undefined);
  assert.equal(Object.hasOwn(initial.page ?? {}, "pageSize"), false);
  assert.deepEqual(initial, {
    page: { pageToken: undefined, includeTotalSize: true },
    stateFilters: [],
    searchJobIdFilter: undefined,
  });

  const continuation = buildExportListRequest("active", "search-1", "next-page");
  assert.equal(continuation.page?.pageSize, undefined);
  assert.equal(Object.hasOwn(continuation.page ?? {}, "pageSize"), false);
  assert.deepEqual(continuation, {
    page: { pageToken: "next-page", includeTotalSize: true },
    stateFilters: [
      ExportJobState.EXPORT_JOB_STATE_QUEUED,
      ExportJobState.EXPORT_JOB_STATE_RUNNING,
    ],
    searchJobIdFilter: "search-1",
  });
});
