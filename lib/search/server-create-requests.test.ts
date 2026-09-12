import assert from "node:assert/strict";
import test from "node:test";

import { CreateExportJobRequest, CreateExportJobResponse } from "@/gen/ts/open_splunk/export_api";
import { ExportJob } from "@/gen/ts/open_splunk/export";
import { PatternSensitivity } from "@/gen/ts/open_splunk/patterns_api";
import { SavedSearch } from "@/gen/ts/open_splunk/saved_search";
import { SearchJob, SearchJobState } from "@/gen/ts/open_splunk/search";
import { GetSystemBootstrapResponse, ServerFeature } from "@/gen/ts/open_splunk/system_api";
import { createOpenSplunkApiClient, type OpenSplunkApiClient } from "@/lib/api";
import { adaptSystemBootstrap } from "@/lib/api/system-bootstrap";
import { BrowserCreateAction } from "@/lib/api/client-request-id";

import { createServerExport } from "./server-exports";
import { adaptServerSearchJob, rerunServerSearchJob } from "./server-jobs";
import { createServerSavedSearch, duplicateServerSavedSearch } from "./server-objects";

const bootstrap = adaptSystemBootstrap(GetSystemBootstrapResponse.fromPartial({
  serverTime: new Date("2026-09-12T00:00:00Z"), features: [
    ServerFeature.SERVER_FEATURE_SEARCH, ServerFeature.SERVER_FEATURE_SAVED_SEARCHES,
    ServerFeature.SERVER_FEATURE_EXPORT_CSV,
  ],
}));
const searchJob = SearchJob.fromPartial({
  searchJobId: "search-1", state: SearchJobState.SEARCH_JOB_STATE_COMPLETED,
  definition: { spl: "index=main | stats count" }, source: {}, progress: {}, createdAt: new Date(),
});
const saved = SavedSearch.fromPartial({ savedSearchId: "saved-1", version: 7n, definition: { name: "Current name", search: searchJob.definition } });

test("saved create, duplicate, rerun, and export constructors preserve their supplied logical-action keys", async () => {
  const seen: Array<{ route: string; key: string | undefined }> = [];
  const client = {
    savedSearches: {
      create: async (request: { clientRequestId?: string }) => { seen.push({ route: "save", key: request.clientRequestId }); return { savedSearch: saved, replayed: true }; },
      duplicate: async (request: { clientRequestId?: string }) => { seen.push({ route: "duplicate", key: request.clientRequestId }); return { savedSearch: saved, replayed: true }; },
    },
    search: { create: async (request: { clientRequestId?: string }) => { seen.push({ route: "search", key: request.clientRequestId }); return { searchJob, replayed: true }; } },
    exports: { create: async (request: { clientRequestId?: string }) => { seen.push({ route: "export", key: request.clientRequestId }); return { exportJob: ExportJob.fromPartial({ exportJobId: "export-1" }), replayed: true }; } },
  } as unknown as OpenSplunkApiClient;
  const action = new BrowserCreateAction();
  const options = { clientRequestId: action.requestId({ search: searchJob.definition }) };
  const save = { ...options, name: "Report", search: { spl: "index=main", earliest: "-1h", latest: "now", indexScope: ["main"] } };
  const first = await createServerSavedSearch(client, bootstrap, save);
  assert.equal(first.status === "available" && first.value.version, 7n, "replay must accept current metadata");
  await createServerSavedSearch(client, bootstrap, save);
  await duplicateServerSavedSearch(client, bootstrap, "saved-1", "Copy", undefined, options);
  await rerunServerSearchJob(client, bootstrap, adaptServerSearchJob(searchJob), options);
  await createServerExport(client, bootstrap, { ...options, searchJobId: "search-1", format: "csv" });
  assert.deepEqual(seen.map((entry) => entry.route), ["save", "save", "duplicate", "search", "export"]);
  assert.ok(seen.every((entry) => entry.key === options.clientRequestId));
  assert.equal(action.requestId({ search: searchJob.definition }), options.clientRequestId);
});

test("pattern export constructors preserve source identity and reject mixed search jobs before dispatch", async () => {
  let calls = 0;
  const source = { $case: "patternMembers" as const, value: {
    searchJobId: "search-1", snapshotRef: "opaque-snapshot", sensitivity: PatternSensitivity.PATTERN_SENSITIVITY_BALANCED, patternId: "opaque-pattern",
  } };
  const client = { exports: { create: async (request: { definition?: { source?: unknown } }) => {
    calls += 1;
    assert.deepEqual(request.definition?.source, source);
    return { exportJob: ExportJob.fromPartial({ exportJobId: "export-1" }) };
  } } } as unknown as OpenSplunkApiClient;
  await createServerExport(client, bootstrap, { searchJobId: "search-1", format: "csv", source });
  await assert.rejects(createServerExport(client, bootstrap, { searchJobId: "search-2", format: "csv", source }), /selected search job/u);
  assert.equal(calls, 1);
});

test("new independent create calls receive valid UUIDs when no logical-action key is supplied", async () => {
  const keys: Array<string | undefined> = [];
  const client = createOpenSplunkApiClient({ baseUrl: "https://example.test", fetch: async (_url, options) => {
    keys.push(CreateExportJobRequest.decode(options?.body as Uint8Array).clientRequestId);
    return new Response(CreateExportJobResponse.encode(CreateExportJobResponse.fromPartial({ exportJob: { exportJobId: "export-1" } })).finish(), { headers: { "content-type": "application/x-protobuf" } });
  } });
  await createServerExport(client, bootstrap, { searchJobId: "search-1", format: "csv" });
  await createServerExport(client, bootstrap, { searchJobId: "search-1", format: "csv" });
  assert.match(keys[0] ?? "", /^[0-9a-f]{8}-[0-9a-f-]{27}$/u);
  assert.notEqual(keys[0], keys[1]);
});
