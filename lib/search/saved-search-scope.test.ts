import assert from "node:assert/strict";
import test from "node:test";

import { SharingScope } from "@/gen/ts/open_splunk/common";
import {
  SavedSearch,
  type SavedSearchDefinition,
} from "@/gen/ts/open_splunk/saved_search";
import type {
  CreateSavedSearchRequest,
  UpdateSavedSearchRequest,
} from "@/gen/ts/open_splunk/saved_search_api";
import { SearchResultTab } from "@/gen/ts/open_splunk/search";
import { ServerFeature } from "@/gen/ts/open_splunk/system_api";
import { OpenSplunkApiClient } from "@/lib/api/open-splunk-client";
import type { ProtobufRoute, ProtobufTransport } from "@/lib/api/protobuf-transport";
import type { SystemBootstrapModel } from "@/lib/api/system-bootstrap";

import {
  createServerSavedSearch,
  getServerSavedSearch,
  type ServerSavedSearch,
} from "./server-objects";
import {
  assertSavedSearchScopeUpdate,
  updateSavedSearchScope,
} from "./saved-search-scope";

function bootstrap(): SystemBootstrapModel {
  return {
    apps: [],
    build: null,
    features: new Set([ServerFeature.SERVER_FEATURE_SAVED_SEARCHES]),
    indexes: [],
    limits: {
      defaultSearchTimeoutMs: 0,
      maximumExportBytes: 0n,
      maximumExportRows: 0n,
      maximumFieldSummaryValues: 0,
      maximumPageSize: 100,
      maximumPreviewRows: 0,
      maximumTimelineBuckets: 0,
      maximumWebsocketFrameBytes: 0n,
      maximumWebsocketSubscriptions: 0,
      searchResultRetentionMs: 0,
    },
    palette: "classic",
    searchWebsocketPath: null,
    selectedAppId: "search",
    serverTime: new Date("2026-09-12T00:00:00Z"),
  };
}

function definition(scope: SharingScope, appId: string | undefined = "search"): SavedSearchDefinition {
  return {
    description: "Operational errors",
    name: "Errors",
    ownerId: "current-user",
    schedule: {
      configVersion: 2n,
      cron: "*/5 * * * *",
      dispatchTtl: "2p",
      enabled: true,
      timezone: "UTC",
    },
    search: {
      appId,
      indexScope: [],
      preferredResultTab: SearchResultTab.SEARCH_RESULT_TAB_EVENTS,
      selectedFields: ["host"],
      spl: "index=main | fields host",
      timeRange: undefined,
      visualization: undefined,
    },
    sharingScope: scope,
  };
}

function protoSavedSearch(
  scope: SharingScope,
  version: bigint,
  appId: string | undefined = "search",
  scheduleVersion = 2n,
): SavedSearch {
  const savedDefinition = definition(scope, appId);
  if (savedDefinition.schedule !== undefined) savedDefinition.schedule.configVersion = scheduleVersion;
  return SavedSearch.fromPartial({
    definition: savedDefinition,
    savedSearchId: "saved-1",
    version,
  });
}

function serverSavedSearch(scope: SharingScope, version = 3n): ServerSavedSearch {
  return {
    createdAt: null,
    description: "Operational errors",
    id: "saved-1",
    name: "Errors",
    ownerId: "current-user",
    schedule: {
      configVersion: 2n,
      cron: "*/5 * * * *",
      dispatchTtl: "2p",
      enabled: true,
      timezone: "UTC",
    },
    scheduleStatus: null,
    search: definition(scope).search!,
    sharingScope: scope,
    updatedAt: null,
    version,
  };
}

test("a scope edit sends only sharing_scope and accepts an independently advanced schedule", async () => {
  const requests: UpdateSavedSearchRequest[] = [];
  const transport = {
    post(_route: ProtobufRoute<unknown, unknown>, request: UpdateSavedSearchRequest) {
      requests.push(request);
      return Promise.resolve({
        savedSearch: protoSavedSearch(SharingScope.SHARING_SCOPE_GLOBAL, 4n, "search", 9n),
      });
    },
  } as unknown as ProtobufTransport;
  const baseline = serverSavedSearch(SharingScope.SHARING_SCOPE_PRIVATE);
  const updated = await updateSavedSearchScope(
    new OpenSplunkApiClient(transport),
    bootstrap(),
    baseline,
    SharingScope.SHARING_SCOPE_GLOBAL,
  );

  assert.equal(updated.sharingScope, SharingScope.SHARING_SCOPE_GLOBAL);
  assert.equal(updated.schedule?.configVersion, 9n);
  assert.equal(requests.length, 1);
  assert.deepEqual(requests[0]?.updateMask, ["sharing_scope"]);
  assert.equal(requests[0]?.savedSearchId, baseline.id);
  assert.equal(requests[0]?.expectedVersion, baseline.version);
  assert.deepEqual(requests[0]?.definition?.search, baseline.search);
  assert.equal(requests[0]?.definition?.sharingScope, SharingScope.SHARING_SCOPE_GLOBAL);
  assert.equal(requests[0]?.definition?.ownerId, "current-user");
  assert.equal(requests[0]?.definition?.schedule, undefined);
});

test("scope edits reject unknown baselines, invalid proposals, and App scope without an app", async () => {
  let calls = 0;
  const client = new OpenSplunkApiClient({
    post() {
      calls += 1;
      return Promise.reject(new Error("must not be called"));
    },
  } as unknown as ProtobufTransport);
  await assert.rejects(
    updateSavedSearchScope(
      client,
      bootstrap(),
      serverSavedSearch(SharingScope.UNRECOGNIZED),
      SharingScope.SHARING_SCOPE_PRIVATE,
    ),
    /invalid baseline/u,
  );
  await assert.rejects(
    updateSavedSearchScope(
      client,
      bootstrap(),
      serverSavedSearch(SharingScope.SHARING_SCOPE_PRIVATE),
      SharingScope.UNRECOGNIZED as never,
    ),
    /invalid baseline or proposed scope/u,
  );
  const noApp = serverSavedSearch(SharingScope.SHARING_SCOPE_PRIVATE);
  noApp.search = { ...noApp.search, appId: undefined };
  await assert.rejects(
    updateSavedSearchScope(client, bootstrap(), noApp, SharingScope.SHARING_SCOPE_APP),
    /requires a saved-search app/u,
  );
  assert.equal(calls, 0);
});

test("scope response validation rejects ownership, search, scope, identity, and version drift", () => {
  const baseline = serverSavedSearch(SharingScope.SHARING_SCOPE_PRIVATE);
  const valid = { ...baseline, sharingScope: SharingScope.SHARING_SCOPE_GLOBAL, version: 4n };
  assert.doesNotThrow(() => assertSavedSearchScopeUpdate(
    baseline,
    SharingScope.SHARING_SCOPE_GLOBAL,
    valid,
  ));
  for (const changed of [
    { ...valid, id: "another" },
    { ...valid, version: 5n },
    { ...valid, sharingScope: SharingScope.SHARING_SCOPE_APP },
    { ...valid, ownerId: "another-user" },
    { ...valid, search: { ...valid.search, spl: "index=audit" } },
  ]) {
    assert.throws(
      () => assertSavedSearchScopeUpdate(baseline, SharingScope.SHARING_SCOPE_GLOBAL, changed),
      /invalid saved-search sharing update/u,
    );
  }
});

test("create defaults to Private, persists each explicit scope, and GET reopens it", async () => {
  const records = new Map<string, SavedSearch>();
  const requests: CreateSavedSearchRequest[] = [];
  const transport = {
    post(route: ProtobufRoute<unknown, unknown>, request: CreateSavedSearchRequest | { savedSearchId: string }) {
      if (route.path.endsWith("/create")) {
        const create = request as CreateSavedSearchRequest;
        requests.push(create);
        const saved = SavedSearch.fromPartial({
          definition: create.definition,
          savedSearchId: `saved-${requests.length}`,
          version: 1n,
        });
        records.set(saved.savedSearchId, saved);
        return Promise.resolve({ savedSearch: saved });
      }
      const get = request as { savedSearchId: string };
      return Promise.resolve({ savedSearch: records.get(get.savedSearchId) });
    },
  } as unknown as ProtobufTransport;
  const client = new OpenSplunkApiClient(transport);
  const scopes: Array<SharingScope | undefined> = [
    undefined,
    SharingScope.SHARING_SCOPE_APP,
    SharingScope.SHARING_SCOPE_GLOBAL,
  ];

  await Promise.all(scopes.map(async (scope, index) => {
    const created = await createServerSavedSearch(client, bootstrap(), {
      name: `Saved ${index}`,
      search: {
        appId: "search",
        earliest: "-15m",
        indexScope: ["main"],
        latest: "now",
        spl: "index=main",
      },
      ...(scope === undefined ? {} : { sharingScope: scope }),
    });
    assert.equal(created.status, "available");
    if (created.status !== "available") return;
    const reopened = await getServerSavedSearch(client, bootstrap(), created.value.id);
    assert.equal(reopened.status, "available");
    if (reopened.status === "available") {
      assert.equal(
        reopened.value.sharingScope,
        scope ?? SharingScope.SHARING_SCOPE_PRIVATE,
      );
    }
  }));
  assert.deepEqual(
    requests.map((request) => request.definition?.sharingScope),
    [
      SharingScope.SHARING_SCOPE_PRIVATE,
      SharingScope.SHARING_SCOPE_APP,
      SharingScope.SHARING_SCOPE_GLOBAL,
    ],
  );
  assert.ok(requests.every((request) => request.definition?.ownerId === undefined));
});
