import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";

import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import { SharingScope, SortDirection } from "@/gen/ts/open_splunk/v1/common";
import {
  KnowledgeDependencyRole,
  KnowledgeObject,
  KnowledgeObjectState,
  KnowledgeObjectType,
  KnowledgeOverwriteBehavior,
  KnowledgeSelectorMatchKind,
} from "@/gen/ts/open_splunk/v1/knowledge";
import {
  ListKnowledgeObjectDependenciesResponse,
  ListKnowledgeObjectDependentsResponse,
  KnowledgeObjectSortBy,
  ListKnowledgeObjectsResponse,
  type GetKnowledgeObjectRequest,
  type GetKnowledgeObjectResponse,
  type KnowledgeManagementDependencyEdge,
  type ListKnowledgeObjectDependenciesRequest,
  type ListKnowledgeObjectDependenciesResponse as ListKnowledgeObjectDependenciesResponseMessage,
  type ListKnowledgeObjectDependentsRequest,
  type ListKnowledgeObjectDependentsResponse as ListKnowledgeObjectDependentsResponseMessage,
  type ListKnowledgeObjectsRequest,
  type ListKnowledgeObjectsResponse as ListKnowledgeObjectsResponseMessage,
} from "@/gen/ts/open_splunk/v1/knowledge_api";
import { HttpError } from "@/lib/api/protobuf-transport";
import { knowledgeRoutes } from "@/lib/api/routes";

import {
  KNOWLEDGE_MANAGER_MAXIMUM_DEPENDENCIES,
  KNOWLEDGE_MANAGER_MAXIMUM_DEPENDENTS,
  KNOWLEDGE_MANAGER_MAXIMUM_FILTER_BYTES,
  KNOWLEDGE_MANAGER_MAXIMUM_GRAPH_RESPONSE_BYTES,
  KNOWLEDGE_MANAGER_MAXIMUM_OBJECTS,
  KNOWLEDGE_MANAGER_MAXIMUM_PAGE_TOKEN_BYTES,
  adaptKnowledgeObject,
  adaptKnowledgePage,
  adaptKnowledgeRelationshipPage,
  knowledgeDetailRequest,
  knowledgeLifecycleStateFilterFromControlValue,
  knowledgeListRequest,
  knowledgeObjectTypeFilterFromControlValue,
  knowledgeSharingScopeFilterFromControlValue,
  knowledgeRelationshipRequest,
  knowledgeSortChoiceFromControlValue,
  knowledgeTextFilterFromDraft,
  loadKnowledgeDetail,
  loadKnowledgePage,
  loadKnowledgeRelationshipPage,
  mergeKnowledgeContinuation,
  mergeKnowledgeRelationshipContinuation,
  type KnowledgeLifecycleStateFilter,
  type KnowledgeDetailQuery,
  type KnowledgeListQuery,
  type KnowledgeReadClient,
  type KnowledgeRelationshipPageDisplay,
  type KnowledgeRelationshipQuery,
} from "./knowledge-manager-data";
import {
  KNOWLEDGE_MANAGER_MAXIMUM_BOOTSTRAP_APPS,
  backendAdminNavigation,
  knowledgeManagerAppOptionsFromBootstrap,
  loadKnowledgeManagerModuleIfAdvertised,
  safeKnowledgeManagerAppOptions,
} from "./knowledge-manager-feature";
import {
  KnowledgeDetail,
  KnowledgeManagerPanel,
  KnowledgeRelationshipSectionView,
  KnowledgeManagerWorkspace,
  commitKnowledgeManagerQueryChange,
  knowledgeManagerUnmountCleanup,
  knowledgeRelationshipSectionKey,
  knowledgeRelationshipUnmountCleanup,
  normalizeAdvancedFilterDrafts,
  resetKnowledgeManagerQuery,
} from "./knowledge-manager-panel";

const pageSize = 50;
const TestKnowledgePanel = () => null;

const unavailableGraphReads = {
  async dependencies(): Promise<ListKnowledgeObjectDependenciesResponseMessage> {
    throw new Error("dependencies must not be called");
  },
  async dependents(): Promise<ListKnowledgeObjectDependentsResponseMessage> {
    throw new Error("dependents must not be called");
  },
};

function fieldAliasObject(options: {
  id?: string;
  name?: string;
  appId?: string;
  version?: bigint;
  state?: KnowledgeObjectState;
} = {}): KnowledgeObject {
  const id = options.id ?? "ko-alias-1";
  const name = options.name ?? "http_status_alias";
  const appId = options.appId ?? "app-observability";
  const state = options.state ?? KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE;
  const updatedAt = new Date("2026-08-02T10:00:00.000Z");
  return KnowledgeObject.fromPartial({
    knowledgeObjectId: id,
    tenantId: "tenant-local",
    appId,
    ownerId: "administrator",
    objectType: KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
    name,
    version: options.version ?? 2n,
    sharingScope: SharingScope.SHARING_SCOPE_APP,
    state,
    definition: {
      appId,
      name,
      description: "Safe administrator description",
      sharingScope: SharingScope.SHARING_SCOPE_APP,
      selector: {
        indexPatterns: [{
          matchKind: KnowledgeSelectorMatchKind.KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT,
          value: "main",
        }],
      },
      body: {
        $case: "fieldAlias",
        value: {
          sourceField: "status",
          destinationField: "http_status",
          overwriteBehavior:
            KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
        },
      },
    },
    definitionSha256: new Uint8Array(32).fill(7),
    createdAt: new Date("2026-08-01T10:00:00.000Z"),
    updatedAt,
    disabledAt: state === KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DISABLED
      ? new Date(updatedAt)
      : undefined,
    deletedAt: state === KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DELETED
      ? new Date(updatedAt)
      : undefined,
  });
}

function calculatedObjectWithSecret(): KnowledgeObject {
  const object = fieldAliasObject({ id: "ko-calculated", name: "latency_bucket", version: 3n });
  object.objectType = KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD;
  object.definition!.body = {
    $case: "calculatedField",
    value: {
      destinationField: "latency_class",
      expression: "TOP_SECRET_AUTHORED_EXPRESSION",
      overwriteBehavior:
        KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
    },
  };
  return object;
}

function listResponse(options: {
  objects?: KnowledgeObject[];
  nextPageToken?: string;
  totalSize?: bigint;
  revision?: bigint;
} = {}): ListKnowledgeObjectsResponseMessage {
  const objects = options.objects ?? [fieldAliasObject()];
  return ListKnowledgeObjectsResponse.fromPartial({
    knowledgeObjects: objects,
    page: {
      nextPageToken: options.nextPageToken,
      totalSize: options.totalSize ?? BigInt(objects.length),
      totalSizeExact: true,
    },
    tenantCatalogRevision: options.revision ?? 7n,
  });
}

function query(
  pageToken: string | null = null,
  overrides: Partial<KnowledgeListQuery> = {},
): KnowledgeListQuery {
  return {
    appId: "app-observability",
    ownerId: null,
    text: null,
    objectType: "all",
    lifecycleState: "all",
    sharingScope: "all",
    selectorText: null,
    sort: "name-ascending",
    pageSize,
    pageToken,
    ...overrides,
  };
}

function relationshipQuery(
  direction: KnowledgeRelationshipQuery["direction"],
  pageToken: string | null = null,
  overrides: Partial<KnowledgeRelationshipQuery> = {},
): KnowledgeRelationshipQuery {
  return {
    direction,
    knowledgeObjectId: "ko-root",
    version: 3n,
    pageSize: 2,
    pageToken,
    ...overrides,
  };
}

function relationshipEdge(
  direction: KnowledgeRelationshipQuery["direction"],
  neighborId: string,
  neighborVersion: bigint,
): KnowledgeManagementDependencyEdge {
  const root = { knowledgeObjectId: "ko-root", version: 3n };
  const neighbor = { knowledgeObjectId: neighborId, version: neighborVersion };
  return {
    source: direction === "dependencies" ? root : neighbor,
    target: direction === "dependencies" ? neighbor : root,
    role: KnowledgeDependencyRole.KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT,
  };
}

function relationshipResponse(
  queryValue: KnowledgeRelationshipQuery,
  options: {
    edges?: KnowledgeManagementDependencyEdge[];
    nextPageToken?: string;
    totalSize?: bigint;
    revision?: bigint;
  } = {},
): ListKnowledgeObjectDependenciesResponseMessage | ListKnowledgeObjectDependentsResponseMessage {
  const edges = options.edges ?? [];
  const common = {
    page: {
      nextPageToken: options.nextPageToken,
      totalSize: options.totalSize ?? BigInt(edges.length),
      totalSizeExact: true,
    },
    tenantCatalogRevision: options.revision ?? 9n,
    resolvedObject: {
      knowledgeObjectId: queryValue.knowledgeObjectId,
      version: queryValue.version,
    },
  };
  return queryValue.direction === "dependencies"
    ? ListKnowledgeObjectDependenciesResponse.fromPartial({
      ...common,
      dependencies: edges,
    })
    : ListKnowledgeObjectDependentsResponse.fromPartial({
      ...common,
      dependents: edges,
    });
}

function jsonWithoutBigInt(value: unknown): string {
  return JSON.stringify(value, (_key, item: unknown) =>
    typeof item === "bigint" ? item.toString() : item);
}

function mediaRuleBodies(css: string, condition: string): string[] {
  const marker = `@media (${condition})`;
  const bodies: string[] = [];
  let cursor = 0;
  while (cursor < css.length) {
    const ruleStart = css.indexOf(marker, cursor);
    if (ruleStart < 0) break;
    const bodyStart = css.indexOf("{", ruleStart + marker.length);
    assert.notEqual(bodyStart, -1, `missing body for ${marker}`);
    let depth = 1;
    let bodyEnd = bodyStart + 1;
    while (bodyEnd < css.length && depth > 0) {
      if (css[bodyEnd] === "{") depth += 1;
      else if (css[bodyEnd] === "}") depth -= 1;
      bodyEnd += 1;
    }
    assert.equal(depth, 0, `unterminated body for ${marker}`);
    bodies.push(css.slice(bodyStart + 1, bodyEnd - 1));
    cursor = bodyEnd;
  }
  return bodies;
}

test("feature-absent navigation is unchanged and invokes no knowledge chunk importer", async () => {
  assert.deepEqual(
    backendAdminNavigation(false).map(({ key, label, detail }) => ({ key, label, detail })),
    [
      { key: "overview", label: "System overview", detail: "Capabilities and limits" },
      { key: "apps", label: "Apps", detail: "Workspaces and defaults" },
      { key: "indexes", label: "Indexes", detail: "State and retention" },
      { key: "collector-fleet", label: "Collector fleet", detail: "Health, queues, and inputs" },
      { key: "collectors", label: "Ingestion tokens", detail: "Credentials and scopes" },
      { key: "access", label: "Users & access", detail: "Not exposed by this server" },
      { key: "server", label: "Server settings", detail: "Read-only limits" },
    ],
  );
  let imports = 0;
  const result = await loadKnowledgeManagerModuleIfAdvertised(false, async () => {
    imports += 1;
    throw new Error("feature-absent importer must not run");
  });
  assert.equal(result, null);
  assert.equal(imports, 0);
});

test("feature-advertised navigation adds one read-only destination and loads its module once", async () => {
  const navigation = backendAdminNavigation(true);
  assert.equal(navigation.filter((item) => item.key === "knowledge").length, 1);
  assert.equal(navigation.find((item) => item.key === "knowledge")?.detail, "Read-only definitions");
  let imports = 0;
  const loaded = await loadKnowledgeManagerModuleIfAdvertised(true, async () => {
    imports += 1;
    return { KnowledgeManagerPanel: TestKnowledgePanel };
  });
  assert.equal(imports, 1);
  assert.equal(loaded?.KnowledgeManagerPanel, TestKnowledgePanel);
});

test("oversized and spoofed app fixtures fail closed before entries are scanned", () => {
  let bootstrapEntryReads = 0;
  const oversizedBootstrap = new Proxy(
    Array.from({ length: KNOWLEDGE_MANAGER_MAXIMUM_BOOTSTRAP_APPS + 1 }, () => ({
      appId: "must-not-be-read",
      displayName: "Must not be read",
      slug: "must-not-be-read",
    })),
    {
      get(target, property, receiver) {
        if (property !== "length") bootstrapEntryReads += 1;
        return Reflect.get(target, property, receiver);
      },
    },
  );
  assert.equal(knowledgeManagerAppOptionsFromBootstrap(oversizedBootstrap), null);
  assert.equal(bootstrapEntryReads, 0);

  let panelEntryReads = 0;
  const oversizedPanel = new Proxy(
    Array.from({ length: KNOWLEDGE_MANAGER_MAXIMUM_BOOTSTRAP_APPS + 1 }, () => ({
      appId: "must-not-be-read",
      label: "Must not be read",
    })),
    {
      get(target, property, receiver) {
        if (property !== "length") panelEntryReads += 1;
        return Reflect.get(target, property, receiver);
      },
    },
  );
  assert.equal(safeKnowledgeManagerAppOptions(oversizedPanel), null);
  assert.equal(panelEntryReads, 0);

  assert.deepEqual(safeKnowledgeManagerAppOptions([{
    appId: "safe-app",
    label: "\ud83d\udca1".repeat(256),
  }]), [{ appId: "safe-app", label: "safe-app" }]);
});

test("unmount cleanup aborts continuation and detail controllers assigned after mount", () => {
  const listRequest = { current: null as AbortController | null };
  const detailRequest = { current: null as AbortController | null };
  const cleanup = knowledgeManagerUnmountCleanup(listRequest, detailRequest);
  const delayedContinuation = new AbortController();
  const delayedDetail = new AbortController();
  listRequest.current = delayedContinuation;
  detailRequest.current = delayedDetail;

  cleanup();

  assert.equal(delayedContinuation.signal.aborted, true);
  assert.equal(delayedDetail.signal.aborted, true);
  assert.equal(listRequest.current, null);
  assert.equal(detailRequest.current, null);
});

test("query changes abort requests and reset list, detail, and continuation state", () => {
  const continuation = new AbortController();
  const detail = new AbortController();
  const listRequest = { current: continuation as AbortController | null };
  const detailRequest = { current: detail as AbortController | null };
  const priorTokens = new Set(["consumed-cursor"]);
  const consumedPageTokens = { current: priorTokens };
  let page: object | null = { stale: true };
  let continuationStale = true;
  let selectedObjectId: string | null = "ko-stale";
  let detailValue: object | null = { stale: true };

  resetKnowledgeManagerQuery(
    listRequest,
    detailRequest,
    consumedPageTokens,
    () => {
      page = null;
      continuationStale = false;
    },
    () => {
      selectedObjectId = null;
      detailValue = null;
    },
  );

  assert.equal(continuation.signal.aborted, true);
  assert.equal(detail.signal.aborted, true);
  assert.equal(listRequest.current, null);
  assert.equal(detailRequest.current, null);
  assert.notEqual(consumedPageTokens.current, priorTokens);
  assert.equal(consumedPageTokens.current.size, 0);
  assert.equal(page, null);
  assert.equal(continuationStale, false);
  assert.equal(selectedObjectId, null);
  assert.equal(detailValue, null);
});

test("closed query changes reset before update and invalid values fail closed", () => {
  const events: string[] = [];
  commitKnowledgeManagerQueryChange<KnowledgeLifecycleStateFilter>(
    "all",
    "active",
    () => events.push("reset"),
    (value) => events.push(`update:${value}`),
    () => events.push("unavailable"),
  );
  assert.equal(events.join(","), "reset,update:active");

  events.length = 0;
  commitKnowledgeManagerQueryChange<KnowledgeLifecycleStateFilter>(
    "active",
    "active",
    () => events.push("reset"),
    (value) => events.push(`update:${value}`),
    () => events.push("unavailable"),
  );
  assert.equal(events.length, 0);

  commitKnowledgeManagerQueryChange<KnowledgeLifecycleStateFilter>(
    "active",
    undefined,
    () => events.push("reset"),
    (value) => events.push(`update:${value}`),
    () => events.push("unavailable"),
  );
  assert.equal(events.join(","), "reset,unavailable");
});

test("enabled list and detail fixtures use bounded generated protobuf requests", async () => {
  const listRequests: ListKnowledgeObjectsRequest[] = [];
  const getRequests: GetKnowledgeObjectRequest[] = [];
  const fixture = fieldAliasObject();
  const client: KnowledgeReadClient = {
    ...unavailableGraphReads,
    async list(request) {
      listRequests.push(request);
      return listResponse({ objects: [fixture], revision: 8n });
    },
    async get(request): Promise<GetKnowledgeObjectResponse> {
      getRequests.push(request);
      return { knowledgeObject: fixture };
    },
  };

  const loaded = await loadKnowledgePage(client, query());
  assert.equal(loaded.status, "available");
  assert.deepEqual(listRequests, [{
    page: { pageSize: 50, pageToken: undefined, includeTotalSize: true },
    appIdFilter: "app-observability",
    ownerIdFilter: undefined,
    textFilter: undefined,
    objectTypeFilters: [],
    stateFilters: [],
    sharingScopeFilters: [],
    selectorTextFilter: undefined,
    sortBy: KnowledgeObjectSortBy.KNOWLEDGE_OBJECT_SORT_BY_NAME,
    sortDirection: SortDirection.SORT_DIRECTION_ASCENDING,
  }]);
  const detail = await loadKnowledgeDetail(client, {
    knowledgeObjectId: fixture.knowledgeObjectId,
    version: fixture.version,
  });
  assert.equal(detail.status, "available");
  assert.deepEqual(getRequests, [{
    knowledgeObjectId: fixture.knowledgeObjectId,
    version: fixture.version,
  }]);
  assert.deepEqual(knowledgeDetailRequest({
    knowledgeObjectId: fixture.knowledgeObjectId,
    version: fixture.version,
  }), getRequests[0]);
});

test("detail reads reject invalid exact identities before I/O", async () => {
  let calls = 0;
  const client: KnowledgeReadClient = {
    ...unavailableGraphReads,
    async list() { throw new Error("list must not be called"); },
    async get() {
      calls += 1;
      return { knowledgeObject: fieldAliasObject() };
    },
  };
  const invalidQueries = [
    { knowledgeObjectId: "", version: 1n },
    { knowledgeObjectId: "ko-\u0000unsafe", version: 1n },
    { knowledgeObjectId: "x".repeat(129), version: 1n },
    { knowledgeObjectId: "ko-safe", version: 0n },
    { knowledgeObjectId: "ko-safe", version: 9_223_372_036_854_775_808n },
    { knowledgeObjectId: "ko-safe", version: 1 as unknown as bigint },
  ] satisfies KnowledgeDetailQuery[];
  const invalidResults = await Promise.all(
    invalidQueries.map((queryValue) => loadKnowledgeDetail(client, queryValue)),
  );
  for (const result of invalidResults) {
    assert.deepEqual(result, { status: "unavailable" });
  }
  assert.equal(calls, 0);
});

test("detail reads fail closed when the returned exact identity differs", async () => {
  const queryValue = { knowledgeObjectId: "ko-exact", version: 4n };
  const mismatches = [
    fieldAliasObject({ id: "ko-other", version: 4n }),
    fieldAliasObject({ id: "ko-exact", version: 5n }),
  ];
  const mismatchResults = await Promise.all(mismatches.map((knowledgeObject) => {
    const client: KnowledgeReadClient = {
      ...unavailableGraphReads,
      async list() { throw new Error("list must not be called"); },
      async get() { return { knowledgeObject }; },
    };
    return loadKnowledgeDetail(client, queryValue);
  }));
  for (const result of mismatchResults) {
    assert.deepEqual(result, { status: "unavailable" });
  }

  const mutableQuery = { ...queryValue };
  const mutatingClient: KnowledgeReadClient = {
    ...unavailableGraphReads,
    async list() { throw new Error("list must not be called"); },
    async get() {
      mutableQuery.knowledgeObjectId = "ko-other";
      mutableQuery.version = 5n;
      return { knowledgeObject: fieldAliasObject({ id: "ko-other", version: 5n }) };
    },
  };
  assert.deepEqual(
    await loadKnowledgeDetail(mutatingClient, mutableQuery),
    { status: "unavailable" },
  );
});

test("relationship reads pin the exact detail version and use independently bounded routes", async () => {
  const dependencyRequests: ListKnowledgeObjectDependenciesRequest[] = [];
  const dependentRequests: ListKnowledgeObjectDependentsRequest[] = [];
  const dependenciesQuery = relationshipQuery("dependencies");
  const dependentsQuery = relationshipQuery("dependents");
  const client: KnowledgeReadClient = {
    async get() { throw new Error("get must not be called"); },
    async list() { throw new Error("list must not be called"); },
    async dependencies(request) {
      dependencyRequests.push(request);
      return relationshipResponse(dependenciesQuery, {
        edges: [relationshipEdge("dependencies", "ko-target", 2n)],
      }) as ListKnowledgeObjectDependenciesResponseMessage;
    },
    async dependents(request) {
      dependentRequests.push(request);
      return relationshipResponse(dependentsQuery, {
        edges: [relationshipEdge("dependents", "ko-source", 4n)],
      }) as ListKnowledgeObjectDependentsResponseMessage;
    },
  };

  const [dependencies, dependents] = await Promise.all([
    loadKnowledgeRelationshipPage(client, dependenciesQuery),
    loadKnowledgeRelationshipPage(client, dependentsQuery),
  ]);
  assert.equal(dependencies.status, "available");
  assert.equal(dependents.status, "available");
  assert.deepEqual(dependencyRequests, [{
    knowledgeObjectId: "ko-root",
    version: 3n,
    page: { pageSize: 2, pageToken: undefined, includeTotalSize: true },
  }]);
  assert.deepEqual(dependentRequests, dependencyRequests);
  if (dependencies.status === "available" && dependents.status === "available") {
    assert.deepEqual(dependencies.page.edges.map((edge) => ({
      id: edge.knowledgeObjectId,
      version: edge.version,
      role: edge.roleLabel,
    })), [{ id: "ko-target", version: 2n, role: "Field input" }]);
    assert.deepEqual(dependents.page.edges.map((edge) => ({
      id: edge.knowledgeObjectId,
      version: edge.version,
      role: edge.roleLabel,
    })), [{ id: "ko-source", version: 4n, role: "Field input" }]);
  }
  assert.equal(
    knowledgeRoutes.dependencies.maximumResponseBytes,
    KNOWLEDGE_MANAGER_MAXIMUM_GRAPH_RESPONSE_BYTES,
  );
  assert.equal(
    knowledgeRoutes.dependents.maximumResponseBytes,
    KNOWLEDGE_MANAGER_MAXIMUM_GRAPH_RESPONSE_BYTES,
  );
});

test("relationship page validation preserves direction-specific progress and UTF-8 binary order", () => {
  const dependencyQuery = relationshipQuery("dependencies");
  const fullOutgoing = adaptKnowledgeRelationshipPage(relationshipResponse(dependencyQuery, {
    edges: [
      relationshipEdge("dependencies", `ko-\ue000`, 1n),
      relationshipEdge("dependencies", `ko-\u{10000}`, 1n),
    ],
    nextPageToken: "outgoing-next",
    totalSize: 3n,
  }), dependencyQuery);
  assert.equal(fullOutgoing.nextPageToken, "outgoing-next");
  assert.deepEqual(
    fullOutgoing.edges.map((edge) => edge.knowledgeObjectId),
    [`ko-\ue000`, `ko-\u{10000}`],
  );

  assert.throws(() => adaptKnowledgeRelationshipPage(relationshipResponse(dependencyQuery, {
    edges: [relationshipEdge("dependencies", "ko-only", 1n)],
    nextPageToken: "short-outgoing-next",
    totalSize: 2n,
  }), dependencyQuery), TypeError);
  assert.throws(() => adaptKnowledgeRelationshipPage(relationshipResponse(dependencyQuery, {
    edges: [
      relationshipEdge("dependencies", `ko-\u{10000}`, 1n),
      relationshipEdge("dependencies", `ko-\ue000`, 1n),
    ],
    nextPageToken: "wrong-order-next",
    totalSize: 3n,
  }), dependencyQuery), TypeError);

  const dependentQuery = relationshipQuery("dependents");
  const shortIncoming = adaptKnowledgeRelationshipPage(relationshipResponse(dependentQuery, {
    edges: [relationshipEdge("dependents", "ko-source", 4n)],
    nextPageToken: "incoming-next",
    totalSize: 2n,
  }), dependentQuery);
  assert.equal(shortIncoming.edges.length, 1);
  assert.equal(shortIncoming.nextPageToken, "incoming-next");
});

test("relationship responses fail closed on roots, edges, totals, and continuations", () => {
  const queryValue = relationshipQuery("dependencies");
  const malformed: Array<
    ListKnowledgeObjectDependenciesResponseMessage | ListKnowledgeObjectDependentsResponseMessage
  > = [];

  const missingEndpoint = relationshipResponse(queryValue, {
    edges: [relationshipEdge("dependencies", "ko-target", 2n)],
  }) as ListKnowledgeObjectDependenciesResponseMessage;
  missingEndpoint.dependencies[0]!.target = undefined;
  malformed.push(missingEndpoint);

  const wrongRoot = relationshipResponse(queryValue, {
    edges: [relationshipEdge("dependencies", "ko-target", 2n)],
  });
  wrongRoot.resolvedObject!.version = 4n;
  malformed.push(wrongRoot);

  const futureRole = relationshipResponse(queryValue, {
    edges: [relationshipEdge("dependencies", "ko-target", 2n)],
  }) as ListKnowledgeObjectDependenciesResponseMessage;
  futureRole.dependencies[0]!.role = KnowledgeDependencyRole.UNRECOGNIZED;
  malformed.push(futureRole);

  const hiddenCountLeak = relationshipResponse(queryValue, {
    edges: [
      relationshipEdge("dependencies", "ko-a", 1n),
      relationshipEdge("dependencies", "ko-b", 1n),
    ],
    nextPageToken: "outgoing-cap-next",
    totalSize: KNOWLEDGE_MANAGER_MAXIMUM_DEPENDENCIES + 1n,
  });
  malformed.push(hiddenCountLeak);

  const loopingCursor = relationshipResponse(
    relationshipQuery("dependencies", "same-cursor"),
    {
      edges: [
        relationshipEdge("dependencies", "ko-a", 1n),
        relationshipEdge("dependencies", "ko-b", 1n),
      ],
      nextPageToken: "same-cursor",
      totalSize: 4n,
    },
  );
  malformed.push(loopingCursor);

  for (const response of malformed) {
    const responseQuery = response === loopingCursor
      ? relationshipQuery("dependencies", "same-cursor")
      : queryValue;
    assert.throws(() => adaptKnowledgeRelationshipPage(response, responseQuery), TypeError);
  }

  assert.throws(() => knowledgeRelationshipRequest({
    ...queryValue,
    pageToken: "x".repeat(KNOWLEDGE_MANAGER_MAXIMUM_PAGE_TOKEN_BYTES + 1),
  }), TypeError);
  const dependentOverflow = relationshipResponse(relationshipQuery("dependents"), {
    edges: [relationshipEdge("dependents", "ko-source", 1n)],
    nextPageToken: "incoming-cap-next",
    totalSize: KNOWLEDGE_MANAGER_MAXIMUM_DEPENDENTS + 1n,
  });
  assert.throws(() => adaptKnowledgeRelationshipPage(
    dependentOverflow,
    relationshipQuery("dependents"),
  ), TypeError);
});

test("relationship continuations retain exact direction, root, revision, total, and edge order", () => {
  const firstQuery = relationshipQuery("dependencies");
  const first = adaptKnowledgeRelationshipPage(relationshipResponse(firstQuery, {
    edges: [
      relationshipEdge("dependencies", "ko-a", 1n),
      relationshipEdge("dependencies", "ko-b", 2n),
    ],
    nextPageToken: "relationship-next",
    totalSize: 3n,
  }), firstQuery);
  const nextQuery = relationshipQuery("dependencies", "relationship-next");
  const continuation = adaptKnowledgeRelationshipPage(relationshipResponse(nextQuery, {
    edges: [relationshipEdge("dependencies", "ko-c", 3n)],
    totalSize: 3n,
  }), nextQuery);
  const merged = mergeKnowledgeRelationshipContinuation(
    first,
    continuation,
    "relationship-next",
    new Set(),
  );
  assert.equal(merged.status, "merged");
  if (merged.status === "merged") {
    assert.deepEqual(merged.page.edges.map((edge) => edge.knowledgeObjectId), [
      "ko-a",
      "ko-b",
      "ko-c",
    ]);
    assert.equal(merged.page.nextPageToken, null);
  }

  const staleCases: KnowledgeRelationshipPageDisplay[] = [
    { ...continuation, tenantCatalogRevision: continuation.tenantCatalogRevision + 1n },
    { ...continuation, totalSize: continuation.totalSize + 1n },
    { ...continuation, direction: "dependents" },
    { ...continuation, resolvedObject: { ...continuation.resolvedObject, version: 2n } },
    { ...continuation, edges: [first.edges[1]!] },
  ];
  for (const stale of staleCases) {
    assert.deepEqual(
      mergeKnowledgeRelationshipContinuation(
        first,
        stale,
        "relationship-next",
        new Set(),
      ),
      { status: "stale" },
    );
  }
});

test("relationship failures are uniform and late-bound cleanup aborts continuation work", async () => {
  const failures: unknown[] = [
    new HttpError({ status: 404, message: "secret hidden root", url: "/knowledge" }),
    new HttpError({ status: 409, message: "secret stale cursor", url: "/knowledge" }),
    new HttpError({ status: 503, message: "secret dependency failure", url: "/knowledge" }),
    new Error("secret decoder failure"),
  ];
  await Promise.all(failures.map(async (failure) => {
    const client: KnowledgeReadClient = {
      async get() { throw new Error("get must not be called"); },
      async list() { throw new Error("list must not be called"); },
      async dependencies() { throw failure; },
      async dependents() { throw failure; },
    };
    assert.deepEqual(
      await loadKnowledgeRelationshipPage(client, relationshipQuery("dependencies")),
      { status: "unavailable" },
    );
  }));

  const request = { current: null as AbortController | null };
  const cleanup = knowledgeRelationshipUnmountCleanup(request);
  const continuation = new AbortController();
  request.current = continuation;
  cleanup();
  assert.equal(continuation.signal.aborted, true);
  assert.equal(request.current, null);

  assert.notEqual(
    knowledgeRelationshipSectionKey("dependencies", "ko-root", 3n),
    knowledgeRelationshipSectionKey("dependencies", "ko-root", 4n),
  );
  assert.notEqual(
    knowledgeRelationshipSectionKey("dependencies", "ko-root", 3n),
    knowledgeRelationshipSectionKey("dependents", "ko-root", 3n),
  );
});

test("filter and sort choices encode exact canonical List request enums", () => {
  assert.deepEqual(knowledgeListRequest(query("cursor-safe", {
    appId: null,
    ownerId: "owner-7",
    text: "latency error",
    objectType: "field-alias",
    lifecycleState: "quarantined",
    sharingScope: "private",
    selectorText: "source::api",
    sort: "updated-descending",
    pageSize: 17,
  })), {
    page: { pageSize: 17, pageToken: "cursor-safe", includeTotalSize: true },
    appIdFilter: undefined,
    ownerIdFilter: "owner-7",
    textFilter: "latency error",
    objectTypeFilters: [KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS],
    stateFilters: [KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_QUARANTINED],
    sharingScopeFilters: [SharingScope.SHARING_SCOPE_PRIVATE],
    selectorTextFilter: "source::api",
    sortBy: KnowledgeObjectSortBy.KNOWLEDGE_OBJECT_SORT_BY_UPDATED_AT,
    sortDirection: SortDirection.SORT_DIRECTION_DESCENDING,
  });

  const objectTypes = [
    ["field-extraction", KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION],
    ["field-alias", KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS],
    ["calculated-field", KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD],
  ] as const;
  for (const [objectType, expected] of objectTypes) {
    assert.deepEqual(
      knowledgeListRequest(query(null, { objectType })).objectTypeFilters,
      [expected],
    );
  }

  const lifecycleStates = [
    ["draft", KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DRAFT],
    ["active", KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE],
    ["disabled", KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DISABLED],
    ["quarantined", KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_QUARANTINED],
    ["deleted", KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DELETED],
  ] as const;
  for (const [lifecycleState, expected] of lifecycleStates) {
    assert.deepEqual(
      knowledgeListRequest(query(null, { lifecycleState })).stateFilters,
      [expected],
    );
  }

  const sharingScopes = [
    ["private", SharingScope.SHARING_SCOPE_PRIVATE],
    ["app", SharingScope.SHARING_SCOPE_APP],
    ["global", SharingScope.SHARING_SCOPE_GLOBAL],
  ] as const;
  for (const [sharingScope, expected] of sharingScopes) {
    assert.deepEqual(
      knowledgeListRequest(query(null, { sharingScope })).sharingScopeFilters,
      [expected],
    );
  }

  const sorts = [
    [
      "name-ascending",
      KnowledgeObjectSortBy.KNOWLEDGE_OBJECT_SORT_BY_NAME,
      SortDirection.SORT_DIRECTION_ASCENDING,
    ],
    [
      "updated-descending",
      KnowledgeObjectSortBy.KNOWLEDGE_OBJECT_SORT_BY_UPDATED_AT,
      SortDirection.SORT_DIRECTION_DESCENDING,
    ],
    [
      "created-descending",
      KnowledgeObjectSortBy.KNOWLEDGE_OBJECT_SORT_BY_CREATED_AT,
      SortDirection.SORT_DIRECTION_DESCENDING,
    ],
    [
      "object-type-ascending",
      KnowledgeObjectSortBy.KNOWLEDGE_OBJECT_SORT_BY_OBJECT_TYPE,
      SortDirection.SORT_DIRECTION_ASCENDING,
    ],
  ] as const;
  for (const [sort, sortBy, sortDirection] of sorts) {
    const request = knowledgeListRequest(query(null, { sort }));
    assert.equal(request.sortBy, sortBy);
    assert.equal(request.sortDirection, sortDirection);
  }
});

test("advanced text filters trim ASCII edges and enforce the committed UTF-8 contract", () => {
  assert.equal(KNOWLEDGE_MANAGER_MAXIMUM_FILTER_BYTES, 255);
  assert.equal(knowledgeTextFilterFromDraft(" \towner-7\r\n"), "owner-7");
  assert.equal(knowledgeTextFilterFromDraft("\u00a0owner-7\u00a0"), "\u00a0owner-7\u00a0");
  assert.equal(knowledgeTextFilterFromDraft("\t\n\v\f\r "), null);
  assert.equal(knowledgeTextFilterFromDraft(` ${"é".repeat(127)}a `), `${"é".repeat(127)}a`);
  assert.equal(knowledgeTextFilterFromDraft("é".repeat(128)), undefined);
  assert.equal(knowledgeTextFilterFromDraft("owner\u0000secret"), undefined);
  assert.equal(knowledgeTextFilterFromDraft("owner\u0085secret"), undefined);
  assert.equal(knowledgeTextFilterFromDraft("owner\ud800secret"), undefined);

  assert.throws(() => knowledgeListRequest(query(null, { ownerId: " owner-7" })), TypeError);
  assert.throws(() => knowledgeListRequest(query(null, { text: "" })), TypeError);
  assert.throws(() => knowledgeListRequest(query(null, { selectorText: "bad\u007ftext" })), TypeError);
});

test("advanced draft normalization reports each invalid field and one atomic tuple", () => {
  assert.deepEqual(normalizeAdvancedFilterDrafts({
    ownerId: " owner-7 ",
    text: " latency error ",
    sharingScope: "private",
    selectorText: " source::api ",
  }), {
    filters: {
      ownerId: "owner-7",
      text: "latency error",
      sharingScope: "private",
      selectorText: "source::api",
    },
    invalid: {
      ownerId: false,
      text: false,
      sharingScope: false,
      selectorText: false,
    },
  });
  assert.deepEqual(normalizeAdvancedFilterDrafts({
    ownerId: "owner\u0000secret",
    text: "ok",
    sharingScope: "future-sharing",
    selectorText: "é".repeat(128),
  }), {
    filters: null,
    invalid: {
      ownerId: true,
      text: false,
      sharingScope: true,
      selectorText: true,
    },
  });
});

test("unknown control and query enums fail closed before a List request", async () => {
  assert.equal(knowledgeObjectTypeFilterFromControlValue("field-alias"), "field-alias");
  assert.equal(knowledgeObjectTypeFilterFromControlValue("future-type"), undefined);
  assert.equal(knowledgeLifecycleStateFilterFromControlValue("deleted"), "deleted");
  assert.equal(knowledgeLifecycleStateFilterFromControlValue("UNRECOGNIZED"), undefined);
  assert.equal(knowledgeSharingScopeFilterFromControlValue("global"), "global");
  assert.equal(knowledgeSharingScopeFilterFromControlValue("shared"), undefined);
  assert.equal(knowledgeSortChoiceFromControlValue("created-descending"), "created-descending");
  assert.equal(knowledgeSortChoiceFromControlValue("name-sideways"), undefined);

  const invalidQueries: KnowledgeListQuery[] = [
    { ...query(), objectType: "future-object-type" as never },
    { ...query(), lifecycleState: "future-state" as never },
    { ...query(), sharingScope: "future-sharing" as never },
    { ...query(), sort: "future-sort" as never },
  ];
  await Promise.all(invalidQueries.map(async (invalidQuery) => {
    assert.throws(() => knowledgeListRequest(invalidQuery), TypeError);
    let listCalls = 0;
    const client: KnowledgeReadClient = {
      ...unavailableGraphReads,
      async list() {
        listCalls += 1;
        return listResponse();
      },
      async get() {
        throw new Error("get must not be called");
      },
    };
    assert.deepEqual(await loadKnowledgePage(client, invalidQuery), { status: "unavailable" });
    assert.equal(listCalls, 0);
  }));
});

test("an empty revision-zero catalog is a valid bounded first page", () => {
  const adapted = adaptKnowledgePage(listResponse({
    objects: [],
    totalSize: 0n,
    revision: 0n,
  }), query());
  assert.deepEqual(adapted, {
    objects: [],
    nextPageToken: null,
    totalSize: 0n,
    tenantCatalogRevision: 0n,
  });
});

test("adapted list/detail models detach caller data and omit authored body text", () => {
  const source = calculatedObjectWithSecret();
  const adapted = adaptKnowledgePage(listResponse({ objects: [source], revision: 9n }), query());
  const object = adapted.objects[0];
  assert.equal(object?.disclosure, "available");
  if (object?.disclosure !== "available") return;
  assert.equal(object.definition.bodyKind, "Calculated field");
  assert.deepEqual(object.definition.bodyFields, [{
    label: "Destination field",
    value: "latency_class",
  }]);
  assert.equal(jsonWithoutBigInt(adapted).includes("TOP_SECRET_AUTHORED_EXPRESSION"), false);

  source.name = "mutated-name";
  source.updatedAt!.setUTCFullYear(1999);
  source.definition!.description = "mutated-description";
  source.definition!.selector!.indexPatterns[0]!.value = "mutated-index";
  source.definition!.body = undefined;

  assert.equal(object.name, "latency_bucket");
  assert.equal(object.updatedAt.toISOString(), "2026-08-02T10:00:00.000Z");
  assert.equal(object.definition.description, "Safe administrator description");
  assert.deepEqual(object.definition.selectors[0]?.patterns, ["main"]);
});

test("unknown enums and corrupt active bodies become non-interactive redacted rows", () => {
  const unknown = calculatedObjectWithSecret();
  unknown.objectType = KnowledgeObjectType.UNRECOGNIZED;
  unknown.name = "SECRET_UNKNOWN_NAME";
  unknown.definition!.description = "SECRET_UNKNOWN_DESCRIPTION";
  const missingBody = fieldAliasObject({ id: "ko-missing", name: "SECRET_MISSING_BODY" });
  missingBody.definition!.body = undefined;

  const adapted = adaptKnowledgePage(
    listResponse({ objects: [unknown, missingBody], totalSize: 2n, revision: 9n }),
    query(),
  );
  assert.deepEqual(adapted.objects.map((object) => object.disclosure), ["redacted", "redacted"]);
  const rendered = jsonWithoutBigInt(adapted);
  assert.equal(rendered.includes("SECRET_UNKNOWN"), false);
  assert.equal(rendered.includes("TOP_SECRET"), false);
  assert.equal(rendered.includes("SECRET_MISSING_BODY"), false);
});

test("an inactive future body exposes only safe scalar metadata and no definition metadata", () => {
  const opaque = fieldAliasObject({
    id: "ko-future",
    name: "future_definition",
    state: KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DISABLED,
  });
  opaque.definition!.description = "SECRET_FUTURE_DESCRIPTION";
  opaque.definition!.selector!.indexPatterns[0]!.value = "SECRET_FUTURE_SELECTOR";
  opaque.definition!.body = undefined;
  const adapted = adaptKnowledgeObject(opaque, 0);
  assert.equal(adapted.disclosure, "available");
  if (adapted.disclosure !== "available") return;
  assert.equal(adapted.name, "future_definition");
  assert.equal(adapted.objectTypeLabel, "Field alias");
  assert.equal(adapted.definition.status, "opaque");
  assert.equal(adapted.definition.description, null);
  assert.deepEqual(adapted.definition.selectors, []);
  assert.equal(jsonWithoutBigInt(adapted).includes("SECRET_FUTURE"), false);
});

test("404, route absence, decoder failure, and server failure are uniformly unavailable", async () => {
  const failures: unknown[] = [
    new HttpError({ status: 404, message: "secret not found detail", url: "/knowledge" }),
    new HttpError({ status: 405, message: "secret method detail", url: "/knowledge" }),
    new HttpError({ status: 501, message: "secret route detail", url: "/knowledge" }),
    new HttpError({ status: 503, message: "secret dependency detail", url: "/knowledge" }),
    new Error("secret decoder detail"),
  ];
  await Promise.all(failures.map(async (failure) => {
    const client: KnowledgeReadClient = {
      ...unavailableGraphReads,
      async list() { throw failure; },
      async get() { throw failure; },
    };
    assert.deepEqual(await loadKnowledgePage(client, query()), { status: "unavailable" });
    assert.deepEqual(await loadKnowledgeDetail(client, {
      knowledgeObjectId: "ko-safe",
      version: 1n,
    }), { status: "unavailable" });
  }));
});

test("short byte-budget pages merge while stale revisions, identities, and tokens are rejected", () => {
  const first = adaptKnowledgePage(listResponse({
    objects: [fieldAliasObject({ id: "ko-a", name: "a", version: 1n })],
    nextPageToken: "cursor-a",
    totalSize: 2n,
    revision: 5n,
  }), query());
  const second = adaptKnowledgePage(listResponse({
    objects: [fieldAliasObject({ id: "ko-b", name: "b", version: 2n })],
    totalSize: 2n,
    revision: 5n,
  }), query("cursor-a"));
  const merged = mergeKnowledgeContinuation(first, second, "cursor-a", new Set());
  assert.equal(merged.status, "merged");
  if (merged.status === "merged") assert.equal(merged.page.objects.length, 2);

  const staleRevision = { ...second, tenantCatalogRevision: 6n };
  assert.deepEqual(
    mergeKnowledgeContinuation(first, staleRevision, "cursor-a", new Set()),
    { status: "stale" },
  );
  const duplicate = { ...second, objects: [first.objects[0]!] };
  assert.deepEqual(
    mergeKnowledgeContinuation(first, duplicate, "cursor-a", new Set()),
    { status: "stale" },
  );
  assert.deepEqual(
    mergeKnowledgeContinuation(first, second, "cursor-a", new Set(["cursor-a"])),
    { status: "stale" },
  );

  const earlyEndFirst = adaptKnowledgePage(listResponse({
    objects: [fieldAliasObject({ id: "ko-early-a", name: "early-a", version: 1n })],
    nextPageToken: "cursor-early",
    totalSize: 3n,
    revision: 5n,
  }), query());
  const earlyEnd = adaptKnowledgePage(listResponse({
    objects: [fieldAliasObject({ id: "ko-early-b", name: "early-b", version: 2n })],
    totalSize: 3n,
    revision: 5n,
  }), query("cursor-early"));
  assert.deepEqual(
    mergeKnowledgeContinuation(earlyEndFirst, earlyEnd, "cursor-early", new Set()),
    { status: "stale" },
  );

  const exactWithTokenFirst = adaptKnowledgePage(listResponse({
    objects: [
      fieldAliasObject({ id: "ko-token-a", name: "token-a", version: 1n }),
      fieldAliasObject({ id: "ko-token-b", name: "token-b", version: 2n }),
    ],
    nextPageToken: "cursor-token",
    totalSize: 3n,
    revision: 5n,
  }), query());
  const exactWithToken = adaptKnowledgePage(listResponse({
    objects: [fieldAliasObject({ id: "ko-token-c", name: "token-c", version: 3n })],
    nextPageToken: "cursor-impossible",
    totalSize: 3n,
    revision: 5n,
  }), query("cursor-token"));
  assert.deepEqual(
    mergeKnowledgeContinuation(exactWithTokenFirst, exactWithToken, "cursor-token", new Set()),
    { status: "stale" },
  );

  const shortContinuationFirst = adaptKnowledgePage(listResponse({
    objects: [fieldAliasObject({ id: "ko-short-a", name: "short-a", version: 1n })],
    nextPageToken: "cursor-short",
    totalSize: 4n,
    revision: 5n,
  }), query());
  const shortContinuation = adaptKnowledgePage(listResponse({
    objects: [fieldAliasObject({ id: "ko-short-b", name: "short-b", version: 2n })],
    nextPageToken: "cursor-short-next",
    totalSize: 4n,
    revision: 5n,
  }), query("cursor-short"));
  assert.equal(
    mergeKnowledgeContinuation(
      shortContinuationFirst,
      shortContinuation,
      "cursor-short",
      new Set(),
    ).status,
    "merged",
  );
});

test("page, cursor, total, revision, and nested definition bounds fail closed", () => {
  assert.throws(() => knowledgeListRequest(query(null, {
    appId: null,
    pageSize: 257,
  })), RangeError);
  assert.throws(() => knowledgeListRequest(query(
    "x".repeat(KNOWLEDGE_MANAGER_MAXIMUM_PAGE_TOKEN_BYTES + 1),
    { appId: null },
  )), TypeError);
  assert.throws(() => adaptKnowledgePage(listResponse({
    totalSize: KNOWLEDGE_MANAGER_MAXIMUM_OBJECTS + 1n,
  }), query()), TypeError);

  const futureRevision = listResponse({ revision: 9_223_372_036_854_775_808n });
  assert.throws(() => adaptKnowledgePage(futureRevision, query()), TypeError);
  const tooManySelectors = fieldAliasObject();
  tooManySelectors.definition!.selector!.hostPatterns = Array.from({ length: 17 }, (_, index) => ({
    matchKind: KnowledgeSelectorMatchKind.KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT,
    value: `host-${index}`,
  }));
  assert.throws(() => adaptKnowledgePage(
    listResponse({ objects: [tooManySelectors] }),
    query(),
  ), TypeError);
});

test("detail markup is read-only, body-safe, keyboard-addressable, and labelled", () => {
  const adapted = adaptKnowledgeObject(calculatedObjectWithSecret(), 0);
  assert.equal(adapted.disclosure, "available");
  if (adapted.disclosure !== "available") return;
  const markup = renderToStaticMarkup(createElement(KnowledgeDetail, { object: adapted }));
  assert.match(markup, /id="knowledge-object-detail-title"/);
  assert.match(markup, /id="knowledge-definition-title"/);
  assert.match(markup, /Definition summary/);
  assert.match(markup, /intentionally omitted/);
  assert.doesNotMatch(markup, /TOP_SECRET_AUTHORED_EXPRESSION/);
  assert.doesNotMatch(markup, /Create|Edit|Delete|Enable|Disable|Save/);
});

test("relationship presentation is independently labelled, escaped, and read-only", () => {
  const queryValue = relationshipQuery("dependencies");
  const page = adaptKnowledgeRelationshipPage(relationshipResponse(queryValue, {
    edges: [relationshipEdge("dependencies", "ko-<script>", 2n)],
  }), queryValue);
  const markup = renderToStaticMarkup(createElement(KnowledgeRelationshipSectionView, {
    direction: "dependencies",
    state: "available",
    page,
    loadingMore: false,
    onRetry: () => undefined,
    onLoadMore: () => undefined,
  }));
  assert.match(markup, /id="knowledge-dependencies-title"/);
  assert.match(markup, /aria-label="Visible direct dependencies"/);
  assert.match(markup, /ko-&lt;script&gt;/);
  assert.match(markup, /Field input/);
  assert.match(markup, /revision 9/);
  assert.doesNotMatch(markup, /<script>|href=|>Create<|>Edit<|>Delete<|>Enable<|>Disable<|>Save</);

  const staleMarkup = renderToStaticMarkup(createElement(KnowledgeRelationshipSectionView, {
    direction: "dependents",
    state: "stale",
    page: { ...page, direction: "dependents" },
    loadingMore: false,
    onRetry: () => undefined,
    onLoadMore: () => undefined,
  }));
  assert.match(staleMarkup, /role="alert"/);
  assert.match(staleMarkup, /Reload dependents/);
  assert.doesNotMatch(staleMarkup, /Load more dependents/);
});

test("the panel loading shell labels every closed filter and exposes no mutation control", () => {
  const markup = renderToStaticMarkup(createElement(KnowledgeManagerPanel, {
    apiBaseUrl: "",
    apps: [{ appId: "app-observability", label: "Observability" }],
    initialAppId: "app-observability",
    maximumPageSize: 50,
  }));
  assert.match(markup, /id="knowledge-manager-title"/);
  assert.match(markup, /Read only/);
  assert.match(markup, /Loading knowledge objects/);
  assert.match(markup, /<label for="knowledge-app-filter"><span>App scope<\/span>/);
  assert.match(markup, /<select id="knowledge-app-filter"/);
  assert.match(markup, /<label for="knowledge-object-type-filter"><span>Object type<\/span>/);
  assert.match(markup, /<select id="knowledge-object-type-filter"/);
  assert.match(markup, /All object types/);
  assert.match(markup, /Field extraction/);
  assert.match(markup, /Field alias/);
  assert.match(markup, /Calculated field/);
  assert.match(markup, /<label for="knowledge-lifecycle-state-filter"><span>Lifecycle state<\/span>/);
  assert.match(markup, /<select id="knowledge-lifecycle-state-filter"/);
  assert.match(markup, /All lifecycle states/);
  assert.match(markup, /Draft/);
  assert.match(markup, /Active/);
  assert.match(markup, /Disabled/);
  assert.match(markup, /Quarantined/);
  assert.match(markup, /Deleted/);
  assert.match(markup, /<label for="knowledge-sort-choice"><span>Sort by<\/span>/);
  assert.match(markup, /<select id="knowledge-sort-choice"/);
  assert.match(markup, /Name A–Z/);
  assert.match(markup, /Updated newest/);
  assert.match(markup, /Created newest/);
  assert.match(markup, /Type A–Z/);
  assert.match(markup, /<legend id="knowledge-advanced-filters-title">Advanced filters<\/legend>/);
  assert.match(markup, /<label for="knowledge-owner-filter"><span>Owner ID<\/span>/);
  assert.match(markup, /<input id="knowledge-owner-filter"[^>]*autoComplete="off"/);
  assert.match(markup, /<label for="knowledge-text-filter"><span>Name or description<\/span>/);
  assert.match(markup, /<input id="knowledge-text-filter"[^>]*autoComplete="off"/);
  assert.match(markup, /<label for="knowledge-sharing-scope-filter"><span>Sharing scope<\/span>/);
  assert.match(markup, /All sharing scopes/);
  assert.match(markup, /Private/);
  assert.match(markup, /Global/);
  assert.match(markup, /<label for="knowledge-selector-text-filter"><span>Selector text<\/span>/);
  assert.match(markup, /No advanced filters applied\./);
  assert.match(markup, />Apply filters<\/button>/);
  assert.match(markup, />Clear filters<\/button>/);
  assert.doesNotMatch(markup, /name="(?:ownerId|text|sharingScope|selectorText)"/);
  assert.doesNotMatch(markup, />Create<|>Edit<|>Delete<|>Enable<|>Disable<|>Save</);
});

test("the available list and detail presentation expose no mutation control", () => {
  const page = adaptKnowledgePage(listResponse(), query());
  const detail = page.objects[0];
  assert.equal(detail?.disclosure, "available");
  if (detail?.disclosure !== "available") return;
  const markup = renderToStaticMarkup(createElement(KnowledgeManagerWorkspace, {
    page,
    selectedObjectId: detail.knowledgeObjectId,
    loadingMore: false,
    continuationStale: false,
    detailState: "available",
    detail,
    onOpen: () => undefined,
    onRowKeyDown: () => undefined,
    registerRow: () => undefined,
    onReloadFirstPage: () => undefined,
    onLoadMore: () => undefined,
    onCloseDetail: () => undefined,
  }));
  assert.match(markup, /aria-label="Knowledge objects"/);
  assert.match(markup, /id="knowledge-object-detail-title"/);
  assert.match(markup, /aria-label="Close knowledge object details"/);
  assert.doesNotMatch(markup, />Create<|>Edit<|>Delete<|>Enable<|>Disable<|>Save</);
});

test("responsive and focus-visible styles cover filters and list/detail collapse", () => {
  const css = readFileSync(path.join(process.cwd(), "app", "globals.css"), "utf8");
  const compactBodies = mediaRuleBodies(css, "max-width: 980px");
  const mobileBodies = mediaRuleBodies(css, "max-width: 760px");
  const narrowBodies = mediaRuleBodies(css, "max-width: 480px");
  assert.match(css, /\.knowledge-manager__filters \{[^}]*grid-template-columns: minmax\(170px, 1\.35fr\) repeat\(3, minmax\(125px, 1fr\)\);/);
  assert.match(css, /\.knowledge-manager__advanced-filter-grid \{[^}]*grid-template-columns: repeat\(4, minmax\(125px, 1fr\)\);/);
  assert.match(css, /\.knowledge-manager__filters label,\n\.knowledge-manager__advanced-filter-grid label \{/);
  assert.match(css, /\.knowledge-manager__filters label > span,\n\.knowledge-manager__advanced-filter-grid label > span \{/);
  assert.match(css, /\.knowledge-manager__filters select,\n\.knowledge-manager__advanced-filter-grid input,\n\.knowledge-manager__advanced-filter-grid select \{/);
  assert.ok(compactBodies.some((body) => (
    /\.knowledge-manager__filters \{ grid-template-columns: repeat\(2, minmax\(0, 1fr\)\); \}/.test(body)
    && /\.knowledge-manager__advanced-filter-grid \{ grid-template-columns: repeat\(2, minmax\(0, 1fr\)\); \}/.test(body)
    && /\.knowledge-manager__workspace--detail \{ grid-template-columns: 1fr; \}/.test(body)
  )));
  assert.ok(mobileBodies.some((body) => (
    /\.knowledge-manager__row \{[^}]*grid-template-columns: minmax\(0, 1fr\) auto;/.test(body)
    && /\.knowledge-manager__toolbar select,\s*\.knowledge-manager__advanced-filter-grid input,\s*\.knowledge-manager__advanced-filter-grid select \{ font-size: 16px; height: 44px; \}/.test(body)
  )));
  assert.ok(narrowBodies.some((body) => (
    /\.knowledge-manager__filters \{ grid-template-columns: 1fr; \}/.test(body)
    && /\.knowledge-manager__advanced-filter-grid \{ grid-template-columns: 1fr; \}/.test(body)
  )));
  assert.match(css, /\.knowledge-manager button:focus-visible/);
  assert.match(css, /\.knowledge-manager input:focus-visible/);
  assert.match(css, /\.knowledge-manager select:focus-visible/);
  assert.match(css, /\.knowledge-manager__detail:focus-visible/);
  assert.match(css, /\.knowledge-manager__relationship-list li \{[^}]*grid-template-columns: minmax\(0, 1fr\) auto;/);
  assert.ok(mobileBodies.some((body) => (
    /\.knowledge-manager__relationship-pagination \{ align-items: stretch; flex-direction: column; \}/.test(body)
    && /\.knowledge-manager__relationship-pagination button \{ min-height: 42px; width: 100%; \}/.test(body)
  )));
});
