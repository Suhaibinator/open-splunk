import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";

import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import { SharingScope, SortDirection } from "@/gen/ts/open_splunk/v1/common";
import {
  KnowledgeObject,
  KnowledgeObjectState,
  KnowledgeObjectType,
  KnowledgeOverwriteBehavior,
  KnowledgeSelectorMatchKind,
} from "@/gen/ts/open_splunk/v1/knowledge";
import {
  KnowledgeObjectSortBy,
  ListKnowledgeObjectsResponse,
  type GetKnowledgeObjectResponse,
  type ListKnowledgeObjectsRequest,
  type ListKnowledgeObjectsResponse as ListKnowledgeObjectsResponseMessage,
} from "@/gen/ts/open_splunk/v1/knowledge_api";
import { HttpError } from "@/lib/api/protobuf-transport";

import {
  KNOWLEDGE_MANAGER_MAXIMUM_OBJECTS,
  KNOWLEDGE_MANAGER_MAXIMUM_PAGE_TOKEN_BYTES,
  adaptKnowledgeObject,
  adaptKnowledgePage,
  knowledgeListRequest,
  loadKnowledgeDetail,
  loadKnowledgePage,
  mergeKnowledgeContinuation,
  type KnowledgeReadClient,
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
  knowledgeManagerUnmountCleanup,
} from "./knowledge-manager-panel";

const pageSize = 50;
const TestKnowledgePanel = () => null;

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

function query(pageToken: string | null = null) {
  return { appId: "app-observability", pageSize, pageToken };
}

function jsonWithoutBigInt(value: unknown): string {
  return JSON.stringify(value, (_key, item: unknown) =>
    typeof item === "bigint" ? item.toString() : item);
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

test("enabled list and detail fixtures use bounded generated protobuf requests", async () => {
  const listRequests: ListKnowledgeObjectsRequest[] = [];
  const getRequests: string[] = [];
  const fixture = fieldAliasObject();
  const client: KnowledgeReadClient = {
    async list(request) {
      listRequests.push(request);
      return listResponse({ objects: [fixture], revision: 8n });
    },
    async get(request): Promise<GetKnowledgeObjectResponse> {
      getRequests.push(request.knowledgeObjectId);
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
  const detail = await loadKnowledgeDetail(client, fixture.knowledgeObjectId);
  assert.equal(detail.status, "available");
  assert.deepEqual(getRequests, [fixture.knowledgeObjectId]);
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
      async list() { throw failure; },
      async get() { throw failure; },
    };
    assert.deepEqual(await loadKnowledgePage(client, query()), { status: "unavailable" });
    assert.deepEqual(await loadKnowledgeDetail(client, "ko-safe"), { status: "unavailable" });
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
  assert.throws(() => knowledgeListRequest({
    appId: null,
    pageSize: 257,
    pageToken: null,
  }), RangeError);
  assert.throws(() => knowledgeListRequest({
    appId: null,
    pageSize: 50,
    pageToken: "x".repeat(KNOWLEDGE_MANAGER_MAXIMUM_PAGE_TOKEN_BYTES + 1),
  }), TypeError);
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

test("the panel loading shell is labelled, read-only, and exposes no mutation control", () => {
  const markup = renderToStaticMarkup(createElement(KnowledgeManagerPanel, {
    apiBaseUrl: "",
    apps: [{ appId: "app-observability", label: "Observability" }],
    initialAppId: "app-observability",
    maximumPageSize: 50,
  }));
  assert.match(markup, /id="knowledge-manager-title"/);
  assert.match(markup, /Read only/);
  assert.match(markup, /Loading knowledge objects/);
  assert.match(markup, /id="knowledge-app-filter"/);
  assert.doesNotMatch(markup, />Create<|>Edit<|>Delete<|>Enable<|>Disable<|>Save</);
});

test("responsive and focus-visible style contracts cover list/detail collapse", () => {
  const css = readFileSync(path.join(process.cwd(), "app", "globals.css"), "utf8");
  assert.match(css, /@media \(max-width: 980px\)[\s\S]*?\.knowledge-manager__workspace--detail \{ grid-template-columns: 1fr; \}/);
  assert.match(css, /@media \(max-width: 760px\)[\s\S]*?\.knowledge-manager__row \{[^}]*grid-template-columns: minmax\(0, 1fr\) auto;/);
  assert.match(css, /\.knowledge-manager button:focus-visible/);
  assert.match(css, /\.knowledge-manager__detail:focus-visible/);
});
