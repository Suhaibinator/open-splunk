import assert from "node:assert/strict";
import test from "node:test";

import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import {
  DiagnosticSeverity,
  SharingScope,
  SortDirection,
} from "@/gen/ts/open_splunk/common";
import {
  KnowledgeDependencyRole,
  KnowledgeObject,
  KnowledgeObjectDefinition,
  KnowledgeObjectState,
  KnowledgeObjectType,
  KnowledgeOverwriteBehavior,
  KnowledgeSelectorMatchKind,
} from "@/gen/ts/open_splunk/knowledge";
import {
  CreateKnowledgeObjectRequest,
  CreateKnowledgeObjectResponse,
  DeleteKnowledgeObjectRequest,
  DeleteKnowledgeObjectResponse,
  KnowledgeQuarantineReason,
  KnowledgeValidationIntent,
  ListKnowledgeObjectDependenciesResponse,
  ListKnowledgeObjectDependentsResponse,
  KnowledgeObjectSortBy,
  ListKnowledgeObjectsResponse,
  PrepareKnowledgeObjectQuarantineRequest,
  PrepareKnowledgeObjectQuarantineResponse,
  QuarantineKnowledgeObjectRequest,
  QuarantineKnowledgeObjectResponse,
  SetKnowledgeObjectStateRequest,
  SetKnowledgeObjectStateResponse,
  UpdateKnowledgeObjectRequest,
  UpdateKnowledgeObjectResponse,
  ValidateKnowledgeObjectRequest,
  ValidateKnowledgeObjectResponse,
  type GetKnowledgeObjectRequest,
  type GetKnowledgeObjectResponse,
  type KnowledgeManagementDependencyEdge,
  type ListKnowledgeObjectDependenciesRequest,
  type ListKnowledgeObjectDependenciesResponse as ListKnowledgeObjectDependenciesResponseMessage,
  type ListKnowledgeObjectDependentsRequest,
  type ListKnowledgeObjectDependentsResponse as ListKnowledgeObjectDependentsResponseMessage,
  type ListKnowledgeObjectsRequest,
  type ListKnowledgeObjectsResponse as ListKnowledgeObjectsResponseMessage,
} from "@/gen/ts/open_splunk/knowledge_api";
import { ServerFeature } from "@/gen/ts/open_splunk/system_api";
import {
  HttpError,
  PROTOBUF_CONTENT_TYPE,
  ProtobufResponseTooLargeError,
} from "@/lib/api/protobuf-transport";
import { knowledgeRoutes } from "@/lib/api/routes";

import {
  KNOWLEDGE_MANAGER_MAXIMUM_DEPENDENCIES,
  KNOWLEDGE_MANAGER_MAXIMUM_DEPENDENTS,
  KNOWLEDGE_MANAGER_MAXIMUM_FILTER_BYTES,
  KNOWLEDGE_MANAGER_MAXIMUM_GRAPH_RESPONSE_BYTES,
  KNOWLEDGE_MANAGER_MAXIMUM_OBJECTS,
  KNOWLEDGE_MANAGER_MAXIMUM_PAGE_TOKEN_BYTES,
  KNOWLEDGE_MANAGER_MAXIMUM_RESPONSE_BYTES,
  adaptKnowledgeCreateResponse,
  adaptKnowledgePrepareQuarantineResponse,
  adaptKnowledgeQuarantineResponse,
  adaptKnowledgeObject,
  adaptKnowledgePage,
  adaptKnowledgeRelationshipPage,
  adaptKnowledgeSetStateResponse,
  adaptKnowledgeUpdateResponse,
  adaptKnowledgeValidationResponse,
  createKnowledgeMutationClient,
  createKnowledgeObject,
  deleteKnowledgeObject,
  knowledgeDetailRequest,
  knowledgeUpdateRequest,
  knowledgeValidateRequest,
  knowledgeLifecycleStateFilterFromControlValue,
  knowledgeListRequest,
  knowledgeObjectTypeFilterFromControlValue,
  knowledgePrepareQuarantineRequest,
  knowledgeQuarantineRequest,
  knowledgeSharingScopeFilterFromControlValue,
  knowledgeRelationshipRequest,
  knowledgeSortChoiceFromControlValue,
  knowledgeTextFilterFromDraft,
  loadKnowledgeDetail,
  loadKnowledgeMutationDetail,
  loadKnowledgePage,
  loadKnowledgeRelationshipPage,
  mergeKnowledgeContinuation,
  mergeKnowledgeRelationshipContinuation,
  prepareKnowledgeObjectQuarantine,
  quarantineKnowledgeObject,
  setKnowledgeObjectState,
  updateKnowledgeObject,
  validateKnowledgeObject,
  type KnowledgeLifecycleStateFilter,
  type KnowledgeDetailQuery,
  type KnowledgeListQuery,
  type KnowledgeMutationClient,
  type KnowledgeReadClient,
  type KnowledgeRelationshipPageDisplay,
  type KnowledgeRelationshipQuery,
} from "./knowledge-manager-data";
import {
  KNOWLEDGE_MANAGER_MAXIMUM_BOOTSTRAP_APPS,
  backendKnowledgeCapabilities,
  backendAdminNavigation,
  knowledgeManagerAppOptionsFromBootstrap,
  safeKnowledgeManagerAppOptions,
} from "./knowledge-manager-feature";
import {
  KnowledgeDetail,
  KnowledgeManagerPanel,
  KnowledgeRelatedObjectInspectorView,
  KnowledgeRelationshipSectionView,
  KnowledgeManagerWorkspace,
  commitKnowledgeManagerQueryChange,
  knowledgeManagerUnmountCleanup,
  knowledgeRelationshipSectionKey,
  knowledgeRelationshipUnmountCleanup,
  normalizeAdvancedFilterDrafts,
  resetKnowledgeManagerQuery,
} from "./knowledge-manager-panel";
import {
  KnowledgeMutationEditor,
  KnowledgeObjectMutationControls,
  KnowledgeQuarantineControl,
  createKnowledgeMutationDraft,
  knowledgeBrowserClientRequestId,
  knowledgeDefinitionFromMutationDraft,
  knowledgeDefinitionUpdateMask,
  knowledgeMutationDraftFromObject,
  type KnowledgeMutationDraft,
} from "./knowledge-manager-mutations";

const pageSize = 50;

const unavailableGraphReads = {
  async dependencies(): Promise<ListKnowledgeObjectDependenciesResponseMessage> {
    throw new Error("dependencies must not be called");
  },
  async dependents(): Promise<ListKnowledgeObjectDependentsResponseMessage> {
    throw new Error("dependents must not be called");
  },
};

const unavailableMutations: KnowledgeMutationClient = {
  async create() { throw new Error("create must not be called"); },
  async validate() { throw new Error("validate must not be called"); },
  async update() { throw new Error("update must not be called"); },
  async setState() { throw new Error("setState must not be called"); },
  async delete() { throw new Error("delete must not be called"); },
  async prepareQuarantine() { throw new Error("prepareQuarantine must not be called"); },
  async quarantine() { throw new Error("quarantine must not be called"); },
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

function mutationFixtures() {
  const definition = KnowledgeObjectDefinition.fromPartial(
    fieldAliasObject().definition!,
  );
  const updatedDefinition = KnowledgeObjectDefinition.fromPartial({
    ...definition,
    description: "Updated administrator description",
  });
  return {
    definition,
    updatedDefinition,
    create: CreateKnowledgeObjectRequest.fromPartial({
      definition,
      initialState: KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DRAFT,
      clientRequestId: "browser-create-request-0001",
    }),
    validate: ValidateKnowledgeObjectRequest.fromPartial({
      definition,
      intent: KnowledgeValidationIntent.KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
    }),
    update: UpdateKnowledgeObjectRequest.fromPartial({
      knowledgeObjectId: "ko-alias-1",
      expectedVersion: 2n,
      definition: updatedDefinition,
      updateMask: ["description"],
      clientRequestId: "browser-update-request-0001",
    }),
    setState: SetKnowledgeObjectStateRequest.fromPartial({
      knowledgeObjectId: "ko-alias-1",
      expectedVersion: 3n,
      state: KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DISABLED,
      clientRequestId: "browser-state-request-00001",
    }),
    delete: DeleteKnowledgeObjectRequest.fromPartial({
      knowledgeObjectId: "ko-alias-1",
      expectedVersion: 4n,
      clientRequestId: "browser-delete-request-0001",
    }),
  };
}

function mutationObject(options: {
  version: bigint;
  state: KnowledgeObjectState;
  definition?: ReturnType<typeof KnowledgeObjectDefinition.fromPartial>;
  id?: string;
}): KnowledgeObject {
  const object = fieldAliasObject({
    id: options.id,
    version: options.version,
    state: options.state,
  });
  if (options.definition !== undefined) {
    object.definition = KnowledgeObjectDefinition.fromPartial(options.definition);
    object.appId = object.definition.appId;
    object.name = object.definition.name;
    object.sharingScope = object.definition.sharingScope;
    object.objectType = object.definition.body?.$case === "fieldExtraction"
      ? KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION
      : object.definition.body?.$case === "calculatedField"
        ? KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD
        : KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS;
  }
  if (options.version === 1n && object.updatedAt !== undefined) {
    object.createdAt = new Date(object.updatedAt);
  }
  return object;
}

async function definitionDigest(
  definition: ReturnType<typeof KnowledgeObjectDefinition.fromPartial>,
): Promise<Uint8Array> {
  const bytes = KnowledgeObjectDefinition.encode(definition).finish();
  return new Uint8Array(await globalThis.crypto.subtle.digest(
    "SHA-256",
    Uint8Array.from(bytes).buffer,
  ));
}

async function sealedMutationObject(options: Parameters<typeof mutationObject>[0]): Promise<KnowledgeObject> {
  const object = mutationObject(options);
  assert.ok(object.definition);
  object.definitionSha256 = await definitionDigest(object.definition);
  return object;
}

async function mutationCurrentAuthorities(fixtures: ReturnType<typeof mutationFixtures>) {
  return {
    update: await sealedMutationObject({
      version: fixtures.update.expectedVersion,
      state: KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE,
      definition: fixtures.definition,
    }),
    setState: await sealedMutationObject({
      version: fixtures.setState.expectedVersion,
      state: KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE,
      definition: fixtures.updatedDefinition,
    }),
  };
}

function invalidCalculatedValidationResponse(
  start: { byteOffset: bigint; line: number; column: number },
  end: { byteOffset: bigint; line: number; column: number },
) {
  return ValidateKnowledgeObjectResponse.fromPartial({
    result: {
      valid: false,
      objectType: KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD,
      diagnostics: [{
        fieldPath: "calculated_field.expression",
        diagnostic: {
          severity: DiagnosticSeverity.DIAGNOSTIC_SEVERITY_ERROR,
          code: "SPL_INVALID_EXPRESSION",
          message: "Invalid calculated expression",
          sourceRange: { start, end },
        },
      }],
    },
    tenantCatalogRevision: 0n,
  });
}

test("knowledge mutation client sends the five exact generated-protobuf authorities", async () => {
  const fixtures = mutationFixtures();
  const current = await mutationCurrentAuthorities(fixtures);
  const paths: string[] = [];
  let fetches = 0;
  const token = new Uint8Array(32).fill(5);
  const fetchImplementation: typeof fetch = async (input, init) => {
    fetches += 1;
    const url = String(input);
    const routePath = new URL(url, "https://example.test").pathname;
    paths.push(routePath);
    const bytes = new Uint8Array(await new Response(init?.body).arrayBuffer());
    let responseBytes: Uint8Array;
    switch (routePath) {
      case "/api/knowledge/objects/create":
        assert.deepEqual(CreateKnowledgeObjectRequest.decode(bytes), fixtures.create);
        responseBytes = CreateKnowledgeObjectResponse.encode(
          CreateKnowledgeObjectResponse.fromPartial({
            knowledgeObject: await sealedMutationObject({
              version: 1n,
              state: KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DRAFT,
              definition: fixtures.definition,
            }),
            tenantCatalogRevision: 10n,
            tenantCatalogStateToken: token,
          }),
        ).finish();
        break;
      case "/api/knowledge/objects/validate":
        assert.deepEqual(ValidateKnowledgeObjectRequest.decode(bytes), fixtures.validate);
        responseBytes = ValidateKnowledgeObjectResponse.encode(
          ValidateKnowledgeObjectResponse.fromPartial({
            result: {
              valid: true,
              objectType: KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
              normalizedDefinition: fixtures.definition,
              definitionSha256: await definitionDigest(fixtures.definition),
              resources: {
                selectorPatterns: 1,
                normalizedDefinitionBytes: BigInt(
                  KnowledgeObjectDefinition.encode(fixtures.definition).finish().byteLength,
                ),
              },
            },
            tenantCatalogRevision: 10n,
          }),
        ).finish();
        break;
      case "/api/knowledge/objects/update":
        assert.deepEqual(UpdateKnowledgeObjectRequest.decode(bytes), fixtures.update);
        responseBytes = UpdateKnowledgeObjectResponse.encode(
          UpdateKnowledgeObjectResponse.fromPartial({
            knowledgeObject: await sealedMutationObject({
              version: 3n,
              state: KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE,
              definition: fixtures.updatedDefinition,
            }),
            tenantCatalogRevision: 11n,
            tenantCatalogStateToken: token,
          }),
        ).finish();
        break;
      case "/api/knowledge/objects/set-state":
        assert.deepEqual(SetKnowledgeObjectStateRequest.decode(bytes), fixtures.setState);
        responseBytes = SetKnowledgeObjectStateResponse.encode(
          SetKnowledgeObjectStateResponse.fromPartial({
            knowledgeObject: await sealedMutationObject({
              version: 4n,
              state: KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DISABLED,
              definition: fixtures.updatedDefinition,
            }),
            tenantCatalogRevision: 12n,
            tenantCatalogStateToken: token,
          }),
        ).finish();
        break;
      case "/api/knowledge/objects/delete":
        assert.deepEqual(DeleteKnowledgeObjectRequest.decode(bytes), fixtures.delete);
        responseBytes = DeleteKnowledgeObjectResponse.encode(
          DeleteKnowledgeObjectResponse.fromPartial({
            knowledgeObjectId: fixtures.delete.knowledgeObjectId,
            deletedVersion: 5n,
            tenantCatalogRevision: 13n,
            tenantCatalogStateToken: token,
          }),
        ).finish();
        break;
      default:
        throw new Error(`unexpected mutation route ${routePath}`);
    }
    return new Response(Uint8Array.from(responseBytes).buffer, {
      status: 200,
      headers: { "Content-Type": PROTOBUF_CONTENT_TYPE },
    });
  };

  const client = createKnowledgeMutationClient({
    baseUrl: "https://example.test",
    fetch: fetchImplementation,
  });
  assert.equal(fetches, 0, "client construction must be traffic-free");
  const created = await createKnowledgeObject(client, fixtures.create);
  const validated = await validateKnowledgeObject(client, fixtures.validate);
  const updated = await updateKnowledgeObject(client, fixtures.update, {
    currentKnowledgeObject: current.update,
  });
  const stated = await setKnowledgeObjectState(client, fixtures.setState, {
    currentKnowledgeObject: current.setState,
  });
  const deleted = await deleteKnowledgeObject(client, fixtures.delete);

  assert.equal(created.knowledgeObject.version, 1n);
  assert.equal(validated.result.valid, true);
  assert.equal(updated.knowledgeObject.version, 3n);
  assert.equal(stated.knowledgeObject.state, KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DISABLED);
  assert.equal(deleted.deletedVersion, 5n);
  assert.deepEqual(paths, [
    "/api/knowledge/objects/create",
    "/api/knowledge/objects/validate",
    "/api/knowledge/objects/update",
    "/api/knowledge/objects/set-state",
    "/api/knowledge/objects/delete",
  ]);
});

test("knowledge mutation adapters reject request mismatches and malformed success bodies", async () => {
  const fixtures = mutationFixtures();
  const current = await mutationCurrentAuthorities(fixtures);
  const token = new Uint8Array(32).fill(4);
  const responseForPath = async (pathValue: string): Promise<Uint8Array> => {
    switch (pathValue) {
      case "/api/knowledge/objects/create":
        return CreateKnowledgeObjectResponse.encode(
          CreateKnowledgeObjectResponse.fromPartial({
            tenantCatalogRevision: 1n,
            tenantCatalogStateToken: token,
          }),
        ).finish();
      case "/api/knowledge/objects/validate":
        return ValidateKnowledgeObjectResponse.encode(
          ValidateKnowledgeObjectResponse.fromPartial({ tenantCatalogRevision: 1n }),
        ).finish();
      case "/api/knowledge/objects/update":
        return UpdateKnowledgeObjectResponse.encode(
          UpdateKnowledgeObjectResponse.fromPartial({
            knowledgeObject: await sealedMutationObject({
              id: "wrong-object",
              version: 3n,
              state: KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE,
              definition: fixtures.updatedDefinition,
            }),
            tenantCatalogRevision: 3n,
            tenantCatalogStateToken: token,
          }),
        ).finish();
      case "/api/knowledge/objects/set-state":
        return SetKnowledgeObjectStateResponse.encode(
          SetKnowledgeObjectStateResponse.fromPartial({
            knowledgeObject: await sealedMutationObject({
              version: 4n,
              state: KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE,
            }),
            tenantCatalogRevision: 4n,
            tenantCatalogStateToken: token,
          }),
        ).finish();
      case "/api/knowledge/objects/delete":
        return DeleteKnowledgeObjectResponse.encode(
          DeleteKnowledgeObjectResponse.fromPartial({
            knowledgeObjectId: fixtures.delete.knowledgeObjectId,
            deletedVersion: 4n,
            tenantCatalogRevision: 4n,
            tenantCatalogStateToken: token,
          }),
        ).finish();
      default:
        throw new Error(`unexpected route ${pathValue}`);
    }
  };
  const client = createKnowledgeMutationClient({
    baseUrl: "https://example.test",
    fetch: async (input) => {
      const pathValue = new URL(String(input)).pathname;
      return new Response(Uint8Array.from(await responseForPath(pathValue)).buffer, {
        status: 200,
        headers: { "Content-Type": PROTOBUF_CONTENT_TYPE },
      });
    },
  });

  await assert.rejects(createKnowledgeObject(client, fixtures.create), TypeError);
  await assert.rejects(validateKnowledgeObject(client, fixtures.validate), TypeError);
  await assert.rejects(updateKnowledgeObject(client, fixtures.update, {
    currentKnowledgeObject: current.update,
  }), TypeError);
  await assert.rejects(setKnowledgeObjectState(client, fixtures.setState, {
    currentKnowledgeObject: current.setState,
  }), TypeError);
  await assert.rejects(deleteKnowledgeObject(client, fixtures.delete), TypeError);
});

test("quarantine preparation and execution bind the signed plan and ordered cascade", async () => {
  const recoveryToken = "A".repeat(96);
  const prepareRequest = PrepareKnowledgeObjectQuarantineRequest.fromPartial({
    knowledgeObjectId: "ko-quarantine-root",
  });
  const preparation = adaptKnowledgePrepareQuarantineResponse(
    PrepareKnowledgeObjectQuarantineResponse.fromPartial({
      rootKnowledgeObjectId: "ko-quarantine-root",
      recoveryToken,
      expiresAt: new Date("2035-08-01T10:10:00.000Z"),
      dependentCount: 1,
      tenantCatalogRevision: 12n,
    }),
    prepareRequest,
  );
  assert.equal(preparation.dependentCount, 1);
  assert.notEqual(preparation.expiresAt, undefined);

  const request = QuarantineKnowledgeObjectRequest.fromPartial({
    recoveryToken,
    clientRequestId: "browser-quarantine-request-0001",
  });
  const response = QuarantineKnowledgeObjectResponse.fromPartial({
    rootKnowledgeObjectId: "ko-quarantine-root",
    transitions: [{
      cascadeOrdinal: 0,
      knowledgeObjectId: "ko-active-dependent",
      previousVersion: 4n,
      quarantinedVersion: 5n,
      reason: KnowledgeQuarantineReason
        .KNOWLEDGE_QUARANTINE_REASON_DEPENDENCY_RECOVERY,
    }, {
      cascadeOrdinal: 1,
      knowledgeObjectId: "ko-quarantine-root",
      previousVersion: 7n,
      quarantinedVersion: 8n,
      reason: KnowledgeQuarantineReason.KNOWLEDGE_QUARANTINE_REASON_ROOT_CORRUPTION,
    }],
    tenantCatalogRevision: 13n,
  });
  const receipt = await adaptKnowledgeQuarantineResponse(
    response,
    request,
    preparation,
  );
  assert.equal(receipt.rootKnowledgeObjectId, "ko-quarantine-root");
  assert.deepEqual(receipt.transitions.map((transition) => transition.knowledgeObjectId), [
    "ko-active-dependent",
    "ko-quarantine-root",
  ]);

  const malformed = QuarantineKnowledgeObjectResponse.fromPartial(response);
  malformed.transitions[0]!.reason = KnowledgeQuarantineReason
    .KNOWLEDGE_QUARANTINE_REASON_ROOT_CORRUPTION;
  await assert.rejects(
    adaptKnowledgeQuarantineResponse(malformed, request, preparation),
    TypeError,
  );
  const wrongRevision = QuarantineKnowledgeObjectResponse.fromPartial({
    ...response,
    tenantCatalogRevision: preparation.tenantCatalogRevision + 2n,
  });
  await assert.rejects(
    adaptKnowledgeQuarantineResponse(wrongRevision, request, preparation),
    TypeError,
  );
  assert.throws(() => knowledgePrepareQuarantineRequest(
    PrepareKnowledgeObjectQuarantineRequest.fromPartial({ knowledgeObjectId: " bad " }),
  ), TypeError);
  assert.throws(() => knowledgeQuarantineRequest(
    QuarantineKnowledgeObjectRequest.fromPartial({
      recoveryToken: `${recoveryToken}=`,
      clientRequestId: "browser-quarantine-request-0001",
    }),
  ), TypeError);
});

test("quarantine client uses both protected routes without exposing its token in a URL", async () => {
  const recoveryToken = "B".repeat(96);
  const paths: string[] = [];
  const client = createKnowledgeMutationClient({
    baseUrl: "https://example.test",
    fetch: async (input, init) => {
      const url = new URL(String(input));
      paths.push(url.pathname);
      assert.equal(url.search, "");
      assert.doesNotMatch(String(input), new RegExp(recoveryToken));
      const bytes = init?.body;
      assert.ok(bytes instanceof Uint8Array);
      if (url.pathname.endsWith("/prepare")) {
        assert.equal(
          PrepareKnowledgeObjectQuarantineRequest.decode(bytes).knowledgeObjectId,
          "ko-quarantine-root",
        );
        return new Response(PrepareKnowledgeObjectQuarantineResponse.encode(
          PrepareKnowledgeObjectQuarantineResponse.fromPartial({
            rootKnowledgeObjectId: "ko-quarantine-root",
            recoveryToken,
            expiresAt: new Date("2035-08-01T10:10:00.000Z"),
            dependentCount: 0,
            tenantCatalogRevision: 20n,
          }),
        ).finish(), { status: 200, headers: { "Content-Type": PROTOBUF_CONTENT_TYPE } });
      }
      const decoded = QuarantineKnowledgeObjectRequest.decode(bytes);
      assert.equal(decoded.recoveryToken, recoveryToken);
      return new Response(QuarantineKnowledgeObjectResponse.encode(
        QuarantineKnowledgeObjectResponse.fromPartial({
          rootKnowledgeObjectId: "ko-quarantine-root",
          transitions: [{
            cascadeOrdinal: 0,
            knowledgeObjectId: "ko-quarantine-root",
            previousVersion: 2n,
            quarantinedVersion: 3n,
            reason: KnowledgeQuarantineReason.KNOWLEDGE_QUARANTINE_REASON_ROOT_CORRUPTION,
          }],
          tenantCatalogRevision: 21n,
        }),
      ).finish(), { status: 200, headers: { "Content-Type": PROTOBUF_CONTENT_TYPE } });
    },
  });
  const preparation = await prepareKnowledgeObjectQuarantine(
    client,
    PrepareKnowledgeObjectQuarantineRequest.fromPartial({
      knowledgeObjectId: "ko-quarantine-root",
    }),
  );
  const receipt = await quarantineKnowledgeObject(
    client,
    QuarantineKnowledgeObjectRequest.fromPartial({
      recoveryToken,
      clientRequestId: "browser-quarantine-request-0002",
    }),
    { preparation },
  );
  assert.equal(receipt.transitions.length, 1);
  assert.deepEqual(paths, [
    "/api/knowledge/objects/quarantine/prepare",
    "/api/knowledge/objects/quarantine",
  ]);
});

test("knowledge mutation authority verifies digests and detaches before asynchronous hashing", async () => {
  const fixtures = mutationFixtures();
  const token = new Uint8Array(32).fill(9);
  const badDigestObject = await sealedMutationObject({
    version: 1n,
    state: KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DRAFT,
    definition: fixtures.definition,
  });
  badDigestObject.definitionSha256[0] ^= 0xff;
  await assert.rejects(
    adaptKnowledgeCreateResponse(
      CreateKnowledgeObjectResponse.fromPartial({
        knowledgeObject: badDigestObject,
        tenantCatalogRevision: 1n,
        tenantCatalogStateToken: token,
      }),
      fixtures.create,
    ),
    TypeError,
  );

  const badLifecycleObject = await sealedMutationObject({
    version: 1n,
    state: KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DRAFT,
    definition: fixtures.definition,
  });
  badLifecycleObject.createdAt = new Date("2026-08-01T10:00:00.000Z");
  await assert.rejects(
    adaptKnowledgeCreateResponse(
      CreateKnowledgeObjectResponse.fromPartial({
        knowledgeObject: badLifecycleObject,
        tenantCatalogRevision: 1n,
        tenantCatalogStateToken: token,
      }),
      fixtures.create,
    ),
    TypeError,
  );

  const digest = await definitionDigest(fixtures.definition);
  const expectedDigest = Uint8Array.from(digest);
  const validationResponse = ValidateKnowledgeObjectResponse.fromPartial({
    result: {
      valid: true,
      objectType: KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
      normalizedDefinition: fixtures.definition,
      definitionSha256: Uint8Array.from(digest),
      resources: {
        selectorPatterns: 1,
        normalizedDefinitionBytes: BigInt(
          KnowledgeObjectDefinition.encode(fixtures.definition).finish().byteLength,
        ),
      },
    },
    tenantCatalogRevision: 1n,
  });
  const pending = adaptKnowledgeValidationResponse(validationResponse, fixtures.validate);
  assert.ok(validationResponse.result?.normalizedDefinition);
  validationResponse.result.normalizedDefinition.name = "mutated-during-digest";
  validationResponse.result.definitionSha256?.fill(0);
  const receipt = await pending;

  assert.equal(receipt.result.normalizedDefinition?.name, fixtures.definition.name);
  assert.deepEqual(receipt.result.definitionSha256, expectedDigest);
});

test("create response binding distinguishes an absent extraction input from whitespace", async () => {
  const fixtures = mutationFixtures();
  const submittedDefinition = KnowledgeObjectDefinition.fromPartial({
    ...fixtures.definition,
    body: {
      $case: "fieldExtraction",
      value: {
        inputField: "",
        overwriteBehavior:
          KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_UNSPECIFIED,
        extraction: {
          $case: "json",
          value: { path: "$.status", outputField: "http_status" },
        },
      },
    },
  });
  const normalizedDefinition = KnowledgeObjectDefinition.fromPartial({
    ...submittedDefinition,
    body: {
      $case: "fieldExtraction",
      value: {
        ...submittedDefinition.body?.value,
        inputField: "_raw",
        overwriteBehavior:
          KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
      },
    },
  });
  const request = CreateKnowledgeObjectRequest.fromPartial({
    definition: submittedDefinition,
    initialState: KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DRAFT,
    clientRequestId: "browser-create-input-0001",
  });
  const response = CreateKnowledgeObjectResponse.fromPartial({
    knowledgeObject: await sealedMutationObject({
      version: 1n,
      state: KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DRAFT,
      definition: normalizedDefinition,
    }),
    tenantCatalogRevision: 1n,
    tenantCatalogStateToken: new Uint8Array(32).fill(3),
  });

  const accepted = await adaptKnowledgeCreateResponse(response, request);
  assert.equal(
    accepted.knowledgeObject.definition?.body?.$case === "fieldExtraction"
      ? accepted.knowledgeObject.definition.body.value.inputField
      : undefined,
    "_raw",
  );

  const whitespaceRequest = CreateKnowledgeObjectRequest.decode(
    CreateKnowledgeObjectRequest.encode(request).finish(),
  );
  assert.equal(whitespaceRequest.definition?.body?.$case, "fieldExtraction");
  if (whitespaceRequest.definition?.body?.$case === "fieldExtraction") {
    whitespaceRequest.definition.body.value.inputField = " \t ";
  }
  await assert.rejects(
    adaptKnowledgeCreateResponse(response, whitespaceRequest),
    TypeError,
  );
});

test("update and state adapters bind every unselected field to the current object", async () => {
  const fixtures = mutationFixtures();
  const current = await mutationCurrentAuthorities(fixtures);
  const token = new Uint8Array(32).fill(6);
  const updateResponse = UpdateKnowledgeObjectResponse.fromPartial({
    knowledgeObject: await sealedMutationObject({
      version: fixtures.update.expectedVersion + 1n,
      state: current.update.state,
      definition: fixtures.updatedDefinition,
    }),
    tenantCatalogRevision: 3n,
    tenantCatalogStateToken: token,
  });
  await adaptKnowledgeUpdateResponse(updateResponse, fixtures.update, current.update);

  const forgedDefinition = KnowledgeObjectDefinition.fromPartial({
    ...fixtures.updatedDefinition,
    name: "forged-unselected-name",
  });
  const forgedUpdateResponse = UpdateKnowledgeObjectResponse.fromPartial({
    ...updateResponse,
    knowledgeObject: await sealedMutationObject({
      version: fixtures.update.expectedVersion + 1n,
      state: current.update.state,
      definition: forgedDefinition,
    }),
  });
  await assert.rejects(
    adaptKnowledgeUpdateResponse(
      forgedUpdateResponse,
      fixtures.update,
      current.update,
    ),
    TypeError,
  );

  const stateResponse = SetKnowledgeObjectStateResponse.fromPartial({
    knowledgeObject: await sealedMutationObject({
      version: fixtures.setState.expectedVersion + 1n,
      state: fixtures.setState.state,
      definition: fixtures.updatedDefinition,
    }),
    tenantCatalogRevision: 4n,
    tenantCatalogStateToken: token,
  });
  await adaptKnowledgeSetStateResponse(
    stateResponse,
    fixtures.setState,
    current.setState,
  );
  const forgedStateResponse = SetKnowledgeObjectStateResponse.fromPartial({
    ...stateResponse,
    knowledgeObject: await sealedMutationObject({
      version: fixtures.setState.expectedVersion + 1n,
      state: fixtures.setState.state,
      definition: forgedDefinition,
    }),
  });
  await assert.rejects(
    adaptKnowledgeSetStateResponse(
      forgedStateResponse,
      fixtures.setState,
      current.setState,
    ),
    TypeError,
  );
});

test("validation diagnostics retain submitted leading trim and canonical EOF authority", async () => {
  const fixtures = mutationFixtures();
  const validationRequest = (expression: string) => ValidateKnowledgeObjectRequest.fromPartial({
    definition: {
      ...fixtures.definition,
      body: {
        $case: "calculatedField",
        value: {
          destinationField: "latency_class",
          expression,
          overwriteBehavior:
            KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
        },
      },
    },
    intent: KnowledgeValidationIntent.KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION,
  });

  const leadingSource = " \n\tcoalesce(\"😀\", mystery(host)) \r\n";
  const leadingRequest = validationRequest(leadingSource);
  const mysteryStart = BigInt(new TextEncoder().encode(
    leadingSource.slice(0, leadingSource.indexOf("mystery")),
  ).byteLength);
  await adaptKnowledgeValidationResponse(
    invalidCalculatedValidationResponse(
      { byteOffset: mysteryStart, line: 2, column: 16 },
      { byteOffset: mysteryStart + 7n, line: 2, column: 23 },
    ),
    leadingRequest,
  );

  const eofSource = "\n\tlower(host \r\n";
  const eofRequest = validationRequest(eofSource);
  const eofOffset = BigInt(new TextEncoder().encode(eofSource).byteLength);
  await adaptKnowledgeValidationResponse(
    invalidCalculatedValidationResponse(
      { byteOffset: eofOffset, line: 3, column: 1 },
      { byteOffset: eofOffset, line: 3, column: 1 },
    ),
    eofRequest,
  );
});

test("update validation verifies ranges against explicit current-definition authority", async () => {
  const fixtures = mutationFixtures();
  const currentDefinition = KnowledgeObjectDefinition.fromPartial({
    ...fixtures.definition,
    body: {
      $case: "calculatedField",
      value: {
        destinationField: "latency_class",
        expression: "α\nx",
        overwriteBehavior:
          KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
      },
    },
  });
  const request = ValidateKnowledgeObjectRequest.fromPartial({
    definition: fixtures.updatedDefinition,
    knowledgeObjectId: "ko-calculated-1",
    expectedVersion: 2n,
    updateMask: ["description"],
    intent: KnowledgeValidationIntent.KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION,
  });
  const currentObject = await sealedMutationObject({
    id: request.knowledgeObjectId,
    version: request.expectedVersion ?? 0n,
    state: KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DRAFT,
    definition: currentDefinition,
  });
  const response = ValidateKnowledgeObjectResponse.fromPartial({
    result: {
      valid: false,
      objectType: KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD,
      diagnostics: [{
        fieldPath: "calculated_field.expression",
        diagnostic: {
          severity: DiagnosticSeverity.DIAGNOSTIC_SEVERITY_ERROR,
          code: "INVALID_EXPRESSION",
          message: "Invalid expression",
          sourceRange: {
            start: { byteOffset: 3n, line: 2, column: 1 },
            end: { byteOffset: 4n, line: 2, column: 2 },
          },
        },
      }],
    },
    tenantCatalogRevision: 2n,
  });

  await assert.rejects(adaptKnowledgeValidationResponse(response, request), TypeError);
  const staleCurrentObject = KnowledgeObject.decode(KnowledgeObject.encode(currentObject).finish());
  staleCurrentObject.version -= 1n;
  await assert.rejects(
    adaptKnowledgeValidationResponse(response, request, staleCurrentObject),
    TypeError,
  );
  const receipt = await adaptKnowledgeValidationResponse(
    response,
    request,
    currentObject,
  );
  assert.equal(receipt.result.diagnostics[0]?.diagnostic?.sourceRange?.start?.line, 2);

  const forged = ValidateKnowledgeObjectResponse.decode(
    ValidateKnowledgeObjectResponse.encode(response).finish(),
  );
  assert.ok(forged.result?.diagnostics[0]?.diagnostic?.sourceRange?.start);
  forged.result.diagnostics[0].diagnostic.sourceRange.start.column = 2;
  await assert.rejects(
    adaptKnowledgeValidationResponse(forged, request, currentObject),
    TypeError,
  );
});

test("update validation rejects inapplicable current lifecycle and body authority", async () => {
  const fixtures = mutationFixtures();
  const currentAlias = await sealedMutationObject({
    version: 2n,
    state: KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DRAFT,
    definition: fixtures.definition,
  });
  const calculatedDefinition = KnowledgeObjectDefinition.fromPartial({
    ...fixtures.definition,
    body: {
      $case: "calculatedField",
      value: {
        destinationField: "latency_class",
        expression: "coalesce(latency_ms, 0)",
        overwriteBehavior:
          KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
      },
    },
  });
  const wrongBodyRequest = ValidateKnowledgeObjectRequest.fromPartial({
    definition: calculatedDefinition,
    knowledgeObjectId: currentAlias.knowledgeObjectId,
    expectedVersion: currentAlias.version,
    updateMask: ["calculated_field"],
    intent: KnowledgeValidationIntent.KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
  });
  const forgedValidResponse = ValidateKnowledgeObjectResponse.fromPartial({
    result: {
      valid: true,
      objectType: KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD,
      normalizedDefinition: calculatedDefinition,
      definitionSha256: await definitionDigest(calculatedDefinition),
      resources: {
        selectorPatterns: 1,
        normalizedDefinitionBytes: BigInt(
          KnowledgeObjectDefinition.encode(calculatedDefinition).finish().byteLength,
        ),
      },
    },
    tenantCatalogRevision: 2n,
  });
  await assert.rejects(
    adaptKnowledgeValidationResponse(
      forgedValidResponse,
      wrongBodyRequest,
      currentAlias,
    ),
    TypeError,
  );

  const deletedCurrent = await sealedMutationObject({
    version: 2n,
    state: KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DELETED,
    definition: fixtures.definition,
  });
  const deletedRequest = ValidateKnowledgeObjectRequest.fromPartial({
    definition: fixtures.updatedDefinition,
    knowledgeObjectId: deletedCurrent.knowledgeObjectId,
    expectedVersion: deletedCurrent.version,
    updateMask: ["description"],
    intent: KnowledgeValidationIntent.KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION,
  });
  const forgedInvalidResponse = ValidateKnowledgeObjectResponse.fromPartial({
    result: {
      valid: false,
      objectType: KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
      fieldViolations: [{
        fieldPath: "description",
        code: "INVALID_DESCRIPTION",
        message: "Invalid description",
      }],
    },
    tenantCatalogRevision: 2n,
  });
  await assert.rejects(
    adaptKnowledgeValidationResponse(
      forgedInvalidResponse,
      deletedRequest,
      deletedCurrent,
    ),
    TypeError,
  );
});

test("knowledge mutation preflight is traffic-free and cancellation reaches fetch", async () => {
  const fixtures = mutationFixtures();
  let fetches = 0;
  let observedSignal: AbortSignal | undefined;
  const client = createKnowledgeMutationClient({
    baseUrl: "https://example.test",
    fetch: async (_input, init) => {
      fetches += 1;
      observedSignal = init?.signal ?? undefined;
      return await new Promise<Response>((_resolve, reject) => {
        const rejectAborted = () => reject(new DOMException("Aborted", "AbortError"));
        if (observedSignal?.aborted) rejectAborted();
        else observedSignal?.addEventListener("abort", rejectAborted, { once: true });
      });
    },
  });

  const invalidUpdate = UpdateKnowledgeObjectRequest.fromPartial({
    ...fixtures.update,
    updateMask: ["name", "description"],
  });
  assert.throws(() => knowledgeUpdateRequest(invalidUpdate), TypeError);
  const invalidValidation = ValidateKnowledgeObjectRequest.fromPartial({
    ...fixtures.validate,
    expectedVersion: 1n,
  });
  assert.throws(() => knowledgeValidateRequest(invalidValidation), TypeError);
  const updateValidationWithoutAuthority = ValidateKnowledgeObjectRequest.fromPartial({
    definition: fixtures.updatedDefinition,
    knowledgeObjectId: fixtures.update.knowledgeObjectId,
    expectedVersion: fixtures.update.expectedVersion,
    updateMask: fixtures.update.updateMask,
    intent: KnowledgeValidationIntent.KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
  });
  await assert.rejects(
    validateKnowledgeObject(client, updateValidationWithoutAuthority),
    TypeError,
  );
  const cardinalityInvalidCandidate = ValidateKnowledgeObjectRequest.fromPartial({
    ...fixtures.validate,
    definition: {
      ...fixtures.definition,
      selector: {
        indexPatterns: Array.from({ length: 18 }, (_value, index) => ({
          matchKind: KnowledgeSelectorMatchKind.KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT,
          value: `index-${index.toString().padStart(2, "0")}`,
        })),
      },
    },
  });
  assert.equal(
    knowledgeValidateRequest(cardinalityInvalidCandidate).definition?.selector
      ?.indexPatterns.length,
    18,
    "candidate-authored cardinality errors must reach server validation",
  );
  const oversizedCandidate = ValidateKnowledgeObjectRequest.fromPartial({
    ...fixtures.validate,
    definition: {
      ...fixtures.definition,
      description: "x".repeat((4 << 20) + (64 << 10)),
    },
  });
  assert.throws(() => knowledgeValidateRequest(oversizedCandidate), TypeError);
  assert.equal(fetches, 0);

  const controller = new AbortController();
  const pending = validateKnowledgeObject(client, fixtures.validate, {
    signal: controller.signal,
  });
  controller.abort();
  await assert.rejects(pending, (error: unknown) =>
    error instanceof DOMException && error.name === "AbortError");
  assert.equal(fetches, 1);
  assert.equal(observedSignal?.aborted, true);
});

test("every knowledge mutation client response is streaming-bounded before decode", async () => {
  const fixtures = mutationFixtures();
  const current = await mutationCurrentAuthorities(fixtures);
  const client = createKnowledgeMutationClient({
    baseUrl: "https://example.test",
    fetch: async () => new Response(null, {
      status: 200,
      headers: {
        "Content-Length": String(KNOWLEDGE_MANAGER_MAXIMUM_RESPONSE_BYTES + 1),
        "Content-Type": PROTOBUF_CONTENT_TYPE,
      },
    }),
  });

  await assert.rejects(
    createKnowledgeObject(client, fixtures.create),
    ProtobufResponseTooLargeError,
  );
  await assert.rejects(
    validateKnowledgeObject(client, fixtures.validate),
    ProtobufResponseTooLargeError,
  );
  await assert.rejects(
    updateKnowledgeObject(client, fixtures.update, {
      currentKnowledgeObject: current.update,
    }),
    ProtobufResponseTooLargeError,
  );
  await assert.rejects(
    setKnowledgeObjectState(client, fixtures.setState, {
      currentKnowledgeObject: current.setState,
    }),
    ProtobufResponseTooLargeError,
  );
  await assert.rejects(
    deleteKnowledgeObject(client, fixtures.delete),
    ProtobufResponseTooLargeError,
  );
  const quarantinePreparation = {
    rootKnowledgeObjectId: "ko-quarantine-root",
    recoveryToken: "C".repeat(96),
    expiresAt: new Date("2035-08-01T10:10:00.000Z"),
    dependentCount: 0,
    tenantCatalogRevision: 1n,
  };
  await assert.rejects(
    prepareKnowledgeObjectQuarantine(
      client,
      PrepareKnowledgeObjectQuarantineRequest.fromPartial({
        knowledgeObjectId: quarantinePreparation.rootKnowledgeObjectId,
      }),
    ),
    ProtobufResponseTooLargeError,
  );
  await assert.rejects(
    quarantineKnowledgeObject(
      client,
      QuarantineKnowledgeObjectRequest.fromPartial({
        recoveryToken: quarantinePreparation.recoveryToken,
        clientRequestId: "browser-quarantine-bounded-0001",
      }),
      { preparation: quarantinePreparation },
    ),
    ProtobufResponseTooLargeError,
  );
});

function jsonWithoutBigInt(value: unknown): string {
  return JSON.stringify(value, (_key, item: unknown) =>
    typeof item === "bigint" ? item.toString() : item);
}

test("feature-absent navigation is unchanged and invokes no knowledge chunk importer", async () => {
  assert.deepEqual(
    backendAdminNavigation(false, false).map(({ key, label, detail }) => ({ key, label, detail })),
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
});

test("an older field-knowledge-only server does not expose lookup management", () => {
  const capabilities = backendKnowledgeCapabilities(new Set([
    ServerFeature.SERVER_FEATURE_KNOWLEDGE_FIELD_OBJECTS,
  ]));
  assert.deepEqual(capabilities, { knowledge: true, lookupManagement: false, quarantine: false });
  const navigation = backendAdminNavigation(
    capabilities.knowledge,
    capabilities.lookupManagement,
  );
  assert.equal(navigation.filter((item) => item.key === "knowledge").length, 1);
  assert.equal(navigation.find((item) => item.key === "knowledge")?.detail, "Tier-1 definitions");
  assert.equal(navigation.filter((item) => item.key === "lookups").length, 0);
});

test("independently advertised lookup management adds its navigation destination", () => {
  const capabilities = backendKnowledgeCapabilities(new Set([
    ServerFeature.SERVER_FEATURE_KNOWLEDGE_FIELD_OBJECTS,
    ServerFeature.SERVER_FEATURE_LOOKUP_MANAGEMENT,
  ]));
  assert.deepEqual(capabilities, { knowledge: true, lookupManagement: true, quarantine: false });
  const navigation = backendAdminNavigation(
    capabilities.knowledge,
    capabilities.lookupManagement,
  );
  assert.equal(navigation.filter((item) => item.key === "knowledge").length, 1);
  assert.equal(navigation.filter((item) => item.key === "lookups").length, 1);
  assert.equal(navigation.find((item) => item.key === "lookups")?.detail, "Exact CSV enrichment");
});

test("lookup capability without field knowledge fails closed", () => {
  const capabilities = backendKnowledgeCapabilities(new Set([
    ServerFeature.SERVER_FEATURE_LOOKUP_MANAGEMENT,
  ]));
  assert.deepEqual(capabilities, { knowledge: false, lookupManagement: false, quarantine: false });
  const navigation = backendAdminNavigation(
    capabilities.knowledge,
    capabilities.lookupManagement,
  );
  assert.equal(navigation.filter((item) => item.key === "knowledge").length, 0);
  assert.equal(navigation.filter((item) => item.key === "lookups").length, 0);
});

test("quarantine capability is independently gated by field knowledge", () => {
  const available = backendKnowledgeCapabilities(new Set([
    ServerFeature.SERVER_FEATURE_KNOWLEDGE_FIELD_OBJECTS,
    ServerFeature.SERVER_FEATURE_KNOWLEDGE_QUARANTINE,
  ]));
  assert.deepEqual(available, {
    knowledge: true,
    lookupManagement: false,
    quarantine: true,
  });

  const orphaned = backendKnowledgeCapabilities(new Set([
    ServerFeature.SERVER_FEATURE_KNOWLEDGE_QUARANTINE,
  ]));
  assert.deepEqual(orphaned, {
    knowledge: false,
    lookupManagement: false,
    quarantine: false,
  });
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

test("mutation detail reads return one exact digest-bound current object", async () => {
  const fixture = await sealedMutationObject({
    version: 2n,
    state: KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE,
  });
  const requests: GetKnowledgeObjectRequest[] = [];
  const signals: Array<AbortSignal | undefined> = [];
  const client: KnowledgeReadClient = {
    ...unavailableGraphReads,
    async list() { throw new Error("list must not be called"); },
    async get(request, options) {
      requests.push(request);
      signals.push(options?.signal);
      return { knowledgeObject: fixture };
    },
  };
  const controller = new AbortController();
  const result = await loadKnowledgeMutationDetail(client, {
    knowledgeObjectId: fixture.knowledgeObjectId,
    version: fixture.version,
  }, { signal: controller.signal });
  assert.equal(result.status, "available");
  assert.deepEqual(requests, [{
    knowledgeObjectId: fixture.knowledgeObjectId,
    version: fixture.version,
  }]);
  assert.equal(signals[0], controller.signal);
  if (result.status === "available") {
    assert.equal(result.object.knowledgeObjectId, fixture.knowledgeObjectId);
    assert.notEqual(result.currentKnowledgeObject, fixture);
    fixture.definition!.name = "caller-mutated";
    fixture.definitionSha256.fill(0);
    assert.notEqual(result.currentKnowledgeObject.definition?.name, "caller-mutated");
    assert.notDeepEqual(result.currentKnowledgeObject.definitionSha256, fixture.definitionSha256);
  }

  const malformed = await sealedMutationObject({
    version: 2n,
    state: KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE,
  });
  malformed.definitionSha256[0] ^= 0xff;
  assert.deepEqual(await loadKnowledgeMutationDetail({
    ...client,
    async get() { return { knowledgeObject: malformed }; },
  }, {
    knowledgeObjectId: malformed.knowledgeObjectId,
    version: malformed.version,
  }), { status: "unavailable" });
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
  const inspectorRequest = { current: null as AbortController | null };
  const cleanup = knowledgeRelationshipUnmountCleanup(request, inspectorRequest);
  const continuation = new AbortController();
  const replacementInspector = new AbortController();
  request.current = continuation;
  inspectorRequest.current = replacementInspector;
  cleanup();
  assert.equal(continuation.signal.aborted, true);
  assert.equal(replacementInspector.signal.aborted, true);
  assert.equal(request.current, null);
  assert.equal(inspectorRequest.current, null);

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
  assert.match(markup, /shown only inside the explicit editor/);
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
    inspector: { state: "closed" },
    onRetry: () => undefined,
    onLoadMore: () => undefined,
    onInspect: () => undefined,
    onRetryInspector: () => undefined,
  }));
  assert.match(markup, /id="knowledge-dependencies-title"/);
  assert.match(markup, /aria-label="Visible direct dependencies"/);
  assert.match(markup, /ko-&lt;script&gt;/);
  assert.match(markup, /Field input/);
  assert.match(markup, /aria-label="Inspect dependency ko-&lt;script&gt;, version 2"/);
  assert.doesNotMatch(markup, /aria-controls="knowledge-dependencies-related-object-inspector"/);
  assert.match(markup, /aria-expanded="false"/);
  assert.match(markup, /revision 9/);
  assert.doesNotMatch(markup, /<script>|href=|>Create<|>Edit<|>Delete<|>Enable<|>Disable<|>Save</);

  const staleMarkup = renderToStaticMarkup(createElement(KnowledgeRelationshipSectionView, {
    direction: "dependents",
    state: "stale",
    page: { ...page, direction: "dependents" },
    loadingMore: false,
    inspector: { state: "closed" },
    onRetry: () => undefined,
    onLoadMore: () => undefined,
    onInspect: () => undefined,
    onRetryInspector: () => undefined,
  }));
  assert.match(staleMarkup, /role="alert"/);
  assert.match(staleMarkup, /Reload dependents/);
  assert.doesNotMatch(staleMarkup, /Load more dependents/);
});

test("related-object inspector states are direction-labelled, exact, escaped, and compact", () => {
  const queryValue = relationshipQuery("dependencies");
  const page = adaptKnowledgeRelationshipPage(relationshipResponse(queryValue, {
    edges: [relationshipEdge("dependencies", "ko-<script>", 2n)],
  }), queryValue);
  const edge = page.edges[0]!;
  const relatedWireObject = fieldAliasObject({
    id: edge.knowledgeObjectId,
    name: "<img src=x onerror=globalThis.__relatedInspectorExecuted=true>",
    version: edge.version,
  });
  relatedWireObject.definition!.description = "Description <script>unsafe()</script> stays text.";
  relatedWireObject.definition!.body = {
    $case: "fieldAlias",
    value: {
      sourceField: "TOP_SECRET_RELATED_SELECTOR_OR_EXPRESSION",
      destinationField: "safe_field",
      overwriteBehavior: KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
    },
  };
  const relatedObject = adaptKnowledgeObject(relatedWireObject, 0);
  assert.equal(relatedObject.disclosure, "available");
  if (relatedObject.disclosure !== "available") return;

  const availableMarkup = renderToStaticMarkup(createElement(
    KnowledgeRelatedObjectInspectorView,
    {
      direction: "dependencies",
      inspector: { state: "available", edge, object: relatedObject },
      onRetry: () => undefined,
    },
  ));
  assert.match(availableMarkup, /id="knowledge-dependencies-related-object-inspector"/);
  assert.match(availableMarkup, /id="knowledge-dependencies-related-object-inspector-title"/);
  assert.match(availableMarkup, /<section[^>]*aria-labelledby="knowledge-dependencies-related-object-inspector-title"/);
  assert.match(availableMarkup, /aria-busy="false"/);
  assert.match(availableMarkup, /aria-live="polite"/);
  assert.match(availableMarkup, /Dependency object inspector/);
  assert.match(availableMarkup, /ko-&lt;script&gt;/);
  assert.match(availableMarkup, /&lt;img src=x onerror=globalThis.__relatedInspectorExecuted=true&gt;/);
  assert.match(availableMarkup, /Description &lt;script&gt;unsafe\(\)&lt;\/script&gt; stays text\./);
  assert.match(availableMarkup, /<dt>Type<\/dt>/);
  assert.match(availableMarkup, /<dt>Updated<\/dt>/);
  assert.doesNotMatch(
    availableMarkup,
    /TOP_SECRET_RELATED_SELECTOR_OR_EXPRESSION|Selectors|Definition summary|Direct relationships/,
  );
  assert.doesNotMatch(
    availableMarkup,
    /<script>|<img|href=|>Create<|>Edit<|>Delete<|>Enable<|>Disable<|>Save/,
  );

  const activeSectionMarkup = renderToStaticMarkup(createElement(
    KnowledgeRelationshipSectionView,
    {
      direction: "dependencies",
      state: "available",
      page,
      loadingMore: false,
      inspector: { state: "available", edge, object: relatedObject },
      onRetry: () => undefined,
      onLoadMore: () => undefined,
      onInspect: () => undefined,
      onRetryInspector: () => undefined,
    },
  ));
  assert.match(activeSectionMarkup, /aria-label="Close dependency ko-&lt;script&gt;, version 2"/);
  assert.match(activeSectionMarkup, /aria-controls="knowledge-dependencies-related-object-inspector"/);
  assert.match(activeSectionMarkup, /aria-expanded="true"/);

  const loadingMarkup = renderToStaticMarkup(createElement(
    KnowledgeRelatedObjectInspectorView,
    {
      direction: "dependents",
      inspector: { state: "loading", edge },
      onRetry: () => undefined,
    },
  ));
  assert.match(loadingMarkup, /id="knowledge-dependents-related-object-inspector"/);
  assert.match(loadingMarkup, /Dependent object inspector/);
  assert.match(loadingMarkup, /aria-busy="true"/);
  assert.match(loadingMarkup, /Loading related object/);
  assert.doesNotMatch(loadingMarkup, />Retry<|Related object unavailable/);

  const unavailableMarkup = renderToStaticMarkup(createElement(
    KnowledgeRelatedObjectInspectorView,
    {
      direction: "dependents",
      inspector: { state: "unavailable", edge },
      onRetry: () => undefined,
    },
  ));
  assert.match(unavailableMarkup, /Related object unavailable\. This object cannot be inspected\./);
  assert.match(unavailableMarkup, /aria-label="Retry dependent ko-&lt;script&gt;, version 2"/);
  assert.match(unavailableMarkup, />Retry<\/button>/);
  assert.doesNotMatch(unavailableMarkup, /forbidden|missing|decoder|route|backend|status 404/i);

  const mismatched = adaptKnowledgeObject(fieldAliasObject({
    id: "ko-mismatch-secret",
    name: "SECRET_MISMATCHED_RELATED_OBJECT",
    version: 9n,
  }), 0);
  assert.equal(mismatched.disclosure, "available");
  const mismatchedMarkup = renderToStaticMarkup(createElement(
    KnowledgeRelatedObjectInspectorView,
    {
      direction: "dependencies",
      inspector: {
        state: "available",
        edge,
        object: mismatched.disclosure === "available" ? mismatched : relatedObject,
      },
      onRetry: () => undefined,
    },
  ));
  assert.match(mismatchedMarkup, /Related object unavailable/);
  assert.doesNotMatch(mismatchedMarkup, /SECRET_MISMATCHED_RELATED_OBJECT/);

  assert.equal(renderToStaticMarkup(createElement(KnowledgeRelatedObjectInspectorView, {
    direction: "dependencies",
    inspector: { state: "closed" },
    onRetry: () => undefined,
  })), "");
});

function completeMutationDraft(
  kind: KnowledgeMutationDraft["kind"],
): KnowledgeMutationDraft {
  return {
    ...createKnowledgeMutationDraft("app-observability"),
    kind,
    name: `${kind}-object`,
    description: "Tier-1 browser definition",
    indexPatterns: "main\nprod-*\nliteral\\*star",
    hostPatterns: "api-?",
    sourcePatterns: "/srv/api.log",
    sourcetypePatterns: "open_splunk:test",
    regexPattern: "status=(?<status>[0-9]+)",
    regexOutputFields: "status\nhttp_status",
    jsonPath: "$.http.status",
    jsonOutputField: "http_status",
    aliasSourceField: "status",
    aliasDestinationField: "http_status",
    calculatedDestinationField: "latency_bucket",
    calculatedExpression: "if(duration_ms > 1000, \"slow\", \"fast\")",
    overwrite: "replace",
  };
}

test("Tier-1 mutation drafts encode all four bodies and exact selector authority", () => {
  const expectedCases = [
    ["regex-extraction", "fieldExtraction", "regex"],
    ["json-extraction", "fieldExtraction", "json"],
    ["field-alias", "fieldAlias", undefined],
    ["calculated-field", "calculatedField", undefined],
  ] as const;
  for (const [kind, bodyCase, extractionCase] of expectedCases) {
    const definition = knowledgeDefinitionFromMutationDraft(completeMutationDraft(kind));
    assert.equal(definition.body?.$case, bodyCase);
    if (definition.body?.$case === "fieldExtraction") {
      assert.equal(definition.body.value.inputField, "_raw");
      assert.equal(definition.body.value.extraction?.$case, extractionCase);
    }
    assert.equal(
      definition.selector?.indexPatterns[0]?.matchKind,
      KnowledgeSelectorMatchKind.KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT,
    );
    assert.equal(
      definition.selector?.indexPatterns[1]?.matchKind,
      KnowledgeSelectorMatchKind.KNOWLEDGE_SELECTOR_MATCH_KIND_WILDCARD,
    );
    assert.equal(
      definition.selector?.indexPatterns[2]?.matchKind,
      KnowledgeSelectorMatchKind.KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT,
    );
    assert.equal(
      definition.selector?.hostPatterns[0]?.matchKind,
      KnowledgeSelectorMatchKind.KNOWLEDGE_SELECTOR_MATCH_KIND_WILDCARD,
    );
  }
  assert.equal(
    knowledgeBrowserClientRequestId(new Uint8Array(16).fill(0xab)),
    `browser-${"ab".repeat(16)}`,
  );
  assert.throws(() => knowledgeBrowserClientRequestId(new Uint8Array(15)), TypeError);
  assert.throws(() => knowledgeDefinitionFromMutationDraft({
    ...completeMutationDraft("field-alias"),
    overwrite: "future" as KnowledgeMutationDraft["overwrite"],
  }), TypeError);
  assert.throws(() => knowledgeDefinitionFromMutationDraft({
    ...completeMutationDraft("field-alias"),
    sharingScope: "future" as KnowledgeMutationDraft["sharingScope"],
  }), TypeError);
});

test("edit drafts retain the exact kind and compute one sorted top-level mask", async () => {
  const current = await sealedMutationObject({
    version: 2n,
    state: KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE,
  });
  const draft = knowledgeMutationDraftFromObject(current);
  assert.ok(draft);
  assert.equal(draft.kind, "field-alias");
  const unchanged = knowledgeDefinitionFromMutationDraft(draft);
  assert.deepEqual(knowledgeDefinitionUpdateMask(current.definition!, unchanged), []);

  const changed = knowledgeDefinitionFromMutationDraft({
    ...draft,
    appId: "app-security",
    name: "renamed_alias",
    description: "Changed",
    sharingScope: "global",
    sourcePatterns: "source::api",
    aliasDestinationField: "status_code",
  });
  assert.deepEqual(knowledgeDefinitionUpdateMask(current.definition!, changed), [
    "app_id",
    "description",
    "field_alias",
    "name",
    "selector",
    "sharing_scope",
  ]);
  const differentKind = knowledgeDefinitionFromMutationDraft(
    completeMutationDraft("calculated-field"),
  );
  assert.throws(
    () => knowledgeDefinitionUpdateMask(current.definition!, differentKind),
    TypeError,
  );
});

test("the mutation editor renders each Tier-1 body without a Preview authority", () => {
  const bodyLabels = new Map<KnowledgeMutationDraft["kind"], string>([
    ["regex-extraction", "Regex pattern"],
    ["json-extraction", "JSON path"],
    ["field-alias", "Source field"],
    ["calculated-field", "Expression"],
  ]);
  for (const [kind, label] of bodyLabels) {
    const markup = renderToStaticMarkup(createElement(KnowledgeMutationEditor, {
      client: unavailableMutations,
      apps: [{ appId: "app-observability", label: "Observability" }],
      initialDraft: completeMutationDraft(kind),
      onCancel: () => undefined,
      onCommitted: () => undefined,
    }));
    assert.match(markup, new RegExp(label));
    assert.match(markup, /Validate draft/);
    assert.match(markup, /Create draft/);
    assert.doesNotMatch(markup, /Preview/);
  }

  const activeMarkup = renderToStaticMarkup(createElement(
    KnowledgeObjectMutationControls,
    {
      client: unavailableMutations,
      apps: [{ appId: "app-observability", label: "Observability" }],
      currentKnowledgeObject: fieldAliasObject(),
      onCommitted: () => undefined,
    },
  ));
  assert.match(activeMarkup, />Edit</);
  assert.match(activeMarkup, />Disable</);
  assert.match(activeMarkup, />Delete</);
  assert.doesNotMatch(activeMarkup, /Preview/);

  const quarantineMarkup = renderToStaticMarkup(createElement(
    KnowledgeQuarantineControl,
    {
      client: unavailableMutations,
      knowledgeObjectId: "ko-corrupt-visible-identity",
      name: "corrupt_visible_identity",
      state: KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE,
      onCommitted: () => undefined,
    },
  ));
  assert.match(quarantineMarkup, /aria-label="Knowledge recovery actions"/);
  assert.match(quarantineMarkup, />Quarantine</);
  assert.doesNotMatch(quarantineMarkup, /recoveryToken|AAAA|definition/);
});

test("the panel loading shell labels every closed filter and exposes gated creation", () => {
  const markup = renderToStaticMarkup(createElement(KnowledgeManagerPanel, {
    apiBaseUrl: "",
    apps: [{ appId: "app-observability", label: "Observability" }],
    initialAppId: "app-observability",
    maximumPageSize: 50,
    quarantineAvailable: false,
  }));
  assert.match(markup, /id="knowledge-manager-title"/);
  assert.match(markup, /Tier 1/);
  assert.match(markup, /Create knowledge object/);
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
  assert.doesNotMatch(markup, />Edit<|>Delete<|>Activate<|>Disable<|>Save changes</);
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
