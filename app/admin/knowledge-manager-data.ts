import {
  DiagnosticSeverity,
  SortDirection,
  SharingScope,
  type SourceRange,
} from "@/gen/ts/open_splunk/common";
import {
  KnowledgeDependencyRole,
  KnowledgeObject,
  KnowledgeObjectState,
  KnowledgeObjectType,
  KnowledgeObjectDefinition,
  KnowledgeOverwriteBehavior,
  KnowledgeSelector,
  KnowledgeSelectorMatchKind,
} from "@/gen/ts/open_splunk/knowledge";
import {
  CreateKnowledgeObjectRequest,
  CreateKnowledgeObjectResponse,
  DeleteKnowledgeObjectRequest,
  DeleteKnowledgeObjectResponse,
  GetKnowledgeObjectRequest,
  KnowledgeValidationIntent,
  KnowledgeObjectSortBy,
  ListKnowledgeObjectDependenciesRequest,
  ListKnowledgeObjectDependentsRequest,
  ListKnowledgeObjectsRequest,
  SetKnowledgeObjectStateRequest,
  SetKnowledgeObjectStateResponse,
  UpdateKnowledgeObjectRequest,
  UpdateKnowledgeObjectResponse,
  ValidateKnowledgeObjectRequest,
  ValidateKnowledgeObjectResponse,
  type CreateKnowledgeObjectRequest as CreateKnowledgeObjectRequestMessage,
  type CreateKnowledgeObjectResponse as CreateKnowledgeObjectResponseMessage,
  type DeleteKnowledgeObjectRequest as DeleteKnowledgeObjectRequestMessage,
  type DeleteKnowledgeObjectResponse as DeleteKnowledgeObjectResponseMessage,
  type GetKnowledgeObjectResponse as GetKnowledgeObjectResponseMessage,
  type KnowledgeManagementDependencyEdge,
  type KnowledgeManagementObjectVersionIdentity,
  type KnowledgeResourceEstimate,
  type KnowledgeValidationDiagnostic,
  type KnowledgeValidationResult,
  type ListKnowledgeObjectDependenciesResponse as ListKnowledgeObjectDependenciesResponseMessage,
  type ListKnowledgeObjectDependentsResponse as ListKnowledgeObjectDependentsResponseMessage,
  type ListKnowledgeObjectsResponse as ListKnowledgeObjectsResponseMessage,
  type SetKnowledgeObjectStateRequest as SetKnowledgeObjectStateRequestMessage,
  type SetKnowledgeObjectStateResponse as SetKnowledgeObjectStateResponseMessage,
  type UpdateKnowledgeObjectRequest as UpdateKnowledgeObjectRequestMessage,
  type UpdateKnowledgeObjectResponse as UpdateKnowledgeObjectResponseMessage,
  type ValidateKnowledgeObjectRequest as ValidateKnowledgeObjectRequestMessage,
  type ValidateKnowledgeObjectResponse as ValidateKnowledgeObjectResponseMessage,
} from "@/gen/ts/open_splunk/knowledge_api";
import {
  ProtobufTransport,
  type ProtobufRequestOptions,
  type ProtobufTransportOptions,
} from "@/lib/api/protobuf-transport";
import {
  MAXIMUM_KNOWLEDGE_GRAPH_RESPONSE_BYTES,
  MAXIMUM_KNOWLEDGE_MANAGEMENT_RESPONSE_BYTES,
  knowledgeRoutes,
} from "@/lib/api/routes";

export const KNOWLEDGE_MANAGER_DEFAULT_PAGE_SIZE = 50;
export const KNOWLEDGE_MANAGER_MAXIMUM_PAGE_SIZE = 256;
export const KNOWLEDGE_MANAGER_MAXIMUM_OBJECTS = 8_192n;
export const KNOWLEDGE_MANAGER_MAXIMUM_DEPENDENCIES = 1_024n;
export const KNOWLEDGE_MANAGER_MAXIMUM_DEPENDENTS = 8_192n;
export const KNOWLEDGE_MANAGER_MAXIMUM_PAGE_TOKEN_BYTES = 4 << 10;
export const KNOWLEDGE_MANAGER_MAXIMUM_FILTER_BYTES = 255;
export const KNOWLEDGE_MANAGER_MAXIMUM_RESPONSE_BYTES =
  MAXIMUM_KNOWLEDGE_MANAGEMENT_RESPONSE_BYTES;
export const KNOWLEDGE_MANAGER_MAXIMUM_GRAPH_RESPONSE_BYTES =
  MAXIMUM_KNOWLEDGE_GRAPH_RESPONSE_BYTES;

const MAXIMUM_IDENTITY_BYTES = 255;
const MAXIMUM_APP_ID_BYTES = 128;
const MAXIMUM_OBJECT_ID_BYTES = 128;
const MAXIMUM_DESCRIPTION_BYTES = 16 << 10;
const MAXIMUM_SELECTOR_PATTERNS_PER_DIMENSION = 16;
const MAXIMUM_SELECTOR_PATTERN_BYTES = 255;
const MAXIMUM_FIELD_EXTRACTION_OUTPUTS = 16;
const MAXIMUM_BODY_TEXT_BYTES = 16 << 10;
const MAXIMUM_REGEX_OR_PATH_BYTES = 4 << 10;
const MAXIMUM_DISPLAY_DESCRIPTION_CODE_POINTS = 480;
const MAXIMUM_SIGNED_REVISION = 9_223_372_036_854_775_807n;
const MINIMUM_CLIENT_REQUEST_ID_BYTES = 16;
const MAXIMUM_CLIENT_REQUEST_ID_BYTES = 128;
const MAXIMUM_UPDATE_MASK_PATHS = 8;
const MAXIMUM_VALIDATION_ISSUES = 256;
const MAXIMUM_VALIDATION_DEPENDENCIES = 1_024;
const MAXIMUM_FIELD_PATH_BYTES = 1 << 10;
const MAXIMUM_ISSUE_CODE_BYTES = 128;
const MAXIMUM_ISSUE_MESSAGE_BYTES = 4 << 10;
const MAXIMUM_DIAGNOSTIC_SUGGESTIONS = 32;
const MAXIMUM_DIAGNOSTIC_SUGGESTION_BYTES = 1 << 10;
const MAXIMUM_FIELD_VIOLATION_TEXT_BYTES = 256 << 10;
const MAXIMUM_DIAGNOSTIC_TEXT_BYTES = 768 << 10;
const MAXIMUM_KNOWLEDGE_MUTATION_REQUEST_BYTES = (4 << 20) + (64 << 10);
const MAXIMUM_SELECTOR_NORMALIZED_BYTES = 8 << 10;
const MAXIMUM_SELECTOR_WILDCARD_WORK_UNITS = 1 << 10;
const MAXIMUM_SEARCH_FIELD_PATH_SEGMENTS = 17;
const MAXIMUM_SEARCH_FIELD_PATH_SEGMENT_BYTES = 256;
const KNOWLEDGE_SELECTOR_CANONICAL_DOMAIN = "open-splunk/knowledge-selector\0";

const KNOWLEDGE_UPDATE_MASK_PATHS = new Set([
  "app_id",
  "name",
  "description",
  "sharing_scope",
  "selector",
  "field_extraction",
  "field_alias",
  "calculated_field",
]);

const RESERVED_DYNAMIC_FIELD_ROOTS = new Set([
  "event_id", "index", "_time", "_indextime", "host", "source", "sourcetype",
  "service", "severity", "level", "message", "_raw", "trace_id", "span_id",
  "collector_id", "batch_id", "tenant_id", "index_name", "event_time", "index_time",
  "collected_at", "event_time_source", "body", "raw", "raw_encoding", "fields",
  "field_names", "field_types", "field_metadata_version", "batch_sequence", "expires_at",
  "visibility_seq",
]);

export interface KnowledgeReadClient {
  get(
    request: ReturnType<typeof GetKnowledgeObjectRequest.fromPartial>,
    options?: ProtobufRequestOptions,
  ): Promise<GetKnowledgeObjectResponseMessage>;
  list(
    request: ReturnType<typeof ListKnowledgeObjectsRequest.fromPartial>,
    options?: ProtobufRequestOptions,
  ): Promise<ListKnowledgeObjectsResponseMessage>;
  dependencies(
    request: ReturnType<typeof ListKnowledgeObjectDependenciesRequest.fromPartial>,
    options?: ProtobufRequestOptions,
  ): Promise<ListKnowledgeObjectDependenciesResponseMessage>;
  dependents(
    request: ReturnType<typeof ListKnowledgeObjectDependentsRequest.fromPartial>,
    options?: ProtobufRequestOptions,
  ): Promise<ListKnowledgeObjectDependentsResponseMessage>;
}

export function createKnowledgeReadClient(
  options: ProtobufTransportOptions = {},
): KnowledgeReadClient {
  const transport = new ProtobufTransport(options);
  return {
    get: (request, requestOptions) => transport.post(
      knowledgeRoutes.get,
      request,
      requestOptions,
    ),
    list: (request, requestOptions) => transport.post(
      knowledgeRoutes.list,
      request,
      requestOptions,
    ),
    dependencies: (request, requestOptions) => transport.post(
      knowledgeRoutes.dependencies,
      request,
      requestOptions,
    ),
    dependents: (request, requestOptions) => transport.post(
      knowledgeRoutes.dependents,
      request,
      requestOptions,
    ),
  };
}

export interface KnowledgeMutationClient {
  create(
    request: CreateKnowledgeObjectRequestMessage,
    options?: ProtobufRequestOptions,
  ): Promise<KnowledgeObjectMutationReceipt>;
  validate(
    request: ValidateKnowledgeObjectRequestMessage,
    options?: KnowledgeValidationRequestOptions,
  ): Promise<KnowledgeValidationReceipt>;
  update(
    request: UpdateKnowledgeObjectRequestMessage,
    options: KnowledgeCurrentObjectMutationOptions,
  ): Promise<KnowledgeObjectMutationReceipt>;
  setState(
    request: SetKnowledgeObjectStateRequestMessage,
    options: KnowledgeCurrentObjectMutationOptions,
  ): Promise<KnowledgeObjectMutationReceipt>;
  delete(
    request: DeleteKnowledgeObjectRequestMessage,
    options?: ProtobufRequestOptions,
  ): Promise<KnowledgeDeleteReceipt>;
}

/**
 * Update validation diagnostics may locate text retained from the current
 * object rather than from a mask-unselected request field. Supplying the
 * exact current object lets the adapter bind those ranges to request ID,
 * expected version, canonical definition, and digest. It is local response
 * context and is never added to the protobuf request.
 */
export interface KnowledgeValidationRequestOptions extends ProtobufRequestOptions {
  readonly currentKnowledgeObject?: KnowledgeObject;
}

export interface KnowledgeCurrentObjectMutationOptions extends ProtobufRequestOptions {
  readonly currentKnowledgeObject: KnowledgeObject;
}

/**
 * Mutation responses may carry a complete bounded definition. Keep every
 * mutation route on the management response ceiling even when the shared
 * route manifest does not need that ceiling for its other consumers.
 */
const boundedKnowledgeMutationRoutes = {
  create: {
    ...knowledgeRoutes.create,
    maximumResponseBytes: KNOWLEDGE_MANAGER_MAXIMUM_RESPONSE_BYTES,
  },
  validate: {
    ...knowledgeRoutes.validate,
    maximumResponseBytes: KNOWLEDGE_MANAGER_MAXIMUM_RESPONSE_BYTES,
  },
  update: {
    ...knowledgeRoutes.update,
    maximumResponseBytes: KNOWLEDGE_MANAGER_MAXIMUM_RESPONSE_BYTES,
  },
  setState: {
    ...knowledgeRoutes.setState,
    maximumResponseBytes: KNOWLEDGE_MANAGER_MAXIMUM_RESPONSE_BYTES,
  },
  delete: {
    ...knowledgeRoutes.delete,
    maximumResponseBytes: KNOWLEDGE_MANAGER_MAXIMUM_RESPONSE_BYTES,
  },
} as const;

export function createKnowledgeMutationClient(
  options: ProtobufTransportOptions = {},
): KnowledgeMutationClient {
  const transport = new ProtobufTransport(options);
  return {
    create: async (submitted, requestOptions) => {
      const request = knowledgeCreateRequest(submitted);
      const response = await transport.post(
        boundedKnowledgeMutationRoutes.create,
        request,
        requestOptions,
      );
      return adaptKnowledgeCreateResponse(response, request);
    },
    validate: async (submitted, requestOptions) => {
      const request = knowledgeValidateRequest(submitted);
      const { currentKnowledgeObject, ...transportOptions } = requestOptions ?? {};
      const detachedCurrentKnowledgeObject = await prepareKnowledgeCurrentObject(
        request,
        currentKnowledgeObject,
      );
      if (
        request.knowledgeObjectId !== undefined
        && detachedCurrentKnowledgeObject === undefined
      ) {
        throw new TypeError("Knowledge update validation requires its exact current object.");
      }
      const response = await transport.post(
        boundedKnowledgeMutationRoutes.validate,
        request,
        transportOptions,
      );
      return adaptKnowledgeValidationResponse(
        response,
        request,
        detachedCurrentKnowledgeObject,
      );
    },
    update: async (submitted, requestOptions) => {
      const request = knowledgeUpdateRequest(submitted);
      const { currentKnowledgeObject, ...transportOptions } = requestOptions;
      const detachedCurrentKnowledgeObject = await prepareKnowledgeCurrentObject(
        request,
        currentKnowledgeObject,
      );
      if (detachedCurrentKnowledgeObject === undefined) {
        throw new TypeError("Knowledge update current object is required.");
      }
      const response = await transport.post(
        boundedKnowledgeMutationRoutes.update,
        request,
        transportOptions,
      );
      return adaptKnowledgeUpdateResponse(response, request, detachedCurrentKnowledgeObject);
    },
    setState: async (submitted, requestOptions) => {
      const request = knowledgeSetStateRequest(submitted);
      const { currentKnowledgeObject, ...transportOptions } = requestOptions;
      const detachedCurrentKnowledgeObject = await prepareKnowledgeCurrentObject(
        request,
        currentKnowledgeObject,
      );
      if (detachedCurrentKnowledgeObject === undefined) {
        throw new TypeError("Knowledge state current object is required.");
      }
      const response = await transport.post(
        boundedKnowledgeMutationRoutes.setState,
        request,
        transportOptions,
      );
      return adaptKnowledgeSetStateResponse(
        response,
        request,
        detachedCurrentKnowledgeObject,
      );
    },
    delete: async (submitted, requestOptions) => {
      const request = knowledgeDeleteRequest(submitted);
      const response = await transport.post(
        boundedKnowledgeMutationRoutes.delete,
        request,
        requestOptions,
      );
      return adaptKnowledgeDeleteResponse(response, request);
    },
  };
}

export interface KnowledgeObjectMutationReceipt {
  knowledgeObject: KnowledgeObject;
  tenantCatalogRevision: bigint;
  tenantCatalogStateToken: Uint8Array;
}

export interface KnowledgeDeleteReceipt {
  knowledgeObjectId: string;
  deletedVersion: bigint;
  tenantCatalogRevision: bigint;
  tenantCatalogStateToken: Uint8Array;
}

export interface KnowledgeValidationReceipt {
  result: KnowledgeValidationResult;
  tenantCatalogRevision: bigint;
}

export function knowledgeCreateRequest(
  request: CreateKnowledgeObjectRequestMessage,
): CreateKnowledgeObjectRequestMessage {
  if (
    request.definition === undefined
    || (
      request.initialState !== KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DRAFT
      && request.initialState !== KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE
    )
    || !validClientRequestID(request.clientRequestId)
  ) {
    throw new TypeError("Knowledge create request is outside the browser contract.");
  }
  return cloneBoundedMutationRequest(CreateKnowledgeObjectRequest, request);
}

export function knowledgeValidateRequest(
  request: ValidateKnowledgeObjectRequestMessage,
): ValidateKnowledgeObjectRequestMessage {
  if (
    request.definition === undefined
    || (
      request.intent !== KnowledgeValidationIntent.KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE
      && request.intent
        !== KnowledgeValidationIntent.KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION
    )
  ) {
    throw new TypeError("Knowledge validation request is outside the browser contract.");
  }
  if (request.knowledgeObjectId === undefined) {
    if (
      request.expectedVersion !== undefined
      || request.updateMask !== undefined
    ) {
      throw new TypeError("Knowledge validation create authority is malformed.");
    }
  } else if (
    !validIdentity(request.knowledgeObjectId, MAXIMUM_OBJECT_ID_BYTES)
    || !validExpectedVersion(request.expectedVersion)
    || !validKnowledgeUpdateMask(request.updateMask)
  ) {
    throw new TypeError("Knowledge validation update authority is malformed.");
  }
  return cloneBoundedMutationRequest(ValidateKnowledgeObjectRequest, request);
}

export function knowledgeUpdateRequest(
  request: UpdateKnowledgeObjectRequestMessage,
): UpdateKnowledgeObjectRequestMessage {
  if (
    !validIdentity(request.knowledgeObjectId, MAXIMUM_OBJECT_ID_BYTES)
    || !validExpectedVersion(request.expectedVersion)
    || request.definition === undefined
    || !validKnowledgeUpdateMask(request.updateMask)
    || !validClientRequestID(request.clientRequestId)
  ) {
    throw new TypeError("Knowledge update request is outside the browser contract.");
  }
  return cloneBoundedMutationRequest(UpdateKnowledgeObjectRequest, request);
}

export function knowledgeSetStateRequest(
  request: SetKnowledgeObjectStateRequestMessage,
): SetKnowledgeObjectStateRequestMessage {
  if (
    !validIdentity(request.knowledgeObjectId, MAXIMUM_OBJECT_ID_BYTES)
    || !validExpectedVersion(request.expectedVersion)
    || (
      request.state !== KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE
      && request.state !== KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DISABLED
    )
    || !validClientRequestID(request.clientRequestId)
  ) {
    throw new TypeError("Knowledge state request is outside the browser contract.");
  }
  return cloneBoundedMutationRequest(SetKnowledgeObjectStateRequest, request);
}

export function knowledgeDeleteRequest(
  request: DeleteKnowledgeObjectRequestMessage,
): DeleteKnowledgeObjectRequestMessage {
  if (
    !validIdentity(request.knowledgeObjectId, MAXIMUM_OBJECT_ID_BYTES)
    || !validExpectedVersion(request.expectedVersion)
    || !validClientRequestID(request.clientRequestId)
  ) {
    throw new TypeError("Knowledge delete request is outside the browser contract.");
  }
  return cloneBoundedMutationRequest(DeleteKnowledgeObjectRequest, request);
}

export async function createKnowledgeObject(
  client: KnowledgeMutationClient,
  submitted: CreateKnowledgeObjectRequestMessage,
  options?: ProtobufRequestOptions,
): Promise<KnowledgeObjectMutationReceipt> {
  return client.create(submitted, options);
}

export async function validateKnowledgeObject(
  client: KnowledgeMutationClient,
  submitted: ValidateKnowledgeObjectRequestMessage,
  options?: KnowledgeValidationRequestOptions,
): Promise<KnowledgeValidationReceipt> {
  return client.validate(submitted, options);
}

export async function updateKnowledgeObject(
  client: KnowledgeMutationClient,
  submitted: UpdateKnowledgeObjectRequestMessage,
  options: KnowledgeCurrentObjectMutationOptions,
): Promise<KnowledgeObjectMutationReceipt> {
  return client.update(submitted, options);
}

export async function setKnowledgeObjectState(
  client: KnowledgeMutationClient,
  submitted: SetKnowledgeObjectStateRequestMessage,
  options: KnowledgeCurrentObjectMutationOptions,
): Promise<KnowledgeObjectMutationReceipt> {
  return client.setState(submitted, options);
}

export async function deleteKnowledgeObject(
  client: KnowledgeMutationClient,
  submitted: DeleteKnowledgeObjectRequestMessage,
  options?: ProtobufRequestOptions,
): Promise<KnowledgeDeleteReceipt> {
  return client.delete(submitted, options);
}

export async function adaptKnowledgeCreateResponse(
  response: CreateKnowledgeObjectResponseMessage,
  request: CreateKnowledgeObjectRequestMessage,
): Promise<KnowledgeObjectMutationReceipt> {
  const detachedRequest = knowledgeCreateRequest(request);
  const detachedResponse = cloneBoundedMutationResponse(
    CreateKnowledgeObjectResponse,
    response,
  );
  const receipt = await adaptKnowledgeObjectMutationReceipt(detachedResponse);
  if (
    receipt.knowledgeObject.version !== 1n
    || receipt.knowledgeObject.state !== detachedRequest.initialState
    || detachedRequest.definition === undefined
    || receipt.knowledgeObject.definition === undefined
    || !definitionMatchesNormalizedSubmission(
      receipt.knowledgeObject.definition,
      detachedRequest.definition,
    )
    || receipt.knowledgeObject.createdAt?.valueOf()
      !== receipt.knowledgeObject.updatedAt?.valueOf()
  ) {
    throw new TypeError("Knowledge create response disagrees with its request.");
  }
  return receipt;
}

export async function adaptKnowledgeUpdateResponse(
  response: UpdateKnowledgeObjectResponseMessage,
  request: UpdateKnowledgeObjectRequestMessage,
  currentKnowledgeObject: KnowledgeObject,
): Promise<KnowledgeObjectMutationReceipt> {
  const detachedRequest = knowledgeUpdateRequest(request);
  const detachedResponse = cloneBoundedMutationResponse(
    UpdateKnowledgeObjectResponse,
    response,
  );
  const [receipt, detachedCurrentKnowledgeObject] = await Promise.all([
    adaptKnowledgeObjectMutationReceipt(detachedResponse),
    prepareKnowledgeCurrentObject(detachedRequest, currentKnowledgeObject),
  ]);
  if (detachedCurrentKnowledgeObject === undefined) {
    throw new TypeError("Knowledge update current object is required.");
  }
  const expectedDefinition = normalizedAppliedUpdateDefinition(
    detachedCurrentKnowledgeObject.definition,
    detachedRequest.definition,
    detachedRequest.updateMask,
  );
  if (
    receipt.knowledgeObject.knowledgeObjectId !== detachedRequest.knowledgeObjectId
    || receipt.knowledgeObject.version !== detachedRequest.expectedVersion + 1n
    || receipt.knowledgeObject.state !== detachedCurrentKnowledgeObject.state
    || (
      detachedCurrentKnowledgeObject.state !== KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DRAFT
      && detachedCurrentKnowledgeObject.state
        !== KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE
      && detachedCurrentKnowledgeObject.state
        !== KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DISABLED
    )
    || receipt.knowledgeObject.definition === undefined
    || expectedDefinition === null
    || !sameKnowledgeDefinition(
      receipt.knowledgeObject.definition,
      expectedDefinition,
    )
    || !mutationResultRetainsCurrentIdentity(
      receipt.knowledgeObject,
      detachedCurrentKnowledgeObject,
    )
    || !sameOptionalDate(
      receipt.knowledgeObject.disabledAt,
      detachedCurrentKnowledgeObject.disabledAt,
    )
  ) {
    throw new TypeError("Knowledge update response disagrees with its request.");
  }
  return receipt;
}

export async function adaptKnowledgeSetStateResponse(
  response: SetKnowledgeObjectStateResponseMessage,
  request: SetKnowledgeObjectStateRequestMessage,
  currentKnowledgeObject: KnowledgeObject,
): Promise<KnowledgeObjectMutationReceipt> {
  const detachedRequest = knowledgeSetStateRequest(request);
  const detachedResponse = cloneBoundedMutationResponse(
    SetKnowledgeObjectStateResponse,
    response,
  );
  const [receipt, detachedCurrentKnowledgeObject] = await Promise.all([
    adaptKnowledgeObjectMutationReceipt(detachedResponse),
    prepareKnowledgeCurrentObject(detachedRequest, currentKnowledgeObject),
  ]);
  if (detachedCurrentKnowledgeObject === undefined) {
    throw new TypeError("Knowledge state current object is required.");
  }
  if (
    receipt.knowledgeObject.knowledgeObjectId !== detachedRequest.knowledgeObjectId
    || receipt.knowledgeObject.version !== detachedRequest.expectedVersion + 1n
    || receipt.knowledgeObject.state !== detachedRequest.state
    || receipt.knowledgeObject.definition === undefined
    || detachedCurrentKnowledgeObject.definition === undefined
    || !sameKnowledgeDefinition(
      receipt.knowledgeObject.definition,
      detachedCurrentKnowledgeObject.definition,
    )
    || !mutationResultRetainsCurrentIdentity(
      receipt.knowledgeObject,
      detachedCurrentKnowledgeObject,
    )
    || !validKnowledgeStateTransition(
      detachedCurrentKnowledgeObject.state,
      detachedRequest.state,
    )
    || (
      detachedRequest.state === KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DISABLED
      && receipt.knowledgeObject.disabledAt?.valueOf()
        !== receipt.knowledgeObject.updatedAt?.valueOf()
    )
  ) {
    throw new TypeError("Knowledge state response disagrees with its request.");
  }
  return receipt;
}

export function adaptKnowledgeDeleteResponse(
  response: DeleteKnowledgeObjectResponseMessage,
  request: DeleteKnowledgeObjectRequestMessage,
): KnowledgeDeleteReceipt {
  const detachedRequest = knowledgeDeleteRequest(request);
  const detachedResponse = cloneBoundedMutationResponse(
    DeleteKnowledgeObjectResponse,
    response,
  );
  if (
    detachedResponse.knowledgeObjectId !== detachedRequest.knowledgeObjectId
    || detachedResponse.deletedVersion !== detachedRequest.expectedVersion + 1n
    || !validCatalogSnapshot(
      detachedResponse.tenantCatalogRevision,
      detachedResponse.tenantCatalogStateToken,
      detachedResponse.deletedVersion,
    )
  ) {
    throw new TypeError("Knowledge delete response disagrees with its request.");
  }
  return {
    knowledgeObjectId: `${detachedResponse.knowledgeObjectId}`,
    deletedVersion: detachedResponse.deletedVersion,
    tenantCatalogRevision: detachedResponse.tenantCatalogRevision,
    tenantCatalogStateToken: Uint8Array.from(detachedResponse.tenantCatalogStateToken),
  };
}

export async function adaptKnowledgeValidationResponse(
  response: ValidateKnowledgeObjectResponseMessage,
  request: ValidateKnowledgeObjectRequestMessage,
  currentKnowledgeObject?: KnowledgeObject,
): Promise<KnowledgeValidationReceipt> {
  const detachedRequest = knowledgeValidateRequest(request);
  const detached = cloneBoundedMutationResponse(
    ValidateKnowledgeObjectResponse,
    response,
  );
  const detachedCurrentKnowledgeObject = currentKnowledgeObject === undefined
    ? undefined
    : cloneBoundedMutationResponse(KnowledgeObject, currentKnowledgeObject);
  const preparedCurrentKnowledgeObject = await prepareKnowledgeCurrentObject(
    detachedRequest,
    detachedCurrentKnowledgeObject,
  );
  if (
    !validKnowledgeValidationUpdateAuthority(
      detachedRequest,
      preparedCurrentKnowledgeObject,
    )
  ) {
    throw new TypeError("Knowledge update validation authority is outside the browser contract.");
  }
  if (
    detached.result === undefined
    || typeof detached.tenantCatalogRevision !== "bigint"
    || detached.tenantCatalogRevision < 0n
    || detached.tenantCatalogRevision > MAXIMUM_SIGNED_REVISION
    || !validKnowledgeValidationResult(
      detached.result,
      detachedRequest,
      detached.tenantCatalogRevision,
      preparedCurrentKnowledgeObject,
    )
  ) {
    throw new TypeError("Knowledge validation response is outside the browser contract.");
  }
  if (
    detached.result.valid
      && !await definitionDigestMatches(
        detached.result.normalizedDefinition,
        detached.result.definitionSha256,
      )
  ) {
    throw new TypeError("Knowledge validation result could not be detached.");
  }
  return {
    result: detached.result,
    tenantCatalogRevision: detached.tenantCatalogRevision,
  };
}

export type KnowledgeDefinitionStatus = "recognized" | "opaque" | "withheld";

export interface KnowledgeSelectorDisplay {
  dimension: "Index" | "Host" | "Source" | "Sourcetype";
  patterns: string[];
}

export interface KnowledgeDefinitionDisplay {
  status: KnowledgeDefinitionStatus;
  description: string | null;
  descriptionTruncated: boolean;
  selectors: KnowledgeSelectorDisplay[];
  bodyKind: string;
  bodyFields: Array<{ label: string; value: string }>;
  overwriteBehavior: string | null;
}

export interface KnowledgeObjectDisplay {
  disclosure: "available";
  key: string;
  knowledgeObjectId: string;
  name: string;
  appId: string;
  ownerId: string;
  objectType: KnowledgeObjectType;
  objectTypeLabel: string;
  state: KnowledgeObjectState;
  stateLabel: string;
  sharingScope: SharingScope;
  sharingScopeLabel: string;
  version: bigint;
  createdAt: Date;
  updatedAt: Date;
  definition: KnowledgeDefinitionDisplay;
}

export interface RedactedKnowledgeObjectDisplay {
  disclosure: "redacted";
  key: string;
  name: "Unavailable object";
  objectTypeLabel: "Unknown type";
  stateLabel: "Unavailable";
  sharingScopeLabel: "Unavailable";
}

export type KnowledgeListItem = KnowledgeObjectDisplay | RedactedKnowledgeObjectDisplay;

export interface KnowledgePageDisplay {
  objects: KnowledgeListItem[];
  nextPageToken: string | null;
  totalSize: bigint;
  tenantCatalogRevision: bigint;
}

export type KnowledgePageLoadResult =
  | { status: "available"; page: KnowledgePageDisplay }
  | { status: "unavailable" };

export type KnowledgeDetailLoadResult =
  | { status: "available"; object: KnowledgeObjectDisplay }
  | { status: "unavailable" };

export type KnowledgeMutationDetailLoadResult =
  | {
    status: "available";
    object: KnowledgeObjectDisplay;
    /** Exact detached authority required by Update, Validate-update, and SetState. */
    currentKnowledgeObject: KnowledgeObject;
  }
  | { status: "unavailable" };

export interface KnowledgeDetailQuery {
  knowledgeObjectId: string;
  version: bigint;
}

export type KnowledgeRelationshipDirection = "dependencies" | "dependents";

export interface KnowledgeRelationshipEdgeDisplay {
  key: string;
  knowledgeObjectId: string;
  version: bigint;
  roleLabel: "Field input";
}

export interface KnowledgeRelationshipPageDisplay {
  direction: KnowledgeRelationshipDirection;
  resolvedObject: {
    knowledgeObjectId: string;
    version: bigint;
  };
  edges: KnowledgeRelationshipEdgeDisplay[];
  nextPageToken: string | null;
  totalSize: bigint;
  tenantCatalogRevision: bigint;
}

export interface KnowledgeRelationshipQuery {
  direction: KnowledgeRelationshipDirection;
  knowledgeObjectId: string;
  version: bigint;
  pageSize: number;
  pageToken: string | null;
}

export type KnowledgeRelationshipPageLoadResult =
  | { status: "available"; page: KnowledgeRelationshipPageDisplay }
  | { status: "unavailable" };

export type KnowledgeObjectTypeFilter =
  | "all"
  | "field-extraction"
  | "field-alias"
  | "calculated-field";

export const KNOWLEDGE_OBJECT_TYPE_FILTER_OPTIONS = [
  { value: "all", label: "All object types" },
  { value: "field-extraction", label: "Field extraction" },
  { value: "field-alias", label: "Field alias" },
  { value: "calculated-field", label: "Calculated field" },
] as const satisfies ReadonlyArray<{ value: KnowledgeObjectTypeFilter; label: string }>;

export type KnowledgeLifecycleStateFilter =
  | "all"
  | "draft"
  | "active"
  | "disabled"
  | "quarantined"
  | "deleted";

export const KNOWLEDGE_LIFECYCLE_STATE_FILTER_OPTIONS = [
  { value: "all", label: "All lifecycle states" },
  { value: "draft", label: "Draft" },
  { value: "active", label: "Active" },
  { value: "disabled", label: "Disabled" },
  { value: "quarantined", label: "Quarantined" },
  { value: "deleted", label: "Deleted" },
] as const satisfies ReadonlyArray<{ value: KnowledgeLifecycleStateFilter; label: string }>;

export type KnowledgeSharingScopeFilter = "all" | "private" | "app" | "global";

export const KNOWLEDGE_SHARING_SCOPE_FILTER_OPTIONS = [
  { value: "all", label: "All sharing scopes" },
  { value: "private", label: "Private" },
  { value: "app", label: "App" },
  { value: "global", label: "Global" },
] as const satisfies ReadonlyArray<{ value: KnowledgeSharingScopeFilter; label: string }>;

export type KnowledgeSortChoice =
  | "name-ascending"
  | "updated-descending"
  | "created-descending"
  | "object-type-ascending";

export const KNOWLEDGE_SORT_OPTIONS = [
  { value: "name-ascending", label: "Name A–Z" },
  { value: "updated-descending", label: "Updated newest" },
  { value: "created-descending", label: "Created newest" },
  { value: "object-type-ascending", label: "Type A–Z" },
] as const satisfies ReadonlyArray<{ value: KnowledgeSortChoice; label: string }>;

export interface KnowledgeListQuery {
  appId: string | null;
  ownerId: string | null;
  text: string | null;
  objectType: KnowledgeObjectTypeFilter;
  lifecycleState: KnowledgeLifecycleStateFilter;
  sharingScope: KnowledgeSharingScopeFilter;
  selectorText: string | null;
  sort: KnowledgeSortChoice;
  pageSize: number;
  pageToken: string | null;
}

export function knowledgeObjectTypeFilterFromControlValue(
  value: string,
): KnowledgeObjectTypeFilter | undefined {
  switch (value) {
    case "all":
    case "field-extraction":
    case "field-alias":
    case "calculated-field":
      return value;
    default:
      return undefined;
  }
}

export function knowledgeLifecycleStateFilterFromControlValue(
  value: string,
): KnowledgeLifecycleStateFilter | undefined {
  switch (value) {
    case "all":
    case "draft":
    case "active":
    case "disabled":
    case "quarantined":
    case "deleted":
      return value;
    default:
      return undefined;
  }
}

export function knowledgeSharingScopeFilterFromControlValue(
  value: string,
): KnowledgeSharingScopeFilter | undefined {
  switch (value) {
    case "all":
    case "private":
    case "app":
    case "global":
      return value;
    default:
      return undefined;
  }
}

/** Trims only the protocol-pinned ASCII edge whitespace before commit. */
export function knowledgeTextFilterFromDraft(
  value: string,
): string | null | undefined {
  const trimmed = trimPinnedASCIIWhitespace(value);
  if (trimmed.length === 0) return null;
  return validCommittedKnowledgeFilter(trimmed) ? trimmed : undefined;
}

export function knowledgeSortChoiceFromControlValue(
  value: string,
): KnowledgeSortChoice | undefined {
  switch (value) {
    case "name-ascending":
    case "updated-descending":
    case "created-descending":
    case "object-type-ascending":
      return value;
    default:
      return undefined;
  }
}

export function boundedKnowledgePageSize(configuredMaximum: number): number {
  if (!Number.isSafeInteger(configuredMaximum) || configuredMaximum <= 0) {
    return KNOWLEDGE_MANAGER_DEFAULT_PAGE_SIZE;
  }
  return Math.min(
    configuredMaximum,
    KNOWLEDGE_MANAGER_DEFAULT_PAGE_SIZE,
    KNOWLEDGE_MANAGER_MAXIMUM_PAGE_SIZE,
  );
}

export function knowledgeListRequest(query: KnowledgeListQuery) {
  if (
    !Number.isSafeInteger(query.pageSize)
    || query.pageSize < 1
    || query.pageSize > KNOWLEDGE_MANAGER_MAXIMUM_PAGE_SIZE
  ) {
    throw new RangeError("Knowledge page size is outside the browser contract.");
  }
  if (query.appId !== null && !validIdentity(query.appId, MAXIMUM_APP_ID_BYTES)) {
    throw new TypeError("Knowledge app filter is outside the browser contract.");
  }
  if (
    (query.ownerId !== null && !validCommittedKnowledgeFilter(query.ownerId))
    || (query.text !== null && !validCommittedKnowledgeFilter(query.text))
    || (query.selectorText !== null && !validCommittedKnowledgeFilter(query.selectorText))
  ) {
    throw new TypeError("Knowledge text filter is outside the browser contract.");
  }
  if (
    query.pageToken !== null
    && !validOpaqueToken(query.pageToken, KNOWLEDGE_MANAGER_MAXIMUM_PAGE_TOKEN_BYTES)
  ) {
    throw new TypeError("Knowledge page token is outside the browser contract.");
  }
  const objectTypeFilters = canonicalKnowledgeObjectTypeFilter(query.objectType);
  const stateFilters = canonicalKnowledgeLifecycleStateFilter(query.lifecycleState);
  const sharingScopeFilters = canonicalKnowledgeSharingScopeFilter(query.sharingScope);
  const sort = canonicalKnowledgeSort(query.sort);
  return ListKnowledgeObjectsRequest.fromPartial({
    page: {
      pageSize: query.pageSize,
      pageToken: query.pageToken ?? undefined,
      includeTotalSize: true,
    },
    appIdFilter: query.appId ?? undefined,
    ownerIdFilter: query.ownerId ?? undefined,
    textFilter: query.text ?? undefined,
    objectTypeFilters,
    stateFilters,
    sharingScopeFilters,
    selectorTextFilter: query.selectorText ?? undefined,
    sortBy: sort.sortBy,
    sortDirection: sort.sortDirection,
  });
}

function canonicalKnowledgeSharingScopeFilter(
  value: KnowledgeSharingScopeFilter,
): SharingScope[] {
  switch (value) {
    case "all":
      return [];
    case "private":
      return [SharingScope.SHARING_SCOPE_PRIVATE];
    case "app":
      return [SharingScope.SHARING_SCOPE_APP];
    case "global":
      return [SharingScope.SHARING_SCOPE_GLOBAL];
    default:
      throw new TypeError("Knowledge sharing scope filter is outside the browser contract.");
  }
}

function canonicalKnowledgeObjectTypeFilter(
  value: KnowledgeObjectTypeFilter,
): KnowledgeObjectType[] {
  switch (value) {
    case "all":
      return [];
    case "field-extraction":
      return [KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION];
    case "field-alias":
      return [KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS];
    case "calculated-field":
      return [KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD];
    default:
      throw new TypeError("Knowledge object type filter is outside the browser contract.");
  }
}

function canonicalKnowledgeLifecycleStateFilter(
  value: KnowledgeLifecycleStateFilter,
): KnowledgeObjectState[] {
  switch (value) {
    case "all":
      return [];
    case "draft":
      return [KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DRAFT];
    case "active":
      return [KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE];
    case "disabled":
      return [KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DISABLED];
    case "quarantined":
      return [KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_QUARANTINED];
    case "deleted":
      return [KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DELETED];
    default:
      throw new TypeError("Knowledge lifecycle state filter is outside the browser contract.");
  }
}

function canonicalKnowledgeSort(value: KnowledgeSortChoice): {
  sortBy: KnowledgeObjectSortBy;
  sortDirection: SortDirection;
} {
  switch (value) {
    case "name-ascending":
      return {
        sortBy: KnowledgeObjectSortBy.KNOWLEDGE_OBJECT_SORT_BY_NAME,
        sortDirection: SortDirection.SORT_DIRECTION_ASCENDING,
      };
    case "updated-descending":
      return {
        sortBy: KnowledgeObjectSortBy.KNOWLEDGE_OBJECT_SORT_BY_UPDATED_AT,
        sortDirection: SortDirection.SORT_DIRECTION_DESCENDING,
      };
    case "created-descending":
      return {
        sortBy: KnowledgeObjectSortBy.KNOWLEDGE_OBJECT_SORT_BY_CREATED_AT,
        sortDirection: SortDirection.SORT_DIRECTION_DESCENDING,
      };
    case "object-type-ascending":
      return {
        sortBy: KnowledgeObjectSortBy.KNOWLEDGE_OBJECT_SORT_BY_OBJECT_TYPE,
        sortDirection: SortDirection.SORT_DIRECTION_ASCENDING,
      };
    default:
      throw new TypeError("Knowledge sort choice is outside the browser contract.");
  }
}

export async function loadKnowledgePage(
  client: KnowledgeReadClient,
  query: KnowledgeListQuery,
  options?: ProtobufRequestOptions,
): Promise<KnowledgePageLoadResult> {
  try {
    const response = await client.list(knowledgeListRequest(query), options);
    return {
      status: "available",
      page: adaptKnowledgePage(response, query),
    };
  } catch {
    // The hidden shell never reflects route, catalog, or decoder detail. This
    // also keeps 404/405/501 and dependency failures observationally uniform.
    return { status: "unavailable" };
  }
}

export function knowledgeDetailRequest(query: KnowledgeDetailQuery) {
  if (
    !validIdentity(query.knowledgeObjectId, MAXIMUM_OBJECT_ID_BYTES)
    || typeof query.version !== "bigint"
    || query.version < 1n
    || query.version > MAXIMUM_SIGNED_REVISION
  ) {
    throw new TypeError("Knowledge detail query is outside the browser contract.");
  }
  return GetKnowledgeObjectRequest.fromPartial({
    knowledgeObjectId: query.knowledgeObjectId,
    version: query.version,
  });
}

export async function loadKnowledgeDetail(
  client: KnowledgeReadClient,
  query: KnowledgeDetailQuery,
  options?: ProtobufRequestOptions,
): Promise<KnowledgeDetailLoadResult> {
  try {
    const request = knowledgeDetailRequest(query);
    const response = await client.get(request, options);
    if (response.knowledgeObject === undefined) return { status: "unavailable" };
    const object = adaptKnowledgeObject(response.knowledgeObject, 0);
    if (
      object.disclosure !== "available"
      || object.knowledgeObjectId !== request.knowledgeObjectId
      || object.version !== request.version
    ) {
      return { status: "unavailable" };
    }
    return { status: "available", object };
  } catch {
    return { status: "unavailable" };
  }
}

/**
 * Reads one exact version for the mutation surface without weakening the
 * display-only projection used elsewhere. The returned protobuf object is
 * detached and its canonical definition digest is verified before exposure.
 */
export async function loadKnowledgeMutationDetail(
  client: KnowledgeReadClient,
  query: KnowledgeDetailQuery,
  options?: ProtobufRequestOptions,
): Promise<KnowledgeMutationDetailLoadResult> {
  try {
    const request = knowledgeDetailRequest(query);
    const response = await client.get(request, options);
    if (response.knowledgeObject === undefined) return { status: "unavailable" };
    const currentKnowledgeObject = await prepareKnowledgeCurrentObject({
      knowledgeObjectId: request.knowledgeObjectId,
      expectedVersion: request.version,
    }, response.knowledgeObject);
    if (currentKnowledgeObject === undefined) return { status: "unavailable" };
    const object = adaptKnowledgeObject(currentKnowledgeObject, 0);
    if (
      object.disclosure !== "available"
      || object.knowledgeObjectId !== request.knowledgeObjectId
      || object.version !== request.version
    ) {
      return { status: "unavailable" };
    }
    return { status: "available", object, currentKnowledgeObject };
  } catch {
    return { status: "unavailable" };
  }
}

function assertKnowledgeRelationshipQuery(
  query: KnowledgeRelationshipQuery,
): void {
  if (
    (query.direction !== "dependencies" && query.direction !== "dependents")
    || !validIdentity(query.knowledgeObjectId, MAXIMUM_OBJECT_ID_BYTES)
    || typeof query.version !== "bigint"
    || query.version < 1n
    || query.version > MAXIMUM_SIGNED_REVISION
    || !Number.isSafeInteger(query.pageSize)
    || query.pageSize < 1
    || query.pageSize > KNOWLEDGE_MANAGER_MAXIMUM_PAGE_SIZE
    || (
      query.pageToken !== null
      && !validOpaqueToken(
        query.pageToken,
        KNOWLEDGE_MANAGER_MAXIMUM_PAGE_TOKEN_BYTES,
      )
    )
  ) {
    throw new TypeError("Knowledge relationship query is outside the browser contract.");
  }
}

export function knowledgeRelationshipRequest(
  query: KnowledgeRelationshipQuery,
) {
  assertKnowledgeRelationshipQuery(query);
  const value = {
    knowledgeObjectId: query.knowledgeObjectId,
    version: query.version,
    page: {
      pageSize: query.pageSize,
      pageToken: query.pageToken ?? undefined,
      includeTotalSize: true,
    },
  };
  return query.direction === "dependencies"
    ? ListKnowledgeObjectDependenciesRequest.fromPartial(value)
    : ListKnowledgeObjectDependentsRequest.fromPartial(value);
}

export async function loadKnowledgeRelationshipPage(
  client: KnowledgeReadClient,
  query: KnowledgeRelationshipQuery,
  options?: ProtobufRequestOptions,
): Promise<KnowledgeRelationshipPageLoadResult> {
  try {
    const request = knowledgeRelationshipRequest(query);
    const response = query.direction === "dependencies"
      ? await client.dependencies(request, options)
      : await client.dependents(request, options);
    return {
      status: "available",
      page: adaptKnowledgeRelationshipPage(response, query),
    };
  } catch {
    // Missing, forbidden, stale, malformed, and unavailable graph reads are
    // intentionally indistinguishable in the dormant administrator surface.
    return { status: "unavailable" };
  }
}

export function adaptKnowledgeRelationshipPage(
  response:
    | ListKnowledgeObjectDependenciesResponseMessage
    | ListKnowledgeObjectDependentsResponseMessage,
  query: KnowledgeRelationshipQuery,
): KnowledgeRelationshipPageDisplay {
  assertKnowledgeRelationshipQuery(query);
  const edges = query.direction === "dependencies"
    ? (response as ListKnowledgeObjectDependenciesResponseMessage).dependencies
    : (response as ListKnowledgeObjectDependentsResponseMessage).dependents;
  if (!Array.isArray(edges) || edges.length > query.pageSize || response.page === undefined) {
    throw new TypeError("Knowledge relationship response has an invalid page shape.");
  }

  const revision = response.tenantCatalogRevision;
  const resolvedObject = response.resolvedObject;
  if (
    typeof revision !== "bigint"
    || revision < 1n
    || revision > MAXIMUM_SIGNED_REVISION
    || !validKnowledgeRelationshipIdentity(resolvedObject, revision)
    || resolvedObject.knowledgeObjectId !== query.knowledgeObjectId
    || resolvedObject.version !== query.version
  ) {
    throw new TypeError("Knowledge relationship response has an invalid root.");
  }

  const nextPageToken = response.page.nextPageToken?.length
    ? response.page.nextPageToken
    : null;
  if (
    nextPageToken !== null
    && (
      edges.length === 0
      || (query.direction === "dependencies" && edges.length !== query.pageSize)
      || !validOpaqueToken(
        nextPageToken,
        KNOWLEDGE_MANAGER_MAXIMUM_PAGE_TOKEN_BYTES,
      )
      || nextPageToken === query.pageToken
    )
  ) {
    throw new TypeError("Knowledge relationship response has an invalid continuation.");
  }
  if (query.pageToken !== null && edges.length === 0) {
    throw new TypeError("Knowledge relationship continuation made no progress.");
  }

  const maximumTotal = query.direction === "dependencies"
    ? KNOWLEDGE_MANAGER_MAXIMUM_DEPENDENCIES
    : KNOWLEDGE_MANAGER_MAXIMUM_DEPENDENTS;
  const totalSize = response.page.totalSize;
  const minimumTotal = BigInt(edges.length)
    + (query.pageToken === null ? 0n : 1n)
    + (nextPageToken === null ? 0n : 1n);
  if (
    typeof totalSize !== "bigint"
    || !response.page.totalSizeExact
    || totalSize < minimumTotal
    || totalSize > maximumTotal
    || (
      query.pageToken === null
      && nextPageToken === null
      && totalSize !== BigInt(edges.length)
    )
  ) {
    throw new TypeError("Knowledge relationship response has an invalid total.");
  }

  const adaptedEdges: KnowledgeRelationshipEdgeDisplay[] = [];
  let previous: KnowledgeRelationshipEdgeDisplay | undefined;
  for (const edge of edges) {
    const adapted = adaptKnowledgeRelationshipEdge(
      edge,
      query.direction,
      resolvedObject,
      revision,
    );
    if (
      previous !== undefined
      && !knowledgeRelationshipEdgeFollows(previous, adapted, query.direction)
    ) {
      throw new TypeError("Knowledge relationship response has invalid edge order.");
    }
    adaptedEdges.push(adapted);
    previous = adapted;
  }

  return {
    direction: query.direction,
    resolvedObject: {
      knowledgeObjectId: `${resolvedObject.knowledgeObjectId}`,
      version: resolvedObject.version,
    },
    edges: adaptedEdges,
    nextPageToken,
    totalSize,
    tenantCatalogRevision: revision,
  };
}

function adaptKnowledgeRelationshipEdge(
  edge: KnowledgeManagementDependencyEdge,
  direction: KnowledgeRelationshipDirection,
  resolvedObject: KnowledgeManagementObjectVersionIdentity,
  revision: bigint,
): KnowledgeRelationshipEdgeDisplay {
  if (
    edge.role !== KnowledgeDependencyRole.KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT
    || !validKnowledgeRelationshipIdentity(edge.source, revision)
    || !validKnowledgeRelationshipIdentity(edge.target, revision)
    || sameKnowledgeRelationshipIdentity(edge.source, edge.target)
  ) {
    throw new TypeError("Knowledge relationship response has an invalid edge.");
  }
  const fixed = direction === "dependencies" ? edge.source : edge.target;
  const opposite = direction === "dependencies" ? edge.target : edge.source;
  if (!sameKnowledgeRelationshipIdentity(fixed, resolvedObject)) {
    throw new TypeError("Knowledge relationship edge disagrees with its root.");
  }
  return {
    key: `${direction}:${opposite.knowledgeObjectId}\0${opposite.version.toString()}\0field-input`,
    knowledgeObjectId: `${opposite.knowledgeObjectId}`,
    version: opposite.version,
    roleLabel: "Field input",
  };
}

function validKnowledgeRelationshipIdentity(
  identity: KnowledgeManagementObjectVersionIdentity | undefined,
  revision: bigint,
): identity is KnowledgeManagementObjectVersionIdentity {
  return identity !== undefined
    && validIdentity(identity.knowledgeObjectId, MAXIMUM_OBJECT_ID_BYTES)
    && typeof identity.version === "bigint"
    && identity.version >= 1n
    && identity.version <= revision;
}

function sameKnowledgeRelationshipIdentity(
  left: KnowledgeManagementObjectVersionIdentity,
  right: KnowledgeManagementObjectVersionIdentity,
): boolean {
  return left.knowledgeObjectId === right.knowledgeObjectId
    && left.version === right.version;
}

function knowledgeRelationshipEdgeFollows(
  previous: KnowledgeRelationshipEdgeDisplay,
  current: KnowledgeRelationshipEdgeDisplay,
  direction: KnowledgeRelationshipDirection,
): boolean {
  const identityOrder = compareUTF8Binary(
    previous.knowledgeObjectId,
    current.knowledgeObjectId,
  );
  if (direction === "dependents") return identityOrder < 0;
  if (identityOrder !== 0) return identityOrder < 0;
  return previous.version < current.version;
}

function compareUTF8Binary(left: string, right: string): number {
  const encoder = new TextEncoder();
  const leftBytes = encoder.encode(left);
  const rightBytes = encoder.encode(right);
  const shared = Math.min(leftBytes.length, rightBytes.length);
  for (let index = 0; index < shared; index += 1) {
    const difference = (leftBytes[index] ?? 0) - (rightBytes[index] ?? 0);
    if (difference !== 0) return difference;
  }
  return leftBytes.length - rightBytes.length;
}

export function mergeKnowledgeRelationshipContinuation(
  current: KnowledgeRelationshipPageDisplay,
  continuation: KnowledgeRelationshipPageDisplay,
  requestedPageToken: string,
  consumedPageTokens: ReadonlySet<string>,
): { status: "merged"; page: KnowledgeRelationshipPageDisplay } | { status: "stale" } {
  if (
    !validOpaqueToken(requestedPageToken, KNOWLEDGE_MANAGER_MAXIMUM_PAGE_TOKEN_BYTES)
    || consumedPageTokens.has(requestedPageToken)
    || current.nextPageToken !== requestedPageToken
    || continuation.direction !== current.direction
    || !sameKnowledgeRelationshipIdentity(
      continuation.resolvedObject,
      current.resolvedObject,
    )
    || continuation.tenantCatalogRevision !== current.tenantCatalogRevision
    || continuation.totalSize !== current.totalSize
    || (
      continuation.nextPageToken !== null
      && (
        continuation.nextPageToken === requestedPageToken
        || consumedPageTokens.has(continuation.nextPageToken)
      )
    )
  ) {
    return { status: "stale" };
  }

  const lastCurrent = current.edges.at(-1);
  const firstContinuation = continuation.edges[0];
  if (
    lastCurrent !== undefined
    && firstContinuation !== undefined
    && !knowledgeRelationshipEdgeFollows(
      lastCurrent,
      firstContinuation,
      current.direction,
    )
  ) {
    return { status: "stale" };
  }
  const seen = new Set(current.edges.map((edge) => edge.key));
  for (const edge of continuation.edges) {
    if (seen.has(edge.key)) return { status: "stale" };
    seen.add(edge.key);
  }
  const edges = [...current.edges, ...continuation.edges];
  const exactTotalReached = BigInt(edges.length) === current.totalSize;
  if (
    BigInt(edges.length) > current.totalSize
    || (continuation.nextPageToken === null) !== exactTotalReached
  ) {
    return { status: "stale" };
  }
  return {
    status: "merged",
    page: {
      direction: current.direction,
      resolvedObject: { ...current.resolvedObject },
      edges,
      nextPageToken: continuation.nextPageToken,
      totalSize: current.totalSize,
      tenantCatalogRevision: current.tenantCatalogRevision,
    },
  };
}

export function adaptKnowledgePage(
  response: ListKnowledgeObjectsResponseMessage,
  query: KnowledgeListQuery,
): KnowledgePageDisplay {
  const pageSize = query.pageSize;
  if (
    !Number.isSafeInteger(pageSize)
    || pageSize < 1
    || pageSize > KNOWLEDGE_MANAGER_MAXIMUM_PAGE_SIZE
    || response.knowledgeObjects.length > pageSize
    || response.knowledgeObjects.length > KNOWLEDGE_MANAGER_MAXIMUM_PAGE_SIZE
    || response.page === undefined
  ) {
    throw new TypeError("Knowledge list response has an invalid page shape.");
  }

  const nextPageToken = response.page.nextPageToken?.length
    ? response.page.nextPageToken
    : null;
  if (
    nextPageToken !== null
    && (
      response.knowledgeObjects.length === 0
      || !validOpaqueToken(nextPageToken, KNOWLEDGE_MANAGER_MAXIMUM_PAGE_TOKEN_BYTES)
      || nextPageToken === query.pageToken
    )
  ) {
    throw new TypeError("Knowledge list response has an invalid continuation.");
  }
  if (query.pageToken !== null && response.knowledgeObjects.length === 0) {
    throw new TypeError("Knowledge continuation ended with an empty page.");
  }
  const totalSize = response.page.totalSize;
  const minimumTotal = BigInt(response.knowledgeObjects.length)
    + (query.pageToken === null ? 0n : 1n)
    + (nextPageToken === null ? 0n : 1n);
  if (
    totalSize === undefined
    || !response.page.totalSizeExact
    || totalSize < minimumTotal
    || totalSize > KNOWLEDGE_MANAGER_MAXIMUM_OBJECTS
    || (query.pageToken === null && nextPageToken === null
      && totalSize !== BigInt(response.knowledgeObjects.length))
    || response.tenantCatalogRevision < 0n
    || response.tenantCatalogRevision > MAXIMUM_SIGNED_REVISION
    || (
      (response.knowledgeObjects.length > 0
        || query.pageToken !== null
        || nextPageToken !== null)
      && response.tenantCatalogRevision === 0n
    )
  ) {
    throw new TypeError("Knowledge list response has invalid snapshot metadata.");
  }

  let responseCharge = 64;
  responseCharge = addBoundedCharge(responseCharge, utf8ByteLength(nextPageToken ?? ""));
  const objects = response.knowledgeObjects.map((object, index) => {
    responseCharge = addBoundedCharge(responseCharge, preflightKnowledgeObjectCharge(object));
    return adaptKnowledgeObject(object, index);
  });
  if (responseCharge > KNOWLEDGE_MANAGER_MAXIMUM_RESPONSE_BYTES) {
    throw new TypeError("Knowledge list response exceeds its browser envelope.");
  }

  const seen = new Set<string>();
  for (const object of objects) {
    if (object.disclosure !== "available") continue;
    if (object.version > response.tenantCatalogRevision) {
      throw new TypeError("Knowledge object version exceeds the catalog revision.");
    }
    if (seen.has(object.knowledgeObjectId)) {
      throw new TypeError("Knowledge list response repeated an object identity.");
    }
    seen.add(object.knowledgeObjectId);
  }

  return {
    objects,
    nextPageToken,
    totalSize,
    tenantCatalogRevision: response.tenantCatalogRevision,
  };
}

export function mergeKnowledgeContinuation(
  current: KnowledgePageDisplay,
  continuation: KnowledgePageDisplay,
  requestedPageToken: string,
  consumedPageTokens: ReadonlySet<string>,
): { status: "merged"; page: KnowledgePageDisplay } | { status: "stale" } {
  if (
    !validOpaqueToken(requestedPageToken, KNOWLEDGE_MANAGER_MAXIMUM_PAGE_TOKEN_BYTES)
    || consumedPageTokens.has(requestedPageToken)
    || current.nextPageToken !== requestedPageToken
    || continuation.tenantCatalogRevision !== current.tenantCatalogRevision
    || continuation.totalSize !== current.totalSize
    || (
      continuation.nextPageToken !== null
      && (continuation.nextPageToken === requestedPageToken
        || consumedPageTokens.has(continuation.nextPageToken))
    )
  ) {
    return { status: "stale" };
  }
  const seen = new Set(
    current.objects.flatMap((object) => object.disclosure === "available"
      ? [object.knowledgeObjectId]
      : []),
  );
  for (const object of continuation.objects) {
    if (object.disclosure === "available" && seen.has(object.knowledgeObjectId)) {
      return { status: "stale" };
    }
    if (object.disclosure === "available") seen.add(object.knowledgeObjectId);
  }
  const continuationObjects = continuation.objects.map((object, index) =>
    object.disclosure === "redacted"
      ? { ...object, key: `redacted-${current.objects.length + index}` }
      : object);
  const combined = [...current.objects, ...continuationObjects];
  const exactTotalReached = BigInt(combined.length) === current.totalSize;
  if (
    BigInt(combined.length) > current.totalSize
    || (continuation.nextPageToken === null) !== exactTotalReached
  ) return { status: "stale" };
  return {
    status: "merged",
    page: {
      objects: combined,
      nextPageToken: continuation.nextPageToken,
      totalSize: current.totalSize,
      tenantCatalogRevision: current.tenantCatalogRevision,
    },
  };
}

export function adaptKnowledgeObject(
  object: KnowledgeObject,
  responseOrdinal: number,
): KnowledgeListItem {
  const redacted = (): RedactedKnowledgeObjectDisplay => ({
    disclosure: "redacted",
    key: `redacted-${responseOrdinal}`,
    name: "Unavailable object",
    objectTypeLabel: "Unknown type",
    stateLabel: "Unavailable",
    sharingScopeLabel: "Unavailable",
  });

  const objectTypeLabel = knowledgeObjectTypeLabel(object.objectType);
  const stateLabel = knowledgeObjectStateLabel(object.state);
  const sharingScopeLabel = knowledgeSharingScopeLabel(object.sharingScope);
  if (
    objectTypeLabel === null
    || stateLabel === null
    || sharingScopeLabel === null
    || !validIdentity(object.knowledgeObjectId, MAXIMUM_OBJECT_ID_BYTES)
    || !validIdentity(object.tenantId, MAXIMUM_IDENTITY_BYTES)
    || !validIdentity(object.appId, MAXIMUM_APP_ID_BYTES)
    || !validIdentity(object.ownerId, MAXIMUM_IDENTITY_BYTES)
    || !validIdentity(object.name, MAXIMUM_IDENTITY_BYTES)
    || object.version <= 0n
    || object.version > MAXIMUM_SIGNED_REVISION
    || !validDate(object.createdAt)
    || !validDate(object.updatedAt)
    || !validKnowledgeLifecycle(object)
  ) {
    return redacted();
  }

  let definition: KnowledgeDefinitionDisplay;
  if (object.state === KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_QUARANTINED) {
    if (object.definition !== undefined || object.definitionSha256.length !== 0) {
      return redacted();
    }
    definition = {
      status: "withheld",
      description: null,
      descriptionTruncated: false,
      selectors: [],
      bodyKind: "Definition withheld",
      bodyFields: [],
      overwriteBehavior: null,
    };
  } else {
    if (
      object.definition === undefined
      || object.definitionSha256.length !== 32
      || object.definition.appId !== object.appId
      || object.definition.name !== object.name
      || object.definition.sharingScope !== object.sharingScope
    ) {
      return redacted();
    }
    const adapted = adaptKnowledgeDefinition(object.definition, object.objectType, object.state);
    if (adapted === null) return redacted();
    definition = adapted;
  }

  return {
    disclosure: "available",
    key: `object:${object.knowledgeObjectId}`,
    knowledgeObjectId: `${object.knowledgeObjectId}`,
    name: `${object.name}`,
    appId: `${object.appId}`,
    ownerId: `${object.ownerId}`,
    objectType: object.objectType,
    objectTypeLabel,
    state: object.state,
    stateLabel,
    sharingScope: object.sharingScope,
    sharingScopeLabel,
    version: object.version,
    createdAt: new Date(object.createdAt as Date),
    updatedAt: new Date(object.updatedAt as Date),
    definition,
  };
}

function adaptKnowledgeDefinition(
  definition: KnowledgeObjectDefinition,
  objectType: KnowledgeObjectType,
  state: KnowledgeObjectState,
): KnowledgeDefinitionDisplay | null {
  if (definition.body === undefined) {
    if (state === KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE) return null;
    return {
      status: "opaque",
      description: null,
      descriptionTruncated: false,
      selectors: [],
      bodyKind: "Definition format unavailable",
      bodyFields: [],
      overwriteBehavior: null,
    };
  }
  if (definition.description !== undefined && !validOptionalText(
    definition.description,
    MAXIMUM_DESCRIPTION_BYTES,
  )) {
    return null;
  }
  const selectors = adaptKnowledgeSelectors(definition.selector);
  if (selectors === null) return null;
  const description = clippedDescription(definition.description);

  switch (definition.body.$case) {
    case "fieldExtraction": {
      if (objectType !== KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION) return null;
      const body = definition.body.value;
      if (
        !validRequiredText(body.inputField, MAXIMUM_IDENTITY_BYTES)
        || knowledgeOverwriteLabel(body.overwriteBehavior) === null
        || body.extraction === undefined
      ) return null;
      if (body.extraction.$case === "regex") {
        const regex = body.extraction.value;
        if (
          !validOpaqueBodyText(regex.pattern, MAXIMUM_REGEX_OR_PATH_BYTES)
          || regex.outputFields.length < 1
          || regex.outputFields.length > MAXIMUM_FIELD_EXTRACTION_OUTPUTS
          || regex.outputFields.some((field) => !validRequiredText(field, MAXIMUM_IDENTITY_BYTES))
          || new Set(regex.outputFields).size !== regex.outputFields.length
        ) return null;
        return {
          status: "recognized",
          ...description,
          selectors,
          bodyKind: "Regex field extraction",
          bodyFields: [
            { label: "Input field", value: `${body.inputField}` },
            { label: "Output fields", value: regex.outputFields.join(", ") },
          ],
          overwriteBehavior: knowledgeOverwriteLabel(body.overwriteBehavior),
        };
      }
      const json = body.extraction.value;
      if (
        !validOpaqueBodyText(json.path, MAXIMUM_REGEX_OR_PATH_BYTES)
        || !validRequiredText(json.outputField, MAXIMUM_IDENTITY_BYTES)
      ) return null;
      return {
        status: "recognized",
        ...description,
        selectors,
        bodyKind: "JSON field extraction",
        bodyFields: [
          { label: "Input field", value: `${body.inputField}` },
          { label: "Output field", value: `${json.outputField}` },
        ],
        overwriteBehavior: knowledgeOverwriteLabel(body.overwriteBehavior),
      };
    }
    case "fieldAlias": {
      if (objectType !== KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS) return null;
      const body = definition.body.value;
      if (
        !validRequiredText(body.sourceField, MAXIMUM_IDENTITY_BYTES)
        || !validRequiredText(body.destinationField, MAXIMUM_IDENTITY_BYTES)
        || body.sourceField === body.destinationField
        || knowledgeOverwriteLabel(body.overwriteBehavior) === null
      ) return null;
      return {
        status: "recognized",
        ...description,
        selectors,
        bodyKind: "Field alias",
        bodyFields: [
          { label: "Source field", value: `${body.sourceField}` },
          { label: "Destination field", value: `${body.destinationField}` },
        ],
        overwriteBehavior: knowledgeOverwriteLabel(body.overwriteBehavior),
      };
    }
    case "calculatedField": {
      if (objectType !== KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD) return null;
      const body = definition.body.value;
      if (
        !validRequiredText(body.destinationField, MAXIMUM_IDENTITY_BYTES)
        || !validOpaqueBodyText(body.expression, MAXIMUM_BODY_TEXT_BYTES)
        || knowledgeOverwriteLabel(body.overwriteBehavior) === null
      ) return null;
      return {
        status: "recognized",
        ...description,
        selectors,
        bodyKind: "Calculated field",
        bodyFields: [{ label: "Destination field", value: `${body.destinationField}` }],
        overwriteBehavior: knowledgeOverwriteLabel(body.overwriteBehavior),
      };
    }
  }
}

function adaptKnowledgeSelectors(selector: KnowledgeSelector | undefined): KnowledgeSelectorDisplay[] | null {
  if (selector === undefined) return [];
  const dimensions: Array<{
    dimension: KnowledgeSelectorDisplay["dimension"];
    values: KnowledgeSelector["indexPatterns"];
  }> = [
    { dimension: "Index", values: selector.indexPatterns },
    { dimension: "Host", values: selector.hostPatterns },
    { dimension: "Source", values: selector.sourcePatterns },
    { dimension: "Sourcetype", values: selector.sourcetypePatterns },
  ];
  const result: KnowledgeSelectorDisplay[] = [];
  for (const dimension of dimensions) {
    if (dimension.values.length > MAXIMUM_SELECTOR_PATTERNS_PER_DIMENSION) return null;
    const seen = new Set<string>();
    const patterns: string[] = [];
    for (const pattern of dimension.values) {
      if (
        !validRequiredText(pattern.value, MAXIMUM_SELECTOR_PATTERN_BYTES)
        || (pattern.matchKind !== KnowledgeSelectorMatchKind.KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT
          && pattern.matchKind !== KnowledgeSelectorMatchKind.KNOWLEDGE_SELECTOR_MATCH_KIND_WILDCARD)
        || seen.has(pattern.value)
      ) return null;
      seen.add(pattern.value);
      patterns.push(`${pattern.value}`);
    }
    if (patterns.length > 0) result.push({ dimension: dimension.dimension, patterns });
  }
  return result;
}

function clippedDescription(value: string | undefined): {
  description: string | null;
  descriptionTruncated: boolean;
} {
  if (value === undefined || value.length === 0) {
    return { description: null, descriptionTruncated: false };
  }
  const codePoints = Array.from(value);
  if (codePoints.length <= MAXIMUM_DISPLAY_DESCRIPTION_CODE_POINTS) {
    return { description: `${value}`, descriptionTruncated: false };
  }
  return {
    description: `${codePoints.slice(0, MAXIMUM_DISPLAY_DESCRIPTION_CODE_POINTS).join("")}…`,
    descriptionTruncated: true,
  };
}

function knowledgeObjectTypeLabel(value: KnowledgeObjectType): string | null {
  switch (value) {
    case KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION:
      return "Field extraction";
    case KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS:
      return "Field alias";
    case KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD:
      return "Calculated field";
    default:
      return null;
  }
}

function knowledgeObjectStateLabel(value: KnowledgeObjectState): string | null {
  switch (value) {
    case KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DRAFT:
      return "Draft";
    case KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE:
      return "Active";
    case KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DISABLED:
      return "Disabled";
    case KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_QUARANTINED:
      return "Quarantined";
    case KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DELETED:
      return "Deleted";
    default:
      return null;
  }
}

function knowledgeSharingScopeLabel(value: SharingScope): string | null {
  switch (value) {
    case SharingScope.SHARING_SCOPE_PRIVATE:
      return "Private";
    case SharingScope.SHARING_SCOPE_APP:
      return "App";
    case SharingScope.SHARING_SCOPE_GLOBAL:
      return "Global";
    default:
      return null;
  }
}

function knowledgeOverwriteLabel(value: KnowledgeOverwriteBehavior): string | null {
  switch (value) {
    case KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING:
      return "Preserve existing values";
    case KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING:
      return "Replace existing values";
    default:
      return null;
  }
}

function validDate(value: Date | undefined): boolean {
  return value instanceof Date && Number.isFinite(value.valueOf());
}

function validKnowledgeLifecycle(object: KnowledgeObject): boolean {
  if (!validDate(object.createdAt) || !validDate(object.updatedAt)) return false;
  const created = (object.createdAt as Date).valueOf();
  const updated = (object.updatedAt as Date).valueOf();
  if (updated < created) return false;
  const equalUpdated = (value: Date | undefined) =>
    validDate(value) && (value as Date).valueOf() === updated;
  switch (object.state) {
    case KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DRAFT:
    case KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE:
      return object.disabledAt === undefined
        && object.quarantinedAt === undefined
        && object.deletedAt === undefined
        && object.quarantineReason === undefined;
    case KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DISABLED:
      return validDate(object.disabledAt)
        && (object.disabledAt as Date).valueOf() >= created
        && (object.disabledAt as Date).valueOf() <= updated
        && object.quarantinedAt === undefined
        && object.deletedAt === undefined
        && object.quarantineReason === undefined;
    case KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_QUARANTINED:
      return object.disabledAt === undefined
        && equalUpdated(object.quarantinedAt)
        && object.deletedAt === undefined
        && (object.quarantineReason === "root_corruption"
          || object.quarantineReason === "dependency_recovery");
    case KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DELETED:
      return object.disabledAt === undefined
        && object.quarantinedAt === undefined
        && equalUpdated(object.deletedAt)
        && object.quarantineReason === undefined;
    default:
      return false;
  }
}

interface GeneratedMessageCodec<T> {
  encode(message: T): { finish(): Uint8Array };
  decode(bytes: Uint8Array): T;
}

function cloneBoundedMutationRequest<T>(
  codec: GeneratedMessageCodec<T>,
  request: T,
): T {
  let encoded: Uint8Array;
  try {
    encoded = codec.encode(request).finish();
  } catch {
    throw new TypeError("Knowledge mutation request cannot be encoded.");
  }
  if (encoded.byteLength === 0 || encoded.byteLength > MAXIMUM_KNOWLEDGE_MUTATION_REQUEST_BYTES) {
    throw new TypeError("Knowledge mutation request exceeds its browser envelope.");
  }
  return codec.decode(encoded);
}

function cloneBoundedMutationResponse<T>(
  codec: GeneratedMessageCodec<T>,
  response: T,
): T {
  let encoded: Uint8Array;
  try {
    encoded = codec.encode(response).finish();
  } catch {
    throw new TypeError("Knowledge mutation response cannot be encoded.");
  }
  if (encoded.byteLength > KNOWLEDGE_MANAGER_MAXIMUM_RESPONSE_BYTES) {
    throw new TypeError("Knowledge mutation response exceeds its browser envelope.");
  }
  try {
    return codec.decode(encoded);
  } catch {
    throw new TypeError("Knowledge mutation response cannot be detached.");
  }
}

async function prepareKnowledgeCurrentObject(
  request: {
    readonly knowledgeObjectId?: string;
    readonly expectedVersion?: bigint;
  },
  current: KnowledgeObject | undefined,
): Promise<KnowledgeObject | undefined> {
  if (current === undefined) return undefined;
  const detached = cloneBoundedMutationResponse(KnowledgeObject, current);
  const adapted = adaptKnowledgeObject(detached, 0);
  if (
    request.knowledgeObjectId === undefined
    || request.expectedVersion === undefined
    || adapted.disclosure !== "available"
    || detached.knowledgeObjectId !== request.knowledgeObjectId
    || detached.version !== request.expectedVersion
    || detached.definition === undefined
    || !validCanonicalMutationDefinition(detached.definition)
    || !await definitionDigestMatches(detached.definition, detached.definitionSha256)
  ) {
    throw new TypeError("Knowledge mutation current object is outside the browser contract.");
  }
  return detached;
}

function validKnowledgeValidationUpdateAuthority(
  request: ValidateKnowledgeObjectRequestMessage,
  current: KnowledgeObject | undefined,
): boolean {
  if (request.knowledgeObjectId === undefined) return current === undefined;
  if (
    current === undefined
    || request.definition === undefined
    || (
      request.intent === KnowledgeValidationIntent.KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION
      && current.state === KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DELETED
    )
  ) return false;
  const selectedBodyPath = request.updateMask?.find((path) =>
    path === "field_extraction" || path === "field_alias" || path === "calculated_field");
  if (selectedBodyPath === undefined) return true;
  const expectedCase = selectedBodyPath === "field_extraction"
    ? "fieldExtraction"
    : selectedBodyPath === "field_alias" ? "fieldAlias" : "calculatedField";
  return current.definition?.body?.$case === expectedCase
    && request.definition.body?.$case === expectedCase;
}

function validExpectedVersion(value: bigint | undefined): value is bigint {
  return typeof value === "bigint" && value >= 1n && value <= MAXIMUM_SIGNED_REVISION;
}

function validClientRequestID(value: string): boolean {
  if (
    typeof value !== "string"
    || value.length < MINIMUM_CLIENT_REQUEST_ID_BYTES
    || value.length > MAXIMUM_CLIENT_REQUEST_ID_BYTES
  ) return false;
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code < 0x21 || code > 0x7e) return false;
  }
  return true;
}

function validKnowledgeUpdateMask(paths: string[] | undefined): paths is string[] {
  if (
    !Array.isArray(paths)
    || paths.length < 1
    || paths.length > MAXIMUM_UPDATE_MASK_PATHS
  ) return false;
  let bodyPaths = 0;
  for (let index = 0; index < paths.length; index += 1) {
    const path = paths[index];
    if (
      typeof path !== "string"
      || !KNOWLEDGE_UPDATE_MASK_PATHS.has(path)
      || (index > 0 && compareUTF8Binary(paths[index - 1] ?? "", path) >= 0)
    ) return false;
    if (
      path === "field_extraction"
      || path === "field_alias"
      || path === "calculated_field"
    ) bodyPaths += 1;
  }
  return bodyPaths <= 1;
}

function validCanonicalMutationDefinition(definition: KnowledgeObjectDefinition): boolean {
  if (
    !validIdentity(definition.appId, MAXIMUM_APP_ID_BYTES)
    || !validIdentity(definition.name, MAXIMUM_IDENTITY_BYTES)
    || (
      definition.description !== undefined
      && (
        !validOptionalText(definition.description, MAXIMUM_DESCRIPTION_BYTES)
        || definition.description.length === 0
        || trimPinnedASCIIWhitespace(definition.description) !== definition.description
      )
    )
    || knowledgeSharingScopeLabel(definition.sharingScope) === null
    || !validCanonicalMutationSelector(definition.selector)
  ) return false;

  const objectType = mutationDefinitionObjectType(definition);
  if (objectType === null || adaptKnowledgeDefinition(
    definition,
    objectType,
    KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DRAFT,
  ) === null) return false;

  switch (definition.body?.$case) {
    case "fieldExtraction": {
      const body = definition.body.value;
      if (body.inputField !== "_raw") return false;
      if (body.extraction?.$case === "regex") {
        return body.extraction.value.outputFields.every((field) =>
          validCanonicalSearchFieldPath(field, true));
      }
      return body.extraction?.$case === "json"
        && validCanonicalSearchFieldPath(body.extraction.value.outputField, true);
    }
    case "fieldAlias":
      return validCanonicalSearchFieldPath(definition.body.value.sourceField, false)
        && validCanonicalSearchFieldPath(definition.body.value.destinationField, true);
    case "calculatedField":
      return validCanonicalSearchFieldPath(definition.body.value.destinationField, true)
        && trimPinnedASCIIWhitespace(definition.body.value.expression)
          === definition.body.value.expression;
    default:
      return false;
  }
}

function mutationDefinitionObjectType(
  definition: KnowledgeObjectDefinition,
): KnowledgeObjectType | null {
  switch (definition.body?.$case) {
    case "fieldExtraction":
      return KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION;
    case "fieldAlias":
      return KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS;
    case "calculatedField":
      return KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD;
    default:
      return null;
  }
}

function validCanonicalMutationSelector(selector: KnowledgeSelector | undefined): boolean {
  if (selector === undefined) return true;
  const dimensions = [
    selector.indexPatterns,
    selector.hostPatterns,
    selector.sourcePatterns,
    selector.sourcetypePatterns,
  ];
  if (dimensions.every((patterns) => patterns.length === 0)) return false;
  let normalizedBytes = utf8ByteLength(KNOWLEDGE_SELECTOR_CANONICAL_DOMAIN) + 12;
  let wildcardWorkUnits = 0;
  for (const patterns of dimensions) {
    if (patterns.length > MAXIMUM_SELECTOR_PATTERNS_PER_DIMENSION) return false;
    for (let index = 0; index < patterns.length; index += 1) {
      const pattern = patterns[index];
      const normalized = pattern === undefined
        ? null
        : normalizeKnowledgeSelectorPattern(pattern.value);
      if (
        pattern === undefined
        || normalized === null
        || normalized.value !== pattern.value
        || pattern.matchKind !== normalized.matchKind
        || (index > 0 && compareUTF8Binary(patterns[index - 1]?.value ?? "", pattern.value) >= 0)
      ) return false;
      normalizedBytes += 4 + utf8ByteLength(pattern.value);
      wildcardWorkUnits += knowledgeSelectorPatternWorkUnits(pattern.value);
    }
  }
  return normalizedBytes <= MAXIMUM_SELECTOR_NORMALIZED_BYTES
    && wildcardWorkUnits <= MAXIMUM_SELECTOR_WILDCARD_WORK_UNITS;
}

function sameBytes(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) return false;
  for (let index = 0; index < left.byteLength; index += 1) {
    if (left[index] !== right[index]) return false;
  }
  return true;
}

async function definitionDigestMatches(
  definition: KnowledgeObjectDefinition | undefined,
  digest: Uint8Array | undefined,
): Promise<boolean> {
  if (
    definition === undefined
    || !(digest instanceof Uint8Array)
    || digest.byteLength !== 32
    || globalThis.crypto?.subtle === undefined
  ) return false;
  try {
    const bytes = KnowledgeObjectDefinition.encode(definition).finish();
    const input = Uint8Array.from(bytes).buffer;
    const computed = new Uint8Array(await globalThis.crypto.subtle.digest("SHA-256", input));
    return sameBytes(computed, digest);
  } catch {
    return false;
  }
}

function definitionMatchesNormalizedSubmission(
  result: KnowledgeObjectDefinition,
  submitted: KnowledgeObjectDefinition,
): boolean {
  const normalized = normalizedSubmittedDefinition(submitted);
  return normalized !== null && sameBytes(
    KnowledgeObjectDefinition.encode(result).finish(),
    KnowledgeObjectDefinition.encode(normalized).finish(),
  );
}

function normalizedAppliedUpdateDefinition(
  current: KnowledgeObjectDefinition | undefined,
  submitted: KnowledgeObjectDefinition | undefined,
  paths: string[] | undefined,
): KnowledgeObjectDefinition | null {
  if (current === undefined || submitted === undefined || !validKnowledgeUpdateMask(paths)) {
    return null;
  }
  const applied = KnowledgeObjectDefinition.decode(
    KnowledgeObjectDefinition.encode(current).finish(),
  );
  for (const path of paths) {
    switch (path) {
      case "app_id":
        applied.appId = submitted.appId;
        break;
      case "name":
        applied.name = submitted.name;
        break;
      case "description":
        applied.description = submitted.description;
        break;
      case "sharing_scope":
        applied.sharingScope = submitted.sharingScope;
        break;
      case "selector":
        applied.selector = submitted.selector === undefined
          ? undefined
          : KnowledgeSelector.decode(KnowledgeSelector.encode(submitted.selector).finish());
        break;
      case "field_extraction":
      case "field_alias":
      case "calculated_field": {
        const expectedCase = path === "field_extraction"
          ? "fieldExtraction"
          : path === "field_alias" ? "fieldAlias" : "calculatedField";
        if (
          applied.body?.$case !== expectedCase
          || submitted.body?.$case !== expectedCase
        ) {
          return null;
        }
        applied.body = KnowledgeObjectDefinition.decode(
          KnowledgeObjectDefinition.encode(
            KnowledgeObjectDefinition.fromPartial({ body: submitted.body }),
          ).finish(),
        ).body;
        break;
      }
    }
  }
  return normalizedSubmittedDefinition(applied);
}

function sameKnowledgeDefinition(
  left: KnowledgeObjectDefinition,
  right: KnowledgeObjectDefinition,
): boolean {
  return sameBytes(
    KnowledgeObjectDefinition.encode(left).finish(),
    KnowledgeObjectDefinition.encode(right).finish(),
  );
}

function sameOptionalDate(left: Date | undefined, right: Date | undefined): boolean {
  return left === undefined
    ? right === undefined
    : right !== undefined && left.valueOf() === right.valueOf();
}

function mutationResultRetainsCurrentIdentity(
  result: KnowledgeObject,
  current: KnowledgeObject,
): boolean {
  return result.knowledgeObjectId === current.knowledgeObjectId
    && result.tenantId === current.tenantId
    && result.ownerId === current.ownerId
    && sameOptionalDate(result.createdAt, current.createdAt)
    && result.updatedAt !== undefined
    && current.updatedAt !== undefined
    && result.updatedAt.valueOf() >= current.updatedAt.valueOf();
}

function validKnowledgeStateTransition(
  current: KnowledgeObjectState,
  target: KnowledgeObjectState,
): boolean {
  if (target === KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE) {
    return current === KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DRAFT
      || current === KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DISABLED;
  }
  return target === KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DISABLED
    && (
      current === KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DRAFT
      || current === KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE
    );
}

function normalizedSubmittedDefinition(
  submitted: KnowledgeObjectDefinition,
): KnowledgeObjectDefinition | null {
  const selector = normalizedSubmittedSelector(submitted.selector);
  const body = normalizedSubmittedBody(submitted.body);
  if (selector === null || body === null) return null;
  return KnowledgeObjectDefinition.fromPartial({
    appId: trimPinnedASCIIWhitespace(submitted.appId),
    name: trimPinnedASCIIWhitespace(submitted.name),
    description: normalizedSubmittedDescription(submitted.description),
    sharingScope: submitted.sharingScope,
    selector,
    body,
  });
}

function normalizedSubmittedDescription(value: string | undefined): string | undefined {
  if (value === undefined) return undefined;
  const normalized = trimPinnedASCIIWhitespace(value);
  return normalized.length === 0 ? undefined : normalized;
}

function normalizedSubmittedBody(
  body: KnowledgeObjectDefinition["body"],
): KnowledgeObjectDefinition["body"] | null {
  switch (body?.$case) {
    case "fieldExtraction": {
      const value = body.value;
      const overwriteBehavior = normalizedSubmittedOverwrite(value.overwriteBehavior);
      if (overwriteBehavior === null || value.extraction === undefined) return null;
      if (value.extraction.$case === "regex") {
        if (
          value.extraction.value.outputFields.length < 1
          || value.extraction.value.outputFields.length > MAXIMUM_FIELD_EXTRACTION_OUTPUTS
        ) return null;
        return {
          $case: "fieldExtraction",
          value: {
            inputField: value.inputField === ""
              ? "_raw"
              : trimPinnedASCIIWhitespace(value.inputField),
            overwriteBehavior,
            extraction: {
              $case: "regex",
              value: {
                pattern: value.extraction.value.pattern,
                outputFields: value.extraction.value.outputFields.map(trimPinnedASCIIWhitespace),
              },
            },
          },
        };
      }
      return {
        $case: "fieldExtraction",
        value: {
          inputField: value.inputField === ""
            ? "_raw"
            : trimPinnedASCIIWhitespace(value.inputField),
          overwriteBehavior,
          extraction: {
            $case: "json",
            value: {
              path: value.extraction.value.path,
              outputField: trimPinnedASCIIWhitespace(value.extraction.value.outputField),
            },
          },
        },
      };
    }
    case "fieldAlias": {
      const overwriteBehavior = normalizedSubmittedOverwrite(body.value.overwriteBehavior);
      if (overwriteBehavior === null) return null;
      return {
        $case: "fieldAlias",
        value: {
          sourceField: trimPinnedASCIIWhitespace(body.value.sourceField),
          destinationField: trimPinnedASCIIWhitespace(body.value.destinationField),
          overwriteBehavior,
        },
      };
    }
    case "calculatedField": {
      if (
        utf8ByteLength(body.value.expression, MAXIMUM_BODY_TEXT_BYTES)
        > MAXIMUM_BODY_TEXT_BYTES
      ) return null;
      const overwriteBehavior = normalizedSubmittedOverwrite(body.value.overwriteBehavior);
      if (overwriteBehavior === null) return null;
      return {
        $case: "calculatedField",
        value: {
          destinationField: trimPinnedASCIIWhitespace(body.value.destinationField),
          expression: trimPinnedASCIIWhitespace(body.value.expression),
          overwriteBehavior,
        },
      };
    }
    default:
      return null;
  }
}

function normalizedSubmittedOverwrite(
  value: KnowledgeOverwriteBehavior,
): KnowledgeOverwriteBehavior | null {
  if (value === KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_UNSPECIFIED) {
    return KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING;
  }
  return knowledgeOverwriteLabel(value) === null ? null : value;
}

function normalizedSubmittedSelector(
  selector: KnowledgeSelector | undefined,
): KnowledgeSelector | null | undefined {
  if (selector === undefined) return undefined;
  const normalizedDimensions: Array<KnowledgeSelector["indexPatterns"] | null> = [
    normalizedSubmittedPatterns(selector.indexPatterns),
    normalizedSubmittedPatterns(selector.hostPatterns),
    normalizedSubmittedPatterns(selector.sourcePatterns),
    normalizedSubmittedPatterns(selector.sourcetypePatterns),
  ];
  if (normalizedDimensions.some((dimension) => dimension === null)) return null;
  const [indexPatterns, hostPatterns, sourcePatterns, sourcetypePatterns] =
    normalizedDimensions as KnowledgeSelector["indexPatterns"][];
  if (
    indexPatterns.length === 0
    && hostPatterns.length === 0
    && sourcePatterns.length === 0
    && sourcetypePatterns.length === 0
  ) return undefined;
  return KnowledgeSelector.fromPartial({
    indexPatterns,
    hostPatterns,
    sourcePatterns,
    sourcetypePatterns,
  });
}

function normalizedSubmittedPatterns(
  patterns: KnowledgeSelector["indexPatterns"],
): KnowledgeSelector["indexPatterns"] | null {
  if (patterns.length > MAXIMUM_SELECTOR_PATTERNS_PER_DIMENSION) return null;
  const normalized = new Map<string, KnowledgeSelector["indexPatterns"][number]>();
  for (const pattern of patterns) {
    const canonical = normalizeKnowledgeSelectorPattern(pattern.value);
    if (
      canonical === null
      || (
        pattern.matchKind !== KnowledgeSelectorMatchKind.KNOWLEDGE_SELECTOR_MATCH_KIND_UNSPECIFIED
        && pattern.matchKind !== canonical.matchKind
      )
    ) return null;
    normalized.set(canonical.value, canonical);
  }
  return [...normalized.values()].toSorted((left, right) =>
    compareUTF8Binary(left.value, right.value));
}

function normalizeKnowledgeSelectorPattern(
  submitted: string,
): KnowledgeSelector["indexPatterns"][number] | null {
  const value = trimPinnedASCIIWhitespace(submitted);
  if (
    value.length === 0
    || utf8ByteLength(value, MAXIMUM_SELECTOR_PATTERN_BYTES) > MAXIMUM_SELECTOR_PATTERN_BYTES
  ) return null;
  let escaped = false;
  let wildcard = false;
  let previousStar = false;
  let canonical = "";
  for (const character of value) {
    if (escaped) {
      if (character !== "*" && character !== "?" && character !== "\\") return null;
      canonical += `\\${character}`;
      escaped = false;
      previousStar = false;
      continue;
    }
    if (character === "\\") {
      escaped = true;
      continue;
    }
    const codePoint = character.codePointAt(0) ?? 0;
    if (codePoint <= 0x1f || (codePoint >= 0x7f && codePoint <= 0x9f)) return null;
    if (character === "*") {
      wildcard = true;
      if (previousStar) continue;
      previousStar = true;
      canonical += character;
      continue;
    }
    if (character === "?") wildcard = true;
    previousStar = false;
    canonical += character;
  }
  if (escaped) return null;
  return {
    value: canonical,
    matchKind: wildcard
      ? KnowledgeSelectorMatchKind.KNOWLEDGE_SELECTOR_MATCH_KIND_WILDCARD
      : KnowledgeSelectorMatchKind.KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT,
  };
}

function knowledgeSelectorPatternWorkUnits(canonical: string): number {
  let escaped = false;
  let units = 0;
  for (const character of canonical) {
    if (escaped) {
      units += 1;
      escaped = false;
    } else if (character === "\\") {
      escaped = true;
    } else if (character === "*") {
      units += 4;
    } else if (character === "?") {
      units += 2;
    } else {
      units += 1;
    }
  }
  return escaped ? MAXIMUM_SELECTOR_WILDCARD_WORK_UNITS + 1 : units;
}

function validCanonicalSearchFieldPath(value: string, rejectReservedRoot: boolean): boolean {
  if (
    value.length === 0
    || utf8ByteLength(value, MAXIMUM_IDENTITY_BYTES) > MAXIMUM_IDENTITY_BYTES
  ) return false;
  const segments: string[] = [];
  let segment = "";
  let escaped = false;
  for (const character of value) {
    if (escaped) {
      if (character !== "\\" && character !== ".") return false;
      segment += character;
      escaped = false;
      continue;
    }
    if (character === "\\") {
      escaped = true;
      continue;
    }
    if (character === ".") {
      if (!validSearchFieldSegment(segment) || segments.length >= MAXIMUM_SEARCH_FIELD_PATH_SEGMENTS - 1) {
        return false;
      }
      segments.push(segment);
      segment = "";
      continue;
    }
    segment += character;
  }
  if (escaped || !validSearchFieldSegment(segment)) return false;
  segments.push(segment);
  const foldedRoot = foldASCIIIdentifier(segments[0] ?? "");
  if (
    rejectReservedRoot
    && (
      foldedRoot.startsWith("__os_")
      || RESERVED_DYNAMIC_FIELD_ROOTS.has(foldedRoot)
    )
  ) return false;
  return normalizeSearchFieldPath(segments) === value;
}

function foldASCIIIdentifier(value: string): string {
  return value.replace(/[A-Z]/g, (character) => character.toLowerCase());
}

function validSearchFieldSegment(value: string): boolean {
  if (
    value.length === 0
    || utf8ByteLength(value, MAXIMUM_SEARCH_FIELD_PATH_SEGMENT_BYTES)
      > MAXIMUM_SEARCH_FIELD_PATH_SEGMENT_BYTES
  ) return false;
  for (const character of value) {
    if (/\p{Cc}/u.test(character)) return false;
  }
  return true;
}

function normalizeSearchFieldPath(segments: readonly string[]): string {
  return segments.map((segment) =>
    segment.replaceAll("\\", "\\\\").replaceAll(".", "\\."),
  ).join(".");
}

async function adaptKnowledgeObjectMutationReceipt(response: {
  knowledgeObject: KnowledgeObject | undefined;
  tenantCatalogRevision: bigint;
  tenantCatalogStateToken: Uint8Array;
}): Promise<KnowledgeObjectMutationReceipt> {
  if (
    response.knowledgeObject === undefined
    || !(response.tenantCatalogStateToken instanceof Uint8Array)
    || response.tenantCatalogStateToken.byteLength !== 32
  ) {
    throw new TypeError("Knowledge mutation response omitted its object.");
  }
  const adapted = adaptKnowledgeObject(response.knowledgeObject, 0);
  const knowledgeObject = adapted.disclosure === "available"
    ? KnowledgeObject.decode(KnowledgeObject.encode(response.knowledgeObject).finish())
    : undefined;
  const tenantCatalogStateToken = Uint8Array.from(response.tenantCatalogStateToken);
  if (
    adapted.disclosure !== "available"
    || knowledgeObject === undefined
    || knowledgeObject.definition === undefined
    || !validCanonicalMutationDefinition(knowledgeObject.definition)
    || !await definitionDigestMatches(
      knowledgeObject.definition,
      knowledgeObject.definitionSha256,
    )
    || !validCatalogSnapshot(
      response.tenantCatalogRevision,
      tenantCatalogStateToken,
      knowledgeObject.version,
    )
  ) {
    throw new TypeError("Knowledge mutation response is outside the browser contract.");
  }
  return {
    knowledgeObject,
    tenantCatalogRevision: response.tenantCatalogRevision,
    tenantCatalogStateToken,
  };
}

function validCatalogSnapshot(
  revision: bigint,
  token: Uint8Array,
  minimumRevision: bigint,
): boolean {
  return typeof revision === "bigint"
    && revision >= minimumRevision
    && revision <= MAXIMUM_SIGNED_REVISION
    && token instanceof Uint8Array
    && token.byteLength === 32;
}

function validKnowledgeValidationResult(
  result: KnowledgeValidationResult,
  request: ValidateKnowledgeObjectRequestMessage,
  revision: bigint,
  currentKnowledgeObject: KnowledgeObject | undefined,
): boolean {
  const currentDefinition = currentKnowledgeObject?.definition;
  if (
    request.definition === undefined
    || (
      request.knowledgeObjectId !== undefined
      && (
        request.expectedVersion === undefined
        || revision < request.expectedVersion
      )
    )
    ||
    typeof result.valid !== "boolean"
    || !Array.isArray(result.fieldViolations)
    || !Array.isArray(result.dependencies)
    || !Array.isArray(result.diagnostics)
    || result.fieldViolations.length > MAXIMUM_VALIDATION_ISSUES
    || result.dependencies.length > MAXIMUM_VALIDATION_DEPENDENCIES
    || result.diagnostics.length > MAXIMUM_VALIDATION_ISSUES
    || result.fieldViolationsTruncated
    || result.diagnosticsTruncated
    || !validValidationFieldViolations(result.fieldViolations)
    || !validValidationDependencies(result.dependencies, revision)
    || !validValidationDiagnostics(
      result.diagnostics,
      (fieldPath) => validationDiagnosticSource(
        result,
        request,
        currentDefinition,
        fieldPath,
      ),
    )
  ) return false;

  const isInactive = request.intent
    === KnowledgeValidationIntent.KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE;
  const hasError = result.diagnostics.some(({ diagnostic }) =>
    diagnostic?.severity === DiagnosticSeverity.DIAGNOSTIC_SEVERITY_ERROR);
  if (!result.valid) {
    return result.normalizedDefinition === undefined
      && result.definitionSha256 === undefined
      && result.resources === undefined
      && result.dependencies.length === 0
      && (result.fieldViolations.length > 0 || hasError)
      && validInvalidValidationObjectType(result.objectType, request, currentDefinition);
  }

  if (
    result.normalizedDefinition === undefined
    || !validCanonicalMutationDefinition(result.normalizedDefinition)
    || mutationDefinitionObjectType(result.normalizedDefinition) !== result.objectType
    || !(result.definitionSha256 instanceof Uint8Array)
    || result.definitionSha256.byteLength !== 32
    || result.resources === undefined
    || result.fieldViolations.length !== 0
    || hasError
    || !validKnowledgeResources(
      result.resources,
      result.dependencies,
      isInactive,
      result.normalizedDefinition,
    )
    || (isInactive && result.dependencies.length !== 0)
  ) return false;
  const expectedDefinition = request.knowledgeObjectId === undefined
    ? normalizedSubmittedDefinition(request.definition)
    : normalizedAppliedUpdateDefinition(
      currentDefinition,
      request.definition,
      request.updateMask,
    );
  return expectedDefinition !== null
    && sameKnowledgeDefinition(result.normalizedDefinition, expectedDefinition);
}

function validInvalidValidationObjectType(
  value: KnowledgeObjectType,
  request: ValidateKnowledgeObjectRequestMessage,
  currentDefinition: KnowledgeObjectDefinition | undefined,
): boolean {
  const expected = expectedValidationObjectType(request, currentDefinition);
  if (expected !== null) return value === expected;
  return value === KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_UNSPECIFIED
    || value === KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION
    || value === KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS
    || value === KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD;
}

function expectedValidationObjectType(
  request: ValidateKnowledgeObjectRequestMessage,
  currentDefinition: KnowledgeObjectDefinition | undefined,
): KnowledgeObjectType | null {
  if (request.definition === undefined) return null;
  if (request.knowledgeObjectId === undefined) {
    return mutationDefinitionObjectType(request.definition)
      ?? KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_UNSPECIFIED;
  }
  const selectedBodyPath = request.updateMask?.find((path) =>
    path === "field_extraction" || path === "field_alias" || path === "calculated_field");
  if (selectedBodyPath === undefined) {
    return currentDefinition === undefined
      ? null
      : mutationDefinitionObjectType(currentDefinition);
  }
  const selectedCase = selectedBodyPath === "field_extraction"
    ? "fieldExtraction"
    : selectedBodyPath === "field_alias" ? "fieldAlias" : "calculatedField";
  return request.definition.body?.$case === selectedCase
    ? mutationDefinitionObjectType(request.definition)
      ?? KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_UNSPECIFIED
    : KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_UNSPECIFIED;
}

function validValidationFieldViolations(
  violations: KnowledgeValidationResult["fieldViolations"],
): boolean {
  let charge = 0;
  let previous: readonly string[] | undefined;
  for (const violation of violations) {
    if (
      !validIssuePath(violation.fieldPath)
      || !validIssueScalar(violation.code, MAXIMUM_ISSUE_CODE_BYTES)
      || !validIssueScalar(violation.message, MAXIMUM_ISSUE_MESSAGE_BYTES)
    ) return false;
    charge += utf8ByteLength(violation.fieldPath)
      + utf8ByteLength(violation.code)
      + utf8ByteLength(violation.message);
    if (charge > MAXIMUM_FIELD_VIOLATION_TEXT_BYTES) return false;
    const key = [violation.fieldPath, violation.code, violation.message] as const;
    if (previous !== undefined && compareStringSequences(previous, key) >= 0) return false;
    previous = key;
  }
  return true;
}

function validValidationDependencies(
  dependencies: KnowledgeValidationResult["dependencies"],
  revision: bigint,
): boolean {
  let previous: { knowledgeObjectId: string; version: bigint } | undefined;
  for (const dependency of dependencies) {
    const target = dependency.target;
    if (
      dependency.role !== KnowledgeDependencyRole.KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT
      || !validKnowledgeRelationshipIdentity(target, revision)
    ) return false;
    if (previous !== undefined) {
      const order = compareUTF8Binary(previous.knowledgeObjectId, target.knowledgeObjectId);
      if (order > 0 || (order === 0 && previous.version >= target.version)) return false;
    }
    previous = target;
  }
  return true;
}

function validValidationDiagnostics(
  diagnostics: KnowledgeValidationDiagnostic[],
  sourceForFieldPath: (fieldPath: string) => string | null,
): boolean {
  let charge = 0;
  let previous: readonly (string | number | bigint)[] | undefined;
  for (const located of diagnostics) {
    const diagnostic = located.diagnostic;
    if (
      !validIssuePath(located.fieldPath)
      || diagnostic === undefined
      || diagnosticSeverityRank(diagnostic.severity) < 0
      || !validIssueScalar(diagnostic.code, MAXIMUM_ISSUE_CODE_BYTES)
      || !validIssueScalar(diagnostic.message, MAXIMUM_ISSUE_MESSAGE_BYTES)
      || !Array.isArray(diagnostic.suggestions)
      || diagnostic.suggestions.length > MAXIMUM_DIAGNOSTIC_SUGGESTIONS
      || !strictlySortedIssueStrings(
        diagnostic.suggestions,
        MAXIMUM_DIAGNOSTIC_SUGGESTION_BYTES,
      )
      || !validSourceRange(
        diagnostic.sourceRange,
        sourceForFieldPath(located.fieldPath),
      )
    ) return false;
    charge += utf8ByteLength(located.fieldPath)
      + utf8ByteLength(diagnostic.code)
      + utf8ByteLength(diagnostic.message);
    for (const suggestion of diagnostic.suggestions) charge += utf8ByteLength(suggestion);
    if (charge > MAXIMUM_DIAGNOSTIC_TEXT_BYTES) return false;
    const range = diagnostic.sourceRange;
    const key = [
      diagnosticSeverityRank(diagnostic.severity),
      located.fieldPath,
      range === undefined ? 0 : 1,
      range?.start?.byteOffset ?? 0n,
      range?.end?.byteOffset ?? 0n,
      diagnostic.code,
      diagnostic.message,
      range?.start?.line ?? 0,
      range?.start?.column ?? 0,
      range?.end?.line ?? 0,
      range?.end?.column ?? 0,
      ...diagnostic.suggestions,
    ] as const;
    if (previous !== undefined && compareValidationKeys(previous, key) >= 0) return false;
    previous = key;
  }
  return true;
}

function diagnosticSeverityRank(severity: DiagnosticSeverity): number {
  switch (severity) {
    case DiagnosticSeverity.DIAGNOSTIC_SEVERITY_ERROR:
      return 0;
    case DiagnosticSeverity.DIAGNOSTIC_SEVERITY_WARNING:
      return 1;
    case DiagnosticSeverity.DIAGNOSTIC_SEVERITY_INFO:
      return 2;
    default:
      return -1;
  }
}

function validSourceRange(
  range: SourceRange | undefined,
  source: string | null,
): boolean {
  if (range === undefined) return true;
  const start = range.start;
  const end = range.end;
  if (
    source === null
    || start === undefined
    || end === undefined
    || typeof start.byteOffset !== "bigint"
    || typeof end.byteOffset !== "bigint"
    || start.byteOffset < 0n
    || end.byteOffset < start.byteOffset
    || end.byteOffset > BigInt(MAXIMUM_KNOWLEDGE_MUTATION_REQUEST_BYTES)
    || !Number.isSafeInteger(start.line)
    || start.line < 1
    || !Number.isSafeInteger(start.column)
    || start.column < 1
    || !Number.isSafeInteger(end.line)
    || end.line < 1
    || !Number.isSafeInteger(end.column)
    || end.column < 1
  ) return false;
  const expectedStart = sourcePositionAtByteOffset(source, Number(start.byteOffset));
  const expectedEnd = sourcePositionAtByteOffset(source, Number(end.byteOffset));
  return expectedStart !== null
    && expectedEnd !== null
    && start.line === expectedStart.line
    && start.column === expectedStart.column
    && end.line === expectedEnd.line
    && end.column === expectedEnd.column;
}

function validationDiagnosticSource(
  result: KnowledgeValidationResult,
  request: ValidateKnowledgeObjectRequestMessage,
  currentDefinition: KnowledgeObjectDefinition | undefined,
  fieldPath: string,
): string | null {
  if (result.valid) {
    return result.normalizedDefinition === undefined
      ? null
      : validationDiagnosticScalar(result.normalizedDefinition, fieldPath);
  }
  if (request.definition === undefined) return null;
  const selectedBodyPath = diagnosticBodyMaskPath(fieldPath);
  if (selectedBodyPath === null) return null;
  if (
    request.knowledgeObjectId !== undefined
    && !request.updateMask?.includes(selectedBodyPath)
  ) {
    return currentDefinition === undefined
      ? null
      : validationDiagnosticScalar(currentDefinition, fieldPath);
  }
  return validationDiagnosticScalar(request.definition, fieldPath);
}

function diagnosticBodyMaskPath(fieldPath: string): string | null {
  if (fieldPath === "calculated_field.expression") return "calculated_field";
  if (fieldPath === "field_extraction.json.path") return "field_extraction";
  return null;
}

function validationDiagnosticScalar(
  definition: KnowledgeObjectDefinition,
  fieldPath: string,
): string | null {
  if (
    fieldPath === "calculated_field.expression"
    && definition.body?.$case === "calculatedField"
  ) return definition.body.value.expression;
  if (
    fieldPath === "field_extraction.json.path"
    && definition.body?.$case === "fieldExtraction"
    && definition.body.value.extraction?.$case === "json"
  ) return definition.body.value.extraction.value.path;
  return null;
}

function sourcePositionAtByteOffset(
  source: string,
  requestedOffset: number,
): { line: number; column: number } | null {
  let byteOffset = 0;
  let line = 1;
  let column = 1;
  for (const character of source) {
    if (byteOffset === requestedOffset) return { line, column };
    const characterBytes = utf8ByteLength(character, 4);
    if (characterBytes > 4 || byteOffset + characterBytes > requestedOffset) return null;
    byteOffset += characterBytes;
    if (character === "\n") {
      line += 1;
      column = 1;
    } else {
      column += 1;
    }
  }
  return byteOffset === requestedOffset ? { line, column } : null;
}

function validKnowledgeResources(
  resources: KnowledgeResourceEstimate,
  dependencies: KnowledgeValidationResult["dependencies"],
  inactive: boolean,
  definition: KnowledgeObjectDefinition,
): boolean {
  const uint32s = [
    resources.selectorPatterns,
    resources.dependencyNodes,
    resources.dependencyEdges,
    resources.generatedOperators,
    resources.generatedFields,
    resources.regexPrograms,
    resources.scalarExpressions,
    resources.scalarExpressionNodes,
    resources.extractionOutputs,
    resources.jsonEvaluationWorkUnits,
    resources.scalarPredicates,
  ];
  const selectorPatterns = definition.selector === undefined
    ? 0
    : definition.selector.indexPatterns.length
      + definition.selector.hostPatterns.length
      + definition.selector.sourcePatterns.length
      + definition.selector.sourcetypePatterns.length;
  const normalizedDefinitionBytes = KnowledgeObjectDefinition.encode(definition).finish().byteLength;
  if (
    uint32s.some((value) => !Number.isSafeInteger(value) || value < 0 || value > 0xffff_ffff)
    || !validUnsigned64(resources.normalizedDefinitionBytes)
    || resources.normalizedDefinitionBytes !== BigInt(normalizedDefinitionBytes)
    || resources.selectorPatterns !== selectorPatterns
    || !validUnsigned64(resources.estimatedRegexWorkUnits)
    || resources.dependencyEdges !== dependencies.length
    || resources.dependencyNodes !== new Set(
      dependencies.map(({ target }) =>
        `${target?.knowledgeObjectId ?? ""}\0${target?.version.toString() ?? ""}`),
    ).size
  ) return false;
  if (!inactive) return true;
  return resources.dependencyNodes === 0
    && resources.dependencyEdges === 0
    && resources.generatedOperators === 0
    && resources.generatedFields === 0
    && resources.regexPrograms === 0
    && resources.estimatedRegexWorkUnits === 0n
    && resources.scalarExpressions === 0
    && resources.scalarExpressionNodes === 0
    && resources.extractionOutputs === 0
    && resources.jsonEvaluationWorkUnits === 0
    && resources.scalarPredicates === 0;
}

function validUnsigned64(value: bigint): boolean {
  return typeof value === "bigint" && value >= 0n && value <= 18_446_744_073_709_551_615n;
}

function validIssuePath(value: string): boolean {
  return typeof value === "string"
    && utf8ByteLength(value, MAXIMUM_FIELD_PATH_BYTES) <= MAXIMUM_FIELD_PATH_BYTES
    && !value.includes("\0");
}

function validIssueScalar(value: string, maximumBytes: number): boolean {
  return typeof value === "string"
    && value.length > 0
    && utf8ByteLength(value, maximumBytes) <= maximumBytes
    && !value.includes("\0");
}

function strictlySortedIssueStrings(values: string[], maximumBytes: number): boolean {
  for (let index = 0; index < values.length; index += 1) {
    if (
      !validIssueScalar(values[index] ?? "", maximumBytes)
      || (index > 0 && compareUTF8Binary(values[index - 1] ?? "", values[index] ?? "") >= 0)
    ) return false;
  }
  return true;
}

function compareStringSequences(left: readonly string[], right: readonly string[]): number {
  const shared = Math.min(left.length, right.length);
  for (let index = 0; index < shared; index += 1) {
    const order = compareUTF8Binary(left[index] ?? "", right[index] ?? "");
    if (order !== 0) return order;
  }
  return left.length - right.length;
}

function compareValidationKeys(
  left: readonly (string | number | bigint)[],
  right: readonly (string | number | bigint)[],
): number {
  const shared = Math.min(left.length, right.length);
  for (let index = 0; index < shared; index += 1) {
    const leftValue = left[index] ?? 0;
    const rightValue = right[index] ?? 0;
    let order: number;
    if (typeof leftValue === "string" && typeof rightValue === "string") {
      order = compareUTF8Binary(leftValue, rightValue);
    } else if (typeof leftValue === "number" && typeof rightValue === "number") {
      order = leftValue - rightValue;
    } else if (typeof leftValue === "bigint" && typeof rightValue === "bigint") {
      order = leftValue < rightValue ? -1 : leftValue > rightValue ? 1 : 0;
    } else {
      order = typeof leftValue < typeof rightValue ? -1 : 1;
    }
    if (order !== 0) return order;
  }
  return left.length - right.length;
}

function validIdentity(value: string, maximumBytes: number): boolean {
  return validRequiredText(value, maximumBytes)
    && trimPinnedASCIIWhitespace(value) === value;
}

function validCommittedKnowledgeFilter(value: string): boolean {
  return validRequiredText(value, KNOWLEDGE_MANAGER_MAXIMUM_FILTER_BYTES)
    && trimPinnedASCIIWhitespace(value) === value;
}

function validRequiredText(value: string, maximumBytes: number): boolean {
  return value.length > 0
    && utf8ByteLength(value, maximumBytes) <= maximumBytes
    && !hasPinnedControl(value);
}

function validOptionalText(value: string, maximumBytes: number): boolean {
  return utf8ByteLength(value, maximumBytes) <= maximumBytes && !hasPinnedControl(value);
}

function validOpaqueBodyText(value: string, maximumBytes: number): boolean {
  return value.length > 0
    && utf8ByteLength(value, maximumBytes) <= maximumBytes
    && !value.includes("\0");
}

function validOpaqueToken(value: string, maximumBytes: number): boolean {
  return value.length > 0
    && utf8ByteLength(value, maximumBytes) <= maximumBytes
    && !hasPinnedControl(value)
    && trimPinnedASCIIWhitespace(value) === value;
}

function trimPinnedASCIIWhitespace(value: string): string {
  return value.replace(/^[\t-\r ]+|[\t-\r ]+$/g, "");
}

function hasPinnedControl(value: string): boolean {
  for (const character of value) {
    const codePoint = character.codePointAt(0) ?? 0;
    if (codePoint <= 0x1f || (codePoint >= 0x7f && codePoint <= 0x9f)) return true;
  }
  return false;
}

export function utf8ByteLength(value: string, stopAfter = Number.MAX_SAFE_INTEGER): number {
  if (!value.isWellFormed()) return Number.MAX_SAFE_INTEGER;
  let bytes = 0;
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code <= 0x7f) bytes += 1;
    else if (code <= 0x7ff) bytes += 2;
    else if (code >= 0xd800 && code <= 0xdbff) {
      bytes += 4;
      index += 1;
    } else bytes += 3;
    if (bytes > stopAfter) return bytes;
  }
  return bytes;
}

function addBoundedCharge(current: number, addition: number): number {
  if (
    !Number.isSafeInteger(addition)
    || addition < 0
    || current > KNOWLEDGE_MANAGER_MAXIMUM_RESPONSE_BYTES - addition
  ) {
    return KNOWLEDGE_MANAGER_MAXIMUM_RESPONSE_BYTES + 1;
  }
  return current + addition;
}

function preflightKnowledgeObjectCharge(object: KnowledgeObject): number {
  if (object.definition?.selector !== undefined) {
    const selector = object.definition.selector;
    if (
      selector.indexPatterns.length > MAXIMUM_SELECTOR_PATTERNS_PER_DIMENSION
      || selector.hostPatterns.length > MAXIMUM_SELECTOR_PATTERNS_PER_DIMENSION
      || selector.sourcePatterns.length > MAXIMUM_SELECTOR_PATTERNS_PER_DIMENSION
      || selector.sourcetypePatterns.length > MAXIMUM_SELECTOR_PATTERNS_PER_DIMENSION
    ) return KNOWLEDGE_MANAGER_MAXIMUM_RESPONSE_BYTES + 1;
  }
  if (
    object.definition?.body?.$case === "fieldExtraction"
    && object.definition.body.value.extraction?.$case === "regex"
    && object.definition.body.value.extraction.value.outputFields.length
      > MAXIMUM_FIELD_EXTRACTION_OUTPUTS
  ) return KNOWLEDGE_MANAGER_MAXIMUM_RESPONSE_BYTES + 1;

  let charge = 256 + object.definitionSha256.length;
  const strings = [
    object.knowledgeObjectId,
    object.tenantId,
    object.appId,
    object.ownerId,
    object.name,
    object.quarantineReason ?? "",
    object.definition?.appId ?? "",
    object.definition?.name ?? "",
    object.definition?.description ?? "",
  ];
  const selector = object.definition?.selector;
  for (const pattern of [
    ...(selector?.indexPatterns ?? []),
    ...(selector?.hostPatterns ?? []),
    ...(selector?.sourcePatterns ?? []),
    ...(selector?.sourcetypePatterns ?? []),
  ]) strings.push(pattern.value);
  const body = object.definition?.body;
  if (body?.$case === "fieldExtraction") {
    strings.push(body.value.inputField);
    if (body.value.extraction?.$case === "regex") {
      strings.push(body.value.extraction.value.pattern, ...body.value.extraction.value.outputFields);
    } else if (body.value.extraction?.$case === "json") {
      strings.push(body.value.extraction.value.path, body.value.extraction.value.outputField);
    }
  } else if (body?.$case === "fieldAlias") {
    strings.push(body.value.sourceField, body.value.destinationField);
  } else if (body?.$case === "calculatedField") {
    strings.push(body.value.destinationField, body.value.expression);
  }
  for (const value of strings) {
    charge = addBoundedCharge(
      charge,
      utf8ByteLength(value, KNOWLEDGE_MANAGER_MAXIMUM_RESPONSE_BYTES),
    );
  }
  return charge;
}
