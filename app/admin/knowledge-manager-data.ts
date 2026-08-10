import { SortDirection, SharingScope } from "@/gen/ts/open_splunk/v1/common";
import {
  KnowledgeDependencyRole,
  KnowledgeObjectState,
  KnowledgeObjectType,
  KnowledgeOverwriteBehavior,
  KnowledgeSelectorMatchKind,
  type KnowledgeObject,
  type KnowledgeObjectDefinition,
  type KnowledgeSelector,
} from "@/gen/ts/open_splunk/v1/knowledge";
import {
  GetKnowledgeObjectRequest,
  KnowledgeObjectSortBy,
  ListKnowledgeObjectDependenciesRequest,
  ListKnowledgeObjectDependentsRequest,
  ListKnowledgeObjectsRequest,
  type GetKnowledgeObjectResponse as GetKnowledgeObjectResponseMessage,
  type KnowledgeManagementDependencyEdge,
  type KnowledgeManagementObjectVersionIdentity,
  type ListKnowledgeObjectDependenciesResponse as ListKnowledgeObjectDependenciesResponseMessage,
  type ListKnowledgeObjectDependentsResponse as ListKnowledgeObjectDependentsResponseMessage,
  type ListKnowledgeObjectsResponse as ListKnowledgeObjectsResponseMessage,
} from "@/gen/ts/open_splunk/v1/knowledge_api";
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

function utf8ByteLength(value: string, stopAfter = Number.MAX_SAFE_INTEGER): number {
  let bytes = 0;
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code <= 0x7f) bytes += 1;
    else if (code <= 0x7ff) bytes += 2;
    else if (code >= 0xd800 && code <= 0xdbff) {
      const low = value.charCodeAt(index + 1);
      if (low < 0xdc00 || low > 0xdfff) return Number.MAX_SAFE_INTEGER;
      bytes += 4;
      index += 1;
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      return Number.MAX_SAFE_INTEGER;
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
