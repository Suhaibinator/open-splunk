import {
  AuditAction,
  AuditActorKind,
  AuditActorRole,
  AuditTargetKind,
  type AuditEvent,
} from "@/gen/ts/open_splunk/audit";
import { SharingScope } from "@/gen/ts/open_splunk/common";
import { KnowledgeObjectType } from "@/gen/ts/open_splunk/knowledge";
import type {
  ListAuditEventsRequest,
  ListAuditEventsResponse,
} from "@/gen/ts/open_splunk/audit_api";
import type { SearchAttemptAuditEvent } from "@/gen/ts/open_splunk/search_attempt_audit";
import type {
  ListSearchAttemptAuditEventsRequest,
  ListSearchAttemptAuditEventsResponse,
} from "@/gen/ts/open_splunk/search_attempt_audit_api";
import { isHttpStatus, type ProtobufRequestOptions } from "@/lib/api";
import { canonicalBoundedServerText } from "@/lib/search/server-text";

export interface AuditListClient {
  auditEvents: {
    list(request: ListAuditEventsRequest, options?: ProtobufRequestOptions): Promise<ListAuditEventsResponse>;
  };
  searchAttemptAudit: {
    list(
      request: ListSearchAttemptAuditEventsRequest,
      options?: ProtobufRequestOptions,
    ): Promise<ListSearchAttemptAuditEventsResponse>;
  };
}

export interface AuditPage<T> {
  items: T[];
  nextPageToken: string | null;
  totalSize: bigint;
  totalSizeExact: true;
}

export interface MutationAuditFilters {
  actions: readonly AuditAction[];
  actorId?: string;
  targetKind?: AuditTargetKind;
}

export interface SearchAttemptAuditFilters {
  actorId?: string;
  ownerId?: string;
}

export interface AuditPageOptions extends ProtobufRequestOptions {
  pageSize: number;
  pageToken?: string;
}

export interface AuditErrorPresentation {
  title: string;
  message: string;
  invalidTraversal: boolean;
}

const MAXIMUM_AUDIT_EVENTS = 100_000n;
const MAXIMUM_AUDIT_ACTOR_ID_BYTES = 255;
const MAXIMUM_AUDIT_TARGET_ID_BYTES = 128;
const MAXIMUM_KNOWLEDGE_APP_ID_BYTES = 128;
const MAXIMUM_AUDIT_PAGE_TOKEN_BYTES = 2 << 10;
const MAXIMUM_SIGNED_INT64 = 9_223_372_036_854_775_807n;
const UNICODE_EDGE_WHITESPACE_RUN = /^\p{White_Space}+|\p{White_Space}+$/gu;

interface MutationAuditActionSpec {
  readonly label: string;
  readonly targetKind: AuditTargetKind;
  readonly versionPolicy: MutationAuditVersionPolicy;
}

interface MutationAuditVersionPolicy {
  readonly minimum: bigint;
  readonly exact: boolean;
}

const EXACTLY_ONE: MutationAuditVersionPolicy = { minimum: 1n, exact: true };
const AT_LEAST_ONE: MutationAuditVersionPolicy = { minimum: 1n, exact: false };
const AT_LEAST_TWO: MutationAuditVersionPolicy = { minimum: 2n, exact: false };
const AT_LEAST_THREE: MutationAuditVersionPolicy = { minimum: 3n, exact: false };

const ACTION_SPECS: ReadonlyMap<AuditAction, MutationAuditActionSpec> = new Map([
  [AuditAction.AUDIT_ACTION_INGESTION_TOKEN_CREATE, { label: "Ingestion token · create", targetKind: AuditTargetKind.AUDIT_TARGET_KIND_INGESTION_TOKEN, versionPolicy: EXACTLY_ONE }],
  [AuditAction.AUDIT_ACTION_INGESTION_TOKEN_UPDATE, { label: "Ingestion token · update", targetKind: AuditTargetKind.AUDIT_TARGET_KIND_INGESTION_TOKEN, versionPolicy: AT_LEAST_TWO }],
  [AuditAction.AUDIT_ACTION_INGESTION_TOKEN_REVOKE, { label: "Ingestion token · revoke", targetKind: AuditTargetKind.AUDIT_TARGET_KIND_INGESTION_TOKEN, versionPolicy: AT_LEAST_TWO }],
  [AuditAction.AUDIT_ACTION_INDEX_CREATE, { label: "Index · create", targetKind: AuditTargetKind.AUDIT_TARGET_KIND_INDEX, versionPolicy: EXACTLY_ONE }],
  [AuditAction.AUDIT_ACTION_INDEX_UPDATE, { label: "Index · update", targetKind: AuditTargetKind.AUDIT_TARGET_KIND_INDEX, versionPolicy: AT_LEAST_TWO }],
  [AuditAction.AUDIT_ACTION_INDEX_ACTIVATE, { label: "Index · activate", targetKind: AuditTargetKind.AUDIT_TARGET_KIND_INDEX, versionPolicy: AT_LEAST_TWO }],
  [AuditAction.AUDIT_ACTION_INDEX_ARCHIVE, { label: "Index · archive", targetKind: AuditTargetKind.AUDIT_TARGET_KIND_INDEX, versionPolicy: AT_LEAST_TWO }],
  [AuditAction.AUDIT_ACTION_INDEX_DELETE_KEEP_DATA, { label: "Index · delete, keep data", targetKind: AuditTargetKind.AUDIT_TARGET_KIND_INDEX, versionPolicy: AT_LEAST_TWO }],
  [AuditAction.AUDIT_ACTION_INDEX_DELETE_DATA, { label: "Index · delete data", targetKind: AuditTargetKind.AUDIT_TARGET_KIND_INDEX, versionPolicy: AT_LEAST_THREE }],
  [AuditAction.AUDIT_ACTION_APP_CREATE, { label: "App · create", targetKind: AuditTargetKind.AUDIT_TARGET_KIND_APP, versionPolicy: EXACTLY_ONE }],
  [AuditAction.AUDIT_ACTION_APP_UPDATE, { label: "App · update", targetKind: AuditTargetKind.AUDIT_TARGET_KIND_APP, versionPolicy: AT_LEAST_TWO }],
  [AuditAction.AUDIT_ACTION_APP_ACTIVATE, { label: "App · activate", targetKind: AuditTargetKind.AUDIT_TARGET_KIND_APP, versionPolicy: AT_LEAST_TWO }],
  [AuditAction.AUDIT_ACTION_APP_ARCHIVE, { label: "App · archive", targetKind: AuditTargetKind.AUDIT_TARGET_KIND_APP, versionPolicy: AT_LEAST_TWO }],
  [AuditAction.AUDIT_ACTION_APP_DELETE, { label: "App · delete", targetKind: AuditTargetKind.AUDIT_TARGET_KIND_APP, versionPolicy: AT_LEAST_TWO }],
  [AuditAction.AUDIT_ACTION_SAVED_SEARCH_CREATE, { label: "Saved search · create", targetKind: AuditTargetKind.AUDIT_TARGET_KIND_SAVED_SEARCH, versionPolicy: EXACTLY_ONE }],
  [AuditAction.AUDIT_ACTION_SAVED_SEARCH_UPDATE, { label: "Saved search · update", targetKind: AuditTargetKind.AUDIT_TARGET_KIND_SAVED_SEARCH, versionPolicy: AT_LEAST_TWO }],
  [AuditAction.AUDIT_ACTION_SAVED_SEARCH_DUPLICATE, { label: "Saved search · duplicate", targetKind: AuditTargetKind.AUDIT_TARGET_KIND_SAVED_SEARCH, versionPolicy: EXACTLY_ONE }],
  [AuditAction.AUDIT_ACTION_SAVED_SEARCH_DELETE, { label: "Saved search · delete", targetKind: AuditTargetKind.AUDIT_TARGET_KIND_SAVED_SEARCH, versionPolicy: AT_LEAST_ONE }],
  [AuditAction.AUDIT_ACTION_KNOWLEDGE_OBJECT_CREATE, { label: "Knowledge object · create", targetKind: AuditTargetKind.AUDIT_TARGET_KIND_KNOWLEDGE_OBJECT, versionPolicy: EXACTLY_ONE }],
  [AuditAction.AUDIT_ACTION_KNOWLEDGE_OBJECT_UPDATE, { label: "Knowledge object · update", targetKind: AuditTargetKind.AUDIT_TARGET_KIND_KNOWLEDGE_OBJECT, versionPolicy: AT_LEAST_TWO }],
  [AuditAction.AUDIT_ACTION_KNOWLEDGE_OBJECT_SCOPE_CHANGE, { label: "Knowledge object · scope change", targetKind: AuditTargetKind.AUDIT_TARGET_KIND_KNOWLEDGE_OBJECT, versionPolicy: AT_LEAST_TWO }],
  [AuditAction.AUDIT_ACTION_KNOWLEDGE_OBJECT_ENABLE, { label: "Knowledge object · enable", targetKind: AuditTargetKind.AUDIT_TARGET_KIND_KNOWLEDGE_OBJECT, versionPolicy: AT_LEAST_TWO }],
  [AuditAction.AUDIT_ACTION_KNOWLEDGE_OBJECT_DISABLE, { label: "Knowledge object · disable", targetKind: AuditTargetKind.AUDIT_TARGET_KIND_KNOWLEDGE_OBJECT, versionPolicy: AT_LEAST_TWO }],
  [AuditAction.AUDIT_ACTION_KNOWLEDGE_OBJECT_DELETE, { label: "Knowledge object · delete", targetKind: AuditTargetKind.AUDIT_TARGET_KIND_KNOWLEDGE_OBJECT, versionPolicy: AT_LEAST_TWO }],
]);

const TARGET_LABELS: ReadonlyMap<AuditTargetKind, string> = new Map([
  [AuditTargetKind.AUDIT_TARGET_KIND_INGESTION_TOKEN, "Ingestion token"],
  [AuditTargetKind.AUDIT_TARGET_KIND_INDEX, "Index"],
  [AuditTargetKind.AUDIT_TARGET_KIND_APP, "App"],
  [AuditTargetKind.AUDIT_TARGET_KIND_SAVED_SEARCH, "Saved search"],
  [AuditTargetKind.AUDIT_TARGET_KIND_KNOWLEDGE_OBJECT, "Knowledge object"],
]);

const KNOWLEDGE_OBJECT_TYPE_LABELS: ReadonlyMap<KnowledgeObjectType, string> = new Map([
  [KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION, "Field extraction"],
  [KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS, "Field alias"],
  [KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD, "Calculated field"],
]);

const KNOWLEDGE_SHARING_SCOPE_LABELS: ReadonlyMap<SharingScope, string> = new Map([
  [SharingScope.SHARING_SCOPE_PRIVATE, "Private"],
  [SharingScope.SHARING_SCOPE_APP, "App"],
  [SharingScope.SHARING_SCOPE_GLOBAL, "Global"],
]);

export function auditIdentifierFromDraft(value: string | undefined): string | undefined {
  if (value === undefined) return undefined;
  return value.replace(UNICODE_EDGE_WHITESPACE_RUN, "") || undefined;
}

function opaquePageToken(value: string | undefined): string | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "string") {
    throw new TypeError("The audit page token was not a string.");
  }
  if (value.length === 0) return undefined;
  if (!canonicalBoundedText(value, MAXIMUM_AUDIT_PAGE_TOKEN_BYTES)) {
    throw new TypeError("The audit page token was not a canonical bounded value.");
  }
  return value;
}

function canonicalBoundedText(value: string, maximumBytes: number): boolean {
  return canonicalBoundedServerText(value, maximumBytes) !== null;
}

function pageRequest(options: AuditPageOptions) {
  if (!Number.isSafeInteger(options.pageSize) || options.pageSize < 1 || options.pageSize > 200) {
    throw new RangeError("Audit page size must be an integer from 1 through 200.");
  }
  return {
    pageSize: options.pageSize,
    pageToken: opaquePageToken(options.pageToken),
    includeTotalSize: true,
  };
}

function requireValidDate(value: Date | undefined, label: string): Date {
  if (!(value instanceof Date) || Number.isNaN(value.valueOf())) {
    throw new TypeError(`${label} did not include a valid occurrence time.`);
  }
  return new Date(value);
}

function assertActor(
  actorKind: AuditActorKind,
  actorRole: AuditActorRole,
  actorId: string,
  actionTargetKind: AuditTargetKind | null,
  label: string,
) {
  const kindRoleValid = actorKind === AuditActorKind.AUDIT_ACTOR_KIND_SYSTEM
    ? actorRole === AuditActorRole.AUDIT_ACTOR_ROLE_SYSTEM
    : actorKind === AuditActorKind.AUDIT_ACTOR_KIND_BROWSER
      && (actorRole === AuditActorRole.AUDIT_ACTOR_ROLE_USER
        || actorRole === AuditActorRole.AUDIT_ACTOR_ROLE_ADMINISTRATOR);
  const actionActorValid = actionTargetKind === null
    || actorKind !== AuditActorKind.AUDIT_ACTOR_KIND_BROWSER
    || actorRole !== AuditActorRole.AUDIT_ACTOR_ROLE_USER
    || actionTargetKind === AuditTargetKind.AUDIT_TARGET_KIND_SAVED_SEARCH;
  if (
    !kindRoleValid
    || !actionActorValid
    || !canonicalBoundedText(actorId, MAXIMUM_AUDIT_ACTOR_ID_BYTES)
  ) {
    throw new TypeError(`${label} included an invalid actor projection.`);
  }
}

function validActionVersion(
  policy: MutationAuditVersionPolicy,
  version: bigint,
): boolean {
  if (version < 1n || version > MAXIMUM_SIGNED_INT64) return false;
  return version >= policy.minimum
    && (!policy.exact || version === policy.minimum);
}

function validKnowledgeMetadata(event: AuditEvent): boolean {
  if (event.targetKind !== AuditTargetKind.AUDIT_TARGET_KIND_KNOWLEDGE_OBJECT) {
    return event.appId === undefined
      && event.objectType === undefined
      && event.sharingScope === undefined;
  }
  return event.appId !== undefined
    && canonicalBoundedText(event.appId, MAXIMUM_KNOWLEDGE_APP_ID_BYTES)
    && event.objectType !== undefined
    && KNOWLEDGE_OBJECT_TYPE_LABELS.has(event.objectType)
    && event.sharingScope !== undefined
    && KNOWLEDGE_SHARING_SCOPE_LABELS.has(event.sharingScope);
}

function adaptMutationEvent(event: AuditEvent): AuditEvent {
  const actionSpec = ACTION_SPECS.get(event.action);
  if (
    typeof event.sequence !== "bigint"
    || typeof event.targetVersion !== "bigint"
    || event.sequence < 1n
    || event.sequence > MAXIMUM_AUDIT_EVENTS
    || actionSpec === undefined
    || !validActionVersion(actionSpec.versionPolicy, event.targetVersion)
  ) {
    throw new TypeError("The mutation audit event included an invalid sequence or target version.");
  }
  const occurredAt = requireValidDate(event.occurredAt, "The mutation audit event");
  assertActor(
    event.actorKind,
    event.actorRole,
    event.actorId,
    actionSpec.targetKind,
    "The mutation audit event",
  );
  if (
    event.targetKind !== actionSpec.targetKind
    || !canonicalBoundedText(event.targetId, MAXIMUM_AUDIT_TARGET_ID_BYTES)
    || !validKnowledgeMetadata(event)
  ) {
    throw new TypeError("The mutation audit event included an invalid action or target projection.");
  }
  return {
    sequence: event.sequence,
    occurredAt,
    actorKind: event.actorKind,
    actorId: `${event.actorId}`,
    actorRole: event.actorRole,
    action: event.action,
    targetKind: event.targetKind,
    targetId: `${event.targetId}`,
    targetVersion: event.targetVersion,
    ...(event.targetKind === AuditTargetKind.AUDIT_TARGET_KIND_KNOWLEDGE_OBJECT
      ? {
          appId: `${event.appId}`,
          objectType: event.objectType,
          sharingScope: event.sharingScope,
        }
      : {}),
  };
}

function adaptSearchAttemptEvent(event: SearchAttemptAuditEvent): SearchAttemptAuditEvent {
  if (
    typeof event.sequence !== "bigint"
    || typeof event.ownerId !== "string"
    || typeof event.searchJobId !== "string"
    || event.sequence <= 0n
    || event.ownerId.trim().length === 0
    || event.searchJobId.trim().length === 0
  ) {
    throw new TypeError("The search-attempt audit event included an invalid sequence, owner, or job ID.");
  }
  requireValidDate(event.occurredAt, "The search-attempt audit event");
  assertActor(
    event.actorKind,
    event.actorRole,
    event.actorId,
    null,
    "The search-attempt audit event",
  );
  return { ...event, occurredAt: new Date(event.occurredAt!) };
}

function adaptPage<T>(
  items: T[],
  responsePage: ListAuditEventsResponse["page"] | ListSearchAttemptAuditEventsResponse["page"],
  label: string,
  options: {
    pageSize: number;
    requestedPageToken: string | undefined;
    maximumTotal?: bigint;
  },
): AuditPage<T> {
  if (responsePage === undefined) throw new TypeError(`${label} did not include page metadata.`);
  if (typeof responsePage.totalSize !== "bigint" || responsePage.totalSizeExact !== true) {
    throw new TypeError(`${label} did not include the requested exact total.`);
  }
  const nextPageToken = opaquePageToken(responsePage.nextPageToken) ?? null;
  if (
    items.length > options.pageSize
    || responsePage.totalSize < BigInt(items.length)
    || (options.maximumTotal !== undefined && responsePage.totalSize > options.maximumTotal)
    || (nextPageToken !== null && (
      items.length !== options.pageSize
      || nextPageToken === options.requestedPageToken
      || responsePage.totalSize <= BigInt(items.length)
    ))
    || (
      options.requestedPageToken === undefined
      && nextPageToken === null
      && responsePage.totalSize !== BigInt(items.length)
    )
  ) {
    throw new TypeError(`${label} included an invalid item count or exact total.`);
  }
  for (let index = 1; index < items.length; index += 1) {
    const previous = items[index - 1] as { sequence: bigint };
    const current = items[index] as { sequence: bigint };
    if (previous.sequence <= current.sequence) {
      throw new TypeError(`${label} was not ordered by descending sequence.`);
    }
  }
  return {
    items,
    nextPageToken,
    totalSize: responsePage.totalSize,
    totalSizeExact: true,
  };
}

function assertResponseItemCount(
  items: readonly unknown[],
  requestedPageSize: number,
  label: string,
): void {
  if (!Array.isArray(items) || items.length > requestedPageSize) {
    throw new TypeError(`${label} exceeded the requested page size.`);
  }
}

export function buildMutationAuditRequest(
  filters: MutationAuditFilters,
  options: AuditPageOptions,
): ListAuditEventsRequest {
  const actions = [...new Set(filters.actions)].toSorted((left, right) => left - right);
  if (actions.some((action) => !ACTION_SPECS.has(action))) {
    throw new RangeError("Mutation audit action filters contain an unsupported value.");
  }
  if (filters.targetKind !== undefined && !TARGET_LABELS.has(filters.targetKind)) {
    throw new RangeError("Mutation audit target-kind filter contains an unsupported value.");
  }
  const actorIdFilter = auditIdentifierFromDraft(filters.actorId);
  if (
    actorIdFilter !== undefined
    && !canonicalBoundedText(actorIdFilter, MAXIMUM_AUDIT_ACTOR_ID_BYTES)
  ) {
    throw new RangeError("Mutation audit actor filter contains an invalid identifier.");
  }
  return {
    page: pageRequest(options),
    actionFilters: actions,
    actorIdFilter,
    targetKindFilter: filters.targetKind,
  };
}

export function buildSearchAttemptAuditRequest(
  filters: SearchAttemptAuditFilters,
  options: AuditPageOptions,
): ListSearchAttemptAuditEventsRequest {
  return {
    page: pageRequest(options),
    actorIdFilter: auditIdentifierFromDraft(filters.actorId),
    ownerIdFilter: auditIdentifierFromDraft(filters.ownerId),
  };
}

export async function listMutationAuditEvents(
  client: AuditListClient,
  filters: MutationAuditFilters,
  options: AuditPageOptions,
): Promise<AuditPage<AuditEvent>> {
  const request = buildMutationAuditRequest(filters, options);
  const pageSize = options.pageSize;
  const requestedPageToken = request.page?.pageToken;
  const response = await client.auditEvents.list(request, options);
  assertResponseItemCount(response.auditEvents, pageSize, "Mutation audit response");
  return adaptPage(
    response.auditEvents.map(adaptMutationEvent),
    response.page,
    "Mutation audit response",
    { pageSize, requestedPageToken, maximumTotal: MAXIMUM_AUDIT_EVENTS },
  );
}

export async function listSearchAttemptAuditEvents(
  client: AuditListClient,
  filters: SearchAttemptAuditFilters,
  options: AuditPageOptions,
): Promise<AuditPage<SearchAttemptAuditEvent>> {
  const request = buildSearchAttemptAuditRequest(filters, options);
  const pageSize = options.pageSize;
  const requestedPageToken = request.page?.pageToken;
  const response = await client.searchAttemptAudit.list(request, options);
  assertResponseItemCount(response.events, pageSize, "Search-attempt audit response");
  return adaptPage(
    response.events.map(adaptSearchAttemptEvent),
    response.page,
    "Search-attempt audit response",
    { pageSize, requestedPageToken, maximumTotal: MAXIMUM_AUDIT_EVENTS },
  );
}

export function auditErrorPresentation(error: unknown, subject: string): AuditErrorPresentation {
  if (isHttpStatus(error, 400)) {
    return {
      title: `${subject} traversal is no longer valid`,
      message: "The server rejected this filtered cursor because it is invalid, expired, or was evicted. Refresh to begin a new traversal.",
      invalidTraversal: true,
    };
  }
  if (isHttpStatus(error, 401)) {
    return {
      title: "Authentication is required",
      message: `Sign in with an administrator session before reading ${subject.toLowerCase()}.`,
      invalidTraversal: false,
    };
  }
  if (isHttpStatus(error, 403)) {
    return {
      title: "Administrator access is required",
      message: `The current principal is not authorized to read ${subject.toLowerCase()}.`,
      invalidTraversal: false,
    };
  }
  if (
    isHttpStatus(error, 404)
    || isHttpStatus(error, 405)
    || isHttpStatus(error, 501)
  ) {
    return {
      title: `${subject} route is unavailable`,
      message: "The backend advertised this audit feature but did not register a compatible list route.",
      invalidTraversal: false,
    };
  }
  if (subject === "Mutation audit" && isHttpStatus(error, 429)) {
    return {
      title: "Audit capacity limit reached",
      message: "The mutation audit store is at capacity. Resolve the server capacity condition before retrying.",
      invalidTraversal: false,
    };
  }
  if (isHttpStatus(error, 503)) {
    return {
      title: `${subject} is unavailable`,
      message: "The server reports that the audit store is unavailable or corrupt. Check server diagnostics before retrying.",
      invalidTraversal: false,
    };
  }
  return {
    title: `${subject} could not be loaded`,
    message: error instanceof Error && error.message.trim() ? error.message : "The server did not return a usable audit response.",
    invalidTraversal: false,
  };
}

export function auditActorKindLabel(kind: AuditActorKind): string {
  if (kind === AuditActorKind.AUDIT_ACTOR_KIND_SYSTEM) return "System";
  if (kind === AuditActorKind.AUDIT_ACTOR_KIND_BROWSER) return "Browser";
  return "Unknown actor kind";
}

export function auditActorRoleLabel(role: AuditActorRole): string {
  if (role === AuditActorRole.AUDIT_ACTOR_ROLE_SYSTEM) return "System";
  if (role === AuditActorRole.AUDIT_ACTOR_ROLE_USER) return "User";
  if (role === AuditActorRole.AUDIT_ACTOR_ROLE_ADMINISTRATOR) return "Administrator";
  return "Unknown role";
}

export function auditTargetKindLabel(kind: AuditTargetKind): string {
  return TARGET_LABELS.get(kind) ?? "Unknown target";
}

export const mutationAuditActionOptions = [...ACTION_SPECS].map(([value, spec]) => ({
  value,
  label: spec.label,
}));

export const mutationAuditTargetOptions = [...TARGET_LABELS].map(([value, label]) => ({
  value,
  label,
}));

export function auditActionLabel(action: AuditAction): string {
  return ACTION_SPECS.get(action)?.label ?? "Unknown action";
}

export function auditKnowledgeObjectTypeLabel(objectType: KnowledgeObjectType): string {
  return KNOWLEDGE_OBJECT_TYPE_LABELS.get(objectType) ?? "Unknown object type";
}

export function auditKnowledgeSharingScopeLabel(sharingScope: SharingScope): string {
  return KNOWLEDGE_SHARING_SCOPE_LABELS.get(sharingScope) ?? "Unknown sharing scope";
}

export function auditTargetDetails(event: AuditEvent): string | null {
  if (
    event.targetKind !== AuditTargetKind.AUDIT_TARGET_KIND_KNOWLEDGE_OBJECT
    || event.appId === undefined
    || event.objectType === undefined
    || event.sharingScope === undefined
  ) return null;
  return `App: ${event.appId} · Type: ${auditKnowledgeObjectTypeLabel(event.objectType)} · Sharing: ${auditKnowledgeSharingScopeLabel(event.sharingScope)}`;
}
