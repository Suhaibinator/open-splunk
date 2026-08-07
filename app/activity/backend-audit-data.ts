import {
  AuditAction,
  AuditActorKind,
  AuditActorRole,
  AuditTargetKind,
  type AuditEvent,
} from "@/gen/ts/open_splunk/v1/audit";
import type {
  ListAuditEventsRequest,
  ListAuditEventsResponse,
} from "@/gen/ts/open_splunk/v1/audit_api";
import type { SearchAttemptAuditEvent } from "@/gen/ts/open_splunk/v1/search_attempt_audit";
import type {
  ListSearchAttemptAuditEventsRequest,
  ListSearchAttemptAuditEventsResponse,
} from "@/gen/ts/open_splunk/v1/search_attempt_audit_api";
import { isHttpStatus, type ProtobufRequestOptions } from "@/lib/api";

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

const AUDIT_ACTIONS = new Set<AuditAction>([
  AuditAction.AUDIT_ACTION_INGESTION_TOKEN_CREATE,
  AuditAction.AUDIT_ACTION_INGESTION_TOKEN_UPDATE,
  AuditAction.AUDIT_ACTION_INGESTION_TOKEN_REVOKE,
  AuditAction.AUDIT_ACTION_INDEX_CREATE,
  AuditAction.AUDIT_ACTION_INDEX_UPDATE,
  AuditAction.AUDIT_ACTION_INDEX_ACTIVATE,
  AuditAction.AUDIT_ACTION_INDEX_ARCHIVE,
  AuditAction.AUDIT_ACTION_INDEX_DELETE_KEEP_DATA,
  AuditAction.AUDIT_ACTION_INDEX_DELETE_DATA,
  AuditAction.AUDIT_ACTION_APP_CREATE,
  AuditAction.AUDIT_ACTION_APP_UPDATE,
  AuditAction.AUDIT_ACTION_APP_ACTIVATE,
  AuditAction.AUDIT_ACTION_APP_ARCHIVE,
  AuditAction.AUDIT_ACTION_APP_DELETE,
  AuditAction.AUDIT_ACTION_SAVED_SEARCH_CREATE,
  AuditAction.AUDIT_ACTION_SAVED_SEARCH_UPDATE,
  AuditAction.AUDIT_ACTION_SAVED_SEARCH_DUPLICATE,
  AuditAction.AUDIT_ACTION_SAVED_SEARCH_DELETE,
]);

const TARGET_KINDS = new Set<AuditTargetKind>([
  AuditTargetKind.AUDIT_TARGET_KIND_INGESTION_TOKEN,
  AuditTargetKind.AUDIT_TARGET_KIND_INDEX,
  AuditTargetKind.AUDIT_TARGET_KIND_APP,
  AuditTargetKind.AUDIT_TARGET_KIND_SAVED_SEARCH,
]);

const ACTOR_KINDS = new Set<AuditActorKind>([
  AuditActorKind.AUDIT_ACTOR_KIND_SYSTEM,
  AuditActorKind.AUDIT_ACTOR_KIND_BROWSER,
]);

const ACTOR_ROLES = new Set<AuditActorRole>([
  AuditActorRole.AUDIT_ACTOR_ROLE_SYSTEM,
  AuditActorRole.AUDIT_ACTOR_ROLE_USER,
  AuditActorRole.AUDIT_ACTOR_ROLE_ADMINISTRATOR,
]);

function normalizedIdentifier(value: string | undefined): string | undefined {
  return value?.trim() || undefined;
}

function opaquePageToken(value: string | undefined): string | undefined {
  return value === undefined || value.length === 0 ? undefined : value;
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
  if (value === undefined || Number.isNaN(value.valueOf())) {
    throw new TypeError(`${label} did not include a valid occurrence time.`);
  }
  return new Date(value);
}

function assertActor(actorKind: AuditActorKind, actorRole: AuditActorRole, actorId: string, label: string) {
  if (!ACTOR_KINDS.has(actorKind) || !ACTOR_ROLES.has(actorRole) || actorId.trim().length === 0) {
    throw new TypeError(`${label} included an invalid actor projection.`);
  }
}

function adaptMutationEvent(event: AuditEvent): AuditEvent {
  if (event.sequence <= 0n || event.targetVersion <= 0n) {
    throw new TypeError("The mutation audit event included an invalid sequence or target version.");
  }
  requireValidDate(event.occurredAt, "The mutation audit event");
  assertActor(event.actorKind, event.actorRole, event.actorId, "The mutation audit event");
  if (!AUDIT_ACTIONS.has(event.action) || !TARGET_KINDS.has(event.targetKind) || event.targetId.trim().length === 0) {
    throw new TypeError("The mutation audit event included an invalid action or target projection.");
  }
  return { ...event, occurredAt: new Date(event.occurredAt!) };
}

function adaptSearchAttemptEvent(event: SearchAttemptAuditEvent): SearchAttemptAuditEvent {
  if (event.sequence <= 0n || event.ownerId.trim().length === 0 || event.searchJobId.trim().length === 0) {
    throw new TypeError("The search-attempt audit event included an invalid sequence, owner, or job ID.");
  }
  requireValidDate(event.occurredAt, "The search-attempt audit event");
  assertActor(event.actorKind, event.actorRole, event.actorId, "The search-attempt audit event");
  return { ...event, occurredAt: new Date(event.occurredAt!) };
}

function adaptPage<T>(
  items: T[],
  responsePage: ListAuditEventsResponse["page"] | ListSearchAttemptAuditEventsResponse["page"],
  label: string,
): AuditPage<T> {
  if (responsePage === undefined) throw new TypeError(`${label} did not include page metadata.`);
  if (responsePage.totalSize === undefined || !responsePage.totalSizeExact) {
    throw new TypeError(`${label} did not include the requested exact total.`);
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
    nextPageToken: opaquePageToken(responsePage.nextPageToken) ?? null,
    totalSize: responsePage.totalSize,
    totalSizeExact: true,
  };
}

export function buildMutationAuditRequest(
  filters: MutationAuditFilters,
  options: AuditPageOptions,
): ListAuditEventsRequest {
  const actions = [...new Set(filters.actions)].toSorted((left, right) => left - right);
  if (actions.some((action) => !AUDIT_ACTIONS.has(action))) {
    throw new RangeError("Mutation audit action filters contain an unsupported value.");
  }
  if (filters.targetKind !== undefined && !TARGET_KINDS.has(filters.targetKind)) {
    throw new RangeError("Mutation audit target-kind filter contains an unsupported value.");
  }
  return {
    page: pageRequest(options),
    actionFilters: actions,
    actorIdFilter: normalizedIdentifier(filters.actorId),
    targetKindFilter: filters.targetKind,
  };
}

export function buildSearchAttemptAuditRequest(
  filters: SearchAttemptAuditFilters,
  options: AuditPageOptions,
): ListSearchAttemptAuditEventsRequest {
  return {
    page: pageRequest(options),
    actorIdFilter: normalizedIdentifier(filters.actorId),
    ownerIdFilter: normalizedIdentifier(filters.ownerId),
  };
}

export async function listMutationAuditEvents(
  client: AuditListClient,
  filters: MutationAuditFilters,
  options: AuditPageOptions,
): Promise<AuditPage<AuditEvent>> {
  const response = await client.auditEvents.list(buildMutationAuditRequest(filters, options), options);
  return adaptPage(response.auditEvents.map(adaptMutationEvent), response.page, "Mutation audit response");
}

export async function listSearchAttemptAuditEvents(
  client: AuditListClient,
  filters: SearchAttemptAuditFilters,
  options: AuditPageOptions,
): Promise<AuditPage<SearchAttemptAuditEvent>> {
  const response = await client.searchAttemptAudit.list(buildSearchAttemptAuditRequest(filters, options), options);
  return adaptPage(response.events.map(adaptSearchAttemptEvent), response.page, "Search-attempt audit response");
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
  if (kind === AuditTargetKind.AUDIT_TARGET_KIND_INGESTION_TOKEN) return "Ingestion token";
  if (kind === AuditTargetKind.AUDIT_TARGET_KIND_INDEX) return "Index";
  if (kind === AuditTargetKind.AUDIT_TARGET_KIND_APP) return "App";
  if (kind === AuditTargetKind.AUDIT_TARGET_KIND_SAVED_SEARCH) return "Saved search";
  return "Unknown target";
}

const ACTION_LABELS = new Map<AuditAction, string>([
  [AuditAction.AUDIT_ACTION_INGESTION_TOKEN_CREATE, "Ingestion token · create"],
  [AuditAction.AUDIT_ACTION_INGESTION_TOKEN_UPDATE, "Ingestion token · update"],
  [AuditAction.AUDIT_ACTION_INGESTION_TOKEN_REVOKE, "Ingestion token · revoke"],
  [AuditAction.AUDIT_ACTION_INDEX_CREATE, "Index · create"],
  [AuditAction.AUDIT_ACTION_INDEX_UPDATE, "Index · update"],
  [AuditAction.AUDIT_ACTION_INDEX_ACTIVATE, "Index · activate"],
  [AuditAction.AUDIT_ACTION_INDEX_ARCHIVE, "Index · archive"],
  [AuditAction.AUDIT_ACTION_INDEX_DELETE_KEEP_DATA, "Index · delete, keep data"],
  [AuditAction.AUDIT_ACTION_INDEX_DELETE_DATA, "Index · delete data"],
  [AuditAction.AUDIT_ACTION_APP_CREATE, "App · create"],
  [AuditAction.AUDIT_ACTION_APP_UPDATE, "App · update"],
  [AuditAction.AUDIT_ACTION_APP_ACTIVATE, "App · activate"],
  [AuditAction.AUDIT_ACTION_APP_ARCHIVE, "App · archive"],
  [AuditAction.AUDIT_ACTION_APP_DELETE, "App · delete"],
  [AuditAction.AUDIT_ACTION_SAVED_SEARCH_CREATE, "Saved search · create"],
  [AuditAction.AUDIT_ACTION_SAVED_SEARCH_UPDATE, "Saved search · update"],
  [AuditAction.AUDIT_ACTION_SAVED_SEARCH_DUPLICATE, "Saved search · duplicate"],
  [AuditAction.AUDIT_ACTION_SAVED_SEARCH_DELETE, "Saved search · delete"],
]);

export const mutationAuditActionOptions = [...ACTION_LABELS].map(([value, label]) => ({ value, label }));

export function auditActionLabel(action: AuditAction): string {
  return ACTION_LABELS.get(action) ?? "Unknown action";
}
