"use client";

import type { FormEvent } from "react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";

import { SortDirection } from "@/gen/ts/open_splunk/v1/common";
import {
  IngestionTokenState,
  type IngestionToken,
} from "@/gen/ts/open_splunk/v1/collector_admin";
import { IngestionTokenSortBy } from "@/gen/ts/open_splunk/v1/collector_admin_api";
import {
  IndexAccessState,
  IndexState,
  type Index,
} from "@/gen/ts/open_splunk/v1/index";
import { IndexSortBy } from "@/gen/ts/open_splunk/v1/index_api";
import {
  DEFAULT_REQUEST_TIMEOUT_MS,
  createOpenSplunkApiClient,
  getSystemBootstrap,
  isAdvertisedFeatureRouteUnavailable,
  isHttpError,
  isHttpStatus,
  isOptionalRouteUnavailable,
  type OpenSplunkApiClient,
  type SystemBootstrapModel,
} from "@/lib/api";
import { searchLaunchHref } from "@/lib/search/launch-url";

import { PageHeading } from "../_components/product-shell";
import { Modal } from "../search-workspace/modal";

type AdminSection = "overview" | "indexes" | "collectors" | "access" | "server";
type AdminModal = "create-index" | "edit-index" | "create-token" | "edit-token";
type ResourceState = "loading" | "available" | "unavailable" | "error";

interface BackendAdminConsoleProps {
  apiBaseUrl: string;
}

interface IndexLoadResult {
  state: Exclude<ResourceState, "loading">;
  indexes: Index[];
  nextPageToken: string | null;
  totalSize: bigint | null;
  totalSizeExact: boolean;
  message?: string;
}

interface TokenLoadResult {
  state: Exclude<ResourceState, "loading">;
  tokens: IngestionToken[];
  nextPageToken: string | null;
  totalSize: bigint | null;
  totalSizeExact: boolean;
  message?: string;
}

interface AdminToast {
  message: string;
  kind: "success" | "warning";
}

interface TokenIndexScopeOption {
  id: string;
  name: string;
  displayName: string;
  ingestible: boolean;
}

type TokenScopeSource = "index-admin" | "bootstrap" | "unavailable";

interface ServerClockAnchor {
  serverTimeMs: number;
  clientMonotonicMs: number;
  uncertaintyMs: number;
}

type TokenCreateOutcomeKind = "pending" | "settled-response" | "ambiguous-failure";

interface TokenCreateDefinitionSnapshot {
  name: string;
  description: string;
  allowedIndexNames: string[];
  expiresAt: Date | undefined;
  armedServerTimeMs: number;
  dispatchedServerTimeMs: number | null;
  outcomeObservedServerTimeMs: number | null;
  requestRoundTripMs: number | null;
  requestTimeoutMs: number;
  clockUncertaintyMs: number;
  outcomeKind: TokenCreateOutcomeKind;
}

interface TokenCreateRecovery {
  attemptId: string;
  ownerId: string;
  definition: TokenCreateDefinitionSnapshot;
  preexistingTokenIds: ReadonlySet<string>;
  confirmedRevokedTokenIds: ReadonlySet<string>;
  failureMessage: string;
  candidates: IngestionToken[];
  reconciliationError: string | null;
}

type TokenCreateGuardMode = "ambiguous" | "issued";
type TokenCreateGuardStorageState = "checking" | "available" | "unavailable";

interface PersistedTokenCreateGuardV1 {
  schemaVersion: 1;
  apiBaseUrl: string;
  attemptId: string;
  ownerId: string;
  mode: TokenCreateGuardMode;
  definition: {
    name: string;
    description: string;
    allowedIndexNames: string[];
    expiresAt: string | null;
    armedServerTimeMs: number;
    dispatchedServerTimeMs: number | null;
    outcomeObservedServerTimeMs: number | null;
    requestRoundTripMs: number | null;
    requestTimeoutMs: number;
    clockUncertaintyMs: number;
    outcomeKind: TokenCreateOutcomeKind;
  };
  preexistingTokenIds: string[];
  confirmedRevokedTokenIds: string[];
  failureMessage: string;
  knownIssuedTokenId: string | null;
}

const NAV_ITEMS: Array<{ key: AdminSection; label: string; detail: string; icon: string }> = [
  { key: "overview", label: "System overview", detail: "Capabilities and limits", icon: "▥" },
  { key: "indexes", label: "Indexes", detail: "State and retention", icon: "▦" },
  { key: "collectors", label: "Data inputs", detail: "Ingestion tokens", icon: "⇣" },
  { key: "access", label: "Users & access", detail: "Not exposed by this server", icon: "♙" },
  { key: "server", label: "Server settings", detail: "Read-only limits", icon: "⚙" },
];

const TOKEN_HISTORY_GUARD_KEY = "__openSplunkTokenGuard";
const TOKEN_CREATE_GUARD_STORAGE_PREFIX = "open-splunk.admin.token-create-guard.v1";
const TOKEN_CREATE_LOCK_PREFIX = "open-splunk.admin.token-create-lock.v1";
const TOKEN_CREATE_CLOCK_EPSILON_MS = 250;

function normalizeApiBaseUrl(apiBaseUrl: string, pageOrigin: string): string {
  const url = new URL(apiBaseUrl.trim() || "/", pageOrigin);
  if (
    url.username.length > 0
    || url.password.length > 0
    || url.search.length > 0
    || url.hash.length > 0
  ) {
    throw new Error("The API base URL cannot contain credentials, a query, or a fragment.");
  }
  const pathname = url.pathname.replace(/\/+$/, "") || "/";
  return `${url.origin}${pathname}`;
}

function tokenCreateGuardStorageKey(normalizedApiBaseUrl: string): string {
  return `${TOKEN_CREATE_GUARD_STORAGE_PREFIX}:${encodeURIComponent(normalizedApiBaseUrl)}`;
}

function tokenCreateLockName(normalizedApiBaseUrl: string): string {
  return `${TOKEN_CREATE_LOCK_PREFIX}:${normalizedApiBaseUrl}`;
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === "string");
}

function serializeTokenCreateGuard(
  normalizedApiBaseUrl: string,
  recovery: TokenCreateRecovery,
  knownIssuedTokenId: string | null,
): PersistedTokenCreateGuardV1 {
  return {
    schemaVersion: 1,
    apiBaseUrl: normalizedApiBaseUrl,
    attemptId: recovery.attemptId,
    ownerId: recovery.ownerId,
    mode: knownIssuedTokenId === null ? "ambiguous" : "issued",
    definition: {
      name: recovery.definition.name,
      description: recovery.definition.description,
      allowedIndexNames: [...recovery.definition.allowedIndexNames],
      expiresAt: recovery.definition.expiresAt?.toISOString() ?? null,
      armedServerTimeMs: recovery.definition.armedServerTimeMs,
      dispatchedServerTimeMs: recovery.definition.dispatchedServerTimeMs,
      outcomeObservedServerTimeMs: recovery.definition.outcomeObservedServerTimeMs,
      requestRoundTripMs: recovery.definition.requestRoundTripMs,
      requestTimeoutMs: recovery.definition.requestTimeoutMs,
      clockUncertaintyMs: recovery.definition.clockUncertaintyMs,
      outcomeKind: recovery.definition.outcomeKind,
    },
    preexistingTokenIds: [...recovery.preexistingTokenIds],
    confirmedRevokedTokenIds: [...recovery.confirmedRevokedTokenIds],
    failureMessage: knownIssuedTokenId === null
      ? "The browser did not observe a trustworthy final outcome for this token creation request."
      : "The token was issued, but its one-time secret is intentionally not stored by the browser.",
    knownIssuedTokenId,
  };
}

function parsePersistedTokenCreateGuard(
  raw: string,
  normalizedApiBaseUrl: string,
): { recovery: TokenCreateRecovery; knownIssuedTokenId: string | null } | null {
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch {
    return null;
  }
  if (typeof value !== "object" || value === null) return null;
  const record = value as Partial<PersistedTokenCreateGuardV1>;
  const definition = record.definition;
  if (
    record.schemaVersion !== 1
    || record.apiBaseUrl !== normalizedApiBaseUrl
    || typeof record.attemptId !== "string"
    || record.attemptId.length === 0
    || typeof record.ownerId !== "string"
    || record.ownerId.length === 0
    || (record.mode !== "ambiguous" && record.mode !== "issued")
    || typeof definition !== "object"
    || definition === null
    || typeof definition.name !== "string"
    || definition.name.length === 0
    || typeof definition.description !== "string"
    || !isStringArray(definition.allowedIndexNames)
    || definition.allowedIndexNames.length === 0
    || new Set(definition.allowedIndexNames).size !== definition.allowedIndexNames.length
    || !(definition.expiresAt === null || typeof definition.expiresAt === "string")
    || !isFiniteNumber(definition.armedServerTimeMs)
    || !(definition.dispatchedServerTimeMs === null
      || isFiniteNumber(definition.dispatchedServerTimeMs))
    || !(definition.outcomeObservedServerTimeMs === null
      || isFiniteNumber(definition.outcomeObservedServerTimeMs))
    || !(definition.requestRoundTripMs === null
      || (isFiniteNumber(definition.requestRoundTripMs) && definition.requestRoundTripMs >= 0))
    || !isFiniteNumber(definition.requestTimeoutMs)
    || definition.requestTimeoutMs <= 0
    || !isFiniteNumber(definition.clockUncertaintyMs)
    || definition.clockUncertaintyMs < 0
    || (
      definition.outcomeKind !== "pending"
      && definition.outcomeKind !== "settled-response"
      && definition.outcomeKind !== "ambiguous-failure"
    )
    || !isStringArray(record.preexistingTokenIds)
    || new Set(record.preexistingTokenIds).size !== record.preexistingTokenIds.length
    || !isStringArray(record.confirmedRevokedTokenIds)
    || new Set(record.confirmedRevokedTokenIds).size !== record.confirmedRevokedTokenIds.length
    || typeof record.failureMessage !== "string"
    || !(record.knownIssuedTokenId === null || typeof record.knownIssuedTokenId === "string")
    || (record.mode === "issued" && !record.knownIssuedTokenId)
    || (record.mode === "ambiguous" && record.knownIssuedTokenId !== null)
  ) {
    return null;
  }
  const expiresAt = definition.expiresAt === null ? undefined : new Date(definition.expiresAt);
  if (expiresAt !== undefined && Number.isNaN(expiresAt.valueOf())) return null;
  return {
    recovery: {
      attemptId: record.attemptId,
      ownerId: record.ownerId,
      definition: {
        name: definition.name,
        description: definition.description,
        allowedIndexNames: [...new Set(definition.allowedIndexNames)].toSorted(),
        expiresAt,
        armedServerTimeMs: definition.armedServerTimeMs,
        dispatchedServerTimeMs: definition.dispatchedServerTimeMs,
        outcomeObservedServerTimeMs: definition.outcomeObservedServerTimeMs,
        requestRoundTripMs: definition.requestRoundTripMs,
        requestTimeoutMs: definition.requestTimeoutMs,
        clockUncertaintyMs: definition.clockUncertaintyMs,
        outcomeKind: definition.outcomeKind,
      },
      preexistingTokenIds: new Set(record.preexistingTokenIds),
      confirmedRevokedTokenIds: new Set(record.confirmedRevokedTokenIds),
      failureMessage: record.failureMessage
        || "The browser did not observe the final outcome of this token creation request.",
      candidates: [],
      reconciliationError: null,
    },
    knownIssuedTokenId: record.knownIssuedTokenId,
  };
}

function historyHasTokenGuard(guardId: string): boolean {
  const state: unknown = window.history.state;
  return typeof state === "object"
    && state !== null
    && TOKEN_HISTORY_GUARD_KEY in state
    && (state as Record<string, unknown>)[TOKEN_HISTORY_GUARD_KEY] === guardId;
}

function historyStateWithTokenGuard(guardId: string): Record<string, unknown> {
  const state: unknown = window.history.state;
  return {
    ...(typeof state === "object" && state !== null ? state : {}),
    [TOKEN_HISTORY_GUARD_KEY]: guardId,
  };
}

function errorMessage(error: unknown): string {
  if (error instanceof Error && error.message.trim().length > 0) return error.message;
  return "The server did not return a usable response.";
}

function formatDate(value: Date | undefined): string {
  if (value === undefined || Number.isNaN(value.valueOf())) return "Never";
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(value);
}

function formatDuration(seconds: bigint | undefined): string {
  if (seconds === undefined || seconds <= 0n) return "Forever";
  const days = seconds / 86_400n;
  if (days > 0n && seconds % 86_400n === 0n) return `${days.toLocaleString()} days`;
  const hours = seconds / 3_600n;
  if (hours > 0n && seconds % 3_600n === 0n) return `${hours.toLocaleString()} hours`;
  return `${seconds.toLocaleString()} seconds`;
}

function retentionFormValue(seconds: bigint | undefined): string {
  if (seconds === undefined || seconds <= 0n) return "forever";
  if (seconds % 86_400n === 0n) return (seconds / 86_400n).toString();
  return `seconds:${seconds}`;
}

function retentionFromForm(value: string): { seconds: bigint; nanos: number } | undefined {
  if (value === "forever") return undefined;
  if (value.startsWith("seconds:")) {
    return { seconds: BigInt(value.slice("seconds:".length)), nanos: 0 };
  }
  return { seconds: BigInt(value) * 86_400n, nanos: 0 };
}

function dateTimeLocalValue(value: Date | undefined): string {
  if (value === undefined || Number.isNaN(value.valueOf())) return "";
  const localValue = new Date(value.valueOf() - value.getTimezoneOffset() * 60_000);
  return localValue.toISOString().slice(0, 16);
}

function expirationFromForm(value: string, authoritativeNowMs?: number): Date | undefined {
  if (value.trim().length === 0) return undefined;
  const expiresAt = new Date(value);
  if (Number.isNaN(expiresAt.valueOf())) throw new Error("Enter a valid token expiration.");
  if (authoritativeNowMs !== undefined && expiresAt.valueOf() <= authoritativeNowMs) {
    throw new Error("Token expiration must be in the future according to the server clock.");
  }
  return expiresAt;
}

function hasSameStrings(left: Iterable<string>, right: Iterable<string>): boolean {
  const leftValues = [...left].toSorted();
  const rightValues = [...right].toSorted();
  return leftValues.length === rightValues.length
    && leftValues.every((value, index) => value === rightValues[index]);
}

function indexStateLabel(state: IndexState): string {
  if (state === IndexState.INDEX_STATE_ACTIVE) return "Active";
  if (state === IndexState.INDEX_STATE_ARCHIVED) return "Archived";
  if (state === IndexState.INDEX_STATE_DELETING) return "Deleting";
  return "Unknown";
}

function indexAccessLabel(state: IndexAccessState | undefined): string {
  if (state === IndexAccessState.INDEX_ACCESS_STATE_ENABLED) return "Enabled";
  if (state === IndexAccessState.INDEX_ACCESS_STATE_DISABLED) return "Disabled";
  return "Unknown";
}

function tokenStateLabel(state: IngestionTokenState): string {
  if (state === IngestionTokenState.INGESTION_TOKEN_STATE_ACTIVE) return "Active";
  if (state === IngestionTokenState.INGESTION_TOKEN_STATE_DISABLED) return "Disabled";
  if (state === IngestionTokenState.INGESTION_TOKEN_STATE_REVOKED) return "Revoked";
  if (state === IngestionTokenState.INGESTION_TOKEN_STATE_EXPIRED) return "Expired";
  return "Unknown";
}

function tokenCanBeRevoked(token: IngestionToken): boolean {
  return token.state === IngestionTokenState.INGESTION_TOKEN_STATE_ACTIVE
    || token.state === IngestionTokenState.INGESTION_TOKEN_STATE_DISABLED;
}

function tokenIsTerminallySafe(token: IngestionToken): boolean {
  return token.state === IngestionTokenState.INGESTION_TOKEN_STATE_REVOKED
    || token.state === IngestionTokenState.INGESTION_TOKEN_STATE_EXPIRED;
}

function isDefiniteTokenCreateFailure(error: unknown): boolean {
  if (!isHttpError(error)) return false;
  return [
    400,
    401,
    403,
    404,
    405,
    409,
    410,
    422,
    501,
  ].includes(error.status);
}

function normalizedPageToken(value: string | undefined): string | null {
  return value?.trim() || null;
}

function countLabel(
  loaded: number,
  totalSize: bigint | null,
  totalSizeExact: boolean,
  singular: string,
  plural: string,
): string {
  const loadedLabel = loaded === 1 ? singular : plural;
  if (totalSize !== null && totalSizeExact) {
    const totalLabel = totalSize === 1n ? singular : plural;
    return BigInt(loaded) < totalSize
      ? `${loaded.toLocaleString()} of ${totalSize.toLocaleString()} ${totalLabel} loaded`
      : `${totalSize.toLocaleString()} ${totalLabel}`;
  }
  if (totalSize !== null) {
    return `${loaded.toLocaleString()} ${loadedLabel} loaded · server estimate ${totalSize.toLocaleString()}`;
  }
  return `${loaded.toLocaleString()} ${loadedLabel} loaded`;
}

function tokenMatchesCreateMetadata(
  token: IngestionToken,
  definition: TokenCreateDefinitionSnapshot,
): boolean {
  const constraints = token.constraints;
  if (
    token.name !== definition.name
    || (token.description ?? "") !== definition.description
    || constraints === undefined
    || !hasSameStrings(constraints.allowedIndexNames, definition.allowedIndexNames)
    || constraints.allowedHostRegexes.length !== 0
    || constraints.allowedSourceRegexes.length !== 0
    || constraints.boundCollectorId !== undefined
    || (token.expiresAt?.valueOf() ?? null) !== (definition.expiresAt?.valueOf() ?? null)
  ) {
    return false;
  }
  return true;
}

function tokenFallsWithinCreateAttributionWindow(
  token: IngestionToken,
  definition: TokenCreateDefinitionSnapshot,
): boolean {
  if (definition.dispatchedServerTimeMs === null) return false;
  const createdAtMs = token.createdAt?.valueOf();
  const timingToleranceMs = Math.max(
    TOKEN_CREATE_CLOCK_EPSILON_MS,
    definition.clockUncertaintyMs,
  );
  const observedServerTimeMs = definition.outcomeObservedServerTimeMs
    ?? definition.dispatchedServerTimeMs;
  const upperBoundServerTimeMs = definition.outcomeKind === "settled-response"
    ? definition.outcomeObservedServerTimeMs
    : Math.max(
        observedServerTimeMs,
        definition.dispatchedServerTimeMs + definition.requestTimeoutMs,
      );
  if (upperBoundServerTimeMs === null) return false;
  return createdAtMs !== undefined
    && createdAtMs >= definition.dispatchedServerTimeMs - timingToleranceMs
    && createdAtMs <= upperBoundServerTimeMs + timingToleranceMs;
}

function tokenMatchesCreateDefinition(
  token: IngestionToken,
  definition: TokenCreateDefinitionSnapshot,
): boolean {
  return tokenMatchesCreateMetadata(token, definition)
    && tokenFallsWithinCreateAttributionWindow(token, definition);
}

function statusClass(label: string): string {
  if (label === "Active") return "complete";
  if (label === "Deleting") return "running";
  if (label === "Unknown") return "warning";
  return "neutral";
}

async function loadIndexPage(
  client: OpenSplunkApiClient,
  pageToken: string | undefined,
  signal: AbortSignal,
): Promise<IndexLoadResult> {
  try {
    const response = await client.indexes.list({
      page: { pageSize: undefined, pageToken, includeTotalSize: true },
      stateFilters: [],
      textFilter: undefined,
      sortBy: IndexSortBy.INDEX_SORT_BY_NAME,
      sortDirection: SortDirection.SORT_DIRECTION_ASCENDING,
      includeStats: false,
    }, { signal });
    const indexes: Index[] = [];
    for (const item of response.indexes) {
      if (item.index !== undefined) indexes.push(item.index);
    }
    return {
      state: "available",
      indexes,
      nextPageToken: normalizedPageToken(response.page?.nextPageToken),
      totalSize: response.page?.totalSize ?? null,
      totalSizeExact: response.page?.totalSizeExact ?? false,
    };
  } catch (error) {
    if (isOptionalRouteUnavailable(error)) {
      return {
        state: "unavailable",
        indexes: [],
        nextPageToken: null,
        totalSize: null,
        totalSizeExact: false,
      };
    }
    if (signal.aborted) throw error;
    return {
      state: "error",
      indexes: [],
      nextPageToken: null,
      totalSize: null,
      totalSizeExact: false,
      message: errorMessage(error),
    };
  }
}

async function loadTokenPage(
  client: OpenSplunkApiClient,
  pageToken: string | undefined,
  signal: AbortSignal,
): Promise<TokenLoadResult> {
  try {
    const response = await client.ingestionTokens.list({
      page: { pageSize: undefined, pageToken, includeTotalSize: true },
      stateFilters: [],
      indexNameFilter: undefined,
      textFilter: undefined,
      sortBy: IngestionTokenSortBy.INGESTION_TOKEN_SORT_BY_NAME,
      sortDirection: SortDirection.SORT_DIRECTION_ASCENDING,
    }, { signal });
    return {
      state: "available",
      tokens: response.ingestionTokens,
      nextPageToken: normalizedPageToken(response.page?.nextPageToken),
      totalSize: response.page?.totalSize ?? null,
      totalSizeExact: response.page?.totalSizeExact ?? false,
    };
  } catch (error) {
    if (isOptionalRouteUnavailable(error)) {
      return {
        state: "unavailable",
        tokens: [],
        nextPageToken: null,
        totalSize: null,
        totalSizeExact: false,
      };
    }
    if (signal.aborted) throw error;
    return {
      state: "error",
      tokens: [],
      nextPageToken: null,
      totalSize: null,
      totalSizeExact: false,
      message: errorMessage(error),
    };
  }
}

async function listTokensForCreateSafety(
  client: OpenSplunkApiClient,
  tokenName: string,
  signal?: AbortSignal,
): Promise<IngestionToken[]> {
  const tokens: IngestionToken[] = [];
  const tokenIds = new Set<string>();
  const seenCursors = new Set<string>();
  let expectedTotal: bigint | null = null;
  let pageToken: string | undefined;
  for (;;) {
    // This complete, name-filtered snapshot is a safety prerequisite for a
    // non-idempotent secret-issuing request, not the Admin table loading path.
    // eslint-disable-next-line no-await-in-loop
    const response = await client.ingestionTokens.list({
      page: { pageSize: undefined, pageToken, includeTotalSize: true },
      stateFilters: [],
      indexNameFilter: undefined,
      textFilter: tokenName,
      sortBy: IngestionTokenSortBy.INGESTION_TOKEN_SORT_BY_CREATED_AT,
      sortDirection: SortDirection.SORT_DIRECTION_DESCENDING,
    }, { signal });
    if (
      response.page?.totalSize === undefined
      || !response.page.totalSizeExact
      || (expectedTotal !== null && response.page.totalSize !== expectedTotal)
    ) {
      throw new Error("The server did not return a stable exact token count for safe creation.");
    }
    expectedTotal = response.page.totalSize;
    for (const token of response.ingestionTokens) {
      if (token.ingestionTokenId.length === 0 || tokenIds.has(token.ingestionTokenId)) {
        throw new Error("The token snapshot contained a missing or duplicate token identifier.");
      }
      tokenIds.add(token.ingestionTokenId);
      tokens.push(token);
    }
    const next = normalizedPageToken(response.page.nextPageToken);
    if (next === null) break;
    if (seenCursors.has(next)) {
      throw new Error("The token snapshot returned a repeated page cursor.");
    }
    seenCursors.add(next);
    pageToken = next;
  }
  if (expectedTotal === null || BigInt(tokens.length) !== expectedTotal) {
    throw new Error("The token snapshot ended before its exact total was loaded.");
  }
  return tokens;
}

export function BackendAdminConsole({ apiBaseUrl }: BackendAdminConsoleProps) {
  const client = useMemo(() => createOpenSplunkApiClient({ baseUrl: apiBaseUrl }), [apiBaseUrl]);
  const [normalizedApiBaseUrl, setNormalizedApiBaseUrl] = useState<string | null>(null);
  const [apiBaseNormalizationError, setApiBaseNormalizationError] = useState<string | null>(null);
  const [section, setSection] = useState<AdminSection>("overview");
  const [bootstrap, setBootstrap] = useState<SystemBootstrapModel | null>(null);
  const [bootstrapError, setBootstrapError] = useState<string | null>(null);
  const [serverClockAnchor, setServerClockAnchor] = useState<ServerClockAnchor | null>(null);
  const [indexes, setIndexes] = useState<Index[]>([]);
  const [indexState, setIndexState] = useState<ResourceState>("loading");
  const [indexError, setIndexError] = useState<string | null>(null);
  const [indexNextPageToken, setIndexNextPageToken] = useState<string | null>(null);
  const [indexTotalSize, setIndexTotalSize] = useState<bigint | null>(null);
  const [indexTotalSizeExact, setIndexTotalSizeExact] = useState(false);
  const [indexLoadingMore, setIndexLoadingMore] = useState(false);
  const [indexPaginationError, setIndexPaginationError] = useState<string | null>(null);
  const [tokens, setTokens] = useState<IngestionToken[]>([]);
  const [tokenState, setTokenState] = useState<ResourceState>("loading");
  const [tokenError, setTokenError] = useState<string | null>(null);
  const [tokenNextPageToken, setTokenNextPageToken] = useState<string | null>(null);
  const [tokenTotalSize, setTokenTotalSize] = useState<bigint | null>(null);
  const [tokenTotalSizeExact, setTokenTotalSizeExact] = useState(false);
  const [tokenLoadingMore, setTokenLoadingMore] = useState(false);
  const [tokenPaginationError, setTokenPaginationError] = useState<string | null>(null);
  const [loadGeneration, setLoadGeneration] = useState(0);
  const [filter, setFilter] = useState("");
  const [modal, setModal] = useState<AdminModal | null>(null);
  const [indexEditTarget, setIndexEditTarget] = useState<Index | null>(null);
  const [indexName, setIndexName] = useState("");
  const [indexDisplayName, setIndexDisplayName] = useState("");
  const [indexDescription, setIndexDescription] = useState("");
  const [retention, setRetention] = useState("30");
  const [indexIngestionAccess, setIndexIngestionAccess] = useState(
    IndexAccessState.INDEX_ACCESS_STATE_ENABLED,
  );
  const [indexSearchAccess, setIndexSearchAccess] = useState(
    IndexAccessState.INDEX_ACCESS_STATE_ENABLED,
  );
  const [tokenEditTarget, setTokenEditTarget] = useState<IngestionToken | null>(null);
  const [tokenName, setTokenName] = useState("");
  const [tokenDescription, setTokenDescription] = useState("");
  const [tokenIndexes, setTokenIndexes] = useState<Set<string>>(new Set());
  const [tokenExpiration, setTokenExpiration] = useState("");
  const [tokenSecret, setTokenSecret] = useState<string | null>(null);
  const [issuedToken, setIssuedToken] = useState<IngestionToken | null>(null);
  const [issuedTokenRecovery, setIssuedTokenRecovery] = useState<TokenCreateRecovery | null>(null);
  const [tokenCreateRecovery, setTokenCreateRecovery] = useState<TokenCreateRecovery | null>(null);
  const [tokenCreateGuardStorageState, setTokenCreateGuardStorageState] =
    useState<TokenCreateGuardStorageState>("checking");
  const [tokenCreateGuardStorageError, setTokenCreateGuardStorageError] = useState<string | null>(null);
  const [tokenCreateLockAvailable, setTokenCreateLockAvailable] = useState<boolean | null>(null);
  const [tokenSecretAcknowledged, setTokenSecretAcknowledged] = useState(false);
  const [revokeTarget, setRevokeTarget] = useState<IngestionToken | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [toast, setToast] = useState<AdminToast | null>(null);
  const tokenProtectionActive = busy === "create-token"
    || issuedToken !== null
    || tokenCreateRecovery !== null;
  const tokenProtectionActiveRef = useRef(tokenProtectionActive);
  const componentMountedRef = useRef(false);
  const tokenHistoryGuardIdRef = useRef<string | null>(null);
  const tokenHistoryCleanupTimerRef = useRef<number | null>(null);
  const tokenGuardOwnerIdRef = useRef<string | null>(null);
  const tokenGuardLeaseRef = useRef<{
    attemptId: string;
    promise: Promise<void>;
    release: () => void;
  } | null>(null);
  const tokenGuardLockOperationAttemptRef = useRef<string | null>(null);
  const tokenRecoveryOperationGenerationRef = useRef(0);
  const tokenCreatePreparationControllerRef = useRef<AbortController | null>(null);
  const indexSeenPageTokensRef = useRef<Set<string>>(new Set());
  const tokenSeenPageTokensRef = useRef<Set<string>>(new Set());
  const indexPageRequestGenerationRef = useRef(0);
  const tokenPageRequestGenerationRef = useRef(0);
  const indexLoadMoreRequestRef = useRef<{
    controller: AbortController;
    generation: number;
    pageToken: string;
  } | null>(null);
  const tokenLoadMoreRequestRef = useRef<{
    controller: AbortController;
    generation: number;
    pageToken: string;
  } | null>(null);
  tokenProtectionActiveRef.current = tokenProtectionActive;

  const load = useCallback(() => {
    indexPageRequestGenerationRef.current += 1;
    tokenPageRequestGenerationRef.current += 1;
    indexLoadMoreRequestRef.current?.controller.abort();
    tokenLoadMoreRequestRef.current?.controller.abort();
    indexLoadMoreRequestRef.current = null;
    tokenLoadMoreRequestRef.current = null;
    setLoadGeneration((current) => current + 1);
  }, []);

  useEffect(() => {
    indexPageRequestGenerationRef.current += 1;
    tokenPageRequestGenerationRef.current += 1;
    indexLoadMoreRequestRef.current?.controller.abort();
    tokenLoadMoreRequestRef.current?.controller.abort();
    indexLoadMoreRequestRef.current = null;
    tokenLoadMoreRequestRef.current = null;
    const controller = new AbortController();
    let current = true;
    setBootstrap(null);
    setBootstrapError(null);
    setServerClockAnchor(null);
    setIndexState("loading");
    setIndexError(null);
    setIndexNextPageToken(null);
    setIndexTotalSize(null);
    setIndexTotalSizeExact(false);
    setIndexLoadingMore(false);
    setIndexPaginationError(null);
    indexSeenPageTokensRef.current = new Set();
    setTokenState("loading");
    setTokenError(null);
    setTokenNextPageToken(null);
    setTokenTotalSize(null);
    setTokenTotalSizeExact(false);
    setTokenLoadingMore(false);
    setTokenPaginationError(null);
    tokenSeenPageTokensRef.current = new Set();

    const bootstrapStartedMonotonicMs = performance.now();
    void getSystemBootstrap(client, undefined, { signal: controller.signal }).then(
      (value) => {
        if (!current) return;
        const bootstrapReceivedMonotonicMs = performance.now();
        const bootstrapRoundTripMs = Math.max(
          0,
          bootstrapReceivedMonotonicMs - bootstrapStartedMonotonicMs,
        );
        setBootstrap(value);
        setServerClockAnchor({
          serverTimeMs: value.serverTime.valueOf() + bootstrapRoundTripMs / 2,
          clientMonotonicMs: bootstrapReceivedMonotonicMs,
          uncertaintyMs: bootstrapRoundTripMs / 2 + TOKEN_CREATE_CLOCK_EPSILON_MS,
        });
      },
      (error: unknown) => {
        if (!current || controller.signal.aborted) return;
        setBootstrap(null);
        setBootstrapError(errorMessage(error));
      },
    );
    // Omitting page_size lets each independently available route choose a safe
    // server-side default even when bootstrap cannot advertise limits.
    void loadIndexPage(client, undefined, controller.signal).then(
      (result) => {
        if (!current) return;
        setIndexState(result.state);
        setIndexes(result.indexes);
        setIndexError(result.message ?? null);
        setIndexNextPageToken(result.nextPageToken);
        setIndexTotalSize(result.totalSize);
        setIndexTotalSizeExact(result.totalSizeExact);
      },
      (error: unknown) => {
        if (!current || controller.signal.aborted) return;
        setIndexState("error");
        setIndexes([]);
        setIndexError(errorMessage(error));
      },
    );
    void loadTokenPage(client, undefined, controller.signal).then(
      (result) => {
        if (!current) return;
        setTokenState(result.state);
        setTokens(result.tokens);
        setTokenError(result.message ?? null);
        setTokenNextPageToken(result.nextPageToken);
        setTokenTotalSize(result.totalSize);
        setTokenTotalSizeExact(result.totalSizeExact);
      },
      (error: unknown) => {
        if (!current || controller.signal.aborted) return;
        setTokenState("error");
        setTokens([]);
        setTokenError(errorMessage(error));
      },
    );

    return () => {
      current = false;
      controller.abort();
    };
  }, [client, loadGeneration]);

  useEffect(() => {
    componentMountedRef.current = true;
    return () => {
      componentMountedRef.current = false;
      indexPageRequestGenerationRef.current += 1;
      tokenPageRequestGenerationRef.current += 1;
      indexLoadMoreRequestRef.current?.controller.abort();
      tokenLoadMoreRequestRef.current?.controller.abort();
      indexLoadMoreRequestRef.current = null;
      tokenLoadMoreRequestRef.current = null;
      tokenRecoveryOperationGenerationRef.current += 1;
      tokenCreatePreparationControllerRef.current?.abort();
      tokenCreatePreparationControllerRef.current = null;
      tokenGuardLockOperationAttemptRef.current = null;
      tokenGuardLeaseRef.current?.release();
      tokenGuardLeaseRef.current = null;
    };
  }, []);

  useEffect(() => {
    try {
      setNormalizedApiBaseUrl(normalizeApiBaseUrl(apiBaseUrl, window.location.origin));
      setApiBaseNormalizationError(null);
    } catch (error) {
      setNormalizedApiBaseUrl(null);
      setApiBaseNormalizationError(errorMessage(error));
      setTokenCreateGuardStorageState("unavailable");
    }
  }, [apiBaseUrl]);

  useEffect(() => {
    if (normalizedApiBaseUrl === null) return;
    const canonicalApiBaseUrl = normalizedApiBaseUrl;
    const key = tokenCreateGuardStorageKey(canonicalApiBaseUrl);
    function handleTokenGuardStorage(event: StorageEvent) {
      if (event.storageArea !== window.localStorage || event.key !== key) return;
      tokenCreatePreparationControllerRef.current?.abort();
      tokenCreatePreparationControllerRef.current = null;
      if (event.newValue === null) {
        if (tokenProtectionActiveRef.current) {
          tokenRecoveryOperationGenerationRef.current += 1;
          tokenGuardLockOperationAttemptRef.current = null;
          releaseTokenGuardLease();
          setBusy(null);
          setTokenCreateGuardStorageState("unavailable");
          setTokenCreateGuardStorageError(
            "The durable token safety guard was removed by another tab while this attempt was active.",
          );
          setToast({
            message: "Another tab removed the active token safety guard. This tab is now read-only and must not dismiss the token dialog.",
            kind: "warning",
          });
        } else {
          setTokenCreateGuardStorageState("available");
          setTokenCreateGuardStorageError(null);
        }
        return;
      }
      const stored = parsePersistedTokenCreateGuard(
        event.newValue,
        canonicalApiBaseUrl,
      );
      tokenRecoveryOperationGenerationRef.current += 1;
      tokenGuardLockOperationAttemptRef.current = null;
      releaseTokenGuardLease();
      setBusy(null);
      setTokenCreateGuardStorageState("unavailable");
      setTokenCreateGuardStorageError(stored === null
        ? "Another tab wrote an unreadable token safety record. Token generation is locked."
        : `Another tab owns token safety attempt ${stored.recovery.attemptId}. Refresh to take over recovery after that tab finishes.`);
      setToast({
        message: stored === null
          ? "A cross-tab token safety update was unreadable. Token actions are locked."
          : "Another tab created or took ownership of a token safety attempt. This tab is now read-only for token recovery.",
        kind: "warning",
      });
    }
    window.addEventListener("storage", handleTokenGuardStorage);
    return () => window.removeEventListener("storage", handleTokenGuardStorage);
  }, [normalizedApiBaseUrl]);

  useEffect(() => {
    if (!tokenProtectionActive) return;
    if (tokenHistoryCleanupTimerRef.current !== null) {
      window.clearTimeout(tokenHistoryCleanupTimerRef.current);
      tokenHistoryCleanupTimerRef.current = null;
    }
    const guardId = tokenHistoryGuardIdRef.current ?? crypto.randomUUID();
    tokenHistoryGuardIdRef.current = guardId;
    if (!historyHasTokenGuard(guardId)) {
      window.history.pushState(
        historyStateWithTokenGuard(guardId),
        "",
        window.location.href,
      );
    }

    function confirmLeaving(event: BeforeUnloadEvent) {
      event.preventDefault();
      event.returnValue = "";
    }
    function blockBackNavigation() {
      if (!tokenProtectionActiveRef.current || historyHasTokenGuard(guardId)) return;
      window.history.pushState(
        historyStateWithTokenGuard(guardId),
        "",
        window.location.href,
      );
      setToast({
        message: "Finish creating, save, or revoke this one-time token before leaving Administration.",
        kind: "warning",
      });
    }
    function blockClientNavigation(event: MouseEvent) {
      if (!tokenProtectionActiveRef.current || !(event.target instanceof Element)) return;
      const link = event.target.closest<HTMLAnchorElement>("a[href]");
      if (link === null) return;
      event.preventDefault();
      event.stopPropagation();
      setToast({
        message: "Finish creating, save, or revoke this one-time token before following another link.",
        kind: "warning",
      });
    }

    window.addEventListener("beforeunload", confirmLeaving);
    window.addEventListener("popstate", blockBackNavigation);
    document.addEventListener("click", blockClientNavigation, true);
    return () => {
      window.removeEventListener("beforeunload", confirmLeaving);
      window.removeEventListener("popstate", blockBackNavigation);
      document.removeEventListener("click", blockClientNavigation, true);
      tokenHistoryCleanupTimerRef.current = window.setTimeout(() => {
        tokenHistoryCleanupTimerRef.current = null;
        if (componentMountedRef.current && tokenProtectionActiveRef.current) return;
        if (historyHasTokenGuard(guardId)) window.history.back();
        if (tokenHistoryGuardIdRef.current === guardId) tokenHistoryGuardIdRef.current = null;
      }, 0);
    };
  }, [tokenProtectionActive]);

  function authoritativeServerNowMs(): number | undefined {
    if (serverClockAnchor === null) return undefined;
    return serverClockAnchor.serverTimeMs
      + Math.max(0, performance.now() - serverClockAnchor.clientMonotonicMs);
  }

  function currentTokenGuardOwnerId(): string {
    const current = tokenGuardOwnerIdRef.current ?? crypto.randomUUID();
    tokenGuardOwnerIdRef.current = current;
    return current;
  }

  function holdTokenGuardLease(attemptId: string): Promise<void> {
    const existing = tokenGuardLeaseRef.current;
    if (existing !== null) {
      if (existing.attemptId !== attemptId) {
        throw new Error("A different token safety attempt already owns this tab's lock lease.");
      }
      return existing.promise;
    }
    let release!: () => void;
    const promise = new Promise<void>((resolve) => {
      release = resolve;
    });
    tokenGuardLeaseRef.current = { attemptId, promise, release };
    return promise;
  }

  function releaseTokenGuardLease(expectedAttemptId?: string) {
    const lease = tokenGuardLeaseRef.current;
    if (lease === null) return;
    if (expectedAttemptId !== undefined && lease.attemptId !== expectedAttemptId) return;
    tokenGuardLeaseRef.current = null;
    lease.release();
  }

  function hasTokenGuardLockContext(attemptId: string): boolean {
    return tokenGuardLockOperationAttemptRef.current === attemptId
      || tokenGuardLeaseRef.current?.attemptId === attemptId;
  }

  function beginTokenRecoveryOperation(): number {
    tokenRecoveryOperationGenerationRef.current += 1;
    return tokenRecoveryOperationGenerationRef.current;
  }

  function ownsTokenCreateGuard(recovery: TokenCreateRecovery): boolean {
    try {
      if (normalizedApiBaseUrl === null) return false;
      const raw = window.localStorage.getItem(
        tokenCreateGuardStorageKey(normalizedApiBaseUrl),
      );
      if (raw === null) return false;
      const stored = parsePersistedTokenCreateGuard(raw, normalizedApiBaseUrl);
      return stored !== null
        && stored.recovery.attemptId === recovery.attemptId
        && stored.recovery.ownerId === recovery.ownerId;
    } catch {
      return false;
    }
  }

  function requireTokenGuardOwnership(recovery: TokenCreateRecovery): boolean {
    if (
      hasTokenGuardLockContext(recovery.attemptId)
      && ownsTokenCreateGuard(recovery)
    ) return true;
    setTokenCreateGuardStorageState("unavailable");
    setTokenCreateGuardStorageError(
      "This tab no longer owns the exact durable token safety guard.",
    );
    setToast({
      message: "Token recovery ownership changed in another tab. This dialog is now read-only.",
      kind: "warning",
    });
    tokenRecoveryOperationGenerationRef.current += 1;
    tokenGuardLockOperationAttemptRef.current = null;
    tokenCreatePreparationControllerRef.current?.abort();
    tokenCreatePreparationControllerRef.current = null;
    setBusy(null);
    releaseTokenGuardLease(recovery.attemptId);
    return false;
  }

  function tokenRecoveryOperationIsCurrent(
    generation: number,
    recovery: TokenCreateRecovery,
  ): boolean {
    return componentMountedRef.current
      && tokenRecoveryOperationGenerationRef.current === generation
      && requireTokenGuardOwnership(recovery);
  }

  function persistTokenCreateGuard(
    recovery: TokenCreateRecovery,
    knownIssuedTokenId: string | null,
    options: {
      allowCreate?: boolean;
      allowOwnershipTakeover?: boolean;
    } = {},
  ): boolean {
    try {
      if (normalizedApiBaseUrl === null) {
        throw new Error("The API base URL has not been normalized for durable token safety.");
      }
      if (!hasTokenGuardLockContext(recovery.attemptId)) {
        throw new Error("This tab no longer holds the token safety Web Lock.");
      }
      const key = tokenCreateGuardStorageKey(normalizedApiBaseUrl);
      const existingRaw = window.localStorage.getItem(key);
      if (existingRaw === null && !options.allowCreate) {
        throw new Error("The durable token safety guard disappeared before it could be updated.");
      }
      if (existingRaw !== null) {
        const existing = parsePersistedTokenCreateGuard(
          existingRaw,
          normalizedApiBaseUrl,
        );
        if (
          existing === null
          || existing.recovery.attemptId !== recovery.attemptId
          || (
            existing.recovery.ownerId !== recovery.ownerId
            && !options.allowOwnershipTakeover
          )
        ) {
          throw new Error("Another tab or token attempt owns the durable safety guard.");
        }
      }
      const record = serializeTokenCreateGuard(
        normalizedApiBaseUrl,
        recovery,
        knownIssuedTokenId,
      );
      // The record is deliberately constructed field-by-field and contains no
      // plaintext credential. localStorage makes the safety guard survive tab
      // closure and broadcasts ownership changes to other same-origin tabs.
      window.localStorage.setItem(
        key,
        JSON.stringify(record),
      );
      setTokenCreateGuardStorageState("available");
      setTokenCreateGuardStorageError(null);
      return true;
    } catch (error) {
      setTokenCreateGuardStorageState("unavailable");
      setTokenCreateGuardStorageError(errorMessage(error));
      return false;
    }
  }

  function clearTokenCreateGuard(
    expectedAttemptId: string,
    expectedOwnerId: string,
  ): boolean {
    try {
      if (normalizedApiBaseUrl === null) {
        throw new Error("The API base URL has not been normalized for durable token safety.");
      }
      if (!hasTokenGuardLockContext(expectedAttemptId)) {
        throw new Error("This tab no longer holds the token safety Web Lock.");
      }
      const key = tokenCreateGuardStorageKey(normalizedApiBaseUrl);
      const raw = window.localStorage.getItem(key);
      if (raw === null) {
        throw new Error("The durable token safety guard disappeared unexpectedly.");
      }
      const stored = parsePersistedTokenCreateGuard(raw, normalizedApiBaseUrl);
      if (
        stored === null
        || stored.recovery.attemptId !== expectedAttemptId
        || stored.recovery.ownerId !== expectedOwnerId
      ) {
        throw new Error("A different or unreadable token safety attempt owns the durable guard.");
      }
      window.localStorage.removeItem(key);
      tokenRecoveryOperationGenerationRef.current += 1;
      if (tokenGuardLockOperationAttemptRef.current === expectedAttemptId) {
        tokenGuardLockOperationAttemptRef.current = null;
      }
      releaseTokenGuardLease(expectedAttemptId);
      setTokenCreateGuardStorageState("available");
      setTokenCreateGuardStorageError(null);
      setBusy(null);
      return true;
    } catch (error) {
      setTokenCreateGuardStorageState("unavailable");
      setTokenCreateGuardStorageError(errorMessage(error));
      return false;
    }
  }

  function observeTokenCreateOutcome(
    definition: TokenCreateDefinitionSnapshot,
    requestStartedMonotonicMs: number,
    outcomeKind: Exclude<TokenCreateOutcomeKind, "pending">,
  ) {
    const roundTripMs = Math.max(0, performance.now() - requestStartedMonotonicMs);
    definition.requestRoundTripMs = roundTripMs;
    definition.outcomeObservedServerTimeMs = authoritativeServerNowMs()
      ?? (definition.dispatchedServerTimeMs === null
        ? null
        : definition.dispatchedServerTimeMs + roundTripMs);
    definition.outcomeKind = outcomeKind;
  }

  function cancelIndexLoadMoreRequest() {
    indexPageRequestGenerationRef.current += 1;
    indexLoadMoreRequestRef.current?.controller.abort();
    indexLoadMoreRequestRef.current = null;
    setIndexLoadingMore(false);
  }

  function cancelTokenLoadMoreRequest() {
    tokenPageRequestGenerationRef.current += 1;
    tokenLoadMoreRequestRef.current?.controller.abort();
    tokenLoadMoreRequestRef.current = null;
    setTokenLoadingMore(false);
  }

  function invalidateIndexPagination(message: string) {
    cancelIndexLoadMoreRequest();
    setIndexNextPageToken(null);
    setIndexPaginationError(message);
  }

  function invalidateTokenPagination(message: string) {
    cancelTokenLoadMoreRequest();
    setTokenNextPageToken(null);
    setTokenPaginationError(message);
  }

  async function loadMoreIndexes() {
    const requestedToken = indexNextPageToken;
    if (requestedToken === null || indexLoadingMore) return;
    if (indexSeenPageTokensRef.current.has(requestedToken)) {
      setIndexNextPageToken(null);
      setIndexPaginationError("The server repeated an index page cursor. Refresh before loading more.");
      return;
    }
    indexSeenPageTokensRef.current.add(requestedToken);
    indexLoadMoreRequestRef.current?.controller.abort();
    const generation = indexPageRequestGenerationRef.current + 1;
    indexPageRequestGenerationRef.current = generation;
    const controller = new AbortController();
    const request = { controller, generation, pageToken: requestedToken };
    indexLoadMoreRequestRef.current = request;
    setIndexLoadingMore(true);
    setIndexPaginationError(null);
    try {
      const result = await loadIndexPage(client, requestedToken, controller.signal);
      if (
        !componentMountedRef.current
        || indexLoadMoreRequestRef.current !== request
        || indexPageRequestGenerationRef.current !== generation
        || request.pageToken !== requestedToken
      ) return;
      if (result.state !== "available") {
        if (result.state === "unavailable") setIndexState("unavailable");
        setIndexNextPageToken(null);
        setIndexPaginationError(result.message ?? "The next index page could not be loaded. Refresh to retry.");
        return;
      }
      if (
        result.nextPageToken !== null
        && indexSeenPageTokensRef.current.has(result.nextPageToken)
      ) {
        setIndexNextPageToken(null);
        setIndexPaginationError("The server repeated an index page cursor. Refresh before loading more.");
        return;
      }
      const loadedIds = new Set(indexes.map((index) => index.indexId));
      if (result.indexes.some((index) => loadedIds.has(index.indexId))) {
        setIndexNextPageToken(null);
        setIndexPaginationError("The server returned an overlapping index page. Refresh before loading more.");
        return;
      }
      setIndexes((current) => [...current, ...result.indexes]);
      setIndexNextPageToken(result.nextPageToken);
      setIndexTotalSize(result.totalSize);
      setIndexTotalSizeExact(result.totalSizeExact);
    } catch (error) {
      if (
        controller.signal.aborted
        || indexLoadMoreRequestRef.current !== request
        || indexPageRequestGenerationRef.current !== generation
      ) return;
      setIndexNextPageToken(null);
      setIndexPaginationError(`The next index page could not be loaded: ${errorMessage(error)}`);
    } finally {
      if (
        componentMountedRef.current
        && indexLoadMoreRequestRef.current === request
        && indexPageRequestGenerationRef.current === generation
      ) {
        indexLoadMoreRequestRef.current = null;
        setIndexLoadingMore(false);
      }
    }
  }

  async function loadMoreTokens() {
    const requestedToken = tokenNextPageToken;
    if (requestedToken === null || tokenLoadingMore) return;
    if (tokenSeenPageTokensRef.current.has(requestedToken)) {
      setTokenNextPageToken(null);
      setTokenPaginationError("The server repeated a token page cursor. Refresh before loading more.");
      return;
    }
    tokenSeenPageTokensRef.current.add(requestedToken);
    tokenLoadMoreRequestRef.current?.controller.abort();
    const generation = tokenPageRequestGenerationRef.current + 1;
    tokenPageRequestGenerationRef.current = generation;
    const controller = new AbortController();
    const request = { controller, generation, pageToken: requestedToken };
    tokenLoadMoreRequestRef.current = request;
    setTokenLoadingMore(true);
    setTokenPaginationError(null);
    try {
      const result = await loadTokenPage(client, requestedToken, controller.signal);
      if (
        !componentMountedRef.current
        || tokenLoadMoreRequestRef.current !== request
        || tokenPageRequestGenerationRef.current !== generation
        || request.pageToken !== requestedToken
      ) return;
      if (result.state !== "available") {
        if (result.state === "unavailable") setTokenState("unavailable");
        setTokenNextPageToken(null);
        setTokenPaginationError(result.message ?? "The next token page could not be loaded. Refresh to retry.");
        return;
      }
      if (
        result.nextPageToken !== null
        && tokenSeenPageTokensRef.current.has(result.nextPageToken)
      ) {
        setTokenNextPageToken(null);
        setTokenPaginationError("The server repeated a token page cursor. Refresh before loading more.");
        return;
      }
      const loadedIds = new Set(tokens.map((token) => token.ingestionTokenId));
      if (result.tokens.some((token) => loadedIds.has(token.ingestionTokenId))) {
        setTokenNextPageToken(null);
        setTokenPaginationError("The server returned an overlapping token page. Refresh before loading more.");
        return;
      }
      setTokens((current) => [...current, ...result.tokens]);
      setTokenNextPageToken(result.nextPageToken);
      setTokenTotalSize(result.totalSize);
      setTokenTotalSizeExact(result.totalSizeExact);
    } catch (error) {
      if (
        controller.signal.aborted
        || tokenLoadMoreRequestRef.current !== request
        || tokenPageRequestGenerationRef.current !== generation
      ) return;
      setTokenNextPageToken(null);
      setTokenPaginationError(`The next token page could not be loaded: ${errorMessage(error)}`);
    } finally {
      if (
        componentMountedRef.current
        && tokenLoadMoreRequestRef.current === request
        && tokenPageRequestGenerationRef.current === generation
      ) {
        tokenLoadMoreRequestRef.current = null;
        setTokenLoadingMore(false);
      }
    }
  }

  const visibleIndexes = useMemo(() => {
    const normalized = filter.trim().toLowerCase();
    return indexes.filter((index) => {
      const definition = index.definition;
      return normalized.length === 0
        || `${definition?.name ?? ""} ${definition?.displayName ?? ""} ${definition?.description ?? ""}`
          .toLowerCase()
          .includes(normalized);
    });
  }, [filter, indexes]);

  function openIndexDialog() {
    setIndexEditTarget(null);
    setIndexName("");
    setIndexDisplayName("");
    setIndexDescription("");
    setRetention("30");
    setIndexIngestionAccess(IndexAccessState.INDEX_ACCESS_STATE_ENABLED);
    setIndexSearchAccess(IndexAccessState.INDEX_ACCESS_STATE_ENABLED);
    setModal("create-index");
  }

  function openTokenDialog() {
    setTokenEditTarget(null);
    setTokenName("");
    setTokenDescription("");
    setTokenIndexes(new Set(ingestibleTokenScopes.slice(0, 1).map((scope) => scope.name)));
    setTokenExpiration("");
    setTokenSecret(null);
    setIssuedToken(null);
    setIssuedTokenRecovery(null);
    setTokenCreateRecovery(null);
    setTokenSecretAcknowledged(false);
    setModal("create-token");
  }

  async function openIndexEditor(index: Index) {
    setBusy(`read-index-${index.indexId}`);
    try {
      const response = await client.indexes.get({
        selector: { selector: { $case: "indexId", value: index.indexId } },
      });
      const current = response.index;
      if (current?.definition === undefined) throw new Error("The server returned an empty index definition.");
      setIndexEditTarget(current);
      setIndexDisplayName(current.definition.displayName || current.definition.name);
      setIndexDescription(current.definition.description ?? "");
      setRetention(retentionFormValue(current.definition.retentionPeriod?.seconds));
      setIndexIngestionAccess(current.definition.ingestionAccess);
      setIndexSearchAccess(current.definition.searchAccess);
      setModal("edit-index");
    } catch (error) {
      setToast({ message: errorMessage(error), kind: "warning" });
      load();
    } finally {
      setBusy(null);
    }
  }

  async function openTokenEditor(token: IngestionToken) {
    setBusy(`read-token-${token.ingestionTokenId}`);
    try {
      const response = await client.ingestionTokens.get({
        ingestionTokenId: token.ingestionTokenId,
      });
      const current = response.ingestionToken;
      if (current === undefined) throw new Error("The server returned an empty ingestion token.");
      setTokenEditTarget(current);
      setTokenName(current.name);
      setTokenDescription(current.description ?? "");
      setTokenIndexes(new Set(current.constraints?.allowedIndexNames ?? []));
      setTokenExpiration(dateTimeLocalValue(current.expiresAt));
      setTokenSecret(null);
      setModal("edit-token");
    } catch (error) {
      setToast({ message: errorMessage(error), kind: "warning" });
      load();
    } finally {
      setBusy(null);
    }
  }

  async function createIndex(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const normalized = indexName.trim().toLowerCase();
    if (!/^[a-z0-9][a-z0-9_-]*$/.test(normalized)) {
      setToast({ message: "Index names must use lowercase letters, numbers, hyphens, or underscores.", kind: "warning" });
      return;
    }
    if (normalized.includes("kvstore")) {
      setToast({ message: "Index names cannot contain the reserved word “kvstore”.", kind: "warning" });
      return;
    }
    cancelIndexLoadMoreRequest();
    setBusy("create-index");
    try {
      const response = await client.indexes.create({
        definition: {
          name: normalized,
          displayName: indexDisplayName.trim() || normalized,
          description: indexDescription.trim() || undefined,
          retentionPeriod: retentionFromForm(retention),
          ingestionAccess: IndexAccessState.INDEX_ACCESS_STATE_ENABLED,
          searchAccess: IndexAccessState.INDEX_ACCESS_STATE_ENABLED,
          defaultSourcetype: undefined,
          limits: undefined,
        },
        clientRequestId: undefined,
      });
      if (response.index === undefined) throw new Error("The server returned an empty index.");
      setIndexes((current) => [...current, response.index as Index].toSorted((left, right) =>
        (left.definition?.name ?? "").localeCompare(right.definition?.name ?? "")));
      setIndexTotalSize(null);
      setIndexTotalSizeExact(false);
      invalidateIndexPagination("The index catalog changed. Refresh to confirm the loaded records.");
      setModal(null);
      setToast({ message: `Index “${normalized}” was created.`, kind: "success" });
    } catch (error) {
      if (isHttpStatus(error, 409)) {
        setToast({ message: `An index named “${normalized}” already exists. Choose another name.`, kind: "warning" });
      } else if (isOptionalRouteUnavailable(error)) {
        setIndexState("unavailable");
        setModal(null);
        setToast({ message: errorMessage(error), kind: "warning" });
      } else {
        setToast({ message: errorMessage(error), kind: "warning" });
      }
    } finally {
      setBusy(null);
    }
  }

  async function updateIndex(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const target = indexEditTarget;
    const definition = target?.definition;
    if (target === null || definition === undefined) return;
    const updateMask: string[] = [];
    if ((indexDisplayName.trim() || definition.name) !== definition.displayName) {
      updateMask.push("display_name");
    }
    if (indexDescription !== (definition.description ?? "")) updateMask.push("description");
    if (retention !== retentionFormValue(definition.retentionPeriod?.seconds)) {
      updateMask.push("retention_period");
    }
    if (indexIngestionAccess !== definition.ingestionAccess) updateMask.push("ingestion_access");
    if (indexSearchAccess !== definition.searchAccess) updateMask.push("search_access");
    if (updateMask.length === 0) return;
    cancelIndexLoadMoreRequest();
    setBusy(`update-index-${target.indexId}`);
    try {
      const response = await client.indexes.update({
        selector: { selector: { $case: "indexId", value: target.indexId } },
        expectedVersion: target.version,
        definition: {
          ...definition,
          displayName: indexDisplayName.trim() || definition.name,
          description: indexDescription.trim() || undefined,
          retentionPeriod: retentionFromForm(retention),
          ingestionAccess: indexIngestionAccess,
          searchAccess: indexSearchAccess,
        },
        updateMask,
      });
      if (response.index === undefined) throw new Error("The server returned an empty index.");
      setIndexes((current) => current.map((item) =>
        item.indexId === response.index?.indexId ? response.index as Index : item));
      invalidateIndexPagination("The index catalog changed. Refresh to confirm the loaded records.");
      setIndexEditTarget(null);
      setModal(null);
      setToast({ message: `Index “${definition.name}” was updated.`, kind: "success" });
    } catch (error) {
      if (isHttpStatus(error, 409)) {
        setIndexEditTarget(null);
        setModal(null);
        setToast({ message: "This index changed on the server. The latest version was reloaded; open Edit and try again.", kind: "warning" });
      } else {
        setToast({ message: errorMessage(error), kind: "warning" });
      }
      load();
    } finally {
      setBusy(null);
    }
  }

  async function changeIndexState(index: Index) {
    const name = index.definition?.name ?? "index";
    const nextState = index.state === IndexState.INDEX_STATE_ACTIVE
      ? IndexState.INDEX_STATE_ARCHIVED
      : IndexState.INDEX_STATE_ACTIVE;
    cancelIndexLoadMoreRequest();
    setBusy(`index-${index.indexId}`);
    try {
      const response = await client.indexes.setState({
        selector: { selector: { $case: "indexId", value: index.indexId } },
        expectedVersion: index.version,
        state: nextState,
      });
      if (response.index === undefined) throw new Error("The server returned an empty index.");
      setIndexes((current) => current.map((item) =>
        item.indexId === response.index?.indexId ? response.index as Index : item));
      invalidateIndexPagination("The index catalog changed. Refresh to confirm the loaded records.");
      setToast({ message: `Index “${name}” is now ${indexStateLabel(nextState).toLowerCase()}.`, kind: "success" });
    } catch (error) {
      if (isAdvertisedFeatureRouteUnavailable(error)) {
        setIndexState("unavailable");
        setToast({ message: errorMessage(error), kind: "warning" });
      } else if (isHttpStatus(error, 404)) {
        setToast({
          message: `Index “${name}” no longer exists or changed while this page was open. The catalog is being reloaded.`,
          kind: "warning",
        });
        load();
      } else {
        setToast({ message: errorMessage(error), kind: "warning" });
        load();
      }
    } finally {
      setBusy(null);
    }
  }

  async function findTokenCreateCandidates(
    recovery: TokenCreateRecovery,
    signal?: AbortSignal,
  ): Promise<IngestionToken[]> {
    const currentTokens = await listTokensForCreateSafety(
      client,
      recovery.definition.name,
      signal,
    );
    return currentTokens.filter((token) =>
      !recovery.preexistingTokenIds.has(token.ingestionTokenId)
      && tokenMatchesCreateMetadata(token, recovery.definition));
  }

  function applyTokenCreateCandidates(
    recovery: TokenCreateRecovery,
    candidates: IngestionToken[],
  ) {
    if (!requireTokenGuardOwnership(recovery)) return;
    for (const candidate of candidates) storeTokenSnapshot(candidate);
    if (candidates.some((candidate) =>
      !tokens.some((token) => token.ingestionTokenId === candidate.ingestionTokenId))) {
      setTokenTotalSize(null);
      setTokenTotalSizeExact(false);
    }
    const unsafeTimingOutliers = candidates.filter((candidate) =>
      !tokenIsTerminallySafe(candidate)
      && !tokenFallsWithinCreateAttributionWindow(candidate, recovery.definition));
    if (unsafeTimingOutliers.length > 0) {
      const outlierRecovery: TokenCreateRecovery = {
        ...recovery,
        candidates,
        reconciliationError: `${unsafeTimingOutliers.length.toLocaleString()} nonterminal exact post-baseline token match${unsafeTimingOutliers.length === 1 ? " is" : "es are"} outside the expected request window. Automatic safe clearing is blocked until every possible live credential is reviewed or revoked.`,
      };
      if (!persistTokenCreateGuard(outlierRecovery, null)) return;
      setTokenCreateRecovery(outlierRecovery);
      setIssuedToken(null);
      setIssuedTokenRecovery(null);
      setTokenSecret(null);
      setTokenSecretAcknowledged(false);
      setModal("create-token");
      setToast({
        message: "A matching token falls outside the expected request timing window. It remains visible and blocks automatic recovery clearing.",
        kind: "warning",
      });
      return;
    }
    const terminalCandidates = candidates.filter(tokenIsTerminallySafe);
    const attributedTerminalCandidates = terminalCandidates.filter((candidate) =>
      tokenFallsWithinCreateAttributionWindow(candidate, recovery.definition));
    const unresolvedCandidates = candidates.filter((candidate) =>
      !tokenIsTerminallySafe(candidate));
    const reconciledIds = new Set(recovery.preexistingTokenIds);
    for (const candidate of terminalCandidates) reconciledIds.add(candidate.ingestionTokenId);
    const nextRecovery: TokenCreateRecovery = {
      ...recovery,
      preexistingTokenIds: reconciledIds,
    };
    if (
      unresolvedCandidates.length === 0
      && (
        attributedTerminalCandidates.length > 0
        || recovery.confirmedRevokedTokenIds.size > 0
      )
    ) {
      if (!clearTokenCreateGuard(recovery.attemptId, recovery.ownerId)) {
        setToast({
          message: "All identified tokens are safe, but the browser could not clear its reload guard. Token generation remains locked.",
          kind: "warning",
        });
        return;
      }
      setTokenCreateRecovery(null);
      setIssuedToken(null);
      setIssuedTokenRecovery(null);
      setTokenSecret(null);
      setTokenSecretAcknowledged(false);
      setModal(null);
      setToast({
        message: "All identified tokens from the uncertain create request are revoked or expired. Token generation is safe again.",
        kind: "success",
      });
      return;
    }
    if (
      unresolvedCandidates.length === 1
      && unresolvedCandidates[0].state !== IngestionTokenState.INGESTION_TOKEN_STATE_UNSPECIFIED
      && unresolvedCandidates[0].state !== IngestionTokenState.UNRECOGNIZED
    ) {
      const candidate = unresolvedCandidates[0];
      if (!persistTokenCreateGuard(nextRecovery, candidate.ingestionTokenId)) return;
      setTokenCreateRecovery(null);
      setIssuedToken(candidate);
      setIssuedTokenRecovery(nextRecovery);
      setTokenSecret(null);
      setTokenSecretAcknowledged(false);
      setModal("create-token");
      setToast({
        message: tokenCanBeRevoked(candidate)
          ? `A newly created token (${candidate.tokenPrefix}) was identified, but its one-time secret was lost. Revoke it before leaving.`
          : `A newly created token (${candidate.tokenPrefix}) was identified without its secret and is already ${tokenStateLabel(candidate.state).toLowerCase()}.`,
        kind: "warning",
      });
      return;
    }
    const unresolvedRecovery: TokenCreateRecovery = {
      ...nextRecovery,
      candidates: unresolvedCandidates,
      reconciliationError: null,
    };
    if (!persistTokenCreateGuard(unresolvedRecovery, null)) return;
    setTokenCreateRecovery(unresolvedRecovery);
    setToast({
      message: unresolvedCandidates.length === 0
        ? "The create request may still have produced a token. No matching token is visible yet; do not submit another create request."
        : `${unresolvedCandidates.length.toLocaleString()} new matching tokens prevent safe automatic identification. Review the possible tokens and check again; do not submit another create request.`,
      kind: "warning",
    });
  }

  async function reconcileTokenCreateRecovery(
    recovery: TokenCreateRecovery,
    inheritedOperationGeneration?: number,
  ) {
    if (!requireTokenGuardOwnership(recovery)) return;
    const operationGeneration = inheritedOperationGeneration
      ?? beginTokenRecoveryOperation();
    setBusy("reconcile-token-create");
    try {
      const candidates = await findTokenCreateCandidates(recovery);
      if (!tokenRecoveryOperationIsCurrent(operationGeneration, recovery)) return;
      applyTokenCreateCandidates(recovery, candidates);
    } catch (error) {
      if (!tokenRecoveryOperationIsCurrent(operationGeneration, recovery)) return;
      const failedRecovery: TokenCreateRecovery = {
        ...recovery,
        reconciliationError: errorMessage(error),
      };
      if (!persistTokenCreateGuard(failedRecovery, null)) return;
      setTokenCreateRecovery(failedRecovery);
      setToast({
        message: `The create outcome is still unknown and reconciliation failed: ${errorMessage(error)} Do not retry token generation.`,
        kind: "warning",
      });
    } finally {
      if (
        componentMountedRef.current
        && tokenRecoveryOperationGenerationRef.current === operationGeneration
      ) {
        setBusy(null);
      }
    }
  }

  async function revokeTokenCreateCandidate(candidate: IngestionToken) {
    const recovery = tokenCreateRecovery;
    if (recovery === null) return;
    if (!requireTokenGuardOwnership(recovery)) return;
    if (!tokenCanBeRevoked(candidate)) return;
    const operationGeneration = beginTokenRecoveryOperation();
    const activeRecovery = recovery;
    cancelTokenLoadMoreRequest();
    setBusy(`recover-token-${candidate.ingestionTokenId}`);

    async function continueAfterConfirmedRevoke(revoked: IngestionToken) {
      if (!tokenRecoveryOperationIsCurrent(operationGeneration, activeRecovery)) return;
      storeTokenSnapshot(revoked);
      const reconciledIds = new Set(activeRecovery.preexistingTokenIds);
      reconciledIds.add(revoked.ingestionTokenId);
      const confirmedRevokedTokenIds = new Set(activeRecovery.confirmedRevokedTokenIds);
      confirmedRevokedTokenIds.add(revoked.ingestionTokenId);
      const nextRecovery: TokenCreateRecovery = {
        ...activeRecovery,
        preexistingTokenIds: reconciledIds,
        confirmedRevokedTokenIds,
        candidates: activeRecovery.candidates.filter((token) =>
          token.ingestionTokenId !== revoked.ingestionTokenId),
        reconciliationError: null,
      };
      if (!persistTokenCreateGuard(nextRecovery, null)) return;
      setTokenCreateRecovery(nextRecovery);
      try {
        const candidates = await findTokenCreateCandidates(nextRecovery);
        if (!tokenRecoveryOperationIsCurrent(operationGeneration, nextRecovery)) return;
        applyTokenCreateCandidates(nextRecovery, candidates);
      } catch (error) {
        if (!tokenRecoveryOperationIsCurrent(operationGeneration, nextRecovery)) return;
        const failedRecovery: TokenCreateRecovery = {
          ...nextRecovery,
          reconciliationError: errorMessage(error),
        };
        if (!persistTokenCreateGuard(failedRecovery, null)) return;
        setTokenCreateRecovery(failedRecovery);
        setToast({
          message: `Candidate ${revoked.tokenPrefix} was revoked, but the remaining create outcome could not be reconciled: ${errorMessage(error)}`,
          kind: "warning",
        });
      }
    }

    try {
      const response = await client.ingestionTokens.revoke({
        ingestionTokenId: candidate.ingestionTokenId,
        expectedVersion: candidate.version,
        reason: undefined,
      });
      const revoked = response.ingestionToken;
      if (
        revoked === undefined
        || revoked.ingestionTokenId !== candidate.ingestionTokenId
        || revoked.state !== IngestionTokenState.INGESTION_TOKEN_STATE_REVOKED
      ) {
        throw new Error("The server did not confirm candidate revocation.");
      }
      await continueAfterConfirmedRevoke(revoked);
    } catch (error) {
      if (!tokenRecoveryOperationIsCurrent(operationGeneration, activeRecovery)) return;
      try {
        const current = await readCurrentToken(candidate);
        if (!tokenRecoveryOperationIsCurrent(operationGeneration, activeRecovery)) return;
        storeTokenSnapshot(current);
        if (tokenIsTerminallySafe(current)) {
          await continueAfterConfirmedRevoke(current);
        } else {
          const failedRecovery: TokenCreateRecovery = {
            ...activeRecovery,
            candidates: activeRecovery.candidates.map((token) =>
              token.ingestionTokenId === current.ingestionTokenId ? current : token),
          };
          if (!persistTokenCreateGuard(failedRecovery, null)) return;
          setTokenCreateRecovery(failedRecovery);
          setToast({
            message: `Candidate revocation was not confirmed: ${errorMessage(error)} The latest ${tokenStateLabel(current.state).toLowerCase()} version remains in the recovery list.`,
            kind: "warning",
          });
        }
      } catch (refreshError) {
        if (!tokenRecoveryOperationIsCurrent(operationGeneration, activeRecovery)) return;
        setToast({
          message: `Candidate revocation was not confirmed: ${errorMessage(error)} Reconciliation also failed: ${errorMessage(refreshError)}`,
          kind: "warning",
        });
      }
    } finally {
      if (
        componentMountedRef.current
        && tokenRecoveryOperationGenerationRef.current === operationGeneration
      ) {
        setBusy(null);
      }
    }
  }

  async function createToken(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (tokenName.trim().length === 0) return;
    if (serverClockAnchor === null) {
      setToast({
        message: "Token generation is disabled until system bootstrap supplies an authoritative server clock.",
        kind: "warning",
      });
      return;
    }
    if (normalizedApiBaseUrl === null) {
      setToast({
        message: `Token generation is disabled until the API base URL has a canonical backend identity.${apiBaseNormalizationError === null ? "" : ` ${apiBaseNormalizationError}`}`,
        kind: "warning",
      });
      return;
    }
    if (tokenCreateGuardStorageState !== "available") {
      setToast({
        message: "Token generation is disabled because this browser cannot persist a non-secret reload safety record.",
        kind: "warning",
      });
      return;
    }
    if (tokenCreateLockAvailable !== true) {
      setToast({
        message: "Token generation is disabled because the browser cannot acquire the required cross-tab safety lock.",
        kind: "warning",
      });
      return;
    }
    if (tokenCreateScopeInvalid) {
      setToast({
        message: tokenScopeSource === "unavailable"
          ? "Token generation is unavailable until the server returns an authoritative index summary."
          : tokenIndexes.size === 0
            ? "Select at least one active, ingestion-enabled index."
            : "Remove unavailable index scopes before generating the token.",
        kind: "warning",
      });
      return;
    }
    let crossTabLockAcquired = false;
    try {
      await navigator.locks.request(
        tokenCreateLockName(normalizedApiBaseUrl),
        { mode: "exclusive", ifAvailable: true },
        async (lock) => {
          if (lock === null) return;
          crossTabLockAcquired = true;
          const existingGuard = window.localStorage.getItem(
            tokenCreateGuardStorageKey(normalizedApiBaseUrl),
          );
          if (existingGuard !== null) {
            setTokenCreateGuardStorageState("unavailable");
            setTokenCreateGuardStorageError(
              "A durable token safety attempt already exists. Refresh to reconcile it before creating another token.",
            );
            setToast({
              message: "Another tab or prior session already has an unresolved token safety attempt.",
              kind: "warning",
            });
            return;
          }
    const createOperationGeneration = beginTokenRecoveryOperation();
    tokenCreatePreparationControllerRef.current?.abort();
    const preparationController = new AbortController();
    tokenCreatePreparationControllerRef.current = preparationController;
    let requestDispatched = false;
    let requestStartedMonotonicMs: number | null = null;
    let recovery: TokenCreateRecovery | null = null;
    let guardClearedByThisOperation = false;
    setBusy("create-token");
    try {
      const initialServerTimeMs = authoritativeServerNowMs();
      if (initialServerTimeMs === undefined) {
        throw new Error("System bootstrap no longer supplies an authoritative server clock.");
      }
      const expiresAt = expirationFromForm(tokenExpiration, initialServerTimeMs);
      const definition: TokenCreateDefinitionSnapshot = {
        name: tokenName.trim(),
        description: tokenDescription.trim(),
        allowedIndexNames: [...tokenIndexes].toSorted(),
        expiresAt,
        armedServerTimeMs: initialServerTimeMs,
        dispatchedServerTimeMs: null,
        outcomeObservedServerTimeMs: null,
        requestRoundTripMs: null,
        requestTimeoutMs: DEFAULT_REQUEST_TIMEOUT_MS,
        clockUncertaintyMs: serverClockAnchor.uncertaintyMs,
        outcomeKind: "pending",
      };
      const preexistingTokens = await listTokensForCreateSafety(
        client,
        definition.name,
        preparationController.signal,
      );
      if (
        preparationController.signal.aborted
        || !componentMountedRef.current
        || tokenRecoveryOperationGenerationRef.current !== createOperationGeneration
        || tokenCreatePreparationControllerRef.current !== preparationController
      ) return;
      tokenCreatePreparationControllerRef.current = null;
      recovery = {
        attemptId: crypto.randomUUID(),
        ownerId: currentTokenGuardOwnerId(),
        definition,
        preexistingTokenIds: new Set(preexistingTokens.map((token) => token.ingestionTokenId)),
        confirmedRevokedTokenIds: new Set(),
        failureMessage: "The browser has not yet observed the final outcome of this request.",
        candidates: [],
        reconciliationError: null,
      };
      tokenGuardLockOperationAttemptRef.current = recovery.attemptId;
      if (!persistTokenCreateGuard(recovery, null, { allowCreate: true })) {
        throw new Error("The browser could not persist the non-secret token creation safety record.");
      }
      cancelTokenLoadMoreRequest();
      requestStartedMonotonicMs = performance.now();
      const dispatchedServerTimeMs = authoritativeServerNowMs();
      if (dispatchedServerTimeMs === undefined) {
        guardClearedByThisOperation = clearTokenCreateGuard(
          recovery.attemptId,
          recovery.ownerId,
        );
        throw new Error("System bootstrap no longer supplies an authoritative server clock.");
      }
      definition.dispatchedServerTimeMs = dispatchedServerTimeMs;
      requestDispatched = true;
      const responsePromise = client.ingestionTokens.create({
        definition: {
          name: definition.name,
          description: definition.description || undefined,
          constraints: {
            allowedIndexNames: definition.allowedIndexNames,
            allowedHostRegexes: [],
            allowedSourceRegexes: [],
            boundCollectorId: undefined,
          },
          expiresAt: definition.expiresAt,
        },
        clientRequestId: undefined,
      });
      // The transport call has synchronously begun before the guard is updated
      // with its true dispatch mapping. If the tab dies first, the pre-armed
      // null-dispatch record makes all exact new matches visible as outliers.
      if (!persistTokenCreateGuard(recovery, null)) {
        // The request is already in flight. Recovery deliberately proceeds
        // from the durable pre-arm record, while this sink prevents a later
        // transport rejection from escaping as an unhandled promise.
        void responsePromise.catch(() => undefined);
        throw new Error("The browser could not update the token safety record with request dispatch timing.");
      }
      const response = await responsePromise;
      if (!tokenRecoveryOperationIsCurrent(createOperationGeneration, recovery)) return;
      observeTokenCreateOutcome(
        definition,
        requestStartedMonotonicMs,
        "settled-response",
      );
      if (
        response.ingestionToken === undefined
        || response.ingestionToken.ingestionTokenId.length === 0
        || response.ingestionToken.version !== 1n
        || response.ingestionToken.state !== IngestionTokenState.INGESTION_TOKEN_STATE_ACTIVE
        || response.ingestionToken.tokenPrefix.length === 0
        || response.plaintextToken.length === 0
        || !response.plaintextToken.startsWith(response.ingestionToken.tokenPrefix)
      ) {
        throw new Error("The server response did not satisfy the one-time token creation contract.");
      }
      const createdToken = response.ingestionToken;
      if (
        recovery.preexistingTokenIds.has(createdToken.ingestionTokenId)
        || createdToken.createdAt === undefined
        || Number.isNaN(createdToken.createdAt.valueOf())
        || !tokenMatchesCreateDefinition(createdToken, definition)
      ) {
        throw new Error("The server response did not match the token creation request.");
      }
      recovery.failureMessage = "The issued token secret would be unavailable after a reload.";
      if (!persistTokenCreateGuard(recovery, createdToken.ingestionTokenId)) {
        throw new Error("The browser could not persist the issued token safety record.");
      }
      setTokens((current) => [...current, createdToken].toSorted((left, right) =>
        left.name.localeCompare(right.name)));
      setTokenTotalSize(null);
      setTokenTotalSizeExact(false);
      invalidateTokenPagination("The token catalog changed. Refresh to confirm the loaded records.");
      setTokenCreateRecovery(null);
      setIssuedToken(createdToken);
      setIssuedTokenRecovery(recovery);
      setTokenSecret(response.plaintextToken);
      setTokenSecretAcknowledged(false);
    } catch (error) {
      if (
        !requestDispatched
        && (
          preparationController.signal.aborted
          || !componentMountedRef.current
          || (
            tokenRecoveryOperationGenerationRef.current !== createOperationGeneration
            && !guardClearedByThisOperation
          )
        )
      ) return;
      if (
        requestDispatched
        && recovery !== null
        && !tokenRecoveryOperationIsCurrent(createOperationGeneration, recovery)
      ) return;
      if (
        requestDispatched
        && requestStartedMonotonicMs !== null
        && recovery !== null
        && recovery.definition.outcomeObservedServerTimeMs === null
      ) {
        observeTokenCreateOutcome(
          recovery.definition,
          requestStartedMonotonicMs,
          "ambiguous-failure",
        );
      }
      if (!requestDispatched) {
        setToast({
          message: `Token generation was not sent because a safe pre-create check failed: ${errorMessage(error)}`,
          kind: "warning",
        });
      } else if (isDefiniteTokenCreateFailure(error) && recovery !== null) {
        guardClearedByThisOperation = clearTokenCreateGuard(
          recovery.attemptId,
          recovery.ownerId,
        );
        if (isOptionalRouteUnavailable(error)) {
          setTokenState("unavailable");
          setModal(null);
        }
        setToast({ message: errorMessage(error), kind: "warning" });
      } else if (recovery !== null) {
        const ambiguousRecovery: TokenCreateRecovery = {
          ...recovery,
          failureMessage: errorMessage(error),
        };
        if (!persistTokenCreateGuard(ambiguousRecovery, null)) return;
        setTokenSecret(null);
        setIssuedTokenRecovery(null);
        setTokenSecretAcknowledged(false);
        setTokenCreateRecovery(ambiguousRecovery);
        setToast({
          message: "The token create request had an uncertain outcome. Generation is locked while the server is checked for a possible unusable token.",
          kind: "warning",
        });
        await reconcileTokenCreateRecovery(
          ambiguousRecovery,
          createOperationGeneration,
        );
      }
    } finally {
      if (tokenCreatePreparationControllerRef.current === preparationController) {
        tokenCreatePreparationControllerRef.current = null;
      }
      if (
        componentMountedRef.current
        && (
          tokenRecoveryOperationGenerationRef.current === createOperationGeneration
          || guardClearedByThisOperation
        )
      ) {
        setBusy(null);
      }
    }
          const shouldHoldTokenGuardLease = (
            componentMountedRef.current
            && recovery !== null
            && tokenRecoveryOperationGenerationRef.current === createOperationGeneration
            && ownsTokenCreateGuard(recovery)
          );
          if (shouldHoldTokenGuardLease && recovery !== null) {
            const lease = holdTokenGuardLease(recovery.attemptId);
            tokenGuardLockOperationAttemptRef.current = null;
            await lease;
          } else {
            tokenGuardLockOperationAttemptRef.current = null;
          }
        },
      );
    } catch (error) {
      setTokenCreateGuardStorageState("unavailable");
      setTokenCreateGuardStorageError(`Cross-tab token lock failed: ${errorMessage(error)}`);
      setToast({
        message: "The browser could not acquire the cross-tab token safety lock.",
        kind: "warning",
      });
      return;
    }
    if (!crossTabLockAcquired) {
      setToast({
        message: "Another tab is currently creating or reconciling an ingestion token. Try again after it finishes.",
        kind: "warning",
      });
    }
  }

  async function updateToken(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const target = tokenEditTarget;
    if (target === null || tokenName.trim().length === 0 || tokenIndexes.size === 0) return;
    const updateMask: string[] = [];
    if (tokenName.trim() !== target.name) updateMask.push("name");
    if (tokenDescription !== (target.description ?? "")) updateMask.push("description");
    if (!hasSameStrings(tokenIndexes, target.constraints?.allowedIndexNames ?? [])) {
      updateMask.push("constraints");
    }
    if (tokenExpiration !== dateTimeLocalValue(target.expiresAt)) updateMask.push("expires_at");
    if (updateMask.length === 0) return;
    cancelTokenLoadMoreRequest();
    setBusy(`update-token-${target.ingestionTokenId}`);
    try {
      const response = await client.ingestionTokens.update({
        ingestionTokenId: target.ingestionTokenId,
        expectedVersion: target.version,
        definition: {
          name: tokenName.trim(),
          description: tokenDescription.trim() || undefined,
          constraints: {
            allowedHostRegexes: [],
            allowedSourceRegexes: [],
            boundCollectorId: undefined,
            ...target.constraints,
            allowedIndexNames: [...tokenIndexes].toSorted(),
          },
          expiresAt: expirationFromForm(tokenExpiration, authoritativeServerNowMs()),
        },
        updateMask,
      });
      if (response.ingestionToken === undefined) {
        throw new Error("The server returned an empty ingestion token.");
      }
      setTokens((current) => current
        .map((item) =>
          item.ingestionTokenId === response.ingestionToken?.ingestionTokenId
            ? response.ingestionToken as IngestionToken
            : item)
        .toSorted((left, right) => left.name.localeCompare(right.name)));
      invalidateTokenPagination("The token catalog changed. Refresh to confirm the loaded records.");
      setTokenEditTarget(null);
      setModal(null);
      setToast({ message: `Token “${response.ingestionToken.name}” was updated.`, kind: "success" });
    } catch (error) {
      if (isHttpStatus(error, 409)) {
        setTokenEditTarget(null);
        setModal(null);
        setToast({ message: "This token changed on the server. The latest version was reloaded; open Edit and try again.", kind: "warning" });
      } else {
        setToast({ message: errorMessage(error), kind: "warning" });
      }
      load();
    } finally {
      setBusy(null);
    }
  }

  function storeTokenSnapshot(token: IngestionToken) {
    invalidateTokenPagination("The token catalog changed. Refresh to confirm the loaded records.");
    setTokens((current) => {
      const exists = current.some((item) => item.ingestionTokenId === token.ingestionTokenId);
      const next = exists
        ? current.map((item) => item.ingestionTokenId === token.ingestionTokenId ? token : item)
        : [...current, token];
      return next.toSorted((left, right) => left.name.localeCompare(right.name));
    });
  }

  async function readCurrentToken(token: IngestionToken): Promise<IngestionToken> {
    const response = await client.ingestionTokens.get({
      ingestionTokenId: token.ingestionTokenId,
    });
    const current = response.ingestionToken;
    if (current === undefined || current.ingestionTokenId !== token.ingestionTokenId) {
      throw new Error("The server did not return the requested token.");
    }
    return current;
  }

  async function reconcileNormalRevoke(token: IngestionToken, revokeError: unknown) {
    try {
      const current = await readCurrentToken(token);
      storeTokenSnapshot(current);
      if (current.state === IngestionTokenState.INGESTION_TOKEN_STATE_REVOKED) {
        setRevokeTarget(null);
        setToast({ message: `Token “${current.name}” is confirmed revoked.`, kind: "success" });
        return;
      }
      setRevokeTarget(current);
      setToast({
        message: `Revocation was not confirmed: ${errorMessage(revokeError)} The latest ${tokenStateLabel(current.state).toLowerCase()} token version was loaded.`,
        kind: "warning",
      });
    } catch (refreshError) {
      setToast({
        message: `Revocation was not confirmed: ${errorMessage(revokeError)} The token could not be reconciled: ${errorMessage(refreshError)}`,
        kind: "warning",
      });
    }
  }

  async function revokeToken(token: IngestionToken) {
    cancelTokenLoadMoreRequest();
    setBusy(`token-${token.ingestionTokenId}`);
    try {
      const response = await client.ingestionTokens.revoke({
        ingestionTokenId: token.ingestionTokenId,
        expectedVersion: token.version,
        reason: undefined,
      });
      if (
        response.ingestionToken === undefined
        || response.ingestionToken.ingestionTokenId !== token.ingestionTokenId
        || response.ingestionToken.state !== IngestionTokenState.INGESTION_TOKEN_STATE_REVOKED
      ) {
        throw new Error("The server did not confirm token revocation.");
      }
      storeTokenSnapshot(response.ingestionToken);
      setRevokeTarget(null);
      setToast({ message: `Token “${token.name}” was revoked.`, kind: "success" });
    } catch (error) {
      await reconcileNormalRevoke(token, error);
    } finally {
      setBusy(null);
    }
  }

  function acknowledgeIssuedToken() {
    if (issuedToken === null) return;
    if (
      issuedTokenRecovery === null
      || !requireTokenGuardOwnership(issuedTokenRecovery)
    ) return;
    const hasSecret = tokenSecret !== null && tokenSecret.length > 0;
    if ((hasSecret && !tokenSecretAcknowledged) || (!hasSecret && tokenCanBeRevoked(issuedToken))) {
      return;
    }
    if (
      issuedTokenRecovery === null
      || !clearTokenCreateGuard(
        issuedTokenRecovery.attemptId,
        issuedTokenRecovery.ownerId,
      )
    ) {
      setToast({
        message: "The token is safe to dismiss, but the browser could not clear its reload guard. Try Done again before leaving.",
        kind: "warning",
      });
      return;
    }
    setIssuedToken(null);
    setIssuedTokenRecovery(null);
    setTokenSecret(null);
    setTokenSecretAcknowledged(false);
    setModal(null);
  }

  async function revokeIssuedToken() {
    const target = issuedToken;
    const recovery = issuedTokenRecovery;
    if (target === null || recovery === null) return;
    if (!requireTokenGuardOwnership(recovery)) return;
    const operationGeneration = beginTokenRecoveryOperation();
    cancelTokenLoadMoreRequest();
    setBusy(`issued-token-${target.ingestionTokenId}`);
    try {
      const response = await client.ingestionTokens.revoke({
        ingestionTokenId: target.ingestionTokenId,
        expectedVersion: target.version,
        reason: undefined,
      });
      if (!tokenRecoveryOperationIsCurrent(operationGeneration, recovery)) return;
      if (
        response.ingestionToken === undefined
        || response.ingestionToken.ingestionTokenId !== target.ingestionTokenId
        || response.ingestionToken.state !== IngestionTokenState.INGESTION_TOKEN_STATE_REVOKED
      ) {
        throw new Error("The server did not confirm token revocation.");
      }
      storeTokenSnapshot(response.ingestionToken);
      if (
        clearTokenCreateGuard(
          recovery.attemptId,
          recovery.ownerId,
        )
      ) {
        setIssuedToken(null);
        setIssuedTokenRecovery(null);
        setTokenSecret(null);
        setTokenSecretAcknowledged(false);
        setModal(null);
        setToast({ message: `Token “${target.name}” was revoked.`, kind: "success" });
      } else {
        setToast({
          message: `Token “${target.name}” was revoked, but the browser could not clear its reload guard. Use Done to retry.`,
          kind: "warning",
        });
      }
    } catch (error) {
      if (!tokenRecoveryOperationIsCurrent(operationGeneration, recovery)) return;
      try {
        const current = await readCurrentToken(target);
        if (!tokenRecoveryOperationIsCurrent(operationGeneration, recovery)) return;
        storeTokenSnapshot(current);
        if (tokenIsTerminallySafe(current)) {
          if (
            clearTokenCreateGuard(
              recovery.attemptId,
              recovery.ownerId,
            )
          ) {
            setIssuedToken(null);
            setIssuedTokenRecovery(null);
            setTokenSecret(null);
            setTokenSecretAcknowledged(false);
            setModal(null);
            setToast({ message: `Token “${current.name}” is confirmed ${tokenStateLabel(current.state).toLowerCase()}.`, kind: "success" });
          } else {
            setToast({
              message: `Token “${current.name}” is safe, but the browser could not clear its reload guard. Use Done to retry.`,
              kind: "warning",
            });
          }
        } else {
          if (!persistTokenCreateGuard(recovery, current.ingestionTokenId)) return;
          setIssuedToken(current);
          setToast({
            message: `Revocation was not confirmed: ${errorMessage(error)} The latest ${tokenStateLabel(current.state).toLowerCase()} token version was loaded; its one-time secret remains open.`,
            kind: "warning",
          });
        }
      } catch (refreshError) {
        if (!tokenRecoveryOperationIsCurrent(operationGeneration, recovery)) return;
        setToast({
          message: `Revocation was not confirmed: ${errorMessage(error)} Reconciliation also failed: ${errorMessage(refreshError)} The one-time secret remains open.`,
          kind: "warning",
        });
      }
    } finally {
      if (
        componentMountedRef.current
        && tokenRecoveryOperationGenerationRef.current === operationGeneration
      ) {
        setBusy(null);
      }
    }
  }

  useEffect(() => {
    if (normalizedApiBaseUrl === null) return;
    let current = true;
    const controller = new AbortController();
    const stop = () => {
      current = false;
      controller.abort();
      tokenRecoveryOperationGenerationRef.current += 1;
      tokenGuardLockOperationAttemptRef.current = null;
      releaseTokenGuardLease();
    };
    let raw: string | null;
    const lockManager = navigator.locks;
    const lockAvailable = typeof lockManager?.request === "function";
    setTokenCreateLockAvailable(lockAvailable);
    try {
      raw = window.localStorage.getItem(
        tokenCreateGuardStorageKey(normalizedApiBaseUrl),
      );
    } catch (error) {
      setTokenCreateGuardStorageState("unavailable");
      setTokenCreateGuardStorageError(errorMessage(error));
      return stop;
    }
    if (raw === null) {
      setTokenCreateGuardStorageState("available");
      setTokenCreateGuardStorageError(null);
      return stop;
    }
    const restored = parsePersistedTokenCreateGuard(raw, normalizedApiBaseUrl);
    if (restored === null) {
      setTokenCreateGuardStorageState("unavailable");
      setTokenCreateGuardStorageError(
        "A token safety record exists but is invalid. Token generation is locked so a possible credential is not overlooked.",
      );
      setToast({
        message: "A saved token safety record could not be read. Token generation remains locked; do not clear browser storage until the possible credential is reviewed.",
        kind: "warning",
      });
      return stop;
    }

    const { recovery: restoredRecovery, knownIssuedTokenId } = restored;
    setSection("collectors");
    setTokenSecret(null);
    setTokenSecretAcknowledged(false);
    setIssuedToken(null);
    setIssuedTokenRecovery(null);
    setTokenCreateRecovery(restoredRecovery);
    setModal("create-token");
    if (!lockAvailable) {
      setTokenCreateGuardStorageState("unavailable");
      setTokenCreateGuardStorageError(
        "This browser does not expose the cross-tab Web Locks API required to own token recovery safely.",
      );
      return stop;
    }

    setBusy("reconcile-token-create");
    void lockManager.request(
      tokenCreateLockName(normalizedApiBaseUrl),
      { mode: "exclusive", ifAvailable: true, signal: controller.signal },
      async (lock) => {
        if (!current) return;
        if (lock === null) {
          setBusy(null);
          setTokenCreateGuardStorageState("unavailable");
          setTokenCreateGuardStorageError(
            "Another tab currently owns token creation recovery. Close it or refresh after it finishes.",
          );
          setToast({
            message: "Another tab is already reconciling this token request. This tab is locked to read-only recovery.",
            kind: "warning",
          });
          return;
        }
        const recovery: TokenCreateRecovery = {
          ...restoredRecovery,
          ownerId: currentTokenGuardOwnerId(),
        };
        tokenGuardLockOperationAttemptRef.current = recovery.attemptId;
        if (!persistTokenCreateGuard(recovery, knownIssuedTokenId, {
          allowOwnershipTakeover: true,
        })) {
          setBusy(null);
          return;
        }
        setTokenCreateRecovery(recovery);
        try {
          if (knownIssuedTokenId === null) {
            const candidates = await findTokenCreateCandidates(
              recovery,
              controller.signal,
            );
            if (current) applyTokenCreateCandidates(recovery, candidates);
            return;
          }
          try {
            const response = await client.ingestionTokens.get({
              ingestionTokenId: knownIssuedTokenId,
            }, { signal: controller.signal });
            if (!current || !requireTokenGuardOwnership(recovery)) return;
            const token = response.ingestionToken;
            if (token === undefined || token.ingestionTokenId !== knownIssuedTokenId) {
              throw new Error("The server did not return the saved issued token.");
            }
            storeTokenSnapshot(token);
            if (tokenCanBeRevoked(token) || tokenIsTerminallySafe(token)) {
              persistTokenCreateGuard(recovery, token.ingestionTokenId);
              setTokenCreateRecovery(null);
              setIssuedToken(token);
              setIssuedTokenRecovery(recovery);
              setToast({
                message: tokenCanBeRevoked(token)
                  ? `The page reloaded after token ${token.tokenPrefix} was issued. Its one-time secret is gone; revoke it before leaving.`
                  : `The page reloaded after token ${token.tokenPrefix} was issued. Its secret is gone, and the token is now ${tokenStateLabel(token.state).toLowerCase()}.`,
                kind: "warning",
              });
              return;
            }
            const unknownRecovery: TokenCreateRecovery = {
              ...recovery,
              candidates: [token],
              reconciliationError: "The saved issued token has an unknown server state. Check again before leaving.",
            };
            persistTokenCreateGuard(unknownRecovery, token.ingestionTokenId);
            setTokenCreateRecovery(unknownRecovery);
          } catch (error) {
            if (!current || controller.signal.aborted) return;
            if (!requireTokenGuardOwnership(recovery)) return;
            const retryRecovery: TokenCreateRecovery = {
              ...recovery,
              reconciliationError: `The saved issued token could not be read directly: ${errorMessage(error)}`,
            };
            setTokenCreateRecovery(retryRecovery);
            const candidates = await findTokenCreateCandidates(
              retryRecovery,
              controller.signal,
            );
            if (current) applyTokenCreateCandidates(retryRecovery, candidates);
          }
        } catch (error) {
          if (!current || controller.signal.aborted) return;
          if (!requireTokenGuardOwnership(recovery)) return;
          const failedRecovery: TokenCreateRecovery = {
            ...recovery,
            reconciliationError: `The saved token request could not be reconciled: ${errorMessage(error)}`,
          };
          persistTokenCreateGuard(failedRecovery, knownIssuedTokenId);
          setTokenCreateRecovery(failedRecovery);
          setToast({
            message: "The saved token request could not be reconciled. Token generation remains locked.",
            kind: "warning",
          });
        } finally {
          if (current) setBusy(null);
          if (current && ownsTokenCreateGuard(recovery)) {
            const lease = holdTokenGuardLease(recovery.attemptId);
            tokenGuardLockOperationAttemptRef.current = null;
            await lease;
          } else {
            tokenGuardLockOperationAttemptRef.current = null;
          }
        }
      },
    ).catch((error: unknown) => {
      if (!current || controller.signal.aborted) return;
      setBusy(null);
      setTokenCreateGuardStorageState("unavailable");
      setTokenCreateGuardStorageError(`Cross-tab recovery lock failed: ${errorMessage(error)}`);
    });
    return stop;
    // Recovery is intentionally keyed only to the API client. The helper
    // closures are safe for this one mount-time reconciliation pass.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [client, normalizedApiBaseUrl]);

  const loadedIndexScopeOptions: TokenIndexScopeOption[] = indexes.flatMap((index) => {
    const definition = index.definition;
    if (definition === undefined || definition.name.length === 0) return [];
    return [{
      id: index.indexId,
      name: definition.name,
      displayName: definition.displayName || definition.name,
      ingestible: index.state === IndexState.INDEX_STATE_ACTIVE
        && definition.ingestionAccess === IndexAccessState.INDEX_ACCESS_STATE_ENABLED,
    }];
  });
  const tokenScopeSource: TokenScopeSource = bootstrap !== null
    ? "bootstrap"
    : indexState === "available"
      && indexNextPageToken === null
      && indexPaginationError === null
      ? "index-admin"
      : "unavailable";
  const tokenScopeOptions: TokenIndexScopeOption[] = tokenScopeSource === "bootstrap"
    ? (() => {
        const merged = new Map((bootstrap?.indexes ?? []).map((index) => [
          index.name,
          {
            id: index.id,
            name: index.name,
            displayName: index.displayName || index.name,
            ingestible: index.ingestible,
          },
        ]));
        for (const option of loadedIndexScopeOptions) merged.set(option.name, option);
        return [...merged.values()].toSorted((left, right) => left.name.localeCompare(right.name));
      })()
    : tokenScopeSource === "index-admin"
      ? loadedIndexScopeOptions
      : [];
  const ingestibleTokenScopes = tokenScopeOptions.filter((option) => option.ingestible);
  const ingestibleIndexNames = new Set(ingestibleTokenScopes.map((option) => option.name));
  const tokenScopeChanged = tokenEditTarget !== null
    && !hasSameStrings(tokenIndexes, tokenEditTarget.constraints?.allowedIndexNames ?? []);
  const tokenHasUnavailableScope = [...tokenIndexes].some((name) => !ingestibleIndexNames.has(name));
  const tokenScopeInvalid = tokenScopeChanged && tokenHasUnavailableScope;
  const tokenCreationBlockReason = serverClockAnchor === null
    ? "System bootstrap has not supplied an authoritative server clock. Token generation is disabled so a one-time credential can always be reconciled safely."
    : normalizedApiBaseUrl === null
      ? `The API base URL does not yet have a canonical backend identity.${apiBaseNormalizationError === null ? "" : ` ${apiBaseNormalizationError}`}`
    : tokenCreateLockAvailable !== true
      ? tokenCreateLockAvailable === null
        ? "The cross-tab token safety lock check is still running."
        : "This browser does not expose the cross-tab Web Locks API required for safe token generation."
    : tokenCreateGuardStorageState !== "available"
      ? tokenCreateGuardStorageState === "checking"
        ? "The browser reload-safety check is still running."
        : `The browser cannot persist the non-secret token safety record. Token generation is disabled.${tokenCreateGuardStorageError === null ? "" : ` ${tokenCreateGuardStorageError}`}`
      : tokenProtectionActive
        ? "Finish saving or revoking the current one-time token before generating another."
        : null;
  const tokenCreateScopeInvalid = tokenScopeSource === "unavailable"
    || tokenIndexes.size === 0
    || tokenHasUnavailableScope
    || tokenCreationBlockReason !== null;
  const indexDefinition = indexEditTarget?.definition;
  const indexHasChanges = indexDefinition !== undefined && (
    (indexDisplayName.trim() || indexDefinition.name) !== indexDefinition.displayName
    || indexDescription !== (indexDefinition.description ?? "")
    || retention !== retentionFormValue(indexDefinition.retentionPeriod?.seconds)
    || indexIngestionAccess !== indexDefinition.ingestionAccess
    || indexSearchAccess !== indexDefinition.searchAccess
  );
  const tokenHasChanges = tokenEditTarget !== null && (
    tokenName.trim() !== tokenEditTarget.name
    || tokenDescription !== (tokenEditTarget.description ?? "")
    || tokenScopeChanged
    || tokenExpiration !== dateTimeLocalValue(tokenEditTarget.expiresAt)
  );
  const activeIndexes = indexes.filter((index) => index.state === IndexState.INDEX_STATE_ACTIVE).length;
  const activeTokens = tokens.filter((token) => token.state === IngestionTokenState.INGESTION_TOKEN_STATE_ACTIVE).length;
  const tokenRevealOpen = issuedToken !== null;
  const tokenRecoveryOpen = tokenCreateRecovery !== null;
  const tokenResolutionOpen = tokenRevealOpen || tokenRecoveryOpen;
  const hasAvailableAdminRoute = indexState === "available" || tokenState === "available";
  const adminRoutesLoading = indexState === "loading" || tokenState === "loading";
  const connectionStatus = bootstrap !== null
    ? { tone: "healthy", title: "API connected", detail: `Server ${bootstrap.serverVersion || "version unavailable"}` }
    : bootstrapError === null
      ? {
          tone: "running",
          title: "Checking API connection",
          detail: hasAvailableAdminRoute
            ? "Administration routes are responding; system bootstrap is still loading"
            : "Loading system bootstrap and administration routes",
        }
      : hasAvailableAdminRoute
      ? { tone: "warning", title: "Partial API connection", detail: "System bootstrap unavailable" }
      : adminRoutesLoading
        ? {
            tone: "running",
            title: "Checking administration routes",
            detail: "System bootstrap is unavailable; administration route checks are still running",
          }
      : { tone: "error", title: "Admin API unavailable", detail: "No administration route is usable" };
  const primaryAction = section === "indexes" && indexState === "available"
    ? <button className="suite-button suite-button--primary" type="button" onClick={openIndexDialog}>＋ Create index</button>
    : section === "collectors" && tokenState === "available"
      ? <button className="suite-button suite-button--primary" type="button" onClick={openTokenDialog} disabled={ingestibleTokenScopes.length === 0 || tokenCreationBlockReason !== null}>Generate token</button>
      : undefined;

  if (
    bootstrap === null
    && bootstrapError === null
    && indexState === "loading"
    && tokenState === "loading"
  ) {
    return (
      <div className="suite-page admin-page" aria-busy="true">
        <PageHeading eyebrow="SYSTEM" title="Administration" description="Connecting to the configured Open Splunk server." />
        <ResourceMessage kind="loading" title="Loading administration" message="Reading server capabilities and resources…" />
      </div>
    );
  }

  return (
    <div className="suite-page admin-page">
      <PageHeading
        eyebrow="SYSTEM"
        title="Administration"
        description="Manage indexes and ingestion tokens exposed by the connected server."
        actions={primaryAction}
      />

      <div className="admin-mobile-section-picker">
        <label htmlFor="admin-section">Administration section</label>
        <select id="admin-section" value={section} onChange={(event) => setSection(event.target.value as AdminSection)}>
          {NAV_ITEMS.map((item) => <option value={item.key} key={item.key}>{item.label}</option>)}
        </select>
      </div>

      <div className="admin-layout">
        <aside className="admin-sidebar" aria-label="Administration navigation">
          <span className="admin-sidebar-label">SETTINGS</span>
          {NAV_ITEMS.map((item) => (
            <button className={section === item.key ? "active" : undefined} type="button" onClick={() => setSection(item.key)} key={item.key}>
              <i aria-hidden="true">{item.icon}</i><span><strong>{item.label}</strong><small>{item.detail}</small></span><b aria-hidden="true">›</b>
            </button>
          ))}
          <div className="admin-sidebar-meta">
            <span className={`status-dot status-dot--${connectionStatus.tone}`} />
            <div>
              <strong>{connectionStatus.title}</strong>
              <small>{connectionStatus.detail}</small>
            </div>
          </div>
        </aside>

        <section className="admin-content" aria-live="polite">
          {section === "overview" ? (
            <BackendOverview
              bootstrap={bootstrap}
              bootstrapError={bootstrapError}
              indexState={indexState}
              indexCount={indexes.length}
              indexTotalSize={indexTotalSize}
              indexTotalSizeExact={indexTotalSizeExact}
              activeIndexes={activeIndexes}
              tokenState={tokenState}
              tokenCount={tokens.length}
              tokenTotalSize={tokenTotalSize}
              tokenTotalSizeExact={tokenTotalSizeExact}
              activeTokens={activeTokens}
              onNavigate={setSection}
              onReload={load}
            />
          ) : null}
          {section === "indexes" ? (
            <BackendIndexes
              state={indexState}
              error={indexError}
              filter={filter}
              indexes={visibleIndexes}
              totalIndexes={indexes.length}
              totalSize={indexTotalSize}
              totalSizeExact={indexTotalSizeExact}
              hasMore={indexNextPageToken !== null}
              loadingMore={indexLoadingMore}
              paginationError={indexPaginationError}
              busy={busy}
              onFilterChange={setFilter}
              onLoadMore={() => void loadMoreIndexes()}
              onReload={load}
              onEdit={(index) => void openIndexEditor(index)}
              onChangeState={(index) => void changeIndexState(index)}
            />
          ) : null}
          {section === "collectors" ? (
            <BackendTokens
              state={tokenState}
              error={tokenError}
              tokens={tokens}
              totalSize={tokenTotalSize}
              totalSizeExact={tokenTotalSizeExact}
              hasMore={tokenNextPageToken !== null}
              loadingMore={tokenLoadingMore}
              paginationError={tokenPaginationError}
              busy={busy}
              onCreate={openTokenDialog}
              onEdit={(token) => void openTokenEditor(token)}
              onReload={load}
              onLoadMore={() => void loadMoreTokens()}
              onRevoke={setRevokeTarget}
              canCreate={ingestibleTokenScopes.length > 0 && tokenCreationBlockReason === null}
              createBlockReason={tokenCreationBlockReason}
              indexState={indexState}
              indexError={indexError}
              scopeSource={tokenScopeSource}
            />
          ) : null}
          {section === "access" ? (
            <ResourceMessage
              kind="unavailable"
              title="Users and access are not exposed"
              message="This backend does not register an authentication or role-administration API. No preview users are shown in backend mode."
            />
          ) : null}
          {section === "server" ? (
            <BackendServerSettings
              bootstrap={bootstrap}
              error={bootstrapError}
              onReload={load}
            />
          ) : null}
        </section>
      </div>

      {modal === "create-index" ? (
        <Modal
          title="Create index"
          subtitle="Create a searchable and ingestible index on the connected server."
          onClose={() => busy === null && setModal(null)}
          footer={<><button className="button secondary" type="button" onClick={() => setModal(null)} disabled={busy !== null}>Cancel</button><button className="button primary" type="submit" form="create-index-form" disabled={busy !== null || indexName.trim().length === 0}>{busy === "create-index" ? "Creating…" : "Create index"}</button></>}
        >
          <form className="admin-form" id="create-index-form" onSubmit={(event) => void createIndex(event)}>
            <label htmlFor="new-index-name"><span>Index name</span><input id="new-index-name" value={indexName} onChange={(event) => setIndexName(event.target.value)} placeholder="application-logs" autoComplete="off" /><small>Lowercase letters, numbers, hyphens, and underscores; “kvstore” is reserved. The name cannot be changed later.</small></label>
            <label htmlFor="new-index-display-name"><span>Display name <small>(optional)</small></span><input id="new-index-display-name" value={indexDisplayName} onChange={(event) => setIndexDisplayName(event.target.value)} placeholder="Application logs" /><small>Shown to administrators. Defaults to the immutable index name.</small></label>
            <label htmlFor="new-index-description"><span>Description <small>(optional)</small></span><input id="new-index-description" value={indexDescription} onChange={(event) => setIndexDescription(event.target.value)} placeholder="Application and request logs" /></label>
            <label htmlFor="new-index-retention"><span>Retention</span><select id="new-index-retention" value={retention} onChange={(event) => setRetention(event.target.value)}><option value="7">7 days</option><option value="14">14 days</option><option value="30">30 days</option><option value="90">90 days</option><option value="forever">Forever</option></select><small>The server applies this period to stored events.</small></label>
          </form>
        </Modal>
      ) : null}

      {modal === "edit-index" && indexEditTarget?.definition !== undefined ? (
        <Modal
          title={`Edit index ${indexEditTarget.definition.name}`}
          subtitle="Update retention and data access without changing the SPL index name."
          onClose={() => {
            if (busy !== null) return;
            setIndexEditTarget(null);
            setModal(null);
          }}
          footer={<><button className="button secondary" type="button" onClick={() => { setIndexEditTarget(null); setModal(null); }} disabled={busy !== null}>Cancel</button><button className="button primary" type="submit" form="edit-index-form" disabled={busy !== null || !indexHasChanges}>{busy === `update-index-${indexEditTarget.indexId}` ? "Saving…" : "Save changes"}</button></>}
        >
          <form className="admin-form" id="edit-index-form" onSubmit={(event) => void updateIndex(event)}>
            <label htmlFor="edit-index-name"><span>Index name</span><input id="edit-index-name" value={indexEditTarget.definition.name} disabled /><small>Index names are immutable because searches and collectors reference them directly.</small></label>
            <label htmlFor="edit-index-display-name"><span>Display name</span><input id="edit-index-display-name" value={indexDisplayName} onChange={(event) => setIndexDisplayName(event.target.value)} placeholder={indexEditTarget.definition.name} /><small>Change the operator-facing label without changing the SPL index name.</small></label>
            <label htmlFor="edit-index-description"><span>Description <small>(optional)</small></span><input id="edit-index-description" value={indexDescription} onChange={(event) => setIndexDescription(event.target.value)} placeholder="Application and request logs" /></label>
            <label htmlFor="edit-index-retention">
              <span>Retention</span>
              <select id="edit-index-retention" value={retention} onChange={(event) => setRetention(event.target.value)}>
                {!["7", "14", "30", "90", "forever"].includes(retention) ? <option value={retention}>{formatDuration(indexEditTarget.definition.retentionPeriod?.seconds)} (current)</option> : null}
                <option value="7">7 days</option><option value="14">14 days</option><option value="30">30 days</option><option value="90">90 days</option><option value="forever">Forever</option>
              </select>
              <small>Changing retention affects how long stored events remain available.</small>
            </label>
            <label htmlFor="edit-index-ingestion-access"><span>Ingestion access</span><select id="edit-index-ingestion-access" value={indexIngestionAccess} onChange={(event) => setIndexIngestionAccess(Number(event.target.value) as IndexAccessState)}><option value={IndexAccessState.INDEX_ACCESS_STATE_ENABLED}>Enabled</option><option value={IndexAccessState.INDEX_ACCESS_STATE_DISABLED}>Disabled</option></select><small>Disabled indexes reject new events and cannot be added to new token scopes.</small></label>
            <label htmlFor="edit-index-search-access"><span>Search access</span><select id="edit-index-search-access" value={indexSearchAccess} onChange={(event) => setIndexSearchAccess(Number(event.target.value) as IndexAccessState)}><option value={IndexAccessState.INDEX_ACCESS_STATE_ENABLED}>Enabled</option><option value={IndexAccessState.INDEX_ACCESS_STATE_DISABLED}>Disabled</option></select><small>Disabled indexes remain configured but cannot be queried.</small></label>
          </form>
        </Modal>
      ) : null}

      {modal === "create-token" ? (
        <Modal
          title={tokenRecoveryOpen
            ? "Resolve uncertain token creation"
            : tokenRevealOpen
              ? "Save this token now"
              : "Generate ingestion token"}
          subtitle={tokenRecoveryOpen
            ? "Do not generate another token until this non-idempotent request is reconciled."
            : tokenRevealOpen
              ? "The server reveals this plaintext credential only once."
              : "Scope a new credential to one or more ingestible indexes."}
          dismissible={!tokenResolutionOpen}
          initialFocus={tokenRecoveryOpen
            ? busy === null ? "#reconcile-token-create" : undefined
            : tokenRevealOpen
            ? tokenSecret === null || tokenSecret.length === 0
              ? busy === null ? "#revoke-issued-token" : undefined
              : "#copy-issued-token"
            : "#new-token-name"}
          onClose={() => {
            if (busy !== null || tokenResolutionOpen) return;
            setTokenSecret(null);
            setModal(null);
          }}
          footer={tokenRecoveryOpen
            ? (
              <button
                id="reconcile-token-create"
                className="button primary"
                type="button"
                disabled={busy !== null || tokenCreateGuardStorageState !== "available" || tokenCreateLockAvailable !== true}
                onClick={() => void reconcileTokenCreateRecovery(tokenCreateRecovery)}
              >
                {busy === "reconcile-token-create" ? "Checking server…" : "Check for created token"}
              </button>
            )
            : !tokenRevealOpen
            ? <><button className="button secondary" type="button" onClick={() => setModal(null)} disabled={busy !== null}>Cancel</button><button className="button primary" type="submit" form="create-token-form" disabled={busy !== null || tokenName.trim().length === 0 || tokenCreateScopeInvalid}>{busy === "create-token" ? "Generating…" : "Generate token"}</button></>
            : (
              <>
                <button id="revoke-issued-token" className="button danger" type="button" disabled={busy !== null || tokenCreateGuardStorageState !== "available" || tokenCreateLockAvailable !== true || !tokenCanBeRevoked(issuedToken)} onClick={() => void revokeIssuedToken()}>
                  {busy === `issued-token-${issuedToken.ingestionTokenId}` ? "Revoking…" : "Revoke unused token"}
                </button>
                <button
                  className="button primary"
                  type="button"
                  disabled={busy !== null || tokenCreateGuardStorageState !== "available" || tokenCreateLockAvailable !== true || (
                    tokenSecret !== null && tokenSecret.length > 0
                      ? !tokenSecretAcknowledged
                      : tokenCanBeRevoked(issuedToken)
                  )}
                  onClick={acknowledgeIssuedToken}
                >
                  Done
                </button>
              </>
            )}
        >
          {tokenRecoveryOpen ? (
            <div className="token-create-recovery">
              <div className="access-mode-notice" role="alert">
                <span>!</span>
                <div>
                  <strong>A token may exist without a recoverable secret</strong>
                  <p>The create request failed ambiguously after it was sent: {tokenCreateRecovery.failureMessage}. Creating another token now could leave duplicate live credentials.</p>
                </div>
              </div>
              {tokenCreateRecovery.reconciliationError === null ? null : (
                <div className="access-mode-notice" role="alert">
                  <span>!</span>
                  <div><strong>Reconciliation could not finish</strong><p>{tokenCreateRecovery.reconciliationError}</p></div>
                </div>
              )}
              <div className="token-recovery-summary">
                <strong>{tokenCreateRecovery.candidates.length === 0
                  ? "No unique new token is visible yet"
                  : tokenCreateRecovery.candidates.length === 1
                    ? "1 possible new token has an unknown state"
                    : `${tokenCreateRecovery.candidates.length.toLocaleString()} possible new tokens`}</strong>
                <p>{tokenCreateRecovery.candidates.length === 0
                  ? "The original request may still be completing. Check again; token generation remains locked."
                  : tokenCreateRecovery.candidates.length === 1
                    ? "The matching token cannot be safely revoked until the server reports a usable state. Check again to refresh its metadata."
                    : "The server returned more than one new token matching this request. Confirm each prefix before revoking possible credentials here, then check again after unrelated candidates are handled."}</p>
              </div>
              {tokenCreateRecovery.candidates.length === 0 ? null : (
                <ul className="token-recovery-list" aria-label="Possible tokens created by the uncertain request">
                  {tokenCreateRecovery.candidates.map((candidate) => (
                    <li key={candidate.ingestionTokenId}>
                      <div><strong>{candidate.name}</strong><code>{candidate.tokenPrefix}</code><small>Created {formatDate(candidate.createdAt)}</small>{tokenFallsWithinCreateAttributionWindow(candidate, tokenCreateRecovery.definition) ? null : <small className="table-warning-detail">Outside expected request window · manual review required</small>}</div>
                      <span className={`status-label status-label--${statusClass(tokenStateLabel(candidate.state))}`}><i />{tokenStateLabel(candidate.state)}</span>
                      <button
                        className="button danger"
                        type="button"
                        disabled={busy !== null || tokenCreateGuardStorageState !== "available" || tokenCreateLockAvailable !== true || !tokenCanBeRevoked(candidate)}
                        onClick={() => void revokeTokenCreateCandidate(candidate)}
                        aria-label={`Revoke possible token ${candidate.tokenPrefix}`}
                      >
                        {busy === `recover-token-${candidate.ingestionTokenId}`
                          ? "Revoking…"
                          : tokenCanBeRevoked(candidate)
                            ? "Revoke candidate"
                            : `Cannot revoke ${tokenStateLabel(candidate.state).toLowerCase()} state`}
                      </button>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          ) : !tokenRevealOpen ? (
            <form className="admin-form" id="create-token-form" onSubmit={(event) => void createToken(event)}>
              <label htmlFor="new-token-name"><span>Token name</span><input id="new-token-name" value={tokenName} onChange={(event) => setTokenName(event.target.value)} placeholder="prod-api-collector" autoComplete="off" /></label>
              <label htmlFor="new-token-description"><span>Description <small>(optional)</small></span><input id="new-token-description" value={tokenDescription} onChange={(event) => setTokenDescription(event.target.value)} placeholder="Production collector credential" /></label>
              <TokenScopePicker idPrefix="new-token" options={tokenScopeOptions} selected={tokenIndexes} onChange={setTokenIndexes} disabled={tokenScopeSource === "unavailable"} />
              {tokenHasUnavailableScope ? <div className="access-mode-notice" role="alert"><span>!</span><div><strong>Choose an available scope</strong><p>Tokens can only be generated for active, ingestion-enabled indexes. Remove the unavailable scope before continuing.</p></div></div> : null}
              {tokenScopeSource === "unavailable" ? <div className="access-mode-notice" role="note"><span>i</span><div><strong>Index scopes are unavailable</strong><p>Token generation is disabled until the server returns an authoritative index summary.</p></div></div> : null}
              {tokenCreationBlockReason === null ? null : <div className="access-mode-notice" role="alert"><span>!</span><div><strong>Token generation is locked</strong><p>{tokenCreationBlockReason}</p></div></div>}
              <label htmlFor="new-token-expiration"><span>Expiration <small>(optional)</small></span><input id="new-token-expiration" type="datetime-local" value={tokenExpiration} onChange={(event) => setTokenExpiration(event.target.value)} /><small>Leave blank for a token that does not expire. Any expiration must be in the future.</small></label>
            </form>
          ) : (
            <div className="token-reveal">
              <span className="token-warning-icon">!</span>
              {tokenSecret === null || tokenSecret.length === 0 ? (
                <p>{tokenCanBeRevoked(issuedToken)
                  ? "The server created this token without returning its plaintext secret. Revoke the unusable token before leaving this dialog."
                  : `This token was identified without its plaintext secret and is confirmed ${tokenStateLabel(issuedToken.state).toLowerCase()}. It is no longer usable.`}</p>
              ) : (
                <>
                  <p>Copy this credential now. Closing, reloading, or navigating away cannot reveal it again.</p>
                  <div><code>{tokenSecret}</code><button id="copy-issued-token" type="button" onClick={() => void navigator.clipboard.writeText(tokenSecret).then(() => setToast({ message: "Token copied to the clipboard.", kind: "success" }), () => setToast({ message: "Copy failed. Select the token text and copy it manually.", kind: "warning" }))}>Copy token</button></div>
                  <label className="admin-checkbox" htmlFor="token-secret-acknowledgement" aria-label="I stored this ingestion token securely">
                    <input
                      id="token-secret-acknowledgement"
                      type="checkbox"
                      checked={tokenSecretAcknowledged}
                      onChange={(event) => setTokenSecretAcknowledged(event.target.checked)}
                    />
                    <span><strong>I stored this token securely</strong><small>Required before this one-time secret can be dismissed.</small></span>
                  </label>
                </>
              )}
            </div>
          )}
        </Modal>
      ) : null}

      {modal === "edit-token" && tokenEditTarget !== null ? (
        <Modal
          title={`Edit token ${tokenEditTarget.name}`}
          subtitle={`Update metadata and index scopes for ${tokenEditTarget.tokenPrefix}. The secret is never retrieved.`}
          onClose={() => {
            if (busy !== null) return;
            setTokenEditTarget(null);
            setModal(null);
          }}
          footer={<><button className="button secondary" type="button" onClick={() => { setTokenEditTarget(null); setModal(null); }} disabled={busy !== null}>Cancel</button><button className="button primary" type="submit" form="edit-token-form" disabled={busy !== null || !tokenHasChanges || tokenName.trim().length === 0 || tokenIndexes.size === 0 || tokenScopeInvalid}>{busy === `update-token-${tokenEditTarget.ingestionTokenId}` ? "Saving…" : "Save changes"}</button></>}
        >
          <form className="admin-form" id="edit-token-form" onSubmit={(event) => void updateToken(event)}>
            <label htmlFor="edit-token-name"><span>Token name</span><input id="edit-token-name" value={tokenName} onChange={(event) => setTokenName(event.target.value)} autoComplete="off" /></label>
            <label htmlFor="edit-token-description"><span>Description <small>(optional)</small></span><input id="edit-token-description" value={tokenDescription} onChange={(event) => setTokenDescription(event.target.value)} placeholder="Production collector credential" /></label>
            <TokenScopePicker idPrefix="edit-token" options={tokenScopeOptions} selected={tokenIndexes} onChange={setTokenIndexes} disabled={tokenScopeSource === "unavailable"} />
            {tokenScopeInvalid ? <div className="access-mode-notice" role="alert"><span>!</span><div><strong>Remove unavailable scopes</strong><p>Tokens can only be saved with active, ingestion-enabled indexes. Uncheck the unavailable scope before saving.</p></div></div> : null}
            {tokenScopeSource === "unavailable" ? <div className="access-mode-notice" role="note"><span>i</span><div><strong>Index scopes are read-only</strong><p>No authoritative index summary is available. Other token metadata can still be saved while the existing scope is preserved.</p></div></div> : null}
            {tokenScopeSource === "bootstrap" ? <div className="access-mode-notice" role="note"><span>i</span><div><strong>Using the complete bootstrap scope catalog</strong><p>The server&apos;s complete eligibility summary supplies unloaded indexes; loaded versioned definitions override matching entries.</p></div></div> : null}
            <label htmlFor="edit-token-expiration"><span>Expiration <small>(optional)</small></span><input id="edit-token-expiration" type="datetime-local" value={tokenExpiration} onChange={(event) => setTokenExpiration(event.target.value)} /><small>Leave blank for no expiration. Setting a new expiration does not reveal or rotate the secret.</small></label>
          </form>
        </Modal>
      ) : null}

      {revokeTarget !== null ? (
        <Modal
          title="Revoke ingestion token"
          subtitle="Collectors using this credential will no longer be able to ingest data."
          onClose={() => busy === null && setRevokeTarget(null)}
          footer={<><button className="button secondary" type="button" onClick={() => setRevokeTarget(null)} disabled={busy !== null}>Keep token</button><button className="button danger" type="button" disabled={busy !== null || !tokenCanBeRevoked(revokeTarget)} onClick={() => void revokeToken(revokeTarget)}>{busy === `token-${revokeTarget.ingestionTokenId}` ? "Revoking…" : tokenCanBeRevoked(revokeTarget) ? "Revoke token" : `Token is ${tokenStateLabel(revokeTarget.state).toLowerCase()}`}</button></>}
        >
          <div className="access-mode-notice" role="note"><span>!</span><div><strong>This action cannot be undone</strong><p>Revoke <code>{revokeTarget.name}</code> ({revokeTarget.tokenPrefix}) scoped to {revokeTarget.constraints?.allowedIndexNames.join(", ") || "its configured indexes"}.</p></div></div>
        </Modal>
      ) : null}

      {toast === null ? null : <output className={`toast toast-${toast.kind}`}><span aria-hidden="true">{toast.kind === "success" ? "✓" : "!"}</span><strong>{toast.message}</strong><button type="button" aria-label="Dismiss notification" onClick={() => setToast(null)}>×</button></output>}
    </div>
  );
}

interface TokenScopePickerProps {
  idPrefix: string;
  options: TokenIndexScopeOption[];
  selected: Set<string>;
  onChange: (value: Set<string>) => void;
  disabled?: boolean;
}

function TokenScopePicker({ idPrefix, options, selected, onChange, disabled = false }: TokenScopePickerProps) {
  const optionByName = new Map(options.map((option) => [option.name, option]));
  const ingestibleNames = options.filter((option) => option.ingestible).map((option) => option.name);
  const ingestibleSet = new Set(ingestibleNames);
  const choices = [...ingestibleNames, ...[...selected].filter((name) => !ingestibleSet.has(name))];

  return (
    <fieldset>
      <legend>Allowed indexes</legend>
      {choices.map((name) => {
        const option = optionByName.get(name);
        const available = ingestibleSet.has(name);
        const inputId = `${idPrefix}-index-${option?.id ?? name}`;
        return (
          <label className="admin-checkbox" htmlFor={inputId} aria-label={`Allow ingestion to ${name}`} key={name}>
            <input
              id={inputId}
              type="checkbox"
              checked={selected.has(name)}
              disabled={disabled}
              onChange={(event) => {
                const next = new Set(selected);
                if (event.target.checked) next.add(name);
                else next.delete(name);
                onChange(next);
              }}
            />
            <span>
              <strong>{name}</strong>
              <small>{available
                ? option?.displayName || "Ingestion enabled"
                : disabled
                  ? "Current scope · index eligibility unavailable"
                  : "Unavailable for ingestion · remove to save"}</small>
            </span>
          </label>
        );
      })}
      {choices.length === 0 ? <p className="resource-footnote">No active, ingestion-enabled indexes are available.</p> : null}
    </fieldset>
  );
}

interface ResourceMessageProps {
  kind: "loading" | "error" | "unavailable" | "empty";
  title: string;
  message: string;
  action?: React.ReactNode;
}

function ResourceMessage({ kind, title, message, action }: ResourceMessageProps) {
  return (
    <div className={`backend-resource-state backend-resource-state--${kind}`} role={kind === "error" ? "alert" : "status"}>
      <span aria-hidden="true">{kind === "loading" ? "↻" : kind === "error" ? "!" : kind === "empty" ? "∅" : "i"}</span>
      <div><strong>{title}</strong><p>{message}</p></div>
      {action}
    </div>
  );
}

interface BackendOverviewProps {
  bootstrap: SystemBootstrapModel | null;
  bootstrapError: string | null;
  indexState: ResourceState;
  indexCount: number;
  indexTotalSize: bigint | null;
  indexTotalSizeExact: boolean;
  activeIndexes: number;
  tokenState: ResourceState;
  tokenCount: number;
  tokenTotalSize: bigint | null;
  tokenTotalSizeExact: boolean;
  activeTokens: number;
  onNavigate: (section: AdminSection) => void;
  onReload: () => void;
}

function BackendOverview(props: BackendOverviewProps) {
  const { bootstrap } = props;
  const indexCount = countLabel(
    props.indexCount,
    props.indexTotalSize,
    props.indexTotalSizeExact,
    "index",
    "indexes",
  );
  const tokenCount = countLabel(
    props.tokenCount,
    props.tokenTotalSize,
    props.tokenTotalSizeExact,
    "token",
    "tokens",
  );
  const indexDetail = props.indexState === "available"
    ? `${props.activeIndexes.toLocaleString()} active in loaded records`
    : props.indexState === "loading"
      ? "Loading catalog…"
      : props.indexState === "error"
        ? "Load failed"
        : "Route unavailable";
  const tokenDetail = props.tokenState === "available"
    ? `${props.activeTokens.toLocaleString()} active in loaded records`
    : props.tokenState === "loading"
      ? "Loading tokens…"
      : props.tokenState === "error"
        ? "Load failed"
        : "Route unavailable";
  return (
    <div className="admin-section-stack">
      <header className="admin-section-header"><div><h2>System overview</h2><p>Capabilities reported by the available server routes.</p></div><button className="suite-button" type="button" onClick={props.onReload}>Refresh</button></header>
      <div className="admin-summary-grid">
        <article><span className="summary-icon summary-icon--green">▦</span><div><small>Indexes</small><strong>{props.indexState === "available" ? indexCount : "—"}</strong><p>{indexDetail}</p></div><button type="button" onClick={() => props.onNavigate("indexes")}>Manage</button></article>
        <article><span className="summary-icon summary-icon--blue">⇣</span><div><small>Ingestion tokens</small><strong>{props.tokenState === "available" ? tokenCount : "—"}</strong><p>{tokenDetail}</p></div><button type="button" onClick={() => props.onNavigate("collectors")}>Inspect</button></article>
        <article><span className="summary-icon summary-icon--violet">⌕</span><div><small>SPL contract</small><strong>{bootstrap?.splCompatibilityVersion || "—"}</strong><p>{bootstrap === null ? "Bootstrap unavailable" : "Server advertised"}</p></div><Link href="/search/">Search</Link></article>
        <article><span className="summary-icon summary-icon--orange">↻</span><div><small>Result retention</small><strong>{bootstrap !== null && bootstrap.limits.searchResultRetentionMs > 0 ? `${Math.round(bootstrap.limits.searchResultRetentionMs / 60_000)}m` : "—"}</strong><p>{bootstrap === null ? "Bootstrap unavailable" : "Read-only server limit"}</p></div><button type="button" onClick={() => props.onNavigate("server")}>Limits</button></article>
      </div>
      {bootstrap === null ? (
        <ResourceMessage
          kind="error"
          title="System bootstrap could not be loaded"
          message={`${props.bootstrapError ?? "The bootstrap route did not return a usable response."} Index and token routes were checked independently and remain available where shown.`}
          action={<button type="button" onClick={props.onReload}>Retry bootstrap</button>}
        />
      ) : (
        <section className="suite-card">
          <header className="suite-card-header"><div><h3>Connection details</h3><p>Values returned by system bootstrap.</p></div><span className="status-label status-label--complete"><i />Connected</span></header>
          <dl className="backend-definition-list">
            <div><dt>Server version</dt><dd>{bootstrap.serverVersion || "Not reported"}</dd></div>
            <div><dt>API version</dt><dd>{bootstrap.apiVersion || "Not reported"}</dd></div>
            <div><dt>Server time</dt><dd>{formatDate(bootstrap.serverTime)}</dd></div>
            <div><dt>Feature flags</dt><dd>{bootstrap.features.size.toLocaleString()}</dd></div>
          </dl>
        </section>
      )}
    </div>
  );
}

interface BackendIndexesProps {
  state: ResourceState;
  error: string | null;
  filter: string;
  indexes: Index[];
  totalIndexes: number;
  totalSize: bigint | null;
  totalSizeExact: boolean;
  hasMore: boolean;
  loadingMore: boolean;
  paginationError: string | null;
  busy: string | null;
  onFilterChange: (value: string) => void;
  onLoadMore: () => void;
  onReload: () => void;
  onEdit: (index: Index) => void;
  onChangeState: (index: Index) => void;
}

function BackendIndexes(props: BackendIndexesProps) {
  if (props.state === "loading") return <ResourceMessage kind="loading" title="Loading indexes" message="Reading the server index catalog…" />;
  if (props.state === "unavailable") return <ResourceMessage kind="unavailable" title="Index administration is unavailable" message="The connected server did not register the index administration routes." action={<button type="button" onClick={props.onReload}>Retry</button>} />;
  if (props.state === "error") return <ResourceMessage kind="error" title="Indexes could not be loaded" message={props.error ?? "The server rejected the index catalog request."} action={<button type="button" onClick={props.onReload}>Retry</button>} />;

  const loadedCount = countLabel(
    props.totalIndexes,
    props.totalSize,
    props.totalSizeExact,
    "index",
    "indexes",
  );

  return (
    <div className="admin-section-stack">
      <header className="admin-section-header"><div><h2>Indexes</h2><p>Authoritative index definitions from the connected server.</p></div><span>{loadedCount}</span></header>
      <div className="resource-toolbar"><label><span className="sr-only">Filter loaded indexes</span><i aria-hidden="true">⌕</i><input value={props.filter} onChange={(event) => props.onFilterChange(event.target.value)} placeholder="Filter loaded indexes" /></label><button type="button" onClick={props.onReload}>Refresh</button></div>
      {props.indexes.length === 0 ? (
        <ResourceMessage kind="empty" title={props.totalIndexes === 0 ? "No indexes configured" : "No matching indexes"} message={props.totalIndexes === 0 ? "Create an index to begin accepting and searching data." : "Try another index name or description."} />
      ) : (
        <div className="suite-card resource-table-card">
          <div className="responsive-table-wrap">
            <table className="product-table admin-resource-table">
              <thead><tr><th scope="col">Name</th><th scope="col">State</th><th scope="col">Ingestion</th><th scope="col">Search</th><th scope="col">Retention</th><th scope="col">Updated</th><th scope="col"><span className="sr-only">Actions</span></th></tr></thead>
              <tbody>{props.indexes.map((index) => {
                const definition = index.definition;
                const name = definition?.name || index.indexId;
                const state = indexStateLabel(index.state);
                const canChange = index.state === IndexState.INDEX_STATE_ACTIVE || index.state === IndexState.INDEX_STATE_ARCHIVED;
                const canEdit = index.state !== IndexState.INDEX_STATE_DELETING && definition !== undefined;
                const canSearch = index.state === IndexState.INDEX_STATE_ACTIVE
                  && definition?.searchAccess === IndexAccessState.INDEX_ACCESS_STATE_ENABLED;
                const nameContent = <><span aria-hidden="true">▦</span><div><strong>{definition?.displayName || name}</strong><small>index={name}{definition?.description ? ` · ${definition.description}` : ""}</small></div></>;
                return (
                  <tr key={index.indexId}>
                    <td>{canSearch
                      ? <Link className="resource-name" href={searchLaunchHref(`index=${name} | sort -_time`)} aria-label={`Search index ${name}`}>{nameContent}</Link>
                      : <div className="resource-name" aria-label={`Index ${name} is not currently searchable`}>{nameContent}</div>}
                    </td>
                    <td><span className={`status-label status-label--${statusClass(state)}`}><i />{state}</span></td>
                    <td>{indexAccessLabel(definition?.ingestionAccess)}</td>
                    <td>{indexAccessLabel(definition?.searchAccess)}</td>
                    <td>{formatDuration(definition?.retentionPeriod?.seconds)}</td>
                    <td>{formatDate(index.updatedAt)}</td>
                    <td><div className="row-actions"><button className="table-action" type="button" aria-label={`Edit index ${name}`} disabled={!canEdit || props.busy !== null} onClick={() => props.onEdit(index)}>{props.busy === `read-index-${index.indexId}` ? "Loading…" : "Edit"}</button><button className="table-action" type="button" aria-label={`${index.state === IndexState.INDEX_STATE_ACTIVE ? "Archive" : "Reactivate"} index ${name}`} disabled={!canChange || props.busy !== null} onClick={() => props.onChangeState(index)}>{props.busy === `index-${index.indexId}` ? "Updating…" : index.state === IndexState.INDEX_STATE_ACTIVE ? "Archive" : "Reactivate"}</button></div></td>
                  </tr>
                );
              })}</tbody>
            </table>
          </div>
        </div>
      )}
      <div className="admin-pagination-footer" aria-live="polite">
        <div>
          <strong>{loadedCount}</strong>
          {props.filter.trim().length === 0 ? null : <small>{props.indexes.length.toLocaleString()} matching loaded records</small>}
          {props.paginationError === null ? null : <small className="table-warning-detail">{props.paginationError}</small>}
        </div>
        {props.hasMore
          ? <button className="button secondary" type="button" disabled={props.loadingMore || props.busy !== null} onClick={props.onLoadMore}>{props.loadingMore ? "Loading…" : "Load more indexes"}</button>
          : null}
      </div>
      <p className="resource-footnote">Event counts, storage use, and source statistics are not exposed by the registered index routes and are intentionally omitted.</p>
    </div>
  );
}

interface BackendTokensProps {
  state: ResourceState;
  error: string | null;
  indexState: ResourceState;
  indexError: string | null;
  scopeSource: TokenScopeSource;
  tokens: IngestionToken[];
  totalSize: bigint | null;
  totalSizeExact: boolean;
  hasMore: boolean;
  loadingMore: boolean;
  paginationError: string | null;
  busy: string | null;
  canCreate: boolean;
  createBlockReason: string | null;
  onCreate: () => void;
  onEdit: (token: IngestionToken) => void;
  onLoadMore: () => void;
  onReload: () => void;
  onRevoke: (token: IngestionToken) => void;
}

function BackendTokens(props: BackendTokensProps) {
  if (props.state === "loading") return <ResourceMessage kind="loading" title="Loading ingestion tokens" message="Reading token metadata from the server…" />;
  if (props.state === "unavailable") return <ResourceMessage kind="unavailable" title="Ingestion tokens are unavailable" message="The connected server did not register the ingestion-token routes. Collector routes are not probed or simulated." action={<button type="button" onClick={props.onReload}>Retry</button>} />;
  if (props.state === "error") return <ResourceMessage kind="error" title="Ingestion tokens could not be loaded" message={props.error ?? "The server rejected the token list request."} action={<button type="button" onClick={props.onReload}>Retry</button>} />;
  const loadedCount = countLabel(
    props.tokens.length,
    props.totalSize,
    props.totalSizeExact,
    "token",
    "tokens",
  );
  const indexAdminDetail = props.indexState === "loading"
    ? "The versioned index catalog is still loading."
    : props.indexError ?? "The versioned index catalog route is unavailable.";

  return (
    <div className="admin-section-stack">
      <header className="admin-section-header"><div><h2>Data inputs</h2><p>Manage server-issued ingestion tokens. Collector inventory is not exposed.</p></div><button className="suite-button suite-button--primary" type="button" disabled={!props.canCreate} onClick={props.onCreate}>Generate token</button></header>
      {props.createBlockReason === null ? null : (
        <div className="access-mode-notice" role="alert">
          <span>!</span>
          <div><strong>Token generation is locked</strong><p>{props.createBlockReason}</p></div>
        </div>
      )}
      {props.indexState === "available" ? null : (
        <div className="access-mode-notice" role="note">
          <span>!</span>
          <div>
            <strong>{props.scopeSource === "bootstrap" ? "Using bootstrap index summaries" : "Index scope data unavailable"}</strong>
            <p>{props.scopeSource === "bootstrap"
              ? `${indexAdminDetail} Token generation and scope edits remain available using bootstrap eligibility data.`
              : props.indexError === null
                ? "Existing tokens can still be inspected, edited, and revoked. Token generation and index-scope changes require an authoritative index summary."
                : `${props.indexError} Existing tokens remain available, but token generation and index-scope changes are disabled.`}</p>
          </div>
        </div>
      )}
      <div className="access-mode-notice" role="note"><span>i</span><div><strong>Collector status is unavailable</strong><p>This server registers ingestion-token management only. It does not expose collector list or health routes.</p></div></div>
      <section className="suite-card token-section">
        <header className="suite-card-header"><div><h3>Ingestion tokens</h3><p>Token secrets are never returned after creation. {loadedCount}.</p></div><button type="button" onClick={props.onReload}>Refresh</button></header>
        {props.tokens.length === 0 ? (
          <ResourceMessage
            kind="empty"
            title="No ingestion tokens"
            message={props.canCreate
              ? "Generate a token scoped to an active, ingestible index."
              : props.scopeSource !== "unavailable"
                ? "No active, ingestion-enabled index is currently available for a new token."
                : "The token route is available, but generation is disabled until an authoritative index summary loads."}
          />
        ) : (
          <div className="responsive-table-wrap"><table className="product-table"><thead><tr><th scope="col">Name</th><th scope="col">Prefix</th><th scope="col">Allowed indexes</th><th scope="col">Expires</th><th scope="col">Last used</th><th scope="col">State</th><th scope="col"><span className="sr-only">Actions</span></th></tr></thead><tbody>{props.tokens.map((token) => {
            const state = tokenStateLabel(token.state);
            const canRevoke = tokenCanBeRevoked(token);
            const canEdit = canRevoke;
            return <tr key={token.ingestionTokenId}><td><strong>{token.name}</strong>{token.description ? <small className="table-secondary">{token.description}</small> : null}</td><td><code>{token.tokenPrefix}</code></td><td>{token.constraints?.allowedIndexNames.join(", ") || "None"}</td><td>{formatDate(token.expiresAt)}</td><td>{formatDate(token.lastUsedAt)}</td><td><span className={`status-label status-label--${statusClass(state)}`}><i />{state}</span></td><td><div className="row-actions"><button className="table-action" type="button" aria-label={`Edit token ${token.name}`} disabled={!canEdit || props.busy !== null} onClick={() => props.onEdit(token)}>{props.busy === `read-token-${token.ingestionTokenId}` ? "Loading…" : "Edit"}</button><button className="table-action" type="button" aria-label={`Revoke token ${token.name}`} disabled={!canRevoke || props.busy !== null} onClick={() => props.onRevoke(token)}>{props.busy === `token-${token.ingestionTokenId}` ? "Revoking…" : canRevoke ? "Revoke" : "—"}</button></div></td></tr>;
          })}</tbody></table></div>
        )}
        <div className="admin-pagination-footer" aria-live="polite">
          <div>
            <strong>{loadedCount}</strong>
            {props.paginationError === null ? null : <small className="table-warning-detail">{props.paginationError}</small>}
          </div>
          {props.hasMore
            ? <button className="button secondary" type="button" disabled={props.loadingMore || props.busy !== null} onClick={props.onLoadMore}>{props.loadingMore ? "Loading…" : "Load more tokens"}</button>
            : null}
        </div>
      </section>
    </div>
  );
}

function BackendServerSettings({
  bootstrap,
  error,
  onReload,
}: {
  bootstrap: SystemBootstrapModel | null;
  error: string | null;
  onReload: () => void;
}) {
  if (bootstrap === null) {
    return (
      <ResourceMessage
        kind="error"
        title="Server limits could not be loaded"
        message={error ?? "The system bootstrap route did not return a usable response."}
        action={<button type="button" onClick={onReload}>Retry bootstrap</button>}
      />
    );
  }
  const limits = bootstrap.limits;
  return (
    <div className="admin-section-stack">
      <header className="admin-section-header"><div><h2>Server settings</h2><p>Read-only limits advertised to this browser.</p></div><span>Bootstrap values</span></header>
      <div className="access-mode-notice" role="note"><span>i</span><div><strong>Configuration writes are unavailable</strong><p>The backend does not register a server-settings route. These values cannot be changed from this page.</p></div></div>
      <section className="suite-card settings-group">
        <header><h3>Search and result limits</h3><p>Authoritative limits returned by system bootstrap.</p></header>
        <dl className="backend-definition-list">
          <div><dt>Maximum page size</dt><dd>{limits.maximumPageSize.toLocaleString()}</dd></div>
          <div><dt>Default search timeout</dt><dd>{limits.defaultSearchTimeoutMs > 0 ? `${(limits.defaultSearchTimeoutMs / 1_000).toLocaleString()} seconds` : "Not reported"}</dd></div>
          <div><dt>Result retention</dt><dd>{limits.searchResultRetentionMs > 0 ? `${(limits.searchResultRetentionMs / 60_000).toLocaleString()} minutes` : "Not reported"}</dd></div>
          <div><dt>Maximum export rows</dt><dd>{limits.maximumExportRows > 0n ? limits.maximumExportRows.toLocaleString() : "Not reported"}</dd></div>
          <div><dt>Maximum export bytes</dt><dd>{limits.maximumExportBytes > 0n ? limits.maximumExportBytes.toLocaleString() : "Not reported"}</dd></div>
          <div><dt>Maximum timeline buckets</dt><dd>{limits.maximumTimelineBuckets > 0 ? limits.maximumTimelineBuckets.toLocaleString() : "Not available"}</dd></div>
        </dl>
      </section>
    </div>
  );
}
