import { SortDirection } from "@/gen/ts/open_splunk/common";
import {
  IngestionTokenPurpose,
  IngestionTokenState,
  type IngestionToken,
  type IngestionTokenHecProfile,
} from "@/gen/ts/open_splunk/collector_admin";
import { IngestionTokenSortBy } from "@/gen/ts/open_splunk/collector_admin_api";
import { isHttpError, type OpenSplunkApiClient } from "@/lib/api";

import {
  INGESTION_MAX_BYTES_PER_SECOND,
  INGESTION_MAX_EVENTS_PER_SECOND,
  normalizeTokenPatterns,
} from "./ingestion-policy-form";
import {
  TOKEN_CREATE_CLOCK_EPSILON_MS,
  isAuthoritativeTokenCreateRejection,
} from "./token-create-recovery-policy";

export type TokenCreateOutcomeKind = "pending" | "settled-response" | "ambiguous-failure";

export interface TokenCreateDefinitionSnapshot {
  name: string;
  description: string;
  boundCollectorId: string;
  allowedIndexNames: string[];
  allowedHostRegexes?: string[];
  allowedSourceRegexes?: string[];
  maxEventsPerSecond?: bigint;
  maxUncompressedBytesPerSecond?: bigint;
  purpose: IngestionTokenPurpose;
  hecProfile: IngestionTokenHecProfile | undefined;
  expiresAt: Date | undefined;
  armedServerTimeMs: number;
  dispatchedServerTimeMs: number | null;
  outcomeObservedServerTimeMs: number | null;
  requestRoundTripMs: number | null;
  requestTimeoutMs: number;
  clockUncertaintyMs: number;
  outcomeKind: TokenCreateOutcomeKind;
}

export interface TokenCreateRecovery {
  attemptId: string;
  ownerId: string;
  definition: TokenCreateDefinitionSnapshot;
  preexistingTokenIds: ReadonlySet<string>;
  confirmedRevokedTokenIds: ReadonlySet<string>;
  failureMessage: string;
  candidates: IngestionToken[];
  reconciliationError: string | null;
}

export type TokenCreateGuardMode = "ambiguous" | "issued";
export type TokenCreateGuardStorageState = "checking" | "available" | "unavailable";

export interface UnreadableTokenCreateRecovery {
  attemptId: string;
  raw: string;
  observedServerTimeMs: number | null;
  candidates: IngestionToken[];
  reconciliationError: string | null;
}

export interface PersistedTokenCreateGuard {
  schemaVersion: 1;
  apiBaseUrl: string;
  attemptId: string;
  ownerId: string;
  mode: TokenCreateGuardMode;
  definition: {
    name: string;
    description: string;
    boundCollectorId: string;
    allowedIndexNames: string[];
    allowedHostRegexes: string[];
    allowedSourceRegexes: string[];
    maxEventsPerSecond: string | null;
    maxUncompressedBytesPerSecond: string | null;
    purpose: IngestionTokenPurpose;
    hecProfile: {
      defaultIndexName: string | null;
      defaultHost: string | null;
      defaultSource: string | null;
      defaultSourcetype: string | null;
      indexerAcknowledgment: boolean;
    } | null;
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

const TOKEN_HISTORY_GUARD_KEY = "__openSplunkTokenGuard";
const TOKEN_CREATE_GUARD_STORAGE_PREFIX = "open-splunk.admin.token-create-guard";
const TOKEN_CREATE_LOCK_PREFIX = "open-splunk.admin.token-create-lock";

export const COLLECTOR_ID_ERROR = "Use 1–128 ASCII characters: start with a letter or number, then use letters, numbers, dot, underscore, colon, or hyphen.";

export function normalizeApiBaseUrl(apiBaseUrl: string, pageOrigin: string): string {
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

export function tokenCreateGuardStorageKey(normalizedApiBaseUrl: string): string {
  return `${TOKEN_CREATE_GUARD_STORAGE_PREFIX}:${encodeURIComponent(normalizedApiBaseUrl)}`;
}

export function tokenCreateLockName(normalizedApiBaseUrl: string): string {
  return `${TOKEN_CREATE_LOCK_PREFIX}:${normalizedApiBaseUrl}`;
}

export function browserSupportsTokenCreateLock(): boolean {
  return typeof navigator.locks?.request === "function";
}

export function requestTokenCreateLock<T>(
  normalizedApiBaseUrl: string,
  options: LockOptions,
  callback: (lock: Lock | null) => Promise<T>,
): Promise<T> {
  return navigator.locks.request(tokenCreateLockName(normalizedApiBaseUrl), options, callback);
}

export function readTokenCreateGuardRaw(normalizedApiBaseUrl: string): string | null {
  return window.localStorage.getItem(tokenCreateGuardStorageKey(normalizedApiBaseUrl));
}

export function writeTokenCreateGuard(
  normalizedApiBaseUrl: string,
  record: PersistedTokenCreateGuard,
): void {
  window.localStorage.setItem(
    tokenCreateGuardStorageKey(normalizedApiBaseUrl),
    JSON.stringify(record),
  );
}

export function removeTokenCreateGuard(normalizedApiBaseUrl: string): void {
  window.localStorage.removeItem(tokenCreateGuardStorageKey(normalizedApiBaseUrl));
}

export function subscribeTokenCreateGuard(
  normalizedApiBaseUrl: string,
  listener: (event: StorageEvent) => void,
): () => void {
  const key = tokenCreateGuardStorageKey(normalizedApiBaseUrl);
  function handleStorage(event: StorageEvent) {
    if (event.storageArea === window.localStorage && event.key === key) listener(event);
  }
  window.addEventListener("storage", handleStorage);
  return () => window.removeEventListener("storage", handleStorage);
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === "string");
}

function isNullableString(value: unknown): value is string | null {
  return value === null || typeof value === "string";
}

export function validCollectorId(value: string): boolean {
  return /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(value);
}

export function tokenUsesHEC(purpose: IngestionTokenPurpose): boolean {
  return purpose === IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_HEC;
}

export function tokenPurposeLabel(purpose: IngestionTokenPurpose): string {
  if (purpose === IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_HEC) return "HEC";
  if (purpose === IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_NATIVE_COLLECTOR) {
    return "Native collector";
  }
  return "Unknown";
}

export function hecProfileSummary(profile: IngestionTokenHecProfile | undefined): string {
  if (profile === undefined) return "Profile unavailable";
  const defaults = [
    profile.defaultIndexName ? `index ${profile.defaultIndexName}` : null,
    profile.defaultHost ? `host ${profile.defaultHost}` : null,
    profile.defaultSource ? `source ${profile.defaultSource}` : null,
    profile.defaultSourcetype ? `sourcetype ${profile.defaultSourcetype}` : null,
  ].filter((value): value is string => value !== null);
  return defaults.length === 0 ? "No token defaults" : `Defaults: ${defaults.join(" · ")}`;
}

function isASCIIWhitespaceCodeUnit(codeUnit: number): boolean {
  return codeUnit === 0x20 || (codeUnit >= 0x09 && codeUnit <= 0x0d);
}

export function validHECMetadataDefault(value: string): boolean {
  if (value.length === 0) return true;
  const byteLength = new TextEncoder().encode(value).byteLength;
  const hasInvalidScalar = [...value].some((character) => {
    const codePoint = character.codePointAt(0) ?? 0;
    return codePoint <= 0x1f
      || (codePoint >= 0x7f && codePoint <= 0x9f)
      || (codePoint >= 0xd800 && codePoint <= 0xdfff);
  });
  return byteLength <= 255
    && !hasInvalidScalar
    && !isASCIIWhitespaceCodeUnit(value.charCodeAt(0))
    && !isASCIIWhitespaceCodeUnit(value.charCodeAt(value.length - 1));
}

export interface HECProfileFormValue {
  defaultIndexName: string;
  defaultHost: string;
  defaultSource: string;
  defaultSourcetype: string;
  indexerAcknowledgment: boolean;
}

export function hecProfileFromForm(value: HECProfileFormValue): IngestionTokenHecProfile {
  return {
    defaultIndexName: value.defaultIndexName || undefined,
    defaultHost: value.defaultHost || undefined,
    defaultSource: value.defaultSource || undefined,
    defaultSourcetype: value.defaultSourcetype || undefined,
    indexerAcknowledgment: value.indexerAcknowledgment,
  };
}

export function hecProfilesMatch(
  left: IngestionTokenHecProfile | undefined,
  right: IngestionTokenHecProfile | undefined,
): boolean {
  return left?.defaultIndexName === right?.defaultIndexName
    && left?.defaultHost === right?.defaultHost
    && left?.defaultSource === right?.defaultSource
    && left?.defaultSourcetype === right?.defaultSourcetype
    && left?.indexerAcknowledgment === right?.indexerAcknowledgment;
}

function shellSingleQuote(value: string): string {
  return `'${value.replaceAll("'", `'"'"'`)}'`;
}

export function hecCurlExample(
  normalizedApiBaseUrl: string | null,
  purpose: IngestionTokenPurpose,
  plaintextToken: string | null,
  indexName: string | null,
): string | null {
  if (
    normalizedApiBaseUrl === null
    || !tokenUsesHEC(purpose)
    || plaintextToken === null
    || plaintextToken.length === 0
    || indexName === null
    || indexName.length === 0
  ) {
    return null;
  }
  const endpoint = `${normalizedApiBaseUrl.replace(/\/+$/, "")}/services/collector/event`;
  const body = JSON.stringify({ event: "hello from Open Splunk", index: indexName });
  return [
    "#!/usr/bin/env bash",
    "set -euo pipefail",
    "umask 077",
    "read -r -s -p 'HEC token: ' OPEN_SPLUNK_HEC_TOKEN",
    "printf '\\n' >&2",
    'OPEN_SPLUNK_HEC_CONFIG="$(mktemp "${TMPDIR:-/tmp}/open-splunk-hec.XXXXXX")"',
    'trap \'rm -f -- "$OPEN_SPLUNK_HEC_CONFIG"\' EXIT',
    "trap 'exit 1' HUP INT TERM",
    'chmod 600 "$OPEN_SPLUNK_HEC_CONFIG"',
    'printf \'header = "Authorization: Splunk %s"\\n\' "$OPEN_SPLUNK_HEC_TOKEN" > "$OPEN_SPLUNK_HEC_CONFIG"',
    "unset OPEN_SPLUNK_HEC_TOKEN",
    `curl --fail-with-body --request POST --config "$OPEN_SPLUNK_HEC_CONFIG" ${shellSingleQuote(endpoint)} \\`,
    `  --header 'Content-Type: application/json' \\`,
    `  --header 'X-Splunk-Request-Channel: 00000000-0000-0000-0000-000000000001' \\`,
    `  --data ${shellSingleQuote(body)}`,
  ].join("\n");
}

export function serializeTokenCreateGuard(
  normalizedApiBaseUrl: string,
  recovery: TokenCreateRecovery,
  knownIssuedTokenId: string | null,
): PersistedTokenCreateGuard {
  return {
    schemaVersion: 1,
    apiBaseUrl: normalizedApiBaseUrl,
    attemptId: recovery.attemptId,
    ownerId: recovery.ownerId,
    mode: knownIssuedTokenId === null ? "ambiguous" : "issued",
    definition: {
      name: recovery.definition.name,
      description: recovery.definition.description,
      boundCollectorId: recovery.definition.boundCollectorId,
      allowedIndexNames: [...recovery.definition.allowedIndexNames],
      allowedHostRegexes: [...(recovery.definition.allowedHostRegexes ?? [])],
      allowedSourceRegexes: [...(recovery.definition.allowedSourceRegexes ?? [])],
      maxEventsPerSecond: recovery.definition.maxEventsPerSecond?.toString() ?? null,
      maxUncompressedBytesPerSecond:
        recovery.definition.maxUncompressedBytesPerSecond?.toString() ?? null,
      purpose: recovery.definition.purpose,
      hecProfile: recovery.definition.hecProfile === undefined ? null : {
        defaultIndexName: recovery.definition.hecProfile.defaultIndexName ?? null,
        defaultHost: recovery.definition.hecProfile.defaultHost ?? null,
        defaultSource: recovery.definition.hecProfile.defaultSource ?? null,
        defaultSourcetype: recovery.definition.hecProfile.defaultSourcetype ?? null,
        indexerAcknowledgment: recovery.definition.hecProfile.indexerAcknowledgment,
      },
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

export function parsePersistedTokenCreateGuard(
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
  const record = value as Partial<PersistedTokenCreateGuard>;
  const definition = record.definition;
  const persistedPurpose = definition?.purpose;
  const persistedHECProfile = definition?.hecProfile;
  const allowedIndexNames = isStringArray(definition?.allowedIndexNames)
    ? definition.allowedIndexNames
    : [];
  const allowedHostRegexes = isStringArray(definition?.allowedHostRegexes)
    ? definition.allowedHostRegexes
    : null;
  const allowedSourceRegexes = isStringArray(definition?.allowedSourceRegexes)
    ? definition.allowedSourceRegexes
    : null;
  let normalizedHostRegexes: string[] | null = null;
  let normalizedSourceRegexes: string[] | null = null;
  try {
    normalizedHostRegexes = allowedHostRegexes === null
      ? null
      : normalizeTokenPatterns(allowedHostRegexes, "Allowed host");
    normalizedSourceRegexes = allowedSourceRegexes === null
      ? null
      : normalizeTokenPatterns(allowedSourceRegexes, "Allowed source");
  } catch {
    return null;
  }
  const persistedEventsRate = definition?.maxEventsPerSecond;
  const persistedBytesRate = definition?.maxUncompressedBytesPerSecond;
  const validPersistedEventsRate = persistedEventsRate === null
    || (typeof persistedEventsRate === "string"
      && persistedEventsRate.length <= INGESTION_MAX_EVENTS_PER_SECOND.toString().length
      && /^[1-9][0-9]*$/.test(persistedEventsRate)
      && BigInt(persistedEventsRate) <= INGESTION_MAX_EVENTS_PER_SECOND);
  const validPersistedBytesRate = persistedBytesRate === null
    || (typeof persistedBytesRate === "string"
      && persistedBytesRate.length <= INGESTION_MAX_BYTES_PER_SECOND.toString().length
      && /^[1-9][0-9]*$/.test(persistedBytesRate)
      && BigInt(persistedBytesRate) <= INGESTION_MAX_BYTES_PER_SECOND);
  const validPersistedHECProfile = typeof persistedHECProfile === "object"
    && persistedHECProfile !== null
    && isNullableString(persistedHECProfile.defaultIndexName)
    && (
      persistedHECProfile.defaultIndexName === null
      || (
        persistedHECProfile.defaultIndexName.length > 0
        && allowedIndexNames.includes(persistedHECProfile.defaultIndexName)
      )
    )
    && isNullableString(persistedHECProfile.defaultHost)
    && (
      persistedHECProfile.defaultHost === null
      || (
        persistedHECProfile.defaultHost.length > 0
        && validHECMetadataDefault(persistedHECProfile.defaultHost)
      )
    )
    && isNullableString(persistedHECProfile.defaultSource)
    && (
      persistedHECProfile.defaultSource === null
      || (
        persistedHECProfile.defaultSource.length > 0
        && validHECMetadataDefault(persistedHECProfile.defaultSource)
      )
    )
    && isNullableString(persistedHECProfile.defaultSourcetype)
    && (
      persistedHECProfile.defaultSourcetype === null
      || (
        persistedHECProfile.defaultSourcetype.length > 0
        && validHECMetadataDefault(persistedHECProfile.defaultSourcetype)
      )
    )
    && typeof persistedHECProfile.indexerAcknowledgment === "boolean";
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
    || typeof definition.boundCollectorId !== "string"
    || !isStringArray(definition.allowedIndexNames)
    || definition.allowedIndexNames.length === 0
    || new Set(definition.allowedIndexNames).size !== definition.allowedIndexNames.length
    || normalizedHostRegexes === null
    || normalizedSourceRegexes === null
    || !validPersistedEventsRate
    || !validPersistedBytesRate
    || (
      persistedPurpose !== IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_NATIVE_COLLECTOR
      && persistedPurpose !== IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_HEC
    )
    || (
      persistedPurpose === IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_NATIVE_COLLECTOR
      && (!validCollectorId(definition.boundCollectorId) || persistedHECProfile !== null)
    )
    || (
      persistedPurpose === IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_HEC
      && (definition.boundCollectorId.length !== 0 || !validPersistedHECProfile)
    )
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
        boundCollectorId: definition.boundCollectorId,
        allowedIndexNames: [...new Set(definition.allowedIndexNames)].toSorted(),
        allowedHostRegexes: normalizedHostRegexes,
        allowedSourceRegexes: normalizedSourceRegexes,
        maxEventsPerSecond: persistedEventsRate === null
          ? undefined
          : BigInt(persistedEventsRate),
        maxUncompressedBytesPerSecond: persistedBytesRate === null
          ? undefined
          : BigInt(persistedBytesRate),
        purpose: persistedPurpose,
        hecProfile: persistedPurpose === IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_HEC
          && validPersistedHECProfile
          ? {
              defaultIndexName: persistedHECProfile.defaultIndexName ?? undefined,
              defaultHost: persistedHECProfile.defaultHost ?? undefined,
              defaultSource: persistedHECProfile.defaultSource ?? undefined,
              defaultSourcetype: persistedHECProfile.defaultSourcetype ?? undefined,
              indexerAcknowledgment: persistedHECProfile.indexerAcknowledgment,
            }
          : undefined,
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

export function historyHasTokenGuard(guardId: string): boolean {
  const state: unknown = window.history.state;
  return typeof state === "object"
    && state !== null
    && TOKEN_HISTORY_GUARD_KEY in state
    && (state as Record<string, unknown>)[TOKEN_HISTORY_GUARD_KEY] === guardId;
}

export function historyStateWithTokenGuard(guardId: string): Record<string, unknown> {
  const state: unknown = window.history.state;
  return {
    ...(typeof state === "object" && state !== null ? state : {}),
    [TOKEN_HISTORY_GUARD_KEY]: guardId,
  };
}

export function hasSameStrings(left: Iterable<string>, right: Iterable<string>): boolean {
  const leftValues = [...left].toSorted();
  const rightValues = [...right].toSorted();
  return leftValues.length === rightValues.length
    && leftValues.every((value, index) => value === rightValues[index]);
}

export function tokenIsTerminallySafe(token: IngestionToken): boolean {
  return token.state === IngestionTokenState.INGESTION_TOKEN_STATE_REVOKED
    || token.state === IngestionTokenState.INGESTION_TOKEN_STATE_EXPIRED;
}

export function isDefiniteTokenCreateFailure(error: unknown): boolean {
  if (!isHttpError(error)) return false;
  if (isAuthoritativeTokenCreateRejection(error)) return true;
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

export function normalizedPageToken(value: string | undefined): string | null {
  return value?.trim() || null;
}


export function tokenMatchesCreateMetadata(
  token: IngestionToken,
  definition: TokenCreateDefinitionSnapshot,
): boolean {
  const constraints = token.constraints;
  if (
    token.name !== definition.name
    || (token.description ?? "") !== definition.description
    || constraints === undefined
    || !hasSameStrings(constraints.allowedIndexNames, definition.allowedIndexNames)
    || !hasSameStrings(constraints.allowedHostRegexes, definition.allowedHostRegexes ?? [])
    || !hasSameStrings(constraints.allowedSourceRegexes, definition.allowedSourceRegexes ?? [])
    || (constraints.boundCollectorId ?? "") !== definition.boundCollectorId
    || (token.ingestionRateLimits?.maxEventsPerSecond ?? undefined)
      !== definition.maxEventsPerSecond
    || (token.ingestionRateLimits?.maxUncompressedBytesPerSecond ?? undefined)
      !== definition.maxUncompressedBytesPerSecond
    || token.purpose !== definition.purpose
    || !hecProfilesMatch(token.hecProfile, definition.hecProfile)
    || (token.expiresAt?.valueOf() ?? null) !== (definition.expiresAt?.valueOf() ?? null)
  ) {
    return false;
  }
  return true;
}

export function tokenFallsWithinCreateAttributionWindow(
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

export function tokenMatchesCreateDefinition(
  token: IngestionToken,
  definition: TokenCreateDefinitionSnapshot,
): boolean {
  return tokenMatchesCreateMetadata(token, definition)
    && tokenFallsWithinCreateAttributionWindow(token, definition);
}

export async function listTokensForCreateSafety(
  client: OpenSplunkApiClient,
  tokenName: string | undefined,
  signal?: AbortSignal,
): Promise<IngestionToken[]> {
  const tokens: IngestionToken[] = [];
  const tokenIds = new Set<string>();
  const seenCursors = new Set<string>();
  let expectedTotal: bigint | null = null;
  async function loadPage(pageToken: string | undefined): Promise<void> {
    // This complete snapshot (name-filtered for a valid guard, unfiltered for
    // a damaged one) is a safety prerequisite, not the Admin table load path.
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
    if (next === null) return;
    if (seenCursors.has(next)) {
      throw new Error("The token snapshot returned a repeated page cursor.");
    }
    seenCursors.add(next);
    await loadPage(next);
  }
  await loadPage(undefined);
  if (expectedTotal === null || BigInt(tokens.length) !== expectedTotal) {
    throw new Error("The token snapshot ended before its exact total was loaded.");
  }
  return tokens;
}
