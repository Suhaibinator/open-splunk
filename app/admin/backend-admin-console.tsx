"use client";

import type { FormEvent } from "react";
import { useCallback, useEffect, useEffectEvent, useMemo, useRef, useState, type RefObject } from "react";
import Link from "next/link";

import { SortDirection, type PageResponse } from "@/gen/ts/open_splunk/common";
import { ServerFeature } from "@/gen/ts/open_splunk/system_api";
import {
  IngestionTokenPurpose,
  IngestionTokenState,
  type IngestionToken,
  type IngestionTokenHecProfile,
} from "@/gen/ts/open_splunk/collector_admin";
import type { GetHECOperationalSnapshotResponse } from "@/gen/ts/open_splunk/hec_admin_api";
import { IngestionTokenSortBy } from "@/gen/ts/open_splunk/collector_admin_api";
import {
  IndexAccessState,
  IndexState,
  type Index,
} from "@/gen/ts/open_splunk/index";
import { IndexDataDeletionMode, IndexSortBy } from "@/gen/ts/open_splunk/index_api";
import {
  DEFAULT_REQUEST_TIMEOUT_MS,
  createOpenSplunkApiClient,
  getSystemBootstrap,
  isAdvertisedFeatureRouteUnavailable,
  isHttpError,
  isHttpStatus,
  isOptionalRouteUnavailable,
  supportsServerFeature,
  type OpenSplunkApiClient,
  type SystemBootstrapModel,
} from "@/lib/api";
import { createErrorMessage } from "@/lib/error-message";
import { searchLaunchHref } from "@/lib/search/launch-url";

import { BackendResourceState } from "../_components/backend-resource-state";
import { AppIcon, type AppIconName } from "../_components/app-icon";
import { formatMediumDateTime } from "../_components/date-format";
import { PageHeading } from "../_components/product-shell";
import { Modal } from "../search-workspace/modal";
import { AppsAdminPanel, CollectorFleetPanel } from "./admin-resource-panels";
import { ADMIN_SECTION_QUERY_PARAMETER, adminSectionPath, resolveAdminSection } from "./admin-navigation";
import { KnowledgeManagerGate } from "./knowledge-manager-gate";
import { LookupManagerGate } from "./lookup-manager-gate";
import {
  TOKEN_CREATE_CLOCK_EPSILON_MS,
  TOKEN_CREATE_ZERO_CONFIRMATION_INTERVAL_MS,
  isAuthoritativeTokenCreateRejection,
  tokenCreateDialogRequiresExclusiveAttention,
  tokenRecoveryEnvironmentCanPoll,
  tokenRecoveryPollDelayMs,
  tokenRecoveryQuiescenceDeadline,
  tokenRecoverySnapshotDecision,
  tokenSecretRequiresNavigationProtection,
  type TokenRecoveryOwnership,
  type ZeroCandidateObservation,
} from "./token-create-recovery-policy";
import {
  backendKnowledgeCapabilities,
  backendAdminNavigation,
  knowledgeManagerAppOptionsFromBootstrap,
  type BackendAdminSection as AdminSection,
} from "./knowledge-manager-feature";

type AdminModal = "create-index" | "edit-index" | "create-token" | "edit-token";
type ResourceState = "loading" | "available" | "unavailable" | "error";

interface BackendAdminConsoleProps {
  apiBaseUrl: string;
}

const BACKEND_ADMIN_SECTIONS: readonly AdminSection[] = [
  "overview",
  "apps",
  "indexes",
  "collector-fleet",
  "collectors",
  "knowledge",
  "lookups",
  "access",
  "server",
];

function adminSectionIcon(section: AdminSection): AppIconName {
  if (section === "overview" || section === "apps") return "dashboard";
  if (section === "indexes" || section === "lookups") return "database";
  if (section === "collector-fleet") return "activity";
  if (section === "collectors") return "download";
  if (section === "knowledge") return "file";
  if (section === "access") return "users";
  return "settings";
}

interface PageLoadResult<T> {
  state: Exclude<ResourceState, "loading">;
  items: T[];
  nextPageToken: string | null;
  totalSize: bigint | null;
  totalSizeExact: boolean;
  message?: string;
}

interface AdminToast {
  message: string;
  kind: "success" | "warning";
}

interface IndexPolicyForm {
  defaultSourcetype: string;
  maxEventBytes: string;
  maxFieldCount: string;
  maxNestingDepth: string;
  maximumFutureSkewSeconds: string;
  maximumEventAgeSeconds: string;
  maxEventsPerSecond: string;
  maxUncompressedBytesPerSecond: string;
}

interface TokenPolicyForm {
  allowedHostRegexes: string;
  allowedSourceRegexes: string;
  maxEventsPerSecond: string;
  maxUncompressedBytesPerSecond: string;
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

interface UnreadableTokenCreateRecovery {
  attemptId: string;
  raw: string;
  observedServerTimeMs: number | null;
  candidates: IngestionToken[];
  reconciliationError: string | null;
}

interface PersistedTokenCreateGuard {
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

function isNullableString(value: unknown): value is string | null {
  return value === null || typeof value === "string";
}

function validCollectorId(value: string): boolean {
  return /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(value);
}

function tokenUsesHEC(purpose: IngestionTokenPurpose): boolean {
  return purpose === IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_HEC;
}

export function tokenPurposeLabel(purpose: IngestionTokenPurpose): string {
  if (purpose === IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_HEC) return "HEC";
  if (purpose === IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_NATIVE_COLLECTOR) {
    return "Native collector";
  }
  return "Unknown";
}

function hecProfileSummary(profile: IngestionTokenHecProfile | undefined): string {
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

interface HECProfileFormValue {
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

function hecProfilesMatch(
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

const errorMessage = createErrorMessage("The server did not return a usable response.");

function formatDate(value: Date | undefined): string {
  return formatMediumDateTime(value, "Never");
}

function formatDuration(seconds: bigint | undefined): string {
  if (seconds === undefined || seconds <= 0n) return "Forever";
  const days = seconds / 86_400n;
  if (days > 0n && seconds % 86_400n === 0n) return `${days.toLocaleString()} days`;
  const hours = seconds / 3_600n;
  if (hours > 0n && seconds % 3_600n === 0n) return `${hours.toLocaleString()} hours`;
  return `${seconds.toLocaleString()} seconds`;
}

function formatOperationalDuration(
  duration: { seconds: bigint; nanos: number } | undefined,
): string {
  if (duration === undefined) return "Not reported";
  if (duration.nanos === 0) return `${duration.seconds.toLocaleString()} seconds`;
  const fractional = duration.nanos.toString().padStart(9, "0").replace(/0+$/, "");
  return `${duration.seconds.toLocaleString()}.${fractional} seconds`;
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

const INDEX_MAX_EVENT_BYTES = 1_048_576n;
const INDEX_MAX_FIELD_COUNT = 1_024;
const INDEX_MAX_NESTING_DEPTH = 16;
const INDEX_MAX_FUTURE_SKEW_SECONDS = 300n;
const INDEX_MAX_EVENT_AGE_SECONDS = 31_536_000n;
const INGESTION_MAX_EVENTS_PER_SECOND = 1_000_000n;
const INGESTION_MAX_BYTES_PER_SECOND = 1_099_511_627_776n;

function optionalUnsignedBigInt(
  value: string,
  label: string,
  maximum: bigint,
): bigint | undefined {
  const normalized = value.trim();
  if (normalized.length === 0 || normalized === "0") return undefined;
  if (!/^[0-9]+$/.test(normalized)) throw new Error(`${label} must be a whole non-negative number.`);
  if (normalized.length > maximum.toString().length) {
    throw new Error(`${label} cannot exceed ${maximum.toLocaleString()}.`);
  }
  const parsed = BigInt(normalized);
  if (parsed > maximum) throw new Error(`${label} cannot exceed ${maximum.toLocaleString()}.`);
  return parsed;
}

function optionalUnsignedNumber(
  value: string,
  label: string,
  maximum: number,
): number | undefined {
  const parsed = optionalUnsignedBigInt(value, label, BigInt(maximum));
  return parsed === undefined ? undefined : Number(parsed);
}

function durationFormValue(duration: { seconds: bigint; nanos: number } | undefined): string {
  if (duration === undefined || (duration.seconds === 0n && duration.nanos === 0)) return "";
  if (duration.nanos === 0) return duration.seconds.toString();
  return `${duration.seconds}.${duration.nanos.toString().padStart(9, "0").replace(/0+$/, "")}`;
}

function optionalDurationFromSeconds(
  value: string,
  label: string,
  maximumSeconds: bigint,
): { seconds: bigint; nanos: number } | undefined {
  const normalized = value.trim();
  if (normalized.length === 0 || /^0(?:\.0+)?$/.test(normalized)) return undefined;
  const match = /^(\d+)(?:\.(\d{1,3}))?$/.exec(normalized);
  if (match === null) throw new Error(`${label} must be non-negative seconds with at most three decimal places.`);
  if (match[1].length > maximumSeconds.toString().length) {
    throw new Error(`${label} cannot exceed ${maximumSeconds.toLocaleString()} seconds.`);
  }
  const seconds = BigInt(match[1]);
  const nanos = Number((match[2] ?? "").padEnd(9, "0"));
  if (seconds > maximumSeconds || (seconds === maximumSeconds && nanos > 0)) {
    throw new Error(`${label} cannot exceed ${maximumSeconds.toLocaleString()} seconds.`);
  }
  return { seconds, nanos };
}

function optionalFormValue(value: bigint | number | undefined): string {
  return value === undefined || value === 0 || value === 0n ? "" : value.toString();
}

function indexPolicyFormFromDefinition(definition?: Index["definition"]): IndexPolicyForm {
  return {
    defaultSourcetype: definition?.defaultSourcetype ?? "",
    maxEventBytes: optionalFormValue(definition?.limits?.maxEventBytes),
    maxFieldCount: optionalFormValue(definition?.limits?.maxFieldCount),
    maxNestingDepth: optionalFormValue(definition?.limits?.maxNestingDepth),
    maximumFutureSkewSeconds: durationFormValue(definition?.limits?.maximumFutureSkew),
    maximumEventAgeSeconds: durationFormValue(definition?.limits?.maximumEventAge),
    maxEventsPerSecond: optionalFormValue(definition?.ingestionRateLimits?.maxEventsPerSecond),
    maxUncompressedBytesPerSecond: optionalFormValue(
      definition?.ingestionRateLimits?.maxUncompressedBytesPerSecond,
    ),
  };
}

function indexPolicyFromForm(form: IndexPolicyForm) {
  const defaultSourcetype = form.defaultSourcetype.trim() || undefined;
  const limits = {
    maxEventBytes: optionalUnsignedBigInt(form.maxEventBytes, "Maximum event bytes", INDEX_MAX_EVENT_BYTES),
    maxFieldCount: optionalUnsignedNumber(form.maxFieldCount, "Maximum field count", INDEX_MAX_FIELD_COUNT),
    maxNestingDepth: optionalUnsignedNumber(
      form.maxNestingDepth,
      "Maximum nesting depth",
      INDEX_MAX_NESTING_DEPTH,
    ),
    maximumFutureSkew: optionalDurationFromSeconds(
      form.maximumFutureSkewSeconds,
      "Maximum future skew",
      INDEX_MAX_FUTURE_SKEW_SECONDS,
    ),
    maximumEventAge: optionalDurationFromSeconds(
      form.maximumEventAgeSeconds,
      "Maximum event age",
      INDEX_MAX_EVENT_AGE_SECONDS,
    ),
  };
  const ingestionRateLimits = {
    maxEventsPerSecond: optionalUnsignedBigInt(
      form.maxEventsPerSecond,
      "Maximum events per second",
      INGESTION_MAX_EVENTS_PER_SECOND,
    ),
    maxUncompressedBytesPerSecond: optionalUnsignedBigInt(
      form.maxUncompressedBytesPerSecond,
      "Maximum uncompressed bytes per second",
      INGESTION_MAX_BYTES_PER_SECOND,
    ),
  };
  return { defaultSourcetype, limits, ingestionRateLimits };
}

function normalizeTokenPatterns(patterns: Iterable<string>, label: string): string[] {
  const unique = new Set(patterns);
  if (unique.size > 16) throw new Error(`${label} accepts at most 16 unique patterns.`);
  const encoder = new TextEncoder();
  let totalBytes = 0;
  for (const pattern of unique) {
    if (pattern.length === 0) throw new Error(`${label} patterns cannot be empty.`);
    const bytes = encoder.encode(pattern).byteLength;
    if (bytes > 512) throw new Error(`${label} patterns cannot exceed 512 UTF-8 bytes each.`);
    if (pattern.includes("\0")) throw new Error(`${label} patterns cannot contain NUL characters.`);
    totalBytes += bytes;
  }
  if (totalBytes > 4_096) throw new Error(`${label} patterns cannot exceed 4,096 UTF-8 bytes in total.`);
  return [...unique].toSorted();
}

export function tokenPatternsFromForm(value: string, label: string): string[] {
  return normalizeTokenPatterns(
    value.split(/\r?\n/).filter((pattern) => pattern.length > 0),
    label,
  );
}

function tokenPolicyFormFromToken(token?: IngestionToken): TokenPolicyForm {
  return {
    allowedHostRegexes: token?.constraints?.allowedHostRegexes.join("\n") ?? "",
    allowedSourceRegexes: token?.constraints?.allowedSourceRegexes.join("\n") ?? "",
    maxEventsPerSecond: optionalFormValue(token?.ingestionRateLimits?.maxEventsPerSecond),
    maxUncompressedBytesPerSecond: optionalFormValue(
      token?.ingestionRateLimits?.maxUncompressedBytesPerSecond,
    ),
  };
}

function tokenPolicyFromForm(form: TokenPolicyForm) {
  return {
    allowedHostRegexes: tokenPatternsFromForm(form.allowedHostRegexes, "Allowed host"),
    allowedSourceRegexes: tokenPatternsFromForm(form.allowedSourceRegexes, "Allowed source"),
    ingestionRateLimits: {
      maxEventsPerSecond: optionalUnsignedBigInt(
        form.maxEventsPerSecond,
        "Maximum token events per second",
        INGESTION_MAX_EVENTS_PER_SECOND,
      ),
      maxUncompressedBytesPerSecond: optionalUnsignedBigInt(
        form.maxUncompressedBytesPerSecond,
        "Maximum token uncompressed bytes per second",
        INGESTION_MAX_BYTES_PER_SECOND,
      ),
    },
  };
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

export function tokenCanSetEnabled(token: IngestionToken): boolean {
  return token.state === IngestionTokenState.INGESTION_TOKEN_STATE_ACTIVE
    || token.state === IngestionTokenState.INGESTION_TOKEN_STATE_DISABLED;
}

function tokenIsTerminallySafe(token: IngestionToken): boolean {
  return token.state === IngestionTokenState.INGESTION_TOKEN_STATE_REVOKED
    || token.state === IngestionTokenState.INGESTION_TOKEN_STATE_EXPIRED;
}

function isDefiniteTokenCreateFailure(error: unknown): boolean {
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

async function loadResourcePage<Response extends { page?: PageResponse | undefined }, T>(
  call: () => Promise<Response>,
  extract: (response: Response) => T[],
  signal: AbortSignal,
): Promise<PageLoadResult<T>> {
  const empty = { items: [] as T[], nextPageToken: null, totalSize: null, totalSizeExact: false };
  try {
    const response = await call();
    return {
      state: "available",
      items: extract(response),
      nextPageToken: normalizedPageToken(response.page?.nextPageToken),
      totalSize: response.page?.totalSize ?? null,
      totalSizeExact: response.page?.totalSizeExact ?? false,
    };
  } catch (error) {
    if (isOptionalRouteUnavailable(error)) return { state: "unavailable", ...empty };
    if (signal.aborted) throw error;
    return { state: "error", ...empty, message: errorMessage(error) };
  }
}

async function loadIndexPage(
  client: OpenSplunkApiClient,
  pageToken: string | undefined,
  signal: AbortSignal,
): Promise<PageLoadResult<Index>> {
  return loadResourcePage(
    () => client.indexes.list({
      page: { pageSize: undefined, pageToken, includeTotalSize: true },
      stateFilters: [],
      textFilter: undefined,
      sortBy: IndexSortBy.INDEX_SORT_BY_NAME,
      sortDirection: SortDirection.SORT_DIRECTION_ASCENDING,
      includeStats: false,
    }, { signal }),
    (response) => response.indexes.flatMap((item) => (item.index === undefined ? [] : [item.index])),
    signal,
  );
}

async function loadTokenPage(
  client: OpenSplunkApiClient,
  pageToken: string | undefined,
  signal: AbortSignal,
): Promise<PageLoadResult<IngestionToken>> {
  return loadResourcePage(
    () => client.ingestionTokens.list({
      page: { pageSize: undefined, pageToken, includeTotalSize: true },
      stateFilters: [],
      indexNameFilter: undefined,
      textFilter: undefined,
      sortBy: IngestionTokenSortBy.INGESTION_TOKEN_SORT_BY_NAME,
      sortDirection: SortDirection.SORT_DIRECTION_ASCENDING,
    }, { signal }),
    (response) => response.ingestionTokens,
    signal,
  );
}

interface LoadMorePageDescriptor<T> {
  noun: string;
  nounArticle: string;
  requestedToken: string | null;
  loadingMore: boolean;
  currentItems: readonly T[];
  itemId: (item: T) => string;
  seenPageTokensRef: RefObject<Set<string>>;
  generationRef: RefObject<number>;
  requestRef: RefObject<{ controller: AbortController; generation: number; pageToken: string } | null>;
  mountedRef: RefObject<boolean>;
  loadPage: (pageToken: string, signal: AbortSignal) => Promise<PageLoadResult<T>>;
  setItems: (update: (current: T[]) => T[]) => void;
  setNextPageToken: (value: string | null) => void;
  setTotalSize: (value: bigint | null) => void;
  setTotalSizeExact: (value: boolean) => void;
  setLoadingMore: (value: boolean) => void;
  setPaginationError: (value: string | null) => void;
  setResourceState: (value: ResourceState) => void;
}

async function loadMorePage<T>({
  noun, nounArticle, requestedToken, loadingMore, currentItems, itemId,
  seenPageTokensRef, generationRef, requestRef, mountedRef, loadPage,
  setItems, setNextPageToken, setTotalSize, setTotalSizeExact,
  setLoadingMore, setPaginationError, setResourceState,
}: LoadMorePageDescriptor<T>): Promise<void> {
  if (requestedToken === null || loadingMore) return;
  if (seenPageTokensRef.current.has(requestedToken)) {
    setNextPageToken(null);
    setPaginationError(`The server repeated ${nounArticle} ${noun} page cursor. Refresh before loading more.`);
    return;
  }
  seenPageTokensRef.current.add(requestedToken);
  requestRef.current?.controller.abort();
  const generation = generationRef.current + 1;
  generationRef.current = generation;
  const controller = new AbortController();
  const request = { controller, generation, pageToken: requestedToken };
  requestRef.current = request;
  setLoadingMore(true);
  setPaginationError(null);
  try {
    const result = await loadPage(requestedToken, controller.signal);
    if (
      !mountedRef.current
      || requestRef.current !== request
      || generationRef.current !== generation
      || request.pageToken !== requestedToken
    ) return;
    if (result.state !== "available") {
      if (result.state === "unavailable") setResourceState("unavailable");
      setNextPageToken(null);
      setPaginationError(result.message ?? `The next ${noun} page could not be loaded. Refresh to retry.`);
      return;
    }
    if (
      result.nextPageToken !== null
      && seenPageTokensRef.current.has(result.nextPageToken)
    ) {
      setNextPageToken(null);
      setPaginationError(`The server repeated ${nounArticle} ${noun} page cursor. Refresh before loading more.`);
      return;
    }
    const loadedIds = new Set(currentItems.map((item) => itemId(item)));
    if (result.items.some((item) => loadedIds.has(itemId(item)))) {
      setNextPageToken(null);
      setPaginationError(`The server returned an overlapping ${noun} page. Refresh before loading more.`);
      return;
    }
    setItems((current) => [...current, ...result.items]);
    setNextPageToken(result.nextPageToken);
    setTotalSize(result.totalSize);
    setTotalSizeExact(result.totalSizeExact);
  } catch (error) {
    if (
      controller.signal.aborted
      || requestRef.current !== request
      || generationRef.current !== generation
    ) return;
    setNextPageToken(null);
    setPaginationError(`The next ${noun} page could not be loaded: ${errorMessage(error)}`);
  } finally {
    if (
      mountedRef.current
      && requestRef.current === request
      && generationRef.current === generation
    ) {
      requestRef.current = null;
      setLoadingMore(false);
    }
  }
}

async function listTokensForCreateSafety(
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

export function BackendAdminConsole({ apiBaseUrl }: BackendAdminConsoleProps) {
  const client = useMemo(() => createOpenSplunkApiClient({ baseUrl: apiBaseUrl }), [apiBaseUrl]);
  const [normalizedApiBaseUrl, setNormalizedApiBaseUrl] = useState<string | null>(null);
  const [apiBaseNormalizationError, setApiBaseNormalizationError] = useState<string | null>(null);
  const [section, setSection] = useState<AdminSection>("overview");
  const [bootstrap, setBootstrap] = useState<SystemBootstrapModel | null>(null);
  const [bootstrapError, setBootstrapError] = useState<string | null>(null);
  const [hecSnapshot, setHECSnapshot] = useState<GetHECOperationalSnapshotResponse | null>(null);
  const [hecState, setHECState] = useState<ResourceState>("loading");
  const [hecError, setHECError] = useState<string | null>(null);
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
  const [indexDeleteTarget, setIndexDeleteTarget] = useState<Index | null>(null);
  const [indexDeleteMode, setIndexDeleteMode] = useState(
    IndexDataDeletionMode.INDEX_DATA_DELETION_MODE_KEEP_DATA,
  );
  const [indexDeleteConfirmation, setIndexDeleteConfirmation] = useState("");
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
  const [indexPolicyForm, setIndexPolicyForm] = useState<IndexPolicyForm>(() =>
    indexPolicyFormFromDefinition());
  const [tokenEditTarget, setTokenEditTarget] = useState<IngestionToken | null>(null);
  const [tokenName, setTokenName] = useState("");
  const [tokenDescription, setTokenDescription] = useState("");
  const [tokenCollectorId, setTokenCollectorId] = useState("");
  const [tokenIndexes, setTokenIndexes] = useState<Set<string>>(new Set());
  const [tokenPurpose, setTokenPurpose] = useState(
    IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_NATIVE_COLLECTOR,
  );
  const [tokenHECDefaultIndex, setTokenHECDefaultIndex] = useState("");
  const [tokenHECDefaultHost, setTokenHECDefaultHost] = useState("");
  const [tokenHECDefaultSource, setTokenHECDefaultSource] = useState("");
  const [tokenHECDefaultSourcetype, setTokenHECDefaultSourcetype] = useState("");
  const [tokenHECIndexerAcknowledgment, setTokenHECIndexerAcknowledgment] = useState(false);
  const [tokenPolicyForm, setTokenPolicyForm] = useState<TokenPolicyForm>(() =>
    tokenPolicyFormFromToken());
  const [tokenExpiration, setTokenExpiration] = useState("");
  const [tokenSecret, setTokenSecret] = useState<string | null>(null);
  const [issuedToken, setIssuedToken] = useState<IngestionToken | null>(null);
  const [issuedTokenRecovery, setIssuedTokenRecovery] = useState<TokenCreateRecovery | null>(null);
  const [tokenCreateRecovery, setTokenCreateRecovery] = useState<TokenCreateRecovery | null>(null);
  const [unreadableTokenCreateRecovery, setUnreadableTokenCreateRecovery] =
    useState<UnreadableTokenCreateRecovery | null>(null);
  const [tokenCreateGuardStorageState, setTokenCreateGuardStorageState] =
    useState<TokenCreateGuardStorageState>("checking");
  const [tokenCreateGuardStorageError, setTokenCreateGuardStorageError] = useState<string | null>(null);
  const [tokenCreateLockAvailable, setTokenCreateLockAvailable] = useState<boolean | null>(null);
  const [tokenRecoveryOwnership, setTokenRecoveryOwnership] =
    useState<TokenRecoveryOwnership>("idle");
  const [tokenRecoveryOwnershipError, setTokenRecoveryOwnershipError] = useState<string | null>(null);
  const [tokenRecoveryChecking, setTokenRecoveryChecking] = useState(false);
  const [tokenRecoveryLastCheckedAt, setTokenRecoveryLastCheckedAt] = useState<number | null>(null);
  const [tokenRecoveryNextCheckAt, setTokenRecoveryNextCheckAt] = useState<number | null>(null);
  const [tokenRecoveryEnvironmentReady, setTokenRecoveryEnvironmentReady] = useState(true);
  const [tokenRecoveryAcquireGeneration, setTokenRecoveryAcquireGeneration] = useState(0);
  const [tokenSecretAcknowledged, setTokenSecretAcknowledged] = useState(false);
  const [revokeTarget, setRevokeTarget] = useState<IngestionToken | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [toast, setToast] = useState<AdminToast | null>(null);
  const tokenGuardActive = busy === "create-token"
    || issuedToken !== null
    || tokenCreateRecovery !== null
    || unreadableTokenCreateRecovery !== null;
  const tokenNavigationProtectionActive = tokenSecretRequiresNavigationProtection(tokenSecret);
  const tokenNavigationProtectionActiveRef = useRef(tokenNavigationProtectionActive);
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
  const tokenRecoveryPollAttemptRef = useRef(0);
  const tokenRecoveryFirstZeroObservationRef = useRef<ZeroCandidateObservation | null>(null);
  const tokenCreateRecoveryRef = useRef<TokenCreateRecovery | null>(null);
  const issuedTokenRecoveryRef = useRef<TokenCreateRecovery | null>(null);
  const unreadableTokenCreateRecoveryRef = useRef<UnreadableTokenCreateRecovery | null>(null);
  const tokenRecoveryCheckingRef = useRef(false);
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
  tokenNavigationProtectionActiveRef.current = tokenNavigationProtectionActive;
  tokenCreateRecoveryRef.current = tokenCreateRecovery;
  issuedTokenRecoveryRef.current = issuedTokenRecovery;
  unreadableTokenCreateRecoveryRef.current = unreadableTokenCreateRecovery;
  tokenRecoveryCheckingRef.current = tokenRecoveryChecking;

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
    setHECSnapshot(null);
    setHECState("loading");
    setHECError(null);
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
        if (!supportsServerFeature(value, ServerFeature.SERVER_FEATURE_HEC_INGESTION)) {
          setHECState("unavailable");
          return;
        }
        void client.hec.getOperationalSnapshot({}, { signal: controller.signal }).then(
          (snapshot) => {
            if (!current) return;
            setHECSnapshot(snapshot);
            setHECState("available");
          },
          (hecReason: unknown) => {
            if (!current || controller.signal.aborted) return;
            setHECSnapshot(null);
            setHECState("error");
            setHECError(isAdvertisedFeatureRouteUnavailable(hecReason)
              ? "The server advertises HEC ingestion but did not register its operational snapshot route."
              : errorMessage(hecReason));
          },
        );
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
        setIndexes(result.items);
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
        setTokens(result.items);
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
    const updateRecoveryEnvironment = () => {
      setTokenRecoveryEnvironmentReady(
        tokenRecoveryEnvironmentCanPoll(document.visibilityState, navigator.onLine),
      );
    };
    updateRecoveryEnvironment();
    document.addEventListener("visibilitychange", updateRecoveryEnvironment);
    window.addEventListener("online", updateRecoveryEnvironment);
    window.addEventListener("offline", updateRecoveryEnvironment);
    return () => {
      document.removeEventListener("visibilitychange", updateRecoveryEnvironment);
      window.removeEventListener("online", updateRecoveryEnvironment);
      window.removeEventListener("offline", updateRecoveryEnvironment);
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
      tokenRecoveryOperationGenerationRef.current += 1;
      tokenGuardLockOperationAttemptRef.current = null;
      releaseTokenGuardLease();
      tokenRecoveryCheckingRef.current = false;
      setTokenRecoveryChecking(false);
      setTokenRecoveryNextCheckAt(null);
      tokenRecoveryFirstZeroObservationRef.current = null;
      if (event.newValue === null) {
        const oldGuard = event.oldValue === null
          ? null
          : parsePersistedTokenCreateGuard(event.oldValue, canonicalApiBaseUrl);
        const activeRecovery = tokenCreateRecoveryRef.current ?? issuedTokenRecoveryRef.current;
        const unreadableRecovery = unreadableTokenCreateRecoveryRef.current;
        const exactResolvedAttempt = !tokenNavigationProtectionActiveRef.current && (
          (
            activeRecovery !== null
            && oldGuard?.recovery.attemptId === activeRecovery.attemptId
          )
          || (
            unreadableRecovery !== null
            && event.oldValue === unreadableRecovery.raw
          )
        );
        if (exactResolvedAttempt || (
          event.oldValue === null
          && activeRecovery === null
          && unreadableRecovery === null
        )) {
          setTokenCreateGuardStorageState("available");
          setTokenCreateGuardStorageError(null);
          setTokenRecoveryOwnership("idle");
          setTokenRecoveryOwnershipError(null);
          setTokenCreateRecovery(null);
          setIssuedToken(null);
          setIssuedTokenRecovery(null);
          setUnreadableTokenCreateRecovery(null);
          setTokenSecret(null);
          setTokenSecretAcknowledged(false);
          setModal((current) => current === "create-token" ? null : current);
          if (exactResolvedAttempt) {
            setToast({
              message: "Token creation recovery finished safely in another tab.",
              kind: "success",
            });
          }
        } else {
          if (activeRecovery === null && unreadableRecovery === null && event.oldValue !== null) {
            if (oldGuard === null) {
              setUnreadableTokenCreateRecovery({
                attemptId: crypto.randomUUID(),
                raw: event.oldValue,
                observedServerTimeMs: null,
                candidates: [],
                reconciliationError: "An unreadable token safety record was removed without a matching resolved attempt.",
              });
            } else {
              setTokenCreateRecovery(oldGuard.recovery);
            }
          }
          setTokenRecoveryOwnership("lost");
          setTokenRecoveryOwnershipError(
            "The durable token safety guard was removed by another tab without a matching resolved attempt.",
          );
          setToast({
            message: "Token recovery ownership changed unexpectedly. Token generation remains locked in this tab.",
            kind: "warning",
          });
        }
        return;
      }
      const stored = parsePersistedTokenCreateGuard(
        event.newValue,
        canonicalApiBaseUrl,
      );
      setTokenCreateGuardStorageState("available");
      setTokenCreateGuardStorageError(null);
      setTokenRecoveryOwnership("contended");
      setTokenRecoveryLastCheckedAt(null);
      setTokenRecoveryOwnershipError(stored === null
        ? "Another tab wrote an unreadable token safety record."
        : `Another tab owns token recovery attempt ${stored.recovery.attemptId}.`);
      if (stored === null) {
        setTokenCreateRecovery(null);
        setIssuedToken(null);
        setIssuedTokenRecovery(null);
        setUnreadableTokenCreateRecovery({
          attemptId: crypto.randomUUID(),
          raw: event.newValue,
          observedServerTimeMs: null,
          candidates: [],
          reconciliationError: "The saved token safety record is unreadable.",
        });
      } else {
        setUnreadableTokenCreateRecovery(null);
        setTokenCreateRecovery(stored.recovery);
        setIssuedToken(null);
        setIssuedTokenRecovery(null);
      }
      setToast({
        message: stored === null
          ? "A cross-tab token safety update was unreadable. Token actions are locked."
          : "Another tab owns this token recovery attempt. This tab remains usable and token generation stays paused.",
        kind: "warning",
      });
    }
    window.addEventListener("storage", handleTokenGuardStorage);
    return () => window.removeEventListener("storage", handleTokenGuardStorage);
  }, [normalizedApiBaseUrl]);

  useEffect(() => {
    if (!tokenNavigationProtectionActive) return;
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
      if (!tokenNavigationProtectionActiveRef.current || historyHasTokenGuard(guardId)) return;
      window.history.pushState(
        historyStateWithTokenGuard(guardId),
        "",
        window.location.href,
      );
      setToast({
        message: "Save or revoke the visible one-time token before leaving Administration.",
        kind: "warning",
      });
    }
    function blockClientNavigation(event: MouseEvent) {
      if (!tokenNavigationProtectionActiveRef.current || !(event.target instanceof Element)) return;
      const link = event.target.closest<HTMLAnchorElement>("a[href]");
      if (link === null) return;
      event.preventDefault();
      event.stopPropagation();
      setToast({
        message: "Save or revoke the visible one-time token before following another link.",
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
        if (componentMountedRef.current && tokenNavigationProtectionActiveRef.current) return;
        if (historyHasTokenGuard(guardId)) window.history.back();
        if (tokenHistoryGuardIdRef.current === guardId) tokenHistoryGuardIdRef.current = null;
      }, 0);
    };
  }, [tokenNavigationProtectionActive]);

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
    return loseTokenGuardOwnership(
      recovery.attemptId,
      "This tab no longer owns the exact durable token safety guard.",
      true,
    );
  }

  function loseTokenGuardOwnership(
    attemptId: string,
    message: string,
    notify = false,
  ): false {
    setTokenRecoveryOwnership("lost");
    setTokenRecoveryOwnershipError(message);
    if (notify) {
      setToast({
        message: "Token recovery ownership changed in another tab. This dialog is now read-only.",
        kind: "warning",
      });
    }
    tokenRecoveryOperationGenerationRef.current += 1;
    tokenGuardLockOperationAttemptRef.current = null;
    tokenCreatePreparationControllerRef.current?.abort();
    tokenCreatePreparationControllerRef.current = null;
    tokenRecoveryCheckingRef.current = false;
    setTokenRecoveryChecking(false);
    setTokenRecoveryNextCheckAt(null);
    releaseTokenGuardLease(attemptId);
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
    if (normalizedApiBaseUrl === null) {
      setTokenRecoveryOwnership("failed");
      setTokenRecoveryOwnershipError(
        "The API base URL has not been normalized for durable token safety.",
      );
      return false;
    }
    if (!hasTokenGuardLockContext(recovery.attemptId)) {
      return loseTokenGuardOwnership(
        recovery.attemptId,
        "This tab no longer holds the token safety Web Lock.",
      );
    }
    const key = tokenCreateGuardStorageKey(normalizedApiBaseUrl);
    let existingRaw: string | null;
    try {
      existingRaw = window.localStorage.getItem(key);
    } catch (error) {
      setTokenCreateGuardStorageState("unavailable");
      setTokenCreateGuardStorageError(errorMessage(error));
      return false;
    }
    if (existingRaw === null && !options.allowCreate) {
      return loseTokenGuardOwnership(
        recovery.attemptId,
        "The durable token safety guard disappeared before it could be updated.",
      );
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
        return loseTokenGuardOwnership(
          recovery.attemptId,
          "Another tab or token attempt owns the durable safety guard.",
        );
      }
    }
    const record = serializeTokenCreateGuard(
      normalizedApiBaseUrl,
      recovery,
      knownIssuedTokenId,
    );
    try {
      // The record is deliberately constructed field-by-field and contains no
      // plaintext credential. localStorage makes the safety guard survive tab
      // closure and broadcasts ownership changes to other same-origin tabs.
      window.localStorage.setItem(
        key,
        JSON.stringify(record),
      );
      setTokenCreateGuardStorageState("available");
      setTokenCreateGuardStorageError(null);
      setTokenRecoveryOwnership("owned");
      setTokenRecoveryOwnershipError(null);
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
    if (normalizedApiBaseUrl === null) {
      setTokenRecoveryOwnership("failed");
      setTokenRecoveryOwnershipError(
        "The API base URL has not been normalized for durable token safety.",
      );
      return false;
    }
    if (!hasTokenGuardLockContext(expectedAttemptId)) {
      return loseTokenGuardOwnership(
        expectedAttemptId,
        "This tab no longer holds the token safety Web Lock.",
      );
    }
    const key = tokenCreateGuardStorageKey(normalizedApiBaseUrl);
    let raw: string | null;
    try {
      raw = window.localStorage.getItem(key);
    } catch (error) {
      setTokenCreateGuardStorageState("unavailable");
      setTokenCreateGuardStorageError(errorMessage(error));
      return false;
    }
    if (raw === null) {
      return loseTokenGuardOwnership(
        expectedAttemptId,
        "The durable token safety guard disappeared unexpectedly.",
      );
    }
    const stored = parsePersistedTokenCreateGuard(raw, normalizedApiBaseUrl);
    if (
      stored === null
      || stored.recovery.attemptId !== expectedAttemptId
      || stored.recovery.ownerId !== expectedOwnerId
    ) {
      return loseTokenGuardOwnership(
        expectedAttemptId,
        "A different or unreadable token safety attempt owns the durable guard.",
      );
    }
    try {
      window.localStorage.removeItem(key);
      tokenRecoveryOperationGenerationRef.current += 1;
      if (tokenGuardLockOperationAttemptRef.current === expectedAttemptId) {
        tokenGuardLockOperationAttemptRef.current = null;
      }
      releaseTokenGuardLease(expectedAttemptId);
      setTokenCreateGuardStorageState("available");
      setTokenCreateGuardStorageError(null);
      setTokenRecoveryOwnership("idle");
      setTokenRecoveryOwnershipError(null);
      tokenRecoveryCheckingRef.current = false;
      setTokenRecoveryChecking(false);
      setTokenRecoveryNextCheckAt(null);
      setTokenRecoveryLastCheckedAt(Date.now());
      tokenRecoveryPollAttemptRef.current = 0;
      tokenRecoveryFirstZeroObservationRef.current = null;
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
    return loadMorePage({
      noun: "index",
      nounArticle: "an",
      requestedToken: indexNextPageToken,
      loadingMore: indexLoadingMore,
      currentItems: indexes,
      itemId: (index) => index.indexId,
      seenPageTokensRef: indexSeenPageTokensRef,
      generationRef: indexPageRequestGenerationRef,
      requestRef: indexLoadMoreRequestRef,
      mountedRef: componentMountedRef,
      loadPage: (pageToken, signal) => loadIndexPage(client, pageToken, signal),
      setItems: setIndexes,
      setNextPageToken: setIndexNextPageToken,
      setTotalSize: setIndexTotalSize,
      setTotalSizeExact: setIndexTotalSizeExact,
      setLoadingMore: setIndexLoadingMore,
      setPaginationError: setIndexPaginationError,
      setResourceState: setIndexState,
    });
  }

  async function loadMoreTokens() {
    return loadMorePage({
      noun: "token",
      nounArticle: "a",
      requestedToken: tokenNextPageToken,
      loadingMore: tokenLoadingMore,
      currentItems: tokens,
      itemId: (token) => token.ingestionTokenId,
      seenPageTokensRef: tokenSeenPageTokensRef,
      generationRef: tokenPageRequestGenerationRef,
      requestRef: tokenLoadMoreRequestRef,
      mountedRef: componentMountedRef,
      loadPage: (pageToken, signal) => loadTokenPage(client, pageToken, signal),
      setItems: setTokens,
      setNextPageToken: setTokenNextPageToken,
      setTotalSize: setTokenTotalSize,
      setTotalSizeExact: setTokenTotalSizeExact,
      setLoadingMore: setTokenLoadingMore,
      setPaginationError: setTokenPaginationError,
      setResourceState: setTokenState,
    });
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
    setIndexPolicyForm(indexPolicyFormFromDefinition());
    setModal("create-index");
  }

  function openTokenDialog() {
    setTokenEditTarget(null);
    setTokenName("");
    setTokenDescription("");
    setTokenCollectorId("");
    setTokenIndexes(new Set(ingestibleTokenScopes.slice(0, 1).map((scope) => scope.name)));
    setTokenPurpose(IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_NATIVE_COLLECTOR);
    setTokenHECDefaultIndex("");
    setTokenHECDefaultHost("");
    setTokenHECDefaultSource("");
    setTokenHECDefaultSourcetype("");
    setTokenHECIndexerAcknowledgment(false);
    setTokenPolicyForm(tokenPolicyFormFromToken());
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
      setIndexPolicyForm(indexPolicyFormFromDefinition(current.definition));
      setModal("edit-index");
    } catch (error) {
      setToast({ message: errorMessage(error), kind: "warning" });
      load();
    } finally {
      setBusy(null);
    }
  }

  async function openIndexDeleteDialog(index: Index) {
    setBusy(`read-index-${index.indexId}`);
    try {
      const response = await client.indexes.get({
        selector: { selector: { $case: "indexId", value: index.indexId } },
      });
      const current = response.index;
      if (current?.definition === undefined) throw new Error("The server returned an empty index definition.");
      if (current.state === IndexState.INDEX_STATE_DELETING) {
        throw new Error("This index is already being deleted.");
      }
      setIndexDeleteTarget(current);
      setIndexDeleteMode(IndexDataDeletionMode.INDEX_DATA_DELETION_MODE_KEEP_DATA);
      setIndexDeleteConfirmation("");
    } catch (error) {
      setToast({ message: errorMessage(error), kind: "warning" });
      load();
    } finally {
      setBusy(null);
    }
  }

  async function deleteIndex(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const target = indexDeleteTarget;
    const confirmationName = target?.definition?.name;
    if (target === null || confirmationName === undefined) return;
    if (indexDeleteConfirmation !== confirmationName) {
      setToast({ message: `Type ${confirmationName} exactly to confirm deletion.`, kind: "warning" });
      return;
    }
    if (
      indexDeleteMode !== IndexDataDeletionMode.INDEX_DATA_DELETION_MODE_KEEP_DATA
      && indexDeleteMode !== IndexDataDeletionMode.INDEX_DATA_DELETION_MODE_DELETE_DATA
    ) {
      setToast({ message: "Choose whether stored index data should be retained or physically deleted.", kind: "warning" });
      return;
    }
    cancelIndexLoadMoreRequest();
    setBusy(`delete-index-${target.indexId}`);
    try {
      const response = await client.indexes.delete({
        selector: { selector: { $case: "indexId", value: target.indexId } },
        expectedVersion: target.version,
        dataDeletionMode: indexDeleteMode,
        confirmationName,
      });
      if (response.indexId !== target.indexId) {
        throw new TypeError("The server acknowledged deletion for a different index.");
      }
      setIndexDeleteTarget(null);
      setIndexDeleteConfirmation("");
      setToast({
        message: indexDeleteMode === IndexDataDeletionMode.INDEX_DATA_DELETION_MODE_DELETE_DATA
          ? `Deletion started for ${confirmationName}${response.deletionOperationId ? ` (${response.deletionOperationId})` : ""}.`
          : `Index ${confirmationName} was removed from the catalog and its stored data was retained.`,
        kind: "success",
      });
      load();
    } catch (error) {
      setToast({ message: errorMessage(error), kind: "warning" });
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
      setTokenCollectorId(current.constraints?.boundCollectorId ?? "");
      setTokenIndexes(new Set(current.constraints?.allowedIndexNames ?? []));
      setTokenPurpose(current.purpose);
      setTokenHECDefaultIndex(current.hecProfile?.defaultIndexName ?? "");
      setTokenHECDefaultHost(current.hecProfile?.defaultHost ?? "");
      setTokenHECDefaultSource(current.hecProfile?.defaultSource ?? "");
      setTokenHECDefaultSourcetype(current.hecProfile?.defaultSourcetype ?? "");
      setTokenHECIndexerAcknowledgment(current.hecProfile?.indexerAcknowledgment ?? false);
      setTokenPolicyForm(tokenPolicyFormFromToken(current));
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
    let policy: ReturnType<typeof indexPolicyFromForm>;
    try {
      policy = indexPolicyFromForm(indexPolicyForm);
    } catch (error) {
      setToast({ message: errorMessage(error), kind: "warning" });
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
          defaultSourcetype: policy.defaultSourcetype,
          limits: policy.limits,
          ingestionRateLimits: policy.ingestionRateLimits,
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
    let policy: ReturnType<typeof indexPolicyFromForm>;
    try {
      policy = indexPolicyFromForm(indexPolicyForm);
    } catch (error) {
      setToast({ message: errorMessage(error), kind: "warning" });
      return;
    }
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
    if (indexPolicyForm.defaultSourcetype !== (definition.defaultSourcetype ?? "")) {
      updateMask.push("default_sourcetype");
    }
    const currentPolicyForm = indexPolicyFormFromDefinition(definition);
    if (
      indexPolicyForm.maxEventBytes !== currentPolicyForm.maxEventBytes
      || indexPolicyForm.maxFieldCount !== currentPolicyForm.maxFieldCount
      || indexPolicyForm.maxNestingDepth !== currentPolicyForm.maxNestingDepth
      || indexPolicyForm.maximumFutureSkewSeconds !== currentPolicyForm.maximumFutureSkewSeconds
      || indexPolicyForm.maximumEventAgeSeconds !== currentPolicyForm.maximumEventAgeSeconds
    ) {
      updateMask.push("limits");
    }
    if (
      indexPolicyForm.maxEventsPerSecond !== currentPolicyForm.maxEventsPerSecond
      || indexPolicyForm.maxUncompressedBytesPerSecond
        !== currentPolicyForm.maxUncompressedBytesPerSecond
    ) {
      updateMask.push("ingestion_rate_limits");
    }
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
          defaultSourcetype: policy.defaultSourcetype,
          limits: policy.limits,
          ingestionRateLimits: policy.ingestionRateLimits,
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

  function scheduleNextTokenRecoveryCheck(delayMs?: number) {
    const delay = delayMs ?? tokenRecoveryPollDelayMs(tokenRecoveryPollAttemptRef.current);
    tokenRecoveryPollAttemptRef.current += 1;
    setTokenRecoveryNextCheckAt(Date.now() + delay);
  }

  function stopAutomaticTokenRecovery() {
    setTokenRecoveryNextCheckAt(null);
    tokenRecoveryPollAttemptRef.current = 0;
  }

  function applyTokenCreateCandidates(
    recovery: TokenCreateRecovery,
    candidates: IngestionToken[],
  ) {
    if (!requireTokenGuardOwnership(recovery)) return;
    setTokenRecoveryLastCheckedAt(Date.now());
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
      stopAutomaticTokenRecovery();
      tokenRecoveryFirstZeroObservationRef.current = null;
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
      stopAutomaticTokenRecovery();
      tokenRecoveryFirstZeroObservationRef.current = null;
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
      stopAutomaticTokenRecovery();
      tokenRecoveryFirstZeroObservationRef.current = null;
      const candidate = unresolvedCandidates[0];
      if (!persistTokenCreateGuard(nextRecovery, candidate.ingestionTokenId)) return;
      setTokenCreateRecovery(null);
      setIssuedToken(candidate);
      setIssuedTokenRecovery(nextRecovery);
      setTokenSecret(null);
      setTokenSecretAcknowledged(false);
      setToast({
        message: tokenCanBeRevoked(candidate)
          ? `A newly created token (${candidate.tokenPrefix}) was identified, but its one-time secret was lost. Revoke it before leaving.`
          : `A newly created token (${candidate.tokenPrefix}) was identified without its secret and is already ${tokenStateLabel(candidate.state).toLowerCase()}.`,
        kind: "warning",
      });
      return;
    }
    if (unresolvedCandidates.length > 0) {
      stopAutomaticTokenRecovery();
      tokenRecoveryFirstZeroObservationRef.current = null;
    }
    const unresolvedRecovery: TokenCreateRecovery = {
      ...nextRecovery,
      candidates: unresolvedCandidates,
      reconciliationError: null,
    };
    if (!persistTokenCreateGuard(unresolvedRecovery, null)) return;
    setTokenCreateRecovery(unresolvedRecovery);
    if (unresolvedCandidates.length === 0) {
      const serverNowMs = authoritativeServerNowMs();
      const decision = serverNowMs === undefined
        ? { kind: "pending" as const, firstObservation: null }
        : tokenRecoverySnapshotDecision({
            candidateCount: unresolvedCandidates.length,
            attemptId: recovery.attemptId,
            serverNowMs,
            quiescenceDeadlineMs: tokenRecoveryQuiescenceDeadline(recovery.definition),
            previousObservation: tokenRecoveryFirstZeroObservationRef.current,
          });
      tokenRecoveryFirstZeroObservationRef.current = decision.firstObservation;
      if (decision.kind === "clear") {
        if (!clearTokenCreateGuard(recovery.attemptId, recovery.ownerId)) return;
        setTokenCreateRecovery(null);
        setIssuedToken(null);
        setIssuedTokenRecovery(null);
        setTokenSecret(null);
        setTokenSecretAcknowledged(false);
        setModal((current) => current === "create-token" ? null : current);
        setToast({
          message: `No token named “${recovery.definition.name}” was created. Token generation is available again.`,
          kind: "success",
        });
        return;
      }
      scheduleNextTokenRecoveryCheck(
        decision.kind === "confirm"
          ? TOKEN_CREATE_ZERO_CONFIRMATION_INTERVAL_MS
          : undefined,
      );
    }
    setToast({
      message: unresolvedCandidates.length === 0
        ? `Open Splunk is still checking whether token “${recovery.definition.name}” was created. You can keep using the rest of the app.`
        : `${unresolvedCandidates.length.toLocaleString()} new matching tokens prevent safe automatic identification. Review the possible tokens and check again; do not submit another create request.`,
      kind: "warning",
    });
  }

  async function reconcileTokenCreateRecovery(
    recovery: TokenCreateRecovery,
    inheritedOperationGeneration?: number,
  ) {
    if (tokenRecoveryCheckingRef.current) return;
    if (!requireTokenGuardOwnership(recovery)) return;
    const operationGeneration = inheritedOperationGeneration
      ?? beginTokenRecoveryOperation();
    setTokenRecoveryNextCheckAt(null);
    tokenRecoveryCheckingRef.current = true;
    setTokenRecoveryChecking(true);
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
      const serverNowMs = authoritativeServerNowMs();
      const deadline = tokenRecoveryQuiescenceDeadline(recovery.definition);
      if (serverNowMs !== undefined && deadline !== null && serverNowMs < deadline) {
        scheduleNextTokenRecoveryCheck();
      } else {
        stopAutomaticTokenRecovery();
      }
      setToast({
        message: `Open Splunk could not check token “${recovery.definition.name}”: ${errorMessage(error)} Token generation remains paused.`,
        kind: "warning",
      });
    } finally {
      if (
        componentMountedRef.current
        && tokenRecoveryOperationGenerationRef.current === operationGeneration
      ) {
        tokenRecoveryCheckingRef.current = false;
        setTokenRecoveryChecking(false);
      }
    }
  }

  function ownsUnreadableTokenCreateGuard(recovery: UnreadableTokenCreateRecovery): boolean {
    try {
      if (normalizedApiBaseUrl === null || !hasTokenGuardLockContext(recovery.attemptId)) {
        return false;
      }
      return window.localStorage.getItem(
        tokenCreateGuardStorageKey(normalizedApiBaseUrl),
      ) === recovery.raw;
    } catch {
      return false;
    }
  }

  function requireUnreadableTokenGuardOwnership(
    recovery: UnreadableTokenCreateRecovery,
  ): boolean {
    if (normalizedApiBaseUrl === null) {
      setTokenRecoveryOwnership("failed");
      setTokenRecoveryOwnershipError(
        "The API base URL has not been normalized for durable token safety.",
      );
      return false;
    }
    if (!hasTokenGuardLockContext(recovery.attemptId)) {
      return loseTokenGuardOwnership(
        recovery.attemptId,
        "This tab no longer holds the token safety Web Lock.",
      );
    }
    try {
      if (window.localStorage.getItem(
        tokenCreateGuardStorageKey(normalizedApiBaseUrl),
      ) === recovery.raw) return true;
    } catch (error) {
      setTokenCreateGuardStorageState("unavailable");
      setTokenCreateGuardStorageError(errorMessage(error));
      tokenRecoveryCheckingRef.current = false;
      setTokenRecoveryChecking(false);
      stopAutomaticTokenRecovery();
      releaseTokenGuardLease(recovery.attemptId);
      return false;
    }
    return loseTokenGuardOwnership(
      recovery.attemptId,
      "This tab no longer owns the exact unreadable token safety record.",
    );
  }

  function clearUnreadableTokenCreateGuard(
    recovery: UnreadableTokenCreateRecovery,
  ): boolean {
    if (!requireUnreadableTokenGuardOwnership(recovery)) return false;
    try {
      if (normalizedApiBaseUrl === null) return false;
      const key = tokenCreateGuardStorageKey(normalizedApiBaseUrl);
      if (window.localStorage.getItem(key) !== recovery.raw) {
        return loseTokenGuardOwnership(
          recovery.attemptId,
          "The unreadable token safety record changed before it could be cleared.",
        );
      }
      window.localStorage.removeItem(key);
      tokenRecoveryOperationGenerationRef.current += 1;
      tokenGuardLockOperationAttemptRef.current = null;
      releaseTokenGuardLease(recovery.attemptId);
      setUnreadableTokenCreateRecovery(null);
      setTokenRecoveryOwnership("idle");
      setTokenRecoveryOwnershipError(null);
      setTokenRecoveryChecking(false);
      tokenRecoveryCheckingRef.current = false;
      setTokenRecoveryLastCheckedAt(Date.now());
      stopAutomaticTokenRecovery();
      tokenRecoveryFirstZeroObservationRef.current = null;
      return true;
    } catch (error) {
      setTokenCreateGuardStorageState("unavailable");
      setTokenCreateGuardStorageError(errorMessage(error));
      return false;
    }
  }

  async function reconcileUnreadableTokenCreateRecovery(
    recovery: UnreadableTokenCreateRecovery,
  ) {
    if (tokenRecoveryCheckingRef.current) return;
    if (!requireUnreadableTokenGuardOwnership(recovery)) return;
    const operationGeneration = beginTokenRecoveryOperation();
    setTokenRecoveryNextCheckAt(null);
    tokenRecoveryCheckingRef.current = true;
    setTokenRecoveryChecking(true);
    try {
      const snapshot = await listTokensForCreateSafety(client, undefined);
      if (
        !componentMountedRef.current
        || tokenRecoveryOperationGenerationRef.current !== operationGeneration
        || !requireUnreadableTokenGuardOwnership(recovery)
      ) return;
      setTokenRecoveryLastCheckedAt(Date.now());
      for (const token of snapshot) storeTokenSnapshot(token);
      const candidates = snapshot.filter((token) => !tokenIsTerminallySafe(token));
      if (candidates.length > 0) {
        tokenRecoveryFirstZeroObservationRef.current = null;
        stopAutomaticTokenRecovery();
        setUnreadableTokenCreateRecovery({
          ...recovery,
          candidates,
          reconciliationError: "Every nonterminal token must become revoked or expired because the damaged record contains no safe attribution data.",
        });
        return;
      }

      const serverNowMs = authoritativeServerNowMs();
      const observedServerTimeMs = recovery.observedServerTimeMs ?? serverNowMs ?? null;
      const nextRecovery = {
        ...recovery,
        observedServerTimeMs,
        candidates: [],
        reconciliationError: null,
      };
      setUnreadableTokenCreateRecovery(nextRecovery);
      const deadline = observedServerTimeMs === null
        ? null
        : observedServerTimeMs
          + 2 * DEFAULT_REQUEST_TIMEOUT_MS
          + Math.max(serverClockAnchor?.uncertaintyMs ?? 0, TOKEN_CREATE_CLOCK_EPSILON_MS);
      const decision = serverNowMs === undefined
        ? { kind: "pending" as const, firstObservation: null }
        : tokenRecoverySnapshotDecision({
            candidateCount: candidates.length,
            attemptId: recovery.attemptId,
            serverNowMs,
            quiescenceDeadlineMs: deadline,
            previousObservation: tokenRecoveryFirstZeroObservationRef.current,
          });
      tokenRecoveryFirstZeroObservationRef.current = decision.firstObservation;
      if (decision.kind === "clear") {
        if (!clearUnreadableTokenCreateGuard(nextRecovery)) return;
        setModal((current) => current === "create-token" ? null : current);
        setToast({
          message: "The damaged token safety record was reconciled. No nonterminal token remained, so token generation is available again.",
          kind: "success",
        });
        return;
      }
      scheduleNextTokenRecoveryCheck(
        decision.kind === "confirm"
          ? TOKEN_CREATE_ZERO_CONFIRMATION_INTERVAL_MS
          : undefined,
      );
    } catch (error) {
      if (
        !componentMountedRef.current
        || tokenRecoveryOperationGenerationRef.current !== operationGeneration
        || !requireUnreadableTokenGuardOwnership(recovery)
      ) return;
      const serverNowMs = authoritativeServerNowMs();
      const observedServerTimeMs = recovery.observedServerTimeMs ?? serverNowMs ?? null;
      const failedRecovery = {
        ...recovery,
        observedServerTimeMs,
        reconciliationError: errorMessage(error),
      };
      setUnreadableTokenCreateRecovery(failedRecovery);
      const deadline = observedServerTimeMs === null
        ? null
        : observedServerTimeMs
          + 2 * DEFAULT_REQUEST_TIMEOUT_MS
          + Math.max(serverClockAnchor?.uncertaintyMs ?? 0, TOKEN_CREATE_CLOCK_EPSILON_MS);
      if (serverNowMs !== undefined && deadline !== null && serverNowMs < deadline) {
        scheduleNextTokenRecoveryCheck();
      } else {
        stopAutomaticTokenRecovery();
      }
    } finally {
      if (
        componentMountedRef.current
        && tokenRecoveryOperationGenerationRef.current === operationGeneration
      ) {
        tokenRecoveryCheckingRef.current = false;
        setTokenRecoveryChecking(false);
      }
    }
  }

  async function revokeUnreadableTokenCandidate(candidate: IngestionToken) {
    const recovery = unreadableTokenCreateRecoveryRef.current;
    if (recovery === null || !tokenCanBeRevoked(candidate)) return;
    if (!requireUnreadableTokenGuardOwnership(recovery)) return;
    setBusy(`recover-damaged-token-${candidate.ingestionTokenId}`);
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
        || !tokenIsTerminallySafe(revoked)
      ) {
        throw new Error("The server did not confirm token revocation.");
      }
      storeTokenSnapshot(revoked);
      await reconcileUnreadableTokenCreateRecovery({
        ...recovery,
        candidates: recovery.candidates.filter((token) =>
          token.ingestionTokenId !== candidate.ingestionTokenId),
      });
    } catch (error) {
      setUnreadableTokenCreateRecovery((current) => current === null ? null : {
        ...current,
        reconciliationError: `Candidate revocation was not confirmed: ${errorMessage(error)}`,
      });
    } finally {
      setBusy(null);
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
    const creatingHECToken = tokenUsesHEC(tokenPurpose);
    if (
      tokenName.trim().length === 0
      || (!creatingHECToken && !validCollectorId(tokenCollectorId))
    ) return;
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
        message: creatingHECToken && !hecEnabled
          ? "HEC token generation is unavailable because the server does not advertise HEC ingestion."
          : tokenScopeSource === "unavailable"
          ? "Token generation is unavailable until the server returns an authoritative index summary."
          : tokenIndexes.size === 0
            ? "Select at least one active, ingestion-enabled index."
            : "Remove unavailable index scopes before generating the token.",
        kind: "warning",
      });
      return;
    }
    let tokenPolicy: ReturnType<typeof tokenPolicyFromForm>;
    try {
      tokenPolicy = tokenPolicyFromForm(tokenPolicyForm);
    } catch (error) {
      setToast({ message: errorMessage(error), kind: "warning" });
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
            setTokenCreateGuardStorageState("available");
            setTokenCreateGuardStorageError(null);
            setTokenRecoveryOwnership("contended");
            setTokenRecoveryOwnershipError(
              "A durable token safety attempt already exists in another tab or prior session.",
            );
            setToast({
              message: "Another tab or prior session already has an unresolved token safety attempt. Open token recovery to take ownership safely.",
              kind: "warning",
            });
            setTokenRecoveryAcquireGeneration((current) => current + 1);
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
    setTokenRecoveryLastCheckedAt(null);
    stopAutomaticTokenRecovery();
    tokenRecoveryFirstZeroObservationRef.current = null;
    setBusy("create-token");
    try {
      const initialServerTimeMs = authoritativeServerNowMs();
      if (initialServerTimeMs === undefined) {
        throw new Error("System bootstrap no longer supplies an authoritative server clock.");
      }
      const expiresAt = expirationFromForm(tokenExpiration, initialServerTimeMs);
      const hecProfile = creatingHECToken ? hecProfileFromForm({
        defaultIndexName: tokenHECDefaultIndex,
        defaultHost: tokenHECDefaultHost,
        defaultSource: tokenHECDefaultSource,
        defaultSourcetype: tokenHECDefaultSourcetype,
        indexerAcknowledgment: tokenHECIndexerAcknowledgment,
      }) : undefined;
      const definition: TokenCreateDefinitionSnapshot = {
        name: tokenName.trim(),
        description: tokenDescription.trim(),
        boundCollectorId: creatingHECToken ? "" : tokenCollectorId,
        allowedIndexNames: [...tokenIndexes].toSorted(),
        allowedHostRegexes: tokenPolicy.allowedHostRegexes,
        allowedSourceRegexes: tokenPolicy.allowedSourceRegexes,
        maxEventsPerSecond: tokenPolicy.ingestionRateLimits.maxEventsPerSecond,
        maxUncompressedBytesPerSecond:
          tokenPolicy.ingestionRateLimits.maxUncompressedBytesPerSecond,
        purpose: tokenPurpose,
        hecProfile,
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
            allowedHostRegexes: definition.allowedHostRegexes ?? [],
            allowedSourceRegexes: definition.allowedSourceRegexes ?? [],
            boundCollectorId: definition.boundCollectorId || undefined,
          },
          expiresAt: definition.expiresAt,
          ingestionRateLimits: {
            maxEventsPerSecond: definition.maxEventsPerSecond,
            maxUncompressedBytesPerSecond: definition.maxUncompressedBytesPerSecond,
          },
          purpose: definition.purpose,
          hecProfile: definition.hecProfile,
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
      setTokenRecoveryOwnership("failed");
      setTokenRecoveryOwnershipError(`Cross-tab token lock failed: ${errorMessage(error)}`);
      setToast({
        message: "The browser could not acquire the cross-tab token safety lock.",
        kind: "warning",
      });
      return;
    }
    if (!crossTabLockAcquired) {
      setTokenRecoveryOwnership("contended");
      setTokenRecoveryOwnershipError(
        "Another tab is currently creating or reconciling an ingestion token.",
      );
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
    const targetUsesHEC = tokenUsesHEC(target.purpose);
    if (targetUsesHEC && tokenHECProfileInvalid) return;
    let tokenPolicy: ReturnType<typeof tokenPolicyFromForm>;
    try {
      tokenPolicy = tokenPolicyFromForm(tokenPolicyForm);
    } catch (error) {
      setToast({ message: errorMessage(error), kind: "warning" });
      return;
    }
    const updatedHECProfile = targetUsesHEC ? hecProfileFromForm({
      defaultIndexName: tokenHECDefaultIndex,
      defaultHost: tokenHECDefaultHost,
      defaultSource: tokenHECDefaultSource,
      defaultSourcetype: tokenHECDefaultSourcetype,
      indexerAcknowledgment: tokenHECIndexerAcknowledgment,
    }) : undefined;
    const hecProfileChanged = targetUsesHEC
      && !hecProfilesMatch(updatedHECProfile, target.hecProfile);
    const scopeChanged = !hasSameStrings(
      tokenIndexes,
      target.constraints?.allowedIndexNames ?? [],
    );
    const hostConstraintsChanged = !hasSameStrings(
      tokenPolicy.allowedHostRegexes,
      target.constraints?.allowedHostRegexes ?? [],
    );
    const sourceConstraintsChanged = !hasSameStrings(
      tokenPolicy.allowedSourceRegexes,
      target.constraints?.allowedSourceRegexes ?? [],
    );
    const rateLimitsChanged = tokenPolicy.ingestionRateLimits.maxEventsPerSecond
      !== target.ingestionRateLimits?.maxEventsPerSecond
      || tokenPolicy.ingestionRateLimits.maxUncompressedBytesPerSecond
        !== target.ingestionRateLimits?.maxUncompressedBytesPerSecond;
    const updateMask: string[] = [];
    if (tokenName.trim() !== target.name) updateMask.push("name");
    if (tokenDescription !== (target.description ?? "")) updateMask.push("description");
    const profileCanBeAppliedBeforeScope = updatedHECProfile?.defaultIndexName === undefined
      || (target.constraints?.allowedIndexNames ?? []).includes(
        updatedHECProfile.defaultIndexName,
      );
    if (hecProfileChanged && profileCanBeAppliedBeforeScope) {
      updateMask.push("hec_profile");
    }
    if (scopeChanged || hostConstraintsChanged || sourceConstraintsChanged) {
      updateMask.push("constraints");
    }
    if (hecProfileChanged && !profileCanBeAppliedBeforeScope) {
      updateMask.push("hec_profile");
    }
    if (
      target.constraints?.boundCollectorId === undefined
      && validCollectorId(tokenCollectorId)
      && !updateMask.includes("constraints")
    ) {
      updateMask.push("constraints.bound_collector_id");
    }
    if (tokenExpiration !== dateTimeLocalValue(target.expiresAt)) updateMask.push("expires_at");
    if (rateLimitsChanged) updateMask.push("ingestion_rate_limits");
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
            ...target.constraints,
            allowedIndexNames: [...tokenIndexes].toSorted(),
            allowedHostRegexes: tokenPolicy.allowedHostRegexes,
            allowedSourceRegexes: tokenPolicy.allowedSourceRegexes,
            boundCollectorId: targetUsesHEC
              ? undefined
              : tokenCollectorId || target.constraints?.boundCollectorId,
          },
          expiresAt: expirationFromForm(tokenExpiration, authoritativeServerNowMs()),
          ingestionRateLimits: tokenPolicy.ingestionRateLimits,
          purpose: target.purpose,
          hecProfile: updatedHECProfile,
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

  async function setTokenEnabled(token: IngestionToken, enabled: boolean) {
    cancelTokenLoadMoreRequest();
    const targetState = enabled
      ? IngestionTokenState.INGESTION_TOKEN_STATE_ACTIVE
      : IngestionTokenState.INGESTION_TOKEN_STATE_DISABLED;
    const operation = enabled ? "re-enable" : "disable";
    setBusy(`token-state-${token.ingestionTokenId}`);
    try {
      const response = await client.ingestionTokens.setState({
        ingestionTokenId: token.ingestionTokenId,
        expectedVersion: token.version,
        enabled,
      });
      const updated = response.ingestionToken;
      if (
        updated === undefined
        || updated.ingestionTokenId !== token.ingestionTokenId
        || updated.version !== token.version + 1n
        || updated.state !== targetState
        || updated.revokedAt !== undefined
      ) {
        throw new Error(`The server did not confirm token ${operation}.`);
      }
      storeTokenSnapshot(updated);
      setToast({
        message: `Token “${updated.name}” was ${enabled ? "re-enabled" : "disabled"}.`,
        kind: "success",
      });
    } catch (stateError) {
      try {
        const current = await readCurrentToken(token);
        storeTokenSnapshot(current);
        if (current.state === targetState) {
          setToast({
            message: `Token “${current.name}” is confirmed ${enabled ? "active" : "disabled"}.`,
            kind: "success",
          });
        } else {
          setToast({
            message: `Token ${operation} was not confirmed: ${errorMessage(stateError)} The latest ${tokenStateLabel(current.state).toLowerCase()} token version was loaded.`,
            kind: "warning",
          });
        }
      } catch (refreshError) {
        setToast({
          message: `Token ${operation} was not confirmed: ${errorMessage(stateError)} The token could not be reconciled: ${errorMessage(refreshError)}`,
          kind: "warning",
        });
      }
    } finally {
      setBusy(null);
    }
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

  const reconcilePersistedTokenCreate = useEffectEvent(() => {
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
      if (
        tokenCreateRecoveryRef.current !== null
        || issuedTokenRecoveryRef.current !== null
        || unreadableTokenCreateRecoveryRef.current !== null
      ) {
        setTokenRecoveryOwnership("lost");
        setTokenRecoveryOwnershipError(
          "The durable token safety guard is missing, so this tab cannot prove that the saved recovery attempt was resolved.",
        );
      } else {
        setTokenRecoveryOwnership("idle");
        setTokenRecoveryOwnershipError(null);
      }
      return stop;
    }
    setTokenCreateGuardStorageState("available");
    setTokenCreateGuardStorageError(null);
    const restored = parsePersistedTokenCreateGuard(raw, normalizedApiBaseUrl);
    setTokenSecret(null);
    setTokenSecretAcknowledged(false);
    setIssuedToken(null);
    setIssuedTokenRecovery(null);
    const unreadableRecovery: UnreadableTokenCreateRecovery | null = restored === null
      ? {
          attemptId: crypto.randomUUID(),
          raw,
          observedServerTimeMs: authoritativeServerNowMs() ?? null,
          candidates: [],
          reconciliationError: "The saved token safety record is unreadable.",
        }
      : null;
    if (restored === null) {
      setTokenCreateRecovery(null);
      setUnreadableTokenCreateRecovery(unreadableRecovery);
    } else {
      setUnreadableTokenCreateRecovery(null);
      setTokenCreateRecovery(restored.recovery);
    }
    if (!lockAvailable) {
      setTokenRecoveryOwnership("failed");
      setTokenRecoveryOwnershipError(
        "This browser does not expose the cross-tab Web Locks API required to own token recovery safely.",
      );
      return stop;
    }

    setTokenRecoveryOwnership("acquiring");
    setTokenRecoveryOwnershipError(null);
    void lockManager.request(
      tokenCreateLockName(normalizedApiBaseUrl),
      { mode: "exclusive", ifAvailable: true, signal: controller.signal },
      async (lock) => {
        if (!current) return;
        if (lock === null) {
          setTokenRecoveryOwnership("contended");
          setTokenRecoveryOwnershipError(
            "Another tab currently owns token creation recovery. Close that tab, then try again.",
          );
          return;
        }
        if (restored === null && unreadableRecovery !== null) {
          tokenGuardLockOperationAttemptRef.current = unreadableRecovery.attemptId;
          setTokenRecoveryOwnership("owned");
          setTokenRecoveryOwnershipError(null);
          setUnreadableTokenCreateRecovery(unreadableRecovery);
          try {
            if (tokenRecoveryEnvironmentCanPoll(document.visibilityState, navigator.onLine)) {
              await reconcileUnreadableTokenCreateRecovery(unreadableRecovery);
            } else {
              setTokenRecoveryNextCheckAt(Date.now());
            }
          } finally {
            if (current && ownsUnreadableTokenCreateGuard(unreadableRecovery)) {
              const lease = holdTokenGuardLease(unreadableRecovery.attemptId);
              tokenGuardLockOperationAttemptRef.current = null;
              await lease;
            } else {
              tokenGuardLockOperationAttemptRef.current = null;
            }
          }
          return;
        }
        if (restored === null) return;
        const { recovery: restoredRecovery, knownIssuedTokenId } = restored;
        const recovery: TokenCreateRecovery = {
          ...restoredRecovery,
          ownerId: currentTokenGuardOwnerId(),
        };
        tokenGuardLockOperationAttemptRef.current = recovery.attemptId;
        if (!persistTokenCreateGuard(recovery, knownIssuedTokenId, {
          allowOwnershipTakeover: true,
        })) {
          setTokenRecoveryOwnership("failed");
          return;
        }
        setTokenCreateRecovery(recovery);
        try {
          if (!tokenRecoveryEnvironmentCanPoll(document.visibilityState, navigator.onLine)) {
            setTokenRecoveryNextCheckAt(Date.now());
            return;
          }
          if (knownIssuedTokenId === null) {
            await reconcileTokenCreateRecovery(recovery);
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
              stopAutomaticTokenRecovery();
              setToast({
                message: tokenCanBeRevoked(token)
                  ? `Token ${token.tokenPrefix} was issued, but its one-time secret is gone. Revoke the unusable token when convenient; the rest of the app remains available.`
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
            stopAutomaticTokenRecovery();
          } catch (error) {
            if (!current || controller.signal.aborted) return;
            if (!requireTokenGuardOwnership(recovery)) return;
            const retryRecovery: TokenCreateRecovery = {
              ...recovery,
              reconciliationError: `The saved issued token could not be read directly: ${errorMessage(error)}`,
            };
            setTokenCreateRecovery(retryRecovery);
            await reconcileTokenCreateRecovery(retryRecovery);
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
      setTokenRecoveryOwnership("failed");
      setTokenRecoveryOwnershipError(`Cross-tab recovery lock failed: ${errorMessage(error)}`);
    });
    return stop;
  });

  useEffect(
    () => reconcilePersistedTokenCreate(),
    [client, normalizedApiBaseUrl, tokenRecoveryAcquireGeneration],
  );

  const runScheduledTokenRecovery = useEffectEvent(() => {
    if (tokenRecoveryOwnership !== "owned" || tokenRecoveryCheckingRef.current) return;
    const recovery = tokenCreateRecoveryRef.current;
    if (recovery !== null) {
      void reconcileTokenCreateRecovery(recovery);
      return;
    }
    const unreadableRecovery = unreadableTokenCreateRecoveryRef.current;
    if (unreadableRecovery !== null) {
      void reconcileUnreadableTokenCreateRecovery(unreadableRecovery);
    }
  });

  const resumeScheduledTokenRecovery = useEffectEvent(() => {
    if (tokenRecoveryOwnership !== "owned" || tokenRecoveryNextCheckAt === null) return;
    setTokenRecoveryNextCheckAt(Date.now());
  });

  useEffect(() => {
    if (
      tokenRecoveryNextCheckAt === null
      || tokenRecoveryOwnership !== "owned"
      || !tokenRecoveryEnvironmentReady
    ) return;
    const timeout = window.setTimeout(
      () => runScheduledTokenRecovery(),
      Math.max(0, tokenRecoveryNextCheckAt - Date.now()),
    );
    return () => window.clearTimeout(timeout);
  }, [
    tokenRecoveryEnvironmentReady,
    tokenRecoveryNextCheckAt,
    tokenRecoveryOwnership,
  ]);

  useEffect(() => {
    if (!tokenRecoveryEnvironmentReady) return;
    resumeScheduledTokenRecovery();
  }, [tokenRecoveryEnvironmentReady]);

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
  const hecEnabled = bootstrap !== null && supportsServerFeature(
    bootstrap,
    ServerFeature.SERVER_FEATURE_HEC_INGESTION,
  );
  const creatingHECToken = tokenUsesHEC(tokenPurpose);
  const editingHECToken = tokenEditTarget !== null && tokenUsesHEC(tokenEditTarget.purpose);
  const editingNativeToken = tokenEditTarget?.purpose
    === IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_NATIVE_COLLECTOR;
  const tokenScopeChanged = tokenEditTarget !== null
    && !hasSameStrings(tokenIndexes, tokenEditTarget.constraints?.allowedIndexNames ?? []);
  const tokenBindingChanged = tokenEditTarget !== null
    && editingNativeToken
    && tokenEditTarget.constraints?.boundCollectorId === undefined
    && validCollectorId(tokenCollectorId);
  const tokenHasUnavailableScope = [...tokenIndexes].some((name) => !ingestibleIndexNames.has(name));
  const tokenScopeInvalid = tokenScopeChanged && tokenHasUnavailableScope;
  const tokenCollectorIdInvalid = !validCollectorId(tokenCollectorId);
  const tokenHECDefaultIndexInvalid = tokenHECDefaultIndex.length > 0
    && !tokenIndexes.has(tokenHECDefaultIndex);
  const tokenHECMetadataInvalid = !validHECMetadataDefault(tokenHECDefaultHost)
    || !validHECMetadataDefault(tokenHECDefaultSource)
    || !validHECMetadataDefault(tokenHECDefaultSourcetype);
  const tokenHECProfileInvalid = tokenHECDefaultIndexInvalid || tokenHECMetadataInvalid;
  const tokenHECProfile = hecProfileFromForm({
    defaultIndexName: tokenHECDefaultIndex,
    defaultHost: tokenHECDefaultHost,
    defaultSource: tokenHECDefaultSource,
    defaultSourcetype: tokenHECDefaultSourcetype,
    indexerAcknowledgment: tokenHECIndexerAcknowledgment,
  });
  const tokenHECProfileChanged = editingHECToken
    && !hecProfilesMatch(tokenHECProfile, tokenEditTarget.hecProfile);
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
      : unreadableTokenCreateRecovery !== null
        ? `A damaged token safety record is being reconciled. Token generation remains paused, but the rest of Administration is available.${unreadableTokenCreateRecovery.reconciliationError === null ? "" : ` Latest check: ${unreadableTokenCreateRecovery.reconciliationError}`}`
      : tokenCreateRecovery !== null
        ? `Open Splunk is checking whether token “${tokenCreateRecovery.definition.name}” was created. Token generation remains paused, but the rest of Administration is available.${tokenCreateRecovery.reconciliationError === null ? "" : ` Latest check: ${tokenCreateRecovery.reconciliationError}`}`
      : issuedToken !== null
        ? tokenNavigationProtectionActive
          ? "Save or revoke the visible one-time token before generating another."
          : `Token ${issuedToken.tokenPrefix} was issued without a recoverable secret. Review or revoke it before generating another.`
      : tokenGuardActive
        ? "A token create request is currently in progress."
        : null;
  const tokenCreateScopeInvalid = tokenScopeSource === "unavailable"
    || tokenIndexes.size === 0
    || tokenHasUnavailableScope
    || (!creatingHECToken && tokenCollectorIdInvalid)
    || (creatingHECToken && !hecEnabled)
    || (creatingHECToken && tokenHECProfileInvalid)
    || tokenCreationBlockReason !== null;
  const indexDefinition = indexEditTarget?.definition;
  const currentIndexPolicyForm = indexPolicyFormFromDefinition(indexDefinition);
  const indexHasChanges = indexDefinition !== undefined && (
    (indexDisplayName.trim() || indexDefinition.name) !== indexDefinition.displayName
    || indexDescription !== (indexDefinition.description ?? "")
    || retention !== retentionFormValue(indexDefinition.retentionPeriod?.seconds)
    || indexIngestionAccess !== indexDefinition.ingestionAccess
    || indexSearchAccess !== indexDefinition.searchAccess
    || indexPolicyForm.defaultSourcetype !== currentIndexPolicyForm.defaultSourcetype
    || indexPolicyForm.maxEventBytes !== currentIndexPolicyForm.maxEventBytes
    || indexPolicyForm.maxFieldCount !== currentIndexPolicyForm.maxFieldCount
    || indexPolicyForm.maxNestingDepth !== currentIndexPolicyForm.maxNestingDepth
    || indexPolicyForm.maximumFutureSkewSeconds
      !== currentIndexPolicyForm.maximumFutureSkewSeconds
    || indexPolicyForm.maximumEventAgeSeconds !== currentIndexPolicyForm.maximumEventAgeSeconds
    || indexPolicyForm.maxEventsPerSecond !== currentIndexPolicyForm.maxEventsPerSecond
    || indexPolicyForm.maxUncompressedBytesPerSecond
      !== currentIndexPolicyForm.maxUncompressedBytesPerSecond
  );
  const tokenHasChanges = tokenEditTarget !== null && (
    tokenName.trim() !== tokenEditTarget.name
    || tokenDescription !== (tokenEditTarget.description ?? "")
    || tokenScopeChanged
    || tokenBindingChanged
    || tokenHECProfileChanged
    || tokenExpiration !== dateTimeLocalValue(tokenEditTarget.expiresAt)
    || tokenPolicyForm.allowedHostRegexes
      !== tokenPolicyFormFromToken(tokenEditTarget).allowedHostRegexes
    || tokenPolicyForm.allowedSourceRegexes
      !== tokenPolicyFormFromToken(tokenEditTarget).allowedSourceRegexes
    || tokenPolicyForm.maxEventsPerSecond
      !== tokenPolicyFormFromToken(tokenEditTarget).maxEventsPerSecond
    || tokenPolicyForm.maxUncompressedBytesPerSecond
      !== tokenPolicyFormFromToken(tokenEditTarget).maxUncompressedBytesPerSecond
  );
  const activeIndexes = indexes.filter((index) => index.state === IndexState.INDEX_STATE_ACTIVE).length;
  const activeTokens = tokens.filter((token) => token.state === IngestionTokenState.INGESTION_TOKEN_STATE_ACTIVE).length;
  const tokenRevealOpen = issuedToken !== null;
  const tokenRecoveryOpen = tokenCreateRecovery !== null || unreadableTokenCreateRecovery !== null;
  const tokenResolutionOpen = tokenRevealOpen || tokenRecoveryOpen;
  const tokenDialogHardBlocked = tokenCreateDialogRequiresExclusiveAttention({
    createRequestInProgress: busy === "create-token",
    plaintextToken: tokenSecret,
  });
  const recoveryAuthenticationError = (
    tokenCreateRecovery?.reconciliationError
    ?? unreadableTokenCreateRecovery?.reconciliationError
    ?? ""
  );
  const recoveryNeedsAuthentication = /auth|sign[ -]?in|administrator token/i.test(
    recoveryAuthenticationError,
  );
  const issuedHECCurlExample = issuedToken === null || !hecEnabled
    ? null
    : hecCurlExample(
        normalizedApiBaseUrl,
        issuedToken.purpose,
        tokenSecret,
        issuedToken.hecProfile?.defaultIndexName
          ?? issuedToken.constraints?.allowedIndexNames[0]
          ?? null,
      );
  const {
    knowledge: knowledgeFeatureAdvertised,
    lookupManagement: lookupFeatureAdvertised,
    quarantine: quarantineFeatureAdvertised,
  } = backendKnowledgeCapabilities(bootstrap?.features ?? null);
  const knowledgeApps = knowledgeFeatureAdvertised && bootstrap !== null
    ? knowledgeManagerAppOptionsFromBootstrap(bootstrap.apps)
    : null;
  const knowledgeAdvertised = knowledgeFeatureAdvertised && knowledgeApps !== null;
  const lookupAdvertised = lookupFeatureAdvertised && knowledgeApps !== null;
  const navigationItems = backendAdminNavigation(knowledgeAdvertised, lookupAdvertised);
  useEffect(() => {
    function restoreSectionFromHistory() {
      const next = resolveAdminSection(
        new URL(window.location.href).searchParams.get(ADMIN_SECTION_QUERY_PARAMETER),
        BACKEND_ADMIN_SECTIONS,
        "overview",
      );
      setSection(next);
    }
    restoreSectionFromHistory();
    window.addEventListener("popstate", restoreSectionFromHistory);
    return () => window.removeEventListener("popstate", restoreSectionFromHistory);
  }, []);
  useEffect(() => {
    if (bootstrap === null && bootstrapError === null) return;
    if (
      (section === "knowledge" && !knowledgeAdvertised)
      || (section === "lookups" && !lookupAdvertised)
    ) {
      setSection("overview");
      window.history.replaceState(null, "", adminSectionPath(window.location.href, "overview"));
    }
  }, [bootstrap, bootstrapError, knowledgeAdvertised, lookupAdvertised, section]);
  function navigateSection(next: AdminSection) {
    if (next === section) return;
    setSection(next);
    window.history.pushState(null, "", adminSectionPath(window.location.href, next));
  }
  const hasAvailableAdminRoute = indexState === "available" || tokenState === "available";
  const adminRoutesLoading = indexState === "loading" || tokenState === "loading";
  const connectionStatus = bootstrap !== null
    ? {
        tone: "healthy",
        title: "API connected",
        detail: bootstrap.build?.sourceRevision
          ? `Revision ${bootstrap.build.sourceRevision.slice(0, 12)}`
          : "System bootstrap connected",
      }
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
  const tokenCreateDisabledReason = tokenCreationBlockReason
    ?? (ingestibleTokenScopes.length === 0
      ? "No active, ingestion-enabled index is currently available for a new token."
      : null);
  function openTokenRecoveryDialog() {
    if (!tokenResolutionOpen) return;
    setModal("create-token");
  }
  function checkOrRetryTokenRecovery() {
    if (tokenRecoveryOwnership === "owned") {
      const recovery = tokenCreateRecoveryRef.current;
      if (recovery !== null) {
        void reconcileTokenCreateRecovery(recovery);
        return;
      }
      const unreadableRecovery = unreadableTokenCreateRecoveryRef.current;
      if (unreadableRecovery !== null) {
        void reconcileUnreadableTokenCreateRecovery(unreadableRecovery);
      }
      return;
    }
    setTokenRecoveryAcquireGeneration((current) => current + 1);
  }
  const primaryAction = section === "indexes" && indexState === "available"
    ? <button className="suite-button suite-button--primary" type="button" onClick={openIndexDialog}><AppIcon name="plus" size="sm" /> Create index</button>
    : section === "collectors" && tokenState === "available"
      ? <button className="suite-button suite-button--primary" type="button" onClick={openTokenDialog} disabled={tokenCreateDisabledReason !== null} aria-describedby={tokenCreateDisabledReason === null ? undefined : "ingestion-token-create-disabled-reason"}>Generate token</button>
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
        <BackendResourceState kind="loading" title="Loading administration" message="Reading server capabilities and resources…" />
      </div>
    );
  }

  return (
    <div className="suite-page admin-page">
      <PageHeading
        eyebrow="SYSTEM"
        title="Administration"
        description="Manage apps, indexes, collectors, and ingestion credentials exposed by the connected server."
        actions={primaryAction}
      />

      <div className="admin-mobile-section-picker">
        <label htmlFor="admin-section">Administration section</label>
        <select id="admin-section" value={section} onChange={(event) => navigateSection(event.target.value as AdminSection)}>
          {navigationItems.map((item) => <option value={item.key} key={item.key}>{item.label}</option>)}
        </select>
      </div>

      <div className="admin-layout">
        <aside className="admin-sidebar" aria-label="Administration navigation">
          <span className="admin-sidebar-label">SETTINGS</span>
          {navigationItems.map((item) => (
            <button className={section === item.key ? "active" : undefined} type="button" aria-current={section === item.key ? "page" : undefined} onClick={() => navigateSection(item.key)} key={item.key}>
              <i aria-hidden="true"><AppIcon name={adminSectionIcon(item.key)} size="md" /></i><span><strong>{item.label}</strong><small>{item.detail}</small></span><b aria-hidden="true"><AppIcon name="chevron-right" size="xs" /></b>
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

        <section className="admin-content">
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
              onNavigate={navigateSection}
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
              onDelete={(index) => void openIndexDeleteDialog(index)}
            />
          ) : null}
          {section === "apps" ? (
            <AppsAdminPanel apiBaseUrl={apiBaseUrl} bootstrap={bootstrap} />
          ) : null}
          {section === "collector-fleet" ? (
            <CollectorFleetPanel apiBaseUrl={apiBaseUrl} bootstrap={bootstrap} />
          ) : null}
          {section === "knowledge" && knowledgeAdvertised && bootstrap !== null && knowledgeApps !== null ? (
            <KnowledgeManagerGate
              apiBaseUrl={apiBaseUrl}
              apps={knowledgeApps}
              initialAppId={bootstrap.selectedAppId}
              maximumPageSize={bootstrap.limits.maximumPageSize}
              quarantineAvailable={quarantineFeatureAdvertised}
            />
          ) : null}
          {section === "lookups" && lookupAdvertised && bootstrap !== null && knowledgeApps !== null ? (
            <LookupManagerGate
              apiBaseUrl={apiBaseUrl}
              apps={knowledgeApps}
              initialAppId={bootstrap.selectedAppId}
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
              onEdit={(token) => void openTokenEditor(token)}
              onReload={load}
              onLoadMore={() => void loadMoreTokens()}
              onRevoke={setRevokeTarget}
              onSetEnabled={(token, enabled) => void setTokenEnabled(token, enabled)}
              canCreate={ingestibleTokenScopes.length > 0 && tokenCreationBlockReason === null}
              createBlockReason={tokenCreateDisabledReason}
              recoveryActionLabel={tokenResolutionOpen ? "Resolve token creation" : null}
              onResolveRecovery={openTokenRecoveryDialog}
              indexState={indexState}
              indexError={indexError}
              scopeSource={tokenScopeSource}
            />
          ) : null}
          {section === "access" ? (
            <BackendResourceState
              kind="unavailable"
              title="Users and access are not exposed"
              message="This backend does not register an authentication or role-administration API. No preview users are shown in backend mode."
            />
          ) : null}
          {section === "server" ? (
            <BackendServerSettings
              bootstrap={bootstrap}
              error={bootstrapError}
              hecState={hecState}
              hecSnapshot={hecSnapshot}
              hecError={hecError}
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
            <IndexPolicyFields
              idPrefix="new-index"
              value={indexPolicyForm}
              onChange={(next) => setIndexPolicyForm((current) => ({ ...current, ...next }))}
            />
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
            <IndexPolicyFields
              idPrefix="edit-index"
              value={indexPolicyForm}
              onChange={(next) => setIndexPolicyForm((current) => ({ ...current, ...next }))}
            />
          </form>
        </Modal>
      ) : null}

      {indexDeleteTarget?.definition !== undefined ? (
        <Modal
          title={`Delete index ${indexDeleteTarget.definition.name}`}
          subtitle="This version-checked operation removes the index definition and can also destroy its stored events."
          onClose={() => {
            if (busy !== null) return;
            setIndexDeleteTarget(null);
            setIndexDeleteConfirmation("");
          }}
          footer={<><button className="button secondary" type="button" onClick={() => { setIndexDeleteTarget(null); setIndexDeleteConfirmation(""); }} disabled={busy !== null}>Cancel</button><button className="button danger" type="submit" form="delete-index-form" disabled={busy !== null || indexDeleteConfirmation !== indexDeleteTarget.definition.name}>{busy === `delete-index-${indexDeleteTarget.indexId}` ? "Deleting…" : "Delete index"}</button></>}
        >
          <form className="admin-form" id="delete-index-form" onSubmit={(event) => void deleteIndex(event)}>
            <div className="access-mode-notice" role="alert"><span>!</span><div><strong>Deletion cannot be undone from the browser</strong><p>The server will reject this request if index version {indexDeleteTarget.version.toLocaleString()} is no longer current.</p></div></div>
            <label htmlFor="delete-index-mode"><span>Stored data</span><select id="delete-index-mode" value={indexDeleteMode} onChange={(event) => setIndexDeleteMode(Number(event.target.value) as IndexDataDeletionMode)}><option value={IndexDataDeletionMode.INDEX_DATA_DELETION_MODE_KEEP_DATA}>Keep physical data</option><option value={IndexDataDeletionMode.INDEX_DATA_DELETION_MODE_DELETE_DATA}>Permanently delete physical data</option></select><small>{indexDeleteMode === IndexDataDeletionMode.INDEX_DATA_DELETION_MODE_DELETE_DATA ? "The backend may run physical deletion asynchronously and return an operation ID." : "Only the control-plane index record is deleted; stored data is preserved."}</small></label>
            <label htmlFor="delete-index-confirmation"><span>Type <code>{indexDeleteTarget.definition.name}</code> to confirm</span><input id="delete-index-confirmation" value={indexDeleteConfirmation} onChange={(event) => setIndexDeleteConfirmation(event.target.value)} autoComplete="off" spellCheck={false} /><small>The backend also checks this exact name before accepting the operation.</small></label>
          </form>
        </Modal>
      ) : null}

      {modal === "create-token" ? (
        <Modal
          title={unreadableTokenCreateRecovery !== null
            ? "Resolve damaged token recovery"
            : tokenRecoveryOpen
            ? "Resolve token creation"
            : tokenRevealOpen
              ? "Save this token now"
              : "Generate ingestion token"}
          subtitle={tokenRecoveryOpen
            ? "New token creation is paused while Open Splunk checks; the rest of the app remains available."
            : tokenRevealOpen
              ? "The server reveals this plaintext credential only once."
              : "Scope a new credential to one or more ingestible indexes."}
          dismissible={!tokenDialogHardBlocked}
          initialFocus={tokenRecoveryOpen
            ? "#reconcile-token-create"
            : tokenRevealOpen
            ? tokenSecret === null || tokenSecret.length === 0
              ? busy === null ? "#revoke-issued-token" : undefined
              : "#copy-issued-token"
            : "#new-token-name"}
          onClose={() => {
            if (tokenDialogHardBlocked) return;
            if (!tokenResolutionOpen) setTokenSecret(null);
            setModal(null);
          }}
          footer={tokenRecoveryOpen
            ? (
              <>
                <button className="button secondary" type="button" onClick={() => setModal(null)}>
                  Close
                </button>
                {recoveryNeedsAuthentication ? (
                  <Link className="button secondary" href="/signin/">Sign in</Link>
                ) : null}
                <button
                  id="reconcile-token-create"
                  className="button primary"
                  type="button"
                  disabled={tokenRecoveryChecking || tokenRecoveryOwnership === "acquiring" || tokenCreateLockAvailable !== true}
                  onClick={checkOrRetryTokenRecovery}
                >
                  {tokenRecoveryChecking
                    ? "Checking…"
                    : tokenRecoveryOwnership === "owned"
                      ? "Check now"
                      : tokenRecoveryOwnership === "acquiring"
                        ? "Acquiring…"
                        : "Try again"}
                </button>
              </>
            )
            : !tokenRevealOpen
            ? <><button className="button secondary" type="button" onClick={() => setModal(null)} disabled={busy !== null}>Cancel</button><button className="button primary" type="submit" form="create-token-form" disabled={busy !== null || tokenName.trim().length === 0 || tokenCreateScopeInvalid}>{busy === "create-token" ? "Generating…" : "Generate token"}</button></>
            : (
              <>
                <button id="revoke-issued-token" className="button danger" type="button" disabled={busy !== null || tokenRecoveryOwnership !== "owned" || !tokenCanBeRevoked(issuedToken)} onClick={() => void revokeIssuedToken()}>
                  {busy === `issued-token-${issuedToken.ingestionTokenId}` ? "Revoking…" : "Revoke unused token"}
                </button>
                <button
                  className="button primary"
                  type="button"
                  disabled={busy !== null || tokenRecoveryOwnership !== "owned" || (
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
                  <strong>{unreadableTokenCreateRecovery === null
                    ? "Open Splunk could not confirm token creation"
                    : "The saved recovery record is damaged"}</strong>
                  <p>{unreadableTokenCreateRecovery === null && tokenCreateRecovery !== null
                    ? `We couldn’t confirm whether the server created token “${tokenCreateRecovery.definition.name}.” New token creation is paused while Open Splunk checks. You can keep using the rest of the app.`
                    : "The record no longer contains enough attribution data to identify one create request. Open Splunk must conservatively review every nonterminal ingestion token. You can keep using the rest of the app."}</p>
                </div>
              </div>
              {tokenRecoveryOwnershipError === null ? null : (
                <div className="access-mode-notice" role="alert">
                  <span>!</span>
                  <div><strong>Recovery ownership: {tokenRecoveryOwnership}</strong><p>{tokenRecoveryOwnershipError}</p></div>
                </div>
              )}
              {(tokenCreateRecovery?.reconciliationError ?? unreadableTokenCreateRecovery?.reconciliationError) === null ? null : (
                <div className="access-mode-notice" role="alert">
                  <span>!</span>
                  <div><strong>Latest check could not finish</strong><p>{tokenCreateRecovery?.reconciliationError ?? unreadableTokenCreateRecovery?.reconciliationError}</p></div>
                </div>
              )}
              <div className="token-recovery-summary">
                <strong>{tokenCreateRecovery === null
                  ? "Damaged-record safety review"
                  : `Checking token “${tokenCreateRecovery.definition.name}”`}</strong>
                {tokenCreateRecovery === null ? (
                  <p>All nonterminal tokens listed below must become revoked or expired before two complete zero-result snapshots can safely remove the damaged guard.</p>
                ) : (
                  <p>Request dispatched {tokenCreateRecovery.definition.dispatchedServerTimeMs === null
                    ? "at an unknown server time"
                    : formatDate(new Date(tokenCreateRecovery.definition.dispatchedServerTimeMs))}. Two complete zero-result snapshots after the safety window are required before Open Splunk concludes that no token was created.</p>
                )}
                <p>
                  Last successful check: {tokenRecoveryLastCheckedAt === null ? "not yet" : formatDate(new Date(tokenRecoveryLastCheckedAt))}.
                  {" "}Next automatic check: {!tokenRecoveryEnvironmentReady
                    ? "paused while this tab is hidden or offline"
                    : tokenRecoveryNextCheckAt === null
                      ? "not scheduled"
                      : formatDate(new Date(tokenRecoveryNextCheckAt))}.
                </p>
              </div>
              {tokenCreateRecovery === null || tokenCreateRecovery.candidates.length === 0 ? null : (
                <ul className="token-recovery-list" aria-label="Possible tokens created by the uncertain request">
                  {tokenCreateRecovery.candidates.map((candidate) => (
                    <li key={candidate.ingestionTokenId}>
                      <div><strong>{candidate.name}</strong><code>{candidate.tokenPrefix}</code><small>Created {formatDate(candidate.createdAt)}</small>{tokenFallsWithinCreateAttributionWindow(candidate, tokenCreateRecovery.definition) ? null : <small className="table-warning-detail">Outside expected request window · manual review required</small>}</div>
                      <span className={`status-label status-label--${statusClass(tokenStateLabel(candidate.state))}`}><i />{tokenStateLabel(candidate.state)}</span>
                      <button
                        className="button danger"
                        type="button"
                        disabled={busy !== null || tokenRecoveryOwnership !== "owned" || !tokenCanBeRevoked(candidate)}
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
              {unreadableTokenCreateRecovery === null || unreadableTokenCreateRecovery.candidates.length === 0 ? null : (
                <ul className="token-recovery-list" aria-label="Nonterminal tokens blocking damaged recovery">
                  {unreadableTokenCreateRecovery.candidates.map((candidate) => (
                    <li key={candidate.ingestionTokenId}>
                      <div><strong>{candidate.name}</strong><code>{candidate.tokenPrefix}</code><small>Created {formatDate(candidate.createdAt)}</small></div>
                      <span className={`status-label status-label--${statusClass(tokenStateLabel(candidate.state))}`}><i />{tokenStateLabel(candidate.state)}</span>
                      <button
                        className="button danger"
                        type="button"
                        disabled={busy !== null || tokenRecoveryOwnership !== "owned" || !tokenCanBeRevoked(candidate)}
                        onClick={() => void revokeUnreadableTokenCandidate(candidate)}
                      >
                        {busy === `recover-damaged-token-${candidate.ingestionTokenId}` ? "Revoking…" : "Revoke token"}
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
              <label htmlFor="new-token-purpose"><span>Purpose</span><select id="new-token-purpose" value={tokenPurpose} onChange={(event) => { const next = Number(event.target.value) as IngestionTokenPurpose; setTokenPurpose(next); if (tokenUsesHEC(next)) setTokenCollectorId(""); }}><option value={IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_NATIVE_COLLECTOR}>Native collector</option><option value={IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_HEC} disabled={!hecEnabled}>HTTP Event Collector (HEC){hecEnabled ? "" : " — disabled on server"}</option></select><small>Purpose is an immutable transport boundary. HEC credentials can only be created while the server advertises HEC ingestion.</small></label>
              {creatingHECToken ? null : (
                <>
                  <label htmlFor="new-token-collector-id"><span>Collector ID</span><input id="new-token-collector-id" value={tokenCollectorId} onChange={(event) => setTokenCollectorId(event.target.value)} placeholder="Paste the collector’s stable ID" autoComplete="off" aria-invalid={tokenCollectorIdInvalid} /><small>Run <code>open-splunk-collector identity -config PATH</code> against the collector’s final state directory, then paste the printed ID. The binding cannot be changed after creation.</small></label>
                  {tokenCollectorIdInvalid ? <div className="access-mode-notice" role="alert"><span>!</span><div><strong>Collector ID is invalid</strong><p>Use 1–128 ASCII characters: start with a letter or number, then use letters, numbers, dot, underscore, colon, or hyphen.</p></div></div> : null}
                </>
              )}
              <TokenScopePicker idPrefix="new-token" options={tokenScopeOptions} selected={tokenIndexes} onChange={setTokenIndexes} disabled={tokenScopeSource === "unavailable"} />
              <TokenPolicyFields
                idPrefix="new-token"
                value={tokenPolicyForm}
                onChange={(next) => setTokenPolicyForm((current) => ({ ...current, ...next }))}
              />
              {creatingHECToken ? (
                <HECTokenProfileFields
                  idPrefix="new-token"
                  selectedIndexes={tokenIndexes}
                  defaultIndex={tokenHECDefaultIndex}
                  onDefaultIndexChange={setTokenHECDefaultIndex}
                  defaultHost={tokenHECDefaultHost}
                  onDefaultHostChange={setTokenHECDefaultHost}
                  defaultSource={tokenHECDefaultSource}
                  onDefaultSourceChange={setTokenHECDefaultSource}
                  defaultSourcetype={tokenHECDefaultSourcetype}
                  onDefaultSourcetypeChange={setTokenHECDefaultSourcetype}
                  indexerAcknowledgment={tokenHECIndexerAcknowledgment}
                  onIndexerAcknowledgmentChange={setTokenHECIndexerAcknowledgment}
                />
              ) : null}
              {creatingHECToken && tokenHECProfileInvalid ? <div className="access-mode-notice" role="alert"><span>!</span><div><strong>HEC defaults are invalid</strong><p>The default index must remain in the allowed scope. Metadata defaults must contain 1–255 UTF-8 bytes without control characters or surrounding ASCII whitespace.</p></div></div> : null}
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
                  ? "The server created this token, but its plaintext secret is no longer available. Revoke the unusable token before generating another; you may close this dialog and keep using the app."
                  : `This token was identified without its plaintext secret and is confirmed ${tokenStateLabel(issuedToken.state).toLowerCase()}. It is no longer usable.`}</p>
              ) : (
                <>
                  <p>Copy this credential now. Closing, reloading, or navigating away cannot reveal it again.</p>
                  <div><code>{tokenSecret}</code><button id="copy-issued-token" type="button" onClick={() => void navigator.clipboard.writeText(tokenSecret).then(() => setToast({ message: "Token copied to the clipboard.", kind: "success" }), () => setToast({ message: "Copy failed. Select the token text and copy it manually.", kind: "warning" }))}>Copy token</button></div>
                  {issuedHECCurlExample === null ? null : (
                    <section className="token-recovery-summary" aria-label="HEC curl example" style={{ gridColumn: "1 / -1" }}>
                      <strong>Send a test HEC event</strong>
                      <p>This command contains the one-time credential and disappears permanently when this dialog is dismissed.</p>
                      <pre style={{ overflowWrap: "anywhere", whiteSpace: "pre-wrap" }}><code>{issuedHECCurlExample}</code></pre>
                      <button type="button" onClick={() => void navigator.clipboard.writeText(issuedHECCurlExample).then(() => setToast({ message: "HEC curl example copied to the clipboard.", kind: "success" }), () => setToast({ message: "Copy failed. Select the curl command and copy it manually.", kind: "warning" }))}>Copy curl example</button>
                    </section>
                  )}
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
          footer={<><button className="button secondary" type="button" onClick={() => { setTokenEditTarget(null); setModal(null); }} disabled={busy !== null}>Cancel</button><button className="button primary" type="submit" form="edit-token-form" disabled={busy !== null || !tokenHasChanges || tokenName.trim().length === 0 || tokenIndexes.size === 0 || tokenScopeInvalid || (editingHECToken ? tokenHECProfileInvalid : editingNativeToken ? tokenCollectorId.length > 0 && tokenCollectorIdInvalid : true)}>{busy === `update-token-${tokenEditTarget.ingestionTokenId}` ? "Saving…" : "Save changes"}</button></>}
        >
          <form className="admin-form" id="edit-token-form" onSubmit={(event) => void updateToken(event)}>
            <label htmlFor="edit-token-name"><span>Token name</span><input id="edit-token-name" value={tokenName} onChange={(event) => setTokenName(event.target.value)} autoComplete="off" /></label>
            <label htmlFor="edit-token-description"><span>Description <small>(optional)</small></span><input id="edit-token-description" value={tokenDescription} onChange={(event) => setTokenDescription(event.target.value)} placeholder="Production collector credential" /></label>
            <div className="access-mode-notice" role="note"><span>i</span><div><strong>{tokenPurposeLabel(tokenEditTarget.purpose)} purpose</strong><p>The token purpose is immutable. {editingHECToken ? "Indexer acknowledgment mode is also fixed at creation." : editingNativeToken ? "This credential can authorize only the native collector transport." : "The server returned an unknown purpose, so transport-specific settings are unavailable."}</p></div></div>
            {editingNativeToken ? <label htmlFor="edit-token-collector-id"><span>Collector ID</span><input id="edit-token-collector-id" value={tokenCollectorId} onChange={(event) => setTokenCollectorId(event.target.value)} readOnly={tokenEditTarget.constraints?.boundCollectorId !== undefined} placeholder="Bind this legacy token once" autoComplete="off" aria-invalid={tokenCollectorId.length > 0 && tokenCollectorIdInvalid} /><small>{tokenEditTarget.constraints?.boundCollectorId === undefined ? "This upgraded legacy token cannot use native gRPC until it is bound. Binding is one-way." : "This security binding is immutable. Rotate the token to use a different collector ID."}</small></label> : null}
            <TokenScopePicker idPrefix="edit-token" options={tokenScopeOptions} selected={tokenIndexes} onChange={setTokenIndexes} disabled={tokenScopeSource === "unavailable"} />
            <TokenPolicyFields
              idPrefix="edit-token"
              value={tokenPolicyForm}
              onChange={(next) => setTokenPolicyForm((current) => ({ ...current, ...next }))}
            />
            {editingHECToken && !hecEnabled ? <div className="access-mode-notice" role="note"><span>i</span><div><strong>HEC ingestion is disabled</strong><p>This stored token can be maintained or revoked, but it cannot reach an active HEC data-plane route until the server advertises HEC ingestion again.</p></div></div> : null}
            {editingHECToken ? (
              <HECTokenProfileFields
                idPrefix="edit-token"
                selectedIndexes={tokenIndexes}
                defaultIndex={tokenHECDefaultIndex}
                onDefaultIndexChange={setTokenHECDefaultIndex}
                defaultHost={tokenHECDefaultHost}
                onDefaultHostChange={setTokenHECDefaultHost}
                defaultSource={tokenHECDefaultSource}
                onDefaultSourceChange={setTokenHECDefaultSource}
                defaultSourcetype={tokenHECDefaultSourcetype}
                onDefaultSourcetypeChange={setTokenHECDefaultSourcetype}
                indexerAcknowledgment={tokenHECIndexerAcknowledgment}
                onIndexerAcknowledgmentChange={setTokenHECIndexerAcknowledgment}
                acknowledgmentReadOnly
              />
            ) : null}
            {editingHECToken && tokenHECProfileInvalid ? <div className="access-mode-notice" role="alert"><span>!</span><div><strong>HEC defaults are invalid</strong><p>The default index must remain in the allowed scope. Metadata defaults must contain 1–255 UTF-8 bytes without control characters or surrounding ASCII whitespace.</p></div></div> : null}
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
          subtitle="Clients using this credential will no longer be able to ingest data."
          onClose={() => busy === null && setRevokeTarget(null)}
          footer={<><button className="button secondary" type="button" onClick={() => setRevokeTarget(null)} disabled={busy !== null}>Keep token</button><button className="button danger" type="button" disabled={busy !== null || !tokenCanBeRevoked(revokeTarget)} onClick={() => void revokeToken(revokeTarget)}>{busy === `token-${revokeTarget.ingestionTokenId}` ? "Revoking…" : tokenCanBeRevoked(revokeTarget) ? "Revoke token" : `Token is ${tokenStateLabel(revokeTarget.state).toLowerCase()}`}</button></>}
        >
          <div className="access-mode-notice" role="note"><span>!</span><div><strong>This action cannot be undone</strong><p>Revoke <code>{revokeTarget.name}</code> ({revokeTarget.tokenPrefix}) scoped to {revokeTarget.constraints?.allowedIndexNames.join(", ") || "its configured indexes"}.</p></div></div>
        </Modal>
      ) : null}

      {toast === null ? null : <output className={`toast toast-${toast.kind}`}><span aria-hidden="true"><AppIcon name={toast.kind === "success" ? "check" : "warning"} size="sm" /></span><strong>{toast.message}</strong><button type="button" aria-label="Dismiss notification" onClick={() => setToast(null)}><AppIcon name="close" size="md" /></button></output>}
    </div>
  );
}

function IndexPolicyFields({
  idPrefix,
  value,
  onChange,
}: {
  idPrefix: string;
  value: IndexPolicyForm;
  onChange: (value: Partial<IndexPolicyForm>) => void;
}) {
  return (
    <fieldset>
      <legend>Ingestion policy <small>(optional)</small></legend>
      <div className="admin-policy-grid">
        <label htmlFor={`${idPrefix}-default-sourcetype`}><span>Default sourcetype</span><input id={`${idPrefix}-default-sourcetype`} value={value.defaultSourcetype} onChange={(event) => onChange({ defaultSourcetype: event.target.value })} maxLength={255} placeholder="_json" /><small>Applied when an admitted event does not provide a sourcetype.</small></label>
        <label htmlFor={`${idPrefix}-max-event-bytes`}><span>Maximum event bytes</span><input id={`${idPrefix}-max-event-bytes`} type="number" min="0" max={INDEX_MAX_EVENT_BYTES.toString()} step="1" value={value.maxEventBytes} onChange={(event) => onChange({ maxEventBytes: event.target.value })} placeholder="Inherit server limit" /><small>Zero or blank inherits the server limit; maximum 1 MiB.</small></label>
        <label htmlFor={`${idPrefix}-max-field-count`}><span>Maximum field count</span><input id={`${idPrefix}-max-field-count`} type="number" min="0" max={INDEX_MAX_FIELD_COUNT} step="1" value={value.maxFieldCount} onChange={(event) => onChange({ maxFieldCount: event.target.value })} placeholder="Inherit server limit" /><small>Zero or blank inherits; maximum {INDEX_MAX_FIELD_COUNT.toLocaleString()} fields.</small></label>
        <label htmlFor={`${idPrefix}-max-nesting-depth`}><span>Maximum nesting depth</span><input id={`${idPrefix}-max-nesting-depth`} type="number" min="0" max={INDEX_MAX_NESTING_DEPTH} step="1" value={value.maxNestingDepth} onChange={(event) => onChange({ maxNestingDepth: event.target.value })} placeholder="Inherit server limit" /><small>Zero or blank inherits; maximum {INDEX_MAX_NESTING_DEPTH} path segments.</small></label>
        <label htmlFor={`${idPrefix}-future-skew`}><span>Maximum future skew (seconds)</span><input id={`${idPrefix}-future-skew`} inputMode="decimal" value={value.maximumFutureSkewSeconds} onChange={(event) => onChange({ maximumFutureSkewSeconds: event.target.value })} placeholder="Inherit server limit" /><small>Zero or blank inherits; maximum 300 seconds.</small></label>
        <label htmlFor={`${idPrefix}-event-age`}><span>Maximum event age (seconds)</span><input id={`${idPrefix}-event-age`} inputMode="decimal" value={value.maximumEventAgeSeconds} onChange={(event) => onChange({ maximumEventAgeSeconds: event.target.value })} placeholder="Inherit server limit" /><small>Zero or blank inherits; maximum 31,536,000 seconds (365 days).</small></label>
        <label htmlFor={`${idPrefix}-events-rate`}><span>Maximum events per second</span><input id={`${idPrefix}-events-rate`} type="number" min="0" max={INGESTION_MAX_EVENTS_PER_SECOND.toString()} step="1" value={value.maxEventsPerSecond} onChange={(event) => onChange({ maxEventsPerSecond: event.target.value })} placeholder="Unlimited" /><small>Zero or blank is unlimited; maximum 1,000,000.</small></label>
        <label htmlFor={`${idPrefix}-bytes-rate`}><span>Maximum bytes per second</span><input id={`${idPrefix}-bytes-rate`} type="number" min="0" max={INGESTION_MAX_BYTES_PER_SECOND.toString()} step="1" value={value.maxUncompressedBytesPerSecond} onChange={(event) => onChange({ maxUncompressedBytesPerSecond: event.target.value })} placeholder="Unlimited" /><small>Uncompressed event bytes; zero or blank is unlimited, maximum 1 TiB/s.</small></label>
      </div>
    </fieldset>
  );
}

function TokenPolicyFields({
  idPrefix,
  value,
  onChange,
}: {
  idPrefix: string;
  value: TokenPolicyForm;
  onChange: (value: Partial<TokenPolicyForm>) => void;
}) {
  return (
    <fieldset>
      <legend>Admission policy <small>(optional)</small></legend>
      <div className="admin-policy-grid">
        <label htmlFor={`${idPrefix}-host-patterns`}><span>Allowed host patterns</span><textarea id={`${idPrefix}-host-patterns`} value={value.allowedHostRegexes} onChange={(event) => onChange({ allowedHostRegexes: event.target.value })} rows={3} spellCheck={false} placeholder={"api-[0-9]+\nworker-[0-9]+"} /><small>One complete-value Go/RE2 pattern per line. Empty means any host.</small></label>
        <label htmlFor={`${idPrefix}-source-patterns`}><span>Allowed source patterns</span><textarea id={`${idPrefix}-source-patterns`} value={value.allowedSourceRegexes} onChange={(event) => onChange({ allowedSourceRegexes: event.target.value })} rows={3} spellCheck={false} placeholder={"/var/log/application\\.log"} /><small>One complete-value Go/RE2 pattern per line. Empty means any source.</small></label>
        <label htmlFor={`${idPrefix}-events-rate`}><span>Maximum events per second</span><input id={`${idPrefix}-events-rate`} type="number" min="0" max={INGESTION_MAX_EVENTS_PER_SECOND.toString()} step="1" value={value.maxEventsPerSecond} onChange={(event) => onChange({ maxEventsPerSecond: event.target.value })} placeholder="Unlimited" /><small>Zero or blank is unlimited; maximum 1,000,000.</small></label>
        <label htmlFor={`${idPrefix}-bytes-rate`}><span>Maximum bytes per second</span><input id={`${idPrefix}-bytes-rate`} type="number" min="0" max={INGESTION_MAX_BYTES_PER_SECOND.toString()} step="1" value={value.maxUncompressedBytesPerSecond} onChange={(event) => onChange({ maxUncompressedBytesPerSecond: event.target.value })} placeholder="Unlimited" /><small>Uncompressed event bytes; zero or blank is unlimited, maximum 1 TiB/s.</small></label>
      </div>
    </fieldset>
  );
}

interface TokenScopePickerProps {
  idPrefix: string;
  options: TokenIndexScopeOption[];
  selected: Set<string>;
  onChange: (value: Set<string>) => void;
  disabled?: boolean;
}

interface HECTokenProfileFieldsProps {
  idPrefix: string;
  selectedIndexes: Set<string>;
  defaultIndex: string;
  onDefaultIndexChange: (value: string) => void;
  defaultHost: string;
  onDefaultHostChange: (value: string) => void;
  defaultSource: string;
  onDefaultSourceChange: (value: string) => void;
  defaultSourcetype: string;
  onDefaultSourcetypeChange: (value: string) => void;
  indexerAcknowledgment: boolean;
  onIndexerAcknowledgmentChange: (value: boolean) => void;
  acknowledgmentReadOnly?: boolean;
}

function HECTokenProfileFields(props: HECTokenProfileFieldsProps) {
  const metadataFields = [
    {
      key: "host",
      label: "Default host",
      value: props.defaultHost,
      placeholder: "api.example.com",
      onChange: props.onDefaultHostChange,
    },
    {
      key: "source",
      label: "Default source",
      value: props.defaultSource,
      placeholder: "http:orders",
      onChange: props.onDefaultSourceChange,
    },
    {
      key: "sourcetype",
      label: "Default sourcetype",
      value: props.defaultSourcetype,
      placeholder: "_json",
      onChange: props.onDefaultSourcetypeChange,
    },
  ] as const;
  return (
    <fieldset>
      <legend>HEC profile</legend>
      <label htmlFor={`${props.idPrefix}-hec-default-index`}>
        <span>Default index <small>(optional)</small></span>
        <select id={`${props.idPrefix}-hec-default-index`} value={props.defaultIndex} onChange={(event) => props.onDefaultIndexChange(event.target.value)} aria-invalid={props.defaultIndex.length > 0 && !props.selectedIndexes.has(props.defaultIndex)}>
          <option value="">No token default (requests must provide an index)</option>
          {[...props.selectedIndexes].toSorted().map((name) => <option value={name} key={name}>{name}</option>)}
        </select>
        <small>When set, this index must remain in the token&apos;s allowed scope. Allowed scope alone is never an implicit default.</small>
      </label>
      {metadataFields.map((field) => {
        const valid = validHECMetadataDefault(field.value);
        const bytes = new TextEncoder().encode(field.value).byteLength;
        return (
          <label htmlFor={`${props.idPrefix}-hec-default-${field.key}`} key={field.key}>
            <span>{field.label} <small>(optional)</small></span>
            <input id={`${props.idPrefix}-hec-default-${field.key}`} value={field.value} onChange={(event) => field.onChange(event.target.value)} placeholder={field.placeholder} autoComplete="off" spellCheck={false} aria-invalid={!valid} />
            <small>{bytes.toLocaleString()} / 255 UTF-8 bytes. Values are preserved exactly and cannot contain controls or surrounding ASCII whitespace.</small>
          </label>
        );
      })}
      <label className="admin-checkbox" htmlFor={`${props.idPrefix}-hec-indexer-acknowledgment`} aria-label="Enable HEC indexer acknowledgment">
        <input id={`${props.idPrefix}-hec-indexer-acknowledgment`} type="checkbox" checked={props.indexerAcknowledgment} disabled={props.acknowledgmentReadOnly} onChange={(event) => props.onIndexerAcknowledgmentChange(event.target.checked)} />
        <span><strong>Indexer acknowledgment</strong><small>{props.acknowledgmentReadOnly ? "This setting is immutable. Rotate the token to change acknowledgment mode." : "Enable channel-scoped acknowledgment IDs. This choice cannot be changed after creation."}</small></span>
      </label>
    </fieldset>
  );
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
        <article><span className="summary-icon summary-icon--green" aria-hidden="true">▦</span><div><small>Indexes</small><strong>{props.indexState === "available" ? indexCount : "—"}</strong><p>{indexDetail}</p></div><button type="button" onClick={() => props.onNavigate("indexes")}>Manage</button></article>
        <article><span className="summary-icon summary-icon--blue" aria-hidden="true">⇣</span><div><small>Ingestion tokens</small><strong>{props.tokenState === "available" ? tokenCount : "—"}</strong><p>{tokenDetail}</p></div><button type="button" onClick={() => props.onNavigate("collectors")}>Inspect</button></article>
        <article><span className="summary-icon summary-icon--violet" aria-hidden="true">⌕</span><div><small>Source revision</small><strong>{bootstrap?.build?.sourceRevision.slice(0, 12) || "—"}</strong><p>{bootstrap === null ? "Bootstrap unavailable" : bootstrap.build === null ? "Not reported" : "Build identity"}</p></div><Link href="/search/">Search</Link></article>
        <article><span className="summary-icon summary-icon--orange" aria-hidden="true">↻</span><div><small>Result retention</small><strong>{bootstrap !== null && bootstrap.limits.searchResultRetentionMs > 0 ? `${Math.round(bootstrap.limits.searchResultRetentionMs / 60_000)}m` : "—"}</strong><p>{bootstrap === null ? "Bootstrap unavailable" : "Read-only server limit"}</p></div><button type="button" onClick={() => props.onNavigate("server")}>Limits</button></article>
      </div>
      {bootstrap === null ? (
        <BackendResourceState
          kind="error"
          title="System bootstrap could not be loaded"
          message={`${props.bootstrapError ?? "The bootstrap route did not return a usable response."} Index and token routes were checked independently and remain available where shown.`}
          action={<button type="button" onClick={props.onReload}>Retry bootstrap</button>}
        />
      ) : (
        <section className="suite-card">
          <header className="suite-card-header"><div><h3>Connection details</h3><p>Values returned by system bootstrap.</p></div><span className="status-label status-label--complete"><i />Connected</span></header>
          <dl className="backend-definition-list">
            <div><dt>Source revision</dt><dd>{bootstrap.build?.sourceRevision || "Not reported"}</dd></div>
            <div><dt>UI build ID</dt><dd>{bootstrap.build?.uiBuildId || "Not reported"}</dd></div>
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
  onDelete: (index: Index) => void;
}

function BackendIndexes(props: BackendIndexesProps) {
  if (props.state === "loading") return <BackendResourceState kind="loading" title="Loading indexes" message="Reading the server index catalog…" />;
  if (props.state === "unavailable") return <BackendResourceState kind="unavailable" title="Index administration is unavailable" message="The connected server did not register the index administration routes." action={<button type="button" onClick={props.onReload}>Retry</button>} />;
  if (props.state === "error") return <BackendResourceState kind="error" title="Indexes could not be loaded" message={props.error ?? "The server rejected the index catalog request."} action={<button type="button" onClick={props.onReload}>Retry</button>} />;

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
      <div className="resource-toolbar"><label><span className="sr-only">Filter loaded indexes</span><i aria-hidden="true"><AppIcon name="search" size="sm" /></i><input value={props.filter} onChange={(event) => props.onFilterChange(event.target.value)} placeholder="Filter loaded indexes" /></label><button type="button" onClick={props.onReload}><AppIcon name="refresh" size="sm" /> Refresh</button></div>
      {props.indexes.length === 0 ? (
        <BackendResourceState kind="empty" title={props.totalIndexes === 0 ? "No indexes configured" : "No matching indexes"} message={props.totalIndexes === 0 ? "Create an index to begin accepting and searching data." : "Try another index name or description."} action={props.totalIndexes > 0 && props.filter.trim().length > 0 ? <button type="button" onClick={() => props.onFilterChange("")}>Clear filter</button> : undefined} />
      ) : (
        <div className="suite-card resource-table-card">
          <div className="responsive-table-wrap">
            <table className="product-table admin-resource-table">
              <caption className="sr-only">Configured indexes</caption>
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
                    <td className="table-long-value">{canSearch
                      ? <Link className="resource-name" href={searchLaunchHref(`index=${name} | sort -_time`)} aria-label={`Search index ${name}`}>{nameContent}</Link>
                      : <div className="resource-name" aria-label={`Index ${name} is not currently searchable`}>{nameContent}</div>}
                    </td>
                    <td><span className={`status-label status-label--${statusClass(state)}`}><i />{state}</span></td>
                    <td>{indexAccessLabel(definition?.ingestionAccess)}</td>
                    <td>{indexAccessLabel(definition?.searchAccess)}</td>
                    <td>{formatDuration(definition?.retentionPeriod?.seconds)}</td>
                    <td>{formatDate(index.updatedAt)}</td>
                    <td><div className="row-actions"><button className="table-action" type="button" aria-label={`Edit index ${name}`} disabled={!canEdit || props.busy !== null} onClick={() => props.onEdit(index)}>{props.busy === `read-index-${index.indexId}` ? "Loading…" : "Edit"}</button><button className="table-action" type="button" aria-label={`${index.state === IndexState.INDEX_STATE_ACTIVE ? "Archive" : "Reactivate"} index ${name}`} disabled={!canChange || props.busy !== null} onClick={() => props.onChangeState(index)}>{props.busy === `index-${index.indexId}` ? "Updating…" : index.state === IndexState.INDEX_STATE_ACTIVE ? "Archive" : "Reactivate"}</button><button className="table-action table-action--danger" type="button" aria-label={`Delete index ${name}`} disabled={!canEdit || props.busy !== null} onClick={() => props.onDelete(index)}>Delete</button></div></td>
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
      <p className="resource-footnote">Event counts, storage use, and the bounded field catalog are available from the Datasets page. Delete uses a current version, an exact-name confirmation, and an explicit physical-data mode.</p>
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
  recoveryActionLabel: string | null;
  onResolveRecovery: () => void;
  onEdit: (token: IngestionToken) => void;
  onLoadMore: () => void;
  onReload: () => void;
  onRevoke: (token: IngestionToken) => void;
  onSetEnabled: (token: IngestionToken, enabled: boolean) => void;
}

function BackendTokens(props: BackendTokensProps) {
  if (props.state === "loading") return <BackendResourceState kind="loading" title="Loading ingestion tokens" message="Reading token metadata from the server…" />;
  if (props.state === "unavailable") return <BackendResourceState kind="unavailable" title="Ingestion tokens are unavailable" message="The connected server did not register the ingestion-token routes. Collector fleet status is loaded independently from its own capability-gated panel." action={<button type="button" onClick={props.onReload}>Retry</button>} />;
  if (props.state === "error") return <BackendResourceState kind="error" title="Ingestion tokens could not be loaded" message={props.error ?? "The server rejected the token list request."} action={<button type="button" onClick={props.onReload}>Retry</button>} />;
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
      <header className="admin-section-header"><div><h2>Ingestion tokens</h2><p>Manage server-issued ingestion credentials and their index scopes.</p></div></header>
      {props.createBlockReason === null ? null : (
        <div id="ingestion-token-create-disabled-reason" className="access-mode-notice token-create-disabled-reason" role="note">
          <span>!</span>
          <div>
            <strong>Token generation is locked</strong>
            <p>{props.createBlockReason}</p>
            {props.recoveryActionLabel === null ? null : (
              <button className="button secondary" type="button" onClick={props.onResolveRecovery}>
                {props.recoveryActionLabel}
              </button>
            )}
          </div>
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
      <section className="suite-card token-section token-section--credentials">
        <header className="suite-card-header"><div><h3>Issued credentials</h3><p>Token secrets are never returned after creation. {loadedCount}.</p></div><button type="button" onClick={props.onReload}>Refresh</button></header>
        {props.tokens.length === 0 ? (
          <BackendResourceState
            kind="empty"
            title="No ingestion tokens"
            message={props.canCreate
              ? "Generate a token scoped to an active, ingestible index."
              : props.scopeSource !== "unavailable"
                ? "No active, ingestion-enabled index is currently available for a new token."
                : "The token route is available, but generation is disabled until an authoritative index summary loads."}
          />
        ) : (
          <div className="responsive-table-wrap"><table className="product-table"><caption className="sr-only">Issued ingestion credentials</caption><thead><tr><th scope="col">Name</th><th scope="col">Purpose</th><th scope="col">Prefix</th><th scope="col">Allowed indexes</th><th scope="col">Expires</th><th scope="col">Last used</th><th scope="col">State</th><th scope="col"><span className="sr-only">Actions</span></th></tr></thead><tbody>{props.tokens.map((token) => {
            const state = tokenStateLabel(token.state);
            const canRevoke = tokenCanBeRevoked(token);
            const canSetEnabled = tokenCanSetEnabled(token);
            const enable = token.state === IngestionTokenState.INGESTION_TOKEN_STATE_DISABLED;
            const hecToken = tokenUsesHEC(token.purpose);
            const nativeToken = token.purpose === IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_NATIVE_COLLECTOR;
            const canEdit = canRevoke && (hecToken || nativeToken);
            return <tr key={token.ingestionTokenId}><td><strong>{token.name}</strong>{token.description ? <small className="table-secondary">{token.description}</small> : null}</td><td><strong>{tokenPurposeLabel(token.purpose)}</strong><small className="table-secondary">{hecToken ? `Indexer ACK ${token.hecProfile?.indexerAcknowledgment ? "enabled" : "disabled"}` : nativeToken ? "gRPC ingestion" : "Transport unavailable"}</small>{hecToken ? <small className="table-secondary">{hecProfileSummary(token.hecProfile)}</small> : null}</td><td><code>{token.tokenPrefix}</code></td><td className="table-long-value">{token.constraints?.allowedIndexNames.join(", ") || "None"}<small className="table-secondary">{hecToken ? token.hecProfile?.defaultIndexName ? `Default ${token.hecProfile.defaultIndexName}` : "No token default index" : nativeToken ? token.constraints?.boundCollectorId === undefined ? "Native collector binding required" : `Collector ${token.constraints.boundCollectorId}` : "Purpose unavailable"}</small></td><td>{formatDate(token.expiresAt)}</td><td>{formatDate(token.lastUsedAt)}</td><td><span className={`status-label status-label--${statusClass(state)}`}><i />{state}</span></td><td><div className="row-actions"><button className="table-action" type="button" aria-label={`Edit token ${token.name}`} disabled={!canEdit || props.busy !== null} onClick={() => props.onEdit(token)}>{props.busy === `read-token-${token.ingestionTokenId}` ? "Loading…" : "Edit"}</button><button className="table-action" type="button" aria-label={`${enable ? "Enable" : "Disable"} token ${token.name}`} disabled={!canSetEnabled || props.busy !== null} onClick={() => props.onSetEnabled(token, enable)}>{props.busy === `token-state-${token.ingestionTokenId}` ? enable ? "Enabling…" : "Disabling…" : canSetEnabled ? enable ? "Enable" : "Disable" : "—"}</button><button className="table-action" type="button" aria-label={`Revoke token ${token.name}`} disabled={!canRevoke || props.busy !== null} onClick={() => props.onRevoke(token)}>{props.busy === `token-${token.ingestionTokenId}` ? "Revoking…" : canRevoke ? "Revoke" : "—"}</button></div></td></tr>;
          })}</tbody></table></div>
        )}
        <div className="admin-pagination-footer admin-pagination-footer--inset" aria-live="polite">
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
  hecState,
  hecSnapshot,
  hecError,
  onReload,
}: {
  bootstrap: SystemBootstrapModel | null;
  error: string | null;
  hecState: ResourceState;
  hecSnapshot: GetHECOperationalSnapshotResponse | null;
  hecError: string | null;
  onReload: () => void;
}) {
  if (bootstrap === null) {
    return (
      <BackendResourceState
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
      {hecState === "unavailable" ? (
        <div className="access-mode-notice" role="note"><span>i</span><div><strong>HTTP Event Collector is disabled</strong><p>The server does not advertise HEC ingestion. HEC token creation and test commands remain unavailable until the data-plane feature is enabled.</p></div></div>
      ) : hecState === "loading" ? (
        <BackendResourceState kind="loading" title="Loading HEC operations" message="Reading the administrator operational snapshot…" />
      ) : hecState === "error" || hecSnapshot === null ? (
        <BackendResourceState kind="error" title="HEC operations could not be loaded" message={hecError ?? "The operational snapshot was empty."} action={<button type="button" onClick={onReload}>Retry</button>} />
      ) : (
        <>
          <section className="suite-card settings-group">
            <header><h3>HTTP Event Collector operations</h3><p>Process-wide counters observed {formatDate(hecSnapshot.observedAt)}.</p></header>
            <dl className="backend-definition-list">
              <div><dt>Requests</dt><dd>{hecSnapshot.request?.requests.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Accepted requests</dt><dd>{hecSnapshot.request?.acceptedRequests.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Events</dt><dd>{hecSnapshot.request?.events.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Uncompressed bytes</dt><dd>{hecSnapshot.request?.uncompressedBytes.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Authentication failures</dt><dd>{hecSnapshot.request?.authenticationFailures.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Decode failures</dt><dd>{hecSnapshot.request?.decodeFailures.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Event-policy failures</dt><dd>{hecSnapshot.request?.eventPolicyFailures.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Rate-limited requests</dt><dd>{hecSnapshot.request?.rateLimitedRequests.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Staging failures</dt><dd>{hecSnapshot.request?.stagingFailures.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Staging duration</dt><dd>{formatOperationalDuration(hecSnapshot.request?.stagingDuration)}</dd></div>
              <div><dt>Shutdown rejections</dt><dd>{hecSnapshot.request?.shutdownRejections.toLocaleString() ?? "Not reported"}</dd></div>
            </dl>
          </section>
          <section className="suite-card settings-group">
            <header><h3>Durability and acknowledgment</h3><p>Queue capacity, reconciliation, and indexer-acknowledgment health.</p></header>
            <dl className="backend-definition-list">
              <div><dt>Durable queue</dt><dd>{hecSnapshot.durable?.queueAvailable ? "Available" : "Unavailable"}</dd></div>
              <div><dt>Request capacity</dt><dd>{hecSnapshot.durable?.requestCapacityAvailable ? "Available" : "Unavailable"}</dd></div>
              <div><dt>Pending outbox reservations</dt><dd>{hecSnapshot.durable?.pendingOutboxReservations.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Pending outbox bytes</dt><dd>{hecSnapshot.durable?.pendingOutboxBytes.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Oldest pending age</dt><dd>{formatOperationalDuration(hecSnapshot.durable?.oldestPendingOutboxAge)}</dd></div>
              <div><dt>Retained requests</dt><dd>{hecSnapshot.durable?.retainedRequests.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Reconciliation</dt><dd>{hecSnapshot.reconciliation?.available ? "Available" : "Unavailable"}</dd></div>
              <div><dt>Reconciliation successes</dt><dd>{hecSnapshot.reconciliation?.successes.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Reconciliation retries</dt><dd>{hecSnapshot.reconciliation?.retries.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Reconciliation ambiguities</dt><dd>{hecSnapshot.reconciliation?.ambiguities.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>ACK service</dt><dd>{hecSnapshot.acknowledgments?.available ? "Available" : "Unavailable"}</dd></div>
              <div><dt>Active ACK channels</dt><dd>{hecSnapshot.acknowledgments?.activeChannels.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Retained ACK channels</dt><dd>{hecSnapshot.acknowledgments?.retainedChannels.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Pending ACK rows</dt><dd>{hecSnapshot.acknowledgments?.pendingRows.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Indexed ACK rows</dt><dd>{hecSnapshot.acknowledgments?.indexedRows.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Expired ACK rows</dt><dd>{hecSnapshot.acknowledgments?.expiredRows.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Terminal failed requests</dt><dd>{hecSnapshot.acknowledgments?.terminalFailedRequests.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>ACK queries</dt><dd>{hecSnapshot.acknowledgments?.queries.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>ACK IDs queried</dt><dd>{hecSnapshot.acknowledgments?.idsQueried.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>ACK query misses</dt><dd>{hecSnapshot.acknowledgments?.misses.toLocaleString() ?? "Not reported"}</dd></div>
            </dl>
          </section>
          <section className="suite-card settings-group">
            <header><h3>HEC protocol failures</h3><p>Bounded non-success response codes reported by the HEC compatibility layer.</p></header>
            {hecSnapshot.protocolFailures.length === 0 ? <p className="settings-group__empty">No protocol failures have been observed.</p> : (
              <dl className="backend-definition-list">
                {hecSnapshot.protocolFailures.map((metric) => <div key={metric.code}><dt>Response code {metric.code}</dt><dd>{metric.count.toLocaleString()}</dd></div>)}
              </dl>
            )}
          </section>
        </>
      )}
    </div>
  );
}
