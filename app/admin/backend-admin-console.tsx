"use client";

import type { FormEvent } from "react";
import { useCallback, useEffect, useEffectEvent, useMemo, useRef, useState, useSyncExternalStore, type RefObject } from "react";
import Link from "next/link";

import { SortDirection, type PageResponse } from "@/gen/ts/open_splunk/common";
import { ServerFeature } from "@/gen/ts/open_splunk/system_api";
import {
  IngestionTokenPurpose,
  IngestionTokenState,
  type IngestionToken,
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
  isHttpStatus,
  isOptionalRouteUnavailable,
  supportsServerFeature,
  type OpenSplunkApiClient,
  type SystemBootstrapModel,
} from "@/lib/api";
import { createErrorMessage } from "@/lib/error-message";

import { FieldNote, fieldControlProps } from "../_components/field-validation";
import {
  indexPolicyFormFromDefinition,
  indexPolicyFromForm,
  indexPolicyIsValid,
  tokenPolicyFormFromToken,
  tokenPolicyFromForm,
  tokenPolicyIsValid,
  type IndexPolicyForm,
  type TokenPolicyForm,
} from "./ingestion-policy-form";
import { BackendResourceState } from "../_components/backend-resource-state";
import { StatusDot, StatusLabel, type StatusTone } from "../_components/status";
import { AppIcon, type AppIconName } from "../_components/app-icon";
import { PageHeading } from "../_components/product-shell";
import { Modal } from "../_components/modal";
import { AppsAdminPanel, CollectorFleetPanel } from "./admin-resource-panels";
import {
  BackendIndexes,
  BackendOverview,
  BackendServerSettings,
  BackendTokens,
  HECTokenProfileFields,
  IndexPolicyFields,
  TokenPolicyFields,
  TokenScopePicker,
  formatDate,
  formatDuration,
  indexStateLabel,
  statusTone,
  tokenCanBeRevoked,
  tokenStateLabel,
  type ResourceState,
  type TokenIndexScopeOption,
  type TokenScopeSource,
} from "./backend-admin-panels";
import { ADMIN_SECTION_QUERY_PARAMETER, adminSectionPath, resolveAdminSection } from "./admin-navigation";
import { KnowledgeManagerGate } from "./knowledge-manager-gate";
import { LookupManagerGate } from "./lookup-manager-gate";
import { IssuedTokenDialog, TokenCreationDialog, type TokenCreateFormValue } from "./token-creation-dialog";
import {
  TOKEN_CREATE_CLOCK_EPSILON_MS,
  TOKEN_CREATE_ZERO_CONFIRMATION_INTERVAL_MS,
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
  COLLECTOR_ID_ERROR,
  hecCurlExample,
  hecProfileFromForm,
  hecProfilesMatch,
  hasSameStrings,
  historyHasTokenGuard,
  historyStateWithTokenGuard,
  isDefiniteTokenCreateFailure,
  listTokensForCreateSafety,
  normalizeApiBaseUrl,
  normalizedPageToken,
  parsePersistedTokenCreateGuard,
  readTokenCreateGuardRaw,
  removeTokenCreateGuard,
  requestTokenCreateLock,
  serializeTokenCreateGuard,
  subscribeTokenCreateGuard,
  tokenFallsWithinCreateAttributionWindow,
  tokenIsTerminallySafe,
  tokenMatchesCreateDefinition,
  tokenMatchesCreateMetadata,
  tokenPurposeLabel,
  tokenUsesHEC,
  validCollectorId,
  validHECMetadataDefault,
  writeTokenCreateGuard,
  type TokenCreateDefinitionSnapshot,
  type TokenCreateGuardStorageState,
  type TokenCreateOutcomeKind,
  type TokenCreateRecovery,
  type UnreadableTokenCreateRecovery,
} from "./token-creation";
import {
  backendKnowledgeCapabilities,
  backendAdminNavigation,
  knowledgeManagerAppOptionsFromBootstrap,
  type BackendAdminSection as AdminSection,
} from "./knowledge-manager-feature";
import {
  TokenRecoveryStartupController,
  type TokenRecoveryStartupContext,
  type TokenRecoveryStartupRecord,
} from "./token-recovery-startup";
import { Select, SelectOption } from "../_components/select";

type AdminModal = "create-index" | "edit-index" | "create-token" | "edit-token";

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

interface ServerClockAnchor {
  serverTimeMs: number;
  clientMonotonicMs: number;
  uncertaintyMs: number;
}


const errorMessage = createErrorMessage("The server did not return a usable response.");

function subscribeToBrowserOrigin(): () => void {
  return () => undefined;
}

function browserOriginSnapshot(): string {
  return window.location.origin;
}

function subscribeToTokenRecoveryEnvironment(listener: () => void): () => void {
  document.addEventListener("visibilitychange", listener);
  window.addEventListener("online", listener);
  window.addEventListener("offline", listener);
  return () => {
    document.removeEventListener("visibilitychange", listener);
    window.removeEventListener("online", listener);
    window.removeEventListener("offline", listener);
  };
}

function tokenRecoveryEnvironmentSnapshot(): boolean {
  return tokenRecoveryEnvironmentCanPoll(document.visibilityState, navigator.onLine);
}

function subscribeToAdminSection(listener: () => void): () => void {
  window.addEventListener("popstate", listener);
  return () => window.removeEventListener("popstate", listener);
}

function adminSectionSnapshot(): AdminSection {
  return resolveAdminSection(
    new URL(window.location.href).searchParams.get(ADMIN_SECTION_QUERY_PARAMETER),
    BACKEND_ADMIN_SECTIONS,
    "overview",
  );
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


export function BackendAdminConsole({ apiBaseUrl }: BackendAdminConsoleProps) {
  const client = useMemo(() => createOpenSplunkApiClient({ baseUrl: apiBaseUrl }), [apiBaseUrl]);
  const browserOrigin = useSyncExternalStore(
    subscribeToBrowserOrigin,
    browserOriginSnapshot,
    () => "",
  );
  const locationSection = useSyncExternalStore(
    subscribeToAdminSection,
    adminSectionSnapshot,
    () => "overview",
  );
  const apiBaseNormalization = useMemo(() => {
    if (browserOrigin.length === 0) return { error: null, value: null };
    try {
      return { error: null, value: normalizeApiBaseUrl(apiBaseUrl, browserOrigin) };
    } catch (error) {
      return { error: errorMessage(error), value: null };
    }
  }, [apiBaseUrl, browserOrigin]);
  const normalizedApiBaseUrl = apiBaseNormalization.value;
  const apiBaseNormalizationError = apiBaseNormalization.error;
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
  const [observedApiBaseNormalizationError, setObservedApiBaseNormalizationError] = useState<string | null>(null);
  if (observedApiBaseNormalizationError !== apiBaseNormalizationError) {
    setObservedApiBaseNormalizationError(apiBaseNormalizationError);
    if (apiBaseNormalizationError !== null) setTokenCreateGuardStorageState("unavailable");
  }
  const [tokenCreateLockAvailable, setTokenCreateLockAvailable] = useState<boolean | null>(null);
  const [tokenRecoveryOwnership, setTokenRecoveryOwnership] =
    useState<TokenRecoveryOwnership>("idle");
  const [tokenRecoveryOwnershipError, setTokenRecoveryOwnershipError] = useState<string | null>(null);
  const [tokenRecoveryChecking, setTokenRecoveryChecking] = useState(false);
  const [tokenRecoveryLastCheckedAt, setTokenRecoveryLastCheckedAt] = useState<number | null>(null);
  const [tokenRecoveryNextCheckAt, setTokenRecoveryNextCheckAt] = useState<number | null>(null);
  const tokenRecoveryEnvironmentReady = useSyncExternalStore(
    subscribeToTokenRecoveryEnvironment,
    tokenRecoveryEnvironmentSnapshot,
    () => true,
  );
  const [observedTokenRecoveryEnvironmentReady, setObservedTokenRecoveryEnvironmentReady] =
    useState(tokenRecoveryEnvironmentReady);
  if (observedTokenRecoveryEnvironmentReady !== tokenRecoveryEnvironmentReady) {
    setObservedTokenRecoveryEnvironmentReady(tokenRecoveryEnvironmentReady);
    if (
      tokenRecoveryEnvironmentReady
      && tokenRecoveryOwnership === "owned"
      && tokenRecoveryNextCheckAt !== null
    ) setTokenRecoveryNextCheckAt(Date.now());
  }
  const [tokenRecoveryAcquireGeneration, setTokenRecoveryAcquireGeneration] = useState(0);
  const [tokenRecoveryStartup] = useState(() => new TokenRecoveryStartupController());
  const tokenRecoveryStartupSnapshot = useSyncExternalStore(
    tokenRecoveryStartup.subscribe,
    tokenRecoveryStartup.getSnapshot,
    tokenRecoveryStartup.getServerSnapshot,
  );
  const [observedTokenRecoveryStartupSnapshot, setObservedTokenRecoveryStartupSnapshot] =
    useState(tokenRecoveryStartupSnapshot);
  const [revokeTarget, setRevokeTarget] = useState<IngestionToken | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [toast, setToast] = useState<AdminToast | null>(null);
  const [serverSettingsDirty, setServerSettingsDirty] = useState(false);
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
  if (observedTokenRecoveryStartupSnapshot !== tokenRecoveryStartupSnapshot) {
    const observedRecord = "record" in observedTokenRecoveryStartupSnapshot
      ? observedTokenRecoveryStartupSnapshot.record
      : null;
    setObservedTokenRecoveryStartupSnapshot(tokenRecoveryStartupSnapshot);
    if (tokenRecoveryStartupSnapshot.kind !== "idle") {
      setTokenCreateLockAvailable(tokenRecoveryStartupSnapshot.lockAvailable);
    }
    switch (tokenRecoveryStartupSnapshot.kind) {
      case "idle":
      case "preflight":
        break;
      case "storage-unavailable":
        setTokenCreateGuardStorageState("unavailable");
        setTokenCreateGuardStorageError(errorMessage(tokenRecoveryStartupSnapshot.error));
        break;
      case "empty":
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
        break;
      case "stored":
      case "lock-unavailable":
      case "acquiring":
      case "contended":
      case "lock-failed":
        setTokenCreateGuardStorageState("available");
        setTokenCreateGuardStorageError(null);
        if (observedRecord !== tokenRecoveryStartupSnapshot.record) {
          setTokenSecret(null);
          setIssuedToken(null);
          setIssuedTokenRecovery(null);
          if (tokenRecoveryStartupSnapshot.record.restored === null) {
            setTokenCreateRecovery(null);
            setUnreadableTokenCreateRecovery(
              tokenRecoveryStartupSnapshot.record.unreadableRecovery,
            );
          } else {
            setUnreadableTokenCreateRecovery(null);
            setTokenCreateRecovery(tokenRecoveryStartupSnapshot.record.restored.recovery);
          }
        }
        if (tokenRecoveryStartupSnapshot.kind === "lock-unavailable") {
          setTokenRecoveryOwnership("failed");
          setTokenRecoveryOwnershipError(
            "This browser does not expose the cross-tab Web Locks API required to own token recovery safely.",
          );
        } else if (tokenRecoveryStartupSnapshot.kind === "acquiring") {
          setTokenRecoveryOwnership("acquiring");
          setTokenRecoveryOwnershipError(null);
        } else if (tokenRecoveryStartupSnapshot.kind === "contended") {
          setTokenRecoveryOwnership("contended");
          setTokenRecoveryOwnershipError(
            "Another tab currently owns token creation recovery. Close that tab, then try again.",
          );
        } else if (tokenRecoveryStartupSnapshot.kind === "lock-failed") {
          setTokenRecoveryOwnership("failed");
          setTokenRecoveryOwnershipError(
            `Cross-tab recovery lock failed: ${errorMessage(tokenRecoveryStartupSnapshot.error)}`,
          );
        }
        break;
    }
  }

  const loadInput = { client, loadGeneration };
  const [activeLoadInput, setActiveLoadInput] = useState(loadInput);
  if (activeLoadInput.client !== loadInput.client || activeLoadInput.loadGeneration !== loadInput.loadGeneration) {
    setActiveLoadInput(loadInput);
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
    setTokenState("loading");
    setTokenError(null);
    setTokenNextPageToken(null);
    setTokenTotalSize(null);
    setTokenTotalSizeExact(false);
    setTokenLoadingMore(false);
    setTokenPaginationError(null);
  }

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
    indexSeenPageTokensRef.current = new Set();
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
    if (normalizedApiBaseUrl === null) return;
    const canonicalApiBaseUrl = normalizedApiBaseUrl;
    function handleTokenGuardStorage(event: StorageEvent) {
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
    return subscribeTokenCreateGuard(canonicalApiBaseUrl, handleTokenGuardStorage);
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
      const raw = readTokenCreateGuardRaw(normalizedApiBaseUrl);
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
    let existingRaw: string | null;
    try {
      existingRaw = readTokenCreateGuardRaw(normalizedApiBaseUrl);
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
      writeTokenCreateGuard(normalizedApiBaseUrl, record);
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
    let raw: string | null;
    try {
      raw = readTokenCreateGuardRaw(normalizedApiBaseUrl);
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
      removeTokenCreateGuard(normalizedApiBaseUrl);
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
    setTokenSecret(null);
    setIssuedToken(null);
    setIssuedTokenRecovery(null);
    setTokenCreateRecovery(null);
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
      return readTokenCreateGuardRaw(normalizedApiBaseUrl) === recovery.raw;
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
      if (readTokenCreateGuardRaw(normalizedApiBaseUrl) === recovery.raw) return true;
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
      if (readTokenCreateGuardRaw(normalizedApiBaseUrl) !== recovery.raw) {
        return loseTokenGuardOwnership(
          recovery.attemptId,
          "The unreadable token safety record changed before it could be cleared.",
        );
      }
      removeTokenCreateGuard(normalizedApiBaseUrl);
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

  async function createToken(value: TokenCreateFormValue) {
    const {
      collectorId,
      description,
      expiration,
      hecDefaultHost,
      hecDefaultIndex,
      hecDefaultSource,
      hecDefaultSourcetype,
      hecIndexerAcknowledgment,
      indexes: selectedIndexes,
      name,
      policy,
      purpose,
    } = value;
    const creatingHECToken = tokenUsesHEC(purpose);
    if (
      name.trim().length === 0
      || (!creatingHECToken && !validCollectorId(collectorId))
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
    const hasUnavailableScope = [...selectedIndexes].some(
      (selectedIndexName) => !ingestibleIndexNames.has(selectedIndexName),
    );
    const hecProfileInvalid = hecDefaultIndex.length > 0 && !selectedIndexes.has(hecDefaultIndex)
      || !validHECMetadataDefault(hecDefaultHost)
      || !validHECMetadataDefault(hecDefaultSource)
      || !validHECMetadataDefault(hecDefaultSourcetype);
    const createScopeInvalid = tokenScopeSource === "unavailable"
      || selectedIndexes.size === 0
      || hasUnavailableScope
      || (!creatingHECToken && !validCollectorId(collectorId))
      || (creatingHECToken && !hecEnabled)
      || (creatingHECToken && hecProfileInvalid)
      || tokenCreationBlockReason !== null;
    if (createScopeInvalid) {
      setToast({
        message: creatingHECToken && !hecEnabled
          ? "HEC token generation is unavailable because the server does not advertise HEC ingestion."
          : tokenScopeSource === "unavailable"
          ? "Token generation is unavailable until the server returns an authoritative index summary."
          : selectedIndexes.size === 0
            ? "Select at least one active, ingestion-enabled index."
            : "Remove unavailable index scopes before generating the token.",
        kind: "warning",
      });
      return;
    }
    let tokenPolicy: ReturnType<typeof tokenPolicyFromForm>;
    try {
      tokenPolicy = tokenPolicyFromForm(policy);
    } catch (error) {
      setToast({ message: errorMessage(error), kind: "warning" });
      return;
    }
    let crossTabLockAcquired = false;
    try {
      await requestTokenCreateLock(
        normalizedApiBaseUrl,
        { mode: "exclusive", ifAvailable: true },
        async (lock) => {
          if (lock === null) return;
          crossTabLockAcquired = true;
          const existingGuard = readTokenCreateGuardRaw(normalizedApiBaseUrl);
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
      const expiresAt = expirationFromForm(expiration, initialServerTimeMs);
      const hecProfile = creatingHECToken ? hecProfileFromForm({
        defaultIndexName: hecDefaultIndex,
        defaultHost: hecDefaultHost,
        defaultSource: hecDefaultSource,
        defaultSourcetype: hecDefaultSourcetype,
        indexerAcknowledgment: hecIndexerAcknowledgment,
      }) : undefined;
      const definition: TokenCreateDefinitionSnapshot = {
        name: name.trim(),
        description: description.trim(),
        boundCollectorId: creatingHECToken ? "" : collectorId,
        allowedIndexNames: [...selectedIndexes].toSorted(),
        allowedHostRegexes: tokenPolicy.allowedHostRegexes,
        allowedSourceRegexes: tokenPolicy.allowedSourceRegexes,
        maxEventsPerSecond: tokenPolicy.ingestionRateLimits.maxEventsPerSecond,
        maxUncompressedBytesPerSecond:
          tokenPolicy.ingestionRateLimits.maxUncompressedBytesPerSecond,
        purpose,
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

  function acknowledgeIssuedToken(secretAcknowledged: boolean) {
    if (issuedToken === null) return;
    if (
      issuedTokenRecovery === null
      || !requireTokenGuardOwnership(issuedTokenRecovery)
    ) return;
    const hasSecret = tokenSecret !== null && tokenSecret.length > 0;
    if ((hasSecret && !secretAcknowledged) || (!hasSecret && tokenCanBeRevoked(issuedToken))) {
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

  const ownPersistedTokenRecovery = useEffectEvent(async (
    record: TokenRecoveryStartupRecord,
    context: TokenRecoveryStartupContext,
  ) => {
    const { restored, unreadableRecovery } = record;
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
        if (context.isCurrent() && ownsUnreadableTokenCreateGuard(unreadableRecovery)) {
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
        }, { signal: context.signal });
        if (!context.isCurrent() || !requireTokenGuardOwnership(recovery)) return;
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
        if (!context.isCurrent()) return;
        if (!requireTokenGuardOwnership(recovery)) return;
        const retryRecovery: TokenCreateRecovery = {
          ...recovery,
          reconciliationError: `The saved issued token could not be read directly: ${errorMessage(error)}`,
        };
        setTokenCreateRecovery(retryRecovery);
        await reconcileTokenCreateRecovery(retryRecovery);
      }
    } catch (error) {
      if (!context.isCurrent()) return;
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
      if (context.isCurrent() && ownsTokenCreateGuard(recovery)) {
        const lease = holdTokenGuardLease(recovery.attemptId);
        tokenGuardLockOperationAttemptRef.current = null;
        await lease;
      } else {
        tokenGuardLockOperationAttemptRef.current = null;
      }
    }
  });

  const startPersistedTokenRecovery = useEffectEvent(() => {
    if (normalizedApiBaseUrl === null) return;
    return tokenRecoveryStartup.start(
      normalizedApiBaseUrl,
      authoritativeServerNowMs() ?? null,
      {
        onCleanup: () => {
          tokenRecoveryOperationGenerationRef.current += 1;
          tokenGuardLockOperationAttemptRef.current = null;
          releaseTokenGuardLease();
        },
        onOwned: ownPersistedTokenRecovery,
      },
    );
  });

  useEffect(
    () => startPersistedTokenRecovery(),
    [client, normalizedApiBaseUrl, tokenRecoveryAcquireGeneration, tokenRecoveryStartup],
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
  const editingHECToken = tokenEditTarget !== null && tokenUsesHEC(tokenEditTarget.purpose);
  const editingNativeToken = tokenEditTarget?.purpose
    === IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_NATIVE_COLLECTOR;
  const tokenScopeChanged = tokenEditTarget !== null
    && !hasSameStrings(tokenIndexes, tokenEditTarget.constraints?.allowedIndexNames ?? []);
  const tokenBindingChanged = tokenEditTarget !== null
    && editingNativeToken
    && tokenEditTarget.constraints?.boundCollectorId === undefined
    && validCollectorId(tokenCollectorId);
  const tokenScopeInvalid = tokenScopeChanged
    && [...tokenIndexes].some((name) => !ingestibleIndexNames.has(name));
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
  const indexDefinition = indexEditTarget?.definition;
  // The index name rule the server enforces, stated once so the field, the
  // submit button and `createIndex` cannot disagree about it. It used to live
  // only inside `createIndex`, which meant Create stayed enabled for a name the
  // handler was about to reject with a toast.
  const indexNameError = indexName.trim().length === 0
    ? null
    : !/^[a-z0-9][a-z0-9_-]*$/.test(indexName.trim().toLowerCase())
      ? "Use lowercase letters, numbers, hyphens, and underscores, starting with a letter or number."
      : indexName.trim().toLowerCase().includes("kvstore")
        ? "“kvstore” is reserved and cannot appear in an index name."
        : null;
  const indexPolicyValid = indexPolicyIsValid(indexPolicyForm);
  const tokenPolicyValid = tokenPolicyIsValid(tokenPolicyForm);
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
  const section = (
    (locationSection === "knowledge" && !knowledgeAdvertised)
    || (locationSection === "lookups" && !lookupAdvertised)
  ) && (bootstrap !== null || bootstrapError !== null)
    ? "overview"
    : locationSection;
  useEffect(() => {
    if (section === locationSection) return;
    window.history.replaceState(null, "", adminSectionPath(window.location.href, section));
  }, [locationSection, section]);
  function navigateSection(next: AdminSection) {
    if (next === section) return;
    if (serverSettingsDirty && !window.confirm("Discard unapplied server settings?")) return;
    window.history.pushState(null, "", adminSectionPath(window.location.href, next));
    window.dispatchEvent(new PopStateEvent("popstate"));
  }
  const hasAvailableAdminRoute = indexState === "available" || tokenState === "available";
  const adminRoutesLoading = indexState === "loading" || tokenState === "loading";
  const connectionStatus: { detail: string; title: string; tone: StatusTone } = bootstrap !== null
    ? {
        tone: "success",
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
    ? <button className="button button--primary" type="button" onClick={openIndexDialog}><AppIcon name="plus" size="sm" /> Create index</button>
    : section === "collectors" && tokenState === "available"
      ? <button className="button button--primary" type="button" onClick={openTokenDialog} disabled={tokenCreateDisabledReason !== null} aria-describedby={tokenCreateDisabledReason === null ? undefined : "ingestion-token-create-disabled-reason"}>Generate token</button>
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
        <Select id="admin-section" value={section} onValueChange={(selectedValue) => navigateSection(selectedValue as AdminSection)}>
          {navigationItems.map((item) => <SelectOption value={item.key} key={item.key}>{item.label}</SelectOption>)}
        </Select>
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
            <StatusDot tone={connectionStatus.tone} />
            <div>
              <strong>{connectionStatus.title}</strong>
              <small>{connectionStatus.detail}</small>
            </div>
          </div>
        </aside>

        <section className="admin-content">
          {section === "overview" ? (
            <BackendOverview
              actions={{ navigate: navigateSection, reload: load }}
              snapshot={{
                bootstrap,
                bootstrapError,
                indexes: {
                  activeCount: activeIndexes,
                  loadedCount: indexes.length,
                  state: indexState,
                  totalSize: indexTotalSize,
                  totalSizeExact: indexTotalSizeExact,
                },
                tokens: {
                  activeCount: activeTokens,
                  loadedCount: tokens.length,
                  state: tokenState,
                  totalSize: tokenTotalSize,
                  totalSizeExact: tokenTotalSizeExact,
                },
              }}
            />
          ) : null}
          {section === "indexes" ? (
            <BackendIndexes
              actions={{
                changeState: (index) => void changeIndexState(index),
                delete: (index) => void openIndexDeleteDialog(index),
                edit: (index) => void openIndexEditor(index),
                loadMore: () => void loadMoreIndexes(),
                reload: load,
                setFilter,
              }}
              catalog={{
                busy,
                error: indexError,
                filter,
                hasMore: indexNextPageToken !== null,
                indexes: visibleIndexes,
                loadingMore: indexLoadingMore,
                paginationError: indexPaginationError,
                state: indexState,
                totalIndexes: indexes.length,
                totalSize: indexTotalSize,
                totalSizeExact: indexTotalSizeExact,
              }}
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
              actions={{
                edit: (token) => void openTokenEditor(token),
                loadMore: () => void loadMoreTokens(),
                reload: load,
                resolveRecovery: openTokenRecoveryDialog,
                revoke: setRevokeTarget,
                setEnabled: (token, enabled) => void setTokenEnabled(token, enabled),
              }}
              catalog={{
                busy,
                error: tokenError,
                hasMore: tokenNextPageToken !== null,
                loadingMore: tokenLoadingMore,
                paginationError: tokenPaginationError,
                state: tokenState,
                tokens,
                totalSize: tokenTotalSize,
                totalSizeExact: tokenTotalSizeExact,
              }}
              creation={{
                blockReason: tokenCreateDisabledReason,
                canCreate: ingestibleTokenScopes.length > 0 && tokenCreationBlockReason === null,
                recoveryActionLabel: tokenResolutionOpen ? "Resolve token creation" : null,
              }}
              scope={{
                error: indexError,
                source: tokenScopeSource,
                state: indexState,
              }}
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
              client={client}
              bootstrap={bootstrap}
              error={bootstrapError}
              hecState={hecState}
              hecSnapshot={hecSnapshot}
              hecError={hecError}
              onReload={load}
              onStatus={(message, kind) => setToast({ message, kind })}
              onDirtyChange={setServerSettingsDirty}
            />
          ) : null}
        </section>
      </div>

      {modal === "create-index" ? (
        <Modal
          title="Create index"
          subtitle="Create a searchable and ingestible index on the connected server."
          onClose={() => busy === null && setModal(null)}
          footer={<><button className="button button--secondary" type="button" onClick={() => setModal(null)} disabled={busy !== null}>Cancel</button><button className="button button--primary" type="submit" form="create-index-form" disabled={busy !== null || indexName.trim().length === 0 || indexNameError !== null || !indexPolicyValid}>{busy === "create-index" ? "Creating…" : "Create index"}</button></>}
        >
          <form className="admin-form" id="create-index-form" onSubmit={(event) => void createIndex(event)}>
            <label htmlFor="new-index-name">
              <span>Index name</span>
              <input autoComplete="off" id="new-index-name" onChange={(event) => setIndexName(event.target.value)} placeholder="application-logs" spellCheck={false} value={indexName} {...fieldControlProps("new-index-name", indexNameError)} />
              <FieldNote error={indexNameError} fieldId="new-index-name">Lowercase letters, numbers, hyphens, and underscores; “kvstore” is reserved. The name cannot be changed later.</FieldNote>
            </label>
            <label htmlFor="new-index-display-name"><span>Display name <small>(optional)</small></span><input id="new-index-display-name" value={indexDisplayName} onChange={(event) => setIndexDisplayName(event.target.value)} placeholder="Application logs" /><small>Shown to administrators. Defaults to the immutable index name.</small></label>
            <label htmlFor="new-index-description"><span>Description <small>(optional)</small></span><input id="new-index-description" value={indexDescription} onChange={(event) => setIndexDescription(event.target.value)} placeholder="Application and request logs" /></label>
            <label htmlFor="new-index-retention"><span>Retention</span><Select id="new-index-retention" value={retention} onValueChange={(selectedValue) => setRetention(selectedValue)}><SelectOption value="7">7 days</SelectOption><SelectOption value="14">14 days</SelectOption><SelectOption value="30">30 days</SelectOption><SelectOption value="90">90 days</SelectOption><SelectOption value="forever">Forever</SelectOption></Select><small>The server applies this period to stored events.</small></label>
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
          footer={<><button className="button button--secondary" type="button" onClick={() => { setIndexEditTarget(null); setModal(null); }} disabled={busy !== null}>Cancel</button><button className="button button--primary" type="submit" form="edit-index-form" disabled={busy !== null || !indexHasChanges || !indexPolicyValid}>{busy === `update-index-${indexEditTarget.indexId}` ? "Saving…" : "Save changes"}</button></>}
        >
          <form className="admin-form" id="edit-index-form" onSubmit={(event) => void updateIndex(event)}>
            <label htmlFor="edit-index-name"><span>Index name</span><input id="edit-index-name" value={indexEditTarget.definition.name} disabled /><small>Index names are immutable because searches and collectors reference them directly.</small></label>
            <label htmlFor="edit-index-display-name"><span>Display name</span><input id="edit-index-display-name" value={indexDisplayName} onChange={(event) => setIndexDisplayName(event.target.value)} placeholder={indexEditTarget.definition.name} /><small>Change the operator-facing label without changing the SPL index name.</small></label>
            <label htmlFor="edit-index-description"><span>Description <small>(optional)</small></span><input id="edit-index-description" value={indexDescription} onChange={(event) => setIndexDescription(event.target.value)} placeholder="Application and request logs" /></label>
            <label htmlFor="edit-index-retention">
              <span>Retention</span>
              <Select id="edit-index-retention" value={retention} onValueChange={(selectedValue) => setRetention(selectedValue)}>
                {!["7", "14", "30", "90", "forever"].includes(retention) ? <SelectOption value={retention}>{formatDuration(indexEditTarget.definition.retentionPeriod?.seconds)} (current)</SelectOption> : null}
                <SelectOption value="7">7 days</SelectOption><SelectOption value="14">14 days</SelectOption><SelectOption value="30">30 days</SelectOption><SelectOption value="90">90 days</SelectOption><SelectOption value="forever">Forever</SelectOption>
              </Select>
              <small>Changing retention affects how long stored events remain available.</small>
            </label>
            <label htmlFor="edit-index-ingestion-access"><span>Ingestion access</span><Select id="edit-index-ingestion-access" value={String(indexIngestionAccess)} onValueChange={(selectedValue) => setIndexIngestionAccess(Number(selectedValue) as IndexAccessState)}><SelectOption value={String(IndexAccessState.INDEX_ACCESS_STATE_ENABLED)}>Enabled</SelectOption><SelectOption value={String(IndexAccessState.INDEX_ACCESS_STATE_DISABLED)}>Disabled</SelectOption></Select><small>Disabled indexes reject new events and cannot be added to new token scopes.</small></label>
            <label htmlFor="edit-index-search-access"><span>Search access</span><Select id="edit-index-search-access" value={String(indexSearchAccess)} onValueChange={(selectedValue) => setIndexSearchAccess(Number(selectedValue) as IndexAccessState)}><SelectOption value={String(IndexAccessState.INDEX_ACCESS_STATE_ENABLED)}>Enabled</SelectOption><SelectOption value={String(IndexAccessState.INDEX_ACCESS_STATE_DISABLED)}>Disabled</SelectOption></Select><small>Disabled indexes remain configured but cannot be queried.</small></label>
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
          footer={<><button className="button button--secondary" type="button" onClick={() => { setIndexDeleteTarget(null); setIndexDeleteConfirmation(""); }} disabled={busy !== null}>Cancel</button><button className="button button--danger" type="submit" form="delete-index-form" disabled={busy !== null || indexDeleteConfirmation !== indexDeleteTarget.definition.name}>{busy === `delete-index-${indexDeleteTarget.indexId}` ? "Deleting…" : "Delete index"}</button></>}
        >
          <form className="admin-form" id="delete-index-form" onSubmit={(event) => void deleteIndex(event)}>
            <div className="access-mode-notice" role="alert"><span>!</span><div><strong>Deletion cannot be undone from the browser</strong><p>The server will reject this request if index version {indexDeleteTarget.version.toLocaleString()} is no longer current.</p></div></div>
            <label htmlFor="delete-index-mode"><span>Stored data</span><Select id="delete-index-mode" value={String(indexDeleteMode)} onValueChange={(selectedValue) => setIndexDeleteMode(Number(selectedValue) as IndexDataDeletionMode)}><SelectOption value={String(IndexDataDeletionMode.INDEX_DATA_DELETION_MODE_KEEP_DATA)}>Keep physical data</SelectOption><SelectOption value={String(IndexDataDeletionMode.INDEX_DATA_DELETION_MODE_DELETE_DATA)}>Permanently delete physical data</SelectOption></Select><small>{indexDeleteMode === IndexDataDeletionMode.INDEX_DATA_DELETION_MODE_DELETE_DATA ? "The backend may run physical deletion asynchronously and return an operation ID." : "Only the control-plane index record is deleted; stored data is preserved."}</small></label>
            <label htmlFor="delete-index-confirmation"><span>Type <code>{indexDeleteTarget.definition.name}</code> to confirm</span><input id="delete-index-confirmation" value={indexDeleteConfirmation} onChange={(event) => setIndexDeleteConfirmation(event.target.value)} autoComplete="off" spellCheck={false} /><small>The backend also checks this exact name before accepting the operation.</small></label>
          </form>
        </Modal>
      ) : null}

      {modal === "create-token" && !tokenResolutionOpen ? (
        <TokenCreationDialog
          blockReason={tokenCreationBlockReason}
          busy={busy !== null}
          hecEnabled={hecEnabled}
          onClose={() => {
            if (busy === null) setModal(null);
          }}
          onSubmit={(value) => void createToken(value)}
          scopeOptions={tokenScopeOptions}
          scopeSource={tokenScopeSource}
        />
      ) : null}

      {modal === "create-token" && tokenRecoveryOpen ? (
        <Modal
          title={unreadableTokenCreateRecovery !== null
            ? "Resolve damaged token recovery"
            : "Resolve token creation"}
          subtitle="New token creation is paused while Open Splunk checks; the rest of the app remains available."
          dismissible={!tokenDialogHardBlocked}
          initialFocus="#reconcile-token-create"
          onClose={() => {
            if (tokenDialogHardBlocked) return;
            setModal(null);
          }}
          footer={(
            <>
              <button className="button button--secondary" type="button" onClick={() => setModal(null)}>
                Close
              </button>
              {recoveryNeedsAuthentication ? (
                <Link className="button button--secondary" href="/signin/">Sign in</Link>
              ) : null}
              <button
                id="reconcile-token-create"
                className="button button--primary"
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
          )}
        >
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
                      <StatusLabel tone={statusTone(tokenStateLabel(candidate.state))}>{tokenStateLabel(candidate.state)}</StatusLabel>
                      <button
                        className="button button--danger"
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
                      <StatusLabel tone={statusTone(tokenStateLabel(candidate.state))}>{tokenStateLabel(candidate.state)}</StatusLabel>
                      <button
                        className="button button--danger"
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
        </Modal>
      ) : null}

      {modal === "create-token" && issuedToken !== null && !tokenRecoveryOpen ? (
        <IssuedTokenDialog
          key={issuedToken.ingestionTokenId}
          busy={busy}
          curlExample={issuedHECCurlExample}
          dismissible={!tokenDialogHardBlocked}
          issuedToken={issuedToken}
          onAcknowledge={acknowledgeIssuedToken}
          onClose={() => {
            if (!tokenDialogHardBlocked) setModal(null);
          }}
          onCopyResult={(message, success) => setToast({
            message,
            kind: success ? "success" : "warning",
          })}
          onRevoke={() => void revokeIssuedToken()}
          ownership={tokenRecoveryOwnership}
          secret={tokenSecret}
        />
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
          footer={<><button className="button button--secondary" type="button" onClick={() => { setTokenEditTarget(null); setModal(null); }} disabled={busy !== null}>Cancel</button><button className="button button--primary" type="submit" form="edit-token-form" disabled={busy !== null || !tokenHasChanges || !tokenPolicyValid || tokenName.trim().length === 0 || tokenIndexes.size === 0 || tokenScopeInvalid || (editingHECToken ? tokenHECProfileInvalid : editingNativeToken ? tokenCollectorId.length > 0 && tokenCollectorIdInvalid : true)}>{busy === `update-token-${tokenEditTarget.ingestionTokenId}` ? "Saving…" : "Save changes"}</button></>}
        >
          <form className="admin-form" id="edit-token-form" onSubmit={(event) => void updateToken(event)}>
            <label htmlFor="edit-token-name"><span>Token name</span><input id="edit-token-name" value={tokenName} onChange={(event) => setTokenName(event.target.value)} autoComplete="off" /></label>
            <label htmlFor="edit-token-description"><span>Description <small>(optional)</small></span><input id="edit-token-description" value={tokenDescription} onChange={(event) => setTokenDescription(event.target.value)} placeholder="Production collector credential" /></label>
            <div className="access-mode-notice" role="note"><span>i</span><div><strong>{tokenPurposeLabel(tokenEditTarget.purpose)} purpose</strong><p>The token purpose is immutable. {editingHECToken ? "Indexer acknowledgment mode is also fixed at creation." : editingNativeToken ? "This credential can authorize only the native collector transport." : "The server returned an unknown purpose, so transport-specific settings are unavailable."}</p></div></div>
            {editingNativeToken ? <label htmlFor="edit-token-collector-id"><span>Collector ID</span><input id="edit-token-collector-id" value={tokenCollectorId} onChange={(event) => setTokenCollectorId(event.target.value)} readOnly={tokenEditTarget.constraints?.boundCollectorId !== undefined} placeholder="Bind this legacy token once" autoComplete="off" spellCheck={false} {...fieldControlProps("edit-token-collector-id", tokenCollectorId.length > 0 && tokenCollectorIdInvalid ? COLLECTOR_ID_ERROR : null)} /><FieldNote error={tokenCollectorId.length > 0 && tokenCollectorIdInvalid ? COLLECTOR_ID_ERROR : null} fieldId="edit-token-collector-id">{tokenEditTarget.constraints?.boundCollectorId === undefined ? "This upgraded legacy token cannot use native gRPC until it is bound. Binding is one-way." : "This security binding is immutable. Rotate the token to use a different collector ID."}</FieldNote></label> : null}
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
          footer={<><button className="button button--secondary" type="button" onClick={() => setRevokeTarget(null)} disabled={busy !== null}>Keep token</button><button className="button button--danger" type="button" disabled={busy !== null || !tokenCanBeRevoked(revokeTarget)} onClick={() => void revokeToken(revokeTarget)}>{busy === `token-${revokeTarget.ingestionTokenId}` ? "Revoking…" : tokenCanBeRevoked(revokeTarget) ? "Revoke token" : `Token is ${tokenStateLabel(revokeTarget.state).toLowerCase()}`}</button></>}
        >
          <div className="access-mode-notice" role="note"><span>!</span><div><strong>This action cannot be undone</strong><p>Revoke <code>{revokeTarget.name}</code> ({revokeTarget.tokenPrefix}) scoped to {revokeTarget.constraints?.allowedIndexNames.join(", ") || "its configured indexes"}.</p></div></div>
        </Modal>
      ) : null}

      {toast === null ? null : <output className={`toast toast-${toast.kind}`}><span aria-hidden="true"><AppIcon name={toast.kind === "success" ? "check" : "warning"} size="sm" /></span><strong>{toast.message}</strong><button type="button" aria-label="Dismiss notification" onClick={() => setToast(null)}><AppIcon name="close" size="md" /></button></output>}
    </div>
  );
}
