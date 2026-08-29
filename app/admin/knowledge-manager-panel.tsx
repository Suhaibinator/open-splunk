"use client";

import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react";

import type { KnowledgeObject } from "@/gen/ts/open_splunk/knowledge";

import { AppIcon, StatusIcon } from "../_components/app-icon";

import {
  KNOWLEDGE_LIFECYCLE_STATE_FILTER_OPTIONS,
  KNOWLEDGE_MANAGER_MAXIMUM_FILTER_BYTES,
  KNOWLEDGE_OBJECT_TYPE_FILTER_OPTIONS,
  KNOWLEDGE_SHARING_SCOPE_FILTER_OPTIONS,
  KNOWLEDGE_SORT_OPTIONS,
  boundedKnowledgePageSize,
  createKnowledgeMutationClient,
  createKnowledgeReadClient,
  knowledgeLifecycleStateFilterFromControlValue,
  knowledgeObjectTypeFilterFromControlValue,
  knowledgeSharingScopeFilterFromControlValue,
  knowledgeSortChoiceFromControlValue,
  knowledgeTextFilterFromDraft,
  loadKnowledgeDetail,
  loadKnowledgeMutationDetail,
  loadKnowledgePage,
  loadKnowledgeRelationshipPage,
  mergeKnowledgeContinuation,
  mergeKnowledgeRelationshipContinuation,
  type KnowledgeLifecycleStateFilter,
  type KnowledgeListItem,
  type KnowledgeObjectDisplay,
  type KnowledgeObjectTypeFilter,
  type KnowledgePageDisplay,
  type KnowledgeReadClient,
  type KnowledgeRelationshipDirection,
  type KnowledgeRelationshipEdgeDisplay,
  type KnowledgeRelationshipPageDisplay,
  type KnowledgeSharingScopeFilter,
  type KnowledgeSortChoice,
} from "./knowledge-manager-data";
import {
  KnowledgeCreateControl,
  KnowledgeObjectMutationControls,
  KnowledgeQuarantineControl,
} from "./knowledge-manager-mutations";
import { KnowledgeManagerPreview } from "./knowledge-manager-preview";
import { createKnowledgePreviewClient } from "./knowledge-manager-preview-data";
import {
  safeKnowledgeManagerAppOptions,
  type KnowledgeManagerPanelProps,
} from "./knowledge-manager-feature";

type ListState = "loading" | "available" | "unavailable";
type DetailState = "closed" | "loading" | "available" | "unavailable";

interface AbortControllerReference {
  current: AbortController | null;
}

interface ConsumedPageTokensReference {
  current: Set<string>;
}

export interface KnowledgeAdvancedFilters {
  ownerId: string | null;
  text: string | null;
  sharingScope: KnowledgeSharingScopeFilter;
  selectorText: string | null;
}

export interface KnowledgeAdvancedFilterDrafts {
  ownerId: string;
  text: string;
  sharingScope: string;
  selectorText: string;
}

export interface KnowledgeAdvancedFilterDraftNormalization {
  filters: KnowledgeAdvancedFilters | null;
  invalid: {
    ownerId: boolean;
    text: boolean;
    sharingScope: boolean;
    selectorText: boolean;
  };
}

const EMPTY_KNOWLEDGE_ADVANCED_FILTERS: KnowledgeAdvancedFilters = {
  ownerId: null,
  text: null,
  sharingScope: "all",
  selectorText: null,
};

function sameKnowledgeAdvancedFilters(
  left: KnowledgeAdvancedFilters,
  right: KnowledgeAdvancedFilters,
): boolean {
  return left.ownerId === right.ownerId
    && left.text === right.text
    && left.sharingScope === right.sharingScope
    && left.selectorText === right.selectorText;
}

export function normalizeAdvancedFilterDrafts(
  drafts: KnowledgeAdvancedFilterDrafts,
): KnowledgeAdvancedFilterDraftNormalization {
  const ownerId = knowledgeTextFilterFromDraft(drafts.ownerId);
  const text = knowledgeTextFilterFromDraft(drafts.text);
  const sharingScope = knowledgeSharingScopeFilterFromControlValue(drafts.sharingScope);
  const selectorText = knowledgeTextFilterFromDraft(drafts.selectorText);
  const invalid = {
    ownerId: ownerId === undefined,
    text: text === undefined,
    sharingScope: sharingScope === undefined,
    selectorText: selectorText === undefined,
  };
  if (
    ownerId === undefined
    || text === undefined
    || sharingScope === undefined
    || selectorText === undefined
  ) return { filters: null, invalid };
  return {
    filters: { ownerId, text, sharingScope, selectorText },
    invalid,
  };
}

/** Aborts both requests current at cleanup time, including later replacements. */
export function knowledgeRelationshipUnmountCleanup(
  relationshipRequest: AbortControllerReference,
  inspectorRequest: AbortControllerReference,
): () => void {
  return () => abortKnowledgeRequests(relationshipRequest, inspectorRequest);
}

/** Forces exact-root relationship state to remount before a new detail paints. */
export function knowledgeRelationshipSectionKey(
  direction: KnowledgeRelationshipDirection,
  knowledgeObjectId: string,
  version: bigint,
): string {
  return `${direction}:${knowledgeObjectId}\0${version.toString()}`;
}

function abortKnowledgeRequests(
  firstRequest: AbortControllerReference,
  secondRequest: AbortControllerReference,
): void {
  firstRequest.current?.abort();
  secondRequest.current?.abort();
  firstRequest.current = null;
  secondRequest.current = null;
}

/** Late-bound cleanup also owns continuation/detail requests started after mount. */
export function knowledgeManagerUnmountCleanup(
  listRequest: AbortControllerReference,
  detailRequest: AbortControllerReference,
): () => void {
  return () => abortKnowledgeRequests(listRequest, detailRequest);
}

/** Query identity changes discard every view derived from the prior cursor. */
export function resetKnowledgeManagerQuery(
  listRequest: AbortControllerReference,
  detailRequest: AbortControllerReference,
  consumedPageTokens: ConsumedPageTokensReference,
  resetList: () => void,
  resetDetail: () => void,
): void {
  abortKnowledgeRequests(listRequest, detailRequest);
  consumedPageTokens.current = new Set();
  resetList();
  resetDetail();
}

/** Invalid DOM values fail closed; repeated values do not discard a valid page. */
export function commitKnowledgeManagerQueryChange<T>(
  current: T,
  next: T | undefined,
  reset: () => void,
  update: (value: T) => void,
  failClosed: () => void,
): void {
  if (next === undefined) {
    reset();
    failClosed();
    return;
  }
  if (Object.is(current, next)) return;
  reset();
  update(next);
}

export function KnowledgeManagerPanel({
  apiBaseUrl,
  apps,
  initialAppId,
  maximumPageSize,
  quarantineAvailable,
}: KnowledgeManagerPanelProps) {
  const client = useMemo(
    () => createKnowledgeReadClient({ baseUrl: apiBaseUrl }),
    [apiBaseUrl],
  );
  const mutationClient = useMemo(
    () => createKnowledgeMutationClient({ baseUrl: apiBaseUrl }),
    [apiBaseUrl],
  );
  const previewClient = useMemo(
    () => createKnowledgePreviewClient({ baseUrl: apiBaseUrl }),
    [apiBaseUrl],
  );
  const appOptions = useMemo(() => safeKnowledgeManagerAppOptions(apps) ?? [], [apps]);
  const pageSize = boundedKnowledgePageSize(maximumPageSize);
  const initialFilter = appOptions.some((app) => app.appId === initialAppId)
    ? initialAppId
    : null;
  const [appId, setAppId] = useState<string | null>(initialFilter);
  const [objectType, setObjectType] = useState<KnowledgeObjectTypeFilter>("all");
  const [lifecycleState, setLifecycleState] =
    useState<KnowledgeLifecycleStateFilter>("all");
  const [sort, setSort] = useState<KnowledgeSortChoice>("name-ascending");
  const [advancedFilters, setAdvancedFilters] = useState<KnowledgeAdvancedFilters>(
    EMPTY_KNOWLEDGE_ADVANCED_FILTERS,
  );
  const [listState, setListState] = useState<ListState>("loading");
  const [page, setPage] = useState<KnowledgePageDisplay | null>(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const [continuationStale, setContinuationStale] = useState(false);
  const [reloadGeneration, setReloadGeneration] = useState(0);
  const [selectedObjectId, setSelectedObjectId] = useState<string | null>(null);
  const [selectedObject, setSelectedObject] = useState<KnowledgeObjectDisplay | null>(null);
  const [detailState, setDetailState] = useState<DetailState>("closed");
  const [detail, setDetail] = useState<KnowledgeObjectDisplay | null>(null);
  const [detailAuthority, setDetailAuthority] = useState<KnowledgeObject | null>(null);
  const [mutationSurfaceGeneration, setMutationSurfaceGeneration] = useState(0);
  const consumedPageTokensRef = useRef(new Set<string>());
  const listRequestRef = useRef<AbortController | null>(null);
  const detailRequestRef = useRef<AbortController | null>(null);
  const rowRefs = useRef(new Map<string, HTMLButtonElement>());
  const detailRef = useRef<HTMLElement>(null);
  const focusDetailWhenReadyRef = useRef(false);
  const advancedFilterRequestAllowedRef = useRef(true);

  useEffect(
    () => knowledgeManagerUnmountCleanup(listRequestRef, detailRequestRef),
    [],
  );

  const resetForQueryChange = useCallback(() => {
    resetKnowledgeManagerQuery(
      listRequestRef,
      detailRequestRef,
      consumedPageTokensRef,
      () => {
        setListState("loading");
        setPage(null);
        setLoadingMore(false);
        setContinuationStale(false);
      },
      () => {
        setSelectedObjectId(null);
        setSelectedObject(null);
        setDetailState("closed");
        setDetail(null);
        setDetailAuthority(null);
        setMutationSurfaceGeneration((value) => value + 1);
        rowRefs.current.clear();
        focusDetailWhenReadyRef.current = false;
      },
    );
  }, []);

  const failClosedQueryControl = useCallback(() => {
    setListState("unavailable");
  }, []);

  const closeDetail = useCallback((restoreFocus: boolean) => {
    const priorId = selectedObjectId;
    detailRequestRef.current?.abort();
    detailRequestRef.current = null;
    setSelectedObjectId(null);
    setSelectedObject(null);
    setDetailState("closed");
    setDetail(null);
    setDetailAuthority(null);
    focusDetailWhenReadyRef.current = false;
    if (restoreFocus && priorId !== null) {
      window.requestAnimationFrame(() => rowRefs.current.get(priorId)?.focus());
    }
  }, [selectedObjectId]);

  useEffect(() => {
    resetForQueryChange();
    if (!advancedFilterRequestAllowedRef.current) {
      setListState("unavailable");
      return;
    }
    const controller = new AbortController();
    listRequestRef.current = controller;

    void loadKnowledgePage(client, {
      appId,
      ...advancedFilters,
      objectType,
      lifecycleState,
      sort,
      pageSize,
      pageToken: null,
    }, { signal: controller.signal }).then((result) => {
      if (controller.signal.aborted || listRequestRef.current !== controller) return;
      listRequestRef.current = null;
      if (result.status === "unavailable") {
        setListState("unavailable");
        return;
      }
      setPage(result.page);
      setListState("available");
    });
    return () => controller.abort();
  }, [
    appId,
    advancedFilters,
    client,
    lifecycleState,
    objectType,
    pageSize,
    reloadGeneration,
    resetForQueryChange,
    sort,
  ]);

  useEffect(() => {
    if (
      !focusDetailWhenReadyRef.current
      || (detailState !== "available" && detailState !== "unavailable")
    ) return;
    focusDetailWhenReadyRef.current = false;
    window.requestAnimationFrame(() => detailRef.current?.focus());
  }, [detailState]);

  useEffect(() => {
    if (selectedObjectId === null) return;
    const handleEscape = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      event.preventDefault();
      closeDetail(true);
    };
    document.addEventListener("keydown", handleEscape);
    return () => document.removeEventListener("keydown", handleEscape);
  }, [closeDetail, selectedObjectId]);

  const openDetail = useCallback((object: KnowledgeObjectDisplay) => {
    detailRequestRef.current?.abort();
    const controller = new AbortController();
    detailRequestRef.current = controller;
    setSelectedObjectId(object.knowledgeObjectId);
    setSelectedObject(object);
    setDetailState("loading");
    setDetail(null);
    setDetailAuthority(null);
    focusDetailWhenReadyRef.current = true;
    void loadKnowledgeMutationDetail(client, {
      knowledgeObjectId: object.knowledgeObjectId,
      version: object.version,
    }, {
      signal: controller.signal,
    }).then((result) => {
      if (controller.signal.aborted || detailRequestRef.current !== controller) return;
      detailRequestRef.current = null;
      if (result.status === "unavailable") {
        setDetailState("unavailable");
        return;
      }
      setDetail(result.object);
      setDetailAuthority(result.currentKnowledgeObject);
      setDetailState("available");
    });
  }, [client]);

  const loadMore = useCallback(async () => {
    const requestedToken = page?.nextPageToken;
    if (
      page === null
      || typeof requestedToken !== "string"
      || loadingMore
      || continuationStale
      || listRequestRef.current !== null
      || consumedPageTokensRef.current.has(requestedToken)
    ) return;
    const controller = new AbortController();
    listRequestRef.current = controller;
    setLoadingMore(true);
    const result = await loadKnowledgePage(client, {
      appId,
      ...advancedFilters,
      objectType,
      lifecycleState,
      sort,
      pageSize,
      pageToken: requestedToken,
    }, { signal: controller.signal });
    if (controller.signal.aborted || listRequestRef.current !== controller) return;
    listRequestRef.current = null;
    setLoadingMore(false);
    if (result.status === "unavailable") {
      setContinuationStale(true);
      return;
    }
    const merged = mergeKnowledgeContinuation(
      page,
      result.page,
      requestedToken,
      consumedPageTokensRef.current,
    );
    if (merged.status === "stale") {
      setContinuationStale(true);
      return;
    }
    consumedPageTokensRef.current.add(requestedToken);
    setPage(merged.page);
  }, [
    appId,
    advancedFilters,
    client,
    continuationStale,
    lifecycleState,
    loadingMore,
    objectType,
    page,
    pageSize,
    sort,
  ]);

  const changeApp = useCallback((value: string) => {
    const next = value === ""
      ? null
      : appOptions.some((app) => app.appId === value) ? value : undefined;
    commitKnowledgeManagerQueryChange(
      appId,
      next,
      resetForQueryChange,
      (accepted) => setAppId(accepted),
      failClosedQueryControl,
    );
  }, [appId, appOptions, failClosedQueryControl, resetForQueryChange]);

  const changeObjectType = useCallback((value: string) => {
    commitKnowledgeManagerQueryChange(
      objectType,
      knowledgeObjectTypeFilterFromControlValue(value),
      resetForQueryChange,
      (accepted) => setObjectType(accepted),
      failClosedQueryControl,
    );
  }, [failClosedQueryControl, objectType, resetForQueryChange]);

  const changeLifecycleState = useCallback((value: string) => {
    commitKnowledgeManagerQueryChange(
      lifecycleState,
      knowledgeLifecycleStateFilterFromControlValue(value),
      resetForQueryChange,
      (accepted) => setLifecycleState(accepted),
      failClosedQueryControl,
    );
  }, [failClosedQueryControl, lifecycleState, resetForQueryChange]);

  const changeSort = useCallback((value: string) => {
    commitKnowledgeManagerQueryChange(
      sort,
      knowledgeSortChoiceFromControlValue(value),
      resetForQueryChange,
      (accepted) => setSort(accepted),
      failClosedQueryControl,
    );
  }, [failClosedQueryControl, resetForQueryChange, sort]);

  const commitAdvancedFilters = useCallback((next: KnowledgeAdvancedFilters) => {
    advancedFilterRequestAllowedRef.current = true;
    resetForQueryChange();
    setAdvancedFilters(next);
  }, [resetForQueryChange]);

  const failClosedAdvancedFilters = useCallback(() => {
    advancedFilterRequestAllowedRef.current = false;
    resetForQueryChange();
    failClosedQueryControl();
  }, [failClosedQueryControl, resetForQueryChange]);

  const retryAdvancedFilters = useCallback(() => {
    advancedFilterRequestAllowedRef.current = true;
    resetForQueryChange();
    setReloadGeneration((value) => value + 1);
  }, [resetForQueryChange]);

  const availableObjects = page?.objects.filter(
    (object): object is KnowledgeObjectDisplay => object.disclosure === "available",
  ) ?? [];

  function handleRowKeyDown(event: React.KeyboardEvent<HTMLButtonElement>, objectId: string) {
    const index = availableObjects.findIndex((object) => object.knowledgeObjectId === objectId);
    if (index < 0) return;
    let nextIndex = index;
    if (event.key === "ArrowDown") nextIndex = Math.min(availableObjects.length - 1, index + 1);
    else if (event.key === "ArrowUp") nextIndex = Math.max(0, index - 1);
    else if (event.key === "Home") nextIndex = 0;
    else if (event.key === "End") nextIndex = availableObjects.length - 1;
    else return;
    event.preventDefault();
    const next = availableObjects[nextIndex];
    if (next !== undefined) rowRefs.current.get(next.knowledgeObjectId)?.focus();
  }

  return (
    <section className="knowledge-manager" aria-labelledby="knowledge-manager-title">
      <header className="knowledge-manager__header">
        <div>
          <span className="knowledge-manager__eyebrow">ADVERTISED CAPABILITY</span>
          <h2 id="knowledge-manager-title">Knowledge Manager</h2>
          <p>Create, validate, and manage visible Tier-1 field knowledge.</p>
        </div>
        <span className="knowledge-manager__readonly" aria-label="Tier-1 management surface">Tier 1</span>
      </header>

      <KnowledgeCreateControl
        key={`create:${mutationSurfaceGeneration}`}
        client={mutationClient}
        apps={appOptions}
        initialAppId={appId}
        onCommitted={() => setReloadGeneration((value) => value + 1)}
      />

      <div className="knowledge-manager__toolbar">
        <div className="knowledge-manager__filters">
          <label htmlFor="knowledge-app-filter">
            <span>App scope</span>
            <select
              id="knowledge-app-filter"
              value={appId ?? ""}
              onChange={(event) => changeApp(event.currentTarget.value)}
              disabled={listState === "loading"}
            >
              <option value="">All readable apps</option>
              {appOptions.map((app) => (
                <option value={app.appId} key={app.appId}>{app.label}</option>
              ))}
            </select>
          </label>
          <label htmlFor="knowledge-object-type-filter">
            <span>Object type</span>
            <select
              id="knowledge-object-type-filter"
              value={objectType}
              onChange={(event) => changeObjectType(event.currentTarget.value)}
              disabled={listState === "loading"}
            >
              {KNOWLEDGE_OBJECT_TYPE_FILTER_OPTIONS.map((option) => (
                <option value={option.value} key={option.value}>{option.label}</option>
              ))}
            </select>
          </label>
          <label htmlFor="knowledge-lifecycle-state-filter">
            <span>Lifecycle state</span>
            <select
              id="knowledge-lifecycle-state-filter"
              value={lifecycleState}
              onChange={(event) => changeLifecycleState(event.currentTarget.value)}
              disabled={listState === "loading"}
            >
              {KNOWLEDGE_LIFECYCLE_STATE_FILTER_OPTIONS.map((option) => (
                <option value={option.value} key={option.value}>{option.label}</option>
              ))}
            </select>
          </label>
          <label htmlFor="knowledge-sort-choice">
            <span>Sort by</span>
            <select
              id="knowledge-sort-choice"
              value={sort}
              onChange={(event) => changeSort(event.currentTarget.value)}
              disabled={listState === "loading"}
            >
              {KNOWLEDGE_SORT_OPTIONS.map((option) => (
                <option value={option.value} key={option.value}>{option.label}</option>
              ))}
            </select>
          </label>
        </div>
        {page === null ? null : (
          <p aria-live="polite">
            <strong>{page.totalSize.toLocaleString()}</strong> visible object{page.totalSize === 1n ? "" : "s"}
            <span>Catalog revision {page.tenantCatalogRevision.toLocaleString()}</span>
          </p>
        )}
      </div>

      <KnowledgeAdvancedFilterForm
        committed={advancedFilters}
        unavailable={listState === "unavailable"}
        onCommit={commitAdvancedFilters}
        onFailClosed={failClosedAdvancedFilters}
        onRetry={retryAdvancedFilters}
      />

      {listState === "loading" ? (
        <KnowledgeStatus
          kind="loading"
          title="Loading knowledge objects"
          message="Reading the first bounded catalog page…"
        />
      ) : null}

      {listState === "available" && page !== null ? (
        <KnowledgeManagerWorkspace
          page={page}
          selectedObjectId={selectedObjectId}
          loadingMore={loadingMore}
          continuationStale={continuationStale}
          detailState={detailState}
          detail={detail}
          detailRef={detailRef}
          unavailableDetailExtension={!quarantineAvailable || selectedObject === null ? null : (
            <KnowledgeQuarantineControl
              key={`quarantine:${selectedObject.knowledgeObjectId}:${selectedObject.version.toString()}`}
              client={mutationClient}
              knowledgeObjectId={selectedObject.knowledgeObjectId}
              name={selectedObject.name}
              state={selectedObject.state}
              onCommitted={() => {
                closeDetail(false);
                setReloadGeneration((value) => value + 1);
              }}
            />
          )}
          detailExtension={detailState === "available"
            && detail !== null
            && detailAuthority !== null ? (
            <>
              <KnowledgeObjectMutationControls
                key={`${detailAuthority.knowledgeObjectId}:${detailAuthority.version.toString()}`}
                client={mutationClient}
                apps={appOptions}
                currentKnowledgeObject={detailAuthority}
                onCommitted={() => {
                  closeDetail(false);
                  setReloadGeneration((value) => value + 1);
                }}
              />
              {quarantineAvailable ? (
                <KnowledgeQuarantineControl
                  key={`quarantine:${detail.knowledgeObjectId}:${detail.version.toString()}`}
                  client={mutationClient}
                  knowledgeObjectId={detail.knowledgeObjectId}
                  name={detail.name}
                  state={detail.state}
                  onCommitted={() => {
                    closeDetail(false);
                    setReloadGeneration((value) => value + 1);
                  }}
                />
              ) : null}
              <KnowledgeManagerPreview
                client={previewClient}
                currentKnowledgeObject={detailAuthority}
              />
              <KnowledgeRelationships
                client={client}
                object={detail}
                pageSize={pageSize}
              />
            </>
          ) : null}
          onOpen={openDetail}
          onRowKeyDown={handleRowKeyDown}
          registerRow={(objectId, element) => {
            if (element === null) rowRefs.current.delete(objectId);
            else rowRefs.current.set(objectId, element);
          }}
          onReloadFirstPage={() => setReloadGeneration((value) => value + 1)}
          onLoadMore={() => void loadMore()}
          onCloseDetail={() => closeDetail(true)}
        />
      ) : null}
    </section>
  );
}

interface KnowledgeAdvancedFilterFormProps {
  committed: KnowledgeAdvancedFilters;
  unavailable: boolean;
  onCommit: (filters: KnowledgeAdvancedFilters) => void;
  onFailClosed: () => void;
  onRetry: () => void;
}

function knowledgeAdvancedFilterDraftsFromCommitted(
  filters: KnowledgeAdvancedFilters,
): KnowledgeAdvancedFilterDrafts {
  return {
    ownerId: filters.ownerId ?? "",
    text: filters.text ?? "",
    sharingScope: filters.sharingScope,
    selectorText: filters.selectorText ?? "",
  };
}

function KnowledgeAdvancedFilterForm({
  committed,
  unavailable,
  onCommit,
  onFailClosed,
  onRetry,
}: KnowledgeAdvancedFilterFormProps) {
  const [drafts, setDrafts] = useState<KnowledgeAdvancedFilterDrafts>(
    () => knowledgeAdvancedFilterDraftsFromCommitted(committed),
  );
  const [status, setStatus] = useState("No advanced filters applied.");
  const [validationAttempted, setValidationAttempted] = useState(false);
  const [failClosed, setFailClosed] = useState(false);
  const normalized = normalizeAdvancedFilterDrafts(drafts);
  const invalidAttempt = validationAttempted && normalized.filters === null;
  const draftMatchesCommitted = !failClosed
    && normalized.filters !== null
    && sameKnowledgeAdvancedFilters(committed, normalized.filters);

  function updateTextDraft(
    field: "ownerId" | "text" | "selectorText",
    value: string,
  ): void {
    setDrafts((current) => ({ ...current, [field]: value }));
    setStatus("Draft filters not applied.");
  }

  function updateSharingScopeDraft(value: string): void {
    setDrafts((current) => ({ ...current, sharingScope: value }));
    if (knowledgeSharingScopeFilterFromControlValue(value) === undefined) {
      setValidationAttempted(true);
      setFailClosed(true);
      setStatus("The sharing scope filter is invalid.");
      onFailClosed();
      return;
    }
    setStatus("Draft filters not applied.");
  }

  function apply(event: React.FormEvent<HTMLFormElement>): void {
    event.preventDefault();
    setValidationAttempted(true);
    if (normalized.filters === null) {
      setFailClosed(true);
      setStatus(
        `Advanced filters must be valid, control-free UTF-8 text no longer than ${KNOWLEDGE_MANAGER_MAXIMUM_FILTER_BYTES} bytes.`,
      );
      onFailClosed();
      return;
    }
    const next = normalized.filters;
    setDrafts(knowledgeAdvancedFilterDraftsFromCommitted(next));
    setValidationAttempted(false);
    setFailClosed(false);
    setStatus("Advanced filters applied.");
    if (sameKnowledgeAdvancedFilters(committed, next)) {
      if (unavailable) onRetry();
      return;
    }
    onCommit(next);
  }

  function clear(): void {
    setDrafts(knowledgeAdvancedFilterDraftsFromCommitted(
      EMPTY_KNOWLEDGE_ADVANCED_FILTERS,
    ));
    setValidationAttempted(false);
    setFailClosed(false);
    setStatus("Advanced filters cleared.");
    if (sameKnowledgeAdvancedFilters(committed, EMPTY_KNOWLEDGE_ADVANCED_FILTERS)) {
      if (unavailable) onRetry();
      return;
    }
    onCommit(EMPTY_KNOWLEDGE_ADVANCED_FILTERS);
  }

  return (
    <>
      <form
        className="knowledge-manager__advanced-filters"
        aria-labelledby="knowledge-advanced-filters-title"
        onSubmit={apply}
        autoComplete="off"
        noValidate
      >
        <fieldset>
          <legend id="knowledge-advanced-filters-title">Advanced filters</legend>
          <div className="knowledge-manager__advanced-filter-grid">
            <label htmlFor="knowledge-owner-filter">
              <span>Owner ID</span>
              <input
                id="knowledge-owner-filter"
                value={drafts.ownerId}
                maxLength={KNOWLEDGE_MANAGER_MAXIMUM_FILTER_BYTES}
                autoComplete="off"
                aria-describedby="knowledge-advanced-filter-status"
                aria-invalid={validationAttempted && normalized.invalid.ownerId || undefined}
                onChange={(event) => updateTextDraft("ownerId", event.currentTarget.value)}
              />
            </label>
            <label htmlFor="knowledge-text-filter">
              <span>Name or description</span>
              <input
                id="knowledge-text-filter"
                value={drafts.text}
                maxLength={KNOWLEDGE_MANAGER_MAXIMUM_FILTER_BYTES}
                autoComplete="off"
                aria-describedby="knowledge-advanced-filter-status"
                aria-invalid={validationAttempted && normalized.invalid.text || undefined}
                onChange={(event) => updateTextDraft("text", event.currentTarget.value)}
              />
            </label>
            <label htmlFor="knowledge-sharing-scope-filter">
              <span>Sharing scope</span>
              <select
                id="knowledge-sharing-scope-filter"
                value={drafts.sharingScope}
                aria-describedby="knowledge-advanced-filter-status"
                aria-invalid={validationAttempted && normalized.invalid.sharingScope || undefined}
                onChange={(event) => updateSharingScopeDraft(event.currentTarget.value)}
              >
                {KNOWLEDGE_SHARING_SCOPE_FILTER_OPTIONS.map((option) => (
                  <option value={option.value} key={option.value}>{option.label}</option>
                ))}
              </select>
            </label>
            <label htmlFor="knowledge-selector-text-filter">
              <span>Selector text</span>
              <input
                id="knowledge-selector-text-filter"
                value={drafts.selectorText}
                maxLength={KNOWLEDGE_MANAGER_MAXIMUM_FILTER_BYTES}
                autoComplete="off"
                aria-describedby="knowledge-advanced-filter-status"
                aria-invalid={validationAttempted && normalized.invalid.selectorText || undefined}
                onChange={(event) => updateTextDraft("selectorText", event.currentTarget.value)}
              />
            </label>
          </div>
          <div className="knowledge-manager__advanced-filter-actions">
            <output
              id="knowledge-advanced-filter-status"
              role={invalidAttempt ? "alert" : "status"}
              aria-live="polite"
            >{status}</output>
            <span>
              <button type="submit">Apply filters</button>
              <button type="button" onClick={clear}>Clear filters</button>
            </span>
          </div>
        </fieldset>
      </form>
      {unavailable ? (
        <KnowledgeStatus
          kind="unavailable"
          title="Knowledge Manager unavailable"
          message="The advertised read-only knowledge service is not available. No catalog detail was returned."
          action={draftMatchesCommitted ? (
            <button type="button" onClick={onRetry}>Retry</button>
          ) : undefined}
        />
      ) : null}
    </>
  );
}

interface KnowledgeManagerWorkspaceProps {
  page: KnowledgePageDisplay;
  selectedObjectId: string | null;
  loadingMore: boolean;
  continuationStale: boolean;
  detailState: DetailState;
  detail: KnowledgeObjectDisplay | null;
  detailRef?: React.Ref<HTMLElement>;
  detailExtension?: React.ReactNode;
  unavailableDetailExtension?: React.ReactNode;
  onOpen: (object: KnowledgeObjectDisplay) => void;
  onRowKeyDown: (event: React.KeyboardEvent<HTMLButtonElement>, objectId: string) => void;
  registerRow: (objectId: string, element: HTMLButtonElement | null) => void;
  onReloadFirstPage: () => void;
  onLoadMore: () => void;
  onCloseDetail: () => void;
}

/** Pure read-only presentation kept separate so every rendered state is directly testable. */
export function KnowledgeManagerWorkspace({
  page,
  selectedObjectId,
  loadingMore,
  continuationStale,
  detailState,
  detail,
  detailRef,
  detailExtension,
  unavailableDetailExtension,
  onOpen,
  onRowKeyDown,
  registerRow,
  onReloadFirstPage,
  onLoadMore,
  onCloseDetail,
}: KnowledgeManagerWorkspaceProps) {
  return (
    <div className={`knowledge-manager__workspace${selectedObjectId === null ? "" : " knowledge-manager__workspace--detail"}`}>
      <div className="knowledge-manager__list-panel" aria-busy={loadingMore}>
        <div className="knowledge-manager__list-heading" aria-hidden="true">
          <span>Object</span><span>Type</span><span>State</span><span>Scope</span>
        </div>
        {page.objects.length === 0 ? (
          <div className="knowledge-manager__empty" aria-live="polite">
            <span aria-hidden="true">◇</span>
            <h3>No knowledge objects</h3>
            <p>No objects match the selected filters.</p>
          </div>
        ) : (
          <ol className="knowledge-manager__list" aria-label="Knowledge objects">
            {page.objects.map((object) => (
              <KnowledgeListRow
                object={object}
                selected={object.disclosure === "available" && object.knowledgeObjectId === selectedObjectId}
                onOpen={onOpen}
                onKeyDown={onRowKeyDown}
                registerRow={registerRow}
                key={object.key}
              />
            ))}
          </ol>
        )}

        {continuationStale ? (
          <div className="knowledge-manager__stale" role="alert">
            <div>
              <strong>Catalog page changed</strong>
              <p>The continuation is no longer safe to combine with this snapshot.</p>
            </div>
            <button type="button" onClick={onReloadFirstPage}>
              Reload first page
            </button>
          </div>
        ) : page.nextPageToken === null ? null : (
          <div className="knowledge-manager__pagination">
            <p>{page.objects.length.toLocaleString()} of {page.totalSize.toLocaleString()} loaded</p>
            <button type="button" onClick={onLoadMore} disabled={loadingMore}>
              {loadingMore ? "Loading next page…" : "Load next page"}
            </button>
          </div>
        )}
      </div>

      {selectedObjectId === null ? null : (
        <section
          className="knowledge-manager__detail"
          id="knowledge-object-detail"
          ref={detailRef}
          aria-labelledby={detailState === "available" ? "knowledge-object-detail-title" : undefined}
          aria-label={detailState === "available" ? undefined : "Knowledge object details"}
          tabIndex={-1}
        >
          <button
            className="knowledge-manager__detail-close"
            type="button"
            aria-label="Close knowledge object details"
            onClick={onCloseDetail}
          ><AppIcon name="close" size="md" /></button>
          {detailState === "loading" ? (
            <KnowledgeStatus
              kind="loading"
              title="Loading object details"
              message="Reading the authorized object projection…"
            />
          ) : null}
          {detailState === "unavailable" ? (
            <>
              <KnowledgeStatus
                kind="unavailable"
                title="Knowledge object unavailable"
                message="This object cannot be inspected. Missing, forbidden, corrupt, and unavailable outcomes reveal no additional detail."
              />
              {unavailableDetailExtension}
            </>
          ) : null}
          {detailState === "available" && detail !== null ? (
            <KnowledgeDetail object={detail}>{detailExtension}</KnowledgeDetail>
          ) : null}
        </section>
      )}
    </div>
  );
}

interface KnowledgeListRowProps {
  object: KnowledgeListItem;
  selected: boolean;
  onOpen: (object: KnowledgeObjectDisplay) => void;
  onKeyDown: (event: React.KeyboardEvent<HTMLButtonElement>, objectId: string) => void;
  registerRow: (objectId: string, element: HTMLButtonElement | null) => void;
}

function KnowledgeListRow({
  object,
  selected,
  onOpen,
  onKeyDown,
  registerRow,
}: KnowledgeListRowProps) {
  if (object.disclosure === "redacted") {
    return (
      <li className="knowledge-manager__row knowledge-manager__row--redacted" aria-label="Unavailable knowledge object">
        <span data-label="Object"><strong>{object.name}</strong><small>Response details were safely redacted.</small></span>
        <span data-label="Type">{object.objectTypeLabel}</span>
        <span data-label="State">{object.stateLabel}</span>
        <span data-label="Scope">{object.sharingScopeLabel}</span>
      </li>
    );
  }
  return (
    <li>
      <button
        className={`knowledge-manager__row${selected ? " knowledge-manager__row--selected" : ""}`}
        type="button"
        aria-controls="knowledge-object-detail"
        aria-expanded={selected}
        onClick={() => onOpen(object)}
        onKeyDown={(event) => onKeyDown(event, object.knowledgeObjectId)}
        ref={(element) => registerRow(object.knowledgeObjectId, element)}
      >
        <span data-label="Object"><strong>{object.name}</strong><small>{object.appId} · v{object.version.toLocaleString()}</small></span>
        <span data-label="Type">{object.objectTypeLabel}</span>
        <span data-label="State"><i className={`knowledge-state knowledge-state--${object.stateLabel.toLowerCase()}`} />{object.stateLabel}</span>
        <span data-label="Scope">{object.sharingScopeLabel}</span>
      </button>
    </li>
  );
}

export function KnowledgeDetail({
  object,
  children,
}: {
  object: KnowledgeObjectDisplay;
  children?: React.ReactNode;
}) {
  return (
    <div className="knowledge-manager__detail-body">
      <header>
        <span>{object.objectTypeLabel}</span>
        <h3 id="knowledge-object-detail-title">{object.name}</h3>
        <p>{object.definition.description ?? "No description provided."}</p>
        {object.definition.descriptionTruncated ? <small>Description shortened for display.</small> : null}
      </header>
      <dl className="knowledge-manager__metadata">
        <div><dt>State</dt><dd>{object.stateLabel}</dd></div>
        <div><dt>Sharing</dt><dd>{object.sharingScopeLabel}</dd></div>
        <div><dt>App</dt><dd>{object.appId}</dd></div>
        <div><dt>Owner</dt><dd>{object.ownerId}</dd></div>
        <div><dt>Version</dt><dd>{object.version.toLocaleString()}</dd></div>
        <div><dt>Updated</dt><dd>{object.updatedAt.toLocaleString()}</dd></div>
      </dl>

      <section className="knowledge-manager__definition" aria-labelledby="knowledge-definition-title">
        <h4 id="knowledge-definition-title">Definition summary</h4>
        <p className={`knowledge-definition-status knowledge-definition-status--${object.definition.status}`}>
          {object.definition.bodyKind}
        </p>
        {object.definition.bodyFields.length === 0 ? null : (
          <dl>
            {object.definition.bodyFields.map((field) => (
              <div key={field.label}><dt>{field.label}</dt><dd>{field.value}</dd></div>
            ))}
            {object.definition.overwriteBehavior === null ? null : (
              <div><dt>On collision</dt><dd>{object.definition.overwriteBehavior}</dd></div>
            )}
          </dl>
        )}
        {object.definition.status === "recognized" ? (
          <p className="knowledge-manager__safe-note">
            Authored pattern, JSON path, and expression text is shown only inside the explicit editor.
          </p>
        ) : null}
      </section>

      <section className="knowledge-manager__selectors" aria-labelledby="knowledge-selectors-title">
        <h4 id="knowledge-selectors-title">Selectors</h4>
        {object.definition.selectors.length === 0 ? <p>Applies without selector restrictions.</p> : (
          object.definition.selectors.map((selector) => (
            <div key={selector.dimension}>
              <strong>{selector.dimension}</strong>
              <ul>
                {selector.patterns.map((pattern) => <li key={pattern}><code>{pattern}</code></li>)}
              </ul>
            </div>
          ))
        )}
      </section>
      {children}
    </div>
  );
}

type KnowledgeRelationshipState = "loading" | "available" | "unavailable" | "stale";
export type KnowledgeRelatedObjectInspector =
  | { state: "closed" }
  | { state: "loading" | "unavailable"; edge: KnowledgeRelationshipEdgeDisplay }
  | {
    state: "available";
    edge: KnowledgeRelationshipEdgeDisplay;
    object: KnowledgeObjectDisplay;
  };

function sameKnowledgeRelationshipEdge(
  left: KnowledgeRelationshipEdgeDisplay,
  right: KnowledgeRelationshipEdgeDisplay,
): boolean {
  return left.knowledgeObjectId === right.knowledgeObjectId
    && left.version === right.version;
}

function KnowledgeRelationships({
  client,
  object,
  pageSize,
}: {
  client: KnowledgeReadClient;
  object: KnowledgeObjectDisplay;
  pageSize: number;
}) {
  return (
    <section
      className="knowledge-manager__relationships"
      aria-labelledby="knowledge-relationships-title"
    >
      <header>
        <h4 id="knowledge-relationships-title">Direct relationships</h4>
        <p>Counts include only currently visible direct relationships.</p>
      </header>
      <KnowledgeRelationshipSection
        key={knowledgeRelationshipSectionKey(
          "dependencies",
          object.knowledgeObjectId,
          object.version,
        )}
        client={client}
        direction="dependencies"
        knowledgeObjectId={object.knowledgeObjectId}
        version={object.version}
        pageSize={pageSize}
      />
      <KnowledgeRelationshipSection
        key={knowledgeRelationshipSectionKey(
          "dependents",
          object.knowledgeObjectId,
          object.version,
        )}
        client={client}
        direction="dependents"
        knowledgeObjectId={object.knowledgeObjectId}
        version={object.version}
        pageSize={pageSize}
      />
    </section>
  );
}

function KnowledgeRelationshipSection({
  client,
  direction,
  knowledgeObjectId,
  version,
  pageSize,
}: {
  client: KnowledgeReadClient;
  direction: KnowledgeRelationshipDirection;
  knowledgeObjectId: string;
  version: bigint;
  pageSize: number;
}) {
  const [state, setState] = useState<KnowledgeRelationshipState>("loading");
  const [page, setPage] = useState<KnowledgeRelationshipPageDisplay | null>(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const [reloadGeneration, setReloadGeneration] = useState(0);
  const requestRef = useRef<AbortController | null>(null);
  const inspectorRequestRef = useRef<AbortController | null>(null);
  const inspectorEdgeRef = useRef<KnowledgeRelationshipEdgeDisplay | null>(null);
  const inspectorTriggerRef = useRef<HTMLButtonElement | null>(null);
  const consumedPageTokensRef = useRef(new Set<string>());
  const [inspector, setInspector] = useState<KnowledgeRelatedObjectInspector>({
    state: "closed",
  });

  const closeInspector = useCallback(() => {
    inspectorRequestRef.current?.abort();
    inspectorRequestRef.current = null;
    inspectorEdgeRef.current = null;
    inspectorTriggerRef.current = null;
    setInspector({ state: "closed" });
  }, []);

  const startInspectorRequest = useCallback((edge: KnowledgeRelationshipEdgeDisplay) => {
    inspectorRequestRef.current?.abort();
    const controller = new AbortController();
    inspectorRequestRef.current = controller;
    inspectorEdgeRef.current = edge;
    setInspector({ state: "loading", edge });
    void loadKnowledgeDetail(client, {
      knowledgeObjectId: edge.knowledgeObjectId,
      version: edge.version,
    }, { signal: controller.signal }).then((result) => {
      if (
        controller.signal.aborted
        || inspectorRequestRef.current !== controller
      ) return;
      inspectorRequestRef.current = null;
      if (result.status === "unavailable") {
        setInspector({ state: "unavailable", edge });
        return;
      }
      setInspector({ state: "available", edge, object: result.object });
    });
  }, [client]);

  const inspectEdge = useCallback((
    edge: KnowledgeRelationshipEdgeDisplay,
    trigger: HTMLButtonElement,
  ) => {
    inspectorTriggerRef.current = trigger;
    const current = inspectorEdgeRef.current;
    if (current !== null && sameKnowledgeRelationshipEdge(current, edge)) {
      closeInspector();
      return;
    }
    startInspectorRequest(edge);
  }, [closeInspector, startInspectorRequest]);

  const retryInspector = useCallback(() => {
    const edge = inspectorEdgeRef.current;
    if (edge === null) return;
    inspectorTriggerRef.current?.focus();
    startInspectorRequest(edge);
  }, [startInspectorRequest]);

  useEffect(() => {
    const cleanup = knowledgeRelationshipUnmountCleanup(requestRef, inspectorRequestRef);
    abortKnowledgeRequests(requestRef, inspectorRequestRef);
    inspectorEdgeRef.current = null;
    inspectorTriggerRef.current = null;
    consumedPageTokensRef.current = new Set();
    setState("loading");
    setPage(null);
    setLoadingMore(false);
    setInspector({ state: "closed" });
    const controller = new AbortController();
    requestRef.current = controller;
    void loadKnowledgeRelationshipPage(client, {
      direction,
      knowledgeObjectId,
      version,
      pageSize,
      pageToken: null,
    }, { signal: controller.signal }).then((result) => {
      if (controller.signal.aborted || requestRef.current !== controller) return;
      requestRef.current = null;
      if (result.status === "unavailable") {
        setState("unavailable");
        return;
      }
      setPage(result.page);
      setState("available");
    });
    return cleanup;
  }, [client, direction, knowledgeObjectId, pageSize, reloadGeneration, version]);

  const loadMore = useCallback(async () => {
    const requestedPageToken = page?.nextPageToken;
    if (
      state !== "available"
      || page === null
      || typeof requestedPageToken !== "string"
      || loadingMore
      || requestRef.current !== null
      || consumedPageTokensRef.current.has(requestedPageToken)
    ) return;
    const controller = new AbortController();
    requestRef.current = controller;
    setLoadingMore(true);
    const result = await loadKnowledgeRelationshipPage(client, {
      direction,
      knowledgeObjectId,
      version,
      pageSize,
      pageToken: requestedPageToken,
    }, { signal: controller.signal });
    if (controller.signal.aborted || requestRef.current !== controller) return;
    requestRef.current = null;
    setLoadingMore(false);
    if (result.status === "unavailable") {
      setState("stale");
      return;
    }
    const merged = mergeKnowledgeRelationshipContinuation(
      page,
      result.page,
      requestedPageToken,
      consumedPageTokensRef.current,
    );
    if (merged.status === "stale") {
      setState("stale");
      return;
    }
    consumedPageTokensRef.current.add(requestedPageToken);
    setPage(merged.page);
  }, [
    client,
    direction,
    knowledgeObjectId,
    loadingMore,
    page,
    pageSize,
    state,
    version,
  ]);

  return (
    <KnowledgeRelationshipSectionView
      direction={direction}
      state={state}
      page={page}
      loadingMore={loadingMore}
      inspector={inspector}
      onRetry={() => setReloadGeneration((value) => value + 1)}
      onLoadMore={() => void loadMore()}
      onInspect={inspectEdge}
      onRetryInspector={retryInspector}
    />
  );
}

export function KnowledgeRelationshipSectionView({
  direction,
  state,
  page,
  loadingMore,
  inspector,
  onRetry,
  onLoadMore,
  onInspect,
  onRetryInspector,
}: {
  direction: KnowledgeRelationshipDirection;
  state: KnowledgeRelationshipState;
  page: KnowledgeRelationshipPageDisplay | null;
  loadingMore: boolean;
  inspector: KnowledgeRelatedObjectInspector;
  onRetry: () => void;
  onLoadMore: () => void;
  onInspect: (edge: KnowledgeRelationshipEdgeDisplay, trigger: HTMLButtonElement) => void;
  onRetryInspector: () => void;
}) {
  const dependencies = direction === "dependencies";
  const title = dependencies ? "Dependencies" : "Dependents";
  const headingId = `knowledge-${direction}-title`;
  return (
    <section
      className="knowledge-manager__relationship-section"
      aria-labelledby={headingId}
      aria-busy={state === "loading" || loadingMore}
    >
      <header>
        <h5 id={headingId}>{title}</h5>
        {page === null ? null : (
          <span>
            {page.totalSize.toLocaleString()} visible · revision {page.tenantCatalogRevision.toLocaleString()}
          </span>
        )}
      </header>
      <p>
        {dependencies
          ? "Exact object versions read directly by this version."
          : "Current object versions that refer directly to this version."}
      </p>
      {state === "loading" ? (
        <output className="knowledge-manager__relationship-message">
          Loading visible {direction}…
        </output>
      ) : null}
      {state === "unavailable" ? (
        <output className="knowledge-manager__relationship-message">
          <span>Relationship data is unavailable.</span>
          <button type="button" onClick={onRetry}>Retry</button>
        </output>
      ) : null}
      {page !== null && page.edges.length === 0 && state !== "loading" ? (
        <p className="knowledge-manager__relationship-message" aria-live="polite">
          No visible direct {direction}.
        </p>
      ) : null}
      {page === null || page.edges.length === 0 ? null : (
        <KnowledgeRelationshipList
          direction={direction}
          edges={page.edges}
          inspectedEdge={inspector.state === "closed" ? null : inspector.edge}
          onInspect={onInspect}
        />
      )}
      <KnowledgeRelatedObjectInspectorView
        direction={direction}
        inspector={inspector}
        onRetry={onRetryInspector}
      />
      {state === "stale" ? (
        <div className="knowledge-manager__relationship-stale" role="alert">
          <span>This relationship page cannot be safely continued.</span>
          <button type="button" onClick={onRetry}>Reload {title.toLowerCase()}</button>
        </div>
      ) : page?.nextPageToken === null || page === null ? null : (
        <div className="knowledge-manager__relationship-pagination" aria-live="polite">
          <span>{page.edges.length.toLocaleString()} of {page.totalSize.toLocaleString()} loaded</span>
          <button type="button" onClick={onLoadMore} disabled={loadingMore}>
            {loadingMore ? "Loading…" : `Load more ${direction}`}
          </button>
        </div>
      )}
    </section>
  );
}

const KnowledgeRelationshipList = memo(function KnowledgeRelationshipList({
  direction,
  edges,
  inspectedEdge,
  onInspect,
}: {
  direction: KnowledgeRelationshipDirection;
  edges: KnowledgeRelationshipEdgeDisplay[];
  inspectedEdge: KnowledgeRelationshipEdgeDisplay | null;
  onInspect: (edge: KnowledgeRelationshipEdgeDisplay, trigger: HTMLButtonElement) => void;
}) {
  const inspectorId = `knowledge-${direction}-related-object-inspector`;
  const endpointLabel = direction === "dependencies" ? "dependency" : "dependent";
  return (
    <ol
      className="knowledge-manager__relationship-list"
      aria-label={`Visible direct ${direction}`}
    >
      {edges.map((edge) => {
        const expanded = inspectedEdge !== null
          && sameKnowledgeRelationshipEdge(inspectedEdge, edge);
        return (
          <li key={edge.key}>
            <code>{edge.knowledgeObjectId}</code>
            <span>v{edge.version.toLocaleString()}</span>
            <button
              className="knowledge-manager__relationship-inspect"
              type="button"
              aria-label={`${expanded ? "Close" : "Inspect"} ${endpointLabel} ${edge.knowledgeObjectId}, version ${edge.version.toString()}`}
              aria-controls={expanded ? inspectorId : undefined}
              aria-expanded={expanded}
              onClick={(event) => onInspect(edge, event.currentTarget)}
            >
              {expanded ? "Close" : "Inspect"}
            </button>
            <span className="knowledge-manager__relationship-role">{edge.roleLabel}</span>
          </li>
        );
      })}
    </ol>
  );
});

/** Compact projection for one explicitly requested endpoint; it never nests graph state. */
export function KnowledgeRelatedObjectInspectorView({
  direction,
  inspector,
  onRetry,
}: {
  direction: KnowledgeRelationshipDirection;
  inspector: KnowledgeRelatedObjectInspector;
  onRetry: () => void;
}) {
  if (inspector.state === "closed") return null;
  const { edge } = inspector;
  const inspectorId = `knowledge-${direction}-related-object-inspector`;
  const headingId = `${inspectorId}-title`;
  const exactObject = inspector.state === "available"
    && inspector.object.knowledgeObjectId === edge.knowledgeObjectId
    && inspector.object.version === edge.version
    ? inspector.object
    : null;
  const unavailable = inspector.state === "unavailable"
    || (inspector.state === "available" && exactObject === null);
  const relationshipLabel = direction === "dependencies" ? "Dependency" : "Dependent";
  return (
    <section
      className="knowledge-manager__related-inspector"
      id={inspectorId}
      aria-labelledby={headingId}
      aria-busy={inspector.state === "loading"}
      aria-live="polite"
    >
      <header>
        <span>{relationshipLabel}</span>
        <h6 id={headingId}>{relationshipLabel} object inspector</h6>
      </header>
      <p className="knowledge-manager__related-identity">
        <code>{edge.knowledgeObjectId}</code>
        <span>v{edge.version.toLocaleString()}</span>
      </p>
      {inspector.state === "loading" ? (
        <output className="knowledge-manager__related-status">
          Loading related object…
        </output>
      ) : null}
      {unavailable ? (
        <output className="knowledge-manager__related-status knowledge-manager__related-status--unavailable">
          <span>Related object unavailable. This object cannot be inspected.</span>
          <button
            type="button"
            aria-label={`Retry ${relationshipLabel.toLowerCase()} ${edge.knowledgeObjectId}, version ${edge.version.toString()}`}
            onClick={onRetry}
          >
            Retry
          </button>
        </output>
      ) : null}
      {exactObject === null ? null : (
        <div className="knowledge-manager__related-object">
          <strong>{exactObject.name}</strong>
          <p>{exactObject.definition.description ?? "No description provided."}</p>
          {exactObject.definition.descriptionTruncated ? (
            <small>Description shortened for display.</small>
          ) : null}
          <dl>
            <div><dt>Type</dt><dd>{exactObject.objectTypeLabel}</dd></div>
            <div><dt>State</dt><dd>{exactObject.stateLabel}</dd></div>
            <div><dt>Sharing</dt><dd>{exactObject.sharingScopeLabel}</dd></div>
            <div><dt>App</dt><dd>{exactObject.appId}</dd></div>
            <div><dt>Owner</dt><dd>{exactObject.ownerId}</dd></div>
            <div><dt>Updated</dt><dd>{exactObject.updatedAt.toLocaleString()}</dd></div>
          </dl>
        </div>
      )}
    </section>
  );
}

function KnowledgeStatus({
  kind,
  title,
  message,
  action,
}: {
  kind: "loading" | "unavailable";
  title: string;
  message: string;
  action?: React.ReactNode;
}) {
  return (
    <output className={`knowledge-manager__status knowledge-manager__status--${kind}`}>
      <StatusIcon tone={kind === "loading" ? "info" : "error"} icon={kind === "loading" ? "loading" : "warning"} spin={kind === "loading"} />
      <span className="knowledge-manager__status-copy"><strong>{title}</strong><span>{message}</span></span>
      {action}
    </output>
  );
}
