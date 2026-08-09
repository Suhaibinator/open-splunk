"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  KNOWLEDGE_LIFECYCLE_STATE_FILTER_OPTIONS,
  KNOWLEDGE_OBJECT_TYPE_FILTER_OPTIONS,
  KNOWLEDGE_SORT_OPTIONS,
  boundedKnowledgePageSize,
  createKnowledgeReadClient,
  knowledgeLifecycleStateFilterFromControlValue,
  knowledgeObjectTypeFilterFromControlValue,
  knowledgeSortChoiceFromControlValue,
  loadKnowledgeDetail,
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
  type KnowledgeRelationshipPageDisplay,
  type KnowledgeSortChoice,
} from "./knowledge-manager-data";
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

/** Aborts the request current at cleanup time, including a later continuation. */
export function knowledgeRelationshipUnmountCleanup(
  request: AbortControllerReference,
): () => void {
  return () => {
    request.current?.abort();
    request.current = null;
  };
}

/** Forces exact-root relationship state to remount before a new detail paints. */
export function knowledgeRelationshipSectionKey(
  direction: KnowledgeRelationshipDirection,
  knowledgeObjectId: string,
  version: bigint,
): string {
  return `${direction}:${knowledgeObjectId}\0${version.toString()}`;
}

function abortKnowledgeManagerRequests(
  listRequest: AbortControllerReference,
  detailRequest: AbortControllerReference,
): void {
  listRequest.current?.abort();
  detailRequest.current?.abort();
  listRequest.current = null;
  detailRequest.current = null;
}

/** Late-bound cleanup also owns continuation/detail requests started after mount. */
export function knowledgeManagerUnmountCleanup(
  listRequest: AbortControllerReference,
  detailRequest: AbortControllerReference,
): () => void {
  return () => abortKnowledgeManagerRequests(listRequest, detailRequest);
}

/** Query identity changes discard every view derived from the prior cursor. */
export function resetKnowledgeManagerQuery(
  listRequest: AbortControllerReference,
  detailRequest: AbortControllerReference,
  consumedPageTokens: ConsumedPageTokensReference,
  resetList: () => void,
  resetDetail: () => void,
): void {
  abortKnowledgeManagerRequests(listRequest, detailRequest);
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
}: KnowledgeManagerPanelProps) {
  const client = useMemo(
    () => createKnowledgeReadClient({ baseUrl: apiBaseUrl }),
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
  const [listState, setListState] = useState<ListState>("loading");
  const [page, setPage] = useState<KnowledgePageDisplay | null>(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const [continuationStale, setContinuationStale] = useState(false);
  const [reloadGeneration, setReloadGeneration] = useState(0);
  const [selectedObjectId, setSelectedObjectId] = useState<string | null>(null);
  const [detailState, setDetailState] = useState<DetailState>("closed");
  const [detail, setDetail] = useState<KnowledgeObjectDisplay | null>(null);
  const consumedPageTokensRef = useRef(new Set<string>());
  const listRequestRef = useRef<AbortController | null>(null);
  const detailRequestRef = useRef<AbortController | null>(null);
  const rowRefs = useRef(new Map<string, HTMLButtonElement>());
  const detailRef = useRef<HTMLElement>(null);
  const focusDetailWhenReadyRef = useRef(false);

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
        setDetailState("closed");
        setDetail(null);
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
    setDetailState("closed");
    setDetail(null);
    focusDetailWhenReadyRef.current = false;
    if (restoreFocus && priorId !== null) {
      window.requestAnimationFrame(() => rowRefs.current.get(priorId)?.focus());
    }
  }, [selectedObjectId]);

  useEffect(() => {
    resetForQueryChange();
    const controller = new AbortController();
    listRequestRef.current = controller;

    void loadKnowledgePage(client, {
      appId,
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
    setDetailState("loading");
    setDetail(null);
    focusDetailWhenReadyRef.current = true;
    void loadKnowledgeDetail(client, object.knowledgeObjectId, {
      signal: controller.signal,
    }).then((result) => {
      if (controller.signal.aborted || detailRequestRef.current !== controller) return;
      detailRequestRef.current = null;
      if (result.status === "unavailable") {
        setDetailState("unavailable");
        return;
      }
      setDetail(result.object);
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
          <p>Inspect visible field knowledge. This KO-0 surface is read-only.</p>
        </div>
        <span className="knowledge-manager__readonly" aria-label="Read-only surface">Read only</span>
      </header>

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

      {listState === "loading" ? (
        <KnowledgeStatus
          kind="loading"
          title="Loading knowledge objects"
          message="Reading the first bounded catalog page…"
        />
      ) : null}
      {listState === "unavailable" ? (
        <KnowledgeStatus
          kind="unavailable"
          title="Knowledge Manager unavailable"
          message="The advertised read-only knowledge service is not available. No catalog detail was returned."
          action={<button type="button" onClick={() => setReloadGeneration((value) => value + 1)}>Retry</button>}
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
          detailExtension={detailState === "available" && detail !== null ? (
            <KnowledgeRelationships
              client={client}
              object={detail}
              pageSize={pageSize}
            />
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

interface KnowledgeManagerWorkspaceProps {
  page: KnowledgePageDisplay;
  selectedObjectId: string | null;
  loadingMore: boolean;
  continuationStale: boolean;
  detailState: DetailState;
  detail: KnowledgeObjectDisplay | null;
  detailRef?: React.Ref<HTMLElement>;
  detailExtension?: React.ReactNode;
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
          >×</button>
          {detailState === "loading" ? (
            <KnowledgeStatus
              kind="loading"
              title="Loading object details"
              message="Reading the authorized object projection…"
            />
          ) : null}
          {detailState === "unavailable" ? (
            <KnowledgeStatus
              kind="unavailable"
              title="Knowledge object unavailable"
              message="This object cannot be inspected. Missing, forbidden, corrupt, and unavailable outcomes reveal no additional detail."
            />
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
        <span><strong>{object.name}</strong><small>Response details were safely redacted.</small></span>
        <span>{object.objectTypeLabel}</span>
        <span>{object.stateLabel}</span>
        <span>{object.sharingScopeLabel}</span>
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
        <span><strong>{object.name}</strong><small>{object.appId} · v{object.version.toLocaleString()}</small></span>
        <span>{object.objectTypeLabel}</span>
        <span><i className={`knowledge-state knowledge-state--${object.stateLabel.toLowerCase()}`} />{object.stateLabel}</span>
        <span>{object.sharingScopeLabel}</span>
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
            Authored pattern, JSON path, and expression text is intentionally omitted from this initial shell.
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
  const consumedPageTokensRef = useRef(new Set<string>());

  useEffect(() => {
    const cleanup = knowledgeRelationshipUnmountCleanup(requestRef);
    requestRef.current?.abort();
    consumedPageTokensRef.current = new Set();
    setState("loading");
    setPage(null);
    setLoadingMore(false);
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
      onRetry={() => setReloadGeneration((value) => value + 1)}
      onLoadMore={() => void loadMore()}
    />
  );
}

export function KnowledgeRelationshipSectionView({
  direction,
  state,
  page,
  loadingMore,
  onRetry,
  onLoadMore,
}: {
  direction: KnowledgeRelationshipDirection;
  state: KnowledgeRelationshipState;
  page: KnowledgeRelationshipPageDisplay | null;
  loadingMore: boolean;
  onRetry: () => void;
  onLoadMore: () => void;
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
        <ol
          className="knowledge-manager__relationship-list"
          aria-label={`Visible direct ${direction}`}
        >
          {page.edges.map((edge) => (
            <li key={edge.key}>
              <code>{edge.knowledgeObjectId}</code>
              <span>v{edge.version.toLocaleString()}</span>
              <span>{edge.roleLabel}</span>
            </li>
          ))}
        </ol>
      )}
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
      <span aria-hidden="true">{kind === "loading" ? "…" : "!"}</span>
      <span className="knowledge-manager__status-copy"><strong>{title}</strong><span>{message}</span></span>
      {action}
    </output>
  );
}
