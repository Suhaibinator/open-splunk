"use client";

import { useCallback, useEffect, useMemo, useRef, useState, type KeyboardEvent } from "react";
import Link from "next/link";

import { SharingScope, SortDirection } from "@/gen/ts/open_splunk/common";
import { SavedSearchSortBy } from "@/gen/ts/open_splunk/saved_search_api";
import { ServerFeature } from "@/gen/ts/open_splunk/system_api";
import {
  createOpenSplunkApiClient,
  getSystemBootstrap,
  recordNextPageToken,
  RepeatedPageCursorError,
  type SystemBootstrapModel,
} from "@/lib/api";
import { supportsServerFeature } from "@/lib/api/system-bootstrap";
import { createErrorMessage } from "@/lib/error-message";
import { savedSearchLaunchHref } from "@/lib/search/launch-url";
import { searchResultViewForDefinition } from "@/lib/search/result-view-navigation";
import {
  nextDuplicateSavedSearchName,
  savedSearchNameValidationError,
} from "@/lib/search/saved-search-names";
import {
  deleteServerSavedSearch,
  duplicateServerSavedSearch,
  getServerSavedSearch,
  listServerSavedSearches,
  renameServerSavedSearch,
  savedSearchForDisplay,
  type ServerSavedSearch,
} from "@/lib/search/server-objects";

import { BackendResourceState } from "../_components/backend-resource-state";
import { StatusLabel } from "../_components/status";
import { AppIcon } from "../_components/app-icon";
import { formatMediumDateTime } from "../_components/date-format";
import { PageHeading } from "../_components/product-shell";
import { Modal } from "../_components/modal";
import { AlertsPanel } from "./alerts-panel";
import {
  reportsViewForKey,
  scheduledReportConfigurationTarget,
  SCHEDULE_REPORT_QUERY_PARAMETER,
  type ReportsView,
} from "./reports-view-state";
import { ScheduledReportActions, ScheduledReportStatus } from "./scheduled-report-controls";

type SavedSearchScope = "all" | "private" | "app" | "global";
type SortOrder = "updated" | "name";
type LoadState = "loading" | "available" | "unavailable" | "error";
type SavedSearchAction = "rename" | "duplicate" | "delete";

interface SavedSearchModal {
  action: SavedSearchAction;
  target: ServerSavedSearch;
}

interface BackendReportsConsoleProps {
  apiBaseUrl: string;
  onViewChange: (view: ReportsView) => void;
  view: ReportsView;
}

const errorMessage = createErrorMessage("The server did not return a usable saved-search response.");

function scopeLabel(scope: SharingScope): string {
  if (scope === SharingScope.SHARING_SCOPE_GLOBAL) return "Global";
  if (scope === SharingScope.SHARING_SCOPE_APP) return "App";
  if (scope === SharingScope.SHARING_SCOPE_PRIVATE) return "Private";
  return "Unknown";
}

function sharingScopeFilters(scope: SavedSearchScope): SharingScope[] {
  if (scope === "global") return [SharingScope.SHARING_SCOPE_GLOBAL];
  if (scope === "app") return [SharingScope.SHARING_SCOPE_APP];
  if (scope === "private") return [SharingScope.SHARING_SCOPE_PRIVATE];
  return [];
}

function selectedScopeLabel(scope: SavedSearchScope): string {
  if (scope === "private") return "Private";
  if (scope === "app") return "App";
  if (scope === "global") return "Global";
  return "All";
}

function formatDate(value: Date | null): string {
  return formatMediumDateTime(value, "Not recorded");
}

function launchHref(savedSearch: ServerSavedSearch): string {
  return savedSearchLaunchHref(
    savedSearch.id,
    true,
    searchResultViewForDefinition(savedSearch.search.spl, savedSearch.search.preferredResultTab),
  );
}

export function BackendReportsConsole({ apiBaseUrl, onViewChange, view }: BackendReportsConsoleProps) {
  const client = useMemo(() => createOpenSplunkApiClient({ baseUrl: apiBaseUrl }), [apiBaseUrl]);
  const [state, setState] = useState<LoadState>("loading");
  const [savedSearches, setSavedSearches] = useState<ServerSavedSearch[]>([]);
  const [appNames, setAppNames] = useState<Record<string, string>>({});
  const [error, setError] = useState<string | null>(null);
  const [complete, setComplete] = useState(true);
  const [nextPageToken, setNextPageToken] = useState<string | null>(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const [loadMoreError, setLoadMoreError] = useState<string | null>(null);
  const [totalSize, setTotalSize] = useState<bigint | null>(null);
  const [totalSizeExact, setTotalSizeExact] = useState(false);
  const [generation, setGeneration] = useState(0);
  const activeLoadGenerationRef = useRef(0);
  const [refreshing, setRefreshing] = useState(false);
  const [query, setQuery] = useState("");
  const [effectiveQuery, setEffectiveQuery] = useState("");
  const [scope, setScope] = useState<SavedSearchScope>("all");
  const [appFilter, setAppFilter] = useState("all");
  const [sort, setSort] = useState<SortOrder>("updated");
  const [modal, setModal] = useState<SavedSearchModal | null>(null);
  const [actionName, setActionName] = useState("");
  const [actionPending, setActionPending] = useState<SavedSearchAction | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [actionNotice, setActionNotice] = useState<string | null>(null);
  const [scheduleTargetId, setScheduleTargetId] = useState<string | null>(null);
  const [systemBootstrap, setSystemBootstrap] = useState<SystemBootstrapModel | null>(null);
  const alertsTabRef = useRef<HTMLButtonElement>(null);
  const savedSearchesTabRef = useRef<HTMLButtonElement>(null);
  const bootstrapRef = useRef<SystemBootstrapModel | null>(null);
  const loadMoreAbortRef = useRef<AbortController | null>(null);
  const actionAbortRef = useRef<AbortController | null>(null);
  const pageTokensSeenRef = useRef<Set<string>>(new Set());
  const hasLoadedRef = useRef(false);
  const reload = useCallback(() => setGeneration((current) => current + 1), []);
  const clearScheduleTarget = useCallback(() => {
    setScheduleTargetId(null);
    const url = new URL(window.location.href);
    url.searchParams.delete(SCHEDULE_REPORT_QUERY_PARAMETER);
    window.history.replaceState(window.history.state, "", url);
  }, []);

  useEffect(() => () => actionAbortRef.current?.abort(), []);

  useEffect(() => {
    const frame = window.requestAnimationFrame(() => {
      try {
        setScheduleTargetId(scheduledReportConfigurationTarget(new URL(window.location.href).searchParams));
      } catch (reason) {
        setActionNotice(reason instanceof Error ? reason.message : "The report schedule link is invalid.");
        clearScheduleTarget();
      }
    });
    return () => window.cancelAnimationFrame(frame);
  }, [clearScheduleTarget]);

  useEffect(() => {
    const timer = window.setTimeout(() => setEffectiveQuery(query.trim()), 250);
    return () => window.clearTimeout(timer);
  }, [query]);

  useEffect(() => {
    activeLoadGenerationRef.current = generation;
    const retainShell = hasLoadedRef.current;
    loadMoreAbortRef.current?.abort();
    loadMoreAbortRef.current = null;
    const controller = new AbortController();
    let current = true;
    queueMicrotask(() => {
      if (!current) return;
      if (!retainShell) setState("loading");
      setRefreshing(retainShell);
      setError(null);
      setLoadMoreError(null);
      setLoadingMore(false);
    });
    if (!retainShell) {
      bootstrapRef.current = null;
      pageTokensSeenRef.current.clear();
      queueMicrotask(() => {
        if (!current || activeLoadGenerationRef.current !== generation) return;
        setNextPageToken(null);
        setSystemBootstrap(null);
      });
    }
    void (async () => {
      try {
        const bootstrap = await getSystemBootstrap(client, undefined, { signal: controller.signal });
        if (!current || activeLoadGenerationRef.current !== generation) return;
        bootstrapRef.current = bootstrap;
        setSystemBootstrap(bootstrap);
        setAppNames(Object.fromEntries(
          bootstrap.apps.map((app) => [app.appId, app.displayName || app.slug || app.appId]),
        ));
        const result = await listServerSavedSearches(client, bootstrap, {
          signal: controller.signal,
          pageSize: Math.max(1, Math.min(bootstrap.limits.maximumPageSize || 50, 100)),
          maximumPages: 1,
          text: effectiveQuery,
          appId: appFilter === "all" ? undefined : appFilter,
          sharingScopes: sharingScopeFilters(scope),
          sortBy: sort === "name"
            ? SavedSearchSortBy.SAVED_SEARCH_SORT_BY_NAME
            : SavedSearchSortBy.SAVED_SEARCH_SORT_BY_UPDATED_AT,
          sortDirection: sort === "name"
            ? SortDirection.SORT_DIRECTION_ASCENDING
            : SortDirection.SORT_DIRECTION_DESCENDING,
        });
        if (!current) return;
        if (result.status === "unavailable") {
          hasLoadedRef.current = false;
          setSavedSearches([]);
          setRefreshing(false);
          setState("unavailable");
          return;
        }
        pageTokensSeenRef.current.clear();
        setSavedSearches(result.value.items);
        setComplete(result.value.complete);
        setNextPageToken(recordNextPageToken(
          pageTokensSeenRef.current,
          result.value.nextPageToken,
          "Saved searches",
        ));
        setTotalSize(result.value.totalSize);
        setTotalSizeExact(result.value.totalSizeExact);
        hasLoadedRef.current = true;
        setRefreshing(false);
        setState("available");
      } catch (reason) {
        if (!current || controller.signal.aborted) return;
        setError(errorMessage(reason));
        setRefreshing(false);
        if (retainShell) {
          setState("available");
        } else {
          hasLoadedRef.current = false;
          setSavedSearches([]);
          setState("error");
        }
      }
    })();
    return () => {
      current = false;
      controller.abort();
      loadMoreAbortRef.current?.abort();
      loadMoreAbortRef.current = null;
    };
  }, [appFilter, client, effectiveQuery, generation, scope, sort]);

  useEffect(() => {
    const bootstrap = bootstrapRef.current;
    if (scheduleTargetId === null || state !== "available" || savedSearches.some((item) => item.id === scheduleTargetId)) return;
    if (bootstrap === null || !supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_SCHEDULED_SEARCHES)) {
      setActionNotice("This server does not advertise scheduled reports.");
      clearScheduleTarget();
      return;
    }
    const controller = new AbortController();
    void (async () => {
      try {
        const result = await getServerSavedSearch(client, bootstrap, scheduleTargetId, { signal: controller.signal });
        if (controller.signal.aborted) return;
        if (result.status === "unavailable") throw new Error("The saved report is unavailable.");
        setSavedSearches((current) => current.some((item) => item.id === result.value.id)
          ? current
          : [result.value, ...current]);
      } catch (reason) {
        if (controller.signal.aborted) return;
        setActionNotice(errorMessage(reason));
        clearScheduleTarget();
      }
    })();
    return () => controller.abort();
  }, [clearScheduleTarget, client, savedSearches, scheduleTargetId, state]);

  const loadMore = useCallback(async () => {
    const bootstrap = bootstrapRef.current;
    const pageToken = nextPageToken;
    if (bootstrap === null || pageToken === null || loadMoreAbortRef.current !== null) return;
    const controller = new AbortController();
    loadMoreAbortRef.current = controller;
    setLoadingMore(true);
    setLoadMoreError(null);
    try {
      const result = await listServerSavedSearches(client, bootstrap, {
        signal: controller.signal,
        pageSize: Math.max(1, Math.min(bootstrap.limits.maximumPageSize || 50, 100)),
        pageToken,
        maximumPages: 1,
        text: effectiveQuery,
        appId: appFilter === "all" ? undefined : appFilter,
        sharingScopes: sharingScopeFilters(scope),
        sortBy: sort === "name"
          ? SavedSearchSortBy.SAVED_SEARCH_SORT_BY_NAME
          : SavedSearchSortBy.SAVED_SEARCH_SORT_BY_UPDATED_AT,
        sortDirection: sort === "name"
          ? SortDirection.SORT_DIRECTION_ASCENDING
          : SortDirection.SORT_DIRECTION_DESCENDING,
      });
      if (controller.signal.aborted || bootstrapRef.current !== bootstrap) return;
      if (result.status === "unavailable") {
        setState("unavailable");
        setNextPageToken(null);
        return;
      }
      setSavedSearches((current) => {
        const savedSearchesById = new Map(current.map((savedSearch) => [savedSearch.id, savedSearch]));
        for (const savedSearch of result.value.items) savedSearchesById.set(savedSearch.id, savedSearch);
        return [...savedSearchesById.values()];
      });
      try {
        setNextPageToken(recordNextPageToken(
          pageTokensSeenRef.current,
          result.value.nextPageToken,
          "Saved searches",
        ));
        setComplete(result.value.complete);
      } catch (reason) {
        setNextPageToken(null);
        setComplete(true);
        setLoadMoreError(reason instanceof Error ? `${reason.message} Further paging was stopped.` : "Saved searches returned an invalid page cursor.");
      }
    } catch (reason) {
      if (!controller.signal.aborted) {
        if (reason instanceof RepeatedPageCursorError) {
          setNextPageToken(null);
          setComplete(true);
          setLoadMoreError(`${reason.message} Further paging was stopped.`);
        } else {
          setLoadMoreError(errorMessage(reason));
        }
      }
    } finally {
      if (loadMoreAbortRef.current === controller) {
        loadMoreAbortRef.current = null;
        setLoadingMore(false);
      }
    }
  }, [appFilter, client, effectiveQuery, nextPageToken, scope, sort]);

  const visible = savedSearches;

  const displayedTotal = totalSizeExact && totalSize !== null
    ? totalSize.toLocaleString()
    : savedSearches.length.toLocaleString();
  const refreshPending = state === "loading" || refreshing;
  const controlsPending = refreshPending || actionPending !== null;
  const actionNameError = savedSearchNameValidationError(actionName);
  const currentBootstrap = systemBootstrap;
  const schedulingAvailable = currentBootstrap !== null
    && supportsServerFeature(currentBootstrap, ServerFeature.SERVER_FEATURE_SCHEDULED_SEARCHES);

  function openAction(nextAction: SavedSearchAction, target: ServerSavedSearch) {
    if (actionPending !== null) return;
    setActionError(null);
    setActionName(nextAction === "duplicate"
      ? nextDuplicateSavedSearchName(target.name, savedSearches.map((candidate) => candidate.name))
      : target.name);
    setModal({ action: nextAction, target });
  }

  function moveReportsView(event: KeyboardEvent<HTMLButtonElement>) {
    const nextView = reportsViewForKey(view, event.key);
    if (nextView === null) return;
    event.preventDefault();
    onViewChange(nextView);
    window.requestAnimationFrame(() => {
      (nextView === "saved-searches" ? savedSearchesTabRef : alertsTabRef).current?.focus();
    });
  }

  function closeAction() {
    if (actionPending !== null) return;
    setModal(null);
    setActionError(null);
  }

  function invalidatePagingAfterMutation() {
    pageTokensSeenRef.current.clear();
    setNextPageToken(null);
    setComplete(true);
    setLoadMoreError(null);
  }

  async function renameSavedSearch() {
    const currentModal = modal;
    const bootstrap = bootstrapRef.current;
    const name = actionName.trim();
    const validationError = savedSearchNameValidationError(actionName);
    if (currentModal?.action !== "rename" || bootstrap === null || name === currentModal.target.name) return;
    if (validationError !== null) {
      setActionError(validationError);
      return;
    }
    const controller = new AbortController();
    actionAbortRef.current?.abort();
    actionAbortRef.current = controller;
    setActionPending("rename");
    setActionError(null);
    try {
      const result = await renameServerSavedSearch(client, bootstrap, currentModal.target, name, {
        signal: controller.signal,
      });
      if (controller.signal.aborted) return;
      if (result.status === "unavailable") throw new Error("The saved-search update route is no longer available.");
      if (result.value.id !== currentModal.target.id) throw new Error("The server returned a different saved search after rename.");
      setSavedSearches((current) => current.map((item) => item.id === result.value.id ? result.value : item));
      invalidatePagingAfterMutation();
      setModal(null);
      setActionNotice(`Renamed “${currentModal.target.name}” to “${result.value.name}”.`);
      reload();
    } catch (reason) {
      if (!controller.signal.aborted) setActionError(errorMessage(reason));
    } finally {
      if (actionAbortRef.current === controller) actionAbortRef.current = null;
      setActionPending(null);
    }
  }

  async function duplicateSavedSearch() {
    const currentModal = modal;
    const bootstrap = bootstrapRef.current;
    const name = actionName.trim();
    const validationError = savedSearchNameValidationError(actionName);
    if (currentModal?.action !== "duplicate" || bootstrap === null) return;
    if (validationError !== null) {
      setActionError(validationError);
      return;
    }
    const controller = new AbortController();
    actionAbortRef.current?.abort();
    actionAbortRef.current = controller;
    setActionPending("duplicate");
    setActionError(null);
    try {
      const result = await duplicateServerSavedSearch(
        client,
        bootstrap,
        currentModal.target.id,
        name,
        currentModal.target.search.appId,
        { signal: controller.signal },
      );
      if (controller.signal.aborted) return;
      if (result.status === "unavailable") throw new Error("The saved-search duplicate route is no longer available.");
      if (result.value.id === currentModal.target.id) throw new Error("The server did not return a distinct saved-search copy.");
      setSavedSearches((current) => [result.value, ...current]);
      setTotalSize((current) => current === null ? null : current + 1n);
      invalidatePagingAfterMutation();
      setModal(null);
      setActionNotice(`Created “${result.value.name}” from “${currentModal.target.name}”.`);
      reload();
    } catch (reason) {
      if (!controller.signal.aborted) setActionError(errorMessage(reason));
    } finally {
      if (actionAbortRef.current === controller) actionAbortRef.current = null;
      setActionPending(null);
    }
  }

  async function deleteSavedSearch() {
    const currentModal = modal;
    const bootstrap = bootstrapRef.current;
    if (currentModal?.action !== "delete" || bootstrap === null) return;
    const controller = new AbortController();
    actionAbortRef.current?.abort();
    actionAbortRef.current = controller;
    setActionPending("delete");
    setActionError(null);
    try {
      const result = await deleteServerSavedSearch(
        client,
        bootstrap,
        currentModal.target.id,
        currentModal.target.version,
        { signal: controller.signal },
      );
      if (controller.signal.aborted) return;
      if (result.status === "unavailable") throw new Error("The saved-search delete route is no longer available.");
      if (result.value !== currentModal.target.id) throw new Error("The server confirmed deletion for a different saved search.");
      setSavedSearches((current) => current.filter((item) => item.id !== currentModal.target.id));
      setTotalSize((current) => current === null || current === 0n ? current : current - 1n);
      invalidatePagingAfterMutation();
      setModal(null);
      setActionNotice(`Deleted “${currentModal.target.name}”. Search history was not changed.`);
      reload();
    } catch (reason) {
      if (!controller.signal.aborted) setActionError(errorMessage(reason));
    } finally {
      if (actionAbortRef.current === controller) actionAbortRef.current = null;
      setActionPending(null);
    }
  }

  return (
    <div className="suite-page reports-page">
      <PageHeading
        eyebrow="SEARCH & REPORTING"
        title={view === "saved-searches" ? "Saved searches" : "Alerts"}
        description={view === "saved-searches"
          ? "Open reusable search definitions persisted by the connected server."
          : "Manage scheduled result conditions and signed webhook delivery."}
        actions={<><Link className="button" href="/search/events/">Open Search</Link>{view === "saved-searches" ? <button className="button button--primary" type="button" aria-busy={refreshPending} aria-disabled={controlsPending} onClick={() => { if (!controlsPending) reload(); }}>{refreshing ? "Refreshing…" : state === "loading" ? "Loading…" : "Refresh"}</button> : null}</>}
      />

      <div className="reports-view-tabs" role="tablist" aria-label="Search reporting views">
        <button ref={savedSearchesTabRef} className={`button ${view === "saved-searches" ? "button--primary" : "button--secondary"}`} id="reports-saved-searches-tab" role="tab" aria-controls="reports-saved-searches-panel" aria-selected={view === "saved-searches"} tabIndex={view === "saved-searches" ? 0 : -1} type="button" onClick={() => onViewChange("saved-searches")} onKeyDown={moveReportsView}>Saved Searches</button>
        <button ref={alertsTabRef} className={`button ${view === "alerts" ? "button--primary" : "button--secondary"}`} id="reports-alerts-tab" role="tab" aria-controls="reports-alerts-panel" aria-selected={view === "alerts"} tabIndex={view === "alerts" ? 0 : -1} type="button" onClick={() => onViewChange("alerts")} onKeyDown={moveReportsView}>Alerts</button>
      </div>

      <div id="reports-saved-searches-panel" role="tabpanel" aria-labelledby="reports-saved-searches-tab" hidden={view !== "saved-searches"}>

      {state === "loading" ? <BackendResourceState kind="loading" title="Loading saved searches" message="Reading persisted definitions from the server…" /> : null}
      {state === "unavailable" ? <BackendResourceState kind="unavailable" title="Saved searches are unavailable" message="The backend does not advertise or register the saved-search service. Scheduled reports are not substituted. Use Refresh to retry." /> : null}
      {state === "error" ? <BackendResourceState kind="error" title="Saved searches could not be loaded" message={`${error ?? "The request failed."} Use Refresh to retry.`} /> : null}

      {state === "available" ? (
        <>
          {refreshing ? <output className="backend-list-notice">Refreshing saved searches. Existing definitions remain visible until the request completes.</output> : null}
          {error === null ? null : <div className="backend-inline-error" role="alert">The latest refresh failed; the previous saved-search snapshot remains visible. {error}</div>}
          {actionNotice === null ? null : <output className="reports-action-notice"><span>{actionNotice}</span><button className="button button--notice-dismiss" type="button" aria-label="Dismiss saved-search action message" onClick={() => setActionNotice(null)}><AppIcon name="close" size="md" /></button></output>}
          <section className="reports-summary" aria-label="Saved search summary">
            <article><span className="reports-metric-icon" aria-hidden="true">▤</span><div><strong>{displayedTotal}</strong><small>{totalSizeExact ? "Matching saved searches" : "Matching definitions loaded"}</small></div></article>
            <article><span className="reports-metric-icon" aria-hidden="true">♙</span><div><strong>{savedSearches.length.toLocaleString()}</strong><small>Definitions loaded</small></div></article>
            <article><span className="reports-metric-icon" aria-hidden="true">◎</span><div><strong>{selectedScopeLabel(scope)}</strong><small>Server-side sharing scope</small></div></article>
          </section>
          {!complete ? (
            <output className="backend-list-notice backend-list-notice--action">
              <span>Loaded {savedSearches.length.toLocaleString()}{totalSizeExact && totalSize !== null ? ` of ${totalSize.toLocaleString()}` : ""} saved searches.</span>
              <button type="button" aria-busy={loadingMore} disabled={refreshing || loadingMore || nextPageToken === null} onClick={() => void loadMore()}>
                {loadingMore ? "Loading more…" : "Load more"}
              </button>
            </output>
          ) : null}
          {loadMoreError === null ? null : <div className="backend-inline-error" role="alert">{loadMoreError} {nextPageToken === null ? null : <button type="button" onClick={() => void loadMore()}>Retry</button>}</div>}

          <section className="suite-card reports-library" aria-labelledby="saved-search-library-title" aria-busy={refreshing}>
            <header className="reports-library-header">
              <div><h2 id="saved-search-library-title">Saved search library</h2><p>Reusable definitions with optional durable schedules and retained run history.</p></div>
              <span><strong>{visible.length}</strong> {visible.length === 1 ? "saved search" : "saved searches"}{complete ? "" : " loaded"}</span>
            </header>

            <div className="reports-scope-bar" aria-label="Server-side saved search sharing scope">
              {([
                ["all", "All saved searches"],
                ["private", "Private"],
                ["app", "App"],
                ["global", "Global"],
              ] as const).map(([id, label]) => (
                <button className={scope === id ? "reports-scope-active" : undefined} type="button" aria-pressed={scope === id} onClick={() => setScope(id)} key={id}>
                  {label}<span>{scope === id ? displayedTotal : null}</span>
                </button>
              ))}
            </div>
            {complete ? null : <p className="reports-loaded-only-note">Scope, text, and ordering are applied by the server. Load more to continue this result set.</p>}

            <div className="reports-toolbar reports-toolbar--backend">
              <label className="reports-search-field">
                <span className="sr-only">Filter saved searches</span><i aria-hidden="true"><AppIcon name="search" size="sm" /></i>
                <input type="search" value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Find by name, SPL, app, or owner" />
              </label>
              <label className="reports-select-field">
                <span>App</span>
                <select value={appFilter} onChange={(event) => setAppFilter(event.target.value)}>
                  <option value="all">All apps</option>
                  {Object.entries(appNames).toSorted((left, right) => left[1].localeCompare(right[1])).map(([appId, appName]) => <option value={appId} key={appId}>{appName}</option>)}
                </select>
              </label>
              <label className="reports-select-field">
                <span>Sort</span>
                <select value={sort} onChange={(event) => setSort(event.target.value as SortOrder)}>
                  <option value="updated">Recently modified</option>
                  <option value="name">Name</option>
                </select>
              </label>
            </div>

            {visible.length === 0 ? (
              <div className="reports-empty">
                <span aria-hidden="true"><AppIcon name="search" size="lg" /></span>
                <strong>{savedSearches.length === 0 ? "No saved searches" : "No matching saved searches"}</strong>
                <p>{savedSearches.length === 0 && effectiveQuery.length === 0 && scope === "all" ? "Save a search from the Search workspace to add its reusable definition here." : "Try another phrase or sharing scope."}</p>
                {savedSearches.length > 0 ? <button type="button" onClick={() => { setQuery(""); setScope("all"); }}>Clear filters</button> : <Link href="/search/events/">Open Search</Link>}
              </div>
            ) : (
              <div className="table-wrap reports-table-wrap">
                <table className="table table--fixed table--cards reports-table reports-table--backend reports-table--scheduled">
                  <thead><tr><th scope="col">Saved search</th><th scope="col">App</th><th scope="col">Sharing</th><th scope="col">Owner</th><th scope="col">Time range</th><th scope="col">Schedule</th><th scope="col">Modified</th><th scope="col"><span className="sr-only">Actions</span></th></tr></thead>
                  <tbody>{visible.map((savedSearch) => {
                    const display = savedSearchForDisplay(savedSearch, formatDate);
                    return (
                      <tr key={savedSearch.id}>
                        <td className="reports-report-column" aria-label={`Saved search ${savedSearch.name}`}>
                          <div className="reports-report-cell">
                            <span className="reports-report-icon reports-report-icon--statistics" aria-hidden="true">⌕</span>
                            <div><Link href={launchHref(savedSearch)}>{savedSearch.name}</Link>{savedSearch.description ? <small>{savedSearch.description}</small> : null}<code>{savedSearch.search.spl}</code></div>
                          </div>
                        </td>
                        <td data-label="App">{savedSearch.search.appId === undefined
                          ? "No app"
                          : appNames[savedSearch.search.appId] ?? savedSearch.search.appId}</td>
                        <td data-label="Sharing"><StatusLabel tone="neutral">{scopeLabel(savedSearch.sharingScope)}</StatusLabel></td>
                        <td data-label="Owner">{savedSearch.ownerId || "Current user"}</td>
                        <td data-label="Time range"><code>{savedSearch.search.timeRange?.earliest ?? "Server default"} → {savedSearch.search.timeRange?.latest ?? "Server default"}</code></td>
                        <td data-label="Schedule">{schedulingAvailable ? <ScheduledReportStatus savedSearch={savedSearch} /> : "Unavailable"}</td>
                        <td data-label="Modified">{display.updatedAt}</td>
                        <td className="reports-open-cell reports-action-cell">
                          <Link href={launchHref(savedSearch)} aria-label={`Open ${savedSearch.name} in Search`}>Open <AppIcon name="chevron-right" size="xs" /></Link>
                          <button type="button" disabled={controlsPending} onClick={() => openAction("rename", savedSearch)} aria-label={`Rename ${savedSearch.name}`}>Rename</button>
                          <button type="button" disabled={controlsPending} onClick={() => openAction("duplicate", savedSearch)} aria-label={`Duplicate ${savedSearch.name}`}>Duplicate</button>
                          {schedulingAvailable && currentBootstrap !== null ? (
                            <ScheduledReportActions
                              autoOpenSchedule={scheduleTargetId === savedSearch.id}
                              bootstrap={currentBootstrap}
                              client={client}
                              disabled={controlsPending}
                              savedSearch={savedSearch}
                              onNotice={setActionNotice}
                              onRefresh={reload}
                              onScheduleOpened={clearScheduleTarget}
                              onUpdated={(updated) => {
                                setSavedSearches((current) => current.map((item) => item.id === updated.id ? updated : item));
                                invalidatePagingAfterMutation();
                              }}
                            />
                          ) : null}
                          <button className="reports-delete-action" type="button" disabled={controlsPending} onClick={() => openAction("delete", savedSearch)} aria-label={`Delete ${savedSearch.name}`}>Delete</button>
                        </td>
                      </tr>
                    );
                  })}</tbody>
                </table>
              </div>
            )}
          </section>

          {schedulingAvailable ? null : <section className="suite-card backend-unavailable-card">
            <header className="suite-card-header"><div><h2>Scheduling and delivery</h2><p>Unavailable in backend mode.</p></div><span aria-hidden="true">i</span></header>
            <p>The backend persists saved search definitions but does not register report scheduling, execution, or delivery routes. No schedule or last-run status is inferred.</p>
          </section>}
        </>
      ) : null}

      {modal?.action === "rename" ? (
        <Modal
          title="Rename saved search"
          subtitle={`Choose a new name for “${modal.target.name}”. Its SPL, sharing, and app stay unchanged.`}
          initialFocus="#reports-saved-search-name"
          dismissible={actionPending === null}
          onClose={closeAction}
          footer={(
            <>
              <button className="button button--secondary" type="button" disabled={actionPending !== null} onClick={closeAction}>Cancel</button>
              <button
                className="button button--primary"
                type="submit"
                form="reports-rename-saved-search"
                disabled={actionPending !== null || actionNameError !== null || actionName.trim() === modal.target.name}
                aria-busy={actionPending === "rename"}
              >
                {actionPending === "rename" ? "Renaming…" : "Rename"}
              </button>
            </>
          )}
        >
          <form className="form-stack" id="reports-rename-saved-search" onSubmit={(event) => { event.preventDefault(); void renameSavedSearch(); }}>
            {actionError === null && actionNameError === null ? null : <p className="reports-action-error" id="reports-rename-name-error" role="alert">{actionError ?? actionNameError}</p>}
            <label><span>Name</span><input id="reports-saved-search-name" value={actionName} disabled={actionPending !== null} maxLength={255} autoComplete="off" aria-invalid={actionNameError !== null} aria-describedby={actionError === null && actionNameError === null ? undefined : "reports-rename-name-error"} onChange={(event) => { setActionName(event.target.value); setActionError(null); }} /></label>
          </form>
        </Modal>
      ) : null}

      {modal?.action === "duplicate" ? (
        <Modal
          title="Duplicate saved search"
          subtitle={`Create an independent copy of “${modal.target.name}” in the same app.`}
          initialFocus="#reports-duplicate-saved-search-name"
          dismissible={actionPending === null}
          onClose={closeAction}
          footer={(
            <>
              <button className="button button--secondary" type="button" disabled={actionPending !== null} onClick={closeAction}>Cancel</button>
              <button className="button button--primary" type="submit" form="reports-duplicate-saved-search" disabled={actionPending !== null || actionNameError !== null} aria-busy={actionPending === "duplicate"}>
                {actionPending === "duplicate" ? "Duplicating…" : "Duplicate"}
              </button>
            </>
          )}
        >
          <form className="form-stack" id="reports-duplicate-saved-search" onSubmit={(event) => { event.preventDefault(); void duplicateSavedSearch(); }}>
            {actionError === null && actionNameError === null ? null : <p className="reports-action-error" id="reports-duplicate-name-error" role="alert">{actionError ?? actionNameError}</p>}
            <label><span>Copy name</span><input id="reports-duplicate-saved-search-name" value={actionName} disabled={actionPending !== null} maxLength={255} autoComplete="off" aria-invalid={actionNameError !== null} aria-describedby={actionError === null && actionNameError === null ? undefined : "reports-duplicate-name-error"} onChange={(event) => { setActionName(event.target.value); setActionError(null); }} /></label>
            <p className="reports-action-hint">The copy keeps the current SPL, time range, result preferences, sharing scope, and app. Future edits do not affect the original.</p>
          </form>
        </Modal>
      ) : null}

      {modal?.action === "delete" ? (
        <Modal
          title="Delete saved search?"
          subtitle={`“${modal.target.name}” will be permanently removed.`}
          dismissible={actionPending === null}
          onClose={closeAction}
          footer={(
            <>
              <button className="button button--secondary" type="button" disabled={actionPending !== null} onClick={closeAction}>Keep saved search</button>
              <button className="button button--danger" type="button" disabled={actionPending !== null} aria-busy={actionPending === "delete"} onClick={() => void deleteSavedSearch()}>
                {actionPending === "delete" ? "Deleting…" : "Delete saved search"}
              </button>
            </>
          )}
        >
          <p>This cannot be undone. Existing search-history entries and their terminal metadata are not removed.</p>
          {actionError === null ? null : <p className="reports-action-error" role="alert">{actionError}</p>}
        </Modal>
      ) : null}
      </div>
      <div id="reports-alerts-panel" role="tabpanel" aria-labelledby="reports-alerts-tab" hidden={view !== "alerts"}>{view === "alerts" ? <AlertsPanel apiBaseUrl={apiBaseUrl} /> : null}</div>
    </div>
  );
}
