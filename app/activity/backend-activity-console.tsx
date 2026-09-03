"use client";

import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type KeyboardEvent as ReactKeyboardEvent,
} from "react";
import Link from "next/link";

import type { SearchHistoryFilter } from "@/gen/ts/open_splunk/history_api";
import { SearchJobState } from "@/gen/ts/open_splunk/search";
import { ServerFeature } from "@/gen/ts/open_splunk/system_api";
import {
  createOpenSplunkApiClient,
  getSystemBootstrap,
  recordNextPageToken,
  RepeatedPageCursorError,
  supportsServerFeature,
  type SystemBootstrapModel,
} from "@/lib/api";
import { createErrorMessage } from "@/lib/error-message";
import { historySearchLaunchHref } from "@/lib/search/launch-url";
import { searchResultViewForDefinition } from "@/lib/search/result-view-navigation";
import {
  clearServerSearchHistory,
  deleteServerSearchHistoryEntry,
  historyEntryForDisplay,
  listServerSearchHistory,
  type ServerSearchHistoryEntry,
} from "@/lib/search/server-objects";

import { BackendResourceState } from "../_components/backend-resource-state";
import { StatusDot, StatusLabel } from "../_components/status";
import { AppIcon } from "../_components/app-icon";
import { PageHeading } from "../_components/product-shell";
import { Modal } from "../_components/modal";
import {
  formatActivityCount,
  formatActivityDate,
  formatActivityDuration,
  searchJobStateTone,
  searchJobStateLabel,
} from "./backend-activity-shared";
import { BackendLiveJobs } from "./backend-live-jobs";
import { BackendMutationAudit, BackendSearchAttemptAudit } from "./backend-audit-views";
import { BackendExportJobs } from "./backend-export-jobs";
import type { ActivityView } from "./activity-navigation";

type ActivityFilter = "all" | "completed" | "failed" | "canceled" | "warnings";
type LoadState = "loading" | "available" | "unavailable" | "error";
type HistoryModal =
  | { action: "delete"; target: ServerSearchHistoryEntry }
  | { action: "clear" };

interface BackendActivityViewProps {
  apiBaseUrl: string;
}

interface BackendActivityConsoleProps extends BackendActivityViewProps {
  onViewChange: (view: ActivityView) => void;
  view: ActivityView;
}

const errorMessage = createErrorMessage("The server did not return a usable search history response.");

function launchHref(entry: ServerSearchHistoryEntry): string {
  return historySearchLaunchHref(
    entry.id,
    true,
    searchResultViewForDefinition(entry.search.spl, entry.search.preferredResultTab),
  );
}

function historyStateFilters(filter: ActivityFilter): SearchJobState[] {
  if (filter === "completed") return [SearchJobState.SEARCH_JOB_STATE_COMPLETED];
  if (filter === "failed") {
    return [
      SearchJobState.SEARCH_JOB_STATE_FAILED,
      SearchJobState.SEARCH_JOB_STATE_EXPIRED,
    ];
  }
  if (filter === "canceled") return [SearchJobState.SEARCH_JOB_STATE_CANCELED];
  return [];
}

function historyServerFilter(filter: ActivityFilter, text: string): SearchHistoryFilter | undefined {
  const stateFilters = historyStateFilters(filter);
  const normalizedText = text.trim();
  if (stateFilters.length === 0 && normalizedText.length === 0) return undefined;
  return {
    appId: undefined,
    stateFilters,
    text: normalizedText || undefined,
    savedSearchId: undefined,
    createdAfter: undefined,
    createdBefore: undefined,
  };
}

function historyFilterLabel(filter: ActivityFilter): string {
  if (filter === "completed") return "completed";
  if (filter === "failed") return "failed or expired";
  if (filter === "canceled") return "canceled";
  if (filter === "warnings") return "warning-bearing";
  return "all";
}

export function BackendActivityConsole({ apiBaseUrl, onViewChange, view }: BackendActivityConsoleProps) {
  const client = useMemo(() => createOpenSplunkApiClient({ baseUrl: apiBaseUrl }), [apiBaseUrl]);
  const [visited, setVisited] = useState<ReadonlySet<ActivityView>>(new Set([view]));
  const [bootstrap, setBootstrap] = useState<SystemBootstrapModel | null>(null);
  const [bootstrapState, setBootstrapState] = useState<"loading" | "available" | "error">("loading");
  const jobTabRef = useRef<HTMLButtonElement>(null);
  const historyTabRef = useRef<HTMLButtonElement>(null);
  const exportsTabRef = useRef<HTMLButtonElement>(null);
  const mutationTabRef = useRef<HTMLButtonElement>(null);
  const attemptTabRef = useRef<HTMLButtonElement>(null);
  const mutationAuditAvailable = bootstrap !== null
    && supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_AUDIT_SEARCH);
  const searchAttemptAuditAvailable = bootstrap !== null
    && supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_SEARCH_ATTEMPT_AUDIT);
  const exportsAvailable = bootstrap !== null && (
    supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_EXPORT_CSV)
    || supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_EXPORT_JSON_LINES)
  );
  const availableViews = useMemo<ActivityView[]>(() => [
    "jobs",
    "history",
    ...(exportsAvailable ? ["exports" as const] : []),
    ...(mutationAuditAvailable ? ["mutations" as const] : []),
    ...(searchAttemptAuditAvailable ? ["attempts" as const] : []),
  ], [exportsAvailable, mutationAuditAvailable, searchAttemptAuditAvailable]);

  useEffect(() => {
    const controller = new AbortController();
    let current = true;
    void getSystemBootstrap(client, undefined, { signal: controller.signal })
      .then((value) => {
        if (!current) return;
        setBootstrap(value);
        setBootstrapState("available");
      })
      .catch(() => {
        if (!current) return;
        setBootstrap(null);
        setBootstrapState("error");
      });
    return () => {
      current = false;
      controller.abort();
    };
  }, [client]);

  const [visitedView, setVisitedView] = useState(view);
  if (visitedView !== view) {
    setVisitedView(view);
    setVisited((current) => new Set(current).add(view));
  }

  function selectView(nextView: ActivityView) {
    onViewChange(nextView);
  }

  const capabilityGatedView = view === "exports" || view === "mutations" || view === "attempts";
  if (capabilityGatedView && bootstrapState !== "available") {
    return (
      <div className="suite-page activity-page backend-activity-page">
        <PageHeading eyebrow="OPERATIONS" title="Activity" description="Inspect retained search and export jobs, search history, and the audit journals advertised by this backend." />
        <BackendResourceState
          kind={bootstrapState === "loading" ? "loading" : "error"}
          title={bootstrapState === "loading" ? "Checking activity capabilities" : "Activity capabilities could not be loaded"}
          message={bootstrapState === "loading" ? "Reading the views advertised by this backend…" : "The requested activity view cannot be verified until the system bootstrap is available."}
          action={bootstrapState === "error" ? <Link href="/activity/jobs/">View current jobs</Link> : undefined}
        />
      </div>
    );
  }
  if (!availableViews.includes(view)) {
    return (
      <div className="suite-page activity-page backend-activity-page">
        <PageHeading eyebrow="OPERATIONS" title="Activity" description="Inspect retained search and export jobs, search history, and the audit journals advertised by this backend." />
        <BackendResourceState
          kind="unavailable"
          title="Activity view not found"
          message="The connected backend does not advertise the capability required by this activity view."
          action={<Link href="/activity/jobs/">View current jobs</Link>}
        />
      </div>
    );
  }

  function navigateTabs(event: ReactKeyboardEvent<HTMLButtonElement>) {
    if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
    event.preventDefault();
    const currentView = event.currentTarget.dataset.view as ActivityView;
    const currentIndex = Math.max(0, availableViews.indexOf(currentView));
    const nextIndex = event.key === "Home"
      ? 0
      : event.key === "End"
        ? availableViews.length - 1
        : event.key === "ArrowLeft"
          ? (currentIndex - 1 + availableViews.length) % availableViews.length
          : (currentIndex + 1) % availableViews.length;
    const nextView = availableViews[nextIndex];
    selectView(nextView);
    window.requestAnimationFrame(() => {
      ({
        jobs: jobTabRef,
        history: historyTabRef,
        exports: exportsTabRef,
        mutations: mutationTabRef,
        attempts: attemptTabRef,
      }[nextView].current)?.focus();
    });
  }

  return (
    <div className="suite-page activity-page backend-activity-page">
      <PageHeading
        eyebrow="OPERATIONS"
        title="Activity"
        description="Inspect retained search and export jobs, search history, and the audit journals advertised by this backend."
      />
      <div className="activity-view-tabs" data-tab-count={availableViews.length} role="tablist" aria-label="Activity data view">
        <button
          ref={jobTabRef}
          id="activity-jobs-tab"
          data-view="jobs"
          role="tab"
          type="button"
          aria-controls="activity-jobs-panel"
          aria-selected={view === "jobs"}
          tabIndex={view === "jobs" ? 0 : -1}
          onClick={() => selectView("jobs")}
          onKeyDown={navigateTabs}
        >
          <span aria-hidden="true"><AppIcon name="activity" size="md" /></span>
          <span><strong>Current jobs</strong><small>Retained transient executions</small></span>
        </button>
        <button
          ref={historyTabRef}
          id="activity-history-tab"
          data-view="history"
          role="tab"
          type="button"
          aria-controls="activity-history-panel"
          aria-selected={view === "history"}
          tabIndex={view === "history" ? 0 : -1}
          onClick={() => selectView("history")}
          onKeyDown={navigateTabs}
        >
          <span aria-hidden="true"><AppIcon name="history" size="md" /></span>
          <span><strong>Search history</strong><small>Persisted terminal metadata</small></span>
        </button>
        {exportsAvailable ? (
          <button
            ref={exportsTabRef}
            id="activity-exports-tab"
            data-view="exports"
            role="tab"
            type="button"
            aria-controls="activity-exports-panel"
            aria-selected={view === "exports"}
            tabIndex={view === "exports" ? 0 : -1}
            onClick={() => selectView("exports")}
            onKeyDown={navigateTabs}
          >
            <span aria-hidden="true"><AppIcon name="download" size="md" /></span>
            <span><strong>Export jobs</strong><small>Retained artifact generation</small></span>
          </button>
        ) : null}
        {mutationAuditAvailable ? (
          <button
            ref={mutationTabRef}
            id="activity-mutations-tab"
            data-view="mutations"
            role="tab"
            type="button"
            aria-controls="activity-mutations-panel"
            aria-selected={view === "mutations"}
            tabIndex={view === "mutations" ? 0 : -1}
            onClick={() => selectView("mutations")}
            onKeyDown={navigateTabs}
          >
            <span aria-hidden="true"><AppIcon name="settings" size="md" /></span>
            <span><strong>Mutation audit</strong><small>Successful configuration changes</small></span>
          </button>
        ) : null}
        {searchAttemptAuditAvailable ? (
          <button
            ref={attemptTabRef}
            id="activity-attempts-tab"
            data-view="attempts"
            role="tab"
            type="button"
            aria-controls="activity-attempts-panel"
            aria-selected={view === "attempts"}
            tabIndex={view === "attempts" ? 0 : -1}
            onClick={() => selectView("attempts")}
            onKeyDown={navigateTabs}
          >
            <span aria-hidden="true"><AppIcon name="search" size="md" /></span>
            <span><strong>Search attempts</strong><small>Admitted-search journal</small></span>
          </button>
        ) : null}
      </div>
      <section id="activity-jobs-panel" role="tabpanel" aria-labelledby="activity-jobs-tab" hidden={view !== "jobs"}>
        <BackendLiveJobs apiBaseUrl={apiBaseUrl} />
      </section>
      <section id="activity-history-panel" role="tabpanel" aria-labelledby="activity-history-tab" hidden={view !== "history"}>
        {visited.has("history") ? <BackendSearchHistory apiBaseUrl={apiBaseUrl} /> : null}
      </section>
      {exportsAvailable ? (
        <section id="activity-exports-panel" role="tabpanel" aria-labelledby="activity-exports-tab" hidden={view !== "exports"}>
          {visited.has("exports") ? <BackendExportJobs apiBaseUrl={apiBaseUrl} bootstrap={bootstrap} /> : null}
        </section>
      ) : null}
      {mutationAuditAvailable ? (
        <section id="activity-mutations-panel" role="tabpanel" aria-labelledby="activity-mutations-tab" hidden={view !== "mutations"}>
          {visited.has("mutations") ? <BackendMutationAudit apiBaseUrl={apiBaseUrl} maximumPageSize={bootstrap.limits.maximumPageSize} /> : null}
        </section>
      ) : null}
      {searchAttemptAuditAvailable ? (
        <section id="activity-attempts-panel" role="tabpanel" aria-labelledby="activity-attempts-tab" hidden={view !== "attempts"}>
          {visited.has("attempts") ? <BackendSearchAttemptAudit apiBaseUrl={apiBaseUrl} maximumPageSize={bootstrap.limits.maximumPageSize} /> : null}
        </section>
      ) : null}
    </div>
  );
}

function BackendSearchHistory({ apiBaseUrl }: BackendActivityViewProps) {
  const client = useMemo(() => createOpenSplunkApiClient({ baseUrl: apiBaseUrl }), [apiBaseUrl]);
  const [state, setState] = useState<LoadState>("loading");
  const [entries, setEntries] = useState<ServerSearchHistoryEntry[]>([]);
  const [appNames, setAppNames] = useState<Record<string, string>>({});
  const [error, setError] = useState<string | null>(null);
  const [complete, setComplete] = useState(true);
  const [nextPageToken, setNextPageToken] = useState<string | null>(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const [loadMoreError, setLoadMoreError] = useState<string | null>(null);
  const [totalSize, setTotalSize] = useState<bigint | null>(null);
  const [totalSizeExact, setTotalSizeExact] = useState(false);
  const [generation, setGeneration] = useState(0);
  const [filter, setFilter] = useState<ActivityFilter>("all");
  const [query, setQuery] = useState("");
  const [textFilter, setTextFilter] = useState("");
  const [modal, setModal] = useState<HistoryModal | null>(null);
  const [actionPending, setActionPending] = useState<"delete" | "clear" | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [actionNotice, setActionNotice] = useState<string | null>(null);
  const bootstrapRef = useRef<SystemBootstrapModel | null>(null);
  const loadMoreAbortRef = useRef<AbortController | null>(null);
  const actionAbortRef = useRef<AbortController | null>(null);
  const pageTokensSeenRef = useRef<Set<string>>(new Set());
  const reload = useCallback(() => setGeneration((current) => current + 1), []);
  const loadInput = { client, filter, generation, textFilter };
  const [activeLoadInput, setActiveLoadInput] = useState(loadInput);
  if (
    activeLoadInput.client !== loadInput.client
    || activeLoadInput.filter !== loadInput.filter
    || activeLoadInput.generation !== loadInput.generation
    || activeLoadInput.textFilter !== loadInput.textFilter
  ) {
    setActiveLoadInput(loadInput);
    setState("loading");
    setError(null);
    setLoadMoreError(null);
    setLoadingMore(false);
    setNextPageToken(null);
  }

  useEffect(() => {
    const timeout = window.setTimeout(() => setTextFilter(query.trim()), 300);
    return () => window.clearTimeout(timeout);
  }, [query]);

  useEffect(() => () => actionAbortRef.current?.abort(), []);

  useEffect(() => {
    if (activeLoadInput.generation !== generation) return;
    loadMoreAbortRef.current?.abort();
    loadMoreAbortRef.current = null;
    const controller = new AbortController();
    let current = true;
    bootstrapRef.current = null;
    pageTokensSeenRef.current.clear();
    void (async () => {
      try {
        const bootstrap = await getSystemBootstrap(client, undefined, { signal: controller.signal });
        bootstrapRef.current = bootstrap;
        setAppNames(Object.fromEntries(
          bootstrap.apps.map((app) => [app.appId, app.displayName || app.slug || app.appId]),
        ));
        const result = await listServerSearchHistory(client, bootstrap, {
          signal: controller.signal,
          states: historyStateFilters(filter),
          text: textFilter || undefined,
          pageSize: Math.max(1, Math.min(bootstrap.limits.maximumPageSize || 50, 100)),
          maximumPages: 1,
        });
        if (!current) return;
        if (result.status === "unavailable") {
          setEntries([]);
          setState("unavailable");
          return;
        }
        setEntries(result.value.items);
        setComplete(result.value.complete);
        setNextPageToken(recordNextPageToken(
          pageTokensSeenRef.current,
          result.value.nextPageToken,
          "Search history",
        ));
        setTotalSize(result.value.totalSize);
        setTotalSizeExact(result.value.totalSizeExact);
        setState("available");
      } catch (reason) {
        if (!current || controller.signal.aborted) return;
        setEntries([]);
        setError(errorMessage(reason));
        setState("error");
      }
    })();
    return () => {
      current = false;
      controller.abort();
      loadMoreAbortRef.current?.abort();
      loadMoreAbortRef.current = null;
    };
  }, [activeLoadInput, client, filter, generation, textFilter]);

  const loadMore = useCallback(async () => {
    const bootstrap = bootstrapRef.current;
    const pageToken = nextPageToken;
    if (bootstrap === null || pageToken === null || loadMoreAbortRef.current !== null) return;
    const controller = new AbortController();
    loadMoreAbortRef.current = controller;
    setLoadingMore(true);
    setLoadMoreError(null);
    try {
      const result = await listServerSearchHistory(client, bootstrap, {
        signal: controller.signal,
        states: historyStateFilters(filter),
        text: textFilter || undefined,
        pageSize: Math.max(1, Math.min(bootstrap.limits.maximumPageSize || 50, 100)),
        pageToken,
        maximumPages: 1,
      });
      if (controller.signal.aborted || bootstrapRef.current !== bootstrap) return;
      if (result.status === "unavailable") {
        setState("unavailable");
        setNextPageToken(null);
        return;
      }
      setEntries((current) => {
        const entriesById = new Map(current.map((entry) => [entry.id, entry]));
        for (const entry of result.value.items) entriesById.set(entry.id, entry);
        return [...entriesById.values()];
      });
      try {
        setNextPageToken(recordNextPageToken(
          pageTokensSeenRef.current,
          result.value.nextPageToken,
          "Search history",
        ));
        setComplete(result.value.complete);
      } catch (reason) {
        setNextPageToken(null);
        setComplete(true);
        setLoadMoreError(reason instanceof Error ? `${reason.message} Further paging was stopped.` : "Search history returned an invalid page cursor.");
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
  }, [client, filter, nextPageToken, textFilter]);

  const filtered = useMemo(() => {
    const normalized = query.trim().toLowerCase();
    return entries.filter((entry) => {
      if (filter === "warnings" && entry.warningCount === 0) return false;
      return normalized.length === 0
        || entry.search.spl.toLowerCase().includes(normalized);
    });
  }, [entries, filter, query]);

  const completed = entries.filter((entry) => entry.finalState === SearchJobState.SEARCH_JOB_STATE_COMPLETED).length;
  const failed = entries.filter((entry) =>
    entry.finalState === SearchJobState.SEARCH_JOB_STATE_FAILED
    || entry.finalState === SearchJobState.SEARCH_JOB_STATE_EXPIRED).length;
  const canceled = entries.filter((entry) => entry.finalState === SearchJobState.SEARCH_JOB_STATE_CANCELED).length;
  const warnings = entries.filter((entry) => entry.warningCount > 0).length;
  const loadedLabel = filter === "warnings"
    ? filtered.length.toLocaleString()
    : totalSizeExact && totalSize !== null
      ? totalSize.toLocaleString()
      : entries.length.toLocaleString();
  const filtersSettled = query.trim() === textFilter;
  const hasActiveHistoryFilter = filter !== "all" || query.trim().length > 0;
  const clearScope = `${filter === "all" ? "all" : historyFilterLabel(filter)} history${textFilter.length > 0 ? ` with SPL containing “${textFilter}”` : ""}`;

  function closeHistoryModal() {
    if (actionPending !== null) return;
    setModal(null);
    setActionError(null);
  }

  function invalidateHistoryPaging() {
    pageTokensSeenRef.current.clear();
    setNextPageToken(null);
    setComplete(true);
    setLoadMoreError(null);
  }

  async function deleteHistoryEntry() {
    const currentModal = modal;
    const bootstrap = bootstrapRef.current;
    if (currentModal?.action !== "delete" || bootstrap === null) return;
    const controller = new AbortController();
    actionAbortRef.current?.abort();
    actionAbortRef.current = controller;
    setActionPending("delete");
    setActionError(null);
    try {
      const result = await deleteServerSearchHistoryEntry(
        client,
        bootstrap,
        currentModal.target.id,
        { signal: controller.signal },
      );
      if (controller.signal.aborted) return;
      if (result.status === "unavailable") throw new Error("The search-history delete route is no longer available.");
      if (result.value !== currentModal.target.id) throw new Error("The server confirmed deletion for a different history entry.");
      setEntries((current) => current.filter((entry) => entry.id !== currentModal.target.id));
      setTotalSize((current) => current === null || current === 0n ? current : current - 1n);
      invalidateHistoryPaging();
      setModal(null);
      setActionNotice(`Deleted search-history entry ${currentModal.target.id}.`);
      reload();
    } catch (reason) {
      if (!controller.signal.aborted) setActionError(errorMessage(reason));
    } finally {
      if (actionAbortRef.current === controller) actionAbortRef.current = null;
      setActionPending(null);
    }
  }

  async function clearHistory() {
    const bootstrap = bootstrapRef.current;
    if (
      modal?.action !== "clear"
      || bootstrap === null
      || filter === "warnings"
      || !filtersSettled
    ) return;
    const controller = new AbortController();
    actionAbortRef.current?.abort();
    actionAbortRef.current = controller;
    setActionPending("clear");
    setActionError(null);
    try {
      const result = await clearServerSearchHistory(
        client,
        bootstrap,
        historyServerFilter(filter, textFilter),
        "CLEAR SEARCH HISTORY",
        { signal: controller.signal },
      );
      if (controller.signal.aborted) return;
      if (result.status === "unavailable") throw new Error("The search-history clear route is no longer available.");
      setEntries([]);
      setTotalSize(0n);
      setTotalSizeExact(true);
      invalidateHistoryPaging();
      setModal(null);
      setActionNotice(`Cleared ${result.value.toLocaleString()} ${result.value === 1n ? "history entry" : "history entries"} from the selected server scope.`);
      reload();
    } catch (reason) {
      if (!controller.signal.aborted) setActionError(errorMessage(reason));
    } finally {
      if (actionAbortRef.current === controller) actionAbortRef.current = null;
      setActionPending(null);
    }
  }

  return (
    <div className="backend-activity-view">
      {state === "loading" ? <BackendResourceState kind="loading" title="Loading search history" message="Reading persisted terminal-search metadata…" /> : null}
      {state === "unavailable" ? <BackendResourceState kind="unavailable" title="Search history is unavailable" message="The backend does not advertise or register persisted search history. Current transient jobs remain available in their separate tab." action={<button type="button" onClick={reload}>Retry</button>} /> : null}
      {state === "error" ? <BackendResourceState kind="error" title="Search history could not be loaded" message={error ?? "The search history request failed."} action={<button type="button" onClick={reload}>Retry</button>} /> : null}

      {state === "available" ? (
        <>
          {actionNotice === null ? null : <output className="history-action-notice"><span>{actionNotice}</span><button type="button" aria-label="Dismiss history action message" onClick={() => setActionNotice(null)}><AppIcon name="close" size="md" /></button></output>}
          <output className="live-jobs-snapshot history-snapshot">
            <span><i aria-hidden="true" />This view contains persisted terminal-search metadata, not the transient job list.</span>
            <div className="history-snapshot-actions">
              <button
                className="button button--compact history-clear-button"
                type="button"
                disabled={actionPending !== null || !filtersSettled || entries.length === 0 || filter === "warnings"}
                title={filter === "warnings" ? "Warnings are not a server-side clear filter. Choose a final-state scope or All." : undefined}
                onClick={() => { setActionError(null); setModal({ action: "clear" }); }}
              >
                Clear matching…
              </button>
              <button className="button button--compact" type="button" disabled={actionPending !== null} onClick={reload}>Refresh history</button>
            </div>
          </output>
          <section className="activity-summary" aria-label="Server-filtered search history summary">
            <article><StatusDot tone="success" /><div><strong>{loadedLabel}</strong><small>{filter === "warnings" ? "Warning matches loaded" : totalSizeExact ? `${historyFilterLabel(filter)} matches` : "Matches loaded"}</small></div></article>
            <article><StatusDot tone="success" /><div><strong>{completed}</strong><small>Completed loaded</small></div></article>
            <article><StatusDot tone="warning" /><div><strong>{warnings}</strong><small>Warnings loaded</small></div></article>
            <article><StatusDot tone="error" /><div><strong>{failed + canceled}</strong><small>Failed/canceled loaded</small></div></article>
          </section>
          {!complete ? (
            <output className="backend-list-notice backend-list-notice--action">
              <span>Loaded {entries.length.toLocaleString()}{totalSizeExact && totalSize !== null ? ` of ${totalSize.toLocaleString()}` : ""} history entries.</span>
              <button type="button" aria-busy={loadingMore} disabled={loadingMore || nextPageToken === null} onClick={() => void loadMore()}>
                {loadingMore ? "Loading more…" : "Load more"}
              </button>
            </output>
          ) : null}
          {loadMoreError === null ? null : <div className="backend-inline-error" role="alert">{loadMoreError} {nextPageToken === null ? null : <button type="button" onClick={() => void loadMore()}>Retry</button>}</div>}
          <section className="suite-card activity-jobs-card">
            <header className="activity-tabs-row">
              <fieldset className="activity-filter-group">
                <legend className="sr-only">Filter search history on the server by final state</legend>
                {(["all", "completed", "failed", "canceled", "warnings"] as const).map((item) => (
                  <button className={`activity-filter-button${filter === item ? " active" : ""}`} aria-pressed={filter === item} type="button" disabled={actionPending !== null} onClick={() => setFilter(item)} key={item}>
                    {item === "failed" ? "Failed or expired" : item[0].toUpperCase() + item.slice(1)}
                  </button>
                ))}
              </fieldset>
              <small className="activity-filter-scope">{filter === "warnings" ? "SPL filter runs on server; warning check applies to loaded matches" : "Final state and SPL filter run on server"}</small>
              <label><span className="sr-only">Filter search history SPL on the server</span><i aria-hidden="true"><AppIcon name="search" size="sm" /></i><input value={query} disabled={actionPending !== null} onChange={(event) => setQuery(event.target.value)} placeholder="Filter SPL on the server" /></label>
            </header>
            {filtered.length === 0 ? (
              <BackendResourceState
                kind="empty"
                title={hasActiveHistoryFilter ? "No matching history" : "No persisted search history"}
                message={hasActiveHistoryFilter
                  ? !complete
                    ? "Load more entries or try another final-state or SPL filter."
                    : "No persisted entries match the selected final-state and SPL filters."
                  : "Completed, failed, or canceled searches will appear here when the backend persists them."}
                action={hasActiveHistoryFilter ? <button type="button" onClick={() => { setFilter("all"); setQuery(""); }}>Clear filters</button> : undefined}
              />
            ) : (
              <div className="table-wrap">
                <table className="table table--cards activity-table backend-history-table">
                  <caption className="sr-only">Persisted terminal search history</caption>
                  <thead><tr><th scope="col">Search</th><th scope="col">Context</th><th scope="col">Final state</th><th scope="col">Runtime</th><th scope="col">Matched events</th><th scope="col">Rows</th><th scope="col">Finished</th><th scope="col"><span className="sr-only">Actions</span></th></tr></thead>
                  <tbody>{filtered.map((entry) => {
                    const label = searchJobStateLabel(entry.finalState);
                    const display = historyEntryForDisplay(entry, formatActivityDate);
                    const appLabel = entry.search.appId === undefined
                      ? "No app"
                      : appNames[entry.search.appId] ?? entry.search.appId;
                    return (
                      <tr key={entry.id}>
                        <td data-label="Search"><Link href={launchHref(entry)} aria-label={`Rerun search ${entry.id}`}><strong>{entry.search.spl}</strong><code>{entry.id}</code>{entry.failureMessage ? <small className="table-error-detail" title={entry.failureMessage}>{entry.failureMessage}</small> : entry.warningCount > 0 ? <small className="table-warning-detail">{entry.warningCount} {entry.warningCount === 1 ? "warning" : "warnings"}</small> : null}</Link></td>
                        <td data-label="Context">
                          <strong>{appLabel}</strong>
                          <small>{display.sourceLabel ?? "Ad hoc"}</small>
                          <code title={display.resolvedTimeRange}>{display.timeRange}</code>
                        </td>
                        <td data-label="Final state"><StatusLabel tone={searchJobStateTone(entry.finalState)}>{label}</StatusLabel></td>
                        <td data-label="Runtime">{formatActivityDuration(entry.durationMs)}</td>
                        <td data-label="Matched events" className="numeric-data">{formatActivityCount(entry.matchedEvents)}</td>
                        <td data-label="Rows" className="numeric-data">{formatActivityCount(entry.producedRows)}</td>
                        <td data-label="Finished">{display.ranAt}</td>
                        <td data-label="Actions">
                          <div className="history-row-actions">
                            <Link className="table-action" href={launchHref(entry)} aria-label={`Rerun search ${entry.id}`}>Rerun <AppIcon name="chevron-right" size="xs" /></Link>
                            <button type="button" disabled={actionPending !== null} aria-label={`Delete history entry ${entry.id}`} onClick={() => { setActionError(null); setModal({ action: "delete", target: entry }); }}>Delete</button>
                          </div>
                        </td>
                      </tr>
                    );
                  })}</tbody>
                </table>
              </div>
            )}
          </section>

        </>
      ) : null}

      {modal?.action === "delete" ? (
        <Modal
          title="Delete search-history entry?"
          subtitle={`Permanently remove terminal metadata for ${modal.target.id}.`}
          dismissible={actionPending === null}
          onClose={closeHistoryModal}
          footer={(
            <>
              <button className="button button--secondary" type="button" disabled={actionPending !== null} onClick={closeHistoryModal}>Keep entry</button>
              <button className="button button--danger" type="button" disabled={actionPending !== null} aria-busy={actionPending === "delete"} onClick={() => void deleteHistoryEntry()}>
                {actionPending === "delete" ? "Deleting…" : "Delete entry"}
              </button>
            </>
          )}
        >
          <p>This cannot be undone. The saved-search definition, if one exists, is not changed.</p>
          <code className="history-confirm-query">{modal.target.search.spl}</code>
          {actionError === null ? null : <p className="history-action-error" role="alert">{actionError}</p>}
        </Modal>
      ) : null}

      {modal?.action === "clear" ? (
        <Modal
          title="Clear matching search history?"
          subtitle={`Permanently remove ${clearScope}.`}
          dismissible={actionPending === null}
          onClose={closeHistoryModal}
          footer={(
            <>
              <button className="button button--secondary" type="button" disabled={actionPending !== null} onClick={closeHistoryModal}>Keep history</button>
              <button className="button button--danger" type="button" disabled={actionPending !== null || !filtersSettled || filter === "warnings"} aria-busy={actionPending === "clear"} onClick={() => void clearHistory()}>
                {actionPending === "clear" ? "Clearing…" : "Clear matching history"}
              </button>
            </>
          )}
        >
          <p>This server-side action includes matches that are not loaded in the table. Saved-search definitions and retained transient jobs are not changed.</p>
          <dl className="history-clear-scope">
            <div><dt>Final state</dt><dd>{filter === "all" ? "Any terminal state" : historyFilterLabel(filter)}</dd></div>
            <div><dt>SPL contains</dt><dd>{textFilter || "Any query"}</dd></div>
          </dl>
          {actionError === null ? null : <p className="history-action-error" role="alert">{actionError}</p>}
        </Modal>
      ) : null}
    </div>
  );
}
