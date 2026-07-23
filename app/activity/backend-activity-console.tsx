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

import { SearchJobState } from "@/gen/ts/open_splunk/v1/search";
import {
  createOpenSplunkApiClient,
  getSystemBootstrap,
  recordNextPageToken,
  RepeatedPageCursorError,
  type SystemBootstrapModel,
} from "@/lib/api";
import { historySearchLaunchHref } from "@/lib/search/launch-url";
import {
  historyEntryForDisplay,
  listServerSearchHistory,
  type ServerSearchHistoryEntry,
} from "@/lib/search/server-objects";

import { PageHeading } from "../_components/product-shell";
import {
  ActivityState,
  formatActivityCount,
  formatActivityDate,
  formatActivityDuration,
  searchJobStateClass,
  searchJobStateLabel,
} from "./backend-activity-shared";
import { BackendLiveJobs } from "./backend-live-jobs";

type ActivityFilter = "all" | "completed" | "failed" | "canceled" | "warnings";
type ActivityView = "jobs" | "history";
type LoadState = "loading" | "available" | "unavailable" | "error";

interface BackendActivityConsoleProps {
  apiBaseUrl: string;
}

function errorMessage(error: unknown): string {
  return error instanceof Error && error.message.trim().length > 0
    ? error.message
    : "The server did not return a usable search history response.";
}

function launchHref(entry: ServerSearchHistoryEntry): string {
  return historySearchLaunchHref(entry.id);
}

export function BackendActivityConsole({ apiBaseUrl }: BackendActivityConsoleProps) {
  const [view, setView] = useState<ActivityView>("jobs");
  const [historyVisited, setHistoryVisited] = useState(false);
  const jobTabRef = useRef<HTMLButtonElement>(null);
  const historyTabRef = useRef<HTMLButtonElement>(null);

  function selectView(nextView: ActivityView) {
    setView(nextView);
    if (nextView === "history") setHistoryVisited(true);
  }

  function navigateTabs(event: ReactKeyboardEvent<HTMLButtonElement>) {
    if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
    event.preventDefault();
    const currentView: ActivityView = event.currentTarget.id === "activity-jobs-tab" ? "jobs" : "history";
    const nextView: ActivityView = event.key === "Home"
      ? "jobs"
      : event.key === "End"
        ? "history"
        : event.key === "ArrowLeft"
          ? currentView === "jobs" ? "history" : "jobs"
          : currentView === "history" ? "jobs" : "history";
    selectView(nextView);
    window.requestAnimationFrame(() => {
      (nextView === "jobs" ? jobTabRef.current : historyTabRef.current)?.focus();
    });
  }

  return (
    <div className="suite-page activity-page backend-activity-page">
      <PageHeading
        eyebrow="OPERATIONS"
        title="Activity"
        description="Inspect retained backend jobs and separately persisted terminal-search history."
      />
      <div className="activity-view-tabs" role="tablist" aria-label="Activity data view">
        <button
          ref={jobTabRef}
          id="activity-jobs-tab"
          role="tab"
          type="button"
          aria-controls="activity-jobs-panel"
          aria-selected={view === "jobs"}
          tabIndex={view === "jobs" ? 0 : -1}
          onClick={() => selectView("jobs")}
          onKeyDown={navigateTabs}
        >
          <span aria-hidden="true">↻</span>
          <span><strong>Current jobs</strong><small>Retained transient executions</small></span>
        </button>
        <button
          ref={historyTabRef}
          id="activity-history-tab"
          role="tab"
          type="button"
          aria-controls="activity-history-panel"
          aria-selected={view === "history"}
          tabIndex={view === "history" ? 0 : -1}
          onClick={() => selectView("history")}
          onKeyDown={navigateTabs}
        >
          <span aria-hidden="true">▤</span>
          <span><strong>Search history</strong><small>Persisted terminal metadata</small></span>
        </button>
      </div>
      <section id="activity-jobs-panel" role="tabpanel" aria-labelledby="activity-jobs-tab" hidden={view !== "jobs"}>
        <BackendLiveJobs apiBaseUrl={apiBaseUrl} />
      </section>
      <section id="activity-history-panel" role="tabpanel" aria-labelledby="activity-history-tab" hidden={view !== "history"}>
        {historyVisited ? <BackendSearchHistory apiBaseUrl={apiBaseUrl} /> : null}
      </section>
    </div>
  );
}

function BackendSearchHistory({ apiBaseUrl }: BackendActivityConsoleProps) {
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
  const bootstrapRef = useRef<SystemBootstrapModel | null>(null);
  const loadMoreAbortRef = useRef<AbortController | null>(null);
  const pageTokensSeenRef = useRef<Set<string>>(new Set());
  const reload = useCallback(() => setGeneration((current) => current + 1), []);

  useEffect(() => {
    loadMoreAbortRef.current?.abort();
    loadMoreAbortRef.current = null;
    const controller = new AbortController();
    let current = true;
    setState("loading");
    setError(null);
    setLoadMoreError(null);
    setLoadingMore(false);
    setNextPageToken(null);
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
  }, [client, generation]);

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
  }, [client, nextPageToken]);

  const filtered = useMemo(() => {
    const normalized = query.trim().toLowerCase();
    return entries.filter((entry) => {
      if (filter === "completed" && entry.finalState !== SearchJobState.SEARCH_JOB_STATE_COMPLETED) return false;
      if (filter === "failed" && entry.finalState !== SearchJobState.SEARCH_JOB_STATE_FAILED && entry.finalState !== SearchJobState.SEARCH_JOB_STATE_EXPIRED) return false;
      if (filter === "canceled" && entry.finalState !== SearchJobState.SEARCH_JOB_STATE_CANCELED) return false;
      if (filter === "warnings" && entry.warningCount === 0) return false;
      const display = historyEntryForDisplay(entry);
      return normalized.length === 0
        || `${entry.search.spl} ${entry.id} ${entry.failureMessage ?? ""} ${entry.search.appId ?? ""} ${appNames[entry.search.appId ?? ""] ?? ""} ${display.sourceLabel ?? ""} ${display.resolvedTimeRange ?? ""}`
          .toLowerCase()
          .includes(normalized);
    });
  }, [appNames, entries, filter, query]);

  const completed = entries.filter((entry) => entry.finalState === SearchJobState.SEARCH_JOB_STATE_COMPLETED).length;
  const failed = entries.filter((entry) =>
    entry.finalState === SearchJobState.SEARCH_JOB_STATE_FAILED
    || entry.finalState === SearchJobState.SEARCH_JOB_STATE_EXPIRED).length;
  const canceled = entries.filter((entry) => entry.finalState === SearchJobState.SEARCH_JOB_STATE_CANCELED).length;
  const warnings = entries.filter((entry) => entry.warningCount > 0).length;
  const loadedLabel = totalSizeExact && totalSize !== null ? totalSize.toLocaleString() : entries.length.toLocaleString();

  return (
    <div className="backend-activity-view">
      {state === "loading" ? <ActivityState kind="loading" title="Loading search history" message="Reading persisted terminal-search metadata…" /> : null}
      {state === "unavailable" ? <ActivityState kind="unavailable" title="Search history is unavailable" message="The backend does not advertise or register persisted search history. Current transient jobs remain available in their separate tab." action={<button type="button" onClick={reload}>Retry</button>} /> : null}
      {state === "error" ? <ActivityState kind="error" title="Search history could not be loaded" message={error ?? "The search history request failed."} action={<button type="button" onClick={reload}>Retry</button>} /> : null}

      {state === "available" ? (
        <>
          <output className="live-jobs-snapshot history-snapshot">
            <span><i aria-hidden="true" />This view contains persisted terminal-search metadata, not the transient job list.</span>
            <button type="button" onClick={reload}>Refresh history</button>
          </output>
          <section className="activity-summary" aria-label="Loaded search history summary">
            <article><span className="status-dot status-dot--healthy" /><div><strong>{loadedLabel}</strong><small>{totalSizeExact ? "Total history entries" : "Entries loaded"}</small></div></article>
            <article><span className="status-dot status-dot--healthy" /><div><strong>{completed}</strong><small>{complete ? "Completed" : "Completed loaded"}</small></div></article>
            <article><span className="status-dot status-dot--warning" /><div><strong>{warnings}</strong><small>{complete ? "With warnings" : "Warnings loaded"}</small></div></article>
            <article><span className="status-dot status-dot--error" /><div><strong>{failed + canceled}</strong><small>{complete ? "Failed or canceled" : "Failed/canceled loaded"}</small></div></article>
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
                <legend className="sr-only">Filter loaded history entries by status</legend>
                {(["all", "completed", "failed", "canceled", "warnings"] as const).map((item) => (
                  <button className={`activity-filter-button${filter === item ? " active" : ""}`} aria-pressed={filter === item} type="button" onClick={() => setFilter(item)} key={item}>
                    {item === "failed" ? "Failed or expired" : item[0].toUpperCase() + item.slice(1)}
                    {item === "completed" ? <span>{completed}</span> : item === "failed" ? <span>{failed}</span> : item === "canceled" ? <span>{canceled}</span> : item === "warnings" ? <span>{warnings}</span> : null}
                  </button>
                ))}
              </fieldset>
              <small className="activity-filter-scope">Loaded entries only</small>
              <label><span className="sr-only">Filter loaded search history entries</span><i aria-hidden="true">⌕</i><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Filter loaded SPL, app, source, or ID" /></label>
            </header>
            {filtered.length === 0 ? (
              <ActivityState
                kind="empty"
                title={entries.length === 0 ? "No persisted search history" : "No matching history"}
                message={entries.length === 0 ? "Completed, failed, or canceled searches will appear here when the backend persists them." : !complete ? "Load more entries or try another status or text filter." : "Try another status or clear the text filter."}
                action={entries.length > 0 ? <button type="button" onClick={() => { setFilter("all"); setQuery(""); }}>Clear filters</button> : undefined}
              />
            ) : (
              <div className="responsive-table-wrap">
                <table className="product-table activity-table backend-history-table">
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
                        <td data-label="Final state"><span className={`status-label status-label--${searchJobStateClass(entry.finalState)}`}><i />{label}</span></td>
                        <td data-label="Runtime">{formatActivityDuration(entry.durationMs)}</td>
                        <td data-label="Matched events" className="numeric-data">{formatActivityCount(entry.matchedEvents)}</td>
                        <td data-label="Rows" className="numeric-data">{formatActivityCount(entry.producedRows)}</td>
                        <td data-label="Finished">{display.ranAt}</td>
                        <td data-label="Actions"><Link className="table-action" href={launchHref(entry)} aria-label={`Rerun search ${entry.id}`}>Rerun ›</Link></td>
                      </tr>
                    );
                  })}</tbody>
                </table>
              </div>
            )}
          </section>

          <section className="suite-card backend-unavailable-card">
            <header className="suite-card-header"><div><h2>Administrative events</h2><p>Unavailable in backend mode.</p></div><span aria-hidden="true">i</span></header>
            <p>The backend does not register an audit-event route, so this page does not fabricate configuration or collector activity.</p>
          </section>
        </>
      ) : null}
    </div>
  );
}
