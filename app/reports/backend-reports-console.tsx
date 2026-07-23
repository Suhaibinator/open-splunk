"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";

import { SharingScope } from "@/gen/ts/open_splunk/v1/common";
import {
  createOpenSplunkApiClient,
  getSystemBootstrap,
  recordNextPageToken,
  RepeatedPageCursorError,
  type SystemBootstrapModel,
} from "@/lib/api";
import { savedSearchLaunchHref } from "@/lib/search/launch-url";
import {
  listServerSavedSearches,
  savedSearchForDisplay,
  type ServerSavedSearch,
} from "@/lib/search/server-objects";

import { PageHeading } from "../_components/product-shell";
import styles from "./reports.module.css";

type SavedSearchScope = "all" | "private" | "app" | "global";
type SortOrder = "updated" | "name";
type LoadState = "loading" | "available" | "unavailable" | "error";

interface BackendReportsConsoleProps {
  apiBaseUrl: string;
}

function errorMessage(error: unknown): string {
  return error instanceof Error && error.message.trim().length > 0
    ? error.message
    : "The server did not return a usable saved-search response.";
}

function scopeLabel(scope: SharingScope): string {
  if (scope === SharingScope.SHARING_SCOPE_GLOBAL) return "Global";
  if (scope === SharingScope.SHARING_SCOPE_APP) return "App";
  if (scope === SharingScope.SHARING_SCOPE_PRIVATE) return "Private";
  return "Unknown";
}

function scopeMatches(savedSearch: ServerSavedSearch, scope: SavedSearchScope): boolean {
  if (scope === "all") return true;
  if (scope === "global") return savedSearch.sharingScope === SharingScope.SHARING_SCOPE_GLOBAL;
  if (scope === "app") return savedSearch.sharingScope === SharingScope.SHARING_SCOPE_APP;
  return savedSearch.sharingScope === SharingScope.SHARING_SCOPE_PRIVATE;
}

function formatDate(value: Date | null): string {
  if (value === null || Number.isNaN(value.valueOf())) return "Not recorded";
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(value);
}

function launchHref(savedSearch: ServerSavedSearch): string {
  return savedSearchLaunchHref(savedSearch.id);
}

export function BackendReportsConsole({ apiBaseUrl }: BackendReportsConsoleProps) {
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
  const [refreshing, setRefreshing] = useState(false);
  const [query, setQuery] = useState("");
  const [scope, setScope] = useState<SavedSearchScope>("all");
  const [sort, setSort] = useState<SortOrder>("updated");
  const bootstrapRef = useRef<SystemBootstrapModel | null>(null);
  const loadMoreAbortRef = useRef<AbortController | null>(null);
  const pageTokensSeenRef = useRef<Set<string>>(new Set());
  const hasLoadedRef = useRef(false);
  const reload = useCallback(() => setGeneration((current) => current + 1), []);

  useEffect(() => {
    const retainShell = hasLoadedRef.current;
    loadMoreAbortRef.current?.abort();
    loadMoreAbortRef.current = null;
    const controller = new AbortController();
    let current = true;
    if (!retainShell) setState("loading");
    setRefreshing(retainShell);
    setError(null);
    setLoadMoreError(null);
    setLoadingMore(false);
    if (!retainShell) {
      setNextPageToken(null);
      bootstrapRef.current = null;
      pageTokensSeenRef.current.clear();
    }
    void (async () => {
      try {
        const bootstrap = await getSystemBootstrap(client, undefined, { signal: controller.signal });
        bootstrapRef.current = bootstrap;
        setAppNames(Object.fromEntries(
          bootstrap.apps.map((app) => [app.appId, app.displayName || app.slug || app.appId]),
        ));
        const result = await listServerSavedSearches(client, bootstrap, {
          signal: controller.signal,
          pageSize: Math.max(1, Math.min(bootstrap.limits.maximumPageSize || 50, 100)),
          maximumPages: 1,
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
      const result = await listServerSavedSearches(client, bootstrap, {
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
  }, [client, nextPageToken]);

  const counts = useMemo(() => ({
    all: savedSearches.length,
    private: savedSearches.filter((item) => item.sharingScope === SharingScope.SHARING_SCOPE_PRIVATE).length,
    app: savedSearches.filter((item) => item.sharingScope === SharingScope.SHARING_SCOPE_APP).length,
    global: savedSearches.filter((item) => item.sharingScope === SharingScope.SHARING_SCOPE_GLOBAL).length,
  }), [savedSearches]);

  const visible = useMemo(() => {
    const normalized = query.trim().toLowerCase();
    return savedSearches
      .filter((item) =>
        scopeMatches(item, scope)
        && (normalized.length === 0
          || `${item.name} ${item.description} ${item.search.spl} ${item.ownerId ?? ""} ${item.search.appId ?? ""} ${appNames[item.search.appId ?? ""] ?? ""}`
            .toLowerCase()
            .includes(normalized)))
      .toSorted((left, right) => {
        if (sort === "name") return left.name.localeCompare(right.name);
        return (right.updatedAt?.valueOf() ?? right.createdAt?.valueOf() ?? 0)
          - (left.updatedAt?.valueOf() ?? left.createdAt?.valueOf() ?? 0);
      });
  }, [appNames, query, savedSearches, scope, sort]);

  const displayedTotal = totalSizeExact && totalSize !== null
    ? totalSize.toLocaleString()
    : savedSearches.length.toLocaleString();
  const refreshPending = state === "loading" || refreshing;

  return (
    <div className={`suite-page ${styles.page}`}>
      <PageHeading
        eyebrow="SEARCH & REPORTING"
        title="Saved searches"
        description="Open reusable search definitions persisted by the connected server."
        actions={<><Link className="suite-button" href="/search/">Open Search</Link><button className="suite-button suite-button--primary" type="button" aria-busy={refreshPending} aria-disabled={refreshPending} onClick={() => { if (!refreshPending) reload(); }}>{refreshing ? "Refreshing…" : state === "loading" ? "Loading…" : "Refresh"}</button></>}
      />

      {state === "loading" ? <SavedSearchState kind="loading" title="Loading saved searches" message="Reading persisted definitions from the server…" /> : null}
      {state === "unavailable" ? <SavedSearchState kind="unavailable" title="Saved searches are unavailable" message="The backend does not advertise or register the saved-search service. Scheduled reports are not substituted. Use Refresh to retry." /> : null}
      {state === "error" ? <SavedSearchState kind="error" title="Saved searches could not be loaded" message={`${error ?? "The request failed."} Use Refresh to retry.`} /> : null}

      {state === "available" ? (
        <>
          {refreshing ? <output className="backend-list-notice">Refreshing saved searches. Existing definitions remain visible until the request completes.</output> : null}
          {error === null ? null : <div className="backend-inline-error" role="alert">The latest refresh failed; the previous saved-search snapshot remains visible. {error}</div>}
          <section className={styles.summary} aria-label="Saved search summary">
            <article><span className={styles.metricIcon} aria-hidden="true">▤</span><div><strong>{displayedTotal}</strong><small>{totalSizeExact ? "Saved searches" : "Definitions loaded"}</small></div></article>
            <article><span className={styles.metricIcon} aria-hidden="true">♙</span><div><strong>{counts.private}</strong><small>{complete ? "Private" : "Private loaded"}</small></div></article>
            <article><span className={styles.metricIcon} aria-hidden="true">◎</span><div><strong>{counts.app + counts.global}</strong><small>{complete ? "Shared" : "Shared loaded"}</small></div></article>
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

          <section className={`suite-card ${styles.library}`} aria-labelledby="saved-search-library-title" aria-busy={refreshing}>
            <header className={styles.libraryHeader}>
              <div><h2 id="saved-search-library-title">Saved search library</h2><p>These are reusable definitions, not scheduled reports.</p></div>
              <span><strong>{visible.length}</strong> {visible.length === 1 ? "saved search" : "saved searches"}{complete ? "" : " loaded"}</span>
            </header>

            <div className={styles.scopeBar} aria-label={complete ? "Saved search sharing scope" : "Saved search sharing scope; counts apply to loaded definitions only"}>
              {([
                ["all", "All saved searches"],
                ["private", "Private"],
                ["app", "App"],
                ["global", "Global"],
              ] as const).map(([id, label]) => (
                <button className={scope === id ? styles.scopeActive : undefined} type="button" aria-pressed={scope === id} onClick={() => setScope(id)} key={id}>
                  {label}<span>{counts[id]}{complete ? null : <small className={styles.loadedCountLabel}> loaded</small>}</span>
                </button>
              ))}
            </div>
            {complete ? null : <p className={styles.loadedOnlyNote}>Scope filters, text filtering, sorting, and badges apply to loaded definitions only.</p>}

            <div className={`${styles.toolbar} ${styles.backendToolbar}`}>
              <label className={styles.searchField}>
                <span className="sr-only">Filter saved searches</span><i aria-hidden="true">⌕</i>
                <input type="search" value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Find by name, SPL, app, or owner" />
              </label>
              <label className={styles.selectField}>
                <span>Sort</span>
                <select value={sort} onChange={(event) => setSort(event.target.value as SortOrder)}>
                  <option value="updated">Recently modified</option>
                  <option value="name">Name</option>
                </select>
              </label>
            </div>

            {visible.length === 0 ? (
              <div className={styles.empty}>
                <span aria-hidden="true">⌕</span>
                <strong>{savedSearches.length === 0 ? "No saved searches" : "No matching saved searches"}</strong>
                <p>{savedSearches.length === 0 ? "Save a search from the Search workspace to add its reusable definition here." : !complete ? "Load more definitions or try another phrase or sharing scope." : "Try another phrase or sharing scope."}</p>
                {savedSearches.length > 0 ? <button type="button" onClick={() => { setQuery(""); setScope("all"); }}>Clear filters</button> : <Link href="/search/">Open Search</Link>}
              </div>
            ) : (
              <div className={styles.tableWrap}>
                <table className={`${styles.table} ${styles.backendTable}`}>
                  <thead><tr><th scope="col">Saved search</th><th scope="col">App</th><th scope="col">Sharing</th><th scope="col">Owner</th><th scope="col">Time range</th><th scope="col">Modified</th><th scope="col"><span className="sr-only">Open</span></th></tr></thead>
                  <tbody>{visible.map((savedSearch) => {
                    const display = savedSearchForDisplay(savedSearch, formatDate);
                    return (
                      <tr key={savedSearch.id}>
                        <td className={styles.reportColumn} aria-label={`Saved search ${savedSearch.name}`}>
                          <div className={styles.reportCell}>
                            <span className={`${styles.reportIcon} ${styles.reportIconStatistics}`} aria-hidden="true">⌕</span>
                            <div><Link href={launchHref(savedSearch)}>{savedSearch.name}</Link>{savedSearch.description ? <small>{savedSearch.description}</small> : null}<code>{savedSearch.search.spl}</code></div>
                          </div>
                        </td>
                        <td data-label="App">{savedSearch.search.appId === undefined
                          ? "No app"
                          : appNames[savedSearch.search.appId] ?? savedSearch.search.appId}</td>
                        <td data-label="Sharing"><span className={styles.status}><i />{scopeLabel(savedSearch.sharingScope)}</span></td>
                        <td data-label="Owner">{savedSearch.ownerId || "Current user"}</td>
                        <td data-label="Time range"><code>{savedSearch.search.timeRange?.earliest ?? "Server default"} → {savedSearch.search.timeRange?.latest ?? "Server default"}</code></td>
                        <td data-label="Modified">{display.updatedAt}</td>
                        <td className={styles.openCell}><Link href={launchHref(savedSearch)} aria-label={`Open ${savedSearch.name} in Search`}>Open in Search <span aria-hidden="true">›</span></Link></td>
                      </tr>
                    );
                  })}</tbody>
                </table>
              </div>
            )}
          </section>

          <section className="suite-card backend-unavailable-card">
            <header className="suite-card-header"><div><h2>Scheduling and delivery</h2><p>Unavailable in backend mode.</p></div><span aria-hidden="true">i</span></header>
            <p>The backend persists saved search definitions but does not register report scheduling, execution, or delivery routes. No schedule or last-run status is inferred.</p>
          </section>
        </>
      ) : null}
    </div>
  );
}

interface SavedSearchStateProps {
  kind: "loading" | "unavailable" | "error";
  title: string;
  message: string;
  action?: React.ReactNode;
}

function SavedSearchState({ kind, title, message, action }: SavedSearchStateProps) {
  return (
    <div className={`backend-resource-state backend-resource-state--${kind}`} role={kind === "error" ? "alert" : "status"}>
      <span aria-hidden="true">{kind === "loading" ? "↻" : kind === "error" ? "!" : "i"}</span>
      <div><strong>{title}</strong><p>{message}</p></div>
      {action}
    </div>
  );
}
