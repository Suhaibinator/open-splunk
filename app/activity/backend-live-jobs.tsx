"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";

import {
  SearchJobState,
  type SearchProgress,
} from "@/gen/ts/open_splunk/v1/search";
import {
  createOpenSplunkApiClient,
  getSystemBootstrap,
  isHttpStatus,
  recordNextPageToken,
  RepeatedPageCursorError,
  type SystemBootstrapModel,
} from "@/lib/api";
import { searchLaunchHref } from "@/lib/search/launch-url";
import {
  cancelServerSearchJob,
  listServerSearchJobs,
  rerunServerSearchJob,
  serverSearchJobCanCancel,
  serverSearchJobOriginLabel,
  type ServerSearchJob,
} from "@/lib/search/server-api";

import {
  ActivityState,
  formatActivityCount,
  formatActivityDate,
  formatActivityDuration,
  formatActivityTime,
  searchJobStateClass,
  searchJobStateLabel,
} from "./backend-activity-shared";
import { formatDecimalBytes } from "../search-workspace/formatters";

type LiveJobFilter = "all" | "active" | "completed" | "failed" | "canceled";
type LoadState = "loading" | "available" | "unavailable" | "error";
type ExactCountKey = LiveJobFilter | "unsuccessful";
type ExactCounts = Record<ExactCountKey, bigint>;

const ACTIVE_STATES = [
  SearchJobState.SEARCH_JOB_STATE_QUEUED,
  SearchJobState.SEARCH_JOB_STATE_PARSING,
  SearchJobState.SEARCH_JOB_STATE_PLANNING,
  SearchJobState.SEARCH_JOB_STATE_RUNNING,
] as const;

const FILTERS: ReadonlyArray<{
  key: LiveJobFilter;
  label: string;
  states: readonly SearchJobState[];
}> = [
  { key: "all", label: "All", states: [] },
  { key: "active", label: "Active", states: ACTIVE_STATES },
  { key: "completed", label: "Completed", states: [SearchJobState.SEARCH_JOB_STATE_COMPLETED] },
  {
    key: "failed",
    label: "Failed or expired",
    states: [
      SearchJobState.SEARCH_JOB_STATE_FAILED,
      SearchJobState.SEARCH_JOB_STATE_EXPIRED,
    ],
  },
  { key: "canceled", label: "Canceled", states: [SearchJobState.SEARCH_JOB_STATE_CANCELED] },
];

const COUNT_FILTERS: ReadonlyArray<{
  key: ExactCountKey;
  states: readonly SearchJobState[];
}> = [
  ...FILTERS,
  {
    key: "unsuccessful",
    states: [
      SearchJobState.SEARCH_JOB_STATE_FAILED,
      SearchJobState.SEARCH_JOB_STATE_CANCELED,
      SearchJobState.SEARCH_JOB_STATE_EXPIRED,
    ],
  },
];

interface BackendLiveJobsProps {
  apiBaseUrl: string;
}

function errorMessage(error: unknown): string {
  return error instanceof Error && error.message.trim().length > 0
    ? error.message
    : "The server did not return a usable search job response.";
}

function elapsedMilliseconds(progress: SearchProgress | null): number {
  const elapsed = progress?.elapsed;
  if (elapsed === undefined) return 0;
  const milliseconds = Number(elapsed.seconds) * 1_000 + elapsed.nanos / 1_000_000;
  return Number.isFinite(milliseconds) && milliseconds >= 0 ? milliseconds : 0;
}

function progressPercent(progress: SearchProgress | null, state: SearchJobState): number | null {
  if (state === SearchJobState.SEARCH_JOB_STATE_COMPLETED) return 100;
  const value = progress?.percentComplete;
  return value !== undefined && Number.isFinite(value)
    ? Math.max(0, Math.min(100, value))
    : null;
}

function jobTimeRange(job: ServerSearchJob): string {
  const earliest = job.definition.timeRange?.earliest;
  const latest = job.definition.timeRange?.latest;
  if (earliest !== undefined && latest !== undefined) return `${earliest} → ${latest}`;
  if (earliest !== undefined) return `${earliest} → server default`;
  if (latest !== undefined) return `server default → ${latest}`;
  return "Server default time range";
}

function openSearchHref(job: ServerSearchJob): string {
  return searchLaunchHref(job.definition.spl, {
    earliest: job.definition.timeRange?.earliest,
    latest: job.definition.timeRange?.latest,
    timezone: job.definition.timeRange?.timezone,
    label: "Opened from Activity",
    run: false,
  });
}

function filterStates(filter: LiveJobFilter): readonly SearchJobState[] {
  return FILTERS.find((candidate) => candidate.key === filter)?.states ?? [];
}

export function BackendLiveJobs({ apiBaseUrl }: BackendLiveJobsProps) {
  const client = useMemo(() => createOpenSplunkApiClient({ baseUrl: apiBaseUrl }), [apiBaseUrl]);
  const [state, setState] = useState<LoadState>("loading");
  const [refreshing, setRefreshing] = useState(false);
  const [jobs, setJobs] = useState<ServerSearchJob[]>([]);
  const [filter, setFilter] = useState<LiveJobFilter>("all");
  const [query, setQuery] = useState("");
  const [textFilter, setTextFilter] = useState("");
  const [appId, setAppId] = useState("");
  const [apps, setApps] = useState<Array<{ id: string; label: string }>>([]);
  const [counts, setCounts] = useState<ExactCounts | null>(null);
  const [countsError, setCountsError] = useState<string | null>(null);
  const [totalSize, setTotalSize] = useState<bigint | null>(null);
  const [complete, setComplete] = useState(true);
  const [nextPageToken, setNextPageToken] = useState<string | null>(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const [loadMoreError, setLoadMoreError] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [refreshedAt, setRefreshedAt] = useState<Date | null>(null);
  const [generation, setGeneration] = useState(0);
  const [jobAction, setJobAction] = useState<{ id: string; kind: "cancel" | "rerun" } | null>(null);
  const [actionNotice, setActionNotice] = useState<{ kind: "success" | "error"; message: string } | null>(null);
  const bootstrapRef = useRef<SystemBootstrapModel | null>(null);
  const loadMoreAbortRef = useRef<AbortController | null>(null);
  const pageTokensSeenRef = useRef<Set<string>>(new Set());
  const jobIdsSeenRef = useRef<Set<string>>(new Set());
  const lastLoadedJobRef = useRef<ServerSearchJob | null>(null);
  const hasLoadedRef = useRef(false);
  const refreshButtonRef = useRef<HTMLButtonElement>(null);
  const reload = useCallback(() => setGeneration((current) => current + 1), []);
  const focusRefreshControl = useCallback(() => {
    window.requestAnimationFrame(() => refreshButtonRef.current?.focus());
  }, []);

  useEffect(() => {
    const timeout = window.setTimeout(() => setTextFilter(query.trim()), 300);
    return () => window.clearTimeout(timeout);
  }, [query]);

  useEffect(() => {
    const retainShell = hasLoadedRef.current;
    loadMoreAbortRef.current?.abort();
    loadMoreAbortRef.current = null;
    const controller = new AbortController();
    let current = true;
    if (!retainShell) setState("loading");
    setRefreshing(retainShell);
    setError(null);
    setCounts(null);
    setCountsError(null);
    if (!retainShell) setJobs([]);
    setTotalSize(null);
    setNextPageToken(null);
    setComplete(true);
    setLoadMoreError(null);
    setLoadingMore(false);
    bootstrapRef.current = null;
    pageTokensSeenRef.current.clear();
    jobIdsSeenRef.current.clear();
    lastLoadedJobRef.current = null;

    void (async () => {
      try {
        const bootstrap = await getSystemBootstrap(client, undefined, { signal: controller.signal });
        if (!current) return;
        bootstrapRef.current = bootstrap;
        setApps(bootstrap.apps
          .filter((app) => app.appId.trim().length > 0)
          .map((app) => ({
            id: app.appId,
            label: app.displayName || app.slug || app.appId,
          })));
        const primary = await listServerSearchJobs(client, bootstrap, {
          signal: controller.signal,
          states: filterStates(filter),
          appId: appId || undefined,
          text: textFilter || undefined,
          pageSize: Math.max(1, Math.min(bootstrap.limits.maximumPageSize || 15, 15)),
          maximumPages: 1,
          includeTotalSize: true,
        });
        if (!current) return;
        if (primary.status === "unavailable") {
          hasLoadedRef.current = false;
          setRefreshing(false);
          setState("unavailable");
          return;
        }
        const primaryPage = primary.value;
        for (const job of primaryPage.items) jobIdsSeenRef.current.add(job.id);
        lastLoadedJobRef.current = primaryPage.items.at(-1) ?? null;
        setJobs(primaryPage.items);
        setTotalSize(primaryPage.totalSize);
        setComplete(primaryPage.complete);
        setNextPageToken(recordNextPageToken(
          pageTokensSeenRef.current,
          primaryPage.nextPageToken,
          "Search jobs",
        ));
        setRefreshedAt(new Date());
        hasLoadedRef.current = true;
        setRefreshing(false);
        setState("available");

        try {
          const pairs = await Promise.all(COUNT_FILTERS.map(async (candidate) => {
            if (candidate.key === filter && primaryPage.totalSize !== null) {
              return [candidate.key, primaryPage.totalSize] as const;
            }
            const countResult = await listServerSearchJobs(client, bootstrap, {
              signal: controller.signal,
              states: candidate.states,
              appId: appId || undefined,
              text: textFilter || undefined,
              pageSize: 1,
              maximumPages: 1,
              includeTotalSize: true,
            });
            if (countResult.status === "unavailable" || countResult.value.totalSize === null) {
              throw new Error("The live job route stopped returning exact state counts.");
            }
            return [candidate.key, countResult.value.totalSize] as const;
          }));
          if (!current) return;
          setCounts(Object.fromEntries(pairs) as ExactCounts);
        } catch (reason) {
          if (current && !controller.signal.aborted) {
            setCountsError(errorMessage(reason));
          }
        }
      } catch (reason) {
        if (!current || controller.signal.aborted) return;
        setError(errorMessage(reason));
        setRefreshing(false);
        if (retainShell) {
          setState("available");
        } else {
          hasLoadedRef.current = false;
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
  }, [appId, client, filter, generation, textFilter]);

  const loadMore = useCallback(async () => {
    const bootstrap = bootstrapRef.current;
    const pageToken = nextPageToken;
    if (bootstrap === null || pageToken === null || loadMoreAbortRef.current !== null) return;
    const controller = new AbortController();
    loadMoreAbortRef.current = controller;
    setLoadingMore(true);
    setLoadMoreError(null);
    try {
      const result = await listServerSearchJobs(client, bootstrap, {
        signal: controller.signal,
        states: filterStates(filter),
        appId: appId || undefined,
        text: textFilter || undefined,
        pageSize: Math.max(1, Math.min(bootstrap.limits.maximumPageSize || 15, 15)),
        pageToken,
        maximumPages: 1,
        includeTotalSize: true,
      });
      if (controller.signal.aborted || bootstrapRef.current !== bootstrap) return;
      if (result.status === "unavailable") {
        setState("unavailable");
        setNextPageToken(null);
        return;
      }
      const previous = lastLoadedJobRef.current;
      const first = result.value.items[0];
      if (
        previous !== null
        && first !== undefined
        && (
          previous.createdAt === null
          || first.createdAt === null
          || previous.createdAt.valueOf() < first.createdAt.valueOf()
          || (
            previous.createdAt.valueOf() === first.createdAt.valueOf()
            && previous.id <= first.id
          )
        )
      ) {
        throw new TypeError("Search jobs continued outside the documented newest-first order.");
      }
      const duplicate = result.value.items.find((job) => jobIdsSeenRef.current.has(job.id));
      if (duplicate !== undefined) {
        throw new TypeError(`Search jobs repeated ${duplicate.id} on a later page.`);
      }
      for (const job of result.value.items) jobIdsSeenRef.current.add(job.id);
      if (result.value.items.length > 0) lastLoadedJobRef.current = result.value.items.at(-1) ?? null;
      setJobs((current) => {
        const byId = new Map(current.map((job) => [job.id, job]));
        for (const job of result.value.items) byId.set(job.id, job);
        return [...byId.values()];
      });
      setTotalSize(result.value.totalSize);
      setNextPageToken(recordNextPageToken(
        pageTokensSeenRef.current,
        result.value.nextPageToken,
        "Search jobs",
      ));
      setComplete(result.value.complete);
    } catch (reason) {
      if (!controller.signal.aborted) {
        if (
          reason instanceof RepeatedPageCursorError
          || reason instanceof TypeError
          || isHttpStatus(reason, 400)
        ) {
          setNextPageToken(null);
          setComplete(true);
          setLoadMoreError(`${errorMessage(reason)} Further paging was stopped; refresh the snapshot to request a new cursor.`);
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
  }, [appId, client, filter, nextPageToken, textFilter]);

  const cancelJob = useCallback(async (job: ServerSearchJob) => {
    const bootstrap = bootstrapRef.current;
    if (bootstrap === null || jobAction !== null || !serverSearchJobCanCancel(job)) return;
    setJobAction({ id: job.id, kind: "cancel" });
    setActionNotice(null);
    try {
      const result = await cancelServerSearchJob(client, bootstrap, job.id);
      if (result.status === "unavailable") {
        setActionNotice({ kind: "error", message: "Cancellation is unavailable on this backend." });
        return;
      }
      setJobs((current) => current.map((candidate) => candidate.id === result.value.id ? result.value : candidate));
      setActionNotice({ kind: "success", message: `Cancellation accepted for ${job.id}. Exact counts were refreshed.` });
      reload();
      focusRefreshControl();
    } catch (reason) {
      setActionNotice({ kind: "error", message: errorMessage(reason) });
    } finally {
      setJobAction(null);
    }
  }, [client, focusRefreshControl, jobAction, reload]);

  const rerunJob = useCallback(async (job: ServerSearchJob) => {
    const bootstrap = bootstrapRef.current;
    if (bootstrap === null || jobAction !== null) return;
    setJobAction({ id: job.id, kind: "rerun" });
    setActionNotice(null);
    try {
      const result = await rerunServerSearchJob(client, bootstrap, job);
      if (result.status === "unavailable") {
        setActionNotice({ kind: "error", message: "Rerunning searches is unavailable on this backend." });
        return;
      }
      setActionNotice({ kind: "success", message: `Started a new ad hoc job ${result.value.id} from ${job.id}.` });
      reload();
      focusRefreshControl();
    } catch (reason) {
      setActionNotice({ kind: "error", message: errorMessage(reason) });
    } finally {
      setJobAction(null);
    }
  }, [client, focusRefreshControl, jobAction, reload]);

  const unsuccessfulCount = counts?.unsuccessful ?? null;
  const loadedCount = jobs.length;
  const filteredDescription = [
    filter === "all" ? null : `${FILTERS.find((candidate) => candidate.key === filter)?.label} state`,
    appId.length === 0 ? null : apps.find((app) => app.id === appId)?.label ?? appId,
    textFilter.length === 0 ? null : `SPL containing “${textFilter}”`,
  ].filter((value): value is string => value !== null).join(" · ");

  return (
    <div className="backend-activity-view">
      {state === "loading" ? <ActivityState kind="loading" title="Loading retained jobs" message="Reading the backend’s current transient search-job snapshot…" /> : null}
      {state === "unavailable" ? <ActivityState kind="unavailable" title="Live job listing is unavailable" message="This backend does not advertise Search or does not register the optional job-list route. Persisted history remains available in its own tab." action={<button type="button" onClick={reload}>Retry</button>} /> : null}
      {state === "error" ? <ActivityState kind="error" title="Live jobs could not be loaded" message={error ?? "The live job request failed."} action={<button type="button" onClick={reload}>Retry</button>} /> : null}

      {state === "available" ? (
        <>
          <section className="activity-summary" aria-label="Exact retained job counts">
            <article><span className="status-dot status-dot--healthy" /><div><strong>{counts === null ? "…" : formatActivityCount(counts.all)}</strong><small>Retained jobs</small></div></article>
            <article><span className="status-dot status-dot--running" /><div><strong>{counts === null ? "…" : formatActivityCount(counts.active)}</strong><small>Active now</small></div></article>
            <article><span className="status-dot status-dot--healthy" /><div><strong>{counts === null ? "…" : formatActivityCount(counts.completed)}</strong><small>Completed</small></div></article>
            <article><span className="status-dot status-dot--error" /><div><strong>{unsuccessfulCount === null ? "…" : formatActivityCount(unsuccessfulCount)}</strong><small>Failed, canceled, or expired</small></div></article>
          </section>

          <output className="live-jobs-snapshot">
            <span><i aria-hidden="true" />Each count is exact for its request; last refreshed <strong>{formatActivityTime(refreshedAt)}</strong>. Job states can change between requests.</span>
            <button
              ref={refreshButtonRef}
              type="button"
              aria-busy={refreshing}
              aria-disabled={refreshing}
              onClick={() => {
                if (!refreshing) reload();
              }}
            >
              {refreshing ? "Refreshing…" : "Refresh snapshot"}
            </button>
          </output>
          {countsError === null ? null : <div className="backend-inline-error" role="alert">Jobs loaded, but exact state counts could not be refreshed: {countsError}<button type="button" onClick={reload}>Retry</button></div>}
          {actionNotice === null ? null : (
            <output className={`backend-action-notice backend-action-notice--${actionNotice.kind}`}>
              <span aria-hidden="true">{actionNotice.kind === "success" ? "✓" : "!"}</span>
              <strong>{actionNotice.message}</strong>
              <button type="button" aria-label="Dismiss job action notification" onClick={() => setActionNotice(null)}>×</button>
            </output>
          )}

          <section className="suite-card activity-jobs-card live-jobs-card">
            <header className="live-jobs-toolbar">
              <fieldset className="activity-filter-group">
                <legend className="sr-only">Live job state filter</legend>
                {FILTERS.map((candidate) => (
                  <button
                    className={`activity-filter-button${filter === candidate.key ? " active" : ""}`}
                    aria-pressed={filter === candidate.key}
                    type="button"
                    onClick={() => setFilter(candidate.key)}
                    key={candidate.key}
                  >
                    {candidate.label}
                    <span aria-label={counts === null ? "Count loading" : `${formatActivityCount(counts[candidate.key])} jobs`}>
                      {counts === null ? "…" : formatActivityCount(counts[candidate.key])}
                    </span>
                  </button>
                ))}
              </fieldset>
              <div className="live-jobs-filter-row">
                <label className="live-jobs-text-filter">
                  <span className="sr-only">Filter live jobs by source SPL</span>
                  <i aria-hidden="true">⌕</i>
                  <input
                    value={query}
                    maxLength={256}
                    onChange={(event) => setQuery(event.target.value)}
                    placeholder="Filter source SPL"
                  />
                </label>
                <label className="live-jobs-app-filter">
                  <span className="sr-only">Filter live jobs by app</span>
                  <select value={appId} onChange={(event) => setAppId(event.target.value)}>
                    <option value="">All apps</option>
                    {apps.map((app) => <option value={app.id} key={app.id}>{app.label}</option>)}
                  </select>
                </label>
              </div>
            </header>

            {refreshing ? (
              <ActivityState
                kind="loading"
                title="Updating retained jobs"
                message="Applying the selected filters to a fresh backend snapshot. Existing rows remain visible until the refresh completes."
              />
            ) : null}
            {error !== null && jobs.length > 0 ? (
              <div className="backend-inline-error" role="alert">
                The latest refresh failed; the previous snapshot remains visible. {error}
                <button type="button" onClick={reload}>Retry</button>
              </div>
            ) : null}
            {jobs.length === 0 && error !== null ? (
              <ActivityState
                kind="error"
                title="Filtered jobs could not be loaded"
                message={error}
                action={<button type="button" onClick={reload}>Retry</button>}
              />
            ) : jobs.length === 0 && !refreshing ? (
              <ActivityState
                kind="empty"
                title={filteredDescription.length === 0 ? "No retained search jobs" : "No matching live jobs"}
                message={filteredDescription.length === 0 ? "New backend searches will appear here while their transient job records are retained." : `No retained jobs match ${filteredDescription}.`}
                action={filteredDescription.length > 0 ? <button type="button" onClick={() => { setFilter("all"); setAppId(""); setQuery(""); setTextFilter(""); }}>Clear filters</button> : undefined}
              />
            ) : jobs.length > 0 ? (
              <div className="responsive-table-wrap live-jobs-table-wrap" aria-busy={refreshing}>
                <table className="product-table live-jobs-table">
                  <caption className="sr-only">Retained transient search jobs</caption>
                  <thead><tr><th scope="col">Search</th><th scope="col">Context</th><th scope="col">State</th><th scope="col">Progress</th><th scope="col">Rows / bytes</th><th scope="col">Created</th><th scope="col"><span className="sr-only">Actions</span></th></tr></thead>
                  <tbody>{jobs.map((job) => {
                    const percent = progressPercent(job.progress, job.state);
                    const approximate = job.progress?.countersAreEstimates ?? false;
                    const appLabel = job.definition.appId === undefined
                      ? "No app"
                      : apps.find((app) => app.id === job.definition.appId)?.label ?? job.definition.appId;
                    const cancelable = serverSearchJobCanCancel(job);
                    return (
                      <tr key={job.id}>
                        <td data-label="Search">
                          <strong className="live-job-spl" title={job.definition.spl}>{job.definition.spl}</strong>
                          <code>{job.id}</code>
                          {job.failureMessage !== null ? <small className="table-error-detail" title={job.failureMessage}>{job.failureMessage}</small> : null}
                          {job.warningCount > 0 ? <small className="table-warning-detail">{job.warningCount} {job.warningCount === 1 ? "warning" : "warnings"}</small> : null}
                        </td>
                        <td data-label="Context">
                          <strong>{appLabel}</strong>
                          <small>{serverSearchJobOriginLabel(job.source)}</small>
                          <code title={job.definition.timeRange?.timezone}>{jobTimeRange(job)}</code>
                        </td>
                        <td data-label="State">
                          <span className={`status-label status-label--${searchJobStateClass(job.state)}`}><i />{searchJobStateLabel(job.state)}</span>
                          {job.failureRetryable ? <small>Retryable failure</small> : null}
                        </td>
                        <td data-label="Progress">
                          <div className="live-job-progress">
                            {percent === null ? <span>{searchJobStateLabel(job.state)}</span> : <progress max={100} value={percent} aria-label={`${searchJobStateLabel(job.state)} ${Math.round(percent)} percent`} />}
                            <small>{percent === null ? null : `${Math.round(percent)}% · `}{formatActivityDuration(elapsedMilliseconds(job.progress))}</small>
                          </div>
                        </td>
                        <td data-label="Rows / bytes" className="numeric-data">
                          <strong title={approximate ? "Server reports estimated counters" : "Server-reported row count"}>{approximate ? "~" : ""}{formatActivityCount(job.progress?.producedRows ?? 0n)} rows</strong>
                          <small title={`${formatActivityCount(job.progress?.resultBytes ?? 0n)} bytes`}>{formatDecimalBytes(job.progress?.resultBytes ?? 0n)}</small>
                        </td>
                        <td data-label="Created">
                          <time dateTime={job.createdAt?.toISOString()}>{formatActivityDate(job.createdAt)}</time>
                        </td>
                        <td data-label="Actions">
                          <div className="live-job-actions">
                            <Link
                              className="table-action"
                              href={openSearchHref(job)}
                              aria-label={`Open SPL from search job ${job.id}`}
                              title="Opens this SPL and time range as an unsubmitted Search draft."
                            >
                              Open SPL
                            </Link>
                            <button type="button" disabled={jobAction !== null || refreshing} aria-label={`Rerun search job ${job.id}`} onClick={() => void rerunJob(job)}>
                              {jobAction?.id === job.id && jobAction.kind === "rerun" ? "Starting…" : "Rerun"}
                            </button>
                            {cancelable ? (
                              <button type="button" disabled={jobAction !== null || refreshing} aria-label={`Cancel search job ${job.id}`} onClick={() => void cancelJob(job)}>
                                {jobAction?.id === job.id && jobAction.kind === "cancel" ? "Canceling…" : "Cancel"}
                              </button>
                            ) : null}
                          </div>
                        </td>
                      </tr>
                    );
                  })}</tbody>
                </table>
              </div>
            ) : null}
          </section>

          {!complete ? (
            <output className="backend-list-notice backend-list-notice--action live-jobs-page-notice">
              <span>Showing {loadedCount.toLocaleString()}{totalSize === null ? "" : ` of ${formatActivityCount(totalSize)}`} jobs matching this filter.</span>
              <button type="button" aria-busy={loadingMore} disabled={loadingMore || nextPageToken === null} onClick={() => void loadMore()}>
                {loadingMore ? "Loading more…" : "Load more"}
              </button>
            </output>
          ) : null}
          {loadMoreError === null ? null : (
            <div className="backend-inline-error" role="alert">
              {loadMoreError}
              <button type="button" onClick={nextPageToken === null ? reload : () => void loadMore()}>
                {nextPageToken === null ? "Refresh snapshot" : "Retry"}
              </button>
            </div>
          )}
        </>
      ) : null}
    </div>
  );
}
