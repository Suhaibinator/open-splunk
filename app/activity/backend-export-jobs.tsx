"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  ExportFormat,
  ExportJobState,
  type ExportJob,
} from "@/gen/ts/open_splunk/export";
import {
  createOpenSplunkApiClient,
  isAdvertisedFeatureRouteUnavailable,
  isHttpStatus,
  recordNextPageToken,
  RepeatedPageCursorError,
  type SystemBootstrapModel,
} from "@/lib/api";
import { durationToMilliseconds } from "@/lib/api/duration";
import { createErrorMessage } from "@/lib/error-message";
import {
  cancelServerExport,
  downloadServerExport,
  exportCanDownload,
} from "@/lib/search/server-exports";

import { BackendResourceState } from "../_components/backend-resource-state";
import { formatDecimalBytes } from "../search-workspace/formatters";
import {
  formatActivityCount,
  formatActivityDate,
  formatActivityDuration,
  formatActivityTime,
} from "./backend-activity-shared";

type ExportFilter = "all" | "active" | "completed" | "failed" | "canceled" | "expired";
type LoadState = "loading" | "available" | "unavailable" | "error";

const FILTERS: ReadonlyArray<{
  key: ExportFilter;
  label: string;
  states: readonly ExportJobState[];
}> = [
  { key: "all", label: "All", states: [] },
  {
    key: "active",
    label: "Active",
    states: [ExportJobState.EXPORT_JOB_STATE_QUEUED, ExportJobState.EXPORT_JOB_STATE_RUNNING],
  },
  { key: "completed", label: "Completed", states: [ExportJobState.EXPORT_JOB_STATE_COMPLETED] },
  { key: "failed", label: "Failed", states: [ExportJobState.EXPORT_JOB_STATE_FAILED] },
  { key: "canceled", label: "Canceled", states: [ExportJobState.EXPORT_JOB_STATE_CANCELED] },
  { key: "expired", label: "Expired", states: [ExportJobState.EXPORT_JOB_STATE_EXPIRED] },
];

interface BackendExportJobsProps {
  apiBaseUrl: string;
  bootstrap: SystemBootstrapModel;
}

const errorMessage = createErrorMessage("The server did not return a usable export job response.");

function filterStates(filter: ExportFilter): readonly ExportJobState[] {
  return FILTERS.find((candidate) => candidate.key === filter)?.states ?? [];
}

function exportStateLabel(state: ExportJobState): string {
  if (state === ExportJobState.EXPORT_JOB_STATE_QUEUED) return "Queued";
  if (state === ExportJobState.EXPORT_JOB_STATE_RUNNING) return "Running";
  if (state === ExportJobState.EXPORT_JOB_STATE_COMPLETED) return "Completed";
  if (state === ExportJobState.EXPORT_JOB_STATE_FAILED) return "Failed";
  if (state === ExportJobState.EXPORT_JOB_STATE_CANCELED) return "Canceled";
  if (state === ExportJobState.EXPORT_JOB_STATE_EXPIRED) return "Expired";
  return "Unknown";
}

function exportStateClass(state: ExportJobState): string {
  if (state === ExportJobState.EXPORT_JOB_STATE_COMPLETED) return "complete";
  if (state === ExportJobState.EXPORT_JOB_STATE_FAILED || state === ExportJobState.EXPORT_JOB_STATE_EXPIRED) return "failed";
  if (state === ExportJobState.EXPORT_JOB_STATE_CANCELED) return "neutral";
  if (state === ExportJobState.EXPORT_JOB_STATE_QUEUED) return "warning";
  return "running";
}

function exportFormatLabel(job: ExportJob): string {
  if (job.format === ExportFormat.EXPORT_FORMAT_CSV) return "CSV";
  if (job.format === ExportFormat.EXPORT_FORMAT_JSON_LINES) return "JSON Lines";
  return "Unknown format";
}

function progressPercent(job: ExportJob): number | null {
  if (job.state === ExportJobState.EXPORT_JOB_STATE_COMPLETED) return 100;
  const value = job.progress?.percentComplete;
  return value !== undefined && Number.isFinite(value) ? Math.max(0, Math.min(100, value)) : null;
}

function artifactIsUnexpired(job: ExportJob, now = Date.now()): boolean {
  if (!exportCanDownload(job)) return false;
  const expiration = job.artifact?.expiresAt ?? job.expiresAt;
  return expiration === undefined || expiration.valueOf() > now;
}

function downloadBlob(blob: Blob, fileName: string): void {
  const objectUrl = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = objectUrl;
  anchor.download = fileName;
  anchor.rel = "noopener";
  document.body.append(anchor);
  anchor.click();
  anchor.remove();
  window.setTimeout(() => URL.revokeObjectURL(objectUrl), 0);
}

export function BackendExportJobs({ apiBaseUrl, bootstrap }: BackendExportJobsProps) {
  const client = useMemo(() => createOpenSplunkApiClient({ baseUrl: apiBaseUrl }), [apiBaseUrl]);
  const [state, setState] = useState<LoadState>("loading");
  const [jobs, setJobs] = useState<ExportJob[]>([]);
  const [filter, setFilter] = useState<ExportFilter>("all");
  const [searchJobIdInput, setSearchJobIdInput] = useState("");
  const [searchJobIdFilter, setSearchJobIdFilter] = useState("");
  const [totalSize, setTotalSize] = useState<bigint | null>(null);
  const [totalSizeExact, setTotalSizeExact] = useState(false);
  const [nextPageToken, setNextPageToken] = useState<string | null>(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [loadMoreError, setLoadMoreError] = useState<string | null>(null);
  const [refreshedAt, setRefreshedAt] = useState<Date | null>(null);
  const [generation, setGeneration] = useState(0);
  const [action, setAction] = useState<{ id: string; kind: "cancel" | "download" } | null>(null);
  const [notice, setNotice] = useState<{ kind: "success" | "error"; message: string } | null>(null);
  const hasLoadedRef = useRef(false);
  const pageTokensSeenRef = useRef<Set<string>>(new Set());
  const jobIdsSeenRef = useRef<Set<string>>(new Set());
  const loadMoreAbortRef = useRef<AbortController | null>(null);
  const reload = useCallback(() => setGeneration((current) => current + 1), []);
  const pageSize = Math.max(1, Math.min(bootstrap.limits.maximumPageSize || 20, 20));

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
    setNextPageToken(null);
    setTotalSize(null);
    setTotalSizeExact(false);
    pageTokensSeenRef.current.clear();
    jobIdsSeenRef.current.clear();

    void client.exports.list({
      page: { pageSize, pageToken: undefined, includeTotalSize: true },
      stateFilters: [...filterStates(filter)],
      searchJobIdFilter: searchJobIdFilter || undefined,
    }, { signal: controller.signal }).then((response) => {
      if (!current) return;
      for (const job of response.exportJobs) jobIdsSeenRef.current.add(job.exportJobId);
      setJobs(response.exportJobs);
      setTotalSize(response.page?.totalSize ?? null);
      setTotalSizeExact(response.page?.totalSizeExact ?? false);
      setNextPageToken(recordNextPageToken(pageTokensSeenRef.current, response.page?.nextPageToken, "Export jobs"));
      setRefreshedAt(new Date());
      setState("available");
      setRefreshing(false);
      hasLoadedRef.current = true;
    }).catch((reason: unknown) => {
      if (!current || controller.signal.aborted) return;
      if (isAdvertisedFeatureRouteUnavailable(reason)) {
        setState("unavailable");
        hasLoadedRef.current = false;
      } else {
        setError(errorMessage(reason));
        setState(retainShell ? "available" : "error");
      }
      setRefreshing(false);
    });

    return () => {
      current = false;
      controller.abort();
      loadMoreAbortRef.current?.abort();
      loadMoreAbortRef.current = null;
    };
  }, [client, filter, generation, pageSize, searchJobIdFilter]);

  const loadMore = useCallback(async () => {
    if (nextPageToken === null || loadMoreAbortRef.current !== null) return;
    const controller = new AbortController();
    const pageToken = nextPageToken;
    loadMoreAbortRef.current = controller;
    setLoadingMore(true);
    setLoadMoreError(null);
    try {
      const response = await client.exports.list({
        page: { pageSize, pageToken, includeTotalSize: true },
        stateFilters: [...filterStates(filter)],
        searchJobIdFilter: searchJobIdFilter || undefined,
      }, { signal: controller.signal });
      if (controller.signal.aborted) return;
      const duplicate = response.exportJobs.find((job) => jobIdsSeenRef.current.has(job.exportJobId));
      if (duplicate !== undefined) throw new TypeError(`Export jobs repeated ${duplicate.exportJobId} on a later page.`);
      for (const job of response.exportJobs) jobIdsSeenRef.current.add(job.exportJobId);
      setJobs((current) => [...current, ...response.exportJobs]);
      setTotalSize(response.page?.totalSize ?? null);
      setTotalSizeExact(response.page?.totalSizeExact ?? false);
      setNextPageToken(recordNextPageToken(pageTokensSeenRef.current, response.page?.nextPageToken, "Export jobs"));
    } catch (reason) {
      if (!controller.signal.aborted) {
        if (reason instanceof RepeatedPageCursorError || reason instanceof TypeError || isHttpStatus(reason, 400)) {
          setNextPageToken(null);
          setLoadMoreError(`${errorMessage(reason)} Further paging was stopped; refresh to request a new cursor.`);
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
  }, [client, filter, nextPageToken, pageSize, searchJobIdFilter]);

  const cancelJob = useCallback(async (job: ExportJob) => {
    if (action !== null) return;
    setAction({ id: job.exportJobId, kind: "cancel" });
    setNotice(null);
    try {
      const result = await cancelServerExport(client, bootstrap, job.exportJobId);
      if (result.status === "unavailable") {
        setNotice({ kind: "error", message: "Export cancellation is unavailable on this backend." });
      } else {
        setJobs((current) => current.map((candidate) => candidate.exportJobId === result.value.exportJobId ? result.value : candidate));
        setNotice({ kind: "success", message: `Cancellation accepted for ${job.exportJobId}.` });
      }
    } catch (reason) {
      setNotice({ kind: "error", message: errorMessage(reason) });
    } finally {
      setAction(null);
    }
  }, [action, bootstrap, client]);

  const downloadJob = useCallback(async (job: ExportJob) => {
    if (action !== null) return;
    setAction({ id: job.exportJobId, kind: "download" });
    setNotice(null);
    try {
      const result = await downloadServerExport(client, bootstrap, job.exportJobId, { apiBaseUrl });
      if (result.status === "unavailable") {
        setNotice({ kind: "error", message: "Export download is unavailable on this backend." });
      } else {
        downloadBlob(result.value.blob, result.value.fileName);
        setNotice({ kind: "success", message: `Downloaded ${result.value.fileName}.` });
      }
    } catch (reason) {
      setNotice({ kind: "error", message: errorMessage(reason) });
    } finally {
      setAction(null);
    }
  }, [action, apiBaseUrl, bootstrap, client]);

  const clearFilters = useCallback(() => {
    setFilter("all");
    setSearchJobIdInput("");
    setSearchJobIdFilter("");
  }, []);
  const filtered = filter !== "all" || searchJobIdFilter.length > 0;

  return (
    <div className="backend-activity-view">
      {state === "loading" ? <BackendResourceState kind="loading" title="Loading retained exports" message="Reading the backend’s retained export-job snapshot…" /> : null}
      {state === "unavailable" ? <BackendResourceState kind="unavailable" title="Export listing is unavailable" message="This backend advertises export formats but does not register the optional export-list route." action={<button type="button" onClick={reload}>Retry</button>} /> : null}
      {state === "error" ? <BackendResourceState kind="error" title="Export jobs could not be loaded" message={error ?? "The export job request failed."} action={<button type="button" onClick={reload}>Retry</button>} /> : null}

      {state === "available" ? (
        <>
          <output className="live-jobs-snapshot history-snapshot">
            <span><i aria-hidden="true" />{totalSizeExact && totalSize !== null ? <><strong>{formatActivityCount(totalSize)}</strong> exact matches; </> : null}last refreshed <strong>{formatActivityTime(refreshedAt)}</strong>. Lifecycle state and expiration can change between pages.</span>
            <button type="button" aria-busy={refreshing} aria-disabled={refreshing} onClick={() => { if (!refreshing) reload(); }}>{refreshing ? "Refreshing…" : "Refresh snapshot"}</button>
          </output>
          {notice === null ? null : <output className={`backend-action-notice backend-action-notice--${notice.kind}`}><span aria-hidden="true">{notice.kind === "success" ? "✓" : "!"}</span><strong>{notice.message}</strong><button type="button" aria-label="Dismiss export action notification" onClick={() => setNotice(null)}>×</button></output>}
          {error !== null && jobs.length > 0 ? <div className="backend-inline-error" role="alert">The latest refresh failed; the prior snapshot remains visible. {error}<button type="button" onClick={reload}>Retry</button></div> : null}

          <section className="suite-card live-jobs-card">
            <header className="live-jobs-toolbar">
              <fieldset className="activity-filter-group">
                <legend className="sr-only">Export job state filter</legend>
                {FILTERS.map((candidate) => <button className={`activity-filter-button${filter === candidate.key ? " active" : ""}`} aria-pressed={filter === candidate.key} type="button" onClick={() => setFilter(candidate.key)} key={candidate.key}>{candidate.label}</button>)}
              </fieldset>
              <form className="live-jobs-filter-row" onSubmit={(event) => { event.preventDefault(); setSearchJobIdFilter(searchJobIdInput.trim()); }}>
                <label className="live-jobs-text-filter"><span className="sr-only">Exact search job ID filter</span><i aria-hidden="true">⌕</i><input value={searchJobIdInput} maxLength={256} onChange={(event) => setSearchJobIdInput(event.target.value)} placeholder="Exact search job ID" /></label>
                <button className="suite-button" type="submit">Apply</button>
                {filtered ? <button className="suite-button suite-button--secondary" type="button" onClick={clearFilters}>Clear</button> : null}
              </form>
            </header>
            {refreshing ? <BackendResourceState kind="loading" title="Updating export jobs" message="Applying exact backend filters to a fresh snapshot. Existing rows remain visible until it completes." /> : null}
            {jobs.length === 0 && !refreshing ? <BackendResourceState kind="empty" title={filtered ? "No matching export jobs" : "No retained export jobs"} message={filtered ? "No retained export jobs match these exact state and search-job filters." : "Exports created from completed searches will appear here while their records are retained."} action={filtered ? <button type="button" onClick={clearFilters}>Clear filters</button> : undefined} /> : null}
            {jobs.length > 0 ? (
              <div className="responsive-table-wrap" aria-busy={refreshing}>
                <table className="product-table live-jobs-table export-jobs-table">
                  <caption className="sr-only">Retained export jobs</caption>
                  <thead><tr><th scope="col">Export</th><th scope="col">Source search</th><th scope="col">State</th><th scope="col">Progress</th><th scope="col">Artifact / failure</th><th scope="col">Created</th><th scope="col"><span className="sr-only">Actions</span></th></tr></thead>
                  <tbody>{jobs.map((job) => {
                    const percent = progressPercent(job);
                    const cancelable = job.state === ExportJobState.EXPORT_JOB_STATE_QUEUED || job.state === ExportJobState.EXPORT_JOB_STATE_RUNNING;
                    const downloadable = artifactIsUnexpired(job);
                    return <tr key={job.exportJobId}>
                      <td data-label="Export"><strong>{exportFormatLabel(job)}</strong><code>{job.exportJobId}</code><small>Version {formatActivityCount(job.stateVersion)}</small></td>
                      <td data-label="Source search"><code>{job.definition?.searchJobId || "Not recorded"}</code><small>{job.definition?.columns.length ? `${job.definition.columns.length} selected columns` : "All result columns"}</small></td>
                      <td data-label="State"><span className={`status-label status-label--${exportStateClass(job.state)}`}><i />{exportStateLabel(job.state)}</span>{job.finishedAt !== undefined ? <small>Finished {formatActivityDate(job.finishedAt)}</small> : null}</td>
                      <td data-label="Progress"><div className="live-job-progress">{percent === null ? <span>{exportStateLabel(job.state)}</span> : <progress max={100} value={percent} aria-label={`${exportStateLabel(job.state)} ${Math.round(percent)} percent`} />}<small>{percent === null ? null : `${Math.round(percent)}% · `}{formatActivityDuration(durationToMilliseconds(job.progress?.elapsed))}</small><small>{formatActivityCount(job.progress?.rowsWritten ?? 0n)} rows · {formatDecimalBytes(job.progress?.bytesWritten ?? 0n)}</small></div></td>
                      <td data-label="Artifact / failure">{job.artifact !== undefined ? <><strong title={`${formatActivityCount(job.artifact.sizeBytes)} bytes`}>{job.artifact.fileName}</strong><small>{formatActivityCount(job.artifact.rowCount)} rows · {formatDecimalBytes(job.artifact.sizeBytes)}</small><small>Expires {formatActivityDate(job.artifact.expiresAt ?? null)}</small></> : job.failure !== undefined ? <><strong className="table-error-detail">{job.failure.message}</strong><small>{job.failure.retryable ? "Retryable failure" : "Terminal failure"} · code {job.failure.code}</small></> : <small>No artifact yet</small>}</td>
                      <td data-label="Created"><time dateTime={job.createdAt?.toISOString()}>{formatActivityDate(job.createdAt ?? null)}</time></td>
                      <td data-label="Actions"><div className="live-job-actions">{downloadable ? <button type="button" aria-label={`Download export ${job.exportJobId}`} disabled={action !== null || refreshing} onClick={() => void downloadJob(job)}>{action?.id === job.exportJobId && action.kind === "download" ? "Downloading…" : "Download"}</button> : null}{cancelable ? <button type="button" aria-label={`Cancel export ${job.exportJobId}`} disabled={action !== null || refreshing} onClick={() => void cancelJob(job)}>{action?.id === job.exportJobId && action.kind === "cancel" ? "Canceling…" : "Cancel"}</button> : null}</div></td>
                    </tr>;
                  })}</tbody>
                </table>
              </div>
            ) : null}
          </section>
          {nextPageToken !== null ? <output className="backend-list-notice backend-list-notice--action live-jobs-page-notice"><span>Showing {jobs.length.toLocaleString()}{totalSizeExact && totalSize !== null ? ` of ${formatActivityCount(totalSize)}` : ""} matching exports.</span><button type="button" aria-busy={loadingMore} disabled={loadingMore} onClick={() => void loadMore()}>{loadingMore ? "Loading more…" : "Load more"}</button></output> : null}
          {loadMoreError === null ? null : <div className="backend-inline-error" role="alert">{loadMoreError}<button type="button" onClick={nextPageToken === null ? reload : () => void loadMore()}>{nextPageToken === null ? "Refresh snapshot" : "Retry"}</button></div>}
        </>
      ) : null}
    </div>
  );
}
