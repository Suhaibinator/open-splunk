import type { ResultRow } from "@/gen/ts/open_splunk/result";
import type { TimelinePoint } from "@/lib/demo/search-data";

import { strictRfc3339Nanoseconds } from "./time-range";

/**
 * Upper bound on timechart buckets the visualization walks from the retained
 * result. Requests use the server's maximum page size, but its byte limit may
 * shorten pages. The cursor walk remains row-bounded and cancellable; reaching
 * this limit with more rows available marks the chart as capped.
 */
export const MAXIMUM_CHART_BUCKETS = 10_000;
export const TIMECHART_PROGRESS_BATCH_SIZE = 1_000;

export type TimechartCoverageStatus = "complete" | "loading" | "capped" | "failed";

/** What the plotted timechart actually covers relative to the server result. */
export interface TimechartCoverage {
  status: TimechartCoverageStatus;
  plottedBuckets: number;
  /** Server-reported total bucket count, when the first page carried one. */
  totalBuckets: number | null;
  totalExact: boolean;
}

export interface TimechartPage {
  rows: ResultRow[];
  nextPageToken: string | null;
}

export interface TimechartFirstPage extends TimechartPage {
  totalSize: number | null;
  totalSizeExact: boolean;
}

export interface TimechartBucketLoad {
  /** Complete raw rows when retained; empty in streaming mode. */
  rows: ResultRow[];
  coverage: TimechartCoverage;
  /** Set when the walk stopped on an error; retained rows or previously delivered batches remain valid. */
  error?: unknown;
}

export interface TimechartBucketBatch {
  /** Rows appended since the preceding publication. */
  rows: ResultRow[];
  coverage: TimechartCoverage;
}

export interface LoadTimechartBucketsOptions {
  firstPage: TimechartFirstPage;
  fetchPage: (pageToken: string) => Promise<TimechartPage>;
  maximumBuckets?: number;
  onProgress?: (batch: TimechartBucketBatch) => void;
  progressBatchSize?: number;
  /** Keep the complete raw row collection in the resolved value. */
  retainRows?: boolean;
  signal?: AbortSignal;
}

export interface TimechartRowSort {
  direction: "asc" | "desc";
  key: "count" | "time";
}

function timechartSortCoordinateNanoseconds(point: TimelinePoint): bigint | null {
  if (point.timeCoordinateNanoseconds !== undefined) return point.timeCoordinateNanoseconds;
  const timestamp = point.timeValue ?? point.earliest;
  if (timestamp === undefined) return null;
  const precise = strictRfc3339Nanoseconds(timestamp);
  if (precise !== null) return precise;
  const milliseconds = Date.parse(timestamp);
  return Number.isFinite(milliseconds) ? BigInt(milliseconds) * 1_000_000n : null;
}

/** Sort table buckets by their exact interval start rather than server row order. */
export function sortTimechartRows(
  points: readonly TimelinePoint[],
  sort: TimechartRowSort,
): TimelinePoint[] {
  return points
    .map((point, index) => ({
      index,
      point,
      time: sort.key === "time" ? timechartSortCoordinateNanoseconds(point) : null,
    }))
    .toSorted((left, right) => {
      let comparison: number;
      if (sort.key === "count") {
        comparison = left.point.count - right.point.count;
      } else {
        const leftTime = left.time;
        const rightTime = right.time;
        if (leftTime === null) return rightTime === null ? left.index - right.index : 1;
        if (rightTime === null) return -1;
        comparison = leftTime < rightTime ? -1 : leftTime > rightTime ? 1 : 0;
      }
      if (comparison === 0) return left.index - right.index;
      return sort.direction === "desc" ? -comparison : comparison;
    })
    .map(({ point }) => point);
}

function coverageFor(
  status: TimechartCoverageStatus,
  plottedBuckets: number,
  firstPage: TimechartFirstPage,
): TimechartCoverage {
  return {
    status,
    plottedBuckets,
    totalBuckets: firstPage.totalSize,
    totalExact: firstPage.totalSize !== null && firstPage.totalSizeExact,
  };
}

function abortError(): DOMException {
  return new DOMException("Timechart bucket loading was aborted.", "AbortError");
}

class TimechartPageFailure {
  constructor(readonly cause: unknown) {}
}

/** Sequential cursor iterator whose settled `next` call releases its page before the next fetch. */
function timechartPageStream(
  initialPageToken: string | null,
  fetchPage: (pageToken: string) => Promise<TimechartPage>,
  signal?: AbortSignal,
): AsyncIterable<TimechartPage> {
  let nextPageToken = initialPageToken;
  const seenTokens = new Set<string>();
  return {
    [Symbol.asyncIterator]() {
      return {
        async next(): Promise<IteratorResult<TimechartPage>> {
          if (signal?.aborted) throw abortError();
          if (nextPageToken === null) return { done: true, value: undefined };
          if (seenTokens.has(nextPageToken)) {
            throw new TimechartPageFailure(
              new Error("Search results repeated a page cursor while loading timechart buckets."),
            );
          }
          const requestedToken = nextPageToken;
          seenTokens.add(requestedToken);
          let page: TimechartPage;
          try {
            page = await fetchPage(requestedToken);
          } catch (error) {
            if (signal?.aborted || (error instanceof DOMException && error.name === "AbortError")) throw error;
            throw new TimechartPageFailure(error);
          }
          if (signal?.aborted) throw abortError();
          nextPageToken = page.nextPageToken?.trim() || null;
          return { done: false, value: page };
        },
      };
    },
  };
}

/**
 * Follows the retained result cursor from the first time-series page until the
 * buckets are complete, the cap is reached, or a page fails. Every page belongs
 * to the same stable server-order snapshot, so rows concatenate directly before
 * chart-only chronological ordering. Progress delivers only newly appended rows in
 * bounded batches so callers can adapt each row once and avoid publishing a
 * new copy of every preceding bucket after each cursor response.
 */
export async function loadTimechartBuckets({
  firstPage,
  fetchPage,
  maximumBuckets = MAXIMUM_CHART_BUCKETS,
  onProgress,
  progressBatchSize = TIMECHART_PROGRESS_BATCH_SIZE,
  retainRows = true,
  signal,
}: LoadTimechartBucketsOptions): Promise<TimechartBucketLoad> {
  if (!Number.isSafeInteger(maximumBuckets) || maximumBuckets <= 0) {
    throw new RangeError("Timechart bucket limit must be a positive safe integer.");
  }
  if (!Number.isSafeInteger(progressBatchSize) || progressBatchSize <= 0) {
    throw new RangeError("Timechart progress batch size must be a positive safe integer.");
  }
  const retainedRows = retainRows ? firstPage.rows.slice(0, maximumBuckets) : [];
  let plottedBuckets = Math.min(firstPage.rows.length, maximumBuckets);
  let omittedRows = firstPage.rows.length > maximumBuckets;
  let pendingRows: ResultRow[] = [];
  const publish = (coverage: TimechartCoverage) => {
    const batch = pendingRows;
    pendingRows = [];
    if (onProgress === undefined) return;
    onProgress({ rows: batch, coverage });
  };
  const completed = (status: Exclude<TimechartCoverageStatus, "loading">, error?: unknown): TimechartBucketLoad => {
    const coverage = coverageFor(status, plottedBuckets, firstPage);
    publish(coverage);
    return error === undefined
      ? { rows: retainedRows, coverage }
      : { rows: retainedRows, coverage, error };
  };
  const firstPageToken = firstPage.nextPageToken?.trim() || null;
  if (signal?.aborted) throw abortError();
  if (firstPageToken === null) return completed(omittedRows ? "capped" : "complete");
  if (plottedBuckets >= maximumBuckets) return completed("capped");

  try {
    for await (const page of timechartPageStream(firstPageToken, fetchPage, signal)) {
      const remaining = maximumBuckets - plottedBuckets;
      const appended = page.rows.slice(0, remaining);
      plottedBuckets += appended.length;
      if (retainRows) retainedRows.push(...appended);
      if (onProgress !== undefined) pendingRows.push(...appended);
      omittedRows ||= page.rows.length > remaining;
      const followingToken = page.nextPageToken?.trim() || null;
      if (followingToken !== null && page.rows.length === 0) {
        return completed("failed", new Error("Search results returned an empty page with a further cursor."));
      }
      if (omittedRows || (plottedBuckets >= maximumBuckets && followingToken !== null)) {
        return completed("capped");
      }
      if (followingToken === null) return completed("complete");
      if (pendingRows.length >= progressBatchSize) {
        publish(coverageFor("loading", plottedBuckets, firstPage));
      }
    }
  } catch (error) {
    if (error instanceof TimechartPageFailure) return completed("failed", error.cause);
    throw error;
  }
  return completed("complete");
}

/** Coverage of a time-series result that fit on its first page. */
export function completeTimechartCoverage(plottedBuckets: number): TimechartCoverage {
  return { status: "complete", plottedBuckets, totalBuckets: plottedBuckets, totalExact: true };
}

const BUCKET_NUMBER_FORMAT = new Intl.NumberFormat("en-US");

/** Chart subtitle that states exactly which buckets are plotted. */
export function describeTimechartCoverage(
  coverage: TimechartCoverage,
  plottedThroughLabel: string | null,
): string {
  if (coverage.status === "complete") return "Timechart across the submitted search range.";
  const plotted = BUCKET_NUMBER_FORMAT.format(coverage.plottedBuckets);
  const through = plottedThroughLabel === null ? "" : ` (through ${plottedThroughLabel})`;
  const total = coverage.totalBuckets === null
    ? null
    : `${coverage.totalExact ? "" : "at least "}${BUCKET_NUMBER_FORMAT.format(coverage.totalBuckets)}`;
  const shown = total === null
    ? `Showing the first ${plotted} buckets${through}.`
    : `Showing the first ${plotted} of ${total} buckets${through}.`;
  switch (coverage.status) {
    case "loading": {
      const remaining = coverage.totalBuckets !== null && coverage.totalExact
        ? Math.max(0, coverage.totalBuckets - coverage.plottedBuckets)
        : null;
      return `${shown} ${remaining === null
        ? "Loading the remaining buckets…"
        : `Loading the remaining ${BUCKET_NUMBER_FORMAT.format(remaining)} buckets…`}`;
    }
    case "capped":
      return `${shown} The chart stops at ${BUCKET_NUMBER_FORMAT.format(MAXIMUM_CHART_BUCKETS)} buckets; widen the timechart span to plot the full range.`;
    case "failed":
      return `${shown} The remaining buckets could not be loaded; run the search again to retry.`;
  }
}

/** Notice for the job strip: the table is one server page, the chart may not be. */
export function describeTimechartStatisticsPage(
  pageNumber: number,
  coverage: TimechartCoverage | null,
): string {
  const page = `Statistics show server page ${BUCKET_NUMBER_FORMAT.format(pageNumber)} of the timechart buckets`;
  return coverage?.status === "complete"
    ? `${page}; the visualization plots every bucket.`
    : `${page}; the visualization plots the buckets loaded so far.`;
}
