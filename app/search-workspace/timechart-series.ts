import type { ResultRow } from "@/gen/ts/open_splunk/result";

/**
 * Upper bound on timechart buckets the visualization walks from the retained
 * result. Server pages are capped at 1,000 rows, so this is at most ten
 * sequential cursor follows; beyond it the chart is marked as truncated.
 */
export const MAXIMUM_CHART_BUCKETS = 10_000;

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
  rows: ResultRow[];
  coverage: TimechartCoverage;
  /** Set when the walk stopped on an error; the rows collected before it remain valid. */
  error?: unknown;
}

export interface LoadTimechartBucketsOptions {
  firstPage: TimechartFirstPage;
  fetchPage: (pageToken: string) => Promise<TimechartPage>;
  maximumBuckets?: number;
  onProgress?: (load: TimechartBucketLoad) => void;
  signal?: AbortSignal;
}

function coverageFor(
  status: TimechartCoverageStatus,
  rows: ResultRow[],
  firstPage: TimechartFirstPage,
): TimechartCoverage {
  return {
    status,
    plottedBuckets: rows.length,
    totalBuckets: firstPage.totalSize,
    totalExact: firstPage.totalSize !== null && firstPage.totalSizeExact,
  };
}

function abortError(): DOMException {
  return new DOMException("Timechart bucket loading was aborted.", "AbortError");
}

/**
 * Follows the retained result cursor from the first time-series page until the
 * buckets are complete, the cap is reached, or a page fails. Every page of a
 * time-series result is one chronological slice of the same snapshot, so the
 * rows concatenate directly; the caller adapts them in one pass so bucket
 * widths are inferred across page edges rather than reset at each one.
 */
export async function loadTimechartBuckets({
  firstPage,
  fetchPage,
  maximumBuckets = MAXIMUM_CHART_BUCKETS,
  onProgress,
  signal,
}: LoadTimechartBucketsOptions): Promise<TimechartBucketLoad> {
  if (!Number.isSafeInteger(maximumBuckets) || maximumBuckets <= 0) {
    throw new RangeError("Timechart bucket limit must be a positive safe integer.");
  }
  const rows = [...firstPage.rows];
  const seenTokens = new Set<string>();
  const failed = (message: string): TimechartBucketLoad => ({
    rows,
    coverage: coverageFor("failed", rows, firstPage),
    error: new Error(message),
  });
  const collectPage = async (nextPageToken: string | null): Promise<TimechartBucketLoad> => {
    if (signal?.aborted) throw abortError();
    if (nextPageToken === null) return { rows, coverage: coverageFor("complete", rows, firstPage) };
    if (rows.length >= maximumBuckets) return { rows, coverage: coverageFor("capped", rows, firstPage) };
    if (seenTokens.has(nextPageToken)) {
      return failed("Search results repeated a page cursor while loading timechart buckets.");
    }
    seenTokens.add(nextPageToken);
    onProgress?.({ rows: [...rows], coverage: coverageFor("loading", rows, firstPage) });
    let page: TimechartPage;
    try {
      page = await fetchPage(nextPageToken);
    } catch (error) {
      if (signal?.aborted || (error instanceof DOMException && error.name === "AbortError")) throw error;
      return { rows, coverage: coverageFor("failed", rows, firstPage), error };
    }
    if (signal?.aborted) throw abortError();
    rows.push(...page.rows);
    const followingToken = page.nextPageToken?.trim() || null;
    if (followingToken !== null && page.rows.length === 0) {
      return failed("Search results returned an empty page with a further cursor.");
    }
    return collectPage(followingToken);
  };
  return collectPage(firstPage.nextPageToken?.trim() || null);
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
