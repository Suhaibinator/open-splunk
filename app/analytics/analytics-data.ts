import { SortDirection } from "@/gen/ts/open_splunk/common";
import type { SearchHistoryEntry } from "@/gen/ts/open_splunk/history";
import { SearchHistorySortBy } from "@/gen/ts/open_splunk/history_api";
import type { FieldProfile as ServerFieldProfile } from "@/gen/ts/open_splunk/result";
import { SearchJobState } from "@/gen/ts/open_splunk/search";
import { valueTypeToJSON } from "@/gen/ts/open_splunk/value";
import type { OpenSplunkApiClient } from "@/lib/api/open-splunk-client";
import { recordNextPageToken } from "@/lib/api/pagination";
import type { ProtobufRequestOptions } from "@/lib/api/protobuf-transport";

export const ANALYTICS_HISTORY_PAGE_LIMIT = 8;
export const ANALYTICS_FIELD_PAGE_LIMIT = 4;
export const ANALYTICS_PAGE_SIZE_LIMIT = 100;
export const ANALYTICS_TREND_BUCKETS = 12;

const TERMINAL_STATES = new Set<SearchJobState>([
  SearchJobState.SEARCH_JOB_STATE_COMPLETED,
  SearchJobState.SEARCH_JOB_STATE_FAILED,
  SearchJobState.SEARCH_JOB_STATE_CANCELED,
  SearchJobState.SEARCH_JOB_STATE_EXPIRED,
]);

export interface AnalyticsHistoryRecord {
  id: string;
  spl: string;
  earliest: string | null;
  latest: string | null;
  timezone: string | null;
  appId: string | null;
  effectiveIndexScope: string[];
  finalState: SearchJobState;
  matchedEvents: bigint;
  scannedRows: bigint;
  scannedBytes: bigint;
  producedRows: bigint;
  durationMs: number | null;
  failureMessage: string | null;
  createdAt: Date;
  finishedAt: Date | null;
}

export interface AnalyticsHistorySnapshot {
  entries: AnalyticsHistoryRecord[];
  nextPageToken: string | null;
  totalSize: bigint | null;
  totalSizeExact: boolean;
  complete: boolean;
}

export interface AnalyticsFieldProfile {
  name: string;
  displayName: string;
  type: string;
  coverage: number;
  presentEvents: bigint;
  nullEvents: bigint;
  missingEvents: bigint;
  cardinality: bigint | null;
  cardinalityApproximate: boolean;
}

export interface AnalyticsFieldSnapshot {
  fields: AnalyticsFieldProfile[];
  sampledEvents: bigint;
  nextPageToken: string | null;
  totalSize: bigint | null;
  totalSizeExact: boolean;
  complete: boolean;
}

export interface AnalyticsTrendBucket {
  start: Date;
  end: Date;
  searchCount: number;
  p95RuntimeMs: number | null;
}

export interface AnalyticsWorkload {
  searchCount: number;
  completedCount: number;
  failedCount: number;
  canceledCount: number;
  expiredCount: number;
  scannedRows: bigint;
  scannedBytes: bigint;
  matchedEvents: bigint;
  producedRows: bigint;
  medianRuntimeMs: number | null;
  p95RuntimeMs: number | null;
  trend: AnalyticsTrendBucket[];
  slowest: AnalyticsHistoryRecord[];
  failures: AnalyticsHistoryRecord[];
}

interface HistoryClient {
  history: Pick<OpenSplunkApiClient["history"], "list">;
}

interface IndexFieldClient {
  indexes: Pick<OpenSplunkApiClient["indexes"], "fields">;
}

function validTimestamp(value: Date | undefined, label: string): Date {
  if (value === undefined || Number.isNaN(value.valueOf())) {
    throw new TypeError(`The analytics history entry did not include a valid ${label}.`);
  }
  return new Date(value);
}

function optionalTimestamp(value: Date | undefined, label: string): Date | null {
  if (value === undefined) return null;
  if (Number.isNaN(value.valueOf())) {
    throw new TypeError(`The analytics history entry included an invalid ${label}.`);
  }
  return new Date(value);
}

function nonnegativeCounter(value: bigint, label: string): bigint {
  if (value < 0n) throw new TypeError(`The analytics history entry included a negative ${label}.`);
  return value;
}

function optionalDurationMilliseconds(
  duration: { seconds: bigint; nanos: number } | undefined,
): number | null {
  if (duration === undefined) return null;
  if (duration.seconds < 0n || !Number.isInteger(duration.nanos) || duration.nanos < 0 || duration.nanos >= 1_000_000_000) {
    throw new TypeError("The analytics history entry included an invalid duration.");
  }
  const milliseconds = Number(duration.seconds) * 1_000 + duration.nanos / 1_000_000;
  if (!Number.isFinite(milliseconds) || milliseconds < 0) {
    throw new TypeError("The analytics history entry included an unsupported duration.");
  }
  return milliseconds;
}

function normalizedScope(values: readonly string[]): string[] {
  const normalized = values.map((value) => value.trim());
  if (normalized.some((value) => value.length === 0) || new Set(normalized).size !== normalized.length) {
    throw new TypeError("The analytics history entry included an invalid effective index scope.");
  }
  return normalized;
}

export function adaptAnalyticsHistoryEntry(entry: SearchHistoryEntry): AnalyticsHistoryRecord {
  const id = entry.searchJobId.trim();
  if (id.length === 0) throw new TypeError("The analytics history entry did not include a search job ID.");
  const definition = entry.definition;
  const spl = definition?.spl ?? "";
  if (spl.trim().length === 0) throw new TypeError(`Search history entry ${id} did not include SPL.`);
  if (!TERMINAL_STATES.has(entry.finalState)) {
    throw new TypeError(`Search history entry ${id} did not include a supported terminal state.`);
  }
  return {
    id,
    spl,
    earliest: definition?.timeRange?.earliest?.trim() || null,
    latest: definition?.timeRange?.latest?.trim() || null,
    timezone: definition?.timeRange?.timezone?.trim() || null,
    appId: definition?.appId?.trim() || null,
    effectiveIndexScope: normalizedScope(entry.effectiveIndexScope),
    finalState: entry.finalState,
    matchedEvents: nonnegativeCounter(entry.matchedEvents, "matched-event count"),
    scannedRows: nonnegativeCounter(entry.scannedRows, "scanned-row count"),
    scannedBytes: nonnegativeCounter(entry.scannedBytes, "scanned-byte count"),
    producedRows: nonnegativeCounter(entry.producedRows, "produced-row count"),
    durationMs: optionalDurationMilliseconds(entry.duration),
    failureMessage: entry.failure?.message.trim() ? entry.failure.message : null,
    createdAt: validTimestamp(entry.createdAt, "creation time"),
    finishedAt: optionalTimestamp(entry.finishedAt, "finish time"),
  };
}

function boundedPageSize(maximumPageSize: number): number {
  const advertised = Number.isSafeInteger(maximumPageSize) && maximumPageSize > 0
    ? maximumPageSize
    : ANALYTICS_PAGE_SIZE_LIMIT;
  return Math.max(1, Math.min(advertised, ANALYTICS_PAGE_SIZE_LIMIT));
}

export async function loadAnalyticsHistory(
  client: HistoryClient,
  options: ProtobufRequestOptions & {
    maximumPageSize: number;
    createdAfter: Date;
    createdBefore: Date;
    appId?: string;
    maximumPages?: number;
  },
): Promise<AnalyticsHistorySnapshot> {
  if (
    Number.isNaN(options.createdAfter.valueOf())
    || Number.isNaN(options.createdBefore.valueOf())
    || options.createdAfter.valueOf() >= options.createdBefore.valueOf()
  ) {
    throw new RangeError("Analytics history requires a valid, increasing time range.");
  }
  const maximumPages = options.maximumPages ?? ANALYTICS_HISTORY_PAGE_LIMIT;
  if (!Number.isSafeInteger(maximumPages) || maximumPages <= 0 || maximumPages > ANALYTICS_HISTORY_PAGE_LIMIT) {
    throw new RangeError(`Analytics history can load between 1 and ${ANALYTICS_HISTORY_PAGE_LIMIT} pages.`);
  }
  const pageSize = boundedPageSize(options.maximumPageSize);
  const entries: AnalyticsHistoryRecord[] = [];
  const seenIds = new Set<string>();
  const seenTokens = new Set<string>();
  let pageToken: string | undefined;
  let totalSize: bigint | null = null;
  let totalSizeExact = false;

  for (let pageIndex = 0; pageIndex < maximumPages; pageIndex += 1) {
    // Cursor pages are intentionally sequential because each opaque token is returned by its predecessor.
    // eslint-disable-next-line no-await-in-loop
    const response = await client.history.list({
      page: { pageSize, pageToken, includeTotalSize: pageIndex === 0 },
      filter: {
        appId: options.appId?.trim() || undefined,
        stateFilters: [],
        text: undefined,
        savedSearchId: undefined,
        createdAfter: options.createdAfter,
        createdBefore: options.createdBefore,
      },
      sortBy: SearchHistorySortBy.SEARCH_HISTORY_SORT_BY_CREATED_AT,
      sortDirection: SortDirection.SORT_DIRECTION_DESCENDING,
    }, options);
    if (response.page === undefined) throw new TypeError("Search history omitted pagination metadata.");
    if (response.historyEntries.length > pageSize) {
      throw new TypeError("Search history exceeded the requested analytics page size.");
    }
    if (pageIndex === 0) {
      totalSize = response.page.totalSize ?? null;
      totalSizeExact = response.page.totalSizeExact;
      if (!totalSizeExact || totalSize === null || totalSize < 0n) {
        throw new TypeError("Search history did not return the requested exact total size.");
      }
    }
    for (const rawEntry of response.historyEntries) {
      const entry = adaptAnalyticsHistoryEntry(rawEntry);
      if (seenIds.has(entry.id)) throw new TypeError(`Search history repeated entry ${entry.id}.`);
      if (
        entry.createdAt.valueOf() < options.createdAfter.valueOf()
        || entry.createdAt.valueOf() >= options.createdBefore.valueOf()
      ) {
        throw new TypeError(`Search history entry ${entry.id} fell outside the requested analytics range.`);
      }
      const previous = entries.at(-1);
      if (
        previous !== undefined
        && (
          previous.createdAt.valueOf() < entry.createdAt.valueOf()
          || (
            previous.createdAt.valueOf() === entry.createdAt.valueOf()
            && previous.id <= entry.id
          )
        )
      ) {
        throw new TypeError("Search history was not ordered newest first.");
      }
      seenIds.add(entry.id);
      entries.push(entry);
    }
    const next = recordNextPageToken(seenTokens, response.page.nextPageToken, "Analytics search history");
    if (next === null) {
      if (totalSize !== BigInt(entries.length)) {
        throw new TypeError("Complete search history did not match its exact total size.");
      }
      return { entries, nextPageToken: null, totalSize, totalSizeExact, complete: true };
    }
    pageToken = next;
  }

  return { entries, nextPageToken: pageToken ?? null, totalSize, totalSizeExact, complete: false };
}

function fieldTypeLabel(field: ServerFieldProfile): string {
  const raw = valueTypeToJSON(field.valueType)
    .replace("VALUE_TYPE_", "")
    .replaceAll("_", " ")
    .toLowerCase();
  return raw === "unspecified" ? "unknown" : raw;
}

function compareUtf8(left: string, right: string): number {
  const encoder = new TextEncoder();
  const leftBytes = encoder.encode(left);
  const rightBytes = encoder.encode(right);
  const length = Math.min(leftBytes.length, rightBytes.length);
  for (let index = 0; index < length; index += 1) {
    if (leftBytes[index] !== rightBytes[index]) return leftBytes[index] - rightBytes[index];
  }
  return leftBytes.length - rightBytes.length;
}

export function adaptAnalyticsFields(fields: readonly ServerFieldProfile[]): {
  fields: AnalyticsFieldProfile[];
  sampledEvents: bigint;
} {
  const names = new Set<string>();
  let sampledEvents: bigint | null = null;
  let previousName: string | null = null;
  const adapted = fields.map((field) => {
    const name = field.fieldName;
    if (name.length === 0 || names.has(name)) throw new TypeError("The field catalog included an unnamed or repeated field.");
    if (previousName !== null && compareUtf8(previousName, name) >= 0) {
      throw new TypeError("The field catalog included fields outside bytewise name order.");
    }
    names.add(name);
    previousName = name;
    if (field.eventCount < 0n || field.nullCount < 0n || field.missingCount < 0n || field.nullCount > field.eventCount) {
      throw new TypeError(`Field ${name} included invalid presence counters.`);
    }
    const fieldSampleSize = field.eventCount + field.missingCount;
    if (sampledEvents === null) sampledEvents = fieldSampleSize;
    if (fieldSampleSize !== sampledEvents) {
      throw new TypeError("The field catalog mixed profiles from different event samples.");
    }
    if (field.distinctCount !== undefined && field.distinctCount > field.eventCount - field.nullCount) {
      throw new TypeError(`Field ${name} included an invalid distinct count.`);
    }
    const coverage = fieldSampleSize === 0n
      ? 0
      : Number(field.eventCount) / Number(fieldSampleSize) * 100;
    return {
      name,
      displayName: field.displayName || name,
      type: fieldTypeLabel(field),
      coverage,
      presentEvents: field.eventCount,
      nullEvents: field.nullCount,
      missingEvents: field.missingCount,
      cardinality: field.distinctCount ?? null,
      cardinalityApproximate: field.distinctCountIsApproximate,
    };
  });
  return { fields: adapted, sampledEvents: sampledEvents ?? 0n };
}

export async function loadAnalyticsFields(
  client: IndexFieldClient,
  options: ProtobufRequestOptions & {
    maximumPageSize: number;
    indexName: string;
    earliest: string;
    latest: string;
    maximumPages?: number;
  },
): Promise<AnalyticsFieldSnapshot> {
  const indexName = options.indexName.trim();
  const earliest = options.earliest.trim();
  const latest = options.latest.trim();
  if (!indexName || !earliest || !latest) throw new TypeError("Index and bounded time range are required for field analytics.");
  const maximumPages = options.maximumPages ?? ANALYTICS_FIELD_PAGE_LIMIT;
  if (!Number.isSafeInteger(maximumPages) || maximumPages <= 0 || maximumPages > ANALYTICS_FIELD_PAGE_LIMIT) {
    throw new RangeError(`Analytics fields can load between 1 and ${ANALYTICS_FIELD_PAGE_LIMIT} pages.`);
  }
  const pageSize = boundedPageSize(options.maximumPageSize);
  const rawFields: ServerFieldProfile[] = [];
  const seenTokens = new Set<string>();
  let pageToken: string | undefined;
  let totalSize: bigint | null = null;
  let totalSizeExact = false;

  for (let pageIndex = 0; pageIndex < maximumPages; pageIndex += 1) {
    // Cursor pages are intentionally sequential because each opaque token is returned by its predecessor.
    // eslint-disable-next-line no-await-in-loop
    const response = await client.indexes.fields({
      selector: { selector: { $case: "indexName", value: indexName } },
      timeRange: { earliest, latest },
      page: { pageSize, pageToken, includeTotalSize: pageIndex === 0 },
      nameFilter: undefined,
    }, options);
    if (response.page === undefined) throw new TypeError("The index field catalog omitted pagination metadata.");
    if (response.fields.length > pageSize) throw new TypeError("The index field catalog exceeded the requested page size.");
    if (pageIndex === 0) {
      totalSize = response.page.totalSize ?? null;
      totalSizeExact = response.page.totalSizeExact;
      if (!totalSizeExact || totalSize === null || totalSize < 0n) {
        throw new TypeError("The index field catalog did not return the requested exact total size.");
      }
    }
    rawFields.push(...response.fields);
    const next = recordNextPageToken(seenTokens, response.page.nextPageToken, "Analytics field catalog");
    if (next === null) {
      const adapted = adaptAnalyticsFields(rawFields);
      if (totalSize !== BigInt(adapted.fields.length)) {
        throw new TypeError("The complete index field catalog did not match its exact total size.");
      }
      return { ...adapted, nextPageToken: null, totalSize, totalSizeExact, complete: true };
    }
    pageToken = next;
  }

  const adapted = adaptAnalyticsFields(rawFields);
  return { ...adapted, nextPageToken: pageToken ?? null, totalSize, totalSizeExact, complete: false };
}

function nearestRank(values: readonly number[], percentile: number): number | null {
  if (values.length === 0) return null;
  const sorted = [...values].toSorted((left, right) => left - right);
  const index = Math.max(0, Math.ceil(percentile * sorted.length) - 1);
  return sorted[index];
}

function median(values: readonly number[]): number | null {
  if (values.length === 0) return null;
  const sorted = [...values].toSorted((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[middle - 1] + sorted[middle]) / 2 : sorted[middle];
}

function sumCounter(entries: readonly AnalyticsHistoryRecord[], select: (entry: AnalyticsHistoryRecord) => bigint): bigint {
  return entries.reduce((sum, entry) => sum + select(entry), 0n);
}

export function deriveAnalyticsWorkload(
  entries: readonly AnalyticsHistoryRecord[],
  range: { start: Date; end: Date; bucketCount?: number },
): AnalyticsWorkload {
  const startMs = range.start.valueOf();
  const endMs = range.end.valueOf();
  const bucketCount = range.bucketCount ?? ANALYTICS_TREND_BUCKETS;
  if (
    Number.isNaN(startMs)
    || Number.isNaN(endMs)
    || startMs >= endMs
    || !Number.isSafeInteger(bucketCount)
    || bucketCount <= 0
    || bucketCount > 100
  ) {
    throw new RangeError("Analytics workload requires a valid time range and bucket count.");
  }
  const filtered = entries.filter((entry) => entry.createdAt.valueOf() >= startMs && entry.createdAt.valueOf() < endMs);
  const durationValues = filtered.flatMap((entry) => entry.durationMs === null ? [] : [entry.durationMs]);
  const bucketWidth = (endMs - startMs) / bucketCount;
  const bucketDurations = Array.from({ length: bucketCount }, () => [] as number[]);
  const bucketCounts = Array.from({ length: bucketCount }, () => 0);
  for (const entry of filtered) {
    const bucketIndex = Math.min(bucketCount - 1, Math.floor((entry.createdAt.valueOf() - startMs) / bucketWidth));
    bucketCounts[bucketIndex] += 1;
    if (entry.durationMs !== null) bucketDurations[bucketIndex].push(entry.durationMs);
  }
  const trend = bucketDurations.map((values, index) => ({
    start: new Date(startMs + bucketWidth * index),
    end: new Date(startMs + bucketWidth * (index + 1)),
    searchCount: bucketCounts[index],
    p95RuntimeMs: nearestRank(values, 0.95),
  }));
  const slowest = filtered
    .filter((entry) => entry.durationMs !== null)
    .toSorted((left, right) => (right.durationMs ?? 0) - (left.durationMs ?? 0) || left.id.localeCompare(right.id))
    .slice(0, 5);
  const failedCount = filtered.filter((entry) => entry.finalState === SearchJobState.SEARCH_JOB_STATE_FAILED).length;
  const failures = filtered
    .filter((entry) => entry.finalState === SearchJobState.SEARCH_JOB_STATE_FAILED)
    .toSorted((left, right) => right.createdAt.valueOf() - left.createdAt.valueOf() || left.id.localeCompare(right.id))
    .slice(0, 5);
  return {
    searchCount: filtered.length,
    completedCount: filtered.filter((entry) => entry.finalState === SearchJobState.SEARCH_JOB_STATE_COMPLETED).length,
    failedCount,
    canceledCount: filtered.filter((entry) => entry.finalState === SearchJobState.SEARCH_JOB_STATE_CANCELED).length,
    expiredCount: filtered.filter((entry) => entry.finalState === SearchJobState.SEARCH_JOB_STATE_EXPIRED).length,
    scannedRows: sumCounter(filtered, (entry) => entry.scannedRows),
    scannedBytes: sumCounter(filtered, (entry) => entry.scannedBytes),
    matchedEvents: sumCounter(filtered, (entry) => entry.matchedEvents),
    producedRows: sumCounter(filtered, (entry) => entry.producedRows),
    medianRuntimeMs: median(durationValues),
    p95RuntimeMs: nearestRank(durationValues, 0.95),
    trend,
    slowest,
    failures,
  };
}
