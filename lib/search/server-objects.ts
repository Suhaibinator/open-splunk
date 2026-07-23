import {
  SortDirection,
  SharingScope,
  type ApiWarning,
  type ResolvedTimeRange,
} from "@/gen/ts/open_splunk/v1/common";
import type { SearchHistoryEntry } from "@/gen/ts/open_splunk/v1/history";
import {
  SearchHistorySortBy,
  type SearchHistoryFilter,
} from "@/gen/ts/open_splunk/v1/history_api";
import type { SavedSearch } from "@/gen/ts/open_splunk/v1/saved_search";
import { SavedSearchSortBy } from "@/gen/ts/open_splunk/v1/saved_search_api";
import {
  SearchJobState,
  SearchJobOrigin,
  SearchResultTab,
  type SearchDefinition,
  type SearchJobSource,
} from "@/gen/ts/open_splunk/v1/search";
import type { VisualizationSpec } from "@/gen/ts/open_splunk/v1/result";
import { ServerFeature } from "@/gen/ts/open_splunk/v1/system_api";
import type {
  DemoHistoryEntry,
  DemoSavedSearch,
} from "@/lib/demo/search-data";
import type { OpenSplunkApiClient } from "@/lib/api/open-splunk-client";
import { recordNextPageToken } from "@/lib/api/pagination";
import {
  featureNotAdvertised,
  isAdvertisedFeatureRouteUnavailable,
  optionalRouteUnavailable,
  type OptionalFeatureResult,
} from "@/lib/api/optional-feature";
import type { ProtobufRequestOptions } from "@/lib/api/protobuf-transport";
import {
  supportsServerFeature,
  type SystemBootstrapModel,
} from "@/lib/api/system-bootstrap";

export interface ServerSearchDefinitionInput {
  spl: string;
  earliest: string;
  latest: string;
  timezone?: string;
  indexScope: readonly string[];
  appId?: string;
  preferredResultTab?: SearchResultTab;
  selectedFields?: readonly string[];
  visualization?: VisualizationSpec | null;
  /** Preserves server-owned presentation metadata during edits. */
  base?: SearchDefinition;
}

export interface ServerSavedSearch {
  id: string;
  version: bigint;
  name: string;
  description: string;
  search: SearchDefinition;
  sharingScope: SharingScope;
  ownerId: string | null;
  createdAt: Date | null;
  updatedAt: Date | null;
}

export interface ServerSearchHistoryEntry {
  id: string;
  search: SearchDefinition;
  source: SearchJobSource | null;
  effectiveIndexScope: string[];
  resolvedTimeRange: ResolvedTimeRange | null;
  finalState: SearchJobState;
  matchedEvents: bigint;
  scannedRows: bigint;
  scannedBytes: bigint;
  producedRows: bigint;
  durationMs: number;
  warnings: ApiWarning[];
  warningCount: number;
  failureMessage: string | null;
  compilerVersion: string;
  createdAt: Date | null;
  startedAt: Date | null;
  finishedAt: Date | null;
}

export interface ServerObjectPage<T> {
  items: T[];
  nextPageToken: string | null;
  totalSize: bigint | null;
  totalSizeExact: boolean;
  complete: boolean;
}

export type ServerObjectDateFormatter = (value: Date | null) => string;

function validDate(date: Date | undefined): Date | null {
  return date !== undefined && !Number.isNaN(date.valueOf()) ? new Date(date) : null;
}

function requireSearchDefinition(search: SearchDefinition | undefined, context: string): SearchDefinition {
  if (search === undefined) throw new TypeError(`${context} did not include a search definition.`);
  return search;
}

export function adaptSavedSearch(savedSearch: SavedSearch): ServerSavedSearch {
  const definition = savedSearch.definition;
  if (definition === undefined) throw new TypeError("The saved search response did not include a definition.");
  return {
    id: savedSearch.savedSearchId,
    version: savedSearch.version,
    name: definition.name,
    description: definition.description ?? "",
    search: requireSearchDefinition(definition.search, "The saved search"),
    sharingScope: definition.sharingScope,
    ownerId: definition.ownerId?.trim() || null,
    createdAt: validDate(savedSearch.createdAt),
    updatedAt: validDate(savedSearch.updatedAt),
  };
}

export function adaptSearchHistoryEntry(entry: SearchHistoryEntry): ServerSearchHistoryEntry {
  const duration = entry.duration;
  return {
    id: entry.searchJobId,
    search: requireSearchDefinition(entry.definition, "The history entry"),
    source: entry.source ?? null,
    effectiveIndexScope: [...entry.effectiveIndexScope],
    resolvedTimeRange: entry.resolvedTimeRange ?? null,
    finalState: entry.finalState,
    matchedEvents: entry.matchedEvents,
    scannedRows: entry.scannedRows,
    scannedBytes: entry.scannedBytes,
    producedRows: entry.producedRows,
    durationMs: duration === undefined
      ? 0
      : Number(duration.seconds) * 1_000 + duration.nanos / 1_000_000,
    warnings: [...entry.warnings],
    warningCount: entry.warnings.length,
    failureMessage: entry.failure?.message ?? null,
    compilerVersion: entry.compilerVersion,
    createdAt: validDate(entry.createdAt),
    startedAt: validDate(entry.startedAt),
    finishedAt: validDate(entry.finishedAt),
  };
}

function safeNumber(value: bigint): number {
  return Number(value > BigInt(Number.MAX_SAFE_INTEGER) ? BigInt(Number.MAX_SAFE_INTEGER) : value);
}

function sharingLabel(scope: SharingScope, ownerId: string | null): string {
  if (scope === SharingScope.SHARING_SCOPE_GLOBAL) return "Global";
  if (scope === SharingScope.SHARING_SCOPE_APP) return "App";
  return ownerId || "Private";
}

export function savedSearchToDemo(savedSearch: ServerSavedSearch): DemoSavedSearch {
  return {
    id: savedSearch.id,
    name: savedSearch.name,
    description: savedSearch.description,
    query: savedSearch.search.spl,
    earliest: savedSearch.search.timeRange?.earliest ?? "",
    latest: savedSearch.search.timeRange?.latest ?? "",
    timezone: savedSearch.search.timeRange?.timezone,
    updatedAt: (savedSearch.updatedAt ?? savedSearch.createdAt)?.toISOString() ?? "",
    owner: sharingLabel(savedSearch.sharingScope, savedSearch.ownerId),
  };
}

function localizedObjectDate(value: Date | null): string {
  return value?.toLocaleString() ?? "Unknown";
}

export function savedSearchForDisplay(
  savedSearch: ServerSavedSearch,
  formatDate: ServerObjectDateFormatter = localizedObjectDate,
): DemoSavedSearch {
  return {
    ...savedSearchToDemo(savedSearch),
    updatedAt: formatDate(savedSearch.updatedAt ?? savedSearch.createdAt),
  };
}

function historyState(state: SearchJobState): DemoHistoryEntry["state"] {
  if (state === SearchJobState.SEARCH_JOB_STATE_CANCELED) return "Canceled";
  if (state === SearchJobState.SEARCH_JOB_STATE_EXPIRED) return "Expired";
  if (state === SearchJobState.SEARCH_JOB_STATE_FAILED) return "Failed";
  return "Completed";
}

function formatDuration(milliseconds: number): string {
  if (!Number.isFinite(milliseconds) || milliseconds <= 0) return "0 ms";
  if (milliseconds < 1_000) return `${Math.round(milliseconds)} ms`;
  return `${(milliseconds / 1_000).toFixed(milliseconds < 10_000 ? 2 : 1)} s`;
}

function historySourceLabel(source: SearchJobSource | null): string {
  if (source?.origin === SearchJobOrigin.SEARCH_JOB_ORIGIN_SAVED_SEARCH) return "Saved search";
  if (source?.origin === SearchJobOrigin.SEARCH_JOB_ORIGIN_HISTORY_RERUN) return "History rerun";
  if (source?.origin === SearchJobOrigin.SEARCH_JOB_ORIGIN_DASHBOARD) return "Dashboard";
  if (source?.origin === SearchJobOrigin.SEARCH_JOB_ORIGIN_API) return "API";
  return "Ad hoc";
}

function formatResolvedHistoryRange(range: ResolvedTimeRange | null): string | undefined {
  if (range?.earliest === undefined || range.latest === undefined) return undefined;
  const timezone = range.timezone || "UTC";
  try {
    const formatter = new Intl.DateTimeFormat("en-US", {
      dateStyle: "short",
      timeStyle: "medium",
      timeZone: timezone,
    });
    return `${formatter.format(range.earliest)} – ${formatter.format(range.latest)} (${timezone})`;
  } catch {
    return `${range.earliest.toISOString()} – ${range.latest.toISOString()}`;
  }
}

export function searchHistoryToDemo(entry: ServerSearchHistoryEntry): DemoHistoryEntry {
  const earliest = entry.search.timeRange?.earliest;
  const latest = entry.search.timeRange?.latest;
  const resultCount = entry.producedRows;
  return {
    id: entry.id,
    query: entry.search.spl,
    timeRange: earliest && latest ? `${earliest} to ${latest}` : "Server default",
    earliest,
    latest,
    timezone: entry.search.timeRange?.timezone,
    appId: entry.search.appId,
    sourceLabel: historySourceLabel(entry.source),
    resolvedTimeRange: formatResolvedHistoryRange(entry.resolvedTimeRange),
    compilerVersion: entry.compilerVersion || undefined,
    state: historyState(entry.finalState),
    events: safeNumber(resultCount),
    eventsExact: resultCount > BigInt(Number.MAX_SAFE_INTEGER)
      ? resultCount.toString()
      : undefined,
    duration: formatDuration(entry.durationMs),
    ranAt: (entry.finishedAt ?? entry.createdAt)?.toISOString() ?? "",
  };
}

export function historyEntryForDisplay(
  entry: ServerSearchHistoryEntry,
  formatDate: ServerObjectDateFormatter = localizedObjectDate,
): DemoHistoryEntry {
  return {
    ...searchHistoryToDemo(entry),
    ranAt: formatDate(entry.finishedAt ?? entry.createdAt),
  };
}

function buildSearchDefinition(input: ServerSearchDefinitionInput): SearchDefinition {
  const spl = input.spl;
  if (spl.trim().length === 0) throw new TypeError("SPL is required.");
  if (input.earliest.trim().length === 0 || input.latest.trim().length === 0) {
    throw new TypeError("Earliest and latest search times are required.");
  }
  if (input.indexScope.length === 0) throw new TypeError("At least one exact index is required.");
  return {
    ...input.base,
    spl,
    timeRange: {
      earliest: input.earliest,
      latest: input.latest,
      timezone: input.timezone?.trim() || input.base?.timeRange?.timezone || undefined,
    },
    appId: input.appId?.trim() || input.base?.appId,
    indexScope: [...new Set(input.indexScope.map((name) => name.trim()).filter(Boolean))],
    preferredResultTab: input.preferredResultTab
      ?? input.base?.preferredResultTab
      ?? SearchResultTab.SEARCH_RESULT_TAB_UNSPECIFIED,
    selectedFields: [...new Set(
      (input.selectedFields ?? input.base?.selectedFields ?? [])
        .map((field) => field.trim())
        .filter(Boolean),
    )],
    visualization: input.visualization === null
      ? undefined
      : input.visualization ?? input.base?.visualization,
  };
}

export interface ListSavedSearchesOptions extends ProtobufRequestOptions {
  appId?: string;
  text?: string;
  sharingScopes?: readonly SharingScope[];
  sortBy?: SavedSearchSortBy;
  sortDirection?: SortDirection;
  pageSize?: number;
  pageToken?: string;
  maximumPages?: number;
}

export async function getServerSavedSearch(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  savedSearchId: string,
  options?: ProtobufRequestOptions,
): Promise<OptionalFeatureResult<ServerSavedSearch>> {
  if (!supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_SAVED_SEARCHES)) {
    return featureNotAdvertised;
  }
  const id = savedSearchId.trim();
  if (id.length === 0) throw new TypeError("Saved search ID is required.");
  try {
    const response = await client.savedSearches.get({ savedSearchId: id }, options);
    if (response.savedSearch === undefined) throw new TypeError("The server returned an empty saved search.");
    return { status: "available", value: adaptSavedSearch(response.savedSearch) };
  } catch (error) {
    if (isAdvertisedFeatureRouteUnavailable(error)) return optionalRouteUnavailable;
    throw error;
  }
}

export async function listServerSavedSearches(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  options: ListSavedSearchesOptions = {},
): Promise<OptionalFeatureResult<ServerObjectPage<ServerSavedSearch>>> {
  if (!supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_SAVED_SEARCHES)) {
    return featureNotAdvertised;
  }
  const pageSize = options.pageSize ?? Math.min(24, bootstrap.limits.maximumPageSize || 24);
  const maximumPages = options.maximumPages ?? 256;
  if (!Number.isInteger(pageSize) || pageSize <= 0) throw new RangeError("Page size must be positive.");
  if (!Number.isInteger(maximumPages) || maximumPages <= 0) {
    throw new RangeError("Maximum pages must be positive.");
  }
  const items: ServerSavedSearch[] = [];
  const initialPageToken = options.pageToken?.trim() || undefined;
  const seenTokens = new Set<string>(initialPageToken === undefined ? [] : [initialPageToken]);
  let pageToken = initialPageToken;
  let totalSize: bigint | null = null;
  let totalSizeExact = false;
  try {
    for (let pageIndex = 0; pageIndex < maximumPages; pageIndex += 1) {
      // Cursor pages are causally ordered and cannot be requested in parallel.
      // eslint-disable-next-line no-await-in-loop
      const response = await client.savedSearches.list({
        page: { pageSize, pageToken, includeTotalSize: pageIndex === 0 && initialPageToken === undefined },
        appIdFilter: options.appId?.trim() || undefined,
        textFilter: options.text?.trim() || undefined,
        sharingScopeFilters: [...new Set(options.sharingScopes ?? [])],
        sortBy: options.sortBy ?? SavedSearchSortBy.SAVED_SEARCH_SORT_BY_UPDATED_AT,
        sortDirection: options.sortDirection ?? SortDirection.SORT_DIRECTION_DESCENDING,
      }, options);
      items.push(...response.savedSearches.map(adaptSavedSearch));
      if (pageIndex === 0) {
        totalSize = response.page?.totalSize ?? null;
        totalSizeExact = response.page?.totalSizeExact ?? false;
      }
      const nextToken = recordNextPageToken(
        seenTokens,
        response.page?.nextPageToken,
        "Saved searches",
      );
      if (nextToken === null) {
        return {
          status: "available",
          value: { items, nextPageToken: null, totalSize, totalSizeExact, complete: true },
        };
      }
      pageToken = nextToken;
    }
    return {
      status: "available",
      value: { items, nextPageToken: pageToken ?? null, totalSize, totalSizeExact, complete: false },
    };
  } catch (error) {
    if (isAdvertisedFeatureRouteUnavailable(error)) return optionalRouteUnavailable;
    throw error;
  }
}

export interface SaveServerSearchOptions extends ProtobufRequestOptions {
  name: string;
  description?: string;
  search: ServerSearchDefinitionInput;
  sharingScope?: SharingScope;
  ownerId?: string;
}

function savedSearchDefinition(options: SaveServerSearchOptions) {
  const name = options.name.trim();
  if (name.length === 0) throw new TypeError("Saved search name is required.");
  return {
    name,
    description: options.description?.trim() || undefined,
    search: buildSearchDefinition(options.search),
    sharingScope: options.sharingScope ?? SharingScope.SHARING_SCOPE_PRIVATE,
    ownerId: options.ownerId?.trim() || undefined,
  };
}

export async function createServerSavedSearch(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  options: SaveServerSearchOptions,
): Promise<OptionalFeatureResult<ServerSavedSearch>> {
  if (!supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_SAVED_SEARCHES)) {
    return featureNotAdvertised;
  }
  try {
    const response = await client.savedSearches.create({
      definition: savedSearchDefinition(options),
      clientRequestId: undefined,
    }, options);
    if (response.savedSearch === undefined) throw new TypeError("The server returned an empty saved search.");
    return { status: "available", value: adaptSavedSearch(response.savedSearch) };
  } catch (error) {
    if (isAdvertisedFeatureRouteUnavailable(error)) return optionalRouteUnavailable;
    throw error;
  }
}

export interface UpdateServerSavedSearchOptions extends SaveServerSearchOptions {
  id: string;
  expectedVersion: bigint;
  updatePaths?: readonly (
    "name" | "description" | "search" | "sharing_scope" | "owner_id"
  )[];
}

export async function updateServerSavedSearch(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  options: UpdateServerSavedSearchOptions,
): Promise<OptionalFeatureResult<ServerSavedSearch>> {
  if (!supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_SAVED_SEARCHES)) {
    return featureNotAdvertised;
  }
  const id = options.id.trim();
  if (id.length === 0) throw new TypeError("Saved search ID is required.");
  if (options.expectedVersion <= 0n) throw new RangeError("Expected version must be positive.");
  try {
    const response = await client.savedSearches.update({
      savedSearchId: id,
      expectedVersion: options.expectedVersion,
      definition: savedSearchDefinition(options),
      updateMask: [...new Set(options.updatePaths ?? ["name", "description", "search", "sharing_scope", "owner_id"])],
    }, options);
    if (response.savedSearch === undefined) throw new TypeError("The server returned an empty saved search.");
    return { status: "available", value: adaptSavedSearch(response.savedSearch) };
  } catch (error) {
    if (isAdvertisedFeatureRouteUnavailable(error)) return optionalRouteUnavailable;
    throw error;
  }
}

export async function duplicateServerSavedSearch(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  savedSearchId: string,
  newName: string,
  destinationAppId?: string,
  options?: ProtobufRequestOptions,
): Promise<OptionalFeatureResult<ServerSavedSearch>> {
  if (!supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_SAVED_SEARCHES)) {
    return featureNotAdvertised;
  }
  try {
    const response = await client.savedSearches.duplicate({
      savedSearchId: savedSearchId.trim(),
      newName: newName.trim(),
      destinationAppId: destinationAppId?.trim() || undefined,
      clientRequestId: undefined,
    }, options);
    if (response.savedSearch === undefined) throw new TypeError("The server returned an empty saved search.");
    return { status: "available", value: adaptSavedSearch(response.savedSearch) };
  } catch (error) {
    if (isAdvertisedFeatureRouteUnavailable(error)) return optionalRouteUnavailable;
    throw error;
  }
}

export async function renameServerSavedSearch(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  savedSearch: ServerSavedSearch,
  newName: string,
  options?: ProtobufRequestOptions,
): Promise<OptionalFeatureResult<ServerSavedSearch>> {
  if (!supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_SAVED_SEARCHES)) {
    return featureNotAdvertised;
  }
  const name = newName.trim();
  if (name.length === 0) throw new TypeError("Saved search name is required.");
  if (savedSearch.version <= 0n) throw new RangeError("Expected version must be positive.");
  try {
    const response = await client.savedSearches.update({
      savedSearchId: savedSearch.id,
      expectedVersion: savedSearch.version,
      definition: {
        name,
        description: savedSearch.description || undefined,
        search: savedSearch.search,
        sharingScope: savedSearch.sharingScope,
        ownerId: savedSearch.ownerId ?? undefined,
      },
      updateMask: ["name"],
    }, options);
    if (response.savedSearch === undefined) throw new TypeError("The server returned an empty saved search.");
    return { status: "available", value: adaptSavedSearch(response.savedSearch) };
  } catch (error) {
    if (isAdvertisedFeatureRouteUnavailable(error)) return optionalRouteUnavailable;
    throw error;
  }
}

export async function deleteServerSavedSearch(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  savedSearchId: string,
  expectedVersion: bigint,
  options?: ProtobufRequestOptions,
): Promise<OptionalFeatureResult<string>> {
  if (!supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_SAVED_SEARCHES)) {
    return featureNotAdvertised;
  }
  if (expectedVersion <= 0n) throw new RangeError("Expected version must be positive.");
  try {
    const response = await client.savedSearches.delete({
      savedSearchId: savedSearchId.trim(),
      expectedVersion,
    }, options);
    return { status: "available", value: response.savedSearchId };
  } catch (error) {
    if (isAdvertisedFeatureRouteUnavailable(error)) return optionalRouteUnavailable;
    throw error;
  }
}

export interface ListServerHistoryOptions extends ProtobufRequestOptions {
  appId?: string;
  states?: readonly SearchJobState[];
  text?: string;
  savedSearchId?: string;
  createdAfter?: Date;
  createdBefore?: Date;
  sortBy?: SearchHistorySortBy;
  sortDirection?: SortDirection;
  pageSize?: number;
  pageToken?: string;
  maximumPages?: number;
}

export async function getServerSearchHistoryEntry(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  searchJobId: string,
  options?: ProtobufRequestOptions,
): Promise<OptionalFeatureResult<ServerSearchHistoryEntry>> {
  if (!supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_SEARCH_HISTORY)) {
    return featureNotAdvertised;
  }
  const id = searchJobId.trim();
  if (id.length === 0) throw new TypeError("Search job ID is required.");
  try {
    const response = await client.history.get({ searchJobId: id }, options);
    if (response.historyEntry === undefined) throw new TypeError("The server returned an empty history entry.");
    return { status: "available", value: adaptSearchHistoryEntry(response.historyEntry) };
  } catch (error) {
    if (isAdvertisedFeatureRouteUnavailable(error)) return optionalRouteUnavailable;
    throw error;
  }
}

function historyFilter(options: ListServerHistoryOptions): SearchHistoryFilter | undefined {
  const filter: SearchHistoryFilter = {
    appId: options.appId?.trim() || undefined,
    stateFilters: [...new Set(options.states ?? [])],
    text: options.text?.trim() || undefined,
    savedSearchId: options.savedSearchId?.trim() || undefined,
    createdAfter: options.createdAfter,
    createdBefore: options.createdBefore,
  };
  return filter.appId || filter.stateFilters.length > 0 || filter.text || filter.savedSearchId
    || filter.createdAfter || filter.createdBefore
    ? filter
    : undefined;
}

export async function listServerSearchHistory(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  options: ListServerHistoryOptions = {},
): Promise<OptionalFeatureResult<ServerObjectPage<ServerSearchHistoryEntry>>> {
  if (!supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_SEARCH_HISTORY)) {
    return featureNotAdvertised;
  }
  const pageSize = options.pageSize ?? Math.min(15, bootstrap.limits.maximumPageSize || 15);
  const maximumPages = options.maximumPages ?? 256;
  if (!Number.isInteger(pageSize) || pageSize <= 0) throw new RangeError("Page size must be positive.");
  if (!Number.isInteger(maximumPages) || maximumPages <= 0) {
    throw new RangeError("Maximum pages must be positive.");
  }
  const items: ServerSearchHistoryEntry[] = [];
  const initialPageToken = options.pageToken?.trim() || undefined;
  const seenTokens = new Set<string>(initialPageToken === undefined ? [] : [initialPageToken]);
  let pageToken = initialPageToken;
  let totalSize: bigint | null = null;
  let totalSizeExact = false;
  try {
    for (let pageIndex = 0; pageIndex < maximumPages; pageIndex += 1) {
      // Cursor pages are causally ordered and cannot be requested in parallel.
      // eslint-disable-next-line no-await-in-loop
      const response = await client.history.list({
        page: { pageSize, pageToken, includeTotalSize: pageIndex === 0 && initialPageToken === undefined },
        filter: historyFilter(options),
        sortBy: options.sortBy ?? SearchHistorySortBy.SEARCH_HISTORY_SORT_BY_CREATED_AT,
        sortDirection: options.sortDirection ?? SortDirection.SORT_DIRECTION_DESCENDING,
      }, options);
      items.push(...response.historyEntries.map(adaptSearchHistoryEntry));
      if (pageIndex === 0) {
        totalSize = response.page?.totalSize ?? null;
        totalSizeExact = response.page?.totalSizeExact ?? false;
      }
      const nextToken = recordNextPageToken(
        seenTokens,
        response.page?.nextPageToken,
        "Search history",
      );
      if (nextToken === null) {
        return {
          status: "available",
          value: { items, nextPageToken: null, totalSize, totalSizeExact, complete: true },
        };
      }
      pageToken = nextToken;
    }
    return {
      status: "available",
      value: { items, nextPageToken: pageToken ?? null, totalSize, totalSizeExact, complete: false },
    };
  } catch (error) {
    if (isAdvertisedFeatureRouteUnavailable(error)) return optionalRouteUnavailable;
    throw error;
  }
}

export async function deleteServerSearchHistoryEntry(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  searchJobId: string,
  options?: ProtobufRequestOptions,
): Promise<OptionalFeatureResult<string>> {
  if (!supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_SEARCH_HISTORY)) {
    return featureNotAdvertised;
  }
  try {
    const response = await client.history.delete({ searchJobId: searchJobId.trim() }, options);
    return { status: "available", value: response.searchJobId };
  } catch (error) {
    if (isAdvertisedFeatureRouteUnavailable(error)) return optionalRouteUnavailable;
    throw error;
  }
}

export async function clearServerSearchHistory(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  filter: SearchHistoryFilter | undefined,
  confirmation: "CLEAR SEARCH HISTORY",
  options?: ProtobufRequestOptions,
): Promise<OptionalFeatureResult<bigint>> {
  if (!supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_SEARCH_HISTORY)) {
    return featureNotAdvertised;
  }
  try {
    const response = await client.history.clear({ filter, confirmation }, options);
    return { status: "available", value: response.deletedCount };
  } catch (error) {
    if (isAdvertisedFeatureRouteUnavailable(error)) return optionalRouteUnavailable;
    throw error;
  }
}
