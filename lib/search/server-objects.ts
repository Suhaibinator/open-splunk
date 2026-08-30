import {
  SortDirection,
  SharingScope,
  type ApiWarning,
  type ResolvedTimeRange,
} from "@/gen/ts/open_splunk/common";
import type { SearchHistoryEntry } from "@/gen/ts/open_splunk/history";
import {
  SearchHistorySortBy,
  type SearchHistoryFilter,
} from "@/gen/ts/open_splunk/history_api";
import {
  ScheduledSearchOutcome,
  type SavedSearch,
} from "@/gen/ts/open_splunk/saved_search";
import { SavedSearchSortBy } from "@/gen/ts/open_splunk/saved_search_api";
import {
  SearchJobState,
  SearchResultTab,
  type SearchDefinition,
  type SearchJobSource,
} from "@/gen/ts/open_splunk/search";
import type { VisualizationSpec } from "@/gen/ts/open_splunk/result";
import { ServerFeature } from "@/gen/ts/open_splunk/system_api";
import type {
  DemoHistoryEntry,
  DemoSavedSearch,
} from "@/lib/demo/search-data";
import {
  durationToMilliseconds,
  formatDurationMilliseconds,
  validDate,
} from "@/lib/api/duration";
import type { OpenSplunkApiClient } from "@/lib/api/open-splunk-client";
import { collectCursorPages } from "@/lib/api/pagination";
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
import { serverSearchJobOriginLabel } from "./server-jobs";
import { adaptRetainedJobReference, type RetainedResultState } from "./server-job-settings";

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
  schedule: ServerSavedSearchSchedule | null;
  scheduleStatus: ServerScheduledSearchStatus | null;
  createdAt: Date | null;
  updatedAt: Date | null;
}

export interface ServerSavedSearchSchedule {
  configVersion: bigint;
  enabled: boolean;
  cron: string;
  timezone: string;
  dispatchTtl: string;
}

export interface ServerScheduledSearchStatus {
  nextRunAt: Date | null;
  lastRunAt: Date | null;
  lastOutcome: ScheduledSearchOutcome;
  latestSearchJobId: string | null;
  latestRetainedResultState: RetainedResultState | null;
  latestResultExpiresAt: Date | null;
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

function requireSearchDefinition(search: SearchDefinition | undefined, context: string): SearchDefinition {
  if (search === undefined) throw new TypeError(`${context} did not include a search definition.`);
  return search;
}

function validScheduledSearchStatusOutcome(outcome: ScheduledSearchOutcome): boolean {
  switch (outcome) {
    case ScheduledSearchOutcome.SCHEDULED_SEARCH_OUTCOME_UNSPECIFIED:
    case ScheduledSearchOutcome.SCHEDULED_SEARCH_OUTCOME_RUNNING:
    case ScheduledSearchOutcome.SCHEDULED_SEARCH_OUTCOME_COMPLETED:
    case ScheduledSearchOutcome.SCHEDULED_SEARCH_OUTCOME_FAILED:
    case ScheduledSearchOutcome.SCHEDULED_SEARCH_OUTCOME_CANCELED:
    case ScheduledSearchOutcome.SCHEDULED_SEARCH_OUTCOME_SKIPPED_OVERLAP:
    case ScheduledSearchOutcome.SCHEDULED_SEARCH_OUTCOME_INTERRUPTED:
      return true;
    case ScheduledSearchOutcome.UNRECOGNIZED:
    default:
      return false;
  }
}

export function adaptSavedSearch(savedSearch: SavedSearch): ServerSavedSearch {
  const definition = savedSearch.definition;
  if (definition === undefined) throw new TypeError("The saved search response did not include a definition.");
  const schedule = definition.schedule;
  const scheduleStatus = savedSearch.scheduleStatus;
  if (
    schedule !== undefined
    && (
      schedule.configVersion <= 0n
      || schedule.cron.trim().length === 0
      || schedule.timezone.trim().length === 0
      || schedule.dispatchTtl.trim().length === 0
    )
  ) {
    throw new TypeError("The saved search response included an invalid schedule projection.");
  }
  if (scheduleStatus !== undefined && !validScheduledSearchStatusOutcome(scheduleStatus.lastOutcome)) {
    throw new TypeError("The saved search response included an unsupported schedule outcome.");
  }
  let latestResult: ReturnType<typeof adaptRetainedJobReference> = {
    retainedResultState: null,
    searchJobExpiresAt: null,
    searchJobId: null,
  };
  if (scheduleStatus !== undefined) {
    latestResult = adaptRetainedJobReference({
      retainedResultStatus: scheduleStatus.latestRetainedResultStatus,
      searchJobExpiresAt: scheduleStatus.latestResultExpiresAt,
      searchJobId: scheduleStatus.latestSearchJobId,
    });
  }
  return {
    id: savedSearch.savedSearchId,
    version: savedSearch.version,
    name: definition.name,
    description: definition.description ?? "",
    search: requireSearchDefinition(definition.search, "The saved search"),
    sharingScope: definition.sharingScope,
    ownerId: definition.ownerId?.trim() || null,
    schedule: schedule === undefined ? null : {
      configVersion: schedule.configVersion,
      enabled: schedule.enabled,
      cron: schedule.cron,
      timezone: schedule.timezone,
      dispatchTtl: schedule.dispatchTtl,
    },
    scheduleStatus: scheduleStatus === undefined ? null : {
      nextRunAt: validDate(scheduleStatus.nextRunAt),
      lastRunAt: validDate(scheduleStatus.lastRunAt),
      lastOutcome: scheduleStatus.lastOutcome,
      latestSearchJobId: latestResult.searchJobId,
      latestRetainedResultState: latestResult.retainedResultState,
      latestResultExpiresAt: latestResult.searchJobExpiresAt,
    },
    createdAt: validDate(savedSearch.createdAt),
    updatedAt: validDate(savedSearch.updatedAt),
  };
}

export function adaptSearchHistoryEntry(entry: SearchHistoryEntry): ServerSearchHistoryEntry {
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
    durationMs: durationToMilliseconds(entry.duration),
    warnings: [...entry.warnings],
    warningCount: entry.warnings.length,
    failureMessage: entry.failure?.message ?? null,
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
  switch (state) {
    case SearchJobState.SEARCH_JOB_STATE_COMPLETED: return "Completed";
    case SearchJobState.SEARCH_JOB_STATE_CANCELED: return "Canceled";
    case SearchJobState.SEARCH_JOB_STATE_EXPIRED: return "Expired";
    case SearchJobState.SEARCH_JOB_STATE_FAILED: return "Failed";
    case SearchJobState.SEARCH_JOB_STATE_INTERRUPTED: return "Interrupted";
    default: throw new TypeError("The history entry returned an unsupported final state.");
  }
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
    sourceLabel: serverSearchJobOriginLabel(entry.source),
    resolvedTimeRange: formatResolvedHistoryRange(entry.resolvedTimeRange),
    state: historyState(entry.finalState),
    events: safeNumber(resultCount),
    eventsExact: resultCount > BigInt(Number.MAX_SAFE_INTEGER)
      ? resultCount.toString()
      : undefined,
    duration: formatDurationMilliseconds(entry.durationMs),
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
  try {
    const collected = await collectCursorPages<ServerSavedSearch>({
      maximumPages,
      pageToken: options.pageToken?.trim() || undefined,
      label: "Saved searches",
      fetchPage: async ({ pageToken, includeTotalSize }) => {
        const response = await client.savedSearches.list({
          page: { pageSize, pageToken, includeTotalSize },
          appIdFilter: options.appId?.trim() || undefined,
          textFilter: options.text?.trim() || undefined,
          sharingScopeFilters: [...new Set(options.sharingScopes ?? [])],
          sortBy: options.sortBy ?? SavedSearchSortBy.SAVED_SEARCH_SORT_BY_UPDATED_AT,
          sortDirection: options.sortDirection ?? SortDirection.SORT_DIRECTION_DESCENDING,
        }, options);
        return { items: response.savedSearches.map(adaptSavedSearch), page: response.page };
      },
    });
    return { status: "available", value: collected };
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
  try {
    const collected = await collectCursorPages<ServerSearchHistoryEntry>({
      maximumPages,
      pageToken: options.pageToken?.trim() || undefined,
      label: "Search history",
      fetchPage: async ({ pageToken, includeTotalSize }) => {
        const response = await client.history.list({
          page: { pageSize, pageToken, includeTotalSize },
          filter: historyFilter(options),
          sortBy: options.sortBy ?? SearchHistorySortBy.SEARCH_HISTORY_SORT_BY_CREATED_AT,
          sortDirection: options.sortDirection ?? SortDirection.SORT_DIRECTION_DESCENDING,
        }, options);
        return { items: response.historyEntries.map(adaptSearchHistoryEntry), page: response.page };
      },
    });
    return { status: "available", value: collected };
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
