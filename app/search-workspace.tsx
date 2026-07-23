"use client";

import {
  type ChangeEvent,
  type Dispatch,
  type FormEvent,
  type KeyboardEvent,
  type PointerEvent,
  type SetStateAction,
  type UIEvent,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import Link from "next/link";

import { SharingScope } from "@/gen/ts/open_splunk/v1/common";
import type { ResolvedTimeRange } from "@/gen/ts/open_splunk/v1/common";
import {
  ExportJobState,
  type ExportJob,
} from "@/gen/ts/open_splunk/v1/export";
import type { SearchHistoryFilter } from "@/gen/ts/open_splunk/v1/history_api";
import {
  DEMO_EVENTS,
  DEMO_FIELDS,
  DEMO_HISTORY,
  DEMO_PATTERNS,
  DEMO_SAVED_SEARCHES,
  DEMO_STATISTICS,
  DEMO_TIMELINE,
  type DemoEvent,
  type DemoField,
  type DemoHistoryEntry,
  type DemoSavedSearch,
  type DemoScalar,
  type TimelinePoint,
} from "@/lib/demo/search-data";
import {
  ResultSchema as ResultSchemaCodec,
  ResultSetKind,
  VisualizationStackMode,
  VisualizationType,
  type ResultRow,
  type ResultSchema,
  type VisualizationSpec,
} from "@/gen/ts/open_splunk/v1/result";
import {
  SearchJobOrigin,
  SearchJobState,
  SearchResultTab,
  type SearchDefinition,
  type SearchJob,
  type SearchProgress,
} from "@/gen/ts/open_splunk/v1/search";
import {
  SearchWebSocketProtocolErrorCode,
  type ResultPreview,
  type SearchWebSocketEvent,
} from "@/gen/ts/open_splunk/v1/search_ws";
import { ServerFeature } from "@/gen/ts/open_splunk/v1/system_api";
import {
  SearchWebSocketClient,
  createOpenSplunkApiClient,
  getSystemBootstrap,
  indexSelectorsFromSPL,
  isHttpError,
  isHttpStatus,
  recordNextPageToken,
  RepeatedPageCursorError,
  resolveExactIndexScope,
  searchJobTarget,
  splIndexScopeIsExhaustive,
  supportsServerFeature,
  type SystemBootstrapModel,
} from "@/lib/api";
import {
  adaptSearchResults,
  compareWorkspaceStatisticValues,
  patternsFromEvents,
  resolveAbsoluteTimeRange,
  timechartSpanMilliseconds,
  timechartRowsForExport,
  timechartValueFields,
  type AdaptedSearchResults,
  type WorkspaceStatistic,
  type WorkspaceStatisticsSort,
  type WorkspaceStatisticsTable,
} from "@/lib/search/backend-data";
import { applyFieldPivot, type PivotMode } from "@/lib/search/query-pivots";
import { splFromFindInput } from "@/lib/search/launch-url";
import {
  cancelServerExport,
  clearServerSearchHistory,
  createServerExport,
  createServerSavedSearch,
  deleteServerSavedSearch,
  deleteServerSearchHistoryEntry,
  downloadServerExport,
  duplicateServerSavedSearch,
  getServerSavedSearch,
  getServerSearchHistoryEntry,
  getServerFieldCatalog,
  getServerFieldSummary,
  getServerTimeline,
  historyEntryForDisplay,
  listServerSavedSearches,
  listServerSearchHistory,
  renameServerSavedSearch,
  savedSearchForDisplay,
  serverFieldToDemoField,
  updateServerSavedSearch,
  waitForServerExport,
  type ServerSavedSearch,
  type ServerSearchHistoryEntry,
} from "@/lib/search/server-api";
import {
  applyDiagnosticFix,
  completionContextAt,
  getQueryDiagnostic,
  isCursorInQuotedValue,
  type SplDiagnostic,
} from "@/lib/search/spl-editor";

import { installModalSurface } from "./_components/modal-surface";
import { SearchComposer } from "./search-workspace/components/search-composer";
import { WorkspaceDialogs } from "./search-workspace/components/workspace-dialogs";
import {
  COMPLETIONS,
  DEFAULT_QUERY,
  EVENT_EXPORT_FIELDS,
  EXPORT_FIELD_LABELS,
  EXPORT_FIELDS_BY_TAB,
  NUMBER_FORMAT,
  TIME_PRESETS,
} from "./search-workspace/constants";
import type {
  ChartStyle,
  DialogActionState,
  EventDisplay,
  ExportArtifactDetails,
  ExportDialogState,
  ExportStage,
  JobPhase,
  MenuName,
  ModalName,
  PatternSensitivity,
  ResultTab,
  SearchMode,
  SearchSettingsCapabilities,
  SearchWorkspaceProps,
  StatsDensity,
  TimePickerSection,
  TimeRange,
  TimelineDisplay,
  ToastState,
} from "./search-workspace/model";
import { serverTimeRangeValidationError } from "./search-workspace/time-range";
import {
  applyLiveResultPreview,
  validateLivePreviewSchema,
  type LivePreviewSnapshot,
} from "./search-workspace/live-preview";
import { EventsPanel } from "./search-workspace/panels/events-panel";
import { PatternsPanel } from "./search-workspace/panels/patterns-panel";
import { StatisticsPanel } from "./search-workspace/panels/statistics-panel";
import { VisualizationPanel } from "./search-workspace/panels/visualization-panel";
import {
  backendJobPhase,
  eventCountForQuery,
  filteredDemoEvents,
  formatBytes,
  formatDuration,
  formatFieldValue,
  hasPipelineCommand,
  jobEventCount,
  phaseLabel,
  queryForPattern,
  resultTabForQuery,
  stateClass,
  timelineBoundaryLabel,
  timelineIndexFromPointer,
} from "./search-workspace/workspace-utils";

const ACTIVE_PHASES = new Set<JobPhase>(["queued", "parsing", "planning", "running", "finalizing"]);
const RESULT_TAB_ORDER: ResultTab[] = ["events", "patterns", "statistics", "visualization"];
const TERMINAL_HISTORY_STATES = [
  SearchJobState.SEARCH_JOB_STATE_COMPLETED,
  SearchJobState.SEARCH_JOB_STATE_FAILED,
  SearchJobState.SEARCH_JOB_STATE_CANCELED,
  SearchJobState.SEARCH_JOB_STATE_EXPIRED,
] as const;
const DEFAULT_BACKEND_PAGE_SIZE = 1_000;
const MAX_CACHED_RESULT_PAGES = 8;
const MAXIMUM_SAVED_SEARCH_NAME_BYTES = 255;
const MAXIMUM_READABLE_DUPLICATE_NAME_ATTEMPTS = 8;
const MAXIMUM_RANDOM_DUPLICATE_NAME_ATTEMPTS = 3;
const REST_RECOVERY_DELAYS_MS = [1_500, 2_500, 5_000, 10_000] as const;

interface BackendBootstrapState {
  response: SystemBootstrapModel;
  receivedAt: number;
}

type BackendConnectionState = "loading" | "ready" | "error";

interface BackendResultPage {
  schema: ResultSchema;
  rows: ResultRow[];
  nextPageToken?: string;
  totalSize?: number;
  totalSizeExact: boolean;
  snapshotComplete: boolean;
}

type BackendPreviewStatus =
  | "disabled"
  | "waiting"
  | "live"
  | "paused"
  | "resyncing"
  | "limited"
  | "finalizing"
  | "finalization-error";

interface BackendPreviewDisplay {
  schema: ResultSchema;
  snapshot: LivePreviewSnapshot;
  adapted: AdaptedSearchResults;
}

interface SavedWorkspaceBaseline {
  savedSearchId: string;
  activeTab: ResultTab;
  chartStyle: ChartStyle;
  chartTitle: string;
  earliest: string;
  latest: string;
  legendVisible: boolean;
  query: string;
  selectedFields: string[];
  showDataLabels: boolean;
  timeZone: string;
}

type BackendObjectMutation =
  | { kind: "save" }
  | { kind: "duplicateSaved"; targetId: string }
  | { kind: "renameSaved"; targetId: string }
  | { kind: "deleteSaved"; targetId: string }
  | { kind: "deleteHistory"; targetId: string };

function mergeDisplayPage<TSource extends { id: string }, TDisplay extends { id: string }>(
  current: readonly TDisplay[],
  page: readonly TSource[],
  adapt: (source: TSource) => TDisplay,
): TDisplay[] {
  const existingIds = new Set(current.map((item) => item.id));
  const updatedById = new Map(page.map((item) => [item.id, item]));
  return [
    ...current.map((item) => {
      const updated = updatedById.get(item.id);
      return updated === undefined ? item : adapt(updated);
    }),
    ...page.filter((item) => !existingIds.has(item.id)).map(adapt),
  ];
}

function savedWorkspaceFingerprint(baseline: SavedWorkspaceBaseline): string {
  return JSON.stringify({
    ...baseline,
    selectedFields: baseline.selectedFields.toSorted(),
  });
}

function exportCellString(value: unknown): string {
  return value !== null && typeof value === "object" ? JSON.stringify(value) : String(value ?? "");
}

function padDatePart(part: number): string {
  return String(part).padStart(2, "0");
}

function formatLocalDateTimeInput(value: string): string {
  const date = new Date(value);
  return `${date.getFullYear()}-${padDatePart(date.getMonth() + 1)}-${padDatePart(date.getDate())}T${padDatePart(date.getHours())}:${padDatePart(date.getMinutes())}`;
}

function resultTabForBackendKind(kind: ResultSetKind): ResultTab {
  switch (kind) {
    case ResultSetKind.RESULT_SET_KIND_STATISTICS:
      return "statistics";
    case ResultSetKind.RESULT_SET_KIND_TIME_SERIES:
      return "visualization";
    case ResultSetKind.RESULT_SET_KIND_EVENTS:
    default:
      return "events";
  }
}

function searchResultTabFromWorkspace(tab: ResultTab): SearchResultTab {
  switch (tab) {
    case "statistics":
      return SearchResultTab.SEARCH_RESULT_TAB_STATISTICS;
    case "visualization":
      return SearchResultTab.SEARCH_RESULT_TAB_VISUALIZATION;
    case "patterns":
    case "events":
    default:
      return SearchResultTab.SEARCH_RESULT_TAB_EVENTS;
  }
}

function workspaceResultTabFromSaved(tab: SearchResultTab): ResultTab | null {
  switch (tab) {
    case SearchResultTab.SEARCH_RESULT_TAB_EVENTS:
      return "events";
    case SearchResultTab.SEARCH_RESULT_TAB_STATISTICS:
      return "statistics";
    case SearchResultTab.SEARCH_RESULT_TAB_VISUALIZATION:
      return "visualization";
    default:
      return null;
  }
}

function visualizationTypeForChartStyle(style: ChartStyle): VisualizationType {
  if (style === "line") return VisualizationType.VISUALIZATION_TYPE_LINE;
  if (style === "horizontal") return VisualizationType.VISUALIZATION_TYPE_BAR;
  return VisualizationType.VISUALIZATION_TYPE_COLUMN;
}

function chartStyleForVisualizationType(type: VisualizationType): ChartStyle | null {
  if (type === VisualizationType.VISUALIZATION_TYPE_LINE) return "line";
  if (type === VisualizationType.VISUALIZATION_TYPE_BAR) return "horizontal";
  if (type === VisualizationType.VISUALIZATION_TYPE_COLUMN) return "column";
  return null;
}

function resultTabCompatibleWithKind(tab: ResultTab, kind: ResultSetKind): boolean {
  if (kind === ResultSetKind.RESULT_SET_KIND_EVENTS) return tab === "events";
  if (
    kind === ResultSetKind.RESULT_SET_KIND_STATISTICS
    || kind === ResultSetKind.RESULT_SET_KIND_TIME_SERIES
  ) {
    return tab === "statistics" || tab === "visualization";
  }
  return false;
}

function searchJobIdForWebSocketEvent(event: SearchWebSocketEvent): string | null {
  if (event.target?.target?.$case === "searchJobId") return event.target.target.value;
  switch (event.payload?.$case) {
    case "searchStateChanged":
    case "resultSchemaAvailable":
    case "resultPreview":
    case "searchTerminal":
      return event.payload.value.searchJobId;
    default:
      return null;
  }
}

function uniqueMessages(messages: Array<string | undefined>): string[] {
  return [...new Set(messages.map((message) => message?.trim()).filter((message): message is string => Boolean(message)))];
}

function equalResultSchemas(left: ResultSchema, right: ResultSchema): boolean {
  const leftBytes = ResultSchemaCodec.encode(left).finish();
  const rightBytes = ResultSchemaCodec.encode(right).finish();
  return leftBytes.length === rightBytes.length
    && leftBytes.every((value, index) => value === rightBytes[index]);
}

type SavedSearchConflictKind = "name" | "version" | "unknown";

function savedSearchConflictKind(error: unknown): SavedSearchConflictKind | null {
  if (!isHttpStatus(error, 409)) return null;
  if (/saved search version conflict/i.test(error.message)) return "version";
  if (/saved search with that name already exists/i.test(error.message)) return "name";
  return "unknown";
}

function truncateUtf8ToByteLength(value: string, maximumBytes: number): string {
  const encoder = new TextEncoder();
  let byteLength = 0;
  let result = "";
  for (const character of value) {
    const characterBytes = encoder.encode(character).byteLength;
    if (byteLength + characterBytes > maximumBytes) break;
    result += character;
    byteLength += characterBytes;
  }
  return result;
}

function savedSearchNameWithSuffix(name: string, suffix: string): string {
  const suffixBytes = new TextEncoder().encode(suffix).byteLength;
  const base = truncateUtf8ToByteLength(
    name.trim(),
    Math.max(0, MAXIMUM_SAVED_SEARCH_NAME_BYTES - suffixBytes),
  ).trimEnd();
  return `${base}${suffix}`.trim();
}

function duplicateSavedSearchName(name: string, copyNumber: number): string {
  return savedSearchNameWithSuffix(
    name,
    copyNumber <= 1 ? " copy" : ` copy ${copyNumber}`,
  );
}

function randomDuplicateSavedSearchName(name: string): string {
  const entropy = globalThis.crypto.randomUUID().replaceAll("-", "");
  return savedSearchNameWithSuffix(name, ` copy ${entropy}`);
}

function currentBackendServerTime(bootstrap: BackendBootstrapState): Date {
  return new Date(bootstrap.response.serverTime.getTime() + Math.max(0, Date.now() - bootstrap.receivedAt));
}

function backendIndexScope(spl: string, bootstrap: BackendBootstrapState): string[] {
  const selectors = indexSelectorsFromSPL(spl);
  const searchableIndexes = bootstrap.response.indexes
    .filter((index) => index.searchable)
    .map((index) => index.name);
  const exactNames = new Set(searchableIndexes);
  for (const selector of selectors) {
    if (selector.includes("*") || selector.includes("?")) {
      throw new Error(
        `Wildcard index selector “${selector}” is not supported by SPL compatibility ${bootstrap.response.splCompatibilityVersion || "v0.1"}. Choose an exact index.`,
      );
    }
    if (!exactNames.has(selector)) {
      const caseMatch = searchableIndexes.find((name) => name.toLowerCase() === selector.toLowerCase());
      throw new Error(
        caseMatch === undefined
          ? `Index “${selector}” is not available in your authorized search scope.`
          : `Index names are case-sensitive. Use “${caseMatch}” instead of “${selector}”.`,
      );
    }
  }
  const selectedApp = bootstrap.response.apps.find((app) => app.appId === bootstrap.response.selectedAppId)
    ?? bootstrap.response.apps[0];
  const appDefaults = selectedApp?.defaultIndexNames.filter((name) => exactNames.has(name)) ?? [];
  const exhaustivelyConstrained = selectors.length > 0 && splIndexScopeIsExhaustive(spl);
  const unconstrainedBaseline = exhaustivelyConstrained
      ? []
      : appDefaults.length > 0
        ? appDefaults
        : searchableIndexes.length <= 128
          ? searchableIndexes
        : (() => {
          throw new Error(
            `This search can match more than the server's 128-index request limit. Select an app with default indexes or constrain every OR branch with exact index= predicates.`,
          );
        })();
  return resolveExactIndexScope({
    spl,
    bootstrap: bootstrap.response,
    // A positive predicate is a filter within the selected app's baseline
    // scope. Union every positive reference so OR and downstream `search`
    // clauses are never silently narrowed, without overflowing the API's
    // bounded scope on installations with a large global catalog.
    requestedIndexes: selectors.length > 0
      ? [...new Set([...unconstrainedBaseline, ...selectors])]
      : appDefaults.length > 0
        ? appDefaults
        : searchableIndexes,
  });
}

function backendMaximumPageSize(bootstrap: BackendBootstrapState): number {
  const advertised = bootstrap.response.limits.maximumPageSize;
  return advertised > 0 ? advertised : DEFAULT_BACKEND_PAGE_SIZE;
}

function normalizedBackendPageSize(requestedPageSize: number, bootstrap: BackendBootstrapState): number {
  return Math.max(1, Math.min(requestedPageSize, backendMaximumPageSize(bootstrap)));
}

function defaultQueryForBootstrap(bootstrap: SystemBootstrapModel): string {
  const searchableByName = new Map(
    bootstrap.indexes
      .filter((index) => index.searchable)
      .map((index) => [index.name.toLowerCase(), index.name]),
  );
  const selectedApp = bootstrap.apps.find((app) => app.appId === bootstrap.selectedAppId);
  const preferred = selectedApp?.defaultIndexNames
    .map((name) => searchableByName.get(name.toLowerCase()))
    .find((name): name is string => name !== undefined);
  const indexName = preferred ?? bootstrap.indexes.find((index) => index.searchable)?.name;
  return indexName === undefined ? "" : `index=${JSON.stringify(indexName)}`;
}

function backendTimeRangeIntent(range: TimeRange, preserveAbsentTimezone: boolean) {
  const explicitTimezone = range.timezone?.trim();
  return {
    earliest: range.earliest,
    latest: range.latest,
    timezone: explicitTimezone
      || (preserveAbsentTimezone
        ? undefined
        : Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC"),
  };
}

function formatResolvedBackendTimeRange(range: ResolvedTimeRange | undefined): string | null {
  if (
    range?.earliest === undefined
    || range.latest === undefined
    || Number.isNaN(range.earliest.valueOf())
    || Number.isNaN(range.latest.valueOf())
  ) {
    return null;
  }
  try {
    const formatter = new Intl.DateTimeFormat("en-US", {
      year: "2-digit",
      month: "numeric",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
      second: "2-digit",
      timeZone: range.timezone || undefined,
    });
    const timezone = range.timezone.trim();
    return `${formatter.format(range.earliest)} – ${formatter.format(range.latest)}${timezone.length > 0 ? ` (${timezone})` : ""}`;
  } catch {
    return `${range.earliest.toLocaleString()} – ${range.latest.toLocaleString()}`;
  }
}

export function SearchWorkspace({ dataMode, apiBaseUrl = "" }: SearchWorkspaceProps) {
  const backendEnabled = dataMode === "backend";
  const initialWorkspaceQuery = backendEnabled ? "" : DEFAULT_QUERY;
  const apiClient = useMemo(() => createOpenSplunkApiClient({ baseUrl: apiBaseUrl }), [apiBaseUrl]);
  const [query, setQuery] = useState(initialWorkspaceQuery);
  const [submittedQuery, setSubmittedQuery] = useState(initialWorkspaceQuery);
  const [defaultSearchQuery, setDefaultSearchQuery] = useState(initialWorkspaceQuery);
  const [backendBootstrapModel, setBackendBootstrapModel] = useState<SystemBootstrapModel | null>(null);
  const [backendConnectionState, setBackendConnectionState] = useState<BackendConnectionState>(
    backendEnabled ? "loading" : "ready",
  );
  const [backendConnectionError, setBackendConnectionError] = useState<string | null>(null);
  const [appSwitchingId, setAppSwitchingId] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<ResultTab>("events");
  const [phase, setPhase] = useState<JobPhase>("completed");
  const [progress, setProgress] = useState(backendEnabled ? 0 : 100);
  const [elapsed, setElapsed] = useState(backendEnabled ? "0.00 s" : "1.82 s");
  const [scannedRows, setScannedRows] = useState(backendEnabled ? 0 : 284_219);
  const [scannedRowsApproximate, setScannedRowsApproximate] = useState(false);
  const [scannedBytes, setScannedBytes] = useState(backendEnabled ? "0 B" : "186 MB");
  const [resolvedTimeRangeLabel, setResolvedTimeRangeLabel] = useState<string | null>(null);
  const [timeRange, setTimeRange] = useState<TimeRange>(TIME_PRESETS[3]);
  const [submittedTimeRange, setSubmittedTimeRange] = useState<TimeRange>(TIME_PRESETS[3]);
  const [draftTimeRange, setDraftTimeRange] = useState<TimeRange>(TIME_PRESETS[3]);
  const [timePickerSection, setTimePickerSection] = useState<TimePickerSection>("presets");
  const [relativeAmount, setRelativeAmount] = useState(24);
  const [relativeUnit, setRelativeUnit] = useState<"m" | "h" | "d">("h");
  const [absoluteStart, setAbsoluteStart] = useState("");
  const [absoluteEnd, setAbsoluteEnd] = useState("");
  const [modal, setModal] = useState<ModalName | null>(null);
  const [menu, setMenu] = useState<MenuName | null>(null);
  const [mobileProductNavOpen, setMobileProductNavOpen] = useState(false);
  const [toast, setToast] = useState<ToastState | null>(null);
  const [fields, setFields] = useState<DemoField[]>(backendEnabled ? [] : DEMO_FIELDS);
  const [backendEvents, setBackendEvents] = useState<DemoEvent[]>([]);
  const [backendStatistics, setBackendStatistics] = useState<WorkspaceStatistic[]>([]);
  const [backendStatisticsTable, setBackendStatisticsTable] = useState<WorkspaceStatisticsTable | null>(null);
  const [backendTimeline, setBackendTimeline] = useState<TimelinePoint[]>([]);
  const [backendEventCount, setBackendEventCount] = useState(0);
  const [backendPrimaryCountLabel, setBackendPrimaryCountLabel] = useState<"events" | "rows">("events");
  const [backendPrimaryCountPrefix, setBackendPrimaryCountPrefix] = useState<"" | "≈" | "≥">("");
  const [backendResultKind, setBackendResultKind] = useState(ResultSetKind.RESULT_SET_KIND_UNSPECIFIED);
  const [backendResultSchema, setBackendResultSchema] = useState<ResultSchema | null>(null);
  const [backendPreviewDisplay, setBackendPreviewDisplay] = useState<BackendPreviewDisplay | null>(null);
  const [backendPreviewStatus, setBackendPreviewStatus] = useState<BackendPreviewStatus>("disabled");
  const [backendPreviewAnnouncement, setBackendPreviewAnnouncement] = useState("");
  const [backendAuthoritativeResultsReady, setBackendAuthoritativeResultsReady] = useState(!backendEnabled);
  const [backendResultTotalRows, setBackendResultTotalRows] = useState<number | null>(null);
  const [backendResultTotalExact, setBackendResultTotalExact] = useState(false);
  const [backendHasNextPage, setBackendHasNextPage] = useState(false);
  const [backendSnapshotComplete, setBackendSnapshotComplete] = useState(true);
  const [backendResultsTruncated, setBackendResultsTruncated] = useState(false);
  const [backendResultsExpired, setBackendResultsExpired] = useState(false);
  const [backendExpiresAt, setBackendExpiresAt] = useState<Date | null>(null);
  const [backendNotices, setBackendNotices] = useState<string[]>([]);
  const [backendResultPageSize, setBackendResultPageSize] = useState(20);
  const [backendFieldsLoading, setBackendFieldsLoading] = useState(false);
  const [backendFieldsLoadingMore, setBackendFieldsLoadingMore] = useState(false);
  const [backendFieldsHasMore, setBackendFieldsHasMore] = useState(false);
  const [backendFieldSummaryLoading, setBackendFieldSummaryLoading] = useState(false);
  const [backendFieldSummaryError, setBackendFieldSummaryError] = useState<string | null>(null);
  const [statisticsDimension, setStatisticsDimension] = useState("level");
  const [activeField, setActiveField] = useState<string | null>(null);
  const [fieldFilter, setFieldFilter] = useState("");
  const [fieldsCollapsed, setFieldsCollapsed] = useState(false);
  const [expandedEvents, setExpandedEvents] = useState<Set<string>>(
    new Set(backendEnabled ? [] : [DEMO_EVENTS[0].id]),
  );
  const [completionOpen, setCompletionOpen] = useState(false);
  const [completionIndex, setCompletionIndex] = useState(0);
  const [editorCaret, setEditorCaret] = useState(initialWorkspaceQuery.length);
  const [editorFocused, setEditorFocused] = useState(false);
  const [searchMode, setSearchMode] = useState<SearchMode>("Smart");
  const [timelineStart, setTimelineStart] = useState<number | null>(null);
  const [timelineEnd, setTimelineEnd] = useState<number | null>(null);
  const [draggingTimeline, setDraggingTimeline] = useState(false);
  const [savedSearches, setSavedSearches] = useState<DemoSavedSearch[]>(backendEnabled ? [] : DEMO_SAVED_SEARCHES);
  const [history, setHistory] = useState<DemoHistoryEntry[]>(backendEnabled ? [] : DEMO_HISTORY);
  const [savedSearchesLoading, setSavedSearchesLoading] = useState(backendEnabled);
  const [historyLoading, setHistoryLoading] = useState(backendEnabled);
  const [savedSearchesLoadingMore, setSavedSearchesLoadingMore] = useState(false);
  const [historyLoadingMore, setHistoryLoadingMore] = useState(false);
  const [savedSearchesNextPageToken, setSavedSearchesNextPageToken] = useState<string | null>(null);
  const [historyNextPageToken, setHistoryNextPageToken] = useState<string | null>(null);
  const [savedSearchesAvailable, setSavedSearchesAvailable] = useState(!backendEnabled);
  const [historyAvailable, setHistoryAvailable] = useState(!backendEnabled);
  const [objectMutation, setObjectMutation] = useState<BackendObjectMutation | null>(null);
  const [historyClearBusy, setHistoryClearBusy] = useState(false);
  const [savedSearchDeleteError, setSavedSearchDeleteError] = useState<string | null>(null);
  const [savedSearchDuplicateError, setSavedSearchDuplicateError] = useState<string | null>(null);
  const [savedSearchRenameError, setSavedSearchRenameError] = useState<string | null>(null);
  const [savedSearchDeleteTargetId, setSavedSearchDeleteTargetId] = useState<string | null>(null);
  const [savedSearchRenameTargetId, setSavedSearchRenameTargetId] = useState<string | null>(null);
  const [savedSearchRenameName, setSavedSearchRenameName] = useState("");
  const [historyDeleteError, setHistoryDeleteError] = useState<string | null>(null);
  const [historyClearError, setHistoryClearError] = useState<string | null>(null);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [cancelError, setCancelError] = useState<string | null>(null);
  const [savedSearchFilter, setSavedSearchFilter] = useState("");
  const [historyFilter, setHistoryFilter] = useState("");
  const [persistedLaunchPending, setPersistedLaunchPending] = useState(false);
  const [saveName, setSaveName] = useState("Production log investigation");
  const [saveDescription, setSaveDescription] = useState("");
  const [saveAsNew, setSaveAsNew] = useState(false);
  const [activeSavedSearchId, setActiveSavedSearchId] = useState<string | null>(null);
  const [savedWorkspaceBaseline, setSavedWorkspaceBaseline] = useState<SavedWorkspaceBaseline | null>(null);
  const [savedBaselineCaptureId, setSavedBaselineCaptureId] = useState<string | null>(null);
  const activeSavedSearchIdRef = useRef<string | null>(null);
  const [exportFormat, setExportFormat] = useState<"csv" | "jsonl">("csv");
  const [exportStage, setExportStage] = useState<ExportStage>("configure");
  const [exportError, setExportError] = useState<string | null>(null);
  const [serverExportJob, setServerExportJob] = useState<ExportJob | null>(null);
  const [exportDownloadState, setExportDownloadState] = useState<DialogActionState>({ status: "idle" });
  const [exportCancelState, setExportCancelState] = useState<DialogActionState>({ status: "idle" });
  const [exportRequestId, setExportRequestId] = useState<string | null>(null);
  const [exportRetryable, setExportRetryable] = useState(true);
  const [demoExportSize, setDemoExportSize] = useState(0);
  const [exportClockTick, setExportClockTick] = useState(0);
  const [exportFields, setExportFields] = useState<string[]>(EVENT_EXPORT_FIELDS);
  const [exportSourceTab, setExportSourceTab] = useState<ResultTab>("events");
  const [chartStyle, setChartStyle] = useState<ChartStyle>("column");
  const [chartTitle, setChartTitle] = useState("Event volume by level");
  const [legendPosition, setLegendPosition] = useState<"bottom" | "right" | "none">("bottom");
  const [showDataLabels, setShowDataLabels] = useState(true);
  const [statsDensity, setStatsDensity] = useState<StatsDensity>("compact");
  const [patternSensitivity, setPatternSensitivity] = useState<PatternSensitivity>("Balanced");
  const [eventDisplay, setEventDisplay] = useState<EventDisplay>("List");
  const [eventPageSize, setEventPageSize] = useState(20);
  const [eventPage, setEventPage] = useState(1);
  const [eventSortDirection, setEventSortDirection] = useState<"asc" | "desc">("desc");
  const [timelineDisplay, setTimelineDisplay] = useState<TimelineDisplay>("Columns");
  const [wrapEvents, setWrapEvents] = useState(true);
  const [statsSort, setStatsSort] = useState<{ key: keyof WorkspaceStatistic; direction: "asc" | "desc" }>({
    key: "count",
    direction: "desc",
  });
  const [genericStatsSort, setGenericStatsSort] = useState<WorkspaceStatisticsSort | null>(null);
  const [timechartSort, setTimechartSort] = useState<{ key: "time" | "count"; direction: "asc" | "desc" }>({ key: "time", direction: "asc" });
  const [showAllFields, setShowAllFields] = useState(false);
  const [globalFind, setGlobalFind] = useState("");
  const timersRef = useRef<number[]>([]);
  const generationRef = useRef(0);
  const searchLaunchRef = useRef(false);
  const backendAbortRef = useRef<AbortController | null>(null);
  const backendPageAbortRef = useRef<AbortController | null>(null);
  const backendMetadataAbortRef = useRef<AbortController | null>(null);
  const backendFieldCatalogAbortRef = useRef<AbortController | null>(null);
  const backendFieldCatalogNextPageTokenRef = useRef<string | null>(null);
  const backendFieldCatalogPageTokensRef = useRef<Set<string>>(new Set());
  const backendJobIdRef = useRef<string | null>(null);
  const backendJobRef = useRef<SearchJob | null>(null);
  const backendJobVersionRef = useRef(0n);
  const backendSocketRef = useRef<SearchWebSocketClient | null>(null);
  const backendPreviewRef = useRef<LivePreviewSnapshot | null>(null);
  const backendPreviewSchemasRef = useRef<Map<string, ResultSchema>>(new Map());
  const backendPreviewRowLimitRef = useRef(0);
  const backendPreviewStatusRef = useRef<BackendPreviewStatus>("disabled");
  const backendCancelPendingRef = useRef(false);
  const backendCancelRequestedRef = useRef(false);
  const backendBootstrapRef = useRef<BackendBootstrapState | null>(null);
  const backendBootstrapPromiseRef = useRef<Promise<BackendBootstrapState> | null>(null);
  const appSwitchAbortRef = useRef<AbortController | null>(null);
  const appSwitchEpochRef = useRef(0);
  const backendObjectMutationRef = useRef(false);
  const backendResultPagesRef = useRef<Map<string, BackendResultPage>>(new Map());
  const backendPageTokensRef = useRef<Map<string, string | undefined>>(new Map());
  const backendPageStartsRef = useRef<Map<string, number>>(new Map());
  const backendResultPageTokensSeenRef = useRef<Set<string>>(new Set());
  const backendPageSizeRef = useRef(20);
  const backendAuthoritativeFieldsRef = useRef(false);
  const backendAuthoritativeTimelineRef = useRef(false);
  const backendFieldSummaryAbortRef = useRef<AbortController | null>(null);
  const backendFieldSummaryCacheRef = useRef<Map<string, string | null>>(new Map());
  const backendObjectAbortRef = useRef<AbortController | null>(null);
  const backendExportAbortRef = useRef<AbortController | null>(null);
  const backendExportDownloadAbortRef = useRef<AbortController | null>(null);
  const backendExportCancelAbortRef = useRef<AbortController | null>(null);
  const serverExportJobRef = useRef<ExportJob | null>(null);
  const exportEpochRef = useRef(0);
  const backendSavedSearchesRef = useRef<Map<string, ServerSavedSearch>>(new Map());
  const backendHistoryRef = useRef<Map<string, ServerSearchHistoryEntry>>(new Map());
  const backendHistoryRerunRef = useRef<ServerSearchHistoryEntry | null>(null);
  const pendingSavedSelectedFieldsRef = useRef<Set<string> | null>(null);
  const pendingSavedPreferredTabRef = useRef<ResultTab | null>(null);
  const pendingSavedVisualizationRef = useRef<VisualizationSpec | undefined>(undefined);
  // Unsupported server visualization types must survive an unrelated quick
  // save. They are replaced only after the user explicitly edits chart settings.
  const preservedSavedVisualizationRef = useRef<VisualizationSpec | null>(null);
  const visualizationEditedRef = useRef(false);
  const backendSavedSearchRefreshEpochRef = useRef(0);
  const backendHistoryRefreshEpochRef = useRef(0);
  const backendSavedSearchLoadMoreRef = useRef(false);
  const backendHistoryLoadMoreRef = useRef(false);
  const backendSavedSearchLoadMoreEpochRef = useRef(0);
  const backendHistoryLoadMoreEpochRef = useRef(0);
  const backendSavedSearchPageTokensRef = useRef<Set<string>>(new Set());
  const backendHistoryPageTokensRef = useRef<Set<string>>(new Set());
  const urlLaunchAppliedRef = useRef(false);
  const persistedLaunchEpochRef = useRef(0);
  const persistedLaunchPendingRef = useRef(false);
  const searchRunnerRef = useRef<(queryText: string, range: TimeRange) => void>(() => undefined);
  const timelineZoomParentRef = useRef<TimeRange | null>(null);
  const editorRef = useRef<HTMLTextAreaElement>(null);
  const highlightRef = useRef<HTMLPreElement>(null);
  const gutterLinesRef = useRef<HTMLDivElement>(null);
  const timePickerRef = useRef<HTMLDivElement>(null);
  const timeRangeButtonRef = useRef<HTMLButtonElement>(null);
  const mobileProductTriggerRef = useRef<HTMLButtonElement>(null);
  const mobileProductDrawerRef = useRef<HTMLDialogElement>(null);
  const saveAsButtonRef = useRef<HTMLButtonElement>(null);
  const saveDialogReturnFocusRef = useRef<HTMLElement | null>(null);
  const menuReturnFocusRef = useRef<HTMLElement | null>(null);

  const isRunning = ACTIVE_PHASES.has(phase);
  const searchIsClosed = submittedQuery.trim().length === 0;
  const backendHasNoSearchableIndexes = backendEnabled
    && backendBootstrapModel !== null
    && !backendBootstrapModel.indexes.some((index) => index.searchable);
  const backendPreviewFeatureSupported = backendEnabled
    && backendBootstrapModel !== null
    && backendBootstrapModel.searchWebsocketPath !== null
    && backendBootstrapModel.limits.maximumPreviewRows > 0
    && supportsServerFeature(
      backendBootstrapModel,
      ServerFeature.SERVER_FEATURE_SEARCH_PREVIEW,
    );
  const runDisabledReason = !isRunning && backendEnabled && backendConnectionState === "loading"
    ? "Search is disabled while the backend connection is loading."
    : !isRunning && backendEnabled && backendConnectionState === "error"
      ? "Retry the backend connection before running a search."
      : !isRunning && backendHasNoSearchableIndexes
        ? "No searchable indexes are available in the current backend scope."
        : !isRunning && query.trim().length === 0
          ? "Enter an SPL search before running."
          : null;
  const backendDisplayingPreview = backendEnabled
    && !backendAuthoritativeResultsReady
    && backendPreviewDisplay !== null;
  const backendPreviewHasRows = backendDisplayingPreview
    && backendPreviewDisplay.snapshot.rows.length > 0;
  const hasResultData = !searchIsClosed
    && (backendEnabled
      ? !backendResultsExpired
        && (backendAuthoritativeResultsReady || backendPreviewHasRows)
        && phase !== "failed"
        && phase !== "canceled"
        && phase !== "expired"
      : phase !== "failed" && phase !== "canceled");
  const diagnostic = useMemo(() => query.trim().length === 0 ? null : getQueryDiagnostic(query), [query]);
  const editorDiagnostic = useMemo(() => {
    if (!backendEnabled || diagnostic?.kind !== "unsupported") return diagnostic;
    const compatibility = backendBootstrapModel?.splCompatibilityVersion;
    return {
      ...diagnostic,
      message: `Command “${diagnostic.token}” is newer than this editor's local command catalog.`,
      suggestion: `Run the search to let the connected server${compatibility ? ` (${compatibility})` : ""} validate it.`,
      actionLabel: undefined,
      removeStart: undefined,
      removeEnd: undefined,
    };
  }, [backendBootstrapModel?.splCompatibilityVersion, backendEnabled, diagnostic]);
  const completionContext = useMemo(() => completionContextAt(query, editorCaret), [editorCaret, query]);
  const filteredCompletions = useMemo(() => {
    const prefix = completionContext?.prefix.toLowerCase() ?? "";
    return prefix.length === 0
      ? COMPLETIONS
      : COMPLETIONS.filter((completion) => completion.label.startsWith(prefix));
  }, [completionContext]);
  const displayedBackendResults = backendDisplayingPreview
    ? backendPreviewDisplay.adapted
    : null;
  const resultEvents = useMemo(
    () => searchIsClosed
      ? []
      : backendEnabled
        ? displayedBackendResults?.events ?? backendEvents
        : filteredDemoEvents(submittedQuery),
    [backendEnabled, backendEvents, displayedBackendResults?.events, searchIsClosed, submittedQuery],
  );
  const timelinePoints = useMemo(() => backendEnabled
    ? backendDisplayingPreview
      ? backendResultKind === ResultSetKind.RESULT_SET_KIND_TIME_SERIES
        ? displayedBackendResults?.timeline ?? []
        : []
      : backendTimeline
    : DEMO_TIMELINE, [
    backendDisplayingPreview,
    backendEnabled,
    backendResultKind,
    backendTimeline,
    displayedBackendResults?.timeline,
  ]);
  const timechartValueColumns = useMemo(() => timechartValueFields(timelinePoints), [timelinePoints]);
  const statisticsRows: WorkspaceStatistic[] = backendEnabled
    ? displayedBackendResults?.statistics ?? backendStatistics
    : DEMO_STATISTICS;
  const isTimechartResult = backendEnabled
    ? backendResultKind === ResultSetKind.RESULT_SET_KIND_TIME_SERIES
    : hasPipelineCommand(submittedQuery, "timechart");
  const genericStatisticsTable = backendEnabled && !isTimechartResult
    ? displayedBackendResults?.statisticsTable ?? backendStatisticsTable
    : null;
  const statisticsRowCount = backendEnabled
    && backendDisplayingPreview
    ? backendPreviewDisplay.snapshot.rows.length
    : backendEnabled
    && backendResultKind !== ResultSetKind.RESULT_SET_KIND_EVENTS
    && backendResultTotalRows !== null
    ? backendResultTotalRows
    : genericStatisticsTable?.rows.length ?? (isTimechartResult ? timelinePoints.length : statisticsRows.length);
  const backendStatisticsPageStart = (() => {
    if (!backendEnabled) return 1;
    if (backendDisplayingPreview) return 1;
    return backendResultPagesRef.current.has(`${backendResultPageSize}:${eventPage}`)
      ? backendPageStartsRef.current.get(`${backendResultPageSize}:${eventPage}`) ?? null
      : null;
  })();
  const baseEventCount = backendEnabled ? backendEventCount : eventCountForQuery(submittedQuery);
  const timelineSelection = useMemo(() => {
    if (timelineStart === null || timelineEnd === null) return null;
    return [Math.min(timelineStart, timelineEnd), Math.max(timelineStart, timelineEnd)] as const;
  }, [timelineEnd, timelineStart]);
  const timelineSelectionZoomable = useMemo(() => {
    if (timelineSelection === null) return false;
    const first = timelinePoints[timelineSelection[0]];
    const last = timelinePoints[timelineSelection[1]];
    const next = timelinePoints[timelineSelection[1] + 1];
    return first?.earliest !== undefined
      && (last?.latest !== undefined || next?.earliest !== undefined);
  }, [timelinePoints, timelineSelection]);
  const selectedTimelineCount = useMemo(() => {
    if (timelineSelection === null) return null;
    const points = timelinePoints.slice(timelineSelection[0], timelineSelection[1] + 1);
    let exactTotal = 0n;
    let projectedTotal = 0;
    let everyCountIsExactInteger = true;
    let hasApproximateCoordinate = false;
    for (const point of points) {
      projectedTotal += point.count;
      hasApproximateCoordinate ||= point.coordinateApproximate === true;
      const exactText = point.exactCount;
      if (exactText !== undefined && /^[+-]?\d+$/.test(exactText)) {
        exactTotal += BigInt(exactText);
      } else if (Number.isSafeInteger(point.count)) {
        exactTotal += BigInt(point.count);
      } else {
        everyCountIsExactInteger = false;
      }
    }
    if (!everyCountIsExactInteger) {
      return {
        count: Math.max(0, Math.min(Number.MAX_SAFE_INTEGER, projectedTotal)),
        approximate: true,
      };
    }
    const overflow = exactTotal > BigInt(Number.MAX_SAFE_INTEGER);
    return {
      count: overflow ? Number.MAX_SAFE_INTEGER : Math.max(0, Number(exactTotal)),
      approximate: overflow || hasApproximateCoordinate,
    };
  }, [timelinePoints, timelineSelection]);
  const visibleEventCount = searchIsClosed
    || phase === "failed"
    || phase === "canceled"
    || phase === "expired"
    || (backendEnabled && backendResultsExpired)
    ? 0
    : selectedTimelineCount === null
      ? baseEventCount
      : Math.min(baseEventCount, selectedTimelineCount.count);
  const visibleCountPrefix = backendEnabled
    ? selectedTimelineCount === null
      ? backendPrimaryCountPrefix
      : selectedTimelineCount.approximate ? "≈" : ""
    : "";
  const pageableEventCount = backendEnabled
    ? backendResultTotalRows ?? resultEvents.length + (backendHasNextPage ? backendResultPageSize : 0)
    : visibleEventCount;
  const currentResultPageSize = backendEnabled ? backendResultPageSize : eventPageSize;
  const eventPageCount = Math.max(
    1,
    backendEnabled
      ? eventPage + (backendHasNextPage ? 1 : 0)
      : Math.ceil(pageableEventCount / currentResultPageSize),
  );
  const pagedResultEvents = useMemo(() => {
    if (resultEvents.length === 0) return [];
    if (backendEnabled) return resultEvents;
    const ordered = eventSortDirection === "desc" ? resultEvents : resultEvents.toReversed();
    const offset = ((eventPage - 1) * eventPageSize) % ordered.length;
    const rotated = [...ordered.slice(offset), ...ordered.slice(0, offset)];
    return rotated.slice(0, Math.min(eventPageSize, rotated.length));
  }, [backendEnabled, eventPage, eventPageSize, eventSortDirection, resultEvents]);
  const dirty = query !== submittedQuery
    || timeRange.earliest !== submittedTimeRange.earliest
    || timeRange.latest !== submittedTimeRange.latest
    || timeRange.timezone !== submittedTimeRange.timezone;
  const currentSavedWorkspace = useMemo<SavedWorkspaceBaseline | null>(() => {
    if (activeSavedSearchId === null) return null;
    const selectedFields = pendingSavedSelectedFieldsRef.current === null
      ? fields.filter((field) => field.selected).map((field) => field.name)
      : [...pendingSavedSelectedFieldsRef.current];
    return {
      savedSearchId: activeSavedSearchId,
      activeTab,
      chartStyle: isTimechartResult && timechartValueColumns.length > 1 ? "line" : chartStyle,
      chartTitle,
      earliest: timeRange.earliest,
      latest: timeRange.latest,
      legendVisible: legendPosition !== "none",
      query,
      selectedFields,
      showDataLabels,
      timeZone: timeRange.timezone ?? "",
    };
  }, [
    activeSavedSearchId,
    activeTab,
    chartStyle,
    chartTitle,
    fields,
    isTimechartResult,
    legendPosition,
    query,
    showDataLabels,
    timeRange.earliest,
    timeRange.latest,
    timeRange.timezone,
    timechartValueColumns.length,
  ]);
  useEffect(() => {
    if (isTimechartResult && timechartValueColumns.length > 1 && chartStyle !== "line") {
      setChartStyle("line");
      if (!visualizationEditedRef.current) {
        setSavedWorkspaceBaseline((current) => current === null
          ? null
          : { ...current, chartStyle: "line" });
      }
    }
  }, [chartStyle, isTimechartResult, timechartValueColumns.length]);
  useEffect(() => {
    if (
      savedBaselineCaptureId === null
      || currentSavedWorkspace?.savedSearchId !== savedBaselineCaptureId
    ) return;
    setSavedWorkspaceBaseline(currentSavedWorkspace);
    setSavedBaselineCaptureId(null);
  }, [currentSavedWorkspace, savedBaselineCaptureId]);
  const savedDefinitionDirty = currentSavedWorkspace !== null
    && savedWorkspaceBaseline?.savedSearchId === currentSavedWorkspace.savedSearchId
    && savedBaselineCaptureId === null
    && savedWorkspaceFingerprint(currentSavedWorkspace) !== savedWorkspaceFingerprint(savedWorkspaceBaseline);
  const editorLineCount = Math.max(2, query.split("\n").length);
  const absoluteTimeInvalid = absoluteStart.trim().length === 0
    || absoluteEnd.trim().length === 0
    || absoluteStart >= absoluteEnd;
  const sortedStatistics = useMemo(() => {
    const rows = [...statisticsRows];
    rows.sort((left, right) => {
      const leftValue = left[statsSort.key];
      const rightValue = right[statsSort.key];
      const leftNumeric = statsSort.key === "percent" ? Number.parseFloat(String(leftValue)) : leftValue;
      const rightNumeric = statsSort.key === "percent" ? Number.parseFloat(String(rightValue)) : rightValue;
      const result = typeof leftNumeric === "number" && typeof rightNumeric === "number"
        ? leftNumeric - rightNumeric
        : String(leftNumeric).localeCompare(String(rightNumeric));
      return statsSort.direction === "asc" ? result : -result;
    });
    return rows;
  }, [statisticsRows, statsSort]);
  const sortedGenericStatisticsRows = useMemo(() => {
    if (genericStatisticsTable === null || genericStatsSort === null) return genericStatisticsTable?.rows ?? [];
    const column = genericStatisticsTable.columns.find((candidate) => candidate.key === genericStatsSort.key);
    if (column === undefined) return genericStatisticsTable.rows;
    return genericStatisticsTable.rows.toSorted((left, right) => {
      const leftValue = left.values[column.key] ?? null;
      const rightValue = right.values[column.key] ?? null;
      if (leftValue === null) return rightValue === null ? 0 : 1;
      if (rightValue === null) return -1;
      const comparison = compareWorkspaceStatisticValues(leftValue, rightValue, column);
      return genericStatsSort.direction === "asc" ? comparison : -comparison;
    });
  }, [genericStatisticsTable, genericStatsSort]);

  useEffect(() => {
    activeSavedSearchIdRef.current = activeSavedSearchId;
  }, [activeSavedSearchId]);
  const exportFieldLabels = useMemo(() => ({
    ...EXPORT_FIELD_LABELS,
    ...Object.fromEntries((backendResultSchema?.columns ?? []).map((column) => [
      column.fieldName,
      column.displayName || column.fieldName,
    ])),
    ...Object.fromEntries((genericStatisticsTable?.columns ?? []).map((column) => [column.key, column.label])),
    ...Object.fromEntries(timechartValueColumns.map((series) => [series, series])),
  }), [backendResultSchema, genericStatisticsTable, timechartValueColumns]);
  const exportDialogState: ExportDialogState = (() => {
    const bootstrap = backendBootstrapRef.current?.response;
    const feature = exportFormat === "csv"
      ? ServerFeature.SERVER_FEATURE_EXPORT_CSV
      : ServerFeature.SERVER_FEATURE_EXPORT_JSON_LINES;
    const featureSupported = !backendEnabled
      || (bootstrap !== undefined && supportsServerFeature(bootstrap, feature));
    const resultMatchesSource = backendResultKind === ResultSetKind.RESULT_SET_KIND_EVENTS
      ? exportSourceTab === "events"
      : exportSourceTab === "statistics" || exportSourceTab === "visualization";
    const jobReady = phase === "completed"
      && backendJobRef.current !== null
      && backendAuthoritativeResultsReady
      && !backendResultsExpired
      && exportSourceTab !== "patterns"
      && resultMatchesSource;
    const common = {
      description: backendEnabled
        ? "Artifacts are produced by the connected server from this completed job."
        : "Creates a local file from the displayed demo rows.",
      sourceTab: exportSourceTab,
      format: exportFormat,
      maximumRows: backendEnabled
        ? bootstrap?.limits.maximumExportRows || null
        : displayedRowsForTab(exportSourceTab),
      maximumBytes: backendEnabled ? bootstrap?.limits.maximumExportBytes || null : null,
    } as const;
    if (exportStage === "configure") {
      const available = !backendEnabled || (featureSupported && jobReady);
      return {
        ...common,
        status: "configure",
        available,
        unavailableReason: available
          ? null
          : !featureSupported
            ? `The server does not advertise ${exportFormat === "csv" ? "CSV" : "JSON Lines"} exports.`
            : exportSourceTab === "patterns"
              ? "Patterns are derived in this browser and are not a server export result."
              : !resultMatchesSource
                ? "This browser-derived view does not match the authoritative server result."
                : backendResultsExpired
                  ? "These retained search results have expired. Run the search again before exporting."
                  : "Complete a backend search before creating an export.",
      };
    }
    const requestId = exportRequestId ?? "local-export";
    if (exportStage === "pending") {
      return {
        ...common,
        status: "pending",
        requestId,
        exportJobId: serverExportJob?.exportJobId ?? null,
        rowsWritten: serverExportJob?.progress?.rowsWritten ?? null,
        bytesWritten: serverExportJob?.progress?.bytesWritten ?? null,
        percentComplete: serverExportJob?.progress?.percentComplete ?? null,
        cancel: exportCancelState,
      };
    }
    if (exportStage === "error") {
      return {
        ...common,
        status: "error",
        requestId: exportRequestId,
        exportJobId: serverExportJob?.exportJobId ?? null,
        error: exportError ?? "The export could not be completed.",
        retryable: exportRetryable,
      };
    }
    const artifact = serverExportJob?.artifact;
    const exportJobId = serverExportJob?.exportJobId ?? "local";
    return {
      ...common,
      status: "ready",
      requestId,
      exportJobId,
      artifact: backendEnabled && artifact !== undefined
        ? {
          downloadHandle: `server:${requestId}:${exportJobId}`,
          fileName: artifact.fileName,
          mediaType: artifact.mediaType,
          rowCount: artifact.rowCount,
          sizeBytes: artifact.sizeBytes,
          expiresAt: artifact.expiresAt ?? null,
        }
        : {
          downloadHandle: `demo:${requestId}`,
          fileName: `open-splunk-${exportSourceTab}.${exportFormat}`,
          mediaType: exportFormat === "csv" ? "text/csv" : "application/x-ndjson",
          rowCount: displayedRowsForTab(exportSourceTab),
          sizeBytes: demoExportSize,
          expiresAt: null,
        },
      download: exportDownloadState,
    };
  })();
  const searchSettingsCapabilities: SearchSettingsCapabilities = backendEnabled
    ? {
      context: "Capabilities are determined by the connected server for this workspace.",
      fieldDiscovery: {
        supported: backendBootstrapRef.current?.response.features.has(
          ServerFeature.SERVER_FEATURE_FIELD_DISCOVERY,
        ) ?? false,
        enabled: backendAuthoritativeFieldsRef.current,
        configurable: false,
        detail: "Field profiles are fetched on demand after an Events search completes.",
        update: { status: "idle" },
      },
      liveResultPreviews: {
        supported: backendPreviewFeatureSupported,
        enabled: backendPreviewFeatureSupported,
        configurable: false,
        detail: backendPreviewFeatureSupported
          ? `The server streams up to ${NUMBER_FORMAT.format(backendBootstrapModel?.limits.maximumPreviewRows ?? 0)} provisional rows while a search runs. Completed results are always replaced from the authoritative REST snapshot.`
          : "This server does not advertise bounded live result previews. Job progress remains live and completed results load over REST.",
        update: { status: "idle" },
      },
      eventSampling: {
        supported: false,
        enabled: false,
        configurable: false,
        detail: "No sampling option is sent. Server truncation and completeness are reported separately.",
        update: { status: "idle" },
      },
    }
    : {
      context: "Demo behavior is fixed so the preview remains deterministic.",
      fieldDiscovery: {
        supported: true,
        enabled: true,
        configurable: false,
        detail: "The bundled demo field catalog is always available.",
        update: { status: "idle" },
      },
      liveResultPreviews: {
        supported: false,
        enabled: false,
        configurable: false,
        detail: "Demo results appear after the simulated job completes.",
        update: { status: "idle" },
      },
      eventSampling: {
        supported: false,
        enabled: false,
        configurable: false,
        detail: "The bundled demo result set is fixed and is not sampled.",
        update: { status: "idle" },
      },
    };
  const sortedTimechartRows = useMemo(() => {
    const rows = [...timelinePoints];
    if (timechartSort.key === "count") rows.sort((left, right) => left.count - right.count);
    return timechartSort.direction === "desc" ? rows.toReversed() : rows;
  }, [timelinePoints, timechartSort]);
  const patternRows = useMemo(() => {
    if (backendEnabled) return patternsFromEvents(resultEvents, baseEventCount, patternSensitivity);
    if (patternSensitivity === "Precise") {
      return [
        { signature: "Request metrics status=200 duration_ms=*", count: 4932, percent: 38.4 },
        { signature: "Request metrics status=201 duration_ms=*", count: 1349, percent: 10.5 },
        ...DEMO_PATTERNS.slice(1),
      ];
    }
    if (patternSensitivity === "Broad") {
      return [
        { signature: "Request *", count: 7889, percent: 61.4 },
        { signature: "Submission *", count: 2174, percent: 16.9 },
        { signature: "* failed while *", count: 1203, percent: 9.3 },
      ];
    }
    return DEMO_PATTERNS;
  }, [backendEnabled, baseEventCount, patternSensitivity, resultEvents]);
  const backendRuntimeNotices = useMemo(() => {
    if (!backendEnabled) return [];
    return uniqueMessages([
      ...backendNotices,
      backendResultsTruncated
        ? "The server retained a bounded result snapshot; the full search may contain additional rows."
        : undefined,
      !backendSnapshotComplete
        ? "This retained result snapshot is incomplete. Counts and page totals are lower bounds."
        : undefined,
      backendResultTotalRows !== null && !backendResultTotalExact
        ? `The server reports at least ${NUMBER_FORMAT.format(backendResultTotalRows)} retained rows.`
        : undefined,
      backendResultKind !== ResultSetKind.RESULT_SET_KIND_EVENTS
        && (backendHasNextPage || eventPage > 1)
        ? `Statistics and visualization show server page ${NUMBER_FORMAT.format(eventPage)}; add an SPL sort for global ordering.`
        : undefined,
      backendExpiresAt === null
        ? undefined
        : `Results are retained until ${backendExpiresAt.toLocaleString()}.`,
    ]);
  }, [
    backendEnabled,
    backendExpiresAt,
    backendNotices,
    backendHasNextPage,
    backendResultKind,
    backendResultTotalExact,
    backendResultTotalRows,
    backendResultsTruncated,
    backendSnapshotComplete,
    eventPage,
  ]);
  const backendPreviewStatusPresentation = (() => {
    const rowCount = backendPreviewDisplay?.snapshot.rows.length ?? 0;
    const bounded = backendPreviewDisplay?.snapshot.truncated === true;
    switch (backendPreviewStatus) {
      case "waiting":
        return {
          title: "Live preview",
          detail: "Waiting for the first displayable result rows.",
          tone: "active",
        };
      case "live":
        return {
          title: "Live preview",
          detail: bounded
            ? `Showing the first ${NUMBER_FORMAT.format(rowCount)} provisional rows. Values and order may change until the search completes.`
            : `${NUMBER_FORMAT.format(rowCount)} provisional rows shown. Values and order may change until the search completes.`,
          tone: "active",
        };
      case "paused":
        return {
          title: "Preview paused",
          detail: `Reconnecting to live updates. The last ${NUMBER_FORMAT.format(rowCount)} provisional rows remain visible.`,
          tone: "paused",
        };
      case "resyncing":
        return {
          title: "Resynchronizing preview",
          detail: "Provisional rows were cleared to avoid displaying a discontinuous result set.",
          tone: "paused",
        };
      case "limited":
        return {
          title: "Live preview is bounded",
          detail: "A complete row did not fit within the negotiated preview limit. Authoritative results will appear when the search completes.",
          tone: "paused",
        };
      case "finalizing":
        return {
          title: "Finalizing results",
          detail: rowCount > 0
            ? `${NUMBER_FORMAT.format(rowCount)} preview rows remain provisional while the authoritative snapshot loads.`
            : "Loading the authoritative result snapshot.",
          tone: "active",
        };
      case "finalization-error":
        return {
          title: "Preview only",
          detail: "The search completed, but authoritative results could not be loaded. These provisional rows cannot be paged or exported.",
          tone: "warning",
        };
      case "disabled":
        return null;
    }
  })();

  useEffect(() => {
    const behavior = window.matchMedia("(prefers-reduced-motion: reduce)").matches ? "auto" : "smooth";
    document.getElementById(`tab-${activeTab}`)?.scrollIntoView({ behavior, block: "nearest", inline: "center" });
  }, [activeTab]);

  useEffect(() => {
    if (window.matchMedia("(max-width: 760px)").matches) setFieldsCollapsed(true);
  }, []);

  useEffect(() => {
    if (!mobileProductNavOpen) return;
    const drawer = mobileProductDrawerRef.current;
    if (drawer === null) return;
    return installModalSurface({
      container: drawer,
      excludedSiblingClassNames: ["search-mobile-backdrop"],
      onEscape: () => setMobileProductNavOpen(false),
      returnFocus: mobileProductTriggerRef.current,
    });
  }, [mobileProductNavOpen]);

  async function ensureBackendBootstrap(): Promise<BackendBootstrapState> {
    const existing = backendBootstrapRef.current;
    if (existing !== null) return existing;
    const pending = backendBootstrapPromiseRef.current;
    if (pending !== null) return pending;

    setBackendConnectionState("loading");
    setBackendConnectionError(null);
    const request = getSystemBootstrap(apiClient)
      .then((response) => {
        if (!supportsServerFeature(response, ServerFeature.SERVER_FEATURE_SEARCH)) {
          throw new Error("This server does not advertise browser search support.");
        }
        const bootstrap = { response, receivedAt: Date.now() };
        backendBootstrapRef.current = bootstrap;
        setBackendBootstrapModel(response);
        setBackendConnectionState("ready");
        setBackendConnectionError(null);
        return bootstrap;
      })
      .catch((error: unknown) => {
        const message = error instanceof Error
          ? error.message
          : "The system bootstrap request failed.";
        setBackendConnectionState("error");
        setBackendConnectionError(message);
        throw error;
      })
      .finally(() => {
        backendBootstrapPromiseRef.current = null;
      });
    backendBootstrapPromiseRef.current = request;
    return request;
  }

  async function retryBackendConnection() {
    if (!backendEnabled || backendConnectionState === "loading") return;
    setPhase("completed");
    setProgress(0);
    try {
      const bootstrap = await ensureBackendBootstrap();
      const authorizedQuery = defaultQueryForBootstrap(bootstrap.response);
      setDefaultSearchQuery(authorizedQuery);
      if (query.trim().length === 0) {
        setQuery(authorizedQuery);
        setEditorCaret(authorizedQuery.length);
      }
      setSavedSearchesLoading(true);
      setHistoryLoading(true);
      void Promise.all([
        refreshBackendSavedSearches(bootstrap),
        refreshBackendHistory(bootstrap),
      ]);
      showToast(
        authorizedQuery.length > 0
          ? "Backend connection restored. Review the authorized search and run it when ready."
          : "Backend connection restored, but this app has no searchable indexes.",
        authorizedQuery.length > 0 ? "success" : "warning",
      );
      window.requestAnimationFrame(() => editorRef.current?.focus());
    } catch (error) {
      setPhase("failed");
      setProgress(100);
      showToast(
        error instanceof Error ? error.message : "Unable to restore the backend connection.",
        "warning",
      );
    }
  }

  async function ensurePersistedLaunchAppContext(
    current: BackendBootstrapState,
    persistedAppId: string | undefined,
    signal: AbortSignal,
  ): Promise<BackendBootstrapState> {
    const requestedAppId = persistedAppId?.trim();
    if (
      requestedAppId === undefined
      || requestedAppId.length === 0
      || current.response.selectedAppId === requestedAppId
    ) {
      return current;
    }
    const response = await getSystemBootstrap(apiClient, requestedAppId, { signal });
    if (!supportsServerFeature(response, ServerFeature.SERVER_FEATURE_SEARCH)) {
      throw new Error("The persisted search belongs to an app that does not expose browser search.");
    }
    if (response.selectedAppId !== requestedAppId) {
      throw new Error("The persisted search belongs to an app that is not available in this backend session.");
    }
    const bootstrap = { response, receivedAt: Date.now() };
    backendBootstrapRef.current = bootstrap;
    backendBootstrapPromiseRef.current = null;
    setBackendBootstrapModel(response);
    setBackendConnectionState("ready");
    setBackendConnectionError(null);
    setDefaultSearchQuery(defaultQueryForBootstrap(response));
    activeSavedSearchIdRef.current = null;
    setActiveSavedSearchId(null);
    backendHistoryRerunRef.current = null;
    backendSavedSearchesRef.current.clear();
    backendHistoryRef.current.clear();
    setSavedSearches([]);
    setHistory([]);
    setSavedSearchesNextPageToken(null);
    setHistoryNextPageToken(null);
    await Promise.all([
      refreshBackendSavedSearches(bootstrap, signal),
      refreshBackendHistory(bootstrap, signal),
    ]);
    if (signal.aborted) throw new DOMException("The persisted search launch was canceled.", "AbortError");
    return bootstrap;
  }

  async function refreshBackendSavedSearches(
    bootstrapOverride?: BackendBootstrapState,
    signal?: AbortSignal,
  ) {
    if (!backendEnabled) return;
    const refreshEpoch = ++backendSavedSearchRefreshEpochRef.current;
    backendSavedSearchLoadMoreEpochRef.current += 1;
    backendSavedSearchLoadMoreRef.current = false;
    backendSavedSearchPageTokensRef.current.clear();
    setSavedSearchesLoadingMore(false);
    setSavedSearchesLoading(true);
    try {
      const bootstrap = bootstrapOverride ?? await ensureBackendBootstrap();
      if (signal?.aborted || backendSavedSearchRefreshEpochRef.current !== refreshEpoch) return;
      const result = await listServerSavedSearches(apiClient, bootstrap.response, {
        appId: bootstrap.response.selectedAppId ?? undefined,
        maximumPages: 1,
        signal,
      });
      if (signal?.aborted || backendSavedSearchRefreshEpochRef.current !== refreshEpoch) return;
      if (result.status === "unavailable") {
        backendSavedSearchesRef.current.clear();
        setSavedSearches([]);
        setSavedSearchesNextPageToken(null);
        setSavedSearchesAvailable(false);
        return;
      }
      const activeSavedSearch = activeSavedSearchIdRef.current === null
        ? undefined
        : backendSavedSearchesRef.current.get(activeSavedSearchIdRef.current);
      const savedSearchItems = activeSavedSearch === undefined
        || result.value.items.some((savedSearch) => savedSearch.id === activeSavedSearch.id)
        ? result.value.items
        : [activeSavedSearch, ...result.value.items];
      backendSavedSearchesRef.current = new Map(
        savedSearchItems.map((savedSearch) => [savedSearch.id, savedSearch]),
      );
      setSavedSearches(savedSearchItems.map((savedSearch) => savedSearchForDisplay(savedSearch)));
      setSavedSearchesNextPageToken(recordNextPageToken(
        backendSavedSearchPageTokensRef.current,
        result.value.nextPageToken,
        "Saved searches",
      ));
      setSavedSearchesAvailable(true);
    } catch (error) {
      if (signal?.aborted || backendSavedSearchRefreshEpochRef.current !== refreshEpoch) return;
      setSavedSearchesNextPageToken(null);
      if (error instanceof RepeatedPageCursorError) {
        setSavedSearchesAvailable(true);
        showToast(`${error.message} Further paging was stopped.`, "warning");
        return;
      }
      setSavedSearchesAvailable(false);
      showToast(error instanceof Error ? error.message : "Unable to load saved searches.", "warning");
    } finally {
      if (!signal?.aborted && backendSavedSearchRefreshEpochRef.current === refreshEpoch) {
        setSavedSearchesLoading(false);
      }
    }
  }

  async function reloadBackendSavedSearchById(
    savedSearchId: string,
  ): Promise<"updated" | "removed"> {
    const bootstrap = backendBootstrapRef.current ?? await ensureBackendBootstrap();
    try {
      const result = await getServerSavedSearch(
        apiClient,
        bootstrap.response,
        savedSearchId,
      );
      if (result.status === "unavailable") {
        setSavedSearchesAvailable(false);
        throw new Error("Saved searches are not available from this server.");
      }
      backendSavedSearchesRef.current.set(result.value.id, result.value);
      const displaySearch = savedSearchForDisplay(result.value);
      setSavedSearches((current) => [
        displaySearch,
        ...current.filter((item) => item.id !== displaySearch.id),
      ]);
      setSavedSearchesAvailable(true);
      return "updated";
    } catch (error) {
      if (!isHttpStatus(error, 404)) throw error;
      backendSavedSearchesRef.current.delete(savedSearchId);
      setSavedSearches((current) => current.filter((item) => item.id !== savedSearchId));
      if (activeSavedSearchIdRef.current === savedSearchId) {
        activeSavedSearchIdRef.current = null;
        setActiveSavedSearchId(null);
        setSavedWorkspaceBaseline(null);
        setSavedBaselineCaptureId(null);
      }
      return "removed";
    }
  }

  async function refreshBackendHistory(
    bootstrapOverride?: BackendBootstrapState,
    signal?: AbortSignal,
  ) {
    if (!backendEnabled) return;
    const refreshEpoch = ++backendHistoryRefreshEpochRef.current;
    backendHistoryLoadMoreEpochRef.current += 1;
    backendHistoryLoadMoreRef.current = false;
    backendHistoryPageTokensRef.current.clear();
    setHistoryLoadingMore(false);
    setHistoryLoading(true);
    try {
      const bootstrap = bootstrapOverride ?? await ensureBackendBootstrap();
      if (signal?.aborted || backendHistoryRefreshEpochRef.current !== refreshEpoch) return;
      const result = await listServerSearchHistory(apiClient, bootstrap.response, {
        appId: bootstrap.response.selectedAppId ?? undefined,
        maximumPages: 1,
        states: TERMINAL_HISTORY_STATES,
        signal,
      });
      if (signal?.aborted || backendHistoryRefreshEpochRef.current !== refreshEpoch) return;
      if (result.status === "unavailable") {
        backendHistoryRef.current.clear();
        setHistory([]);
        setHistoryNextPageToken(null);
        setHistoryAvailable(false);
        return;
      }
      const activeHistoryEntry = backendHistoryRerunRef.current;
      const historyItems = activeHistoryEntry === null
        || result.value.items.some((entry) => entry.id === activeHistoryEntry.id)
        ? result.value.items
        : [activeHistoryEntry, ...result.value.items];
      backendHistoryRef.current = new Map(
        historyItems.map((entry) => [entry.id, entry]),
      );
      setHistory(historyItems.map((entry) => historyEntryForDisplay(entry)));
      setHistoryNextPageToken(recordNextPageToken(
        backendHistoryPageTokensRef.current,
        result.value.nextPageToken,
        "Search history",
      ));
      setHistoryAvailable(true);
    } catch (error) {
      if (signal?.aborted || backendHistoryRefreshEpochRef.current !== refreshEpoch) return;
      setHistoryNextPageToken(null);
      if (error instanceof RepeatedPageCursorError) {
        setHistoryAvailable(true);
        showToast(`${error.message} Further paging was stopped.`, "warning");
        return;
      }
      setHistoryAvailable(false);
      showToast(error instanceof Error ? error.message : "Unable to load search history.", "warning");
    } finally {
      if (!signal?.aborted && backendHistoryRefreshEpochRef.current === refreshEpoch) {
        setHistoryLoading(false);
      }
    }
  }

  async function loadMoreBackendSavedSearches() {
    const pageToken = savedSearchesNextPageToken;
    if (!backendEnabled || pageToken === null || backendSavedSearchLoadMoreRef.current) return;
    const refreshEpoch = backendSavedSearchRefreshEpochRef.current;
    const bootstrap = backendBootstrapRef.current;
    if (bootstrap === null) return;
    const appId = bootstrap.response.selectedAppId;
    const loadMoreEpoch = ++backendSavedSearchLoadMoreEpochRef.current;
    backendSavedSearchLoadMoreRef.current = true;
    setSavedSearchesLoadingMore(true);
    try {
      const result = await listServerSavedSearches(apiClient, bootstrap.response, {
        appId: appId ?? undefined,
        pageToken,
        maximumPages: 1,
      });
      if (
        backendSavedSearchRefreshEpochRef.current !== refreshEpoch
        || backendBootstrapRef.current?.response.selectedAppId !== appId
      ) return;
      if (result.status === "unavailable") {
        setSavedSearchesAvailable(false);
        setSavedSearchesNextPageToken(null);
        return;
      }
      for (const savedSearch of result.value.items) {
        backendSavedSearchesRef.current.set(savedSearch.id, savedSearch);
      }
      setSavedSearches((current) =>
        mergeDisplayPage(current, result.value.items, savedSearchForDisplay)
      );
      setSavedSearchesNextPageToken(recordNextPageToken(
        backendSavedSearchPageTokensRef.current,
        result.value.nextPageToken,
        "Saved searches",
      ));
    } catch (error) {
      if (
        backendSavedSearchRefreshEpochRef.current === refreshEpoch
        && backendBootstrapRef.current?.response.selectedAppId === appId
      ) {
        if (error instanceof RepeatedPageCursorError) setSavedSearchesNextPageToken(null);
        showToast(error instanceof Error ? error.message : "Unable to load more saved searches.", "warning");
      }
    } finally {
      if (
        backendSavedSearchLoadMoreEpochRef.current === loadMoreEpoch
        &&
        backendSavedSearchRefreshEpochRef.current === refreshEpoch
        && backendBootstrapRef.current?.response.selectedAppId === appId
      ) {
        backendSavedSearchLoadMoreRef.current = false;
        setSavedSearchesLoadingMore(false);
      }
    }
  }

  async function loadMoreBackendHistory() {
    const pageToken = historyNextPageToken;
    if (!backendEnabled || pageToken === null || backendHistoryLoadMoreRef.current) return;
    const refreshEpoch = backendHistoryRefreshEpochRef.current;
    const bootstrap = backendBootstrapRef.current;
    if (bootstrap === null) return;
    const appId = bootstrap.response.selectedAppId;
    const loadMoreEpoch = ++backendHistoryLoadMoreEpochRef.current;
    backendHistoryLoadMoreRef.current = true;
    setHistoryLoadingMore(true);
    try {
      const result = await listServerSearchHistory(apiClient, bootstrap.response, {
        appId: appId ?? undefined,
        pageToken,
        maximumPages: 1,
        states: TERMINAL_HISTORY_STATES,
      });
      if (
        backendHistoryRefreshEpochRef.current !== refreshEpoch
        || backendBootstrapRef.current?.response.selectedAppId !== appId
      ) return;
      if (result.status === "unavailable") {
        setHistoryAvailable(false);
        setHistoryNextPageToken(null);
        return;
      }
      for (const entry of result.value.items) {
        backendHistoryRef.current.set(entry.id, entry);
      }
      setHistory((current) =>
        mergeDisplayPage(current, result.value.items, historyEntryForDisplay)
      );
      setHistoryNextPageToken(recordNextPageToken(
        backendHistoryPageTokensRef.current,
        result.value.nextPageToken,
        "Search history",
      ));
    } catch (error) {
      if (
        backendHistoryRefreshEpochRef.current === refreshEpoch
        && backendBootstrapRef.current?.response.selectedAppId === appId
      ) {
        if (error instanceof RepeatedPageCursorError) setHistoryNextPageToken(null);
        showToast(error instanceof Error ? error.message : "Unable to load more search history.", "warning");
      }
    } finally {
      if (
        backendHistoryLoadMoreEpochRef.current === loadMoreEpoch
        &&
        backendHistoryRefreshEpochRef.current === refreshEpoch
        && backendBootstrapRef.current?.response.selectedAppId === appId
      ) {
        backendHistoryLoadMoreRef.current = false;
        setHistoryLoadingMore(false);
      }
    }
  }

  useEffect(() => {
    if (!backendEnabled) return;
    backendObjectAbortRef.current?.abort();
    const controller = new AbortController();
    backendObjectAbortRef.current = controller;
    void ensureBackendBootstrap()
      .then(async (bootstrap) => {
        if (controller.signal.aborted) return;
        await Promise.all([
          refreshBackendSavedSearches(bootstrap, controller.signal),
          refreshBackendHistory(bootstrap, controller.signal),
        ]);
      })
      .catch((error: unknown) => {
        if (controller.signal.aborted) return;
        setSavedSearchesLoading(false);
        setHistoryLoading(false);
        setSavedSearchesAvailable(false);
        setHistoryAvailable(false);
        showToast(error instanceof Error ? error.message : "Unable to initialize backend libraries.", "warning");
      });
    return () => {
      controller.abort();
      if (backendObjectAbortRef.current === controller) backendObjectAbortRef.current = null;
    };
    // Refreshes after mutations and terminal searches are explicit; rerunning
    // this initializer for newly-created function identities would race them.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [apiClient, backendEnabled]);

  function replaceBackendNotices(job: SearchJob) {
    setBackendNotices(uniqueMessages([
      ...job.warnings.map((warning) => warning.message),
      ...job.diagnostics.map((jobDiagnostic) => jobDiagnostic.message),
      ...(job.failure?.diagnostics ?? []).map((jobDiagnostic) => jobDiagnostic.message),
      job.failure?.message,
    ]));
  }

  function updateBackendPreviewStatus(
    status: BackendPreviewStatus,
    announcement?: string,
  ) {
    backendPreviewStatusRef.current = status;
    setBackendPreviewStatus(status);
    if (announcement !== undefined) setBackendPreviewAnnouncement(announcement);
  }

  function clearBackendPreview(
    status: BackendPreviewStatus = "disabled",
    announcement?: string,
  ) {
    backendPreviewRef.current = null;
    setBackendPreviewDisplay(null);
    updateBackendPreviewStatus(status, announcement);
  }

  function resetBackendResultState() {
    backendResultPagesRef.current.clear();
    backendPageTokensRef.current.clear();
    backendPageStartsRef.current.clear();
    backendResultPageTokensSeenRef.current.clear();
    const pageSize = backendBootstrapRef.current === null
      ? eventPageSize
      : normalizedBackendPageSize(eventPageSize, backendBootstrapRef.current);
    backendPageSizeRef.current = pageSize;
    setBackendResultPageSize(pageSize);
    backendPageTokensRef.current.set(`${pageSize}:1`, undefined);
    backendPageStartsRef.current.set(`${pageSize}:1`, 1);
    backendAuthoritativeFieldsRef.current = false;
    backendAuthoritativeTimelineRef.current = false;
    backendFieldCatalogAbortRef.current?.abort();
    backendFieldCatalogAbortRef.current = null;
    backendFieldCatalogNextPageTokenRef.current = null;
    backendFieldCatalogPageTokensRef.current.clear();
    backendFieldSummaryAbortRef.current?.abort();
    backendFieldSummaryCacheRef.current.clear();
    setBackendFieldsLoading(false);
    setBackendFieldsLoadingMore(false);
    setBackendFieldsHasMore(false);
    setBackendFieldSummaryLoading(false);
    setBackendFieldSummaryError(null);
    setBackendResultKind(ResultSetKind.RESULT_SET_KIND_UNSPECIFIED);
    setBackendResultSchema(null);
    backendPreviewRef.current = null;
    backendPreviewSchemasRef.current.clear();
    backendPreviewRowLimitRef.current = 0;
    setBackendPreviewDisplay(null);
    backendPreviewStatusRef.current = "disabled";
    setBackendPreviewStatus("disabled");
    setBackendPreviewAnnouncement("");
    setBackendAuthoritativeResultsReady(false);
    setBackendResultTotalRows(null);
    setBackendResultTotalExact(false);
    setBackendHasNextPage(false);
    setBackendSnapshotComplete(true);
    setBackendResultsTruncated(false);
    setBackendResultsExpired(false);
    setBackendExpiresAt(null);
    setBackendNotices([]);
  }

  function clearTimers() {
    for (const timer of timersRef.current) window.clearTimeout(timer);
    timersRef.current = [];
  }

  function schedule(callback: () => void, delay: number) {
    const timer = window.setTimeout(callback, delay);
    timersRef.current.push(timer);
  }

  function showToast(message: string, tone: ToastState["tone"] = "info") {
    setToast({ message, tone });
  }

  function focusEditor(offset: number) {
    window.requestAnimationFrame(() => {
      const editor = editorRef.current;
      if (editor === null) return;
      const safeOffset = Math.max(0, Math.min(offset, editor.value.length));
      editor.focus();
      editor.setSelectionRange(safeOffset, safeOffset);
      setEditorCaret(safeOffset);
    });
  }

  async function copyText(text: string, successMessage: string) {
    try {
      if (navigator.clipboard !== undefined) {
        await navigator.clipboard.writeText(text);
      } else {
        const fallback = document.createElement("textarea");
        fallback.value = text;
        fallback.setAttribute("readonly", "");
        fallback.style.position = "fixed";
        fallback.style.opacity = "0";
        document.body.append(fallback);
        fallback.select();
        const copied = document.execCommand("copy");
        fallback.remove();
        if (!copied) throw new Error("Clipboard copy was rejected.");
      }
      showToast(successMessage, "success");
    } catch {
      showToast("Could not access the clipboard. Select and copy the value manually.", "warning");
    }
  }

  function updateRelativeRange(amount: number, unit: "m" | "h" | "d") {
    const safeAmount = Math.max(1, Math.min(999, Math.floor(amount || 1)));
    const unitLabel = unit === "m" ? "minute" : unit === "h" ? "hour" : "day";
    setRelativeAmount(safeAmount);
    setRelativeUnit(unit);
    setDraftTimeRange({
      label: `Last ${safeAmount} ${unitLabel}${safeAmount === 1 ? "" : "s"}`,
      earliest: `-${safeAmount}${unit}`,
      latest: "now",
    });
  }

  function updateAbsoluteRange(start: string, end: string) {
    setAbsoluteStart(start);
    setAbsoluteEnd(end);
    const startDate = new Date(start);
    const endDate = new Date(end);
    setDraftTimeRange({
      label: "Custom date range",
      earliest: Number.isNaN(startDate.valueOf()) ? start : startDate.toISOString(),
      latest: Number.isNaN(endDate.valueOf()) ? end : endDate.toISOString(),
      timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC",
    });
  }

  function seedAbsoluteRange() {
    const now = backendBootstrapRef.current === null
      ? new Date()
      : currentBackendServerTime(backendBootstrapRef.current);
    try {
      const resolved = resolveAbsoluteTimeRange(timeRange.earliest, timeRange.latest, now);
      updateAbsoluteRange(formatLocalDateTimeInput(resolved.earliest), formatLocalDateTimeInput(resolved.latest));
    } catch {
      const fallback = resolveAbsoluteTimeRange("-24h", "now", now);
      updateAbsoluteRange(formatLocalDateTimeInput(fallback.earliest), formatLocalDateTimeInput(fallback.latest));
    }
  }

  function closeTimePicker(restoreFocus = true) {
    setModal(null);
    if (restoreFocus) {
      window.requestAnimationFrame(() => timeRangeButtonRef.current?.focus());
    }
  }

  useEffect(() => {
    serverExportJobRef.current = serverExportJob;
  }, [serverExportJob]);

  useEffect(() => {
    if (modal !== "export" || exportStage !== "ready") return;
    const expiresAt = serverExportJob?.artifact?.expiresAt;
    if (expiresAt === undefined) return;
    const bootstrap = backendBootstrapRef.current;
    const now = bootstrap === null ? Date.now() : currentBackendServerTime(bootstrap).valueOf();
    const remaining = expiresAt.valueOf() - now;
    if (remaining <= 0) return;
    const timer = window.setTimeout(
      () => setExportClockTick((current) => current + 1),
      Math.max(50, Math.min(1_000, remaining + 25)),
    );
    return () => window.clearTimeout(timer);
  }, [exportClockTick, exportStage, modal, serverExportJob?.artifact?.expiresAt]);

  useEffect(() => {
    return () => {
      clearTimers();
      backendAbortRef.current?.abort();
      backendPageAbortRef.current?.abort();
      backendMetadataAbortRef.current?.abort();
      backendFieldCatalogAbortRef.current?.abort();
      backendFieldSummaryAbortRef.current?.abort();
      backendObjectAbortRef.current?.abort();
      backendExportAbortRef.current?.abort();
      backendExportDownloadAbortRef.current?.abort();
      backendExportCancelAbortRef.current?.abort();
      appSwitchAbortRef.current?.abort();
      backendSocketRef.current?.dispose();
      const exportJob = serverExportJobRef.current;
      const bootstrap = backendBootstrapRef.current;
      if (
        exportJob !== null
        && bootstrap !== null
        && (
          exportJob.state === ExportJobState.EXPORT_JOB_STATE_QUEUED
          || exportJob.state === ExportJobState.EXPORT_JOB_STATE_RUNNING
        )
      ) {
        void cancelServerExport(
          apiClient,
          bootstrap.response,
          exportJob.exportJobId,
          { timeoutMs: 5_000 },
        ).catch(() => undefined);
      }
    };
  }, [apiClient]);

  useEffect(() => {
    if (!backendEnabled || backendExpiresAt === null || backendResultsExpired) return;
    let timer: number | undefined;
    const markExpired = () => {
      setBackendResultsExpired(true);
      setPhase("expired");
      setBackendNotices((current) => uniqueMessages([...current, "These retained search results have expired. Run the search again to refresh them."]));
    };
    const armExpiryCheck = () => {
      const bootstrap = backendBootstrapRef.current;
      const serverNow = bootstrap === null ? Date.now() : currentBackendServerTime(bootstrap).getTime();
      const remaining = backendExpiresAt.getTime() - serverNow;
      if (remaining <= 0) {
        markExpired();
        return;
      }
      timer = window.setTimeout(armExpiryCheck, Math.min(remaining, 2_147_483_647));
    };
    armExpiryCheck();
    return () => {
      if (timer !== undefined) window.clearTimeout(timer);
    };
  }, [backendEnabled, backendExpiresAt, backendResultsExpired]);

  useEffect(() => {
    backendFieldSummaryAbortRef.current?.abort();
    setBackendFieldSummaryError(null);
    if (
      !backendEnabled
      || activeField === null
      || phase !== "completed"
      || !backendAuthoritativeFieldsRef.current
    ) {
      setBackendFieldSummaryLoading(false);
      return;
    }
    const job = backendJobRef.current;
    const bootstrap = backendBootstrapRef.current;
    if (job === null || bootstrap === null) return;
    const cacheKey = `${job.searchJobId}:${activeField}`;
    if (backendFieldSummaryCacheRef.current.has(cacheKey)) {
      setBackendFieldSummaryError(backendFieldSummaryCacheRef.current.get(cacheKey) ?? null);
      setBackendFieldSummaryLoading(false);
      return;
    }
    const controller = new AbortController();
    const generation = generationRef.current;
    backendFieldSummaryAbortRef.current = controller;
    setBackendFieldSummaryLoading(true);
    void getServerFieldSummary(apiClient, bootstrap.response, job.searchJobId, activeField, {
      maxValues: bootstrap.response.limits.maximumFieldSummaryValues || 10,
      signal: controller.signal,
    }).then((result) => {
      if (
        controller.signal.aborted
        || generationRef.current !== generation
        || backendJobIdRef.current !== job.searchJobId
      ) {
        return;
      }
      if (result.status === "unavailable") {
        const message = "Top values are not available from this server.";
        backendFieldSummaryCacheRef.current.set(cacheKey, message);
        setBackendFieldSummaryError(message);
        return;
      }
      const summary = result.value;
      if (activeField !== summary.profile.name) return;
      backendFieldSummaryCacheRef.current.set(cacheKey, null);
      setFields((current) => current.map((field) =>
        field.name === summary.profile.name
          ? {
            ...serverFieldToDemoField(summary.profile, summary.topValues),
            selected: field.selected,
          }
          : field
      ));
      if (
        summary.topValuesAreApproximate
        || summary.topValues.some((value) => value.countIsApproximate)
      ) {
        setBackendNotices((current) => uniqueMessages([
          ...current,
          `Top values for ${summary.profile.displayName} are approximate.`,
        ]));
      }
    }).catch((error: unknown) => {
      if (controller.signal.aborted) return;
      if (isHttpStatus(error, 410)) {
        clearBackendPreview("disabled", "Search results expired. Live preview rows were discarded.");
        setBackendAuthoritativeResultsReady(false);
        setBackendResultsExpired(true);
        setPhase("expired");
        setBackendFieldSummaryError("These retained field results have expired.");
      } else {
        setBackendFieldSummaryError(error instanceof Error ? error.message : "Unable to load field values.");
      }
    }).finally(() => {
      if (backendFieldSummaryAbortRef.current === controller) {
        backendFieldSummaryAbortRef.current = null;
        setBackendFieldSummaryLoading(false);
      }
    });
    return () => controller.abort();
  }, [activeField, apiClient, backendEnabled, phase]);

  useEffect(() => {
    const phoneViewport = window.matchMedia("(max-width: 760px)");
    const collapseForPhone = (event?: MediaQueryListEvent) => {
      if (event?.matches ?? phoneViewport.matches) {
        setFieldsCollapsed(true);
        setExpandedEvents(new Set());
      }
    };
    collapseForPhone();
    phoneViewport.addEventListener("change", collapseForPhone);
    return () => phoneViewport.removeEventListener("change", collapseForPhone);
  }, []);

  useEffect(() => {
    if (toast === null) return;
    const timer = window.setTimeout(() => setToast(null), 3200);
    return () => window.clearTimeout(timer);
  }, [toast]);

  useEffect(() => {
    if (urlLaunchAppliedRef.current) return;
    urlLaunchAppliedRef.current = true;
    const url = new URL(window.location.href);
    const sharedQuery = url.searchParams.get("q");
    const savedSearchLaunchId = url.searchParams.get("savedSearchId")?.trim() || null;
    const historySearchLaunchId = url.searchParams.get("historySearchId")?.trim() || null;
    const initialQuery = sharedQuery === null || sharedQuery.trim().length === 0
      ? initialWorkspaceQuery
      : sharedQuery;
    const earliest = url.searchParams.get("earliest");
    const latest = url.searchParams.get("latest");
    const sharedTimezone = url.searchParams.get("timezone")?.trim() || undefined;
    const sharedRange = TIME_PRESETS.find((preset) => preset.earliest === earliest && preset.latest === latest);
    let initialRange = TIME_PRESETS[3];
    timelineZoomParentRef.current = null;
    if (sharedQuery !== null && sharedQuery.trim().length > 0) {
      setQuery(sharedQuery);
      setEditorCaret(sharedQuery.length);
    }
    if (earliest !== null && latest !== null) {
      const restoredRange = {
        ...(sharedRange ?? { label: url.searchParams.get("label") || `${earliest} to ${latest}`, earliest, latest }),
        timezone: sharedTimezone,
      };
      initialRange = restoredRange;
      setTimeRange(restoredRange);
      setDraftTimeRange(restoredRange);
    }
    const hasContextualQuery = sharedQuery !== null && sharedQuery.trim().length > 0;
    const shouldRunContextualQuery = hasContextualQuery && url.searchParams.get("run") !== "0";
    const shouldRunPersistedSearch = url.searchParams.get("run") !== "0";
    if (backendEnabled && savedSearchLaunchId !== null && historySearchLaunchId !== null) {
      setPhase("failed");
      setProgress(100);
      showToast("A search launch can reference either a saved search or a history entry, not both.", "warning");
      return;
    }
    if (backendEnabled && (savedSearchLaunchId !== null || historySearchLaunchId !== null)) {
      const controller = new AbortController();
      const launchEpoch = ++persistedLaunchEpochRef.current;
      let launchTimer: number | null = null;
      persistedLaunchPendingRef.current = true;
      setPersistedLaunchPending(true);
      setPhase("queued");
      setProgress(1);
      void ensureBackendBootstrap()
        .then(async (bootstrap) => {
          if (controller.signal.aborted || persistedLaunchEpochRef.current !== launchEpoch) return;
          if (savedSearchLaunchId !== null) {
            const result = await getServerSavedSearch(
              apiClient,
              bootstrap.response,
              savedSearchLaunchId,
              { signal: controller.signal },
            );
            if (result.status === "unavailable") {
              throw new Error("Saved-search launches are not available from this server.");
            }
            if (controller.signal.aborted || persistedLaunchEpochRef.current !== launchEpoch) return;
            const savedSearch = result.value;
            await ensurePersistedLaunchAppContext(
              bootstrap,
              savedSearch.search.appId,
              controller.signal,
            );
            if (controller.signal.aborted || persistedLaunchEpochRef.current !== launchEpoch) return;
            backendSavedSearchesRef.current.set(savedSearch.id, savedSearch);
            const displaySearch = savedSearchForDisplay(savedSearch);
            setSavedSearches((current) => [
              displaySearch,
              ...current.filter((item) => item.id !== displaySearch.id),
            ]);
            const hasSavedRange = displaySearch.earliest.trim().length > 0
              && displaySearch.latest.trim().length > 0;
            const launchRange = hasSavedRange
              ? {
                label: displaySearch.earliest === "-24h"
                  ? "Last 24 hours"
                  : `${displaySearch.earliest} to ${displaySearch.latest}`,
                earliest: displaySearch.earliest,
                latest: displaySearch.latest,
                timezone: displaySearch.timezone,
              }
              : initialRange;
            persistedLaunchPendingRef.current = false;
            setPersistedLaunchPending(false);
            openSavedSearch(displaySearch, initialRange);
            if (shouldRunPersistedSearch) {
              launchTimer = window.setTimeout(
                () => {
                  if (!controller.signal.aborted && persistedLaunchEpochRef.current === launchEpoch) {
                    searchRunnerRef.current(savedSearch.search.spl, launchRange);
                  }
                },
                0,
              );
            }
            return;
          }
          const result = await getServerSearchHistoryEntry(
            apiClient,
            bootstrap.response,
            historySearchLaunchId ?? "",
            { signal: controller.signal },
          );
          if (result.status === "unavailable") {
            throw new Error("Search-history launches are not available from this server.");
          }
          if (controller.signal.aborted || persistedLaunchEpochRef.current !== launchEpoch) return;
          const historyEntry = result.value;
          await ensurePersistedLaunchAppContext(
            bootstrap,
            historyEntry.search.appId,
            controller.signal,
          );
          if (controller.signal.aborted || persistedLaunchEpochRef.current !== launchEpoch) return;
          backendHistoryRef.current.set(historyEntry.id, historyEntry);
          const displayEntry = historyEntryForDisplay(historyEntry);
          setHistory((current) => [
            displayEntry,
            ...current.filter((item) => item.id !== displayEntry.id),
          ]);
          persistedLaunchPendingRef.current = false;
          setPersistedLaunchPending(false);
          openHistoryEntry(displayEntry, shouldRunPersistedSearch);
        })
        .catch((error: unknown) => {
          if (controller.signal.aborted || persistedLaunchEpochRef.current !== launchEpoch) return;
          persistedLaunchPendingRef.current = false;
          setPersistedLaunchPending(false);
          setPhase("failed");
          setProgress(100);
          showToast(error instanceof Error ? error.message : "Unable to open the persisted search.", "warning");
        });
      return () => {
        controller.abort();
        if (launchTimer !== null) window.clearTimeout(launchTimer);
        if (persistedLaunchEpochRef.current === launchEpoch) {
          persistedLaunchEpochRef.current += 1;
          persistedLaunchPendingRef.current = false;
          setPersistedLaunchPending(false);
        }
        urlLaunchAppliedRef.current = false;
      };
    }
    if (hasContextualQuery && !shouldRunContextualQuery) {
      setSubmittedQuery("");
      setSubmittedTimeRange(initialRange);
      setPhase("completed");
      setProgress(0);
      setElapsed("0.00 s");
      setScannedRows(0);
      setScannedRowsApproximate(false);
      setScannedBytes("0 B");
      setResolvedTimeRangeLabel(null);
      setBackendEventCount(0);
      setBackendPrimaryCountLabel("events");
      setBackendPrimaryCountPrefix("");
      setBackendEvents([]);
      setBackendStatistics([]);
      setBackendStatisticsTable(null);
      setBackendTimeline([]);
      setBackendResultKind(ResultSetKind.RESULT_SET_KIND_UNSPECIFIED);
      setBackendResultTotalRows(null);
      setBackendResultTotalExact(false);
      setBackendHasNextPage(false);
      setBackendSnapshotComplete(true);
      setBackendResultsTruncated(false);
      setBackendResultsExpired(false);
      setBackendExpiresAt(null);
      setBackendNotices([]);
      setTimelineStart(null);
      setTimelineEnd(null);
      setEventPage(1);
      setActiveTab("events");
    }
    if (backendEnabled && !hasContextualQuery) {
      void ensureBackendBootstrap()
        .then((bootstrap) => {
          const authorizedQuery = defaultQueryForBootstrap(bootstrap.response);
          setDefaultSearchQuery(authorizedQuery);
          setQuery(authorizedQuery);
          setEditorCaret(authorizedQuery.length);
          if (authorizedQuery.length > 0) {
            window.setTimeout(() => searchRunnerRef.current(authorizedQuery, initialRange), 0);
          } else {
            setSubmittedQuery("");
            setPhase("completed");
            setProgress(0);
          }
        })
        .catch((error: unknown) => {
          setPhase("failed");
          setProgress(100);
          showToast(error instanceof Error ? error.message : "Unable to initialize backend search.", "warning");
        });
    } else if (shouldRunContextualQuery) {
      window.setTimeout(() => searchRunnerRef.current(initialQuery, initialRange), 0);
    }
    // URL launch is intentionally applied once for this mounted workspace.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [backendEnabled]);

  useEffect(() => {
    setCompletionIndex((current) => Math.max(0, Math.min(current, filteredCompletions.length - 1)));
  }, [filteredCompletions.length]);

  useEffect(() => {
    setEventPage((current) => Math.min(current, eventPageCount));
  }, [eventPageCount]);

  useEffect(() => {
    function closeTransientUi(event: globalThis.KeyboardEvent) {
      if (event.key !== "Escape") return;
      if (modal === "time") closeTimePicker();
      else if (modal !== null) setModal(null);
      else if (activeField !== null) setActiveField(null);
      else if (menu !== null) setMenu(null);
      else setCompletionOpen(false);
    }
    window.addEventListener("keydown", closeTransientUi);
    return () => window.removeEventListener("keydown", closeTransientUi);
  }, [activeField, menu, modal]);

  useEffect(() => {
    if (menu === null) return;
    if (document.activeElement instanceof HTMLElement) menuReturnFocusRef.current = document.activeElement;
    function navigateOpenMenu(event: globalThis.KeyboardEvent) {
      const popover = Array.from(document.querySelectorAll<HTMLElement>('.floating-menu[role="menu"]'))
        .find((candidate) => candidate.offsetParent !== null);
      if (popover === undefined) return;
      const items = Array.from(popover.querySelectorAll<HTMLElement>('[role="menuitem"], [role="menuitemradio"]'));
      if (items.length === 0) return;
      const current = items.indexOf(document.activeElement as HTMLElement);
      let next = current;
      if (event.key === "ArrowDown") next = current < 0 ? 0 : (current + 1) % items.length;
      else if (event.key === "ArrowUp") next = current < 0 ? items.length - 1 : (current - 1 + items.length) % items.length;
      else if (event.key === "Home") next = 0;
      else if (event.key === "End") next = items.length - 1;
      else if (event.key === "Escape") {
        event.preventDefault();
        setMenu(null);
        window.requestAnimationFrame(() => menuReturnFocusRef.current?.focus());
        return;
      } else return;
      event.preventDefault();
      items[next]?.focus();
    }
    document.addEventListener("keydown", navigateOpenMenu);
    return () => document.removeEventListener("keydown", navigateOpenMenu);
  }, [menu]);

  useEffect(() => {
    if (modal !== "time") return;
    if (window.matchMedia("(max-width: 760px)").matches) return;
    function handleOutsidePointer(event: globalThis.PointerEvent) {
      if (event.target instanceof Node && !timePickerRef.current?.contains(event.target)) {
        setModal(null);
      }
    }
    document.addEventListener("pointerdown", handleOutsidePointer);
    return () => document.removeEventListener("pointerdown", handleOutsidePointer);
  }, [modal]);

  function applyBackendProgress(
    jobProgress: SearchProgress | undefined,
    nextPhase: JobPhase,
    generation: number,
  ) {
    if (generationRef.current !== generation) return;
    const phaseProgress: Record<JobPhase, number> = {
      queued: 4,
      parsing: 14,
      planning: 27,
      running: 58,
      finalizing: 94,
      completed: 100,
      failed: 100,
      canceled: 100,
      expired: 100,
    };
    setPhase(nextPhase);
    const reportedPercent = jobProgress?.percentComplete;
    const effectivePercent = reportedPercent !== undefined && Number.isFinite(reportedPercent)
      ? reportedPercent
      : phaseProgress[nextPhase];
    setProgress(Math.max(0, Math.min(100, effectivePercent)));
    setElapsed(formatDuration(jobProgress?.elapsed));
    const reportedScannedRows = jobProgress?.scannedRows ?? 0n;
    const scannedRowsOverflow = reportedScannedRows > BigInt(Number.MAX_SAFE_INTEGER);
    setScannedRows(scannedRowsOverflow
      ? Number.MAX_SAFE_INTEGER
      : Number(reportedScannedRows));
    setScannedRowsApproximate(Boolean(jobProgress?.countersAreEstimates) || scannedRowsOverflow);
    setScannedBytes(formatBytes(
      backendEnabled
        ? jobProgress?.resultBytes ?? 0n
        : jobProgress?.scannedBytes ?? 0n,
    ));
    if (jobProgress !== undefined) {
      const matchedEvents = jobProgress.matchedEvents;
      const count = matchedEvents || jobProgress.producedRows;
      const job = backendJobRef.current;
      const resultKind = job?.resultKind !== undefined
        && job.resultKind !== ResultSetKind.RESULT_SET_KIND_UNSPECIFIED
        ? job.resultKind
        : job?.resultSchema?.resultKind ?? ResultSetKind.RESULT_SET_KIND_UNSPECIFIED;
      setBackendEventCount(Math.min(Number.MAX_SAFE_INTEGER, Number(count)));
      setBackendPrimaryCountLabel(
        matchedEvents > 0n || resultKind === ResultSetKind.RESULT_SET_KIND_EVENTS
          ? "events"
          : "rows",
      );
      setBackendPrimaryCountPrefix(
        count > BigInt(Number.MAX_SAFE_INTEGER)
          ? "≥"
          : jobProgress.countersAreEstimates
            ? "≈"
            : "",
      );
    }
  }

  function applyBackendJob(job: SearchJob, generation: number): boolean {
    if (generationRef.current !== generation || backendJobIdRef.current !== job.searchJobId) return false;
    if (job.stateVersion < backendJobVersionRef.current) return false;
    backendJobVersionRef.current = job.stateVersion;
    backendJobRef.current = job;
    setResolvedTimeRangeLabel(formatResolvedBackendTimeRange(job.resolvedTimeRange));
    const nextPhase = backendJobPhase(job.state);
    applyBackendProgress(job.progress, nextPhase, generation);
    setBackendEventCount(jobEventCount(job));
    const kind = job.resultKind !== ResultSetKind.RESULT_SET_KIND_UNSPECIFIED
      ? job.resultKind
      : job.resultSchema?.resultKind ?? ResultSetKind.RESULT_SET_KIND_UNSPECIFIED;
    if (kind !== ResultSetKind.RESULT_SET_KIND_UNSPECIFIED) setBackendResultKind(kind);
    setBackendPrimaryCountLabel(
      (job.progress?.matchedEvents ?? 0n) > 0n
      || kind === ResultSetKind.RESULT_SET_KIND_EVENTS
        ? "events"
        : "rows",
    );
    const primaryCount = (job.progress?.matchedEvents ?? 0n)
      || (job.progress?.producedRows ?? 0n);
    setBackendPrimaryCountPrefix(
      primaryCount > BigInt(Number.MAX_SAFE_INTEGER)
        ? "≥"
        : job.progress?.countersAreEstimates
          ? "≈"
          : "",
    );
    setBackendResultsTruncated(job.resultsTruncated);
    if (nextPhase === "expired") setBackendResultsExpired(true);
    setBackendExpiresAt(job.expiresAt ?? null);
    replaceBackendNotices(job);
    return true;
  }

  function applyBackendResultPage(page: BackendResultPage) {
    const isTimeSeries = page.schema.resultKind === ResultSetKind.RESULT_SET_KIND_TIME_SERIES;
    const adapted = adaptSearchResults(
      page.schema,
      page.rows,
      isTimeSeries,
      timechartSpanMilliseconds(
        backendJobRef.current?.definition?.spl ?? submittedQuery,
      ) ?? undefined,
    );
    clearBackendPreview("disabled", "Authoritative search results loaded.");
    setBackendAuthoritativeResultsReady(true);
    setBackendEvents(adapted.events);
    if (!backendAuthoritativeFieldsRef.current && page.schema.resultKind !== ResultSetKind.RESULT_SET_KIND_EVENTS) {
      setFields([]);
    }
    setBackendStatistics(adapted.statistics);
    setBackendStatisticsTable(adapted.statisticsTable);
    setGenericStatsSort(null);
    setStatisticsDimension(adapted.statisticDimension);
    if (isTimeSeries) {
      setBackendTimeline(adapted.timeline);
      backendAuthoritativeTimelineRef.current = true;
    } else if (!backendAuthoritativeTimelineRef.current) {
      setBackendTimeline([]);
    }
    setExpandedEvents(new Set(adapted.events[0] ? [adapted.events[0].id] : []));
    setBackendResultKind(page.schema.resultKind);
    setBackendResultSchema(page.schema);
    if (page.totalSize !== undefined) {
      setBackendResultTotalRows(page.totalSize);
      setBackendResultTotalExact(page.totalSizeExact);
    }
    setBackendHasNextPage(page.nextPageToken !== undefined);
    setBackendSnapshotComplete(page.snapshotComplete);
  }

  async function fetchBackendResultPage(
    job: SearchJob,
    pageNumber: number,
    requestedPageSize: number,
    bootstrap: BackendBootstrapState,
    signal: AbortSignal,
    generation: number,
  ): Promise<BackendResultPage> {
    if (generationRef.current !== generation || backendJobIdRef.current !== job.searchJobId) {
      throw new DOMException("Search was superseded.", "AbortError");
    }
    const pageSize = normalizedBackendPageSize(requestedPageSize, bootstrap);
    const cacheKey = `${pageSize}:${pageNumber}`;
    const cached = backendResultPagesRef.current.get(cacheKey);
    if (cached !== undefined) {
      backendResultPagesRef.current.delete(cacheKey);
      backendResultPagesRef.current.set(cacheKey, cached);
      applyBackendResultPage(cached);
      return cached;
    }
    if (!backendPageTokensRef.current.has(cacheKey)) {
      throw new Error("That result page cannot be opened until the preceding cursor page has loaded.");
    }
    const pageToken = backendPageTokensRef.current.get(cacheKey);
    const response = await apiClient.search.results({
      searchJobId: job.searchJobId,
      page: {
        pageSize,
        pageToken,
        includeTotalSize: pageNumber === 1,
      },
      columns: [],
      allowPartialResults: false,
    }, { signal });
    if (generationRef.current !== generation || backendJobIdRef.current !== job.searchJobId) {
      throw new DOMException("Search was superseded.", "AbortError");
    }
    const resultPage = response.resultPage;
    if (resultPage === undefined) throw new Error("The search completed without a result page.");
    const schema = resultPage.schema ?? job.resultSchema;
    if (schema === undefined) throw new Error("The search completed without a result schema.");
    const totalSize = resultPage.page?.totalSize;
    let nextPageToken: string | undefined;
    const nextPageKey = `${pageSize}:${pageNumber + 1}`;
    const rawNextPageToken = resultPage.page?.nextPageToken?.trim() || null;
    const knownNextPageToken = backendPageTokensRef.current.get(nextPageKey);
    if (backendPageTokensRef.current.has(nextPageKey)) {
      if (rawNextPageToken === knownNextPageToken) {
        nextPageToken = knownNextPageToken;
      } else {
        setBackendNotices((current) => uniqueMessages([
          ...current,
          "The retained result cursor changed while revisiting a page. Further paging was stopped.",
        ]));
      }
    } else {
      try {
        nextPageToken = recordNextPageToken(
          backendResultPageTokensSeenRef.current,
          rawNextPageToken,
          "Search results",
        ) ?? undefined;
      } catch (error) {
        setBackendNotices((current) => uniqueMessages([
          ...current,
          `${error instanceof Error ? error.message : "Search results returned an invalid page cursor."} Further paging was stopped.`,
        ]));
      }
    }
    const page: BackendResultPage = {
      schema,
      rows: resultPage.rows,
      nextPageToken,
      totalSize: totalSize === undefined ? undefined : Math.min(Number.MAX_SAFE_INTEGER, Number(totalSize)),
      totalSizeExact: (resultPage.page?.totalSizeExact ?? false)
        && (totalSize === undefined || totalSize <= BigInt(Number.MAX_SAFE_INTEGER)),
      snapshotComplete: resultPage.snapshotComplete,
    };
    backendResultPagesRef.current.set(cacheKey, page);
    while (backendResultPagesRef.current.size > MAX_CACHED_RESULT_PAGES) {
      const oldestKey = backendResultPagesRef.current.keys().next().value;
      if (oldestKey === undefined) break;
      backendResultPagesRef.current.delete(oldestKey);
    }
    if (page.nextPageToken !== undefined) {
      backendPageTokensRef.current.set(`${pageSize}:${pageNumber + 1}`, page.nextPageToken);
      const currentStart = backendPageStartsRef.current.get(cacheKey) ?? 1;
      backendPageStartsRef.current.set(`${pageSize}:${pageNumber + 1}`, currentStart + page.rows.length);
    }
    applyBackendResultPage(page);
    return page;
  }

  async function fetchInitialBackendResults(
    job: SearchJob,
    bootstrap: BackendBootstrapState,
    signal: AbortSignal,
    generation: number,
  ) {
    const kind = job.resultKind !== ResultSetKind.RESULT_SET_KIND_UNSPECIFIED
      ? job.resultKind
      : job.resultSchema?.resultKind ?? ResultSetKind.RESULT_SET_KIND_UNSPECIFIED;
    const requestedPageSize = kind === ResultSetKind.RESULT_SET_KIND_EVENTS
      ? eventPageSize
      : backendMaximumPageSize(bootstrap);
    const pageSize = normalizedBackendPageSize(requestedPageSize, bootstrap);
    backendPageSizeRef.current = pageSize;
    setBackendResultPageSize(pageSize);
    backendPageTokensRef.current.set(`${pageSize}:1`, undefined);
    backendPageStartsRef.current.set(`${pageSize}:1`, 1);
    await fetchBackendResultPage(job, 1, pageSize, bootstrap, signal, generation);
  }

  async function fetchAuthoritativeBackendMetadata(
    job: SearchJob,
    bootstrap: BackendBootstrapState,
    generation: number,
  ) {
    const kind = job.resultKind !== ResultSetKind.RESULT_SET_KIND_UNSPECIFIED
      ? job.resultKind
      : job.resultSchema?.resultKind ?? ResultSetKind.RESULT_SET_KIND_UNSPECIFIED;
    if (kind !== ResultSetKind.RESULT_SET_KIND_EVENTS) return;
    backendMetadataAbortRef.current?.abort();
    backendFieldCatalogAbortRef.current?.abort();
    backendFieldCatalogAbortRef.current = null;
    backendFieldCatalogNextPageTokenRef.current = null;
    backendFieldCatalogPageTokensRef.current.clear();
    setBackendFieldsHasMore(false);
    setBackendFieldsLoadingMore(false);
    const controller = new AbortController();
    backendMetadataAbortRef.current = controller;
    setBackendFieldsLoading(true);
    const [timelineResult, fieldResult] = await Promise.allSettled([
      getServerTimeline(apiClient, bootstrap.response, job.searchJobId, {
        signal: controller.signal,
      }),
      getServerFieldCatalog(apiClient, bootstrap.response, job.searchJobId, {
        maximumPages: 1,
        signal: controller.signal,
      }),
    ]);
    if (
      controller.signal.aborted
      || generationRef.current !== generation
      || backendJobIdRef.current !== job.searchJobId
    ) {
      if (backendMetadataAbortRef.current === controller) {
        backendMetadataAbortRef.current = null;
        setBackendFieldsLoading(false);
      }
      return;
    }

    if (timelineResult.status === "fulfilled" && timelineResult.value.status === "available") {
      backendAuthoritativeTimelineRef.current = true;
      setBackendTimeline(timelineResult.value.value.points);
      if (
        !timelineResult.value.value.complete
        || timelineResult.value.value.buckets.some((bucket) => bucket.partial)
      ) {
        setBackendNotices((current) => uniqueMessages([
          ...current,
          "The server timeline contains partial buckets.",
        ]));
      }
    } else if (timelineResult.status === "fulfilled") {
      setBackendTimeline([]);
      setBackendNotices((current) => uniqueMessages([
        ...current,
        "The server did not expose an authoritative timeline for this search.",
      ]));
    } else if (isHttpStatus(timelineResult.reason, 410)) {
      setBackendResultsExpired(true);
      setPhase("expired");
    } else if (!(timelineResult.reason instanceof DOMException && timelineResult.reason.name === "AbortError")) {
      setBackendTimeline([]);
      setBackendNotices((current) => uniqueMessages([
        ...current,
        timelineResult.reason instanceof Error
          ? `Timeline unavailable: ${timelineResult.reason.message}`
          : "The authoritative timeline could not be loaded.",
      ]));
    }

    if (fieldResult.status === "fulfilled" && fieldResult.value.status === "available") {
      backendAuthoritativeFieldsRef.current = true;
      try {
        backendFieldCatalogNextPageTokenRef.current = recordNextPageToken(
          backendFieldCatalogPageTokensRef.current,
          fieldResult.value.value.nextPageToken,
          "The field catalog",
        );
        setBackendFieldsHasMore(backendFieldCatalogNextPageTokenRef.current !== null);
      } catch (error) {
        backendFieldCatalogNextPageTokenRef.current = null;
        setBackendFieldsHasMore(false);
        setBackendNotices((current) => uniqueMessages([
          ...current,
          error instanceof Error ? error.message : "The field catalog returned an invalid page cursor.",
        ]));
      }
      const savedSelection = pendingSavedSelectedFieldsRef.current;
      setFields(fieldResult.value.value.fields.map((profile) => {
        const field = serverFieldToDemoField(profile);
        return savedSelection === null
          ? field
          : { ...field, selected: savedSelection.has(field.name) };
      }));
      pendingSavedSelectedFieldsRef.current = null;
    } else if (fieldResult.status === "fulfilled") {
      pendingSavedSelectedFieldsRef.current = null;
      backendFieldCatalogNextPageTokenRef.current = null;
      setBackendFieldsHasMore(false);
      setFields([]);
      setBackendNotices((current) => uniqueMessages([
        ...current,
        "The server did not expose an authoritative field catalog for this search.",
      ]));
    } else if (isHttpStatus(fieldResult.reason, 410)) {
      pendingSavedSelectedFieldsRef.current = null;
      setBackendResultsExpired(true);
      setPhase("expired");
    } else if (!(fieldResult.reason instanceof DOMException && fieldResult.reason.name === "AbortError")) {
      pendingSavedSelectedFieldsRef.current = null;
      setFields([]);
      setBackendNotices((current) => uniqueMessages([
        ...current,
        fieldResult.reason instanceof Error
          ? `Field catalog unavailable: ${fieldResult.reason.message}`
          : "The authoritative field catalog could not be loaded.",
      ]));
    }
    setBackendFieldsLoading(false);
    if (backendMetadataAbortRef.current === controller) backendMetadataAbortRef.current = null;
  }

  async function loadMoreBackendFields() {
    const bootstrap = backendBootstrapRef.current;
    const job = backendJobRef.current;
    const pageToken = backendFieldCatalogNextPageTokenRef.current;
    if (
      bootstrap === null
      || job === null
      || pageToken === null
      || backendFieldCatalogAbortRef.current !== null
      || !backendAuthoritativeFieldsRef.current
    ) return;
    const controller = new AbortController();
    const generation = generationRef.current;
    backendFieldCatalogAbortRef.current = controller;
    setBackendFieldsLoadingMore(true);
    try {
      const result = await getServerFieldCatalog(apiClient, bootstrap.response, job.searchJobId, {
        maximumPages: 1,
        pageToken,
        signal: controller.signal,
      });
      if (
        controller.signal.aborted
        || generationRef.current !== generation
        || backendJobIdRef.current !== job.searchJobId
      ) return;
      if (result.status === "unavailable") {
        backendFieldCatalogNextPageTokenRef.current = null;
        setBackendFieldsHasMore(false);
        setBackendNotices((current) => uniqueMessages([
          ...current,
          "Additional server fields are not available.",
        ]));
        return;
      }
      setFields((current) => {
        const fieldsByName = new Map(current.map((field) => [field.name, field]));
        for (const profile of result.value.fields) {
          const previous = fieldsByName.get(profile.name);
          const next = serverFieldToDemoField(profile);
          fieldsByName.set(profile.name, previous === undefined
            ? next
            : { ...next, selected: previous.selected, values: previous.values });
        }
        return [...fieldsByName.values()];
      });
      try {
        backendFieldCatalogNextPageTokenRef.current = recordNextPageToken(
          backendFieldCatalogPageTokensRef.current,
          result.value.nextPageToken,
          "The field catalog",
        );
        setBackendFieldsHasMore(backendFieldCatalogNextPageTokenRef.current !== null);
      } catch (error) {
        backendFieldCatalogNextPageTokenRef.current = null;
        setBackendFieldsHasMore(false);
        setBackendNotices((current) => uniqueMessages([
          ...current,
          `${error instanceof Error ? error.message : "The field catalog returned an invalid page cursor."} Further paging was stopped.`,
        ]));
      }
    } catch (error) {
      if (controller.signal.aborted) return;
      if (isHttpStatus(error, 410)) {
        backendFieldCatalogNextPageTokenRef.current = null;
        setBackendFieldsHasMore(false);
        setBackendResultsExpired(true);
        setPhase("expired");
      } else if (error instanceof RepeatedPageCursorError) {
        backendFieldCatalogNextPageTokenRef.current = null;
        setBackendFieldsHasMore(false);
        setBackendNotices((current) => uniqueMessages([
          ...current,
          `${error.message} Further paging was stopped.`,
        ]));
      } else {
        showToast(error instanceof Error ? error.message : "Unable to load more server fields.", "warning");
      }
    } finally {
      if (backendFieldCatalogAbortRef.current === controller) {
        backendFieldCatalogAbortRef.current = null;
        setBackendFieldsLoadingMore(false);
      }
    }
  }

  async function getAuthoritativeBackendJob(
    searchJobId: string,
    signal: AbortSignal,
    generation: number,
  ): Promise<SearchJob> {
    const response = await apiClient.search.get({
      searchJobId,
      includePlan: false,
      includeGeneratedSql: false,
    }, { signal });
    if (generationRef.current !== generation || backendJobIdRef.current !== searchJobId) {
      throw new DOMException("Search was superseded.", "AbortError");
    }
    if (response.searchJob === undefined) throw new Error("The server returned an empty search job response.");
    applyBackendJob(response.searchJob, generation);
    return response.searchJob;
  }

  function registerBackendPreviewSchema(
    schema: ResultSchema | undefined,
    generation: number,
    searchJobId: string,
  ): boolean {
    if (
      schema === undefined
      || generationRef.current !== generation
      || backendJobIdRef.current !== searchJobId
    ) return false;

    const validationError = validateLivePreviewSchema(schema);
    if (validationError !== null) {
      clearBackendPreview(
        "resyncing",
        "The live result preview was discarded because its schema was invalid.",
      );
      setBackendNotices((current) => uniqueMessages([
        ...current,
        `${validationError} Waiting for authoritative results.`,
      ]));
      return false;
    }

    const existing = backendPreviewSchemasRef.current.get(schema.schemaId);
    if (
      backendPreviewRef.current !== null
      && backendPreviewRef.current.schemaId !== schema.schemaId
    ) {
      clearBackendPreview(
        "waiting",
        "The live result schema changed. Waiting for a fresh preview snapshot.",
      );
    }
    if (existing !== undefined) {
      if (schema.revision < existing.revision) return false;
      if (schema.revision === existing.revision) {
        if (equalResultSchemas(existing, schema)) return true;
        clearBackendPreview(
          "resyncing",
          "The live result preview was discarded because its schema changed unexpectedly.",
        );
        setBackendNotices((current) => uniqueMessages([
          ...current,
          "The server changed a preview schema without advancing its revision. Waiting for authoritative results.",
        ]));
        return false;
      }
      if (backendPreviewRef.current?.schemaId === schema.schemaId) {
        clearBackendPreview(
          "waiting",
          "The live result schema changed. Waiting for a fresh preview snapshot.",
        );
      }
    }

    backendPreviewSchemasRef.current.set(schema.schemaId, schema);
    setBackendResultSchema(schema);
    setBackendResultKind(schema.resultKind);
    return true;
  }

  function applyBackendResultPreview(
    preview: ResultPreview,
    generation: number,
    searchJobId: string,
  ) {
    if (
      generationRef.current !== generation
      || backendJobIdRef.current !== searchJobId
      || preview.searchJobId !== searchJobId
    ) return;

    const schema = backendPreviewSchemasRef.current.get(preview.schemaId);
    if (schema === undefined) {
      clearBackendPreview(
        "resyncing",
        "The live result preview was discarded because its schema was unavailable.",
      );
      setBackendNotices((current) => uniqueMessages([
        ...current,
        "The server sent preview rows before their result schema. Waiting for authoritative results.",
      ]));
      return;
    }

    const previous = backendPreviewRef.current;
    const applied = applyLiveResultPreview(
      previous,
      schema,
      preview,
      backendPreviewRowLimitRef.current,
    );
    if (applied.status === "ignored") return;
    if (applied.status === "invalid") {
      clearBackendPreview(
        "resyncing",
        "The live result preview was discarded because it failed validation.",
      );
      setBackendNotices((current) => uniqueMessages([
        ...current,
        `${applied.message} Waiting for authoritative results.`,
      ]));
      return;
    }

    backendPreviewRef.current = applied.snapshot;
    if (applied.snapshot.rows.length === 0) {
      setBackendPreviewDisplay(null);
      updateBackendPreviewStatus(
        applied.snapshot.truncated ? "limited" : "waiting",
        applied.snapshot.truncated
          ? "The bounded live preview could not include a complete row. Waiting for authoritative results."
          : "Live preview is waiting for result rows.",
      );
      return;
    }

    try {
      const adapted = adaptSearchResults(
        schema,
        applied.snapshot.rows,
        schema.resultKind === ResultSetKind.RESULT_SET_KIND_TIME_SERIES,
        timechartSpanMilliseconds(
          backendJobRef.current?.definition?.spl ?? submittedQuery,
        ) ?? undefined,
      );
      setBackendPreviewDisplay({ schema, snapshot: applied.snapshot, adapted });
      setBackendResultSchema(schema);
      setBackendResultKind(schema.resultKind);
      setActiveTab((current) => resultTabCompatibleWithKind(current, schema.resultKind)
        ? current
        : resultTabForBackendKind(schema.resultKind));
      const firstVisibleSnapshot = previous === null || previous.rows.length === 0;
      updateBackendPreviewStatus(
        "live",
        firstVisibleSnapshot
          ? `Live preview available with ${NUMBER_FORMAT.format(applied.snapshot.rows.length)} provisional rows.`
          : undefined,
      );
    } catch (error) {
      clearBackendPreview(
        "resyncing",
        "The live result preview was discarded because it could not be displayed safely.",
      );
      setBackendNotices((current) => uniqueMessages([
        ...current,
        `${error instanceof Error ? error.message : "The preview result could not be adapted."} Waiting for authoritative results.`,
      ]));
    }
  }

  function monitorBackendJob(
    initialJob: SearchJob,
    bootstrap: BackendBootstrapState,
    signal: AbortSignal,
    generation: number,
  ): Promise<SearchJob> {
    const maximumPreviewRows = bootstrap.response.limits.maximumPreviewRows;
    let previewsEnabled = bootstrap.response.searchWebsocketPath !== null
      && maximumPreviewRows > 0
      && supportsServerFeature(
        bootstrap.response,
        ServerFeature.SERVER_FEATURE_SEARCH_PREVIEW,
      );
    const negotiatedPreviewRows = previewsEnabled
      ? Math.min(maximumPreviewRows, Math.max(1, backendPageSizeRef.current))
      : 0;
    backendPreviewRowLimitRef.current = negotiatedPreviewRows;
    if (previewsEnabled) {
      updateBackendPreviewStatus(
        "waiting",
        "Live preview enabled. Waiting for result rows.",
      );
      registerBackendPreviewSchema(initialJob.resultSchema, generation, initialJob.searchJobId);
    } else {
      updateBackendPreviewStatus("disabled");
    }

    return new Promise<SearchJob>((resolve, reject) => {
      let latestPhase = backendJobPhase(initialJob.state);
      let settled = false;
      let recoveryAttempt = 0;
      let recoveryTimer: number | null = null;
      let recoveryRequest: Promise<SearchJob> | null = null;
      let subscriptionId: string | null = null;
      let sequenceGapPending = false;
      let previewSubscriptionPending = previewsEnabled;
      let previewFallbackAttempted = false;
      const cleanups: Array<() => void> = [];
      const websocketPath = bootstrap.response.searchWebsocketPath;
      const socket = websocketPath === null
        ? null
        : new SearchWebSocketClient({ path: websocketPath, baseUrl: apiBaseUrl });

      function cleanup() {
        if (recoveryTimer !== null) window.clearTimeout(recoveryTimer);
        for (const dispose of cleanups) dispose();
        if (socket !== null && subscriptionId !== null) socket.unsubscribe(subscriptionId);
        socket?.dispose();
        if (backendSocketRef.current === socket) backendSocketRef.current = null;
      }

      function finish(job: SearchJob) {
        if (settled) return;
        settled = true;
        cleanup();
        resolve(job);
      }

      function fail(error: unknown) {
        if (settled) return;
        settled = true;
        cleanup();
        reject(error);
      }

      function scheduleRecovery(delay?: number) {
        if (settled || signal.aborted || generationRef.current !== generation) return;
        if (recoveryTimer !== null) window.clearTimeout(recoveryTimer);
        const fallbackDelay = REST_RECOVERY_DELAYS_MS[Math.min(recoveryAttempt, REST_RECOVERY_DELAYS_MS.length - 1)];
        recoveryTimer = window.setTimeout(() => {
          recoveryTimer = null;
          void recoverFromRest();
        }, delay ?? fallbackDelay);
      }

      async function fetchAuthoritative(): Promise<SearchJob> {
        if (recoveryRequest !== null) return recoveryRequest;
        recoveryRequest = getAuthoritativeBackendJob(initialJob.searchJobId, signal, generation)
          .finally(() => {
            recoveryRequest = null;
          });
        return recoveryRequest;
      }

      async function recoverFromRest() {
        try {
          const job = await fetchAuthoritative();
          if (sequenceGapPending) {
            sequenceGapPending = false;
            setBackendNotices((current) => uniqueMessages([
              ...current,
              "Live job updates were resynchronized from the server after a sequence gap.",
            ]));
          }
          latestPhase = backendJobPhase(job.state);
          if (!ACTIVE_PHASES.has(backendJobPhase(job.state))) {
            finish(job);
            return;
          }
          recoveryAttempt = Math.min(recoveryAttempt + 1, REST_RECOVERY_DELAYS_MS.length - 1);
          scheduleRecovery();
        } catch (error) {
          if (signal.aborted || generationRef.current !== generation) {
            fail(error);
            return;
          }
          if (
            isHttpError(error)
            && error.status >= 400
            && error.status < 500
            && error.status !== 408
            && error.status !== 429
          ) {
            if (error.status === 410) setBackendResultsExpired(true);
            fail(error);
            return;
          }
          recoveryAttempt = Math.min(recoveryAttempt + 1, REST_RECOVERY_DELAYS_MS.length - 1);
          scheduleRecovery();
        }
      }

      function refreshWatchdog() {
        recoveryAttempt = 0;
        scheduleRecovery();
      }

      const abortListener = () => fail(signal.reason ?? new DOMException("Search was canceled.", "AbortError"));
      signal.addEventListener("abort", abortListener, { once: true });
      cleanups.push(() => signal.removeEventListener("abort", abortListener));

      if (socket === null) {
        scheduleRecovery(0);
        return;
      }

      backendSocketRef.current = socket;
      cleanups.push(socket.onEvent((event) => {
        if (settled) return;
        if (event.payload?.$case === "subscriptionAcknowledged") {
          const acknowledgedTarget = event.payload.value.target?.target;
          if (
            acknowledgedTarget?.$case === "searchJobId"
            && acknowledgedTarget.value === initialJob.searchJobId
            && event.payload.value.subscriptionId === subscriptionId
          ) {
            previewSubscriptionPending = false;
          }
          return;
        }
        if (searchJobIdForWebSocketEvent(event) !== initialJob.searchJobId) return;
        refreshWatchdog();
        switch (event.payload?.$case) {
          case "searchProgress":
            applyBackendProgress(event.payload.value, latestPhase, generation);
            break;
          case "searchStateChanged": {
            const change = event.payload.value;
            if (change.stateVersion >= backendJobVersionRef.current) {
              backendJobVersionRef.current = change.stateVersion;
              latestPhase = backendJobPhase(change.state);
              setPhase(latestPhase);
            }
            break;
          }
          case "resultSchemaAvailable": {
            const schema = event.payload.value.schema;
            if (previewsEnabled) {
              registerBackendPreviewSchema(
                schema,
                generation,
                initialJob.searchJobId,
              );
            } else if (schema !== undefined) {
              setBackendResultSchema(schema);
              if (schema.resultKind !== ResultSetKind.RESULT_SET_KIND_UNSPECIFIED) {
                setBackendResultKind(schema.resultKind);
              }
            }
            break;
          }
          case "resultPreview":
            // Opted-out subscriptions still receive zero-row continuity
            // markers so target sequences remain contiguous. They are not
            // displayable previews and must not be validated against the zero
            // negotiated row limit.
            if (previewsEnabled) {
              applyBackendResultPreview(
                event.payload.value,
                generation,
                initialJob.searchJobId,
              );
            }
            break;
          case "warning": {
            const message = event.payload.value.warning?.message;
            if (message) setBackendNotices((current) => uniqueMessages([...current, message]));
            break;
          }
          case "searchTerminal":
            latestPhase = backendJobPhase(event.payload.value.state);
            applyBackendProgress(
              event.payload.value.finalProgress,
              latestPhase,
              generation,
            );
            if (backendPreviewRef.current !== null) {
              updateBackendPreviewStatus(
                "finalizing",
                "Search complete. Replacing the live preview with authoritative results.",
              );
            }
            scheduleRecovery(0);
            break;
        }
      }));
      cleanups.push(socket.onConnectionStateChange((state) => {
        if (settled || !previewsEnabled) return;
        if (state === "reconnecting" || state === "closed") {
          if (backendPreviewRef.current?.rows.length) {
            updateBackendPreviewStatus(
              "paused",
              "Live preview paused while the connection is restored. The last provisional rows remain visible.",
            );
          }
          return;
        }
        if (state === "open" && backendPreviewStatusRef.current === "paused") {
          updateBackendPreviewStatus(
            backendPreviewRef.current?.rows.length ? "live" : "waiting",
            "Live preview connection restored.",
          );
        }
      }));
      cleanups.push(socket.onError(() => {
        if (previewsEnabled && backendPreviewRef.current?.rows.length) {
          updateBackendPreviewStatus(
            "paused",
            "Live preview paused while the connection is restored. The last provisional rows remain visible.",
          );
        }
        scheduleRecovery(0);
      }));
      cleanups.push(socket.onProtocolError((error) => {
        const previewRejected = previewsEnabled
          && previewSubscriptionPending
          && !previewFallbackAttempted
          && (
            error.code === SearchWebSocketProtocolErrorCode.SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_INVALID_COMMAND
            || error.code === SearchWebSocketProtocolErrorCode.SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_UNSUPPORTED_COMMAND
          );
        if (previewRejected) {
          previewFallbackAttempted = true;
          previewSubscriptionPending = false;
          previewsEnabled = false;
          backendPreviewRowLimitRef.current = 0;
          clearBackendPreview(
            "disabled",
            "This server declined live previews. Search progress remains connected.",
          );
          setBackendNotices((current) => uniqueMessages([
            ...current,
            "This server declined live result previews; complete results will still load normally.",
          ]));
          try {
            subscriptionId = socket.subscribe(
              searchJobTarget(initialJob.searchJobId),
              { includePreviews: false },
            );
          } catch {
            scheduleRecovery(0);
          }
          return;
        }
        setBackendNotices((current) => uniqueMessages([...current, `Live job updates failed: ${error.message}`]));
        scheduleRecovery(0);
      }));
      cleanups.push(socket.onSequenceGap((gap) => {
        if (
          gap.target.target?.$case !== "searchJobId"
          || gap.target.target.value !== initialJob.searchJobId
        ) return;
        sequenceGapPending = true;
        clearBackendPreview(
          previewsEnabled ? "resyncing" : "disabled",
          "Live preview was cleared while job updates are resynchronized.",
        );
        setBackendNotices((current) => uniqueMessages([...current, "Live job updates skipped a sequence; resynchronizing from the server…"]));
        scheduleRecovery(0);
      }));
      cleanups.push(socket.onResynchronizationRequired(async (notice) => {
        if (notice.target.target?.$case !== "searchJobId"
          || notice.target.target.value !== initialJob.searchJobId
          || settled) return;
        clearBackendPreview(
          previewsEnabled ? "resyncing" : "disabled",
          "Live preview was cleared while job updates are resynchronized.",
        );
        backendPreviewSchemasRef.current.clear();
        const job = await fetchAuthoritative();
        latestPhase = backendJobPhase(job.state);
        const schemaReady = !previewsEnabled
          || job.resultSchema === undefined
          || registerBackendPreviewSchema(
            job.resultSchema,
            generation,
            initialJob.searchJobId,
          );
        notice.acknowledge();
        if (!ACTIVE_PHASES.has(backendJobPhase(job.state))) finish(job);
        else {
          if (previewsEnabled && schemaReady) {
            updateBackendPreviewStatus(
              "waiting",
              "Job updates resynchronized. Waiting for a fresh live preview.",
            );
          }
          refreshWatchdog();
        }
      }));
      subscriptionId = socket.subscribe(
        searchJobTarget(initialJob.searchJobId),
        previewsEnabled
          ? {
              includePreviews: true,
              previewRowLimit: negotiatedPreviewRows,
            }
          : { includePreviews: false },
      );
      scheduleRecovery();
    });
  }

  async function applyBackendTerminalJob(
    job: SearchJob,
    bootstrap: BackendBootstrapState,
    signal: AbortSignal,
    generation: number,
  ) {
    const terminalPhase = backendJobPhase(job.state);
    if (terminalPhase === "completed") {
      setPhase("finalizing");
      setProgress(96);
      if (backendPreviewRef.current !== null) {
        updateBackendPreviewStatus(
          "finalizing",
          "Search complete. Replacing the live preview with authoritative results.",
        );
      }
      try {
        await fetchInitialBackendResults(job, bootstrap, signal, generation);
      } catch (error) {
        if (
          signal.aborted
          || generationRef.current !== generation
          || backendJobIdRef.current !== job.searchJobId
        ) throw error;
        if (isHttpStatus(error, 410)) {
          clearBackendPreview(
            "disabled",
            "Search results expired. Live preview rows were discarded.",
          );
          setBackendAuthoritativeResultsReady(false);
          setBackendResultsExpired(true);
          setPhase("expired");
          setProgress(100);
          setBackendNotices((current) => uniqueMessages([
            ...current,
            "These retained search results have expired. Run the search again to refresh them.",
          ]));
          showToast("Search results expired. Run the search again.", "warning");
          void refreshBackendHistory(bootstrap);
          return;
        }
        setPhase("completed");
        setProgress(100);
        const message = error instanceof Error
          ? error.message
          : "The authoritative result snapshot could not be loaded.";
        if (backendPreviewRef.current !== null) {
          updateBackendPreviewStatus(
            "finalization-error",
            "Search completed, but authoritative results could not be loaded. The visible rows remain provisional.",
          );
          setBackendNotices((current) => uniqueMessages([
            ...current,
            `Authoritative results could not be loaded: ${message} The visible preview remains provisional and cannot be exported.`,
          ]));
        } else {
          updateBackendPreviewStatus("disabled");
          setBackendNotices((current) => uniqueMessages([
            ...current,
            `Search completed, but authoritative results could not be loaded: ${message}`,
          ]));
        }
        showToast("Search completed, but its authoritative results could not be loaded.", "warning");
        void refreshBackendHistory(bootstrap);
        return;
      }
      if (generationRef.current !== generation || backendJobIdRef.current !== job.searchJobId) return;
      setPhase("completed");
      setProgress(100);
      const kind = job.resultKind !== ResultSetKind.RESULT_SET_KIND_UNSPECIFIED
        ? job.resultKind
        : job.resultSchema?.resultKind ?? ResultSetKind.RESULT_SET_KIND_UNSPECIFIED;
      const savedPreferredTab = pendingSavedPreferredTabRef.current;
      setActiveTab(
        savedPreferredTab !== null && resultTabCompatibleWithKind(savedPreferredTab, kind)
          ? savedPreferredTab
          : resultTabForBackendKind(kind),
      );
      pendingSavedPreferredTabRef.current = null;
      if (kind !== ResultSetKind.RESULT_SET_KIND_EVENTS) {
        pendingSavedSelectedFieldsRef.current = null;
      }
      const savedVisualization = pendingSavedVisualizationRef.current;
      pendingSavedVisualizationRef.current = undefined;
      if (savedVisualization !== undefined) {
        // restoreBackendPresentation already applied the supported settings.
      } else if (kind === ResultSetKind.RESULT_SET_KIND_TIME_SERIES) {
        setChartStyle("line");
        setChartTitle("Event volume over time");
      } else {
        setChartStyle("column");
        setChartTitle("Event volume by level");
      }
      void fetchAuthoritativeBackendMetadata(job, bootstrap, generation);
    } else if (terminalPhase === "failed") {
      clearBackendPreview("disabled", "Search failed. Live preview rows were discarded.");
      setBackendAuthoritativeResultsReady(false);
      pendingSavedPreferredTabRef.current = null;
      pendingSavedSelectedFieldsRef.current = null;
      pendingSavedVisualizationRef.current = undefined;
      preservedSavedVisualizationRef.current = null;
      showToast(job.failure?.message || "The backend search failed.", "warning");
    } else if (terminalPhase === "canceled") {
      clearBackendPreview("disabled", "Search canceled. Live preview rows were discarded.");
      setBackendAuthoritativeResultsReady(false);
      pendingSavedPreferredTabRef.current = null;
      pendingSavedSelectedFieldsRef.current = null;
      pendingSavedVisualizationRef.current = undefined;
      showToast("Search canceled.", "warning");
    } else if (terminalPhase === "expired") {
      clearBackendPreview("disabled", "Search expired. Live preview rows were discarded.");
      setBackendAuthoritativeResultsReady(false);
      pendingSavedPreferredTabRef.current = null;
      pendingSavedSelectedFieldsRef.current = null;
      pendingSavedVisualizationRef.current = undefined;
      setBackendResultsExpired(true);
      showToast("Search results expired. Run the search again.", "warning");
    }
    void refreshBackendHistory(bootstrap);
  }

  async function runBackendSearch(nextQuery: string, rangeOverride: TimeRange = timeRange) {
    const generation = ++generationRef.current;
    const launchTimeRange = rangeOverride;
    const launchSavedSearchId = activeSavedSearchIdRef.current;
    const launchHistoryEntry = launchSavedSearchId === null
      ? backendHistoryRerunRef.current
      : null;
    if (launchSavedSearchId === null && launchHistoryEntry === null) {
      pendingSavedPreferredTabRef.current = null;
      pendingSavedVisualizationRef.current = undefined;
      preservedSavedVisualizationRef.current = null;
      pendingSavedSelectedFieldsRef.current = fields.length > 0
        ? new Set(fields.filter((field) => field.selected).map((field) => field.name))
        : null;
    }
    const supersededJobId = backendJobIdRef.current;
    const shouldCancelSupersededJob = supersededJobId !== null && isRunning;
    backendAbortRef.current?.abort();
    backendPageAbortRef.current?.abort();
    backendMetadataAbortRef.current?.abort();
    backendFieldSummaryAbortRef.current?.abort();
    backendSocketRef.current?.dispose();
    backendSocketRef.current = null;
    const controller = new AbortController();
    backendAbortRef.current = controller;
    backendJobIdRef.current = null;
    backendJobRef.current = null;
    backendJobVersionRef.current = 0n;
    backendCancelPendingRef.current = false;
    backendCancelRequestedRef.current = false;
    setCancelError(null);
    clearTimers();
    setToast(null);
    setMenu(null);
    setCompletionOpen(false);
    setSubmittedQuery(nextQuery);
    setSubmittedTimeRange(launchTimeRange);
    setQuery(nextQuery);
    setPhase("queued");
    setProgress(4);
    setElapsed("0.00 s");
    setScannedRows(0);
    setScannedRowsApproximate(false);
    setScannedBytes("0 B");
    setResolvedTimeRangeLabel(null);
    setBackendEventCount(0);
    setBackendPrimaryCountLabel("events");
    setBackendPrimaryCountPrefix("");
    setBackendEvents([]);
    setBackendStatistics([]);
    setBackendStatisticsTable(null);
    setGenericStatsSort(null);
    setBackendTimeline([]);
    resetBackendResultState();
    setFields([]);
    setActiveField(null);
    setTimelineStart(null);
    setTimelineEnd(null);
    setEventPage(1);

    try {
      if (shouldCancelSupersededJob && supersededJobId !== null) {
        try {
          await apiClient.search.cancel(
            { searchJobId: supersededJobId, reason: undefined },
            { timeoutMs: 5_000 },
          );
        } catch (error) {
          if (generationRef.current === generation) {
            showToast(error instanceof Error
              ? `The previous search could not be canceled: ${error.message}`
              : "The previous search could not be canceled.", "warning");
          }
        }
      }
      const bootstrap = await ensureBackendBootstrap();
      if (generationRef.current !== generation || controller.signal.aborted) return;
      const timeRangeError = serverTimeRangeValidationError(
        launchTimeRange,
        currentBackendServerTime(bootstrap),
      );
      if (timeRangeError !== null) throw new Error(timeRangeError);
      const savedExecution = launchSavedSearchId === null
        ? undefined
        : backendSavedSearchesRef.current.get(launchSavedSearchId);
      const launchAppId = savedExecution !== undefined
        ? savedExecution.search.appId
        : launchHistoryEntry !== null
          ? launchHistoryEntry.search.appId
          : bootstrap.response.selectedAppId ?? undefined;
      if (
        launchAppId !== undefined
        && !bootstrap.response.apps.some((app) => app.appId === launchAppId)
      ) {
        throw new Error("The persisted search belongs to an app that is not available in this backend session.");
      }
      const response = await apiClient.search.create({
        definition: {
          spl: nextQuery,
          // Keep relative/calendar intent intact. The server resolves it once,
          // against its own authoritative clock, and persists both forms.
          timeRange: backendTimeRangeIntent(
            launchTimeRange,
            savedExecution !== undefined || launchHistoryEntry !== null,
          ),
          appId: launchAppId,
          indexScope: backendDispatchIndexScope(
            nextQuery,
            bootstrap,
            launchSavedSearchId,
            launchHistoryEntry,
          ),
          preferredResultTab: 0,
          selectedFields: [],
          visualization: undefined,
        },
        // The current create handler only accepts persisted SAVED_SEARCH
        // provenance. HISTORY_RERUN exists in the wire model but is rejected by
        // internal/server/api.go, so history drafts are dispatched as ad hoc.
        source: savedExecution === undefined
          ? undefined
          : {
            origin: SearchJobOrigin.SEARCH_JOB_ORIGIN_SAVED_SEARCH,
            savedSearchId: savedExecution.id,
            historySearchId: undefined,
            dashboardId: undefined,
          },
        options: undefined,
      });
      if (backendHistoryRerunRef.current?.id === launchHistoryEntry?.id) {
        backendHistoryRerunRef.current = null;
      }
      let job = response.searchJob;
      if (job === undefined || job.searchJobId.length === 0) throw new Error("The server did not return a search job ID.");
      if (generationRef.current !== generation || controller.signal.aborted) {
        void apiClient.search.cancel(
          { searchJobId: job.searchJobId, reason: undefined },
          { timeoutMs: 5_000 },
        ).catch(() => undefined);
        return;
      }
      backendJobIdRef.current = job.searchJobId;
      applyBackendJob(job, generation);
      if (backendCancelRequestedRef.current) {
        try {
          const cancellation = await apiClient.search.cancel({
            searchJobId: job.searchJobId,
            reason: undefined,
          });
          if (generationRef.current !== generation) return;
          if (cancellation.searchJob === undefined) {
            throw new Error("The server returned an empty cancellation response.");
          }
          job = cancellation.searchJob;
          applyBackendJob(job, generation);
          await applyBackendTerminalJob(job, bootstrap, controller.signal, generation);
        } catch (error) {
          if (generationRef.current !== generation || controller.signal.aborted) return;
          const message = error instanceof Error
            ? `Search cancellation failed: ${error.message}`
            : "The server could not cancel this search.";
          setCancelError(message);
          showToast(message, "warning");
          job = await getAuthoritativeBackendJob(job.searchJobId, controller.signal, generation);
          if (ACTIVE_PHASES.has(backendJobPhase(job.state))) {
            job = await monitorBackendJob(job, bootstrap, controller.signal, generation);
          }
          await applyBackendTerminalJob(job, bootstrap, controller.signal, generation);
        } finally {
          if (generationRef.current === generation) {
            backendCancelRequestedRef.current = false;
            backendCancelPendingRef.current = false;
          }
        }
        return;
      }
      if (ACTIVE_PHASES.has(backendJobPhase(job.state))) {
        job = await monitorBackendJob(job, bootstrap, controller.signal, generation);
      }

      if (generationRef.current !== generation) return;
      await applyBackendTerminalJob(job, bootstrap, controller.signal, generation);
    } catch (error) {
      if (controller.signal.aborted || generationRef.current !== generation) return;
      if (backendCancelRequestedRef.current && backendJobIdRef.current === null) {
        backendCancelRequestedRef.current = false;
        backendCancelPendingRef.current = false;
        setPhase("canceled");
        setProgress(100);
        showToast("Search canceled before dispatch completed.", "warning");
        return;
      }
      if (isHttpStatus(error, 410)) {
        clearBackendPreview(
          "disabled",
          "Search results expired. Live preview rows were discarded.",
        );
        setBackendAuthoritativeResultsReady(false);
        setBackendResultsExpired(true);
        setPhase("expired");
        setProgress(100);
        setBackendNotices((current) => uniqueMessages([
          ...current,
          "These retained search results have expired. Run the search again to refresh them.",
        ]));
        showToast("Search results expired. Run the search again.", "warning");
        return;
      }
      clearBackendPreview("disabled", "Search updates failed. Live preview rows were discarded.");
      setBackendAuthoritativeResultsReady(false);
      setPhase("failed");
      setProgress(100);
      showToast(error instanceof Error ? error.message : "Unable to run the backend search.", "warning");
    } finally {
      if (backendAbortRef.current === controller) backendAbortRef.current = null;
    }
  }

  searchRunnerRef.current = (queryText, range) => {
    runSearch(queryText, range);
  };

  function backendWorkspaceTransitionBlocked(): boolean {
    if (!backendEnabled) return false;
    if (persistedLaunchPendingRef.current) {
      showToast("Wait for the persisted search to finish opening.", "warning");
      return true;
    }
    if (backendObjectMutationRef.current) {
      showToast("Wait for the current saved-search or history change to finish.", "warning");
      return true;
    }
    if (appSwitchAbortRef.current !== null) {
      showToast("Wait for the app switch to finish.", "warning");
      return true;
    }
    return false;
  }

  function runSearch(queryOverride?: string, rangeOverride: TimeRange = timeRange) {
    if (searchLaunchRef.current) return;
    if (backendWorkspaceTransitionBlocked()) return;
    const nextQuery = queryOverride ?? query;
    const nextDiagnostic = getQueryDiagnostic(nextQuery);
    if (nextDiagnostic !== null && (!backendEnabled || nextDiagnostic.kind !== "unsupported")) {
      setQuery(nextQuery);
      setCompletionOpen(false);
      showToast(nextDiagnostic.message, "warning");
      focusEditor(nextQuery.trim().length === 0 ? 0 : nextQuery.length);
      return;
    }
    if (
      exportStage !== "configure"
      || exportRequestId !== null
      || serverExportJobRef.current !== null
      || exportDownloadState.status !== "idle"
    ) {
      resetExport();
    }
    searchLaunchRef.current = true;
    window.setTimeout(() => {
      searchLaunchRef.current = false;
    }, 0);
    if (backendEnabled) {
      void runBackendSearch(nextQuery, rangeOverride);
      return;
    }
    generationRef.current += 1;
    const generation = generationRef.current;
    const launchTimeRange = rangeOverride;
    clearTimers();
    setToast(null);
    setMenu(null);
    setCompletionOpen(false);
    setSubmittedQuery(nextQuery);
    setSubmittedTimeRange(launchTimeRange);
    setQuery(nextQuery);
    setPhase("queued");
    setProgress(4);
    setElapsed("0.00 s");
    setScannedRows(0);
    setScannedRowsApproximate(false);
    setScannedBytes("0 B");
    setResolvedTimeRangeLabel(null);
    setTimelineStart(null);
    setTimelineEnd(null);
    setEventPage(1);

    schedule(() => {
      if (generationRef.current !== generation) return;
      setPhase("parsing");
      setProgress(14);
      setElapsed("0.08 s");
    }, 120);

    schedule(() => {
      if (generationRef.current !== generation) return;
      setPhase("planning");
      setProgress(27);
      setElapsed("0.19 s");
    }, 280);
    schedule(() => {
      if (generationRef.current !== generation) return;
      setPhase("running");
      setProgress(58);
      setElapsed("0.74 s");
      setScannedRows(91_402);
      setScannedBytes("57.4 MB");
    }, 520);
    schedule(() => {
      if (generationRef.current !== generation) return;
      setProgress(82);
      setElapsed("1.31 s");
      setScannedRows(218_775);
      setScannedBytes("142 MB");
    }, 930);
    schedule(() => {
      if (generationRef.current !== generation) return;
      setPhase("finalizing");
      setProgress(94);
      setElapsed("1.66 s");
      setScannedRows(284_219);
      setScannedBytes("186 MB");
    }, 1260);
    schedule(() => {
      if (generationRef.current !== generation) return;
      setPhase("completed");
      setProgress(100);
      setElapsed("1.82 s");
      setActiveTab(resultTabForQuery(nextQuery));
      if (hasPipelineCommand(nextQuery, "timechart")) {
        setChartStyle("line");
        setChartTitle("Event volume over time");
      } else {
        setChartStyle("column");
        setChartTitle("Event volume by level");
      }
      setHistory((current) => [
        {
          id: `hist-${Date.now()}`,
          query: nextQuery,
          timeRange: launchTimeRange.label,
          earliest: launchTimeRange.earliest,
          latest: launchTimeRange.latest,
          state: "Completed",
          events: eventCountForQuery(nextQuery),
          duration: "1.82 s",
          ranAt: "Just now",
        },
        ...current,
      ]);
    }, 1540);
  }

  function cancelSearch() {
    if (!isRunning) return;
    if (backendEnabled) {
      const searchJobId = backendJobIdRef.current;
      if (backendCancelPendingRef.current) return;
      backendCancelPendingRef.current = true;
      backendCancelRequestedRef.current = true;
      setCancelError(null);
      showToast("Canceling search…");
      if (searchJobId === null) return;
      const generation = ++generationRef.current;
      backendAbortRef.current?.abort();
      backendPageAbortRef.current?.abort();
      backendSocketRef.current?.dispose();
      backendSocketRef.current = null;
      void apiClient.search.cancel({ searchJobId, reason: undefined })
        .then(async (response) => {
          if (generationRef.current !== generation || backendJobIdRef.current !== searchJobId) return;
          const job = response.searchJob;
          if (job === undefined) throw new Error("The server returned an empty cancellation response.");
          applyBackendJob(job, generation);
          const bootstrap = await ensureBackendBootstrap();
          await applyBackendTerminalJob(job, bootstrap, new AbortController().signal, generation);
        })
        .catch(async (error: unknown) => {
          if (generationRef.current !== generation || backendJobIdRef.current !== searchJobId) return;
          const message = error instanceof Error
            ? `Search cancellation failed: ${error.message}`
            : "The server could not cancel this search.";
          setCancelError(message);
          showToast(message, "warning");
          const controller = new AbortController();
          backendAbortRef.current = controller;
          try {
            const bootstrap = await ensureBackendBootstrap();
            let job = await getAuthoritativeBackendJob(searchJobId, controller.signal, generation);
            if (ACTIVE_PHASES.has(backendJobPhase(job.state))) {
              job = await monitorBackendJob(job, bootstrap, controller.signal, generation);
            }
            await applyBackendTerminalJob(job, bootstrap, controller.signal, generation);
          } catch (recoveryError) {
            if (!controller.signal.aborted && generationRef.current === generation) {
              showToast(recoveryError instanceof Error ? recoveryError.message : "Unable to resynchronize the search job.", "warning");
            }
          } finally {
            if (backendAbortRef.current === controller) backendAbortRef.current = null;
          }
        })
        .finally(() => {
          if (generationRef.current === generation) {
            backendCancelPendingRef.current = false;
            backendCancelRequestedRef.current = false;
          }
        });
      return;
    }
    generationRef.current += 1;
    clearTimers();
    setPhase("canceled");
    setProgress(100);
    setHistory((current) => [
      {
        id: `hist-${Date.now()}`,
        query: submittedQuery,
        timeRange: submittedTimeRange.label,
        earliest: submittedTimeRange.earliest,
        latest: submittedTimeRange.latest,
        state: "Canceled",
        events: 0,
        duration: elapsed,
        ranAt: "Just now",
      },
      ...current,
    ]);
    showToast("Search canceled.", "warning");
  }

  function handleEditorKeyDown(event: KeyboardEvent<HTMLTextAreaElement>) {
    if ((event.metaKey || event.ctrlKey) && event.key === "Enter") {
      event.preventDefault();
      if (backendWorkspaceTransitionBlocked()) return;
      if (runDisabledReason !== null) {
        showToast(runDisabledReason, "warning");
        return;
      }
      if (dirty) timelineZoomParentRef.current = null;
      runSearch();
      return;
    }
    if (event.ctrlKey && event.key === " ") {
      event.preventDefault();
      const caret = event.currentTarget.selectionStart;
      if (isCursorInQuotedValue(query, caret)) {
        setCompletionOpen(false);
        return;
      }
      setEditorCaret(caret);
      setCompletionIndex(0);
      setCompletionOpen(true);
      return;
    }
    if (!completionOpen) return;
    if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      if (filteredCompletions.length === 0) return;
      setCompletionIndex((current) => {
        const delta = event.key === "ArrowDown" ? 1 : -1;
        return (current + delta + filteredCompletions.length) % filteredCompletions.length;
      });
      return;
    }
    if ((event.key === "Enter" || event.key === "Tab") && filteredCompletions.length > 0) {
      event.preventDefault();
      insertCompletion(filteredCompletions[completionIndex]?.insertion ?? filteredCompletions[0].insertion);
      return;
    }
    if (event.key === "Escape") {
      event.preventDefault();
      setCompletionOpen(false);
    }
  }

  function insertCompletion(insertion: string) {
    const editor = editorRef.current;
    const selectionStart = editor?.selectionStart ?? editorCaret;
    const selectionEnd = editor?.selectionEnd ?? selectionStart;
    if (isCursorInQuotedValue(query, selectionStart)) {
      setCompletionOpen(false);
      return;
    }
    const context = completionContextAt(query, selectionStart);
    let nextQuery: string;
    let nextCaret: number;

    if (context !== null) {
      nextQuery = `${query.slice(0, context.fragmentStart)}${insertion}${query.slice(Math.max(context.fragmentEnd, selectionEnd))}`;
      nextCaret = context.fragmentStart + insertion.length;
    } else {
      const before = query.slice(0, selectionStart);
      const after = query.slice(selectionEnd);
      const separator = before.length === 0 || before.endsWith("\n") ? "" : "\n";
      const inserted = `${separator}| ${insertion}`;
      nextQuery = `${before}${inserted}${after}`;
      nextCaret = before.length + inserted.length;
    }

    setQuery(nextQuery);
    backendHistoryRerunRef.current = null;
    setEditorCaret(nextCaret);
    setCompletionOpen(false);
    focusEditor(nextCaret);
  }

  function handleEditorChange(event: ChangeEvent<HTMLTextAreaElement>) {
    const nextQuery = event.target.value;
    const caret = event.target.selectionStart;
    const context = completionContextAt(nextQuery, caret);
    setQuery(nextQuery);
    backendHistoryRerunRef.current = null;
    if (modal === "time") setModal(null);
    setEditorCaret(caret);
    setCompletionIndex(0);
    setCompletionOpen(context !== null && context.followsPipeline);
  }

  function handleEditorScroll(event: UIEvent<HTMLTextAreaElement>) {
    const editor = event.currentTarget;
    if (highlightRef.current !== null) {
      highlightRef.current.scrollTop = editor.scrollTop;
      highlightRef.current.scrollLeft = editor.scrollLeft;
    }
    if (gutterLinesRef.current !== null) {
      gutterLinesRef.current.style.transform = `translateY(${-editor.scrollTop}px)`;
    }
  }

  function fixDiagnostic(currentDiagnostic: SplDiagnostic) {
    const nextQuery = applyDiagnosticFix(query, currentDiagnostic);
    setQuery(nextQuery);
    backendHistoryRerunRef.current = null;
    setCompletionOpen(false);
    focusEditor(nextQuery.length);
  }

  function applyPivot(field: string, value: DemoScalar, mode: PivotMode, runImmediately = false) {
    if (backendWorkspaceTransitionBlocked()) return;
    const nextQuery = applyFieldPivot(
      query,
      field,
      value,
      mode,
      mode === "new" && backendEnabled
        ? backendJobRef.current?.effectiveIndexScope
        : undefined,
    );
    if (nextQuery === query) {
      showToast(`The field “${field}” cannot be represented safely in this SPL version.`, "warning");
      return;
    }
    setQuery(nextQuery);
    backendHistoryRerunRef.current = null;
    setEditorCaret(nextQuery.length);
    setActiveField(null);
    if (mode === "new") {
      activeSavedSearchIdRef.current = null;
      setActiveSavedSearchId(null);
      backendHistoryRerunRef.current = null;
      pendingSavedPreferredTabRef.current = null;
      pendingSavedSelectedFieldsRef.current = null;
      pendingSavedVisualizationRef.current = undefined;
    }
    if (runImmediately || mode === "new") {
      timelineZoomParentRef.current = null;
      runSearch(nextQuery);
    }
    else showToast(mode === "exclude" ? `Excluded ${field}=${formatFieldValue(value)} from the draft.` : `Added ${field} to the draft.`, "success");
    focusEditor(nextQuery.length);
  }

  function toggleField(fieldName: string) {
    setFields((current) => current.map((field) => (field.name === fieldName ? { ...field, selected: !field.selected } : field)));
  }

  function toggleEvent(eventId: string) {
    setExpandedEvents((current) => {
      const next = new Set(current);
      if (next.has(eventId)) next.delete(eventId);
      else next.add(eventId);
      return next;
    });
  }

  async function openBackendEventPage(pageNumber: number) {
    const job = backendJobRef.current;
    const bootstrap = backendBootstrapRef.current;
    const generation = generationRef.current;
    const pageSize = backendPageSizeRef.current;
    if (!backendEnabled || job === null || bootstrap === null || phase !== "completed") return;
    const targetPage = Math.max(1, pageNumber);
    if (!backendPageTokensRef.current.has(`${pageSize}:${targetPage}`)
      && !backendResultPagesRef.current.has(`${pageSize}:${targetPage}`)) {
      showToast("Load the preceding result page before following its cursor.", "warning");
      return;
    }
    backendPageAbortRef.current?.abort();
    const controller = new AbortController();
    backendPageAbortRef.current = controller;
    try {
      await fetchBackendResultPage(
        job,
        targetPage,
        pageSize,
        bootstrap,
        controller.signal,
        generation,
      );
      if (generationRef.current === generation && backendJobIdRef.current === job.searchJobId) {
        setEventPage(targetPage);
      }
    } catch (error) {
      if (controller.signal.aborted || generationRef.current !== generation) return;
      if (isHttpStatus(error, 410)) {
        setBackendResultsExpired(true);
        setPhase("expired");
        setBackendNotices((current) => uniqueMessages([...current, "These retained search results have expired. Run the search again to refresh them."]));
      } else {
        showToast(error instanceof Error ? error.message : "Unable to load that result page.", "warning");
      }
    } finally {
      if (backendPageAbortRef.current === controller) backendPageAbortRef.current = null;
    }
  }

  const changeEventPage: Dispatch<SetStateAction<number>> = (update) => {
    if (!backendEnabled) {
      setEventPage(update);
      return;
    }
    const nextPage = typeof update === "function" ? update(eventPage) : update;
    void openBackendEventPage(nextPage);
  };

  const changeEventPageSize: Dispatch<SetStateAction<number>> = (update) => {
    const nextSize = typeof update === "function" ? update(eventPageSize) : update;
    setEventPageSize(nextSize);
    setEventPage(1);
    if (!backendEnabled) return;
    const bootstrap = backendBootstrapRef.current;
    if (bootstrap === null) return;
    const pageSize = normalizedBackendPageSize(nextSize, bootstrap);
    backendPageAbortRef.current?.abort();
    backendPageSizeRef.current = pageSize;
    setBackendResultPageSize(pageSize);
    backendResultPagesRef.current.clear();
    backendPageTokensRef.current.clear();
    backendPageStartsRef.current.clear();
    backendResultPageTokensSeenRef.current.clear();
    backendPageTokensRef.current.set(`${pageSize}:1`, undefined);
    backendPageStartsRef.current.set(`${pageSize}:1`, 1);
    void openBackendEventPage(1);
  };

  function startTimelineDrag(event: PointerEvent<HTMLDivElement>) {
    const index = timelineIndexFromPointer(event, timelinePoints.length);
    if (index === null) return;
    event.currentTarget.setPointerCapture(event.pointerId);
    setDraggingTimeline(true);
    setTimelineStart(index);
    setTimelineEnd(index);
  }

  function moveTimelineDrag(event: PointerEvent<HTMLDivElement>) {
    if (!draggingTimeline) return;
    const index = timelineIndexFromPointer(event, timelinePoints.length);
    if (index !== null) setTimelineEnd(index);
  }

  function endTimelineDrag(event: PointerEvent<HTMLDivElement>) {
    if (draggingTimeline && event.currentTarget.hasPointerCapture(event.pointerId)) {
      event.currentTarget.releasePointerCapture(event.pointerId);
    }
    setDraggingTimeline(false);
  }

  function zoomTimeline() {
    if (backendWorkspaceTransitionBlocked()) return;
    if (timelineSelection === null || !timelineSelectionZoomable) {
      if (timelineSelection !== null) {
        showToast("This bucket has no authoritative end boundary, so it cannot be used for time zoom.", "warning");
      }
      return;
    }
    const first = timelinePoints[timelineSelection[0]];
    const last = timelinePoints[timelineSelection[1]];
    if (first === undefined || last === undefined) return;
    const intervalEndLabel = timelinePoints[timelineSelection[1] + 1]?.label ?? last.latest ?? timelineBoundaryLabel(timelineSelection[1] + 1);
    const latest = last.latest ?? timelinePoints[timelineSelection[1] + 1]?.earliest;
    if (first.earliest === undefined || latest === undefined) return;
    const narrowedRange = {
      label: `${first.label} – ${intervalEndLabel}`,
      earliest: first.earliest,
      latest,
      timezone: submittedTimeRange.timezone,
    };
    timelineZoomParentRef.current = submittedTimeRange;
    setTimeRange(narrowedRange);
    setDraftTimeRange(narrowedRange);
    setTimelineStart(null);
    setTimelineEnd(null);
    runSearch(submittedQuery, narrowedRange);
  }

  function zoomOutTimeline() {
    if (backendWorkspaceTransitionBlocked()) return;
    const parentRange = timelineZoomParentRef.current;
    if (parentRange === null) return;
    timelineZoomParentRef.current = null;
    setTimeRange(parentRange);
    setDraftTimeRange(parentRange);
    runSearch(submittedQuery, parentRange);
  }

  function openSaveDialog(returnFocus?: HTMLElement | null, forceNew = activeSavedSearchId === null) {
    saveDialogReturnFocusRef.current = returnFocus
      ?? (document.activeElement instanceof HTMLElement ? document.activeElement : null);
    const existing = savedSearches.find((item) => item.id === activeSavedSearchId);
    setSaveAsNew(forceNew);
    setSaveName(existing === undefined
      ? "Production log investigation"
      : forceNew
        ? duplicateSavedSearchName(existing.name, 1)
        : existing.name);
    setSaveDescription(savedSearches.find((item) => item.id === activeSavedSearchId)?.description ?? "");
    setSaveError(null);
    setModal("save");
    setMenu(null);
  }

  function savedSearchIndexScope(
    searchText: string,
    bootstrap: BackendBootstrapState,
    existing?: ServerSavedSearch,
  ): string[] {
    const historyEntry = existing === undefined ? backendHistoryRerunRef.current : null;
    const persistedSearch = existing?.search ?? historyEntry?.search;
    const persistedScope = existing?.search.indexScope ?? historyEntry?.effectiveIndexScope ?? [];
    if (persistedSearch !== undefined) {
      if (searchText === persistedSearch.spl && persistedScope.length > 0) {
        return resolveExactIndexScope({
          spl: searchText,
          bootstrap: bootstrap.response,
          requestedIndexes: persistedScope,
        });
      }
      const selectors = indexSelectorsFromSPL(searchText);
      const savedAppDefaults = bootstrap.response.apps
        .find((app) => app.appId === persistedSearch.appId)
        ?.defaultIndexNames ?? [];
      const authorizedIndexes = new Set(
        bootstrap.response.indexes
          .filter((index) => index.searchable)
          .map((index) => index.name),
      );
      const authorizedSavedAppDefaults = savedAppDefaults
        .filter((indexName) => authorizedIndexes.has(indexName));
      const baseline = persistedScope.length > 0
        ? persistedScope
        : authorizedSavedAppDefaults.length > 0
          ? authorizedSavedAppDefaults
          : [...authorizedIndexes];
      const requestedIndexes = selectors.length === 0
        ? baseline
        : splIndexScopeIsExhaustive(searchText)
          ? selectors
          : [...new Set([...baseline, ...selectors])];
      return resolveExactIndexScope({
        spl: searchText,
        bootstrap: bootstrap.response,
        requestedIndexes,
      });
    }
    return backendIndexScope(searchText, bootstrap);
  }

  function backendDispatchIndexScope(
    searchText: string,
    bootstrap: BackendBootstrapState,
    savedSearchId: string | null,
    historyEntry: ServerSearchHistoryEntry | null,
  ): string[] {
    if ((historyEntry?.effectiveIndexScope.length ?? 0) > 0) {
      return resolveExactIndexScope({
        spl: searchText,
        bootstrap: bootstrap.response,
        requestedIndexes: historyEntry?.effectiveIndexScope,
      });
    }
    const saved = savedSearchId === null
      ? undefined
      : backendSavedSearchesRef.current.get(savedSearchId);
    if ((saved?.search.indexScope.length ?? 0) > 0) {
      return resolveExactIndexScope({
        spl: searchText,
        bootstrap: bootstrap.response,
        requestedIndexes: saved?.search.indexScope,
      });
    }
    const persistedAppId = saved !== undefined
      ? saved.search.appId
      : historyEntry?.search.appId;
    if (saved !== undefined || historyEntry !== null) {
      const searchableIndexes = bootstrap.response.indexes
        .filter((index) => index.searchable)
        .map((index) => index.name);
      const searchableIndexNames = new Set(searchableIndexes);
      const persistedAppDefaults = persistedAppId === undefined
        ? []
        : bootstrap.response.apps
          .find((app) => app.appId === persistedAppId)
          ?.defaultIndexNames
          .filter((indexName) => searchableIndexNames.has(indexName)) ?? [];
      const selectors = indexSelectorsFromSPL(searchText);
      const baseline = persistedAppDefaults.length > 0
        ? persistedAppDefaults
        : searchableIndexes;
      return resolveExactIndexScope({
        spl: searchText,
        bootstrap: bootstrap.response,
        requestedIndexes: selectors.length > 0 && splIndexScopeIsExhaustive(searchText)
          ? selectors
          : [...new Set([...baseline, ...selectors])],
      });
    }
    return backendIndexScope(searchText, bootstrap);
  }

  async function saveBackendSearch(
    forceCreate: boolean,
    nameOverride = saveName,
    descriptionOverride = saveDescription,
  ) {
    const trimmedName = nameOverride.trim();
    const trimmedDescription = descriptionOverride.trim();
    if (trimmedName.length === 0 || objectMutation !== null) return;
    if (appSwitchAbortRef.current !== null || backendObjectMutationRef.current) {
      showToast("Wait for the current app operation to finish before saving.", "warning");
      return;
    }
    backendObjectMutationRef.current = true;
    setSaveError(null);
    setObjectMutation({ kind: "save" });
    let operation: "create" | "update" = "create";
    let updatedSavedSearchId: string | null = null;
    try {
      const bootstrap = await ensureBackendBootstrap();
      const existing = forceCreate || activeSavedSearchId === null
        ? undefined
        : backendSavedSearchesRef.current.get(activeSavedSearchId);
      operation = existing === undefined ? "create" : "update";
      updatedSavedSearchId = existing?.id ?? null;
      const schemaFields = backendResultSchema?.columns
        .map((column) => column.fieldName)
        .filter(Boolean) ?? [];
      const existingVisualization = existing?.search.visualization;
      const visualization: VisualizationSpec | undefined = preservedSavedVisualizationRef.current
        ?? (activeTab === "visualization" || visualizationEditedRef.current
        ? {
          ...existingVisualization,
          type: visualizationTypeForChartStyle(
            isTimechartResult && timechartValueColumns.length > 1 ? "line" : chartStyle,
          ),
          title: chartTitle.trim() || undefined,
          xField: isTimechartResult
            ? schemaFields.find((field) => field === "_time") ?? schemaFields[0] ?? existingVisualization?.xField
            : existingVisualization?.xField ?? schemaFields[0],
          yFields: timechartValueColumns.length > 0
            ? timechartValueColumns
            : existingVisualization?.yFields ?? schemaFields.slice(1),
          stackMode: existingVisualization?.stackMode
            ?? VisualizationStackMode.VISUALIZATION_STACK_MODE_NONE,
          showLegend: legendPosition !== "none",
          showDataLabels,
        }
        : existingVisualization);
      const search = {
        spl: query,
        earliest: timeRange.earliest,
        latest: timeRange.latest,
        timezone: timeRange.timezone
          ?? existing?.search.timeRange?.timezone
          ?? Intl.DateTimeFormat().resolvedOptions().timeZone,
        indexScope: savedSearchIndexScope(query, bootstrap, existing),
        appId: existing !== undefined
          ? existing.search.appId
          : backendHistoryRerunRef.current !== null
            ? backendHistoryRerunRef.current.search.appId
            : bootstrap.response.selectedAppId ?? undefined,
        preferredResultTab: searchResultTabFromWorkspace(activeTab),
        selectedFields: pendingSavedSelectedFieldsRef.current === null
          ? fields.filter((field) => field.selected).map((field) => field.name)
          : [...pendingSavedSelectedFieldsRef.current],
        visualization,
        base: existing?.search ?? backendHistoryRerunRef.current?.search,
      };
      const result = existing === undefined
        ? await createServerSavedSearch(apiClient, bootstrap.response, {
          name: trimmedName,
          description: trimmedDescription,
          search,
          sharingScope: SharingScope.SHARING_SCOPE_PRIVATE,
        })
        : await updateServerSavedSearch(apiClient, bootstrap.response, {
          id: existing.id,
          expectedVersion: existing.version,
          name: trimmedName,
          description: trimmedDescription,
          search,
          sharingScope: existing.sharingScope,
          ownerId: existing.ownerId ?? undefined,
          updatePaths: [
            ...(existing.name === trimmedName ? [] : ["name" as const]),
            ...(existing.description === trimmedDescription ? [] : ["description" as const]),
            "search" as const,
          ],
        });
      if (result.status === "unavailable") {
        setSavedSearchesAvailable(false);
        throw new Error("Saved searches are not available from this server.");
      }
      backendSavedSearchesRef.current.set(result.value.id, result.value);
      backendHistoryRerunRef.current = null;
      activeSavedSearchIdRef.current = result.value.id;
      setActiveSavedSearchId(result.value.id);
      visualizationEditedRef.current = false;
      setSavedWorkspaceBaseline(null);
      setSavedBaselineCaptureId(result.value.id);
      setModal(null);
      const displaySearch = savedSearchForDisplay(result.value);
      setSavedSearches((current) => [
        displaySearch,
        ...current.filter((item) => item.id !== displaySearch.id),
      ]);
      showToast(`Saved “${result.value.name}”.`, "success");
    } catch (error) {
      let message: string;
      const conflict = savedSearchConflictKind(error);
      if (conflict === "version" && updatedSavedSearchId !== null) {
        try {
          const reloadStatus = await reloadBackendSavedSearchById(updatedSavedSearchId);
          message = reloadStatus === "removed"
            ? "This saved search was deleted on the server. Save again to create a new search."
            : "This saved search changed on the server. The latest version was reloaded; review and save again.";
        } catch (reloadError) {
          message = reloadError instanceof Error
            ? `This saved search changed, but its latest version could not be loaded: ${reloadError.message}`
            : "This saved search changed, but its latest version could not be loaded.";
        }
      } else if (conflict === "name" || (conflict === "unknown" && operation === "create")) {
        message = `A saved search named “${trimmedName}” already exists in this app. Choose a different name.`;
      } else {
        message = error instanceof Error ? error.message : "Unable to save this search.";
      }
      setSaveError(message);
      if (modal !== "save") showToast(message, "warning");
    } finally {
      backendObjectMutationRef.current = false;
      setObjectMutation(null);
    }
  }

  function saveSearch() {
    const trimmedName = saveName.trim();
    if (trimmedName.length === 0) return;
    if (backendEnabled) {
      void saveBackendSearch(saveAsNew || activeSavedSearchId === null);
      return;
    }
    const id = saveAsNew || activeSavedSearchId === null ? `saved-${Date.now()}` : activeSavedSearchId;
    const saved: DemoSavedSearch = {
      id,
      name: trimmedName,
      description: saveDescription.trim(),
      query,
      earliest: timeRange.earliest,
      latest: timeRange.latest,
      updatedAt: "Just now",
      owner: "admin",
    };
    setSavedSearches((current) => [saved, ...current.filter((item) => item.id !== id)]);
    activeSavedSearchIdRef.current = id;
    setActiveSavedSearchId(id);
    setSavedWorkspaceBaseline(null);
    setSavedBaselineCaptureId(id);
    setModal(null);
    showToast(`Saved “${trimmedName}”.`, "success");
  }

  function quickSave() {
    if (activeSavedSearchId === null) {
      openSaveDialog();
      return;
    }
    const existing = savedSearches.find((item) => item.id === activeSavedSearchId);
    if (existing === undefined) {
      openSaveDialog();
      return;
    }
    if (backendEnabled) {
      const serverExisting = backendSavedSearchesRef.current.get(activeSavedSearchId);
      if (serverExisting === undefined) {
        openSaveDialog();
        return;
      }
      setSaveName(serverExisting.name);
      setSaveDescription(serverExisting.description);
      setSaveAsNew(false);
      void saveBackendSearch(false, serverExisting.name, serverExisting.description);
      return;
    }
    setSavedSearches((current) =>
      current.map((item) => (item.id === activeSavedSearchId ? { ...item, query, earliest: timeRange.earliest, latest: timeRange.latest, updatedAt: "Just now" } : item)),
    );
    setSavedWorkspaceBaseline(null);
    setSavedBaselineCaptureId(activeSavedSearchId);
    showToast(`Saved changes to “${existing.name}”.`, "success");
  }

  function restoreBackendPresentation(search: SearchDefinition | undefined): string | null {
    if (!backendEnabled || search === undefined) return null;
    visualizationEditedRef.current = false;
    pendingSavedSelectedFieldsRef.current = new Set(search.selectedFields);
    const preferredTab = workspaceResultTabFromSaved(search.preferredResultTab);
    pendingSavedPreferredTabRef.current = preferredTab;
    if (preferredTab !== null) setActiveTab(preferredTab);

    const visualization = search.visualization;
    if (visualization === undefined) {
      pendingSavedVisualizationRef.current = undefined;
      preservedSavedVisualizationRef.current = null;
      return null;
    }
    const restoredStyle = chartStyleForVisualizationType(visualization.type);
    if (restoredStyle === null) {
      pendingSavedVisualizationRef.current = undefined;
      preservedSavedVisualizationRef.current = visualization;
      return "Its saved chart type is not available in this workspace; the server definition was preserved.";
    }
    preservedSavedVisualizationRef.current = null;
    pendingSavedVisualizationRef.current = visualization;
    setChartStyle(restoredStyle);
    setChartTitle(
      visualization.title?.trim()
      || (chartStyleForVisualizationType(visualization.type) === "line"
        ? "Event volume over time"
        : "Event volume by level"),
    );
    setLegendPosition(visualization.showLegend ? "bottom" : "none");
    setShowDataLabels(visualization.showDataLabels);
    return null;
  }

  function clearDisplayedJobForDraft(nextRange: TimeRange) {
    generationRef.current += 1;
    clearTimers();
    backendAbortRef.current?.abort();
    backendPageAbortRef.current?.abort();
    backendMetadataAbortRef.current?.abort();
    backendFieldSummaryAbortRef.current?.abort();
    backendSocketRef.current?.dispose();
    backendSocketRef.current = null;
    backendJobIdRef.current = null;
    backendJobRef.current = null;
    backendJobVersionRef.current = 0n;
    resetBackendResultState();
    resetExport();
    setSubmittedQuery("");
    setSubmittedTimeRange(nextRange);
    setPhase("completed");
    setProgress(0);
    setElapsed("0.00 s");
    setScannedRows(0);
    setScannedRowsApproximate(false);
    setScannedBytes("0 B");
    setResolvedTimeRangeLabel(null);
    setBackendEventCount(0);
    setBackendPrimaryCountLabel("events");
    setBackendPrimaryCountPrefix("");
    setBackendEvents([]);
    setBackendStatistics([]);
    setBackendStatisticsTable(null);
    setBackendTimeline([]);
    if (backendEnabled) setFields([]);
    setActiveField(null);
    setExpandedEvents(new Set());
    setTimelineStart(null);
    setTimelineEnd(null);
    setEventPage(1);
  }

  function openSavedSearch(saved: DemoSavedSearch, fallbackRange: TimeRange = timeRange) {
    if (backendWorkspaceTransitionBlocked()) return;
    if (isRunning) {
      showToast("Cancel the active search before opening a saved search.", "warning");
      return;
    }
    const hasSavedRange = saved.earliest.trim().length > 0 && saved.latest.trim().length > 0;
    const savedRange = hasSavedRange
      ? {
        label: saved.earliest === "-24h" ? "Last 24 hours" : `${saved.earliest} to ${saved.latest}`,
        earliest: saved.earliest,
        latest: saved.latest,
        timezone: saved.timezone,
      }
      : fallbackRange;
    clearDisplayedJobForDraft(savedRange);
    setQuery(saved.query);
    setEditorCaret(saved.query.length);
    setTimeRange(savedRange);
    setDraftTimeRange(savedRange);
    timelineZoomParentRef.current = null;
    activeSavedSearchIdRef.current = saved.id;
    setActiveSavedSearchId(saved.id);
    setSavedWorkspaceBaseline(null);
    setSavedBaselineCaptureId(saved.id);
    backendHistoryRerunRef.current = null;
    const presentationNotice = restoreBackendPresentation(
      backendSavedSearchesRef.current.get(saved.id)?.search,
    );
    setModal(null);
    showToast(
      presentationNotice
        ? `Opened “${saved.name}”. ${presentationNotice}`
        : hasSavedRange
          ? `Opened “${saved.name}”. Run it when ready.`
          : `Opened “${saved.name}” with the current workspace time range.`,
      presentationNotice ? "warning" : "info",
    );
    focusEditor(saved.query.length);
  }

  function openHistoryEntry(entry: DemoHistoryEntry, rerun: boolean, focusSearchEditor = true): boolean {
    if (backendWorkspaceTransitionBlocked()) return false;
    if (isRunning && !rerun) {
      showToast("Cancel the active search before restoring a history draft.", "warning");
      return false;
    }
    const restoredRange = entry.earliest !== undefined && entry.latest !== undefined
      ? { label: entry.timeRange, earliest: entry.earliest, latest: entry.latest, timezone: entry.timezone }
      : TIME_PRESETS.find((preset) => preset.label === entry.timeRange) ?? timeRange;
    if (!rerun) clearDisplayedJobForDraft(restoredRange);
    setQuery(entry.query);
    setEditorCaret(entry.query.length);
    setTimeRange(restoredRange);
    setDraftTimeRange(restoredRange);
    timelineZoomParentRef.current = null;
    if (backendEnabled) {
      const serverEntry = backendHistoryRef.current.get(entry.id) ?? null;
      backendHistoryRerunRef.current = serverEntry;
      restoreBackendPresentation(serverEntry?.search);
    }
    activeSavedSearchIdRef.current = null;
    setActiveSavedSearchId(null);
    setModal(null);
    if (rerun) runSearch(entry.query, restoredRange);
    else showToast("Search restored without running.", "info");
    if (focusSearchEditor) focusEditor(entry.query.length);
    return true;
  }

  function saveHistoryEntry(entry: DemoHistoryEntry) {
    if (backendWorkspaceTransitionBlocked()) return;
    saveDialogReturnFocusRef.current = document.activeElement instanceof HTMLElement
      ? document.activeElement
      : null;
    if (!openHistoryEntry(entry, false, false)) return;
    setSaveAsNew(true);
    setSaveName(`History search – ${entry.ranAt}`);
    setSaveDescription("Saved from search history.");
    setSaveError(null);
    setModal("save");
  }

  function nextDuplicateSavedSearchName(
    name: string,
    attemptedNames: ReadonlySet<string> = new Set(),
  ): string {
    const names = new Set([
      ...savedSearches.map((savedSearch) => savedSearch.name),
      ...attemptedNames,
    ]);
    let copyNumber = 1;
    let candidate = duplicateSavedSearchName(name, copyNumber);
    while (names.has(candidate)) {
      copyNumber += 1;
      candidate = duplicateSavedSearchName(name, copyNumber);
    }
    return candidate;
  }

  async function duplicateSavedSearch(id: string) {
    const displaySearch = savedSearches.find((savedSearch) => savedSearch.id === id);
    if (displaySearch === undefined) return;
    const initialName = nextDuplicateSavedSearchName(displaySearch.name);
    if (!backendEnabled) {
      const duplicate: DemoSavedSearch = {
        ...displaySearch,
        id: `saved-${Date.now()}`,
        name: initialName,
        updatedAt: "Just now",
      };
      setSavedSearches((current) => [duplicate, ...current]);
      showToast(`Duplicated as “${initialName}”.`, "success");
      return;
    }
    const bootstrap = backendBootstrapRef.current;
    const savedSearch = backendSavedSearchesRef.current.get(id);
    if (
      bootstrap === null
      || savedSearch === undefined
      || objectMutation !== null
      || appSwitchAbortRef.current !== null
      || backendObjectMutationRef.current
    ) return;
    backendObjectMutationRef.current = true;
    setSavedSearchDuplicateError(null);
    setObjectMutation({ kind: "duplicateSaved", targetId: id });
    try {
      const attemptedNames = new Set<string>();
      let result: Awaited<ReturnType<typeof duplicateServerSavedSearch>> | null = null;
      for (let attempt = 0; attempt < MAXIMUM_READABLE_DUPLICATE_NAME_ATTEMPTS; attempt += 1) {
        const candidate = nextDuplicateSavedSearchName(displaySearch.name, attemptedNames);
        attemptedNames.add(candidate);
        try {
          // Name uniqueness is authoritative on the server. Retry a bounded
          // sequence so collisions outside the incrementally loaded library
          // do not trap the Duplicate action.
          // eslint-disable-next-line no-await-in-loop
          result = await duplicateServerSavedSearch(
            apiClient,
            bootstrap.response,
            id,
            candidate,
            savedSearch.search.appId,
          );
          break;
        } catch (error) {
          // Duplicate has no optimistic-version input; the backend contract's
          // only conflict response for this route is a destination-name clash.
          if (!isHttpStatus(error, 409)) throw error;
        }
      }
      for (
        let attempt = 0;
        result === null && attempt < MAXIMUM_RANDOM_DUPLICATE_NAME_ATTEMPTS;
        attempt += 1
      ) {
        let candidate = randomDuplicateSavedSearchName(displaySearch.name);
        while (attemptedNames.has(candidate)) {
          candidate = randomDuplicateSavedSearchName(displaySearch.name);
        }
        attemptedNames.add(candidate);
        try {
          // A high-entropy suffix escapes arbitrarily dense unloaded copy
          // sequences without turning one click into unbounded API traffic.
          // eslint-disable-next-line no-await-in-loop
          result = await duplicateServerSavedSearch(
            apiClient,
            bootstrap.response,
            id,
            candidate,
            savedSearch.search.appId,
          );
        } catch (error) {
          if (!isHttpStatus(error, 409)) throw error;
        }
      }
      if (result === null) {
        throw new Error("Unable to reserve a unique copy name after multiple server conflicts. Try again.");
      }
      if (result.status === "unavailable") {
        throw new Error("Saved-search duplication is not available from this server.");
      }
      backendSavedSearchesRef.current.set(result.value.id, result.value);
      const duplicate = savedSearchForDisplay(result.value);
      setSavedSearches((current) => [duplicate, ...current.filter((item) => item.id !== duplicate.id)]);
      showToast(`Duplicated as “${result.value.name}”.`, "success");
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to duplicate this saved search.";
      setSavedSearchDuplicateError(message);
      showToast(message, "warning");
    } finally {
      backendObjectMutationRef.current = false;
      setObjectMutation(null);
    }
  }

  async function renameSavedSearch() {
    const id = savedSearchRenameTargetId;
    const name = savedSearchRenameName.trim();
    if (id === null || name.length === 0) return;
    if (!backendEnabled) {
      setSavedSearches((current) => {
        const existing = current.find((savedSearch) => savedSearch.id === id);
        if (existing === undefined) return current;
        return [
          { ...existing, name, updatedAt: "Just now" },
          ...current.filter((savedSearch) => savedSearch.id !== id),
        ];
      });
      setSavedSearchRenameTargetId(null);
      setModal("open");
      showToast(`Renamed saved search to “${name}”.`, "success");
      return;
    }
    const bootstrap = backendBootstrapRef.current;
    const savedSearch = backendSavedSearchesRef.current.get(id);
    if (
      bootstrap === null
      || savedSearch === undefined
      || objectMutation !== null
      || appSwitchAbortRef.current !== null
      || backendObjectMutationRef.current
    ) return;
    backendObjectMutationRef.current = true;
    setSavedSearchRenameError(null);
    setObjectMutation({ kind: "renameSaved", targetId: id });
    try {
      const result = await renameServerSavedSearch(
        apiClient,
        bootstrap.response,
        savedSearch,
        name,
      );
      if (result.status === "unavailable") {
        throw new Error("Saved-search rename is not available from this server.");
      }
      backendSavedSearchesRef.current.set(result.value.id, result.value);
      const renamed = savedSearchForDisplay(result.value);
      // The library is sorted by updated_at descending; a successful rename
      // updates that timestamp, so move the returned server record to the top.
      setSavedSearches((current) => [
        renamed,
        ...current.filter((item) => item.id !== renamed.id),
      ]);
      setSavedSearchRenameTargetId(null);
      setModal("open");
      showToast(`Renamed saved search to “${result.value.name}”.`, "success");
    } catch (error) {
      const conflict = savedSearchConflictKind(error);
      let conflictMessage: string | null = null;
      let removedOnReload = false;
      if (conflict === "version") {
        try {
          const reloadStatus = await reloadBackendSavedSearchById(id);
          if (reloadStatus === "removed") {
            setSavedSearchRenameTargetId(null);
            setModal("open");
            removedOnReload = true;
            conflictMessage = "This saved search was deleted on the server.";
          }
        } catch (reloadError) {
          conflictMessage = reloadError instanceof Error
            ? `This saved search changed, but its latest version could not be loaded: ${reloadError.message}`
            : "This saved search changed, but its latest version could not be loaded.";
        }
      }
      const message = conflictMessage ?? (
        conflict === "version"
          ? "This saved search changed on the server. The latest version was reloaded; review the name and retry."
          : conflict === "name"
            ? `A saved search named “${name}” already exists in this app. Choose a different name.`
          : error instanceof Error ? error.message : "Unable to rename this saved search."
      );
      setSavedSearchRenameError(message);
      if (removedOnReload) {
        showToast(message, "warning");
      }
    } finally {
      backendObjectMutationRef.current = false;
      setObjectMutation(null);
    }
  }

  async function deleteSavedSearch(id: string) {
    if (backendEnabled) {
      const bootstrap = backendBootstrapRef.current;
      const saved = backendSavedSearchesRef.current.get(id);
      if (
        bootstrap === null
        || saved === undefined
        || objectMutation !== null
        || appSwitchAbortRef.current !== null
        || backendObjectMutationRef.current
      ) return;
      backendObjectMutationRef.current = true;
      setSavedSearchDeleteError(null);
      setObjectMutation({ kind: "deleteSaved", targetId: id });
      try {
        const result = await deleteServerSavedSearch(
          apiClient,
          bootstrap.response,
          id,
          saved.version,
        );
        if (result.status === "unavailable") throw new Error("Saved searches are not available from this server.");
        backendSavedSearchesRef.current.delete(id);
        setSavedSearches((current) => current.filter((item) => item.id !== id));
        setSavedSearchDeleteTargetId(null);
        setModal("open");
        if (activeSavedSearchId === id) {
          activeSavedSearchIdRef.current = null;
          setActiveSavedSearchId(null);
        }
        showToast("Saved search deleted.", "warning");
      } catch (error) {
        if (isHttpStatus(error, 409)) {
          await refreshBackendSavedSearches(bootstrap);
          setSavedSearchDeleteError("This saved search changed on the server. The latest version was reloaded.");
        } else {
          setSavedSearchDeleteError(error instanceof Error ? error.message : "Unable to delete the saved search.");
        }
      } finally {
        backendObjectMutationRef.current = false;
        setObjectMutation(null);
      }
      return;
    }
    setSavedSearches((current) => current.filter((item) => item.id !== id));
    setSavedSearchDeleteTargetId(null);
    setModal("open");
    if (activeSavedSearchId === id) {
      activeSavedSearchIdRef.current = null;
      setActiveSavedSearchId(null);
    }
    showToast("Saved search deleted.", "warning");
  }

  async function deleteHistoryEntry(id: string) {
    if (backendEnabled) {
      const bootstrap = backendBootstrapRef.current;
      if (
        bootstrap === null
        || objectMutation !== null
        || appSwitchAbortRef.current !== null
        || backendObjectMutationRef.current
      ) return;
      backendObjectMutationRef.current = true;
      setHistoryDeleteError(null);
      setObjectMutation({ kind: "deleteHistory", targetId: id });
      try {
        const result = await deleteServerSearchHistoryEntry(apiClient, bootstrap.response, id);
        if (result.status === "unavailable") throw new Error("Search history is not available from this server.");
        backendHistoryRef.current.delete(id);
        setHistory((current) => current.filter((item) => item.id !== id));
        showToast("History entry deleted.", "warning");
      } catch (error) {
        setHistoryDeleteError(error instanceof Error ? error.message : "Unable to delete the history entry.");
      } finally {
        backendObjectMutationRef.current = false;
        setObjectMutation(null);
      }
      return;
    }
    setHistory((current) => current.filter((item) => item.id !== id));
    showToast("History entry deleted.", "warning");
  }

  async function clearHistory() {
    if (!backendEnabled) {
      setHistory([]);
      setModal("history");
      return;
    }
    const bootstrap = backendBootstrapRef.current;
    if (
      bootstrap === null
      || historyClearBusy
      || appSwitchAbortRef.current !== null
      || backendObjectMutationRef.current
    ) return;
    const selectedAppId = bootstrap.response.selectedAppId;
    if (selectedAppId === null) {
      setHistoryClearError("The server did not select an app, so a cross-app history clear was not attempted.");
      return;
    }
    setHistoryClearError(null);
    backendObjectMutationRef.current = true;
    setHistoryClearBusy(true);
    try {
      const filter: SearchHistoryFilter = {
        appId: selectedAppId,
        stateFilters: [],
        text: undefined,
        savedSearchId: undefined,
        createdAfter: undefined,
        createdBefore: undefined,
      };
      const result = await clearServerSearchHistory(
        apiClient,
        bootstrap.response,
        filter,
        "CLEAR SEARCH HISTORY",
      );
      if (result.status === "unavailable") throw new Error("Search history is not available from this server.");
      backendHistoryRefreshEpochRef.current += 1;
      backendHistoryLoadMoreEpochRef.current += 1;
      backendHistoryLoadMoreRef.current = false;
      backendHistoryPageTokensRef.current.clear();
      setHistoryLoading(false);
      setHistoryLoadingMore(false);
      setHistory([]);
      backendHistoryRef.current.clear();
      setHistoryNextPageToken(null);
      setModal("history");
      showToast(`Cleared ${NUMBER_FORMAT.format(result.value)} history entries.`, "warning");
    } catch (error) {
      setHistoryClearError(error instanceof Error ? error.message : "Unable to clear search history.");
    } finally {
      backendObjectMutationRef.current = false;
      setHistoryClearBusy(false);
    }
  }

  function exportFieldsForTab(tab: ResultTab): string[] {
    if (backendEnabled) {
      return backendResultSchema?.columns
        .map((column) => column.fieldName) ?? [];
    }
    if (isTimechartResult && (tab === "statistics" || tab === "visualization")) {
      return ["_time", ...timechartValueColumns];
    }
    if (genericStatisticsTable !== null && (tab === "statistics" || tab === "visualization")) {
      return genericStatisticsTable.columns.map((column) => column.key);
    }
    return EXPORT_FIELDS_BY_TAB[tab];
  }

  function displayedRowsForTab(tab: ResultTab): number {
    if (tab === "events") return resultEvents.length;
    if (tab === "patterns") return patternRows.length;
    if (isTimechartResult) return timelinePoints.length;
    return genericStatisticsTable?.rows.length ?? statisticsRows.length;
  }

  function toggleExportField(fieldName: string) {
    setExportFields((current) =>
      current.includes(fieldName) ? current.filter((field) => field !== fieldName) : [...current, fieldName],
    );
  }

  function openExportDialog(sourceTab: ResultTab = activeTab) {
    if (
      backendEnabled
      && !backendAuthoritativeResultsReady
      && exportStage === "configure"
    ) {
      showToast("Wait for authoritative results before exporting.", "warning");
      return;
    }
    if (exportStage === "pending" || exportStage === "ready") {
      setMenu(null);
      setModal("export");
      return;
    }
    setExportSourceTab(sourceTab);
    setExportFields(exportFieldsForTab(sourceTab));
    setExportStage("configure");
    setExportError(null);
    setExportRequestId(null);
    setExportRetryable(true);
    setDemoExportSize(0);
    setServerExportJob(null);
    setExportDownloadState({ status: "idle" });
    setExportCancelState({ status: "idle" });
    setMenu(null);
    setModal("export");
  }

  function buildLocalExportArtifact(): { blob: Blob; filename: string } {
    const filename = `open-splunk-${exportSourceTab}.${exportFormat}`;
    const rows: Record<string, unknown>[] = exportSourceTab === "events"
      ? resultEvents.map((event) => Object.fromEntries(exportFields.map((field) => [field, event.fields[field] ?? (field === "_raw" ? event.raw : null)])))
      : exportSourceTab === "patterns"
        ? patternRows.map((pattern) => ({ pattern: pattern.signature, count: pattern.count, percent: pattern.percent }))
        : isTimechartResult
          ? timechartRowsForExport(sortedTimechartRows)
          : genericStatisticsTable !== null
            ? sortedGenericStatisticsRows.map((row) => row.values)
            : sortedStatistics.map((row) => ({ level: row.level, count: row.count, percent: row.percent, avgDuration: row.avgDuration }));
    const selectedRows = rows.map((row) => Object.fromEntries(exportFields.map((field) => [field, row[field] ?? null])));
    const content = exportFormat === "jsonl"
      ? selectedRows.map((row) => JSON.stringify(row)).join("\n")
      : [
        exportFields.map((field) => `"${(exportFieldLabels[field] ?? field).replaceAll('"', '""')}"`).join(","),
        ...selectedRows.map((row) =>
          exportFields
            .map((field) => `"${exportCellString(row[field]).replaceAll('"', '""')}"`)
            .join(","),
        ),
      ].join("\n");
    return {
      filename,
      blob: new Blob([content], { type: exportFormat === "csv" ? "text/csv" : "application/x-ndjson" }),
    };
  }

  async function prepareExport() {
    const exportEpoch = ++exportEpochRef.current;
    const requestId = `export-${Date.now()}-${generationRef.current}`;
    setExportRequestId(requestId);
    setExportRetryable(true);
    setDemoExportSize(0);
    if (!backendEnabled) {
      setDemoExportSize(buildLocalExportArtifact().blob.size);
      setExportStage("ready");
      return;
    }
    const bootstrap = backendBootstrapRef.current;
    const job = backendJobRef.current;
    if (
      bootstrap === null
      || job === null
      || phase !== "completed"
      || backendResultsExpired
      || exportSourceTab === "patterns"
    ) {
      setExportError("Complete a retained backend result before creating this export.");
      setExportRetryable(false);
      setExportStage("error");
      return;
    }
    backendExportAbortRef.current?.abort();
    const controller = new AbortController();
    backendExportAbortRef.current = controller;
    setExportError(null);
    setExportDownloadState({ status: "idle" });
    setExportCancelState({ status: "idle" });
    setServerExportJob(null);
    setExportStage("pending");
    try {
      const created = await createServerExport(apiClient, bootstrap.response, {
        searchJobId: job.searchJobId,
        format: exportFormat === "csv" ? "csv" : "json-lines",
        columns: exportFields,
        rowLimit: bootstrap.response.limits.maximumExportRows > 0n
          ? bootstrap.response.limits.maximumExportRows
          : undefined,
        byteLimit: bootstrap.response.limits.maximumExportBytes > 0n
          ? bootstrap.response.limits.maximumExportBytes
          : undefined,
        csvHeaderMode: "field-names",
        jsonIntegerEncoding: "string",
        signal: controller.signal,
      });
      if (created.status === "unavailable") {
        throw new Error("The selected export format is not available from this server.");
      }
      if (controller.signal.aborted || exportEpochRef.current !== exportEpoch) return;
      serverExportJobRef.current = created.value;
      setServerExportJob(created.value);
      const completed = await waitForServerExport(
        apiClient,
        bootstrap.response,
        created.value,
        {
          signal: controller.signal,
          websocketBaseUrl: apiBaseUrl,
          onUpdate: (updatedJob) => {
            if (
              controller.signal.aborted
              || exportEpochRef.current !== exportEpoch
              || updatedJob.exportJobId !== created.value.exportJobId
            ) return;
            serverExportJobRef.current = updatedJob;
            setServerExportJob(updatedJob);
          },
        },
      );
      if (completed.status === "unavailable") {
        throw new Error("The export endpoint became unavailable.");
      }
      if (controller.signal.aborted || exportEpochRef.current !== exportEpoch) return;
      serverExportJobRef.current = completed.value;
      setServerExportJob(completed.value);
      if (
        completed.value.state !== ExportJobState.EXPORT_JOB_STATE_COMPLETED
        || completed.value.artifact === undefined
      ) {
        setExportRetryable(completed.value.failure?.retryable ?? false);
        throw new Error(
          completed.value.failure?.message
          || (completed.value.state === ExportJobState.EXPORT_JOB_STATE_EXPIRED
            ? "The export expired before it became available."
            : completed.value.state === ExportJobState.EXPORT_JOB_STATE_CANCELED
              ? "The export was canceled."
              : "The server could not complete this export."),
        );
      }
      setExportStage("ready");
    } catch (error) {
      if (controller.signal.aborted || exportEpochRef.current !== exportEpoch) return;
      if (isHttpError(error)) {
        setExportRetryable(error.status === 408 || error.status === 409 || error.status === 429 || error.status >= 500);
      }
      setExportError(error instanceof Error ? error.message : "Unable to create the export.");
      setExportStage("error");
    } finally {
      if (backendExportAbortRef.current === controller && exportEpochRef.current === exportEpoch) {
        backendExportAbortRef.current = null;
      }
    }
  }

  async function downloadExport(artifactDetails?: ExportArtifactDetails) {
    if (backendEnabled) {
      const bootstrap = backendBootstrapRef.current;
      const exportJob = serverExportJobRef.current ?? serverExportJob;
      const downloadEpoch = exportEpochRef.current;
      if (
        bootstrap === null
        || exportJob === null
        || exportJob.state !== ExportJobState.EXPORT_JOB_STATE_COMPLETED
        || artifactDetails === undefined
        || artifactDetails.downloadHandle !== `server:${exportRequestId}:${exportJob.exportJobId}`
      ) {
        setExportDownloadState({ status: "error", error: "The server export is not ready." });
        return;
      }
      if (
        artifactDetails.expiresAt !== null
        && artifactDetails.expiresAt.valueOf() <= currentBackendServerTime(bootstrap).valueOf()
      ) {
        setExportDownloadState({ status: "error", error: "This export artifact has expired. Create a new export." });
        return;
      }
      backendExportDownloadAbortRef.current?.abort();
      const controller = new AbortController();
      backendExportDownloadAbortRef.current = controller;
      setExportDownloadState({ status: "pending" });
      try {
        const downloaded = await downloadServerExport(
          apiClient,
          bootstrap.response,
          exportJob.exportJobId,
          { apiBaseUrl, signal: controller.signal },
        );
        if (downloaded.status === "unavailable") {
          throw new Error("The secure export download endpoint is unavailable.");
        }
        if (controller.signal.aborted || exportEpochRef.current !== downloadEpoch) return;
        const url = URL.createObjectURL(downloaded.value.blob);
        const anchor = document.createElement("a");
        anchor.href = url;
        anchor.download = downloaded.value.fileName;
        anchor.click();
        window.setTimeout(() => URL.revokeObjectURL(url), 0);
        setExportDownloadState({ status: "idle" });
        setModal(null);
        setExportStage("configure");
        setExportRequestId(null);
        serverExportJobRef.current = null;
        setServerExportJob(null);
        showToast(`Downloaded ${downloaded.value.fileName}.`, "success");
      } catch (error) {
        if (controller.signal.aborted || exportEpochRef.current !== downloadEpoch) return;
        setExportDownloadState({
          status: "error",
          error: error instanceof Error ? error.message : "Unable to download the export.",
        });
      } finally {
        if (backendExportDownloadAbortRef.current === controller) {
          backendExportDownloadAbortRef.current = null;
        }
      }
      return;
    }
    if (artifactDetails !== undefined && artifactDetails.downloadHandle !== `demo:${exportRequestId}`) {
      setExportDownloadState({ status: "error", error: "That export artifact is no longer current." });
      return;
    }
    const { blob, filename } = buildLocalExportArtifact();
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = filename;
    anchor.click();
    URL.revokeObjectURL(url);
    setModal(null);
    setExportStage("configure");
    setExportRequestId(null);
    showToast(`Downloaded ${filename}.`, "success");
  }

  function clearExportState(abortCancellation = true) {
    exportEpochRef.current += 1;
    backendExportAbortRef.current?.abort();
    backendExportAbortRef.current = null;
    backendExportDownloadAbortRef.current?.abort();
    backendExportDownloadAbortRef.current = null;
    if (abortCancellation) {
      backendExportCancelAbortRef.current?.abort();
      backendExportCancelAbortRef.current = null;
    }
    setExportStage("configure");
    setExportError(null);
    setExportRequestId(null);
    setExportRetryable(true);
    setDemoExportSize(0);
    setExportDownloadState({ status: "idle" });
    setExportCancelState({ status: "idle" });
    serverExportJobRef.current = null;
    setServerExportJob(null);
  }

  function resetExport() {
    const exportJob = serverExportJobRef.current ?? serverExportJob;
    const bootstrap = backendBootstrapRef.current;
    clearExportState();
    if (
      backendEnabled
      && bootstrap !== null
      && exportJob !== null
      && (
        exportJob.state === ExportJobState.EXPORT_JOB_STATE_QUEUED
        || exportJob.state === ExportJobState.EXPORT_JOB_STATE_RUNNING
      )
    ) {
      void cancelServerExport(
        apiClient,
        bootstrap.response,
        exportJob.exportJobId,
        { timeoutMs: 5_000 },
      ).catch(() => undefined);
    }
  }

  async function cancelActiveExport() {
    if (!backendEnabled) {
      clearExportState();
      setModal(null);
      return;
    }
    const bootstrap = backendBootstrapRef.current;
    const exportJob = serverExportJobRef.current ?? serverExportJob;
    if (
      bootstrap === null
      || exportJob === null
      || (
        exportJob.state !== ExportJobState.EXPORT_JOB_STATE_QUEUED
        && exportJob.state !== ExportJobState.EXPORT_JOB_STATE_RUNNING
      )
      || backendExportCancelAbortRef.current !== null
    ) return;
    const controller = new AbortController();
    backendExportCancelAbortRef.current = controller;
    setExportCancelState({ status: "pending" });
    try {
      const result = await cancelServerExport(
        apiClient,
        bootstrap.response,
        exportJob.exportJobId,
        { signal: controller.signal },
      );
      if (controller.signal.aborted) return;
      if (result.status === "unavailable") {
        throw new Error("The server export cancellation endpoint is unavailable.");
      }
      serverExportJobRef.current = result.value;
      setServerExportJob(result.value);
      if (
        result.value.state === ExportJobState.EXPORT_JOB_STATE_CANCELED
        || result.value.state === ExportJobState.EXPORT_JOB_STATE_FAILED
        || result.value.state === ExportJobState.EXPORT_JOB_STATE_EXPIRED
      ) {
        clearExportState(false);
        setModal(null);
        showToast("Export canceled.", "warning");
        return;
      }
      if (
        result.value.state === ExportJobState.EXPORT_JOB_STATE_COMPLETED
        && result.value.artifact !== undefined
      ) {
        exportEpochRef.current += 1;
        backendExportAbortRef.current?.abort();
        backendExportAbortRef.current = null;
        setExportCancelState({ status: "idle" });
        setExportStage("ready");
        showToast("The export completed before cancellation; its artifact is still available.", "info");
        return;
      }
      setExportCancelState({
        status: "error",
        error: "The server did not confirm cancellation. The export is still tracked; try again.",
      });
    } catch (error) {
      if (controller.signal.aborted) return;
      setExportCancelState({
        status: "error",
        error: error instanceof Error
          ? `Cancellation failed: ${error.message}`
          : "Cancellation failed. The export is still tracked; try again.",
      });
    } finally {
      if (backendExportCancelAbortRef.current === controller) {
        backendExportCancelAbortRef.current = null;
      }
    }
  }

  function updateStatsSort(key: keyof WorkspaceStatistic) {
    setStatsSort((current) => ({ key, direction: current.key === key && current.direction === "desc" ? "asc" : "desc" }));
  }

  function updateGenericStatsSort(key: string) {
    setGenericStatsSort((current) => current?.key === key
      ? { key, direction: current.direction === "asc" ? "desc" : "asc" }
      : { key, direction: "asc" });
  }

  function clearPersistedContextForAdHocSearch() {
    activeSavedSearchIdRef.current = null;
    setActiveSavedSearchId(null);
    backendHistoryRerunRef.current = null;
    pendingSavedPreferredTabRef.current = null;
    pendingSavedSelectedFieldsRef.current = null;
    pendingSavedVisualizationRef.current = undefined;
    preservedSavedVisualizationRef.current = null;
    visualizationEditedRef.current = false;
    setSavedWorkspaceBaseline(null);
    setSavedBaselineCaptureId(null);
  }

  function handleGlobalFind(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (backendWorkspaceTransitionBlocked()) return;
    if (globalFind.trim().length === 0) {
      showToast("Enter a term or SPL search to find events.", "warning");
      return;
    }
    const nextQuery = splFromFindInput(globalFind, backendEnabled ? "" : "gradethis");
    timelineZoomParentRef.current = null;
    clearPersistedContextForAdHocSearch();
    setGlobalFind("");
    runSearch(nextQuery, timeRange);
  }

  async function switchBackendApp(appId: string) {
    if (!backendEnabled || isRunning) {
      if (isRunning) showToast("Cancel the active search before switching apps.", "warning");
      return;
    }
    if (backendObjectMutationRef.current || objectMutation !== null || historyClearBusy) {
      showToast("Wait for the saved-search or history change to finish before switching apps.", "warning");
      return;
    }
    if (exportStage === "pending" || exportDownloadState.status === "pending") {
      showToast("Wait for or cancel the active export before switching apps.", "warning");
      return;
    }
    const switchEpoch = ++appSwitchEpochRef.current;
    appSwitchAbortRef.current?.abort();
    const controller = new AbortController();
    appSwitchAbortRef.current = controller;
    setAppSwitchingId(appId);
    setMenu(null);
    try {
      const response = await getSystemBootstrap(apiClient, appId, { signal: controller.signal });
      if (controller.signal.aborted || appSwitchEpochRef.current !== switchEpoch) return;
      if (!supportsServerFeature(response, ServerFeature.SERVER_FEATURE_SEARCH)) {
        throw new Error("The selected app does not expose browser search.");
      }
      generationRef.current += 1;
      backendAbortRef.current?.abort();
      backendPageAbortRef.current?.abort();
      backendMetadataAbortRef.current?.abort();
      backendFieldSummaryAbortRef.current?.abort();
      backendSocketRef.current?.dispose();
      backendSocketRef.current = null;
      resetExport();
      const bootstrap = { response, receivedAt: Date.now() };
      backendBootstrapRef.current = bootstrap;
      backendBootstrapPromiseRef.current = null;
      setBackendBootstrapModel(response);
      setBackendConnectionState("ready");
      setBackendConnectionError(null);
      const nextQuery = defaultQueryForBootstrap(response);
      setDefaultSearchQuery(nextQuery);
      setQuery(nextQuery);
      setSubmittedQuery("");
      setEditorCaret(nextQuery.length);
      activeSavedSearchIdRef.current = null;
      setActiveSavedSearchId(null);
      backendHistoryRerunRef.current = null;
      preservedSavedVisualizationRef.current = null;
      backendJobIdRef.current = null;
      backendJobRef.current = null;
      resetBackendResultState();
      setBackendEvents([]);
      setBackendStatistics([]);
      setBackendStatisticsTable(null);
      setBackendTimeline([]);
      setBackendEventCount(0);
      setBackendPrimaryCountLabel("events");
      setBackendPrimaryCountPrefix("");
      setFields([]);
      setPhase("completed");
      setProgress(0);
      setElapsed("0.00 s");
      setScannedRows(0);
      setScannedRowsApproximate(false);
      setScannedBytes("0 B");
      setResolvedTimeRangeLabel(null);
      setSavedSearches([]);
      setHistory([]);
      setSavedSearchesNextPageToken(null);
      setHistoryNextPageToken(null);
      backendSavedSearchesRef.current.clear();
      backendHistoryRef.current.clear();
      await Promise.all([
        refreshBackendSavedSearches(bootstrap, controller.signal),
        refreshBackendHistory(bootstrap, controller.signal),
      ]);
      if (controller.signal.aborted || appSwitchEpochRef.current !== switchEpoch) return;
      const selected = response.apps.find((app) => app.appId === response.selectedAppId);
      showToast(`Switched to ${selected?.displayName || "the selected app"}.`, "success");
      focusEditor(nextQuery.length);
    } catch (error) {
      if (controller.signal.aborted || appSwitchEpochRef.current !== switchEpoch) return;
      showToast(error instanceof Error ? error.message : "Unable to switch apps.", "warning");
    } finally {
      if (appSwitchEpochRef.current === switchEpoch) {
        setAppSwitchingId(null);
        if (appSwitchAbortRef.current === controller) appSwitchAbortRef.current = null;
      }
    }
  }

  function handleManualTimeRangeChange(nextRange: TimeRange) {
    timelineZoomParentRef.current = null;
    backendHistoryRerunRef.current = null;
    setTimeRange(backendEnabled && nextRange.timezone === undefined
      ? {
        ...nextRange,
        timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC",
      }
      : nextRange);
  }

  function closeSearchWorkspace() {
    if (backendEnabled && backendObjectMutationRef.current) {
      showToast("Wait for the current saved-search or history change to finish.", "warning");
      return;
    }
    if (isRunning) cancelSearch();
    resetExport();
    setQuery("");
    setSubmittedQuery("");
    activeSavedSearchIdRef.current = null;
    setActiveSavedSearchId(null);
    backendHistoryRerunRef.current = null;
    pendingSavedPreferredTabRef.current = null;
    pendingSavedSelectedFieldsRef.current = null;
    pendingSavedVisualizationRef.current = undefined;
    preservedSavedVisualizationRef.current = null;
    setTimelineStart(null);
    setTimelineEnd(null);
    setMenu(null);
    window.requestAnimationFrame(() => editorRef.current?.focus());
  }

  function resultTabAvailable(tab: ResultTab): boolean {
    if (!backendEnabled || !hasResultData) return true;
    if (backendResultKind === ResultSetKind.RESULT_SET_KIND_EVENTS) return tab === "events";
    if (
      backendResultKind === ResultSetKind.RESULT_SET_KIND_STATISTICS
      || backendResultKind === ResultSetKind.RESULT_SET_KIND_TIME_SERIES
    ) {
      return tab === "statistics" || tab === "visualization";
    }
    return false;
  }

  function handleResultTabKeyDown(event: KeyboardEvent<HTMLButtonElement>, currentTab: ResultTab) {
    const currentIndex = RESULT_TAB_ORDER.indexOf(currentTab);
    const availableTabs = RESULT_TAB_ORDER.filter(resultTabAvailable);
    const availableIndex = availableTabs.indexOf(currentTab);
    let nextTab: ResultTab | undefined;
    if (event.key === "ArrowLeft") {
      nextTab = availableTabs[Math.max(0, availableIndex - 1)];
    } else if (event.key === "ArrowRight") {
      nextTab = availableTabs[Math.min(availableTabs.length - 1, availableIndex + 1)];
    } else if (event.key === "Home") {
      nextTab = availableTabs[0];
    } else if (event.key === "End") {
      nextTab = availableTabs.at(-1);
    } else return;
    if (currentIndex < 0 || nextTab === undefined) return;
    event.preventDefault();
    setActiveTab(nextTab);
    window.requestAnimationFrame(() => document.getElementById(`tab-${nextTab}`)?.focus());
  }

  const emptyResultPresentation = backendEnabled && backendConnectionState === "loading"
    ? {
        icon: "…",
        title: "Connecting to the backend",
        detail: "Loading your authorized apps and index scope before search is enabled.",
      }
    : backendEnabled && backendConnectionState === "error"
      ? {
          icon: "!",
          title: "Backend connection unavailable",
          detail: backendConnectionError
            ?? "The system bootstrap request failed before an authorized search scope could be loaded.",
        }
    : searchIsClosed && backendHasNoSearchableIndexes
    ? {
        icon: "!",
        title: "No searchable indexes are available",
        detail: "This app has no searchable index in your current backend scope. Review index access or switch apps.",
      }
    : searchIsClosed
    ? {
        icon: "⌕",
        title: "Start a new search",
        detail: "Enter SPL, choose a time range, and run the search to inspect events and statistics.",
      }
    : backendEnabled && backendPreviewStatus === "limited"
      ? {
          icon: "…",
          title: "Live preview is bounded",
          detail: "A complete row did not fit the preview limit. The authoritative result snapshot will appear when the search completes.",
        }
    : backendEnabled && backendPreviewStatus === "resyncing"
      ? {
          icon: "↻",
          title: "Resynchronizing live results",
          detail: "Provisional rows were cleared after an update discontinuity. A fresh preview or the authoritative result snapshot will replace them.",
        }
    : isRunning
    ? {
        icon: "…",
        title: "Search is running",
        detail: "Results will appear here as soon as the backend produces them.",
      }
    : backendEnabled && backendResultsExpired
      ? {
          icon: "⌛",
          title: "Search results expired",
          detail: "The server retention window ended. Run the search again to create a fresh result snapshot.",
        }
    : phase === "failed"
      ? {
          icon: "!",
          title: "Search failed before results were produced",
          detail: "Review the search error, adjust the SPL or time range, and run it again.",
        }
      : {
          icon: "×",
          title: "Search was canceled",
          detail: "The timeline, fields, and result views were cleared so they cannot be mistaken for this job's output.",
        };
  const emptyStateRunQuery = searchIsClosed
    ? defaultSearchQuery
    : query.trim().length > 0
      ? query
      : submittedQuery;
  const emptyStateCanRun = emptyStateRunQuery.trim().length > 0
    && (!backendEnabled || (
      backendConnectionState === "ready"
      && !backendHasNoSearchableIndexes
    ));
  const selectedBackendApp = backendBootstrapModel?.apps.find(
    (app) => app.appId === backendBootstrapModel.selectedAppId,
  );
  const workspaceAppName = backendEnabled
    ? selectedBackendApp?.displayName
      || (backendConnectionState === "loading" ? "Connecting…" : "Backend unavailable")
    : "Search & Reporting";

  return (
    <div className="splunk-shell" data-testid="search-workspace" id="search">
      <a className="skip-link" href="#search-main-content">Skip to search workspace</a>
      <header className="product-bar">
        <div className="product-left">
          <button ref={mobileProductTriggerRef} className="search-mobile-trigger" type="button" aria-label="Open product navigation" aria-expanded={mobileProductNavOpen} onClick={() => setMobileProductNavOpen(true)}><span /><span /><span /></button>
          <Link className="wordmark" href="/" aria-label="Open Splunk home"><span>open</span><b>&gt;</b><span>splunk</span></Link>
          <div className="header-menu-wrap">
            <button className="product-menu-button" type="button" aria-haspopup="menu" aria-expanded={menu === "app"} aria-busy={appSwitchingId !== null || (backendEnabled && backendConnectionState === "loading")} onClick={() => setMenu(menu === "app" ? null : "app")}>
              App: <strong>{workspaceAppName}</strong> <span aria-hidden="true">▾</span>
            </button>
            {menu === "app" ? (
              <div className="floating-menu app-menu" role="menu">
                <span className="menu-label">{backendEnabled ? "Server apps" : "Your apps"}</span>
                {backendEnabled
                  ? backendBootstrapModel === null
                    ? backendConnectionState === "error"
                      ? <button role="menuitem" type="button" onClick={() => { setMenu(null); void retryBackendConnection(); }}><span className="app-glyph">!</span><span><strong>Retry backend connection</strong><small>System bootstrap is unavailable</small></span></button>
                      : <button role="menuitem" type="button" disabled><span className="app-glyph">…</span><span><strong>Loading apps</strong><small>Reading system bootstrap</small></span></button>
                    : backendBootstrapModel.apps.length === 0
                      ? <button role="menuitem" type="button" disabled><span className="app-glyph">—</span><span><strong>No authorized apps</strong><small>The backend returned an empty app catalog</small></span></button>
                      : backendBootstrapModel.apps.map((app) => {
                      const selected = app.appId === backendBootstrapModel.selectedAppId;
                      return (
                        <button
                          className={selected ? "selected" : undefined}
                          aria-busy={appSwitchingId === app.appId}
                          disabled={
                            isRunning
                            || appSwitchingId !== null
                            || objectMutation !== null
                            || historyClearBusy
                            || selected
                          }
                          key={app.appId}
                          role="menuitem"
                          type="button"
                          onClick={() => void switchBackendApp(app.appId)}
                        >
                          <span className="app-glyph">⌕</span>
                          <span>
                            <strong>{appSwitchingId === app.appId ? "Switching…" : app.displayName || app.slug || app.appId}</strong>
                            <small>{app.defaultIndexNames.length > 0 ? `Default ${app.defaultIndexNames.join(", ")}` : "No default index"}</small>
                          </span>
                          {selected ? <b>✓</b> : null}
                        </button>
                      );
                    })
                  : <>
                      <Link role="menuitem" href="/search/" className="selected"><span className="app-glyph">⌕</span><span><strong>Search &amp; Reporting</strong><small>Search all authorized indexes</small></span><b>✓</b></Link>
                      <Link role="menuitem" href="/dashboards/"><span className="app-glyph">G</span><span><strong>GradeThis Operations</strong><small>Default index: gradethis</small></span></Link>
                    </>}
                <div className="menu-separator" />
                <Link role="menuitem" href="/admin/"><span className="app-glyph">＋</span><span><strong>Manage apps</strong></span></Link>
              </div>
            ) : null}
          </div>
        </div>
        <nav className="product-utilities" aria-label="Product utilities">
          <Link className="health-indicator" href="/admin/" title="Open system health"><span /> Healthy</Link>
          <button type="button" onClick={() => showToast("No new messages.")}>Messages <span aria-hidden="true">▾</span></button>
          <Link href="/admin/">Settings <span aria-hidden="true">▾</span></Link>
          <div className="header-menu-wrap">
            <button type="button" aria-haspopup="menu" aria-expanded={menu === "activity"} onClick={() => setMenu(menu === "activity" ? null : "activity")}>Activity <span className="activity-count">1</span> <span aria-hidden="true">▾</span></button>
            {menu === "activity" ? (
              <div className="floating-menu utility-menu" role="menu">
                <span className="menu-label">Activity</span>
                <button aria-label={`Open active search job: ${phaseLabel(phase)}`} role="menuitem" type="button" onClick={() => { setModal("jobs"); setMenu(null); }}><span className={`mini-status ${stateClass(phase)}`} /> <span><strong>{phaseLabel(phase)}</strong><small>{visibleCountPrefix}{NUMBER_FORMAT.format(visibleEventCount)} results · {elapsed}</small></span></button>
                <Link role="menuitem" href="/activity/">View all activity</Link>
              </div>
            ) : null}
          </div>
          <div className="header-menu-wrap">
            <button type="button" aria-haspopup="menu" aria-expanded={menu === "help"} onClick={() => setMenu(menu === "help" ? null : "help")}>Help <span aria-hidden="true">▾</span></button>
            {menu === "help" ? (
              <div className="floating-menu utility-menu help-menu" role="menu">
                <span className="menu-label">Search help</span>
                <button role="menuitem" type="button" onClick={() => showToast("SPL reference will open in a documentation pane.")}>SPL command reference</button>
                <button role="menuitem" type="button" onClick={() => showToast("Tip: press Ctrl+Space inside the editor for completions.")}>Keyboard shortcuts</button>
                <button role="menuitem" type="button" onClick={() => showToast("Open Splunk frontend preview · v0.1.0")}>About Open Splunk</button>
              </div>
            ) : null}
          </div>
          <form className="global-search" aria-label="Find events" onSubmit={handleGlobalFind}>
            <label className="sr-only" htmlFor="search-workspace-find">Find events or enter SPL</label>
            <input id="search-workspace-find" placeholder="Find" value={globalFind} onChange={(event) => setGlobalFind(event.target.value)} />
            <button type="submit" aria-label="Run Find">⌕</button>
          </form>
          <div className="header-menu-wrap">
            <button className="user-button" type="button" aria-label="User menu" aria-haspopup="menu" aria-expanded={menu === "user"} onClick={() => setMenu(menu === "user" ? null : "user")}><span>A</span> Administrator <b>▾</b></button>
            {menu === "user" ? (
              <div className="floating-menu utility-menu user-menu" role="menu">
                <div className="user-summary"><span>A</span><strong>Administrator</strong><small>admin@localhost</small></div>
                <Link role="menuitem" href="/admin/">Account settings</Link>
                <button role="menuitem" type="button" onClick={() => showToast("Open Splunk is running in trusted-network mode.")}>Session details</button>
                <Link role="menuitem" href="/signin/">Sign out</Link>
              </div>
            ) : null}
          </div>
        </nav>
      </header>

      <nav className="app-bar" aria-label="Search and Reporting navigation">
        <div className="app-tabs">
          <Link className="active" href="/search/">Search</Link>
          <Link href="/analytics/">Analytics</Link>
          <Link href="/datasets/">Datasets</Link>
          <Link href="/reports/">Reports</Link>
          <Link href="/activity/">Activity</Link>
          <Link href="/dashboards/">Dashboards</Link>
        </div>
        <div className="app-identity"><span aria-hidden="true">⌕</span><strong>{workspaceAppName}</strong></div>
      </nav>

      {mobileProductNavOpen ? (
        <>
          <button className="search-mobile-backdrop" type="button" aria-label="Close product navigation" onClick={() => setMobileProductNavOpen(false)} />
          <dialog ref={mobileProductDrawerRef} className="search-mobile-drawer" open aria-modal="true" aria-label="Product navigation">
            <header><div><span>A</span><div><strong>Administrator</strong><small>admin@localhost</small></div></div><button type="button" aria-label="Close product navigation" onClick={() => setMobileProductNavOpen(false)}>×</button></header>
            <span className="search-mobile-label">APPLICATION</span>
            <Link href="/"><i>⌂</i>Home</Link><Link className="active" href="/search/"><i>⌕</i>{workspaceAppName}</Link><Link href="/analytics/"><i>⌁</i>Analytics</Link><Link href="/datasets/"><i>▦</i>Datasets</Link><Link href="/reports/"><i>▤</i>Reports</Link><Link href="/dashboards/"><i>▥</i>Dashboards</Link>
            <span className="search-mobile-label">SYSTEM</span>
            <Link href="/activity/"><i>↻</i>Activity <b className="activity-count">1</b></Link><Link href="/admin/"><i>⚙</i>Administration</Link><Link href="/signin/"><i>⇥</i>Sign out</Link>
          </dialog>
        </>
      ) : null}

      {menu !== null ? <button type="button" className="menu-dismiss" aria-label="Close menu" onClick={() => setMenu(null)} /> : null}

      <main className="search-page" id="search-main-content" tabIndex={-1}>
        <header className="search-title-row">
          <div className="search-title">
            <h1>{activeSavedSearchId === null ? "New Search" : savedSearches.find((item) => item.id === activeSavedSearchId)?.name}</h1>
            {savedDefinitionDirty ? (
              <span className="unsaved-dot" title="This saved search has changes that have not been saved">Unsaved changes</span>
            ) : dirty ? (
              <span className="search-draft-hint" title="The editor differs from the displayed search job">
                {submittedQuery.trim().length === 0 && activeSavedSearchId !== null
                  ? "Run to load results"
                  : "Run to apply draft"}
              </span>
            ) : null}
            <span
              className={`demo-badge${backendEnabled ? " backend-data-badge" : ""}`}
              title={backendEnabled ? "Searches run against the Open Splunk backend" : "Searches use deterministic frontend fixtures"}
            ><i /> {backendEnabled ? "Backend data" : "Demo data"}</span>
          </div>
          <div className="search-actions" aria-label="Search actions">
            <button type="button" onClick={() => setModal("open")}><span aria-hidden="true">⌕</span> Open</button>
            <button className="search-action-save" type="button" onClick={quickSave}><span aria-hidden="true">✓</span> Save</button>
            <div className="header-menu-wrap">
              <button ref={saveAsButtonRef} type="button" aria-haspopup="menu" aria-expanded={menu === "save-as"} onClick={() => setMenu(menu === "save-as" ? null : "save-as")}>Save As <span aria-hidden="true">▾</span></button>
              {menu === "save-as" ? (
                <div className="floating-menu action-menu" role="menu">
                  <button role="menuitem" type="button" onClick={() => openSaveDialog(saveAsButtonRef.current, true)}><span>⌕</span><span><strong>Saved search</strong><small>Preserve this SPL and time range</small></span></button>
                  <button role="menuitem" type="button" onClick={() => showToast("Reports extend saved searches in a later phase.")}><span>▤</span><span><strong>Report</strong><small>Save table and visualization settings</small></span></button>
                  <button role="menuitem" type="button" onClick={() => showToast("Alerts are planned after scheduled searches.")}><span>⚑</span><span><strong>Alert</strong><small>Schedule and notify</small></span></button>
                </div>
              ) : null}
            </div>
            <button type="button" onClick={() => setModal("history")}><span aria-hidden="true">↶</span> History</button>
            <button
              type="button"
              disabled={backendEnabled && !backendAuthoritativeResultsReady}
              title={backendEnabled && !backendAuthoritativeResultsReady
                ? "Authoritative results are required before export"
                : undefined}
              onClick={() => openExportDialog()}
            ><span aria-hidden="true">⇩</span> Export</button>
            <button className="close-search" type="button" onClick={closeSearchWorkspace}>Close</button>
            <div className="header-menu-wrap mobile-search-actions">
              <button type="button" aria-haspopup="menu" aria-expanded={menu === "search-actions"} onClick={() => setMenu(menu === "search-actions" ? null : "search-actions")}>More <span aria-hidden="true">▾</span></button>
              {menu === "search-actions" ? <div className="floating-menu mobile-search-menu" role="menu"><button role="menuitem" type="button" onClick={() => { setModal("open"); setMenu(null); }}>⌕ <span>Open saved search</span></button><button role="menuitem" type="button" onClick={() => openSaveDialog(null, true)}>＋ <span>Save as new</span></button><button role="menuitem" type="button" onClick={() => { setModal("history"); setMenu(null); }}>↶ <span>Search history</span></button><button role="menuitem" type="button" disabled={backendEnabled && !backendAuthoritativeResultsReady} title={backendEnabled && !backendAuthoritativeResultsReady ? "Authoritative results are required before export" : undefined} onClick={() => { openExportDialog(); setMenu(null); }}>⇩ <span>Export results</span></button><Link role="menuitem" href="/activity/">ⓘ <span>View activity</span></Link><button role="menuitem" type="button" onClick={closeSearchWorkspace}>× <span>Close search</span></button></div> : null}
            </div>
          </div>
        </header>

        <SearchComposer
          absoluteEnd={absoluteEnd}
          absoluteStart={absoluteStart}
          absoluteTimeInvalid={absoluteTimeInvalid}
          backendTimeSyntax={backendEnabled}
          completionIndex={completionIndex}
          completionOpen={completionOpen}
          diagnostic={editorDiagnostic}
          draftTimeRange={draftTimeRange}
          editorFocused={editorFocused}
          editorLineCount={editorLineCount}
          editorRef={editorRef}
          filteredCompletions={filteredCompletions}
          gutterLinesRef={gutterLinesRef}
          highlightRef={highlightRef}
          isRunning={isRunning}
          launchPending={persistedLaunchPending}
          modal={modal}
          query={query}
          relativeAmount={relativeAmount}
          relativeUnit={relativeUnit}
          runDisabledReason={runDisabledReason}
          timePickerRef={timePickerRef}
          timePickerSection={timePickerSection}
          timeRange={timeRange}
          timeRangeButtonRef={timeRangeButtonRef}
          onAbsoluteRangeChange={updateAbsoluteRange}
          onCancelSearch={cancelSearch}
          onCloseTimePicker={closeTimePicker}
          onCompletionIndexChange={setCompletionIndex}
          onCompletionOpenChange={setCompletionOpen}
          onDiagnosticFix={fixDiagnostic}
          onDraftTimeRangeChange={setDraftTimeRange}
          onEditorCaretChange={setEditorCaret}
          onEditorChange={handleEditorChange}
          onEditorFocusedChange={setEditorFocused}
          onEditorKeyDown={handleEditorKeyDown}
          onEditorScroll={handleEditorScroll}
          onInsertCompletion={insertCompletion}
          onModalChange={setModal}
          onRelativeRangeChange={updateRelativeRange}
          onRunSearch={() => {
            if (backendWorkspaceTransitionBlocked()) return;
            if (runDisabledReason !== null) {
              showToast(runDisabledReason, "warning");
              return;
            }
            if (dirty) timelineZoomParentRef.current = null;
            runSearch();
          }}
          onSeedAbsoluteRange={seedAbsoluteRange}
          onTimePickerSectionChange={setTimePickerSection}
          onTimeRangeChange={handleManualTimeRangeChange}
        />

        <section className={`job-strip${searchIsClosed ? " is-closed" : ""}`} data-testid="job-strip" aria-label="Search job status" aria-busy={isRunning}>
          <div className="job-primary">
            <span className={`job-state-icon ${stateClass(phase)}`} aria-hidden="true">{phase === "completed" ? "✓" : phase === "failed" ? "!" : phase === "canceled" ? "×" : phase === "expired" ? "⌛" : ""}</span>
            <span className="job-result-copy">
              <output className="sr-only" aria-live="polite" aria-atomic="true">Search status: {persistedLaunchPending ? "Opening persisted search" : phaseLabel(phase)}</output>
              <strong aria-hidden="true">{persistedLaunchPending ? "Opening persisted search" : phaseLabel(phase)}</strong>
              <span>{visibleCountPrefix}{NUMBER_FORMAT.format(visibleEventCount)} {backendEnabled ? backendPrimaryCountLabel : "events"}</span>
              <small data-testid="job-time-range">
                {!backendEnabled && submittedTimeRange.label === "Last 24 hours"
                  ? "7/20/26 3:44:00 PM to 7/21/26 3:44:00 PM"
                  : resolvedTimeRangeLabel ?? submittedTimeRange.label}
              </small>
            </span>
            {backendEnabled
              ? <button className="sampling-button" type="button" onClick={() => setModal("settings")}>Server result policy <span aria-hidden="true">ⓘ</span></button>
              : <button className="sampling-button" type="button" onClick={() => showToast("The demo result set is fixed and is not sampled.")}>No Event Sampling <span aria-hidden="true">▾</span></button>}
          </div>
          <div className="job-metrics" aria-label="Job metrics">
            <span><small>Scanned</small><strong>{scannedRowsApproximate ? "≈ " : ""}{NUMBER_FORMAT.format(scannedRows)} rows</strong></span>
            <span><small>{backendEnabled ? "Result data" : "Data"}</small><strong>{scannedBytes}</strong></span>
            <span><small>Elapsed</small><strong>{elapsed}</strong></span>
            <span><small>Progress</small><strong aria-hidden="true">{progress}%</strong><progress className="sr-only" aria-label="Search progress" max={100} value={progress}>{progress}%</progress></span>
          </div>
          <div className="job-controls">
            <button type="button" onClick={() => setModal("jobs")}>Job <span aria-hidden="true">▾</span></button>
            <button type="button" aria-label="Inspect search job" title="Inspect job" onClick={() => setModal("inspect")}>ⓘ</button>
            <button
              type="button"
              aria-label="Refresh results"
              title={submittedQuery.trim().length === 0
                ? "Run a search before refreshing results"
                : backendEnabled && backendConnectionState !== "ready"
                  ? "Restore the backend connection before refreshing results"
                  : "Refresh results"}
              disabled={submittedQuery.trim().length === 0 || (backendEnabled && backendConnectionState !== "ready")}
              onClick={() => runSearch(submittedQuery, submittedTimeRange)}
            >↻</button>
            <button
              type="button"
              aria-label="Share search"
              title={submittedQuery.trim().length === 0 ? "Run a search before sharing it" : "Share search"}
              disabled={submittedQuery.trim().length === 0}
              onClick={() => {
                const url = new URL(window.location.href);
                url.searchParams.set("q", submittedQuery);
                url.searchParams.set("earliest", submittedTimeRange.earliest);
                url.searchParams.set("latest", submittedTimeRange.latest);
                url.searchParams.set("label", submittedTimeRange.label);
                if (submittedTimeRange.timezone) {
                  url.searchParams.set("timezone", submittedTimeRange.timezone);
                } else {
                  url.searchParams.delete("timezone");
                }
                url.searchParams.set("run", "1");
                url.hash = "search";
                void copyText(url.toString(), "Search link copied to the clipboard.");
              }}
            >⌁</button>
            <div className="header-menu-wrap search-mode-wrap">
              {backendEnabled
                ? <button type="button" title="Execution options are controlled by the connected server" onClick={() => setModal("settings")}><span aria-hidden="true">⚡</span> Server Mode <span aria-hidden="true">ⓘ</span></button>
                : <button type="button" aria-haspopup="menu" aria-expanded={menu === "search-mode"} onClick={() => setMenu(menu === "search-mode" ? null : "search-mode")}><span aria-hidden="true">⚡</span> {searchMode} Mode <span aria-hidden="true">▾</span></button>}
              {!backendEnabled && menu === "search-mode" ? (
                <div className="floating-menu mode-menu" role="menu">
                  {(["Fast", "Smart", "Verbose"] as const).map((modeName) => (
                    <button role="menuitemradio" aria-checked={searchMode === modeName} type="button" key={modeName} onClick={() => { setSearchMode(modeName); setMenu(null); }}>
                      <span className="radio-mark">{searchMode === modeName ? "●" : "○"}</span><span><strong>{modeName}</strong><small>{modeName === "Fast" ? "Prioritize search performance" : modeName === "Smart" ? "Balance speed and field discovery" : "Discover all available fields"}</small></span>
                    </button>
                  ))}
                </div>
              ) : null}
            </div>
          </div>
          {isRunning ? <span className="job-progress-bar" aria-hidden="true" style={{ width: `${progress}%` }} /> : null}
        </section>

        {backendRuntimeNotices.length === 0 || searchIsClosed ? null : (
          <output className="system-notice" aria-label="Backend result notices">
            <span className="system-notice__icon" aria-hidden="true">!</span>
            <div>
              <strong>{backendRuntimeNotices.length === 1 ? "Backend result notice" : "Backend result notices"}</strong>
              <small>{backendRuntimeNotices.join(" ")}</small>
            </div>
          </output>
        )}

        {backendEnabled ? (
          <output className="sr-only" aria-live="polite" aria-atomic="true">
            {backendPreviewAnnouncement}
          </output>
        ) : null}
        {backendPreviewStatusPresentation === null || searchIsClosed ? null : (
          <section
            className={`backend-preview-status status-${backendPreviewStatus} is-${backendPreviewStatusPresentation.tone}`}
            aria-label="Live result preview status"
            data-status={backendPreviewStatus}
            data-testid="backend-preview-status"
          >
            <span className="backend-preview-status__pulse" aria-hidden="true" />
            <strong>{backendPreviewStatusPresentation.title}</strong>
            <span>{backendPreviewStatusPresentation.detail}</span>
          </section>
        )}

        {backendEnabled
        && hasResultData
        && backendResultKind !== ResultSetKind.RESULT_SET_KIND_EVENTS
        && (backendHasNextPage || eventPage > 1) ? (
          <nav className="backend-result-pager" aria-label="Server result pages">
            <span>
              Server page {NUMBER_FORMAT.format(eventPage)}
              {backendResultTotalRows === null
                ? ""
                : ` · ${backendResultTotalExact ? "" : "at least "}${NUMBER_FORMAT.format(backendResultTotalRows)} total rows`}
            </span>
            <small>Column sorting applies to the loaded page. Use SPL <code>sort</code> for global ordering.</small>
            <div>
              <button className="button secondary compact" type="button" disabled={eventPage === 1} onClick={() => void openBackendEventPage(eventPage - 1)}>‹ Previous</button>
              <button className="button secondary compact" type="button" disabled={!backendHasNextPage} onClick={() => void openBackendEventPage(eventPage + 1)}>Next ›</button>
            </div>
          </nav>
        ) : null}

        <div className={`result-tabs${searchIsClosed ? " is-closed" : ""}`} role="tablist" aria-label="Search result views">
          {([
            ["events", "Events", backendEnabled && backendResultKind !== ResultSetKind.RESULT_SET_KIND_EVENTS ? "0" : `${visibleCountPrefix}${NUMBER_FORMAT.format(visibleEventCount)}`],
            ["patterns", "Patterns", backendEnabled ? "0" : hasResultData ? String(patternRows.length) : "0"],
            ["statistics", "Statistics", backendEnabled && backendResultKind === ResultSetKind.RESULT_SET_KIND_EVENTS
              ? "0"
              : hasResultData
                ? `${backendDisplayingPreview
                  ? backendPreviewDisplay?.snapshot.truncated ? "≥" : ""
                  : backendEnabled && !backendResultTotalExact ? "≥" : ""}${NUMBER_FORMAT.format(statisticsRowCount)}`
                : "0"],
            ["visualization", "Visualization", ""],
          ] as const).map(([id, label, count]) => (
            <button
              id={`tab-${id}`}
              data-testid={`result-tab-${id}`}
              role="tab"
              aria-selected={activeTab === id}
              aria-controls={`panel-${id}`}
              tabIndex={activeTab === id ? 0 : -1}
              className={activeTab === id ? "active" : ""}
              type="button"
              key={id}
              disabled={!resultTabAvailable(id)}
              title={resultTabAvailable(id) ? undefined : "This view is not available for the server result type."}
              onClick={() => setActiveTab(id)}
              onKeyDown={(event) => handleResultTabKeyDown(event, id)}
            >
              {label}{count.length === 0 ? null : <span>{count}</span>}
            </button>
          ))}
        </div>

        {!hasResultData ? (
          <section
            id={`panel-${activeTab}`}
            role="tabpanel"
            aria-labelledby={`tab-${activeTab}`}
            className="job-empty-results"
            data-testid="job-empty-results"
            aria-live="polite"
          >
            <span aria-hidden="true">{emptyResultPresentation.icon}</span>
            <strong>{emptyResultPresentation.title}</strong>
            <p>{emptyResultPresentation.detail}</p>
            {backendEnabled && backendConnectionState === "error"
              ? <button className="button secondary compact" type="button" onClick={() => void retryBackendConnection()}>Retry backend connection</button>
              : backendHasNoSearchableIndexes
                ? <Link className="button secondary compact" href="/admin/">Review index access</Link>
                : emptyStateCanRun
                  ? <button className="button secondary compact" type="button" onClick={() => {
                      if (backendWorkspaceTransitionBlocked()) return;
                      timelineZoomParentRef.current = null;
                      clearPersistedContextForAdHocSearch();
                      runSearch(emptyStateRunQuery);
                    }}>{searchIsClosed ? "Run the default search" : "Run search again"}</button>
                  : null}
          </section>
        ) : null}

        {hasResultData && activeTab === "events" ? (
          <EventsPanel
            activeField={activeField}
            backendEnabled={backendEnabled}
            backendHasNextPage={backendHasNextPage}
            backendResultTotalExact={backendResultTotalExact}
            backendResultTotalRows={backendResultTotalRows}
            defaultQuery={defaultSearchQuery}
            draggingTimeline={draggingTimeline}
            eventDisplay={eventDisplay}
            eventPage={eventPage}
            eventPageCount={eventPageCount}
            eventPageSize={currentResultPageSize}
            eventSortDirection={eventSortDirection}
            expandedEvents={expandedEvents}
            fieldFilter={fieldFilter}
            fieldSummaryError={backendEnabled ? backendFieldSummaryError : null}
            fieldSummaryLoading={backendEnabled && backendFieldSummaryLoading}
            fields={fields}
            fieldsCollapsed={fieldsCollapsed}
            fieldsHasMore={backendEnabled && backendFieldsHasMore}
            fieldsLoading={backendEnabled && backendFieldsLoading}
            fieldsLoadingMore={backendEnabled && backendFieldsLoadingMore}
            isPreview={backendDisplayingPreview}
            menu={menu}
            maximumEventPageSize={backendEnabled && backendBootstrapRef.current !== null
              ? backendMaximumPageSize(backendBootstrapRef.current)
              : null}
            pagedResultEvents={pagedResultEvents}
            previewTruncated={backendPreviewDisplay?.snapshot.truncated === true}
            resultEvents={resultEvents}
            showAllFields={showAllFields}
            submittedQuery={submittedQuery}
            timelineDisplay={timelineDisplay}
            timelinePoints={timelinePoints}
            timelineSelection={timelineSelection}
            timelineSelectionZoomable={timelineSelectionZoomable}
            wrapEvents={wrapEvents}
            applyPivot={applyPivot}
            copyText={copyText}
            endTimelineDrag={endTimelineDrag}
            moveTimelineDrag={moveTimelineDrag}
            onLoadMoreFields={() => void loadMoreBackendFields()}
            setActiveField={setActiveField}
            setEventDisplay={setEventDisplay}
            setEventPage={changeEventPage}
            setEventPageSize={changeEventPageSize}
            setEventSortDirection={setEventSortDirection}
            setFieldFilter={setFieldFilter}
            setFieldsCollapsed={setFieldsCollapsed}
            setMenu={setMenu}
            setQuery={setQuery}
            setShowAllFields={setShowAllFields}
            setTimelineDisplay={setTimelineDisplay}
            setTimelineEnd={setTimelineEnd}
            setTimelineStart={setTimelineStart}
            setWrapEvents={setWrapEvents}
            showToast={showToast}
            startTimelineDrag={startTimelineDrag}
            toggleEvent={toggleEvent}
            toggleField={toggleField}
            zoomTimeline={zoomTimeline}
            zoomOutTimeline={zoomOutTimeline}
            canZoomOut={timelineZoomParentRef.current !== null}
          />
        ) : null}

        {hasResultData && activeTab === "patterns" ? (
          <PatternsPanel
            menu={menu}
            patternRows={patternRows}
            patternSensitivity={patternSensitivity}
            onMenuChange={setMenu}
            onPatternSensitivityChange={setPatternSensitivity}
            onShowToast={showToast}
            onTabChange={setActiveTab}
            onViewEvents={(signature) => {
              if (backendWorkspaceTransitionBlocked()) return;
              timelineZoomParentRef.current = null;
              runSearch(queryForPattern(submittedQuery, signature), submittedTimeRange);
            }}
          />
        ) : null}

        {hasResultData && activeTab === "statistics" ? (
          <StatisticsPanel
            elapsed={elapsed}
            genericStatisticsTable={genericStatisticsTable}
            genericStatsSort={genericStatsSort}
            isPreview={backendDisplayingPreview}
            isTimechartResult={isTimechartResult}
            menu={menu}
            pageNumber={backendEnabled ? eventPage : 1}
            pageStart={backendStatisticsPageStart}
            previewTruncated={backendPreviewDisplay?.snapshot.truncated === true}
            resultTotalExact={backendDisplayingPreview
              ? backendPreviewDisplay?.snapshot.truncated !== true
              : !backendEnabled || backendResultTotalExact}
            resultTotalRows={backendDisplayingPreview
              ? backendPreviewDisplay?.snapshot.rows.length ?? 0
              : backendEnabled ? backendResultTotalRows : statisticsRowCount}
            sortedGenericStatisticsRows={sortedGenericStatisticsRows}
            sortedStatistics={sortedStatistics}
            sortedTimechartRows={sortedTimechartRows}
            statisticsDimension={statisticsDimension}
            statisticsRows={statisticsRows}
            statsDensity={statsDensity}
            statsSort={statsSort}
            timechartSort={timechartSort}
            timelinePoints={timelinePoints}
            onApplyPivot={(field, value) => applyPivot(field, value, "include")}
            onExport={() => openExportDialog("statistics")}
            onGenericStatsSortChange={updateGenericStatsSort}
            onMenuChange={setMenu}
            onStatsDensityChange={setStatsDensity}
            onStatsSortChange={updateStatsSort}
            onTimechartSortChange={setTimechartSort}
          />
        ) : null}

        {hasResultData && activeTab === "visualization" ? (
          <VisualizationPanel
            chartStyle={chartStyle}
            chartTitle={chartTitle}
            isPreview={backendDisplayingPreview}
            isTimechartResult={isTimechartResult}
            legendPosition={legendPosition}
            showDataLabels={showDataLabels}
            previewTruncated={backendPreviewDisplay?.snapshot.truncated === true}
            statisticsDimension={statisticsDimension}
            statisticsRows={statisticsRows}
            timelinePoints={timelinePoints}
            onApplyPivot={(field, value, mode) => applyPivot(field, value, mode)}
            onChartStyleChange={setChartStyle}
            onChartTitleChange={setChartTitle}
            onLegendPositionChange={setLegendPosition}
            onShowDataLabelsChange={setShowDataLabels}
            onVisualizationEdited={() => {
              preservedSavedVisualizationRef.current = null;
              visualizationEditedRef.current = true;
            }}
            onShowToast={showToast}
          />
        ) : null}
      </main>

      <WorkspaceDialogs
        activeSavedSearchId={activeSavedSearchId}
        activeTab={activeTab}
        appName={workspaceAppName}
        currentTimeMs={backendEnabled && backendBootstrapRef.current !== null
          ? currentBackendServerTime(backendBootstrapRef.current).valueOf()
          : Date.now()}
        dataMetricLabel={backendEnabled ? "Result data" : "Scanned data"}
        displayedExportRows={displayedRowsForTab(exportSourceTab)}
        elapsed={elapsed}
        exportFieldOptions={backendEnabled
          ? exportFieldsForTab(exportSourceTab)
          : exportSourceTab === "events"
            ? ["_time", "level", "logger", "message", "trace_id", "host", "source", "status", "duration_ms", "_raw"]
            : exportFieldsForTab(exportSourceTab)}
        exportFieldLabels={exportFieldLabels}
        exportFields={exportFields}
        exportState={exportDialogState}
        history={history}
        historyHasMore={backendEnabled && historyNextPageToken !== null}
        historyLibraryStatus={historyLoading
          ? "loading"
          : historyAvailable ? "available" : "unavailable"}
        historyLoadingMore={historyLoadingMore}
        historyClearState={historyClearBusy
          ? { status: "pending" }
          : historyClearError === null
            ? { status: "idle" }
            : { status: "error", error: historyClearError }}
        historyDeleteState={objectMutation?.kind === "deleteHistory"
          ? { status: "pending", targetId: objectMutation.targetId }
          : historyDeleteError === null
            ? { status: "idle" }
            : { status: "error", error: historyDeleteError }}
        historyFilter={historyFilter}
        jobCancelState={backendCancelPendingRef.current
          ? { status: "pending" }
          : cancelError === null
            ? { status: "idle" }
            : { status: "error", error: cancelError }}
        jobInspectorNotices={backendEnabled ? backendRuntimeNotices : []}
        isRunning={isRunning}
        modal={modal}
        phase={phase}
        resultCountLabel={backendEnabled ? backendPrimaryCountLabel : "events"}
        resultCountPrefix={visibleCountPrefix}
        saveDescription={saveDescription}
        saveDialogReturnFocus={saveDialogReturnFocusRef.current}
        saveName={saveName}
        saveState={objectMutation?.kind === "save"
          ? { status: "pending" }
          : saveError === null
            ? { status: "idle" }
            : { status: "error", error: saveError }}
        savedSearchFilter={savedSearchFilter}
        savedSearchDeleteState={objectMutation?.kind === "deleteSaved"
          ? { status: "pending", targetId: objectMutation.targetId }
          : savedSearchDeleteError === null
            ? { status: "idle" }
            : { status: "error", error: savedSearchDeleteError }}
        savedSearchDuplicateState={objectMutation?.kind === "duplicateSaved"
          ? { status: "pending", targetId: objectMutation.targetId }
          : savedSearchDuplicateError === null
            ? { status: "idle" }
            : { status: "error", error: savedSearchDuplicateError }}
        savedSearchDeleteTarget={savedSearchDeleteTargetId === null
          ? null
          : savedSearches.find((savedSearch) => savedSearch.id === savedSearchDeleteTargetId) ?? null}
        savedSearchRenameName={savedSearchRenameName}
        savedSearchRenameState={objectMutation?.kind === "renameSaved"
          ? { status: "pending", targetId: objectMutation.targetId }
          : savedSearchRenameError === null
            ? { status: "idle" }
            : { status: "error", error: savedSearchRenameError, targetId: savedSearchRenameTargetId }}
        savedSearchRenameTarget={savedSearchRenameTargetId === null
          ? null
          : savedSearches.find((savedSearch) => savedSearch.id === savedSearchRenameTargetId) ?? null}
        savedSearchHasMore={backendEnabled && savedSearchesNextPageToken !== null}
        savedSearchLibraryStatus={savedSearchesLoading
          ? "loading"
          : savedSearchesAvailable ? "available" : "unavailable"}
        savedSearchLoadingMore={savedSearchesLoadingMore}
        savedSearches={savedSearches}
        scannedBytes={scannedBytes}
        resolvedTimeRangeLabel={resolvedTimeRangeLabel}
        scannedRows={scannedRows}
        scannedRowsApproximate={scannedRowsApproximate}
        searchId={backendEnabled ? backendJobIdRef.current ?? "Pending dispatch" : `scheduler_admin_search_${generationRef.current || 1}`}
        searchMode={backendEnabled ? "Server controlled" : searchMode}
        searchSettingsCapabilities={searchSettingsCapabilities}
        submittedQuery={submittedQuery}
        submittedTimeRange={submittedTimeRange}
        timeRange={timeRange}
        visibleEventCount={visibleEventCount}
        onCancelExport={() => void cancelActiveExport()}
        onCancelSearch={cancelSearch}
        onClearHistory={() => void clearHistory()}
        onDeleteHistoryEntry={deleteHistoryEntry}
        onDeleteSavedSearch={deleteSavedSearch}
        onDuplicateSavedSearch={(id) => void duplicateSavedSearch(id)}
        onRenameSavedSearch={() => void renameSavedSearch()}
        onRequestRenameSavedSearch={(id) => {
          const savedSearch = savedSearches.find((item) => item.id === id);
          if (savedSearch === undefined) return;
          setSavedSearchRenameError(null);
          setSavedSearchRenameTargetId(id);
          setSavedSearchRenameName(savedSearch.name);
          setModal("rename-saved-search");
        }}
        onRequestDeleteSavedSearch={(id) => {
          setSavedSearchDeleteError(null);
          setSavedSearchDeleteTargetId(id);
          setModal("delete-saved-search");
        }}
        onDownloadExport={downloadExport}
        onExportFieldToggle={toggleExportField}
        onExportFormatChange={(format) => {
          resetExport();
          setExportFormat(format);
        }}
        onHistoryEntryOpen={openHistoryEntry}
        onHistoryEntrySave={saveHistoryEntry}
        onHistoryFilterChange={setHistoryFilter}
        onLoadMoreHistory={() => void loadMoreBackendHistory()}
        onLoadMoreSavedSearches={() => void loadMoreBackendSavedSearches()}
        onModalChange={setModal}
        onOpenSavedSearch={openSavedSearch}
        onPrepareExport={() => void prepareExport()}
        onResetExport={resetExport}
        onSaveDescriptionChange={setSaveDescription}
        onSaveNameChange={setSaveName}
        onSaveSearch={saveSearch}
        onSavedSearchFilterChange={setSavedSearchFilter}
        onSavedSearchRenameNameChange={setSavedSearchRenameName}
        onSearchCapabilityChange={(capability) => {
          const label = capability === "fieldDiscovery"
            ? "Field discovery"
            : capability === "liveResultPreviews"
              ? "Live result previews"
              : "Event sampling";
          showToast(`${label} is managed by the current data source.`, "info");
        }}
      />

      {toast === null ? null : (
        <output className={`toast toast-${toast.tone}`} data-testid="toast">
          <span aria-hidden="true">{toast.tone === "success" ? "✓" : toast.tone === "warning" ? "!" : "i"}</span>
          <strong>{toast.message}</strong>
          <button type="button" aria-label="Dismiss notification" onClick={() => setToast(null)}>×</button>
        </output>
      )}
    </div>
  );
}
