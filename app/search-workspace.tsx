"use client";

/* oxlint-disable jsx-a11y/prefer-tag-over-role */

import {
  type ChangeEvent,
  type KeyboardEvent,
  type PointerEvent,
  type ReactNode,
  type UIEvent,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

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
import type { ResultRow } from "@/gen/ts/open_splunk/v1/result";
import { SearchJobState, type SearchJob } from "@/gen/ts/open_splunk/v1/search";
import { createOpenSplunkApiClient } from "@/lib/api";
import {
  adaptSearchResults,
  indexesFromSPL,
  patternsFromEvents,
  resolveAbsoluteTimeRange,
  type SearchDataMode,
  type WorkspaceStatistic,
} from "@/lib/search/backend-data";
import { applyFieldPivot, type PivotMode } from "@/lib/search/query-pivots";
import {
  applyDiagnosticFix,
  completionContextAt,
  getQueryDiagnostic,
  isCursorInQuotedValue,
  type SplDiagnostic,
} from "@/lib/search/spl-editor";

type ResultTab = "events" | "patterns" | "statistics" | "visualization";
type ModalName = "time" | "save" | "open" | "history" | "export" | "settings" | "jobs" | "inspect";
type MenuName =
  | "app"
  | "user"
  | "activity"
  | "help"
  | "search-mode"
  | "save-as"
  | "stats-format"
  | "pattern-sensitivity"
  | "event-display"
  | "event-page-size"
  | "timeline-format";
type JobPhase =
  | "queued"
  | "parsing"
  | "planning"
  | "running"
  | "finalizing"
  | "completed"
  | "failed"
  | "canceled";
type SearchMode = "Smart" | "Fast" | "Verbose";
type ExportStage = "configure" | "preparing" | "ready";
type TimePickerSection = "presets" | "relative" | "range" | "advanced";
type StatsDensity = "compact" | "standard";
type PatternSensitivity = "Precise" | "Balanced" | "Broad";
type EventDisplay = "List" | "Raw";
type TimelineDisplay = "Columns" | "Compact";
type ChartStyle = "column" | "horizontal" | "line";

interface TimeRange {
  label: string;
  earliest: string;
  latest: string;
}

interface ToastState {
  message: string;
  tone: "success" | "info" | "warning";
}

interface ModalProps {
  title: string;
  subtitle?: string;
  wide?: boolean;
  onClose: () => void;
  children: ReactNode;
  footer?: ReactNode;
  returnFocus?: HTMLElement | null;
}

interface SearchWorkspaceProps {
  dataMode: SearchDataMode;
  apiBaseUrl?: string;
}

const DEFAULT_QUERY = "index=gradethis\n| sort -_time";
const NUMBER_FORMAT = new Intl.NumberFormat("en-US");
const COMPACT_NUMBER_FORMAT = new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 });

const TIME_PRESETS: TimeRange[] = [
  { label: "Last 15 minutes", earliest: "-15m", latest: "now" },
  { label: "Last 60 minutes", earliest: "-60m", latest: "now" },
  { label: "Last 4 hours", earliest: "-4h", latest: "now" },
  { label: "Last 24 hours", earliest: "-24h", latest: "now" },
  { label: "Last 7 days", earliest: "-7d", latest: "now" },
  { label: "Last 30 days", earliest: "-30d", latest: "now" },
  { label: "Today", earliest: "@d", latest: "now" },
  { label: "Yesterday", earliest: "-1d@d", latest: "@d" },
  { label: "All time", earliest: "0", latest: "now" },
];

const COMPLETIONS = [
  { label: "stats", insertion: "stats count by level", detail: "Calculate aggregate statistics." },
  { label: "timechart", insertion: "timechart span=5m count by level", detail: "Create a time-series result." },
  { label: "table", insertion: "table _time level logger message trace_id", detail: "Keep fields in the listed order." },
  { label: "sort", insertion: "sort -_time", detail: "Sort newest events first." },
  { label: "where", insertion: "where status >= 500", detail: "Filter with an evaluated expression." },
];

const ACTIVE_PHASES = new Set<JobPhase>(["queued", "parsing", "planning", "running", "finalizing"]);

const EVENT_EXPORT_FIELDS = ["_time", "level", "logger", "message", "trace_id"];
const EXPORT_FIELDS_BY_TAB: Record<ResultTab, string[]> = {
  events: EVENT_EXPORT_FIELDS,
  patterns: ["pattern", "count", "percent"],
  statistics: ["level", "count", "percent", "avgDuration"],
  visualization: ["level", "count", "percent", "avgDuration"],
};

const EXPORT_FIELD_LABELS: Record<string, string> = {
  avgDuration: "avg(duration_ms)",
  percent: "% of results",
};

function Modal({ title, subtitle, wide = false, onClose, children, footer, returnFocus = null }: ModalProps) {
  const dialogRef = useRef<HTMLDialogElement>(null);
  const returnFocusRef = useRef<HTMLElement | null>(null);

  useEffect(() => {
    const dialog = dialogRef.current;
    if (dialog === null) return;
    const mountedDialog: HTMLDialogElement = dialog;
    returnFocusRef.current = returnFocus ?? (document.activeElement instanceof HTMLElement ? document.activeElement : null);

    const focusableControls = () => Array.from(mountedDialog.querySelectorAll<HTMLElement>(
      'button:not(:disabled), input:not(:disabled), select:not(:disabled), textarea:not(:disabled), a[href], [tabindex]:not([tabindex="-1"])',
    )).filter((element) => !element.hasAttribute("hidden") && element.getClientRects().length > 0);

    const focusFrame = window.requestAnimationFrame(() => {
      (focusableControls()[0] ?? mountedDialog).focus();
    });

    function trapFocus(event: globalThis.KeyboardEvent) {
      if (event.key !== "Tab") return;
      const controls = focusableControls();
      if (controls.length === 0) {
        event.preventDefault();
        mountedDialog.focus();
        return;
      }
      const first = controls[0];
      const last = controls.at(-1) ?? first;
      if (event.shiftKey && (document.activeElement === first || !mountedDialog.contains(document.activeElement))) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && (document.activeElement === last || !mountedDialog.contains(document.activeElement))) {
        event.preventDefault();
        first.focus();
      }
    }

    document.addEventListener("keydown", trapFocus);
    return () => {
      window.cancelAnimationFrame(focusFrame);
      document.removeEventListener("keydown", trapFocus);
      if (returnFocusRef.current?.isConnected) returnFocusRef.current.focus();
    };
  }, [returnFocus]);

  return (
    <div className="modal-layer" data-testid="modal-layer">
      <button className="modal-backdrop" aria-label="Close dialog" type="button" onClick={onClose} />
      <dialog
        ref={dialogRef}
        open
        className={`modal-card${wide ? " modal-card-wide" : ""}`}
        aria-labelledby="modal-title"
        aria-modal="true"
        tabIndex={-1}
      >
        <header className="modal-header">
          <div>
            <h2 id="modal-title">{title}</h2>
            {subtitle === undefined ? null : <p>{subtitle}</p>}
          </div>
          <button className="icon-button close-button" aria-label="Close dialog" type="button" onClick={onClose}>
            ×
          </button>
        </header>
        <div className="modal-body">{children}</div>
        {footer === undefined ? null : <footer className="modal-footer">{footer}</footer>}
      </dialog>
    </div>
  );
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function syntaxTokens(query: string): ReactNode[] {
  const tokenPattern = /(\b(?:index|host|source|sourcetype|level|status|trace_id|message|path)\b(?=\s*=)|\b(?:sort|stats|timechart|table|where|eval|fields|rename|head|top|rare|rex|spath|bin|transaction|join|map|subsearch)\b|\b(?:count|avg|sum|min|max|p95|dc|values|tonumber|replace)\b|\b(?:AND|OR|NOT)\b|"(?:\\.|[^"\\])*"|\|)/gi;
  const unsupported = new Set(["transaction", "join", "map", "subsearch"]);
  const parts = query.split(tokenPattern).filter((part) => part !== undefined && part.length > 0);

  let sourceOffset = 0;
  return parts.map((part) => {
    sourceOffset += part.length;
    const lower = part.toLowerCase();
    let className = "spl-plain";
    if (part === "|") className = "spl-pipe";
    else if (part.startsWith('"')) className = "spl-string";
    else if (["and", "or", "not"].includes(lower)) className = "spl-boolean";
    else if (unsupported.has(lower)) className = "spl-error-token";
    else if (/^(sort|stats|timechart|table|where|eval|fields|rename|head|top|rare|rex|spath|bin)$/i.test(part)) {
      className = "spl-command";
    } else if (/^(count|avg|sum|min|max|p95|dc|values|tonumber|replace)$/i.test(part)) {
      className = "spl-function";
    } else if (/^(index|host|source|sourcetype|level|status|trace_id|message|path)$/i.test(part)) {
      className = "spl-field";
    }
    return (
      <span className={className} key={`${sourceOffset}-${part}`}>
        {part}
      </span>
    );
  });
}

function eventCountForQuery(query: string): number {
  const lowered = query.toLowerCase();
  if (lowered.includes("trace_id=")) return 18;
  if (lowered.includes("connection refused")) return 391;
  if (lowered.includes("status>=500") || lowered.includes("status >= 500")) return 812;
  if (lowered.includes("level=error") && lowered.includes("level=warn")) return 3923;
  if (lowered.includes("level=error")) return 1432;
  if (lowered.includes("level=warn")) return 2491;
  return 12_846;
}

function filteredDemoEvents(query: string): DemoEvent[] {
  const lowered = query.toLowerCase();
  let events = DEMO_EVENTS;
  if (lowered.includes("connection refused")) {
    events = events.filter((event) => String(event.fields.message).toLowerCase().includes("connection refused"));
  } else if (lowered.includes("status>=500") || lowered.includes("status >= 500")) {
    events = events.filter((event) => Number(event.fields.status ?? 0) >= 500);
  } else if (lowered.includes("level=error") && lowered.includes("level=warn")) {
    events = events.filter((event) => ["ERROR", "WARN"].includes(String(event.fields.level)));
  } else if (lowered.includes("level=error")) {
    events = events.filter((event) => event.fields.level === "ERROR");
  } else if (lowered.includes("level=warn")) {
    events = events.filter((event) => event.fields.level === "WARN");
  }

  return events;
}

function resultTabForQuery(query: string): ResultTab {
  const lowered = query.toLowerCase();
  if (lowered.includes("timechart") || lowered.includes(" chart ")) return "visualization";
  if (lowered.includes("stats") || lowered.includes(" top ") || lowered.includes(" rare ")) return "statistics";
  return "events";
}

function highlightedRaw(raw: string, query: string): ReactNode[] {
  const breakableRaw = raw.replaceAll(",", ",\u200b");
  const quoted = Array.from(query.matchAll(/"([^"\\]*(?:\\.[^"\\]*)*)"/g), (match) => match[1]).filter(Boolean);
  const fieldTerms = ["ERROR", "WARN", ...quoted].filter((term, index, terms) => terms.indexOf(term) === index).slice(0, 5);
  if (fieldTerms.length === 0) return [breakableRaw];
  const pattern = new RegExp(`(${fieldTerms.map(escapeRegExp).join("|")})`, "gi");
  let sourceOffset = 0;
  return breakableRaw.split(pattern).map((part) => {
    sourceOffset += part.length;
    return fieldTerms.some((term) => term.toLowerCase() === part.toLowerCase()) ? (
      <mark key={`${sourceOffset}-${part}`}>{part}</mark>
    ) : (
      <span key={`${sourceOffset}-${part}`}>{part}</span>
    );
  });
}

function queryForPattern(signature: string): string {
  const normalized = signature.replace(/\*+/g, "*").replaceAll('"', '\\"');
  const boundedPattern = normalized.replace(/^\*+|\*+$/g, "");
  return `index=gradethis\n| search _raw="*${boundedPattern}*"`;
}

function formatFieldValue(value: DemoScalar): string {
  if (value === null) return "null";
  return typeof value === "boolean" ? (value ? "true" : "false") : String(value);
}

function phaseLabel(phase: JobPhase): string {
  switch (phase) {
    case "queued":
      return "Queued";
    case "parsing":
      return "Parsing SPL";
    case "planning":
      return "Planning";
    case "running":
      return "Running";
    case "finalizing":
      return "Finalizing";
    case "completed":
      return "Completed";
    case "failed":
      return "Failed";
    case "canceled":
      return "Canceled";
  }
}

function stateClass(phase: JobPhase): string {
  if (phase === "completed") return "state-success";
  if (phase === "failed") return "state-error";
  if (phase === "canceled") return "state-muted";
  return "state-running";
}

function backendJobPhase(state: SearchJobState): JobPhase {
  switch (state) {
    case SearchJobState.SEARCH_JOB_STATE_QUEUED:
      return "queued";
    case SearchJobState.SEARCH_JOB_STATE_PARSING:
      return "parsing";
    case SearchJobState.SEARCH_JOB_STATE_PLANNING:
      return "planning";
    case SearchJobState.SEARCH_JOB_STATE_RUNNING:
      return "running";
    case SearchJobState.SEARCH_JOB_STATE_FINALIZING:
      return "finalizing";
    case SearchJobState.SEARCH_JOB_STATE_COMPLETED:
      return "completed";
    case SearchJobState.SEARCH_JOB_STATE_CANCELED:
      return "canceled";
    case SearchJobState.SEARCH_JOB_STATE_FAILED:
    case SearchJobState.SEARCH_JOB_STATE_EXPIRED:
      return "failed";
    default:
      return "queued";
  }
}

function formatDuration(duration: { seconds: bigint; nanos: number } | undefined): string {
  if (duration === undefined) return "0.00 s";
  const seconds = Number(duration.seconds) + duration.nanos / 1_000_000_000;
  return seconds < 1 ? `${Math.max(0, Math.round(seconds * 1000))} ms` : `${seconds.toFixed(2)} s`;
}

function formatBytes(value: bigint): string {
  const bytes = Number(value);
  if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const unit = Math.min(units.length - 1, Math.floor(Math.log(bytes) / Math.log(1024)));
  const scaled = bytes / 1024 ** unit;
  return `${scaled >= 10 || unit === 0 ? scaled.toFixed(0) : scaled.toFixed(1)} ${units[unit]}`;
}

function jobEventCount(job: SearchJob): number {
  const count = job.progress?.matchedEvents ?? job.progress?.producedRows ?? 0n;
  return Math.min(Number.MAX_SAFE_INTEGER, Number(count));
}

function timelineIndexFromPointer(event: PointerEvent<HTMLElement>): number | null {
  const target = (event.target as HTMLElement).closest<HTMLElement>("[data-timeline-index]");
  if (target === null) return null;
  const index = Number(target.dataset.timelineIndex);
  return Number.isFinite(index) ? index : null;
}

function timelineBoundaryLabel(bucketIndex: number): string {
  return new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" })
    .format(new Date(Date.UTC(2026, 6, 21, 0, bucketIndex * 20)));
}

export function SearchWorkspace({ dataMode, apiBaseUrl = "" }: SearchWorkspaceProps) {
  const backendEnabled = dataMode === "backend";
  const apiClient = useMemo(() => createOpenSplunkApiClient({ baseUrl: apiBaseUrl }), [apiBaseUrl]);
  const [query, setQuery] = useState(DEFAULT_QUERY);
  const [submittedQuery, setSubmittedQuery] = useState(DEFAULT_QUERY);
  const [activeTab, setActiveTab] = useState<ResultTab>("events");
  const [phase, setPhase] = useState<JobPhase>(backendEnabled ? "queued" : "completed");
  const [progress, setProgress] = useState(backendEnabled ? 0 : 100);
  const [elapsed, setElapsed] = useState(backendEnabled ? "0.00 s" : "1.82 s");
  const [scannedRows, setScannedRows] = useState(backendEnabled ? 0 : 284_219);
  const [scannedBytes, setScannedBytes] = useState(backendEnabled ? "0 B" : "186 MB");
  const [timeRange, setTimeRange] = useState<TimeRange>(TIME_PRESETS[3]);
  const [submittedTimeRange, setSubmittedTimeRange] = useState<TimeRange>(TIME_PRESETS[3]);
  const [draftTimeRange, setDraftTimeRange] = useState<TimeRange>(TIME_PRESETS[3]);
  const [timePickerSection, setTimePickerSection] = useState<TimePickerSection>("presets");
  const [relativeAmount, setRelativeAmount] = useState(24);
  const [relativeUnit, setRelativeUnit] = useState<"m" | "h" | "d">("h");
  const [absoluteStart, setAbsoluteStart] = useState("2026-07-20T15:44");
  const [absoluteEnd, setAbsoluteEnd] = useState("2026-07-21T15:44");
  const [modal, setModal] = useState<ModalName | null>(null);
  const [menu, setMenu] = useState<MenuName | null>(null);
  const [toast, setToast] = useState<ToastState | null>(null);
  const [fields, setFields] = useState<DemoField[]>(backendEnabled ? [] : DEMO_FIELDS);
  const [backendEvents, setBackendEvents] = useState<DemoEvent[]>([]);
  const [backendStatistics, setBackendStatistics] = useState<WorkspaceStatistic[]>([]);
  const [backendTimeline, setBackendTimeline] = useState<TimelinePoint[]>([]);
  const [backendEventCount, setBackendEventCount] = useState(0);
  const [statisticsDimension, setStatisticsDimension] = useState("level");
  const [activeField, setActiveField] = useState<string | null>(null);
  const [fieldFilter, setFieldFilter] = useState("");
  const [fieldsCollapsed, setFieldsCollapsed] = useState(false);
  const [expandedEvents, setExpandedEvents] = useState<Set<string>>(
    new Set(backendEnabled ? [] : [DEMO_EVENTS[0].id]),
  );
  const [completionOpen, setCompletionOpen] = useState(false);
  const [completionIndex, setCompletionIndex] = useState(0);
  const [editorCaret, setEditorCaret] = useState(DEFAULT_QUERY.length);
  const [editorFocused, setEditorFocused] = useState(false);
  const [searchMode, setSearchMode] = useState<SearchMode>("Smart");
  const [timelineStart, setTimelineStart] = useState<number | null>(null);
  const [timelineEnd, setTimelineEnd] = useState<number | null>(null);
  const [draggingTimeline, setDraggingTimeline] = useState(false);
  const [savedSearches, setSavedSearches] = useState<DemoSavedSearch[]>(backendEnabled ? [] : DEMO_SAVED_SEARCHES);
  const [history, setHistory] = useState<DemoHistoryEntry[]>(backendEnabled ? [] : DEMO_HISTORY);
  const [savedSearchFilter, setSavedSearchFilter] = useState("");
  const [historyFilter, setHistoryFilter] = useState("");
  const [saveName, setSaveName] = useState("Production log investigation");
  const [saveDescription, setSaveDescription] = useState("");
  const [activeSavedSearchId, setActiveSavedSearchId] = useState<string | null>(null);
  const [exportFormat, setExportFormat] = useState<"csv" | "jsonl">("csv");
  const [exportStage, setExportStage] = useState<ExportStage>("configure");
  const [exportFields, setExportFields] = useState<string[]>(EVENT_EXPORT_FIELDS);
  const [exportSourceTab, setExportSourceTab] = useState<ResultTab>("events");
  const [chartStyle, setChartStyle] = useState<ChartStyle>("column");
  const [chartTitle, setChartTitle] = useState("Event volume by level");
  const [legendPosition, setLegendPosition] = useState<"bottom" | "right" | "none">("bottom");
  const [showDataLabels, setShowDataLabels] = useState(true);
  const [statsDensity, setStatsDensity] = useState<StatsDensity>("compact");
  const [statsHasScrolled, setStatsHasScrolled] = useState(false);
  const [patternSensitivity, setPatternSensitivity] = useState<PatternSensitivity>("Balanced");
  const [eventDisplay, setEventDisplay] = useState<EventDisplay>("List");
  const [eventPageSize, setEventPageSize] = useState<10 | 20 | 50>(20);
  const [eventPage, setEventPage] = useState(1);
  const [eventSortDirection, setEventSortDirection] = useState<"asc" | "desc">("desc");
  const [timelineDisplay, setTimelineDisplay] = useState<TimelineDisplay>("Columns");
  const [wrapEvents, setWrapEvents] = useState(true);
  const [statsSort, setStatsSort] = useState<{ key: keyof WorkspaceStatistic; direction: "asc" | "desc" }>({
    key: "count",
    direction: "desc",
  });
  const [timechartSort, setTimechartSort] = useState<{ key: "time" | "count"; direction: "asc" | "desc" }>({ key: "time", direction: "asc" });
  const [showAllFields, setShowAllFields] = useState(false);
  const timersRef = useRef<number[]>([]);
  const generationRef = useRef(0);
  const searchLaunchRef = useRef(false);
  const backendAbortRef = useRef<AbortController | null>(null);
  const backendJobIdRef = useRef<string | null>(null);
  const backendAutoRunRef = useRef(false);
  const backendSearchRunnerRef = useRef<(queryText: string, range: TimeRange) => void>(() => undefined);
  const editorRef = useRef<HTMLTextAreaElement>(null);
  const highlightRef = useRef<HTMLPreElement>(null);
  const gutterLinesRef = useRef<HTMLDivElement>(null);
  const timePickerRef = useRef<HTMLDivElement>(null);
  const timeRangeButtonRef = useRef<HTMLButtonElement>(null);
  const saveAsButtonRef = useRef<HTMLButtonElement>(null);
  const saveDialogReturnFocusRef = useRef<HTMLElement | null>(null);

  const isRunning = ACTIVE_PHASES.has(phase);
  const hasResultData = backendEnabled ? phase === "completed" : phase !== "failed" && phase !== "canceled";
  const diagnostic = useMemo(() => getQueryDiagnostic(query), [query]);
  const completionContext = useMemo(() => completionContextAt(query, editorCaret), [editorCaret, query]);
  const filteredCompletions = useMemo(() => {
    const prefix = completionContext?.prefix.toLowerCase() ?? "";
    return prefix.length === 0
      ? COMPLETIONS
      : COMPLETIONS.filter((completion) => completion.label.startsWith(prefix));
  }, [completionContext]);
  const resultEvents = useMemo(
    () => backendEnabled ? backendEvents : filteredDemoEvents(submittedQuery),
    [backendEnabled, backendEvents, submittedQuery],
  );
  const timelinePoints = backendEnabled ? backendTimeline : DEMO_TIMELINE;
  const statisticsRows = backendEnabled ? backendStatistics : DEMO_STATISTICS;
  const isTimechartResult = /\btimechart\b/i.test(submittedQuery);
  const baseEventCount = backendEnabled ? backendEventCount : eventCountForQuery(submittedQuery);
  const timelineSelection = useMemo(() => {
    if (timelineStart === null || timelineEnd === null) return null;
    return [Math.min(timelineStart, timelineEnd), Math.max(timelineStart, timelineEnd)] as const;
  }, [timelineEnd, timelineStart]);
  const selectedTimelineCount = useMemo(() => {
    if (timelineSelection === null) return null;
    return timelinePoints.slice(timelineSelection[0], timelineSelection[1] + 1).reduce((total, point) => total + point.count, 0);
  }, [timelinePoints, timelineSelection]);
  const visibleEventCount = phase === "failed" || phase === "canceled"
    ? 0
    : selectedTimelineCount === null
      ? baseEventCount
      : Math.min(baseEventCount, selectedTimelineCount);
  const pageableEventCount = backendEnabled ? resultEvents.length : visibleEventCount;
  const eventPageCount = Math.max(1, Math.ceil(pageableEventCount / eventPageSize));
  const pagedResultEvents = useMemo(() => {
    if (resultEvents.length === 0) return [];
    const ordered = eventSortDirection === "desc" ? resultEvents : resultEvents.toReversed();
    if (backendEnabled) {
      const offset = (eventPage - 1) * eventPageSize;
      return ordered.slice(offset, offset + eventPageSize);
    }
    const offset = (eventPage - 1) % ordered.length;
    const rotated = [...ordered.slice(offset), ...ordered.slice(0, offset)];
    return rotated.slice(0, Math.min(eventPageSize, rotated.length));
  }, [backendEnabled, eventPage, eventPageSize, eventSortDirection, resultEvents]);
  const selectedFields = fields.filter((field) => field.selected);
  const interestingFields = fields.filter((field) => field.interesting && !field.selected);
  const visibleInterestingFields = (showAllFields ? interestingFields : interestingFields.slice(0, 8)).filter((field) =>
    field.name.toLowerCase().includes(fieldFilter.toLowerCase()),
  );
  const activeFieldData = fields.find((field) => field.name === activeField) ?? null;
  const dirty = query !== submittedQuery
    || timeRange.earliest !== submittedTimeRange.earliest
    || timeRange.latest !== submittedTimeRange.latest;
  const editorLineCount = Math.max(2, query.split("\n").length);
  const absoluteTimeInvalid = absoluteStart.trim().length === 0
    || absoluteEnd.trim().length === 0
    || absoluteStart >= absoluteEnd;
  const maxTimelineCount = Math.max(1, ...timelinePoints.map((point) => point.count));
  const maxStatisticsCount = Math.max(1, ...statisticsRows.map((row) => row.count));
  const lineChartPoints = useMemo(() => timelinePoints.map((point, index) => {
    const x = (index / Math.max(1, timelinePoints.length - 1)) * 1000;
    const y = 292 - (point.count / maxTimelineCount) * 254;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" "), [maxTimelineCount, timelinePoints]);
  const sortedStatistics = useMemo(() => {
    const rows = [...statisticsRows];
    rows.sort((left, right) => {
      const leftValue = left[statsSort.key];
      const rightValue = right[statsSort.key];
      const result = typeof leftValue === "number" && typeof rightValue === "number"
        ? leftValue - rightValue
        : String(leftValue).localeCompare(String(rightValue));
      return statsSort.direction === "asc" ? result : -result;
    });
    return rows;
  }, [statisticsRows, statsSort]);
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
    setDraftTimeRange({
      label: "Custom date range",
      earliest: start,
      latest: end,
    });
  }

  function closeTimePicker(restoreFocus = true) {
    setModal(null);
    if (restoreFocus) {
      window.requestAnimationFrame(() => timeRangeButtonRef.current?.focus());
    }
  }

  useEffect(() => {
    return () => {
      clearTimers();
      backendAbortRef.current?.abort();
    };
  }, []);

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
    const url = new URL(window.location.href);
    const sharedQuery = url.searchParams.get("q");
    const initialQuery = sharedQuery === null || sharedQuery.trim().length === 0 ? DEFAULT_QUERY : sharedQuery;
    const earliest = url.searchParams.get("earliest");
    const latest = url.searchParams.get("latest");
    const sharedRange = TIME_PRESETS.find((preset) => preset.earliest === earliest && preset.latest === latest);
    let initialRange = TIME_PRESETS[3];
    if (sharedQuery !== null && sharedQuery.trim().length > 0) {
      setQuery(sharedQuery);
      setSubmittedQuery(sharedQuery);
      setEditorCaret(sharedQuery.length);
      setActiveTab(resultTabForQuery(sharedQuery));
    }
    if (earliest !== null && latest !== null) {
      const restoredRange = sharedRange ?? { label: `${earliest} to ${latest}`, earliest, latest };
      initialRange = restoredRange;
      setTimeRange(restoredRange);
      setSubmittedTimeRange(restoredRange);
      setDraftTimeRange(restoredRange);
    }
    if (backendEnabled && !backendAutoRunRef.current) {
      backendAutoRunRef.current = true;
      window.setTimeout(() => backendSearchRunnerRef.current(initialQuery, initialRange), 0);
    }
  }, [backendEnabled]);

  useEffect(() => {
    setCompletionIndex((current) => Math.max(0, Math.min(current, filteredCompletions.length - 1)));
  }, [filteredCompletions.length]);

  useEffect(() => {
    if (activeTab === "statistics") setStatsHasScrolled(false);
  }, [activeTab]);

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
    if (modal !== "time") return;
    function handleOutsidePointer(event: globalThis.PointerEvent) {
      if (event.target instanceof Node && !timePickerRef.current?.contains(event.target)) {
        setModal(null);
      }
    }
    document.addEventListener("pointerdown", handleOutsidePointer);
    return () => document.removeEventListener("pointerdown", handleOutsidePointer);
  }, [modal]);

  function applyBackendJob(job: SearchJob) {
    const nextPhase = backendJobPhase(job.state);
    const phaseProgress: Record<JobPhase, number> = {
      queued: 4,
      parsing: 14,
      planning: 27,
      running: 58,
      finalizing: 94,
      completed: 100,
      failed: 100,
      canceled: 100,
    };
    setPhase(nextPhase);
    setProgress(Math.max(0, Math.min(100, job.progress?.percentComplete ?? phaseProgress[nextPhase])));
    setElapsed(formatDuration(job.progress?.elapsed));
    setScannedRows(Math.min(Number.MAX_SAFE_INTEGER, Number(job.progress?.scannedRows ?? 0n)));
    setScannedBytes(formatBytes(job.progress?.scannedBytes ?? 0n));
    setBackendEventCount(jobEventCount(job));
  }

  async function fetchBackendResults(job: SearchJob, signal: AbortSignal, generation: number, queryText: string) {
    const rows: ResultRow[] = [];
    let schema = job.resultSchema;
    let totalRows: bigint | undefined;
    const maximumRows = 50_000;

    async function fetchPage(pageToken?: string): Promise<void> {
      const response = await apiClient.search.results({
        searchJobId: job.searchJobId,
        page: { pageSize: 1_000, pageToken, includeTotalSize: pageToken === undefined },
        columns: [],
        allowPartialResults: false,
      }, { signal });
      if (generationRef.current !== generation) return;
      const resultPage = response.resultPage;
      if (resultPage === undefined) throw new Error("The search completed without a result page.");
      schema ??= resultPage.schema;
      rows.push(...resultPage.rows.slice(0, maximumRows - rows.length));
      totalRows ??= resultPage.page?.totalSize;
      const nextPageToken = rows.length < maximumRows ? resultPage.page?.nextPageToken : undefined;
      if (nextPageToken) await fetchPage(nextPageToken);
    }

    await fetchPage();

    if (schema === undefined) throw new Error("The search completed without a result schema.");
    const adapted = adaptSearchResults(schema, rows, /\btimechart\b/i.test(queryText));
    setBackendEvents(adapted.events);
    setFields(adapted.fields);
    setBackendStatistics(adapted.statistics);
    setStatisticsDimension(adapted.statisticDimension);
    setBackendTimeline(adapted.timeline);
    setExpandedEvents(new Set(adapted.events[0] ? [adapted.events[0].id] : []));
    const exactRows = totalRows === undefined ? rows.length : Math.min(Number.MAX_SAFE_INTEGER, Number(totalRows));
    setBackendEventCount((current) => Math.max(current, exactRows));
    if (totalRows !== undefined && totalRows > BigInt(maximumRows)) {
      showToast(`Loaded the first ${NUMBER_FORMAT.format(maximumRows)} rows from this search.`, "warning");
    }
  }

  async function runBackendSearch(nextQuery: string, rangeOverride: TimeRange = timeRange) {
    const generation = ++generationRef.current;
    const launchTimeRange = rangeOverride;
    backendAbortRef.current?.abort();
    const controller = new AbortController();
    backendAbortRef.current = controller;
    backendJobIdRef.current = null;
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
    setScannedBytes("0 B");
    setBackendEventCount(0);
    setBackendEvents([]);
    setBackendStatistics([]);
    setBackendTimeline([]);
    setFields([]);
    setTimelineStart(null);
    setTimelineEnd(null);
    setEventPage(1);

    try {
      const absoluteRange = resolveAbsoluteTimeRange(launchTimeRange.earliest, launchTimeRange.latest);
      const response = await apiClient.search.create({
        definition: {
          spl: nextQuery,
          timeRange: absoluteRange,
          appId: undefined,
          indexScope: indexesFromSPL(nextQuery),
          preferredResultTab: 0,
          selectedFields: [],
          visualization: undefined,
        },
        source: undefined,
        options: undefined,
        clientRequestId: globalThis.crypto?.randomUUID?.(),
      }, { signal: controller.signal });
      let job = response.searchJob;
      if (job === undefined || job.searchJobId.length === 0) throw new Error("The server did not return a search job ID.");
      backendJobIdRef.current = job.searchJobId;
      applyBackendJob(job);

      async function pollUntilTerminal(currentJob: SearchJob): Promise<SearchJob> {
        if (!ACTIVE_PHASES.has(backendJobPhase(currentJob.state))) return currentJob;
        await new Promise((resolve) => window.setTimeout(resolve, 180));
        if (controller.signal.aborted || generationRef.current !== generation) return currentJob;
        const poll = await apiClient.search.get({
          searchJobId: currentJob.searchJobId,
          includePlan: false,
          includeGeneratedSql: false,
        }, { signal: controller.signal });
        if (poll.searchJob === undefined) throw new Error("The server returned an empty search job response.");
        applyBackendJob(poll.searchJob);
        return pollUntilTerminal(poll.searchJob);
      }
      job = await pollUntilTerminal(job);

      if (generationRef.current !== generation) return;
      const terminalPhase = backendJobPhase(job.state);
      if (terminalPhase === "completed") {
        setPhase("finalizing");
        setProgress(96);
        await fetchBackendResults(job, controller.signal, generation, nextQuery);
        if (generationRef.current !== generation) return;
        setPhase("completed");
        setProgress(100);
        setActiveTab(resultTabForQuery(nextQuery));
        if (/\btimechart\b/i.test(nextQuery)) {
          setChartStyle("line");
          setChartTitle("Event volume over time");
        } else {
          setChartStyle("column");
          setChartTitle("Event volume by level");
        }
      } else if (terminalPhase === "failed") {
        showToast(job.failure?.message || "The backend search failed.", "warning");
      }
    } catch (error) {
      if (controller.signal.aborted || generationRef.current !== generation) return;
      setPhase("failed");
      setProgress(100);
      showToast(error instanceof Error ? error.message : "Unable to run the backend search.", "warning");
    } finally {
      if (backendAbortRef.current === controller) backendAbortRef.current = null;
    }
  }

  backendSearchRunnerRef.current = (queryText, range) => {
    void runBackendSearch(queryText, range);
  };

  function runSearch(queryOverride?: string) {
    if (searchLaunchRef.current) return;
    const nextQuery = queryOverride ?? query;
    if (nextQuery.trim().length === 0) {
      setQuery(nextQuery);
      setCompletionOpen(false);
      showToast("Enter an SPL search before running.", "warning");
      focusEditor(0);
      return;
    }
    searchLaunchRef.current = true;
    window.setTimeout(() => {
      searchLaunchRef.current = false;
    }, 0);
    if (backendEnabled) {
      void runBackendSearch(nextQuery);
      return;
    }
    generationRef.current += 1;
    const generation = generationRef.current;
    const launchTimeRange = timeRange;
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
    setScannedBytes("0 B");
    setTimelineStart(null);
    setTimelineEnd(null);
    setEventPage(1);

    const nextDiagnostic = getQueryDiagnostic(nextQuery);
    schedule(() => {
      if (generationRef.current !== generation) return;
      setPhase("parsing");
      setProgress(14);
      setElapsed("0.08 s");
    }, 120);

    if (nextDiagnostic !== null) {
      schedule(() => {
        if (generationRef.current !== generation) return;
        setPhase("failed");
        setProgress(100);
        setElapsed("0.11 s");
        setHistory((current) => [
          {
            id: `hist-${Date.now()}`,
            query: nextQuery,
            timeRange: launchTimeRange.label,
            state: "Failed",
            events: 0,
            duration: "0.11 s",
            ranAt: "Just now",
          },
          ...current,
        ]);
      }, 390);
      return;
    }

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
      if (/\btimechart\b/i.test(nextQuery)) {
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
      generationRef.current += 1;
      backendAbortRef.current?.abort();
      setPhase("canceled");
      setProgress(100);
      showToast("Search canceled.", "warning");
      if (searchJobId !== null) {
        void apiClient.search.cancel({ searchJobId, reason: undefined }).catch((error: unknown) => {
          showToast(error instanceof Error ? error.message : "The server could not cancel this search.", "warning");
        });
      }
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
    setEditorCaret(nextCaret);
    setCompletionOpen(false);
    focusEditor(nextCaret);
  }

  function handleEditorChange(event: ChangeEvent<HTMLTextAreaElement>) {
    const nextQuery = event.target.value;
    const caret = event.target.selectionStart;
    const context = completionContextAt(nextQuery, caret);
    setQuery(nextQuery);
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
    setCompletionOpen(false);
    focusEditor(nextQuery.length);
  }

  function applyPivot(field: string, value: DemoScalar, mode: PivotMode, runImmediately = false) {
    const nextQuery = applyFieldPivot(query, field, value, mode);
    setQuery(nextQuery);
    setEditorCaret(nextQuery.length);
    setActiveField(null);
    if (runImmediately || mode === "new") runSearch(nextQuery);
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

  function startTimelineDrag(event: PointerEvent<HTMLDivElement>) {
    const index = timelineIndexFromPointer(event);
    if (index === null) return;
    event.currentTarget.setPointerCapture(event.pointerId);
    setDraggingTimeline(true);
    setTimelineStart(index);
    setTimelineEnd(index);
  }

  function moveTimelineDrag(event: PointerEvent<HTMLDivElement>) {
    if (!draggingTimeline) return;
    const index = timelineIndexFromPointer(event);
    if (index !== null) setTimelineEnd(index);
  }

  function endTimelineDrag(event: PointerEvent<HTMLDivElement>) {
    if (draggingTimeline && event.currentTarget.hasPointerCapture(event.pointerId)) {
      event.currentTarget.releasePointerCapture(event.pointerId);
    }
    setDraggingTimeline(false);
  }

  function zoomTimeline() {
    if (timelineSelection === null) return;
    const first = timelinePoints[timelineSelection[0]];
    const last = timelinePoints[timelineSelection[1]];
    if (first === undefined || last === undefined) return;
    const intervalEndLabel = timelinePoints[timelineSelection[1] + 1]?.label ?? last.latest ?? timelineBoundaryLabel(timelineSelection[1] + 1);
    setTimeRange({
      label: `${first.label} – ${intervalEndLabel}`,
      earliest: first.earliest ?? first.label,
      latest: last.latest ?? timelinePoints[timelineSelection[1] + 1]?.earliest ?? intervalEndLabel,
    });
    setTimelineStart(null);
    setTimelineEnd(null);
    showToast("Time range narrowed to the selected timeline interval.", "success");
  }

  function openSaveDialog(returnFocus?: HTMLElement | null) {
    saveDialogReturnFocusRef.current = returnFocus
      ?? (document.activeElement instanceof HTMLElement ? document.activeElement : null);
    setSaveName(activeSavedSearchId === null ? "Production log investigation" : savedSearches.find((item) => item.id === activeSavedSearchId)?.name ?? "Saved search");
    setSaveDescription(savedSearches.find((item) => item.id === activeSavedSearchId)?.description ?? "");
    setModal("save");
    setMenu(null);
  }

  function saveSearch() {
    const trimmedName = saveName.trim();
    if (trimmedName.length === 0) return;
    const id = activeSavedSearchId ?? `saved-${Date.now()}`;
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
    setActiveSavedSearchId(id);
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
    setSavedSearches((current) =>
      current.map((item) => (item.id === activeSavedSearchId ? { ...item, query, earliest: timeRange.earliest, latest: timeRange.latest, updatedAt: "Just now" } : item)),
    );
    showToast(`Saved changes to “${existing.name}”.`, "success");
  }

  function openSavedSearch(saved: DemoSavedSearch) {
    const savedRange = {
      label: saved.earliest === "-24h" ? "Last 24 hours" : `${saved.earliest} to ${saved.latest}`,
      earliest: saved.earliest,
      latest: saved.latest,
    };
    setQuery(saved.query);
    setEditorCaret(saved.query.length);
    setTimeRange(savedRange);
    setDraftTimeRange(savedRange);
    setActiveSavedSearchId(saved.id);
    setModal(null);
    showToast(`Opened “${saved.name}”. Run it when ready.`, "info");
    focusEditor(saved.query.length);
  }

  function openHistoryEntry(entry: DemoHistoryEntry, rerun: boolean) {
    setQuery(entry.query);
    setEditorCaret(entry.query.length);
    setModal(null);
    if (rerun) runSearch(entry.query);
    else showToast("Search restored without running.", "info");
    focusEditor(entry.query.length);
  }

  function deleteSavedSearch(id: string) {
    setSavedSearches((current) => current.filter((item) => item.id !== id));
    if (activeSavedSearchId === id) setActiveSavedSearchId(null);
    showToast("Saved search deleted.", "warning");
  }

  function deleteHistoryEntry(id: string) {
    setHistory((current) => current.filter((item) => item.id !== id));
    showToast("History entry deleted.", "warning");
  }

  function exportFieldsForTab(tab: ResultTab): string[] {
    if (isTimechartResult && (tab === "statistics" || tab === "visualization")) return ["_time", "count"];
    return EXPORT_FIELDS_BY_TAB[tab];
  }

  function toggleExportField(fieldName: string) {
    setExportFields((current) =>
      current.includes(fieldName) ? current.filter((field) => field !== fieldName) : [...current, fieldName],
    );
  }

  function openExportDialog(sourceTab: ResultTab = activeTab) {
    setExportSourceTab(sourceTab);
    setExportFields(exportFieldsForTab(sourceTab));
    setExportStage("configure");
    setMenu(null);
    setModal("export");
  }

  function prepareExport() {
    setExportStage("preparing");
    schedule(() => setExportStage("ready"), 920);
  }

  function downloadExport() {
    const filename = `open-splunk-${exportSourceTab}.${exportFormat}`;
    const rows = exportSourceTab === "events"
      ? resultEvents.map((event) => Object.fromEntries(exportFields.map((field) => [field, event.fields[field] ?? (field === "_raw" ? event.raw : null)])))
      : exportSourceTab === "patterns"
        ? patternRows.map((pattern) => ({ pattern: pattern.signature, count: pattern.count, percent: pattern.percent }))
        : isTimechartResult
          ? sortedTimechartRows.map((row) => ({ _time: row.label, count: row.count }))
          : sortedStatistics.map((row) => ({ level: row.level, count: row.count, percent: row.percent, avgDuration: row.avgDuration }));
    const selectedRows = rows.map((row) => Object.fromEntries(exportFields.map((field) => [field, row[field as keyof typeof row] ?? null])));
    const content = exportFormat === "jsonl"
      ? selectedRows.map((row) => JSON.stringify(row)).join("\n")
      : [
          exportFields.map((field) => EXPORT_FIELD_LABELS[field] ?? field).join(","),
          ...selectedRows.map((row) =>
            exportFields
              .map((field) => `"${String(row[field] ?? "").replaceAll('"', '""')}"`)
              .join(","),
          ),
        ].join("\n");
    const blob = new Blob([content], { type: exportFormat === "csv" ? "text/csv" : "application/x-ndjson" });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = filename;
    anchor.click();
    URL.revokeObjectURL(url);
    setModal(null);
    setExportStage("configure");
    showToast(`Downloaded ${filename}.`, "success");
  }

  function updateStatsSort(key: keyof WorkspaceStatistic) {
    setStatsSort((current) => ({ key, direction: current.key === key && current.direction === "desc" ? "asc" : "desc" }));
  }

  const modalContents = (() => {
    if (modal === "save") {
      return (
        <Modal
          title={activeSavedSearchId === null ? "Save search" : "Save search as"}
          subtitle="Preserve the SPL and relative time range for reuse."
          onClose={() => setModal(null)}
          returnFocus={saveDialogReturnFocusRef.current}
          footer={
            <>
              <button className="button secondary" type="button" onClick={() => setModal(null)}>Cancel</button>
              <button className="button primary" type="button" disabled={saveName.trim().length === 0} onClick={saveSearch}>Save</button>
            </>
          }
        >
          <div className="form-stack" data-testid="save-search-dialog">
            <label>
              <span>Name</span>
              <input value={saveName} onChange={(event) => setSaveName(event.target.value)} />
            </label>
            <label>
              <span>Description <small>optional</small></span>
              <textarea value={saveDescription} onChange={(event) => setSaveDescription(event.target.value)} rows={3} />
            </label>
            <div className="form-summary">
              <span>App</span><strong>Search &amp; Reporting</strong>
              <span>Time range</span><strong>{timeRange.label}</strong>
              <span>Result view</span><strong>{activeTab[0].toUpperCase() + activeTab.slice(1)}</strong>
            </div>
          </div>
        </Modal>
      );
    }
    if (modal === "open") {
      const filtered = savedSearches.filter((item) =>
        `${item.name} ${item.description} ${item.query}`.toLowerCase().includes(savedSearchFilter.toLowerCase()),
      );
      return (
        <Modal title="Open a saved search" subtitle={`${savedSearches.length} searches in this app`} wide onClose={() => setModal(null)}>
          <div className="library-toolbar">
            <label className="filter-input">
              <span aria-hidden="true">⌕</span>
              <input
                aria-label="Filter saved searches"
                placeholder="Filter saved searches"
                value={savedSearchFilter}
                onChange={(event) => setSavedSearchFilter(event.target.value)}
              />
            </label>
            <select aria-label="Sort saved searches" defaultValue="updated">
              <option value="updated">Recently updated</option>
              <option value="name">Name</option>
            </select>
          </div>
          <div className="saved-list" data-testid="saved-search-list">
            {filtered.map((saved) => (
              <article className="saved-row" key={saved.id}>
                <button className="saved-row-main" type="button" onClick={() => openSavedSearch(saved)}>
                  <span className="saved-icon" aria-hidden="true">⌕</span>
                  <span>
                    <strong>{saved.name}</strong>
                    <small>{saved.description}</small>
                    <code>{saved.query.replaceAll("\n", " ")}</code>
                  </span>
                </button>
                <div className="saved-row-meta">
                  <span>{saved.updatedAt}</span>
                  <button className="icon-button" aria-label={`Delete ${saved.name}`} type="button" onClick={() => deleteSavedSearch(saved.id)}>×</button>
                </div>
              </article>
            ))}
            {filtered.length === 0 ? <div className="empty-state"><strong>No saved searches match</strong><span>Try another name or SPL term.</span></div> : null}
          </div>
        </Modal>
      );
    }
    if (modal === "history") {
      const filtered = history.filter((item) => item.query.toLowerCase().includes(historyFilter.toLowerCase()));
      return (
        <Modal title="Search history" subtitle="Every attempted search, including canceled and failed jobs." wide onClose={() => setModal(null)}>
          <div className="library-toolbar">
            <label className="filter-input">
              <span aria-hidden="true">⌕</span>
              <input
                aria-label="Filter search history"
                placeholder="Filter by SPL"
                value={historyFilter}
                onChange={(event) => setHistoryFilter(event.target.value)}
              />
            </label>
            <button className="button secondary compact" type="button" onClick={() => setHistory([])}>Clear history</button>
          </div>
          <div className="history-table" data-testid="history-list">
            <div className="history-head"><span>Search</span><span>Status</span><span>Events</span><span>Duration</span><span>Ran</span><span>Actions</span></div>
            {filtered.map((entry) => (
              <article className="history-row" key={entry.id}>
                <code title={entry.query}>{entry.query}</code>
                <span className={`history-state history-${entry.state.toLowerCase()}`}>{entry.state}</span>
                <span>{NUMBER_FORMAT.format(entry.events)}</span>
                <span>{entry.duration}</span>
                <span>{entry.ranAt}</span>
                <div className="row-actions">
                  <button type="button" onClick={() => openHistoryEntry(entry, false)}>Open</button>
                  <button type="button" onClick={() => openHistoryEntry(entry, true)}>Run again</button>
                  <button aria-label="Delete history entry" type="button" onClick={() => deleteHistoryEntry(entry.id)}>×</button>
                </div>
              </article>
            ))}
          </div>
        </Modal>
      );
    }
    if (modal === "export") {
      return (
        <Modal
          title={`Export ${exportSourceTab}`}
          subtitle="Create a bounded artifact from the displayed search result."
          onClose={() => {
            setModal(null);
            setExportStage("configure");
          }}
          footer={
            exportStage === "ready" ? (
              <>
                <span className="export-ready-note">✓ Artifact ready · expires in 10 minutes</span>
                <button className="button primary" type="button" onClick={downloadExport}>Download .{exportFormat}</button>
              </>
            ) : (
              <>
                <button className="button secondary" type="button" onClick={() => setModal(null)}>Cancel</button>
                <button className="button primary" type="button" disabled={exportStage === "preparing" || exportFields.length === 0} onClick={prepareExport}>
                  {exportStage === "preparing" ? "Preparing…" : "Create export"}
                </button>
              </>
            )
          }
        >
          <div className="form-stack" data-testid="export-dialog">
            <fieldset className="segmented-fieldset" disabled={exportStage !== "configure"}>
              <legend>Format</legend>
              <label className={exportFormat === "csv" ? "selected" : ""} htmlFor="export-format-csv">
                <input id="export-format-csv" aria-label="Export as CSV" type="radio" name="format" checked={exportFormat === "csv"} onChange={() => setExportFormat("csv")} />
                <span><strong>CSV</strong><small>Spreadsheet-compatible table</small></span>
              </label>
              <label className={exportFormat === "jsonl" ? "selected" : ""} htmlFor="export-format-jsonl">
                <input id="export-format-jsonl" aria-label="Export as JSON Lines" type="radio" name="format" checked={exportFormat === "jsonl"} onChange={() => setExportFormat("jsonl")} />
                <span><strong>JSON Lines</strong><small>Typed record per line</small></span>
              </label>
            </fieldset>
            <fieldset className="export-fields" disabled={exportStage !== "configure"}>
              <legend>Columns <small>{exportFields.length} selected</small></legend>
              {(exportSourceTab === "events"
                ? ["_time", "level", "logger", "message", "trace_id", "host", "source", "status", "duration_ms", "_raw"]
                : exportFieldsForTab(exportSourceTab)
              ).map((field) => (
                <label key={field} htmlFor={`export-field-${field}`}>
                  <input id={`export-field-${field}`} aria-label={`Include ${field} in export`} type="checkbox" checked={exportFields.includes(field)} onChange={() => toggleExportField(field)} />
                  <code>{EXPORT_FIELD_LABELS[field] ?? field}</code>
                </label>
              ))}
            </fieldset>
            <div className="export-limit"><span>Maximum rows</span><strong>50,000</strong><small>{exportSourceTab === "events" ? `${resultEvents.length} displayed events` : `${exportSourceTab === "patterns" ? patternRows.length : isTimechartResult ? timelinePoints.length : statisticsRows.length} displayed rows`}</small></div>
            {exportStage === "preparing" ? <div className="export-progress"><span style={{ width: "72%" }} /><strong>Materializing results…</strong></div> : null}
          </div>
        </Modal>
      );
    }
    if (modal === "settings") {
      return (
        <Modal title="Search preferences" subtitle="Settings apply to this browser." onClose={() => setModal(null)} footer={<button className="button primary" type="button" onClick={() => setModal(null)}>Done</button>}>
          <div className="settings-list" data-testid="settings-dialog">
            <label htmlFor="setting-wrap-events"><span><strong>Wrap event text</strong><small>Keep long raw events within the results pane.</small></span><input id="setting-wrap-events" aria-label="Wrap event text" type="checkbox" defaultChecked /></label>
            <label htmlFor="setting-discover-fields"><span><strong>Discover interesting fields</strong><small>Profile fields that appear in at least 20% of events.</small></span><input id="setting-discover-fields" aria-label="Discover interesting fields" type="checkbox" defaultChecked /></label>
            <label htmlFor="setting-live-previews"><span><strong>Live result previews</strong><small>Render bounded rows while a search runs.</small></span><input id="setting-live-previews" aria-label="Live result previews" type="checkbox" defaultChecked /></label>
            <label htmlFor="setting-compact-density"><span><strong>Compact density</strong><small>Show more events in the available viewport.</small></span><input id="setting-compact-density" aria-label="Compact density" type="checkbox" defaultChecked /></label>
          </div>
        </Modal>
      );
    }
    if (modal === "jobs") {
      return (
        <Modal title="Activity & jobs" subtitle="Search jobs retained for this session." wide onClose={() => setModal(null)}>
          <div className="jobs-list" data-testid="jobs-dialog">
            <article className="job-card active-job-card">
              <div className={`job-card-state ${stateClass(phase)}`}><span />{phaseLabel(phase)}</div>
              <code>{submittedQuery.replaceAll("\n", " ")}</code>
              <div className="job-card-stats"><span>{NUMBER_FORMAT.format(visibleEventCount)} events</span><span>{scannedBytes} scanned</span><span>{elapsed}</span></div>
              {isRunning ? <button className="button danger compact" type="button" onClick={cancelSearch}>Cancel</button> : null}
            </article>
            {history.slice(0, 4).map((entry) => (
              <article className="job-card" key={entry.id}>
                <div className={`job-card-state history-${entry.state.toLowerCase()}`}><span />{entry.state}</div>
                <code>{entry.query}</code>
                <div className="job-card-stats"><span>{NUMBER_FORMAT.format(entry.events)} events</span><span>{entry.duration}</span><span>{entry.ranAt}</span></div>
              </article>
            ))}
          </div>
        </Modal>
      );
    }
    if (modal === "inspect") {
      return (
        <Modal title="Search job inspector" subtitle="Dispatch and execution details for the displayed result." wide onClose={() => setModal(null)} footer={<button className="button primary" type="button" onClick={() => setModal(null)}>Done</button>}>
          <div className="job-inspector" data-testid="job-inspector">
            <section><span>Status</span><strong className={`inspector-state ${stateClass(phase)}`}><i />{phaseLabel(phase)}</strong></section>
            <section><span>Search ID</span><code>{backendEnabled ? backendJobIdRef.current ?? "Pending dispatch" : `scheduler_admin_search_${generationRef.current || 1}`}</code></section>
            <section><span>Search mode</span><strong>{searchMode}</strong></section>
            <section><span>Time range</span><strong>{submittedTimeRange.label}</strong></section>
            <section><span>Scanned</span><strong>{NUMBER_FORMAT.format(scannedRows)} rows · {scannedBytes}</strong></section>
            <section><span>Elapsed</span><strong>{elapsed}</strong></section>
            <div className="inspector-query"><span>Dispatched SPL</span><code>{submittedQuery}</code></div>
            <p>No skipped events, sequence gaps, or execution warnings were reported for this job.</p>
          </div>
        </Modal>
      );
    }
    return null;
  })();

  return (
    <main className="splunk-shell" data-testid="search-workspace" id="search">
      <header className="product-bar">
        <div className="product-left">
          <a className="wordmark" href="#search" aria-label="Open Splunk home"><span>open</span><b>&gt;</b><span>splunk</span></a>
          <div className="header-menu-wrap">
            <button className="product-menu-button" type="button" onClick={() => setMenu(menu === "app" ? null : "app")}>
              App: <strong>Search &amp; Reporting</strong> <span aria-hidden="true">▾</span>
            </button>
            {menu === "app" ? (
              <div className="floating-menu app-menu" role="menu">
                <span className="menu-label">Your apps</span>
                <button role="menuitem" type="button" className="selected"><span className="app-glyph">⌕</span><span><strong>Search &amp; Reporting</strong><small>Search all authorized indexes</small></span><b>✓</b></button>
                <button role="menuitem" type="button"><span className="app-glyph">G</span><span><strong>GradeThis Operations</strong><small>Default index: gradethis</small></span></button>
                <div className="menu-separator" />
                <button role="menuitem" type="button" onClick={() => showToast("App management is planned for the multi-app phase.")}><span className="app-glyph">＋</span><span><strong>Manage apps</strong></span></button>
              </div>
            ) : null}
          </div>
        </div>
        <nav className="product-utilities" aria-label="Product utilities">
          <button type="button" className="health-indicator" title="Server status: healthy"><span /> Healthy</button>
          <button type="button" onClick={() => showToast("No new messages.")}>Messages <span aria-hidden="true">▾</span></button>
          <button type="button" onClick={() => setModal("settings")}>Settings <span aria-hidden="true">▾</span></button>
          <div className="header-menu-wrap">
            <button type="button" onClick={() => setMenu(menu === "activity" ? null : "activity")}>Activity <span className="activity-count">1</span> <span aria-hidden="true">▾</span></button>
            {menu === "activity" ? (
              <div className="floating-menu utility-menu" role="menu">
                <span className="menu-label">Activity</span>
                <button aria-label={`Open active search job: ${phaseLabel(phase)}`} role="menuitem" type="button" onClick={() => { setModal("jobs"); setMenu(null); }}><span className={`mini-status ${stateClass(phase)}`} /> <span><strong>{phaseLabel(phase)}</strong><small>{NUMBER_FORMAT.format(visibleEventCount)} results · {elapsed}</small></span></button>
                <button role="menuitem" type="button" onClick={() => { setModal("history"); setMenu(null); }}>View all search history</button>
              </div>
            ) : null}
          </div>
          <div className="header-menu-wrap">
            <button type="button" onClick={() => setMenu(menu === "help" ? null : "help")}>Help <span aria-hidden="true">▾</span></button>
            {menu === "help" ? (
              <div className="floating-menu utility-menu help-menu" role="menu">
                <span className="menu-label">Search help</span>
                <button role="menuitem" type="button" onClick={() => showToast("SPL reference will open in a documentation pane.")}>SPL command reference</button>
                <button role="menuitem" type="button" onClick={() => showToast("Tip: press Ctrl+Space inside the editor for completions.")}>Keyboard shortcuts</button>
                <button role="menuitem" type="button" onClick={() => showToast("Open Splunk frontend preview · v0.1.0")}>About Open Splunk</button>
              </div>
            ) : null}
          </div>
          <label className="global-search">
            <span className="sr-only">Find</span>
            <input placeholder="Find" />
            <span aria-hidden="true">⌕</span>
          </label>
          <div className="header-menu-wrap">
            <button className="user-button" type="button" aria-label="User menu" onClick={() => setMenu(menu === "user" ? null : "user")}><span>A</span> Administrator <b>▾</b></button>
            {menu === "user" ? (
              <div className="floating-menu utility-menu user-menu" role="menu">
                <div className="user-summary"><span>A</span><strong>Administrator</strong><small>admin@localhost</small></div>
                <button role="menuitem" type="button" onClick={() => showToast("Profile settings are not required for single-user mode.")}>Account settings</button>
                <button role="menuitem" type="button" onClick={() => showToast("Open Splunk is running in trusted-network mode.")}>Session details</button>
              </div>
            ) : null}
          </div>
        </nav>
      </header>

      <nav className="app-bar" aria-label="Search and Reporting navigation">
        <div className="app-tabs">
          <button className="active" type="button">Search</button>
          <button type="button" onClick={() => showToast("Analytics opens from a transforming search.")}>Analytics</button>
          <button type="button" onClick={() => showToast("Datasets are planned after the search workspace.")}>Datasets</button>
          <button type="button" onClick={() => setModal("open")}>Reports</button>
          <button type="button" onClick={() => showToast("Alerts are planned for the hardening phase.")}>Alerts</button>
          <button type="button" onClick={() => showToast("Dashboards follow saved searches and reports.")}>Dashboards</button>
        </div>
        <div className="app-identity"><span aria-hidden="true">⌕</span><strong>Search &amp; Reporting</strong></div>
      </nav>

      {menu !== null ? <button type="button" className="menu-dismiss" aria-label="Close menu" onClick={() => setMenu(null)} /> : null}

      <section className="search-page">
        <header className="search-title-row">
          <div className="search-title">
            <h1>{activeSavedSearchId === null ? "New Search" : savedSearches.find((item) => item.id === activeSavedSearchId)?.name}</h1>
            {dirty ? <span className="unsaved-dot" title="The draft differs from the displayed search job">Run to apply changes</span> : null}
            <span
              className={`demo-badge${backendEnabled ? " backend-data-badge" : ""}`}
              title={backendEnabled ? "Searches run against the Open Splunk backend" : "Searches use deterministic frontend fixtures"}
            ><i /> {backendEnabled ? "Backend data" : "Demo data"}</span>
          </div>
          <div className="search-actions" aria-label="Search actions">
            <button type="button" onClick={() => setModal("open")}><span aria-hidden="true">⌕</span> Open</button>
            <button type="button" onClick={quickSave}><span aria-hidden="true">✓</span> Save</button>
            <div className="header-menu-wrap">
              <button ref={saveAsButtonRef} type="button" onClick={() => setMenu(menu === "save-as" ? null : "save-as")}>Save As <span aria-hidden="true">▾</span></button>
              {menu === "save-as" ? (
                <div className="floating-menu action-menu" role="menu">
                  <button role="menuitem" type="button" onClick={() => openSaveDialog(saveAsButtonRef.current)}><span>⌕</span><span><strong>Saved search</strong><small>Preserve this SPL and time range</small></span></button>
                  <button role="menuitem" type="button" onClick={() => showToast("Reports extend saved searches in a later phase.")}><span>▤</span><span><strong>Report</strong><small>Save table and visualization settings</small></span></button>
                  <button role="menuitem" type="button" onClick={() => showToast("Alerts are planned after scheduled searches.")}><span>⚑</span><span><strong>Alert</strong><small>Schedule and notify</small></span></button>
                </div>
              ) : null}
            </div>
            <button type="button" onClick={() => setModal("history")}><span aria-hidden="true">↶</span> History</button>
            <button type="button" onClick={() => openExportDialog()}><span aria-hidden="true">⇩</span> Export</button>
            <button className="close-search" type="button" onClick={() => { setQuery(""); setSubmittedQuery(""); setActiveSavedSearchId(null); }}>Close</button>
          </div>
        </header>

        <section className="search-composer" aria-label="SPL search">
          <div
            className={`spl-editor${editorFocused ? " focused" : ""}${diagnostic === null ? "" : " has-error"}`}
            role="combobox"
            aria-label="SPL command editor"
            aria-expanded={completionOpen}
            aria-haspopup="listbox"
            aria-controls={completionOpen ? "spl-completion-list" : undefined}
          >
            <div className="editor-gutter" aria-hidden="true">
              <div className="editor-gutter-lines" ref={gutterLinesRef}>
                {Array.from({ length: editorLineCount }, (_, index) => <span key={index + 1}>{index + 1}</span>)}
              </div>
            </div>
            <pre className="editor-highlight" ref={highlightRef} aria-hidden="true">{syntaxTokens(query)}{query.endsWith("\n") ? "\n " : null}</pre>
            <textarea
              ref={editorRef}
              data-testid="search-input"
              aria-label="Search with SPL"
              aria-describedby={`${diagnostic === null ? "editor-help" : "editor-diagnostic"} spl-completion-status`}
              aria-autocomplete="list"
              aria-controls={completionOpen ? "spl-completion-list" : undefined}
              aria-activedescendant={completionOpen && filteredCompletions.length > 0 ? `spl-completion-${completionIndex}` : undefined}
              value={query}
              rows={2}
              spellCheck={false}
              autoCapitalize="off"
              autoComplete="off"
              onChange={handleEditorChange}
              onFocus={() => {
                setEditorFocused(true);
                if (modal === "time") setModal(null);
              }}
              onBlur={() => window.setTimeout(() => {
                setEditorFocused(false);
                setCompletionOpen(false);
              }, 120)}
              onKeyDown={handleEditorKeyDown}
              onScroll={handleEditorScroll}
              onSelect={(event) => setEditorCaret(event.currentTarget.selectionStart)}
            />
            <div className="editor-meta" id="editor-help">
              <span>SPL</span><span>Ctrl+Space for commands</span><span>⌘↵ to run</span>
            </div>
            <span className="sr-only" id="spl-completion-status" aria-live="polite">
              {completionOpen
                ? filteredCompletions.length === 0
                  ? "No matching SPL commands."
                  : `${filteredCompletions.length} suggestions available. Use Up and Down arrows, then Enter or Tab to insert.`
                : "Suggestions closed."}
            </span>
            {completionOpen ? (
              <div className="completion-menu" id="spl-completion-list" data-testid="completion-menu" role="listbox" aria-label="SPL suggestions">
                <div className="completion-title"><span>Commands</span><small>Enter a pipeline stage</small></div>
                {filteredCompletions.map((completion, index) => (
                  <button
                    id={`spl-completion-${index}`}
                    role="option"
                    aria-selected={index === completionIndex}
                    data-highlighted={index === completionIndex}
                    type="button"
                    key={completion.label}
                    onMouseEnter={() => setCompletionIndex(index)}
                    onMouseDown={(event) => event.preventDefault()}
                    onClick={() => insertCompletion(completion.insertion)}
                  >
                    <code>{completion.label}</code><span>{completion.detail}</span><kbd>{index === completionIndex ? "↵" : ""}</kbd>
                  </button>
                ))}
                {filteredCompletions.length === 0 ? <p className="completion-empty">No matching SPL commands</p> : null}
              </div>
            ) : null}
          </div>
          <div className="time-picker-wrap" ref={timePickerRef}>
            <button
              ref={timeRangeButtonRef}
              className="time-range-button"
              data-testid="time-range-button"
              type="button"
              aria-haspopup="dialog"
              aria-expanded={modal === "time"}
              aria-controls={modal === "time" ? "time-range-popover" : undefined}
              onClick={() => {
                setCompletionOpen(false);
                if (modal === "time") {
                  closeTimePicker();
                  return;
                }
                setDraftTimeRange(timeRange);
                setTimePickerSection("presets");
                setModal("time");
              }}
            >
              <span aria-hidden="true">◷</span>
              <span><small>Time range</small><strong>{timeRange.label}</strong></span>
              <span aria-hidden="true">▾</span>
            </button>
            {modal === "time" ? (
              <section
                className="time-popover"
                id="time-range-popover"
                data-testid="time-picker-dialog"
                role="dialog"
                aria-modal="false"
                aria-labelledby="time-popover-title"
              >
                <header className="time-popover-header">
                  <div><strong id="time-popover-title">Select time range</strong><small>America/Los_Angeles</small></div>
                  <button type="button" aria-label="Close time range" onClick={() => closeTimePicker()}>×</button>
                </header>
                <div className="time-picker-layout">
                  <aside className="time-picker-nav" aria-label="Time range categories">
                    {([
                      ["presets", "Presets"],
                      ["relative", "Relative"],
                      ["range", "Date & time range"],
                      ["advanced", "Advanced"],
                    ] as const).map(([section, label]) => (
                      <button
                        className={timePickerSection === section ? "active" : ""}
                        type="button"
                        aria-pressed={timePickerSection === section}
                        key={section}
                        onClick={() => {
                          setTimePickerSection(section);
                          if (section === "relative") updateRelativeRange(relativeAmount, relativeUnit);
                          if (section === "range") updateAbsoluteRange(absoluteStart, absoluteEnd);
                        }}
                      >{label}</button>
                    ))}
                  </aside>
                  <div className="time-picker-content">
                    {timePickerSection === "presets" ? (
                      <>
                        <h3>Common time ranges</h3>
                        <div className="preset-grid">
                          {TIME_PRESETS.map((preset) => (
                            <button
                              className={draftTimeRange.label === preset.label ? "selected" : ""}
                              type="button"
                              key={preset.label}
                              onClick={() => setDraftTimeRange(preset)}
                            >
                              <span>{preset.label}</span>
                              {draftTimeRange.label === preset.label ? <span aria-hidden="true">✓</span> : null}
                            </button>
                          ))}
                        </div>
                      </>
                    ) : null}
                    {timePickerSection === "relative" ? (
                      <div className="time-form-section">
                        <h3>Relative time</h3>
                        <p>Search backward from the current moment.</p>
                        <div className="relative-time-row">
                          <label><span>Last</span><input type="number" min="1" max="999" value={relativeAmount} onChange={(event) => updateRelativeRange(Number(event.target.value), relativeUnit)} /></label>
                          <label><span>Unit</span><select value={relativeUnit} onChange={(event) => updateRelativeRange(relativeAmount, event.target.value as "m" | "h" | "d")}><option value="m">Minutes</option><option value="h">Hours</option><option value="d">Days</option></select></label>
                          <label><span>Anchor</span><select value="now" disabled><option value="now">Now</option></select></label>
                        </div>
                      </div>
                    ) : null}
                    {timePickerSection === "range" ? (
                      <div className="time-form-section">
                        <h3>Date &amp; time range</h3>
                        <p>Use local time in America/Los_Angeles.</p>
                        <div className="absolute-time-row">
                          <label><span>Start</span><input type="datetime-local" max={absoluteEnd} value={absoluteStart} onInput={(event) => updateAbsoluteRange(event.currentTarget.value, absoluteEnd)} /></label>
                          <label><span>End</span><input type="datetime-local" min={absoluteStart} value={absoluteEnd} onInput={(event) => updateAbsoluteRange(absoluteStart, event.currentTarget.value)} /></label>
                        </div>
                        {absoluteTimeInvalid ? <p className="time-validation" role="alert">End must be later than start.</p> : null}
                      </div>
                    ) : null}
                    {timePickerSection === "advanced" ? (
                      <div className="time-form-section">
                        <h3>Advanced time modifiers</h3>
                        <p>Enter SPL relative modifiers or ISO timestamps.</p>
                        <div className="absolute-time-row">
                          <label><span>Earliest</span><input value={draftTimeRange.earliest} onChange={(event) => setDraftTimeRange({ ...draftTimeRange, label: "Custom time range", earliest: event.target.value })} /></label>
                          <label><span>Latest</span><input value={draftTimeRange.latest} onChange={(event) => setDraftTimeRange({ ...draftTimeRange, label: "Custom time range", latest: event.target.value })} /></label>
                        </div>
                      </div>
                    ) : null}
                  </div>
                </div>
                <div className="range-preview time-popover-preview">
                  <span>Earliest <code>{draftTimeRange.earliest}</code></span>
                  <span>Latest <code>{draftTimeRange.latest}</code></span>
                </div>
                <footer className="time-popover-footer">
                  <button className="button secondary compact" type="button" onClick={() => closeTimePicker()}>Cancel</button>
                  <button
                    className="button primary compact"
                    type="button"
                    disabled={draftTimeRange.earliest.trim().length === 0
                      || draftTimeRange.latest.trim().length === 0
                      || (timePickerSection === "range" && absoluteTimeInvalid)}
                    onClick={() => {
                      setTimeRange(draftTimeRange);
                      closeTimePicker();
                    }}
                  >Apply</button>
                </footer>
              </section>
            ) : null}
          </div>
          <button
            className={`run-button${isRunning ? " cancel" : ""}`}
            data-testid="run-search"
            type="button"
            aria-label={isRunning ? "Cancel search" : "Run search"}
            onClick={(event) => {
              if (isRunning) {
                if (event.detail > 1) return;
                cancelSearch();
              } else {
                runSearch();
              }
            }}
          >
            <span aria-hidden="true">{isRunning ? "■" : "⌕"}</span>
            <strong>{isRunning ? "Cancel" : "Search"}</strong>
          </button>
        </section>

        {diagnostic === null ? null : (
          <div className="diagnostic-strip" id="editor-diagnostic" role="alert" data-testid="search-diagnostic">
            <span className="diagnostic-icon">!</span>
            <span><strong>{diagnostic.message}</strong><small>Line {diagnostic.line}, column {diagnostic.column} · {diagnostic.suggestion}</small></span>
            {diagnostic.actionLabel === undefined ? null : (
              <button type="button" onClick={() => fixDiagnostic(diagnostic)}>{diagnostic.actionLabel}</button>
            )}
          </div>
        )}

        <section className="job-strip" data-testid="job-strip" aria-label="Search job status">
          <div className="job-primary">
            <span className={`job-state-icon ${stateClass(phase)}`} aria-hidden="true">{phase === "completed" ? "✓" : phase === "failed" ? "!" : phase === "canceled" ? "×" : ""}</span>
            <span className="job-result-copy">
              <strong>{phaseLabel(phase)}</strong>
              <span>{NUMBER_FORMAT.format(visibleEventCount)} events</span>
              <small data-testid="job-time-range">
                {!backendEnabled && submittedTimeRange.label === "Last 24 hours"
                  ? "7/20/26 3:44:00 PM to 7/21/26 3:44:00 PM"
                  : submittedTimeRange.label}
              </small>
            </span>
            <button className="sampling-button" type="button">No Event Sampling <span aria-hidden="true">▾</span></button>
          </div>
          <div className="job-metrics" aria-label="Job metrics">
            <span><small>Scanned</small><strong>{NUMBER_FORMAT.format(scannedRows)} rows</strong></span>
            <span><small>Data</small><strong>{scannedBytes}</strong></span>
            <span><small>Elapsed</small><strong>{elapsed}</strong></span>
            <span><small>Progress</small><strong>{progress}%</strong></span>
          </div>
          <div className="job-controls">
            <button type="button" onClick={() => setModal("jobs")}>Job <span aria-hidden="true">▾</span></button>
            <button type="button" aria-label="Inspect search job" title="Inspect job" onClick={() => setModal("inspect")}>ⓘ</button>
            <button type="button" aria-label="Refresh results" title="Refresh results" onClick={() => runSearch(submittedQuery)}>↻</button>
            <button
              type="button"
              aria-label="Share search"
              title="Share search"
              onClick={() => {
                const url = new URL(window.location.href);
                url.searchParams.set("q", submittedQuery);
                url.searchParams.set("earliest", submittedTimeRange.earliest);
                url.searchParams.set("latest", submittedTimeRange.latest);
                url.hash = "search";
                void copyText(url.toString(), "Search link copied to the clipboard.");
              }}
            >⌁</button>
            <div className="header-menu-wrap search-mode-wrap">
              <button type="button" onClick={() => setMenu(menu === "search-mode" ? null : "search-mode")}><span aria-hidden="true">⚡</span> {searchMode} Mode <span aria-hidden="true">▾</span></button>
              {menu === "search-mode" ? (
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
          {isRunning ? <span className="job-progress-bar" style={{ width: `${progress}%` }} /> : null}
        </section>

        <div className="result-tabs" role="tablist" aria-label="Search result views">
          {([
            ["events", "Events", NUMBER_FORMAT.format(visibleEventCount)],
            ["patterns", "Patterns", hasResultData ? String(patternRows.length) : "0"],
            ["statistics", "Statistics", hasResultData ? String(isTimechartResult ? timelinePoints.length : statisticsRows.length) : "0"],
            ["visualization", "Visualization", ""],
          ] as const).map(([id, label, count]) => (
            <button
              id={`tab-${id}`}
              data-testid={`result-tab-${id}`}
              role="tab"
              aria-selected={activeTab === id}
              aria-controls={`panel-${id}`}
              className={activeTab === id ? "active" : ""}
              type="button"
              key={id}
              onClick={() => setActiveTab(id)}
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
          >
            <span aria-hidden="true">{phase === "failed" ? "!" : "×"}</span>
            <strong>{phase === "failed" ? "Search failed before results were produced" : "Search was canceled"}</strong>
            <p>The timeline, fields, and result views are cleared so they cannot be mistaken for this job&apos;s output.</p>
            <button className="button secondary compact" type="button" onClick={() => runSearch(DEFAULT_QUERY)}>Run the default search</button>
          </section>
        ) : null}

        {hasResultData && activeTab === "events" ? (
          <section id="panel-events" role="tabpanel" aria-labelledby="tab-events" className="events-panel">
            <section className="timeline-section" data-testid="timeline" aria-label="Event timeline">
              <div className="timeline-toolbar">
                <div>
                  <div className="header-menu-wrap result-menu-wrap">
                    <button type="button" aria-haspopup="menu" aria-expanded={menu === "timeline-format"} onClick={() => setMenu(menu === "timeline-format" ? null : "timeline-format")}>Format Timeline <span aria-hidden="true">▾</span></button>
                    {menu === "timeline-format" ? (
                      <div className="floating-menu result-control-menu" role="menu" aria-label="Timeline format">
                        {(["Columns", "Compact"] as const).map((display) => (
                          <button role="menuitemradio" aria-checked={timelineDisplay === display} type="button" key={display} onClick={() => { setTimelineDisplay(display); setMenu(null); }}><span className="radio-mark">{timelineDisplay === display ? "●" : "○"}</span><span><strong>{display}</strong><small>{display === "Columns" ? "Full-height event volume" : "Condensed activity profile"}</small></span></button>
                        ))}
                      </div>
                    ) : null}
                  </div>
                  <button type="button" onClick={() => showToast("Timeline is already at the full selected range.")}>− Zoom Out</button>
                  <button type="button" disabled={timelineSelection === null} onClick={zoomTimeline}>＋ Zoom to Selection</button>
                  <button type="button" disabled={timelineSelection === null} onClick={() => { setTimelineStart(null); setTimelineEnd(null); }}>× Deselect</button>
                </div>
                <span>{timelineSelection === null ? (backendEnabled ? "Automatic time span" : "20 minutes per column") : `${timelinePoints[timelineSelection[0]]?.label ?? ""} – ${timelinePoints[timelineSelection[1]]?.label ?? ""}`}</span>
              </div>
              <div
                className={`timeline-chart timeline-${timelineDisplay.toLowerCase()}${draggingTimeline ? " dragging" : ""}`}
                onPointerDown={startTimelineDrag}
                onPointerMove={moveTimelineDrag}
                onPointerUp={endTimelineDrag}
                onPointerCancel={endTimelineDrag}
              >
                <div className="timeline-grid-lines" aria-hidden="true"><span /><span /><span /></div>
                <div className="timeline-bars">
                  {timelinePoints.map((point, index) => {
                    const selected = timelineSelection !== null && index >= timelineSelection[0] && index <= timelineSelection[1];
                    return (
                      <button
                        type="button"
                        className={selected ? "selected" : ""}
                        data-timeline-index={index}
                        aria-label={`${point.label}: ${NUMBER_FORMAT.format(point.count)} events`}
                        title={`${point.label}\n${NUMBER_FORMAT.format(point.count)} events`}
                        key={point.id}
                        style={{ height: `${Math.max(4, (point.count / maxTimelineCount) * 100)}%` }}
                      />
                    );
                  })}
                </div>
                {timelineSelection === null ? null : (
                  <div
                    className="timeline-selection"
                    aria-hidden="true"
                    style={{
                      left: `${(timelineSelection[0] / timelinePoints.length) * 100}%`,
                      width: `${((timelineSelection[1] - timelineSelection[0] + 1) / timelinePoints.length) * 100}%`,
                    }}
                  />
                )}
              </div>
              <div className="timeline-axis" aria-hidden="true"><span>Jul 20, 4 PM</span><span>Jul 20, 10 PM</span><span>Jul 21, 4 AM</span><span>Jul 21, 10 AM</span><span>Jul 21, 4 PM</span></div>
            </section>

            <div className={`events-layout${fieldsCollapsed ? " fields-collapsed" : ""}`}>
              <aside className="fields-rail" data-testid="fields-rail" aria-label="Search fields">
                <div className="fields-topbar">
                  <button type="button" onClick={() => setFieldsCollapsed(!fieldsCollapsed)}><span aria-hidden="true">{fieldsCollapsed ? "»" : "‹"}</span>{fieldsCollapsed ? null : "Hide Fields"}</button>
                  {fieldsCollapsed ? null : <button type="button" onClick={() => setShowAllFields(true)}>▦ All Fields</button>}
                </div>
                {fieldsCollapsed ? (
                  <button className="vertical-fields-label" type="button" onClick={() => setFieldsCollapsed(false)}>Fields</button>
                ) : (
                  <>
                    <label className="field-filter">
                      <span aria-hidden="true">⌕</span>
                      <input aria-label="Filter fields" placeholder="Filter fields" value={fieldFilter} onChange={(event) => setFieldFilter(event.target.value)} />
                    </label>
                    <div className="field-group">
                      <h2>Selected Fields <span>{selectedFields.length}</span></h2>
                      <div className="field-list">
                        {selectedFields.filter((field) => field.name.toLowerCase().includes(fieldFilter.toLowerCase())).map((field) => (
                          <button type="button" key={field.name} className={activeField === field.name ? "active" : ""} onClick={() => setActiveField(activeField === field.name ? null : field.name)}>
                            <span className={`field-type type-${field.type}`}>{field.type === "number" ? "#" : field.type === "boolean" ? "✓" : "a"}</span>
                            <span>{field.displayName}</span><b>{COMPACT_NUMBER_FORMAT.format(field.distinctCount)}</b>
                          </button>
                        ))}
                      </div>
                    </div>
                    <div className="field-group interesting-group">
                      <h2>Interesting Fields <span>{interestingFields.length}</span></h2>
                      <div className="field-list">
                        {visibleInterestingFields.map((field) => (
                          <button type="button" key={field.name} className={activeField === field.name ? "active" : ""} onClick={() => setActiveField(activeField === field.name ? null : field.name)}>
                            <span className={`field-type type-${field.type}`}>{field.type === "number" ? "#" : field.type === "boolean" ? "✓" : "a"}</span>
                            <span>{field.displayName}</span><b>{COMPACT_NUMBER_FORMAT.format(field.distinctCount)}</b>
                          </button>
                        ))}
                      </div>
                      {!showAllFields && interestingFields.length > 8 ? <button className="more-fields" type="button" onClick={() => setShowAllFields(true)}>Show {interestingFields.length - 8} more fields</button> : null}
                    </div>
                  </>
                )}
                {activeFieldData === null || fieldsCollapsed ? null : (
                  <section className="field-inspector" data-testid="field-inspector" aria-label={`${activeFieldData.displayName} field summary`}>
                    <header>
                      <div><span className={`field-type type-${activeFieldData.type}`}>{activeFieldData.type === "number" ? "#" : "a"}</span><strong>{activeFieldData.displayName}</strong></div>
                      <button className="icon-button" type="button" aria-label="Close field summary" onClick={() => setActiveField(null)}>×</button>
                    </header>
                    <div className="field-summary-meta">
                      <span><strong>{NUMBER_FORMAT.format(activeFieldData.eventCount)}</strong> events</span>
                      <span><strong>{NUMBER_FORMAT.format(activeFieldData.distinctCount)}</strong> values</span>
                    </div>
                    <label className="select-field-checkbox"><input type="checkbox" checked={activeFieldData.selected} onChange={() => toggleField(activeFieldData.name)} /> Selected field</label>
                    <h3>Top values</h3>
                    <div className="top-values">
                      {activeFieldData.values.map((item) => {
                        const percent = Math.max(2, (item.count / activeFieldData.eventCount) * 100);
                        return (
                          <div className="top-value" key={formatFieldValue(item.value)}>
                            <button type="button" title="Include this value" onClick={() => applyPivot(activeFieldData.name, item.value, "include")}><span>{formatFieldValue(item.value)}</span><b>{NUMBER_FORMAT.format(item.count)}</b></button>
                            <span className="value-bar" style={{ width: `${percent}%` }} />
                            <div><button type="button" onClick={() => applyPivot(activeFieldData.name, item.value, "include")} aria-label={`Include ${formatFieldValue(item.value)}`}>＋</button><button type="button" onClick={() => applyPivot(activeFieldData.name, item.value, "exclude")} aria-label={`Exclude ${formatFieldValue(item.value)}`}>−</button></div>
                          </div>
                        );
                      })}
                    </div>
                    <footer><button type="button" onClick={() => setShowAllFields(true)}>View all values</button><button type="button" onClick={() => applyPivot(activeFieldData.name, activeFieldData.values[0]?.value ?? "*", "new", true)}>New search</button></footer>
                  </section>
                )}
              </aside>

              {!fieldsCollapsed ? <button className="fields-mobile-dismiss" aria-label="Close fields panel" type="button" onClick={() => setFieldsCollapsed(true)} /> : null}
              <section className={`event-results display-${eventDisplay.toLowerCase()}${wrapEvents ? " wrap-events" : " nowrap-events"}`} aria-label="Events">
                <div className="event-toolbar">
                  <div>
                    <button className="mobile-fields-button" type="button" onClick={() => setFieldsCollapsed(false)}>☰ Fields</button>
                    <div className="header-menu-wrap result-menu-wrap">
                      <button type="button" aria-haspopup="menu" aria-expanded={menu === "event-display"} onClick={() => setMenu(menu === "event-display" ? null : "event-display")}>{eventDisplay} <span aria-hidden="true">▾</span></button>
                      {menu === "event-display" ? (
                        <div className="floating-menu result-control-menu" role="menu" aria-label="Event display">
                          {(["List", "Raw"] as const).map((display) => (
                            <button role="menuitemradio" aria-checked={eventDisplay === display} type="button" key={display} onClick={() => { setEventDisplay(display); setMenu(null); }}><span className="radio-mark">{eventDisplay === display ? "●" : "○"}</span><span><strong>{display}</strong><small>{display === "List" ? "Fields, metadata, and raw event" : "Raw event text with minimal chrome"}</small></span></button>
                          ))}
                        </div>
                      ) : null}
                    </div>
                    <button type="button" aria-pressed={wrapEvents} title={wrapEvents ? "Turn event wrapping off" : "Wrap long event text"} onClick={() => setWrapEvents((current) => !current)}><span aria-hidden="true">✎</span> {wrapEvents ? "Wrap on" : "Wrap off"}</button>
                    <div className="header-menu-wrap result-menu-wrap">
                      <button type="button" aria-haspopup="menu" aria-expanded={menu === "event-page-size"} onClick={() => setMenu(menu === "event-page-size" ? null : "event-page-size")}>{eventPageSize} Per Page <span aria-hidden="true">▾</span></button>
                      {menu === "event-page-size" ? (
                        <div className="floating-menu result-control-menu page-size-menu" role="menu" aria-label="Events per page">
                          {([10, 20, 50] as const).map((size) => <button role="menuitemradio" aria-checked={eventPageSize === size} type="button" key={size} onClick={() => { setEventPageSize(size); setEventPage(1); setMenu(null); }}>{eventPageSize === size ? "✓" : ""}<span><strong>{size} events</strong></span></button>)}
                        </div>
                      ) : null}
                    </div>
                  </div>
                  <nav aria-label="Event pages">
                    <button type="button" disabled={eventPage === 1} onClick={() => setEventPage((current) => Math.max(1, current - 1))}>‹ Prev</button>
                    {[1, 2, 3].filter((page) => page <= eventPageCount).map((page) => <button className={eventPage === page ? "active" : ""} aria-current={eventPage === page ? "page" : undefined} type="button" key={page} onClick={() => setEventPage(page)}>{page}</button>)}
                    {eventPage > 3 && eventPage < eventPageCount ? <><span>…</span><button className="active" aria-current="page" type="button">{NUMBER_FORMAT.format(eventPage)}</button></> : null}
                    {eventPageCount > 4 ? <span>…</span> : null}
                    {eventPageCount > 3 ? <button className={eventPage === eventPageCount ? "active" : ""} aria-current={eventPage === eventPageCount ? "page" : undefined} type="button" onClick={() => setEventPage(eventPageCount)}>{NUMBER_FORMAT.format(eventPageCount)}</button> : null}
                    <button type="button" disabled={eventPage === eventPageCount} onClick={() => setEventPage((current) => Math.min(eventPageCount, current + 1))}>Next ›</button>
                  </nav>
                </div>
                <div className="event-head"><span /><button type="button" aria-label={`Sort by time, ${eventSortDirection === "desc" ? "ascending" : "descending"}`} onClick={() => { setEventSortDirection((current) => current === "desc" ? "asc" : "desc"); setEventPage(1); }}>Time <span aria-hidden="true">{eventSortDirection === "desc" ? "↓" : "↑"}</span></button><span>Event</span></div>
                <div className="event-list" data-testid="event-list">
                  {pagedResultEvents.map((event) => {
                    const expanded = expandedEvents.has(event.id);
                    const level = String(event.fields.level ?? "INFO").toLowerCase();
                    return (
                      <article className={`event-row level-${level}${expanded ? " expanded" : ""}`} data-testid={`event-row-${event.id}`} key={event.id}>
                        <button className="event-expander" type="button" aria-label={`${expanded ? "Collapse" : "Expand"} event`} aria-expanded={expanded} onClick={() => toggleEvent(event.id)}>{expanded ? "⌄" : "›"}</button>
                        <button className="event-time" type="button" title="Find nearby events" onClick={() => showToast("Choose a nearby interval from the time range picker.")}><span>{event.timeLabel.split(", ")[0]}</span><strong>{event.timeLabel.split(", ").slice(1).join(", ")}</strong></button>
                        <div className="event-content">
                          <button className="event-raw" type="button" aria-label={`${expanded ? "Collapse" : "Expand"} event details`} onClick={() => toggleEvent(event.id)}>{highlightedRaw(event.raw, submittedQuery)}</button>
                          <div className="event-chips">
                            {["host", "source", "sourcetype"].map((fieldName) => (
                              <button type="button" key={fieldName} onClick={() => setActiveField(fieldName)}><span>{fieldName}</span> = {formatFieldValue(event.fields[fieldName] ?? "")}</button>
                            ))}
                          </div>
                          {expanded ? (
                            <div className="event-detail">
                              <header><strong>Event fields</strong><span>{Object.keys(event.fields).length} fields · typed JSON</span><button type="button" onClick={() => void copyText(event.raw, "Raw event copied.")}>Copy raw</button></header>
                              <div className="event-field-grid">
                                {Object.entries(event.fields).map(([fieldName, fieldValue]) => (
                                  <div className="event-field" key={fieldName}>
                                    <button className="event-field-name" type="button" onClick={() => setActiveField(fieldName)}>{fieldName}</button>
                                    <span className={`value-type value-${fieldValue === null ? "null" : typeof fieldValue}`}>{fieldValue === null ? "null" : typeof fieldValue}</span>
                                    <code>{formatFieldValue(fieldValue)}</code>
                                    <div className="event-field-actions">
                                      <button type="button" title="Include in current search" aria-label={`Include ${fieldName}`} onClick={() => applyPivot(fieldName, fieldValue, "include")}>＋</button>
                                      <button type="button" title="Exclude from current search" aria-label={`Exclude ${fieldName}`} onClick={() => applyPivot(fieldName, fieldValue, "exclude")}>−</button>
                                      <button type="button" title="Open as new search" aria-label={`New search for ${fieldName}`} onClick={() => applyPivot(fieldName, fieldValue, "new", true)}>⌕</button>
                                    </div>
                                  </div>
                                ))}
                              </div>
                            </div>
                          ) : null}
                        </div>
                      </article>
                    );
                  })}
                  {resultEvents.length === 0 ? <div className="empty-state event-empty"><strong>No events found</strong><span>Widen the time range or remove a field filter.</span><button className="button secondary compact" type="button" onClick={() => setQuery("index=gradethis")}>Reset search</button></div> : null}
                </div>
              </section>
            </div>
          </section>
        ) : null}

        {hasResultData && activeTab === "patterns" ? (
          <section id="panel-patterns" role="tabpanel" aria-labelledby="tab-patterns" className="patterns-panel">
            <header className="result-view-header">
              <div><h2>Event patterns</h2><p>Similar raw events grouped into recurring signatures.</p></div>
              <div className="header-menu-wrap result-menu-wrap">
                <button className="button secondary compact" type="button" aria-haspopup="menu" aria-expanded={menu === "pattern-sensitivity"} onClick={() => setMenu(menu === "pattern-sensitivity" ? null : "pattern-sensitivity")}>Sensitivity: {patternSensitivity} <span aria-hidden="true">▾</span></button>
                {menu === "pattern-sensitivity" ? (
                  <div className="floating-menu result-control-menu" role="menu" aria-label="Pattern sensitivity">
                    {(["Precise", "Balanced", "Broad"] as const).map((sensitivity) => (
                      <button role="menuitemradio" aria-checked={patternSensitivity === sensitivity} type="button" key={sensitivity} onClick={() => { setPatternSensitivity(sensitivity); setMenu(null); showToast(`Pattern sensitivity set to ${sensitivity.toLowerCase()}.`, "success"); }}><span className="radio-mark">{patternSensitivity === sensitivity ? "●" : "○"}</span><span><strong>{sensitivity}</strong><small>{sensitivity === "Precise" ? "More, narrowly matched patterns" : sensitivity === "Balanced" ? "A practical grouping of recurring events" : "Fewer, more inclusive patterns"}</small></span></button>
                    ))}
                  </div>
                ) : null}
              </div>
            </header>
            <div className="pattern-table">
              <div className="pattern-head"><span>Pattern</span><span className="pattern-events-head">Events</span><span className="pattern-coverage-head">Coverage</span><span className="pattern-action-head">Action</span></div>
              {patternRows.map((pattern, index) => (
                <article key={pattern.signature}>
                  <span className="pattern-rank">{index + 1}</span>
                  <code title={pattern.signature}>{pattern.signature}</code>
                  <strong className="pattern-event-count">{NUMBER_FORMAT.format(pattern.count)}<span> events</span></strong>
                  <div className="pattern-coverage"><span style={{ width: `${pattern.percent}%` }} /><b>{pattern.percent}%</b></div>
                  <button className="pattern-action" type="button" onClick={() => { const nextQuery = queryForPattern(pattern.signature); setActiveTab("events"); runSearch(nextQuery); }}>View events <span aria-hidden="true">›</span></button>
                </article>
              ))}
            </div>
          </section>
        ) : null}

        {hasResultData && activeTab === "statistics" ? (
          <section id="panel-statistics" role="tabpanel" aria-labelledby="tab-statistics" className="statistics-panel">
            <header className="result-view-header">
              <div><h2>Statistics</h2><p>{isTimechartResult ? timelinePoints.length : statisticsRows.length} rows · completed in {elapsed}</p></div>
              <div>
                <button className="button secondary compact" type="button" onClick={() => openExportDialog("statistics")}>⇩ Export</button>
                <div className="header-menu-wrap result-menu-wrap">
                  <button className="button secondary compact" type="button" aria-haspopup="menu" aria-expanded={menu === "stats-format"} onClick={() => setMenu(menu === "stats-format" ? null : "stats-format")}>Format <span aria-hidden="true">▾</span></button>
                  {menu === "stats-format" ? (
                    <div className="floating-menu result-control-menu" role="menu" aria-label="Statistics table format">
                      {(["compact", "standard"] as const).map((density) => (
                        <button role="menuitemradio" aria-checked={statsDensity === density} type="button" key={density} onClick={() => { setStatsDensity(density); setMenu(null); }}><span className="radio-mark">{statsDensity === density ? "●" : "○"}</span><span><strong>{density === "compact" ? "Compact rows" : "Standard rows"}</strong><small>{density === "compact" ? "Fit more results on screen" : "Add breathing room for scanning"}</small></span></button>
                      ))}
                    </div>
                  ) : null}
                </div>
              </div>
            </header>
            <div className={`statistics-table-frame${statsHasScrolled ? " has-scrolled" : ""}`}>
            <div className="statistics-table-shell" role="region" aria-label="Scrollable statistics table" onScroll={(event) => { if (event.currentTarget.scrollLeft > 12) setStatsHasScrolled(true); }}>
            {isTimechartResult ? (
              <table className={`statistics-table timechart-table density-${statsDensity}`} aria-label="Timechart statistics">
                <colgroup><col className="timechart-col-time" /><col className="timechart-col-count" /></colgroup>
                <thead><tr>{([[
                  "time", "_time", false], ["count", "count", true],
                ] as const).map(([key, label, numeric]) => {
                  const sorted = timechartSort.key === key;
                  const nextDirection = sorted && timechartSort.direction === "desc" ? "ascending" : "descending";
                  return <th className={numeric ? "numeric-cell" : undefined} scope="col" aria-sort={sorted ? (timechartSort.direction === "desc" ? "descending" : "ascending") : "none"} key={key}><button type="button" aria-label={`Sort by ${label}, ${nextDirection}`} onClick={() => setTimechartSort((current) => ({ key, direction: current.key === key && current.direction === "desc" ? "asc" : "desc" }))}><span>{label}</span><i className={sorted ? "sort-active" : ""} aria-hidden="true">{sorted ? (timechartSort.direction === "desc" ? "↓" : "↑") : "↕"}</i></button></th>;
                })}</tr></thead>
                <tbody>{sortedTimechartRows.map((row) => <tr key={row.id}><td><time>{row.label}</time></td><td className="numeric-cell">{NUMBER_FORMAT.format(row.count)}</td></tr>)}</tbody>
              </table>
            ) : (
            <table className={`statistics-table density-${statsDensity}`} aria-label="Search statistics">
              <colgroup><col className="statistics-col-level" /><col className="statistics-col-count" /><col className="statistics-col-percent" /><col className="statistics-col-average" /></colgroup>
              <thead>
                <tr>
                  {([
                    ["level", statisticsDimension, false], ["count", "count", true], ["percent", "% of results", true], ["avgDuration", "avg(duration_ms)", true],
                  ] as const).map(([key, label, numeric]) => {
                    const sorted = statsSort.key === key;
                    const nextDirection = sorted && statsSort.direction === "desc" ? "ascending" : "descending";
                    return (
                      <th
                        scope="col"
                        key={key}
                        className={numeric ? "numeric-cell" : undefined}
                        aria-sort={sorted ? (statsSort.direction === "desc" ? "descending" : "ascending") : "none"}
                      >
                        <button type="button" aria-label={`Sort by ${label}, ${nextDirection}`} onClick={() => updateStatsSort(key)}><span>{label}</span><i className={sorted ? "sort-active" : ""} aria-hidden="true">{sorted ? (statsSort.direction === "desc" ? "↓" : "↑") : "↕"}</i></button>
                      </th>
                    );
                  })}
                </tr>
              </thead>
              <tbody>
                {sortedStatistics.map((row) => (
                  <tr key={row.level}>
                    <td><button className="statistics-value-link" type="button" title={`Add ${statisticsDimension}=${row.level} to the draft search`} onClick={() => applyPivot(statisticsDimension, row.level, "include")}><span className={`severity-dot severity-${row.level.toLowerCase()}`} />{row.level}</button></td>
                    <td className="numeric-cell">{NUMBER_FORMAT.format(row.count)}</td>
                    <td className="numeric-cell">{row.percent}</td>
                    <td className="numeric-cell">{Number.isFinite(row.avgDuration) ? <>{row.avgDuration.toFixed(1)} <span className="numeric-unit">ms</span></> : "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            )}
            </div>
            <span className="statistics-scroll-hint" aria-hidden="true">More columns <b>→</b></span>
            </div>
            <footer className="statistics-footer">{isTimechartResult ? <><span>Showing 1–{timelinePoints.length} of {timelinePoints.length} rows</span><span>Sorted by {timechartSort.key === "time" ? "_time" : "count"} · {timechartSort.direction === "desc" ? "descending" : "ascending"}</span></> : <><span>Showing 1–{statisticsRows.length} of {statisticsRows.length} rows</span><span>Sorted by {statsSort.key === "avgDuration" ? "avg(duration_ms)" : statsSort.key === "level" ? statisticsDimension : statsSort.key} · {statsSort.direction === "desc" ? "descending" : "ascending"}</span></>}</footer>
          </section>
        ) : null}

        {hasResultData && activeTab === "visualization" ? (
          <section id="panel-visualization" role="tabpanel" aria-labelledby="tab-visualization" className="visualization-panel">
            <header className="result-view-header"><div><h2>{chartTitle.trim() || "Untitled visualization"}</h2><p>{isTimechartResult ? "Timechart across the submitted search range." : "Aggregation of the displayed event set."}</p></div><fieldset className="chart-toggle"><legend className="sr-only">Chart style</legend><button className={chartStyle === "column" ? "active" : ""} type="button" aria-pressed={chartStyle === "column"} onClick={() => { setChartStyle("column"); setChartTitle(isTimechartResult ? "Event volume over time" : "Event volume by level"); }}>▥ Column</button><button className={chartStyle === "horizontal" ? "active" : ""} type="button" aria-pressed={chartStyle === "horizontal"} disabled={isTimechartResult} title={isTimechartResult ? "Bar charts require categorical results" : undefined} onClick={() => { setChartStyle("horizontal"); setChartTitle("Event volume by level"); }}>☷ Bar</button><button className={chartStyle === "line" ? "active" : ""} type="button" aria-pressed={chartStyle === "line"} disabled={!isTimechartResult} title={!isTimechartResult ? "Line charts require time-series results" : undefined} onClick={() => { setChartStyle("line"); setChartTitle("Event volume over time"); }}>⌁ Line</button><button type="button" onClick={() => showToast("Area and scatter charts become available for compatible result shapes.")}>More…</button></fieldset></header>
            <div className={`visualization-canvas chart-${chartStyle} legend-${legendPosition}`} data-testid="visualization-chart">
              <div className="chart-y-axis" aria-hidden="true">{(isTimechartResult ? [maxTimelineCount, maxTimelineCount * 0.75, maxTimelineCount * 0.5, maxTimelineCount * 0.25, 0].map((value) => COMPACT_NUMBER_FORMAT.format(Math.round(value))) : ["10k", "7.5k", "5k", "2.5k", "0"]).map((label) => <span key={`${isTimechartResult ? "time" : "category"}-${label}`}>{label}</span>)}</div>
              <div className="chart-plot">
                <div className="chart-grid" aria-hidden="true"><span /><span /><span /><span /></div>
                {chartStyle === "line" ? (
                  <div className="line-chart" data-testid="line-chart">
                    <svg viewBox="0 0 1000 320" preserveAspectRatio="none" role="img" aria-label="Event count over the selected time range">
                      <defs><linearGradient id="event-line-fill" x1="0" x2="0" y1="0" y2="1"><stop offset="0%" stopColor="#5b9b37" stopOpacity="0.28" /><stop offset="100%" stopColor="#5b9b37" stopOpacity="0.02" /></linearGradient></defs>
                      <polygon points={`0,306 ${lineChartPoints} 1000,306`} fill="url(#event-line-fill)" />
                      <polyline points={lineChartPoints} fill="none" stroke="#4f8c2f" strokeWidth="4" vectorEffect="non-scaling-stroke" />
                      {timelinePoints.filter((_, index) => index % 12 === 0 || index === timelinePoints.length - 1).map((point) => {
                        const index = timelinePoints.indexOf(point);
                        const x = (index / Math.max(1, timelinePoints.length - 1)) * 1000;
                        const y = 292 - (point.count / maxTimelineCount) * 254;
                        return <g key={point.id}><circle cx={x} cy={y} r="6" fill="#fff" stroke="#4f8c2f" strokeWidth="3"><title>{point.label}: {NUMBER_FORMAT.format(point.count)} events</title></circle>{showDataLabels ? <text x={x} y={Math.max(24, y - 15)}>{COMPACT_NUMBER_FORMAT.format(point.count)}</text> : null}</g>;
                      })}
                    </svg>
                    <div className="line-chart-axis" aria-hidden="true"><span>Jul 20, 4 PM</span><span>Jul 20, 10 PM</span><span>Jul 21, 4 AM</span><span>Jul 21, 10 AM</span><span>Jul 21, 4 PM</span></div>
                  </div>
                ) : isTimechartResult ? (
                  <div className="timechart-columns" data-testid="timechart-columns">
                    <div className="timechart-column-bars">
                      {timelinePoints.map((point, index) => (
                        <button type="button" key={point.id} aria-label={`${point.label}: ${NUMBER_FORMAT.format(point.count)} events`} title={`${point.label}\n${NUMBER_FORMAT.format(point.count)} events`}><span style={{ height: `${Math.max(3, (point.count / maxTimelineCount) * 100)}%` }} />{showDataLabels && (index % 12 === 0 || index === timelinePoints.length - 1) ? <b>{COMPACT_NUMBER_FORMAT.format(point.count)}</b> : null}</button>
                      ))}
                    </div>
                    <div className="line-chart-axis" aria-hidden="true"><span>Jul 20, 4 PM</span><span>Jul 20, 10 PM</span><span>Jul 21, 4 AM</span><span>Jul 21, 10 AM</span><span>Jul 21, 4 PM</span></div>
                  </div>
                ) : (
                  <div className="chart-bars">
                    {statisticsRows.map((row) => (
                      <div className="chart-series" key={row.level}>
                        <button type="button" title={`${row.level}: ${NUMBER_FORMAT.format(row.count)} events`} onClick={() => applyPivot(statisticsDimension, row.level, "include")} style={{ "--bar-size": `${(row.count / maxStatisticsCount) * 100}%` } as React.CSSProperties}>
                          <span className={`chart-fill chart-fill-${row.level.toLowerCase()}`} />{showDataLabels ? <b>{NUMBER_FORMAT.format(row.count)}</b> : null}
                        </button>
                        <strong>{row.level}</strong>
                      </div>
                    ))}
                  </div>
                )}
              </div>
              {legendPosition === "none" ? null : <div className="chart-legend">{isTimechartResult ? <span><i className="legend-info" />Events</span> : statisticsRows.map((row) => <span key={row.level}><i className={`legend-${row.level.toLowerCase()}`} />{row.level}</span>)}</div>}
            </div>
            <aside className="visualization-settings"><h3>Visualization</h3><label><span>Title</span><input value={chartTitle} onChange={(event) => setChartTitle(event.target.value)} /></label><label><span>Legend</span><select value={legendPosition} onChange={(event) => setLegendPosition(event.target.value as "bottom" | "right" | "none")}><option value="bottom">Bottom</option><option value="right">Right</option><option value="none">Hidden</option></select></label><label><span>Data labels</span><input type="checkbox" checked={showDataLabels} onChange={(event) => setShowDataLabels(event.target.checked)} /></label></aside>
          </section>
        ) : null}
      </section>

      {modalContents}

      {toast === null ? null : (
        <output className={`toast toast-${toast.tone}`} data-testid="toast">
          <span aria-hidden="true">{toast.tone === "success" ? "✓" : toast.tone === "warning" ? "!" : "i"}</span>
          <strong>{toast.message}</strong>
          <button type="button" aria-label="Dismiss notification" onClick={() => setToast(null)}>×</button>
        </output>
      )}
    </main>
  );
}
