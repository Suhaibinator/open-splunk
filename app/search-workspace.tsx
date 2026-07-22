"use client";

import {
  type ChangeEvent,
  type FormEvent,
  type KeyboardEvent,
  type PointerEvent,
  type UIEvent,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import Link from "next/link";

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
import type { SearchJob } from "@/gen/ts/open_splunk/v1/search";
import { createOpenSplunkApiClient } from "@/lib/api";
import {
  adaptSearchResults,
  compareWorkspaceStatisticValues,
  indexesFromSPL,
  patternsFromEvents,
  resolveAbsoluteTimeRange,
  timechartRowsForExport,
  timechartValueFields,
  type WorkspaceStatistic,
  type WorkspaceStatisticsSort,
  type WorkspaceStatisticsTable,
} from "@/lib/search/backend-data";
import { applyFieldPivot, type PivotMode } from "@/lib/search/query-pivots";
import { splFromFindInput } from "@/lib/search/launch-url";
import {
  applyDiagnosticFix,
  completionContextAt,
  getQueryDiagnostic,
  isCursorInQuotedValue,
  type SplDiagnostic,
} from "@/lib/search/spl-editor";

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
  EventDisplay,
  ExportStage,
  JobPhase,
  MenuName,
  ModalName,
  PatternSensitivity,
  ResultTab,
  SearchMode,
  SearchWorkspaceProps,
  StatsDensity,
  TimePickerSection,
  TimeRange,
  TimelineDisplay,
  ToastState,
} from "./search-workspace/model";
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
  const [saveAsNew, setSaveAsNew] = useState(false);
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
  const [genericStatsSort, setGenericStatsSort] = useState<WorkspaceStatisticsSort | null>(null);
  const [timechartSort, setTimechartSort] = useState<{ key: "time" | "count"; direction: "asc" | "desc" }>({ key: "time", direction: "asc" });
  const [showAllFields, setShowAllFields] = useState(false);
  const [globalFind, setGlobalFind] = useState("");
  const timersRef = useRef<number[]>([]);
  const generationRef = useRef(0);
  const searchLaunchRef = useRef(false);
  const backendAbortRef = useRef<AbortController | null>(null);
  const backendJobIdRef = useRef<string | null>(null);
  const urlLaunchAppliedRef = useRef(false);
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
  const hasResultData = !searchIsClosed && (backendEnabled ? phase === "completed" : phase !== "failed" && phase !== "canceled");
  const diagnostic = useMemo(() => query.trim().length === 0 ? null : getQueryDiagnostic(query), [query]);
  const completionContext = useMemo(() => completionContextAt(query, editorCaret), [editorCaret, query]);
  const filteredCompletions = useMemo(() => {
    const prefix = completionContext?.prefix.toLowerCase() ?? "";
    return prefix.length === 0
      ? COMPLETIONS
      : COMPLETIONS.filter((completion) => completion.label.startsWith(prefix));
  }, [completionContext]);
  const resultEvents = useMemo(
    () => searchIsClosed ? [] : backendEnabled ? backendEvents : filteredDemoEvents(submittedQuery),
    [backendEnabled, backendEvents, searchIsClosed, submittedQuery],
  );
  const timelinePoints = backendEnabled ? backendTimeline : DEMO_TIMELINE;
  const timechartValueColumns = useMemo(() => timechartValueFields(timelinePoints), [timelinePoints]);
  const statisticsRows: WorkspaceStatistic[] = backendEnabled ? backendStatistics : DEMO_STATISTICS;
  const isTimechartResult = hasPipelineCommand(submittedQuery, "timechart");
  const genericStatisticsTable = backendEnabled && !isTimechartResult ? backendStatisticsTable : null;
  const statisticsRowCount = genericStatisticsTable?.rows.length ?? (isTimechartResult ? timelinePoints.length : statisticsRows.length);
  const baseEventCount = backendEnabled ? backendEventCount : eventCountForQuery(submittedQuery);
  const timelineSelection = useMemo(() => {
    if (timelineStart === null || timelineEnd === null) return null;
    return [Math.min(timelineStart, timelineEnd), Math.max(timelineStart, timelineEnd)] as const;
  }, [timelineEnd, timelineStart]);
  const selectedTimelineCount = useMemo(() => {
    if (timelineSelection === null) return null;
    return timelinePoints.slice(timelineSelection[0], timelineSelection[1] + 1).reduce((total, point) => total + point.count, 0);
  }, [timelinePoints, timelineSelection]);
  const visibleEventCount = searchIsClosed || phase === "failed" || phase === "canceled"
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
    const offset = ((eventPage - 1) * eventPageSize) % ordered.length;
    const rotated = [...ordered.slice(offset), ...ordered.slice(0, offset)];
    return rotated.slice(0, Math.min(eventPageSize, rotated.length));
  }, [backendEnabled, eventPage, eventPageSize, eventSortDirection, resultEvents]);
  const dirty = query !== submittedQuery
    || timeRange.earliest !== submittedTimeRange.earliest
    || timeRange.latest !== submittedTimeRange.latest;
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
  const exportFieldLabels = useMemo(() => ({
    ...EXPORT_FIELD_LABELS,
    ...Object.fromEntries((genericStatisticsTable?.columns ?? []).map((column) => [column.key, column.label])),
    ...Object.fromEntries(timechartValueColumns.map((series) => [series, series])),
  }), [genericStatisticsTable, timechartValueColumns]);
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
    const trigger = mobileProductTriggerRef.current;
    const previousOverflow = document.body.style.overflow;
    const inertedElements: HTMLElement[] = [];
    document.body.style.overflow = "hidden";
    let current: HTMLElement | null = drawer;
    while (current !== null && current.parentElement !== null && current !== document.body) {
      const parent = current.parentElement;
      for (const sibling of parent.children) {
        if (sibling !== current
          && sibling instanceof HTMLElement
          && !sibling.classList.contains("search-mobile-backdrop")
          && !sibling.inert) {
          sibling.inert = true;
          inertedElements.push(sibling);
        }
      }
      current = parent;
    }
    window.requestAnimationFrame(() => drawer?.querySelector<HTMLElement>("button, a")?.focus());
    function handleKeyDown(event: globalThis.KeyboardEvent) {
      if (event.key === "Escape") {
        setMobileProductNavOpen(false);
        return;
      }
      if (event.key !== "Tab" || drawer === null) return;
      const controls = Array.from(drawer.querySelectorAll<HTMLElement>('button, a[href], [tabindex]:not([tabindex="-1"])'));
      const first = controls[0];
      const last = controls.at(-1);
      if (first === undefined || last === undefined) return;
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    }
    document.addEventListener("keydown", handleKeyDown);
    return () => {
      document.body.style.overflow = previousOverflow;
      for (const element of inertedElements) element.inert = false;
      document.removeEventListener("keydown", handleKeyDown);
      window.requestAnimationFrame(() => trigger?.focus());
    };
  }, [mobileProductNavOpen]);

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

  function seedAbsoluteRange() {
    try {
      const resolved = resolveAbsoluteTimeRange(timeRange.earliest, timeRange.latest);
      updateAbsoluteRange(formatLocalDateTimeInput(resolved.earliest), formatLocalDateTimeInput(resolved.latest));
    } catch {
      const fallback = resolveAbsoluteTimeRange("-24h", "now");
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
    if (urlLaunchAppliedRef.current) return;
    urlLaunchAppliedRef.current = true;
    const url = new URL(window.location.href);
    const sharedQuery = url.searchParams.get("q");
    const initialQuery = sharedQuery === null || sharedQuery.trim().length === 0 ? DEFAULT_QUERY : sharedQuery;
    const earliest = url.searchParams.get("earliest");
    const latest = url.searchParams.get("latest");
    const sharedRange = TIME_PRESETS.find((preset) => preset.earliest === earliest && preset.latest === latest);
    let initialRange = TIME_PRESETS[3];
    timelineZoomParentRef.current = null;
    if (sharedQuery !== null && sharedQuery.trim().length > 0) {
      setQuery(sharedQuery);
      setEditorCaret(sharedQuery.length);
    }
    if (earliest !== null && latest !== null) {
      const restoredRange = sharedRange ?? { label: url.searchParams.get("label") || `${earliest} to ${latest}`, earliest, latest };
      initialRange = restoredRange;
      setTimeRange(restoredRange);
      setDraftTimeRange(restoredRange);
    }
    const hasContextualQuery = sharedQuery !== null && sharedQuery.trim().length > 0;
    const shouldRunContextualQuery = hasContextualQuery && url.searchParams.get("run") !== "0";
    if (hasContextualQuery && !shouldRunContextualQuery) {
      setSubmittedQuery("");
      setSubmittedTimeRange(initialRange);
      setPhase("completed");
      setProgress(0);
      setElapsed("0.00 s");
      setScannedRows(0);
      setScannedBytes("0 B");
      setBackendEventCount(0);
      setBackendEvents([]);
      setBackendStatistics([]);
      setBackendStatisticsTable(null);
      setBackendTimeline([]);
      setTimelineStart(null);
      setTimelineEnd(null);
      setEventPage(1);
      setActiveTab("events");
    }
    if ((backendEnabled && !hasContextualQuery) || shouldRunContextualQuery) {
      window.setTimeout(() => searchRunnerRef.current(initialQuery, initialRange), 0);
    }
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
    const adapted = adaptSearchResults(schema, rows, hasPipelineCommand(queryText, "timechart"));
    setBackendEvents(adapted.events);
    setFields(adapted.fields);
    setBackendStatistics(adapted.statistics);
    setBackendStatisticsTable(adapted.statisticsTable);
    setGenericStatsSort(null);
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
    setBackendStatisticsTable(null);
    setGenericStatsSort(null);
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
        if (hasPipelineCommand(nextQuery, "timechart")) {
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

  searchRunnerRef.current = (queryText, range) => {
    runSearch(queryText, range);
  };

  function runSearch(queryOverride?: string, rangeOverride: TimeRange = timeRange) {
    if (searchLaunchRef.current) return;
    const nextQuery = queryOverride ?? query;
    const nextDiagnostic = getQueryDiagnostic(nextQuery);
    if (nextDiagnostic !== null) {
      setQuery(nextQuery);
      setCompletionOpen(false);
      showToast(nextDiagnostic.message, "warning");
      focusEditor(nextQuery.trim().length === 0 ? 0 : nextQuery.length);
      return;
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
    setScannedBytes("0 B");
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
    if (timelineSelection === null) return;
    const first = timelinePoints[timelineSelection[0]];
    const last = timelinePoints[timelineSelection[1]];
    if (first === undefined || last === undefined) return;
    const intervalEndLabel = timelinePoints[timelineSelection[1] + 1]?.label ?? last.latest ?? timelineBoundaryLabel(timelineSelection[1] + 1);
    const narrowedRange = {
      label: `${first.label} – ${intervalEndLabel}`,
      earliest: first.earliest ?? first.label,
      latest: last.latest ?? timelinePoints[timelineSelection[1] + 1]?.earliest ?? intervalEndLabel,
    };
    timelineZoomParentRef.current = submittedTimeRange;
    setTimeRange(narrowedRange);
    setDraftTimeRange(narrowedRange);
    setTimelineStart(null);
    setTimelineEnd(null);
    runSearch(submittedQuery, narrowedRange);
  }

  function zoomOutTimeline() {
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
    setSaveName(existing === undefined ? "Production log investigation" : forceNew ? `${existing.name} copy` : existing.name);
    setSaveDescription(savedSearches.find((item) => item.id === activeSavedSearchId)?.description ?? "");
    setModal("save");
    setMenu(null);
  }

  function saveSearch() {
    const trimmedName = saveName.trim();
    if (trimmedName.length === 0) return;
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
    timelineZoomParentRef.current = null;
    setActiveSavedSearchId(saved.id);
    setModal(null);
    showToast(`Opened “${saved.name}”. Run it when ready.`, "info");
    focusEditor(saved.query.length);
  }

  function openHistoryEntry(entry: DemoHistoryEntry, rerun: boolean) {
    const restoredRange = entry.earliest !== undefined && entry.latest !== undefined
      ? { label: entry.timeRange, earliest: entry.earliest, latest: entry.latest }
      : TIME_PRESETS.find((preset) => preset.label === entry.timeRange) ?? timeRange;
    setQuery(entry.query);
    setEditorCaret(entry.query.length);
    setTimeRange(restoredRange);
    setDraftTimeRange(restoredRange);
    timelineZoomParentRef.current = null;
    setModal(null);
    if (rerun) runSearch(entry.query, restoredRange);
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
    if (isTimechartResult && (tab === "statistics" || tab === "visualization")) {
      return ["_time", ...timechartValueColumns];
    }
    if (genericStatisticsTable !== null && (tab === "statistics" || tab === "visualization")) {
      return genericStatisticsTable.columns.map((column) => column.key);
    }
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

  function updateGenericStatsSort(key: string) {
    setGenericStatsSort((current) => current?.key === key
      ? { key, direction: current.direction === "asc" ? "desc" : "asc" }
      : { key, direction: "asc" });
  }

  function handleGlobalFind(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (globalFind.trim().length === 0) {
      showToast("Enter a term or SPL search to find events.", "warning");
      return;
    }
    const nextQuery = splFromFindInput(globalFind);
    timelineZoomParentRef.current = null;
    setActiveSavedSearchId(null);
    setGlobalFind("");
    runSearch(nextQuery, timeRange);
  }

  function handleManualTimeRangeChange(nextRange: TimeRange) {
    timelineZoomParentRef.current = null;
    setTimeRange(nextRange);
  }

  function closeSearchWorkspace() {
    if (isRunning) cancelSearch();
    setQuery("");
    setSubmittedQuery("");
    setActiveSavedSearchId(null);
    setTimelineStart(null);
    setTimelineEnd(null);
    setMenu(null);
    window.requestAnimationFrame(() => editorRef.current?.focus());
  }

  function handleResultTabKeyDown(event: KeyboardEvent<HTMLButtonElement>, currentTab: ResultTab) {
    const currentIndex = RESULT_TAB_ORDER.indexOf(currentTab);
    let nextIndex = currentIndex;
    if (event.key === "ArrowLeft") nextIndex = Math.max(0, currentIndex - 1);
    else if (event.key === "ArrowRight") nextIndex = Math.min(RESULT_TAB_ORDER.length - 1, currentIndex + 1);
    else if (event.key === "Home") nextIndex = 0;
    else if (event.key === "End") nextIndex = RESULT_TAB_ORDER.length - 1;
    else return;
    event.preventDefault();
    const nextTab = RESULT_TAB_ORDER[nextIndex];
    setActiveTab(nextTab);
    window.requestAnimationFrame(() => document.getElementById(`tab-${nextTab}`)?.focus());
  }

  const emptyResultPresentation = searchIsClosed
    ? {
        icon: "⌕",
        title: "Start a new search",
        detail: "Enter SPL, choose a time range, and run the search to inspect events and statistics.",
      }
    : isRunning
    ? {
        icon: "…",
        title: "Search is running",
        detail: "Results will appear here as soon as the backend produces them.",
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

  return (
    <div className="splunk-shell" data-testid="search-workspace" id="search">
      <a className="skip-link" href="#search-main-content">Skip to search workspace</a>
      <header className="product-bar">
        <div className="product-left">
          <button ref={mobileProductTriggerRef} className="search-mobile-trigger" type="button" aria-label="Open product navigation" aria-expanded={mobileProductNavOpen} onClick={() => setMobileProductNavOpen(true)}><span /><span /><span /></button>
          <Link className="wordmark" href="/" aria-label="Open Splunk home"><span>open</span><b>&gt;</b><span>splunk</span></Link>
          <div className="header-menu-wrap">
            <button className="product-menu-button" type="button" aria-haspopup="menu" aria-expanded={menu === "app"} onClick={() => setMenu(menu === "app" ? null : "app")}>
              App: <strong>Search &amp; Reporting</strong> <span aria-hidden="true">▾</span>
            </button>
            {menu === "app" ? (
              <div className="floating-menu app-menu" role="menu">
                <span className="menu-label">Your apps</span>
                <Link role="menuitem" href="/search/" className="selected"><span className="app-glyph">⌕</span><span><strong>Search &amp; Reporting</strong><small>Search all authorized indexes</small></span><b>✓</b></Link>
                <Link role="menuitem" href="/dashboards/"><span className="app-glyph">G</span><span><strong>GradeThis Operations</strong><small>Default index: gradethis</small></span></Link>
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
                <button aria-label={`Open active search job: ${phaseLabel(phase)}`} role="menuitem" type="button" onClick={() => { setModal("jobs"); setMenu(null); }}><span className={`mini-status ${stateClass(phase)}`} /> <span><strong>{phaseLabel(phase)}</strong><small>{NUMBER_FORMAT.format(visibleEventCount)} results · {elapsed}</small></span></button>
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
            <span aria-hidden="true">⌕</span>
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
        <div className="app-identity"><span aria-hidden="true">⌕</span><strong>Search &amp; Reporting</strong></div>
      </nav>

      {mobileProductNavOpen ? (
        <>
          <button className="search-mobile-backdrop" type="button" aria-label="Close product navigation" onClick={() => setMobileProductNavOpen(false)} />
          <dialog ref={mobileProductDrawerRef} className="search-mobile-drawer" open aria-modal="true" aria-label="Product navigation">
            <header><div><span>A</span><div><strong>Administrator</strong><small>admin@localhost</small></div></div><button type="button" aria-label="Close product navigation" onClick={() => setMobileProductNavOpen(false)}>×</button></header>
            <span className="search-mobile-label">APPLICATION</span>
            <Link href="/"><i>⌂</i>Home</Link><Link className="active" href="/search/"><i>⌕</i>Search &amp; Reporting</Link><Link href="/analytics/"><i>⌁</i>Analytics</Link><Link href="/datasets/"><i>▦</i>Datasets</Link><Link href="/reports/"><i>▤</i>Reports</Link><Link href="/dashboards/"><i>▥</i>Dashboards</Link>
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
            {dirty ? <span className="unsaved-dot" title="The draft differs from the displayed search job">Run to apply changes</span> : null}
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
            <button type="button" onClick={() => openExportDialog()}><span aria-hidden="true">⇩</span> Export</button>
            <button className="close-search" type="button" onClick={closeSearchWorkspace}>Close</button>
            <div className="header-menu-wrap mobile-search-actions">
              <button type="button" aria-haspopup="menu" aria-expanded={menu === "search-actions"} onClick={() => setMenu(menu === "search-actions" ? null : "search-actions")}>More <span aria-hidden="true">▾</span></button>
              {menu === "search-actions" ? <div className="floating-menu mobile-search-menu" role="menu"><button role="menuitem" type="button" onClick={() => { setModal("open"); setMenu(null); }}>⌕ <span>Open saved search</span></button><button role="menuitem" type="button" onClick={() => openSaveDialog(null, true)}>＋ <span>Save as new</span></button><button role="menuitem" type="button" onClick={() => { setModal("history"); setMenu(null); }}>↶ <span>Search history</span></button><button role="menuitem" type="button" onClick={() => { openExportDialog(); setMenu(null); }}>⇩ <span>Export results</span></button><Link role="menuitem" href="/activity/">ⓘ <span>View activity</span></Link><button role="menuitem" type="button" onClick={closeSearchWorkspace}>× <span>Close search</span></button></div> : null}
            </div>
          </div>
        </header>

        <SearchComposer
          absoluteEnd={absoluteEnd}
          absoluteStart={absoluteStart}
          absoluteTimeInvalid={absoluteTimeInvalid}
          completionIndex={completionIndex}
          completionOpen={completionOpen}
          diagnostic={diagnostic}
          draftTimeRange={draftTimeRange}
          editorFocused={editorFocused}
          editorLineCount={editorLineCount}
          editorRef={editorRef}
          filteredCompletions={filteredCompletions}
          gutterLinesRef={gutterLinesRef}
          highlightRef={highlightRef}
          isRunning={isRunning}
          modal={modal}
          query={query}
          relativeAmount={relativeAmount}
          relativeUnit={relativeUnit}
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
            if (dirty) timelineZoomParentRef.current = null;
            runSearch();
          }}
          onSeedAbsoluteRange={seedAbsoluteRange}
          onTimePickerSectionChange={setTimePickerSection}
          onTimeRangeChange={handleManualTimeRangeChange}
        />

        <section className={`job-strip${searchIsClosed ? " is-closed" : ""}`} data-testid="job-strip" aria-label="Search job status" aria-busy={isRunning}>
          <div className="job-primary">
            <span className={`job-state-icon ${stateClass(phase)}`} aria-hidden="true">{phase === "completed" ? "✓" : phase === "failed" ? "!" : phase === "canceled" ? "×" : ""}</span>
            <span className="job-result-copy">
              <output className="sr-only" aria-live="polite" aria-atomic="true">Search status: {phaseLabel(phase)}</output>
              <strong aria-hidden="true">{phaseLabel(phase)}</strong>
              <span>{NUMBER_FORMAT.format(visibleEventCount)} events</span>
              <small data-testid="job-time-range">
                {!backendEnabled && submittedTimeRange.label === "Last 24 hours"
                  ? "7/20/26 3:44:00 PM to 7/21/26 3:44:00 PM"
                  : submittedTimeRange.label}
              </small>
            </span>
            <button className="sampling-button" type="button" onClick={() => showToast("Event sampling is off; all matching events are included.")}>No Event Sampling <span aria-hidden="true">▾</span></button>
          </div>
          <div className="job-metrics" aria-label="Job metrics">
            <span><small>Scanned</small><strong>{NUMBER_FORMAT.format(scannedRows)} rows</strong></span>
            <span><small>Data</small><strong>{scannedBytes}</strong></span>
            <span><small>Elapsed</small><strong>{elapsed}</strong></span>
            <span><small>Progress</small><strong aria-hidden="true">{progress}%</strong><progress className="sr-only" aria-label="Search progress" max={100} value={progress}>{progress}%</progress></span>
          </div>
          <div className="job-controls">
            <button type="button" onClick={() => setModal("jobs")}>Job <span aria-hidden="true">▾</span></button>
            <button type="button" aria-label="Inspect search job" title="Inspect job" onClick={() => setModal("inspect")}>ⓘ</button>
            <button type="button" aria-label="Refresh results" title="Refresh results" onClick={() => runSearch(submittedQuery, submittedTimeRange)}>↻</button>
            <button
              type="button"
              aria-label="Share search"
              title="Share search"
              onClick={() => {
                const url = new URL(window.location.href);
                url.searchParams.set("q", submittedQuery);
                url.searchParams.set("earliest", submittedTimeRange.earliest);
                url.searchParams.set("latest", submittedTimeRange.latest);
                url.searchParams.set("label", submittedTimeRange.label);
                url.searchParams.set("run", "1");
                url.hash = "search";
                void copyText(url.toString(), "Search link copied to the clipboard.");
              }}
            >⌁</button>
            <div className="header-menu-wrap search-mode-wrap">
              <button type="button" aria-haspopup="menu" aria-expanded={menu === "search-mode"} onClick={() => setMenu(menu === "search-mode" ? null : "search-mode")}><span aria-hidden="true">⚡</span> {searchMode} Mode <span aria-hidden="true">▾</span></button>
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
          {isRunning ? <span className="job-progress-bar" aria-hidden="true" style={{ width: `${progress}%` }} /> : null}
        </section>

        <div className={`result-tabs${searchIsClosed ? " is-closed" : ""}`} role="tablist" aria-label="Search result views">
          {([
            ["events", "Events", NUMBER_FORMAT.format(visibleEventCount)],
            ["patterns", "Patterns", hasResultData ? String(patternRows.length) : "0"],
            ["statistics", "Statistics", hasResultData ? String(statisticsRowCount) : "0"],
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
            <button className="button secondary compact" type="button" onClick={() => { timelineZoomParentRef.current = null; runSearch(DEFAULT_QUERY); }}>Run the default search</button>
          </section>
        ) : null}

        {hasResultData && activeTab === "events" ? (
          <EventsPanel
            activeField={activeField}
            backendEnabled={backendEnabled}
            draggingTimeline={draggingTimeline}
            eventDisplay={eventDisplay}
            eventPage={eventPage}
            eventPageCount={eventPageCount}
            eventPageSize={eventPageSize}
            eventSortDirection={eventSortDirection}
            expandedEvents={expandedEvents}
            fieldFilter={fieldFilter}
            fields={fields}
            fieldsCollapsed={fieldsCollapsed}
            menu={menu}
            pagedResultEvents={pagedResultEvents}
            resultEvents={resultEvents}
            showAllFields={showAllFields}
            submittedQuery={submittedQuery}
            timelineDisplay={timelineDisplay}
            timelinePoints={timelinePoints}
            timelineSelection={timelineSelection}
            wrapEvents={wrapEvents}
            applyPivot={applyPivot}
            copyText={copyText}
            endTimelineDrag={endTimelineDrag}
            moveTimelineDrag={moveTimelineDrag}
            setActiveField={setActiveField}
            setEventDisplay={setEventDisplay}
            setEventPage={setEventPage}
            setEventPageSize={setEventPageSize}
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
            isTimechartResult={isTimechartResult}
            menu={menu}
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
            isTimechartResult={isTimechartResult}
            legendPosition={legendPosition}
            showDataLabels={showDataLabels}
            statisticsDimension={statisticsDimension}
            statisticsRows={statisticsRows}
            timelinePoints={timelinePoints}
            onApplyPivot={(field, value, mode) => applyPivot(field, value, mode)}
            onChartStyleChange={setChartStyle}
            onChartTitleChange={setChartTitle}
            onLegendPositionChange={setLegendPosition}
            onShowDataLabelsChange={setShowDataLabels}
            onShowToast={showToast}
          />
        ) : null}
      </main>

      <WorkspaceDialogs
        activeSavedSearchId={activeSavedSearchId}
        activeTab={activeTab}
        displayedExportRows={exportSourceTab === "events" ? resultEvents.length : exportSourceTab === "patterns" ? patternRows.length : isTimechartResult ? timelinePoints.length : genericStatisticsTable?.rows.length ?? statisticsRows.length}
        elapsed={elapsed}
        exportFieldOptions={exportSourceTab === "events" ? ["_time", "level", "logger", "message", "trace_id", "host", "source", "status", "duration_ms", "_raw"] : exportFieldsForTab(exportSourceTab)}
        exportFieldLabels={exportFieldLabels}
        exportFields={exportFields}
        exportFormat={exportFormat}
        exportSourceTab={exportSourceTab}
        exportStage={exportStage}
        history={history}
        historyFilter={historyFilter}
        isRunning={isRunning}
        modal={modal}
        phase={phase}
        saveDescription={saveDescription}
        saveDialogReturnFocus={saveDialogReturnFocusRef.current}
        saveName={saveName}
        savedSearchFilter={savedSearchFilter}
        savedSearches={savedSearches}
        scannedBytes={scannedBytes}
        scannedRows={scannedRows}
        searchId={backendEnabled ? backendJobIdRef.current ?? "Pending dispatch" : `scheduler_admin_search_${generationRef.current || 1}`}
        searchMode={searchMode}
        submittedQuery={submittedQuery}
        submittedTimeRange={submittedTimeRange}
        timeRange={timeRange}
        visibleEventCount={visibleEventCount}
        onCancelSearch={cancelSearch}
        onClearHistory={() => setHistory([])}
        onDeleteHistoryEntry={deleteHistoryEntry}
        onDeleteSavedSearch={deleteSavedSearch}
        onDownloadExport={downloadExport}
        onExportFieldToggle={toggleExportField}
        onExportFormatChange={setExportFormat}
        onHistoryEntryOpen={openHistoryEntry}
        onHistoryFilterChange={setHistoryFilter}
        onModalChange={setModal}
        onOpenSavedSearch={openSavedSearch}
        onPrepareExport={prepareExport}
        onResetExport={() => setExportStage("configure")}
        onSaveDescriptionChange={setSaveDescription}
        onSaveNameChange={setSaveName}
        onSaveSearch={saveSearch}
        onSavedSearchFilterChange={setSavedSearchFilter}
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
