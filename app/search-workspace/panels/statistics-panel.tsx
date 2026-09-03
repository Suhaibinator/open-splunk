/* oxlint-disable jsx-a11y/no-noninteractive-tabindex -- The virtualized overflow region must be focusable so keyboard users can scroll it. */
import {
  type CSSProperties,
  type Dispatch,
  type KeyboardEvent,
  type PointerEvent,
  type ReactNode,
  type SetStateAction,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

import { ValueType } from "@/gen/ts/open_splunk/value";
import type { DemoScalar, TimelinePoint } from "@/lib/demo/search-data";
import {
  compareWorkspaceNumericValues,
  type WorkspaceStatistic,
  type WorkspaceStatisticsColumn,
  type WorkspaceStatisticsRow,
  type WorkspaceStatisticsSort,
  type WorkspaceStatisticsTable,
  type WorkspaceStatisticsValue,
} from "@/lib/search/backend-data";

import { NUMBER_FORMAT } from "../constants";
import { AppIcon } from "../../_components/app-icon";
import { Modal } from "../../_components/modal";
import { formatGroupedNumericText } from "../formatters";
import type { MenuName, StatsDensity } from "../model";
import {
  createColumnLayout,
  reconcileColumnLayout,
  resizeColumn,
  type StatisticsColumnDefinition,
  type StatisticsColumnLayout,
  type StatisticsColumnLayoutStore,
  toggleColumn,
  visibleColumns,
  visibleColumnWidth,
} from "./statistics-column-layout";
import {
  StatsFlatMultivalueValue,
  StatsMultivalueList,
  statsFlatMultivalueDisplay,
  statsMultivalueLineMembers,
  statsMultivalueTitle,
  statsMultivalueVisibleMemberCount,
} from "../statistics-multivalue";
import {
  statsSparklineSegments,
  statsSparklineValues,
  statsSparklineValuesForPresentation,
} from "../statistics-sparkline";
import {
  calculateVirtualTableWindow,
  maximumVirtualTableScrollTop,
  VIRTUAL_TABLE_VIEWPORT_HEIGHT,
  type VirtualTableWindow,
} from "../virtual-table";

type StatsSort = { key: keyof WorkspaceStatistic; direction: "asc" | "desc" };
type TimechartSort = { key: "time" | "count"; direction: "asc" | "desc" };
type TimechartSeriesSort = { key: string; direction: "asc" | "desc" };

interface StatisticsPanelProps {
  columnLayoutStore: StatisticsColumnLayoutStore;
  elapsed: string;
  genericStatisticsTable: WorkspaceStatisticsTable | null;
  genericStatsSort: WorkspaceStatisticsSort | null;
  isPreview: boolean;
  isTimechartResult: boolean;
  menu: MenuName | null;
  pageNumber: number;
  pageStart: number | null;
  resultTotalExact: boolean;
  resultTotalRows: number | null;
  previewTruncated: boolean;
  resultIdentity: number;
  sortedGenericStatisticsRows: WorkspaceStatisticsRow[];
  sortedStatistics: WorkspaceStatistic[];
  sortedTimechartRows: TimelinePoint[];
  statisticsDimension: string;
  statisticsRows: WorkspaceStatistic[];
  statsDensity: StatsDensity;
  statsSort: StatsSort;
  submittedQuery: string;
  timechartSort: TimechartSort;
  timechartValueColumns: string[];
  timelinePoints: TimelinePoint[];
  onApplyPivot: (field: string, value: DemoScalar) => void;
  onExport: () => void;
  onGenericStatsSortChange: (key: string) => void;
  onMenuChange: (menu: MenuName | null) => void;
  onStatsDensityChange: (density: StatsDensity) => void;
  onStatsSortChange: (key: keyof WorkspaceStatistic) => void;
  onTimechartSortChange: Dispatch<SetStateAction<TimechartSort>>;
}

const GENERIC_NUMBER_FORMAT = new Intl.NumberFormat("en-US", { maximumFractionDigits: 8 });
const GENERIC_TIMESTAMP_FORMAT = new Intl.DateTimeFormat("en-US", {
  month: "short",
  day: "numeric",
  year: "numeric",
  hour: "numeric",
  minute: "2-digit",
  second: "2-digit",
});
const COMPACT_STATISTICS_ROW_HEIGHT = 42;
const STANDARD_STATISTICS_ROW_HEIGHT = 52;
const STATISTICS_HEADER_HEIGHT = 37;
const STATS_SPARKLINE_WIDTH = 128;
const STATS_SPARKLINE_HEIGHT = 28;

const STATISTICS_COLUMN_SCALE_TOKENS = {
  maximum: "--space-statistics-column-maximum",
  minimum: "--space-statistics-column-minimum",
  numeric: "--space-statistics-column-numeric",
  step: "--space-statistics-column-step",
  text: "--space-statistics-column-text",
  time: "--space-statistics-column-time",
} as const;

interface StatisticsTableShellStyle extends CSSProperties {
  "--statistics-header-height": string;
  "--statistics-row-height": string;
}

interface StatisticsPanelColumn extends StatisticsColumnDefinition {
  label: string;
  numeric: boolean;
}

interface StatisticsColumnScale {
  maximum: number;
  minimum: number;
  numeric: number;
  step: number;
  text: number;
  time: number;
}

function readStatisticsColumnScale(): StatisticsColumnScale | null {
  if (typeof window === "undefined") return null;
  const computedStyle = window.getComputedStyle(document.documentElement);
  const entries = Object.entries(STATISTICS_COLUMN_SCALE_TOKENS).map(([key, token]) => [
    key,
    Number.parseFloat(computedStyle.getPropertyValue(token)),
  ] as const);
  if (entries.some(([, value]) => !Number.isFinite(value) || value <= 0)) return null;
  return Object.fromEntries(entries) as unknown as StatisticsColumnScale;
}

interface StatisticsColumnResizeHandleProps {
  column: StatisticsPanelColumn;
  keyboardStep: number | null;
  layout: StatisticsColumnLayout;
  onResize: (id: string, deltaPx: number) => void;
}

function StatisticsColumnResizeHandle({
  column,
  keyboardStep,
  layout,
  onResize,
}: StatisticsColumnResizeHandleProps) {
  const lastClientX = useRef<number | null>(null);
  const width = layout.find((item) => item.id === column.id)?.width
    ?? column.defaultWidth;

  function endPointerResize(event: PointerEvent<HTMLSpanElement>): void {
    if (event.currentTarget.hasPointerCapture(event.pointerId)) {
      event.currentTarget.releasePointerCapture(event.pointerId);
    }
    lastClientX.current = null;
  }

  function handleKeyboardResize(event: KeyboardEvent<HTMLSpanElement>): void {
    if (
      keyboardStep === null
      || (event.key !== "ArrowLeft" && event.key !== "ArrowRight")
    ) return;
    event.preventDefault();
    onResize(
      column.id,
      event.key === "ArrowRight"
        ? keyboardStep
        : -keyboardStep,
    );
  }

  return (
    <span
      className="statistics-column-resizer"
      role="separator"
      aria-label={`Resize ${column.label} column`}
      aria-orientation="vertical"
      aria-valuemax={column.maximumWidth ?? undefined}
      aria-valuemin={column.minimumWidth ?? undefined}
      aria-valuenow={width ?? undefined}
      tabIndex={0}
      onKeyDown={handleKeyboardResize}
      onPointerCancel={endPointerResize}
      onPointerDown={(event) => {
        event.preventDefault();
        event.currentTarget.setPointerCapture(event.pointerId);
        lastClientX.current = event.clientX;
      }}
      onPointerMove={(event) => {
        if (
          lastClientX.current === null
          || !event.currentTarget.hasPointerCapture(event.pointerId)
        ) return;
        const deltaPx = event.clientX - lastClientX.current;
        if (deltaPx === 0) return;
        lastClientX.current = event.clientX;
        onResize(column.id, deltaPx);
      }}
      onPointerUp={endPointerResize}
    />
  );
}

function serializedGenericValue(value: WorkspaceStatisticsValue): string {
  return typeof value === "object" && value !== null ? JSON.stringify(value) : String(value);
}

function formatGenericValue(value: WorkspaceStatisticsValue, column: WorkspaceStatisticsColumn): string {
  if (value === null) return "—";
  const flatMultivalue = statsFlatMultivalueDisplay(
    value,
    column.flatMultivalueDelimiter,
  );
  if (flatMultivalue !== undefined) return flatMultivalue;
  if (column.valueType === ValueType.VALUE_TYPE_TIMESTAMP) {
    const date = new Date(serializedGenericValue(value));
    if (!Number.isNaN(date.valueOf())) {
      return GENERIC_TIMESTAMP_FORMAT.format(date);
    }
  }
  if (column.numeric) {
    return typeof value === "number" && Number.isFinite(value)
      ? GENERIC_NUMBER_FORMAT.format(value)
      : formatGroupedNumericText(serializedGenericValue(value));
  }
  return serializedGenericValue(value);
}

function StatsSparklineCell({ value }: { value: WorkspaceStatisticsValue }) {
  const values = statsSparklineValues(value);
  if (values === null) return null;
  const segments = statsSparklineSegments(values, STATS_SPARKLINE_WIDTH, STATS_SPARKLINE_HEIGHT);
  if (segments.length === 0) return <span aria-label="Sparkline has no numeric points">—</span>;
  const description = values.map((point) => point === null ? "missing" : String(point)).join(", ");
  return (
    <svg
      className="statistics-sparkline"
      viewBox={`0 0 ${STATS_SPARKLINE_WIDTH} ${STATS_SPARKLINE_HEIGHT}`}
      aria-label={`Sparkline values: ${description}`}
    >
      {segments.map((segment) => segment.length === 1 ? (
        <circle
          // A segment can contain one isolated bucket between missing values.
          key={`point-${segment[0]}`}
          cx={segment[0].split(",")[0]}
          cy={segment[0].split(",")[1]}
          r="1.75"
        />
      ) : (
        <polyline key={`line-${segment.join(" ")}`} points={segment.join(" ")} />
      ))}
    </svg>
  );
}

function renderGenericValue(value: WorkspaceStatisticsValue, column: WorkspaceStatisticsColumn): ReactNode {
  if (statsSparklineValuesForPresentation(value, column.statsSparkline) !== null) {
    return <StatsSparklineCell value={value} />;
  }
  return formatGenericValue(value, column);
}

interface TimechartSeriesCell {
  displayValue: number | string;
  coordinateApproximate: boolean;
}

function timechartSeriesCell(
  point: TimelinePoint,
  seriesName: string,
  hasExplicitSeries: boolean,
): TimechartSeriesCell | null {
  if (!hasExplicitSeries) {
    return {
      displayValue: point.exactCount ?? point.count,
      coordinateApproximate: point.coordinateApproximate === true,
    };
  }
  const coordinate = point.series?.[seriesName];
  if (coordinate === undefined) return null;
  const exact = point.exactSeries?.[seriesName];
  return {
    displayValue: exact ?? coordinate,
    coordinateApproximate: exact !== undefined,
  };
}

function formatTimechartSeriesCell(cell: TimechartSeriesCell): string {
  return typeof cell.displayValue === "string"
    ? formatGroupedNumericText(cell.displayValue)
    : NUMBER_FORMAT.format(cell.displayValue);
}

function VirtualTableSpacer({
  columnCount,
  height,
}: {
  columnCount: number;
  height: number;
}) {
  if (height <= 0) return null;
  return (
    <tr className="virtual-table-spacer" aria-hidden="true">
      {/* The hidden cell exists only to preserve native table geometry. */}
      <td aria-label="Virtual table spacing" colSpan={Math.max(1, columnCount)}>
        <span style={{ height }}> </span>
      </td>
    </tr>
  );
}

function visibleRows<Row>(rows: Row[], window: VirtualTableWindow): Row[] {
  return window.virtualized ? rows.slice(window.startIndex, window.endIndex) : rows;
}

export function StatisticsPanel({
  columnLayoutStore,
  elapsed,
  genericStatisticsTable,
  genericStatsSort,
  isPreview,
  isTimechartResult,
  menu,
  pageNumber,
  pageStart,
  resultTotalExact,
  resultTotalRows,
  previewTruncated,
  resultIdentity,
  sortedGenericStatisticsRows,
  sortedStatistics,
  sortedTimechartRows,
  statisticsDimension,
  statisticsRows,
  statsDensity,
  statsSort,
  submittedQuery,
  timechartSort,
  timechartValueColumns,
  timelinePoints,
  onApplyPivot,
  onExport,
  onGenericStatsSortChange,
  onMenuChange,
  onStatsDensityChange,
  onStatsSortChange,
  onTimechartSortChange,
}: StatisticsPanelProps) {
  const [hasScrolled, setHasScrolled] = useState(false);
  const [verticalScrollTop, setVerticalScrollTop] = useState(0);
  const [tableViewportHeight, setTableViewportHeight] = useState(
    VIRTUAL_TABLE_VIEWPORT_HEIGHT - STATISTICS_HEADER_HEIGHT,
  );
  const [timechartSeriesSort, setTimechartSeriesSort] = useState<TimechartSeriesSort | null>(null);
  const [multivalueDialog, setMultivalueDialog] = useState<{
    label: string;
    fieldName: string;
    members: string[];
  } | null>(null);
  const tableShellRef = useRef<HTMLDivElement>(null);
  const hasExplicitTimechartSeries = timelinePoints.some(
    (point) => Object.keys(point.series ?? {}).length > 0,
  );
  const timechartSeries = timechartValueColumns;
  const [columnScale, setColumnScale] = useState<StatisticsColumnScale | null>(null);
  useEffect(() => {
    const frame = window.requestAnimationFrame(() => setColumnScale(readStatisticsColumnScale()));
    return () => window.cancelAnimationFrame(frame);
  }, []);
  const timechartColumns = useMemo<StatisticsPanelColumn[]>(() => [
    {
      id: "_time",
      label: "_time",
      numeric: false,
      defaultWidth: columnScale?.time ?? null,
      maximumWidth: columnScale?.maximum ?? null,
      minimumWidth: columnScale?.minimum ?? null,
    },
    ...timechartSeries.map((series) => ({
      id: series,
      label: series,
      numeric: true,
      defaultWidth: columnScale?.numeric ?? null,
      maximumWidth: columnScale?.maximum ?? null,
      minimumWidth: columnScale?.minimum ?? null,
    })),
  ], [columnScale, timechartSeries]);
  const genericColumns = useMemo<StatisticsPanelColumn[]>(() => (
    genericStatisticsTable?.columns.map((column) => ({
      id: column.key,
      label: column.label,
      numeric: column.numeric,
      defaultWidth: column.numeric
        ? columnScale?.numeric ?? null
        : columnScale?.text ?? null,
      maximumWidth: columnScale?.maximum ?? null,
      minimumWidth: columnScale?.minimum ?? null,
    })) ?? []
  ), [columnScale, genericStatisticsTable?.columns]);
  const legacyColumns = useMemo<StatisticsPanelColumn[]>(() => [
    {
      id: "level",
      label: statisticsDimension,
      numeric: false,
      defaultWidth: columnScale?.time ?? null,
      maximumWidth: columnScale?.maximum ?? null,
      minimumWidth: columnScale?.minimum ?? null,
    },
    {
      id: "count",
      label: "count",
      numeric: true,
      defaultWidth: columnScale?.numeric ?? null,
      maximumWidth: columnScale?.maximum ?? null,
      minimumWidth: columnScale?.minimum ?? null,
    },
    {
      id: "percent",
      label: "% of results",
      numeric: true,
      defaultWidth: columnScale?.numeric ?? null,
      maximumWidth: columnScale?.maximum ?? null,
      minimumWidth: columnScale?.minimum ?? null,
    },
    {
      id: "avgDuration",
      label: "avg(duration_ms)",
      numeric: true,
      defaultWidth: columnScale?.numeric ?? null,
      maximumWidth: columnScale?.maximum ?? null,
      minimumWidth: columnScale?.minimum ?? null,
    },
  ], [columnScale, statisticsDimension]);
  const panelColumns = isTimechartResult
    ? timechartColumns
    : genericStatisticsTable === null
      ? legacyColumns
      : genericColumns;
  const layoutQueryKey = submittedQuery;
  const [columnLayoutState, setColumnLayoutState] = useState(() => {
    const stored = columnLayoutStore.get(layoutQueryKey);
    return {
      query: layoutQueryKey,
      layout: stored === undefined
        ? createColumnLayout(panelColumns)
        : [...stored],
    };
  });
  const columnLayout = columnLayoutState.query === layoutQueryKey
    ? reconcileColumnLayout(columnLayoutState.layout, panelColumns)
    : reconcileColumnLayout(
        columnLayoutStore.get(layoutQueryKey) ?? createColumnLayout(panelColumns),
        panelColumns,
      );
  const visibleColumnLayout = visibleColumns(columnLayout);
  const visibleColumnIds = new Set(visibleColumnLayout.map((column) => column.id));
  const visiblePanelColumns = panelColumns.filter((column) => visibleColumnIds.has(column.id));
  const visibleGenericColumns = genericStatisticsTable?.columns.filter(
    (column) => visibleColumnIds.has(column.key),
  ) ?? [];
  const tableMinimumWidth = visibleColumnWidth(columnLayout);

  useEffect(() => {
    if (columnScale !== null) columnLayoutStore.set(layoutQueryKey, columnLayout);
  }, [columnLayout, columnLayoutStore, columnScale, layoutQueryKey]);

  function updateColumnLayout(
    transform: (layout: StatisticsColumnLayout) => StatisticsColumnLayout,
  ): void {
    setColumnLayoutState((current) => {
      const currentLayout = current.query === layoutQueryKey
        ? reconcileColumnLayout(current.layout, panelColumns)
        : reconcileColumnLayout(
          columnLayoutStore.get(layoutQueryKey) ?? [],
          panelColumns,
        );
      const layout = [...transform(currentLayout)];
      columnLayoutStore.set(layoutQueryKey, layout);
      return {
        query: layoutQueryKey,
        layout,
      };
    });
  }

  function resizeStatisticsColumn(id: string, deltaPx: number): void {
    updateColumnLayout((layout) => resizeColumn(layout, id, deltaPx));
  }
  const activeTimechartSeriesSort = timechartSeriesSort !== null
    && hasExplicitTimechartSeries
    && timechartSeries.includes(timechartSeriesSort.key)
    ? timechartSeriesSort
    : null;
  const displayedTimechartRows = useMemo(() => activeTimechartSeriesSort === null
    ? timechartSort.key === "time"
      ? sortedTimechartRows
      : timelinePoints.toSorted((left, right) => {
        const leftValue = timechartSeriesCell(left, "count", false);
        const rightValue = timechartSeriesCell(right, "count", false);
        if (leftValue === null) return rightValue === null ? 0 : 1;
        if (rightValue === null) return -1;
        const comparison = compareWorkspaceNumericValues(
          leftValue.displayValue,
          rightValue.displayValue,
        );
        return timechartSort.direction === "desc" ? -comparison : comparison;
      })
    : timelinePoints.toSorted((left, right) => {
      const leftValue = timechartSeriesCell(left, activeTimechartSeriesSort.key, true);
      const rightValue = timechartSeriesCell(right, activeTimechartSeriesSort.key, true);
      if (leftValue === null) return rightValue === null ? 0 : 1;
      if (rightValue === null) return -1;
      const comparison = compareWorkspaceNumericValues(
        leftValue.displayValue,
        rightValue.displayValue,
      );
      return activeTimechartSeriesSort.direction === "desc" ? -comparison : comparison;
    }), [
    activeTimechartSeriesSort,
    sortedTimechartRows,
    timechartSort,
    timelinePoints,
  ]);
  const displayedRowCount = isTimechartResult
    ? timelinePoints.length
    : genericStatisticsTable?.rows.length ?? statisticsRows.length;
  const displayedColumnCount = visibleColumnLayout.length;
  const statisticsRowHeight = statsDensity === "compact"
    ? COMPACT_STATISTICS_ROW_HEIGHT
    : STANDARD_STATISTICS_ROW_HEIGHT;
  const virtualWindow = calculateVirtualTableWindow({
    columnCount: displayedColumnCount,
    rowCount: displayedRowCount,
    rowHeight: statisticsRowHeight,
    scrollTop: verticalScrollTop,
    viewportHeight: tableViewportHeight,
  });
  const visibleTimechartRows = visibleRows(displayedTimechartRows, virtualWindow);
  const visibleGenericStatisticsRows = visibleRows(sortedGenericStatisticsRows, virtualWindow);
  const visibleStatistics = visibleRows(sortedStatistics, virtualWindow);
  const tableShellStyle: StatisticsTableShellStyle = {
    "--statistics-header-height": `${STATISTICS_HEADER_HEIGHT}px`,
    "--statistics-row-height": `${statisticsRowHeight}px`,
    ...(virtualWindow.virtualized ? { maxHeight: VIRTUAL_TABLE_VIEWPORT_HEIGHT } : {}),
  };
  const firstDisplayedRow = displayedRowCount === 0 ? 0 : pageStart;
  const lastDisplayedRow = firstDisplayedRow === null || displayedRowCount === 0
    ? null
    : firstDisplayedRow + displayedRowCount - 1;
  const totalDescription = isPreview
    ? `${NUMBER_FORMAT.format(displayedRowCount)} provisional ${displayedRowCount === 1 ? "row" : "rows"}`
    : resultTotalRows === null
      ? "total unavailable"
      : resultTotalExact
        ? `${NUMBER_FORMAT.format(resultTotalRows)} rows`
        : `at least ${NUMBER_FORMAT.format(resultTotalRows)} rows`;
  const displayedRange = displayedRowCount === 0
    ? "Showing 0 rows"
    : firstDisplayedRow === null || lastDisplayedRow === null
      ? `Server page ${NUMBER_FORMAT.format(pageNumber)} · ${NUMBER_FORMAT.format(displayedRowCount)} rows on this page`
      : `Showing ${NUMBER_FORMAT.format(firstDisplayedRow)}–${NUMBER_FORMAT.format(lastDisplayedRow)}`;

  useEffect(() => {
    const shell = tableShellRef.current;
    if (shell === null || !virtualWindow.virtualized) return;
    const updateViewportHeight = (): void => {
      setTableViewportHeight(Math.max(1, shell.clientHeight - STATISTICS_HEADER_HEIGHT));
    };
    const viewportFrame = window.requestAnimationFrame(updateViewportHeight);
    const observer = new ResizeObserver(updateViewportHeight);
    observer.observe(shell);
    return () => {
      window.cancelAnimationFrame(viewportFrame);
      observer.disconnect();
    };
  }, [virtualWindow.virtualized]);

  const scrollResetKey = JSON.stringify([
    genericStatsSort,
    pageNumber,
    resultIdentity,
    statsDensity,
    statsSort,
    timechartSeriesSort,
    timechartSort,
  ]);
  const [activeScrollResetKey, setActiveScrollResetKey] = useState(scrollResetKey);
  if (activeScrollResetKey !== scrollResetKey) {
    setActiveScrollResetKey(scrollResetKey);
    setVerticalScrollTop(0);
    setMultivalueDialog(null);
  }

  useEffect(() => {
    if (activeScrollResetKey !== scrollResetKey) return;
    const shell = tableShellRef.current;
    if (shell !== null) shell.scrollTop = 0;
  }, [activeScrollResetKey, scrollResetKey]);

  useEffect(() => {
    const maximumScrollTop = maximumVirtualTableScrollTop({
      columnCount: displayedColumnCount,
      rowCount: displayedRowCount,
      rowHeight: statisticsRowHeight,
      viewportHeight: tableViewportHeight,
    });
    setVerticalScrollTop((currentScrollTop) => {
      const nextScrollTop = Math.min(currentScrollTop, maximumScrollTop);
      const shell = tableShellRef.current;
      if (shell !== null && nextScrollTop !== currentScrollTop) {
        shell.scrollTop = nextScrollTop === 0
          ? 0
          : nextScrollTop + STATISTICS_HEADER_HEIGHT;
      }
      return nextScrollTop;
    });
  }, [
    displayedColumnCount,
    displayedRowCount,
    statisticsRowHeight,
    tableViewportHeight,
  ]);

  return (
    <section id="panel-statistics" role="tabpanel" aria-labelledby="tab-statistics" className="statistics-panel">
      <header className="result-view-header">
        <div>
          <div className="result-title-line">
            <h2>Statistics</h2>
            {isPreview ? <span className="preview-context-badge"><i aria-hidden="true" /> Live preview</span> : null}
          </div>
          <p>{isPreview
            ? `${totalDescription} · values and row order may change while the search runs${previewTruncated ? " · preview limit reached" : ""}`
            : `${totalDescription} · completed in ${elapsed}`}</p>
        </div>
        <div>
          <button className="button button--secondary button--compact" type="button" disabled={isPreview} title={isPreview ? "Export becomes available after authoritative results load." : undefined} onClick={onExport}><AppIcon name="download" size="sm" /> Export</button>
          <div className="header-menu-wrap result-menu-wrap">
            <button className="button button--secondary button--compact" type="button" aria-haspopup="menu" aria-expanded={menu === "stats-format"} onClick={() => onMenuChange(menu === "stats-format" ? null : "stats-format")}>Format <AppIcon name="chevron-down" size="xs" /></button>
            {menu === "stats-format" ? (
              <div className="floating-menu result-control-menu" role="menu" aria-label="Statistics table format">
                {(["compact", "standard"] as const).map((density) => (
                  <button role="menuitemradio" aria-checked={statsDensity === density} type="button" key={density} onClick={() => { onStatsDensityChange(density); onMenuChange(null); }}><span className="radio-mark">{statsDensity === density ? "●" : "○"}</span><span><strong>{density === "compact" ? "Compact rows" : "Standard rows"}</strong><small>{density === "compact" ? "Fit more results on screen" : "Add breathing room for scanning"}</small></span></button>
                ))}
              </div>
            ) : null}
          </div>
          <div className="header-menu-wrap result-menu-wrap">
            <button className="button button--secondary button--compact" type="button" aria-haspopup="menu" aria-expanded={menu === "statistics-columns"} disabled={panelColumns.length === 0} onClick={() => onMenuChange(menu === "statistics-columns" ? null : "statistics-columns")}>Columns <AppIcon name="chevron-down" size="xs" /></button>
            {menu === "statistics-columns" ? (
              <div className="floating-menu result-control-menu statistics-columns-menu" role="menu" aria-label="Statistics table columns">
                {panelColumns.map((column) => {
                  const visible = columnLayout.find((item) => item.id === column.id)?.visible ?? true;
                  const finalVisibleColumn = visible && visibleColumnLayout.length === 1;
                  return (
                    <button
                      role="menuitemcheckbox"
                      aria-checked={visible}
                      disabled={finalVisibleColumn}
                      title={finalVisibleColumn ? "At least one statistics column must remain visible." : undefined}
                      type="button"
                      key={column.id}
                      onClick={() => updateColumnLayout((layout) => toggleColumn(layout, column.id))}
                    >
                      <span className="radio-mark">{visible ? "✓" : ""}</span>
                      <span><strong>{column.label}</strong><small>{finalVisibleColumn ? "The final visible column cannot be hidden" : visible ? "Shown in the table" : "Hidden from the table"}</small></span>
                    </button>
                  );
                })}
              </div>
            ) : null}
          </div>
        </div>
      </header>
      <div className={`statistics-table-frame${hasScrolled ? " has-scrolled" : ""}`}>
        <section
          className={`statistics-table-shell${virtualWindow.virtualized ? " statistics-table-shell--virtualized" : ""}`}
          aria-label="Scrollable statistics table"
          tabIndex={virtualWindow.virtualized ? 0 : undefined}
          ref={tableShellRef}
          data-virtualized={virtualWindow.virtualized ? "true" : "false"}
          data-density={statsDensity}
          style={tableShellStyle}
          onScroll={(event) => {
            if (event.currentTarget.scrollLeft > 12) setHasScrolled(true);
            const nextScrollTop = Math.max(
              0,
              event.currentTarget.scrollTop - STATISTICS_HEADER_HEIGHT,
            );
            setVerticalScrollTop((currentScrollTop) => {
              const currentWindow = calculateVirtualTableWindow({
                columnCount: displayedColumnCount,
                rowCount: displayedRowCount,
                rowHeight: statisticsRowHeight,
                scrollTop: currentScrollTop,
                viewportHeight: tableViewportHeight,
              });
              const nextWindow = calculateVirtualTableWindow({
                columnCount: displayedColumnCount,
                rowCount: displayedRowCount,
                rowHeight: statisticsRowHeight,
                scrollTop: nextScrollTop,
                viewportHeight: tableViewportHeight,
              });
              return currentWindow.startIndex === nextWindow.startIndex
                && currentWindow.endIndex === nextWindow.endIndex
                ? currentScrollTop
                : nextScrollTop;
            });
          }}
        >
          {visiblePanelColumns.length === 0 ? (
            <div className="statistics-no-columns" role="status">
              <strong>All columns are hidden</strong>
              <span>Use the Columns menu to show table data.</span>
            </div>
          ) : isTimechartResult ? (
            <table
              className={`statistics-table statistics-table--fixed statistics-table--user-layout timechart-table density-${statsDensity}`}
              width={tableMinimumWidth ?? undefined}
              aria-label={isPreview ? "Live preview timechart statistics" : "Timechart statistics"}
              aria-rowcount={virtualWindow.virtualized ? displayedRowCount + 1 : undefined}
              data-total-rows={displayedRowCount}
            >
              <colgroup>
                {visibleColumnLayout.map((column) => <col key={column.id} width={column.width ?? undefined} />)}
              </colgroup>
              <thead>
                <tr>
                  {visiblePanelColumns.map((column) => {
                    if (column.id === "_time") {
                      const sorted = activeTimechartSeriesSort === null && timechartSort.key === "time";
                      const nextDirection = sorted && timechartSort.direction === "desc" ? "ascending" : "descending";
                      return (
                        <th scope="col" aria-sort={sorted ? (timechartSort.direction === "desc" ? "descending" : "ascending") : "none"} key={column.id}>
                          <button
                            type="button"
                            aria-label={`Sort by _time, ${nextDirection}`}
                            onClick={() => {
                              setTimechartSeriesSort(null);
                              onTimechartSortChange((current) => ({ key: "time", direction: current.key === "time" && current.direction === "desc" ? "asc" : "desc" }));
                            }}
                          >
                            <span>_time</span>
                            <i className={sorted ? "sort-active" : ""} aria-hidden="true">{sorted ? (timechartSort.direction === "desc" ? "↓" : "↑") : "↕"}</i>
                          </button>
                          <StatisticsColumnResizeHandle column={column} keyboardStep={columnScale?.step ?? null} layout={columnLayout} onResize={resizeStatisticsColumn} />
                        </th>
                      );
                    }
                    const seriesName = column.id;
                    const sorted = hasExplicitTimechartSeries
                      ? activeTimechartSeriesSort?.key === seriesName
                      : activeTimechartSeriesSort === null && timechartSort.key === "count";
                    const direction = hasExplicitTimechartSeries ? activeTimechartSeriesSort?.direction : timechartSort.direction;
                    const nextDirection = sorted && direction === "desc" ? "ascending" : "descending";
                    return (
                      <th className="numeric-cell" scope="col" aria-sort={sorted ? (direction === "desc" ? "descending" : "ascending") : "none"} key={seriesName}>
                        <button
                          type="button"
                          aria-label={`Sort by ${seriesName}, ${nextDirection}`}
                          onClick={() => {
                            if (hasExplicitTimechartSeries) {
                              setTimechartSeriesSort((current) => ({
                                key: seriesName,
                                direction: current?.key === seriesName && current.direction === "desc" ? "asc" : "desc",
                              }));
                            } else {
                              setTimechartSeriesSort(null);
                              onTimechartSortChange((current) => ({ key: "count", direction: current.key === "count" && current.direction === "desc" ? "asc" : "desc" }));
                            }
                          }}
                        >
                          <span>{seriesName}</span>
                          <i className={sorted ? "sort-active" : ""} aria-hidden="true">{sorted ? (direction === "desc" ? "↓" : "↑") : "↕"}</i>
                        </button>
                        <StatisticsColumnResizeHandle column={column} keyboardStep={columnScale?.step ?? null} layout={columnLayout} onResize={resizeStatisticsColumn} />
                      </th>
                    );
                  })}
                </tr>
              </thead>
              <tbody>
                <VirtualTableSpacer
                  columnCount={displayedColumnCount}
                  height={virtualWindow.paddingTop}
                />
                {visibleTimechartRows.map((row, visibleIndex) => (
                  <tr
                    key={row.id}
                    aria-rowindex={virtualWindow.virtualized
                      ? virtualWindow.startIndex + visibleIndex + 2
                      : undefined}
                  >
                    {visiblePanelColumns.map((column) => {
                      if (column.id === "_time") {
                        return <td key={column.id}><time dateTime={row.earliest}>{row.label}</time></td>;
                      }
                      const seriesName = column.id;
                      const cell = timechartSeriesCell(row, seriesName, hasExplicitTimechartSeries);
                      return (
                        <td
                          className="numeric-cell"
                          key={seriesName}
                          title={cell?.coordinateApproximate ? "Exact server value; the chart coordinate is approximate." : undefined}
                        >
                          {cell === null ? "—" : (
                            <>
                              {formatTimechartSeriesCell(cell)}
                              {cell.coordinateApproximate ? (
                                <>
                                  <span className="numeric-unit" aria-hidden="true"> ≈ chart</span>
                                  <span className="sr-only">; chart coordinate approximate</span>
                                </>
                              ) : null}
                            </>
                          )}
                        </td>
                      );
                    })}
                  </tr>
                ))}
                <VirtualTableSpacer
                  columnCount={displayedColumnCount}
                  height={virtualWindow.paddingBottom}
                />
              </tbody>
            </table>
          ) : genericStatisticsTable !== null ? (
            <table
              className={`statistics-table statistics-table--fixed statistics-table--user-layout density-${statsDensity}`}
              width={tableMinimumWidth ?? undefined}
              aria-label={isPreview ? "Live preview search statistics" : "Backend search statistics"}
              aria-rowcount={virtualWindow.virtualized ? displayedRowCount + 1 : undefined}
              data-total-rows={displayedRowCount}
            >
              <colgroup>
                {visibleColumnLayout.map((column) => <col key={column.id} width={column.width ?? undefined} />)}
              </colgroup>
              <thead>
                <tr>
                  {visibleGenericColumns.map((column) => {
                    const panelColumn = genericColumns.find((candidate) => candidate.id === column.key);
                    if (panelColumn === undefined) return null;
                    const sorted = genericStatsSort?.key === column.key;
                    const nextDirection = sorted && genericStatsSort.direction === "asc" ? "descending" : "ascending";
                    return (
                      <th
                        scope="col"
                        key={column.key}
                        className={column.numeric ? "numeric-cell" : undefined}
                        aria-sort={sorted ? (genericStatsSort.direction === "desc" ? "descending" : "ascending") : "none"}
                      >
                        <button type="button" aria-label={`Sort by ${column.label}, ${nextDirection}`} onClick={() => onGenericStatsSortChange(column.key)}>
                          <span>{column.label}</span>
                          <i className={sorted ? "sort-active" : ""} aria-hidden="true">{sorted ? (genericStatsSort.direction === "desc" ? "↓" : "↑") : "↕"}</i>
                        </button>
                        <StatisticsColumnResizeHandle column={panelColumn} keyboardStep={columnScale?.step ?? null} layout={columnLayout} onResize={resizeStatisticsColumn} />
                      </th>
                    );
                  })}
                </tr>
              </thead>
              <tbody>
                {sortedGenericStatisticsRows.length === 0 ? (
                  <tr><td className="statistics-table-empty" colSpan={Math.max(1, displayedColumnCount)}>No statistics rows were returned.</td></tr>
                ) : (
                  <>
                    <VirtualTableSpacer
                      columnCount={displayedColumnCount}
                      height={virtualWindow.paddingTop}
                    />
                    {visibleGenericStatisticsRows.map((row, visibleIndex) => (
                      <tr
                        key={row.id}
                        aria-rowindex={virtualWindow.virtualized
                          ? virtualWindow.startIndex + visibleIndex + 2
                          : undefined}
                      >
                        {visibleGenericColumns.map((column) => {
                          const value = row.values[column.key] ?? null;
                          // An invisible delimiter stacks its members instead of
                          // joining them; LIST columns are never pivotable, so
                          // this branch owns the whole cell.
                          const members = statsMultivalueLineMembers(
                            value,
                            column.flatMultivalueDelimiter,
                          );
                          if (members !== undefined) {
                            return (
                              <td
                                className={column.numeric ? "numeric-cell" : undefined}
                                key={column.key}
                                title={statsMultivalueTitle(members)}
                                style={{ maxWidth: 420, overflow: "hidden" }}
                              >
                                <StatsMultivalueList
                                  fieldName={column.fieldName}
                                  members={members}
                                  visibleMemberCount={statsMultivalueVisibleMemberCount(
                                    statsDensity,
                                    members.length,
                                  )}
                                  onShowAll={() => setMultivalueDialog({
                                    label: column.label,
                                    fieldName: column.fieldName,
                                    members,
                                  })}
                                />
                              </td>
                            );
                          }
                          const formatted = renderGenericValue(value, column);
                          const rendered = (
                            <StatsFlatMultivalueValue
                              delimiter={column.flatMultivalueDelimiter}
                              value={formatted}
                            />
                          );
                          const pivotValue = row.pivotValues[column.key];
                          return (
                            <td
                              className={column.numeric ? "numeric-cell" : undefined}
                              key={column.key}
                              title={value === null ? "Null" : serializedGenericValue(value)}
                              style={{
                                maxWidth: 420,
                                overflow: "hidden",
                                textOverflow: "ellipsis",
                                whiteSpace: "nowrap",
                              }}
                            >
                              {column.pivotable && pivotValue !== undefined ? (
                                <button
                                  className="statistics-value-link"
                                  type="button"
                                  title={`Add ${column.fieldName}=${serializedGenericValue(value)} to the draft search`}
                                  onClick={() => onApplyPivot(column.fieldName, pivotValue)}
                                >
                                  {rendered}
                                </button>
                              ) : rendered}
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                    <VirtualTableSpacer
                      columnCount={displayedColumnCount}
                      height={virtualWindow.paddingBottom}
                    />
                  </>
                )}
              </tbody>
            </table>
          ) : (
            <table
              className={`statistics-table statistics-table--user-layout density-${statsDensity}`}
              width={tableMinimumWidth ?? undefined}
              aria-label={isPreview ? "Live preview search statistics" : "Search statistics"}
              aria-rowcount={virtualWindow.virtualized ? displayedRowCount + 1 : undefined}
              data-total-rows={displayedRowCount}
            >
              <colgroup>
                {visibleColumnLayout.map((column) => <col key={column.id} width={column.width ?? undefined} />)}
              </colgroup>
              <thead>
                <tr>
                  {visiblePanelColumns.map((column) => {
                    const key = column.id as keyof WorkspaceStatistic;
                    const sorted = statsSort.key === key;
                    const nextDirection = sorted && statsSort.direction === "desc" ? "ascending" : "descending";
                    return (
                      <th scope="col" key={key} className={column.numeric ? "numeric-cell" : undefined} aria-sort={sorted ? (statsSort.direction === "desc" ? "descending" : "ascending") : "none"}>
                        <button type="button" aria-label={`Sort by ${column.label}, ${nextDirection}`} onClick={() => onStatsSortChange(key)}><span>{column.label}</span><i className={sorted ? "sort-active" : ""} aria-hidden="true">{sorted ? (statsSort.direction === "desc" ? "↓" : "↑") : "↕"}</i></button>
                        <StatisticsColumnResizeHandle column={column} keyboardStep={columnScale?.step ?? null} layout={columnLayout} onResize={resizeStatisticsColumn} />
                      </th>
                    );
                  })}
                </tr>
              </thead>
              <tbody>
                <VirtualTableSpacer columnCount={displayedColumnCount} height={virtualWindow.paddingTop} />
                {visibleStatistics.map((row, visibleIndex) => (
                  <tr
                    key={row.id ?? row.level}
                    aria-rowindex={virtualWindow.virtualized
                      ? virtualWindow.startIndex + visibleIndex + 2
                      : undefined}
                  >
                    {visiblePanelColumns.map((column) => {
                      if (column.id === "level") {
                        return <td key={column.id}>{row.pivotable === false ? row.level : <button className="statistics-value-link" type="button" title={`Add ${statisticsDimension}=${row.level} to the draft search`} onClick={() => onApplyPivot(statisticsDimension, row.pivotValue !== undefined ? row.pivotValue : row.level)}><span className={`severity-dot severity-${row.level.toLowerCase()}`} />{row.level}</button>}</td>;
                      }
                      if (column.id === "count") {
                        return <td className="numeric-cell" key={column.id}>{NUMBER_FORMAT.format(row.count)}</td>;
                      }
                      if (column.id === "percent") {
                        return <td className="numeric-cell" key={column.id}>{row.percent}</td>;
                      }
                      return <td className="numeric-cell" key={column.id}>{Number.isFinite(row.avgDuration) ? <>{row.avgDuration.toFixed(1)} <span className="numeric-unit">ms</span></> : "—"}</td>;
                    })}
                  </tr>
                ))}
                <VirtualTableSpacer columnCount={displayedColumnCount} height={virtualWindow.paddingBottom} />
              </tbody>
            </table>
          )}
        </section>
        <span className="statistics-scroll-hint" aria-hidden="true">More columns <b><AppIcon name="chevron-right" size="sm" /></b></span>
      </div>
      <footer className={`statistics-footer${isPreview ? " statistics-footer--preview" : ""}`}>{isPreview
        ? <>
          <span>Showing {NUMBER_FORMAT.format(displayedRowCount)} provisional {displayedRowCount === 1 ? "row" : "rows"}</span>
          <span>{previewTruncated ? "Preview limit reached · final results may contain additional rows" : "Live rows refresh as the search produces results · final totals arrive on completion"}</span>
        </>
        : isTimechartResult
        ? <><span>{displayedRange} · {totalDescription}</span><span>Sorted by {activeTimechartSeriesSort?.key ?? (timechartSort.key === "time" ? "_time" : "count")} · {(activeTimechartSeriesSort?.direction ?? timechartSort.direction) === "desc" ? "descending" : "ascending"}</span></>
        : genericStatisticsTable !== null
          ? <><span>{displayedRange} · {totalDescription}</span><span>{genericStatsSort === null ? "Server-provided row order" : `Sorted by ${genericStatisticsTable.columns.find((column) => column.key === genericStatsSort.key)?.label ?? genericStatsSort.key} · ${genericStatsSort.direction === "desc" ? "descending" : "ascending"}`} · values retain server types</span></>
          : <><span>{displayedRange} · {totalDescription}</span><span>Sorted by {statsSort.key === "avgDuration" ? "avg(duration_ms)" : statsSort.key === "level" ? statisticsDimension : statsSort.key} · {statsSort.direction === "desc" ? "descending" : "ascending"}</span></>}
      </footer>
      {multivalueDialog !== null ? (
        <Modal
          title={multivalueDialog.label}
          subtitle={`${NUMBER_FORMAT.format(multivalueDialog.members.length)} values`}
          onClose={() => setMultivalueDialog(null)}
        >
          <ul className="statistics-multivalue-dialog-list">
            {multivalueDialog.members.map((member, index) => (
              <li key={`${index.toString()}-${member}`}>{member}</li>
            ))}
          </ul>
        </Modal>
      ) : null}
    </section>
  );
}
