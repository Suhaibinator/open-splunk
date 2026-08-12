/* oxlint-disable jsx-a11y/prefer-tag-over-role */

import {
  type CSSProperties,
  type Dispatch,
  type ReactNode,
  type SetStateAction,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

import { ValueType } from "@/gen/ts/open_splunk/v1/value";
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
import { formatGroupedNumericText } from "../formatters";
import type { MenuName, StatsDensity } from "../model";
import { statsFlatMultivalueDisplay } from "../statistics-multivalue";
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

interface StatisticsTableShellStyle extends CSSProperties {
  "--statistics-header-height": string;
  "--statistics-row-height": string;
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
      role="img"
      aria-label={`Sparkline values: ${description}`}
    >
      {segments.map((segment, index) => segment.length === 1 ? (
        <circle
          // A segment can contain one isolated bucket between missing values.
          key={`point-${index}`}
          cx={segment[0].split(",")[0]}
          cy={segment[0].split(",")[1]}
          r="1.75"
        />
      ) : (
        <polyline key={`line-${index}`} points={segment.join(" ")} />
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
      {/* oxlint-disable-next-line jsx-a11y/control-has-associated-label */}
      <td colSpan={Math.max(1, columnCount)}>
        <span style={{ height }}> </span>
      </td>
    </tr>
  );
}

function visibleRows<Row>(rows: Row[], window: VirtualTableWindow): Row[] {
  return window.virtualized ? rows.slice(window.startIndex, window.endIndex) : rows;
}

export function StatisticsPanel({
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
  const tableShellRef = useRef<HTMLDivElement>(null);
  const hasExplicitTimechartSeries = timelinePoints.some(
    (point) => Object.keys(point.series ?? {}).length > 0,
  );
  const timechartSeries = timechartValueColumns;
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
  const displayedColumnCount = isTimechartResult
    ? timechartSeries.length + 1
    : genericStatisticsTable?.columns.length ?? 4;
  const statisticsRowHeight = statsDensity === "compact"
    ? COMPACT_STATISTICS_ROW_HEIGHT
    : STANDARD_STATISTICS_ROW_HEIGHT;
  const virtualWindow = useMemo(() => calculateVirtualTableWindow({
    columnCount: displayedColumnCount,
    rowCount: displayedRowCount,
    rowHeight: statisticsRowHeight,
    scrollTop: verticalScrollTop,
    viewportHeight: tableViewportHeight,
  }), [
    displayedColumnCount,
    displayedRowCount,
    statisticsRowHeight,
    tableViewportHeight,
    verticalScrollTop,
  ]);
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
    updateViewportHeight();
    const observer = new ResizeObserver(updateViewportHeight);
    observer.observe(shell);
    return () => observer.disconnect();
  }, [virtualWindow.virtualized]);

  useEffect(() => {
    const shell = tableShellRef.current;
    if (shell !== null) shell.scrollTop = 0;
    setVerticalScrollTop(0);
  }, [
    genericStatsSort,
    pageNumber,
    resultIdentity,
    statsDensity,
    statsSort,
    timechartSeriesSort,
    timechartSort,
  ]);

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
          <button className="button secondary compact" type="button" disabled={isPreview} title={isPreview ? "Export becomes available after authoritative results load." : undefined} onClick={onExport}>⇩ Export</button>
          <div className="header-menu-wrap result-menu-wrap">
            <button className="button secondary compact" type="button" aria-haspopup="menu" aria-expanded={menu === "stats-format"} onClick={() => onMenuChange(menu === "stats-format" ? null : "stats-format")}>Format <span aria-hidden="true">▾</span></button>
            {menu === "stats-format" ? (
              <div className="floating-menu result-control-menu" role="menu" aria-label="Statistics table format">
                {(["compact", "standard"] as const).map((density) => (
                  <button role="menuitemradio" aria-checked={statsDensity === density} type="button" key={density} onClick={() => { onStatsDensityChange(density); onMenuChange(null); }}><span className="radio-mark">{statsDensity === density ? "●" : "○"}</span><span><strong>{density === "compact" ? "Compact rows" : "Standard rows"}</strong><small>{density === "compact" ? "Fit more results on screen" : "Add breathing room for scanning"}</small></span></button>
                ))}
              </div>
            ) : null}
          </div>
        </div>
      </header>
      <div className={`statistics-table-frame${hasScrolled ? " has-scrolled" : ""}`}>
        <div
          className={`statistics-table-shell${virtualWindow.virtualized ? " statistics-table-shell--virtualized" : ""}`}
          role="region"
          aria-label="Scrollable statistics table"
          ref={tableShellRef}
          // A focusable named region lets keyboard users scroll a virtualized table.
          // oxlint-disable-next-line jsx-a11y/no-noninteractive-tabindex
          tabIndex={virtualWindow.virtualized ? 0 : undefined}
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
          {isTimechartResult ? (
            <table
              className={`statistics-table statistics-table--fixed timechart-table density-${statsDensity}`}
              style={{
                minWidth: `${Math.max(520, 260 + timechartSeries.length * 150)}px`,
              }}
              aria-label={isPreview ? "Live preview timechart statistics" : "Timechart statistics"}
              aria-rowcount={virtualWindow.virtualized ? displayedRowCount + 1 : undefined}
              data-total-rows={displayedRowCount}
            >
              <colgroup>
                <col style={{ minWidth: 220, width: `${Math.max(35, 70 - timechartSeries.length * 5)}%` }} />
                {timechartSeries.map((series) => <col key={series} style={{ minWidth: 140 }} />)}
              </colgroup>
              <thead>
                <tr>
                  {(() => {
                    const sorted = activeTimechartSeriesSort === null && timechartSort.key === "time";
                    const nextDirection = sorted && timechartSort.direction === "desc" ? "ascending" : "descending";
                    return (
                      <th scope="col" aria-sort={sorted ? (timechartSort.direction === "desc" ? "descending" : "ascending") : "none"}>
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
                      </th>
                    );
                  })()}
                  {timechartSeries.map((seriesName) => {
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
                      </th>
                    );
                  })}
                </tr>
              </thead>
              <tbody>
                <VirtualTableSpacer
                  columnCount={timechartSeries.length + 1}
                  height={virtualWindow.paddingTop}
                />
                {visibleTimechartRows.map((row, visibleIndex) => (
                  <tr
                    key={row.id}
                    aria-rowindex={virtualWindow.virtualized
                      ? virtualWindow.startIndex + visibleIndex + 2
                      : undefined}
                  >
                    <td><time dateTime={row.earliest}>{row.label}</time></td>
                    {timechartSeries.map((seriesName) => {
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
                  columnCount={timechartSeries.length + 1}
                  height={virtualWindow.paddingBottom}
                />
              </tbody>
            </table>
          ) : genericStatisticsTable !== null ? (
            <table
              className={`statistics-table statistics-table--fixed density-${statsDensity}`}
              style={{
                minWidth: `${Math.max(640, genericStatisticsTable.columns.length * 160)}px`,
              }}
              aria-label={isPreview ? "Live preview search statistics" : "Backend search statistics"}
              aria-rowcount={virtualWindow.virtualized ? displayedRowCount + 1 : undefined}
              data-total-rows={displayedRowCount}
            >
              <thead>
                <tr>
                  {genericStatisticsTable.columns.map((column) => {
                    const sorted = genericStatsSort?.key === column.key;
                    const nextDirection = sorted && genericStatsSort.direction === "asc" ? "descending" : "ascending";
                    return (
                      <th
                        scope="col"
                        key={column.key}
                        className={column.numeric ? "numeric-cell" : undefined}
                        aria-sort={sorted ? (genericStatsSort.direction === "desc" ? "descending" : "ascending") : "none"}
                        style={{ minWidth: column.numeric ? 128 : 168 }}
                      >
                        <button style={{ width: "100%" }} type="button" aria-label={`Sort by ${column.label}, ${nextDirection}`} onClick={() => onGenericStatsSortChange(column.key)}>
                          <span>{column.label}</span>
                          <i className={sorted ? "sort-active" : ""} aria-hidden="true">{sorted ? (genericStatsSort.direction === "desc" ? "↓" : "↑") : "↕"}</i>
                        </button>
                      </th>
                    );
                  })}
                </tr>
              </thead>
              <tbody>
                {sortedGenericStatisticsRows.length === 0 ? (
                  <tr><td colSpan={Math.max(1, genericStatisticsTable.columns.length)} style={{ textAlign: "center" }}>No statistics rows were returned.</td></tr>
                ) : (
                  <>
                    <VirtualTableSpacer
                      columnCount={genericStatisticsTable.columns.length}
                      height={virtualWindow.paddingTop}
                    />
                    {visibleGenericStatisticsRows.map((row, visibleIndex) => (
                      <tr
                        key={row.id}
                        aria-rowindex={virtualWindow.virtualized
                          ? virtualWindow.startIndex + visibleIndex + 2
                          : undefined}
                      >
                        {genericStatisticsTable.columns.map((column) => {
                          const value = row.values[column.key] ?? null;
                          const formatted = renderGenericValue(value, column);
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
                                  {formatted}
                                </button>
                              ) : formatted}
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                    <VirtualTableSpacer
                      columnCount={genericStatisticsTable.columns.length}
                      height={virtualWindow.paddingBottom}
                    />
                  </>
                )}
              </tbody>
            </table>
          ) : (
            <table
              className={`statistics-table density-${statsDensity}`}
              aria-label={isPreview ? "Live preview search statistics" : "Search statistics"}
              aria-rowcount={virtualWindow.virtualized ? displayedRowCount + 1 : undefined}
              data-total-rows={displayedRowCount}
            >
              <colgroup><col className="statistics-col-level" /><col className="statistics-col-count" /><col className="statistics-col-percent" /><col className="statistics-col-average" /></colgroup>
              <thead>
                <tr>
                  {([
                    ["level", statisticsDimension, false], ["count", "count", true], ["percent", "% of results", true], ["avgDuration", "avg(duration_ms)", true],
                  ] as const).map(([key, label, numeric]) => {
                    const sorted = statsSort.key === key;
                    const nextDirection = sorted && statsSort.direction === "desc" ? "ascending" : "descending";
                    return (
                      <th scope="col" key={key} className={numeric ? "numeric-cell" : undefined} aria-sort={sorted ? (statsSort.direction === "desc" ? "descending" : "ascending") : "none"}>
                        <button type="button" aria-label={`Sort by ${label}, ${nextDirection}`} onClick={() => onStatsSortChange(key)}><span>{label}</span><i className={sorted ? "sort-active" : ""} aria-hidden="true">{sorted ? (statsSort.direction === "desc" ? "↓" : "↑") : "↕"}</i></button>
                      </th>
                    );
                  })}
                </tr>
              </thead>
              <tbody>
                <VirtualTableSpacer columnCount={4} height={virtualWindow.paddingTop} />
                {visibleStatistics.map((row, visibleIndex) => (
                  <tr
                    key={row.id ?? row.level}
                    aria-rowindex={virtualWindow.virtualized
                      ? virtualWindow.startIndex + visibleIndex + 2
                      : undefined}
                  >
                    <td>{row.pivotable === false ? row.level : <button className="statistics-value-link" type="button" title={`Add ${statisticsDimension}=${row.level} to the draft search`} onClick={() => onApplyPivot(statisticsDimension, row.pivotValue !== undefined ? row.pivotValue : row.level)}><span className={`severity-dot severity-${row.level.toLowerCase()}`} />{row.level}</button>}</td>
                    <td className="numeric-cell">{NUMBER_FORMAT.format(row.count)}</td>
                    <td className="numeric-cell">{row.percent}</td>
                    <td className="numeric-cell">{Number.isFinite(row.avgDuration) ? <>{row.avgDuration.toFixed(1)} <span className="numeric-unit">ms</span></> : "—"}</td>
                  </tr>
                ))}
                <VirtualTableSpacer columnCount={4} height={virtualWindow.paddingBottom} />
              </tbody>
            </table>
          )}
        </div>
        <span className="statistics-scroll-hint" aria-hidden="true">More columns <b>→</b></span>
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
    </section>
  );
}
