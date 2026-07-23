/* oxlint-disable jsx-a11y/prefer-tag-over-role */

import { type Dispatch, type SetStateAction, useState } from "react";

import { ValueType } from "@/gen/ts/open_splunk/v1/value";
import type { DemoScalar, TimelinePoint } from "@/lib/demo/search-data";
import {
  compareWorkspaceNumericValues,
  timechartValueFields,
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

type StatsSort = { key: keyof WorkspaceStatistic; direction: "asc" | "desc" };
type TimechartSort = { key: "time" | "count"; direction: "asc" | "desc" };
type TimechartSeriesSort = { key: string; direction: "asc" | "desc" };

interface StatisticsPanelProps {
  elapsed: string;
  genericStatisticsTable: WorkspaceStatisticsTable | null;
  genericStatsSort: WorkspaceStatisticsSort | null;
  isTimechartResult: boolean;
  menu: MenuName | null;
  pageNumber: number;
  pageStart: number | null;
  resultTotalExact: boolean;
  resultTotalRows: number | null;
  sortedGenericStatisticsRows: WorkspaceStatisticsRow[];
  sortedStatistics: WorkspaceStatistic[];
  sortedTimechartRows: TimelinePoint[];
  statisticsDimension: string;
  statisticsRows: WorkspaceStatistic[];
  statsDensity: StatsDensity;
  statsSort: StatsSort;
  timechartSort: TimechartSort;
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

function serializedGenericValue(value: WorkspaceStatisticsValue): string {
  return typeof value === "object" && value !== null ? JSON.stringify(value) : String(value);
}

function formatGenericValue(value: WorkspaceStatisticsValue, column: WorkspaceStatisticsColumn): string {
  if (value === null) return "—";
  if (column.valueType === ValueType.VALUE_TYPE_TIMESTAMP) {
    const date = new Date(serializedGenericValue(value));
    if (!Number.isNaN(date.valueOf())) {
      return new Intl.DateTimeFormat("en-US", {
        month: "short",
        day: "numeric",
        year: "numeric",
        hour: "numeric",
        minute: "2-digit",
        second: "2-digit",
      }).format(date);
    }
  }
  if (column.numeric) {
    return typeof value === "number" && Number.isFinite(value)
      ? GENERIC_NUMBER_FORMAT.format(value)
      : formatGroupedNumericText(serializedGenericValue(value));
  }
  return serializedGenericValue(value);
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

export function StatisticsPanel({
  elapsed,
  genericStatisticsTable,
  genericStatsSort,
  isTimechartResult,
  menu,
  pageNumber,
  pageStart,
  resultTotalExact,
  resultTotalRows,
  sortedGenericStatisticsRows,
  sortedStatistics,
  sortedTimechartRows,
  statisticsDimension,
  statisticsRows,
  statsDensity,
  statsSort,
  timechartSort,
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
  const [timechartSeriesSort, setTimechartSeriesSort] = useState<TimechartSeriesSort | null>(null);
  const explicitTimechartSeries = timechartValueFields(timelinePoints).filter((field) =>
    timelinePoints.some((point) => Object.hasOwn(point.series ?? {}, field)),
  );
  const hasExplicitTimechartSeries = explicitTimechartSeries.length > 0;
  const timechartSeries = hasExplicitTimechartSeries ? explicitTimechartSeries : ["count"];
  const activeTimechartSeriesSort = timechartSeriesSort !== null
    && explicitTimechartSeries.includes(timechartSeriesSort.key)
    ? timechartSeriesSort
    : null;
  const displayedTimechartRows = activeTimechartSeriesSort === null
    ? timechartSort.key === "time"
      ? sortedTimechartRows
      : timelinePoints.toSorted((left, right) => {
        const leftValue = timechartSeriesCell(left, "count", false);
        const rightValue = timechartSeriesCell(right, "count", false);
        if (leftValue === null) return rightValue === null ? 0 : 1;
        if (rightValue === null) return -1;
        const comparison = compareWorkspaceNumericValues(leftValue.displayValue, rightValue.displayValue);
        return timechartSort.direction === "desc" ? -comparison : comparison;
      })
    : timelinePoints.toSorted((left, right) => {
      const leftValue = timechartSeriesCell(left, activeTimechartSeriesSort.key, true);
      const rightValue = timechartSeriesCell(right, activeTimechartSeriesSort.key, true);
      if (leftValue === null) return rightValue === null ? 0 : 1;
      if (rightValue === null) return -1;
      const comparison = compareWorkspaceNumericValues(leftValue.displayValue, rightValue.displayValue);
      return activeTimechartSeriesSort.direction === "desc" ? -comparison : comparison;
    });
  const displayedRowCount = isTimechartResult
    ? timelinePoints.length
    : genericStatisticsTable?.rows.length ?? statisticsRows.length;
  const firstDisplayedRow = displayedRowCount === 0 ? 0 : pageStart;
  const lastDisplayedRow = firstDisplayedRow === null || displayedRowCount === 0
    ? null
    : firstDisplayedRow + displayedRowCount - 1;
  const totalDescription = resultTotalRows === null
    ? "total unavailable"
    : resultTotalExact
      ? `${NUMBER_FORMAT.format(resultTotalRows)} rows`
      : `at least ${NUMBER_FORMAT.format(resultTotalRows)} rows`;
  const displayedRange = displayedRowCount === 0
    ? "Showing 0 rows"
    : firstDisplayedRow === null || lastDisplayedRow === null
      ? `Server page ${NUMBER_FORMAT.format(pageNumber)} · ${NUMBER_FORMAT.format(displayedRowCount)} rows on this page`
      : `Showing ${NUMBER_FORMAT.format(firstDisplayedRow)}–${NUMBER_FORMAT.format(lastDisplayedRow)}`;

  return (
    <section id="panel-statistics" role="tabpanel" aria-labelledby="tab-statistics" className="statistics-panel">
      <header className="result-view-header">
        <div><h2>Statistics</h2><p>{totalDescription} · completed in {elapsed}</p></div>
        <div>
          <button className="button secondary compact" type="button" onClick={onExport}>⇩ Export</button>
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
        <div className="statistics-table-shell" role="region" aria-label="Scrollable statistics table" onScroll={(event) => { if (event.currentTarget.scrollLeft > 12) setHasScrolled(true); }}>
          {isTimechartResult ? (
            <table
              className={`statistics-table timechart-table density-${statsDensity}`}
              style={{ minWidth: `${Math.max(520, 260 + timechartSeries.length * 150)}px`, tableLayout: "auto" }}
              aria-label="Timechart statistics"
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
                {displayedTimechartRows.map((row) => (
                  <tr key={row.id}>
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
              </tbody>
            </table>
          ) : genericStatisticsTable !== null ? (
            <table
              className={`statistics-table density-${statsDensity}`}
              style={{ minWidth: `${Math.max(640, genericStatisticsTable.columns.length * 160)}px`, tableLayout: "auto" }}
              aria-label="Backend search statistics"
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
                ) : sortedGenericStatisticsRows.map((row) => (
                  <tr key={row.id}>
                    {genericStatisticsTable.columns.map((column) => {
                      const value = row.values[column.key] ?? null;
                      const formatted = formatGenericValue(value, column);
                      const pivotValue = row.pivotValues[column.key];
                      return (
                        <td
                          className={column.numeric ? "numeric-cell" : undefined}
                          key={column.key}
                          title={value === null ? "Null" : serializedGenericValue(value)}
                          style={{ maxWidth: 420, overflowWrap: "anywhere", whiteSpace: column.numeric ? "nowrap" : undefined }}
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
              </tbody>
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
                      <th scope="col" key={key} className={numeric ? "numeric-cell" : undefined} aria-sort={sorted ? (statsSort.direction === "desc" ? "descending" : "ascending") : "none"}>
                        <button type="button" aria-label={`Sort by ${label}, ${nextDirection}`} onClick={() => onStatsSortChange(key)}><span>{label}</span><i className={sorted ? "sort-active" : ""} aria-hidden="true">{sorted ? (statsSort.direction === "desc" ? "↓" : "↑") : "↕"}</i></button>
                      </th>
                    );
                  })}
                </tr>
              </thead>
              <tbody>
                {sortedStatistics.map((row) => (
                  <tr key={row.id ?? row.level}>
                    <td>{row.pivotable === false ? row.level : <button className="statistics-value-link" type="button" title={`Add ${statisticsDimension}=${row.level} to the draft search`} onClick={() => onApplyPivot(statisticsDimension, row.pivotValue !== undefined ? row.pivotValue : row.level)}><span className={`severity-dot severity-${row.level.toLowerCase()}`} />{row.level}</button>}</td>
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
      <footer className="statistics-footer">{isTimechartResult
        ? <><span>{displayedRange} · {totalDescription}</span><span>Sorted by {activeTimechartSeriesSort?.key ?? (timechartSort.key === "time" ? "_time" : "count")} · {(activeTimechartSeriesSort?.direction ?? timechartSort.direction) === "desc" ? "descending" : "ascending"}</span></>
        : genericStatisticsTable !== null
          ? <><span>{displayedRange} · {totalDescription}</span><span>{genericStatsSort === null ? "Server-provided row order" : `Sorted by ${genericStatisticsTable.columns.find((column) => column.key === genericStatsSort.key)?.label ?? genericStatsSort.key} · ${genericStatsSort.direction === "desc" ? "descending" : "ascending"}`} · values retain server types</span></>
          : <><span>{displayedRange} · {totalDescription}</span><span>Sorted by {statsSort.key === "avgDuration" ? "avg(duration_ms)" : statsSort.key === "level" ? statisticsDimension : statsSort.key} · {statsSort.direction === "desc" ? "descending" : "ascending"}</span></>}
      </footer>
    </section>
  );
}
