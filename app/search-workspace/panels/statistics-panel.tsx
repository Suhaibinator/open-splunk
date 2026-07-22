/* oxlint-disable jsx-a11y/prefer-tag-over-role */

import { type Dispatch, type SetStateAction, useState } from "react";

import type { TimelinePoint } from "@/lib/demo/search-data";
import type { WorkspaceStatistic } from "@/lib/search/backend-data";

import { NUMBER_FORMAT } from "../constants";
import type { MenuName, StatsDensity } from "../model";

type StatsSort = { key: keyof WorkspaceStatistic; direction: "asc" | "desc" };
type TimechartSort = { key: "time" | "count"; direction: "asc" | "desc" };

interface StatisticsPanelProps {
  elapsed: string;
  isTimechartResult: boolean;
  menu: MenuName | null;
  sortedStatistics: WorkspaceStatistic[];
  sortedTimechartRows: TimelinePoint[];
  statisticsDimension: string;
  statisticsRows: WorkspaceStatistic[];
  statsDensity: StatsDensity;
  statsSort: StatsSort;
  timechartSort: TimechartSort;
  timelinePoints: TimelinePoint[];
  onApplyPivot: (field: string, value: string) => void;
  onExport: () => void;
  onMenuChange: (menu: MenuName | null) => void;
  onStatsDensityChange: (density: StatsDensity) => void;
  onStatsSortChange: (key: keyof WorkspaceStatistic) => void;
  onTimechartSortChange: Dispatch<SetStateAction<TimechartSort>>;
}

export function StatisticsPanel({
  elapsed,
  isTimechartResult,
  menu,
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
  onMenuChange,
  onStatsDensityChange,
  onStatsSortChange,
  onTimechartSortChange,
}: StatisticsPanelProps) {
  const [hasScrolled, setHasScrolled] = useState(false);

  return (
    <section id="panel-statistics" role="tabpanel" aria-labelledby="tab-statistics" className="statistics-panel">
      <header className="result-view-header">
        <div><h2>Statistics</h2><p>{isTimechartResult ? timelinePoints.length : statisticsRows.length} rows · completed in {elapsed}</p></div>
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
            <table className={`statistics-table timechart-table density-${statsDensity}`} aria-label="Timechart statistics">
              <colgroup><col className="timechart-col-time" /><col className="timechart-col-count" /></colgroup>
              <thead><tr>{([[
                "time", "_time", false], ["count", "count", true],
              ] as const).map(([key, label, numeric]) => {
                const sorted = timechartSort.key === key;
                const nextDirection = sorted && timechartSort.direction === "desc" ? "ascending" : "descending";
                return <th className={numeric ? "numeric-cell" : undefined} scope="col" aria-sort={sorted ? (timechartSort.direction === "desc" ? "descending" : "ascending") : "none"} key={key}><button type="button" aria-label={`Sort by ${label}, ${nextDirection}`} onClick={() => onTimechartSortChange((current) => ({ key, direction: current.key === key && current.direction === "desc" ? "asc" : "desc" }))}><span>{label}</span><i className={sorted ? "sort-active" : ""} aria-hidden="true">{sorted ? (timechartSort.direction === "desc" ? "↓" : "↑") : "↕"}</i></button></th>;
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
                      <th scope="col" key={key} className={numeric ? "numeric-cell" : undefined} aria-sort={sorted ? (statsSort.direction === "desc" ? "descending" : "ascending") : "none"}>
                        <button type="button" aria-label={`Sort by ${label}, ${nextDirection}`} onClick={() => onStatsSortChange(key)}><span>{label}</span><i className={sorted ? "sort-active" : ""} aria-hidden="true">{sorted ? (statsSort.direction === "desc" ? "↓" : "↑") : "↕"}</i></button>
                      </th>
                    );
                  })}
                </tr>
              </thead>
              <tbody>
                {sortedStatistics.map((row) => (
                  <tr key={row.level}>
                    <td><button className="statistics-value-link" type="button" title={`Add ${statisticsDimension}=${row.level} to the draft search`} onClick={() => onApplyPivot(statisticsDimension, row.level)}><span className={`severity-dot severity-${row.level.toLowerCase()}`} />{row.level}</button></td>
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
  );
}
