import type { CSSProperties } from "react";

import type { TimelinePoint } from "@/lib/demo/search-data";
import type { WorkspaceStatistic } from "@/lib/search/backend-data";
import type { PivotMode } from "@/lib/search/query-pivots";

import { COMPACT_NUMBER_FORMAT, NUMBER_FORMAT } from "../constants";
import type { ChartStyle, LegendPosition } from "../model";
import {
  TIME_SERIES_COLORS,
  TimeSeriesLineChart,
  timelineSeriesDisplayName,
  timelineSeriesNames,
} from "../charts/time-series-line-chart";

import styles from "./visualization-panel.module.css";

interface VisualizationPanelProps {
  chartStyle: ChartStyle;
  chartTitle: string;
  isTimechartResult: boolean;
  legendPosition: LegendPosition;
  showDataLabels: boolean;
  statisticsDimension: string;
  statisticsRows: WorkspaceStatistic[];
  timelinePoints: TimelinePoint[];
  onApplyPivot: (fieldName: string, fieldValue: string, mode: PivotMode) => void;
  onChartStyleChange: (style: ChartStyle) => void;
  onChartTitleChange: (title: string) => void;
  onLegendPositionChange: (position: LegendPosition) => void;
  onShowDataLabelsChange: (show: boolean) => void;
  onShowToast: (message: string) => void;
}

function timeAxisLabels(points: TimelinePoint[]): TimelinePoint[] {
  if (points.length <= 5) return points;
  return Array.from(new Set([0, 0.25, 0.5, 0.75, 1].map((ratio) => Math.round(ratio * (points.length - 1)))))
    .map((index) => points[index]);
}

const CATEGORY_COLORS = ["#5f9f3a", "#2f7fa6", "#e49a2c", "#8b67a8", "#c6534c", "#4d9a8a"] as const;

function categoryColor(category: string, index: number): string {
  const semanticColor = {
    info: "#5f9c3a",
    warn: "#dda229",
    error: "#c84f48",
    debug: "#5290b0",
  }[category.toLowerCase()];
  return semanticColor ?? CATEGORY_COLORS[index % CATEGORY_COLORS.length];
}

export function VisualizationPanel({
  chartStyle,
  chartTitle,
  isTimechartResult,
  legendPosition,
  showDataLabels,
  statisticsDimension,
  statisticsRows,
  timelinePoints,
  onApplyPivot,
  onChartStyleChange,
  onChartTitleChange,
  onLegendPositionChange,
  onShowDataLabelsChange,
  onShowToast,
}: VisualizationPanelProps) {
  const displayedStatisticsRows = statisticsRows.length > 8
    ? statisticsRows.toSorted((left, right) => right.count - left.count).slice(0, 8)
    : statisticsRows;
  const maxTimelineCount = Math.max(1, ...timelinePoints.map((point) => point.count));
  const maxStatisticsCount = Math.max(1, ...displayedStatisticsRows.map((row) => row.count));
  const chartAxisMaximum = isTimechartResult ? maxTimelineCount : maxStatisticsCount;
  const timelineAxisLabels = timeAxisLabels(timelinePoints);
  const timelineSeries = timelineSeriesNames(timelinePoints);
  const isLineChart = chartStyle === "line" && isTimechartResult;
  const hasCategoricalChart = isTimechartResult || statisticsRows.length > 0;
  const statisticsMeasure = statisticsRows[0]?.measureLabel ?? "count";
  const backendCategoricalResult = statisticsRows[0]?.measureLabel !== undefined;
  const inferredCategoricalTitle = `${statisticsMeasure} by ${statisticsDimension}`;
  const resolvedChartTitle = !isTimechartResult
    && backendCategoricalResult
    && chartTitle === "Event volume by level"
    ? inferredCategoricalTitle
    : chartTitle;

  function selectChartStyle(style: ChartStyle) {
    onChartStyleChange(style);
    onChartTitleChange(isTimechartResult
      ? "Event volume over time"
      : backendCategoricalResult
        ? inferredCategoricalTitle
        : "Event volume by level");
  }

  return (
    <section id="panel-visualization" role="tabpanel" aria-labelledby="tab-visualization" className="visualization-panel">
      <header className="result-view-header">
        <div>
          <h2>{resolvedChartTitle.trim() || "Untitled visualization"}</h2>
          <p>{isTimechartResult
            ? "Timechart across the submitted search range."
            : hasCategoricalChart
              ? backendCategoricalResult
                ? `${statisticsMeasure} grouped by ${statisticsDimension}.${statisticsRows.length > displayedStatisticsRows.length ? ` Showing the top ${displayedStatisticsRows.length} of ${statisticsRows.length} categories.` : ""}`
                : "Aggregation of the displayed event set."
              : "This result shape cannot be represented faithfully as a categorical chart."}</p>
        </div>
        <fieldset className="chart-toggle">
          <legend className="sr-only">Chart style</legend>
          <button className={chartStyle === "column" ? "active" : ""} type="button" aria-pressed={chartStyle === "column"} disabled={!hasCategoricalChart} title={!hasCategoricalChart ? "Column charts require one dimension and one non-negative numeric measure" : undefined} onClick={() => selectChartStyle("column")}>▥ Column</button>
          <button className={chartStyle === "horizontal" ? "active" : ""} type="button" aria-pressed={chartStyle === "horizontal"} disabled={isTimechartResult || !hasCategoricalChart} title={isTimechartResult ? "Bar charts require categorical results" : !hasCategoricalChart ? "Bar charts require one dimension and one non-negative numeric measure" : undefined} onClick={() => selectChartStyle("horizontal")}>☷ Bar</button>
          <button className={chartStyle === "line" ? "active" : ""} type="button" aria-pressed={chartStyle === "line"} disabled={!isTimechartResult} title={!isTimechartResult ? "Line charts require time-series results" : undefined} onClick={() => selectChartStyle("line")}>⌁ Line</button>
          <button type="button" onClick={() => onShowToast("Area and scatter charts become available for compatible result shapes.")}>More…</button>
        </fieldset>
      </header>
      <div className={`visualization-canvas chart-${chartStyle} legend-${legendPosition}${isLineChart ? " visualization-canvas--line" : ""}`} data-testid="visualization-chart">
        {!hasCategoricalChart ? (
          <output className={styles.emptyState}>
            <span className={styles.emptyStateIcon} aria-hidden="true"><span /><span /><span /></span>
            <strong>No compatible chart for these results</strong>
            <p>Return one categorical dimension and one non-negative numeric measure, or use a timechart for a time-series visualization. The complete server result remains available in Statistics.</p>
          </output>
        ) : isLineChart ? (
          <TimeSeriesLineChart points={timelinePoints} />
        ) : (
          <>
            <div className="chart-y-axis" aria-hidden="true">
              {[1, 0.75, 0.5, 0.25, 0].map((ratio) => (
                <span key={`${isTimechartResult ? "time" : "category"}-${ratio}`}>
                  {COMPACT_NUMBER_FORMAT.format(Math.round(chartAxisMaximum * ratio))}
                </span>
              ))}
            </div>
            <div className="chart-plot">
              <div className="chart-grid" aria-hidden="true"><span /><span /><span /><span /></div>
              {isTimechartResult ? (
                <div className="timechart-columns" data-testid="timechart-columns">
                  <div className="timechart-column-bars">
                    {timelinePoints.map((point, index) => (
                      <button type="button" key={point.id} aria-label={`${point.label}: ${NUMBER_FORMAT.format(point.count)} events`} title={`${point.label}\n${NUMBER_FORMAT.format(point.count)} events`}>
                        <span style={{ height: `${Math.max(3, (point.count / maxTimelineCount) * 100)}%` }} />
                        {showDataLabels && (index % 12 === 0 || index === timelinePoints.length - 1) ? <b>{COMPACT_NUMBER_FORMAT.format(point.count)}</b> : null}
                      </button>
                    ))}
                  </div>
                  <div className="line-chart-axis" aria-hidden="true">
                    {timelineAxisLabels.map((point) => <span key={point.id}>{point.label}</span>)}
                  </div>
                </div>
              ) : (
                <div className={`chart-bars${displayedStatisticsRows.length > 4 ? ` ${styles.denseBars}` : ""}`}>
                  {displayedStatisticsRows.map((row, index) => {
                    const color = categoryColor(row.level, index);
                    return (
                      <div className="chart-series" key={row.level}>
                        <button
                          type="button"
                          aria-label={`${row.level}: ${statisticsMeasure} ${NUMBER_FORMAT.format(row.count)}`}
                          title={`${row.level}\n${statisticsMeasure}: ${NUMBER_FORMAT.format(row.count)}`}
                          onClick={() => onApplyPivot(statisticsDimension, row.level, "include")}
                          style={{ "--bar-size": `${(row.count / maxStatisticsCount) * 100}%` } as CSSProperties}
                        >
                          <span className="chart-fill" style={{ backgroundColor: color }} />
                          {showDataLabels ? <b>{NUMBER_FORMAT.format(row.count)}</b> : null}
                        </button>
                        <strong>{row.level}</strong>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          </>
        )}
        {!hasCategoricalChart || legendPosition === "none" ? null : (
          <div className="chart-legend">
            {isTimechartResult
              ? isLineChart
                ? timelineSeries.map((name, index) => (
                  <span key={name}>
                    <i style={{ backgroundColor: TIME_SERIES_COLORS[index % TIME_SERIES_COLORS.length] }} />
                    {timelineSeriesDisplayName(name)}
                  </span>
                ))
                : <span><i className="legend-info" />Events</span>
              : displayedStatisticsRows.map((row, index) => <span key={row.level}><i style={{ backgroundColor: categoryColor(row.level, index) }} />{row.level}</span>)}
          </div>
        )}
      </div>
      <aside className="visualization-settings">
        <h3>Visualization</h3>
        <label><span>Title</span><input value={resolvedChartTitle} onChange={(event) => onChartTitleChange(event.target.value)} /></label>
        <label><span>Legend</span><select value={legendPosition} onChange={(event) => onLegendPositionChange(event.target.value as LegendPosition)}><option value="bottom">Bottom</option><option value="right">Right</option><option value="none">Hidden</option></select></label>
        {isLineChart ? (
          <div className="visualization-interaction-note"><strong>Inspect values</strong><span>Hover, tap, or focus the plot and use the arrow keys.</span></div>
        ) : (
          <label><span>Data labels</span><input type="checkbox" checked={showDataLabels} disabled={!hasCategoricalChart} onChange={(event) => onShowDataLabelsChange(event.target.checked)} /></label>
        )}
      </aside>
    </section>
  );
}
