import type { CSSProperties } from "react";

import type { TimelinePoint } from "@/lib/demo/search-data";
import type { WorkspaceStatistic } from "@/lib/search/backend-data";
import type { PivotMode } from "@/lib/search/query-pivots";

import { COMPACT_NUMBER_FORMAT, NUMBER_FORMAT } from "../constants";
import type { ChartStyle, LegendPosition } from "../model";
import { TimeSeriesLineChart } from "../charts/time-series-line-chart";

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
  const maxTimelineCount = Math.max(1, ...timelinePoints.map((point) => point.count));
  const maxStatisticsCount = Math.max(1, ...statisticsRows.map((row) => row.count));
  const timelineAxisLabels = timeAxisLabels(timelinePoints);
  const isLineChart = chartStyle === "line" && isTimechartResult;

  function selectChartStyle(style: ChartStyle) {
    onChartStyleChange(style);
    onChartTitleChange(style === "horizontal" ? "Event volume by level" : isTimechartResult ? "Event volume over time" : "Event volume by level");
  }

  return (
    <section id="panel-visualization" role="tabpanel" aria-labelledby="tab-visualization" className="visualization-panel">
      <header className="result-view-header">
        <div>
          <h2>{chartTitle.trim() || "Untitled visualization"}</h2>
          <p>{isTimechartResult ? "Timechart across the submitted search range." : "Aggregation of the displayed event set."}</p>
        </div>
        <fieldset className="chart-toggle">
          <legend className="sr-only">Chart style</legend>
          <button className={chartStyle === "column" ? "active" : ""} type="button" aria-pressed={chartStyle === "column"} onClick={() => selectChartStyle("column")}>▥ Column</button>
          <button className={chartStyle === "horizontal" ? "active" : ""} type="button" aria-pressed={chartStyle === "horizontal"} disabled={isTimechartResult} title={isTimechartResult ? "Bar charts require categorical results" : undefined} onClick={() => selectChartStyle("horizontal")}>☷ Bar</button>
          <button className={chartStyle === "line" ? "active" : ""} type="button" aria-pressed={chartStyle === "line"} disabled={!isTimechartResult} title={!isTimechartResult ? "Line charts require time-series results" : undefined} onClick={() => selectChartStyle("line")}>⌁ Line</button>
          <button type="button" onClick={() => onShowToast("Area and scatter charts become available for compatible result shapes.")}>More…</button>
        </fieldset>
      </header>
      <div className={`visualization-canvas chart-${chartStyle} legend-${legendPosition}${isLineChart ? " visualization-canvas--line" : ""}`} data-testid="visualization-chart">
        {isLineChart ? (
          <TimeSeriesLineChart points={timelinePoints} />
        ) : (
          <>
            <div className="chart-y-axis" aria-hidden="true">
              {(isTimechartResult
                ? [maxTimelineCount, maxTimelineCount * 0.75, maxTimelineCount * 0.5, maxTimelineCount * 0.25, 0].map((value) => COMPACT_NUMBER_FORMAT.format(Math.round(value)))
                : ["10k", "7.5k", "5k", "2.5k", "0"]
              ).map((label) => <span key={`${isTimechartResult ? "time" : "category"}-${label}`}>{label}</span>)}
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
                <div className="chart-bars">
                  {statisticsRows.map((row) => (
                    <div className="chart-series" key={row.level}>
                      <button
                        type="button"
                        title={`${row.level}: ${NUMBER_FORMAT.format(row.count)} events`}
                        onClick={() => onApplyPivot(statisticsDimension, row.level, "include")}
                        style={{ "--bar-size": `${(row.count / maxStatisticsCount) * 100}%` } as CSSProperties}
                      >
                        <span className={`chart-fill chart-fill-${row.level.toLowerCase()}`} />
                        {showDataLabels ? <b>{NUMBER_FORMAT.format(row.count)}</b> : null}
                      </button>
                      <strong>{row.level}</strong>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </>
        )}
        {legendPosition === "none" ? null : (
          <div className="chart-legend">
            {isTimechartResult
              ? <span><i className="legend-info" />Events</span>
              : statisticsRows.map((row) => <span key={row.level}><i className={`legend-${row.level.toLowerCase()}`} />{row.level}</span>)}
          </div>
        )}
      </div>
      <aside className="visualization-settings">
        <h3>Visualization</h3>
        <label><span>Title</span><input value={chartTitle} onChange={(event) => onChartTitleChange(event.target.value)} /></label>
        <label><span>Legend</span><select value={legendPosition} onChange={(event) => onLegendPositionChange(event.target.value as LegendPosition)}><option value="bottom">Bottom</option><option value="right">Right</option><option value="none">Hidden</option></select></label>
        {isLineChart ? (
          <div className="visualization-interaction-note"><strong>Values on hover</strong><span>Move across the plot, or focus it and use the arrow keys.</span></div>
        ) : (
          <label><span>Data labels</span><input type="checkbox" checked={showDataLabels} onChange={(event) => onShowDataLabelsChange(event.target.checked)} /></label>
        )}
      </aside>
    </section>
  );
}
