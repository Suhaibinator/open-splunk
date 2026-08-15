import { niceStep } from "../charts/chart-scale";
import {
  type KeyboardEvent,
  type PointerEvent,
  useEffect,
  useId,
  useMemo,
  useRef,
  useState,
} from "react";

import type { DemoScalar, TimelinePoint } from "@/lib/demo/search-data";
import type {
  WorkspaceStatistic,
  WorkspaceStatisticSeries,
} from "@/lib/search/backend-data";
import type { PivotMode } from "@/lib/search/query-pivots";

import {
  TIME_SERIES_COLORS,
  TimeSeriesLineChart,
  formatTimelineSeriesValue,
  timelineSeriesDisplayName,
  timelineSeriesNames,
} from "../charts/time-series-line-chart";
import { categoricalActivation } from "../categorical-interaction";
import { COMPACT_NUMBER_FORMAT, NUMBER_FORMAT } from "../constants";
import { formatExactNumericText } from "../formatters";
import type { ChartStyle, LegendPosition } from "../model";

import styles from "./visualization-panel.module.css";

interface VisualizationPanelProps {
  chartStyle: ChartStyle;
  chartTitle: string;
  isPreview: boolean;
  isTimechartResult: boolean;
  legendPosition: LegendPosition;
  showDataLabels: boolean;
  statisticsDimension: string;
  statisticsRows: WorkspaceStatistic[];
  timelinePoints: TimelinePoint[];
  onApplyPivot: (fieldName: string, fieldValue: DemoScalar, mode: PivotMode) => void;
  onChartStyleChange: (style: ChartStyle) => void;
  onChartTitleChange: (title: string) => void;
  onLegendPositionChange: (position: LegendPosition) => void;
  onShowDataLabelsChange: (show: boolean) => void;
  onVisualizationEdited: () => void;
  onShowToast: (message: string) => void;
  previewTruncated: boolean;
}

interface StatisticSeriesDefinition {
  key: string;
  label: string;
}

interface ChartScale {
  minimum: number;
  maximum: number;
  ticks: number[];
}

interface CategoricalChartProps {
  dimension: string;
  horizontal: boolean;
  rows: WorkspaceStatistic[];
  series: StatisticSeriesDefinition[];
  showDataLabels: boolean;
  onApplyPivot: VisualizationPanelProps["onApplyPivot"];
}

const CATEGORY_COLORS = ["#5f9f3a", "#2f7fa6", "#e49a2c", "#8b67a8", "#c6534c", "#4d9a8a"] as const;
const MAX_CATEGORICAL_ROWS = 12;
const LEGACY_SERIES_KEY = "__events__";

function timeAxisLabels(points: TimelinePoint[]): TimelinePoint[] {
  if (points.length <= 5) return points;
  return Array.from(new Set([0, 0.25, 0.5, 0.75, 1].map((ratio) => Math.round(ratio * (points.length - 1)))))
    .map((index) => points[index]);
}

function categoryColor(category: string, index: number): string {
  const semanticColor = {
    info: "#5f9c3a",
    warn: "#dda229",
    error: "#c84f48",
    debug: "#5290b0",
  }[category.toLowerCase()];
  return semanticColor ?? CATEGORY_COLORS[index % CATEGORY_COLORS.length];
}

function formatExactNumeric(value: string | undefined, coordinate: number, compact = false): string {
  if (value === undefined) {
    return (compact ? COMPACT_NUMBER_FORMAT : NUMBER_FORMAT).format(coordinate);
  }
  if (compact) {
    return formatExactNumericText(value, { compact: true, compactSuffix: "s" });
  }
  return formatExactNumericText(value);
}

function rowSeries(row: WorkspaceStatistic, definition: StatisticSeriesDefinition): WorkspaceStatisticSeries {
  if (definition.key === LEGACY_SERIES_KEY) {
    return {
      key: definition.key,
      label: definition.label,
      value: row.count,
      exactValue: row.exactCount,
      coordinateApproximate: row.coordinateApproximate,
    };
  }
  return row.series?.find((item) => item.key === definition.key) ?? {
    key: definition.key,
    label: definition.label,
    value: null,
  };
}

function categoricalSeriesDefinitions(rows: WorkspaceStatistic[]): StatisticSeriesDefinition[] {
  const definitions = new Map<string, StatisticSeriesDefinition>();
  for (const row of rows) {
    for (const item of row.series ?? []) {
      if (!definitions.has(item.key)) {
        definitions.set(item.key, { key: item.key, label: item.label });
      }
    }
  }
  if (definitions.size > 0) return [...definitions.values()];
  return rows.length > 0
    ? [{ key: LEGACY_SERIES_KEY, label: rows[0].measureLabel ?? "Events" }]
    : [];
}

function statisticMagnitude(row: WorkspaceStatistic): number {
  const values = row.series?.flatMap((series) =>
    series.value === null || !Number.isFinite(series.value) ? [] : [Math.abs(series.value)],
  );
  return values !== undefined && values.length > 0
    ? values.reduce((sum, value) => sum + value, 0)
    : Math.abs(row.count);
}

function categoricalScale(
  rows: WorkspaceStatistic[],
  series: StatisticSeriesDefinition[],
): ChartScale {
  const values = rows.flatMap((row) => series.flatMap((definition) => {
    const value = rowSeries(row, definition).value;
    return value === null || !Number.isFinite(value) ? [] : [value];
  }));
  const dataMinimum = values.length === 0 ? 0 : Math.min(...values);
  const dataMaximum = values.length === 0 ? 0 : Math.max(...values);
  const rawMinimum = Math.min(0, dataMinimum);
  const rawMaximum = Math.max(0, dataMaximum);
  const span = rawMinimum === rawMaximum ? 1 : rawMaximum - rawMinimum;
  const step = niceStep(span);
  const minimum = Math.floor(rawMinimum / step) * step;
  const maximum = Math.max(minimum + step, Math.ceil(rawMaximum / step) * step);
  const intervalCount = Math.max(1, Math.round((maximum - minimum) / step));
  const ticks = Array.from({ length: intervalCount + 1 }, (_, index) => {
    const value = maximum - (index * step);
    return Math.abs(value) < step / 1_000_000 ? 0 : value;
  });
  return { minimum, maximum, ticks };
}

function verticalGeometry(value: number, scale: ChartScale): { top: number; height: number } {
  const range = scale.maximum - scale.minimum;
  const start = Math.max(0, value);
  const end = Math.min(0, value);
  return {
    top: ((scale.maximum - start) / range) * 100,
    height: (Math.abs(start - end) / range) * 100,
  };
}

function horizontalGeometry(value: number, scale: ChartScale): { left: number; width: number } {
  const range = scale.maximum - scale.minimum;
  return {
    left: ((Math.min(0, value) - scale.minimum) / range) * 100,
    width: (Math.abs(value) / range) * 100,
  };
}

function displaySeriesValue(series: WorkspaceStatisticSeries, compact = false): string {
  if (series.value === null) return "No value";
  return formatExactNumeric(series.exactValue, series.value, compact);
}

function seriesColor(index: number): string {
  return TIME_SERIES_COLORS[index % TIME_SERIES_COLORS.length];
}

function CategoricalTooltip({
  activeRow,
  dimension,
  inspectorId,
  onBlur,
  onClose,
  onDrilldown,
  onPointerLeave,
  rowIndex,
  series,
}: {
  activeRow: WorkspaceStatistic | null;
  dimension: string;
  inspectorId: string;
  onBlur: () => void;
  onClose: () => void;
  onDrilldown: (row: WorkspaceStatistic) => void;
  onPointerLeave: () => void;
  rowIndex: number;
  series: StatisticSeriesDefinition[];
}) {
  if (activeRow === null) return null;
  const backendSeries = activeRow.series !== undefined;
  const approximatePosition = series.some((definition) =>
    rowSeries(activeRow, definition).coordinateApproximate === true,
  );
  return (
    <section
      className={styles.categoricalTooltip}
      id={inspectorId}
      aria-label={`Values for ${activeRow.level}`}
      data-categorical-inspector="true"
      data-testid="categorical-chart-tooltip"
      onBlurCapture={(event) => {
        if (!(event.relatedTarget instanceof Node) || !event.currentTarget.contains(event.relatedTarget)) {
          onBlur();
        }
      }}
      onPointerLeave={onPointerLeave}
    >
      <div className={styles.categoricalTooltipHeader}>
        <strong title={activeRow.level}>{activeRow.level}</strong>
        <button
          type="button"
          aria-label="Close chart value inspector"
          onClick={onClose}
        >
          ×
        </button>
      </div>
      {series.map((definition, seriesIndex) => {
        const value = rowSeries(activeRow, definition);
        return (
          <span key={definition.key}>
            <i
              aria-hidden="true"
              style={{ backgroundColor: backendSeries ? seriesColor(seriesIndex) : categoryColor(activeRow.level, rowIndex) }}
            />
            <span>{definition.label}</span>
            <b>{displaySeriesValue(value)}</b>
          </span>
        );
      })}
      {approximatePosition ? (
        <small className={styles.categoricalTooltipPrecision}>Chart position is approximate; displayed server values are exact.</small>
      ) : null}
      {activeRow.pivotable === false ? (
        <small className={styles.categoricalTooltipUnavailable}>Drilldown is unavailable for this typed value.</small>
      ) : (
        <button
          className={styles.categoricalTooltipAction}
          type="button"
          onClick={() => onDrilldown(activeRow)}
        >
          Add {dimension} value to search <span aria-hidden="true">›</span>
        </button>
      )}
    </section>
  );
}

function CategoricalChart({
  dimension,
  horizontal,
  rows,
  series,
  showDataLabels,
  onApplyPivot,
}: CategoricalChartProps) {
  const hintId = useId();
  const inspectorId = `${hintId}-inspector`;
  const buttonRefs = useRef<Array<HTMLButtonElement | null>>([]);
  const lastPointerTypeRef = useRef<string | null>(null);
  const [activeIndex, setActiveIndex] = useState<number | null>(null);
  const [pinnedIndex, setPinnedIndex] = useState<number | null>(null);
  const scale = useMemo(() => categoricalScale(rows, series), [rows, series]);
  const approximate = rows.some((row) =>
    row.coordinateApproximate === true || row.series?.some((item) => item.coordinateApproximate) === true,
  );
  const activeRow = activeIndex === null ? null : rows[activeIndex] ?? null;
  const backendSeries = rows.some((row) => row.series !== undefined);
  const inspectDescription = activeRow === null
    ? `Inspect ${dimension} categories. Use Left and Right arrow keys to move between categories.`
    : `${activeRow.level}. ${series.map((definition) => {
      const value = rowSeries(activeRow, definition);
      return `${definition.label} ${displaySeriesValue(value)}${value.coordinateApproximate ? "; displayed value exact, chart position approximate" : ""}`;
    }).join(", ")}.${activeRow.pivotable === false ? " Drilldown is unavailable for this typed value." : ` Activate to add this ${dimension} value to the search.`}`;

  useEffect(() => {
    setActiveIndex((current) => current === null || rows.length === 0 ? null : Math.min(current, rows.length - 1));
    setPinnedIndex((current) => current === null || rows.length === 0 ? null : Math.min(current, rows.length - 1));
  }, [rows.length]);

  function handleKeyDown(event: KeyboardEvent<HTMLButtonElement>, index: number) {
    if (event.key === "Enter" || event.key === " ") {
      lastPointerTypeRef.current = null;
      return;
    }
    let next: number | null = index;
    if (event.key === "ArrowRight" || event.key === "ArrowDown") next = Math.min(rows.length - 1, index + 1);
    else if (event.key === "ArrowLeft" || event.key === "ArrowUp") next = Math.max(0, index - 1);
    else if (event.key === "Home") next = 0;
    else if (event.key === "End") next = rows.length - 1;
    else if (event.key === "Escape") next = null;
    else return;
    event.preventDefault();
    setPinnedIndex(null);
    setActiveIndex(next);
    if (next === null) event.currentTarget.blur();
    else buttonRefs.current[next]?.focus({ preventScroll: true });
  }

  function handlePointerLeave(event: PointerEvent<HTMLButtonElement>) {
    if (
      event.relatedTarget instanceof Element
      && event.relatedTarget.closest("[data-categorical-inspector='true']") !== null
    ) {
      return;
    }
    const rowIndex = buttonRefs.current.indexOf(event.currentTarget);
    if (document.activeElement !== event.currentTarget && pinnedIndex !== rowIndex) {
      setActiveIndex(null);
    }
  }

  function activateRow(row: WorkspaceStatistic) {
    if (row.pivotable === false) return;
    onApplyPivot(
      dimension,
      row.pivotValue !== undefined ? row.pivotValue : row.level,
      "include",
    );
  }

  function closeInspector() {
    setPinnedIndex(null);
    setActiveIndex(null);
  }

  function drilldownFromInspector(row: WorkspaceStatistic) {
    activateRow(row);
    closeInspector();
  }

  function handleInspectorPointerLeave() {
    const focusedCategory = buttonRefs.current.some((button) => button === document.activeElement);
    if (pinnedIndex === null && !focusedCategory) setActiveIndex(null);
  }

  function handleCategoryClick(row: WorkspaceStatistic, rowIndex: number) {
    const pointerType = lastPointerTypeRef.current;
    lastPointerTypeRef.current = null;
    if (categoricalActivation(pointerType) === "inspect") {
      setPinnedIndex(rowIndex);
      setActiveIndex(rowIndex);
      return;
    }
    activateRow(row);
  }

  if (horizontal) {
    const minimumRowHeight = Math.max(30, (series.length * 17) + 8);
    return (
      <div className={`${styles.categoricalChart} ${styles.horizontalChart}`} data-testid="categorical-chart">
        <div className={styles.horizontalScroller}>
          <div
            className={styles.horizontalSurface}
            style={{
              minHeight: `max(100%, ${(rows.length * minimumRowHeight) + 39}px)`,
              minWidth: series.length > 3 ? `${560 + (series.length * 20)}px` : "520px",
            }}
          >
            <div className={styles.horizontalGrid} aria-hidden="true">
              {scale.ticks.map((tick) => (
                <span
                  key={tick}
                  className={tick === 0 ? styles.zeroGridLine : undefined}
                  style={{ left: `${((tick - scale.minimum) / (scale.maximum - scale.minimum)) * 100}%` }}
                />
              ))}
            </div>
            <div
              className={styles.horizontalGroups}
              style={{ gridTemplateRows: `repeat(${Math.max(1, rows.length)}, minmax(${minimumRowHeight}px, 1fr))` }}
            >
              {rows.map((row, rowIndex) => (
                <button
                  key={row.id ?? row.level}
                  ref={(element) => { buttonRefs.current[rowIndex] = element; }}
                  type="button"
                  className={styles.horizontalGroup}
                  aria-controls={inspectorId}
                  aria-describedby={hintId}
                  aria-expanded={rowIndex === activeIndex}
                  aria-label={rowIndex === activeIndex ? inspectDescription : `${row.level}; inspect chart values`}
                  onBlur={(event) => {
                    if (
                      event.relatedTarget instanceof Element
                      && event.relatedTarget.closest("[data-categorical-inspector='true']") !== null
                    ) {
                      return;
                    }
                    if (pinnedIndex !== rowIndex) setActiveIndex(null);
                  }}
                  onClick={() => handleCategoryClick(row, rowIndex)}
                  onFocus={() => setActiveIndex(rowIndex)}
                  onKeyDown={(event) => handleKeyDown(event, rowIndex)}
                  onPointerDown={(event) => {
                    lastPointerTypeRef.current = event.pointerType;
                    setActiveIndex(rowIndex);
                    if (event.pointerType === "touch" || event.pointerType === "pen") {
                      setPinnedIndex(rowIndex);
                    } else {
                      setPinnedIndex(null);
                    }
                    event.currentTarget.focus({ preventScroll: true });
                  }}
                  onPointerEnter={() => setActiveIndex(rowIndex)}
                  onPointerLeave={handlePointerLeave}
                >
                  <strong title={row.level}>{row.level}</strong>
                  <span className={styles.horizontalBars} aria-hidden="true">
                    {series.map((definition, seriesIndex) => {
                      const item = rowSeries(row, definition);
                      if (item.value === null) return <span className={styles.horizontalSlot} key={definition.key} />;
                      const geometry = horizontalGeometry(item.value, scale);
                      const color = backendSeries ? seriesColor(seriesIndex) : categoryColor(row.level, rowIndex);
                      return (
                        <span className={styles.horizontalSlot} key={definition.key}>
                          <i
                            className={styles.horizontalBar}
                            style={{ backgroundColor: color, left: `${geometry.left}%`, width: `${geometry.width}%` }}
                          />
                          {showDataLabels ? (
                            <b
                              className={styles.horizontalDataLabel}
                              style={{
                                left: item.value >= 0
                                  ? `calc(${geometry.left + geometry.width}% + 5px)`
                                  : `calc(${geometry.left}% - 5px)`,
                                transform: item.value >= 0 ? undefined : "translateX(-100%)",
                              }}
                            >
                              {item.coordinateApproximate ? "≈" : ""}{displaySeriesValue(item, true)}
                            </b>
                          ) : null}
                        </span>
                      );
                    })}
                  </span>
                </button>
              ))}
            </div>
            <div className={styles.horizontalAxis} aria-hidden="true">
              {scale.ticks.toReversed().map((tick) => (
                <span key={tick}>{approximate ? "≈" : ""}{COMPACT_NUMBER_FORMAT.format(tick)}</span>
              ))}
            </div>
            <CategoricalTooltip
              activeRow={activeRow}
              dimension={dimension}
              inspectorId={inspectorId}
              onBlur={() => { if (pinnedIndex === null) setActiveIndex(null); }}
              onClose={closeInspector}
              onDrilldown={drilldownFromInspector}
              onPointerLeave={handleInspectorPointerLeave}
              rowIndex={activeIndex ?? 0}
              series={series}
            />
          </div>
        </div>
        <p className="sr-only" id={hintId}>Use arrow keys to move between categories. Home and End jump to the first and last category. Enter applies an available drilldown. Escape clears the value.</p>
        <output className="sr-only" aria-live="polite">{activeRow === null ? "" : inspectDescription}</output>
      </div>
    );
  }

  const minimumGroupWidth = Math.max(72, (series.length * 24) + 24);
  return (
    <div className={styles.categoricalChart} data-testid="categorical-chart">
      <div className={styles.categoricalYAxis} aria-hidden="true">
        {scale.ticks.map((tick) => <span key={tick}>{approximate ? "≈" : ""}{COMPACT_NUMBER_FORMAT.format(tick)}</span>)}
      </div>
      <div className={styles.categoricalScroller}>
        <div
          className={styles.categoricalSurface}
          style={{ minWidth: `max(100%, ${rows.length * minimumGroupWidth}px)` }}
        >
          <div className={styles.categoricalGrid} aria-hidden="true">
            {scale.ticks.map((tick) => (
              <span
                key={tick}
                className={tick === 0 ? styles.zeroGridLine : undefined}
                style={{ top: `${((scale.maximum - tick) / (scale.maximum - scale.minimum)) * 100}%` }}
              />
            ))}
          </div>
          <div
            className={styles.categoricalGroups}
            style={{ gridTemplateColumns: `repeat(${Math.max(1, rows.length)}, minmax(${minimumGroupWidth}px, 1fr))` }}
          >
            {rows.map((row, rowIndex) => (
              <button
                key={row.id ?? row.level}
                ref={(element) => { buttonRefs.current[rowIndex] = element; }}
                type="button"
                className={styles.categoricalGroup}
                aria-controls={inspectorId}
                aria-describedby={hintId}
                aria-expanded={rowIndex === activeIndex}
                aria-label={rowIndex === activeIndex ? inspectDescription : `${row.level}; inspect chart values`}
                onBlur={(event) => {
                  if (
                    event.relatedTarget instanceof Element
                    && event.relatedTarget.closest("[data-categorical-inspector='true']") !== null
                  ) {
                    return;
                  }
                  if (pinnedIndex !== rowIndex) setActiveIndex(null);
                }}
                onClick={() => handleCategoryClick(row, rowIndex)}
                onFocus={() => setActiveIndex(rowIndex)}
                onKeyDown={(event) => handleKeyDown(event, rowIndex)}
                onPointerDown={(event) => {
                  lastPointerTypeRef.current = event.pointerType;
                  setActiveIndex(rowIndex);
                  if (event.pointerType === "touch" || event.pointerType === "pen") {
                    setPinnedIndex(rowIndex);
                  } else {
                    setPinnedIndex(null);
                  }
                  event.currentTarget.focus({ preventScroll: true });
                }}
                onPointerEnter={() => setActiveIndex(rowIndex)}
                onPointerLeave={handlePointerLeave}
              >
                <span className={styles.verticalBars} aria-hidden="true">
                  {series.map((definition, seriesIndex) => {
                    const item = rowSeries(row, definition);
                    if (item.value === null) return <span className={styles.verticalSlot} key={definition.key} />;
                    const geometry = verticalGeometry(item.value, scale);
                    const color = backendSeries ? seriesColor(seriesIndex) : categoryColor(row.level, rowIndex);
                    const dataLabelTop = item.value >= 0
                      ? `max(2px, calc(${geometry.top}% - 17px))`
                      : `calc(${geometry.top + geometry.height}% + 3px)`;
                    return (
                      <span className={styles.verticalSlot} key={definition.key}>
                        <i
                          className={styles.verticalBar}
                          style={{
                            backgroundColor: color,
                            height: item.value === 0 ? "2px" : `${geometry.height}%`,
                            top: item.value === 0 ? `calc(${geometry.top}% - 1px)` : `${geometry.top}%`,
                          }}
                        />
                        {showDataLabels ? (
                          <b className={styles.verticalDataLabel} style={{ top: dataLabelTop }}>
                            {item.coordinateApproximate ? "≈" : ""}{displaySeriesValue(item, true)}
                          </b>
                        ) : null}
                      </span>
                    );
                  })}
                </span>
                <strong title={row.level}>{row.level}</strong>
              </button>
            ))}
          </div>
          <CategoricalTooltip
            activeRow={activeRow}
            dimension={dimension}
            inspectorId={inspectorId}
            onBlur={() => { if (pinnedIndex === null) setActiveIndex(null); }}
            onClose={closeInspector}
            onDrilldown={drilldownFromInspector}
            onPointerLeave={handleInspectorPointerLeave}
            rowIndex={activeIndex ?? 0}
            series={series}
          />
        </div>
      </div>
      <p className="sr-only" id={hintId}>Use arrow keys to move between categories. Home and End jump to the first and last category. Enter applies an available drilldown. Escape clears the value.</p>
      <output className="sr-only" aria-live="polite">{activeRow === null ? "" : inspectDescription}</output>
    </div>
  );
}

export function VisualizationPanel({
  chartStyle,
  chartTitle,
  isPreview,
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
  onVisualizationEdited,
  onShowToast,
  previewTruncated,
}: VisualizationPanelProps) {
  const displayedStatisticsRows = statisticsRows.length > MAX_CATEGORICAL_ROWS
    ? statisticsRows
      .map((row, index) => ({ row, index, magnitude: statisticMagnitude(row) }))
      .toSorted((left, right) => right.magnitude - left.magnitude || left.index - right.index)
      .slice(0, MAX_CATEGORICAL_ROWS)
      .map(({ row }) => row)
    : statisticsRows;
  const categoricalSeries = categoricalSeriesDefinitions(displayedStatisticsRows);
  const maxTimelineCount = Math.max(1, ...timelinePoints.map((point) => point.count));
  const chartAxisMaximum = maxTimelineCount;
  const timelineAxisLabels = timeAxisLabels(timelinePoints);
  const timelineSeries = timelineSeriesNames(timelinePoints);
  const hasApproximateCoordinates = isTimechartResult
    ? timelinePoints.some((point) => point.coordinateApproximate === true)
    : statisticsRows.some((row) =>
      row.coordinateApproximate === true || row.series?.some((series) => series.coordinateApproximate) === true,
    );
  const splitTimechart = isTimechartResult && timelineSeries.length > 1;
  const isLineChart = isTimechartResult && (chartStyle === "line" || splitTimechart);
  const effectiveChartStyle = isLineChart ? "line" : chartStyle;
  const hasCategoricalChart = isTimechartResult
    ? timelinePoints.length > 0
    : displayedStatisticsRows.length > 0 && categoricalSeries.length > 0;
  const backendCategoricalResult = statisticsRows.some((row) => row.series !== undefined);
  const seriesSummary = categoricalSeries.length === 1
    ? categoricalSeries[0]?.label ?? "Results"
    : categoricalSeries.length === 2
      ? `${categoricalSeries[0].label} and ${categoricalSeries[1].label}`
      : `${categoricalSeries.length} series`;
  const inferredCategoricalTitle = `${seriesSummary} by ${statisticsDimension}`;
  const resolvedChartTitle = !isTimechartResult
    && backendCategoricalResult
    && chartTitle === "Event volume by level"
    ? inferredCategoricalTitle
    : chartTitle;

  function selectChartStyle(style: ChartStyle) {
    onVisualizationEdited();
    onChartStyleChange(style);
    onChartTitleChange(isTimechartResult
      ? "Event volume over time"
      : backendCategoricalResult
        ? inferredCategoricalTitle
        : "Event volume by level");
  }

  return (
    <section id="panel-visualization" role="tabpanel" aria-labelledby="tab-visualization" className={`visualization-panel${isPreview ? " visualization-panel--preview" : ""}`}>
      <header className="result-view-header">
        <div>
          <div className="result-title-line">
            <h2>{resolvedChartTitle.trim() || "Untitled visualization"}</h2>
            {isPreview ? <span className="preview-context-badge"><i aria-hidden="true" /> Live preview</span> : null}
          </div>
          <p>{isPreview
            ? `${isTimechartResult
              ? "The chart updates as time-series rows arrive. Its scale and values may change until completion."
              : hasCategoricalChart
                ? "The chart updates as result rows arrive. Categories, values, and ordering remain provisional."
                : "Waiting for a preview result shape that can be charted."}${previewTruncated ? " The preview limit was reached; the final chart may include additional data." : ""}`
            : isTimechartResult
              ? `Timechart across the submitted search range.${hasApproximateCoordinates ? " The plotted scale is approximate for values beyond the browser’s exact integer range; hover or focus a point for its exact server value." : ""}`
              : hasCategoricalChart
                ? backendCategoricalResult
                  ? `${categoricalSeries.length === 1 ? categoricalSeries[0].label : `${categoricalSeries.length} complete series`} grouped by ${statisticsDimension}.${statisticsRows.length > displayedStatisticsRows.length ? ` Showing the top ${displayedStatisticsRows.length} of ${statisticsRows.length} categories.` : ""}${hasApproximateCoordinates ? " The plotted scale is approximate for values beyond the browser’s exact integer range; exact server values appear on hover or focus." : ""}`
                  : "Aggregation of the displayed event set."
                : "This result shape cannot be represented faithfully as a categorical chart."}</p>
        </div>
        <fieldset className="chart-toggle">
          <legend className="sr-only">Chart style</legend>
          <button className={effectiveChartStyle === "column" ? "active" : ""} type="button" aria-pressed={effectiveChartStyle === "column"} disabled={!hasCategoricalChart || splitTimechart} title={splitTimechart ? "Split-series timecharts use Line so no server series is collapsed" : !hasCategoricalChart ? "Column charts require one dimension and at least one numeric measure" : undefined} onClick={() => selectChartStyle("column")}>▥ Column</button>
          <button className={chartStyle === "horizontal" ? "active" : ""} type="button" aria-pressed={chartStyle === "horizontal"} disabled={isTimechartResult || !hasCategoricalChart} title={isTimechartResult ? "Bar charts require categorical results" : !hasCategoricalChart ? "Bar charts require one dimension and at least one numeric measure" : undefined} onClick={() => selectChartStyle("horizontal")}>☷ Bar</button>
          <button className={isLineChart ? "active" : ""} type="button" aria-pressed={isLineChart} disabled={!isTimechartResult} title={!isTimechartResult ? "Line charts require time-series results" : undefined} onClick={() => selectChartStyle("line")}>⌁ Line</button>
          <button type="button" onClick={() => onShowToast("Area and scatter charts become available for compatible result shapes.")}>More…</button>
        </fieldset>
      </header>
      <div
        className={`visualization-canvas chart-${effectiveChartStyle} legend-${legendPosition}${isLineChart ? " visualization-canvas--line" : ""}${!isTimechartResult ? ` ${styles.categoricalCanvas}` : ""}${isPreview ? " visualization-canvas--preview" : ""}`}
        data-testid="visualization-chart"
      >
        {!hasCategoricalChart ? (
          <output className={styles.emptyState}>
            <span className={styles.emptyStateIcon} aria-hidden="true"><span /><span /><span /></span>
            <strong>No compatible chart for these results</strong>
            <p>{isPreview
              ? "The live preview has not produced a chart-compatible result shape yet. Statistics will update if compatible provisional rows arrive."
              : "Return one categorical dimension and at least one numeric measure, or use a timechart for a time-series visualization. The complete server result remains available in Statistics."}</p>
          </output>
        ) : isLineChart ? (
          <TimeSeriesLineChart points={timelinePoints} />
        ) : isTimechartResult ? (
          <>
            <div className="chart-y-axis" aria-hidden="true">
              {[1, 0.75, 0.5, 0.25, 0].map((ratio) => (
                <span key={`time-${ratio}`}>
                  {hasApproximateCoordinates ? "≈" : ""}{COMPACT_NUMBER_FORMAT.format(Math.round(chartAxisMaximum * ratio))}
                </span>
              ))}
            </div>
            <div className="chart-plot">
              <div className="chart-grid" aria-hidden="true"><span /><span /><span /><span /></div>
              <div className="timechart-columns" data-testid="timechart-columns">
                <div className="timechart-column-bars">
                  {timelinePoints.map((point, index) => (
                    <button
                      type="button"
                      key={point.id}
                      aria-label={`${point.label}: ${formatTimelineSeriesValue(point, "Events")} events${point.coordinateApproximate ? "; chart position approximate" : ""}`}
                      title={`${point.label}\n${formatTimelineSeriesValue(point, "Events")} events${point.coordinateApproximate ? "\nChart position is approximate" : ""}`}
                    >
                      <span style={{ height: `${Math.max(3, (point.count / maxTimelineCount) * 100)}%` }} />
                      {showDataLabels && (index % 12 === 0 || index === timelinePoints.length - 1)
                        ? <b>{point.coordinateApproximate ? "≈" : ""}{formatTimelineSeriesValue(point, "Events", "Events", true)}</b>
                        : null}
                    </button>
                  ))}
                </div>
                <div className="line-chart-axis" aria-hidden="true">
                  {timelineAxisLabels.map((point) => <span key={point.id}>{point.label}</span>)}
                </div>
              </div>
            </div>
          </>
        ) : (
          <CategoricalChart
            dimension={statisticsDimension}
            horizontal={effectiveChartStyle === "horizontal"}
            rows={displayedStatisticsRows}
            series={categoricalSeries}
            showDataLabels={showDataLabels}
            onApplyPivot={onApplyPivot}
          />
        )}
        {!hasCategoricalChart || legendPosition === "none" ? null : (
          <div className="chart-legend">
            {isTimechartResult
              ? isLineChart
                ? timelineSeries.map((name, index) => (
                  <span key={name}>
                    <i style={{ backgroundColor: seriesColor(index) }} />
                    {timelineSeriesDisplayName(name)}
                  </span>
                ))
                : <span><i className="legend-info" />Events</span>
              : backendCategoricalResult
                ? categoricalSeries.map((series, index) => (
                  <span key={series.key}>
                    <i style={{ backgroundColor: seriesColor(index) }} />
                    {series.label}
                  </span>
                ))
                : displayedStatisticsRows.map((row, index) => (
                  <span key={row.id ?? row.level}>
                    <i style={{ backgroundColor: categoryColor(row.level, index) }} />
                    {row.level}
                  </span>
                ))}
          </div>
        )}
      </div>
      <aside className="visualization-settings">
        <h3>Visualization</h3>
        <label><span>Title</span><input value={resolvedChartTitle} onChange={(event) => {
          onVisualizationEdited();
          onChartTitleChange(event.target.value);
        }} /></label>
        <label><span>Legend</span><select value={legendPosition} onChange={(event) => {
          onVisualizationEdited();
          onLegendPositionChange(event.target.value as LegendPosition);
        }}><option value="bottom">Bottom</option><option value="right">Right</option><option value="none">Hidden</option></select></label>
        {isLineChart ? (
          <div className="visualization-interaction-note"><strong>Inspect values</strong><span>Hover, tap, or focus the plot and use the arrow keys.</span></div>
        ) : (
          <>
            <label><span>Data labels</span><input type="checkbox" checked={showDataLabels} disabled={!hasCategoricalChart} onChange={(event) => {
              onVisualizationEdited();
              onShowDataLabelsChange(event.target.checked);
            }} /></label>
            {!isTimechartResult && hasCategoricalChart ? (
              <div className="visualization-interaction-note">
                <strong>Inspect values</strong>
                <span>Hover or focus a category to compare exact values. On touch, tap once to inspect, then use the inspector action to drill down.</span>
              </div>
            ) : null}
          </>
        )}
      </aside>
    </section>
  );
}
