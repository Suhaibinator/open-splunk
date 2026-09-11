import { linearTickScale, projectScaleValue } from "../charts/chart-scale";
import {
  addStackCoordinate,
  addStackMagnitude,
  createStackMagnitudeTotal,
  normalizeStackValue,
  stackMagnitudeCoordinate,
  stackMagnitudeIsApproximate,
  type StackedChartRow,
  type StackMagnitudeTotal,
} from "../charts/chart-stacking";
import {
  type FocusEvent,
  type KeyboardEvent,
  type PointerEvent,
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

import { AppIcon } from "../../_components/app-icon";
import { Button } from "../../_components/button";

import {
  TIME_SERIES_COLORS,
  TimeSeriesLineChart,
  timelineChartModel,
  timelineSeriesDisplayName,
} from "../charts/time-series-line-chart";
import { categoricalActivation } from "../categorical-interaction";
import { COMPACT_NUMBER_FORMAT, NUMBER_FORMAT } from "../constants";
import { formatExactNumericText } from "../formatters";
import type { ChartStyle, LegendPosition, StackMode } from "../model";
import { describeTimechartCoverage, type TimechartCoverage } from "../timechart-series";
import { Select, SelectOption } from "../../_components/select";

interface VisualizationPanelProps {
  chartStyle: ChartStyle;
  chartTitle: string;
  isPreview: boolean;
  isTimechartResult: boolean;
  legendPosition: LegendPosition;
  showDataLabels: boolean;
  stackMode: StackMode;
  statisticsDimension: string;
  statisticsRows: WorkspaceStatistic[];
  /** Which buckets of a server time-series result are plotted; null outside backend timecharts. */
  timechartCoverage: TimechartCoverage | null;
  timelinePoints: TimelinePoint[];
  onApplyPivot: (fieldName: string, fieldValue: DemoScalar, mode: PivotMode) => void;
  onChartStyleChange: (style: ChartStyle) => void;
  onChartTitleChange: (title: string) => void;
  onCopySeriesLabel: (label: string) => void;
  onLegendPositionChange: (position: LegendPosition) => void;
  onShowDataLabelsChange: (show: boolean) => void;
  onStackModeChange: (mode: StackMode) => void;
  onVisualizationEdited: () => void;
  previewTruncated: boolean;
}

const TIMELINE_SERIES_WINDOW_SIZE = 24;

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
  model: CategoricalChartModel;
  seriesEnd: number;
  seriesStart: number;
  showDataLabels: boolean;
  stackMode: StackMode;
  onApplyPivot: VisualizationPanelProps["onApplyPivot"];
}

// The first six of the categorical ramp, sliced from the one array that
// declares it rather than retyped, so the two charts cannot drift apart.
const CATEGORY_COLORS = TIME_SERIES_COLORS.slice(0, 6);
const MAX_CATEGORICAL_ROWS = 12;
const LEGACY_SERIES_KEY = "__events__";

interface CategoricalChartRow {
  hasFinite: boolean;
  maximum: number;
  minimum: number;
  negativeTotal: StackMagnitudeTotal;
  positiveTotal: StackMagnitudeTotal;
  row: WorkspaceStatistic;
  seriesByKey: Map<string, WorkspaceStatisticSeries>;
}

export interface CategoricalChartModel {
  approximate: boolean;
  backendSeries: boolean;
  domains: Record<StackMode, number[]>;
  rows: CategoricalChartRow[];
  series: StatisticSeriesDefinition[];
}

function categoryColor(category: string, index: number): string {
  // The four level swatches the log data itself carries, not the outcome of a
  // search: `--level-*`, never `--status-*`. See docs/theming.md.
  const semanticColor = {
    info: "var(--level-info)",
    warn: "var(--level-warn)",
    error: "var(--level-error)",
    debug: "var(--level-debug)",
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

function indexedRowSeries(
  row: CategoricalChartRow,
  definition: StatisticSeriesDefinition,
): WorkspaceStatisticSeries {
  if (definition.key === LEGACY_SERIES_KEY) {
    return {
      key: definition.key,
      label: definition.label,
      value: row.row.count,
      exactValue: row.row.exactCount,
      coordinateApproximate: row.row.coordinateApproximate,
    };
  }
  return row.seriesByKey.get(definition.key) ?? {
    key: definition.key,
    label: definition.label,
    value: null,
  };
}

function extendCategoricalDomain(
  domains: Record<StackMode, number[]>,
  mode: StackMode,
  minimum: number,
  maximum: number,
) {
  const domain = domains[mode];
  if (domain.length === 0) {
    domain.push(minimum, maximum);
    return;
  }
  domain[0] = Math.min(domain[0], minimum);
  domain[1] = Math.max(domain[1], maximum);
}

/** Index each categorical row and derive stable full-series domains once. */
export function categoricalChartModel(rows: WorkspaceStatistic[]): CategoricalChartModel {
  const definitions = new Map<string, StatisticSeriesDefinition>();
  let approximate = false;
  const indexedRows = rows.map((row): CategoricalChartRow => {
    const seriesByKey = new Map<string, WorkspaceStatisticSeries>();
    let hasFinite = false;
    let maximum = 0;
    let minimum = 0;
    const negativeTotal = createStackMagnitudeTotal();
    const positiveTotal = createStackMagnitudeTotal();
    approximate ||= row.coordinateApproximate === true;
    for (const item of row.series ?? []) {
      if (!definitions.has(item.key)) {
        definitions.set(item.key, { key: item.key, label: item.label });
      }
      if (seriesByKey.has(item.key)) continue;
      seriesByKey.set(item.key, item);
      approximate ||= item.coordinateApproximate === true;
      if (item.value === null || !Number.isFinite(item.value)) continue;
      hasFinite = true;
      maximum = Math.max(maximum, item.value);
      minimum = Math.min(minimum, item.value);
      addStackMagnitude(item.value < 0 ? negativeTotal : positiveTotal, item.value);
    }
    return { hasFinite, maximum, minimum, negativeTotal, positiveTotal, row, seriesByKey };
  });
  if (definitions.size === 0 && rows.length > 0) {
    definitions.set(LEGACY_SERIES_KEY, {
      key: LEGACY_SERIES_KEY,
      label: rows[0].measureLabel ?? "Events",
    });
    for (const indexed of indexedRows) {
      const value = indexed.row.count;
      indexed.hasFinite = Number.isFinite(value);
      indexed.maximum = Number.isFinite(value) ? Math.max(0, value) : 0;
      indexed.minimum = Number.isFinite(value) ? Math.min(0, value) : 0;
      indexed.negativeTotal = createStackMagnitudeTotal();
      indexed.positiveTotal = createStackMagnitudeTotal();
      if (Number.isFinite(value)) {
        addStackMagnitude(value < 0 ? indexed.negativeTotal : indexed.positiveTotal, value);
      }
    }
  }
  const domains: Record<StackMode, number[]> = { none: [], stacked: [], stacked100: [] };
  for (const row of indexedRows) {
    if (!row.hasFinite) continue;
    extendCategoricalDomain(domains, "none", row.minimum, row.maximum);
    extendCategoricalDomain(
      domains,
      "stacked",
      -stackMagnitudeCoordinate(row.negativeTotal),
      stackMagnitudeCoordinate(row.positiveTotal),
    );
    extendCategoricalDomain(
      domains,
      "stacked100",
      row.negativeTotal.maximum === 0 ? 0 : -100,
      row.positiveTotal.maximum === 0 ? 0 : 100,
    );
  }
  return {
    approximate: approximate || indexedRows.some((row) =>
      stackMagnitudeIsApproximate(row.negativeTotal)
      || stackMagnitudeIsApproximate(row.positiveTotal)
    ),
    backendSeries: rows.some((row) => row.series !== undefined),
    domains,
    rows: indexedRows,
    series: [...definitions.values()],
  };
}

function statisticMagnitude(row: WorkspaceStatistic): number {
  if (row.series === undefined) return Math.abs(row.count);
  let hasFinite = false;
  let magnitude = 0;
  for (const series of row.series) {
    if (series.value === null || !Number.isFinite(series.value)) continue;
    hasFinite = true;
    magnitude += Math.abs(series.value);
  }
  return hasFinite ? magnitude : Math.abs(row.count);
}

export function categoricalStackWindow(
  model: CategoricalChartModel,
  seriesStart: number,
  seriesEnd: number,
  stackMode: StackMode,
): { domain: number[]; rows: StackedChartRow[] } {
  const rows = model.rows.map((row) => {
    let negative = 0;
    let positive = 0;
    const visible: StackedChartRow = [];
    const scanStart = stackMode === "none" ? seriesStart : 0;
    for (let seriesIndex = scanStart; seriesIndex < seriesEnd; seriesIndex += 1) {
      const definition = model.series[seriesIndex];
      if (definition === undefined) continue;
      const raw = indexedRowSeries(row, definition).value;
      if (raw === null || !Number.isFinite(raw)) {
        if (seriesIndex >= seriesStart) visible.push({ end: 0, raw: null, start: 0 });
        continue;
      }
      const value = stackMode === "stacked100"
        ? normalizeStackValue(raw, row.positiveTotal, row.negativeTotal)
        : raw;
      let start = 0;
      let end = value;
      if (stackMode !== "none") {
        if (value >= 0) {
          start = positive;
          positive = addStackCoordinate(positive, value);
          end = positive;
        } else {
          start = negative;
          negative = addStackCoordinate(negative, value);
          end = negative;
        }
      }
      if (seriesIndex >= seriesStart) visible.push({ end, raw, start });
    }
    return visible;
  });
  return { domain: model.domains[stackMode], rows };
}

function verticalGeometry(start: number, end: number, scale: ChartScale): { top: number; height: number } {
  const upper = Math.max(start, end);
  const lower = Math.min(start, end);
  const projectedUpper = projectScaleValue(upper, scale);
  const projectedLower = projectScaleValue(lower, scale);
  return {
    top: (1 - projectedUpper) * 100,
    height: (projectedUpper - projectedLower) * 100,
  };
}

function horizontalGeometry(start: number, end: number, scale: ChartScale): { left: number; width: number } {
  const projectedStart = projectScaleValue(Math.min(start, end), scale);
  const projectedEnd = projectScaleValue(Math.max(start, end), scale);
  return {
    left: projectedStart * 100,
    width: (projectedEnd - projectedStart) * 100,
  };
}

function displaySeriesValue(series: WorkspaceStatisticSeries, compact = false): string {
  if (series.value === null) return "No value";
  return formatExactNumeric(series.exactValue, series.value, compact);
}

function seriesColor(index: number): string {
  return TIME_SERIES_COLORS[index % TIME_SERIES_COLORS.length];
}

function formatAxisTick(value: number, approximate: boolean, stackMode: StackMode): string {
  return `${approximate ? "≈" : ""}${COMPACT_NUMBER_FORMAT.format(value)}${stackMode === "stacked100" ? "%" : ""}`;
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
  seriesStart,
}: {
  activeRow: CategoricalChartRow | null;
  dimension: string;
  inspectorId: string;
  onBlur: () => void;
  onClose: () => void;
  onDrilldown: (row: WorkspaceStatistic) => void;
  onPointerLeave: () => void;
  rowIndex: number;
  series: StatisticSeriesDefinition[];
  seriesStart: number;
}) {
  if (activeRow === null) return null;
  const sourceRow = activeRow.row;
  const backendSeries = sourceRow.series !== undefined;
  const approximatePosition = series.some((definition) =>
    indexedRowSeries(activeRow, definition).coordinateApproximate === true,
  );
  return (
    <section
      className="visualization-tooltip"
      id={inspectorId}
      aria-label={`Values for ${sourceRow.level}`}
      data-categorical-inspector="true"
      data-testid="categorical-chart-tooltip"
      onBlurCapture={(event) => {
        if (!(event.relatedTarget instanceof Node) || !event.currentTarget.contains(event.relatedTarget)) {
          onBlur();
        }
      }}
      onPointerLeave={onPointerLeave}
    >
      <div className="visualization-tooltip-header">
        <strong title={sourceRow.level}>{sourceRow.level}</strong>
        <button
          type="button"
          aria-label="Close chart value inspector"
          onClick={onClose}
        >
          <AppIcon name="close" size="md" />
        </button>
      </div>
      {series.map((definition, seriesIndex) => {
        const value = indexedRowSeries(activeRow, definition);
        return (
          <span key={definition.key}>
            <i
              aria-hidden="true"
              style={{ backgroundColor: backendSeries ? seriesColor(seriesStart + seriesIndex) : categoryColor(sourceRow.level, rowIndex) }}
            />
            <span>{definition.label}</span>
            <b>{displaySeriesValue(value)}</b>
          </span>
        );
      })}
      {approximatePosition ? (
        <small className="visualization-tooltip-precision">Chart position is approximate; displayed server values are exact.</small>
      ) : null}
      {sourceRow.pivotable === false ? (
        <small className="visualization-tooltip-unavailable">Drilldown is unavailable for this typed value.</small>
      ) : (
        <button
          className="visualization-tooltip-action"
          type="button"
          onClick={() => onDrilldown(sourceRow)}
        >
          Add {dimension} value to search <AppIcon name="chevron-right" size="xs" />
        </button>
      )}
    </section>
  );
}

function CategoricalChart({
  dimension,
  horizontal,
  model,
  seriesEnd,
  seriesStart,
  showDataLabels,
  stackMode,
  onApplyPivot,
}: CategoricalChartProps) {
  const hintId = useId();
  const inspectorId = `${hintId}-inspector`;
  const buttonRefs = useRef<Array<HTMLButtonElement | null>>([]);
  const lastPointerTypeRef = useRef<string | null>(null);
  const [activeIndex, setActiveIndex] = useState<number | null>(null);
  const [pinnedIndex, setPinnedIndex] = useState<number | null>(null);
  const rows = model.rows;
  const boundedSeriesStart = Math.min(
    Math.max(0, seriesStart),
    Math.max(0, model.series.length - 1),
  );
  const boundedSeriesEnd = Math.min(
    model.series.length,
    Math.max(boundedSeriesStart + 1, seriesEnd),
  );
  const series = useMemo(
    () => model.series.slice(boundedSeriesStart, boundedSeriesEnd),
    [boundedSeriesEnd, boundedSeriesStart, model.series],
  );
  const [activeRowCount, setActiveRowCount] = useState(rows.length);
  const stackWindow = useMemo(
    () => categoricalStackWindow(model, boundedSeriesStart, boundedSeriesEnd, stackMode),
    [boundedSeriesEnd, boundedSeriesStart, model, stackMode],
  );
  const stackedRows = stackWindow.rows;
  const scale = useMemo(() => linearTickScale(stackWindow.domain), [stackWindow.domain]);
  const approximate = model.approximate;
  const activeRow = activeIndex === null ? null : rows[activeIndex] ?? null;
  const backendSeries = model.backendSeries;
  const inspectDescription = activeRow === null
    ? `Inspect ${dimension} categories. Use Left and Right arrow keys to move between categories.`
    : `${activeRow.row.level}. ${series.map((definition) => {
      const value = indexedRowSeries(activeRow, definition);
      return `${definition.label} ${displaySeriesValue(value)}${value.coordinateApproximate ? "; displayed value exact, chart position approximate" : ""}`;
    }).join(", ")}.${activeRow.row.pivotable === false ? " Drilldown is unavailable for this typed value." : ` Activate to add this ${dimension} value to the search.`}`;

  if (activeRowCount !== rows.length) {
    setActiveRowCount(rows.length);
    setActiveIndex((current) => current === null || rows.length === 0 ? null : Math.min(current, rows.length - 1));
    setPinnedIndex((current) => current === null || rows.length === 0 ? null : Math.min(current, rows.length - 1));
  }

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

  // The horizontal and column branches differ only in layout: the category
  // button contract and the inspector surface are one definition shared by both.
  function categoryButtonProps(row: WorkspaceStatistic, rowIndex: number, className: string) {
    return {
      "aria-controls": inspectorId,
      "aria-describedby": hintId,
      "aria-expanded": rowIndex === activeIndex,
      "aria-label": rowIndex === activeIndex
        ? inspectDescription
        : `${row.level}; inspect chart values`,
      className,
      onBlur: (event: FocusEvent<HTMLButtonElement>) => {
        if (
          event.relatedTarget instanceof Element
          && event.relatedTarget.closest("[data-categorical-inspector='true']") !== null
        ) {
          return;
        }
        if (pinnedIndex !== rowIndex) setActiveIndex(null);
      },
      onClick: () => handleCategoryClick(row, rowIndex),
      onFocus: () => setActiveIndex(rowIndex),
      onKeyDown: (event: KeyboardEvent<HTMLButtonElement>) => handleKeyDown(event, rowIndex),
      onPointerDown: (event: PointerEvent<HTMLButtonElement>) => {
        lastPointerTypeRef.current = event.pointerType;
        setActiveIndex(rowIndex);
        if (event.pointerType === "touch" || event.pointerType === "pen") {
          setPinnedIndex(rowIndex);
        } else {
          setPinnedIndex(null);
        }
        event.currentTarget.focus({ preventScroll: true });
      },
      onPointerEnter: () => setActiveIndex(rowIndex),
      onPointerLeave: handlePointerLeave,
      ref: (element: HTMLButtonElement | null) => { buttonRefs.current[rowIndex] = element; },
      type: "button" as const,
    };
  }

  const inspector = (
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
      seriesStart={boundedSeriesStart}
    />
  );

  const screenReaderHint = (
    <>
      <p className="sr-only" id={hintId}>Use arrow keys to move between categories. Home and End jump to the first and last category. Enter applies an available drilldown. Escape clears the value.</p>
      <output className="sr-only" aria-live="polite">{activeRow === null ? "" : inspectDescription}</output>
    </>
  );

  if (horizontal) {
    const renderedSeriesCount = stackMode === "none" ? series.length : 1;
    const minimumRowHeight = Math.max(30, (renderedSeriesCount * 17) + 8);
    return (
      <div className="visualization-chart visualization-chart--horizontal" data-testid="categorical-chart">
        <div className="visualization-horizontal-scroller">
          <div
            className="visualization-horizontal-surface"
            style={{
              minHeight: `max(100%, ${(rows.length * minimumRowHeight) + 39}px)`,
              minWidth: series.length > 3 ? `${560 + (series.length * 20)}px` : "520px",
            }}
          >
            <div className="visualization-horizontal-grid" aria-hidden="true">
              {scale.ticks.map((tick) => (
                <span
                  key={tick}
                  className={tick === 0 ? "visualization-grid-line--zero" : undefined}
                  style={{ left: `${projectScaleValue(tick, scale) * 100}%` }}
                />
              ))}
            </div>
            <div
              className="visualization-horizontal-groups"
              style={{ gridTemplateRows: `repeat(${Math.max(1, rows.length)}, minmax(${minimumRowHeight}px, 1fr))` }}
            >
              {rows.map((row, rowIndex) => (
                <button
                  key={row.row.id ?? row.row.level}
                  {...categoryButtonProps(row.row, rowIndex, "visualization-horizontal-group")}
                >
                  <strong title={row.row.level}>{row.row.level}</strong>
                  <span
                    className={`visualization-horizontal-bars${stackMode === "none" ? "" : " is-stacked"}`}
                    aria-hidden="true"
                  >
                    {series.map((definition, seriesIndex) => {
                      const item = indexedRowSeries(row, definition);
                      const stackedValue = stackedRows[rowIndex]?.[seriesIndex];
                      if (item.value === null || stackedValue === undefined || stackedValue.raw === null) {
                        return <span className="visualization-horizontal-slot" key={definition.key} />;
                      }
                      const geometry = horizontalGeometry(stackedValue.start, stackedValue.end, scale);
                      const color = backendSeries
                        ? seriesColor(boundedSeriesStart + seriesIndex)
                        : categoryColor(row.row.level, rowIndex);
                      return (
                        <span className="visualization-horizontal-slot" key={definition.key}>
                          <i
                            className="visualization-horizontal-bar"
                            data-chart-end={stackedValue.end}
                            data-chart-raw={item.value}
                            data-chart-start={stackedValue.start}
                            style={{ backgroundColor: color, left: `${geometry.left}%`, width: `${geometry.width}%` }}
                          />
                          {showDataLabels ? (
                            <b
                              className="visualization-horizontal-data-label"
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
            <div className="visualization-horizontal-axis" aria-hidden="true">
              {scale.ticks.toReversed().map((tick) => (
                <span key={tick}>{formatAxisTick(tick, approximate, stackMode)}</span>
              ))}
            </div>
            {inspector}
          </div>
        </div>
        {screenReaderHint}
      </div>
    );
  }

  const renderedSeriesCount = stackMode === "none" ? series.length : 1;
  const minimumGroupWidth = Math.max(72, (renderedSeriesCount * 24) + 24);
  return (
    <div className="visualization-chart" data-testid="categorical-chart">
      <div className="visualization-vertical-y-axis" aria-hidden="true">
        {scale.ticks.map((tick) => (
          <span key={tick}>{formatAxisTick(tick, approximate, stackMode)}</span>
        ))}
      </div>
      <div className="visualization-vertical-scroller">
        <div
          className="visualization-vertical-surface"
          style={{ minWidth: `max(100%, ${rows.length * minimumGroupWidth}px)` }}
        >
          <div className="visualization-vertical-grid" aria-hidden="true">
            {scale.ticks.map((tick) => (
              <span
                key={tick}
                className={tick === 0 ? "visualization-grid-line--zero" : undefined}
                style={{ top: `${(1 - projectScaleValue(tick, scale)) * 100}%` }}
              />
            ))}
          </div>
          <div
            className="visualization-vertical-groups"
            style={{ gridTemplateColumns: `repeat(${Math.max(1, rows.length)}, minmax(${minimumGroupWidth}px, 1fr))` }}
          >
            {rows.map((row, rowIndex) => (
              <button
                key={row.row.id ?? row.row.level}
                {...categoryButtonProps(row.row, rowIndex, "visualization-vertical-group")}
              >
                <span
                  className={`visualization-vertical-bars${stackMode === "none" ? "" : " is-stacked"}`}
                  aria-hidden="true"
                >
                  {series.map((definition, seriesIndex) => {
                    const item = indexedRowSeries(row, definition);
                    const stackedValue = stackedRows[rowIndex]?.[seriesIndex];
                    if (item.value === null || stackedValue === undefined || stackedValue.raw === null) {
                      return <span className="visualization-vertical-slot" key={definition.key} />;
                    }
                    const geometry = verticalGeometry(stackedValue.start, stackedValue.end, scale);
                    const color = backendSeries
                      ? seriesColor(boundedSeriesStart + seriesIndex)
                      : categoryColor(row.row.level, rowIndex);
                    const dataLabelTop = item.value >= 0
                      ? `max(2px, calc(${geometry.top}% - 17px))`
                      : `calc(${geometry.top + geometry.height}% + 3px)`;
                    return (
                      <span className="visualization-vertical-slot" key={definition.key}>
                        <i
                          className="visualization-vertical-bar"
                          data-chart-end={stackedValue.end}
                          data-chart-raw={item.value}
                          data-chart-start={stackedValue.start}
                          style={{
                            backgroundColor: color,
                            height: item.value === 0 ? "2px" : `${geometry.height}%`,
                            top: item.value === 0 ? `calc(${geometry.top}% - 1px)` : `${geometry.top}%`,
                          }}
                        />
                        {showDataLabels ? (
                          <b className="visualization-vertical-data-label" style={{ top: dataLabelTop }}>
                            {item.coordinateApproximate ? "≈" : ""}{displaySeriesValue(item, true)}
                          </b>
                        ) : null}
                      </span>
                    );
                  })}
                </span>
                <strong title={row.row.level}>{row.row.level}</strong>
              </button>
            ))}
          </div>
          {inspector}
        </div>
      </div>
      {screenReaderHint}
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
  stackMode,
  statisticsDimension,
  statisticsRows,
  timechartCoverage,
  timelinePoints,
  onApplyPivot,
  onChartStyleChange,
  onChartTitleChange,
  onCopySeriesLabel,
  onLegendPositionChange,
  onShowDataLabelsChange,
  onStackModeChange,
  onVisualizationEdited,
  previewTruncated,
}: VisualizationPanelProps) {
  const displayedStatisticsRows = useMemo(() => statisticsRows.length > MAX_CATEGORICAL_ROWS
    ? statisticsRows
      .map((row, index) => ({ row, index, magnitude: statisticMagnitude(row) }))
      .toSorted((left, right) => right.magnitude - left.magnitude || left.index - right.index)
      .slice(0, MAX_CATEGORICAL_ROWS)
      .map(({ row }) => row)
    : statisticsRows, [statisticsRows]);
  const categoricalModel = useMemo(
    () => categoricalChartModel(displayedStatisticsRows),
    [displayedStatisticsRows],
  );
  const categoricalSeries = categoricalModel.series;
  const timechartModel = useMemo(() => timelineChartModel(timelinePoints), [timelinePoints]);
  const timelineSeries = timechartModel.series.names;
  const [seriesOffset, setSeriesOffset] = useState(0);
  const activeSeriesCount = isTimechartResult ? timelineSeries.length : categoricalSeries.length;
  const maximumSeriesOffset = Math.floor(
    Math.max(0, activeSeriesCount - 1) / TIMELINE_SERIES_WINDOW_SIZE,
  ) * TIMELINE_SERIES_WINDOW_SIZE;
  const boundedSeriesOffset = Math.min(
    seriesOffset,
    maximumSeriesOffset,
  );
  const activeSeriesEnd = Math.min(
    activeSeriesCount,
    boundedSeriesOffset + TIMELINE_SERIES_WINDOW_SIZE,
  );
  const visibleTimelineSeries = timelineSeries.slice(boundedSeriesOffset, activeSeriesEnd);
  const visibleCategoricalSeries = categoricalSeries.slice(boundedSeriesOffset, activeSeriesEnd);
  const seriesWindowed = activeSeriesCount > TIMELINE_SERIES_WINDOW_SIZE;
  const hasApproximateCoordinates = isTimechartResult
    ? timechartModel.hasApproximateCoordinates
    : categoricalModel.approximate;
  const splitTimechart = isTimechartResult && timelineSeries.length > 1;
  const isTimeSeriesChart = isTimechartResult;
  const effectiveChartStyle = isTimeSeriesChart
    ? chartStyle === "area"
      ? "area"
      : chartStyle === "column" && !splitTimechart
        ? "column"
        : "line"
    : chartStyle;
  const hasCategoricalChart = isTimechartResult
    ? timelinePoints.length > 0
    : displayedStatisticsRows.length > 0 && categoricalSeries.length > 0;
  const backendCategoricalResult = categoricalModel.backendSeries;
  const categoricalSeriesResult = !isTimechartResult
    && categoricalModel.backendSeries
    && categoricalSeries.length > 0;
  const supportsStacking = splitTimechart || categoricalSeriesResult;
  const effectiveStackMode = supportsStacking ? stackMode : "none";
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
    <section id="panel-visualization" role="tabpanel" aria-labelledby="tab-visualization" className="visualization-panel">
      <header className="result-view-header">
        <div>
          <div className="result-title-line">
            <h2>{resolvedChartTitle.trim() || "Untitled visualization"}</h2>
            {isPreview ? <span className="preview-context-badge"><i aria-hidden="true" /> Live preview</span> : null}
            {!isPreview && isTimechartResult && timechartCoverage !== null && timechartCoverage.status !== "complete" ? (
              <span
                className={`badge ${timechartCoverage.status === "loading" ? "badge--info" : "badge--warning"}`}
                data-testid="timechart-coverage-badge"
                data-coverage={timechartCoverage.status}
              >
                {timechartCoverage.status === "loading" ? "Loading buckets" : "Incomplete"}
              </span>
            ) : null}
          </div>
          <p>{isPreview
            ? `${isTimechartResult
              ? "The chart updates as time-series rows arrive. Its scale and values may change until completion."
              : hasCategoricalChart
                ? "The chart updates as result rows arrive. Categories, values, and ordering remain provisional."
                : "Waiting for a preview result shape that can be charted."}${previewTruncated ? " The preview limit was reached; the final chart may include additional data." : ""}`
            : isTimechartResult
              ? `${timechartCoverage === null
                ? "Timechart across the submitted search range."
                : describeTimechartCoverage(timechartCoverage, timechartModel.points.at(-1)?.label ?? null)}${hasApproximateCoordinates ? " The plotted scale is approximate for values beyond the browser’s exact integer range; hover or focus a point for its exact server value." : ""}`
              : hasCategoricalChart
                ? backendCategoricalResult
                  ? `${categoricalSeries.length === 1 ? categoricalSeries[0].label : `${categoricalSeries.length} complete series`} grouped by ${statisticsDimension}.${statisticsRows.length > displayedStatisticsRows.length ? ` Showing the top ${displayedStatisticsRows.length} of ${statisticsRows.length} categories.` : ""}${hasApproximateCoordinates ? " The plotted scale is approximate for values beyond the browser’s exact integer range; exact server values appear on hover or focus." : ""}`
                  : "Aggregation of the displayed event set."
                : "This result shape cannot be represented faithfully as a categorical chart."}</p>
        </div>
        <fieldset className="chart-toggle">
          <legend className="sr-only">Chart style</legend>
          <button className={effectiveChartStyle === "column" ? "active" : ""} type="button" aria-pressed={effectiveChartStyle === "column"} disabled={!hasCategoricalChart || splitTimechart} title={splitTimechart ? "Split-series timecharts use Line or Area so no server series is collapsed" : !hasCategoricalChart ? "Column charts require one dimension and at least one numeric measure" : undefined} onClick={() => selectChartStyle("column")}><AppIcon name="column-chart" size="sm" /> Column</button>
          <button className={effectiveChartStyle === "horizontal" ? "active" : ""} type="button" aria-pressed={effectiveChartStyle === "horizontal"} disabled={isTimechartResult || !hasCategoricalChart} title={isTimechartResult ? "Bar charts require categorical results" : !hasCategoricalChart ? "Bar charts require one dimension and at least one numeric measure" : undefined} onClick={() => selectChartStyle("horizontal")}><AppIcon name="bar-chart" size="sm" /> Bar</button>
          <button className={effectiveChartStyle === "line" ? "active" : ""} type="button" aria-pressed={effectiveChartStyle === "line"} disabled={!isTimechartResult} title={!isTimechartResult ? "Line charts require time-series results" : undefined} onClick={() => selectChartStyle("line")}><AppIcon name="analytics" size="sm" /> Line</button>
          <button className={effectiveChartStyle === "area" ? "active" : ""} type="button" aria-pressed={effectiveChartStyle === "area"} disabled={!isTimechartResult} title={!isTimechartResult ? "Area charts require time-series results" : undefined} onClick={() => selectChartStyle("area")}><AppIcon name="analytics" size="sm" /> Area</button>
        </fieldset>
      </header>
      <div
        className={`visualization-canvas chart-${effectiveChartStyle} legend-${legendPosition}${isTimeSeriesChart ? " visualization-canvas--line" : ""}${!isTimechartResult ? " visualization-canvas--categorical" : ""}${isPreview ? " visualization-canvas--preview" : ""}`}
        data-stack-mode={effectiveStackMode}
        data-testid="visualization-chart"
      >
        {!hasCategoricalChart ? (
          <output className="visualization-empty-state">
            <span className="visualization-empty-state-icon" aria-hidden="true"><span /><span /><span /></span>
            <strong>No compatible chart for these results</strong>
            <p>{isPreview
              ? "The live preview has not produced a chart-compatible result shape yet. Statistics will update if compatible provisional rows arrive."
              : "Return one categorical dimension and at least one numeric measure, or use a timechart for a time-series visualization. The complete server result remains available in Statistics."}</p>
          </output>
        ) : isTimeSeriesChart ? (
          <TimeSeriesLineChart
            chartStyle={effectiveChartStyle === "area"
              ? "area"
              : effectiveChartStyle === "column"
                ? "column"
                : "line"}
            model={timechartModel}
            onCopySeriesLabel={onCopySeriesLabel}
            points={timelinePoints}
            seriesEnd={activeSeriesEnd}
            seriesStart={boundedSeriesOffset}
            showDataLabels={showDataLabels}
            stackMode={effectiveStackMode}
          />
        ) : (
          <CategoricalChart
            dimension={statisticsDimension}
            horizontal={effectiveChartStyle === "horizontal"}
            model={categoricalModel}
            seriesEnd={activeSeriesEnd}
            seriesStart={boundedSeriesOffset}
            showDataLabels={showDataLabels}
            stackMode={effectiveStackMode}
            onApplyPivot={onApplyPivot}
          />
        )}
        {!hasCategoricalChart || legendPosition === "none" ? null : (
          <div className="chart-legend">
            {isTimechartResult
              ? isTimeSeriesChart
                ? visibleTimelineSeries.map((name, index) => {
                  const visibleLabel = timelineSeriesDisplayName(name);
                  return (
                    <span key={name}>
                      <i style={{ backgroundColor: seriesColor(boundedSeriesOffset + index) }} />
                      {visibleLabel}
                      <Button
                        aria-label={`Copy series label ${visibleLabel}`}
                        icon
                        onClick={() => onCopySeriesLabel(visibleLabel)}
                        size="compact"
                        title={`Copy ${visibleLabel}`}
                        variant="ghost"
                      >
                        <AppIcon name="copy" size="xs" />
                      </Button>
                    </span>
                  );
                })
                : <span><i className="legend-info" />Events</span>
              : backendCategoricalResult
                ? visibleCategoricalSeries.map((series, index) => (
                  <span key={series.key}>
                    <i style={{ backgroundColor: seriesColor(boundedSeriesOffset + index) }} />
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
        <label htmlFor="visualization-panel-choice-816"><span>Legend</span><Select id="visualization-panel-choice-816" value={legendPosition} onValueChange={(selectedValue) => {
          onVisualizationEdited();
          onLegendPositionChange(selectedValue as LegendPosition);
        }}><SelectOption value="bottom">Bottom</SelectOption><SelectOption value="right">Right</SelectOption><SelectOption value="none">Hidden</SelectOption></Select></label>
        {supportsStacking ? (
          <label htmlFor="visualization-panel-choice-821"><span>Stacking</span><Select id="visualization-panel-choice-821" value={effectiveStackMode} onValueChange={(selectedValue) => {
            onVisualizationEdited();
            onStackModeChange(selectedValue as StackMode);
          }}><SelectOption value="none">None</SelectOption><SelectOption value="stacked">Stacked</SelectOption><SelectOption value="stacked100">100%</SelectOption></Select></label>
        ) : null}
        {isTimeSeriesChart ? (
          <>
            {effectiveChartStyle === "column" ? (
              <label><span>Data labels</span><input type="checkbox" checked={showDataLabels} onChange={(event) => {
                onVisualizationEdited();
                onShowDataLabelsChange(event.target.checked);
              }} /></label>
            ) : null}
            <div className="visualization-interaction-note"><strong>Inspect values</strong><span>Hover, tap, or focus the plot and use the arrow keys.</span></div>
          </>
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
        {seriesWindowed ? (
          <div className="visualization-interaction-note">
            <strong>Rendered series</strong>
            <span>Showing {NUMBER_FORMAT.format(boundedSeriesOffset + 1)}–{NUMBER_FORMAT.format(activeSeriesEnd)} of {NUMBER_FORMAT.format(activeSeriesCount)}. Every series remains available here and in Statistics.</span>
            <div className="visualization-series-controls">
              <button
                className="button button--secondary button--compact"
                type="button"
                disabled={boundedSeriesOffset === 0}
                onClick={() => setSeriesOffset(Math.max(0, boundedSeriesOffset - TIMELINE_SERIES_WINDOW_SIZE))}
              >
                Previous series
              </button>
              <button
                className="button button--secondary button--compact"
                type="button"
                disabled={activeSeriesEnd === activeSeriesCount}
                onClick={() => setSeriesOffset(activeSeriesEnd)}
              >
                Next series
              </button>
            </div>
          </div>
        ) : null}
      </aside>
    </section>
  );
}
