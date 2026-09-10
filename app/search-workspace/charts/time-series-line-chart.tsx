import {
  type KeyboardEvent,
  type PointerEvent,
  useEffect,
  useId,
  useMemo,
  useRef,
  useState,
} from "react";

import type { TimelinePoint } from "@/lib/demo/search-data";

import { COMPACT_NUMBER_FORMAT, NUMBER_FORMAT } from "../constants";
import { formatExactNumericText } from "../formatters";
import type { StackMode } from "../model";
import { linearTickScale } from "./chart-scale";
import type { StackedChartRow } from "./chart-stacking";

const VIEWBOX_WIDTH = 1000;
const VIEWBOX_HEIGHT = 300;
/**
 * The categorical ramp, in assignment order.
 *
 * These are `var()` references rather than hex so the palette is the token
 * layer's `--chart-series-*` and not a second copy of it: every consumer feeds
 * them to an inline `style`, where a custom property resolves exactly as it
 * does in a stylesheet, and nothing does colour arithmetic on a member.
 */
export const TIME_SERIES_COLORS = [
  "var(--chart-series-1)",
  "var(--chart-series-2)",
  "var(--chart-series-3)",
  "var(--chart-series-4)",
  "var(--chart-series-5)",
  "var(--chart-series-6)",
  "var(--chart-series-7)",
  "var(--chart-series-8)",
  "var(--chart-series-9)",
  "var(--chart-series-10)",
  "var(--chart-series-11)",
  "var(--chart-series-12)",
] as const;

interface TimeSeriesLineChartProps {
  chartStyle?: "area" | "column" | "line";
  points: TimelinePoint[];
  seriesEnd?: number;
  seriesLabel?: string;
  seriesStart?: number;
  showDataLabels?: boolean;
  stackMode?: StackMode;
}

interface TimeSeriesCoordinate {
  startY: number;
  x: number;
  y: number;
}

export interface TimelineColumnCoordinate {
  centerPercent: number;
  leftPercent: number;
  widthPercent: number;
}

interface TimelineCoordinateIndexEntry {
  index: number;
  x: number;
}

interface TimeSeriesPathGeometry {
  areaSegments: Array<{ key: string; points: string }>;
  columnPath: string;
  lineSegments: Array<{ key: string; points: string }>;
  name: string;
  seriesIndex: number;
}

/**
 * Lay out exact bucket starts by subtracting a nearby BigInt origin before
 * conversion to Number. Legacy rows without bounds retain ordinal spacing.
 */
export function timelineXCoordinates(points: readonly TimelinePoint[]): number[] {
  if (points.length <= 1) return points.map(() => VIEWBOX_WIDTH / 2);
  const exact = points.map((point) => point.timeCoordinateNanoseconds);
  if (exact.some((coordinate) => coordinate === undefined)) {
    return points.map((_point, index) => (index / (points.length - 1)) * VIEWBOX_WIDTH);
  }
  const coordinates = exact as bigint[];
  const minimum = coordinates.reduce((current, coordinate) => coordinate < current ? coordinate : current);
  const maximum = coordinates.reduce((current, coordinate) => coordinate > current ? coordinate : current);
  const range = maximum - minimum;
  if (range === 0n) return points.map(() => VIEWBOX_WIDTH / 2);
  const numericRange = Number(range);
  return coordinates.map((coordinate) => (Number(coordinate - minimum) / numericRange) * VIEWBOX_WIDTH);
}

/** Place exact bucket intervals within their complete authoritative extent. */
export function timelineColumnCoordinates(
  points: readonly TimelinePoint[],
): TimelineColumnCoordinate[] {
  if (points.length === 0) return [];
  const exact = points.map((point) => ({
    earliest: point.timeCoordinateNanoseconds,
    latest: point.timeLatestCoordinateNanoseconds,
  }));
  if (exact.some(({ earliest, latest }) => (
    earliest === undefined || latest === undefined || earliest >= latest
  ))) {
    return points.map((_point, index) => ({
      centerPercent: ((index + 0.5) / points.length) * 100,
      leftPercent: (index / points.length) * 100,
      widthPercent: 100 / points.length,
    }));
  }
  const bounds = exact as Array<{ earliest: bigint; latest: bigint }>;
  const minimum = bounds.reduce((current, bound) => bound.earliest < current ? bound.earliest : current, bounds[0].earliest);
  const maximum = bounds.reduce((current, bound) => bound.latest > current ? bound.latest : current, bounds[0].latest);
  const range = maximum - minimum;
  if (range <= 0n) return points.map(() => ({ centerPercent: 50, leftPercent: 0, widthPercent: 100 }));
  const numericRange = Number(range);
  return bounds.map(({ earliest, latest }) => {
    const leftPercent = (Number(earliest - minimum) / numericRange) * 100;
    const widthPercent = (Number(latest - earliest) / numericRange) * 100;
    return { centerPercent: leftPercent + widthPercent / 2, leftPercent, widthPercent };
  });
}

/** Collapse duplicate x positions and sort once for logarithmic pointer lookup. */
export function timelineCoordinateIndex(
  coordinates: readonly number[],
): TimelineCoordinateIndexEntry[] {
  const sorted = coordinates
    .map((x, index) => ({ index, x }))
    .toSorted((left, right) => left.x - right.x || left.index - right.index);
  return sorted.filter((entry, index) => index === 0 || entry.x !== sorted[index - 1].x);
}

export function nearestTimelineCoordinateIndex(
  coordinateIndex: readonly TimelineCoordinateIndexEntry[],
  targetX: number,
): number | null {
  if (coordinateIndex.length === 0) return null;
  let low = 0;
  let high = coordinateIndex.length;
  while (low < high) {
    const middle = low + Math.floor((high - low) / 2);
    if (coordinateIndex[middle].x < targetX) low = middle + 1;
    else high = middle;
  }
  const right = coordinateIndex[Math.min(low, coordinateIndex.length - 1)];
  const left = coordinateIndex[Math.max(0, low - 1)];
  const leftDistance = Math.abs(left.x - targetX);
  const rightDistance = Math.abs(right.x - targetX);
  if (leftDistance < rightDistance) return left.index;
  if (rightDistance < leftDistance) return right.index;
  return Math.min(left.index, right.index);
}

export function timelineSeriesNames(points: TimelinePoint[], fallbackLabel = "Events"): string[] {
  const names = new Set<string>();
  points.forEach((point) => Object.keys(point.series ?? {}).forEach((name) => names.add(name)));
  return names.size === 0 ? [fallbackLabel] : [...names];
}

export function timelineSeriesDisplayName(name: string): string {
  return /^(?:count|count\(.+\))$/i.test(name) ? "Events" : name;
}

function pointSeriesValue(point: TimelinePoint, name: string, fallbackLabel: string): number {
  return point.series?.[name] ?? (name === fallbackLabel ? point.count : 0);
}

function seriesColorIndex(index: number): number {
  return (index % TIME_SERIES_COLORS.length) + 1;
}

function pointSeriesCoordinate(
  point: TimelinePoint,
  name: string,
  fallbackLabel: string,
): number | null {
  const value = point.series?.[name] ?? (name === fallbackLabel ? point.count : null);
  return value !== null && Number.isFinite(value) ? value : null;
}

interface TimelineStackWindow {
  domain: number[];
  rows: StackedChartRow[];
}

export function timelinePointInspectionLabel(point: TimelinePoint): string {
  return point.earliest === undefined || point.latest === undefined
    ? point.label
    : `${point.label}, exact bucket ${point.earliest} to ${point.latest}`;
}

function normalizedStackValue(value: number, positiveTotal: number, negativeTotal: number): number {
  if (value > 0) return positiveTotal === 0 ? 0 : (value / positiveTotal) * 100;
  if (value < 0) return negativeTotal === 0 ? 0 : (value / negativeTotal) * 100;
  return 0;
}

/**
 * Scan all values to keep global axes and stack baselines stable, while only
 * allocating coordinate objects for the visible series window.
 */
function timelineStackWindow(
  points: readonly TimelinePoint[],
  seriesNames: readonly string[],
  seriesLabel: string,
  seriesStart: number,
  seriesEnd: number,
  stackMode: StackMode,
): TimelineStackWindow {
  const domain: number[] = [];
  const rows = points.map((point) => {
    let hasFinite = false;
    let minimum = 0;
    let maximum = 0;
    let negativeTotal = 0;
    let positiveTotal = 0;
    for (const name of seriesNames) {
      const raw = pointSeriesCoordinate(point, name, seriesLabel);
      if (raw === null) continue;
      hasFinite = true;
      minimum = Math.min(minimum, raw);
      maximum = Math.max(maximum, raw);
      if (raw < 0) negativeTotal += Math.abs(raw);
      else positiveTotal += raw;
    }
    if (hasFinite) {
      if (stackMode === "none") domain.push(minimum, maximum);
      else if (stackMode === "stacked100") {
        domain.push(negativeTotal === 0 ? 0 : -100, positiveTotal === 0 ? 0 : 100);
      } else {
        domain.push(-negativeTotal, positiveTotal);
      }
    }

    let negative = 0;
    let positive = 0;
    const visible: StackedChartRow = [];
    seriesNames.forEach((name, seriesIndex) => {
      const raw = pointSeriesCoordinate(point, name, seriesLabel);
      if (raw === null) {
        if (seriesIndex >= seriesStart && seriesIndex < seriesEnd) {
          visible.push({ end: 0, raw: null, start: 0 });
        }
        return;
      }
      const value = stackMode === "stacked100"
        ? normalizedStackValue(raw, positiveTotal, negativeTotal)
        : raw;
      let start = 0;
      let end = value;
      if (stackMode !== "none") {
        if (value >= 0) {
          start = positive;
          positive += value;
          end = positive;
        } else {
          start = negative;
          negative += value;
          end = negative;
        }
      }
      if (seriesIndex >= seriesStart && seriesIndex < seriesEnd) {
        visible.push({ end, raw, start });
      }
    });
    return visible;
  });
  return { domain, rows };
}

export function formatTimelineSeriesValue(
  point: TimelinePoint,
  name: string,
  fallbackLabel = "Events",
  compact = false,
): string {
  const exact = point.exactSeries?.[name]
    ?? (name === fallbackLabel ? point.exactCount : undefined);
  return exact === undefined
    ? (compact ? COMPACT_NUMBER_FORMAT : NUMBER_FORMAT).format(pointSeriesValue(point, name, fallbackLabel))
    : formatExactNumericText(exact, { compact, compactSuffix: "s" });
}

function contiguousSegments(
  coordinates: readonly (TimeSeriesCoordinate | null)[],
): TimeSeriesCoordinate[][] {
  const segments: TimeSeriesCoordinate[][] = [];
  let activeSegment: TimeSeriesCoordinate[] | null = null;
  for (const coordinate of coordinates) {
    if (coordinate === null) {
      activeSegment = null;
      continue;
    }
    if (activeSegment === null) {
      activeSegment = [];
      segments.push(activeSegment);
    }
    activeSegment.push(coordinate);
  }
  return segments;
}

export function timelineTickIndices(length: number, targetCount: number): number[] {
  if (length <= 1) return [0];
  return Array.from(
    new Set(Array.from({ length: Math.min(length, targetCount) }, (_, index) =>
      Math.round((index / Math.max(1, Math.min(length, targetCount) - 1)) * (length - 1)),
    )),
  );
}

function formatAxisTick(value: number, approximate: boolean, stackMode: StackMode): string {
  return `${approximate ? "≈" : ""}${COMPACT_NUMBER_FORMAT.format(value)}${stackMode === "stacked100" ? "%" : ""}`;
}

export function TimeSeriesLineChart({
  chartStyle = "line",
  points,
  seriesEnd,
  seriesLabel = "Events",
  seriesStart = 0,
  showDataLabels = false,
  stackMode = "none",
}: TimeSeriesLineChartProps) {
  const plotRef = useRef<HTMLDivElement>(null);
  const inspectButtonRef = useRef<HTMLButtonElement>(null);
  const hintId = useId();
  const [activeIndex, setActiveIndex] = useState<number | null>(null);
  const [activePoints, setActivePoints] = useState(points);
  const [plotWidth, setPlotWidth] = useState(900);
  const [keyboardActive, setKeyboardActive] = useState(false);
  const seriesNames = useMemo(() => timelineSeriesNames(points, seriesLabel), [points, seriesLabel]);
  const boundedSeriesStart = Math.min(Math.max(0, seriesStart), Math.max(0, seriesNames.length - 1));
  const boundedSeriesEnd = Math.min(
    seriesNames.length,
    Math.max(boundedSeriesStart + 1, seriesEnd ?? seriesNames.length),
  );
  const renderedSeriesNames = useMemo(
    () => seriesNames.slice(boundedSeriesStart, boundedSeriesEnd),
    [boundedSeriesEnd, boundedSeriesStart, seriesNames],
  );
  const stackWindow = useMemo(
    () => timelineStackWindow(
      points,
      seriesNames,
      seriesLabel,
      boundedSeriesStart,
      boundedSeriesEnd,
      stackMode,
    ),
    [boundedSeriesEnd, boundedSeriesStart, points, seriesLabel, seriesNames, stackMode],
  );
  const { minimum, maximum, ticks } = useMemo(
    () => linearTickScale(stackWindow.domain),
    [stackWindow.domain],
  );
  const axisRange = maximum - minimum;
  const hasApproximateCoordinates = points.some((point) => point.coordinateApproximate === true);
  const xCoordinates = useMemo(() => timelineXCoordinates(points), [points]);
  const columnCoordinates = useMemo(() => timelineColumnCoordinates(points), [points]);
  const interactionXCoordinates = useMemo(() => chartStyle === "column"
    ? columnCoordinates.map((coordinate) => (coordinate.centerPercent / 100) * VIEWBOX_WIDTH)
    : xCoordinates, [chartStyle, columnCoordinates, xCoordinates]);
  const pointerCoordinateIndex = useMemo(
    () => timelineCoordinateIndex(interactionXCoordinates),
    [interactionXCoordinates],
  );

  useEffect(() => {
    const plot = plotRef.current;
    if (plot === null) return;
    const updateWidth = () => setPlotWidth(plot.getBoundingClientRect().width);
    const widthFrame = window.requestAnimationFrame(updateWidth);
    const observer = new ResizeObserver(updateWidth);
    observer.observe(plot);
    return () => {
      window.cancelAnimationFrame(widthFrame);
      observer.disconnect();
    };
  }, []);

  if (activePoints !== points) {
    setActivePoints(points);
    setActiveIndex((current) => current === null || points.length === 0 ? null : Math.min(current, points.length - 1));
  }

  const seriesCoordinates = useMemo(() => renderedSeriesNames.map((name, renderedSeriesIndex) => {
    const seriesIndex = boundedSeriesStart + renderedSeriesIndex;
    return {
      name,
      seriesIndex,
      points: points.map((_point, index) => {
        const value = stackWindow.rows[index]?.[renderedSeriesIndex];
        if (value === undefined || value.raw === null) return null;
        const projectedEnd = Math.min(maximum, Math.max(minimum, value.end));
        const projectedStart = Math.min(maximum, Math.max(minimum, value.start));
        return {
          x: interactionXCoordinates[index] ?? VIEWBOX_WIDTH / 2,
          y: VIEWBOX_HEIGHT - ((projectedEnd - minimum) / axisRange) * VIEWBOX_HEIGHT,
          startY: VIEWBOX_HEIGHT - ((projectedStart - minimum) / axisRange) * VIEWBOX_HEIGHT,
        };
      }),
    };
  }), [axisRange, boundedSeriesStart, interactionXCoordinates, maximum, minimum, points, renderedSeriesNames, stackWindow.rows]);
  const seriesPathGeometry = useMemo<TimeSeriesPathGeometry[]>(() => seriesCoordinates.map((series) => {
    const segments = contiguousSegments(series.points);
    return {
      areaSegments: chartStyle === "area" ? segments.map((segment) => ({
        key: `${series.name}-area-${segment[0]?.x}`,
        points: [
          ...segment.map(({ x, y }) => `${x.toFixed(2)},${y.toFixed(2)}`),
          ...segment.toReversed().map(({ startY, x }) => `${x.toFixed(2)},${startY.toFixed(2)}`),
        ].join(" "),
      })) : [],
      columnPath: chartStyle === "column" ? series.points.flatMap((coordinate, index) => {
        const column = columnCoordinates[index];
        if (coordinate === null || column === undefined) return [];
        const left = (column.leftPercent / 100) * VIEWBOX_WIDTH;
        const right = ((column.leftPercent + column.widthPercent) / 100) * VIEWBOX_WIDTH;
        return [`M${left.toFixed(2)},${coordinate.startY.toFixed(2)}V${coordinate.y.toFixed(2)}H${right.toFixed(2)}V${coordinate.startY.toFixed(2)}Z`];
      }).join("") : "",
      lineSegments: chartStyle === "column" ? [] : segments.map((segment) => ({
        key: `${series.name}-line-${segment[0]?.x}`,
        points: segment.map(({ x, y }) => `${x.toFixed(2)},${y.toFixed(2)}`).join(" "),
      })),
      name: series.name,
      seriesIndex: series.seriesIndex,
    };
  }), [chartStyle, columnCoordinates, seriesCoordinates]);
  const seriesPaths = useMemo(() => (
    <>
      {chartStyle === "area" ? seriesPathGeometry.flatMap((series) => (
        series.areaSegments.map((segment) => (
          <polygon
            className="time-series-chart__area time-series-chart__series"
            data-series-color={seriesColorIndex(series.seriesIndex)}
            data-series-name={series.name}
            key={segment.key}
            points={segment.points}
          />
        ))
      )) : null}
      {chartStyle === "column" ? seriesPathGeometry.map((series) => (
        <path
          className="time-series-chart__columns time-series-chart__series"
          data-series-color={seriesColorIndex(series.seriesIndex)}
          data-series-name={series.name}
          d={series.columnPath}
          key={`${series.name}-columns`}
        />
      )) : null}
      {seriesPathGeometry.flatMap((series) => (
        series.lineSegments.map((segment) => (
          <polyline
            className="time-series-chart__line time-series-chart__series"
            data-series-color={seriesColorIndex(series.seriesIndex)}
            data-series-name={series.name}
            key={segment.key}
            points={segment.points}
          />
        ))
      ))}
    </>
  ), [chartStyle, seriesPathGeometry]);
  const xTicks = timelineTickIndices(points.length, plotWidth < 520 ? 3 : plotWidth < 820 ? 4 : 5);
  const activePoint = activeIndex === null ? null : points[activeIndex] ?? null;
  const activeCoordinates = activeIndex === null ? [] : seriesCoordinates.flatMap((series) => {
    const coordinate = series.points[activeIndex];
    return coordinate === undefined || coordinate === null
      ? []
      : [{ ...coordinate, name: series.name, seriesIndex: series.seriesIndex }];
  });
  const activeCoordinate = activeCoordinates.reduce<(typeof activeCoordinates)[number] | null>((highest, coordinate) =>
    highest === null || coordinate.y < highest.y ? coordinate : highest, null);
  const activeXPercent = activeCoordinate === null ? 0 : (activeCoordinate.x / VIEWBOX_WIDTH) * 100;
  const activeYPercent = activeCoordinate === null ? 0 : (activeCoordinate.y / VIEWBOX_HEIGHT) * 100;

  function indexFromPointer(event: PointerEvent<HTMLButtonElement>): number | null {
    if (points.length === 0) return null;
    const bounds = event.currentTarget.getBoundingClientRect();
    const ratio = Math.min(1, Math.max(0, (event.clientX - bounds.left) / Math.max(1, bounds.width)));
    const targetX = ratio * VIEWBOX_WIDTH;
    return nearestTimelineCoordinateIndex(pointerCoordinateIndex, targetX);
  }

  function inspectFromPointer(event: PointerEvent<HTMLButtonElement>) {
    setKeyboardActive(false);
    setActiveIndex(indexFromPointer(event));
  }

  function handleKeyDown(event: KeyboardEvent<HTMLButtonElement>) {
    if (points.length === 0) return;
    const current = activeIndex ?? 0;
    let next: number | null = current;
    if (event.key === "ArrowRight") next = Math.min(points.length - 1, current + 1);
    else if (event.key === "ArrowLeft") next = Math.max(0, current - 1);
    else if (event.key === "Home") next = 0;
    else if (event.key === "End") next = points.length - 1;
    else if (event.key === "Escape") next = null;
    else return;
    event.preventDefault();
    setKeyboardActive(next !== null);
    setActiveIndex(next);
  }

  if (points.length === 0) {
    return <div className="time-series-chart time-series-chart--empty">No time-series data to visualize.</div>;
  }

  const tooltipHorizontal = activeXPercent < 18 ? "start" : activeXPercent > 82 ? "end" : "center";
  const tooltipVertical = activeYPercent < 28 ? "below" : "above";
  const activePointLabel = activePoint === null ? "" : timelinePointInspectionLabel(activePoint);
  const activeDescription = activePoint === null
    ? `Inspect ${seriesLabel.toLowerCase()} over time. Use Left and Right arrow keys to move between time buckets.`
    : `${activePointLabel}, ${renderedSeriesNames.map((name) => `${timelineSeriesDisplayName(name)} ${formatTimelineSeriesValue(activePoint, name, seriesLabel)}`).join(", ")}${activePoint.coordinateApproximate ? ". Chart position is approximate; displayed values are exact." : ""}`;

  return (
    <div
      className="time-series-chart"
      data-chart-style={chartStyle}
      data-stack-mode={stackMode}
      data-testid="line-chart"
    >
      <div className="time-series-chart__y-axis" aria-hidden="true">
        {ticks.map((tick) => (
          <span key={tick}>{formatAxisTick(tick, hasApproximateCoordinates, stackMode)}</span>
        ))}
      </div>
      <div className="time-series-chart__plot" ref={plotRef}>
        <svg viewBox={`0 0 ${VIEWBOX_WIDTH} ${VIEWBOX_HEIGHT}`} preserveAspectRatio="none" aria-hidden="true">
          <g className="time-series-chart__grid">
            {ticks.map((tick) => {
              const y = VIEWBOX_HEIGHT - ((tick - minimum) / axisRange) * VIEWBOX_HEIGHT;
              return (
                <line
                  className={tick === 0 ? "is-zero" : undefined}
                  key={tick}
                  x1="0"
                  x2={VIEWBOX_WIDTH}
                  y1={y}
                  y2={y}
                />
              );
            })}
          </g>
          {seriesPaths}
          {chartStyle === "column" && showDataLabels ? xTicks.map((index) => {
            const point = points[index];
            const coordinate = seriesCoordinates[0]?.points[index];
            const column = columnCoordinates[index];
            if (point === undefined || coordinate === undefined || coordinate === null || column === undefined) return null;
            return (
              <text
                className="time-series-chart__data-label"
                key={`${point.id}-label`}
                textAnchor={column.leftPercent <= 0
                  ? "start"
                  : column.leftPercent + column.widthPercent >= 100
                    ? "end"
                    : "middle"}
                x={coordinate.x}
                y={Math.max(12, coordinate.y - 6)}
              >
                {point.coordinateApproximate ? "≈" : ""}{formatTimelineSeriesValue(point, "Events", "Events", true)}
              </text>
            );
          }) : null}
        </svg>
        <button
          ref={inspectButtonRef}
          type="button"
          className="time-series-chart__inspect"
          aria-describedby={hintId}
          aria-label={activeDescription}
          onBlur={() => { setKeyboardActive(false); setActiveIndex(null); }}
          onFocus={() => { setKeyboardActive(true); setActiveIndex((current) => current ?? 0); }}
          onKeyDown={handleKeyDown}
          onPointerDown={(event) => {
            inspectFromPointer(event);
            event.currentTarget.focus({ preventScroll: true });
          }}
          onPointerMove={inspectFromPointer}
          onPointerLeave={() => { if (!keyboardActive) setActiveIndex(null); }}
        >
          <span className="sr-only">Inspect chart values</span>
        </button>
        {activePoint === null || activeCoordinate === null ? null : (
          <>
            <span className="time-series-chart__crosshair" aria-hidden="true" style={{ left: `${activeXPercent}%` }} />
            {activeCoordinates.map((coordinate) => (
              <span
                className="time-series-chart__marker time-series-chart__series"
                aria-hidden="true"
                data-series-color={seriesColorIndex(coordinate.seriesIndex)}
                key={coordinate.name}
                style={{
                  left: `${(coordinate.x / VIEWBOX_WIDTH) * 100}%`,
                  top: `${(coordinate.y / VIEWBOX_HEIGHT) * 100}%`,
                }}
              />
            ))}
            <div
              className={`time-series-chart__tooltip is-${tooltipHorizontal} is-${tooltipVertical}`}
              role="tooltip"
              style={{ left: `${activeXPercent}%`, top: `${activeYPercent}%` }}
            >
              <strong>{activePointLabel}</strong>
              {renderedSeriesNames.map((name, renderedSeriesIndex) => (
                <span key={name}>
                  <i
                    aria-hidden="true"
                    className="time-series-chart__series"
                    data-series-color={seriesColorIndex(boundedSeriesStart + renderedSeriesIndex)}
                  />
                  <span>{timelineSeriesDisplayName(name)}</span>
                  <b>{formatTimelineSeriesValue(activePoint, name, seriesLabel)}</b>
                </span>
              ))}
            </div>
          </>
        )}
      </div>
      <div className="time-series-chart__axis-spacer" aria-hidden="true" />
      <div className="time-series-chart__x-axis" aria-hidden="true">
        {xTicks.map((index) => {
          const column = chartStyle === "column" ? columnCoordinates[index] : undefined;
          const xPercent = column === undefined
            ? (xCoordinates[index] / VIEWBOX_WIDTH) * 100
            : column.leftPercent <= 0
              ? 0
              : column.leftPercent + column.widthPercent >= 100
                ? 100
                : column.centerPercent;
          return (
            <span
              key={points[index].id}
              data-edge={xPercent <= 0 ? "start" : xPercent >= 100 ? "end" : undefined}
              style={{ left: `${xPercent}%` }}
            >
              {points[index].label}
            </span>
          );
        })}
      </div>
      <p className="sr-only" id={hintId}>Use Left and Right arrow keys to move through time buckets. Home and End jump to the first and last bucket. Escape clears the value.</p>
      <output className="sr-only" aria-live="polite">{activePoint === null ? "" : activeDescription}</output>
    </div>
  );
}
