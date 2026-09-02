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
import { stackChartRows, stackedChartDomain } from "./chart-stacking";

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
  chartStyle?: "area" | "line";
  points: TimelinePoint[];
  seriesLabel?: string;
  stackMode?: StackMode;
}

interface TimeSeriesCoordinate {
  startY: number;
  x: number;
  y: number;
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

function tickIndices(length: number, targetCount: number): number[] {
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
  seriesLabel = "Events",
  stackMode = "none",
}: TimeSeriesLineChartProps) {
  const plotRef = useRef<HTMLDivElement>(null);
  const inspectButtonRef = useRef<HTMLButtonElement>(null);
  const hintId = useId();
  const [activeIndex, setActiveIndex] = useState<number | null>(null);
  const [plotWidth, setPlotWidth] = useState(900);
  const [keyboardActive, setKeyboardActive] = useState(false);
  const seriesNames = useMemo(() => timelineSeriesNames(points, seriesLabel), [points, seriesLabel]);
  const stackedRows = useMemo(() => stackChartRows(
    points.map((point) => seriesNames.map((name) => pointSeriesCoordinate(point, name, seriesLabel))),
    stackMode,
  ), [points, seriesLabel, seriesNames, stackMode]);
  const { minimum, maximum, ticks } = useMemo(
    () => linearTickScale(stackedChartDomain(stackedRows)),
    [stackedRows],
  );
  const axisRange = maximum - minimum;
  const hasApproximateCoordinates = points.some((point) => point.coordinateApproximate === true);

  useEffect(() => {
    const plot = plotRef.current;
    if (plot === null) return;
    const updateWidth = () => setPlotWidth(plot.getBoundingClientRect().width);
    updateWidth();
    const observer = new ResizeObserver(updateWidth);
    observer.observe(plot);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    setActiveIndex((current) => current === null || points.length === 0 ? null : Math.min(current, points.length - 1));
  }, [points]);

  const seriesCoordinates = useMemo(() => seriesNames.map((name, seriesIndex) => ({
    name,
    points: points.map((_point, index) => {
      const value = stackedRows[index]?.[seriesIndex];
      if (value === undefined || value.raw === null) return null;
      const projectedEnd = Math.min(maximum, Math.max(minimum, value.end));
      const projectedStart = Math.min(maximum, Math.max(minimum, value.start));
      return {
        x: points.length <= 1 ? VIEWBOX_WIDTH / 2 : (index / (points.length - 1)) * VIEWBOX_WIDTH,
        y: VIEWBOX_HEIGHT - ((projectedEnd - minimum) / axisRange) * VIEWBOX_HEIGHT,
        startY: VIEWBOX_HEIGHT - ((projectedStart - minimum) / axisRange) * VIEWBOX_HEIGHT,
      };
    }),
  })), [axisRange, maximum, minimum, points, seriesNames, stackedRows]);
  const xTicks = tickIndices(points.length, plotWidth < 520 ? 3 : plotWidth < 820 ? 4 : 5);
  const activePoint = activeIndex === null ? null : points[activeIndex] ?? null;
  const activeCoordinates = activeIndex === null ? [] : seriesCoordinates.flatMap((series, seriesIndex) => {
    const coordinate = series.points[activeIndex];
    return coordinate === undefined || coordinate === null
      ? []
      : [{ ...coordinate, name: series.name, seriesIndex }];
  });
  const activeCoordinate = activeCoordinates.reduce<(typeof activeCoordinates)[number] | null>((highest, coordinate) =>
    highest === null || coordinate.y < highest.y ? coordinate : highest, null);
  const activeXPercent = activeCoordinate === null ? 0 : (activeCoordinate.x / VIEWBOX_WIDTH) * 100;
  const activeYPercent = activeCoordinate === null ? 0 : (activeCoordinate.y / VIEWBOX_HEIGHT) * 100;

  function indexFromPointer(event: PointerEvent<HTMLButtonElement>): number | null {
    if (points.length === 0) return null;
    const bounds = event.currentTarget.getBoundingClientRect();
    const ratio = Math.min(1, Math.max(0, (event.clientX - bounds.left) / Math.max(1, bounds.width)));
    return Math.round(ratio * Math.max(0, points.length - 1));
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
  const activeDescription = activePoint === null
    ? `Inspect ${seriesLabel.toLowerCase()} over time. Use Left and Right arrow keys to move between time buckets.`
    : `${activePoint.label}, ${seriesNames.map((name) => `${timelineSeriesDisplayName(name)} ${formatTimelineSeriesValue(activePoint, name, seriesLabel)}`).join(", ")}${activePoint.coordinateApproximate ? ". Chart position is approximate; displayed values are exact." : ""}`;

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
          {chartStyle === "area" ? seriesCoordinates.flatMap((series, seriesIndex) => (
            contiguousSegments(series.points).map((segment) => (
              <polygon
                className="time-series-chart__area time-series-chart__series"
                data-series-color={seriesColorIndex(seriesIndex)}
                data-series-name={series.name}
                key={`${series.name}-area-${segment[0]?.x}`}
                points={[
                  ...segment.map(({ x, y }) => `${x.toFixed(2)},${y.toFixed(2)}`),
                  ...segment.toReversed().map(({ startY, x }) => `${x.toFixed(2)},${startY.toFixed(2)}`),
                ].join(" ")}
              />
            ))
          )) : null}
          {seriesCoordinates.flatMap((series, seriesIndex) => (
            contiguousSegments(series.points).map((segment) => (
              <polyline
                className="time-series-chart__line time-series-chart__series"
                data-series-color={seriesColorIndex(seriesIndex)}
                data-series-name={series.name}
                key={`${series.name}-line-${segment[0]?.x}`}
                points={segment.map(({ x, y }) => `${x.toFixed(2)},${y.toFixed(2)}`).join(" ")}
              />
            ))
          ))}
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
              <strong>{activePoint.label}</strong>
              {seriesNames.map((name, seriesIndex) => (
                <span key={name}>
                  <i
                    aria-hidden="true"
                    className="time-series-chart__series"
                    data-series-color={seriesColorIndex(seriesIndex)}
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
        {xTicks.map((index) => <span key={points[index].id}>{points[index].label}</span>)}
      </div>
      <p className="sr-only" id={hintId}>Use Left and Right arrow keys to move through time buckets. Home and End jump to the first and last bucket. Escape clears the value.</p>
      <output className="sr-only" aria-live="polite">{activePoint === null ? "" : activeDescription}</output>
    </div>
  );
}
