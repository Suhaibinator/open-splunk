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

const VIEWBOX_WIDTH = 1000;
const VIEWBOX_HEIGHT = 300;
export const TIME_SERIES_COLORS = ["#5f9f3a", "#2f7fa6", "#e49a2c", "#8b67a8", "#c6534c", "#4d9a8a"] as const;

interface TimeSeriesLineChartProps {
  points: TimelinePoint[];
  seriesLabel?: string;
}

function niceStep(maximum: number, targetIntervals = 4): number {
  if (maximum <= 0) return 1;
  const roughStep = maximum / targetIntervals;
  const power = 10 ** Math.floor(Math.log10(roughStep));
  const fraction = roughStep / power;
  const niceFraction = fraction <= 1 ? 1 : fraction <= 2 ? 2 : fraction <= 5 ? 5 : 10;
  return niceFraction * power;
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

function axisScale(points: TimelinePoint[], seriesNames: string[], fallbackLabel: string): { maximum: number; ticks: number[] } {
  const dataMaximum = Math.max(0, ...points.flatMap((point) => seriesNames.map((name) => pointSeriesValue(point, name, fallbackLabel))));
  const step = niceStep(dataMaximum);
  const maximum = Math.max(step, Math.ceil(dataMaximum / step) * step);
  const ticks: number[] = [];
  for (let value = maximum; value >= 0; value -= step) ticks.push(value);
  return { maximum, ticks };
}

function tickIndices(length: number, targetCount: number): number[] {
  if (length <= 1) return [0];
  return Array.from(
    new Set(Array.from({ length: Math.min(length, targetCount) }, (_, index) =>
      Math.round((index / Math.max(1, Math.min(length, targetCount) - 1)) * (length - 1)),
    )),
  );
}

export function TimeSeriesLineChart({ points, seriesLabel = "Events" }: TimeSeriesLineChartProps) {
  const plotRef = useRef<HTMLDivElement>(null);
  const inspectButtonRef = useRef<HTMLButtonElement>(null);
  const hintId = useId();
  const [activeIndex, setActiveIndex] = useState<number | null>(null);
  const [plotWidth, setPlotWidth] = useState(900);
  const [keyboardActive, setKeyboardActive] = useState(false);
  const seriesNames = useMemo(() => timelineSeriesNames(points, seriesLabel), [points, seriesLabel]);
  const { maximum, ticks } = useMemo(() => axisScale(points, seriesNames, seriesLabel), [points, seriesLabel, seriesNames]);

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

  const seriesCoordinates = useMemo(() => seriesNames.map((name) => ({
    name,
    points: points.map((point, index) => ({
      x: points.length <= 1 ? VIEWBOX_WIDTH / 2 : (index / (points.length - 1)) * VIEWBOX_WIDTH,
      y: VIEWBOX_HEIGHT - (pointSeriesValue(point, name, seriesLabel) / maximum) * VIEWBOX_HEIGHT,
    })),
  })), [maximum, points, seriesLabel, seriesNames]);
  const xTicks = tickIndices(points.length, plotWidth < 520 ? 3 : plotWidth < 820 ? 4 : 5);
  const activePoint = activeIndex === null ? null : points[activeIndex] ?? null;
  const activeCoordinates = activeIndex === null ? [] : seriesCoordinates.flatMap((series, seriesIndex) => {
    const coordinate = series.points[activeIndex];
    return coordinate === undefined ? [] : [{ ...coordinate, name: series.name, seriesIndex }];
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
    : `${activePoint.label}, ${seriesNames.map((name) => `${timelineSeriesDisplayName(name)} ${NUMBER_FORMAT.format(pointSeriesValue(activePoint, name, seriesLabel))}`).join(", ")}`;

  return (
    <div className="time-series-chart" data-testid="line-chart">
      <div className="time-series-chart__y-axis" aria-hidden="true">
        {ticks.map((tick) => <span key={tick}>{COMPACT_NUMBER_FORMAT.format(tick)}</span>)}
      </div>
      <div className="time-series-chart__plot" ref={plotRef}>
        <svg viewBox={`0 0 ${VIEWBOX_WIDTH} ${VIEWBOX_HEIGHT}`} preserveAspectRatio="none" aria-hidden="true">
          <g className="time-series-chart__grid">
            {ticks.map((tick) => {
              const y = VIEWBOX_HEIGHT - (tick / maximum) * VIEWBOX_HEIGHT;
              return <line key={tick} x1="0" x2={VIEWBOX_WIDTH} y1={y} y2={y} />;
            })}
          </g>
          {seriesCoordinates.map((series, seriesIndex) => (
            <polyline
              className="time-series-chart__line"
              key={series.name}
              points={series.points.map(({ x, y }) => `${x.toFixed(2)},${y.toFixed(2)}`).join(" ")}
              style={{ stroke: TIME_SERIES_COLORS[seriesIndex % TIME_SERIES_COLORS.length] }}
            />
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
                className="time-series-chart__marker"
                aria-hidden="true"
                key={coordinate.name}
                style={{
                  borderColor: TIME_SERIES_COLORS[coordinate.seriesIndex % TIME_SERIES_COLORS.length],
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
                  <i aria-hidden="true" style={{ backgroundColor: TIME_SERIES_COLORS[seriesIndex % TIME_SERIES_COLORS.length] }} />
                  <span>{timelineSeriesDisplayName(name)}</span>
                  <b>{NUMBER_FORMAT.format(pointSeriesValue(activePoint, name, seriesLabel))}</b>
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
