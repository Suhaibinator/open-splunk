"use client";

import Link from "next/link";
import {
  type CSSProperties,
  useMemo,
  useState,
} from "react";

import { useRovingChartFocus } from "@/app/_components/use-roving-chart-focus";
import { TimeSeriesLineChart } from "@/app/search-workspace/charts/time-series-line-chart";
import type { TimelinePoint } from "@/lib/demo/search-data";
import { searchLaunchHref } from "@/lib/search/launch-url";
import { StatusLabel } from "../_components/status";
import { ProductShell } from "../_components/product-shell";

import { BackendDashboardManager } from "./backend-dashboard-manager";
import { Select, SelectOption } from "../_components/select";

const LATENCY_VALUES = [
  368, 412, 386, 447, 401, 462, 508, 476, 535, 492, 518, 451,
  486, 542, 617, 574, 503, 528, 682, 604, 711, 638, 552, 438,
];

const VOLUME_VALUES = [
  3_872, 4_968, 4_391, 5_712, 5_184, 6_149, 7_116, 6_432,
  5_779, 5_338, 6_041, 6_688, 7_712, 7_031, 6_264, 5_834,
  6_531, 6_947, 7_453, 6_192, 5_427, 6_084, 6_771, 6_329,
];

const NUMBER_FORMAT = new Intl.NumberFormat("en-US");
const TIME_FORMAT = new Intl.DateTimeFormat("en-US", {
  day: "numeric",
  hour: "numeric",
  month: "short",
  timeZone: "UTC",
});

const RANGE_OPTIONS = [
  {
    value: "24h",
    label: "Last 24 hours",
    earliest: "-24h",
    start: Date.UTC(2026, 6, 20, 16),
    durationHours: 24,
    volumeMultiplier: 1,
    bucketDescription: "Events indexed per hour",
    searchSpan: "1h",
  },
  {
    value: "7d",
    label: "Last 7 days",
    earliest: "-7d",
    start: Date.UTC(2026, 6, 14, 16),
    durationHours: 168,
    volumeMultiplier: 7,
    bucketDescription: "Indexed event volume across seven days",
    searchSpan: "6h",
  },
  {
    value: "30d",
    label: "Last 30 days",
    earliest: "-30d",
    start: Date.UTC(2026, 5, 21, 16),
    durationHours: 720,
    volumeMultiplier: 30,
    bucketDescription: "Indexed event volume across 30 days",
    searchSpan: "1d",
  },
] as const;

type RangeValue = (typeof RANGE_OPTIONS)[number]["value"];

const SERVICES = [
  ["gradethis-api", "Healthy", "84,219", "0.9%", "412 ms", "▁▂▂▃▂▃▅▃"],
  ["notification-worker", "Degraded", "18,402", "3.7%", "682 ms", "▂▂▃▅▄▆▇▆"],
  ["realtime-hub", "Healthy", "14,840", "0.4%", "74 ms", "▂▃▂▃▃▂▃▂"],
  ["grading-service", "Healthy", "11,000", "0.7%", "391 ms", "▂▂▃▂▄▃▃▄"],
] as const;

const NOTABLE_EVENTS = [
  {
    severity: "ERROR",
    message: "Database query failed while loading submission",
    source: "submission-service · 2 min ago",
    query: 'index=gradethis level=ERROR logger="submission-service" "Database query failed while loading submission"',
  },
  {
    severity: "WARN",
    message: "Request completed above latency threshold",
    source: "request-middleware · 4 min ago",
    query: 'index=gradethis level=WARN logger="request-middleware" "latency threshold"',
  },
  {
    severity: "ERROR",
    message: "Connection refused while delivering notification",
    source: "notification-worker · 7 min ago",
    query: 'index=gradethis level=ERROR logger="notification-worker" "connection refused"',
  },
  {
    severity: "WARN",
    message: "Client approaching request limit",
    source: "rate-limiter · 12 min ago",
    query: 'index=gradethis level=WARN logger="rate-limiter" "request limit"',
  },
] as const;

function timelinePoints(range: (typeof RANGE_OPTIONS)[number]): TimelinePoint[] {
  const step = (range.durationHours * 60 * 60 * 1_000) / (LATENCY_VALUES.length - 1);
  return LATENCY_VALUES.map((value, index) => {
    const label = TIME_FORMAT.format(new Date(range.start + step * index));
    const adjustedValue = Math.round(value * (range.value === "24h" ? 1 : 0.96 + (index % 5) * 0.018));
    return {
      id: `${range.value}-latency-${index}`,
      label,
      count: adjustedValue,
      series: { "p95 latency (ms)": adjustedValue },
    };
  });
}

function volumePoints(range: (typeof RANGE_OPTIONS)[number]) {
  const step = (range.durationHours * 60 * 60 * 1_000) / (VOLUME_VALUES.length - 1);
  return VOLUME_VALUES.map((value, index) => ({
    id: `${range.value}-volume-${index}`,
    label: TIME_FORMAT.format(new Date(range.start + step * index)),
    value: value * range.volumeMultiplier,
  }));
}

interface VolumeBarChartProps {
  points: ReturnType<typeof volumePoints>;
}

function VolumeBarChart({ points }: VolumeBarChartProps) {
  const { activeIndex, setActiveIndex, focusIndex, itemRefs, handleKeyDown, handleFocus } = useRovingChartFocus(points.length);
  const maximum = Math.max(...points.map((point) => point.value), 1);

  return (
    <div className="operations-volume-chart">
      <div className="operations-volume-scroll">
        <div className="operations-volume-scroll-content">
          <fieldset className="operations-volume-plot">
            <legend className="sr-only">Indexed events by time bucket</legend>
            {points.map((point, index) => {
              const isActive = activeIndex === index;
              return (
                <button
                  aria-describedby={isActive ? `${point.id}-tooltip` : undefined}
                  aria-label={`${point.label}: ${NUMBER_FORMAT.format(point.value)} indexed events`}
                  className={`operations-volume-bar ${isActive ? "operations-volume-bar--active" : ""}`}
                  key={point.id}
                  onBlur={() => setActiveIndex(null)}
                  onClick={() => setActiveIndex(index)}
                  onFocus={(event) => {
                    handleFocus(index);
                    event.currentTarget.scrollIntoView({ block: "nearest", inline: "nearest" });
                  }}
                  onKeyDown={(event) => handleKeyDown(event, index)}
                  onPointerEnter={() => setActiveIndex(index)}
                  onPointerLeave={(event) => {
                    if (document.activeElement !== event.currentTarget) setActiveIndex(null);
                  }}
                  ref={(element) => { itemRefs.current[index] = element; }}
                  tabIndex={focusIndex === index ? 0 : -1}
                  type="button"
                >
                  <span
                    className="operations-volume-fill"
                    style={{ "--bar-height": `${Math.max(5, (point.value / maximum) * 100)}%` } as CSSProperties}
                  />
                </button>
              );
            })}
          </fieldset>
          <div className="operations-volume-axis" aria-hidden="true"><span>{points[0]?.label}</span><span>{points.at(-1)?.label}</span></div>
        </div>
      </div>
      {activeIndex === null ? null : (
        <span className="operations-volume-tooltip" id={`${points[activeIndex]!.id}-tooltip`} role="tooltip">
          <strong>{points[activeIndex]!.label}</strong>
          <span>{NUMBER_FORMAT.format(points[activeIndex]!.value)} events</span>
        </span>
      )}
      <p className="operations-chart-hint">Hover, tap, or focus a bar. Scroll the chart or use arrow keys to inspect adjacent buckets.</p>
    </div>
  );
}

interface OperationsDashboardProps {
  apiBaseUrl: string;
  dataMode: "backend" | "demo";
}

export function OperationsDashboard({ apiBaseUrl, dataMode }: OperationsDashboardProps) {
  if (dataMode === "backend") {
    return <BackendDashboardManager apiBaseUrl={apiBaseUrl} />;
  }
  return <ProductShell activeSection="dashboards" apiBaseUrl={apiBaseUrl} appName="GradeThis Operations" dataMode="demo"><DemoOperationsDashboard /></ProductShell>;
}

function DemoOperationsDashboard() {
  const [rangeValue, setRangeValue] = useState<RangeValue>("24h");
  const range = RANGE_OPTIONS.find((option) => option.value === rangeValue) ?? RANGE_OPTIONS[0];
  const latency = useMemo(() => timelinePoints(range), [range]);
  const volume = useMemo(() => volumePoints(range), [range]);
  const totalRequests = volume.reduce((total, point) => total + point.value, 0);
  const errorEvents = Math.round(totalRequests * 0.0112);
  const latestLatency = latency.at(-1)?.count ?? 438;
  const previousLatency = latency.at(-2)?.count ?? latestLatency;
  const latencyDelta = latestLatency - previousLatency;
  const searchOptions = {
    earliest: range.earliest,
    latest: "now",
    label: range.label,
    run: true,
  };
  const fixtureSearchHref = (spl: string) => searchLaunchHref(spl, searchOptions);

  return (
    <div className="suite-page dashboard-page">
      <header className="dashboard-title-row">
        <div>
          <span className="suite-eyebrow">GRADETHIS OPERATIONS</span>
          <h1>Service overview</h1>
          <p>Preview production signals across the API, workers, and real-time services.</p>
        </div>
        <div className="operations-header-actions">
          <label htmlFor="operations-dashboard-choice-231" className="operations-range-picker">
            <span>Metrics range</span>
            <Select id="operations-dashboard-choice-231" value={rangeValue} onValueChange={(selectedValue) => setRangeValue(selectedValue as RangeValue)}>
              {RANGE_OPTIONS.map((option) => <SelectOption key={option.value} value={option.value}>{option.label}</SelectOption>)}
            </Select>
          </label>
          <span className="badge badge--outline operations-preview-badge">Preview data</span>
          <span className="operations-update-status">Fixture timestamp: Jul 21, 4:00 PM · Range scopes request and latency metrics; service health is the current fixture.</span>
        </div>
      </header>

      <section className="dashboard-metrics" aria-label="Key service metrics">
        <article><small>Total requests</small><strong>{NUMBER_FORMAT.format(totalRequests)}</strong><span className="metric-positive">↑ 6.3%</span><p>vs previous {range.label.toLowerCase()}</p></article>
        <article><small>Error rate</small><strong>1.12%</strong><span className="metric-negative">↑ 0.18%</span><p>{NUMBER_FORMAT.format(errorEvents)} error events</p></article>
        <article><small>p95 latency</small><strong>{NUMBER_FORMAT.format(latestLatency)} ms</strong><span className={latencyDelta <= 0 ? "metric-positive" : "metric-negative"}>{latencyDelta <= 0 ? "↓" : "↑"} {NUMBER_FORMAT.format(Math.abs(latencyDelta))} ms</span><p>Latest bucket · all API routes</p></article>
        <article><small>Active services</small><strong>7 / 7</strong><span className="metric-negative">1 degraded</span><p>notification-worker needs attention</p></article>
      </section>

      <div className="dashboard-grid">
        <section className="suite-card dashboard-panel dashboard-panel--wide">
          <header className="suite-card-header">
            <div><h2>API latency</h2><p>p95 response duration over time</p></div>
            <Link href={fixtureSearchHref("index=gradethis duration_ms=* | stats p95(duration_ms) AS p95_ms BY path | sort -p95_ms")}>Open in Search</Link>
          </header>
          <div className="operations-line-chart">
            <span className="operations-chart-unit">milliseconds</span>
            <TimeSeriesLineChart points={latency} seriesLabel="p95 latency (ms)" />
            <p className="operations-chart-hint">Hover, tap, or focus the plot. Use arrow keys to inspect adjacent buckets.</p>
          </div>
        </section>

        <section className="suite-card dashboard-panel">
          <header className="suite-card-header">
            <div><h2>Event volume</h2><p>{range.bucketDescription}</p></div>
            <Link href={fixtureSearchHref(`index=gradethis | timechart span=${range.searchSpan} count BY level`)}>Inspect</Link>
          </header>
          <VolumeBarChart points={volume} />
        </section>

        <section className="suite-card dashboard-panel">
          <header className="suite-card-header">
            <div><h2>Errors by service</h2><p>Share of {NUMBER_FORMAT.format(errorEvents)} errors</p></div>
            <Link href={fixtureSearchHref("index=gradethis level=ERROR | stats count by service")}>View events</Link>
          </header>
          <div className="service-breakdown">
            <figure className="donut-chart operations-donut-figure"><figcaption className="sr-only">Errors by service: gradethis-api 48.2%, notification-worker 27.4%, realtime-hub 16.1%, other 8.3%</figcaption><span><strong>{NUMBER_FORMAT.format(errorEvents)}</strong><small>errors</small></span></figure>
            <ol><li><i className="donut-color-1" /><span>gradethis-api</span><strong>48.2%</strong></li><li><i className="donut-color-2" /><span>notification-worker</span><strong>27.4%</strong></li><li><i className="donut-color-3" /><span>realtime-hub</span><strong>16.1%</strong></li><li><i className="donut-color-4" /><span>other</span><strong>8.3%</strong></li></ol>
          </div>
        </section>

        <section className="suite-card dashboard-panel dashboard-panel--wide">
          <header className="suite-card-header">
            <div><h2>Service health</h2><p>Current request volume, latency, and errors</p></div>
            <span className="badge badge--outline" title="Service configuration requires the management API.">Read only</span>
          </header>
          <div className="table-wrap">
            <table className="table service-table">
              <thead><tr><th scope="col">Service</th><th scope="col">Status</th><th scope="col">Requests</th><th scope="col">Error rate</th><th scope="col">p95 latency</th><th scope="col">Trend</th></tr></thead>
              <tbody>{SERVICES.map(([service, status, requests, errors, serviceLatency, trend]) => (
                <tr key={service}>
                  <td><Link className="operations-service-link" href={fixtureSearchHref(`index=gradethis service="${service}" | stats count p95(duration_ms) as p95_ms`)}>{service}</Link></td>
                  <td><StatusLabel tone={status === "Healthy" ? "success" : "warning"}>{status}</StatusLabel></td>
                  <td>{requests}</td><td>{errors}</td><td>{serviceLatency}</td><td><span className={status === "Healthy" ? "sparkline-good" : "sparkline-warn"}>{trend}</span></td>
                </tr>
              ))}</tbody>
            </table>
          </div>
        </section>

        <section className="suite-card dashboard-panel">
          <header className="suite-card-header">
            <div><h2>Recent notable events</h2><p>Errors and elevated warnings</p></div>
            <Link href={fixtureSearchHref("index=gradethis (level=ERROR OR level=WARN) | sort -_time")}>All events</Link>
          </header>
          <ol className="notable-events operations-notable-list">
            {NOTABLE_EVENTS.map((event) => (
              <li key={event.message}>
                <span className={`badge ${event.severity === "ERROR" ? "badge--error" : "badge--warning"}`}>{event.severity}</span>
                <div><Link href={fixtureSearchHref(event.query)}>{event.message}</Link><small>{event.source}</small></div>
              </li>
            ))}
          </ol>
        </section>
      </div>
    </div>
  );
}
