"use client";

import Link from "next/link";
import {
  type CSSProperties,
  type PointerEvent as ReactPointerEvent,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
} from "react";

import { ServerFeature } from "@/gen/ts/open_splunk/system_api";
import {
  createOpenSplunkApiClient,
  getSystemBootstrap,
  isOptionalRouteUnavailable,
  supportsServerFeature,
  type SystemBootstrapModel,
} from "@/lib/api";
import { createErrorMessage } from "@/lib/error-message";
import {
  backendAppHref,
  currentBackendAppId,
  replaceBackendAppId,
  subscribeToBackendAppId,
} from "@/lib/search/app-navigation";
import { historySearchLaunchHref, searchLaunchHref } from "@/lib/search/launch-url";
import { isSplFieldRepresentable } from "@/lib/search/query-pivots";
import { formatSplValue } from "@/lib/search/spl-syntax";

import { BackendResourceState } from "../_components/backend-resource-state";
import { AppIcon } from "../_components/app-icon";
import { PageHeading } from "../_components/product-shell";
import { useRovingChartFocus } from "../_components/use-roving-chart-focus";
import {
  deriveAnalyticsWorkload,
  loadAnalyticsFields,
  loadAnalyticsHistory,
  type AnalyticsFieldProfile,
  type AnalyticsFieldSnapshot,
  type AnalyticsHistoryRecord,
  type AnalyticsHistorySnapshot,
  type AnalyticsWorkload,
} from "./analytics-data";
import styles from "./analytics.module.css";
import { AnalyticsSampleStatus } from "./analytics-sample-status";

type RangeKey = "1h" | "24h" | "7d";
type EnvironmentKey = "all" | "production" | "staging";
type CoverageFilter = "all" | "complete" | "partial" | "sparse";
type FieldSort = "coverage" | "cardinality" | "name";

interface FieldProfile {
  name: string;
  type: string;
  coverage: number;
  cardinality: number | bigint | null;
  example: string | null;
}

interface QueryInsight {
  title: string;
  detail: string;
  impact: string;
  severity: "high" | "medium" | "low";
  query: string;
  signal: string;
}

const RANGE_OPTIONS: Array<{
  value: RangeKey;
  label: string;
  earliest: string;
  multiplier: number;
  bucket: string;
}> = [
  { value: "1h", label: "Last 60 minutes", earliest: "-1h", multiplier: 0.05, bucket: "5 minutes" },
  { value: "24h", label: "Last 24 hours", earliest: "-24h", multiplier: 1, bucket: "2 hours" },
  { value: "7d", label: "Last 7 days", earliest: "-7d", multiplier: 6.7, bucket: "12 hours" },
];

const ENVIRONMENT_OPTIONS: Array<{ value: EnvironmentKey; label: string; multiplier: number }> = [
  { value: "all", label: "All environments", multiplier: 1 },
  { value: "production", label: "Production", multiplier: 0.89 },
  { value: "staging", label: "Staging", multiplier: 0.11 },
];

const BASE_P95 = [
  1.36, 1.18, 1.24, 1.07, 1.16, 1.31, 1.42, 1.28, 1.19, 1.73, 1.48, 1.34,
  1.22, 1.09, 1.17, 1.26, 1.54, 1.39, 1.31, 1.21, 1.46, 1.33, 1.27, 1.18,
];

const FIELD_PROFILES: FieldProfile[] = [
  { name: "_time", type: "timestamp", coverage: 100, cardinality: 12_846, example: "2026-07-21T22:42:17.483Z" },
  { name: "host", type: "string", coverage: 100, cardinality: 3, example: "api-prod-03" },
  { name: "source", type: "string", coverage: 100, cardinality: 4, example: "/var/log/gradethis/app.json" },
  { name: "sourcetype", type: "string", coverage: 100, cardinality: 2, example: "go:zap:json" },
  { name: "service", type: "string", coverage: 96.8, cardinality: 7, example: "gradethis-api" },
  { name: "level", type: "string", coverage: 94.2, cardinality: 4, example: "ERROR" },
  { name: "duration_ms", type: "number", coverage: 77.1, cardinality: 1_842, example: "827" },
  { name: "trace_id", type: "string", coverage: 71.4, cardinality: 10_293, example: "4b9f0f06…" },
  { name: "path", type: "string", coverage: 66.9, cardinality: 42, example: "/api/submissions/grade" },
  { name: "user_id", type: "string", coverage: 36.2, cardinality: 3_106, example: "usr_8W4H20" },
  { name: "submission_id", type: "string", coverage: 22.5, cardinality: 1_904, example: "sub_01J1QF8…" },
  { name: "retry_count", type: "number", coverage: 8.7, cardinality: 5, example: "3" },
];

const QUERY_INSIGHTS: QueryInsight[] = [
  {
    title: "Wide scan on submission errors",
    detail: "The query reads 14.7× more events than it returns. Add service and sourcetype constraints before transforming.",
    impact: "≈ 38 s/day saved",
    severity: "high",
    query: "index=gradethis service=gradethis-api sourcetype=go:zap:json level=ERROR submission_id=* | stats count by path",
    signal: "14.7× scan ratio",
  },
  {
    title: "Wildcard path aggregation",
    detail: "A scheduled search groups every request path. Filtering to API traffic first reduces high-cardinality work.",
    impact: "≈ 21% fewer rows",
    severity: "medium",
    query: "index=gradethis logger=request-middleware path=/api/* | stats p95(duration_ms) as p95_ms count by path | sort -p95_ms",
    signal: "42 path values",
  },
  {
    title: "Repeated latency pipeline",
    detail: "Three similar searches can share one grouped p95 result by service.",
    impact: "3 queries → 1",
    severity: "low",
    query: "index=gradethis duration_ms=* | stats p95(duration_ms) AS p95_ms BY service | sort -p95_ms",
    signal: "Runs every 15 min",
  },
];

const SLOW_SEARCHES = [
  {
    name: "Submission failure investigation",
    owner: "Administrator",
    duration: 4.82,
    scan: "3.8M",
    query: "index=gradethis submission_id=* (level=ERROR OR level=WARN) | stats count by trace_id | sort -count",
  },
  {
    name: "Latency by endpoint",
    owner: "Administrator",
    duration: 3.41,
    scan: "2.1M",
    query: "index=gradethis duration_ms=* | stats p95(duration_ms) as p95_ms count by path | sort -p95_ms",
  },
  {
    name: "Authentication anomaly review",
    owner: "Security team",
    duration: 2.76,
    scan: "1.4M",
    query: "index=gradethis (status=401 OR status=403) | stats count by host path",
  },
  {
    name: "Worker retry pressure",
    owner: "Platform team",
    duration: 1.93,
    scan: "892K",
    query: "index=gradethis logger=notification-worker retry_count>0 | timechart span=1h count by operation",
  },
] as const;

const NUMBER_FORMAT = new Intl.NumberFormat("en-US");
const DECIMAL_FORMAT = new Intl.NumberFormat("en-US", { maximumFractionDigits: 1 });

function formatCardinality(value: number | bigint | null) {
  if (value === null) return "Not provided";
  if (typeof value === "bigint") return value.toLocaleString();
  if (value >= 1_000) return `${DECIMAL_FORMAT.format(value / 1_000)}K`;
  return NUMBER_FORMAT.format(value);
}

function relativeBucketLabel(remainingMinutes: number) {
  if (remainingMinutes <= 0) return "Now";
  if (remainingMinutes < 60) return `${remainingMinutes} min ago`;
  const totalHours = Math.round(remainingMinutes / 60);
  if (totalHours < 24) return `${totalHours}h ago`;
  const days = Math.floor(totalHours / 24);
  const hours = totalHours % 24;
  return hours === 0 ? `${days}d ago` : `${days}d ${hours}h ago`;
}

function PerformanceTrend({ values, labels }: { values: Array<number | null>; labels: string[] }) {
  const width = 720;
  const height = 184;
  const left = 18;
  const right = 18;
  const top = 14;
  const bottom = 25;
  const plotWidth = width - left - right;
  const plotHeight = height - top - bottom;
  const observedValues = values.flatMap((value) => value === null ? [] : [value]);
  const maximum = Math.max(...observedValues) * 1.12;
  const minimum = Math.max(0, Math.min(...observedValues) * 0.78);
  const coordinates = values.map((value, index) => value === null ? null : ({
    value,
    valueIndex: index,
    x: left + (index / Math.max(1, values.length - 1)) * plotWidth,
    y: top + (1 - (value - minimum) / Math.max(0.01, maximum - minimum)) * plotHeight,
  }));
  const points = coordinates.flatMap((point) => point === null ? [] : [point]);
  const { activeIndex, setActiveIndex, focusIndex, itemRefs, handleKeyDown, handleFocus } = useRovingChartFocus(points.length);
  const lineSegments = coordinates.slice(1).flatMap((point, index) => {
    const previous = coordinates[index];
    return previous === null || point === null ? [] : [{ previous, point }];
  });
  const areaPoints = points.length === values.length
    ? `${left},${height - bottom} ${points.map((point) => `${point.x},${point.y}`).join(" ")} ${width - right},${height - bottom}`
    : null;

  function activateNearestPoint(event: ReactPointerEvent<HTMLDivElement>) {
    if (points.length === 0) return;
    const bounds = event.currentTarget.getBoundingClientRect();
    if (bounds.width <= 0) return;
    const ratio = Math.max(0, Math.min(1, (event.clientX - bounds.left) / bounds.width));
    const sourceIndex = Math.round(ratio * Math.max(0, values.length - 1));
    const nearestPointIndex = points.reduce((nearest, point, index) => (
      Math.abs(point.valueIndex - sourceIndex) < Math.abs(points[nearest].valueIndex - sourceIndex) ? index : nearest
    ), 0);
    setActiveIndex(nearestPointIndex);
  }

  return (
    <figure className={styles.trendFigure}>
      <div className={styles.trendPlot}>
        <svg aria-hidden="true" preserveAspectRatio="none" viewBox={`0 0 ${width} ${height}`}>
          <defs>
            <linearGradient id="analytics-trend-fill" x1="0" x2="0" y1="0" y2="1">
              {/* `stop-color` as an attribute is not a CSS declaration, so the
                  token has to come through `style` for `var()` to resolve. */}
              <stop offset="0%" style={{ stopColor: "var(--chart-series-1)", stopOpacity: 0.24 }} />
              <stop offset="100%" style={{ stopColor: "var(--chart-series-1)", stopOpacity: 0.02 }} />
            </linearGradient>
          </defs>
          {[0.25, 0.5, 0.75, 1].map((position) => (
            <line className={styles.gridLine} key={position} x1={left} x2={width - right} y1={top + plotHeight * position} y2={top + plotHeight * position} />
          ))}
          {areaPoints === null ? null : <polygon fill="url(#analytics-trend-fill)" points={areaPoints} />}
          {lineSegments.map(({ previous, point }) => (
            <line className={styles.trendLine} key={`${previous.valueIndex}-${point.valueIndex}`} x1={previous.x} x2={point.x} y1={previous.y} y2={point.y} />
          ))}
        </svg>
        <div
          className={styles.trendPoints}
          aria-label="p95 search runtime trend"
          onPointerDown={activateNearestPoint}
          onPointerMove={activateNearestPoint}
          onPointerLeave={(event) => {
            if (!event.currentTarget.contains(document.activeElement)) setActiveIndex(null);
          }}
        >
          {points.map((coordinate, index) => {
            const isActive = activeIndex === index;
            const edge = coordinate.valueIndex < 3 ? styles.tooltipStart : coordinate.valueIndex > values.length - 4 ? styles.tooltipEnd : "";
            return (
              <button
                aria-label={`${labels[coordinate.valueIndex]}: ${coordinate.value.toFixed(2)} seconds p95 runtime`}
                className={`${styles.trendPoint} ${isActive ? styles.trendPointActive : ""}`}
                key={`${labels[coordinate.valueIndex]}-${coordinate.valueIndex}`}
                onBlur={() => setActiveIndex(null)}
                onFocus={() => handleFocus(index)}
                onKeyDown={(event) => handleKeyDown(event, index)}
                ref={(element) => { itemRefs.current[index] = element; }}
                style={{
                  "--point-x": `${(coordinate.x / width) * 100}%`,
                  "--point-y": `${(coordinate.y / height) * 100}%`,
                } as CSSProperties}
                tabIndex={focusIndex === index ? 0 : -1}
                type="button"
              >
                <span className={styles.pointMarker} />
                {isActive ? (
                  <span className={`${styles.trendTooltip} ${edge}`} role="tooltip">
                    <strong>{coordinate.value.toFixed(2)} s</strong>
                    <small>{labels[coordinate.valueIndex]}</small>
                  </span>
                ) : null}
              </button>
            );
          })}
        </div>
        <span className={`${styles.axisLabel} ${styles.axisStart}`} aria-hidden="true">{labels[0]}</span>
        <span className={`${styles.axisLabel} ${styles.axisMiddle}`} aria-hidden="true">{labels[Math.floor(labels.length / 2)]}</span>
        <span className={`${styles.axisLabel} ${styles.axisEnd}`} aria-hidden="true">{labels.at(-1)}</span>
      </div>
      <figcaption>Hover, tap, or focus a point for its value. Use arrow keys to move between buckets.</figcaption>
    </figure>
  );
}

function fieldMatchesCoverage(field: FieldProfile, filter: CoverageFilter) {
  if (filter === "complete") return field.coverage >= 90;
  if (filter === "partial") return field.coverage >= 40 && field.coverage < 90;
  if (filter === "sparse") return field.coverage < 40;
  return true;
}

interface AnalyticsConsoleProps {
  dataMode: "backend" | "demo";
  apiBaseUrl: string;
}

export function AnalyticsConsole({ dataMode, apiBaseUrl }: AnalyticsConsoleProps) {
  if (dataMode === "backend") return <BackendAnalyticsConsole apiBaseUrl={apiBaseUrl} />;
  return <DemoAnalyticsConsole />;
}

function DemoAnalyticsConsole() {
  const [rangeKey, setRangeKey] = useState<RangeKey>("24h");
  const [environmentKey, setEnvironmentKey] = useState<EnvironmentKey>("all");
  const [fieldQuery, setFieldQuery] = useState("");
  const [coverageFilter, setCoverageFilter] = useState<CoverageFilter>("all");
  const [fieldSort, setFieldSort] = useState<FieldSort>("coverage");

  const range = RANGE_OPTIONS.find((option) => option.value === rangeKey) ?? RANGE_OPTIONS[1];
  const environment = ENVIRONMENT_OPTIONS.find((option) => option.value === environmentKey) ?? ENVIRONMENT_OPTIONS[0];
  const searchOptions = {
    earliest: range.earliest,
    latest: "now",
    label: range.label,
    run: true,
  };
  const fixtureSearchHref = (spl: string) => searchLaunchHref(spl, searchOptions);
  const environmentSPL = environmentKey === "all" ? "" : ` environment=${environmentKey}`;
  const scale = range.multiplier * environment.multiplier;
  const searchCount = Math.max(18, Math.round(2_841 * scale));
  const scannedEvents = Math.round(284_219_000 * scale);
  const failedSearches = Math.max(1, Math.round(searchCount * 0.006));

  const trendValues = useMemo(() => {
    const environmentAdjustment = environmentKey === "production" ? 1.05 : environmentKey === "staging" ? 0.82 : 1;
    const rangeAdjustment = rangeKey === "1h" ? 0.91 : rangeKey === "7d" ? 1.08 : 1;
    return BASE_P95.map((value, index) => Number((value * environmentAdjustment * rangeAdjustment * (1 + (index % 4) * 0.006)).toFixed(2)));
  }, [environmentKey, rangeKey]);

  const trendLabels = useMemo(() => {
    const rangeMinutes = rangeKey === "1h" ? 60 : rangeKey === "7d" ? 7 * 24 * 60 : 24 * 60;
    return BASE_P95.map((_, index) => {
      const remaining = Math.round(rangeMinutes * (1 - index / Math.max(1, BASE_P95.length - 1)));
      return relativeBucketLabel(remaining);
    });
  }, [rangeKey]);

  const visibleFields = useMemo(() => {
    const normalized = fieldQuery.trim().toLowerCase();
    return FIELD_PROFILES.filter((field) => (
      fieldMatchesCoverage(field, coverageFilter)
      && (normalized.length === 0 || `${field.name} ${field.type} ${field.example}`.toLowerCase().includes(normalized))
    )).toSorted((left, right) => {
      if (fieldSort === "name") return left.name.localeCompare(right.name);
      if (fieldSort === "cardinality") return Number(right.cardinality ?? -1) - Number(left.cardinality ?? -1);
      return right.coverage - left.coverage;
    });
  }, [coverageFilter, fieldQuery, fieldSort]);

  function clearFieldFilters() {
    setFieldQuery("");
    setCoverageFilter("all");
    setFieldSort("coverage");
  }

  return (
    <div className={`suite-page ${styles.page}`}>
      <PageHeading
        eyebrow="SEARCH & REPORTING"
        title="Analytics"
        description="Explore preview search-performance fixtures, query cost, and field coverage."
        actions={(
          <>
            <span className="badge badge--outline" data-testid="analytics-updated">Preview data</span>
            <Link className="button button--primary" href={fixtureSearchHref(`index=gradethis${environmentSPL}`)}>Open Search</Link>
          </>
        )}
      />

      <section className={styles.contextBar} aria-label="Analytics context">
        <div>
          <span className={styles.contextIcon} aria-hidden="true">⌁</span>
          <div><strong>Search workload</strong><small>Filters update summary, trend, and sample-count fixtures; insight lists remain illustrative.</small></div>
        </div>
        <label>
          <span>Time range</span>
          <select data-testid="analytics-range" value={rangeKey} onChange={(event) => setRangeKey(event.target.value as RangeKey)}>
            {RANGE_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
          </select>
        </label>
        <label>
          <span>Environment</span>
          <select data-testid="analytics-environment" value={environmentKey} onChange={(event) => setEnvironmentKey(event.target.value as EnvironmentKey)}>
            {ENVIRONMENT_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
          </select>
        </label>
      </section>

      <section className={styles.metricGrid} aria-label="Search analytics summary">
        <Link title="Open a representative grouped search" href={fixtureSearchHref(`index=gradethis${environmentSPL} | stats count by service | sort -count`)}>
          <span>Searches run</span><strong>{NUMBER_FORMAT.format(searchCount)}</strong><small>↑ 8.4% from prior period</small><i aria-hidden="true">↗</i>
        </Link>
        <Link title="Open a representative success-status search" href={fixtureSearchHref(`index=gradethis${environmentSPL} (status=200 OR status=201) | stats count by status`)}>
          <span>Success rate</span><strong>99.4%</strong><small>{NUMBER_FORMAT.format(failedSearches)} failed searches</small><i aria-hidden="true">↗</i>
        </Link>
        <Link title="Open a representative latency search" href={fixtureSearchHref(`index=gradethis${environmentSPL} duration_ms=* | stats p95(duration_ms) as p95_ms`)}>
          <span>Median runtime</span><strong>1.18 s</strong><small>p95 is {trendValues.at(-1)?.toFixed(2)} s</small><i aria-hidden="true">↗</i>
        </Link>
        <Link href={fixtureSearchHref(`index=gradethis${environmentSPL} | stats count by sourcetype | sort -count`)}>
          <span>Events scanned</span><strong>{scannedEvents >= 1_000_000 ? `${DECIMAL_FORMAT.format(scannedEvents / 1_000_000)}M` : `${DECIMAL_FORMAT.format(scannedEvents / 1_000)}K`}</strong><small>21.8 scanned per result</small><i aria-hidden="true">↗</i>
        </Link>
      </section>

      <div className={styles.primaryGrid}>
        <section className={`suite-card ${styles.performancePanel}`} aria-labelledby="performance-title">
          <header className={styles.panelHeader}>
            <div><h2 id="performance-title">Search performance</h2><p>p95 runtime in {range.bucket} buckets</p></div>
            <div className={styles.legend}><span /><span>p95 runtime</span><b>seconds</b></div>
          </header>
          <PerformanceTrend values={trendValues} labels={trendLabels} />
          <footer className={styles.performanceFooter}>
            <div><span>Fastest</span><strong>{Math.min(...trendValues).toFixed(2)} s</strong></div>
            <div><span>Typical p95</span><strong>{(trendValues.reduce((sum, value) => sum + value, 0) / trendValues.length).toFixed(2)} s</strong></div>
            <div><span>Slowest</span><strong>{Math.max(...trendValues).toFixed(2)} s</strong></div>
            <Link href={fixtureSearchHref(`index=gradethis${environmentSPL} duration_ms=* | stats p95(duration_ms) AS p95_ms BY service | sort -p95_ms`)}>Investigate latency <AppIcon name="chevron-right" size="xs" /></Link>
          </footer>
        </section>

        <aside className={`suite-card ${styles.insightsPanel}`} aria-labelledby="insights-title">
          <header className={styles.panelHeader}>
            <div><h2 id="insights-title">Query insights</h2><p>Highest-value optimization opportunities</p></div>
            <span className={styles.insightCount}>{QUERY_INSIGHTS.length}</span>
          </header>
          <ol className={styles.insightList}>
            {QUERY_INSIGHTS.map((insight) => (
              <li key={insight.title}>
                <div className={styles.insightTopline}>
                  <span className={`${styles.severity} ${styles[`severity${insight.severity}`]}`}>{insight.severity}</span>
                  <small>{insight.signal}</small>
                </div>
                <h3>{insight.title}</h3>
                <p>{insight.detail}</p>
                <footer><strong>{insight.impact}</strong><Link href={fixtureSearchHref(insight.query)}>Inspect SPL <AppIcon name="chevron-right" size="xs" /></Link></footer>
              </li>
            ))}
          </ol>
        </aside>
      </div>

      <div className={styles.secondaryGrid}>
        <section className={`suite-card ${styles.fieldsPanel}`} aria-labelledby="fields-title">
          <header className={styles.panelHeader}>
            <div><h2 id="fields-title">Field coverage</h2><p>Presence and cardinality across {NUMBER_FORMAT.format(Math.round(12_846 * scale))} sampled events</p></div>
            <span className={styles.resultCount}><strong>{visibleFields.length}</strong> {visibleFields.length === 1 ? "field" : "fields"}</span>
          </header>
          <div className={styles.fieldToolbar}>
            <label className={styles.fieldSearch}>
              <span className="sr-only">Filter fields</span><i aria-hidden="true"><AppIcon name="search" size="sm" /></i>
              <input data-testid="analytics-field-filter" type="search" placeholder="Filter fields or values" value={fieldQuery} onChange={(event) => setFieldQuery(event.target.value)} />
            </label>
            <label>
              <span>Coverage</span>
              <select data-testid="analytics-coverage" value={coverageFilter} onChange={(event) => setCoverageFilter(event.target.value as CoverageFilter)}>
                <option value="all">Any coverage</option>
                <option value="complete">90–100%</option>
                <option value="partial">40–89%</option>
                <option value="sparse">Below 40%</option>
              </select>
            </label>
            <label>
              <span>Sort</span>
              <select data-testid="analytics-field-sort" value={fieldSort} onChange={(event) => setFieldSort(event.target.value as FieldSort)}>
                <option value="coverage">Coverage</option>
                <option value="cardinality">Cardinality</option>
                <option value="name">Field name</option>
              </select>
            </label>
          </div>

          {visibleFields.length === 0 ? (
            <div className={styles.emptyFields}>
              <span aria-hidden="true"><AppIcon name="search" size="lg" /></span><strong>No fields match these filters</strong><p>Clear the filters to return to the complete field profile.</p>
              <button className="button" onClick={clearFieldFilters} type="button">Clear filters</button>
            </div>
          ) : (
            <div className={styles.fieldList}>
              <div className={styles.fieldListHeader} aria-hidden="true"><span>Field</span><span>Coverage</span><span>Distinct</span><span>Example</span><span /></div>
              <ul>
                {visibleFields.map((field) => (
                  <li key={field.name}>
                    <div className={styles.fieldIdentity}><code>{field.name}</code><span className={styles.fieldType}>{field.type}</span></div>
                    <div className={styles.coverageCell}>
                      <span><i style={{ width: `${field.coverage}%` }} /></span><strong>{field.coverage.toFixed(field.coverage % 1 === 0 ? 0 : 1)}%</strong>
                    </div>
                    <span className={styles.cardinality}>{formatCardinality(field.cardinality)}</span>
                    <code className={styles.example}>{field.example}</code>
                    <Link aria-label={`Analyze ${field.name} in Search`} href={fixtureSearchHref(`index=gradethis${environmentSPL} ${field.name}=* | stats count by ${field.name} | sort -count`)}>Analyze <AppIcon name="chevron-right" size="xs" /></Link>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </section>

        <section className={`suite-card ${styles.slowestPanel}`} aria-labelledby="slowest-title">
          <header className={styles.panelHeader}>
            <div><h2 id="slowest-title">Slowest recurring searches</h2><p>Average completed runtime for this period</p></div>
          </header>
          <ol className={styles.slowestList}>
            {SLOW_SEARCHES.map((search, index) => (
              <li key={search.name}>
                <span className={styles.rank}>{index + 1}</span>
                <div className={styles.searchDetail}>
                  <Link href={fixtureSearchHref(search.query)}>{search.name}</Link>
                  <small>{search.owner} · {search.scan} scanned</small>
                  <span className={styles.durationTrack}><i style={{ width: `${(search.duration / SLOW_SEARCHES[0].duration) * 100}%` }} /></span>
                </div>
                <strong>{search.duration.toFixed(2)} s</strong>
              </li>
            ))}
          </ol>
          <footer className={styles.slowestFooter}>
            <span>Ordered by average runtime</span>
            <Link href={fixtureSearchHref(`index=gradethis${environmentSPL} duration_ms=* | stats p95(duration_ms) as p95_ms count by service | sort -p95_ms`)}>View complete workload <AppIcon name="chevron-right" size="xs" /></Link>
          </footer>
        </section>
      </div>
    </div>
  );
}

type BackendLoadState = "loading" | "available" | "unavailable" | "error";
type FieldLoadState = "idle" | BackendLoadState;

const backendErrorMessage = createErrorMessage("The backend returned an unusable analytics response.");
const COMPACT_FORMAT = new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 });

function rangeMilliseconds(rangeKey: RangeKey): number {
  if (rangeKey === "1h") return 60 * 60 * 1_000;
  if (rangeKey === "7d") return 7 * 24 * 60 * 60 * 1_000;
  return 24 * 60 * 60 * 1_000;
}

function formatCounter(value: bigint): string {
  return COMPACT_FORMAT.format(value);
}

function formatBytes(value: bigint): string {
  const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
  let unitIndex = 0;
  let divisor = 1n;
  while (unitIndex < units.length - 1 && value / divisor >= 1_024n) {
    divisor *= 1_024n;
    unitIndex += 1;
  }
  if (unitIndex === 0) return `${value.toLocaleString()} B`;
  const tenths = (value * 10n + divisor / 2n) / divisor;
  return `${Number(tenths) / 10} ${units[unitIndex]}`;
}

function formatRuntime(milliseconds: number | null): string {
  if (milliseconds === null) return "Not reported";
  if (milliseconds < 1_000) return `${Math.round(milliseconds)} ms`;
  return `${(milliseconds / 1_000).toFixed(milliseconds < 10_000 ? 2 : 1)} s`;
}

function appLabel(app: SystemBootstrapModel["apps"][number]): string {
  return app.displayName || app.slug || app.appId;
}

function historyTitle(entry: AnalyticsHistoryRecord): string {
  const firstLine = entry.spl.split("\n", 1)[0].trim();
  return firstLine.length > 74 ? `${firstLine.slice(0, 73)}…` : firstLine;
}

function historyScopeLabel(entry: AnalyticsHistoryRecord): string {
  return entry.effectiveIndexScope.length === 0
    ? "No admitted index scope"
    : entry.effectiveIndexScope.map((index) => `index=${index}`).join(", ");
}

function backendTrendLabels(workload: AnalyticsWorkload, rangeKey: RangeKey): string[] {
  const options: Intl.DateTimeFormatOptions = rangeKey === "7d"
    ? { month: "short", day: "numeric", hour: "numeric" }
    : { hour: "numeric", minute: "2-digit" };
  const formatter = new Intl.DateTimeFormat("en-US", options);
  return workload.trend.map((bucket) => formatter.format(bucket.start));
}

function compareCardinality(left: FieldProfile["cardinality"], right: FieldProfile["cardinality"]): number {
  if (left === null) return right === null ? 0 : 1;
  if (right === null) return -1;
  const leftValue = typeof left === "bigint" ? left : BigInt(left);
  const rightValue = typeof right === "bigint" ? right : BigInt(right);
  return leftValue === rightValue ? 0 : leftValue > rightValue ? -1 : 1;
}

function backendFieldForDisplay(field: AnalyticsFieldProfile): FieldProfile {
  return {
    name: field.name,
    type: field.type,
    coverage: field.coverage,
    cardinality: field.cardinality,
    example: null,
  };
}

function fieldAnalysisHref(indexName: string, fieldName: string, earliest: string): string | null {
  if (!isSplFieldRepresentable(fieldName)) return null;
  const base = `index=${formatSplValue(indexName)}`;
  const spl = `${base} ${fieldName}=* | stats count by ${fieldName} | sort -count`;
  return searchLaunchHref(spl, { earliest, latest: "now", run: false });
}

function BackendAnalyticsConsole({ apiBaseUrl }: { apiBaseUrl: string }) {
  const client = useMemo(() => createOpenSplunkApiClient({ baseUrl: apiBaseUrl }), [apiBaseUrl]);
  const preferredAppId = useSyncExternalStore(
    subscribeToBackendAppId,
    currentBackendAppId,
    () => undefined,
  );
  const [rangeKey, setRangeKey] = useState<RangeKey>("24h");
  const [appId, setAppId] = useState("all");
  const [fieldIndexName, setFieldIndexName] = useState("");
  const [fieldQuery, setFieldQuery] = useState("");
  const [coverageFilter, setCoverageFilter] = useState<CoverageFilter>("all");
  const [fieldSort, setFieldSort] = useState<FieldSort>("coverage");
  const [generation, setGeneration] = useState(0);
  const [bootstrapState, setBootstrapState] = useState<Exclude<BackendLoadState, "unavailable">>("loading");
  const [bootstrap, setBootstrap] = useState<SystemBootstrapModel | null>(null);
  const [bootstrapError, setBootstrapError] = useState<string | null>(null);
  const [historyState, setHistoryState] = useState<BackendLoadState>("loading");
  const [historySnapshot, setHistorySnapshot] = useState<AnalyticsHistorySnapshot | null>(null);
  const [historyRange, setHistoryRange] = useState<{ start: Date; end: Date } | null>(null);
  const [historyError, setHistoryError] = useState<string | null>(null);
  const [fieldState, setFieldState] = useState<FieldLoadState>("idle");
  const [fieldSnapshot, setFieldSnapshot] = useState<AnalyticsFieldSnapshot | null>(null);
  const [fieldError, setFieldError] = useState<string | null>(null);
  const historyControllerRef = useRef<AbortController | null>(null);
  const fieldControllerRef = useRef<AbortController | null>(null);
  const appliedAppPreferenceRef = useRef<{
    initialized: boolean;
    value: string | undefined;
  }>({ initialized: false, value: undefined });
  const range = RANGE_OPTIONS.find((option) => option.value === rangeKey) ?? RANGE_OPTIONS[1];
  const refresh = useCallback(() => setGeneration((current) => current + 1), []);

  useEffect(() => {
    const controller = new AbortController();
    let current = true;
    historyControllerRef.current?.abort();
    fieldControllerRef.current?.abort();
    setBootstrapState("loading");
    setBootstrapError(null);
    setBootstrap(null);
    setHistoryState("loading");
    setHistorySnapshot(null);
    setHistoryRange(null);
    setFieldState("idle");
    setFieldSnapshot(null);
    void getSystemBootstrap(client, preferredAppId, { signal: controller.signal }).then(
      (loaded) => {
        if (!current) return;
        const synchronizeAppScope = !appliedAppPreferenceRef.current.initialized
          || appliedAppPreferenceRef.current.value !== preferredAppId;
        appliedAppPreferenceRef.current = { initialized: true, value: preferredAppId };
        setAppId((selected) => synchronizeAppScope
          ? loaded.selectedAppId ?? "all"
          : selected === "all" || loaded.apps.some((app) => app.appId === selected)
            ? selected
            : loaded.selectedAppId ?? "all");
        const searchableIndexesByName = new Map(
          loaded.indexes
            .filter((index) => index.searchable)
            .map((index) => [index.name.toLowerCase(), index.name]),
        );
        const selectedApp = loaded.apps.find((app) => app.appId === loaded.selectedAppId);
        const defaultFieldIndexName = selectedApp?.defaultIndexNames
          .map((name) => searchableIndexesByName.get(name.toLowerCase()))
          .find((name): name is string => name !== undefined)
          ?? loaded.indexes.find((index) => index.searchable)?.name
          ?? loaded.indexes[0]?.name
          ?? "";
        setFieldIndexName((selected) => {
          if (synchronizeAppScope) return defaultFieldIndexName;
          if (loaded.indexes.some((index) => index.name === selected)) return selected;
          return defaultFieldIndexName;
        });
        setBootstrap(loaded);
        setBootstrapState("available");
      },
      (reason: unknown) => {
        if (!current || controller.signal.aborted) return;
        setBootstrapError(backendErrorMessage(reason));
        setBootstrapState("error");
      },
    );
    return () => {
      current = false;
      controller.abort();
    };
  }, [client, generation, preferredAppId]);

  useEffect(() => {
    if (bootstrap === null) return;
    historyControllerRef.current?.abort();
    const controller = new AbortController();
    historyControllerRef.current = controller;
    let current = true;
    setHistorySnapshot(null);
    setHistoryError(null);
    if (!supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_SEARCH_HISTORY)) {
      setHistoryState("unavailable");
      return () => {
        current = false;
        controller.abort();
      };
    }
    setHistoryState("loading");
    const end = new Date(bootstrap.serverTime);
    const start = new Date(end.valueOf() - rangeMilliseconds(rangeKey));
    void loadAnalyticsHistory(client, {
      signal: controller.signal,
      maximumPageSize: bootstrap.limits.maximumPageSize,
      createdAfter: start,
      createdBefore: end,
      appId: appId === "all" ? undefined : appId,
    }).then(
      (snapshot) => {
        if (!current) return;
        setHistorySnapshot(snapshot);
        setHistoryRange({ start, end });
        setHistoryState("available");
      },
      (reason: unknown) => {
        if (!current || controller.signal.aborted) return;
        if (isOptionalRouteUnavailable(reason)) {
          setHistoryState("unavailable");
        } else {
          setHistoryError(backendErrorMessage(reason));
          setHistoryState("error");
        }
      },
    );
    return () => {
      current = false;
      controller.abort();
      if (historyControllerRef.current === controller) historyControllerRef.current = null;
    };
  }, [appId, bootstrap, client, rangeKey]);

  useEffect(() => {
    if (bootstrap === null) return;
    fieldControllerRef.current?.abort();
    const controller = new AbortController();
    fieldControllerRef.current = controller;
    let current = true;
    setFieldSnapshot(null);
    setFieldError(null);
    if (!supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_INDEX_ADMIN)) {
      setFieldState("unavailable");
      return () => {
        current = false;
        controller.abort();
      };
    }
    if (fieldIndexName.length === 0) {
      setFieldState("idle");
      return () => {
        current = false;
        controller.abort();
      };
    }
    setFieldState("loading");
    void loadAnalyticsFields(client, {
      signal: controller.signal,
      maximumPageSize: bootstrap.limits.maximumPageSize,
      indexName: fieldIndexName,
      earliest: range.earliest,
      latest: "now",
    }).then(
      (snapshot) => {
        if (!current) return;
        setFieldSnapshot(snapshot);
        setFieldState("available");
      },
      (reason: unknown) => {
        if (!current || controller.signal.aborted) return;
        if (isOptionalRouteUnavailable(reason)) {
          setFieldState("unavailable");
        } else {
          setFieldError(backendErrorMessage(reason));
          setFieldState("error");
        }
      },
    );
    return () => {
      current = false;
      controller.abort();
      if (fieldControllerRef.current === controller) fieldControllerRef.current = null;
    };
  }, [bootstrap, client, fieldIndexName, range.earliest]);

  useEffect(() => () => {
    historyControllerRef.current?.abort();
    fieldControllerRef.current?.abort();
  }, []);

  const workload = useMemo(() => (
    historySnapshot === null || historyRange === null
      ? null
      : deriveAnalyticsWorkload(historySnapshot.entries, historyRange)
  ), [historyRange, historySnapshot]);
  const backendFields = useMemo(() => fieldSnapshot?.fields.map(backendFieldForDisplay) ?? [], [fieldSnapshot]);
  const visibleFields = useMemo(() => {
    const normalized = fieldQuery.trim().toLowerCase();
    return backendFields.filter((field) => (
      fieldMatchesCoverage(field, coverageFilter)
      && (normalized.length === 0 || `${field.name} ${field.type}`.toLowerCase().includes(normalized))
    )).toSorted((left, right) => {
      if (fieldSort === "name") return left.name.localeCompare(right.name);
      if (fieldSort === "cardinality") return compareCardinality(left.cardinality, right.cardinality);
      return right.coverage - left.coverage || left.name.localeCompare(right.name);
    });
  }, [backendFields, coverageFilter, fieldQuery, fieldSort]);

  function clearFieldFilters() {
    setFieldQuery("");
    setCoverageFilter("all");
    setFieldSort("coverage");
  }

  function selectAnalyticsApp(nextAppId: string) {
    setAppId(nextAppId);
    if (nextAppId !== "all") replaceBackendAppId(nextAppId);
  }

  const navigationAppId = bootstrap?.selectedAppId ?? preferredAppId;
  const contextualHref = (href: string, appIdOverride?: string | null) => {
    const destinationAppId = appIdOverride ?? navigationAppId;
    return destinationAppId === undefined || destinationAppId === null
      ? href
      : backendAppHref(href, destinationAppId);
  };

  const observedTrendValues = workload?.trend.flatMap((bucket) => bucket.p95RuntimeMs === null ? [] : [bucket.p95RuntimeMs / 1_000]) ?? [];
  const trendValues = workload?.trend.map((bucket) => bucket.p95RuntimeMs === null ? null : bucket.p95RuntimeMs / 1_000) ?? [];
  const slowestMaximum = workload?.slowest[0]?.durationMs ?? null;

  return (
    <div className={`suite-page ${styles.page}`}>
      <PageHeading
        eyebrow="SEARCH & REPORTING"
        title="Analytics"
        description="Inspect bounded search-history workload metrics and available index field snapshots from the connected backend."
        actions={(
          <>
            {historySnapshot === null ? <span className="badge badge--success badge--outline">{bootstrapState === "loading" ? "Connecting backend" : bootstrapState === "error" ? "Backend unavailable" : "Connected backend"}</span> : (
              <AnalyticsSampleStatus
                className={`badge badge--outline ${historySnapshot.complete ? "badge--success" : "badge--warning"}`}
                complete={historySnapshot.complete}
                loaded={historySnapshot.entries.length}
                totalSize={historySnapshot.totalSize}
                totalSizeExact={historySnapshot.totalSizeExact}
              />
            )}
            <button className="button button--primary" disabled={bootstrapState === "loading"} onClick={refresh} type="button">
              {bootstrapState === "loading" ? "Refreshing…" : "Refresh"}
            </button>
          </>
        )}
      />

      <section className={styles.contextBar} aria-label="Analytics context">
        <div>
          <span className={styles.contextIcon} aria-hidden="true">⌁</span>
          <div><strong>Persisted search workload</strong><small>Terminal search metadata only; bounded to eight backend pages per refresh.</small></div>
        </div>
        <label>
          <span>Time range</span>
          <select data-testid="analytics-range" disabled={bootstrapState !== "available"} value={rangeKey} onChange={(event) => setRangeKey(event.target.value as RangeKey)}>
            {RANGE_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
          </select>
        </label>
        <label>
          <span>App</span>
          <select data-testid="analytics-app" disabled={bootstrapState !== "available"} value={appId} onChange={(event) => selectAnalyticsApp(event.target.value)}>
            <option value="all">All authorized apps (analytics only)</option>
            {bootstrap?.apps.map((app) => <option key={app.appId} value={app.appId}>{appLabel(app)}</option>)}
          </select>
        </label>
      </section>

      {bootstrapState === "loading" ? <BackendResourceState kind="loading" title="Connecting analytics" message="Loading the backend capability and resource catalog…" /> : null}
      {bootstrapState === "error" ? <BackendResourceState kind="error" title="Analytics could not connect" message={bootstrapError ?? "System bootstrap failed."} action={<button type="button" onClick={refresh}>Retry</button>} /> : null}
      {bootstrapState === "available" && historyState === "loading" ? <BackendResourceState kind="loading" title="Loading search workload" message={`Reading bounded search history for ${range.label.toLowerCase()}…`} /> : null}
      {bootstrapState === "available" && historyState === "unavailable" ? <BackendResourceState kind="unavailable" title="Search analytics are unavailable" message="The connected backend does not advertise or register persisted search history." action={<button type="button" onClick={refresh}>Retry</button>} /> : null}
      {bootstrapState === "available" && historyState === "error" ? <BackendResourceState kind="error" title="Search analytics could not be loaded" message={historyError ?? "The search-history request failed."} action={<button type="button" onClick={refresh}>Retry</button>} /> : null}
      {historyState === "available" && historySnapshot !== null && workload?.searchCount === 0 ? (
        <BackendResourceState kind="empty" title="No search history in this range" message="No terminal searches matched the selected time range and app. Try a wider range or another app." />
      ) : null}

      {historyState === "available" && historySnapshot !== null && workload !== null && workload.searchCount > 0 ? (
        <>
          {historySnapshot.complete ? null : (
            <output className={styles.partialNotice}>
              Metrics below describe the {historySnapshot.entries.length.toLocaleString()} loaded entries. Further paging stopped at the browser safety limit, so totals and percentiles are a partial sample.
            </output>
          )}
          <section className={styles.metricGrid} aria-label="Search analytics summary">
            <article><span>Searches observed</span><strong>{NUMBER_FORMAT.format(workload.searchCount)}</strong><small>{workload.completedCount.toLocaleString()} completed in the loaded {historySnapshot.complete ? "range" : "sample"}</small></article>
            <article><span>Failed searches</span><strong>{NUMBER_FORMAT.format(workload.failedCount)}</strong><small>{workload.canceledCount.toLocaleString()} canceled · {workload.expiredCount.toLocaleString()} expired</small></article>
            <article><span>Median runtime</span><strong>{formatRuntime(workload.medianRuntimeMs)}</strong><small>{workload.p95RuntimeMs === null ? "No p95 duration reported" : `p95 ${formatRuntime(workload.p95RuntimeMs)}`}</small></article>
            <article><span>Rows scanned</span><strong>{formatCounter(workload.scannedRows)}</strong><small>{formatBytes(workload.scannedBytes)} read · {formatCounter(workload.producedRows)} rows produced</small></article>
          </section>

          <div className={styles.primaryGrid}>
            <section className={`suite-card ${styles.performancePanel}`} aria-labelledby="performance-title">
              <header className={styles.panelHeader}>
                <div><h2 id="performance-title">Search performance</h2><p>Observed p95 runtime in {range.bucket} buckets; gaps mean no duration was reported</p></div>
                <div className={styles.legend}><span /><span>p95 runtime</span><b>seconds</b></div>
              </header>
              {observedTrendValues.length === 0 ? (
                <BackendResourceState kind="empty" title="No runtime measurements" message="The matching history entries do not include duration metadata." />
              ) : (
                <PerformanceTrend values={trendValues} labels={backendTrendLabels(workload, rangeKey)} />
              )}
              <footer className={styles.performanceFooter}>
                <div><span>Lowest bucket p95</span><strong>{observedTrendValues.length === 0 ? "Not reported" : `${Math.min(...observedTrendValues).toFixed(2)} s`}</strong></div>
                <div><span>Median search</span><strong>{formatRuntime(workload.medianRuntimeMs)}</strong></div>
                <div><span>Highest bucket p95</span><strong>{observedTrendValues.length === 0 ? "Not reported" : `${Math.max(...observedTrendValues).toFixed(2)} s`}</strong></div>
                <Link href={contextualHref("/activity/")}>Inspect history <AppIcon name="chevron-right" size="xs" /></Link>
              </footer>
            </section>

            <aside className={`suite-card ${styles.insightsPanel}`} aria-labelledby="failures-title">
              <header className={styles.panelHeader}>
                <div><h2 id="failures-title">Recent failures</h2><p>Retained backend failure details in this sample</p></div>
                <span className={styles.insightCount}>{workload.failedCount}</span>
              </header>
              {workload.failures.length === 0 ? (
                <BackendResourceState kind="empty" title="No failed searches" message="No matching history entry finished in the failed state." />
              ) : (
                <ol className={styles.insightList}>
                  {workload.failures.map((entry) => (
                    <li key={entry.id}>
                      <div className={styles.insightTopline}><span className={`${styles.severity} ${styles.severityhigh}`}>Failed</span><small>{formatRuntime(entry.durationMs)}</small></div>
                      <h3>{historyTitle(entry)}</h3>
                      <p>{entry.failureMessage ?? "The backend retained no failure message for this search."}</p>
                      <footer><strong>{formatCounter(entry.scannedRows)} rows scanned</strong><Link href={contextualHref(historySearchLaunchHref(entry.id, false), entry.appId)}>Open search <AppIcon name="chevron-right" size="xs" /></Link></footer>
                    </li>
                  ))}
                </ol>
              )}
            </aside>
          </div>

          <div className={styles.secondaryGrid}>
            <section className={`suite-card ${styles.fieldsPanel}`} aria-labelledby="fields-title">
              <header className={styles.panelHeader}>
                <div><h2 id="fields-title">Field coverage</h2><p>Exact presence counters from a bounded index snapshot; distinct counts may be unavailable</p></div>
                {fieldSnapshot === null ? null : <span className={styles.resultCount}><strong>{visibleFields.length}</strong> shown · {fieldSnapshot.sampledEvents.toLocaleString()} events</span>}
              </header>
              <div className={`${styles.fieldToolbar} ${styles.backendFieldToolbar}`}>
                <label>
                  <span>Index</span>
                  <select data-testid="analytics-field-index" value={fieldIndexName} onChange={(event) => setFieldIndexName(event.target.value)}>
                    {bootstrap?.indexes.length === 0 ? <option value="">No indexes</option> : null}
                    {bootstrap?.indexes.map((index) => <option key={index.id} value={index.name}>{index.displayName}</option>)}
                  </select>
                </label>
                <label className={styles.fieldSearch}>
                  <span className="sr-only">Filter fields</span><i aria-hidden="true"><AppIcon name="search" size="sm" /></i>
                  <input data-testid="analytics-field-filter" type="search" placeholder="Filter field names or types" value={fieldQuery} onChange={(event) => setFieldQuery(event.target.value)} />
                </label>
                <label>
                  <span>Coverage</span>
                  <select data-testid="analytics-coverage" value={coverageFilter} onChange={(event) => setCoverageFilter(event.target.value as CoverageFilter)}>
                    <option value="all">Any coverage</option><option value="complete">90–100%</option><option value="partial">40–89%</option><option value="sparse">Below 40%</option>
                  </select>
                </label>
                <label>
                  <span>Sort</span>
                  <select data-testid="analytics-field-sort" value={fieldSort} onChange={(event) => setFieldSort(event.target.value as FieldSort)}>
                    <option value="coverage">Coverage</option><option value="cardinality">Cardinality</option><option value="name">Field name</option>
                  </select>
                </label>
              </div>
              {fieldState === "idle" ? <BackendResourceState kind="empty" title="No index available" message="The backend bootstrap did not return an index for field analysis." /> : null}
              {fieldState === "loading" ? <BackendResourceState kind="loading" title="Loading field coverage" message={`Capturing ${fieldIndexName || "the selected index"} for ${range.label.toLowerCase()}…`} /> : null}
              {fieldState === "unavailable" ? <BackendResourceState kind="unavailable" title="Field coverage is unavailable" message="The backend does not advertise the index administration field catalog, or the route is unavailable." /> : null}
              {fieldState === "error" ? <BackendResourceState kind="error" title="Field coverage could not be loaded" message={fieldError ?? "The field-catalog request failed."} /> : null}
              {fieldState === "available" && fieldSnapshot !== null && !fieldSnapshot.complete ? (
                <output className={styles.partialNotice}>Showing {fieldSnapshot.fields.length.toLocaleString()} loaded fields; the field snapshot reached the browser page limit.</output>
              ) : null}
              {fieldState === "available" && fieldSnapshot !== null && fieldSnapshot.fields.length === 0 ? <BackendResourceState kind="empty" title="No fields observed" message="This index had no field profiles in the selected range." /> : null}
              {fieldState === "available" && fieldSnapshot !== null && fieldSnapshot.fields.length > 0 && visibleFields.length === 0 ? (
                <div className={styles.emptyFields}><span aria-hidden="true"><AppIcon name="search" size="lg" /></span><strong>No fields match these filters</strong><p>Clear the filters to return to the loaded field profile.</p><button className="button" onClick={clearFieldFilters} type="button">Clear filters</button></div>
              ) : null}
              {fieldState === "available" && fieldSnapshot !== null && visibleFields.length > 0 ? (
                <div className={styles.fieldList}>
                  <div className={styles.fieldListHeader} aria-hidden="true"><span>Field</span><span>Coverage</span><span>Distinct</span><span>Null / missing</span><span /></div>
                  <ul>
                    {visibleFields.map((field) => {
                      const original = fieldSnapshot.fields.find((candidate) => candidate.name === field.name);
                      const analysisHref = fieldAnalysisHref(fieldIndexName, field.name, range.earliest);
                      return (
                        <li key={field.name}>
                          <div className={styles.fieldIdentity}><code>{field.name}</code><span className={styles.fieldType}>{field.type}</span></div>
                          <div className={styles.coverageCell}><span><i style={{ width: `${field.coverage}%` }} /></span><strong>{field.coverage.toFixed(field.coverage % 1 === 0 ? 0 : 1)}%</strong></div>
                          <span className={styles.cardinality}>{original?.cardinalityApproximate ? "≈ " : ""}{formatCardinality(field.cardinality)}</span>
                          <code className={styles.example}>{original === undefined ? "Not reported" : `${original.nullEvents.toLocaleString()} / ${original.missingEvents.toLocaleString()}`}</code>
                          {analysisHref === null
                            ? <span className={styles.unavailableAction} title="This field name cannot be represented by the current SPL grammar.">Unavailable</span>
                            : <Link aria-label={`Analyze ${field.name} in Search`} href={contextualHref(analysisHref)}>Analyze <AppIcon name="chevron-right" size="xs" /></Link>}
                        </li>
                      );
                    })}
                  </ul>
                </div>
              ) : null}
            </section>

            <section className={`suite-card ${styles.slowestPanel}`} aria-labelledby="slowest-title">
              <header className={styles.panelHeader}><div><h2 id="slowest-title">Slowest searches</h2><p>Individual retained runtimes, not inferred recurrence</p></div></header>
              {workload.slowest.length === 0 ? (
                <BackendResourceState kind="empty" title="No measured runtimes" message="The matching history entries do not retain durations." />
              ) : (
                <ol className={styles.slowestList}>
                  {workload.slowest.map((entry, index) => (
                    <li key={entry.id}>
                      <span className={styles.rank}>{index + 1}</span>
                      <div className={styles.searchDetail}>
                        <Link href={contextualHref(historySearchLaunchHref(entry.id, false), entry.appId)}>{historyTitle(entry)}</Link>
                        <small>{historyScopeLabel(entry)} · {formatCounter(entry.scannedRows)} rows scanned</small>
                        <span className={styles.durationTrack}><i style={{ width: slowestMaximum === null || slowestMaximum === 0 ? "0%" : `${((entry.durationMs ?? 0) / slowestMaximum) * 100}%` }} /></span>
                      </div>
                      <strong>{formatRuntime(entry.durationMs)}</strong>
                    </li>
                  ))}
                </ol>
              )}
              <footer className={styles.slowestFooter}><span>Ordered by retained runtime</span><Link href={contextualHref("/activity/")}>View search history <AppIcon name="chevron-right" size="xs" /></Link></footer>
            </section>
          </div>
        </>
      ) : null}
    </div>
  );
}
