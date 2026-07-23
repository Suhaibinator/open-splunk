"use client";

import Link from "next/link";
import {
  type CSSProperties,
  type KeyboardEvent,
  type PointerEvent as ReactPointerEvent,
  useMemo,
  useRef,
  useState,
} from "react";

import { searchLaunchHref } from "@/lib/search/launch-url";

import { PageHeading } from "../_components/product-shell";
import styles from "./analytics.module.css";

type RangeKey = "1h" | "24h" | "7d";
type EnvironmentKey = "all" | "production" | "staging";
type CoverageFilter = "all" | "complete" | "partial" | "sparse";
type FieldSort = "coverage" | "cardinality" | "name";

interface FieldProfile {
  name: string;
  type: "string" | "number" | "timestamp";
  coverage: number;
  cardinality: number;
  example: string;
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
  { name: "path", type: "string", coverage: 66.9, cardinality: 42, example: "/api/v1/submissions/grade" },
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

function formatCardinality(value: number) {
  if (value >= 10_000) return `${DECIMAL_FORMAT.format(value / 1_000)}K`;
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

function PerformanceTrend({ values, labels }: { values: number[]; labels: string[] }) {
  const [activeIndex, setActiveIndex] = useState<number | null>(null);
  const [focusIndex, setFocusIndex] = useState(0);
  const pointRefs = useRef<Array<HTMLButtonElement | null>>([]);
  const maximum = Math.max(...values) * 1.12;
  const minimum = Math.max(0, Math.min(...values) * 0.78);
  const width = 720;
  const height = 184;
  const left = 18;
  const right = 18;
  const top = 14;
  const bottom = 25;
  const plotWidth = width - left - right;
  const plotHeight = height - top - bottom;
  const coordinates = values.map((value, index) => ({
    x: left + (index / Math.max(1, values.length - 1)) * plotWidth,
    y: top + (1 - (value - minimum) / Math.max(0.01, maximum - minimum)) * plotHeight,
  }));
  const linePoints = coordinates.map((point) => `${point.x},${point.y}`).join(" ");
  const areaPoints = `${left},${height - bottom} ${linePoints} ${width - right},${height - bottom}`;

  function moveFocus(index: number) {
    const nextIndex = Math.max(0, Math.min(values.length - 1, index));
    setFocusIndex(nextIndex);
    setActiveIndex(nextIndex);
    pointRefs.current[nextIndex]?.focus({ preventScroll: true });
  }

  function handleKeyDown(event: KeyboardEvent<HTMLButtonElement>, index: number) {
    if (event.key === "ArrowRight" || event.key === "ArrowDown") moveFocus(index + 1);
    else if (event.key === "ArrowLeft" || event.key === "ArrowUp") moveFocus(index - 1);
    else if (event.key === "Home") moveFocus(0);
    else if (event.key === "End") moveFocus(values.length - 1);
    else if (event.key === "Escape") setActiveIndex(null);
    else return;
    event.preventDefault();
  }

  function activateNearestPoint(event: ReactPointerEvent<HTMLDivElement>) {
    const bounds = event.currentTarget.getBoundingClientRect();
    if (bounds.width <= 0) return;
    const ratio = Math.max(0, Math.min(1, (event.clientX - bounds.left) / bounds.width));
    setActiveIndex(Math.round(ratio * (values.length - 1)));
  }

  return (
    <figure className={styles.trendFigure}>
      <div className={styles.trendPlot}>
        <svg aria-hidden="true" preserveAspectRatio="none" viewBox={`0 0 ${width} ${height}`}>
          <defs>
            <linearGradient id="analytics-trend-fill" x1="0" x2="0" y1="0" y2="1">
              <stop offset="0%" stopColor="#5e963c" stopOpacity="0.24" />
              <stop offset="100%" stopColor="#5e963c" stopOpacity="0.02" />
            </linearGradient>
          </defs>
          {[0.25, 0.5, 0.75, 1].map((position) => (
            <line className={styles.gridLine} key={position} x1={left} x2={width - right} y1={top + plotHeight * position} y2={top + plotHeight * position} />
          ))}
          <polygon fill="url(#analytics-trend-fill)" points={areaPoints} />
          <polyline className={styles.trendLine} points={linePoints} />
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
          {coordinates.map((coordinate, index) => {
            const isActive = activeIndex === index;
            const edge = index < 3 ? styles.tooltipStart : index > values.length - 4 ? styles.tooltipEnd : "";
            return (
              <button
                aria-label={`${labels[index]}: ${values[index].toFixed(2)} seconds p95 runtime`}
                className={`${styles.trendPoint} ${isActive ? styles.trendPointActive : ""}`}
                key={labels[index]}
                onBlur={() => setActiveIndex(null)}
                onFocus={() => { setFocusIndex(index); setActiveIndex(index); }}
                onKeyDown={(event) => handleKeyDown(event, index)}
                ref={(element) => { pointRefs.current[index] = element; }}
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
                    <strong>{values[index].toFixed(2)} s</strong>
                    <small>{labels[index]}</small>
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
}

export function AnalyticsConsole({ dataMode }: AnalyticsConsoleProps) {
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
    label: dataMode === "backend" ? `${range.label} example draft` : range.label,
    run: dataMode !== "backend",
  };
  const fixtureSearchHref = (spl: string) => searchLaunchHref(
    dataMode === "backend"
      ? spl.replace(/\bindex=(?:gradethis|payments)\b/g, "index=*")
      : spl,
    searchOptions,
  );
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
      if (fieldSort === "cardinality") return right.cardinality - left.cardinality;
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
            <output className={styles.updateStatus} data-testid="analytics-updated">Static preview snapshot</output>
            <button className="suite-button" data-testid="analytics-refresh" disabled title="Analytics uses a static preview snapshot." type="button">Static preview</button>
            <Link className="suite-button suite-button--primary" href={fixtureSearchHref(`index=gradethis${environmentSPL}`)}>{dataMode === "backend" ? "Open example draft" : "Open Search"}</Link>
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
            <Link href={fixtureSearchHref(`index=gradethis${environmentSPL} duration_ms=* | stats p95(duration_ms) AS p95_ms BY service | sort -p95_ms`)}>Investigate latency →</Link>
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
                <footer><strong>{insight.impact}</strong><Link href={fixtureSearchHref(insight.query)}>Inspect SPL →</Link></footer>
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
              <span className="sr-only">Filter fields</span><i aria-hidden="true">⌕</i>
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
              <span aria-hidden="true">⌕</span><strong>No fields match these filters</strong><p>Clear the filters to return to the complete field profile.</p>
              <button className="suite-button" onClick={clearFieldFilters} type="button">Clear filters</button>
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
                    <Link aria-label={`Analyze ${field.name} in Search`} href={fixtureSearchHref(`index=gradethis${environmentSPL} ${field.name}=* | stats count by ${field.name} | sort -count`)}>Analyze →</Link>
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
            <Link href={fixtureSearchHref(`index=gradethis${environmentSPL} duration_ms=* | stats p95(duration_ms) as p95_ms count by service | sort -p95_ms`)}>View complete workload →</Link>
          </footer>
        </section>
      </div>
    </div>
  );
}
