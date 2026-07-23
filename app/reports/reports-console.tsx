"use client";

import Link from "next/link";
import { useMemo, useState } from "react";

import type { SearchDataMode } from "@/lib/search/backend-data";
import { searchLaunchHref } from "@/lib/search/launch-url";

import { PageHeading } from "../_components/product-shell";
import { BackendReportsConsole } from "./backend-reports-console";
import styles from "./reports.module.css";

type ReportScope = "all" | "mine" | "scheduled" | "favorites";
type ReportType = "all" | "chart" | "statistics" | "events";
type ReportStatus = "Scheduled" | "Manual" | "Paused";
type SortOrder = "modified" | "name" | "nextRun";

interface ReportDefinition {
  id: string;
  name: string;
  description: string;
  query: string;
  owner: string;
  type: Exclude<ReportType, "all">;
  status: ReportStatus;
  cadence: string;
  nextRunMinutes: number | null;
  modifiedMinutes: number;
  lastRun: "Succeeded" | "Failed" | "Not run";
  updated: string;
}

const REPORTS: ReportDefinition[] = [
  {
    id: "service-error-rate",
    name: "Service error rate",
    description: "Error percentage and event count by application service.",
    query: "index=gradethis | timechart span=15m count by service",
    owner: "Administrator",
    type: "chart",
    status: "Scheduled",
    cadence: "Every 15 minutes",
    nextRunMinutes: 6,
    modifiedMinutes: 18,
    lastRun: "Succeeded",
    updated: "18 min ago",
  },
  {
    id: "api-latency-p95",
    name: "API latency — p95 by route",
    description: "Request volume across endpoints in the public GradeThis API.",
    query: "index=gradethis path=* | timechart span=30m count by path",
    owner: "Administrator",
    type: "chart",
    status: "Scheduled",
    cadence: "Every 30 minutes",
    nextRunMinutes: 21,
    modifiedMinutes: 74,
    lastRun: "Succeeded",
    updated: "1 hr ago",
  },
  {
    id: "submission-pipeline",
    name: "Submission pipeline health",
    description: "Queued, graded, retried, and failed submissions by hour.",
    query: "index=gradethis (logger=grading-service OR logger=submission-service) | timechart span=1h count by level",
    owner: "Administrator",
    type: "statistics",
    status: "Scheduled",
    cadence: "Hourly",
    nextRunMinutes: 48,
    modifiedMinutes: 195,
    lastRun: "Failed",
    updated: "3 hr ago",
  },
  {
    id: "authentication-failures",
    name: "Authentication failures",
    description: "Unauthorized and expired-session requests with client context.",
    query: "index=gradethis (status=401 OR status=403) | sort -_time",
    owner: "Security team",
    type: "events",
    status: "Scheduled",
    cadence: "Daily at 6:00 AM",
    nextRunMinutes: 392,
    modifiedMinutes: 1440,
    lastRun: "Succeeded",
    updated: "Yesterday",
  },
  {
    id: "request-volume",
    name: "Request volume by method",
    description: "HTTP request distribution for capacity review.",
    query: "index=gradethis method=* | stats count by method | sort -count",
    owner: "Administrator",
    type: "statistics",
    status: "Manual",
    cadence: "Run on demand",
    nextRunMinutes: null,
    modifiedMinutes: 2880,
    lastRun: "Succeeded",
    updated: "2 days ago",
  },
  {
    id: "notification-retries",
    name: "Notification delivery retries",
    description: "Retry pressure and failed email deliveries from the worker queue.",
    query: "index=gradethis logger=notification-worker retry_count>0 | timechart span=1h count by operation",
    owner: "Platform team",
    type: "chart",
    status: "Paused",
    cadence: "Paused · was hourly",
    nextRunMinutes: null,
    modifiedMinutes: 4780,
    lastRun: "Succeeded",
    updated: "3 days ago",
  },
  {
    id: "rate-limit-warnings",
    name: "Clients near rate limit",
    description: "Recent clients with low remaining request capacity.",
    query: "index=gradethis logger=rate-limiter level=WARN | table _time trace_id path limit remaining | sort -_time",
    owner: "Administrator",
    type: "events",
    status: "Manual",
    cadence: "Run on demand",
    nextRunMinutes: null,
    modifiedMinutes: 10080,
    lastRun: "Not run",
    updated: "1 week ago",
  },
];

const INITIAL_FAVORITES = new Set(["service-error-rate", "api-latency-p95"]);

const SCOPE_LABELS: Array<{ id: ReportScope; label: string }> = [
  { id: "all", label: "All reports" },
  { id: "mine", label: "Owned by me" },
  { id: "scheduled", label: "Scheduled" },
  { id: "favorites", label: "Favorites" },
];

function reportIcon(type: ReportDefinition["type"]) {
  if (type === "chart") return "⌁";
  if (type === "statistics") return "▦";
  return "☷";
}

function compareReports(left: ReportDefinition, right: ReportDefinition, sort: SortOrder) {
  if (sort === "name") return left.name.localeCompare(right.name);
  if (sort === "nextRun") {
    return (left.nextRunMinutes ?? Number.POSITIVE_INFINITY) - (right.nextRunMinutes ?? Number.POSITIVE_INFINITY);
  }
  return left.modifiedMinutes - right.modifiedMinutes;
}

interface ReportsConsoleProps {
  dataMode: SearchDataMode;
  apiBaseUrl: string;
}

export function ReportsConsole({ dataMode, apiBaseUrl }: ReportsConsoleProps) {
  if (dataMode === "backend") return <BackendReportsConsole apiBaseUrl={apiBaseUrl} />;
  return <DemoReportsConsole />;
}

function DemoReportsConsole() {
  const [query, setQuery] = useState("");
  const [scope, setScope] = useState<ReportScope>("all");
  const [type, setType] = useState<ReportType>("all");
  const [status, setStatus] = useState<"all" | ReportStatus>("all");
  const [sort, setSort] = useState<SortOrder>("modified");
  const [favorites, setFavorites] = useState(INITIAL_FAVORITES);

  const counts = useMemo(() => ({
    all: REPORTS.length,
    mine: REPORTS.filter((report) => report.owner === "Administrator").length,
    scheduled: REPORTS.filter((report) => report.status === "Scheduled").length,
    favorites: REPORTS.filter((report) => favorites.has(report.id)).length,
  }), [favorites]);

  const visibleReports = useMemo(() => {
    const normalizedQuery = query.trim().toLowerCase();
    return REPORTS.filter((report) => {
      if (scope === "mine" && report.owner !== "Administrator") return false;
      if (scope === "scheduled" && report.status !== "Scheduled") return false;
      if (scope === "favorites" && !favorites.has(report.id)) return false;
      if (type !== "all" && report.type !== type) return false;
      if (status !== "all" && report.status !== status) return false;
      return normalizedQuery.length === 0 || `${report.name} ${report.description} ${report.query} ${report.owner}`.toLowerCase().includes(normalizedQuery);
    }).toSorted((left, right) => compareReports(left, right, sort));
  }, [favorites, query, scope, sort, status, type]);

  function toggleFavorite(id: string) {
    setFavorites((current) => {
      const next = new Set(current);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  function resetFilters() {
    setQuery("");
    setScope("all");
    setType("all");
    setStatus("all");
    setSort("modified");
  }

  return (
    <div className={`suite-page ${styles.page}`}>
      <PageHeading
        eyebrow="SEARCH & REPORTING"
        title="Reports"
        description="Curated searches for recurring operational questions and scheduled delivery."
        actions={(
          <>
            <Link className="suite-button" href="/search/">Open Search</Link>
            <Link
              className="suite-button suite-button--primary"
              href={searchLaunchHref("index=gradethis | stats count by service", { run: false })}
              title="Open a report-shaped SPL draft. Report creation is not connected in this preview."
            >
              ＋ Draft in Search
            </Link>
          </>
        )}
      />

      <section className={styles.summary} aria-label="Report summary">
        <article><span className={styles.metricIcon} aria-hidden="true">▤</span><div><strong>{REPORTS.length}</strong><small>Total reports</small></div></article>
        <article><span className={styles.metricIcon} aria-hidden="true">◷</span><div><strong>{counts.scheduled}</strong><small>Active schedules</small></div></article>
        <article><span className={`${styles.metricIcon} ${styles.metricIconWarning}`} aria-hidden="true">!</span><div><strong>1</strong><small>Run needs attention</small></div></article>
      </section>

      <section className={`suite-card ${styles.library}`} aria-labelledby="report-library-title">
        <header className={styles.libraryHeader}>
          <div>
            <h2 id="report-library-title">Report library</h2>
            <p>Search, open, and manage your working set.</p>
          </div>
          <span><strong>{visibleReports.length}</strong> {visibleReports.length === 1 ? "report" : "reports"}</span>
        </header>

        <div className={styles.scopeBar} aria-label="Report scope">
          {SCOPE_LABELS.map((item) => (
            <button
              className={scope === item.id ? styles.scopeActive : undefined}
              type="button"
              aria-pressed={scope === item.id}
              onClick={() => setScope(item.id)}
              key={item.id}
            >
              {item.label}<span>{counts[item.id]}</span>
            </button>
          ))}
        </div>

        <div className={styles.toolbar}>
          <label className={styles.searchField}>
            <span className="sr-only">Filter reports</span>
            <i aria-hidden="true">⌕</i>
            <input
              type="search"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Find by name, SPL, or owner"
            />
          </label>
          <label className={styles.selectField}>
            <span>Type</span>
            <select value={type} onChange={(event) => setType(event.target.value as ReportType)}>
              <option value="all">All types</option>
              <option value="chart">Charts</option>
              <option value="statistics">Statistics</option>
              <option value="events">Event lists</option>
            </select>
          </label>
          <label className={styles.selectField}>
            <span>Status</span>
            <select value={status} onChange={(event) => setStatus(event.target.value as "all" | ReportStatus)}>
              <option value="all">All statuses</option>
              <option value="Scheduled">Scheduled</option>
              <option value="Manual">Manual</option>
              <option value="Paused">Paused</option>
            </select>
          </label>
          <label className={styles.selectField}>
            <span>Sort</span>
            <select value={sort} onChange={(event) => setSort(event.target.value as SortOrder)}>
              <option value="modified">Recently modified</option>
              <option value="name">Name</option>
              <option value="nextRun">Next scheduled run</option>
            </select>
          </label>
        </div>

        {visibleReports.length === 0 ? (
          <div className={styles.empty}>
            <span aria-hidden="true">⌕</span>
            <strong>No matching reports</strong>
            <p>Try another phrase or broaden the report filters.</p>
            <button type="button" onClick={resetFilters}>Clear all filters</button>
          </div>
        ) : (
          <div className={styles.tableWrap}>
            <table className={styles.table}>
              <thead>
                <tr>
                  <th scope="col"><span className="sr-only">Favorite</span></th>
                  <th scope="col">Report</th>
                  <th scope="col">Owner</th>
                  <th scope="col">Schedule</th>
                  <th scope="col">Last run</th>
                  <th scope="col">Modified</th>
                  <th scope="col"><span className="sr-only">Open</span></th>
                </tr>
              </thead>
              <tbody>
                {visibleReports.map((report) => {
                  const isFavorite = favorites.has(report.id);
                  return (
                    <tr key={report.id}>
                      <td className={styles.favoriteCell}>
                        <button
                          className={isFavorite ? styles.favoriteActive : undefined}
                          type="button"
                          aria-label={`${isFavorite ? "Remove" : "Add"} ${report.name} ${isFavorite ? "from" : "to"} favorites`}
                          aria-pressed={isFavorite}
                          onClick={() => toggleFavorite(report.id)}
                        >
                          {isFavorite ? "★" : "☆"}
                        </button>
                      </td>
                      <td className={styles.reportColumn} aria-labelledby={`report-name-${report.id}`}>
                        <div className={styles.reportCell}>
                          <span
                            className={`${styles.reportIcon} ${report.type === "statistics" ? styles.reportIconStatistics : report.type === "events" ? styles.reportIconEvents : ""}`}
                            aria-hidden="true"
                          >
                            {reportIcon(report.type)}
                          </span>
                          <div>
                            <Link id={`report-name-${report.id}`} href={searchLaunchHref(report.query)}>{report.name}</Link>
                            <small>{report.description}</small>
                            <code>{report.query}</code>
                          </div>
                        </div>
                      </td>
                      <td data-label="Owner">
                        <span className={styles.owner}><i>{report.owner === "Administrator" ? "A" : report.owner[0]}</i>{report.owner}</span>
                      </td>
                      <td data-label="Schedule">
                        <span className={`${styles.status} ${styles[`status${report.status}`]}`}><i />{report.status}</span>
                        <small className={styles.cellSecondary}>{report.cadence}</small>
                      </td>
                      <td data-label="Last run">
                        <span className={`${styles.runStatus} ${report.lastRun === "Failed" ? styles.runFailed : report.lastRun === "Succeeded" ? styles.runSucceeded : ""}`}>
                          <i />{report.lastRun}
                        </span>
                      </td>
                      <td data-label="Modified">
                        <span className={styles.modified}>{report.updated}</span>
                      </td>
                      <td className={styles.openCell}>
                        <Link href={searchLaunchHref(report.query)} aria-label={`Open ${report.name} in Search`}>Open in Search <span aria-hidden="true">›</span></Link>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  );
}
