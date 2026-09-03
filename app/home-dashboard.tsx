"use client";

import { useEffect, useMemo, useState, useSyncExternalStore } from "react";
import Link from "next/link";

import {
  createOpenSplunkApiClient,
  getSystemBootstrap,
} from "@/lib/api";
import { createErrorMessage } from "@/lib/error-message";
import {
  backendAppHref,
  currentBackendAppId,
  subscribeToBackendAppId,
} from "@/lib/search/app-navigation";
import { type ExampleDraftTitle, exampleDraft } from "@/lib/search/example-drafts";
import { historySearchLaunchHref, searchLaunchHref } from "@/lib/search/launch-url";
import {
  listServerSearchHistory,
  type ServerSearchHistoryEntry,
} from "@/lib/search/server-objects";

import { BackendResourceState } from "./_components/backend-resource-state";
import { StatusLabel, type StatusTone } from "./_components/status";
import { AppIcon } from "./_components/app-icon";
import { formatMediumDateTime } from "./_components/date-format";
import { homeSearchFinishedAt, homeSearchStatus } from "./home-dashboard-data";

interface HomeDashboardProps {
  apiBaseUrl?: string;
  dataMode: "backend" | "demo";
}

type RecentHistoryState = "loading" | "available" | "unavailable" | "error";

// The preview's recent searches replay the example drafts, so a query this
// table links to is the one the search workspace's examples gallery loads.
const RECENT_SEARCHES: { ago: string; events: string; query: string; title: string; tone: StatusTone }[] = ([
  { title: "Production errors by service", events: "1,432", ago: "7 min ago", tone: "success" },
  { title: "Slowest API routes", events: "42", ago: "34 min ago", tone: "success" },
  { title: "Notification worker retries", events: "391", ago: "Yesterday", tone: "success" },
  { title: "Checkout trace investigation", events: "—", ago: "Yesterday", tone: "error" },
] satisfies { ago: string; events: string; title: ExampleDraftTitle; tone: StatusTone }[]).map((search) => ({
  ago: search.ago,
  events: search.events,
  query: exampleDraft(search.title).spl,
  title: search.title,
  tone: search.tone,
}));

const recentHistoryErrorMessage = createErrorMessage("The server did not return usable recent search history.");

function fixtureSearchHref(query: string): string {
  return searchLaunchHref(query);
}

function BackendRecentSearches({
  apiBaseUrl,
  preferredAppId,
}: {
  apiBaseUrl: string;
  preferredAppId: string | undefined;
}) {
  const client = useMemo(() => createOpenSplunkApiClient({ baseUrl: apiBaseUrl }), [apiBaseUrl]);
  const [state, setState] = useState<RecentHistoryState>("loading");
  const [entries, setEntries] = useState<ServerSearchHistoryEntry[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [generation, setGeneration] = useState(0);
  const [selectedAppId, setSelectedAppId] = useState<string | null>(null);
  const navigationAppId = selectedAppId ?? preferredAppId;
  const contextualHref = (href: string) => navigationAppId === undefined
    ? href
    : backendAppHref(href, navigationAppId);

  useEffect(() => {
    const controller = new AbortController();
    let current = true;
    setState("loading");
    setError(null);
    setSelectedAppId(null);
    void (async () => {
      try {
        const bootstrap = await getSystemBootstrap(client, preferredAppId, { signal: controller.signal });
        if (!current) return;
        setSelectedAppId(bootstrap.selectedAppId);
        const result = await listServerSearchHistory(client, bootstrap, {
          appId: bootstrap.selectedAppId ?? undefined,
          maximumPages: 1,
          pageSize: Math.max(1, Math.min(4, bootstrap.limits.maximumPageSize || 4)),
          signal: controller.signal,
        });
        if (!current) return;
        if (result.status === "unavailable") {
          setEntries([]);
          setState("unavailable");
          return;
        }
        setEntries(result.value.items.slice(0, 4));
        setState("available");
      } catch (reason) {
        if (!current || controller.signal.aborted) return;
        setEntries([]);
        setError(recentHistoryErrorMessage(reason));
        setState("error");
      }
    })();
    return () => {
      current = false;
      controller.abort();
    };
  }, [client, generation, preferredAppId]);

  let content;
  if (state === "loading") {
    content = <BackendResourceState kind="loading" title="Loading recent searches" message="Reading the latest persisted terminal searches from the backend." />;
  } else if (state === "error") {
    content = <BackendResourceState kind="error" title="Recent searches unavailable" message={error ?? "The recent search request failed."} action={<button type="button" onClick={() => setGeneration((current) => current + 1)}>Retry</button>} />;
  } else if (state === "unavailable") {
    content = <BackendResourceState kind="unavailable" title="Search history is not enabled" message="This server does not advertise the persisted search-history feature." action={<Link href={contextualHref("/search/events/")}>Open Search</Link>} />;
  } else if (entries.length === 0) {
    content = <BackendResourceState kind="empty" title="No persisted search history" message="Completed, failed, or canceled searches for this app will appear here after the backend retains them." action={<Link href={contextualHref("/search/events/")}>Run a search</Link>} />;
  } else {
    content = (
      <div className="table-wrap">
        <table className="table recent-searches-table backend-home-history-table">
          <caption className="sr-only">Recent persisted backend searches</caption>
          <thead><tr><th scope="col">Search</th><th scope="col">Results</th><th scope="col">Status</th><th scope="col">Last run</th></tr></thead>
          <tbody>{entries.map((entry) => {
            const status = homeSearchStatus(entry.finalState);
            return (
              <tr key={entry.id}>
                <td><Link href={contextualHref(historySearchLaunchHref(entry.id))} aria-label={`Rerun search ${entry.id}`}><strong>{entry.search.spl}</strong><code>{entry.id}</code>{entry.failureMessage === null ? null : <small className="table-error-detail">{entry.failureMessage}</small>}</Link></td>
                <td className="numeric-data">{entry.producedRows.toLocaleString()}</td>
                <td><StatusLabel tone={status.tone}>{status.label}</StatusLabel></td>
                <td>{formatMediumDateTime(homeSearchFinishedAt(entry), "Not recorded")}</td>
              </tr>
            );
          })}</tbody>
        </table>
      </div>
    );
  }

  return (
    <section className="suite-card recent-searches-card recent-searches-card--backend">
      <header className="suite-card-header"><div><h2>Recent searches</h2><p>Rerun terminal searches retained for the selected backend app.</p></div><Link href={contextualHref("/activity/history/")}>View all activity</Link></header>
      {content}
    </section>
  );
}

export function HomeDashboard({ apiBaseUrl = "", dataMode }: HomeDashboardProps) {
  const preferredAppId = useSyncExternalStore(
    subscribeToBackendAppId,
    currentBackendAppId,
    () => undefined,
  );
  const productHref = (href: string) => dataMode === "backend" && preferredAppId !== undefined
    ? backendAppHref(href, preferredAppId)
    : href;
  return (
    <div className="suite-page home-page">
      <header className="home-hero">
        <div>
          <span className="suite-eyebrow">OPEN SPLUNK</span>
          <h1>{dataMode === "backend" ? "Welcome to your local workspace" : "Good afternoon, Administrator"}</h1>
          <p>{dataMode === "backend"
            ? "Open Search and the backend-supported resource catalogs; Administration reports connection state."
            : "Explore the deterministic search, administration, and operations preview."}</p>
        </div>
        <div className="home-hero-actions">
          <Link className="button button--primary" href={productHref("/search/events/")}>New search</Link>
          <Link className="button" href={productHref("/admin/")}>{dataMode === "backend" ? "Administration" : "Administration preview"}</Link>
        </div>
      </header>

      <section className="system-notice" aria-label="System status">
        <span className="system-notice__icon" aria-hidden="true"><AppIcon name={dataMode === "backend" ? "info" : "check"} size="xs" /></span>
        <div>
          <strong>{dataMode === "backend" ? "Backend mode selected" : "Demo workspace ready"}</strong>
          <small>{dataMode === "backend" ? "Connected surfaces report their own backend availability and errors." : "Explore the interface with deterministic sample data."}</small>
        </div>
        <span className={`badge ${dataMode === "backend" ? "badge--success" : "badge--warning"}`}>{dataMode === "backend" ? "Backend mode" : "Demo data"}</span>
        <Link href={productHref("/admin/")}>{dataMode === "backend" ? "Check connection" : "Open settings"} <AppIcon name="chevron-right" size="xs" /></Link>
      </section>

      <section className="home-metrics" aria-label={dataMode === "backend" ? "Backend-supported surfaces" : "Preview deployment summary"}>
        {dataMode === "backend" ? (
          <>
            <article><span className="metric-symbol metric-symbol--green" aria-hidden="true">⌕</span><div><small>Search workspace</small><strong>Search</strong><span>Query authorized indexes when available</span></div></article>
            <article><span className="metric-symbol metric-symbol--blue" aria-hidden="true">▦</span><div><small>Index catalog</small><strong>Datasets</strong><span>Read bootstrap summaries when available</span></div></article>
            <article><span className="metric-symbol metric-symbol--orange" aria-hidden="true">▤</span><div><small>Persisted definitions</small><strong>Reports</strong><span>Available when registered by the server</span></div></article>
            <article><span className="metric-symbol metric-symbol--slate" aria-hidden="true">↻</span><div><small>Backend execution</small><strong>Activity</strong><span>Inspect retained jobs and history</span></div></article>
          </>
        ) : (
          <>
            <article><span className="metric-symbol metric-symbol--green" aria-hidden="true">▦</span><div><small>Preview events today</small><strong>18.6M</strong><span className="metric-positive">↑ 8.4% fixture change</span></div></article>
            <article><span className="metric-symbol metric-symbol--blue" aria-hidden="true">⌕</span><div><small>Preview searches today</small><strong>143</strong><span>Median 1.7 seconds</span></div></article>
            <article><span className="metric-symbol metric-symbol--orange" aria-hidden="true">◴</span><div><small>Preview collector lag</small><strong>1.8s</strong><span>Fixture inputs current</span></div></article>
            <article><span className="metric-symbol metric-symbol--slate" aria-hidden="true">▰</span><div><small>Preview storage</small><strong>284 GB</strong><span>Sample index · 30 day retention</span></div></article>
          </>
        )}
      </section>

      <div className="home-content-grid">
        <section className="suite-card home-apps-card">
          <header className="suite-card-header"><div><h2>Apps</h2><p>Choose a workspace for your next task.</p></div><Link href={productHref("/admin/")}>Administration</Link></header>
          <div className="app-launcher-grid">
            <Link className="app-launch-card" href={productHref("/search/events/")}>
              <span className="app-launch-icon" aria-hidden="true">⌕</span>
              <div><strong>Search &amp; Reporting</strong><p>Explore events, build searches, and create visualizations.</p><small>Recently used</small></div>
              <b aria-hidden="true"><AppIcon name="chevron-right" size="sm" /></b>
            </Link>
            <Link className="app-launch-card" href={productHref("/dashboards/")}>
              <span className="app-launch-icon app-launch-icon--grade" aria-hidden="true">G</span>
              <div><strong>GradeThis Operations</strong><p>{dataMode === "backend" ? "Open persisted dashboards and execute their panel searches." : "Illustrative service-health and latency layout."}</p><small>{dataMode === "backend" ? "Backend dashboards" : "Static preview"}</small></div>
              <b aria-hidden="true"><AppIcon name="chevron-right" size="sm" /></b>
            </Link>
            <Link className="app-launch-card" href={productHref("/datasets/")}>
              <span className="app-launch-icon app-launch-icon--data" aria-hidden="true">▦</span>
              <div><strong>Data Manager</strong><p>{dataMode === "backend" ? "Browse authorized index summaries from system bootstrap." : "Explore the deterministic index catalog preview."}</p><small>{dataMode === "backend" ? "Backend catalog" : "3 preview indexes"}</small></div>
              <b aria-hidden="true"><AppIcon name="chevron-right" size="sm" /></b>
            </Link>
          </div>
        </section>

        <aside className="suite-card getting-started-card">
          <header className="suite-card-header"><div><h2>{dataMode === "backend" ? "Connected workflow" : "Explore the preview"}</h2><p>{dataMode === "backend" ? "Open backend-supported surfaces." : "Try each deterministic workspace."}</p></div></header>
          {dataMode === "backend" ? (
            <ol className="setup-checklist">
              <li><span aria-hidden="true">1</span><div><strong>Run an SPL search</strong><small>Query authorized indexes</small></div><Link href={productHref("/search/events/")}>Open</Link></li>
              <li><span aria-hidden="true">2</span><div><strong>Browse index summaries</strong><small>Read the bootstrap catalog</small></div><Link href={productHref("/datasets/")}>Open</Link></li>
              <li><span aria-hidden="true">3</span><div><strong>Review saved definitions</strong><small>When registered by the server</small></div><Link href={productHref("/reports/saved-searches/")}>Open</Link></li>
              <li><span aria-hidden="true">4</span><div><strong>Inspect search activity</strong><small>Jobs and history remain separate</small></div><Link href={productHref("/activity/jobs/")}>Open</Link></li>
            </ol>
          ) : (
            <ol className="setup-checklist">
              <li><span aria-hidden="true">1</span><div><strong>Run a preview search</strong><small>Use deterministic events</small></div><Link href="/search/events/">Open</Link></li>
              <li><span aria-hidden="true">2</span><div><strong>Browse preview datasets</strong><small>Inspect fixture index cards</small></div><Link href="/datasets/">Open</Link></li>
              <li><span aria-hidden="true">3</span><div><strong>Review preview activity</strong><small>Inspect sample job states</small></div><Link href="/activity/jobs/">Open</Link></li>
              <li><span aria-hidden="true">4</span><div><strong>Open administration</strong><small>Explore sample controls</small></div><Link href="/admin/">Open</Link></li>
            </ol>
          )}
        </aside>
      </div>

      {dataMode === "backend" ? <BackendRecentSearches apiBaseUrl={apiBaseUrl} preferredAppId={preferredAppId} /> : (
        <section className="suite-card recent-searches-card">
          <header className="suite-card-header"><div><h2>Preview recent searches</h2><p>Resume a deterministic sample investigation.</p></div><Link href="/activity/jobs/">View preview activity</Link></header>
          <div className="table-wrap">
            <table className="table recent-searches-table">
              <thead><tr><th scope="col">Search</th><th scope="col">Results</th><th scope="col">Status</th><th scope="col">Last run</th></tr></thead>
              <tbody>{RECENT_SEARCHES.map((search) => (
                <tr key={search.title}>
                  <td><Link href={fixtureSearchHref(search.query)} aria-label={`Open preview search: ${search.title}`}><strong>{search.title}</strong><code>{search.query}</code></Link></td>
                  <td className="numeric-data">{search.events}</td><td><StatusLabel tone={search.tone}>{search.tone === "success" ? "Completed" : "Failed"}</StatusLabel></td><td>{search.ago}</td>
                </tr>
              ))}</tbody>
            </table>
          </div>
        </section>
      )}
    </div>
  );
}
