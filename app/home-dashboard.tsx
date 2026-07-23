import Link from "next/link";

import { searchLaunchHref } from "@/lib/search/launch-url";

interface HomeDashboardProps {
  dataMode: "backend" | "demo";
}

const RECENT_SEARCHES = [
  { title: "Production errors by service", query: "index=gradethis level=ERROR | stats count by service", events: "1,432", ago: "7 min ago", tone: "complete" },
  { title: "Slowest API routes", query: "index=gradethis duration_ms=* | stats p95(duration_ms) AS p95_ms BY path | sort -p95_ms", events: "42", ago: "34 min ago", tone: "complete" },
  { title: "Notification worker retries", query: "index=gradethis logger=notification-worker retry_count>0", events: "391", ago: "Yesterday", tone: "complete" },
  { title: "Checkout trace investigation", query: "index=payments trace_id=\"8e1c…\"", events: "—", ago: "Yesterday", tone: "failed" },
];

function backendSafeExampleQuery(query: string): string {
  return query
    .replace(/^index=(?:gradethis|payments)\b/, "index=*")
    .replace(/trace_id="[^"]*…[^"]*"/, "trace_id=*");
}

export function HomeDashboard({ dataMode }: HomeDashboardProps) {
  const fixtureSearchHref = (query: string) => searchLaunchHref(query, {
    label: dataMode === "backend" ? "Example search draft" : undefined,
    run: dataMode !== "backend",
  });

  return (
    <div className="suite-page home-page">
      <header className="home-hero">
        <div>
          <span className="suite-eyebrow">OPEN SPLUNK</span>
          <h1>Good afternoon, Administrator</h1>
          <p>{dataMode === "backend"
            ? "Open Search and the backend-supported resource catalogs; Administration reports connection state."
            : "Explore the deterministic search, administration, and operations preview."}</p>
        </div>
        <div className="home-hero-actions">
          <Link className="suite-button suite-button--primary" href="/search/">New search</Link>
          <Link className="suite-button" href="/admin/">{dataMode === "backend" ? "Administration" : "Administration preview"}</Link>
        </div>
      </header>

      <section className="system-notice" aria-label="System status">
        <span className="system-notice__icon" aria-hidden="true">{dataMode === "backend" ? "↔" : "✓"}</span>
        <div>
          <strong>{dataMode === "backend" ? "Backend mode selected" : "Demo workspace ready"}</strong>
          <small>{dataMode === "backend" ? "Administration reports connection health; fixture-only pages remain visibly marked." : "Explore the interface with deterministic sample data."}</small>
        </div>
        <span className={`mode-pill mode-pill--${dataMode}`}>{dataMode === "backend" ? "Backend mode" : "Demo data"}</span>
        <Link href="/admin/">{dataMode === "backend" ? "Check connection" : "Open settings"} <span aria-hidden="true">›</span></Link>
      </section>

      <section className="home-metrics" aria-label={dataMode === "backend" ? "Backend-supported surfaces" : "Preview deployment summary"}>
        {dataMode === "backend" ? (
          <>
            <article><span className="metric-symbol metric-symbol--green">⌕</span><div><small>Search workspace</small><strong>Search</strong><span>Query authorized indexes when available</span></div></article>
            <article><span className="metric-symbol metric-symbol--blue">▦</span><div><small>Index catalog</small><strong>Datasets</strong><span>Read bootstrap summaries when available</span></div></article>
            <article><span className="metric-symbol metric-symbol--orange">▤</span><div><small>Persisted definitions</small><strong>Reports</strong><span>Available when registered by the server</span></div></article>
            <article><span className="metric-symbol metric-symbol--slate">↻</span><div><small>Backend execution</small><strong>Activity</strong><span>Inspect retained jobs and history</span></div></article>
          </>
        ) : (
          <>
            <article><span className="metric-symbol metric-symbol--green">▦</span><div><small>Preview events today</small><strong>18.6M</strong><span className="metric-positive">↑ 8.4% fixture change</span></div></article>
            <article><span className="metric-symbol metric-symbol--blue">⌕</span><div><small>Preview searches today</small><strong>143</strong><span>Median 1.7 seconds</span></div></article>
            <article><span className="metric-symbol metric-symbol--orange">◴</span><div><small>Preview collector lag</small><strong>1.8s</strong><span>Fixture inputs current</span></div></article>
            <article><span className="metric-symbol metric-symbol--slate">▰</span><div><small>Preview storage</small><strong>284 GB</strong><span>Sample index · 30 day retention</span></div></article>
          </>
        )}
      </section>

      <div className="home-content-grid">
        <section className="suite-card home-apps-card">
          <header className="suite-card-header"><div><h2>Apps</h2><p>Choose a workspace for your next task.</p></div><Link href="/admin/">Administration</Link></header>
          <div className="app-launcher-grid">
            <Link className="app-launch-card" href="/search/">
              <span className="app-launch-icon">⌕</span>
              <div><strong>Search &amp; Reporting</strong><p>Explore events, build searches, and create visualizations.</p><small>Recently used</small></div>
              <b aria-hidden="true">›</b>
            </Link>
            <Link className="app-launch-card" href="/dashboards/">
              <span className="app-launch-icon app-launch-icon--grade">G</span>
              <div><strong>GradeThis Operations</strong><p>Illustrative service-health and latency layout.</p><small>Static preview</small></div>
              <b aria-hidden="true">›</b>
            </Link>
            <Link className="app-launch-card" href="/datasets/">
              <span className="app-launch-icon app-launch-icon--data">▦</span>
              <div><strong>Data Manager</strong><p>{dataMode === "backend" ? "Browse authorized index summaries from system bootstrap." : "Explore the deterministic index catalog preview."}</p><small>{dataMode === "backend" ? "Backend catalog" : "3 preview indexes"}</small></div>
              <b aria-hidden="true">›</b>
            </Link>
          </div>
        </section>

        <aside className="suite-card getting-started-card">
          <header className="suite-card-header"><div><h2>{dataMode === "backend" ? "Connected workflow" : "Explore the preview"}</h2><p>{dataMode === "backend" ? "Open backend-supported surfaces." : "Try each deterministic workspace."}</p></div></header>
          {dataMode === "backend" ? (
            <ol className="setup-checklist">
              <li><span>1</span><div><strong>Run an SPL search</strong><small>Query authorized indexes</small></div><Link href="/search/">Open</Link></li>
              <li><span>2</span><div><strong>Browse index summaries</strong><small>Read the bootstrap catalog</small></div><Link href="/datasets/">Open</Link></li>
              <li><span>3</span><div><strong>Review saved definitions</strong><small>When registered by the server</small></div><Link href="/reports/">Open</Link></li>
              <li><span>4</span><div><strong>Inspect search activity</strong><small>Jobs and history remain separate</small></div><Link href="/activity/">Open</Link></li>
            </ol>
          ) : (
            <ol className="setup-checklist">
              <li><span>1</span><div><strong>Run a preview search</strong><small>Use deterministic events</small></div><Link href="/search/">Open</Link></li>
              <li><span>2</span><div><strong>Browse preview datasets</strong><small>Inspect fixture index cards</small></div><Link href="/datasets/">Open</Link></li>
              <li><span>3</span><div><strong>Review preview activity</strong><small>Inspect sample job states</small></div><Link href="/activity/">Open</Link></li>
              <li><span>4</span><div><strong>Open administration</strong><small>Explore sample controls</small></div><Link href="/admin/">Open</Link></li>
            </ol>
          )}
        </aside>
      </div>

      <section className="suite-card recent-searches-card">
        <header className="suite-card-header"><div><h2>{dataMode === "backend" ? "Example search drafts" : "Preview recent searches"}</h2><p>{dataMode === "backend" ? "These illustrative queries open without submitting a backend job." : "Resume a deterministic sample investigation."}</p></div><Link href={dataMode === "backend" ? "/search/" : "/activity/"}>{dataMode === "backend" ? "Open Search" : "View preview activity"}</Link></header>
        <div className="responsive-table-wrap">
          <table className={`product-table recent-searches-table${dataMode === "backend" ? " recent-searches-table--drafts" : ""}`}>
            <thead><tr><th scope="col">{dataMode === "backend" ? "Example search" : "Search"}</th>{dataMode === "backend" ? null : <><th scope="col">Results</th><th scope="col">Status</th><th scope="col">Last run</th></>}<th scope="col"><span className="sr-only">Action</span></th></tr></thead>
            <tbody>
              {RECENT_SEARCHES.map((search) => {
                const exampleQuery = dataMode === "backend"
                  ? backendSafeExampleQuery(search.query)
                  : search.query;
                return (
                  <tr key={search.title}>
                    <td><Link href={fixtureSearchHref(exampleQuery)} aria-label={`${dataMode === "backend" ? "Open example draft" : "Open preview search"}: ${search.title}`}><strong>{search.title}</strong><code>{exampleQuery}</code></Link></td>
                    {dataMode === "backend" ? null : <><td className="numeric-data">{search.events}</td><td><span className={`status-label status-label--${search.tone}`}><i />{search.tone === "complete" ? "Completed" : "Failed"}</span></td><td>{search.ago}</td></>}
                    <td><Link className="table-action" href={fixtureSearchHref(exampleQuery)} aria-label={`${dataMode === "backend" ? "Open example draft" : "Open"} ${search.title}`}>{dataMode === "backend" ? "Open draft" : "Open"} <span aria-hidden="true">›</span></Link></td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
