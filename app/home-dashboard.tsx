import Link from "next/link";

import { searchLaunchHref } from "@/lib/search/launch-url";

interface HomeDashboardProps {
  dataMode: "backend" | "demo";
}

const RECENT_SEARCHES = [
  { title: "Production errors by service", query: "index=gradethis level=ERROR | stats count by service", events: "1,432", ago: "7 min ago", tone: "complete" },
  { title: "API latency over time", query: "index=gradethis | timechart span=5m p95(duration_ms)", events: "12,846", ago: "34 min ago", tone: "complete" },
  { title: "Notification worker retries", query: "index=gradethis logger=notification-worker retry_count>0", events: "391", ago: "Yesterday", tone: "complete" },
  { title: "Checkout trace investigation", query: "index=payments trace_id=\"8e1c…\"", events: "—", ago: "Yesterday", tone: "failed" },
];

export function HomeDashboard({ dataMode }: HomeDashboardProps) {
  return (
    <div className="suite-page home-page">
      <header className="home-hero">
        <div>
          <span className="suite-eyebrow">OPEN SPLUNK</span>
          <h1>Good afternoon, Administrator</h1>
          <p>Search your data, monitor collection health, and continue recent investigations.</p>
        </div>
        <div className="home-hero-actions">
          <Link className="suite-button suite-button--primary" href="/search/">New search</Link>
          <Link className="suite-button" href="/admin/">Add data</Link>
        </div>
      </header>

      <section className="system-notice" aria-label="System status">
        <span className="system-notice__icon">✓</span>
        <div>
          <strong>{dataMode === "backend" ? "Search backend configured" : "Demo workspace ready"}</strong>
          <small>{dataMode === "backend" ? "Use Search to inspect live backend data; launcher metrics remain illustrative." : "Explore the full interface with deterministic sample data."}</small>
        </div>
        <span className={`mode-pill mode-pill--${dataMode}`}>{dataMode === "backend" ? "Backend data" : "Demo data"}</span>
        <Link href="/admin/">View health <span aria-hidden="true">›</span></Link>
      </section>

      <section className="home-metrics" aria-label="Deployment summary">
        <article><span className="metric-symbol metric-symbol--green">▦</span><div><small>gradethis events today</small><strong>18.6M</strong><span className="metric-positive">↑ 8.4% from yesterday</span></div></article>
        <article><span className="metric-symbol metric-symbol--blue">⌕</span><div><small>Searches today</small><strong>143</strong><span>Median 1.7 seconds</span></div></article>
        <article><span className="metric-symbol metric-symbol--orange">◴</span><div><small>Oldest collector lag</small><strong>1.8s</strong><span>All inputs current</span></div></article>
        <article><span className="metric-symbol metric-symbol--slate">▰</span><div><small>gradethis storage</small><strong>284 GB</strong><span>Primary index · 30 day retention</span></div></article>
      </section>

      <div className="home-content-grid">
        <section className="suite-card home-apps-card">
          <header className="suite-card-header"><div><h2>Apps</h2><p>Choose a workspace for your next task.</p></div><Link href="/admin/">Manage apps</Link></header>
          <div className="app-launcher-grid">
            <Link className="app-launch-card" href="/search/">
              <span className="app-launch-icon">⌕</span>
              <div><strong>Search &amp; Reporting</strong><p>Explore events, build searches, and create visualizations.</p><small>Recently used</small></div>
              <b aria-hidden="true">›</b>
            </Link>
            <Link className="app-launch-card" href="/dashboards/">
              <span className="app-launch-icon app-launch-icon--grade">G</span>
              <div><strong>GradeThis Operations</strong><p>Service health, API latency, and grading activity.</p><small>3 dashboards</small></div>
              <b aria-hidden="true">›</b>
            </Link>
            <Link className="app-launch-card" href="/datasets/">
              <span className="app-launch-icon app-launch-icon--data">▦</span>
              <div><strong>Data Manager</strong><p>Review indexes, source types, fields, and retention.</p><small>3 indexes</small></div>
              <b aria-hidden="true">›</b>
            </Link>
          </div>
        </section>

        <aside className="suite-card getting-started-card">
          <header className="suite-card-header"><div><h2>Get started</h2><p>Complete your deployment setup.</p></div><span>3 of 4</span></header>
          <ol className="setup-checklist">
            <li className="is-complete"><span>✓</span><div><strong>Create your first index</strong><small>gradethis · 30 day retention</small></div></li>
            <li className="is-complete"><span>✓</span><div><strong>Connect a collector</strong><small>2 collectors reporting</small></div></li>
            <li className="is-complete"><span>✓</span><div><strong>Run an SPL search</strong><small>143 searches completed today</small></div></li>
            <li><span>4</span><div><strong>Invite another administrator</strong><small>Configure access and roles</small></div><Link href="/admin/">Continue</Link></li>
          </ol>
        </aside>
      </div>

      <section className="suite-card recent-searches-card">
        <header className="suite-card-header"><div><h2>Recent searches</h2><p>Resume an investigation or inspect a previous job.</p></div><Link href="/activity/">View all activity</Link></header>
        <div className="responsive-table-wrap">
          <table className="product-table recent-searches-table">
            <thead><tr><th scope="col">Search</th><th scope="col">Results</th><th scope="col">Status</th><th scope="col">Last run</th><th scope="col"><span className="sr-only">Action</span></th></tr></thead>
            <tbody>
              {RECENT_SEARCHES.map((search) => (
                <tr key={search.title}>
                  <td><Link href={searchLaunchHref(search.query)} aria-label={`Open search: ${search.title}`}><strong>{search.title}</strong><code>{search.query}</code></Link></td>
                  <td className="numeric-data">{search.events}</td>
                  <td><span className={`status-label status-label--${search.tone}`}><i />{search.tone === "complete" ? "Completed" : "Failed"}</span></td>
                  <td>{search.ago}</td>
                  <td><Link className="table-action" href={searchLaunchHref(search.query)} aria-label={`Open ${search.title}`}>Open <span aria-hidden="true">›</span></Link></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
