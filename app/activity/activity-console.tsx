"use client";

import { useMemo, useState } from "react";
import Link from "next/link";

import type { SearchDataMode } from "@/lib/search/backend-data";
import { searchLaunchHref } from "@/lib/search/launch-url";

import { PageHeading } from "../_components/product-shell";
import { BackendActivityConsole } from "./backend-activity-console";

type ActivityFilter = "all" | "running" | "completed" | "failed";

const JOBS = [
  { id: "scheduler_admin_search_147", spl: "index=gradethis level=ERROR | stats count by service", status: "completed", owner: "Administrator", runtime: "1.82 s", events: "1,432", started: "7 min ago" },
  { id: "scheduler_admin_search_146", spl: "index=gradethis | timechart span=5m count by level", status: "running", owner: "Administrator", runtime: "8.34 s", events: "9,812", started: "Now" },
  { id: "scheduler_admin_search_145", spl: "index=platform logger=notification-worker retry_count>0", status: "completed", owner: "Administrator", runtime: "0.94 s", events: "391", started: "34 min ago" },
  { id: "scheduler_admin_search_144", spl: "index=payments trace_id=\"8e1c71a2\"", status: "failed", owner: "Administrator", runtime: "0.12 s", events: "0", started: "Yesterday" },
  { id: "scheduler_admin_search_143", spl: "index=gradethis status>=500 | top path", status: "completed", owner: "Administrator", runtime: "2.17 s", events: "812", started: "Yesterday" },
];

interface ActivityConsoleProps {
  dataMode: SearchDataMode;
  apiBaseUrl: string;
}

export function ActivityConsole({ dataMode, apiBaseUrl }: ActivityConsoleProps) {
  if (dataMode === "backend") return <BackendActivityConsole apiBaseUrl={apiBaseUrl} />;
  return <DemoActivityConsole />;
}

function DemoActivityConsole() {
  const [filter, setFilter] = useState<ActivityFilter>("all");
  const [query, setQuery] = useState("");
  const [notice, setNotice] = useState<string | null>(null);
  const filtered = useMemo(() => JOBS.filter((job) => (filter === "all" || job.status === filter) && `${job.spl} ${job.id}`.toLowerCase().includes(query.toLowerCase())), [filter, query]);

  function exportAuditLog() {
    const contents = [
      "time,event,detail",
      '12:42 PM,Search limits updated,"Administrator changed maximum concurrent searches from 3 to 4."',
      '12:29 PM,Collector authenticated,"api-prod-03 connected with token prefix ospl_ing_a73…"',
      '11:58 AM,Index state changed,"internal was paused by Administrator."',
    ].join("\n");
    const url = URL.createObjectURL(new Blob([contents], { type: "text/csv;charset=utf-8" }));
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = "open-splunk-audit.csv";
    anchor.click();
    URL.revokeObjectURL(url);
    setNotice("Audit log exported.");
  }

  return (
    <div className="suite-page activity-page">
      <PageHeading eyebrow="OPERATIONS" title="Activity" description="Inspect the preview search jobs, exports, and recent system activity." actions={<button className="suite-button" type="button" disabled title="Activity uses a static preview snapshot.">Static preview</button>} />
      <section className="activity-summary" aria-label="Activity summary"><article><span className="status-dot status-dot--running" /><div><strong>1</strong><small>Running now</small></div></article><article><span className="status-dot status-dot--healthy" /><div><strong>142</strong><small>Completed today</small></div></article><article><span className="status-dot status-dot--warning" /><div><strong>2</strong><small>Warnings today</small></div></article><article><span className="status-dot status-dot--error" /><div><strong>1</strong><small>Failed today</small></div></article></section>
      <section className="suite-card activity-jobs-card">
        <header className="activity-tabs-row">
          <div className="activity-filter-group" aria-label="Job status filter">{(["all", "running", "completed", "failed"] as const).map((item) => <button className={`activity-filter-button${filter === item ? " active" : ""}`} aria-pressed={filter === item} type="button" onClick={() => setFilter(item)} key={item}>{item[0].toUpperCase() + item.slice(1)}{item === "running" ? <span>1</span> : null}</button>)}</div>
          <label><span className="sr-only">Filter activity</span><i aria-hidden="true">⌕</i><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Filter SPL or job ID" /></label>
        </header>
        <div className="responsive-table-wrap"><table className="product-table activity-table"><thead><tr><th scope="col">Search</th><th scope="col">Status</th><th scope="col">Owner</th><th scope="col">Runtime</th><th scope="col">Events</th><th scope="col">Started</th><th scope="col"><span className="sr-only">Actions</span></th></tr></thead><tbody>{filtered.map((job) => <tr key={job.id}><td><Link href={searchLaunchHref(job.spl)} aria-label={`Open search job ${job.id}`}><strong>{job.spl}</strong><code>{job.id}</code></Link></td><td><span className={`status-label status-label--${job.status === "completed" ? "complete" : job.status}`}><i />{job.status[0].toUpperCase() + job.status.slice(1)}</span></td><td>{job.owner}</td><td>{job.runtime}</td><td className="numeric-data">{job.events}</td><td>{job.started}</td><td><Link className="table-action" href={searchLaunchHref(job.spl)}>Open ›</Link></td></tr>)}</tbody></table></div>
        {filtered.length === 0 ? <div className="product-empty-state"><span>⌕</span><strong>No matching activity</strong><p>Try another status or clear the filter.</p><button type="button" onClick={() => { setFilter("all"); setQuery(""); }}>Clear filters</button></div> : null}
      </section>
      <section className="suite-card audit-preview"><header className="suite-card-header"><div><h2>Administrative events</h2><p>Configuration and access changes recorded by the control plane.</p></div><button type="button" onClick={exportAuditLog}>Export audit log</button></header><ol><li><span className="audit-time">12:42 PM</span><i className="audit-glyph">⚙</i><div><strong>Search limits updated</strong><p>Administrator changed maximum concurrent searches from 3 to 4.</p></div></li><li><span className="audit-time">12:29 PM</span><i className="audit-glyph">⇣</i><div><strong>Collector authenticated</strong><p>api-prod-03 connected with token prefix ospl_ing_a73…</p></div></li><li><span className="audit-time">11:58 AM</span><i className="audit-glyph">▦</i><div><strong>Index state changed</strong><p>internal was paused by Administrator.</p></div></li></ol></section>
      {notice === null ? null : <output className="toast toast-success"><span>✓</span><strong>{notice}</strong><button type="button" aria-label="Dismiss notification" onClick={() => setNotice(null)}>×</button></output>}
    </div>
  );
}
