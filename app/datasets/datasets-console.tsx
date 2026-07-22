"use client";

import Link from "next/link";
import { useMemo, useState } from "react";

import { searchLaunchHref } from "@/lib/search/launch-url";

import { PageHeading } from "../_components/product-shell";

const DATASETS = [
  { name: "gradethis", description: "GradeThis application and request logs", events: "18.6M", sources: 4, fields: 42, size: "284 GB", retention: "30 days", color: "green", status: "Active" },
  { name: "platform", description: "Workers, notification delivery, and infrastructure", events: "4.2M", sources: 7, fields: 31, size: "91 GB", retention: "14 days", color: "blue", status: "Active" },
  { name: "internal", description: "Open Splunk server and collector diagnostics", events: "643K", sources: 3, fields: 26, size: "12 GB", retention: "7 days", color: "orange", status: "Paused" },
] as const;

const COMMON_FIELDS = [
  ["host", "100%", "3 values"],
  ["source", "100%", "14 values"],
  ["sourcetype", "100%", "5 values"],
  ["level", "99.8%", "4 values"],
  ["trace_id", "80.4%", "10.3K values"],
  ["duration_ms", "68.1%", "1.8K values"],
  ["service", "96.2%", "7 values"],
  ["environment", "100%", "2 values"],
] as const;

export function DatasetsConsole() {
  const [filter, setFilter] = useState("");
  const [view, setView] = useState<"cards" | "table">("cards");
  const visible = useMemo(
    () => DATASETS.filter((item) => `${item.name} ${item.description}`.toLowerCase().includes(filter.toLowerCase())),
    [filter],
  );

  return (
    <div className="suite-page datasets-page">
      <PageHeading
        eyebrow="DATA"
        title="Datasets"
        description="Understand available indexes, source types, and field coverage."
        actions={<><Link className="suite-button" href="/admin/">Manage indexes</Link><Link className="suite-button suite-button--primary" href="/search/">Search data</Link></>}
      />

      <div className="dataset-toolbar">
        <label><span className="sr-only">Filter datasets</span><i aria-hidden="true">⌕</i><input value={filter} onChange={(event) => setFilter(event.target.value)} placeholder="Find an index or description" /></label>
        <fieldset className="dataset-view-toggle">
          <legend className="sr-only">Dataset view</legend>
          <button className={view === "cards" ? "active" : undefined} type="button" aria-pressed={view === "cards"} onClick={() => setView("cards")}>▥ Cards</button>
          <button className={view === "table" ? "active" : undefined} type="button" aria-pressed={view === "table"} onClick={() => setView("table")}>☷ Table</button>
        </fieldset>
      </div>

      {view === "cards" ? (
        <div className="dataset-grid">
          {visible.map((item) => (
            <article className="dataset-card" key={item.name}>
              <header>
                <span className={`dataset-icon dataset-icon--${item.color}`} aria-hidden="true">▦</span>
                <div><h2>{item.name}</h2><p>{item.description}</p></div>
                <span className={`status-label status-label--${item.status === "Active" ? "complete" : "neutral"}`}><i />{item.status}</span>
              </header>
              <dl>
                <div><dt>Events today</dt><dd>{item.events}</dd></div>
                <div><dt>Source types</dt><dd>{item.sources}</dd></div>
                <div><dt>Known fields</dt><dd>{item.fields}</dd></div>
                <div><dt>Storage</dt><dd>{item.size}</dd></div>
              </dl>
              <div className="dataset-retention">
                <span aria-hidden="true"><i style={{ width: item.name === "gradethis" ? "64%" : item.name === "platform" ? "42%" : "18%" }} /></span>
                <small>{item.retention} retention · {item.status === "Paused" ? "ingestion paused" : item.name === "gradethis" ? "11 days remaining in current partition" : "healthy"}</small>
              </div>
              <footer>
                <Link href={searchLaunchHref(`index=${item.name} | sort -_time`)}>Search index</Link>
                <Link href={searchLaunchHref(`index=${item.name} | stats count by sourcetype | sort -count`)}>Explore sources</Link>
                <button className="row-overflow" type="button" aria-label={`Actions for ${item.name}`} disabled title="Dataset actions are managed from Administration">•••</button>
              </footer>
            </article>
          ))}
        </div>
      ) : (
        <section className="suite-card">
          <div className="responsive-table-wrap">
            <table className="product-table">
              <thead><tr><th scope="col">Name</th><th scope="col">Events today</th><th scope="col">Source types</th><th scope="col">Fields</th><th scope="col">Storage</th><th scope="col">Retention</th><th scope="col"><span className="sr-only">Action</span></th></tr></thead>
              <tbody>{visible.map((item) => <tr key={item.name}><td><strong>{item.name}</strong><small className="table-secondary">{item.description}</small></td><td>{item.events}</td><td>{item.sources}</td><td>{item.fields}</td><td>{item.size}</td><td>{item.retention}</td><td><Link className="table-action" href={searchLaunchHref(`index=${item.name} | sort -_time`)} aria-label={`Search ${item.name}`}>Search ›</Link></td></tr>)}</tbody>
            </table>
          </div>
        </section>
      )}

      {visible.length === 0 ? <div className="product-empty-state"><span aria-hidden="true">⌕</span><strong>No matching datasets</strong><p>Try another index or source-type name.</p><button type="button" onClick={() => setFilter("")}>Clear filter</button></div> : null}

      <section className="suite-card field-catalog">
        <header className="suite-card-header"><div><h2>Common fields</h2><p>High-coverage fields discovered across active indexes.</p></div><Link href={searchLaunchHref("index=gradethis | stats count by sourcetype | sort -count")}>Profile source types</Link></header>
        <div className="field-catalog-grid">
          {COMMON_FIELDS.map(([name, coverage, values]) => (
            <Link href={searchLaunchHref(`index=gradethis | stats count by ${name}`)} key={name} aria-label={`Analyze values for ${name}`}>
              <span>{name === "duration_ms" ? "#" : "a"}</span><div><strong>{name}</strong><small>{coverage} coverage · {values}</small></div><b aria-hidden="true">›</b>
            </Link>
          ))}
        </div>
      </section>
    </div>
  );
}
