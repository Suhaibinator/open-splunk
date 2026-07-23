"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import Link from "next/link";

import { IndexAccessState, IndexState } from "@/gen/ts/open_splunk/v1/index";
import {
  createOpenSplunkApiClient,
  getSystemBootstrap,
  type BrowserIndexModel,
  type SystemBootstrapModel,
} from "@/lib/api";
import { searchLaunchHref } from "@/lib/search/launch-url";

import { PageHeading } from "../_components/product-shell";

interface BackendDatasetsConsoleProps {
  apiBaseUrl: string;
}

function errorMessage(error: unknown): string {
  return error instanceof Error && error.message.trim().length > 0
    ? error.message
    : "The server did not return a usable bootstrap response.";
}

function stateLabel(index: BrowserIndexModel): string {
  if (index.state === IndexState.INDEX_STATE_ACTIVE) return "Active";
  if (index.state === IndexState.INDEX_STATE_ARCHIVED) return "Archived";
  if (index.state === IndexState.INDEX_STATE_DELETING) return "Deleting";
  return "Unknown";
}

function accessLabel(value: IndexAccessState): string {
  if (value === IndexAccessState.INDEX_ACCESS_STATE_ENABLED) return "Enabled";
  if (value === IndexAccessState.INDEX_ACCESS_STATE_DISABLED) return "Disabled";
  return "Unknown";
}

export function BackendDatasetsConsole({ apiBaseUrl }: BackendDatasetsConsoleProps) {
  const client = useMemo(() => createOpenSplunkApiClient({ baseUrl: apiBaseUrl }), [apiBaseUrl]);
  const [bootstrap, setBootstrap] = useState<SystemBootstrapModel | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [generation, setGeneration] = useState(0);
  const [filter, setFilter] = useState("");
  const [view, setView] = useState<"cards" | "table">("cards");
  const reload = useCallback(() => setGeneration((current) => current + 1), []);

  useEffect(() => {
    const controller = new AbortController();
    let current = true;
    setLoading(true);
    setError(null);
    void getSystemBootstrap(client, undefined, { signal: controller.signal }).then(
      (response) => {
        if (!current) return;
        setBootstrap(response);
        setLoading(false);
      },
      (reason: unknown) => {
        if (!current || controller.signal.aborted) return;
        setBootstrap(null);
        setError(errorMessage(reason));
        setLoading(false);
      },
    );
    return () => {
      current = false;
      controller.abort();
    };
  }, [client, generation]);

  const visible = useMemo(() => {
    const normalized = filter.trim().toLowerCase();
    return (bootstrap?.indexes ?? []).filter((index) =>
      normalized.length === 0
      || `${index.name} ${index.displayName}`.toLowerCase().includes(normalized));
  }, [bootstrap, filter]);

  return (
    <div className="suite-page datasets-page">
      <PageHeading
        eyebrow="DATA"
        title="Datasets"
        description="Browse the indexes authorized for this browser session."
        actions={<><Link className="suite-button" href="/admin/">Manage indexes</Link><Link className="suite-button suite-button--primary" href="/search/">Search data</Link></>}
      />

      {loading ? (
        <DatasetState kind="loading" title="Loading datasets" message="Reading the authorized index catalog from system bootstrap…" />
      ) : error !== null ? (
        <DatasetState kind="error" title="Datasets could not be loaded" message={error} action={<button type="button" onClick={reload}>Retry</button>} />
      ) : bootstrap === null ? null : (
        <>
          <div className="dataset-toolbar">
            <label><span className="sr-only">Filter datasets</span><i aria-hidden="true">⌕</i><input value={filter} onChange={(event) => setFilter(event.target.value)} placeholder="Find an index" /></label>
            <fieldset className="dataset-view-toggle">
              <legend className="sr-only">Dataset view</legend>
              <button className={view === "cards" ? "active" : undefined} type="button" aria-pressed={view === "cards"} onClick={() => setView("cards")}>▥ Cards</button>
              <button className={view === "table" ? "active" : undefined} type="button" aria-pressed={view === "table"} onClick={() => setView("table")}>☷ Table</button>
            </fieldset>
          </div>

          {visible.length === 0 ? (
            <DatasetState
              kind="empty"
              title={bootstrap.indexes.length === 0 ? "No authorized indexes" : "No matching datasets"}
              message={bootstrap.indexes.length === 0 ? "The backend did not return any indexes authorized for search." : "Try another index name."}
              action={filter.length > 0 ? <button type="button" onClick={() => setFilter("")}>Clear filter</button> : undefined}
            />
          ) : view === "cards" ? (
            <div className="dataset-grid backend-dataset-grid">
              {visible.map((index, position) => {
                const state = stateLabel(index);
                return (
                  <article className="dataset-card" key={index.id}>
                    <header>
                      <span className={`dataset-icon dataset-icon--${position % 3 === 0 ? "green" : position % 3 === 1 ? "blue" : "orange"}`} aria-hidden="true">▦</span>
                      <div><h2>{index.displayName}</h2><p><code>index={index.name}</code></p></div>
                      <span className={`status-label status-label--${state === "Active" ? "complete" : state === "Deleting" ? "running" : "neutral"}`}><i />{state}</span>
                    </header>
                    <dl>
                      <div><dt>Search</dt><dd>{accessLabel(index.searchAccess)}</dd></div>
                      <div><dt>Ingestion</dt><dd>{accessLabel(index.ingestionAccess)}</dd></div>
                      <div><dt>Searchable now</dt><dd>{index.searchable ? "Yes" : "No"}</dd></div>
                      <div><dt>Ingestible now</dt><dd>{index.ingestible ? "Yes" : "No"}</dd></div>
                    </dl>
                    <div className="dataset-retention backend-dataset-omission">
                      <small>Event volume, storage, sources, fields, and retention are not included in bootstrap.</small>
                    </div>
                    <footer>
                      {index.searchable ? <Link href={searchLaunchHref(`index=${index.name} | sort -_time`)}>Search index</Link> : <span className="dataset-action-unavailable">Search unavailable</span>}
                      {index.searchable ? <Link href={searchLaunchHref(`index=${index.name} | stats count by sourcetype | sort -count`)}>Explore source types</Link> : null}
                    </footer>
                  </article>
                );
              })}
            </div>
          ) : (
            <section className="suite-card">
              <div className="responsive-table-wrap">
                <table className="product-table">
                  <thead><tr><th scope="col">Index</th><th scope="col">State</th><th scope="col">Search access</th><th scope="col">Ingestion access</th><th scope="col"><span className="sr-only">Action</span></th></tr></thead>
                  <tbody>{visible.map((index) => <tr key={index.id}><td><strong>{index.displayName}</strong><small className="table-secondary">index={index.name}</small></td><td>{stateLabel(index)}</td><td>{accessLabel(index.searchAccess)}</td><td>{accessLabel(index.ingestionAccess)}</td><td>{index.searchable ? <Link className="table-action" href={searchLaunchHref(`index=${index.name} | sort -_time`)} aria-label={`Search ${index.name}`}>Search ›</Link> : "Unavailable"}</td></tr>)}</tbody>
                </table>
              </div>
            </section>
          )}

          <section className="suite-card backend-unavailable-card">
            <header className="suite-card-header"><div><h2>Field catalog and source profiles</h2><p>Unavailable for index browsing in this server version.</p></div><span aria-hidden="true">i</span></header>
            <p>The frontend does not call the unregistered index field, statistics, or storage routes. Run an explicit search to inspect fields or source types for a searchable index.</p>
          </section>
        </>
      )}
    </div>
  );
}

interface DatasetStateProps {
  kind: "loading" | "error" | "empty";
  title: string;
  message: string;
  action?: React.ReactNode;
}

function DatasetState({ kind, title, message, action }: DatasetStateProps) {
  return (
    <div className={`backend-resource-state backend-resource-state--${kind}`} role={kind === "error" ? "alert" : "status"}>
      <span aria-hidden="true">{kind === "loading" ? "↻" : kind === "error" ? "!" : "∅"}</span>
      <div><strong>{title}</strong><p>{message}</p></div>
      {action}
    </div>
  );
}
