"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import Link from "next/link";

import { SortDirection } from "@/gen/ts/open_splunk/v1/common";
import {
  IndexAccessState,
  IndexState,
  type Index,
} from "@/gen/ts/open_splunk/v1/index";
import { IndexSortBy } from "@/gen/ts/open_splunk/v1/index_api";
import {
  createOpenSplunkApiClient,
  getSystemBootstrap,
  isOptionalRouteUnavailable,
  type BrowserIndexModel,
  type OpenSplunkApiClient,
  type SystemBootstrapModel,
} from "@/lib/api";
import { searchLaunchHref } from "@/lib/search/launch-url";

import { PageHeading } from "../_components/product-shell";
import { IndexObservabilityPanel } from "./index-observability-panel";

interface BackendDatasetsConsoleProps {
  apiBaseUrl: string;
}

type DefinitionLoadState = "idle" | "loading" | "available" | "unavailable" | "error";

interface AuthorizedIndexDefinitions {
  state: Exclude<DefinitionLoadState, "idle" | "loading">;
  definitions: Map<string, Index>;
  message?: string;
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

function retentionLabel(index: Index | undefined): string {
  const seconds = index?.definition?.retentionPeriod?.seconds;
  if (seconds === undefined || seconds <= 0n) return "Forever";
  const days = seconds / 86_400n;
  if (days > 0n && seconds % 86_400n === 0n) return `${days.toLocaleString()} days`;
  const hours = seconds / 3_600n;
  if (hours > 0n && seconds % 3_600n === 0n) return `${hours.toLocaleString()} hours`;
  return `${seconds.toLocaleString()} seconds`;
}

function updatedLabel(index: Index | undefined): string {
  const updatedAt = index?.updatedAt;
  if (updatedAt === undefined || Number.isNaN(updatedAt.valueOf())) return "Not recorded";
  return new Intl.DateTimeFormat(undefined, { dateStyle: "medium" }).format(updatedAt);
}

async function loadAuthorizedIndexDefinitions(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  signal: AbortSignal,
): Promise<AuthorizedIndexDefinitions> {
  const authorizedById = new Map(bootstrap.indexes.map((index) => [index.id, index]));
  if (authorizedById.size === 0) return { state: "available", definitions: new Map() };
  const definitions = new Map<string, Index>();
  const seenPageTokens = new Set<string>();
  const pageSize = Math.max(1, Math.min(bootstrap.limits.maximumPageSize || 100, 100));
  let pageToken: string | undefined;
  try {
    for (let page = 0; page < 256; page += 1) {
      // Index cursors are causally ordered and must be traversed sequentially.
      // eslint-disable-next-line no-await-in-loop
      const response = await client.indexes.list({
        page: { pageSize, pageToken, includeTotalSize: false },
        stateFilters: [],
        textFilter: undefined,
        sortBy: IndexSortBy.INDEX_SORT_BY_NAME,
        sortDirection: SortDirection.SORT_DIRECTION_ASCENDING,
        includeStats: false,
      }, { signal });
      for (const item of response.indexes) {
        const index = item.index;
        if (index === undefined) continue;
        const authorized = authorizedById.get(index.indexId);
        if (authorized === undefined) continue;
        if (index.definition?.name !== authorized.name) {
          throw new TypeError(`Index ${index.indexId} did not match its authorized bootstrap identity.`);
        }
        const existing = definitions.get(index.indexId);
        if (existing !== undefined && existing.version !== index.version) {
          throw new TypeError(`Index ${index.indexId} repeated with conflicting versions.`);
        }
        definitions.set(index.indexId, index);
      }
      if (definitions.size === authorizedById.size) {
        return { state: "available", definitions };
      }
      const nextPageToken = response.page?.nextPageToken?.trim() || null;
      if (nextPageToken === null) return { state: "available", definitions };
      if (seenPageTokens.has(nextPageToken)) {
        throw new TypeError("The index catalog repeated a page cursor.");
      }
      seenPageTokens.add(nextPageToken);
      pageToken = nextPageToken;
    }
    throw new RangeError("The index catalog exceeded the 256-page browser safety limit.");
  } catch (reason) {
    if (isOptionalRouteUnavailable(reason)) {
      return { state: "unavailable", definitions: new Map() };
    }
    if (signal.aborted) throw reason;
    return { state: "error", definitions: new Map(), message: errorMessage(reason) };
  }
}

export function BackendDatasetsConsole({ apiBaseUrl }: BackendDatasetsConsoleProps) {
  const client = useMemo(() => createOpenSplunkApiClient({ baseUrl: apiBaseUrl }), [apiBaseUrl]);
  const [bootstrap, setBootstrap] = useState<SystemBootstrapModel | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [generation, setGeneration] = useState(0);
  const [filter, setFilter] = useState("");
  const [view, setView] = useState<"cards" | "table">("cards");
  const [definitionState, setDefinitionState] = useState<DefinitionLoadState>("idle");
  const [definitions, setDefinitions] = useState<Map<string, Index>>(new Map());
  const [definitionError, setDefinitionError] = useState<string | null>(null);
  const [observedIndexId, setObservedIndexId] = useState<string | null>(null);
  const reload = useCallback(() => setGeneration((current) => current + 1), []);

  useEffect(() => {
    const controller = new AbortController();
    let current = true;
    setLoading(true);
    setError(null);
    setDefinitionState("idle");
    setDefinitions(new Map());
    setDefinitionError(null);
    void (async () => {
      try {
        const response = await getSystemBootstrap(client, undefined, { signal: controller.signal });
        if (!current) return;
        setBootstrap(response);
        setLoading(false);
        setDefinitionState("loading");
        const loaded = await loadAuthorizedIndexDefinitions(client, response, controller.signal);
        if (!current) return;
        setDefinitions(loaded.definitions);
        setDefinitionError(loaded.message ?? null);
        setDefinitionState(loaded.state);
      } catch (reason) {
        if (!current || controller.signal.aborted) return;
        setBootstrap(null);
        setDefinitions(new Map());
        setDefinitionState("idle");
        setError(errorMessage(reason));
        setLoading(false);
      }
    })();
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
  const observedIndex = bootstrap?.indexes.find((index) => index.id === observedIndexId) ?? null;

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
          {definitionState === "loading" ? <output className="dataset-definition-notice">Loading retention and index metadata from the registered administration route…</output> : null}
          {definitionState === "unavailable" ? <output className="dataset-definition-notice dataset-definition-notice--muted">The optional index-definition route is unavailable. Search authorization and state still come from system bootstrap.</output> : null}
          {definitionState === "error" ? <div className="backend-inline-error" role="alert">Index details could not be enriched. Bootstrap authorization remains available. {definitionError}</div> : null}

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
                const detail = definitions.get(index.id);
                return (
                  <article className="dataset-card" key={index.id}>
                    <header>
                      <span className={`dataset-icon dataset-icon--${position % 3 === 0 ? "green" : position % 3 === 1 ? "blue" : "orange"}`} aria-hidden="true">▦</span>
                      <div><h2>{index.displayName}</h2><p><code>index={index.name}</code></p>{detail?.definition?.description ? <small>{detail.definition.description}</small> : null}</div>
                      <span className={`status-label status-label--${state === "Active" ? "complete" : state === "Deleting" ? "running" : "neutral"}`}><i />{state}</span>
                    </header>
                    <dl>
                      <div><dt>Search</dt><dd>{accessLabel(index.searchAccess)}</dd></div>
                      <div><dt>Ingestion</dt><dd>{accessLabel(index.ingestionAccess)}</dd></div>
                      <div><dt>Searchable now</dt><dd>{index.searchable ? "Yes" : "No"}</dd></div>
                      <div><dt>Ingestible now</dt><dd>{index.ingestible ? "Yes" : "No"}</dd></div>
                    </dl>
                    <div className={`dataset-retention${detail === undefined ? " backend-dataset-omission" : ""}`}>
                      {detail === undefined ? (
                        <small>{definitionState === "loading"
                          ? "Loading retention and index settings…"
                          : definitionState === "unavailable"
                            ? "The optional index-definition route is not registered."
                            : definitionState === "error"
                              ? "Index settings could not be loaded; bootstrap access remains authoritative."
                              : "The server did not return extended settings for this authorized index."}</small>
                      ) : (
                        <dl className="dataset-definition-meta">
                          <div><dt>Retention</dt><dd>{retentionLabel(detail)}</dd></div>
                          <div><dt>Default source type</dt><dd>{detail.definition?.defaultSourcetype || "Not set"}</dd></div>
                          <div><dt>Updated</dt><dd>{updatedLabel(detail)}</dd></div>
                        </dl>
                      )}
                    </div>
                    <footer>
                      {index.searchable ? <Link href={searchLaunchHref(`index=${index.name} | sort -_time`)}>Search index</Link> : <span className="dataset-action-unavailable">Search unavailable</span>}
                      {index.searchable ? <Link href={searchLaunchHref(`index=${index.name} | stats count by sourcetype | sort -count`)}>Explore source types</Link> : null}
                      <button type="button" aria-pressed={observedIndexId === index.id} onClick={() => setObservedIndexId(index.id)}>{observedIndexId === index.id ? "Inspecting profile" : "Inspect profile"}</button>
                    </footer>
                  </article>
                );
              })}
            </div>
          ) : (
            <section className="suite-card">
              <div className="responsive-table-wrap">
                <table className="product-table">
                  <thead><tr><th scope="col">Index</th><th scope="col">State</th><th scope="col">Search access</th><th scope="col">Ingestion access</th><th scope="col">Retention</th><th scope="col">Default source type</th><th scope="col"><span className="sr-only">Action</span></th></tr></thead>
                  <tbody>{visible.map((index) => {
                    const detail = definitions.get(index.id);
                    return <tr key={index.id}><td><strong>{index.displayName}</strong><small className="table-secondary">index={index.name}</small></td><td>{stateLabel(index)}</td><td>{accessLabel(index.searchAccess)}</td><td>{accessLabel(index.ingestionAccess)}</td><td>{detail === undefined ? "Not available" : retentionLabel(detail)}</td><td>{detail === undefined ? "Not available" : detail.definition?.defaultSourcetype || "Not set"}</td><td><div className="row-actions">{index.searchable ? <Link className="table-action" href={searchLaunchHref(`index=${index.name} | sort -_time`)} aria-label={`Search ${index.name}`}>Search ›</Link> : "Unavailable"}<button className="table-action" type="button" aria-pressed={observedIndexId === index.id} onClick={() => setObservedIndexId(index.id)}>{observedIndexId === index.id ? "Inspecting" : "Profile"}</button></div></td></tr>;
                  })}</tbody>
                </table>
              </div>
            </section>
          )}

          {observedIndex === null ? (
            <section className="suite-card index-observability-prompt">
              <header className="suite-card-header"><div><h2>Statistics and field catalog</h2><p>Select an index to inspect its connected data profile.</p></div><span aria-hidden="true">⌕</span></header>
              <p>The backend can report event and storage statistics plus a paginated field snapshot over an explicit time range. Choose <strong>Inspect profile</strong> on any index above.</p>
            </section>
          ) : <IndexObservabilityPanel client={client} index={observedIndex} />}
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
