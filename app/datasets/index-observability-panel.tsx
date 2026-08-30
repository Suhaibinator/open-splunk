"use client";

import { useEffect, useMemo, useRef, useState } from "react";

import type { IndexStats } from "@/gen/ts/open_splunk/index";
import type { FieldProfile } from "@/gen/ts/open_splunk/result";
import { valueTypeToJSON } from "@/gen/ts/open_splunk/value";
import {
  isOptionalRouteUnavailable,
  type BrowserIndexModel,
  type OpenSplunkApiClient,
} from "@/lib/api";
import { createErrorMessage } from "@/lib/error-message";

import { BackendResourceState } from "../_components/backend-resource-state";
import { formatMediumDateTime } from "../_components/date-format";
import {
  fieldCountLabel,
  mergeIndexFieldPage,
  normalizeIndexObservationQuery,
  type IndexFieldSnapshot,
} from "./index-observability-data";
import { summarizeByteQuantity } from "@/lib/byte-quantity";

interface IndexObservabilityPanelProps {
  client: OpenSplunkApiClient;
  index: BrowserIndexModel;
}

type LoadState = "loading" | "available" | "unavailable" | "error";

const errorMessage = createErrorMessage("The server returned an unusable index analysis response.");

function dateLabel(value: Date | undefined): string {
  return formatMediumDateTime(value, "No events");
}

function typeLabel(field: FieldProfile): string {
  const raw = valueTypeToJSON(field.valueType)
    .replace("VALUE_TYPE_", "")
    .replaceAll("_", " ")
    .toLowerCase();
  return raw === "unspecified" ? "Unknown" : raw.replace(/^./, (value) => value.toUpperCase());
}

function statAccuracy(stats: IndexStats): string {
  return stats.estimates ? "Estimated snapshot" : "Exact snapshot";
}

export function IndexObservabilityPanel({ client, index }: IndexObservabilityPanelProps) {
  const [earliest, setEarliest] = useState("-24h");
  const [latest, setLatest] = useState("now");
  const [nameFilter, setNameFilter] = useState("");
  const [submitted, setSubmitted] = useState({ earliest: "-24h", latest: "now", nameFilter: "" });
  const [statsState, setStatsState] = useState<LoadState>("loading");
  const [stats, setStats] = useState<IndexStats | null>(null);
  const [statsError, setStatsError] = useState<string | null>(null);
  const [fieldsState, setFieldsState] = useState<LoadState>("loading");
  const [fieldSnapshot, setFieldSnapshot] = useState<IndexFieldSnapshot | null>(null);
  const [fieldsError, setFieldsError] = useState<string | null>(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const [validationError, setValidationError] = useState<string | null>(null);
  const [generation, setGeneration] = useState(0);
  const snapshotRef = useRef<IndexFieldSnapshot | null>(null);
  const moreControllerRef = useRef<AbortController | null>(null);

  const selector = useMemo(() => ({
    selector: { $case: "indexId" as const, value: index.id },
  }), [index.id]);

  function applyDraft() {
    const next = normalizeIndexObservationQuery({ earliest, latest, nameFilter });
    if (next === null) {
      setValidationError("Both earliest and latest are required for a bounded field snapshot.");
      return;
    }
    setValidationError(null);
    setSubmitted(next);
  }

  function refreshSubmitted() {
    setValidationError(null);
    setGeneration((current) => current + 1);
  }

  useEffect(() => {
    const controller = new AbortController();
    let current = true;
    moreControllerRef.current?.abort();
    moreControllerRef.current = null;
    snapshotRef.current = null;
    setStatsState("loading");
    setStats(null);
    setStatsError(null);
    setFieldsState("loading");
    setFieldSnapshot(null);
    setFieldsError(null);
    setLoadingMore(false);

    void client.indexes.stats({ selector }, { signal: controller.signal }).then(
      (response) => {
        if (!current) return;
        if (response.stats === undefined || response.stats.indexId !== index.id) {
          throw new TypeError("The server did not return statistics for the selected index.");
        }
        setStats(response.stats);
        setStatsState("available");
      },
      (reason: unknown) => {
        if (!current || controller.signal.aborted) return;
        setStatsState(isOptionalRouteUnavailable(reason) ? "unavailable" : "error");
        setStatsError(errorMessage(reason));
      },
    ).catch((reason: unknown) => {
      if (!current || controller.signal.aborted) return;
      setStatsState("error");
      setStatsError(errorMessage(reason));
    });

    void client.indexes.fields({
      selector,
      timeRange: { earliest: submitted.earliest, latest: submitted.latest },
      page: { pageSize: 50, pageToken: undefined, includeTotalSize: true },
      nameFilter: submitted.nameFilter || undefined,
    }, { signal: controller.signal }).then(
      (response) => {
        if (!current) return;
        const snapshot = mergeIndexFieldPage(null, response);
        snapshotRef.current = snapshot;
        setFieldSnapshot(snapshot);
        setFieldsState("available");
      },
      (reason: unknown) => {
        if (!current || controller.signal.aborted) return;
        setFieldsState(isOptionalRouteUnavailable(reason) ? "unavailable" : "error");
        setFieldsError(errorMessage(reason));
      },
    ).catch((reason: unknown) => {
      if (!current || controller.signal.aborted) return;
      setFieldsState("error");
      setFieldsError(errorMessage(reason));
    });

    return () => {
      current = false;
      controller.abort();
    };
  }, [client, generation, index.id, selector, submitted]);

  async function loadMore() {
    const current = snapshotRef.current;
    const pageToken = current?.nextPageToken;
    if (current === null || pageToken === null || pageToken === undefined || loadingMore) return;
    moreControllerRef.current?.abort();
    const controller = new AbortController();
    moreControllerRef.current = controller;
    setLoadingMore(true);
    setFieldsError(null);
    try {
      const response = await client.indexes.fields({
        selector,
        timeRange: { earliest: submitted.earliest, latest: submitted.latest },
        page: { pageSize: 50, pageToken, includeTotalSize: true },
        nameFilter: submitted.nameFilter || undefined,
      }, { signal: controller.signal });
      if (controller.signal.aborted || moreControllerRef.current !== controller) return;
      const next = mergeIndexFieldPage(snapshotRef.current, response, pageToken);
      snapshotRef.current = next;
      setFieldSnapshot(next);
    } catch (reason) {
      if (controller.signal.aborted) return;
      setFieldsError(`The next field page could not be loaded: ${errorMessage(reason)}`);
    } finally {
      if (moreControllerRef.current === controller) {
        moreControllerRef.current = null;
        setLoadingMore(false);
      }
    }
  }

  return (
    <section className="suite-card index-observability-card" aria-labelledby="index-observability-title">
      <header className="suite-card-header">
        <div>
          <h2 id="index-observability-title">Statistics and field catalog</h2>
          <p>Connected analysis for <code>index={index.name}</code>.</p>
        </div>
        <button type="button" onClick={refreshSubmitted}>Refresh applied query</button>
      </header>

      <form className="index-observability-controls" onSubmit={(event) => { event.preventDefault(); applyDraft(); }}>
        <label><span>Earliest</span><input value={earliest} aria-invalid={validationError !== null || undefined} aria-describedby="index-observability-range-error" onChange={(event) => { setEarliest(event.target.value); setValidationError(null); }} placeholder="-24h" /></label>
        <label><span>Latest</span><input value={latest} aria-invalid={validationError !== null || undefined} aria-describedby="index-observability-range-error" onChange={(event) => { setLatest(event.target.value); setValidationError(null); }} placeholder="now" /></label>
        <label><span>Field name contains</span><input value={nameFilter} onChange={(event) => setNameFilter(event.target.value)} placeholder="Optional, case-sensitive" /></label>
        <button className="button button--primary" type="submit">Apply</button>
      </form>
      {validationError === null ? null : <div id="index-observability-range-error" className="backend-inline-error" role="alert">{validationError}</div>}

      {statsState === "loading" ? <BackendResourceState kind="loading" title="Loading index statistics" message="Reading the current index snapshot…" /> : null}
      {statsState === "unavailable" ? <BackendResourceState kind="unavailable" title="Statistics route unavailable" message={statsError ?? "The connected server did not register index statistics."} /> : null}
      {statsState === "error" ? <BackendResourceState kind="error" title="Index statistics could not be loaded" message={statsError ?? "The statistics request failed."} /> : null}
      {statsState === "available" && stats !== null ? (
        <dl className="index-stats-grid">
          <div><dt>Events</dt><dd>{stats.eventCount.toLocaleString()}</dd></div>
          <div><dt>Storage</dt><dd>{summarizeByteQuantity(stats.storageBytes)}</dd></div>
          <div><dt>Earliest event</dt><dd>{dateLabel(stats.earliestEventTime)}</dd></div>
          <div><dt>Latest event</dt><dd>{dateLabel(stats.latestEventTime)}</dd></div>
          <div><dt>Measured</dt><dd>{dateLabel(stats.measuredAt)}</dd></div>
          <div><dt>Accuracy</dt><dd>{statAccuracy(stats)}</dd></div>
        </dl>
      ) : null}

      <div className="index-field-heading">
        <div><h3>Fields</h3><p>{submitted.earliest} to {submitted.latest}{submitted.nameFilter ? ` · matching “${submitted.nameFilter}”` : ""}</p></div>
        {fieldSnapshot === null ? null : <span>{fieldCountLabel(fieldSnapshot)}</span>}
      </div>
      {fieldsState === "loading" ? <BackendResourceState kind="loading" title="Loading field catalog" message="Capturing a bounded field snapshot…" /> : null}
      {fieldsState === "unavailable" ? <BackendResourceState kind="unavailable" title="Field catalog route unavailable" message={fieldsError ?? "The connected server did not register index field analysis."} /> : null}
      {fieldsState === "error" ? <BackendResourceState kind="error" title="Field catalog could not be loaded" message={fieldsError ?? "The field request failed."} /> : null}
      {fieldsState === "available" && fieldSnapshot !== null && fieldSnapshot.fields.length === 0 ? (
        <BackendResourceState kind="empty" title="No fields observed" message="No field profiles matched this index, time range, and name filter." />
      ) : null}
      {fieldsState === "available" && fieldSnapshot !== null && fieldSnapshot.fields.length > 0 ? (
        <div className="table-wrap">
          <table className="table index-field-table">
            <caption className="sr-only">Observed fields for index {index.name}</caption>
            <thead><tr><th scope="col">Field</th><th scope="col">Type</th><th scope="col">Events</th><th scope="col">Null</th><th scope="col">Missing</th><th scope="col">Profile</th></tr></thead>
            <tbody>{fieldSnapshot.fields.map((field) => (
              <tr key={field.fieldName}>
                <td><strong>{field.displayName || field.fieldName}</strong>{field.displayName && field.displayName !== field.fieldName ? <small className="table-secondary">{field.fieldName}</small> : null}</td>
                <td>{typeLabel(field)}</td>
                <td>{field.eventCount.toLocaleString()}</td>
                <td>{field.nullCount.toLocaleString()}</td>
                <td>{field.missingCount.toLocaleString()}</td>
                <td>{field.interesting ? "Interesting" : field.selected ? "Selected" : "Observed"}</td>
              </tr>
            ))}</tbody>
          </table>
        </div>
      ) : null}
      {fieldsError !== null && fieldsState === "available" ? <div className="backend-inline-error" role="alert">{fieldsError}</div> : null}
      {fieldSnapshot?.nextPageToken ? <div className="index-field-footer"><button className="button" type="button" disabled={loadingMore} onClick={() => void loadMore()}>{loadingMore ? "Loading…" : "Load more fields"}</button></div> : null}
    </section>
  );
}
