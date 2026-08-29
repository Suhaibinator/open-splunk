"use client";

import type { FormEvent } from "react";
import { useCallback, useEffect, useMemo, useState } from "react";

import { AppState, type AppWorkspace } from "@/gen/ts/open_splunk/app";
import { AppSortBy } from "@/gen/ts/open_splunk/app_api";
import {
  CollectorAdministrativeState,
  CollectorConnectionState,
  type CollectorRecord,
} from "@/gen/ts/open_splunk/collector_admin";
import { CollectorSortBy } from "@/gen/ts/open_splunk/collector_admin_api";
import {
  collectorCapabilityToJSON,
  collectorInputStateToJSON,
  type CollectorInputHealth,
} from "@/gen/ts/open_splunk/collector";
import { SortDirection } from "@/gen/ts/open_splunk/common";
import { ServerFeature } from "@/gen/ts/open_splunk/system_api";
import {
  createOpenSplunkApiClient,
  isOptionalRouteUnavailable,
  type SystemBootstrapModel,
} from "@/lib/api";
import { createErrorMessage } from "@/lib/error-message";

import { BackendResourceState } from "../_components/backend-resource-state";
import { AppIcon } from "../_components/app-icon";
import { formatMediumDateTime } from "../_components/date-format";
import { Modal } from "../_components/modal";
import {
  appForm,
  blankAppForm,
  definitionFromForm,
  sameStrings,
  type AppFormState,
} from "./admin-resource-data";

type LoadState = "loading" | "available" | "unavailable" | "error";

interface PanelProps {
  apiBaseUrl: string;
  bootstrap: SystemBootstrapModel | null;
}

const errorMessage = createErrorMessage("The server did not return a usable response.");

function formatDate(value: Date | undefined): string {
  return formatMediumDateTime(value, "Never");
}

function countLabel(loaded: number, total: bigint | null, exact: boolean, singular: string, plural: string): string {
  if (total !== null && exact) return BigInt(loaded) < total
    ? `${loaded.toLocaleString()} of ${total.toLocaleString()} ${plural} loaded`
    : `${total.toLocaleString()} ${total === 1n ? singular : plural}`;
  return `${loaded.toLocaleString()} ${loaded === 1 ? singular : plural} loaded`;
}

function appSelector(app: AppWorkspace) {
  return { selector: { $case: "appId" as const, value: app.appId } };
}

function appStateLabel(state: AppState): string {
  if (state === AppState.APP_STATE_ACTIVE) return "Active";
  if (state === AppState.APP_STATE_ARCHIVED) return "Archived";
  return "Unknown";
}

function AppFields({ form, onChange, editing }: {
  form: AppFormState;
  onChange: (form: AppFormState) => void;
  editing: boolean;
}) {
  return (
    <div className="admin-form">
      <label htmlFor="app-slug"><span>Slug</span><input id="app-slug" value={form.slug} disabled={editing} required pattern="[a-z0-9][a-z0-9_-]*" onChange={(event) => onChange({ ...form, slug: event.target.value })} /><small>Lowercase durable identifier; it cannot be changed later.</small></label>
      <label htmlFor="app-display-name"><span>Display name</span><input id="app-display-name" value={form.displayName} required onChange={(event) => onChange({ ...form, displayName: event.target.value })} /></label>
      <label htmlFor="app-description"><span>Description <small>(optional)</small></span><input id="app-description" value={form.description} onChange={(event) => onChange({ ...form, description: event.target.value })} /></label>
      <label htmlFor="app-indexes"><span>Default indexes <small>(optional)</small></span><input id="app-indexes" value={form.indexNames} placeholder="main, security" onChange={(event) => onChange({ ...form, indexNames: event.target.value })} /><small>Comma-separated index names used as the app’s default search scope.</small></label>
      <label className="admin-checkbox"><input type="checkbox" aria-label="Configure a default time range" checked={form.hasTimeRange} onChange={(event) => onChange({ ...form, hasTimeRange: event.target.checked })} /><span><strong>Configure a default time range</strong><small>Otherwise consumers use their endpoint defaults.</small></span></label>
      {form.hasTimeRange ? <>
        <label htmlFor="app-earliest"><span>Earliest</span><input id="app-earliest" value={form.earliest} placeholder="-24h" onChange={(event) => onChange({ ...form, earliest: event.target.value })} /></label>
        <label htmlFor="app-latest"><span>Latest</span><input id="app-latest" value={form.latest} placeholder="now" onChange={(event) => onChange({ ...form, latest: event.target.value })} /></label>
        <label htmlFor="app-timezone"><span>Timezone</span><input id="app-timezone" value={form.timezone} placeholder="UTC" onChange={(event) => onChange({ ...form, timezone: event.target.value })} /></label>
      </> : null}
    </div>
  );
}

export function AppsAdminPanel({ apiBaseUrl, bootstrap }: PanelProps) {
  const client = useMemo(() => createOpenSplunkApiClient({ baseUrl: apiBaseUrl }), [apiBaseUrl]);
  const capability = bootstrap === null ? null : bootstrap.features.has(ServerFeature.SERVER_FEATURE_APP_ADMIN);
  const [state, setState] = useState<LoadState>("loading");
  const [apps, setApps] = useState<AppWorkspace[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [query, setQuery] = useState("");
  const [appliedQuery, setAppliedQuery] = useState("");
  const [stateFilter, setStateFilter] = useState<"all" | "active" | "archived">("all");
  const [nextPageToken, setNextPageToken] = useState<string | null>(null);
  const [total, setTotal] = useState<bigint | null>(null);
  const [totalExact, setTotalExact] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [generation, setGeneration] = useState(0);
  const [modal, setModal] = useState<"create" | "edit" | "delete" | null>(null);
  const [target, setTarget] = useState<AppWorkspace | null>(null);
  const [form, setForm] = useState<AppFormState>(blankAppForm);
  const [confirmation, setConfirmation] = useState("");
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);

  const filters = useMemo(() => stateFilter === "active"
    ? [AppState.APP_STATE_ACTIVE]
    : stateFilter === "archived" ? [AppState.APP_STATE_ARCHIVED] : [], [stateFilter]);

  const load = useCallback(() => setGeneration((value) => value + 1), []);

  useEffect(() => {
    if (capability === false) {
      setState("unavailable");
      setApps([]);
      return;
    }
    const controller = new AbortController();
    setState("loading");
    setError(null);
    void client.apps.list({
      page: { pageSize: undefined, pageToken: undefined, includeTotalSize: true },
      stateFilters: filters,
      textFilter: appliedQuery.trim() || undefined,
      sortBy: AppSortBy.APP_SORT_BY_DISPLAY_NAME,
      sortDirection: SortDirection.SORT_DIRECTION_ASCENDING,
    }, { signal: controller.signal }).then((response) => {
      if (controller.signal.aborted) return;
      setApps(response.apps);
      setNextPageToken(response.page?.nextPageToken?.trim() || null);
      setTotal(response.page?.totalSize ?? null);
      setTotalExact(response.page?.totalSizeExact ?? false);
      setState("available");
    }, (loadError: unknown) => {
      if (controller.signal.aborted) return;
      if (isOptionalRouteUnavailable(loadError)) {
        setState("unavailable");
        setApps([]);
      } else {
        setState("error");
        setError(errorMessage(loadError));
      }
    });
    return () => controller.abort();
  }, [appliedQuery, capability, client, filters, generation]);

  async function loadMore() {
    if (nextPageToken === null || loadingMore) return;
    const requestedPageToken = nextPageToken;
    setLoadingMore(true);
    try {
      const response = await client.apps.list({
        page: { pageSize: undefined, pageToken: requestedPageToken, includeTotalSize: true },
        stateFilters: filters,
        textFilter: appliedQuery.trim() || undefined,
        sortBy: AppSortBy.APP_SORT_BY_DISPLAY_NAME,
        sortDirection: SortDirection.SORT_DIRECTION_ASCENDING,
      });
      const next = response.page?.nextPageToken?.trim() || null;
      if (next === requestedPageToken) throw new Error("The app catalog repeated its page cursor.");
      const loadedIds = new Set(apps.map((app) => app.appId));
      if (response.apps.some((app) => !app.appId || loadedIds.has(app.appId))) {
        throw new Error("The app catalog returned a missing or duplicate app identifier.");
      }
      setApps((current) => [...current, ...response.apps]);
      setNextPageToken(next);
      setTotal(response.page?.totalSize ?? total);
      setTotalExact(response.page?.totalSizeExact ?? totalExact);
    } catch (loadError) {
      setNotice(errorMessage(loadError));
    } finally {
      setLoadingMore(false);
    }
  }

  async function openEditor(app: AppWorkspace) {
    setBusy(true);
    setNotice(null);
    try {
      const response = await client.apps.get({ selector: appSelector(app) });
      if (response.app === undefined || response.app.definition === undefined) throw new Error("The server returned an empty app workspace.");
      setTarget(response.app);
      setForm(appForm(response.app));
      setModal("edit");
    } catch (readError) {
      setNotice(errorMessage(readError));
    } finally {
      setBusy(false);
    }
  }

  async function createApp(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const definition = definitionFromForm(form);
    setBusy(true);
    setNotice(null);
    try {
      const response = await client.apps.create({ definition, clientRequestId: undefined });
      if (response.app === undefined) throw new Error("The server returned an empty app workspace.");
      setModal(null);
      setNotice(`App “${response.app.definition?.displayName || definition.slug}” was created.`);
      load();
    } catch (mutationError) {
      setNotice(errorMessage(mutationError));
    } finally {
      setBusy(false);
    }
  }

  async function updateApp(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (target === null || target.definition === undefined) return;
    const definition = definitionFromForm(form);
    const updateMask: string[] = [];
    if (definition.displayName !== target.definition.displayName) updateMask.push("display_name");
    if ((definition.description ?? "") !== (target.definition.description ?? "")) updateMask.push("description");
    if (!sameStrings(definition.defaultIndexNames, target.definition.defaultIndexNames)) updateMask.push("default_index_names");
    const previousRange = target.definition.defaultTimeRange;
    if (
      definition.defaultTimeRange?.earliest !== previousRange?.earliest
      || definition.defaultTimeRange?.latest !== previousRange?.latest
      || definition.defaultTimeRange?.timezone !== previousRange?.timezone
    ) updateMask.push("default_time_range");
    if (updateMask.length === 0) {
      setModal(null);
      return;
    }
    setBusy(true);
    setNotice(null);
    try {
      const response = await client.apps.update({
        selector: appSelector(target),
        expectedVersion: target.version,
        definition,
        updateMask,
      });
      if (response.app === undefined) throw new Error("The server returned an empty app workspace.");
      setModal(null);
      setNotice(`App “${definition.displayName}” was updated.`);
      load();
    } catch (mutationError) {
      setNotice(errorMessage(mutationError));
    } finally {
      setBusy(false);
    }
  }

  async function changeState(app: AppWorkspace) {
    const nextState = app.state === AppState.APP_STATE_ACTIVE ? AppState.APP_STATE_ARCHIVED : AppState.APP_STATE_ACTIVE;
    setBusy(true);
    setNotice(null);
    try {
      const response = await client.apps.setState({ selector: appSelector(app), expectedVersion: app.version, state: nextState });
      if (response.app === undefined) throw new Error("The server returned an empty app workspace.");
      setNotice(`App “${app.definition?.displayName || app.appId}” is now ${appStateLabel(nextState).toLowerCase()}.`);
      load();
    } catch (mutationError) {
      setNotice(errorMessage(mutationError));
    } finally {
      setBusy(false);
    }
  }

  async function deleteApp(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const slug = target?.definition?.slug;
    if (target === null || slug === undefined || confirmation !== slug) return;
    setBusy(true);
    setNotice(null);
    try {
      await client.apps.delete({ selector: appSelector(target), expectedVersion: target.version, confirmationSlug: confirmation });
      setModal(null);
      setTarget(null);
      setConfirmation("");
      setNotice(`App “${slug}” was permanently deleted.`);
      load();
    } catch (mutationError) {
      setNotice(errorMessage(mutationError));
    } finally {
      setBusy(false);
    }
  }

  if (state === "loading") return <BackendResourceState kind="loading" title="Loading apps" message="Reading app workspaces from the server…" />;
  if (state === "unavailable") return <BackendResourceState kind="unavailable" title="App administration is unavailable" message="The connected server does not advertise the complete app-administration capability." />;
  if (state === "error") return <BackendResourceState kind="error" title="Apps could not be loaded" message={error ?? "The app catalog request failed."} action={<button type="button" onClick={load}>Retry</button>} />;

  return (
    <div className="admin-section-stack">
      <header className="admin-section-header"><div><h2>Apps</h2><p>Manage UI and knowledge-object scopes, default indexes, and time ranges.</p></div><button className="suite-button suite-button--primary" type="button" onClick={() => { setForm(blankAppForm()); setTarget(null); setNotice(null); setModal("create"); }}><AppIcon name="plus" size="sm" /> Create app</button></header>
      {notice === null ? null : <output className="access-mode-notice"><span>i</span><div><strong>App administration</strong><p>{notice}</p></div></output>}
      <form className="admin-toolbar" onSubmit={(event) => { event.preventDefault(); setAppliedQuery(query); }}>
        <label><span className="sr-only">Filter apps</span><input type="search" value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Filter apps by name or slug" /></label>
        <label><span className="sr-only">App state</span><select value={stateFilter} onChange={(event) => setStateFilter(event.target.value as typeof stateFilter)}><option value="all">All states</option><option value="active">Active</option><option value="archived">Archived</option></select></label>
        <button type="submit">Apply</button><button type="button" onClick={load}>Refresh</button>
      </form>
      {apps.length === 0 ? <BackendResourceState kind="empty" title={appliedQuery || stateFilter !== "all" ? "No matching apps" : "No apps configured"} message={appliedQuery || stateFilter !== "all" ? "Clear the filters to show the complete app catalog." : "Create an app to define a UI and knowledge-object scope."} action={appliedQuery || stateFilter !== "all" ? <button type="button" onClick={() => { setQuery(""); setAppliedQuery(""); setStateFilter("all"); }}>Clear filters</button> : undefined} /> : (
        <div className="suite-card resource-table-card"><div className="responsive-table-wrap"><table className="product-table admin-resource-table">
          <caption className="sr-only">Configured apps</caption>
          <thead><tr><th scope="col">Name</th><th scope="col">Default indexes</th><th scope="col">Default time</th><th scope="col">State</th><th scope="col">Updated</th><th scope="col"><span className="sr-only">Actions</span></th></tr></thead>
          <tbody>{apps.map((app) => {
            const definition = app.definition;
            const stateLabel = appStateLabel(app.state);
            const appName = definition?.displayName || app.appId;
            return <tr key={app.appId}>
              <td className="table-long-value"><strong>{appName}</strong><small className="table-secondary">{definition?.slug ?? app.appId}{definition?.description ? ` · ${definition.description}` : ""}</small></td>
              <td className="table-long-value">{definition?.defaultIndexNames.join(", ") || "Server default"}</td>
              <td>{definition?.defaultTimeRange === undefined ? "Server default" : `${definition.defaultTimeRange.earliest ?? "-24h"} to ${definition.defaultTimeRange.latest ?? "now"}`}<small className="table-secondary">{definition?.defaultTimeRange?.timezone ?? ""}</small></td>
              <td><span className={`status-label status-label--${stateLabel === "Active" ? "complete" : "neutral"}`}><i />{stateLabel}</span></td>
              <td>{formatDate(app.updatedAt)}</td>
              <td><div className="row-actions"><button className="table-action" type="button" aria-label={`Edit app ${appName}`} disabled={busy || definition === undefined} onClick={() => void openEditor(app)}>Edit</button><button className="table-action" type="button" aria-label={`${app.state === AppState.APP_STATE_ACTIVE ? "Archive" : "Activate"} app ${appName}`} disabled={busy || (app.state !== AppState.APP_STATE_ACTIVE && app.state !== AppState.APP_STATE_ARCHIVED)} onClick={() => void changeState(app)}>{app.state === AppState.APP_STATE_ACTIVE ? "Archive" : "Activate"}</button>{app.state === AppState.APP_STATE_ARCHIVED ? <button className="table-action table-action--danger" type="button" aria-label={`Delete app ${appName}`} disabled={busy} onClick={() => { setTarget(app); setConfirmation(""); setNotice(null); setModal("delete"); }}>Delete</button> : null}</div></td>
            </tr>;
          })}</tbody>
        </table></div></div>
      )}
      <div className="admin-pagination-footer"><strong>{countLabel(apps.length, total, totalExact, "app", "apps")}</strong>{nextPageToken === null ? null : <button className="button secondary" type="button" disabled={loadingMore || busy} onClick={() => void loadMore()}>{loadingMore ? "Loading…" : "Load more apps"}</button>}</div>

      {modal === "create" ? <Modal title="Create app" subtitle="Create an app workspace and its default search context." onClose={() => !busy && setModal(null)} footer={<><button className="button secondary" type="button" disabled={busy} onClick={() => setModal(null)}>Cancel</button><button className="button primary" type="submit" form="create-app-form" disabled={busy || !form.slug.trim() || !form.displayName.trim()}>{busy ? "Creating…" : "Create app"}</button></>}><form id="create-app-form" onSubmit={(event) => void createApp(event)}>{notice === null ? null : <div className="access-mode-notice" role="alert"><span>!</span><div><strong>App could not be created</strong><p>{notice}</p></div></div>}<AppFields form={form} onChange={setForm} editing={false} /></form></Modal> : null}
      {modal === "edit" && target !== null ? <Modal title={`Edit ${target.definition?.displayName || "app"}`} subtitle="The app slug remains immutable." onClose={() => !busy && setModal(null)} footer={<><button className="button secondary" type="button" disabled={busy} onClick={() => setModal(null)}>Cancel</button><button className="button primary" type="submit" form="edit-app-form" disabled={busy || !form.displayName.trim()}>{busy ? "Saving…" : "Save changes"}</button></>}><form id="edit-app-form" onSubmit={(event) => void updateApp(event)}>{notice === null ? null : <div className="access-mode-notice" role="alert"><span>!</span><div><strong>App could not be updated</strong><p>{notice}</p></div></div>}<AppFields form={form} onChange={setForm} editing /></form></Modal> : null}
      {modal === "delete" && target?.definition !== undefined ? <Modal title={`Delete ${target.definition.displayName}`} subtitle="This permanently removes an archived, unreferenced app." onClose={() => !busy && setModal(null)} footer={<><button className="button secondary" type="button" disabled={busy} onClick={() => setModal(null)}>Cancel</button><button className="button danger" type="submit" form="delete-app-form" disabled={busy || confirmation !== target.definition.slug}>{busy ? "Deleting…" : "Delete permanently"}</button></>}><form id="delete-app-form" className="admin-form" onSubmit={(event) => void deleteApp(event)}><div className="access-mode-notice" role="alert"><span>!</span><div><strong>This cannot be undone</strong><p>{notice ?? "Deletion succeeds only if the app is archived and no saved objects still reference it."}</p></div></div><label htmlFor="delete-app-confirmation"><span>Type <code>{target.definition.slug}</code> to confirm</span><input id="delete-app-confirmation" value={confirmation} onChange={(event) => setConfirmation(event.target.value)} autoComplete="off" /></label></form></Modal> : null}
    </div>
  );
}

function collectorConnectionLabel(state: CollectorConnectionState): string {
  if (state === CollectorConnectionState.COLLECTOR_CONNECTION_STATE_ONLINE) return "Online";
  if (state === CollectorConnectionState.COLLECTOR_CONNECTION_STATE_STALE) return "Stale";
  if (state === CollectorConnectionState.COLLECTOR_CONNECTION_STATE_OFFLINE) return "Offline";
  if (state === CollectorConnectionState.COLLECTOR_CONNECTION_STATE_DISABLED) return "Disabled";
  return "Unknown";
}

function titleCaseEnum(value: string, prefix: string): string {
  return value.replace(prefix, "").toLowerCase().split("_").map((part) => part.charAt(0).toUpperCase() + part.slice(1)).join(" ");
}

function formatBytes(value: bigint): string {
  const number = Number(value);
  if (!Number.isFinite(number)) return `${value.toLocaleString()} B`;
  if (number < 1024) return `${number.toLocaleString()} B`;
  const units = ["KiB", "MiB", "GiB", "TiB"];
  let amount = number;
  let unit = "B";
  for (const candidate of units) {
    amount /= 1024;
    unit = candidate;
    if (amount < 1024) break;
  }
  return `${amount.toLocaleString(undefined, { maximumFractionDigits: 1 })} ${unit}`;
}

function formatAge(seconds: bigint | undefined, nanos: number | undefined): string {
  if (seconds === undefined) return "None";
  const total = Number(seconds) + (nanos ?? 0) / 1_000_000_000;
  if (total < 60) return `${Math.round(total)}s`;
  if (total < 3600) return `${Math.round(total / 60)}m`;
  return `${(total / 3600).toLocaleString(undefined, { maximumFractionDigits: 1 })}h`;
}

function InputRows({ inputs }: { inputs: CollectorInputHealth[] }) {
  if (inputs.length === 0) return <BackendResourceState kind="empty" title="No input telemetry" message="This collector has not reported any configured input health." />;
  return <div className="responsive-table-wrap"><table className="product-table"><caption className="sr-only">Collector inputs</caption><thead><tr><th scope="col">Input</th><th scope="col">State</th><th scope="col">Sources</th><th scope="col">Events read</th><th scope="col">Bytes read</th><th scope="col">Last event</th></tr></thead><tbody>{inputs.map((input) => <tr key={input.inputId}><td><strong>{input.inputId}</strong>{input.statusMessage ? <small className="table-secondary">{input.statusMessage}</small> : null}</td><td>{titleCaseEnum(collectorInputStateToJSON(input.state), "COLLECTOR_INPUT_STATE_")}</td><td>{input.activeSources.toLocaleString()} active<small className="table-secondary">{input.discoveredSources.toLocaleString()} discovered</small></td><td>{input.eventsReadTotal.toLocaleString()}</td><td>{formatBytes(input.bytesReadTotal)}</td><td>{formatDate(input.lastEventAt)}</td></tr>)}</tbody></table></div>;
}

export function CollectorFleetPanel({ apiBaseUrl, bootstrap }: PanelProps) {
  const client = useMemo(() => createOpenSplunkApiClient({ baseUrl: apiBaseUrl }), [apiBaseUrl]);
  const capability = bootstrap === null ? null : bootstrap.features.has(ServerFeature.SERVER_FEATURE_COLLECTOR_ADMIN);
  const [state, setState] = useState<LoadState>("loading");
  const [collectors, setCollectors] = useState<CollectorRecord[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [query, setQuery] = useState("");
  const [appliedQuery, setAppliedQuery] = useState("");
  const [indexFilter, setIndexFilter] = useState("");
  const [appliedIndexFilter, setAppliedIndexFilter] = useState("");
  const [stateFilter, setStateFilter] = useState<"all" | "online" | "stale" | "offline" | "disabled">("all");
  const [nextPageToken, setNextPageToken] = useState<string | null>(null);
  const [total, setTotal] = useState<bigint | null>(null);
  const [totalExact, setTotalExact] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [generation, setGeneration] = useState(0);
  const [target, setTarget] = useState<CollectorRecord | null>(null);
  const [modal, setModal] = useState<"details" | "rename" | null>(null);
  const [displayName, setDisplayName] = useState("");
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);

  const filters = useMemo(() => {
    if (stateFilter === "online") return [CollectorConnectionState.COLLECTOR_CONNECTION_STATE_ONLINE];
    if (stateFilter === "stale") return [CollectorConnectionState.COLLECTOR_CONNECTION_STATE_STALE];
    if (stateFilter === "offline") return [CollectorConnectionState.COLLECTOR_CONNECTION_STATE_OFFLINE];
    if (stateFilter === "disabled") return [CollectorConnectionState.COLLECTOR_CONNECTION_STATE_DISABLED];
    return [];
  }, [stateFilter]);
  const load = useCallback(() => setGeneration((value) => value + 1), []);

  useEffect(() => {
    if (capability === false) {
      setState("unavailable");
      setCollectors([]);
      return;
    }
    const controller = new AbortController();
    setState("loading");
    setError(null);
    void client.collectors.list({
      page: { pageSize: undefined, pageToken: undefined, includeTotalSize: true },
      stateFilters: filters,
      indexNameFilter: appliedIndexFilter.trim() || undefined,
      textFilter: appliedQuery.trim() || undefined,
      sortBy: CollectorSortBy.COLLECTOR_SORT_BY_DISPLAY_NAME,
      sortDirection: SortDirection.SORT_DIRECTION_ASCENDING,
    }, { signal: controller.signal }).then((response) => {
      if (controller.signal.aborted) return;
      setCollectors(response.collectors);
      setNextPageToken(response.page?.nextPageToken?.trim() || null);
      setTotal(response.page?.totalSize ?? null);
      setTotalExact(response.page?.totalSizeExact ?? false);
      setState("available");
    }, (loadError: unknown) => {
      if (controller.signal.aborted) return;
      if (isOptionalRouteUnavailable(loadError)) {
        setState("unavailable");
        setCollectors([]);
      } else {
        setState("error");
        setError(errorMessage(loadError));
      }
    });
    return () => controller.abort();
  }, [appliedIndexFilter, appliedQuery, capability, client, filters, generation]);

  async function loadMore() {
    if (nextPageToken === null || loadingMore) return;
    const requestedPageToken = nextPageToken;
    setLoadingMore(true);
    try {
      const response = await client.collectors.list({
        page: { pageSize: undefined, pageToken: requestedPageToken, includeTotalSize: true },
        stateFilters: filters,
        indexNameFilter: appliedIndexFilter.trim() || undefined,
        textFilter: appliedQuery.trim() || undefined,
        sortBy: CollectorSortBy.COLLECTOR_SORT_BY_DISPLAY_NAME,
        sortDirection: SortDirection.SORT_DIRECTION_ASCENDING,
      });
      const next = response.page?.nextPageToken?.trim() || null;
      if (next === requestedPageToken) throw new Error("The collector catalog repeated its page cursor.");
      const loadedIds = new Set(collectors.map((collector) => collector.collectorId));
      if (response.collectors.some((collector) => !collector.collectorId || loadedIds.has(collector.collectorId))) {
        throw new Error("The collector catalog returned a missing or duplicate collector identifier.");
      }
      setCollectors((current) => [...current, ...response.collectors]);
      setNextPageToken(next);
      setTotal(response.page?.totalSize ?? total);
      setTotalExact(response.page?.totalSizeExact ?? totalExact);
    } catch (loadError) {
      setNotice(errorMessage(loadError));
    } finally {
      setLoadingMore(false);
    }
  }

  async function readCollector(collector: CollectorRecord, nextModal: "details" | "rename") {
    setBusy(true);
    setNotice(null);
    try {
      const response = await client.collectors.get({ collectorId: collector.collectorId });
      if (response.collector === undefined) throw new Error("The server returned an empty collector record.");
      setTarget(response.collector);
      setDisplayName(response.collector.displayName ?? "");
      setModal(nextModal);
    } catch (readError) {
      setNotice(errorMessage(readError));
    } finally {
      setBusy(false);
    }
  }

  async function renameCollector(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (target === null) return;
    setBusy(true);
    setNotice(null);
    try {
      await client.collectors.update({
        collectorId: target.collectorId,
        expectedVersion: target.version,
        displayName: displayName.trim() || undefined,
        updateMask: ["display_name"],
      });
      setModal(null);
      setNotice(`Collector “${displayName.trim() || target.collectorId}” was updated.`);
      load();
    } catch (mutationError) {
      setNotice(errorMessage(mutationError));
    } finally {
      setBusy(false);
    }
  }

  async function changeState(collector: CollectorRecord) {
    const nextState = collector.administrativeState === CollectorAdministrativeState.COLLECTOR_ADMINISTRATIVE_STATE_DISABLED
      ? CollectorAdministrativeState.COLLECTOR_ADMINISTRATIVE_STATE_ENABLED
      : CollectorAdministrativeState.COLLECTOR_ADMINISTRATIVE_STATE_DISABLED;
    setBusy(true);
    setNotice(null);
    try {
      await client.collectors.setState({ collectorId: collector.collectorId, expectedVersion: collector.version, administrativeState: nextState });
      setNotice(`Collector “${collector.displayName || collector.collectorId}” was ${nextState === CollectorAdministrativeState.COLLECTOR_ADMINISTRATIVE_STATE_ENABLED ? "enabled" : "disabled"}.`);
      load();
    } catch (mutationError) {
      setNotice(errorMessage(mutationError));
    } finally {
      setBusy(false);
    }
  }

  if (state === "loading") return <BackendResourceState kind="loading" title="Loading collector fleet" message="Reading collector health and queue telemetry…" />;
  if (state === "unavailable") return <BackendResourceState kind="unavailable" title="Collector fleet is unavailable" message="The connected server does not advertise collector administration." />;
  if (state === "error") return <BackendResourceState kind="error" title="Collectors could not be loaded" message={error ?? "The collector list request failed."} action={<button type="button" onClick={load}>Retry</button>} />;

  return (
    <div className="admin-section-stack">
      <header className="admin-section-header"><div><h2>Collector fleet</h2><p>Inspect runtime health, durable queues, inputs, and administrative state.</p></div><button className="suite-button" type="button" onClick={load}>Refresh</button></header>
      {notice === null ? null : <output className="access-mode-notice"><span>i</span><div><strong>Collector administration</strong><p>{notice}</p></div></output>}
      <form className="admin-toolbar admin-toolbar--collector" onSubmit={(event) => { event.preventDefault(); setAppliedQuery(query); setAppliedIndexFilter(indexFilter); }}>
        <label><span className="sr-only">Filter collectors</span><input type="search" value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Name, ID, or hostname" /></label>
        <label><span className="sr-only">Filter by index</span><input value={indexFilter} onChange={(event) => setIndexFilter(event.target.value)} placeholder="Authorized index" /></label>
        <label><span className="sr-only">Connection state</span><select value={stateFilter} onChange={(event) => setStateFilter(event.target.value as typeof stateFilter)}><option value="all">All states</option><option value="online">Online</option><option value="stale">Stale</option><option value="offline">Offline</option><option value="disabled">Disabled</option></select></label>
        <button type="submit">Apply</button>
      </form>
      {collectors.length === 0 ? <BackendResourceState kind="empty" title={appliedQuery || appliedIndexFilter || stateFilter !== "all" ? "No matching collectors" : "No collectors connected"} message={appliedQuery || appliedIndexFilter || stateFilter !== "all" ? "Clear the filters to show the complete collector fleet." : "Collectors appear after they establish an authenticated connection."} action={appliedQuery || appliedIndexFilter || stateFilter !== "all" ? <button type="button" onClick={() => { setQuery(""); setAppliedQuery(""); setIndexFilter(""); setAppliedIndexFilter(""); setStateFilter("all"); }}>Clear filters</button> : undefined} /> : (
        <div className="suite-card resource-table-card"><div className="responsive-table-wrap"><table className="product-table admin-resource-table collector-fleet-table">
          <caption className="sr-only">Collector fleet</caption>
          <thead><tr><th scope="col">Collector</th><th scope="col">Connection</th><th scope="col">Queue</th><th scope="col">Inputs</th><th scope="col">Authorized indexes</th><th scope="col">Last seen</th><th scope="col"><span className="sr-only">Actions</span></th></tr></thead>
          <tbody>{collectors.map((collector) => {
            const connection = collectorConnectionLabel(collector.connectionState);
            const disabled = collector.administrativeState === CollectorAdministrativeState.COLLECTOR_ADMINISTRATIVE_STATE_DISABLED;
            return <tr key={collector.collectorId}>
              <td><strong>{collector.displayName || collector.hostname || collector.collectorId}</strong><small className="table-secondary">{collector.collectorId}{collector.sourceRevision ? ` · rev ${collector.sourceRevision.slice(0, 12)}` : ""}</small></td>
              <td><span className={`status-label status-label--${connection === "Online" ? "complete" : connection === "Stale" ? "warning" : "neutral"}`}><i />{connection}</span><small className="table-secondary">{disabled ? "Administratively disabled" : `${collector.operatingSystem ?? "OS unknown"}${collector.architecture ? ` / ${collector.architecture}` : ""}`}</small></td>
              <td>{collector.queue === undefined ? "Not reported" : <>{formatBytes(collector.queue.queuedBytes)}<small className="table-secondary">{collector.queue.queuedEvents.toLocaleString()} events · oldest {formatAge(collector.queue.oldestEventAge?.seconds, collector.queue.oldestEventAge?.nanos)}</small></>}</td>
              <td>{collector.inputs.length.toLocaleString()}<small className="table-secondary">{collector.inputs.filter((input) => titleCaseEnum(collectorInputStateToJSON(input.state), "COLLECTOR_INPUT_STATE_") === "Healthy").length.toLocaleString()} healthy</small></td>
              <td className="table-long-value">{collector.authorizedIndexes.join(", ") || "None"}</td>
              <td>{formatDate(collector.lastSeenAt)}</td>
              <td><div className="row-actions"><button className="table-action" type="button" aria-label={`View details for ${collector.displayName || collector.collectorId}`} disabled={busy} onClick={() => void readCollector(collector, "details")}>Details</button><button className="table-action" type="button" aria-label={`Rename ${collector.displayName || collector.collectorId}`} disabled={busy} onClick={() => void readCollector(collector, "rename")}>Rename</button><button className="table-action" type="button" aria-label={`${disabled ? "Enable" : "Disable"} ${collector.displayName || collector.collectorId}`} disabled={busy} onClick={() => void changeState(collector)}>{disabled ? "Enable" : "Disable"}</button></div></td>
            </tr>;
          })}</tbody>
        </table></div></div>
      )}
      <div className="admin-pagination-footer"><strong>{countLabel(collectors.length, total, totalExact, "collector", "collectors")}</strong>{nextPageToken === null ? null : <button className="button secondary" type="button" disabled={loadingMore || busy} onClick={() => void loadMore()}>{loadingMore ? "Loading…" : "Load more collectors"}</button>}</div>

      {modal === "rename" && target !== null ? <Modal title={`Rename ${target.displayName || target.collectorId}`} subtitle="Set an operator-facing label or leave it blank to clear the label." onClose={() => !busy && setModal(null)} footer={<><button className="button secondary" type="button" disabled={busy} onClick={() => setModal(null)}>Cancel</button><button className="button primary" type="submit" form="rename-collector-form" disabled={busy}>{busy ? "Saving…" : "Save name"}</button></>}><form id="rename-collector-form" className="admin-form" onSubmit={(event) => void renameCollector(event)}>{notice === null ? null : <div className="access-mode-notice" role="alert"><span>!</span><div><strong>Collector could not be renamed</strong><p>{notice}</p></div></div>}<label htmlFor="collector-display-name"><span>Display name</span><input id="collector-display-name" value={displayName} onChange={(event) => setDisplayName(event.target.value)} placeholder={target.collectorId} /></label></form></Modal> : null}
      {modal === "details" && target !== null ? <Modal wide title={target.displayName || target.collectorId} subtitle="Live collector metadata from the server." onClose={() => setModal(null)} footer={<button className="button primary" type="button" onClick={() => setModal(null)}>Close</button>}>
        <div className="collector-detail-stack">
          <dl className="backend-definition-list">
            <div><dt>Collector ID</dt><dd><code>{target.collectorId}</code></dd></div><div><dt>Hostname</dt><dd>{target.hostname ?? "Not reported"}</dd></div><div><dt>Runtime</dt><dd>{[target.operatingSystem, target.architecture, target.sourceRevision ? `rev ${target.sourceRevision.slice(0, 12)}` : undefined].filter(Boolean).join(" · ") || "Not reported"}</dd></div><div><dt>Active instance</dt><dd>{target.activeInstanceId ?? "None"}</dd></div><div><dt>Connected</dt><dd>{formatDate(target.connectedAt)}</dd></div><div><dt>Last seen</dt><dd>{formatDate(target.lastSeenAt)}</dd></div>
          </dl>
          <section className="suite-card"><header className="suite-card-header"><div><h3>Durable queue</h3><p>Backlog, delivery, and rejection telemetry.</p></div></header>{target.queue === undefined ? <BackendResourceState kind="empty" title="Queue telemetry unavailable" message="The collector has not reported queue statistics." /> : <dl className="backend-definition-list"><div><dt>Queued</dt><dd>{target.queue.queuedEvents.toLocaleString()} events · {formatBytes(target.queue.queuedBytes)}</dd></div><div><dt>Oldest event</dt><dd>{formatAge(target.queue.oldestEventAge?.seconds, target.queue.oldestEventAge?.nanos)}</dd></div><div><dt>Sent / acknowledged</dt><dd>{target.queue.sentEventsTotal.toLocaleString()} / {target.queue.acknowledgedEventsTotal.toLocaleString()}</dd></div><div><dt>Retried batches</dt><dd>{target.queue.retriedBatchesTotal.toLocaleString()}</dd></div><div><dt>Rejected / dropped</dt><dd>{target.queue.rejectedEventsTotal.toLocaleString()} / {target.queue.droppedEventsTotal.toLocaleString()}</dd></div></dl>}</section>
          <section className="suite-card"><header className="suite-card-header"><div><h3>Inputs</h3><p>Health and progress for every reported input.</p></div></header><InputRows inputs={target.inputs} /></section>
          <section className="suite-card"><header className="suite-card-header"><div><h3>Authorization and capabilities</h3></div></header><dl className="backend-definition-list"><div><dt>Authorized indexes</dt><dd>{target.authorizedIndexes.join(", ") || "None"}</dd></div><div><dt>Capabilities</dt><dd>{target.capabilities.map((item) => titleCaseEnum(collectorCapabilityToJSON(item), "COLLECTOR_CAPABILITY_")).join(", ") || "None reported"}</dd></div></dl></section>
        </div>
      </Modal> : null}
    </div>
  );
}
