"use client";

import {
  type FormEvent,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

import type { AppSummary } from "@/gen/ts/open_splunk/app";
import { SharingScope } from "@/gen/ts/open_splunk/common";
import {
  type Dashboard,
  type DashboardDefinition,
  type DashboardPanel,
} from "@/gen/ts/open_splunk/dashboard";
import { SearchJobState, searchJobStateToJSON } from "@/gen/ts/open_splunk/search";
import { ServerFeature } from "@/gen/ts/open_splunk/system_api";
import type { TypedValue } from "@/gen/ts/open_splunk/value";
import { createOpenSplunkApiClient, getSystemBootstrap } from "@/lib/api";
import { preferredBackendAppId, replaceBackendAppId } from "@/lib/search/app-navigation";

import {
  cloneDashboardDefinition,
  dashboardDefinitionsEqual,
  reconcileSavedDashboardDraft,
} from "./dashboard-editor-state";
import {
  dashboardActionError,
  dashboardLoadError,
  dashboardPanelRunCanPublish,
  type DashboardLoadMode,
  type DashboardManagerError,
} from "./dashboard-manager-state";
import {
  dashboardPanelWaitTimeoutMs,
  waitForDashboardSearchJob,
} from "./dashboard-panel-runner";
import styles from "./operations-dashboard.module.css";

interface BackendDashboardManagerProps {
  apiBaseUrl: string;
}

interface PanelResult {
  state: SearchJobState;
  jobId?: string;
  columns?: Array<{ key: string; label: string }>;
  rows?: Array<{ id: string; cells: string[] }>;
  error?: string;
}

interface ActivePanelRun {
  controller: AbortController;
  jobId?: string;
}

const MAXIMUM_DASHBOARD_PANELS = 24;

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "The dashboard request failed.";
}

function panelID(): string {
  const random = globalThis.crypto?.randomUUID?.() ?? `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  return `panel_${random}`;
}

function preferredDashboardAppID(): string | undefined {
  return preferredBackendAppId(globalThis.location.search);
}

function newPanel(appId: string, indexName: string, row: number): DashboardPanel {
  return {
    panelId: panelID(),
    title: "New panel",
    search: {
      spl: indexName ? `index=${indexName}` : "",
      timeRange: { earliest: "-24h", latest: "now", timezone: "UTC" },
      appId,
      indexScope: indexName ? [indexName] : [],
      preferredResultTab: 0,
      selectedFields: [],
      visualization: undefined,
    },
    column: 0,
    row,
    width: 12,
    height: 4,
  };
}

function formatValue(value: TypedValue | undefined): string {
  switch (value?.kind?.$case) {
    case "nullValue": return "null";
    case "missingValue": return "";
    case "stringValue": return value.kind.value;
    case "sint64Value":
    case "uint64Value": return value.kind.value.toString();
    case "doubleValue": return Number.isFinite(value.kind.value) ? String(value.kind.value) : "";
    case "boolValue": return value.kind.value ? "true" : "false";
    case "timestampValue": return value.kind.value.toISOString();
    case "durationValue": return `${value.kind.value.seconds.toString()}.${String(value.kind.value.nanos).padStart(9, "0")}s`;
    case "decimalValue": return value.kind.value.value;
    case "bytesValue": return `[${value.kind.value.byteLength} bytes]`;
    case "listValue": return `[${value.kind.value.values.map(formatValue).join(", ")}]`;
    case "objectValue": return JSON.stringify(Object.fromEntries(value.kind.value.fields.map((field) => [field.name, formatValue(field.value)])));
    default: return "";
  }
}

function stateLabel(state: SearchJobState): string {
  return searchJobStateToJSON(state)
    .replace("SEARCH_JOB_STATE_", "")
    .replaceAll("_", " ")
    .toLowerCase();
}

export function BackendDashboardManager({ apiBaseUrl }: BackendDashboardManagerProps) {
  const client = useMemo(() => createOpenSplunkApiClient({ baseUrl: apiBaseUrl }), [apiBaseUrl]);
  const [dashboards, setDashboards] = useState<Dashboard[]>([]);
  const [selectedID, setSelectedID] = useState("");
  const [draft, setDraft] = useState<DashboardDefinition | null>(null);
  const [apps, setApps] = useState<AppSummary[]>([]);
  const [appID, setAppID] = useState("");
  const [appName, setAppName] = useState("Dashboard workspace");
  const [indexNames, setIndexNames] = useState<string[]>([]);
  const [newName, setNewName] = useState("");
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [saving, setSaving] = useState(false);
  const [switchingAppID, setSwitchingAppID] = useState<string | null>(null);
  const [defaultSearchTimeoutMs, setDefaultSearchTimeoutMs] = useState(0);
  const [available, setAvailable] = useState(true);
  const [error, setError] = useState<DashboardManagerError | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [panelResults, setPanelResults] = useState<Record<string, PanelResult>>({});
  const loadGeneration = useRef(0);
  const loadRequests = useRef(new Set<AbortController>());
  const activePanelRuns = useRef(new Map<string, ActivePanelRun>());
  const selectedIDRef = useRef("");
  const savePromiseRef = useRef<Promise<Dashboard | null> | null>(null);

  const selected = dashboards.find((dashboard) => dashboard.dashboardId === selectedID) ?? null;
  const dirty = useMemo(() => (
    selected?.definition !== undefined
    && draft !== null
    && !dashboardDefinitionsEqual(draft, selected.definition)
  ), [draft, selected]);
  const workspaceBusy = saving || refreshing || switchingAppID !== null;

  const stopPanelRun = useCallback((panelIdValue: string, reason: string) => {
    const active = activePanelRuns.current.get(panelIdValue);
    if (!active) return;
    activePanelRuns.current.delete(panelIdValue);
    active.controller.abort(new DOMException(reason, "AbortError"));
    if (active.jobId) {
      void client.search.cancel({ searchJobId: active.jobId, reason }).catch(() => undefined);
    }
  }, [client]);

  const stopAllPanelRuns = useCallback((reason: string) => {
    for (const panelIdValue of activePanelRuns.current.keys()) {
      stopPanelRun(panelIdValue, reason);
    }
  }, [stopPanelRun]);

  const load = useCallback(async (
    preferredAppId: string | undefined,
    mode: DashboardLoadMode,
  ): Promise<boolean> => {
    const generation = ++loadGeneration.current;
    for (const request of loadRequests.current) request.abort();
    loadRequests.current.clear();
    const controller = new AbortController();
    loadRequests.current.add(controller);
    if (mode === "initial") setLoading(true);
    if (mode === "reload") setRefreshing(true);
    if (mode === "switch") {
      setSwitchingAppID(preferredAppId ?? "");
    }
    setError(null);
    try {
      const bootstrap = await getSystemBootstrap(client, preferredAppId, { signal: controller.signal });
      if (!bootstrap.features.has(ServerFeature.SERVER_FEATURE_DASHBOARDS)) {
        if (mode === "initial") {
          if (generation === loadGeneration.current) setAvailable(false);
          return false;
        }
        throw new Error("The backend no longer advertises persisted dashboard support.");
      }
      const selectedApp = bootstrap.apps.find((app) => app.appId === bootstrap.selectedAppId) ?? bootstrap.apps[0];
      if (!selectedApp) throw new Error("No active app is available for dashboards.");
      if (mode === "switch" && preferredAppId !== undefined && selectedApp.appId !== preferredAppId) {
        throw new Error("The requested dashboard app is no longer available.");
      }
      const response = await client.dashboards.list({ appIdFilter: selectedApp.appId }, { signal: controller.signal });
      if (generation !== loadGeneration.current) return false;
      const retainedID = mode === "switch" ? "" : selectedIDRef.current;
      const nextSelected = response.dashboards.find((dashboard) => dashboard.dashboardId === retainedID)
        ?? response.dashboards[0]
        ?? null;
      if (mode !== "initial") stopAllPanelRuns(mode === "switch" ? "Dashboard app changed" : "Dashboards reloaded");
      setAvailable(true);
      setApps(bootstrap.apps);
      setAppID(selectedApp.appId);
      setAppName(selectedApp.displayName || selectedApp.slug || "Dashboard workspace");
      setIndexNames(bootstrap.indexes.filter((index) => index.searchable).map((index) => index.name));
      setDefaultSearchTimeoutMs(bootstrap.limits.defaultSearchTimeoutMs);
      setDashboards(response.dashboards);
      selectedIDRef.current = nextSelected?.dashboardId ?? "";
      setSelectedID(selectedIDRef.current);
      setDraft(nextSelected?.definition ? cloneDashboardDefinition(nextSelected.definition) : null);
      setPanelResults({});
      if (mode === "switch") replaceBackendAppId(selectedApp.appId);
      return true;
    } catch (requestError) {
      if (!controller.signal.aborted && generation === loadGeneration.current) {
        setError(dashboardLoadError(errorMessage(requestError), mode, preferredAppId));
      }
      return false;
    } finally {
      loadRequests.current.delete(controller);
      if (generation === loadGeneration.current) {
        if (mode === "initial") setLoading(false);
        if (mode === "reload") setRefreshing(false);
        if (mode === "switch") setSwitchingAppID(null);
      }
    }
  }, [client, stopAllPanelRuns]);

  useEffect(() => {
    const requests = loadRequests.current;
    const runs = activePanelRuns.current;
    void load(preferredDashboardAppID(), "initial");
    return () => {
      loadGeneration.current += 1;
      for (const request of requests) request.abort();
      requests.clear();
      for (const active of runs.values()) {
        active.controller.abort(new DOMException("Dashboard closed", "AbortError"));
        if (active.jobId) {
          void client.search.cancel({ searchJobId: active.jobId, reason: "Dashboard closed" }).catch(() => undefined);
        }
      }
      runs.clear();
    };
  }, [client, load]);

  useEffect(() => {
    if (!dirty) return;
    const warnAboutUnsavedWork = (event: BeforeUnloadEvent) => {
      event.preventDefault();
      event.returnValue = "";
    };
    globalThis.addEventListener("beforeunload", warnAboutUnsavedWork);
    return () => globalThis.removeEventListener("beforeunload", warnAboutUnsavedWork);
  }, [dirty]);

  function confirmDiscardChanges(): boolean {
    return !dirty || globalThis.confirm("Discard the unsaved changes to this dashboard?");
  }

  async function createDashboard(event: FormEvent) {
    event.preventDefault();
    const name = newName.trim();
    if (!name || !appID || workspaceBusy || !confirmDiscardChanges()) return;
    setSaving(true);
    setError(null);
    try {
      const response = await client.dashboards.create({
        definition: {
          name,
          description: undefined,
          appId: appID,
          sharingScope: SharingScope.SHARING_SCOPE_PRIVATE,
          ownerId: undefined,
          panels: indexNames[0] ? [newPanel(appID, indexNames[0], 0)] : [],
        },
      });
      if (!response.dashboard?.definition) throw new Error("The server returned an empty dashboard.");
      stopAllPanelRuns("A dashboard was created");
      setDashboards((current) => [response.dashboard!, ...current]);
      selectedIDRef.current = response.dashboard.dashboardId;
      setSelectedID(response.dashboard.dashboardId);
      setDraft(cloneDashboardDefinition(response.dashboard.definition));
      setPanelResults({});
      setNewName("");
      setNotice("Dashboard created.");
    } catch (requestError) {
      setError(dashboardActionError(errorMessage(requestError)));
    } finally {
      setSaving(false);
    }
  }

  function saveDashboard(): Promise<Dashboard | null> {
    const activeSave = savePromiseRef.current;
    if (activeSave !== null) return activeSave;
    if (!selected || !draft) return Promise.resolve(null);
    if (!dirty) return Promise.resolve(selected);
    const dashboardID = selected.dashboardId;
    const submittedDraft = cloneDashboardDefinition(draft);
    let request!: Promise<Dashboard | null>;
    request = (async () => {
      setSaving(true);
      setError(null);
      try {
        const response = await client.dashboards.update({
          dashboardId: dashboardID,
          expectedVersion: selected.version,
          definition: submittedDraft,
        });
        if (!response.dashboard?.definition) throw new Error("The server returned an empty dashboard.");
        setDashboards((current) => current.map((dashboard) => (
          dashboard.dashboardId === response.dashboard!.dashboardId ? response.dashboard! : dashboard
        )));
        if (selectedIDRef.current === dashboardID) {
          setDraft((current) => reconcileSavedDashboardDraft(
            current,
            submittedDraft,
            response.dashboard!.definition!,
          ));
        }
        setNotice("Dashboard version saved.");
        return response.dashboard;
      } catch (requestError) {
        setError(dashboardActionError(errorMessage(requestError)));
        return null;
      } finally {
        if (savePromiseRef.current === request) {
          savePromiseRef.current = null;
          setSaving(false);
        }
      }
    })();
    savePromiseRef.current = request;
    return request;
  }

  async function deleteDashboard() {
    if (!selected || !globalThis.confirm(`Delete “${selected.definition?.name ?? "this dashboard"}”?`)) return;
    stopAllPanelRuns("Dashboard deleted");
    setSaving(true);
    setError(null);
    try {
      await client.dashboards.delete({ dashboardId: selected.dashboardId, expectedVersion: selected.version });
      const remaining = dashboards.filter((dashboard) => dashboard.dashboardId !== selected.dashboardId);
      const nextSelected = remaining[0] ?? null;
      setDashboards(remaining);
      selectedIDRef.current = nextSelected?.dashboardId ?? "";
      setSelectedID(selectedIDRef.current);
      setDraft(nextSelected?.definition ? cloneDashboardDefinition(nextSelected.definition) : null);
      setPanelResults({});
      setNotice("Dashboard deleted.");
    } catch (requestError) {
      setError(dashboardActionError(errorMessage(requestError)));
    } finally {
      setSaving(false);
    }
  }

  async function runPanel(panel: DashboardPanel) {
    if (!selected || refreshing || switchingAppID !== null) return;
    const persisted = dirty ? await saveDashboard() : selected;
    if (!persisted) return;
    stopPanelRun(panel.panelId, "A newer panel run started");
    const controller = new AbortController();
    const active: ActivePanelRun = { controller };
    const waitTimeoutMs = dashboardPanelWaitTimeoutMs(defaultSearchTimeoutMs);
    let terminalJobObserved = false;
    activePanelRuns.current.set(panel.panelId, active);
    setPanelResults((current) => ({ ...current, [panel.panelId]: { state: SearchJobState.SEARCH_JOB_STATE_QUEUED } }));
    try {
      const response = await client.dashboards.runPanel(
        { dashboardId: persisted.dashboardId, panelId: panel.panelId },
        { signal: controller.signal },
      );
      const initialJob = response.searchJob;
      if (!initialJob) throw new Error("The server returned an empty search job.");
      if (!dashboardPanelRunCanPublish(
        active,
        activePanelRuns.current.get(panel.panelId),
        controller.signal.aborted,
      )) {
        void client.search.cancel({ searchJobId: initialJob.searchJobId, reason: "Panel run superseded" }).catch(() => undefined);
        return;
      }
      active.jobId = initialJob.searchJobId;
      setPanelResults((current) => ({ ...current, [panel.panelId]: { state: initialJob.state, jobId: initialJob.searchJobId } }));
      const job = await waitForDashboardSearchJob(initialJob, {
        defaultSearchTimeoutMs,
        signal: controller.signal,
        getJob: async (searchJobId, signal) => {
          const polled = await client.search.get(
            { searchJobId, includePlan: false, includeGeneratedSql: false },
            { signal },
          );
          const nextJob = polled.searchJob;
          if (!nextJob) throw new Error("The server returned an empty search job.");
          if (nextJob.searchJobId !== searchJobId) {
            throw new Error("The server returned a different search job.");
          }
          if (dashboardPanelRunCanPublish(
            active,
            activePanelRuns.current.get(panel.panelId),
            signal.aborted,
          )) {
            setPanelResults((current) => ({
              ...current,
              [panel.panelId]: { state: nextJob.state, jobId: nextJob.searchJobId },
            }));
          }
          return nextJob;
        },
      });
      if (!dashboardPanelRunCanPublish(
        active,
        activePanelRuns.current.get(panel.panelId),
        controller.signal.aborted,
      )) return;
      terminalJobObserved = true;
      if (job.state !== SearchJobState.SEARCH_JOB_STATE_COMPLETED) {
        throw new Error(job.failure?.message || `Panel search ${stateLabel(job.state)}.`);
      }
      const result = await client.search.results({
        searchJobId: job.searchJobId,
        page: { pageSize: 20, pageToken: undefined, includeTotalSize: false },
        columns: [],
        allowPartialResults: false,
      }, { signal: controller.signal });
      if (!dashboardPanelRunCanPublish(
        active,
        activePanelRuns.current.get(panel.panelId),
        controller.signal.aborted,
      )) return;
      const columns = result.resultPage?.schema?.columns.map((column) => ({ key: column.fieldName, label: column.displayName || column.fieldName })) ?? [];
      const rows = result.resultPage?.rows.map((row) => ({ id: row.rowId, cells: row.cells.map(formatValue) })) ?? [];
      setPanelResults((current) => ({ ...current, [panel.panelId]: { state: job.state, jobId: job.searchJobId, columns, rows } }));
    } catch (requestError) {
      if (dashboardPanelRunCanPublish(
        active,
        activePanelRuns.current.get(panel.panelId),
        controller.signal.aborted,
      )) {
        if (active.jobId && !terminalJobObserved) {
          void client.search.cancel({
            searchJobId: active.jobId,
            reason: `Dashboard panel wait stopped before its ${waitTimeoutMs}ms limit`,
          }).catch(() => undefined);
        }
        setPanelResults((current) => ({
          ...current,
          [panel.panelId]: {
            state: current[panel.panelId]?.state ?? SearchJobState.SEARCH_JOB_STATE_FAILED,
            jobId: current[panel.panelId]?.jobId,
            error: errorMessage(requestError),
          },
        }));
      }
    } finally {
      if (activePanelRuns.current.get(panel.panelId) === active) {
        activePanelRuns.current.delete(panel.panelId);
      }
    }
  }

  function selectApp(nextAppID: string) {
    if (!nextAppID || nextAppID === appID || workspaceBusy || !confirmDiscardChanges()) return;
    void load(nextAppID, "switch");
  }

  function selectDashboard(nextDashboardID: string) {
    if (nextDashboardID === selectedID || workspaceBusy || !confirmDiscardChanges()) return;
    const nextSelected = dashboards.find((dashboard) => dashboard.dashboardId === nextDashboardID);
    if (!nextSelected?.definition) return;
    stopAllPanelRuns("Dashboard selection changed");
    selectedIDRef.current = nextDashboardID;
    setSelectedID(nextDashboardID);
    setDraft(cloneDashboardDefinition(nextSelected.definition));
    setPanelResults({});
  }

  function retryDashboardLoad() {
    const retry = error?.retry;
    if (!retry || workspaceBusy || !confirmDiscardChanges()) return;
    void load(retry.appId, retry.mode);
  }

  function removePanel(panelIdValue: string) {
    stopPanelRun(panelIdValue, "Dashboard panel removed");
    setDraft((current) => current ? {
      ...current,
      panels: current.panels.filter((candidate) => candidate.panelId !== panelIdValue),
    } : current);
  }

  function updatePanel(panelIdValue: string, update: (panel: DashboardPanel) => DashboardPanel) {
    setDraft((current) => current ? {
      ...current,
      panels: current.panels.map((panel) => panel.panelId === panelIdValue ? update(panel) : panel),
    } : current);
  }

  if (loading) {
    return <div className="suite-page dashboard-page"><section className="suite-card"><p className={styles.stateMessage}>Loading dashboards…</p></section></div>;
  }
  if (!available) {
    return <div className="suite-page dashboard-page"><section className="suite-card"><h1>Dashboards unavailable</h1><p className={styles.stateMessage}>This backend does not advertise persisted dashboard support.</p></section></div>;
  }

  return (
    <div className="suite-page dashboard-page">
      <header className="dashboard-title-row">
        <div><span className="suite-eyebrow">{appName.toUpperCase()}</span><h1>Dashboards</h1><p>Build persisted panels and run their server-authoritative searches.</p></div>
        <form className={styles.createForm} onSubmit={createDashboard}>
          <label><span>App</span><select value={appID} disabled={workspaceBusy || loading} aria-busy={switchingAppID !== null} onChange={(event) => selectApp(event.target.value)}>{apps.map((app) => <option key={app.appId} value={app.appId}>{app.displayName || app.slug || app.appId}</option>)}</select></label>
          <label><span>New dashboard name</span><input value={newName} disabled={workspaceBusy} onChange={(event) => setNewName(event.target.value)} maxLength={255} placeholder="Service overview" /></label>
          <button type="submit" disabled={workspaceBusy || !newName.trim()}>Create dashboard</button>
        </form>
      </header>

      {error ? <div className={styles.errorBanner} role="alert"><span>{error.message}</span>{error.retry ? <button type="button" disabled={workspaceBusy} onClick={retryDashboardLoad}>{error.retry.mode === "switch" ? "Retry app switch" : "Reload"}</button> : null}</div> : null}
      {switchingAppID !== null ? <output className={styles.stateMessage}>Switching dashboard app…</output> : null}
      {refreshing ? <output className={styles.stateMessage}>Reloading dashboards…</output> : null}
      {notice ? <output className={styles.noticeBanner}>{notice}</output> : null}

      <div className={styles.managerLayout}>
        <aside className={`suite-card ${styles.dashboardList}`} aria-label="Saved dashboards">
          <h2>Saved dashboards</h2>
          {dashboards.length === 0 ? <p>No dashboards yet.</p> : (
            <ul>{dashboards.map((dashboard) => (
              <li key={dashboard.dashboardId}><button className={dashboard.dashboardId === selectedID ? styles.selectedDashboard : ""} disabled={workspaceBusy} type="button" onClick={() => selectDashboard(dashboard.dashboardId)}><strong>{dashboard.definition?.name ?? "Untitled"}</strong><span>{dashboard.definition?.panels.length ?? 0} panels · v{dashboard.version.toString()}</span></button></li>
            ))}</ul>
          )}
        </aside>

        <main className={styles.dashboardEditor}>
          {!selected || !draft ? (
            <section className="suite-card"><h2>Select a dashboard</h2><p className={styles.stateMessage}>Choose an existing dashboard or create one to start editing.</p></section>
          ) : (
            <>
              <section className={`suite-card ${styles.definitionEditor}`}>
                <header className="suite-card-header"><div><h2>Dashboard settings</h2><p>Changes use optimistic versioning.</p></div><div className={styles.editorActions}><button type="button" onClick={() => void saveDashboard()} disabled={workspaceBusy || !dirty || !draft.name.trim()}>Save</button><button className={styles.dangerButton} type="button" onClick={() => void deleteDashboard()} disabled={workspaceBusy}>Delete</button></div></header>
                <div className={styles.settingsGrid}>
                  <label><span>Name</span><input value={draft.name} disabled={workspaceBusy} maxLength={255} onChange={(event) => setDraft({ ...draft, name: event.target.value })} /></label>
                  <label><span>Description</span><input value={draft.description ?? ""} disabled={workspaceBusy} maxLength={16384} onChange={(event) => setDraft({ ...draft, description: event.target.value || undefined })} /></label>
                </div>
              </section>

              <div className={styles.panelToolbar}><h2>Panels</h2><button type="button" disabled={workspaceBusy || !indexNames[0] || draft.panels.length >= MAXIMUM_DASHBOARD_PANELS} title={draft.panels.length >= MAXIMUM_DASHBOARD_PANELS ? `Dashboards support up to ${MAXIMUM_DASHBOARD_PANELS} panels.` : undefined} onClick={() => setDraft({ ...draft, panels: [...draft.panels, newPanel(appID, indexNames[0] ?? "", draft.panels.length * 4)] })}>Add panel</button></div>
              {draft.panels.length >= MAXIMUM_DASHBOARD_PANELS ? <output className={styles.stateMessage}>This dashboard has reached the {MAXIMUM_DASHBOARD_PANELS}-panel limit.</output> : null}
              {draft.panels.length === 0 ? <section className="suite-card"><p className={styles.stateMessage}>This dashboard has no panels. Add one after at least one searchable index is available.</p></section> : null}
              {draft.panels.map((panel) => {
                const result = panelResults[panel.panelId];
                return (
                  <section className={`suite-card dashboard-panel ${styles.livePanel}`} key={panel.panelId}>
                    <header className="suite-card-header"><div><h2>{panel.title || "Untitled panel"}</h2><p>{panel.search?.spl || "No SPL configured"}</p></div><div className={styles.editorActions}><button type="button" onClick={() => void runPanel(panel)} disabled={workspaceBusy}>Run</button><button className={styles.dangerButton} type="button" onClick={() => removePanel(panel.panelId)} disabled={workspaceBusy}>Remove</button></div></header>
                    <div className={styles.panelFields}>
                      <label><span>Title</span><input value={panel.title} disabled={workspaceBusy} maxLength={255} onChange={(event) => updatePanel(panel.panelId, (current) => ({ ...current, title: event.target.value }))} /></label>
                      <label className={styles.splField}><span>SPL</span><textarea value={panel.search?.spl ?? ""} disabled={workspaceBusy} rows={3} onChange={(event) => updatePanel(panel.panelId, (current) => ({ ...current, search: current.search ? { ...current.search, spl: event.target.value } : current.search }))} /></label>
                      <label><span>Earliest</span><input value={panel.search?.timeRange?.earliest ?? ""} disabled={workspaceBusy} onChange={(event) => updatePanel(panel.panelId, (current) => ({ ...current, search: current.search ? { ...current.search, timeRange: { ...current.search.timeRange, earliest: event.target.value, latest: current.search.timeRange?.latest, timezone: current.search.timeRange?.timezone } } : current.search }))} /></label>
                      <label><span>Latest</span><input value={panel.search?.timeRange?.latest ?? ""} disabled={workspaceBusy} onChange={(event) => updatePanel(panel.panelId, (current) => ({ ...current, search: current.search ? { ...current.search, timeRange: { ...current.search.timeRange, earliest: current.search.timeRange?.earliest, latest: event.target.value, timezone: current.search.timeRange?.timezone } } : current.search }))} /></label>
                      <label><span>Indexes</span><input value={panel.search?.indexScope.join(", ") ?? ""} disabled={workspaceBusy} onChange={(event) => updatePanel(panel.panelId, (current) => ({ ...current, search: current.search ? { ...current.search, indexScope: event.target.value.split(",").map((value) => value.trim()).filter(Boolean) } : current.search }))} /></label>
                    </div>
                    {result ? <div className={styles.panelResult}>
                      <p className={result.error ? styles.resultError : styles.resultStatus}>{result.error ?? `Search ${stateLabel(result.state)}${result.jobId ? ` · ${result.jobId}` : ""}`}</p>
                      {result.columns && result.columns.length > 0 ? <div className="responsive-table-wrap"><table className="product-table"><thead><tr>{result.columns.map((column) => <th key={column.key} scope="col">{column.label}</th>)}</tr></thead><tbody>{result.rows?.map((row) => <tr key={row.id}>{result.columns!.map((column, columnIndex) => <td key={column.key}>{row.cells[columnIndex] ?? ""}</td>)}</tr>)}</tbody></table></div> : null}
                      {result.rows && result.rows.length === 0 ? <p className={styles.stateMessage}>The search completed with no rows.</p> : null}
                    </div> : null}
                  </section>
                );
              })}
            </>
          )}
        </main>
      </div>
    </div>
  );
}
