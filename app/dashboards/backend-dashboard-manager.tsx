"use client";

import {
  type FormEvent,
  type ReactNode,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

import type { AppSummary, AppWorkspace } from "@/gen/ts/open_splunk/app";
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
import { ProductShell } from "../_components/product-shell";
import { AppCreateDialog } from "../_components/app-create-dialog";

import {
  cloneDashboardDefinition,
  dashboardDefinitionsEqual,
  reconcileSavedDashboardDraft,
} from "./dashboard-editor-state";
import {
  dashboardActionError,
  dashboardLoadError,
  dashboardPanelRunCanPublish,
  dashboardViewState,
  type DashboardLoadMode,
  type DashboardManagerError,
} from "./dashboard-manager-state";
import {
  dashboardPanelWaitTimeoutMs,
  waitForDashboardSearchJob,
} from "./dashboard-panel-runner";

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

async function fetchDashboardCatalog(
  client: ReturnType<typeof createOpenSplunkApiClient>,
  preferredAppId: string | undefined,
  mode: DashboardLoadMode,
  signal: AbortSignal,
) {
  const bootstrap = await getSystemBootstrap(client, preferredAppId, { signal });
  if (!bootstrap.features.has(ServerFeature.SERVER_FEATURE_DASHBOARDS)) {
    return { available: false as const };
  }
  const selectedApp = bootstrap.apps.find((app) => app.appId === bootstrap.selectedAppId) ?? bootstrap.apps[0];
  if (mode === "switch" && preferredAppId !== undefined && selectedApp?.appId !== preferredAppId) {
    throw new Error("The requested dashboard app is no longer available.");
  }
  if (!selectedApp) return { available: true as const, bootstrap, dashboards: [], selectedApp: undefined };
  const response = await client.dashboards.list({ appIdFilter: selectedApp.appId }, { signal });
  return { available: true as const, bootstrap, dashboards: response.dashboards, selectedApp };
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
  const [loadedCatalog, setLoadedCatalog] = useState(false);
  const [appAdminAvailable, setAppAdminAvailable] = useState(false);
  const [createAppOpen, setCreateAppOpen] = useState(false);
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
  const viewState = dashboardViewState({
    appCount: apps.length,
    available,
    dashboardCount: dashboards.length,
    error,
    loadedCatalog,
    loading,
  });

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
    try {
      const result = await fetchDashboardCatalog(client, preferredAppId, mode, controller.signal);
      if (!result.available) {
        if (mode === "initial") {
          if (generation === loadGeneration.current) setAvailable(false);
          return false;
        }
        throw new Error("The backend no longer advertises persisted dashboard support.");
      }
      if (generation !== loadGeneration.current) return false;
      setApps(result.bootstrap.apps);
      setAppAdminAvailable(result.bootstrap.features.has(ServerFeature.SERVER_FEATURE_APP_ADMIN));
      setLoadedCatalog(true);
      if (!result.selectedApp) {
        stopAllPanelRuns("Dashboard app catalog is empty");
        setAppID("");
        setAppName("Dashboard workspace");
        setIndexNames([]);
        setDashboards([]);
        selectedIDRef.current = "";
        setSelectedID("");
        setDraft(null);
        setPanelResults({});
        return true;
      }
      const retainedID = mode === "switch" ? "" : selectedIDRef.current;
      const nextSelected = result.dashboards.find((dashboard) => dashboard.dashboardId === retainedID)
        ?? result.dashboards[0]
        ?? null;
      if (mode !== "initial") stopAllPanelRuns(mode === "switch" ? "Dashboard app changed" : "Dashboards reloaded");
      setAvailable(true);
      setAppID(result.selectedApp.appId);
      setAppName(result.selectedApp.displayName || result.selectedApp.slug || "Dashboard workspace");
      setIndexNames(result.bootstrap.indexes.filter((index) => index.searchable).map((index) => index.name));
      setDefaultSearchTimeoutMs(result.bootstrap.limits.defaultSearchTimeoutMs);
      setDashboards(result.dashboards);
      selectedIDRef.current = nextSelected?.dashboardId ?? "";
      setSelectedID(selectedIDRef.current);
      setDraft(nextSelected?.definition ? cloneDashboardDefinition(nextSelected.definition) : null);
      setPanelResults({});
      replaceBackendAppId(result.selectedApp.appId);
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

  const beginLoad = useCallback((preferredAppId: string | undefined, mode: DashboardLoadMode) => {
    if (mode === "initial") setLoading(true);
    if (mode === "reload") setRefreshing(true);
    if (mode === "switch") setSwitchingAppID(preferredAppId ?? "");
    setError(null);
    void load(preferredAppId, mode);
  }, [load]);

  useEffect(() => {
    const requests = loadRequests.current;
    const runs = activePanelRuns.current;
    const preferredAppId = preferredDashboardAppID();
    const generation = ++loadGeneration.current;
    const controller = new AbortController();
    requests.add(controller);
    void fetchDashboardCatalog(client, preferredAppId, "initial", controller.signal).then((result) => {
      if (controller.signal.aborted || generation !== loadGeneration.current) return;
      if (!result.available) {
        setAvailable(false);
        return;
      }
      setApps(result.bootstrap.apps);
      setAppAdminAvailable(result.bootstrap.features.has(ServerFeature.SERVER_FEATURE_APP_ADMIN));
      setLoadedCatalog(true);
      if (!result.selectedApp) {
        setAppID("");
        setAppName("Dashboard workspace");
        setIndexNames([]);
        setDashboards([]);
        selectedIDRef.current = "";
        setSelectedID("");
        setDraft(null);
        setPanelResults({});
        return;
      }
      const nextSelected = result.dashboards.find((dashboard) => dashboard.dashboardId === selectedIDRef.current)
        ?? result.dashboards[0]
        ?? null;
      setAvailable(true);
      setAppID(result.selectedApp.appId);
      replaceBackendAppId(result.selectedApp.appId);
      setAppName(result.selectedApp.displayName || result.selectedApp.slug || "Dashboard workspace");
      setIndexNames(result.bootstrap.indexes.filter((index) => index.searchable).map((index) => index.name));
      setDefaultSearchTimeoutMs(result.bootstrap.limits.defaultSearchTimeoutMs);
      setDashboards(result.dashboards);
      selectedIDRef.current = nextSelected?.dashboardId ?? "";
      setSelectedID(selectedIDRef.current);
      setDraft(nextSelected?.definition ? cloneDashboardDefinition(nextSelected.definition) : null);
      setPanelResults({});
    }).catch((requestError: unknown) => {
      if (!controller.signal.aborted && generation === loadGeneration.current) {
        setError(dashboardLoadError(errorMessage(requestError), "initial", preferredAppId));
      }
    }).finally(() => {
      requests.delete(controller);
      if (generation === loadGeneration.current) setLoading(false);
    });
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
  }, [client]);

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
    if (!nextAppID || saving || refreshing) return;
    if (nextAppID === appID) {
      if (switchingAppID !== null) {
        loadGeneration.current += 1;
        for (const request of loadRequests.current) request.abort();
        loadRequests.current.clear();
        setSwitchingAppID(null);
        setError(null);
      }
      return;
    }
    if (!confirmDiscardChanges()) return;
    beginLoad(nextAppID, "switch");
  }

  function useCreatedApp(app: AppWorkspace) {
    setCreateAppOpen(false);
    beginLoad(app.appId, "switch");
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
    beginLoad(retry.appId, retry.mode);
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

  const shell = (content: ReactNode) => (
    <ProductShell
      activeSection="dashboards"
      apiBaseUrl={apiBaseUrl}
      appName="Dashboards"
      backendAppCatalog={{
        apps,
        error: error?.message,
        onRetry: retryDashboardLoad,
        onSelect: selectApp,
        selectedAppId: appID || null,
        state: loading ? "loading" : error && apps.length === 0 ? "error" : "available",
      }}
      dataMode="backend"
      disclosure={false}
    >
      {content}
    </ProductShell>
  );

  if (viewState === "loading") {
    return shell(<div className="suite-page dashboard-page"><section className="suite-card"><div className="suite-card-body"><h1>Dashboards</h1><p>Loading dashboards…</p></div></section></div>);
  }
  if (viewState === "unavailable") {
    return shell(<div className="suite-page dashboard-page"><section className="suite-card"><div className="suite-card-body"><h1>Dashboards unavailable</h1><p>This backend does not support persisted dashboards.</p></div></section></div>);
  }

  return shell(
    <div className="suite-page dashboard-page">
      <header className="dashboard-title-row">
        <div><h1>Dashboards</h1><p>Build dashboards and run their searches.</p></div>
        {dashboards.length > 0 ? <form className="operations-create-form" onSubmit={createDashboard}>
          <label><span>New dashboard name</span><input value={newName} disabled={workspaceBusy} onChange={(event) => setNewName(event.target.value)} maxLength={255} placeholder="Service overview" /></label>
          <button className="button button--primary" type="submit" disabled={workspaceBusy || !newName.trim()}>Create dashboard</button>
        </form> : null}
      </header>

      {error ? <div className="operations-error-banner" role="alert"><span>{error.message}</span>{error.retry ? <button className="button button--primary" type="button" disabled={workspaceBusy} onClick={retryDashboardLoad}>{error.retry.mode === "switch" ? "Retry app switch" : "Retry"}</button> : null}</div> : null}
      {switchingAppID !== null ? <output className="operations-state-message">Switching dashboard app…</output> : null}
      {refreshing ? <output className="operations-state-message">Reloading dashboards…</output> : null}
      {notice ? <output className="operations-notice-banner">{notice}</output> : null}

      {viewState === "no-apps" ? (
        <section className="suite-card operations-empty-dashboard"><div className="suite-card-body">
          <h2>Create an app workspace first</h2>
          <p>{appAdminAvailable
            ? "Dashboards belong to an app workspace. Create one here, or ask an administrator to give you access."
            : "Dashboards belong to an app workspace. Please ask an administrator to give you access."}</p>
          {appAdminAvailable ? <button className="button button--primary" type="button" onClick={() => setCreateAppOpen(true)}>Create app</button> : <button className="button button--secondary" type="button" onClick={() => beginLoad(undefined, "reload")}>Check again</button>}
        </div></section>
      ) : viewState === "empty" ? (
        <section className="suite-card operations-empty-dashboard"><div className="suite-card-body">
          <h2>Create your first dashboard</h2>
          <p>Add a dashboard to begin building panels for {appName}.</p>
          <form className="operations-create-form" onSubmit={createDashboard}>
            <label><span>Dashboard name</span><input value={newName} disabled={workspaceBusy} onChange={(event) => setNewName(event.target.value)} maxLength={255} placeholder="Service overview" /></label>
            <button className="button button--primary" type="submit" disabled={workspaceBusy || !newName.trim()}>Create dashboard</button>
          </form>
        </div></section>
      ) : viewState === "ready" ? <div className="operations-manager-layout">
        <aside className="suite-card operations-dashboard-list" aria-label="Saved dashboards">
          <h2>Saved dashboards</h2>
          <ul>{dashboards.map((dashboard) => (
              <li key={dashboard.dashboardId}><button className={dashboard.dashboardId === selectedID ? "operations-dashboard-selected" : ""} disabled={workspaceBusy} type="button" onClick={() => selectDashboard(dashboard.dashboardId)}><strong>{dashboard.definition?.name ?? "Untitled"}</strong><span>{dashboard.definition?.panels.length ?? 0} panels · v{dashboard.version.toString()}</span></button></li>
            ))}</ul>
        </aside>

        <section className="operations-dashboard-editor">
          {!selected || !draft ? null : (
            <>
              <section className="suite-card operations-definition-editor">
                <header className="suite-card-header"><div><h2>Dashboard settings</h2><p>Changes use optimistic versioning.</p></div><div className="operations-editor-actions"><button className="button button--primary" type="button" onClick={() => void saveDashboard()} disabled={workspaceBusy || !dirty || !draft.name.trim()}>Save</button><button className="button button--danger" type="button" onClick={() => void deleteDashboard()} disabled={workspaceBusy}>Delete</button></div></header>
                <div className="operations-settings-grid">
                  <label><span>Name</span><input value={draft.name} disabled={workspaceBusy} maxLength={255} onChange={(event) => setDraft({ ...draft, name: event.target.value })} /></label>
                  <label><span>Description</span><input value={draft.description ?? ""} disabled={workspaceBusy} maxLength={16384} onChange={(event) => setDraft({ ...draft, description: event.target.value || undefined })} /></label>
                </div>
              </section>

              <div className="operations-panel-toolbar"><div><h2>Panels</h2>{indexNames.length === 0 ? <p>No searchable indexes are available. Add one before creating panels.</p> : null}</div><button className="button button--primary" type="button" disabled={workspaceBusy || !indexNames[0] || draft.panels.length >= MAXIMUM_DASHBOARD_PANELS} title={draft.panels.length >= MAXIMUM_DASHBOARD_PANELS ? `Dashboards support up to ${MAXIMUM_DASHBOARD_PANELS} panels.` : undefined} onClick={() => setDraft({ ...draft, panels: [...draft.panels, newPanel(appID, indexNames[0] ?? "", draft.panels.length * 4)] })}>Add panel</button></div>
              {draft.panels.length >= MAXIMUM_DASHBOARD_PANELS ? <output className="operations-state-message">This dashboard has reached the {MAXIMUM_DASHBOARD_PANELS}-panel limit.</output> : null}
              {draft.panels.length === 0 ? <section className="suite-card"><p className="operations-state-message">This dashboard has no panels. Add one after at least one searchable index is available.</p></section> : null}
              {draft.panels.map((panel) => {
                const result = panelResults[panel.panelId];
                return (
                  <section className="suite-card dashboard-panel operations-live-panel" key={panel.panelId}>
                    <header className="suite-card-header"><div><h2>{panel.title || "Untitled panel"}</h2><p>{panel.search?.spl || "No SPL configured"}</p></div><div className="operations-editor-actions"><button className="button button--primary" type="button" onClick={() => void runPanel(panel)} disabled={workspaceBusy}>Run</button><button className="button button--danger" type="button" onClick={() => removePanel(panel.panelId)} disabled={workspaceBusy}>Remove</button></div></header>
                    <div className="operations-panel-fields">
                      <label><span>Title</span><input value={panel.title} disabled={workspaceBusy} maxLength={255} onChange={(event) => updatePanel(panel.panelId, (current) => ({ ...current, title: event.target.value }))} /></label>
                      <label className="operations-spl-field"><span>SPL</span><textarea value={panel.search?.spl ?? ""} disabled={workspaceBusy} rows={3} onChange={(event) => updatePanel(panel.panelId, (current) => ({ ...current, search: current.search ? { ...current.search, spl: event.target.value } : current.search }))} /></label>
                      <label><span>Earliest</span><input value={panel.search?.timeRange?.earliest ?? ""} disabled={workspaceBusy} onChange={(event) => updatePanel(panel.panelId, (current) => ({ ...current, search: current.search ? { ...current.search, timeRange: { ...current.search.timeRange, earliest: event.target.value, latest: current.search.timeRange?.latest, timezone: current.search.timeRange?.timezone } } : current.search }))} /></label>
                      <label><span>Latest</span><input value={panel.search?.timeRange?.latest ?? ""} disabled={workspaceBusy} onChange={(event) => updatePanel(panel.panelId, (current) => ({ ...current, search: current.search ? { ...current.search, timeRange: { ...current.search.timeRange, earliest: current.search.timeRange?.earliest, latest: event.target.value, timezone: current.search.timeRange?.timezone } } : current.search }))} /></label>
                      <label><span>Indexes</span><input value={panel.search?.indexScope.join(", ") ?? ""} disabled={workspaceBusy} onChange={(event) => updatePanel(panel.panelId, (current) => ({ ...current, search: current.search ? { ...current.search, indexScope: event.target.value.split(",").map((value) => value.trim()).filter(Boolean) } : current.search }))} /></label>
                    </div>
                    {result ? <div className="operations-panel-result">
                      <p className={result.error ? "operations-result-error" : "operations-result-status"}>{result.error ?? `Search ${stateLabel(result.state)}${result.jobId ? ` · ${result.jobId}` : ""}`}</p>
                      {result.columns && result.columns.length > 0 ? <div className="table-wrap"><table className="table"><thead><tr>{result.columns.map((column) => <th key={column.key} scope="col">{column.label}</th>)}</tr></thead><tbody>{result.rows?.map((row) => <tr key={row.id}>{result.columns!.map((column, columnIndex) => <td key={column.key}>{row.cells[columnIndex] ?? ""}</td>)}</tr>)}</tbody></table></div> : null}
                      {result.rows && result.rows.length === 0 ? <p className="operations-state-message">The search completed with no rows.</p> : null}
                    </div> : null}
                  </section>
                );
              })}
            </>
          )}
        </section>
      </div> : null}
      {createAppOpen ? <AppCreateDialog apiBaseUrl={apiBaseUrl} onClose={() => setCreateAppOpen(false)} onCreated={useCreatedApp} /> : null}
    </div>
  );
}
