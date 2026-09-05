export type DashboardLoadMode = "initial" | "reload" | "switch";

export interface DashboardLoadRetry {
  readonly appId: string | undefined;
  readonly mode: "reload" | "switch";
}

export interface DashboardManagerError {
  readonly message: string;
  readonly retry: DashboardLoadRetry | null;
}

export type DashboardViewState = "loading" | "unavailable" | "error" | "no-apps" | "empty" | "ready";

/** Keeps request failures and missing prerequisites distinct from a valid empty catalog. */
export function dashboardViewState(input: {
  appCount: number;
  available: boolean;
  dashboardCount: number;
  error: DashboardManagerError | null;
  loadedCatalog: boolean;
  loading: boolean;
}): DashboardViewState {
  if (input.loading) return "loading";
  if (!input.available) return "unavailable";
  if (!input.loadedCatalog) return input.error === null ? "loading" : "error";
  if (input.appCount === 0) return "no-apps";
  if (input.dashboardCount === 0) return "empty";
  return "ready";
}

/** Keeps retry authority attached to the load request that actually failed. */
export function dashboardLoadError(
  message: string,
  mode: DashboardLoadMode,
  requestedAppId: string | undefined,
): DashboardManagerError {
  return {
    message,
    retry: {
      appId: requestedAppId,
      mode: mode === "switch" ? "switch" : "reload",
    },
  };
}

/** Non-load failures must never inherit an earlier app-switch retry target. */
export function dashboardActionError(message: string): DashboardManagerError {
  return { message, retry: null };
}

/** Allows async panel output only from the exact run that still owns the slot. */
export function dashboardPanelRunCanPublish<T>(
  expectedRun: T,
  activeRun: T | undefined,
  aborted: boolean,
): boolean {
  return !aborted && activeRun === expectedRun;
}
