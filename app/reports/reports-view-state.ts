export type ReportsView = "alerts" | "saved-searches";

export const SCHEDULE_REPORT_QUERY_PARAMETER = "scheduleSavedSearchId";

const REPORT_VIEWS: readonly ReportsView[] = ["saved-searches", "alerts"];

export function reportsViewForKey(current: ReportsView, key: string): ReportsView | null {
  if (key === "Home") return REPORT_VIEWS[0];
  if (key === "End") return REPORT_VIEWS.at(-1) ?? null;
  if (key !== "ArrowLeft" && key !== "ArrowRight") return null;
  const currentIndex = REPORT_VIEWS.indexOf(current);
  const direction = key === "ArrowRight" ? 1 : -1;
  return REPORT_VIEWS[(currentIndex + direction + REPORT_VIEWS.length) % REPORT_VIEWS.length] ?? null;
}

export function scheduledReportConfigurationHref(savedSearchId: string): string {
  const id = savedSearchId.trim();
  if (id.length === 0) throw new TypeError("A saved-search ID is required to configure a report.");
  const parameters = new URLSearchParams({ [SCHEDULE_REPORT_QUERY_PARAMETER]: id });
  return `/reports/?${parameters.toString()}`;
}

export function scheduledReportConfigurationTarget(parameters: URLSearchParams): string | null {
  const values = parameters.getAll(SCHEDULE_REPORT_QUERY_PARAMETER);
  if (values.length === 0) return null;
  if (values.length !== 1 || values[0].trim().length === 0) {
    throw new TypeError("The report schedule link is invalid.");
  }
  return values[0].trim();
}
