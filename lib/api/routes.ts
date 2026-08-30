import * as ExportApi from "@/gen/ts/open_splunk/export_api";
import * as HistoryApi from "@/gen/ts/open_splunk/history_api";
import * as HecAdminApi from "@/gen/ts/open_splunk/hec_admin_api";
import * as IndexApi from "@/gen/ts/open_splunk/index_api";
import * as AppApi from "@/gen/ts/open_splunk/app_api";
import * as AlertApi from "@/gen/ts/open_splunk/alert_api";
import * as AuditApi from "@/gen/ts/open_splunk/audit_api";
import * as CollectorAdminApi from "@/gen/ts/open_splunk/collector_admin_api";
import * as DashboardApi from "@/gen/ts/open_splunk/dashboard_api";
import * as KnowledgeApi from "@/gen/ts/open_splunk/knowledge_api";
import * as LookupApi from "@/gen/ts/open_splunk/lookup_api";
import * as SavedSearchApi from "@/gen/ts/open_splunk/saved_search_api";
import * as ScheduleApi from "@/gen/ts/open_splunk/schedule_api";
import * as SearchApi from "@/gen/ts/open_splunk/search_api";
import * as SearchAttemptAuditApi from "@/gen/ts/open_splunk/search_attempt_audit_api";
import * as SearchInspectionApi from "@/gen/ts/open_splunk/search_inspection_api";
import * as ServerSettingsApi from "@/gen/ts/open_splunk/server_settings_api";
import * as SystemApi from "@/gen/ts/open_splunk/system_api";

import { defineProtobufRoute, type ProtobufRoute } from "./protobuf-transport";

export const MAXIMUM_KNOWLEDGE_MANAGEMENT_RESPONSE_BYTES = 8 << 20;
export const MAXIMUM_LOOKUP_MANAGEMENT_RESPONSE_BYTES = 9 << 20;
export const MAXIMUM_KNOWLEDGE_GRAPH_RESPONSE_BYTES = 128 << 10;
export const MAXIMUM_SEARCH_INSPECTION_RESPONSE_BYTES = 8 << 20;
export const MAXIMUM_DASHBOARD_LIST_RESPONSE_BYTES = 8 << 20;

function readPreviewVarint(
  bytes: Uint8Array,
  start: number,
): { readonly value: bigint; readonly next: number } {
  let value = 0n;
  let shift = 0n;
  for (let index = start; index < bytes.byteLength && index < start + 10; index += 1) {
    const octet = bytes[index] ?? 0;
    value |= BigInt(octet & 0x7f) << shift;
    if ((octet & 0x80) === 0) return { value, next: index + 1 };
    shift += 7n;
  }
  throw new TypeError("Knowledge Preview response contains an invalid varint.");
}

function validatePreviewResponseEnvelopeWire(bytes: Uint8Array): void {
  let position = 0;
  let previousField = 0;
  const singular = new Set<number>();
  while (position < bytes.byteLength) {
    const tag = readPreviewVarint(bytes, position);
    position = tag.next;
    const field = Number(tag.value >> 3n);
    const wireType = Number(tag.value & 7n);
    const lengthDelimited = field >= 1 && field <= 5;
    const varint = field === 6 || field === 7;
    if (
      field === 0
      || field < previousField
      || (!lengthDelimited && !varint)
      || (lengthDelimited && wireType !== 2)
      || (varint && wireType !== 0)
      || (field !== 4 && field !== 5 && singular.has(field))
    ) {
      throw new TypeError("Knowledge Preview response envelope is malformed.");
    }
    previousField = field;
    if (field !== 4 && field !== 5) singular.add(field);
    if (lengthDelimited) {
      const length = readPreviewVarint(bytes, position);
      position = length.next;
      if (length.value > BigInt(bytes.byteLength - position)) {
        throw new TypeError("Knowledge Preview response is truncated.");
      }
      position += Number(length.value);
    } else {
      position = readPreviewVarint(bytes, position).next;
    }
  }
}

/**
 * Preview responses are server-sealed deterministic protobuf. Validate the
 * complete top-level frame before invoking the generated decoder so unknown,
 * duplicate, wrong-wire, truncated, or out-of-order envelope authority fails
 * closed without round-tripping Timestamp precision through JavaScript Date.
 */
const strictPreviewKnowledgeObjectResponse = {
  encode: KnowledgeApi.PreviewKnowledgeObjectResponse.encode,
  decode(
    input: Parameters<typeof KnowledgeApi.PreviewKnowledgeObjectResponse.decode>[0],
    length?: number,
  ): ReturnType<typeof KnowledgeApi.PreviewKnowledgeObjectResponse.decode> {
    if (!(input instanceof Uint8Array) || length !== undefined) {
      throw new TypeError("Knowledge Preview requires one complete response frame.");
    }
    validatePreviewResponseEnvelopeWire(input);
    return KnowledgeApi.PreviewKnowledgeObjectResponse.decode(input);
  },
};

/** Derives a generated request type from a route without duplicating contracts. */
export type RouteRequest<TRoute> = TRoute extends ProtobufRoute<infer TRequest, unknown> ? TRequest : never;

/** Derives a generated response type from a route without duplicating contracts. */
export type RouteResponse<TRoute> = TRoute extends ProtobufRoute<unknown, infer TResponse> ? TResponse : never;

export const systemRoutes = {
  bootstrap: defineProtobufRoute(
    "/api/system/bootstrap",
    SystemApi.GetSystemBootstrapRequest,
    SystemApi.GetSystemBootstrapResponse,
  ),
} as const;

export const serverSettingsRoutes = {
  get: defineProtobufRoute(
    "/api/server/settings/get",
    ServerSettingsApi.GetServerSettingsRequest,
    ServerSettingsApi.GetServerSettingsResponse,
  ),
  update: defineProtobufRoute(
    "/api/server/settings/update",
    ServerSettingsApi.UpdateServerSettingsRequest,
    ServerSettingsApi.UpdateServerSettingsResponse,
  ),
} as const;

export const indexRoutes = {
  create: defineProtobufRoute(
    "/api/indexes/create",
    IndexApi.CreateIndexRequest,
    IndexApi.CreateIndexResponse,
  ),
  get: defineProtobufRoute(
    "/api/indexes/get",
    IndexApi.GetIndexRequest,
    IndexApi.GetIndexResponse,
  ),
  list: defineProtobufRoute(
    "/api/indexes/list",
    IndexApi.ListIndexesRequest,
    IndexApi.ListIndexesResponse,
  ),
  fields: defineProtobufRoute(
    "/api/indexes/fields/list",
    IndexApi.ListIndexFieldsRequest,
    IndexApi.ListIndexFieldsResponse,
  ),
  update: defineProtobufRoute(
    "/api/indexes/update",
    IndexApi.UpdateIndexRequest,
    IndexApi.UpdateIndexResponse,
  ),
  setState: defineProtobufRoute(
    "/api/indexes/state/set",
    IndexApi.SetIndexStateRequest,
    IndexApi.SetIndexStateResponse,
  ),
  delete: defineProtobufRoute(
    "/api/indexes/delete",
    IndexApi.DeleteIndexRequest,
    IndexApi.DeleteIndexResponse,
  ),
  stats: defineProtobufRoute(
    "/api/indexes/stats/get",
    IndexApi.GetIndexStatsRequest,
    IndexApi.GetIndexStatsResponse,
  ),
} as const;

export const appRoutes = {
  create: defineProtobufRoute(
    "/api/apps/create",
    AppApi.CreateAppRequest,
    AppApi.CreateAppResponse,
  ),
  get: defineProtobufRoute(
    "/api/apps/get",
    AppApi.GetAppRequest,
    AppApi.GetAppResponse,
  ),
  list: defineProtobufRoute(
    "/api/apps/list",
    AppApi.ListAppsRequest,
    AppApi.ListAppsResponse,
  ),
  update: defineProtobufRoute(
    "/api/apps/update",
    AppApi.UpdateAppRequest,
    AppApi.UpdateAppResponse,
  ),
  setState: defineProtobufRoute(
    "/api/apps/state/set",
    AppApi.SetAppStateRequest,
    AppApi.SetAppStateResponse,
  ),
  delete: defineProtobufRoute(
    "/api/apps/delete",
    AppApi.DeleteAppRequest,
    AppApi.DeleteAppResponse,
  ),
} as const;

export const knowledgeRoutes = {
  create: defineProtobufRoute(
    "/api/knowledge/objects/create",
    KnowledgeApi.CreateKnowledgeObjectRequest,
    KnowledgeApi.CreateKnowledgeObjectResponse,
  ),
  get: defineProtobufRoute(
    "/api/knowledge/objects/get",
    KnowledgeApi.GetKnowledgeObjectRequest,
    KnowledgeApi.GetKnowledgeObjectResponse,
    { maximumResponseBytes: MAXIMUM_KNOWLEDGE_MANAGEMENT_RESPONSE_BYTES },
  ),
  list: defineProtobufRoute(
    "/api/knowledge/objects/list",
    KnowledgeApi.ListKnowledgeObjectsRequest,
    KnowledgeApi.ListKnowledgeObjectsResponse,
    { maximumResponseBytes: MAXIMUM_KNOWLEDGE_MANAGEMENT_RESPONSE_BYTES },
  ),
  dependencies: defineProtobufRoute(
    "/api/knowledge/objects/dependencies",
    KnowledgeApi.ListKnowledgeObjectDependenciesRequest,
    KnowledgeApi.ListKnowledgeObjectDependenciesResponse,
    { maximumResponseBytes: MAXIMUM_KNOWLEDGE_GRAPH_RESPONSE_BYTES },
  ),
  dependents: defineProtobufRoute(
    "/api/knowledge/objects/dependents",
    KnowledgeApi.ListKnowledgeObjectDependentsRequest,
    KnowledgeApi.ListKnowledgeObjectDependentsResponse,
    { maximumResponseBytes: MAXIMUM_KNOWLEDGE_GRAPH_RESPONSE_BYTES },
  ),
  validate: defineProtobufRoute(
    "/api/knowledge/objects/validate",
    KnowledgeApi.ValidateKnowledgeObjectRequest,
    KnowledgeApi.ValidateKnowledgeObjectResponse,
    { maximumResponseBytes: MAXIMUM_KNOWLEDGE_MANAGEMENT_RESPONSE_BYTES },
  ),
  preview: defineProtobufRoute(
    "/api/knowledge/objects/preview",
    KnowledgeApi.PreviewKnowledgeObjectRequest,
    strictPreviewKnowledgeObjectResponse,
    { maximumResponseBytes: MAXIMUM_KNOWLEDGE_MANAGEMENT_RESPONSE_BYTES },
  ),
  update: defineProtobufRoute(
    "/api/knowledge/objects/update",
    KnowledgeApi.UpdateKnowledgeObjectRequest,
    KnowledgeApi.UpdateKnowledgeObjectResponse,
  ),
  setState: defineProtobufRoute(
    "/api/knowledge/objects/set-state",
    KnowledgeApi.SetKnowledgeObjectStateRequest,
    KnowledgeApi.SetKnowledgeObjectStateResponse,
  ),
  delete: defineProtobufRoute(
    "/api/knowledge/objects/delete",
    KnowledgeApi.DeleteKnowledgeObjectRequest,
    KnowledgeApi.DeleteKnowledgeObjectResponse,
  ),
  prepareQuarantine: defineProtobufRoute(
    "/api/knowledge/objects/quarantine/prepare",
    KnowledgeApi.PrepareKnowledgeObjectQuarantineRequest,
    KnowledgeApi.PrepareKnowledgeObjectQuarantineResponse,
  ),
  quarantine: defineProtobufRoute(
    "/api/knowledge/objects/quarantine",
    KnowledgeApi.QuarantineKnowledgeObjectRequest,
    KnowledgeApi.QuarantineKnowledgeObjectResponse,
  ),
} as const;

export const lookupRoutes = {
  create: defineProtobufRoute(
    "/api/knowledge/lookups/create",
    LookupApi.CreateLookupRequest,
    LookupApi.CreateLookupResponse,
    { maximumResponseBytes: MAXIMUM_LOOKUP_MANAGEMENT_RESPONSE_BYTES },
  ),
  get: defineProtobufRoute(
    "/api/knowledge/lookups/get",
    LookupApi.GetLookupRequest,
    LookupApi.GetLookupResponse,
    { maximumResponseBytes: MAXIMUM_LOOKUP_MANAGEMENT_RESPONSE_BYTES },
  ),
  list: defineProtobufRoute(
    "/api/knowledge/lookups/list",
    LookupApi.ListLookupsRequest,
    LookupApi.ListLookupsResponse,
    { maximumResponseBytes: MAXIMUM_LOOKUP_MANAGEMENT_RESPONSE_BYTES },
  ),
  replace: defineProtobufRoute(
    "/api/knowledge/lookups/replace",
    LookupApi.ReplaceLookupRequest,
    LookupApi.ReplaceLookupResponse,
    { maximumResponseBytes: MAXIMUM_LOOKUP_MANAGEMENT_RESPONSE_BYTES },
  ),
  setState: defineProtobufRoute(
    "/api/knowledge/lookups/state/set",
    LookupApi.SetLookupStateRequest,
    LookupApi.SetLookupStateResponse,
    { maximumResponseBytes: MAXIMUM_LOOKUP_MANAGEMENT_RESPONSE_BYTES },
  ),
  delete: defineProtobufRoute(
    "/api/knowledge/lookups/delete",
    LookupApi.DeleteLookupRequest,
    LookupApi.DeleteLookupResponse,
    { maximumResponseBytes: MAXIMUM_LOOKUP_MANAGEMENT_RESPONSE_BYTES },
  ),
  preview: defineProtobufRoute(
    "/api/knowledge/lookups/preview",
    LookupApi.PreviewLookupRequest,
    LookupApi.PreviewLookupResponse,
    { maximumResponseBytes: MAXIMUM_LOOKUP_MANAGEMENT_RESPONSE_BYTES },
  ),
} as const;

export const collectorRoutes = {
  list: defineProtobufRoute(
    "/api/collectors/list",
    CollectorAdminApi.ListCollectorsRequest,
    CollectorAdminApi.ListCollectorsResponse,
  ),
  get: defineProtobufRoute(
    "/api/collectors/get",
    CollectorAdminApi.GetCollectorRequest,
    CollectorAdminApi.GetCollectorResponse,
  ),
  update: defineProtobufRoute(
    "/api/collectors/update",
    CollectorAdminApi.UpdateCollectorRequest,
    CollectorAdminApi.UpdateCollectorResponse,
  ),
  setState: defineProtobufRoute(
    "/api/collectors/state/set",
    CollectorAdminApi.SetCollectorEnabledRequest,
    CollectorAdminApi.SetCollectorEnabledResponse,
  ),
} as const;

export const auditEventRoutes = {
  list: defineProtobufRoute(
    "/api/audit/events/list",
    AuditApi.ListAuditEventsRequest,
    AuditApi.ListAuditEventsResponse,
  ),
} as const;

export const searchAttemptAuditRoutes = {
  list: defineProtobufRoute(
    "/api/audit/search-attempts/list",
    SearchAttemptAuditApi.ListSearchAttemptAuditEventsRequest,
    SearchAttemptAuditApi.ListSearchAttemptAuditEventsResponse,
  ),
} as const;

export const ingestionTokenRoutes = {
  create: defineProtobufRoute(
    "/api/ingestion-tokens/create",
    CollectorAdminApi.CreateIngestionTokenRequest,
    CollectorAdminApi.CreateIngestionTokenResponse,
  ),
  get: defineProtobufRoute(
    "/api/ingestion-tokens/get",
    CollectorAdminApi.GetIngestionTokenRequest,
    CollectorAdminApi.GetIngestionTokenResponse,
  ),
  list: defineProtobufRoute(
    "/api/ingestion-tokens/list",
    CollectorAdminApi.ListIngestionTokensRequest,
    CollectorAdminApi.ListIngestionTokensResponse,
  ),
  update: defineProtobufRoute(
    "/api/ingestion-tokens/update",
    CollectorAdminApi.UpdateIngestionTokenRequest,
    CollectorAdminApi.UpdateIngestionTokenResponse,
  ),
  setState: defineProtobufRoute(
    "/api/ingestion-tokens/state/set",
    CollectorAdminApi.SetIngestionTokenEnabledRequest,
    CollectorAdminApi.SetIngestionTokenEnabledResponse,
  ),
  revoke: defineProtobufRoute(
    "/api/ingestion-tokens/revoke",
    CollectorAdminApi.RevokeIngestionTokenRequest,
    CollectorAdminApi.RevokeIngestionTokenResponse,
  ),
} as const;

export const hecOperationsRoutes = {
  get: defineProtobufRoute(
    "/api/hec/operations/get",
    HecAdminApi.GetHECOperationalSnapshotRequest,
    HecAdminApi.GetHECOperationalSnapshotResponse,
  ),
} as const;

export const searchRoutes = {
  validate: defineProtobufRoute(
    "/api/search/validate",
    SearchApi.ValidateSearchRequest,
    SearchApi.ValidateSearchResponse,
  ),
  suggestions: defineProtobufRoute(
    "/api/search/suggestions",
    SearchApi.GetSearchSuggestionsRequest,
    SearchApi.GetSearchSuggestionsResponse,
  ),
  create: defineProtobufRoute(
    "/api/search/jobs/create",
    SearchApi.CreateSearchJobRequest,
    SearchApi.CreateSearchJobResponse,
  ),
  get: defineProtobufRoute(
    "/api/search/jobs/get",
    SearchApi.GetSearchJobRequest,
    SearchApi.GetSearchJobResponse,
  ),
  list: defineProtobufRoute(
    "/api/search/jobs/list",
    SearchApi.ListSearchJobsRequest,
    SearchApi.ListSearchJobsResponse,
  ),
  results: defineProtobufRoute(
    "/api/search/jobs/results",
    SearchApi.GetSearchResultsRequest,
    SearchApi.GetSearchResultsResponse,
  ),
  fields: defineProtobufRoute(
    "/api/search/jobs/fields/list",
    SearchApi.ListSearchFieldsRequest,
    SearchApi.ListSearchFieldsResponse,
  ),
  fieldSummary: defineProtobufRoute(
    "/api/search/jobs/field-summary",
    SearchApi.GetSearchFieldSummaryRequest,
    SearchApi.GetSearchFieldSummaryResponse,
  ),
  timeline: defineProtobufRoute(
    "/api/search/jobs/timeline",
    SearchApi.GetSearchTimelineRequest,
    SearchApi.GetSearchTimelineResponse,
  ),
  cancel: defineProtobufRoute(
    "/api/search/jobs/cancel",
    SearchApi.CancelSearchJobRequest,
    SearchApi.CancelSearchJobResponse,
  ),
  getSettings: defineProtobufRoute(
    "/api/search/jobs/settings/get",
    SearchApi.GetSearchJobSettingsRequest,
    SearchApi.GetSearchJobSettingsResponse,
  ),
  updateSettings: defineProtobufRoute(
    "/api/search/jobs/settings/update",
    SearchApi.UpdateSearchJobSettingsRequest,
    SearchApi.UpdateSearchJobSettingsResponse,
  ),
  share: defineProtobufRoute(
    "/api/search/jobs/share",
    SearchApi.ShareSearchJobRequest,
    SearchApi.ShareSearchJobResponse,
  ),
  inspect: defineProtobufRoute(
    "/api/search/jobs/inspect",
    SearchInspectionApi.InspectSearchJobRequest,
    SearchInspectionApi.InspectSearchJobResponse,
    { maximumResponseBytes: MAXIMUM_SEARCH_INSPECTION_RESPONSE_BYTES },
  ),
} as const;

export const savedSearchRoutes = {
  create: defineProtobufRoute(
    "/api/saved-searches/create",
    SavedSearchApi.CreateSavedSearchRequest,
    SavedSearchApi.CreateSavedSearchResponse,
  ),
  get: defineProtobufRoute(
    "/api/saved-searches/get",
    SavedSearchApi.GetSavedSearchRequest,
    SavedSearchApi.GetSavedSearchResponse,
  ),
  list: defineProtobufRoute(
    "/api/saved-searches/list",
    SavedSearchApi.ListSavedSearchesRequest,
    SavedSearchApi.ListSavedSearchesResponse,
  ),
  update: defineProtobufRoute(
    "/api/saved-searches/update",
    SavedSearchApi.UpdateSavedSearchRequest,
    SavedSearchApi.UpdateSavedSearchResponse,
  ),
  duplicate: defineProtobufRoute(
    "/api/saved-searches/duplicate",
    SavedSearchApi.DuplicateSavedSearchRequest,
    SavedSearchApi.DuplicateSavedSearchResponse,
  ),
  delete: defineProtobufRoute(
    "/api/saved-searches/delete",
    SavedSearchApi.DeleteSavedSearchRequest,
    SavedSearchApi.DeleteSavedSearchResponse,
  ),
  setSchedule: defineProtobufRoute(
    "/api/saved-searches/schedule/set",
    SavedSearchApi.SetSavedSearchScheduleRequest,
    SavedSearchApi.SetSavedSearchScheduleResponse,
  ),
  run: defineProtobufRoute(
    "/api/saved-searches/run",
    SavedSearchApi.RunSavedSearchRequest,
    SavedSearchApi.RunSavedSearchResponse,
  ),
  listRuns: defineProtobufRoute(
    "/api/saved-searches/runs/list",
    SavedSearchApi.ListScheduledSearchRunsRequest,
    SavedSearchApi.ListScheduledSearchRunsResponse,
  ),
} as const;

export const alertRoutes = {
  create: defineProtobufRoute("/api/alerts/create", AlertApi.CreateAlertRequest, AlertApi.CreateAlertResponse),
  get: defineProtobufRoute("/api/alerts/get", AlertApi.GetAlertRequest, AlertApi.GetAlertResponse),
  list: defineProtobufRoute("/api/alerts/list", AlertApi.ListAlertsRequest, AlertApi.ListAlertsResponse),
  update: defineProtobufRoute("/api/alerts/update", AlertApi.UpdateAlertRequest, AlertApi.UpdateAlertResponse),
  setState: defineProtobufRoute("/api/alerts/state/set", AlertApi.SetAlertEnabledRequest, AlertApi.SetAlertEnabledResponse),
  delete: defineProtobufRoute("/api/alerts/delete", AlertApi.DeleteAlertRequest, AlertApi.DeleteAlertResponse),
  run: defineProtobufRoute("/api/alerts/run", AlertApi.RunAlertRequest, AlertApi.RunAlertResponse),
  testWebhook: defineProtobufRoute("/api/alerts/webhook/test", AlertApi.TestAlertWebhookRequest, AlertApi.TestAlertWebhookResponse),
  rotateSecret: defineProtobufRoute("/api/alerts/secret/rotate", AlertApi.RotateAlertSecretRequest, AlertApi.RotateAlertSecretResponse),
  listRuns: defineProtobufRoute("/api/alerts/runs/list", AlertApi.ListAlertRunsRequest, AlertApi.ListAlertRunsResponse),
} as const;

export const scheduleRoutes = {
  validate: defineProtobufRoute(
    "/api/schedules/validate",
    ScheduleApi.ValidateScheduleRequest,
    ScheduleApi.ValidateScheduleResponse,
  ),
} as const;

export const dashboardRoutes = {
  create: defineProtobufRoute(
    "/api/dashboards/create",
    DashboardApi.CreateDashboardRequest,
    DashboardApi.CreateDashboardResponse,
  ),
  get: defineProtobufRoute(
    "/api/dashboards/get",
    DashboardApi.GetDashboardRequest,
    DashboardApi.GetDashboardResponse,
  ),
  list: defineProtobufRoute(
    "/api/dashboards/list",
    DashboardApi.ListDashboardsRequest,
    DashboardApi.ListDashboardsResponse,
    { maximumResponseBytes: MAXIMUM_DASHBOARD_LIST_RESPONSE_BYTES },
  ),
  update: defineProtobufRoute(
    "/api/dashboards/update",
    DashboardApi.UpdateDashboardRequest,
    DashboardApi.UpdateDashboardResponse,
  ),
  delete: defineProtobufRoute(
    "/api/dashboards/delete",
    DashboardApi.DeleteDashboardRequest,
    DashboardApi.DeleteDashboardResponse,
  ),
  runPanel: defineProtobufRoute(
    "/api/dashboards/panels/run",
    DashboardApi.RunDashboardPanelRequest,
    DashboardApi.RunDashboardPanelResponse,
  ),
} as const;

export const historyRoutes = {
  get: defineProtobufRoute(
    "/api/search/history/get",
    HistoryApi.GetSearchHistoryEntryRequest,
    HistoryApi.GetSearchHistoryEntryResponse,
  ),
  list: defineProtobufRoute(
    "/api/search/history/list",
    HistoryApi.ListSearchHistoryRequest,
    HistoryApi.ListSearchHistoryResponse,
  ),
  delete: defineProtobufRoute(
    "/api/search/history/delete",
    HistoryApi.DeleteSearchHistoryEntryRequest,
    HistoryApi.DeleteSearchHistoryEntryResponse,
  ),
  clear: defineProtobufRoute(
    "/api/search/history/clear",
    HistoryApi.ClearSearchHistoryRequest,
    HistoryApi.ClearSearchHistoryResponse,
  ),
} as const;

export const exportRoutes = {
  create: defineProtobufRoute(
    "/api/search/exports/create",
    ExportApi.CreateExportJobRequest,
    ExportApi.CreateExportJobResponse,
  ),
  get: defineProtobufRoute(
    "/api/search/exports/get",
    ExportApi.GetExportJobRequest,
    ExportApi.GetExportJobResponse,
  ),
  list: defineProtobufRoute(
    "/api/search/exports/list",
    ExportApi.ListExportJobsRequest,
    ExportApi.ListExportJobsResponse,
  ),
  cancel: defineProtobufRoute(
    "/api/search/exports/cancel",
    ExportApi.CancelExportJobRequest,
    ExportApi.CancelExportJobResponse,
  ),
} as const;

export const openSplunkRoutes = {
  alerts: alertRoutes,
  system: systemRoutes,
  serverSettings: serverSettingsRoutes,
  apps: appRoutes,
  collectors: collectorRoutes,
  auditEvents: auditEventRoutes,
  searchAttemptAudit: searchAttemptAuditRoutes,
  indexes: indexRoutes,
  knowledge: knowledgeRoutes,
  lookups: lookupRoutes,
  ingestionTokens: ingestionTokenRoutes,
  hec: hecOperationsRoutes,
  search: searchRoutes,
  savedSearches: savedSearchRoutes,
  schedules: scheduleRoutes,
  dashboards: dashboardRoutes,
  history: historyRoutes,
  exports: exportRoutes,
} as const;
