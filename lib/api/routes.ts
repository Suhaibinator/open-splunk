import * as ExportApi from "@/gen/ts/open_splunk/v1/export_api";
import * as HistoryApi from "@/gen/ts/open_splunk/v1/history_api";
import * as HecAdminApi from "@/gen/ts/open_splunk/v1/hec_admin_api";
import * as IndexApi from "@/gen/ts/open_splunk/v1/index_api";
import * as AppApi from "@/gen/ts/open_splunk/v1/app_api";
import * as AuditApi from "@/gen/ts/open_splunk/v1/audit_api";
import * as CollectorAdminApi from "@/gen/ts/open_splunk/v1/collector_admin_api";
import * as KnowledgeApi from "@/gen/ts/open_splunk/v1/knowledge_api";
import * as SavedSearchApi from "@/gen/ts/open_splunk/v1/saved_search_api";
import * as SearchApi from "@/gen/ts/open_splunk/v1/search_api";
import * as SearchAttemptAuditApi from "@/gen/ts/open_splunk/v1/search_attempt_audit_api";
import * as SearchInspectionApi from "@/gen/ts/open_splunk/v1/search_inspection_api";
import * as SystemApi from "@/gen/ts/open_splunk/v1/system_api";

import { defineProtobufRoute, type ProtobufRoute } from "./protobuf-transport";

export const MAXIMUM_KNOWLEDGE_MANAGEMENT_RESPONSE_BYTES = 8 << 20;
export const MAXIMUM_KNOWLEDGE_GRAPH_RESPONSE_BYTES = 128 << 10;
export const MAXIMUM_SEARCH_INSPECTION_RESPONSE_BYTES = 8 << 20;

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
    "/api/v1/system/bootstrap",
    SystemApi.GetSystemBootstrapRequest,
    SystemApi.GetSystemBootstrapResponse,
  ),
} as const;

export const indexRoutes = {
  create: defineProtobufRoute(
    "/api/v1/indexes/create",
    IndexApi.CreateIndexRequest,
    IndexApi.CreateIndexResponse,
  ),
  get: defineProtobufRoute(
    "/api/v1/indexes/get",
    IndexApi.GetIndexRequest,
    IndexApi.GetIndexResponse,
  ),
  list: defineProtobufRoute(
    "/api/v1/indexes/list",
    IndexApi.ListIndexesRequest,
    IndexApi.ListIndexesResponse,
  ),
  fields: defineProtobufRoute(
    "/api/v1/indexes/fields/list",
    IndexApi.ListIndexFieldsRequest,
    IndexApi.ListIndexFieldsResponse,
  ),
  update: defineProtobufRoute(
    "/api/v1/indexes/update",
    IndexApi.UpdateIndexRequest,
    IndexApi.UpdateIndexResponse,
  ),
  setState: defineProtobufRoute(
    "/api/v1/indexes/state/set",
    IndexApi.SetIndexStateRequest,
    IndexApi.SetIndexStateResponse,
  ),
  delete: defineProtobufRoute(
    "/api/v1/indexes/delete",
    IndexApi.DeleteIndexRequest,
    IndexApi.DeleteIndexResponse,
  ),
  stats: defineProtobufRoute(
    "/api/v1/indexes/stats/get",
    IndexApi.GetIndexStatsRequest,
    IndexApi.GetIndexStatsResponse,
  ),
} as const;

export const appRoutes = {
  create: defineProtobufRoute(
    "/api/v1/apps/create",
    AppApi.CreateAppRequest,
    AppApi.CreateAppResponse,
  ),
  get: defineProtobufRoute(
    "/api/v1/apps/get",
    AppApi.GetAppRequest,
    AppApi.GetAppResponse,
  ),
  list: defineProtobufRoute(
    "/api/v1/apps/list",
    AppApi.ListAppsRequest,
    AppApi.ListAppsResponse,
  ),
  update: defineProtobufRoute(
    "/api/v1/apps/update",
    AppApi.UpdateAppRequest,
    AppApi.UpdateAppResponse,
  ),
  setState: defineProtobufRoute(
    "/api/v1/apps/state/set",
    AppApi.SetAppStateRequest,
    AppApi.SetAppStateResponse,
  ),
  delete: defineProtobufRoute(
    "/api/v1/apps/delete",
    AppApi.DeleteAppRequest,
    AppApi.DeleteAppResponse,
  ),
} as const;

export const knowledgeRoutes = {
  create: defineProtobufRoute(
    "/api/v1/knowledge/objects/create",
    KnowledgeApi.CreateKnowledgeObjectRequest,
    KnowledgeApi.CreateKnowledgeObjectResponse,
  ),
  get: defineProtobufRoute(
    "/api/v1/knowledge/objects/get",
    KnowledgeApi.GetKnowledgeObjectRequest,
    KnowledgeApi.GetKnowledgeObjectResponse,
    { maximumResponseBytes: MAXIMUM_KNOWLEDGE_MANAGEMENT_RESPONSE_BYTES },
  ),
  list: defineProtobufRoute(
    "/api/v1/knowledge/objects/list",
    KnowledgeApi.ListKnowledgeObjectsRequest,
    KnowledgeApi.ListKnowledgeObjectsResponse,
    { maximumResponseBytes: MAXIMUM_KNOWLEDGE_MANAGEMENT_RESPONSE_BYTES },
  ),
  dependencies: defineProtobufRoute(
    "/api/v1/knowledge/objects/dependencies",
    KnowledgeApi.ListKnowledgeObjectDependenciesRequest,
    KnowledgeApi.ListKnowledgeObjectDependenciesResponse,
    { maximumResponseBytes: MAXIMUM_KNOWLEDGE_GRAPH_RESPONSE_BYTES },
  ),
  dependents: defineProtobufRoute(
    "/api/v1/knowledge/objects/dependents",
    KnowledgeApi.ListKnowledgeObjectDependentsRequest,
    KnowledgeApi.ListKnowledgeObjectDependentsResponse,
    { maximumResponseBytes: MAXIMUM_KNOWLEDGE_GRAPH_RESPONSE_BYTES },
  ),
  validate: defineProtobufRoute(
    "/api/v1/knowledge/objects/validate",
    KnowledgeApi.ValidateKnowledgeObjectRequest,
    KnowledgeApi.ValidateKnowledgeObjectResponse,
    { maximumResponseBytes: MAXIMUM_KNOWLEDGE_MANAGEMENT_RESPONSE_BYTES },
  ),
  preview: defineProtobufRoute(
    "/api/v1/knowledge/objects/preview",
    KnowledgeApi.PreviewKnowledgeObjectRequest,
    strictPreviewKnowledgeObjectResponse,
    { maximumResponseBytes: MAXIMUM_KNOWLEDGE_MANAGEMENT_RESPONSE_BYTES },
  ),
  update: defineProtobufRoute(
    "/api/v1/knowledge/objects/update",
    KnowledgeApi.UpdateKnowledgeObjectRequest,
    KnowledgeApi.UpdateKnowledgeObjectResponse,
  ),
  setState: defineProtobufRoute(
    "/api/v1/knowledge/objects/set-state",
    KnowledgeApi.SetKnowledgeObjectStateRequest,
    KnowledgeApi.SetKnowledgeObjectStateResponse,
  ),
  delete: defineProtobufRoute(
    "/api/v1/knowledge/objects/delete",
    KnowledgeApi.DeleteKnowledgeObjectRequest,
    KnowledgeApi.DeleteKnowledgeObjectResponse,
  ),
} as const;

export const collectorRoutes = {
  list: defineProtobufRoute(
    "/api/v1/collectors/list",
    CollectorAdminApi.ListCollectorsRequest,
    CollectorAdminApi.ListCollectorsResponse,
  ),
  get: defineProtobufRoute(
    "/api/v1/collectors/get",
    CollectorAdminApi.GetCollectorRequest,
    CollectorAdminApi.GetCollectorResponse,
  ),
  update: defineProtobufRoute(
    "/api/v1/collectors/update",
    CollectorAdminApi.UpdateCollectorRequest,
    CollectorAdminApi.UpdateCollectorResponse,
  ),
  setState: defineProtobufRoute(
    "/api/v1/collectors/state/set",
    CollectorAdminApi.SetCollectorEnabledRequest,
    CollectorAdminApi.SetCollectorEnabledResponse,
  ),
} as const;

export const auditEventRoutes = {
  list: defineProtobufRoute(
    "/api/v1/audit/events/list",
    AuditApi.ListAuditEventsRequest,
    AuditApi.ListAuditEventsResponse,
  ),
} as const;

export const searchAttemptAuditRoutes = {
  list: defineProtobufRoute(
    "/api/v1/audit/search-attempts/list",
    SearchAttemptAuditApi.ListSearchAttemptAuditEventsRequest,
    SearchAttemptAuditApi.ListSearchAttemptAuditEventsResponse,
  ),
} as const;

export const ingestionTokenRoutes = {
  create: defineProtobufRoute(
    "/api/v1/ingestion-tokens/create",
    CollectorAdminApi.CreateIngestionTokenRequest,
    CollectorAdminApi.CreateIngestionTokenResponse,
  ),
  get: defineProtobufRoute(
    "/api/v1/ingestion-tokens/get",
    CollectorAdminApi.GetIngestionTokenRequest,
    CollectorAdminApi.GetIngestionTokenResponse,
  ),
  list: defineProtobufRoute(
    "/api/v1/ingestion-tokens/list",
    CollectorAdminApi.ListIngestionTokensRequest,
    CollectorAdminApi.ListIngestionTokensResponse,
  ),
  update: defineProtobufRoute(
    "/api/v1/ingestion-tokens/update",
    CollectorAdminApi.UpdateIngestionTokenRequest,
    CollectorAdminApi.UpdateIngestionTokenResponse,
  ),
  setState: defineProtobufRoute(
    "/api/v1/ingestion-tokens/state/set",
    CollectorAdminApi.SetIngestionTokenEnabledRequest,
    CollectorAdminApi.SetIngestionTokenEnabledResponse,
  ),
  revoke: defineProtobufRoute(
    "/api/v1/ingestion-tokens/revoke",
    CollectorAdminApi.RevokeIngestionTokenRequest,
    CollectorAdminApi.RevokeIngestionTokenResponse,
  ),
} as const;

export const hecOperationsRoutes = {
  get: defineProtobufRoute(
    "/api/v1/hec/operations/get",
    HecAdminApi.GetHECOperationalSnapshotRequest,
    HecAdminApi.GetHECOperationalSnapshotResponse,
  ),
} as const;

export const searchRoutes = {
  validate: defineProtobufRoute(
    "/api/v1/search/validate",
    SearchApi.ValidateSearchRequest,
    SearchApi.ValidateSearchResponse,
  ),
  suggestions: defineProtobufRoute(
    "/api/v1/search/suggestions",
    SearchApi.GetSearchSuggestionsRequest,
    SearchApi.GetSearchSuggestionsResponse,
  ),
  create: defineProtobufRoute(
    "/api/v1/search/jobs/create",
    SearchApi.CreateSearchJobRequest,
    SearchApi.CreateSearchJobResponse,
  ),
  get: defineProtobufRoute(
    "/api/v1/search/jobs/get",
    SearchApi.GetSearchJobRequest,
    SearchApi.GetSearchJobResponse,
  ),
  list: defineProtobufRoute(
    "/api/v1/search/jobs/list",
    SearchApi.ListSearchJobsRequest,
    SearchApi.ListSearchJobsResponse,
  ),
  results: defineProtobufRoute(
    "/api/v1/search/jobs/results",
    SearchApi.GetSearchResultsRequest,
    SearchApi.GetSearchResultsResponse,
  ),
  fields: defineProtobufRoute(
    "/api/v1/search/jobs/fields/list",
    SearchApi.ListSearchFieldsRequest,
    SearchApi.ListSearchFieldsResponse,
  ),
  fieldSummary: defineProtobufRoute(
    "/api/v1/search/jobs/field-summary",
    SearchApi.GetSearchFieldSummaryRequest,
    SearchApi.GetSearchFieldSummaryResponse,
  ),
  timeline: defineProtobufRoute(
    "/api/v1/search/jobs/timeline",
    SearchApi.GetSearchTimelineRequest,
    SearchApi.GetSearchTimelineResponse,
  ),
  cancel: defineProtobufRoute(
    "/api/v1/search/jobs/cancel",
    SearchApi.CancelSearchJobRequest,
    SearchApi.CancelSearchJobResponse,
  ),
  inspect: defineProtobufRoute(
    "/api/v1/search/jobs/inspect",
    SearchInspectionApi.InspectSearchJobRequest,
    SearchInspectionApi.InspectSearchJobResponse,
    { maximumResponseBytes: MAXIMUM_SEARCH_INSPECTION_RESPONSE_BYTES },
  ),
} as const;

export const savedSearchRoutes = {
  create: defineProtobufRoute(
    "/api/v1/saved-searches/create",
    SavedSearchApi.CreateSavedSearchRequest,
    SavedSearchApi.CreateSavedSearchResponse,
  ),
  get: defineProtobufRoute(
    "/api/v1/saved-searches/get",
    SavedSearchApi.GetSavedSearchRequest,
    SavedSearchApi.GetSavedSearchResponse,
  ),
  list: defineProtobufRoute(
    "/api/v1/saved-searches/list",
    SavedSearchApi.ListSavedSearchesRequest,
    SavedSearchApi.ListSavedSearchesResponse,
  ),
  update: defineProtobufRoute(
    "/api/v1/saved-searches/update",
    SavedSearchApi.UpdateSavedSearchRequest,
    SavedSearchApi.UpdateSavedSearchResponse,
  ),
  duplicate: defineProtobufRoute(
    "/api/v1/saved-searches/duplicate",
    SavedSearchApi.DuplicateSavedSearchRequest,
    SavedSearchApi.DuplicateSavedSearchResponse,
  ),
  delete: defineProtobufRoute(
    "/api/v1/saved-searches/delete",
    SavedSearchApi.DeleteSavedSearchRequest,
    SavedSearchApi.DeleteSavedSearchResponse,
  ),
} as const;

export const historyRoutes = {
  get: defineProtobufRoute(
    "/api/v1/search/history/get",
    HistoryApi.GetSearchHistoryEntryRequest,
    HistoryApi.GetSearchHistoryEntryResponse,
  ),
  list: defineProtobufRoute(
    "/api/v1/search/history/list",
    HistoryApi.ListSearchHistoryRequest,
    HistoryApi.ListSearchHistoryResponse,
  ),
  delete: defineProtobufRoute(
    "/api/v1/search/history/delete",
    HistoryApi.DeleteSearchHistoryEntryRequest,
    HistoryApi.DeleteSearchHistoryEntryResponse,
  ),
  clear: defineProtobufRoute(
    "/api/v1/search/history/clear",
    HistoryApi.ClearSearchHistoryRequest,
    HistoryApi.ClearSearchHistoryResponse,
  ),
} as const;

export const exportRoutes = {
  create: defineProtobufRoute(
    "/api/v1/search/exports/create",
    ExportApi.CreateExportJobRequest,
    ExportApi.CreateExportJobResponse,
  ),
  get: defineProtobufRoute(
    "/api/v1/search/exports/get",
    ExportApi.GetExportJobRequest,
    ExportApi.GetExportJobResponse,
  ),
  list: defineProtobufRoute(
    "/api/v1/search/exports/list",
    ExportApi.ListExportJobsRequest,
    ExportApi.ListExportJobsResponse,
  ),
  cancel: defineProtobufRoute(
    "/api/v1/search/exports/cancel",
    ExportApi.CancelExportJobRequest,
    ExportApi.CancelExportJobResponse,
  ),
} as const;

export const openSplunkRoutes = {
  system: systemRoutes,
  apps: appRoutes,
  collectors: collectorRoutes,
  auditEvents: auditEventRoutes,
  searchAttemptAudit: searchAttemptAuditRoutes,
  indexes: indexRoutes,
  knowledge: knowledgeRoutes,
  ingestionTokens: ingestionTokenRoutes,
  hec: hecOperationsRoutes,
  search: searchRoutes,
  savedSearches: savedSearchRoutes,
  history: historyRoutes,
  exports: exportRoutes,
} as const;
