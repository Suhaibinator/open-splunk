import * as ExportApi from "@/gen/ts/open_splunk/v1/export_api";
import * as HistoryApi from "@/gen/ts/open_splunk/v1/history_api";
import * as SavedSearchApi from "@/gen/ts/open_splunk/v1/saved_search_api";
import * as SearchApi from "@/gen/ts/open_splunk/v1/search_api";
import * as SystemApi from "@/gen/ts/open_splunk/v1/system_api";

import { defineProtobufRoute, type ProtobufRoute } from "./protobuf-transport";

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

export const searchRoutes = {
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
  search: searchRoutes,
  savedSearches: savedSearchRoutes,
  history: historyRoutes,
  exports: exportRoutes,
} as const;
