package server

import "net/http"

// browserAPIRouteFeatures records which optional route families a handler
// composes. Every gate that adds a browser API route must be a bool field
// here: the authz route-table coverage test enables every field to enumerate
// the full route set, so a gate expressed any other way would hide its routes
// from that test.
type browserAPIRouteFeatures struct {
	searchPatterns      bool
	searchArtifacts     bool
	scheduledReports    bool
	alerts              bool
	searchHistory       bool
	dashboards          bool
	indexAdmin          bool
	indexStatistics     bool
	indexFields         bool
	ingestionTokens     bool
	hecOperations       bool
	auditEvents         bool
	searchAttemptAudit  bool
	serverSettings      bool
	collectorAdmin      bool
	appAdmin            bool
	knowledgeManagement bool
	knowledgeQuarantine bool
	knowledgePreview    bool
	lookupManagement    bool
	exports             bool
	searchTimelines     bool
	searchInspections   bool
	searchFields        bool
	searchSuggestions   bool
	searchWebSocket     bool
}

func (handler *apiHandler) browserAPIRouteFeatures() browserAPIRouteFeatures {
	_, knowledgeQuarantine := readyKnowledgeQuarantine(handler.knowledgeWriter)
	return browserAPIRouteFeatures{
		searchPatterns:      handler.searchPatterns != nil,
		searchArtifacts:     handler.searchArtifacts != nil,
		scheduledReports:    handler.scheduledReports != nil,
		alerts:              handler.alertsEnabled,
		searchHistory:       handler.searchHistory != nil,
		dashboards:          handler.dashboards != nil,
		indexAdmin:          handler.indexAdmin != nil,
		indexStatistics:     handler.indexStatistics != nil,
		indexFields:         handler.indexFields != nil,
		ingestionTokens:     handler.ingestionTokens != nil,
		hecOperations:       handler.hecOperations != nil,
		auditEvents:         handler.auditEvents != nil,
		searchAttemptAudit:  handler.searchAttemptAuditEvents != nil,
		serverSettings:      handler.serverSettings != nil,
		collectorAdmin:      handler.collectorAdmin != nil,
		appAdmin:            handler.appAdmin != nil,
		knowledgeManagement: handler.knowledgeManagementConfigured(),
		knowledgeQuarantine: knowledgeQuarantine,
		knowledgePreview:    handler.knowledgePreviewConfigured(),
		lookupManagement:    handler.lookupManagementConfigured(),
		exports:             handler.exports != nil,
		searchTimelines:     handler.searchTimelines != nil,
		searchInspections:   handler.searchInspections != nil,
		searchFields:        handler.searchFields != nil,
		searchSuggestions:   handler.searchSuggestions != nil,
		searchWebSocket:     handler.searchWebSocket != nil,
	}
}

// browserAPIRoutes returns the exact browser API route allowlist (path to its
// only method) and the subset that requires an administrator credential.
func browserAPIRoutes(features browserAPIRouteFeatures) (map[string]string, map[string]struct{}) {
	apiRoutes := postAPIRoutes(
		"/api/system/bootstrap",
		"/api/search/validate",
		"/api/search/jobs/create",
		"/api/search/jobs/get",
		searchJobsListPath,
		"/api/search/jobs/results",
		nearbyContextPath,
		"/api/search/jobs/cancel",
		"/api/saved-searches/create",
		"/api/saved-searches/get",
		"/api/saved-searches/list",
		"/api/saved-searches/update",
		"/api/saved-searches/duplicate",
		"/api/saved-searches/delete",
	)
	administratorRoutes := make(map[string]struct{}, 25)
	if features.searchPatterns {
		apiRoutes[apiPathPrefix+searchPatternsListRoute] = http.MethodPost
		apiRoutes[apiPathPrefix+searchPatternMembersRoute] = http.MethodPost
	}
	if features.searchArtifacts {
		for _, path := range []string{
			"/api/search/jobs/settings/get",
			"/api/search/jobs/settings/update",
			"/api/search/jobs/share",
		} {
			apiRoutes[path] = http.MethodPost
		}
	}
	if features.scheduledReports {
		for _, path := range []string{
			"/api/saved-searches/schedule/set",
			"/api/saved-searches/run",
			"/api/saved-searches/runs/list",
		} {
			apiRoutes[path] = http.MethodPost
		}
	}
	if features.scheduledReports || features.alerts {
		apiRoutes["/api/schedules/validate"] = http.MethodPost
	}
	if features.alerts {
		for _, path := range []string{
			"/api/alerts/create",
			"/api/alerts/get",
			"/api/alerts/list",
			"/api/alerts/update",
			"/api/alerts/state/set",
			"/api/alerts/delete",
			"/api/alerts/run",
			"/api/alerts/webhook/test",
			"/api/alerts/secret/rotate",
			"/api/alerts/runs/list",
		} {
			apiRoutes[path] = http.MethodPost
			administratorRoutes[path] = struct{}{}
		}
	}
	if features.searchHistory {
		for _, path := range []string{
			"/api/search/history/get",
			"/api/search/history/list",
			"/api/search/history/delete",
			"/api/search/history/clear",
		} {
			apiRoutes[path] = http.MethodPost
		}
	}
	if features.dashboards {
		for _, path := range []string{
			"/api/dashboards/create",
			"/api/dashboards/get",
			"/api/dashboards/list",
			"/api/dashboards/update",
			"/api/dashboards/delete",
			"/api/dashboards/panels/run",
		} {
			apiRoutes[path] = http.MethodPost
		}
	}
	if features.indexAdmin {
		for _, path := range []string{
			"/api/indexes/create",
			"/api/indexes/get",
			"/api/indexes/list",
			"/api/indexes/update",
			"/api/indexes/state/set",
			"/api/indexes/delete",
		} {
			apiRoutes[path] = http.MethodPost
			administratorRoutes[path] = struct{}{}
		}
	}
	if features.indexStatistics {
		apiRoutes["/api/indexes/stats/get"] = http.MethodPost
		administratorRoutes["/api/indexes/stats/get"] = struct{}{}
	}
	if features.indexFields {
		apiRoutes[indexFieldsListPath] = http.MethodPost
		administratorRoutes[indexFieldsListPath] = struct{}{}
	}
	if features.ingestionTokens {
		for _, path := range []string{
			"/api/ingestion-tokens/create",
			"/api/ingestion-tokens/get",
			"/api/ingestion-tokens/list",
			"/api/ingestion-tokens/update",
			"/api/ingestion-tokens/state/set",
			"/api/ingestion-tokens/revoke",
		} {
			apiRoutes[path] = http.MethodPost
			administratorRoutes[path] = struct{}{}
		}
	}
	if features.hecOperations {
		apiRoutes[hecOperationsPath] = http.MethodPost
		administratorRoutes[hecOperationsPath] = struct{}{}
	}
	if features.auditEvents {
		apiRoutes[auditEventsListPath] = http.MethodPost
		administratorRoutes[auditEventsListPath] = struct{}{}
	}
	if features.searchAttemptAudit {
		apiRoutes[searchAttemptAuditListPath] = http.MethodPost
		administratorRoutes[searchAttemptAuditListPath] = struct{}{}
	}
	if features.serverSettings {
		for _, path := range []string{
			"/api/server/settings/get", "/api/server/settings/update",
			"/api/server/appearance/get", "/api/server/appearance/update",
		} {
			apiRoutes[path] = http.MethodPost
			administratorRoutes[path] = struct{}{}
		}
	}
	if features.collectorAdmin {
		for _, path := range []string{
			"/api/collectors/get",
			"/api/collectors/list",
			"/api/collectors/update",
			"/api/collectors/state/set",
		} {
			apiRoutes[path] = http.MethodPost
			administratorRoutes[path] = struct{}{}
		}
	}
	if features.appAdmin {
		for _, path := range []string{
			"/api/apps/create",
			"/api/apps/get",
			"/api/apps/list",
			"/api/apps/update",
			"/api/apps/state/set",
			"/api/apps/delete",
		} {
			apiRoutes[path] = http.MethodPost
			administratorRoutes[path] = struct{}{}
		}
	}
	if features.knowledgeManagement {
		for _, path := range []string{
			knowledgeObjectsCreatePath,
			knowledgeObjectsGetPath,
			knowledgeObjectsListPath,
			knowledgeObjectsDependenciesPath,
			knowledgeObjectsDependentsPath,
			knowledgeObjectsValidatePath,
			knowledgeObjectsUpdatePath,
			knowledgeObjectsSetStatePath,
			knowledgeObjectsDeletePath,
		} {
			apiRoutes[path] = http.MethodPost
		}
	}
	if features.knowledgeQuarantine {
		for _, path := range []string{
			knowledgeObjectsQuarantinePreparePath,
			knowledgeObjectsQuarantinePath,
		} {
			apiRoutes[path] = http.MethodPost
		}
	}
	if features.knowledgePreview {
		apiRoutes[knowledgeObjectsPreviewPath] = http.MethodPost
	}
	if features.lookupManagement {
		for _, path := range []string{
			lookupCreatePath,
			lookupGetPath,
			lookupListPath,
			lookupReplacePath,
			lookupSetStatePath,
			lookupDeletePath,
			lookupPreviewPath,
		} {
			apiRoutes[path] = http.MethodPost
			administratorRoutes[path] = struct{}{}
		}
	}
	if features.exports {
		for _, path := range []string{
			"/api/search/exports/create",
			"/api/search/exports/get",
			exportJobsListPath,
			"/api/search/exports/cancel",
		} {
			apiRoutes[path] = http.MethodPost
		}
		apiRoutes[exportDownloadPath] = http.MethodGet
	}
	if features.searchTimelines {
		apiRoutes[searchTimelinePath] = http.MethodPost
	}
	if features.searchInspections {
		apiRoutes[searchInspectionPath] = http.MethodPost
		administratorRoutes[searchInspectionPath] = struct{}{}
	}
	if features.searchFields {
		apiRoutes[searchFieldsListPath] = http.MethodPost
		apiRoutes[searchFieldSummaryPath] = http.MethodPost
	}
	if features.searchSuggestions {
		apiRoutes[searchSuggestionsPath] = http.MethodPost
	}
	if features.searchWebSocket {
		apiRoutes[searchWebSocketPath] = http.MethodGet
	}
	return apiRoutes, administratorRoutes
}
