package authz

import (
	"net/http"
	"slices"
)

// Requirement is what one browser API route demands of its caller.
type Requirement struct {
	// Method is the only HTTP method the route serves.
	Method string
	// Public routes are served before sign-in and need no principal. Only the
	// system bootstrap is public: the sign-in page paints from it.
	Public bool
	// Capability is the capability the principal must hold in the active
	// organization. It is empty exactly when Public is true.
	Capability Capability
}

func post(capability Capability) Requirement {
	return Requirement{Method: http.MethodPost, Capability: capability}
}

func get(capability Capability) Requirement {
	return Requirement{Method: http.MethodGet, Capability: capability}
}

// routeRequirements maps every browser API path the server can register to
// its requirement. A path missing here is denied. Optional route families
// are listed whether or not a given process composes them.
var routeRequirements = map[string]Requirement{
	// System.
	"/api/system/bootstrap": {Method: http.MethodPost, Public: true},

	// Search, retained results, history, and exports: the caller's own jobs.
	"/api/search/validate":              post(CapabilitySearch),
	"/api/search/suggestions":           post(CapabilitySearch),
	"/api/search/jobs/create":           post(CapabilitySearch),
	"/api/search/jobs/get":              post(CapabilitySearch),
	"/api/search/jobs/list":             post(CapabilitySearch),
	"/api/search/jobs/results":          post(CapabilitySearch),
	"/api/search/jobs/nearby/prepare":   post(CapabilitySearch),
	"/api/search/jobs/cancel":           post(CapabilitySearch),
	"/api/search/jobs/timeline":         post(CapabilitySearch),
	"/api/search/jobs/fields/list":      post(CapabilitySearch),
	"/api/search/jobs/field-summary":    post(CapabilitySearch),
	"/api/search/jobs/patterns/list":    post(CapabilitySearch),
	"/api/search/jobs/patterns/members": post(CapabilitySearch),
	"/api/search/jobs/settings/get":     post(CapabilitySearch),
	"/api/search/jobs/settings/update":  post(CapabilitySearch),
	"/api/search/jobs/share":            post(CapabilitySearch),
	"/api/search/ws":                    get(CapabilitySearch),
	"/api/search/history/get":           post(CapabilitySearch),
	"/api/search/history/list":          post(CapabilitySearch),
	"/api/search/history/delete":        post(CapabilitySearch),
	"/api/search/history/clear":         post(CapabilitySearch),
	"/api/search/exports/create":        post(CapabilitySearch),
	"/api/search/exports/get":           post(CapabilitySearch),
	"/api/search/exports/list":          post(CapabilitySearch),
	"/api/search/exports/cancel":        post(CapabilitySearch),
	"/api/search/exports/download":      get(CapabilitySearch),

	// Search inspection exposes generated SQL and the physical plan, so it
	// stays with index administration as it was administrator-only before.
	"/api/search/jobs/inspect": post(CapabilityIndexesAdmin),

	// Saved searches the caller owns, including their report run history.
	"/api/saved-searches/create":    post(CapabilityObjectsOwn),
	"/api/saved-searches/get":       post(CapabilityObjectsOwn),
	"/api/saved-searches/list":      post(CapabilityObjectsOwn),
	"/api/saved-searches/update":    post(CapabilityObjectsOwn),
	"/api/saved-searches/duplicate": post(CapabilityObjectsOwn),
	"/api/saved-searches/delete":    post(CapabilityObjectsOwn),
	"/api/saved-searches/runs/list": post(CapabilityObjectsOwn),

	// Scheduled reports.
	"/api/saved-searches/schedule/set": post(CapabilityReportsWrite),
	"/api/saved-searches/run":          post(CapabilityReportsWrite),
	"/api/schedules/validate":          post(CapabilityReportsWrite),

	// Dashboards.
	"/api/dashboards/create":     post(CapabilityObjectsOwn),
	"/api/dashboards/get":        post(CapabilityObjectsOwn),
	"/api/dashboards/list":       post(CapabilityObjectsOwn),
	"/api/dashboards/update":     post(CapabilityObjectsOwn),
	"/api/dashboards/delete":     post(CapabilityObjectsOwn),
	"/api/dashboards/panels/run": post(CapabilityObjectsOwn),

	// Alerts.
	"/api/alerts/create":        post(CapabilityAlertsWrite),
	"/api/alerts/get":           post(CapabilityAlertsWrite),
	"/api/alerts/list":          post(CapabilityAlertsWrite),
	"/api/alerts/update":        post(CapabilityAlertsWrite),
	"/api/alerts/state/set":     post(CapabilityAlertsWrite),
	"/api/alerts/delete":        post(CapabilityAlertsWrite),
	"/api/alerts/run":           post(CapabilityAlertsWrite),
	"/api/alerts/webhook/test":  post(CapabilityAlertsWrite),
	"/api/alerts/secret/rotate": post(CapabilityAlertsWrite),
	"/api/alerts/runs/list":     post(CapabilityAlertsWrite),

	// Knowledge objects; quarantine is destructive and needs knowledge.admin.
	"/api/knowledge/objects/create":             post(CapabilityKnowledgeWrite),
	"/api/knowledge/objects/get":                post(CapabilityKnowledgeWrite),
	"/api/knowledge/objects/list":               post(CapabilityKnowledgeWrite),
	"/api/knowledge/objects/dependencies":       post(CapabilityKnowledgeWrite),
	"/api/knowledge/objects/dependents":         post(CapabilityKnowledgeWrite),
	"/api/knowledge/objects/validate":           post(CapabilityKnowledgeWrite),
	"/api/knowledge/objects/preview":            post(CapabilityKnowledgeWrite),
	"/api/knowledge/objects/update":             post(CapabilityKnowledgeWrite),
	"/api/knowledge/objects/set-state":          post(CapabilityKnowledgeWrite),
	"/api/knowledge/objects/delete":             post(CapabilityKnowledgeWrite),
	"/api/knowledge/objects/quarantine/prepare": post(CapabilityKnowledgeAdmin),
	"/api/knowledge/objects/quarantine":         post(CapabilityKnowledgeAdmin),

	// Lookups.
	"/api/knowledge/lookups/create":    post(CapabilityLookupsWrite),
	"/api/knowledge/lookups/get":       post(CapabilityLookupsWrite),
	"/api/knowledge/lookups/list":      post(CapabilityLookupsWrite),
	"/api/knowledge/lookups/replace":   post(CapabilityLookupsWrite),
	"/api/knowledge/lookups/state/set": post(CapabilityLookupsWrite),
	"/api/knowledge/lookups/delete":    post(CapabilityLookupsWrite),
	"/api/knowledge/lookups/preview":   post(CapabilityLookupsWrite),

	// Indexes.
	"/api/indexes/create":      post(CapabilityIndexesAdmin),
	"/api/indexes/get":         post(CapabilityIndexesAdmin),
	"/api/indexes/list":        post(CapabilityIndexesAdmin),
	"/api/indexes/update":      post(CapabilityIndexesAdmin),
	"/api/indexes/state/set":   post(CapabilityIndexesAdmin),
	"/api/indexes/delete":      post(CapabilityIndexesAdmin),
	"/api/indexes/stats/get":   post(CapabilityIndexesAdmin),
	"/api/indexes/fields/list": post(CapabilityIndexesAdmin),

	// Ingestion tokens.
	"/api/ingestion-tokens/create":    post(CapabilityIngestionAdmin),
	"/api/ingestion-tokens/get":       post(CapabilityIngestionAdmin),
	"/api/ingestion-tokens/list":      post(CapabilityIngestionAdmin),
	"/api/ingestion-tokens/update":    post(CapabilityIngestionAdmin),
	"/api/ingestion-tokens/state/set": post(CapabilityIngestionAdmin),
	"/api/ingestion-tokens/revoke":    post(CapabilityIngestionAdmin),

	// HEC operations.
	"/api/hec/operations/get": post(CapabilityHECAdmin),

	// Collectors.
	"/api/collectors/get":       post(CapabilityCollectorsAdmin),
	"/api/collectors/list":      post(CapabilityCollectorsAdmin),
	"/api/collectors/update":    post(CapabilityCollectorsAdmin),
	"/api/collectors/state/set": post(CapabilityCollectorsAdmin),

	// Apps.
	"/api/apps/create":    post(CapabilityAppsAdmin),
	"/api/apps/get":       post(CapabilityAppsAdmin),
	"/api/apps/list":      post(CapabilityAppsAdmin),
	"/api/apps/update":    post(CapabilityAppsAdmin),
	"/api/apps/state/set": post(CapabilityAppsAdmin),
	"/api/apps/delete":    post(CapabilityAppsAdmin),

	// Audit trails.
	"/api/audit/events/list":          post(CapabilityAuditRead),
	"/api/audit/search-attempts/list": post(CapabilityAuditRead),

	// Deployment settings and appearance: site administrators only.
	"/api/server/settings/get":      post(CapabilityServerAdmin),
	"/api/server/settings/update":   post(CapabilityServerAdmin),
	"/api/server/appearance/get":    post(CapabilityServerAdmin),
	"/api/server/appearance/update": post(CapabilityServerAdmin),
}

// RouteRequirement returns the requirement of the browser API route at path.
// It reports false for any path the table does not name; callers must deny
// such requests.
func RouteRequirement(path string) (Requirement, bool) {
	requirement, ok := routeRequirements[path]
	return requirement, ok
}

// Routes returns every path in the route table in sorted order.
func Routes() []string {
	result := make([]string, 0, len(routeRequirements))
	for path := range routeRequirements {
		result = append(result, path)
	}
	slices.Sort(result)
	return result
}

// Allowed reports whether a principal holding capabilities may call method on
// path. Unknown paths and mismatched methods are denied. Public routes are
// allowed with any set, including the empty one.
func Allowed(capabilities CapabilitySet, method, path string) bool {
	requirement, ok := routeRequirements[path]
	if !ok || requirement.Method != method {
		return false
	}
	if requirement.Public {
		return true
	}
	return capabilities.Has(requirement.Capability)
}
