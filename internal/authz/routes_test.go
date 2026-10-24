package authz

import (
	"net/http"
	"strings"
	"testing"
)

func TestRouteTableEntriesAreWellFormed(t *testing.T) {
	t.Parallel()

	routes := Routes()
	if len(routes) == 0 {
		t.Fatal("route table is empty")
	}
	public := 0
	for _, path := range routes {
		requirement, ok := RouteRequirement(path)
		if !ok {
			t.Fatalf("Routes() listed %q but RouteRequirement rejected it", path)
		}
		if !strings.HasPrefix(path, "/api/") || strings.HasSuffix(path, "/") {
			t.Errorf("%q is not an exact /api route path", path)
		}
		if requirement.Method != http.MethodPost && requirement.Method != http.MethodGet {
			t.Errorf("%q method = %q", path, requirement.Method)
		}
		switch {
		case requirement.Public:
			public++
			if requirement.Capability != "" {
				t.Errorf("public route %q also names capability %q", path, requirement.Capability)
			}
		case !requirement.Capability.Valid():
			t.Errorf("%q names non-canonical capability %q", path, requirement.Capability)
		}
	}
	if public != 1 {
		t.Errorf("public routes = %d, want only the system bootstrap", public)
	}
	if requirement, _ := RouteRequirement("/api/system/bootstrap"); !requirement.Public {
		t.Error("system bootstrap is not public")
	}
}

func TestUnknownRoutesAreDenied(t *testing.T) {
	t.Parallel()

	everything := CapabilitiesFor(OrgRoleOwner, true)
	for _, path := range []string{
		"",
		"/",
		"/api",
		"/api/",
		"/api/indexes",
		"/api/indexes/list/",
		"/API/indexes/list",
		"/api/search/jobs/create/../../indexes/create",
		"/api/admin/everything",
		"/healthz",
		"/readyz",
	} {
		if _, ok := RouteRequirement(path); ok {
			t.Errorf("RouteRequirement(%q) found an entry", path)
		}
		if Allowed(everything, http.MethodPost, path) || Allowed(everything, http.MethodGet, path) {
			t.Errorf("Allowed(site admin, %q) = true for an unknown route", path)
		}
	}
}

func TestAllowedRequiresTheRouteMethod(t *testing.T) {
	t.Parallel()

	everything := CapabilitiesFor(OrgRoleOwner, true)
	if Allowed(everything, http.MethodGet, "/api/search/jobs/create") {
		t.Error("GET on a POST route was allowed")
	}
	if Allowed(everything, http.MethodPost, "/api/search/ws") {
		t.Error("POST on the GET websocket route was allowed")
	}
	if Allowed(CapabilitySet{}, http.MethodGet, "/api/system/bootstrap") {
		t.Error("GET on the POST bootstrap route was allowed")
	}
}

func TestPublicBootstrapNeedsNoCapability(t *testing.T) {
	t.Parallel()

	if !Allowed(CapabilitySet{}, http.MethodPost, "/api/system/bootstrap") {
		t.Fatal("bootstrap denied to an anonymous caller")
	}
}

func TestEveryRouteIsReachableBySiteAdmin(t *testing.T) {
	t.Parallel()

	site := CapabilitiesFor(OrgRoleUnspecified, true)
	for _, path := range Routes() {
		requirement, _ := RouteRequirement(path)
		if !Allowed(site, requirement.Method, path) {
			t.Errorf("site admin denied %s %s", requirement.Method, path)
		}
	}
}

func TestRoleRouteMatrix(t *testing.T) {
	t.Parallel()

	// Spot checks per family, spelled independently of the table.
	cases := []struct {
		method string
		path   string
		// lowest role allowed; 0 means site administrators only.
		minimum OrgRole
	}{
		{http.MethodPost, "/api/search/jobs/create", OrgRoleMember},
		{http.MethodPost, "/api/search/jobs/results", OrgRoleMember},
		{http.MethodGet, "/api/search/ws", OrgRoleMember},
		{http.MethodPost, "/api/search/history/clear", OrgRoleMember},
		{http.MethodGet, "/api/search/exports/download", OrgRoleMember},
		{http.MethodPost, "/api/saved-searches/create", OrgRoleMember},
		{http.MethodPost, "/api/dashboards/panels/run", OrgRoleMember},
		{http.MethodPost, "/api/saved-searches/schedule/set", OrgRoleModerator},
		{http.MethodPost, "/api/alerts/create", OrgRoleModerator},
		{http.MethodPost, "/api/knowledge/objects/update", OrgRoleModerator},
		{http.MethodPost, "/api/knowledge/lookups/replace", OrgRoleModerator},
		{http.MethodPost, "/api/knowledge/objects/quarantine", OrgRoleAdmin},
		{http.MethodPost, "/api/indexes/create", OrgRoleAdmin},
		{http.MethodPost, "/api/search/jobs/inspect", OrgRoleAdmin},
		{http.MethodPost, "/api/ingestion-tokens/revoke", OrgRoleAdmin},
		{http.MethodPost, "/api/collectors/update", OrgRoleAdmin},
		{http.MethodPost, "/api/hec/operations/get", OrgRoleAdmin},
		{http.MethodPost, "/api/apps/delete", OrgRoleAdmin},
		{http.MethodPost, "/api/audit/events/list", OrgRoleAdmin},
		{http.MethodPost, "/api/server/settings/update", OrgRoleUnspecified},
		{http.MethodPost, "/api/server/appearance/update", OrgRoleUnspecified},
	}
	for _, testCase := range cases {
		for _, role := range []OrgRole{OrgRoleUnspecified, OrgRoleMember, OrgRoleModerator, OrgRoleAdmin, OrgRoleOwner} {
			want := testCase.minimum != OrgRoleUnspecified && role >= testCase.minimum
			if got := Allowed(CapabilitiesFor(role, false), testCase.method, testCase.path); got != want {
				t.Errorf("Allowed(%s, %s %s) = %t, want %t", role, testCase.method, testCase.path, got, want)
			}
		}
		if !Allowed(CapabilitiesFor(OrgRoleUnspecified, true), testCase.method, testCase.path) {
			t.Errorf("site admin denied %s %s", testCase.method, testCase.path)
		}
	}
}
