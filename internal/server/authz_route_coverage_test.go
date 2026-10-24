package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/authz"
)

// everyBrowserAPIRouteFeature enables every optional route family. It
// reflects over the struct so a newly added gate is enabled without editing
// this test, and fails if a gate is expressed as anything but a bool.
func everyBrowserAPIRouteFeature(t *testing.T) browserAPIRouteFeatures {
	t.Helper()
	var features browserAPIRouteFeatures
	value := reflect.ValueOf(&features).Elem()
	for index := range value.NumField() {
		field := value.Type().Field(index)
		if field.Type.Kind() != reflect.Bool {
			t.Fatalf("browserAPIRouteFeatures.%s is %s; every route gate must be a bool", field.Name, field.Type)
		}
		reflect.NewAt(field.Type, value.Field(index).Addr().UnsafePointer()).Elem().SetBool(true)
	}
	return features
}

func sortedRoutePaths(routes map[string]string) []string {
	paths := make([]string, 0, len(routes))
	for path := range routes {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

func TestAuthzRouteTableCoversEveryRegisteredBrowserAPIRoute(t *testing.T) {
	t.Parallel()

	routes, _ := browserAPIRoutes(everyBrowserAPIRouteFeature(t))
	for _, path := range sortedRoutePaths(routes) {
		requirement, ok := authz.RouteRequirement(path)
		if !ok {
			t.Errorf("registered route %s %s has no authz table entry", routes[path], path)
			continue
		}
		if requirement.Method != routes[path] {
			t.Errorf("authz table method for %s = %s, server registers %s", path, requirement.Method, routes[path])
		}
	}
	for _, path := range authz.Routes() {
		if _, ok := routes[path]; !ok {
			t.Errorf("authz table names %s, which the server never registers", path)
		}
	}
}

func TestAuthzRouteTableKeepsAdministratorRoutesFromMembers(t *testing.T) {
	t.Parallel()

	routes, administratorRoutes := browserAPIRoutes(everyBrowserAPIRouteFeature(t))
	member := authz.CapabilitiesFor(authz.OrgRoleMember, false)
	for path := range administratorRoutes {
		if authz.Allowed(member, routes[path], path) {
			t.Errorf("administrator route %s is granted to members", path)
		}
	}
	for _, path := range []string{
		knowledgeObjectsCreatePath,
		knowledgeObjectsGetPath,
		knowledgeObjectsListPath,
		knowledgeObjectsDependenciesPath,
		knowledgeObjectsDependentsPath,
		knowledgeObjectsValidatePath,
		knowledgeObjectsPreviewPath,
		knowledgeObjectsUpdatePath,
		knowledgeObjectsSetStatePath,
		knowledgeObjectsDeletePath,
		knowledgeObjectsQuarantinePreparePath,
		knowledgeObjectsQuarantinePath,
	} {
		if authz.Allowed(member, routes[path], path) {
			t.Errorf("administrator-authenticated knowledge route %s is granted to members", path)
		}
	}
	owner := authz.CapabilitiesFor(authz.OrgRoleOwner, false)
	for path := range routes {
		if !strings.HasPrefix(path, "/api/server/") {
			continue
		}
		requirement, _ := authz.RouteRequirement(path)
		if requirement.Capability != authz.CapabilityServerAdmin || authz.Allowed(owner, routes[path], path) {
			t.Errorf("deployment route %s must need server.admin, got %q", path, requirement.Capability)
		}
	}
}

func TestBrowserAPIRouteFeaturesEachGateAddsRoutes(t *testing.T) {
	t.Parallel()

	base, _ := browserAPIRoutes(browserAPIRouteFeatures{})
	all := everyBrowserAPIRouteFeature(t)
	value := reflect.ValueOf(all)
	for index := range value.NumField() {
		name := value.Type().Field(index).Name
		var single browserAPIRouteFeatures
		singleValue := reflect.ValueOf(&single).Elem()
		reflect.NewAt(singleValue.Field(index).Type(), singleValue.Field(index).Addr().UnsafePointer()).Elem().SetBool(true)
		routes, _ := browserAPIRoutes(single)
		if len(routes) <= len(base) {
			t.Errorf("feature %s adds no browser API route", name)
		}
		for path, method := range base {
			if routes[path] != method {
				t.Errorf("feature %s dropped core route %s", name, path)
			}
		}
	}
}

// TestNewHandlerServesOnlyAuthzTableRoutes drives the real handler: each
// table route is either absent (404) or registered with the table's method,
// and a minimal handler still serves the always-on core routes.
func TestNewHandlerServesOnlyAuthzTableRoutes(t *testing.T) {
	t.Parallel()

	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{},
		Indexes:    fakeIndexCatalog{},
		WebUI:      testUI(),
	})
	core, _ := browserAPIRoutes(browserAPIRouteFeatures{})
	registered := 0
	for _, path := range authz.Routes() {
		requirement, _ := authz.RouteRequirement(path)
		wrongMethod := http.MethodGet
		if requirement.Method == http.MethodGet {
			wrongMethod = http.MethodPost
		}
		request := httptest.NewRequestWithContext(context.Background(), wrongMethod, path, nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		switch response.Code {
		case http.StatusNotFound:
			if _, isCore := core[path]; isCore {
				t.Errorf("core route %s is not served", path)
			}
		case http.StatusMethodNotAllowed:
			registered++
			if allow := response.Header().Get("Allow"); allow != requirement.Method {
				t.Errorf("%s Allow = %q, authz table says %s", path, allow, requirement.Method)
			}
		default:
			t.Errorf("%s %s = %d, want 404 or 405", wrongMethod, path, response.Code)
		}
	}
	if registered < len(core) {
		t.Fatalf("real handler registered %d table routes, fewer than the %d core routes", registered, len(core))
	}
	unknown := httptest.NewRecorder()
	handler.ServeHTTP(unknown, httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/api/not-a-route", nil))
	if unknown.Code != http.StatusNotFound {
		t.Fatalf("unknown API route status = %d", unknown.Code)
	}
}
