package server

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/authz"
	"google.golang.org/protobuf/proto"
)

// apiPathAliases returns request targets that differ from path but that a
// lenient router, a URL normalizer, or a percent-decoder could resolve to it.
// Each must stay outside the route the authz table names for path.
func apiPathAliases(path string) []string {
	rest := strings.TrimPrefix(path, apiPathPrefix+"/")
	last := strings.LastIndex(path, "/")
	candidates := []string{
		path + "/",
		path + "//",
		"/" + path,
		apiPathPrefix + "//" + rest,
		apiPathPrefix + "/./" + rest,
		apiPathPrefix + "/x/../" + rest,
		apiPathPrefix + "/%2e/" + rest,
		apiPathPrefix + "/x/%2e%2e/" + rest,
		path[:last] + "%2F" + path[last+1:],
		path[:last] + "%2f" + path[last+1:],
		"/api%2F" + rest,
		"/api%2F" + strings.ReplaceAll(rest, "/", "%2F"),
		apiPathPrefix + "/" + fmt.Sprintf("%%%02X", rest[0]) + rest[1:],
		strings.ToUpper(path),
		apiPathPrefix + "/" + strings.ToUpper(rest[:1]) + rest[1:],
		path + ";",
		path + ";x=1",
		path + "%00",
		path + "%20",
		path + "%2F",
		path + ".json",
		path + "/x",
		path[:last],
	}
	aliases := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		// A shorter route can be another route's parent, such as the
		// quarantine route beneath its prepare step.
		if _, isRoute := authz.RouteRequirement(candidate); !isRoute {
			aliases = append(aliases, candidate)
		}
	}
	return aliases
}

func TestEveryAuthzTableRouteIsItsOwnEscapedForm(t *testing.T) {
	t.Parallel()

	for _, path := range authz.Routes() {
		if escaped := (&url.URL{Path: path}).EscapedPath(); escaped != path {
			t.Errorf("route %s escapes to %s; canonical clients could not reach it", path, escaped)
		}
	}
}

// TestAPIBoundaryAdmitsOnlyExactCanonicalRoutes drives the exact-route
// boundary directly, without ServeMux's cleaning redirects, so every alias of
// every route (including the GET websocket and export download routes) must
// be refused by the boundary itself before authorization or dispatch.
func TestAPIBoundaryAdmitsOnlyExactCanonicalRoutes(t *testing.T) {
	t.Parallel()

	routes, _ := browserAPIRoutes(everyBrowserAPIRouteFeature(t))
	admitted := ""
	boundary := exactAPIRoutes(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		admitted = request.Method + " " + request.URL.Path
	}), routes)
	serve := func(method, target string) int {
		admitted = ""
		request := httptest.NewRequestWithContext(context.Background(), method, target, nil)
		response := httptest.NewRecorder()
		boundary.ServeHTTP(response, request)
		return response.Code
	}

	for _, path := range authz.Routes() {
		requirement, _ := authz.RouteRequirement(path)
		serve(requirement.Method, path)
		if admitted != requirement.Method+" "+path {
			t.Errorf("exact %s %s was not admitted", requirement.Method, path)
		}
		for _, method := range []string{
			http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut,
			http.MethodPatch, http.MethodDelete, http.MethodOptions,
			http.MethodTrace, strings.ToLower(requirement.Method), "PRI",
		} {
			if method == requirement.Method {
				continue
			}
			if status := serve(method, path); admitted != "" || status != http.StatusMethodNotAllowed {
				t.Errorf("%s %s = %d admitted %q, want 405 and no dispatch", method, path, status, admitted)
			}
		}
		for _, alias := range apiPathAliases(path) {
			for _, method := range []string{requirement.Method, http.MethodHead} {
				status := serve(method, alias)
				if admitted != "" {
					t.Errorf("alias %s %s of %s was admitted as %q", method, alias, path, admitted)
				}
				if status != http.StatusNotFound && status != http.StatusMethodNotAllowed {
					t.Errorf("alias %s %s = %d, want 404 or 405", method, alias, status)
				}
			}
		}
	}
}

// TestBrowserHandlerRefusesAdministratorRouteAliases sends every alias of
// every administrator route through the real handler with a valid
// administrator credential. A positive control proves the credential works on
// the exact path; no alias may reach the service, and an escaped alias of the
// API namespace is answered by the API, never by the SPA.
func TestBrowserHandlerRefusesAdministratorRouteAliases(t *testing.T) {
	t.Parallel()

	principal := browserGatePrincipal(t, browserGateTenantID, browserGateOwnerID, auth.BrowserRoleAdministrator)
	authenticator := &recordingBrowserAuthenticator{
		fn: func(context.Context, []byte) (auth.BrowserPrincipal, error) {
			return principal, nil
		},
	}
	handler, indexes, tokens := newBrowserGateHandler(t, authenticator, defaultRouteTimeout)
	listBody, err := proto.Marshal(&opensplunk.ListIndexesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	serve := func(method, target string) *httptest.ResponseRecorder {
		request := httptest.NewRequestWithContext(context.Background(), method, target, bytes.NewReader(listBody))
		request.Header.Set("Content-Type", "application/x-protobuf")
		return serveBrowserGateRequest(handler, request, adminIntegrationBearerToken)
	}

	before := indexes.callCount()
	if control := serve(http.MethodPost, "/api/indexes/list"); control.Code != http.StatusOK || indexes.callCount() != before+1 {
		t.Fatalf("positive control: status = %d, index calls = %d, body = %s", control.Code, indexes.callCount()-before, control.Body.String())
	}

	for _, path := range browserGateAdministratorPaths {
		for _, alias := range apiPathAliases(path) {
			for _, method := range []string{http.MethodPost, http.MethodGet, http.MethodHead} {
				indexCalls, tokenCalls := indexes.callCount(), tokens.callCount()
				authCalls := authenticator.callCount()
				response := serve(method, alias)
				if indexes.callCount() != indexCalls || tokens.callCount() != tokenCalls {
					t.Errorf("alias %s %s of %s reached an administrative service (status %d)", method, alias, path, response.Code)
				}
				if authenticator.callCount() != authCalls {
					t.Errorf("alias %s %s of %s reached administrator authentication", method, alias, path)
				}
				if response.Code < 300 && strings.Contains(response.Body.String(), "ui-marker") {
					request := httptest.NewRequestWithContext(context.Background(), method, alias, nil)
					if inAPINamespace(request.URL.Path) || inAPINamespace(request.URL.EscapedPath()) {
						t.Errorf("API namespace alias %s %s of %s was served by the SPA", method, alias, path)
					}
				}
				if response.Code >= 200 && response.Code < 300 && !strings.Contains(response.Body.String(), "ui-marker") && method != http.MethodHead {
					t.Errorf("alias %s %s of %s = %d with a non-SPA body", method, alias, path, response.Code)
				}
			}
		}
	}
}

func TestEscapedAPINamespaceAliasesAreAPIErrorsNotTheSPA(t *testing.T) {
	t.Parallel()

	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{},
		Indexes:    fakeIndexCatalog{},
		WebUI:      testUI(),
	})
	for _, target := range []string{
		"/api%2Findexes%2Flist",
		"/api%2Fsearch%2Fws",
		"/api%2Fsearch%2Fexports%2Fdownload",
		"/api%2fsystem%2fbootstrap",
		"/%61pi/search/ws",
		"/%61pi",
		"/%61pi/",
	} {
		for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPost} {
			request := httptest.NewRequestWithContext(context.Background(), method, target, nil)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusNotFound {
				t.Errorf("%s %s = %d, want 404", method, target, response.Code)
			}
			if got := response.Header().Get("Content-Type"); got != "application/json; charset=utf-8" {
				t.Errorf("%s %s Content-Type = %q, want the API error type", method, target, got)
			}
			if strings.Contains(response.Body.String(), "ui-marker") {
				t.Errorf("%s %s served the SPA document", method, target)
			}
		}
	}
	// Browser routes that merely resemble the namespace still reach the SPA.
	for _, target := range []string{"/apis", "/api-docs", "/API", "/search/api"} {
		request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, target, nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "ui-marker") {
			t.Errorf("GET %s = %d, want the SPA document", target, response.Code)
		}
	}
}
