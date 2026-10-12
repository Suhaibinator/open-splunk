package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/authorization"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/resourcepolicy"
)

type sourceReadPolicyStore struct{}

func (sourceReadPolicyStore) OrganizationForIndex(context.Context, string) (int64, error) {
	return 7, nil
}
func (sourceReadPolicyStore) Rules(_ context.Context, index string) ([]authorization.Rule, error) {
	if index == "idx-secret" {
		return []authorization.Rule{{IndexID: index, SubjectKind: "user", SubjectID: 91, Capability: authorization.Search, Effect: "deny"}}, nil
	}
	return nil, nil
}

type viewerManagementPolicyStore struct{}

func (viewerManagementPolicyStore) OrganizationForIndex(context.Context, string) (int64, error) {
	return 7, nil
}
func (viewerManagementPolicyStore) Rules(context.Context, string) ([]authorization.Rule, error) {
	return []authorization.Rule{{SubjectKind: "user", SubjectID: 91, Capability: authorization.Search, Effect: "deny"}, {SubjectKind: "user", SubjectID: 91, Capability: authorization.Manage, Effect: "allow"}, {SubjectKind: "user", SubjectID: 91, Capability: authorization.Grants, Effect: "allow"}}, nil
}

func TestViewerExplicitManagementReachesIndexCapabilityBoundary(t *testing.T) {
	policy, err := authorization.NewService(viewerManagementPolicyStore{}, orgMembershipStub{role: authorization.ViewerRole, member: true}, "physical")
	if err != nil {
		t.Fatal(err)
	}
	handler := identityBoundary(t, authorization.Subject{UserID: 91, UserType: 12}, orgMembershipStub{role: authorization.ViewerRole, member: true})
	handler.authorization = policy
	handler.administratorRoutes = map[string]struct{}{"/api/indexes/update": {}, "/api/indexes/permissions/get": {}, "/api/indexes/get": {}}
	for _, route := range []string{"/api/indexes/update", "/api/indexes/permissions/get", "/api/indexes/get"} {
		called := false
		boundary := handler.protectBrowserAPIRoutes(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			called = true
			capability := indexRouteCapability(route)
			if route == "/api/indexes/permissions/get" {
				capability = authorization.Grants
			}
			if err := handler.authorizeIndexCapability(r.Context(), "index", capability); err != nil {
				t.Fatalf("explicit index capability denied: %v", err)
			}
			w.WriteHeader(http.StatusOK)
		}))
		request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "http://example.test"+route, nil)
		request.Header.Set("X-OpenSplunk-Organization-ID", "7")
		response := httptest.NewRecorder()
		boundary.ServeHTTP(response, request)
		if response.Code != http.StatusOK || !called {
			t.Fatalf("Viewer override stopped by role gate %s: %d", route, response.Code)
		}
	}
}

func TestBootstrapHidesDeniedAppDefaultsAndPreservesStoredPreference(t *testing.T) {
	policy, err := authorization.NewService(sourceReadPolicyStore{}, orgMembershipStub{role: authorization.ViewerRole, member: true}, "physical")
	if err != nil {
		t.Fatal(err)
	}
	stored := &opensplunk.AppSummary{AppId: "app-one", DefaultIndexNames: []string{"main", "secret"}}
	handler := &apiHandler{authorization: policy, indexes: fakeIndexCatalog{indexes: []control.Index{{ID: "idx-main", Definition: control.IndexDefinition{Name: "main", SearchEnabled: true}, State: control.IndexStateActive}, {ID: "idx-secret", Definition: control.IndexDefinition{Name: "secret", SearchEnabled: true}, State: control.IndexStateActive}}}, bootstrap: BootstrapConfig{Apps: []*opensplunk.AppSummary{stored}}, now: time.Now}
	ctx := authorization.WithScope(t.Context(), authorization.Scope{Subject: authorization.Subject{UserID: 91, UserType: 12}, OrganizationID: 7, PhysicalTenant: "physical"})
	response, err := handler.getSystemBootstrap(httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/system/bootstrap", nil), &opensplunk.GetSystemBootstrapRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.GetIndexes()) != 1 || !slices.Equal(response.GetApps()[0].GetDefaultIndexNames(), []string{"main"}) {
		t.Fatalf("denied index leaked in bootstrap: %+v", response)
	}
	if !slices.Equal(stored.GetDefaultIndexNames(), []string{"main", "secret"}) {
		t.Fatal("projection mutated persisted preference")
	}
}

func TestAppProjectionDefaultsUseCurrentSearchVisibility(t *testing.T) {
	handler := &apiHandler{}
	definition := &opensplunk.AppDefinition{DefaultIndexNames: []string{"main", "secret"}}
	ctx := resourcepolicy.WithProvider(t.Context(), func(context.Context) (resourcepolicy.Snapshot, error) {
		return resourcepolicy.Snapshot{IndexNames: []string{"main"}}, nil
	})
	if err := handler.filterAppDefaults(ctx, definition); err != nil || !slices.Equal(definition.GetDefaultIndexNames(), []string{"main"}) {
		t.Fatalf("app defaults leaked: %+v %v", definition, err)
	}
}

func TestStoredSearchSourcePreflightIncludesAuthoredIndexOutsideDeclaredScope(t *testing.T) {
	handler := identityBoundary(t, authorization.Subject{UserID: 91, UserType: 12}, orgMembershipStub{role: authorization.ViewerRole, member: true})
	ctx := authorization.WithScope(t.Context(), authorization.Scope{Subject: authorization.Subject{UserID: 91, UserType: 12}, OrganizationID: 7, PhysicalTenant: "physical"})
	ctx = resourcepolicy.WithProvider(ctx, func(context.Context) (resourcepolicy.Snapshot, error) {
		return resourcepolicy.Snapshot{IndexNames: []string{"main"}}, nil
	})
	definition := &opensplunk.SearchDefinition{Spl: "index=secret", IndexScope: []string{"main"}}
	if err := handler.authorizeStoredSearchDefinition(ctx, definition); err == nil {
		t.Fatal("declared main scope hid authored secret index")
	}
}
