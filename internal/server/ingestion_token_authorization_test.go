package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Suhaibinator/SRouter/pkg/router"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/authorization"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

type ingestionOrganizationPolicies struct {
	organizations map[string]int64
	rules         []authorization.Rule
}

func (p ingestionOrganizationPolicies) OrganizationForIndex(_ context.Context, id string) (int64, error) {
	return p.organizations[id], nil
}
func (p ingestionOrganizationPolicies) Rules(context.Context, string) ([]authorization.Rule, error) {
	return p.rules, nil
}

type ingestionOrganizationMembership struct{ role int32 }

func (m ingestionOrganizationMembership) EffectiveRole(context.Context, uint64, int64) (int32, bool, error) {
	return m.role, true, nil
}

func TestIngestionCredentialAdministrationRequiresOwningOrganizationAdmin(t *testing.T) {
	for _, test := range []struct {
		name           string
		role, userType int32
		selectedOrg    int64
		indexes        []string
		rules          []authorization.Rule
		recordOrg      int64
		denied         bool
	}{
		{name: "organization admin own indexes", role: 23, userType: 12, selectedOrg: 1, indexes: []string{"main"}, recordOrg: 1},
		{name: "viewer with delegated grants and ingest", role: 1, userType: 12, selectedOrg: 1, indexes: []string{"main"}, recordOrg: 1, rules: []authorization.Rule{{SubjectKind: "user", SubjectID: 42, Capability: authorization.Grants, Effect: "allow"}, {SubjectKind: "user", SubjectID: 42, Capability: authorization.Ingest, Effect: "allow"}}, denied: true},
		{name: "foreign token immutable organization", role: 23, userType: 12, selectedOrg: 1, indexes: []string{"main"}, recordOrg: 2, denied: true},
		{name: "cross organization indexes", role: 23, userType: 12, selectedOrg: 1, indexes: []string{"main", "foreign"}, recordOrg: 1, denied: true},
		{name: "explicit ingest denial", role: 23, userType: 12, selectedOrg: 1, indexes: []string{"main"}, recordOrg: 1, rules: []authorization.Rule{{SubjectKind: "user", SubjectID: 42, Capability: authorization.Ingest, Effect: "deny"}}, denied: true},
		{name: "site admin still selects single organization", role: 1, userType: 44, selectedOrg: 1, indexes: []string{"main", "foreign"}, recordOrg: 1, denied: true},
		{name: "site admin owning organization overrides deny", role: 1, userType: 44, selectedOrg: 1, indexes: []string{"main"}, recordOrg: 1, rules: []authorization.Rule{{SubjectKind: "user", SubjectID: 42, Capability: authorization.Ingest, Effect: "deny"}}},
		{name: "site admin missing selection", role: 1, userType: 44, indexes: []string{"main"}, recordOrg: 1, denied: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			policies := ingestionOrganizationPolicies{organizations: map[string]int64{"index-main": 1, "index-foreign": 2}, rules: test.rules}
			service, err := authorization.NewService(policies, ingestionOrganizationMembership{role: test.role}, "physical")
			if err != nil {
				t.Fatal(err)
			}
			handler := &apiHandler{authorization: service, indexes: fakeIndexCatalog{indexes: []control.Index{{ID: "index-main", Definition: control.IndexDefinition{Name: "main"}}, {ID: "index-foreign", Definition: control.IndexDefinition{Name: "foreign"}}}}}
			ctx := authorization.WithScope(context.Background(), authorization.Scope{Subject: authorization.Subject{UserID: 42, UserType: test.userType}, OrganizationID: test.selectedOrg, PhysicalTenant: "physical"})
			err = handler.authorizeIngestionTokenRecord(ctx, auth.CollectorToken{IssuerUserID: 1, OrganizationID: test.recordOrg, AllowedIndexNames: test.indexes})
			if err == nil {
				err = handler.authorizeIngestionTokenIndexes(ctx, test.indexes)
			}
			if test.denied {
				var httpError *router.HTTPError
				if !errors.As(err, &httpError) || (httpError.StatusCode != http.StatusForbidden && (test.selectedOrg != 0 || httpError.StatusCode != http.StatusBadRequest)) {
					t.Fatalf("error=%v, want403", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOwnOrganizationCanInspectAndRevokeBlockedIngestionCredential(t *testing.T) {
	policies := ingestionOrganizationPolicies{organizations: map[string]int64{"index-main": 2}}
	service, err := authorization.NewService(policies, ingestionOrganizationMembership{role: authorization.AdminRole}, "physical")
	if err != nil {
		t.Fatal(err)
	}
	handler := &apiHandler{authorization: service, indexes: fakeIndexCatalog{indexes: []control.Index{{ID: "index-main", Definition: control.IndexDefinition{Name: "main"}}}}}
	ctx := authorization.WithScope(context.Background(), authorization.Scope{Subject: authorization.Subject{UserID: 42, UserType: 12}, OrganizationID: 1, PhysicalTenant: "physical"})
	if err := handler.authorizeIngestionTokenRecord(ctx, auth.CollectorToken{IssuerUserID: 1, OrganizationID: 1, AllowedIndexNames: []string{"main"}}); err != nil {
		t.Fatalf("own credential metadata and revocation denied: %v", err)
	}
	if err := handler.authorizeIngestionTokenIndexes(ctx, []string{"main"}); err == nil {
		t.Fatal("moved source permitted credential enable or mint")
	}
}

func TestIngestionTokenBindingDistinguishesUnenrolledAndForeignCollectors(t *testing.T) {
	for _, test := range []struct {
		name                  string
		existing, visible     bool
		wantStatus, wantCalls int
	}{
		{name: "own collector", existing: true, visible: true, wantCalls: 1},
		{name: "new enrollment", wantCalls: 2},
		{name: "foreign collector", existing: true, wantStatus: http.StatusForbidden, wantCalls: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			policies := ingestionOrganizationPolicies{organizations: map[string]int64{"index-main": 1}}
			service, err := authorization.NewService(policies, ingestionOrganizationMembership{role: authorization.AdminRole}, "physical")
			if err != nil {
				t.Fatal(err)
			}
			collector := &fakeCollectorAdministration{getFn: func(_ context.Context, scope collectorfleet.Scope, id string) (collectorfleet.CatalogEntry, error) {
				if scope.TenantID != "physical" || id != "collector-binding" {
					t.Fatalf("unexpected binding lookup: %+v %q", scope, id)
				}
				if !test.existing || scope.IndexAccess != "" && !test.visible {
					return collectorfleet.CatalogEntry{}, control.ErrNotFound
				}
				return collectorfleet.CatalogEntry{}, nil
			}}
			handler := &apiHandler{tenantID: "physical", authorization: service, collectorAdmin: collector, indexes: fakeIndexCatalog{indexes: []control.Index{{ID: "index-main", Definition: control.IndexDefinition{Name: "main"}}}}}
			ctx := authorization.WithScope(context.Background(), authorization.Scope{Subject: authorization.Subject{UserID: 42, UserType: 12}, OrganizationID: 1, PhysicalTenant: "physical"})
			principal, err := auth.NewBrowserPrincipal("physical", "42", auth.BrowserRoleAdministrator)
			if err != nil {
				t.Fatal(err)
			}
			ctx = context.WithValue(ctx, browserPrincipalContextKey{}, principal)
			request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/ingestion-tokens/create", nil).WithContext(ctx)
			err = handler.authorizeIngestionTokenCollectorBinding(request, "collector-binding")
			if test.wantStatus > 0 {
				var httpError *router.HTTPError
				if !errors.As(err, &httpError) || httpError.StatusCode != test.wantStatus {
					t.Fatalf("binding error=%v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if collector.calls()[0] != test.wantCalls {
				t.Fatalf("binding lookups=%d want%d", collector.calls()[0], test.wantCalls)
			}
		})
	}
}
