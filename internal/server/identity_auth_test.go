package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/authorization"
)

type userSessionStub struct {
	subject authorization.Subject
	err     error
}

func (s userSessionStub) AuthenticateRequest(r *http.Request) (*http.Request, authorization.Subject, error) {
	return r, s.subject, s.err
}

type orgMembershipStub struct {
	role   int32
	member bool
}

func (s orgMembershipStub) EffectiveRole(context.Context, uint64, int64) (int32, bool, error) {
	return s.role, s.member, nil
}

type identityPolicyStore struct{}

func (identityPolicyStore) OrganizationForIndex(context.Context, string) (int64, error) {
	return 7, nil
}
func (identityPolicyStore) Rules(context.Context, string) ([]authorization.Rule, error) {
	return nil, nil
}
func (identityPolicyStore) SetOrganization(context.Context, string, int64, int64) error { return nil }
func (identityPolicyStore) SetRule(context.Context, authorization.Rule, int64) error    { return nil }
func (identityPolicyStore) DeleteRule(context.Context, authorization.Rule, int64) error { return nil }

func identityBoundary(t *testing.T, subject authorization.Subject, membership orgMembershipStub) *apiHandler {
	t.Helper()
	policy, err := authorization.NewService(identityPolicyStore{}, membership, "physical")
	if err != nil {
		t.Fatal(err)
	}
	return &apiHandler{identityAuthenticator: userSessionStub{subject: subject}, authorization: policy, tenantID: "physical", ownerID: "old-static-owner", browserAllowedHosts: map[string]struct{}{"example.test": {}}}
}
func TestIdentityBoundaryAuthenticatesOrdinaryRoutesAndUsesCurrentOwner(t *testing.T) {
	handler := identityBoundary(t, authorization.Subject{UserID: 91, UserType: 12}, orgMembershipStub{role: authorization.ViewerRole, member: true})
	called := false
	boundary := handler.protectBrowserAPIRoutes(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		scope := handler.accessScope(r.Context())
		if scope.OwnerID != "91" || scope.OrganizationID != "7" || scope.SiteAdministrator || scope.TenantID != "physical" {
			t.Fatalf("request escaped authenticated scope: %+v", scope)
		}
		if handler.savedSearchScope(r.Context()).OrganizationID != "7" || handler.dashboardScope(r.Context()).OrganizationID != "7" {
			t.Fatal("saved resources lost organization")
		}
		w.WriteHeader(http.StatusOK)
	}))
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "http://example.test/api/search/jobs/list", nil)
	request.Header.Set("X-OpenSplunk-Organization-ID", "7")
	response := httptest.NewRecorder()
	boundary.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !called {
		t.Fatalf("ordinary session was refused: %d", response.Code)
	}
	handler.identityAuthenticator = userSessionStub{err: authorization.ErrUnauthorized}
	called = false
	response = httptest.NewRecorder()
	boundary.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized || called {
		t.Fatal("ordinary API remained open without a valid user")
	}
}
func TestIdentityBoundaryRejectsNonMemberOrganizationSelection(t *testing.T) {
	handler := identityBoundary(t, authorization.Subject{UserID: 91, UserType: 12}, orgMembershipStub{})
	boundary := handler.protectBrowserAPIRoutes(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("nonmember reached API") }))
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "http://example.test/api/system/bootstrap", nil)
	request.Header.Set("X-OpenSplunk-Organization-ID", "7")
	response := httptest.NewRecorder()
	boundary.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("nonmember response=%d", response.Code)
	}
}
func TestSiteAdministratorCanActWithoutAnOrganization(t *testing.T) {
	handler := identityBoundary(t, authorization.Subject{UserID: 91, UserType: authorization.SiteAdminUserType}, orgMembershipStub{})
	boundary := handler.protectBrowserAPIRoutes(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !handler.accessScope(r.Context()).SiteAdministrator {
			t.Fatal("site authority lost")
		}
		w.WriteHeader(http.StatusOK)
	}))
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "http://example.test/api/server/settings/update", nil)
	response := httptest.NewRecorder()
	boundary.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("site administrator refused: %d", response.Code)
	}
}
func TestOrganizationAdminCannotReachClusterAdministration(t *testing.T) {
	handler := identityBoundary(t, authorization.Subject{UserID: 91, UserType: 12}, orgMembershipStub{role: authorization.AdminRole, member: true})
	boundary := handler.protectBrowserAPIRoutes(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("organization admin reached cluster API") }))
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "http://example.test/api/server/settings/update", nil)
	request.Header.Set("X-OpenSplunk-Organization-ID", "7")
	response := httptest.NewRecorder()
	boundary.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("organization admin response=%d", response.Code)
	}
}
func TestCookieDownloadShapeUsesAnExportIDAndRejectsExtraAuthority(t *testing.T) {
	for _, test := range []struct {
		query string
		valid bool
	}{{"export_job_id=export1&organization_id=7", true}, {"export_job_id=export1&download_token=secret", false}, {"export_job_id=export1&export_job_id=export2", false}, {"organization_id=7", false}} {
		r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://example.test/api/search/exports/download?"+test.query, nil)
		if validAuthenticatedExportDownloadShape(r) != test.valid {
			t.Fatalf("download shape %q validity=%v", test.query, test.valid)
		}
	}
}
