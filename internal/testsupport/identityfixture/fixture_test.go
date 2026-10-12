package identityfixture

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/authorization"
)

func TestNativeGoogleFixtureUsesPersistedUserAndMembership(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_TEST_MINIO_ENDPOINT") == "" {
		t.Skip("set isolated PostgreSQL and MinIO integration endpoints")
	}
	fixture := New(t, Options{PhysicalTenant: "fixture-instance"})
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/search", nil)
	request.AddCookie(&http.Cookie{Name: CookieName, Value: fixture.Session, Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode})
	_, subject, err := fixture.Runtime.AuthenticateRequest(request)
	if err != nil || !subject.IsSiteAdmin() || subject.UserID != 42 {
		t.Fatalf("native Google session subject=%#v err=%v", subject, err)
	}
	if err := fixture.Authorization.AuthorizeOrganization(context.Background(), subject, 1, authorization.AdminRole); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Database.GORMDB().Exec("UPDATE gocommon_users SET user_type=12 WHERE id=?", subject.UserID).Error; err != nil {
		t.Fatal(err)
	}
	_, demoted, err := fixture.Runtime.AuthenticateRequest(request)
	if err != nil || demoted.IsSiteAdmin() {
		t.Fatalf("same cookie retained stale admin type: %#v %v", demoted, err)
	}
	if err := fixture.Authorization.AuthorizeOrganization(context.Background(), demoted, 1, authorization.AdminRole); err != nil {
		t.Fatal("explicit owner membership disappeared after site demotion:", err)
	}
}
