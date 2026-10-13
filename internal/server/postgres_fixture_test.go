package server

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/authorization"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/pgtest"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

var serverDatabaseFixtures sync.Map

func serverTestDSN(t testing.TB, fixture string) string {
	t.Helper()
	if strings.HasPrefix(fixture, "postgres://") || strings.HasPrefix(fixture, "postgresql://") {
		return fixture
	}
	key := t.Name() + "\x00" + fixture
	if dsn, ok := serverDatabaseFixtures.Load(key); ok {
		return dsn.(string)
	}
	dsn := pgtest.DSN(t)
	serverDatabaseFixtures.Store(key, dsn)
	t.Cleanup(func() { serverDatabaseFixtures.Delete(key) })
	return dsn
}
func openTestControl(t testing.TB, ctx context.Context, fixture string) (*control.DB, error) {
	t.Helper()
	return control.Open(ctx, serverTestDSN(t, fixture))
}

func serverFixtureScope(ctx context.Context, userID uint64) context.Context {
	return authorization.WithScope(ctx, authorization.Scope{Subject: authorization.Subject{UserID: userID, UserType: 12}, OrganizationID: 1, PhysicalTenant: "tenant-1"})
}

func serverFixturePrincipalContext(ctx context.Context, principal auth.BrowserPrincipal) context.Context {
	userID, _ := strconv.ParseUint(principal.OwnerID(), 10, 64)
	userType := int32(12)
	if principal.IsAdministrator() {
		userType = 44
	}
	ctx = authorization.WithScope(ctx, authorization.Scope{Subject: authorization.Subject{UserID: userID, UserType: userType}, OrganizationID: 1, PhysicalTenant: principal.TenantID()})
	return context.WithValue(ctx, browserPrincipalContextKey{}, principal)
}

func serverFixtureUserIdentity(t testing.TB, userID uint64) IdentityAuthenticator {
	t.Helper()
	session, err := testsupport.NewSessionAuthenticator(userID, 12, nil)
	if err != nil {
		t.Fatal(err)
	}
	return fixtureOrganizationIdentity{session}
}
