package lookupcatalog

import (
	"errors"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestLookupOrganizationIsolationAndSiteAdministratorOwnerBypass(t *testing.T) {
	database, assets, appIDs := newCatalogHarness(t)
	apps, err := control.NewAppCatalog(database, control.AppCatalogOptions{CursorKey: []byte("0123456789abcdef0123456789abcdef")})
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := apps.CreateApp(t.Context(), control.AppAccessScope{TenantID: "tenant-lookups", OrganizationID: 2}, control.AppDefinition{Slug: "foreign", DisplayName: "Foreign"})
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := New(database, assets, Options{})
	if err != nil {
		t.Fatal(err)
	}
	asset := publishCatalogAsset(t, assets, "service_id,owner\napi,alice\n")
	first, err := catalog.Create(t.Context(), CreateRequest{OrganizationID: 1, TenantID: "tenant-lookups", OwnerID: "owner-lookups", Definition: catalogDefinition(appIDs[0], "globalorg", opensplunk.SharingScope_SHARING_SCOPE_GLOBAL, false), Asset: asset})
	if err != nil {
		t.Fatal(err)
	}
	second, err := catalog.Create(t.Context(), CreateRequest{OrganizationID: 2, TenantID: "tenant-lookups", OwnerID: "owner-lookups", Definition: catalogDefinition(foreign.ID, "globalorg", opensplunk.SharingScope_SHARING_SCOPE_GLOBAL, false), Asset: asset})
	if err != nil {
		t.Fatal(err)
	}
	if first.GetLookupId() == second.GetLookupId() {
		t.Fatal("same lookup ID")
	}
	if _, err := catalog.GetResolved(t.Context(), GetRequest{OrganizationID: 2, TenantID: "tenant-lookups", OwnerID: "owner-lookups", LookupID: first.GetLookupId()}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("foreign body access: %v", err)
	}
	resolved, err := catalog.ResolveAdmission(t.Context(), ResolveScope{OrganizationID: 2, TenantID: "tenant-lookups", PrincipalID: "owner-lookups", AppID: foreign.ID, Names: []string{"globalorg"}})
	if err != nil || len(resolved.Explicit) != 1 || resolved.Explicit[0].Lookup.GetLookupId() != second.GetLookupId() {
		t.Fatalf("scoped GLOBAL resolution %#v %v", resolved, err)
	}
	if _, err := catalog.Get(t.Context(), GetRequest{OrganizationID: 1, TenantID: "tenant-lookups", OwnerID: "someone-else", LookupID: first.GetLookupId()}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("owner constraint: %v", err)
	}
	if _, err := catalog.Get(t.Context(), GetRequest{OrganizationID: 1, SiteAdministrator: true, TenantID: "tenant-lookups", OwnerID: "siteadmin", LookupID: first.GetLookupId()}); err != nil {
		t.Fatalf("siteadmin owner bypass: %v", err)
	}
	if _, err := catalog.Replace(t.Context(), ReplaceRequest{OrganizationID: 1, TenantID: "tenant-lookups", OwnerID: "owner-lookups", LookupID: first.GetLookupId(), ExpectedVersion: 1, Definition: catalogDefinition(foreign.ID, "move", opensplunk.SharingScope_SHARING_SCOPE_GLOBAL, false), Asset: asset}); err == nil {
		t.Fatal("cross-org move accepted")
	}
	if _, err := database.SQLDB().ExecContext(t.Context(), "UPDATE knowledge_lookup_definitions SET app_id=$1 WHERE lookup_id=$2", foreign.ID, first.GetLookupId()); err == nil {
		t.Fatal("direct cross-org mutation accepted")
	}
}
