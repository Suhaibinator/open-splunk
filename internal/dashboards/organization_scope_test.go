package dashboards

import (
	"errors"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestOrganizationSharingAndSiteAdministratorAuthority(t *testing.T) {
	store := openTestStore(t)
	owner := AccessScope{OwnerID: "owner-a", OrganizationID: "1"}
	reader := AccessScope{OwnerID: "owner-b", OrganizationID: "1"}
	outsider := AccessScope{OwnerID: "owner-b", OrganizationID: "2"}
	definition := testDefinition("shared")
	definition.SharingScope = opensplunk.SharingScope_SHARING_SCOPE_APP
	created, err := store.Create(t.Context(), owner, definition)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(t.Context(), reader, created.GetDashboardId()); err != nil {
		t.Fatalf("shared read: %v", err)
	}
	if _, err := store.Get(t.Context(), outsider, created.GetDashboardId()); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("cross-organization read: %v", err)
	}
	if _, err := store.Update(t.Context(), reader, created.GetDashboardId(), created.GetVersion(), created.GetDefinition()); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("shared reader update: %v", err)
	}
	admin := AccessScope{OwnerID: "site-admin", SiteAdministrator: true}
	if _, err := store.Update(t.Context(), admin, created.GetDashboardId(), created.GetVersion(), created.GetDefinition()); err != nil {
		t.Fatalf("site administrator update: %v", err)
	}
}
