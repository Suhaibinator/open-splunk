package savedobjects

import (
	"errors"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestOrganizationSharingDoesNotGrantMutationOrCrossOrganizationRead(t *testing.T) {
	_, store := openTestStore(t)
	owner := AccessScope{OwnerID: "owner-a", OrganizationID: "1"}
	reader := AccessScope{OwnerID: "owner-b", OrganizationID: "1"}
	outsider := AccessScope{OwnerID: "owner-b", OrganizationID: "2"}
	definition := savedSearchDefinition("shared", "search")
	definition.SharingScope = opensplunk.SharingScope_SHARING_SCOPE_GLOBAL
	created, err := store.Create(t.Context(), owner, definition)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(t.Context(), reader, created.GetSavedSearchId()); err != nil {
		t.Fatalf("same-organization shared read: %v", err)
	}
	if _, err := store.Get(t.Context(), outsider, created.GetSavedSearchId()); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("cross-organization read: %v", err)
	}
	if err := store.Delete(t.Context(), reader, created.GetSavedSearchId(), created.GetVersion()); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("shared reader mutation: %v", err)
	}
	page, err := store.List(t.Context(), outsider, ListRequest{})
	if err != nil || len(page.SavedSearches) != 0 {
		t.Fatalf("cross-organization list: %+v,%v", page, err)
	}
	admin := AccessScope{OwnerID: "site-admin", SiteAdministrator: true}
	if _, err := store.Get(t.Context(), admin, created.GetSavedSearchId()); err != nil {
		t.Fatalf("site administrator read: %v", err)
	}
	if err := store.Delete(t.Context(), admin, created.GetSavedSearchId(), created.GetVersion()); err != nil {
		t.Fatalf("site administrator mutation: %v", err)
	}
}
