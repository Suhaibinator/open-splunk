package savedobjects

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/pgmeta"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestIndependentSavedSearchOwnersCanWriteWhileAnotherOwnerWaits(t *testing.T) {
	database, store := openTestStore(t)
	owned, err := store.Create(t.Context(), AccessScope{OwnerID: "held-owner", OrganizationID: "1"}, savedSearchDefinition("Owned", "legacy-app"))
	if err != nil {
		t.Fatal(err)
	}
	held := pgmeta.BeginCatalogReader(database.GORMDB().WithContext(t.Context()), "saved-content:held-owner")
	if held.Error != nil {
		t.Fatal(held.Error)
	}
	defer held.Rollback()
	independent, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := store.Create(independent, AccessScope{OwnerID: "other-owner", OrganizationID: "1"}, savedSearchDefinition("Independent", "legacy-app")); err != nil {
		t.Fatalf("independent create: %v", err)
	}
	conflicting, cancelConflict := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancelConflict()
	if _, err := store.Create(conflicting, AccessScope{OwnerID: "held-owner", OrganizationID: "1"}, savedSearchDefinition("Held", "legacy-app")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("same owner should contend: %v", err)
	}
	adminWait, cancelAdmin := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancelAdmin()
	if _, err := store.Update(adminWait, AccessScope{OwnerID: "site-admin", OrganizationID: "1", SiteAdministrator: true}, owned.SavedSearchId, owned.Version, savedSearchDefinition("Admin edit", "legacy-app"), &fieldmaskpb.FieldMask{Paths: []string{"definition.name"}}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("administrator must contend on the creator's resource key: %v", err)
	}
	read, cancelRead := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelRead()
	snapshot := pgmeta.Begin(database.GORMDB().WithContext(read))
	if snapshot.Error != nil {
		t.Fatalf("plain transaction acquired a writer lock: %v", snapshot.Error)
	}
	defer snapshot.Rollback()
	if err := snapshot.Exec("SELECT 1").Error; err != nil {
		t.Fatal(err)
	}
}
