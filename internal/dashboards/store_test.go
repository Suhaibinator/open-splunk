package dashboards

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	db, err := control.Open(context.Background(), filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	store, err := New(db, Options{
		TenantID: "tenant-a",
		Clock: func() time.Time {
			now = now.Add(time.Microsecond)
			return now
		},
		IDGenerator: func() (string, error) { return "dash_test", nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func testDefinition(name string) *opensplunk.DashboardDefinition {
	appID := "search"
	earliest, latest, timezone := "-24h", "now", "UTC"
	return &opensplunk.DashboardDefinition{
		Name: name, AppId: appID,
		SharingScope: opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
		Panels: []*opensplunk.DashboardPanel{{
			PanelId: "panel-1", Title: "Events", Width: 12, Height: 4,
			Search: &opensplunk.SearchDefinition{
				Spl: "index=main", AppId: &appID, IndexScope: []string{"main"},
				TimeRange: &opensplunk.TimeRangeSpec{Earliest: &earliest, Latest: &latest, Timezone: &timezone},
			},
		}},
	}
}

func TestStoreCRUDIsOwnerScopedAndVersioned(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	ownerA := AccessScope{OwnerID: "owner-a"}
	created, err := store.Create(ctx, ownerA, testDefinition("Operations"))
	if err != nil {
		t.Fatal(err)
	}
	if created.GetVersion() != 1 || created.GetDefinition().GetOwnerId() != ownerA.OwnerID {
		t.Fatalf("created dashboard = %+v", created)
	}
	created.Definition.Name = "caller mutation"
	got, err := store.Get(ctx, ownerA, created.GetDashboardId())
	if err != nil {
		t.Fatal(err)
	}
	if got.GetDefinition().GetName() != "Operations" {
		t.Fatalf("persisted name = %q", got.GetDefinition().GetName())
	}
	if _, err := store.Get(ctx, AccessScope{OwnerID: "owner-b"}, got.GetDashboardId()); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("cross-owner Get() error = %v, want not found", err)
	}
	listed, err := store.List(ctx, ownerA, nil)
	if err != nil || len(listed) != 1 || !proto.Equal(listed[0], got) {
		t.Fatalf("List() = %+v, %v", listed, err)
	}

	updatedDefinition := proto.Clone(got.GetDefinition()).(*opensplunk.DashboardDefinition)
	updatedDefinition.Name = "Platform"
	updated, err := store.Update(ctx, ownerA, got.GetDashboardId(), got.GetVersion(), updatedDefinition)
	if err != nil {
		t.Fatal(err)
	}
	if updated.GetVersion() != 2 || updated.GetDefinition().GetName() != "Platform" || !updated.GetUpdatedAt().AsTime().After(updated.GetCreatedAt().AsTime()) {
		t.Fatalf("updated dashboard = %+v", updated)
	}
	if _, err := store.Update(ctx, ownerA, got.GetDashboardId(), 1, updatedDefinition); !errors.Is(err, control.ErrVersionConflict) {
		t.Fatalf("stale Update() error = %v, want version conflict", err)
	}
	if err := store.Delete(ctx, ownerA, updated.GetDashboardId(), updated.GetVersion()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, ownerA, updated.GetDashboardId()); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("Get(deleted) error = %v, want not found", err)
	}
}

func TestStoreRejectsDuplicateNamesAndForgedOwners(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	scope := AccessScope{OwnerID: "owner-a"}
	if _, err := store.Create(ctx, scope, testDefinition("Operations")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(ctx, scope, testDefinition("Operations")); !errors.Is(err, control.ErrAlreadyExists) {
		t.Fatalf("duplicate Create() error = %v, want already exists", err)
	}
	forged := testDefinition("Other")
	forgedOwner := "owner-b"
	forged.OwnerId = &forgedOwner
	if _, err := store.Create(ctx, scope, forged); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("forged-owner Create() error = %v, want invalid argument", err)
	}
}

func TestStoreEnforcesOwnerCapacityAcrossConcurrentCreates(t *testing.T) {
	ctx := context.Background()
	db, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var ids atomic.Uint64
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	store, err := New(db, Options{
		TenantID: "tenant-a",
		IDGenerator: func() (string, error) {
			id := ids.Add(1)
			if id > maximumDashboardsPerOwner-1 {
				ready <- struct{}{}
				<-release
			}
			return fmt.Sprintf("dash-%d", id), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	scope := AccessScope{OwnerID: "owner-a"}
	for index := range maximumDashboardsPerOwner - 1 {
		if _, err := store.Create(ctx, scope, testDefinition(fmt.Sprintf("Dashboard %d", index))); err != nil {
			t.Fatal(err)
		}
	}
	results := make(chan error, 2)
	for index := range 2 {
		go func(index int) {
			_, createErr := store.Create(ctx, scope, testDefinition(fmt.Sprintf("Concurrent %d", index)))
			results <- createErr
		}(index)
	}
	<-ready
	<-ready
	close(release)
	first, second := <-results, <-results
	if (first == nil) == (second == nil) {
		t.Fatalf("concurrent Create() errors = %v, %v; want exactly one success", first, second)
	}
	failed := first
	if failed == nil {
		failed = second
	}
	if !errors.Is(failed, control.ErrCapacityExceeded) {
		t.Fatalf("losing Create() error = %v, want capacity exceeded", failed)
	}
	listed, err := store.List(ctx, scope, nil)
	if err != nil || len(listed) != maximumDashboardsPerOwner {
		t.Fatalf("List() after concurrent capacity admission = %d records, %v", len(listed), err)
	}
}
