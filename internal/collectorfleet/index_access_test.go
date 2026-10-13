package collectorfleet

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestIndexAccessScopesCollectorCountsPagesAndMutations(t *testing.T) {
	database, store := openTestStore(t)
	catalog := newCatalogForTest(t, database)
	ctx := context.Background()
	now := time.Date(2026, 10, 12, 10, 0, 0, 0, time.UTC)
	for _, item := range []struct {
		id      string
		indexes []string
	}{
		{"alpha", []string{"main"}}, {"beta", []string{"main"}},
		{"foreign", []string{"other"}}, {"mixed", []string{"main", "other"}},
	} {
		claimCatalogTestCollector(t, store, "tenant-a", item.id, item.id, nil, now, 0, item.indexes...)
	}
	scope, err := RestrictIndexes("tenant-a", "12:1", []string{"main"})
	if err != nil {
		t.Fatal(err)
	}
	page, err := catalog.List(ctx, scope, nil, ListRequest{PageSize: 1, IncludeTotal: true})
	if err != nil || page.TotalSize == nil || *page.TotalSize != 2 || len(page.Entries) != 1 || page.NextPageToken == nil {
		t.Fatalf("authorized first page = %+v, %v", page, err)
	}
	otherScope, err := RestrictIndexes("tenant-a", "12:2", []string{"main"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.List(ctx, otherScope, nil, ListRequest{PageSize: 1, IncludeTotal: true, PageToken: *page.NextPageToken}); err == nil {
		t.Fatal("cross-organization continuation accepted")
	}
	for _, id := range []string{"foreign", "mixed"} {
		if _, err := catalog.Get(ctx, scope, id, nil); !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("Get(%s): %v", id, err)
		}
		if _, err := store.GetAdministration(ctx, scope, id); !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("GetAdministration(%s): %v", id, err)
		}
		if _, err := store.SetAdministrativeState(ctx, scope, id, 1, AdministrativeStateDisabled, now.Add(time.Minute)); !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("SetAdministrativeState(%s): %v", id, err)
		}
	}
	if _, err := store.SetAdministrativeState(ctx, scope, "alpha", 1, AdministrativeStateDisabled, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	denied, err := RestrictIndexes("tenant-a", "12:1", nil)
	if err != nil {
		t.Fatal(err)
	}
	page, err = catalog.List(ctx, denied, nil, ListRequest{IncludeTotal: true})
	if err != nil || page.TotalSize == nil || *page.TotalSize != 0 || len(page.Entries) != 0 {
		t.Fatalf("denied page = %+v, %v", page, err)
	}
	page, err = catalog.List(ctx, Scope{TenantID: "tenant-a"}, nil, ListRequest{IncludeTotal: true})
	if err != nil || page.TotalSize == nil || *page.TotalSize != 4 {
		t.Fatalf("site/internal page = %+v, %v", page, err)
	}
}
