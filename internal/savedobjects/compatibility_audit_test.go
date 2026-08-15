package savedobjects

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

func TestScanAllForCompatibilityAuditUsesOneQueryAcrossOwners(t *testing.T) {
	database, store := openTestStore(t)
	ctx := context.Background()
	type fixture struct {
		owner  string
		name   string
		source string
	}
	fixtures := []fixture{
		{owner: "owner-b", name: "alpha", source: "index=main | eval b=1"},
		{owner: "owner-a", name: "zeta", source: "index=main | eval a=2"},
		{owner: "owner-a", name: "alpha", source: "index=main | eval a=1"},
		{owner: "owner-c", name: "middle", source: "index=main | eval c=1"},
		{owner: "owner-b", name: "gamma", source: "index=main | eval b=2"},
	}
	ids := make([]string, len(fixtures))
	for index, fixture := range fixtures {
		definition := savedSearchDefinition(fixture.name, "")
		definition.Search.Spl = fixture.source
		created, err := store.Create(ctx, AccessScope{OwnerID: fixture.owner}, definition)
		if err != nil {
			t.Fatalf("Create(%d) error = %v", index, err)
		}
		ids[index] = created.GetSavedSearchId()
	}

	const callbackName = "savedobjects:test-count-compatibility-audit-queries"
	var queries atomic.Int64
	rowCallbacks := database.GORMDB().Callback().Row()
	if err := rowCallbacks.Before("gorm:row").Register(callbackName, func(*gorm.DB) {
		queries.Add(1)
	}); err != nil {
		t.Fatalf("register query counter: %v", err)
	}
	t.Cleanup(func() {
		if err := rowCallbacks.Remove(callbackName); err != nil {
			t.Errorf("remove query counter: %v", err)
		}
	})

	type visited struct{ id, source string }
	got := make([]visited, 0, len(fixtures))
	scanned, err := store.ScanAllForCompatibilityAudit(
		ctx,
		func(id, source string) error {
			got = append(got, visited{id: id, source: source})
			return nil
		},
	)
	if err != nil {
		t.Fatalf("ScanAllForCompatibilityAudit() error = %v", err)
	}
	want := []visited{
		{id: ids[2], source: fixtures[2].source},
		{id: ids[1], source: fixtures[1].source},
		{id: ids[0], source: fixtures[0].source},
		{id: ids[4], source: fixtures[4].source},
		{id: ids[3], source: fixtures[3].source},
	}
	if scanned != uint64(len(want)) || !reflect.DeepEqual(got, want) {
		t.Fatalf("ScanAllForCompatibilityAudit() = (%d, %#v), want (%d, %#v)",
			scanned, got, len(want), want)
	}
	if got := queries.Load(); got != 1 {
		t.Fatalf("compatibility-audit query count = %d, want 1", got)
	}
}

func TestScanAllForCompatibilityAuditStopsAfterCancellation(t *testing.T) {
	_, store := openTestStore(t)
	for index := range 3 {
		if _, err := store.Create(
			context.Background(),
			AccessScope{OwnerID: "owner"},
			savedSearchDefinition(string(rune('a'+index)), ""),
		); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	visits := 0
	scanned, err := store.ScanAllForCompatibilityAudit(ctx, func(_, _ string) error {
		visits++
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) || scanned != 1 || visits != 1 {
		t.Fatalf("canceled scan = (%d, %d visits, %v), want (1, 1, context.Canceled)",
			scanned, visits, err)
	}
}

func TestScanAllForCompatibilityAuditRejectsCorruptRecordsBeforeCallback(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *control.DB, string)
	}{
		{
			name: "invalid definition protobuf",
			mutate: func(t *testing.T, database *control.DB, id string) {
				t.Helper()
				if _, err := database.SQLDB().ExecContext(
					context.Background(),
					`UPDATE saved_searches SET definition_proto = x'ff' WHERE saved_search_id = ?`,
					id,
				); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "indexed metadata mismatch",
			mutate: func(t *testing.T, database *control.DB, id string) {
				t.Helper()
				if _, err := database.SQLDB().ExecContext(
					context.Background(),
					`UPDATE saved_searches SET name = 'forged-name' WHERE saved_search_id = ?`,
					id,
				); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			database, store := openTestStore(t)
			created, err := store.Create(
				context.Background(),
				AccessScope{OwnerID: "owner"},
				savedSearchDefinition("corrupt", ""),
			)
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(t, database, created.GetSavedSearchId())
			called := false
			if _, err := store.ScanAllForCompatibilityAudit(
				context.Background(),
				func(_, _ string) error {
					called = true
					return nil
				},
			); err == nil || called {
				t.Fatalf("corrupt compatibility audit = (called %t, %v), want rejection before callback", called, err)
			}
		})
	}
}
