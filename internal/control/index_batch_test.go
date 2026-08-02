package control

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"gorm.io/gorm"
)

func TestGetIndexesByNamesUsesOneVisibleQueryAndPreservesRequestOrder(
	t *testing.T,
) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	created := make(map[string]Index)
	for _, name := range []string{"alpha", "bravo", "charlie"} {
		index, err := db.CreateIndex(ctx, enabledIndex(name))
		if err != nil {
			t.Fatalf("CreateIndex(%q): %v", name, err)
		}
		created[name] = index
	}

	var queryCount atomic.Int64
	const callbackName = "test:count-index-batch-queries"
	if err := db.GORMDB().Callback().Query().Before("gorm:query").Register(
		callbackName,
		func(*gorm.DB) { queryCount.Add(1) },
	); err != nil {
		t.Fatalf("register query callback: %v", err)
	}
	t.Cleanup(func() {
		if err := db.GORMDB().Callback().Query().Remove(callbackName); err != nil {
			t.Errorf("remove query callback: %v", err)
		}
	})

	requested := []string{"charlie", "alpha", "bravo"}
	indexes, err := db.GetIndexesByNames(ctx, requested)
	if err != nil {
		t.Fatalf("GetIndexesByNames(): %v", err)
	}
	if got := queryCount.Load(); got != 1 {
		t.Fatalf("GORM query count = %d, want 1", got)
	}
	gotNames := make([]string, len(indexes))
	for position, index := range indexes {
		gotNames[position] = index.Definition.Name
		if index.ID != created[requested[position]].ID {
			t.Fatalf(
				"indexes[%d].ID = %q, want %q",
				position,
				index.ID,
				created[requested[position]].ID,
			)
		}
	}
	if !slices.Equal(gotNames, requested) {
		t.Fatalf("result names = %v, want %v", gotNames, requested)
	}
}

func TestGetIndexesByNamesFailsClosedWhenAnyNameIsMissingOrTombstoned(
	t *testing.T,
) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	active, err := db.CreateIndex(ctx, enabledIndex("active"))
	if err != nil {
		t.Fatalf("CreateIndex(active): %v", err)
	}
	deleted, err := db.CreateIndex(ctx, enabledIndex("deleted"))
	if err != nil {
		t.Fatalf("CreateIndex(deleted): %v", err)
	}
	deleted, err = db.SetIndexState(
		ctx,
		deleted.ID,
		deleted.Version,
		IndexStateArchived,
	)
	if err != nil {
		t.Fatalf("SetIndexState(deleted): %v", err)
	}
	if _, err := db.DeleteIndex(
		ctx,
		deleted.ID,
		deleted.Version,
		deleted.Definition.Name,
	); err != nil {
		t.Fatalf("DeleteIndex(deleted): %v", err)
	}

	for _, names := range [][]string{
		{active.Definition.Name, "missing"},
		{active.Definition.Name, deleted.Definition.Name},
	} {
		indexes, err := db.GetIndexesByNames(ctx, names)
		if !errors.Is(err, ErrNotFound) || indexes != nil {
			t.Fatalf(
				"GetIndexesByNames(%v) = (%#v, %v), want nil ErrNotFound",
				names,
				indexes,
				err,
			)
		}
	}
}

func TestGetIndexesByNamesRejectsInvalidOrUnboundedBatches(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	tooMany := make([]string, MaximumPhysicalIndexRecords+1)
	for index := range tooMany {
		tooMany[index] = "name"
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, test := range []struct {
		name    string
		ctx     context.Context
		indexes []string
		want    error
	}{
		{name: "nil context", indexes: []string{"main"}, want: ErrInvalidArgument},
		{name: "canceled context", ctx: canceled, indexes: []string{"main"}, want: context.Canceled},
		{name: "empty", ctx: context.Background(), want: ErrInvalidArgument},
		{name: "noncanonical", ctx: context.Background(), indexes: []string{"Main"}, want: ErrInvalidArgument},
		{name: "duplicate", ctx: context.Background(), indexes: []string{"main", "main"}, want: ErrInvalidArgument},
		{name: "too many", ctx: context.Background(), indexes: tooMany, want: ErrInvalidArgument},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			indexes, err := db.GetIndexesByNames(test.ctx, test.indexes)
			if !errors.Is(err, test.want) || indexes != nil {
				t.Fatalf(
					"GetIndexesByNames() = (%#v, %v), want nil %v",
					indexes,
					err,
					test.want,
				)
			}
		})
	}
}

func TestGetIndexesByNamesClonesNamesBeforeQuery(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	created, err := db.CreateIndex(context.Background(), enabledIndex("main"))
	if err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	storage := []byte("main")
	names := []string{string(storage)}
	indexes, err := db.GetIndexesByNames(context.Background(), names)
	if err != nil {
		t.Fatalf("GetIndexesByNames(): %v", err)
	}
	storage[0] = 'x'
	names[0] = strings.Repeat("x", 4)
	if len(indexes) != 1 || indexes[0].ID != created.ID {
		t.Fatalf("GetIndexesByNames() = %#v, want %#v", indexes, created)
	}
}
