package control

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestIndexLifecycleNormalizesNameAndUsesOptimisticVersions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)

	created, err := db.CreateIndex(ctx, IndexDefinition{
		Name:              "  GradeThis-Prod  ",
		DisplayName:       "GradeThis Production",
		Description:       "production application logs",
		RetentionPeriod:   30 * 24 * time.Hour,
		IngestionEnabled:  true,
		SearchEnabled:     true,
		DefaultSourcetype: "go:zap:json",
		Limits: IndexLimits{
			MaxEventBytes:     1 << 20,
			MaxFieldCount:     256,
			MaxNestingDepth:   16,
			MaximumFutureSkew: 5 * time.Minute,
			MaximumEventAge:   90 * 24 * time.Hour,
		},
	})
	if err != nil {
		t.Fatalf("CreateIndex() error = %v", err)
	}
	if created.ID == "" || created.Definition.Name != "gradethis-prod" || created.Version != 1 || created.State != IndexStateActive {
		t.Fatalf("CreateIndex() = %#v", created)
	}
	if !created.CreatedAt.Equal(created.UpdatedAt) || created.CreatedAt.IsZero() {
		t.Fatalf("created timestamps = %v / %v", created.CreatedAt, created.UpdatedAt)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `UPDATE indexes SET name = 'renamed-directly' WHERE index_id = ?`, created.ID); err == nil {
		t.Fatal("direct immutable index-name update unexpectedly succeeded")
	}

	byName, err := db.GetIndexByName(ctx, " GRADETHIS-PROD ")
	if err != nil {
		t.Fatalf("GetIndexByName() error = %v", err)
	}
	if byName.ID != created.ID || byName.Definition != created.Definition {
		t.Fatalf("GetIndexByName() = %#v, want %#v", byName, created)
	}
	byID, err := db.GetIndex(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetIndex() error = %v", err)
	}
	if byID != byName {
		t.Fatalf("GetIndex() = %#v, want %#v", byID, byName)
	}

	replacement := created.Definition
	replacement.Name = "GRADETHIS-PROD" // same normalized immutable name
	replacement.DisplayName = "Production Logs"
	updated, err := db.UpdateIndex(ctx, created.ID, created.Version, replacement)
	if err != nil {
		t.Fatalf("UpdateIndex() error = %v", err)
	}
	if updated.Version != 2 || updated.Definition.DisplayName != "Production Logs" || updated.Definition.Name != created.Definition.Name {
		t.Fatalf("UpdateIndex() = %#v", updated)
	}

	_, err = db.UpdateIndex(ctx, created.ID, created.Version, replacement)
	if !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale UpdateIndex() error = %v, want ErrVersionConflict", err)
	}

	rename := replacement
	rename.Name = "renamed-index"
	_, err = db.UpdateIndex(ctx, created.ID, updated.Version, rename)
	if !errors.Is(err, ErrImmutableName) {
		t.Fatalf("renaming UpdateIndex() error = %v, want ErrImmutableName", err)
	}

	archived, err := db.SetIndexState(ctx, created.ID, updated.Version, IndexStateArchived)
	if err != nil {
		t.Fatalf("SetIndexState() error = %v", err)
	}
	if archived.Version != 3 || archived.State != IndexStateArchived {
		t.Fatalf("SetIndexState() = %#v", archived)
	}
}

func TestConcurrentIndexUpdatesAllowOneOptimisticWinner(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	created, err := db.CreateIndex(ctx, enabledIndex("concurrent"))
	if err != nil {
		t.Fatalf("CreateIndex(): %v", err)
	}

	start := make(chan struct{})
	results := make(chan error, 2)
	var wait sync.WaitGroup
	wait.Add(2)
	for _, displayName := range []string{"winner-a", "winner-b"} {
		definition := created.Definition
		definition.DisplayName = displayName
		go func() {
			defer wait.Done()
			<-start
			_, updateErr := db.UpdateIndex(ctx, created.ID, created.Version, definition)
			results <- updateErr
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	var successes, conflicts int
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrVersionConflict):
			conflicts++
		default:
			t.Errorf("UpdateIndex() unexpected error = %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("concurrent results: successes=%d conflicts=%d, want 1/1", successes, conflicts)
	}
	current, err := db.GetIndex(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetIndex(): %v", err)
	}
	if current.Version != 2 {
		t.Fatalf("current version = %d, want 2", current.Version)
	}
}

func TestCreateAndListIndexes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	for _, name := range []string{"z-last", "1-first", "middle_index"} {
		if _, err := db.CreateIndex(ctx, enabledIndex(name)); err != nil {
			t.Fatalf("CreateIndex(%q): %v", name, err)
		}
	}

	indexes, err := db.ListIndexes(ctx)
	if err != nil {
		t.Fatalf("ListIndexes() error = %v", err)
	}
	wantNames := []string{"1-first", "middle_index", "z-last"}
	if len(indexes) != len(wantNames) {
		t.Fatalf("ListIndexes() count = %d, want %d", len(indexes), len(wantNames))
	}
	for i, want := range wantNames {
		if indexes[i].Definition.Name != want {
			t.Errorf("ListIndexes()[%d].Name = %q, want %q", i, indexes[i].Definition.Name, want)
		}
	}

	_, err = db.CreateIndex(ctx, enabledIndex(" MIDDLE_INDEX "))
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("duplicate CreateIndex() error = %v, want ErrAlreadyExists", err)
	}
}

func TestNormalizeIndexNameHonorsSplunkRestrictions(t *testing.T) {
	t.Parallel()

	valid := map[string]string{
		" Main ":        "main",
		"123":           "123",
		"foo_bar-baz":   "foo_bar-baz",
		"UPPER-and_LOW": "upper-and_low",
	}
	for input, want := range valid {
		got, err := NormalizeIndexName(input)
		if err != nil || got != want {
			t.Errorf("NormalizeIndexName(%q) = %q, %v; want %q, nil", input, got, err, want)
		}
	}

	for _, input := range []string{"", " ", "_internal", "-leading", "has space", "has.dot", "café", "mykvstorelogs"} {
		if _, err := NormalizeIndexName(input); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("NormalizeIndexName(%q) error = %v, want ErrInvalidArgument", input, err)
		}
	}
}

func FuzzNormalizeIndexName(f *testing.F) {
	for _, seed := range []string{"main", " GRADETHIS-Prod ", "_internal", "café", "kvstore", "a/b"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		normalized, err := NormalizeIndexName(input)
		if err != nil {
			return
		}
		if normalized != strings.ToLower(normalized) || !splunkIndexName.MatchString(normalized) || strings.Contains(normalized, "kvstore") {
			t.Fatalf("NormalizeIndexName(%q) returned invalid canonical name %q", input, normalized)
		}
		second, err := NormalizeIndexName(normalized)
		if err != nil || second != normalized {
			t.Fatalf("normalization is not idempotent: first=%q second=%q err=%v", normalized, second, err)
		}
	})
}

func TestIndexValidationAndNotFoundErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)

	definition := enabledIndex("oversized")
	definition.Limits.MaxEventBytes = math.MaxUint64
	if _, err := db.CreateIndex(ctx, definition); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("oversized CreateIndex() error = %v, want ErrInvalidArgument", err)
	}
	if _, err := db.GetIndex(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetIndex(missing) error = %v, want ErrNotFound", err)
	}
	if _, err := db.GetIndexByName(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetIndexByName(missing) error = %v, want ErrNotFound", err)
	}
	if _, err := db.UpdateIndex(ctx, "missing", 1, enabledIndex("missing")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("UpdateIndex(missing) error = %v, want ErrNotFound", err)
	}
	if _, err := db.SetIndexState(ctx, "missing", 1, IndexStateArchived); !errors.Is(err, ErrNotFound) {
		t.Fatalf("SetIndexState(missing) error = %v, want ErrNotFound", err)
	}
	created, err := db.CreateIndex(ctx, IndexDefinition{Name: "default-display"})
	if err != nil {
		t.Fatalf("CreateIndex(default display): %v", err)
	}
	if created.Definition.DisplayName != "default-display" {
		t.Fatalf("default display name = %q", created.Definition.DisplayName)
	}
	if _, err := db.SetIndexState(ctx, created.ID, created.Version+1, IndexStateArchived); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("SetIndexState(stale) error = %v, want ErrVersionConflict", err)
	}
	if _, err := db.SetIndexState(ctx, created.ID, created.Version, IndexState("invented")); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("SetIndexState(invalid) error = %v, want ErrInvalidArgument", err)
	}
}

func openTestDB(t *testing.T) *DB {
	t.Helper()

	db, err := Open(context.Background(), filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close(): %v", err)
		}
	})
	return db
}

func enabledIndex(name string) IndexDefinition {
	return IndexDefinition{
		Name:             name,
		DisplayName:      name,
		IngestionEnabled: true,
		SearchEnabled:    true,
	}
}
