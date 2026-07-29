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

func TestDeleteIndexKeepDataCreatesTerminalTombstone(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	created, err := db.CreateIndex(ctx, enabledIndex("retained-events"))
	if err != nil {
		t.Fatalf("CreateIndex(): %v", err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `
		INSERT INTO ingestion_tokens (
			ingestion_token_id,
			version,
			name,
			token_prefix,
			token_digest,
			state,
			created_at_unix_micro,
			updated_at_unix_micro,
			bound_collector_id
		) VALUES (
			'token-retained',
			1,
			'retained',
			'prefix123',
			zeroblob(32),
			'active',
			1,
			1,
			'collector-1'
		)`,
	); err != nil {
		t.Fatalf("seed ingestion token: %v", err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `
		INSERT INTO ingestion_token_indexes (ingestion_token_id, index_id)
		VALUES ('token-retained', ?)`,
		created.ID,
	); err != nil {
		t.Fatalf("seed ingestion token index: %v", err)
	}
	archived, err := db.SetIndexState(ctx, created.ID, created.Version, IndexStateArchived)
	if err != nil {
		t.Fatalf("archive index: %v", err)
	}

	deletedID, err := db.DeleteIndex(
		ctx,
		archived.ID,
		archived.Version,
		archived.Definition.Name,
	)
	if err != nil {
		t.Fatalf("DeleteIndex(): %v", err)
	}
	if deletedID != archived.ID {
		t.Fatalf("DeleteIndex() ID = %q, want %q", deletedID, archived.ID)
	}

	if _, err := db.GetIndex(ctx, archived.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetIndex(tombstoned) error = %v, want ErrNotFound", err)
	}
	if _, err := db.GetIndexByName(ctx, archived.Definition.Name); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetIndexByName(tombstoned) error = %v, want ErrNotFound", err)
	}
	listed, err := db.ListIndexes(ctx)
	if err != nil {
		t.Fatalf("ListIndexes(): %v", err)
	}
	if len(listed) != 0 {
		t.Fatalf("ListIndexes() = %#v, want no visible indexes", listed)
	}

	var storedState IndexState
	var storedName string
	if err := db.SQLDB().QueryRowContext(ctx, `
		SELECT state, name
		FROM indexes
		WHERE index_id = ?`,
		archived.ID,
	).Scan(&storedState, &storedName); err != nil {
		t.Fatalf("read retained index row: %v", err)
	}
	if storedState != IndexStateArchived || storedName != archived.Definition.Name {
		t.Fatalf("retained index row = state %q name %q", storedState, storedName)
	}
	var tombstone indexDeletionTombstoneRecord
	if err := db.GORMDB().WithContext(ctx).
		Where("index_id = ?", archived.ID).
		Take(&tombstone).Error; err != nil {
		t.Fatalf("read deletion tombstone: %v", err)
	}
	if tombstone.IndexID != archived.ID ||
		tombstone.Name != archived.Definition.Name ||
		tombstone.DeletedVersion != int64(archived.Version) ||
		tombstone.DeletedAtUnixMicro <= 0 {
		t.Fatalf("deletion tombstone = %#v", tombstone)
	}
	var tokenReferenceCount int
	if err := db.SQLDB().QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM ingestion_token_indexes
		WHERE ingestion_token_id = 'token-retained' AND index_id = ?`,
		archived.ID,
	).Scan(&tokenReferenceCount); err != nil {
		t.Fatalf("count retained token references: %v", err)
	}
	if tokenReferenceCount != 1 {
		t.Fatalf("retained token references = %d, want 1", tokenReferenceCount)
	}

	if _, err := db.CreateIndex(ctx, enabledIndex(archived.Definition.Name)); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("CreateIndex(tombstoned name) error = %v, want ErrAlreadyExists", err)
	}
	if _, err := db.UpdateIndex(ctx, archived.ID, archived.Version, archived.Definition); !errors.Is(err, ErrNotFound) {
		t.Fatalf("UpdateIndex(tombstoned) error = %v, want ErrNotFound", err)
	}
	if _, err := db.SetIndexState(ctx, archived.ID, archived.Version, IndexStateActive); !errors.Is(err, ErrNotFound) {
		t.Fatalf("SetIndexState(tombstoned) error = %v, want ErrNotFound", err)
	}
	if _, err := db.DeleteIndex(ctx, archived.ID, archived.Version, archived.Definition.Name); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DeleteIndex(tombstoned) error = %v, want ErrNotFound", err)
	}
}

func TestDeleteIndexValidatesConfirmationVersionAndState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	active, err := db.CreateIndex(ctx, enabledIndex("active-delete"))
	if err != nil {
		t.Fatalf("CreateIndex(active): %v", err)
	}
	archived, err := db.CreateIndex(ctx, enabledIndex("archived-delete"))
	if err != nil {
		t.Fatalf("CreateIndex(archived): %v", err)
	}
	archived, err = db.SetIndexState(ctx, archived.ID, archived.Version, IndexStateArchived)
	if err != nil {
		t.Fatalf("archive index: %v", err)
	}
	deleting, err := db.CreateIndex(ctx, enabledIndex("physical-delete"))
	if err != nil {
		t.Fatalf("CreateIndex(deleting): %v", err)
	}
	deleting, err = db.SetIndexState(
		ctx,
		deleting.ID,
		deleting.Version,
		IndexStateArchived,
	)
	if err != nil {
		t.Fatalf("archive deleting index: %v", err)
	}
	if _, err := db.BeginIndexDataDeletion(
		ctx,
		deleting.ID,
		deleting.Version,
		deleting.Definition.Name,
	); err != nil {
		t.Fatalf("begin physical deletion: %v", err)
	}
	deleting, err = db.GetIndex(ctx, deleting.ID)
	if err != nil {
		t.Fatalf("read deleting index: %v", err)
	}

	tests := map[string]struct {
		id           string
		version      uint64
		confirmation string
		want         error
	}{
		"zero version": {
			id:           archived.ID,
			confirmation: archived.Definition.Name,
			want:         ErrInvalidArgument,
		},
		"blank index ID": {
			id:           " ",
			version:      1,
			confirmation: archived.Definition.Name,
			want:         ErrInvalidArgument,
		},
		"missing index": {
			id:           "missing",
			version:      1,
			confirmation: "missing",
			want:         ErrNotFound,
		},
		"stale version": {
			id:           archived.ID,
			version:      archived.Version - 1,
			confirmation: archived.Definition.Name,
			want:         ErrVersionConflict,
		},
		"active state": {
			id:           active.ID,
			version:      active.Version,
			confirmation: active.Definition.Name,
			want:         ErrDependencyConflict,
		},
		"deleting state": {
			id:           deleting.ID,
			version:      deleting.Version,
			confirmation: deleting.Definition.Name,
			want:         ErrDependencyConflict,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			_, deleteErr := db.DeleteIndex(
				ctx,
				test.id,
				test.version,
				test.confirmation,
			)
			if !errors.Is(deleteErr, test.want) {
				t.Fatalf("DeleteIndex() error = %v, want %v", deleteErr, test.want)
			}
		})
	}

	for _, confirmation := range []string{
		"",
		" archived-delete",
		"ARCHIVED-DELETE",
		"archived-delete ",
		"different-index",
	} {
		_, deleteErr := db.DeleteIndex(
			ctx,
			archived.ID,
			archived.Version,
			confirmation,
		)
		if !errors.Is(deleteErr, ErrInvalidArgument) {
			t.Errorf("DeleteIndex(confirmation %q) error = %v, want ErrInvalidArgument", confirmation, deleteErr)
		}
	}
}

func TestConcurrentIndexDeletionAllowsOneTerminalWinner(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	created, err := db.CreateIndex(ctx, enabledIndex("concurrent-delete"))
	if err != nil {
		t.Fatalf("CreateIndex(): %v", err)
	}
	archived, err := db.SetIndexState(ctx, created.ID, created.Version, IndexStateArchived)
	if err != nil {
		t.Fatalf("archive index: %v", err)
	}

	start := make(chan struct{})
	results := make(chan error, 2)
	var wait sync.WaitGroup
	wait.Add(2)
	for range 2 {
		go func() {
			defer wait.Done()
			<-start
			_, deleteErr := db.DeleteIndex(
				ctx,
				archived.ID,
				archived.Version,
				archived.Definition.Name,
			)
			results <- deleteErr
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	var successes, notFound int
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrNotFound):
			notFound++
		default:
			t.Errorf("DeleteIndex() unexpected error = %v", err)
		}
	}
	if successes != 1 || notFound != 1 {
		t.Fatalf("concurrent results: successes=%d not-found=%d, want 1/1", successes, notFound)
	}
}

func TestIndexDeletionTombstoneSQLGuards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	active, err := db.CreateIndex(ctx, enabledIndex("guard-active"))
	if err != nil {
		t.Fatalf("CreateIndex(active): %v", err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `
		INSERT INTO index_deletion_tombstones (
			index_id, name, deleted_version, deleted_at_unix_micro
		) VALUES (?, ?, ?, ?)`,
		active.ID,
		active.Definition.Name,
		active.Version,
		time.Now().UnixMicro(),
	); err == nil {
		t.Fatal("tombstone for active index unexpectedly succeeded")
	}
	if _, err := db.SQLDB().ExecContext(ctx, `
		INSERT INTO index_deletion_tombstones (
			index_id, name, deleted_version, deleted_at_unix_micro
		) VALUES ('missing', 'missing', 1, ?)`,
		time.Now().UnixMicro(),
	); err == nil {
		t.Fatal("tombstone for missing index unexpectedly succeeded")
	}

	archived, err := db.SetIndexState(ctx, active.ID, active.Version, IndexStateArchived)
	if err != nil {
		t.Fatalf("archive index: %v", err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `
		INSERT INTO index_deletion_tombstones (
			index_id, name, deleted_version, deleted_at_unix_micro
		) VALUES (?, 'wrong-name', ?, ?)`,
		archived.ID,
		archived.Version,
		time.Now().UnixMicro(),
	); err == nil {
		t.Fatal("tombstone with mismatched name unexpectedly succeeded")
	}
	if _, err := db.DeleteIndex(ctx, archived.ID, archived.Version, archived.Definition.Name); err != nil {
		t.Fatalf("DeleteIndex(): %v", err)
	}

	for name, statement := range map[string]string{
		"update index":     `UPDATE indexes SET state = 'active' WHERE index_id = ?`,
		"delete index":     `DELETE FROM indexes WHERE index_id = ?`,
		"update tombstone": `UPDATE index_deletion_tombstones SET name = 'changed' WHERE index_id = ?`,
		"delete tombstone": `DELETE FROM index_deletion_tombstones WHERE index_id = ?`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := db.SQLDB().ExecContext(ctx, statement, archived.ID); err == nil {
				t.Fatal("direct SQL mutation unexpectedly succeeded")
			}
		})
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
	subMillisecond := enabledIndex("sub-millisecond-retention")
	subMillisecond.RetentionPeriod = time.Nanosecond
	if _, err := db.CreateIndex(ctx, subMillisecond); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("sub-millisecond CreateIndex() error = %v, want ErrInvalidArgument", err)
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
	if _, err := db.SetIndexState(ctx, created.ID, created.Version, IndexStateDeleting); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("SetIndexState(deleting) error = %v, want ErrInvalidArgument", err)
	}
}

func TestIndexOperationsPreserveCanceledContextIdentity(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	created, err := db.CreateIndex(context.Background(), enabledIndex("canceled"))
	if err != nil {
		t.Fatalf("CreateIndex(): %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	tests := map[string]func() error{
		"create": func() error {
			_, operationErr := db.CreateIndex(ctx, enabledIndex("canceled-create"))
			return operationErr
		},
		"get": func() error {
			_, operationErr := db.GetIndex(ctx, created.ID)
			return operationErr
		},
		"get by name": func() error {
			_, operationErr := db.GetIndexByName(ctx, created.Definition.Name)
			return operationErr
		},
		"list": func() error {
			_, operationErr := db.ListIndexes(ctx)
			return operationErr
		},
		"update": func() error {
			_, operationErr := db.UpdateIndex(ctx, created.ID, created.Version, created.Definition)
			return operationErr
		},
		"set state": func() error {
			_, operationErr := db.SetIndexState(ctx, created.ID, created.Version, IndexStateArchived)
			return operationErr
		},
		"delete": func() error {
			_, operationErr := db.DeleteIndex(ctx, created.ID, created.Version, created.Definition.Name)
			return operationErr
		},
	}
	for name, operation := range tests {
		t.Run(name, func(t *testing.T) {
			if operationErr := operation(); !errors.Is(operationErr, context.Canceled) {
				t.Fatalf("operation error = %v, want context.Canceled", operationErr)
			}
		})
	}
}

func TestIndexReadsRejectCorruptRecordsWithoutLeakingFields(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	created, err := db.CreateIndex(ctx, IndexDefinition{
		Name:        "corrupt",
		Description: "private-description-sentinel",
	})
	if err != nil {
		t.Fatalf("CreateIndex(): %v", err)
	}

	connection, err := db.SQLDB().Conn(ctx)
	if err != nil {
		t.Fatalf("acquire corruption connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(ctx, `PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatalf("disable test-only check constraints: %v", err)
	}
	if _, err := connection.ExecContext(ctx, `
		UPDATE indexes
		SET ingestion_enabled = 2
		WHERE index_id = ?`, created.ID); err != nil {
		t.Fatalf("corrupt index record: %v", err)
	}

	_, err = db.GetIndex(ctx, created.ID)
	if err == nil || !strings.Contains(err.Error(), "invalid index record in control-plane database") {
		t.Fatalf("GetIndex() error = %v, want invalid-record error", err)
	}
	if strings.Contains(err.Error(), created.Definition.Description) {
		t.Fatalf("GetIndex() error disclosed persisted field: %v", err)
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
