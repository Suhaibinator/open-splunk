package control

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestBeginIndexDataDeletionAtomicallyPersistsRestartableIntent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scope := IndexDataDeletionScope{TenantID: "tenant-a"}
	path := filepath.Join(t.TempDir(), "control.sqlite")
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}

	created, err := db.CreateIndex(ctx, enabledIndex("physical-removal"))
	if err != nil {
		t.Fatalf("CreateIndex(): %v", err)
	}
	archived, err := db.SetIndexState(ctx, created.ID, created.Version, IndexStateArchived)
	if err != nil {
		t.Fatalf("archive index: %v", err)
	}
	operation, err := db.BeginIndexDataDeletion(
		ctx,
		scope,
		archived.ID,
		archived.Version,
		archived.Definition.Name,
	)
	if err != nil {
		t.Fatalf("BeginIndexDataDeletion(): %v", err)
	}
	if operation.ID == "" ||
		operation.TenantID != scope.TenantID ||
		operation.IndexID != archived.ID ||
		operation.IndexName != archived.Definition.Name ||
		operation.ArchivedVersion != archived.Version ||
		operation.DeletingVersion != archived.Version+1 ||
		operation.CreatedAt.IsZero() {
		t.Fatalf("operation = %#v", operation)
	}

	deleting, err := db.GetIndex(ctx, archived.ID)
	if err != nil {
		t.Fatalf("GetIndex(deleting): %v", err)
	}
	if deleting.State != IndexStateDeleting ||
		deleting.Version != operation.DeletingVersion ||
		deleting.Definition != archived.Definition ||
		!deleting.UpdatedAt.Equal(operation.CreatedAt) ||
		deleting.UpdatedAt.Before(archived.UpdatedAt) {
		t.Fatalf("deleting index = %#v, operation = %#v, archived = %#v", deleting, operation, archived)
	}

	retried, err := db.BeginIndexDataDeletion(
		ctx,
		scope,
		archived.ID,
		archived.Version,
		archived.Definition.Name,
	)
	if err != nil {
		t.Fatalf("idempotent BeginIndexDataDeletion(): %v", err)
	}
	if retried != operation {
		t.Fatalf("idempotent operation = %#v, want %#v", retried, operation)
	}
	unchanged, err := db.GetIndex(ctx, archived.ID)
	if err != nil {
		t.Fatalf("GetIndex(after retry): %v", err)
	}
	if unchanged != deleting {
		t.Fatalf("idempotent retry changed index: got %#v, want %#v", unchanged, deleting)
	}
	if _, err := db.BeginIndexDataDeletion(
		ctx,
		scope,
		archived.ID,
		operation.DeletingVersion,
		archived.Definition.Name,
	); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("retry with deleting version error = %v, want ErrVersionConflict", err)
	}
	if _, err := db.BeginIndexDataDeletion(
		ctx,
		scope,
		archived.ID,
		archived.Version,
		"different-index",
	); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("retry with changed confirmation error = %v, want ErrInvalidArgument", err)
	}
	if _, err := db.BeginIndexDataDeletion(
		ctx,
		IndexDataDeletionScope{TenantID: "tenant-b"},
		archived.ID,
		archived.Version,
		archived.Definition.Name,
	); !errors.Is(err, ErrDependencyConflict) {
		t.Fatalf("retry with changed tenant error = %v, want ErrDependencyConflict", err)
	}

	got, err := db.GetIndexDeletionOperation(ctx, operation.ID)
	if err != nil {
		t.Fatalf("GetIndexDeletionOperation(): %v", err)
	}
	if got != operation {
		t.Fatalf("GetIndexDeletionOperation() = %#v, want %#v", got, operation)
	}
	next, err := db.NextIndexDeletionOperation(ctx)
	if err != nil {
		t.Fatalf("NextIndexDeletionOperation(): %v", err)
	}
	if next != operation {
		t.Fatalf("NextIndexDeletionOperation() = %#v, want %#v", next, operation)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	db, err = Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	restarted, err := db.NextIndexDeletionOperation(ctx)
	if err != nil {
		t.Fatalf("NextIndexDeletionOperation(after restart): %v", err)
	}
	if restarted != operation {
		t.Fatalf("restarted operation = %#v, want %#v", restarted, operation)
	}
	if _, err := db.BeginIndexDataDeletion(
		ctx,
		IndexDataDeletionScope{TenantID: "tenant-b"},
		archived.ID,
		archived.Version,
		archived.Definition.Name,
	); !errors.Is(err, ErrDependencyConflict) {
		t.Fatalf(
			"cross-tenant retry after restart error = %v, want ErrDependencyConflict",
			err,
		)
	}
}

func TestBeginIndexDataDeletionRejectsInvalidTenantWithoutMutation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	created, err := db.CreateIndex(ctx, enabledIndex("tenant-validation"))
	if err != nil {
		t.Fatalf("CreateIndex(): %v", err)
	}
	archived, err := db.SetIndexState(
		ctx,
		created.ID,
		created.Version,
		IndexStateArchived,
	)
	if err != nil {
		t.Fatalf("archive index: %v", err)
	}

	invalidUTF8 := string([]byte{0xff})
	for name, tenantID := range map[string]string{
		"empty":               "",
		"leading whitespace":  " tenant",
		"trailing whitespace": "tenant ",
		"unicode whitespace":  "\u00a0tenant",
		"ASCII control":       "tenant\nother",
		"Unicode control":     "tenant\u0085other",
		"NUL":                 "tenant\x00other",
		"invalid UTF-8":       invalidUTF8,
		"oversized":           strings.Repeat("t", maximumTenantIDBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := db.BeginIndexDataDeletion(
				ctx,
				IndexDataDeletionScope{TenantID: tenantID},
				archived.ID,
				archived.Version,
				archived.Definition.Name,
			); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("error = %v, want ErrInvalidArgument", err)
			}
		})
	}

	var count int64
	if err := db.GORMDB().WithContext(ctx).
		Model(&indexDeletionOperationRecord{}).
		Count(&count).Error; err != nil {
		t.Fatalf("count operations: %v", err)
	}
	if count != 0 {
		t.Fatalf("invalid tenants persisted %d operations, want 0", count)
	}
	current, err := db.GetIndex(ctx, archived.ID)
	if err != nil {
		t.Fatalf("GetIndex(): %v", err)
	}
	if current != archived {
		t.Fatalf("invalid tenants changed index: got %#v, want %#v", current, archived)
	}
}

func TestBeginIndexDataDeletionSupportsFinalSQLiteVersion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	created, err := db.CreateIndex(ctx, enabledIndex("version-boundary"))
	if err != nil {
		t.Fatalf("CreateIndex(): %v", err)
	}
	archived, err := db.SetIndexState(ctx, created.ID, created.Version, IndexStateArchived)
	if err != nil {
		t.Fatalf("archive index: %v", err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `
		UPDATE indexes
		SET version = ?
		WHERE index_id = ?`,
		int64(math.MaxInt64-1),
		archived.ID,
	); err != nil {
		t.Fatalf("seed penultimate version: %v", err)
	}

	operation, err := db.BeginIndexDataDeletion(
		ctx,
		IndexDataDeletionScope{TenantID: "tenant"},
		archived.ID,
		math.MaxInt64-1,
		archived.Definition.Name,
	)
	if err != nil {
		t.Fatalf("BeginIndexDataDeletion(): %v", err)
	}
	if operation.ArchivedVersion != math.MaxInt64-1 ||
		operation.DeletingVersion != math.MaxInt64 {
		t.Fatalf("boundary operation = %#v", operation)
	}
	deleting, err := db.GetIndex(ctx, archived.ID)
	if err != nil {
		t.Fatalf("GetIndex(): %v", err)
	}
	if deleting.State != IndexStateDeleting ||
		deleting.Version != math.MaxInt64 {
		t.Fatalf("boundary deleting index = %#v", deleting)
	}
}

func TestBeginIndexDataDeletionClassifiesCompletedOperationIDCollision(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	completedOperation := createIndexDeletionOperation(
		t,
		db,
		"completed-operation-id",
	)
	completedAttempt, err := db.EnsureIndexDeletionMutationAttempt(
		ctx,
		completedOperation.ID,
		IndexDeletionMutationTarget{
			TenantID:  "tenant",
			Database:  "open_splunk",
			Table:     "events",
			TableUUID: "81234567-89ab-4cde-8fab-0123456789ab",
		},
	)
	if err != nil {
		t.Fatalf("EnsureIndexDeletionMutationAttempt(): %v", err)
	}
	if _, err := db.CompleteIndexDataDeletion(
		ctx,
		completedAttempt,
	); err != nil {
		t.Fatalf("CompleteIndexDataDeletion(): %v", err)
	}

	created, err := db.CreateIndex(ctx, enabledIndex("operation-id-collision"))
	if err != nil {
		t.Fatalf("CreateIndex(): %v", err)
	}
	archived, err := db.SetIndexState(
		ctx,
		created.ID,
		created.Version,
		IndexStateArchived,
	)
	if err != nil {
		t.Fatalf("archive index: %v", err)
	}
	if _, err := db.beginIndexDataDeletionTransaction(
		ctx,
		completedOperation.ID,
		"tenant",
		archived.ID,
		archived.Version,
		archived.Definition.Name,
		nil,
	); !errors.Is(err, errIndexDeletionOperationIDCollision) {
		t.Fatalf(
			"completed operation ID collision error = %v, want %v",
			err,
			errIndexDeletionOperationIDCollision,
		)
	}

	unchanged, err := db.GetIndex(ctx, archived.ID)
	if err != nil {
		t.Fatalf("GetIndex(after collision): %v", err)
	}
	if unchanged != archived {
		t.Fatalf(
			"completed ID collision changed index: got %#v, want %#v",
			unchanged,
			archived,
		)
	}
}

func TestBeginIndexDataDeletionValidatesExactArchivedIntent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	active, err := db.CreateIndex(ctx, enabledIndex("active-physical"))
	if err != nil {
		t.Fatalf("CreateIndex(active): %v", err)
	}
	archived, err := db.CreateIndex(ctx, enabledIndex("archived-physical"))
	if err != nil {
		t.Fatalf("CreateIndex(archived): %v", err)
	}
	archived, err = db.SetIndexState(ctx, archived.ID, archived.Version, IndexStateArchived)
	if err != nil {
		t.Fatalf("archive index: %v", err)
	}
	tombstoned, err := db.CreateIndex(ctx, enabledIndex("terminal-physical"))
	if err != nil {
		t.Fatalf("CreateIndex(tombstoned): %v", err)
	}
	tombstoned, err = db.SetIndexState(ctx, tombstoned.ID, tombstoned.Version, IndexStateArchived)
	if err != nil {
		t.Fatalf("archive tombstoned index: %v", err)
	}
	if _, err := db.DeleteIndex(ctx, tombstoned.ID, tombstoned.Version, tombstoned.Definition.Name); err != nil {
		t.Fatalf("DeleteIndex(): %v", err)
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
		"version ceiling": {
			id:           archived.ID,
			version:      math.MaxInt64,
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
		"tombstoned index": {
			id:           tombstoned.ID,
			version:      tombstoned.Version,
			confirmation: tombstoned.Definition.Name,
			want:         ErrNotFound,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			_, beginErr := db.BeginIndexDataDeletion(
				ctx,
				IndexDataDeletionScope{TenantID: "tenant"},
				test.id,
				test.version,
				test.confirmation,
			)
			if !errors.Is(beginErr, test.want) {
				t.Fatalf("BeginIndexDataDeletion() error = %v, want %v", beginErr, test.want)
			}
		})
	}

	for _, confirmation := range []string{
		"",
		" archived-physical",
		"ARCHIVED-PHYSICAL",
		"archived-physical ",
		"different-index",
	} {
		_, beginErr := db.BeginIndexDataDeletion(
			ctx,
			IndexDataDeletionScope{TenantID: "tenant"},
			archived.ID,
			archived.Version,
			confirmation,
		)
		if !errors.Is(beginErr, ErrInvalidArgument) {
			t.Errorf(
				"BeginIndexDataDeletion(confirmation %q) error = %v, want ErrInvalidArgument",
				confirmation,
				beginErr,
			)
		}
	}

	var operations int64
	if err := db.GORMDB().WithContext(ctx).
		Model(&indexDeletionOperationRecord{}).
		Count(&operations).Error; err != nil {
		t.Fatalf("count failed operations: %v", err)
	}
	if operations != 0 {
		t.Fatalf("failed admissions persisted %d operations, want 0", operations)
	}
	current, err := db.GetIndex(ctx, archived.ID)
	if err != nil {
		t.Fatalf("GetIndex(archived): %v", err)
	}
	if current != archived {
		t.Fatalf("failed admissions changed index: got %#v, want %#v", current, archived)
	}
}

func TestConcurrentIndexDataDeletionAdmissionsConvergeOnOneOperation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	created, err := db.CreateIndex(ctx, enabledIndex("concurrent-physical"))
	if err != nil {
		t.Fatalf("CreateIndex(): %v", err)
	}
	archived, err := db.SetIndexState(ctx, created.ID, created.Version, IndexStateArchived)
	if err != nil {
		t.Fatalf("archive index: %v", err)
	}

	const callers = 8
	start := make(chan struct{})
	results := make(chan IndexDeletionOperation, callers)
	errs := make(chan error, callers)
	var wait sync.WaitGroup
	wait.Add(callers)
	for range callers {
		go func() {
			defer wait.Done()
			<-start
			operation, beginErr := db.BeginIndexDataDeletion(
				ctx,
				IndexDataDeletionScope{TenantID: "tenant"},
				archived.ID,
				archived.Version,
				archived.Definition.Name,
			)
			results <- operation
			errs <- beginErr
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(errs)

	for beginErr := range errs {
		if beginErr != nil {
			t.Errorf("BeginIndexDataDeletion() error = %v", beginErr)
		}
	}
	var want IndexDeletionOperation
	for operation := range results {
		if want.ID == "" {
			want = operation
			continue
		}
		if operation != want {
			t.Errorf("concurrent operation = %#v, want %#v", operation, want)
		}
	}
	var count int64
	if err := db.GORMDB().WithContext(ctx).
		Model(&indexDeletionOperationRecord{}).
		Count(&count).Error; err != nil {
		t.Fatalf("count operations: %v", err)
	}
	if count != 1 {
		t.Fatalf("operation count = %d, want 1", count)
	}
	deleting, err := db.GetIndex(ctx, archived.ID)
	if err != nil {
		t.Fatalf("GetIndex(): %v", err)
	}
	if deleting.State != IndexStateDeleting || deleting.Version != archived.Version+1 {
		t.Fatalf("deleting index = %#v", deleting)
	}
}

func TestConcurrentIndexDataDeletionAdmissionsCannotRebindTenant(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	created, err := db.CreateIndex(ctx, enabledIndex("concurrent-tenants"))
	if err != nil {
		t.Fatalf("CreateIndex(): %v", err)
	}
	archived, err := db.SetIndexState(
		ctx,
		created.ID,
		created.Version,
		IndexStateArchived,
	)
	if err != nil {
		t.Fatalf("archive index: %v", err)
	}

	type admissionResult struct {
		tenantID  string
		operation IndexDeletionOperation
		err       error
	}
	start := make(chan struct{})
	results := make(chan admissionResult, 2)
	for _, tenantID := range []string{"tenant-a", "tenant-b"} {
		go func() {
			<-start
			operation, beginErr := db.BeginIndexDataDeletion(
				ctx,
				IndexDataDeletionScope{TenantID: tenantID},
				archived.ID,
				archived.Version,
				archived.Definition.Name,
			)
			results <- admissionResult{
				tenantID:  tenantID,
				operation: operation,
				err:       beginErr,
			}
		}()
	}
	close(start)

	var winner admissionResult
	var loser admissionResult
	for range 2 {
		result := <-results
		if result.err == nil {
			if winner.tenantID != "" {
				t.Fatalf("multiple tenant admissions succeeded: %#v and %#v", winner, result)
			}
			winner = result
			continue
		}
		loser = result
	}
	if winner.tenantID == "" {
		t.Fatalf("no tenant admission succeeded; loser = %#v", loser)
	}
	if !errors.Is(loser.err, ErrDependencyConflict) {
		t.Fatalf("losing tenant error = %v, want ErrDependencyConflict", loser.err)
	}
	if winner.operation.TenantID != winner.tenantID {
		t.Fatalf("winner operation = %#v, tenant = %q", winner.operation, winner.tenantID)
	}
	stored, err := db.GetIndexDeletionOperation(ctx, winner.operation.ID)
	if err != nil {
		t.Fatalf("GetIndexDeletionOperation(): %v", err)
	}
	if stored != winner.operation {
		t.Fatalf("stored operation = %#v, want %#v", stored, winner.operation)
	}
	deleting, err := db.GetIndex(ctx, archived.ID)
	if err != nil {
		t.Fatalf("GetIndex(): %v", err)
	}
	if deleting.State != IndexStateDeleting ||
		deleting.Version != archived.Version+1 {
		t.Fatalf("deleting index = %#v", deleting)
	}
}

func TestConcurrentPhysicalAndKeepDataDeletionLeaveOneDurableWinner(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	created, err := db.CreateIndex(ctx, enabledIndex("concurrent-delete-modes"))
	if err != nil {
		t.Fatalf("CreateIndex(): %v", err)
	}
	archived, err := db.SetIndexState(ctx, created.ID, created.Version, IndexStateArchived)
	if err != nil {
		t.Fatalf("archive index: %v", err)
	}

	start := make(chan struct{})
	physicalResult := make(chan error, 1)
	keepDataResult := make(chan error, 1)
	go func() {
		<-start
		_, beginErr := db.BeginIndexDataDeletion(
			ctx,
			IndexDataDeletionScope{TenantID: "tenant"},
			archived.ID,
			archived.Version,
			archived.Definition.Name,
		)
		physicalResult <- beginErr
	}()
	go func() {
		<-start
		_, deleteErr := db.DeleteIndex(
			ctx,
			archived.ID,
			archived.Version,
			archived.Definition.Name,
		)
		keepDataResult <- deleteErr
	}()
	close(start)
	physicalErr := <-physicalResult
	keepDataErr := <-keepDataResult
	if (physicalErr == nil) == (keepDataErr == nil) {
		t.Fatalf(
			"concurrent deletion results = physical %v, keep-data %v; want exactly one success",
			physicalErr,
			keepDataErr,
		)
	}
	if physicalErr != nil &&
		!errors.Is(physicalErr, ErrNotFound) &&
		!errors.Is(physicalErr, ErrVersionConflict) {
		t.Fatalf("physical deletion loser error = %v", physicalErr)
	}
	if keepDataErr != nil &&
		!errors.Is(keepDataErr, ErrNotFound) &&
		!errors.Is(keepDataErr, ErrVersionConflict) &&
		!errors.Is(keepDataErr, ErrDependencyConflict) {
		t.Fatalf("keep-data deletion loser error = %v", keepDataErr)
	}

	var operationCount, tombstoneCount int64
	if err := db.GORMDB().WithContext(ctx).
		Model(&indexDeletionOperationRecord{}).
		Where("index_id = ?", archived.ID).
		Count(&operationCount).Error; err != nil {
		t.Fatalf("count deletion operations: %v", err)
	}
	if err := db.GORMDB().WithContext(ctx).
		Model(&indexDeletionTombstoneRecord{}).
		Where("index_id = ?", archived.ID).
		Count(&tombstoneCount).Error; err != nil {
		t.Fatalf("count deletion tombstones: %v", err)
	}
	if operationCount+tombstoneCount != 1 {
		t.Fatalf(
			"durable deletion markers = operations %d, tombstones %d; want exactly one",
			operationCount,
			tombstoneCount,
		)
	}
	if operationCount == 1 {
		current, err := db.GetIndex(ctx, archived.ID)
		if err != nil {
			t.Fatalf("GetIndex(physical winner): %v", err)
		}
		if current.State != IndexStateDeleting ||
			current.Version != archived.Version+1 {
			t.Fatalf("physical winner index = %#v", current)
		}
		return
	}
	if _, err := db.GetIndex(ctx, archived.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetIndex(keep-data winner) error = %v, want ErrNotFound", err)
	}
}

func TestBeginIndexDataDeletionRollsBackWhenTransitionFails(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	created, err := db.CreateIndex(ctx, enabledIndex("transition-rollback"))
	if err != nil {
		t.Fatalf("CreateIndex(): %v", err)
	}
	archived, err := db.SetIndexState(ctx, created.ID, created.Version, IndexStateArchived)
	if err != nil {
		t.Fatalf("archive index: %v", err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `
		CREATE TRIGGER test_index_deletion_transition_failure
		AFTER UPDATE OF state ON indexes
		WHEN NEW.state = 'deleting'
		BEGIN
			SELECT RAISE(ABORT, 'injected deletion transition failure');
		END`,
	); err != nil {
		t.Fatalf("install transition failure: %v", err)
	}

	if _, err := db.BeginIndexDataDeletion(
		ctx,
		IndexDataDeletionScope{TenantID: "tenant"},
		archived.ID,
		archived.Version,
		archived.Definition.Name,
	); err == nil {
		t.Fatal("BeginIndexDataDeletion() unexpectedly succeeded")
	}
	var operationCount int64
	if err := db.GORMDB().WithContext(ctx).
		Model(&indexDeletionOperationRecord{}).
		Count(&operationCount).Error; err != nil {
		t.Fatalf("count rolled-back operations: %v", err)
	}
	if operationCount != 0 {
		t.Fatalf("rolled-back operation count = %d, want 0", operationCount)
	}
	current, err := db.GetIndex(ctx, archived.ID)
	if err != nil {
		t.Fatalf("GetIndex(): %v", err)
	}
	if current != archived {
		t.Fatalf("failed transition changed index: got %#v, want %#v", current, archived)
	}
}

func TestIndexDeletionOperationDiscoveryIsBoundedAndDeterministic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	operations := make([]IndexDeletionOperation, 0, 3)
	for _, name := range []string{"discovery-a", "discovery-b", "discovery-c"} {
		created, err := db.CreateIndex(ctx, enabledIndex(name))
		if err != nil {
			t.Fatalf("CreateIndex(%q): %v", name, err)
		}
		archived, err := db.SetIndexState(ctx, created.ID, created.Version, IndexStateArchived)
		if err != nil {
			t.Fatalf("archive %q: %v", name, err)
		}
		operation, err := db.BeginIndexDataDeletion(
			ctx,
			IndexDataDeletionScope{TenantID: "tenant"},
			archived.ID,
			archived.Version,
			archived.Definition.Name,
		)
		if err != nil {
			t.Fatalf("BeginIndexDataDeletion(%q): %v", name, err)
		}
		operations = append(operations, operation)
	}
	sort.Slice(operations, func(left, right int) bool {
		if operations[left].CreatedAt.Equal(operations[right].CreatedAt) {
			return operations[left].ID < operations[right].ID
		}
		return operations[left].CreatedAt.Before(operations[right].CreatedAt)
	})

	for range 3 {
		next, err := db.NextIndexDeletionOperation(ctx)
		if err != nil {
			t.Fatalf("NextIndexDeletionOperation(): %v", err)
		}
		if next != operations[0] {
			t.Fatalf("NextIndexDeletionOperation() = %#v, want oldest %#v", next, operations[0])
		}
	}
	if _, err := db.GetIndexDeletionOperation(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetIndexDeletionOperation(missing) error = %v, want ErrNotFound", err)
	}
	if _, err := db.GetIndexDeletionOperation(ctx, " "); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("GetIndexDeletionOperation(blank) error = %v, want ErrInvalidArgument", err)
	}
}

func TestIndexDeletionOperationSQLGuards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	created, err := db.CreateIndex(ctx, enabledIndex("operation-guards"))
	if err != nil {
		t.Fatalf("CreateIndex(): %v", err)
	}
	archived, err := db.SetIndexState(ctx, created.ID, created.Version, IndexStateArchived)
	if err != nil {
		t.Fatalf("archive index: %v", err)
	}

	if _, err := db.SQLDB().ExecContext(ctx, `
		UPDATE indexes
		SET state = 'deleting',
		    version = version + 1
		WHERE index_id = ?`,
		archived.ID,
	); err == nil {
		t.Fatal("entering deleting without an operation unexpectedly succeeded")
	}
	if _, err := db.SQLDB().ExecContext(ctx, `
		INSERT INTO indexes (
			index_id, version, name, display_name, ingestion_enabled,
			search_enabled, state, created_at_unix_micro, updated_at_unix_micro
		) VALUES ('direct-deleting', 1, 'direct-deleting', 'Direct', 0, 0, 'deleting', 1, 1)`,
	); err == nil {
		t.Fatal("inserting a deleting index without an operation unexpectedly succeeded")
	}

	createdAt := max(time.Now().UTC().UnixMicro(), archived.UpdatedAt.UnixMicro())
	invalidOperations := map[string]struct {
		statement string
		arguments []any
	}{
		"missing index": {
			statement: `
			INSERT INTO index_deletion_operations (
				deletion_operation_id, index_id, index_name,
				tenant_id, archived_index_version, created_at_unix_micro
			) VALUES ('idxdel_missing', 'missing', 'missing', 'tenant', 1, 1)`,
		},
		"wrong name": {
			statement: `
			INSERT INTO index_deletion_operations (
				deletion_operation_id, index_id, index_name,
				tenant_id, archived_index_version, created_at_unix_micro
			) VALUES ('idxdel_wrong_name', ?, 'wrong-name', 'tenant', ?, ?)`,
			arguments: []any{archived.ID, archived.Version, createdAt},
		},
		"wrong version": {
			statement: `
			INSERT INTO index_deletion_operations (
				deletion_operation_id, index_id, index_name,
				tenant_id, archived_index_version, created_at_unix_micro
			) VALUES ('idxdel_wrong_version', ?, ?, 'tenant', ?, ?)`,
			arguments: []any{
				archived.ID,
				archived.Definition.Name,
				archived.Version - 1,
				createdAt,
			},
		},
		"old timestamp": {
			statement: `
			INSERT INTO index_deletion_operations (
				deletion_operation_id, index_id, index_name,
				tenant_id, archived_index_version, created_at_unix_micro
			) VALUES ('idxdel_old_timestamp', ?, ?, 'tenant', ?, 1)`,
			arguments: []any{
				archived.ID,
				archived.Definition.Name,
				archived.Version,
			},
		},
		"blank operation ID": {
			statement: `
			INSERT INTO index_deletion_operations (
				deletion_operation_id, index_id, index_name,
				tenant_id, archived_index_version, created_at_unix_micro
			) VALUES (?, ?, ?, 'tenant', ?, ?)`,
			arguments: []any{
				" ",
				archived.ID,
				archived.Definition.Name,
				archived.Version,
				createdAt,
			},
		},
		"multibyte operation ID": {
			statement: `
			INSERT INTO index_deletion_operations (
				deletion_operation_id, index_id, index_name,
				tenant_id, archived_index_version, created_at_unix_micro
			) VALUES (?, ?, ?, 'tenant', ?, ?)`,
			arguments: []any{
				strings.Repeat("é", 64),
				archived.ID,
				archived.Definition.Name,
				archived.Version,
				createdAt,
			},
		},
		"nul operation ID": {
			statement: `
			INSERT INTO index_deletion_operations (
				deletion_operation_id, index_id, index_name,
				tenant_id, archived_index_version, created_at_unix_micro
			) VALUES (?, ?, ?, 'tenant', ?, ?)`,
			arguments: []any{
				"a\x00",
				archived.ID,
				archived.Definition.Name,
				archived.Version,
				createdAt,
			},
		},
	}
	for name, statement := range invalidOperations {
		t.Run(name, func(t *testing.T) {
			if _, err := db.SQLDB().ExecContext(
				ctx,
				statement.statement,
				statement.arguments...,
			); err == nil {
				t.Fatal("invalid operation insert unexpectedly succeeded")
			}
		})
	}
	for name, tenant := range map[string]struct {
		operationID string
		tenantID    string
	}{
		"blank": {
			operationID: "idxdel_blank_tenant",
		},
		"padded": {
			operationID: "idxdel_padded_tenant",
			tenantID:    " tenant",
		},
		"control": {
			operationID: "idxdel_control_tenant",
			tenantID:    "tenant\nother",
		},
		"unicode control": {
			operationID: "idxdel_unicode_control_tenant",
			tenantID:    "tenant\u0085other",
		},
		"NUL": {
			operationID: "idxdel_nul_tenant",
			tenantID:    "tenant\x00other",
		},
		"oversized": {
			operationID: "idxdel_large_tenant",
			tenantID:    strings.Repeat("t", maximumTenantIDBytes+1),
		},
	} {
		t.Run("tenant/"+name, func(t *testing.T) {
			if _, err := db.SQLDB().ExecContext(ctx, `
				INSERT INTO index_deletion_operations (
					deletion_operation_id, index_id, index_name,
					tenant_id, archived_index_version, created_at_unix_micro
				) VALUES (?, ?, ?, ?, ?, ?)`,
				tenant.operationID,
				archived.ID,
				archived.Definition.Name,
				tenant.tenantID,
				archived.Version,
				createdAt,
			); err == nil {
				t.Fatal("invalid tenant insert unexpectedly succeeded")
			}
		})
	}

	operation, err := db.BeginIndexDataDeletion(
		ctx,
		IndexDataDeletionScope{TenantID: "tenant"},
		archived.ID,
		archived.Version,
		archived.Definition.Name,
	)
	if err != nil {
		t.Fatalf("BeginIndexDataDeletion(): %v", err)
	}
	for name, statement := range map[string]string{
		"update operation": `
			UPDATE index_deletion_operations
			SET index_name = 'changed'
			WHERE deletion_operation_id = ?`,
		"update operation tenant": `
			UPDATE index_deletion_operations
			SET tenant_id = 'other'
			WHERE deletion_operation_id = ?`,
		"delete operation": `
			DELETE FROM index_deletion_operations
			WHERE deletion_operation_id = ?`,
		"update deleting index": `
			UPDATE indexes
			SET description = 'changed'
			WHERE index_id = ?`,
		"reactivate deleting index": `
			UPDATE indexes
			SET state = 'active', version = version + 1
			WHERE index_id = ?`,
		"delete deleting index": `
			DELETE FROM indexes
			WHERE index_id = ?`,
	} {
		t.Run(name, func(t *testing.T) {
			id := operation.ID
			if name == "update deleting index" ||
				name == "reactivate deleting index" ||
				name == "delete deleting index" {
				id = archived.ID
			}
			if _, err := db.SQLDB().ExecContext(ctx, statement, id); err == nil {
				t.Fatal("direct SQL mutation unexpectedly succeeded")
			}
		})
	}

	deleting, err := db.GetIndex(ctx, archived.ID)
	if err != nil {
		t.Fatalf("GetIndex(deleting): %v", err)
	}
	if _, err := db.UpdateIndex(
		ctx,
		deleting.ID,
		deleting.Version,
		deleting.Definition,
	); !errors.Is(err, ErrDependencyConflict) {
		t.Fatalf("UpdateIndex(deleting) error = %v, want ErrDependencyConflict", err)
	}
	if _, err := db.SetIndexState(
		ctx,
		deleting.ID,
		deleting.Version,
		IndexStateArchived,
	); !errors.Is(err, ErrDependencyConflict) {
		t.Fatalf("SetIndexState(deleting) error = %v, want ErrDependencyConflict", err)
	}
}

func TestIndexDeletionOperationDecoderRejectsInvalidTenant(t *testing.T) {
	t.Parallel()

	base := indexDeletionOperationRecord{
		DeletionOperationID:  "idxdel_decoder",
		IndexID:              "index-decoder",
		IndexName:            "index-decoder",
		TenantID:             "tenant",
		ArchivedIndexVersion: 1,
		CreatedAtUnixMicro:   1,
	}
	invalidUTF8 := string([]byte{0xff})
	for name, tenantID := range map[string]string{
		"empty":              "",
		"padded":             " tenant",
		"Unicode whitespace": "\u00a0tenant",
		"control":            "tenant\u0085other",
		"NUL":                "tenant\x00other",
		"invalid UTF-8":      invalidUTF8,
		"oversized":          strings.Repeat("t", maximumTenantIDBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			record := base
			record.TenantID = tenantID
			if _, err := indexDeletionOperationFromRecord(record); !errors.Is(
				err,
				errInvalidIndexDeletionOperation,
			) {
				t.Fatalf("error = %v, want errInvalidIndexDeletionOperation", err)
			}
		})
	}
}

func TestIndexDeletionOperationReplaceCannotOrphanDeletingIndex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	first, err := db.CreateIndex(ctx, enabledIndex("replace-first"))
	if err != nil {
		t.Fatalf("CreateIndex(first): %v", err)
	}
	first, err = db.SetIndexState(ctx, first.ID, first.Version, IndexStateArchived)
	if err != nil {
		t.Fatalf("archive first index: %v", err)
	}
	operation, err := db.BeginIndexDataDeletion(
		ctx,
		IndexDataDeletionScope{TenantID: "tenant"},
		first.ID,
		first.Version,
		first.Definition.Name,
	)
	if err != nil {
		t.Fatalf("BeginIndexDataDeletion(first): %v", err)
	}

	second, err := db.CreateIndex(ctx, enabledIndex("replace-second"))
	if err != nil {
		t.Fatalf("CreateIndex(second): %v", err)
	}
	second, err = db.SetIndexState(ctx, second.ID, second.Version, IndexStateArchived)
	if err != nil {
		t.Fatalf("archive second index: %v", err)
	}
	createdAt := max(time.Now().UTC().UnixMicro(), second.UpdatedAt.UnixMicro())
	if _, err := db.SQLDB().ExecContext(ctx, `
		INSERT OR REPLACE INTO index_deletion_operations (
			deletion_operation_id,
			index_id,
			index_name,
			tenant_id,
			archived_index_version,
			created_at_unix_micro
		) VALUES (?, ?, ?, ?, ?, ?)`,
		operation.ID,
		second.ID,
		second.Definition.Name,
		"tenant",
		second.Version,
		createdAt,
	); err == nil {
		t.Fatal("operation identity replacement unexpectedly succeeded")
	}

	currentFirst, err := db.GetIndex(ctx, first.ID)
	if err != nil {
		t.Fatalf("GetIndex(first): %v", err)
	}
	if currentFirst.State != IndexStateDeleting ||
		currentFirst.Version != operation.DeletingVersion {
		t.Fatalf("first index after replacement attempt = %#v", currentFirst)
	}
	currentSecond, err := db.GetIndex(ctx, second.ID)
	if err != nil {
		t.Fatalf("GetIndex(second): %v", err)
	}
	if currentSecond != second {
		t.Fatalf("second index after replacement attempt = %#v, want %#v", currentSecond, second)
	}
	gotOperation, err := db.GetIndexDeletionOperation(ctx, operation.ID)
	if err != nil {
		t.Fatalf("GetIndexDeletionOperation(): %v", err)
	}
	if gotOperation != operation {
		t.Fatalf("operation after replacement attempt = %#v, want %#v", gotOperation, operation)
	}
	var count int64
	if err := db.GORMDB().WithContext(ctx).
		Model(&indexDeletionOperationRecord{}).
		Count(&count).Error; err != nil {
		t.Fatalf("count operations: %v", err)
	}
	if count != 1 {
		t.Fatalf("operation count after replacement attempt = %d, want 1", count)
	}
}

func TestIndexDeletionOperationsPreserveCanceledContextAndAtomicity(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	created, err := db.CreateIndex(context.Background(), enabledIndex("canceled-physical"))
	if err != nil {
		t.Fatalf("CreateIndex(): %v", err)
	}
	archived, err := db.SetIndexState(
		context.Background(),
		created.ID,
		created.Version,
		IndexStateArchived,
	)
	if err != nil {
		t.Fatalf("archive index: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := db.BeginIndexDataDeletion(
		ctx,
		IndexDataDeletionScope{TenantID: "tenant"},
		archived.ID,
		archived.Version,
		archived.Definition.Name,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("BeginIndexDataDeletion(canceled) error = %v, want context.Canceled", err)
	}
	if _, err := db.GetIndexDeletionOperation(ctx, "idxdel_missing"); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetIndexDeletionOperation(canceled) error = %v, want context.Canceled", err)
	}
	if _, err := db.NextIndexDeletionOperation(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("NextIndexDeletionOperation(canceled) error = %v, want context.Canceled", err)
	}
	var count int64
	if err := db.GORMDB().WithContext(context.Background()).
		Model(&indexDeletionOperationRecord{}).
		Count(&count).Error; err != nil {
		t.Fatalf("count operations: %v", err)
	}
	if count != 0 {
		t.Fatalf("canceled admission persisted %d operations, want 0", count)
	}
	current, err := db.GetIndex(context.Background(), archived.ID)
	if err != nil {
		t.Fatalf("GetIndex(): %v", err)
	}
	if current != archived {
		t.Fatalf("canceled admission changed index: got %#v, want %#v", current, archived)
	}
}

func TestIndexDeletionOperationRelationshipReadPreservesCancellation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	created, err := db.CreateIndex(ctx, enabledIndex("relationship-canceled"))
	if err != nil {
		t.Fatalf("CreateIndex(): %v", err)
	}
	archived, err := db.SetIndexState(ctx, created.ID, created.Version, IndexStateArchived)
	if err != nil {
		t.Fatalf("archive index: %v", err)
	}
	operation, err := db.BeginIndexDataDeletion(
		ctx,
		IndexDataDeletionScope{TenantID: "tenant"},
		archived.ID,
		archived.Version,
		archived.Definition.Name,
	)
	if err != nil {
		t.Fatalf("BeginIndexDataDeletion(): %v", err)
	}
	var record indexDeletionOperationRecord
	if err := db.GORMDB().WithContext(ctx).
		Where("deletion_operation_id = ?", operation.ID).
		Take(&record).Error; err != nil {
		t.Fatalf("read operation record: %v", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := validatedIndexDeletionOperation(
		db.GORMDB().WithContext(canceled),
		record,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf(
			"validatedIndexDeletionOperation(canceled relationship read) error = %v, want context.Canceled",
			err,
		)
	}
}
