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

func TestCompleteIndexDataDeletionAtomicallyPersistsTerminalAudit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "control.sqlite")
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	operation := createIndexDeletionOperation(t, db, "terminal-physical")
	target := IndexDeletionMutationTarget{
		TenantID:  "tenant",
		Database:  "open_splunk",
		Table:     "events",
		TableUUID: "51234567-89ab-4cde-8fab-0123456789ab",
	}
	attempt, err := db.EnsureIndexDeletionMutationAttempt(
		ctx,
		operation.ID,
		target,
	)
	if err != nil {
		t.Fatalf("EnsureIndexDeletionMutationAttempt(): %v", err)
	}
	deleting, err := db.GetIndex(ctx, operation.IndexID)
	if err != nil {
		t.Fatalf("GetIndex(deleting): %v", err)
	}

	completion, err := db.CompleteIndexDataDeletion(
		ctx,
		attempt,
	)
	if err != nil {
		t.Fatalf("CompleteIndexDataDeletion(): %v", err)
	}
	if completion.DeletionOperationID != operation.ID ||
		completion.CorrelationID != attempt.CorrelationID ||
		completion.IndexID != operation.IndexID ||
		completion.IndexName != operation.IndexName ||
		completion.ArchivedVersion != operation.ArchivedVersion ||
		completion.DeletedVersion != operation.DeletingVersion ||
		completion.Target != attempt.Target ||
		completion.ProtocolVersion != attempt.ProtocolVersion ||
		!completion.OperationCreatedAt.Equal(operation.CreatedAt) ||
		!completion.MutationCreatedAt.Equal(attempt.CreatedAt) ||
		completion.CompletedAt.Before(attempt.CreatedAt) {
		t.Fatalf(
			"completion = %#v, operation = %#v, attempt = %#v",
			completion,
			operation,
			attempt,
		)
	}
	if _, err := db.GetIndex(ctx, operation.IndexID); !errors.Is(
		err,
		ErrNotFound,
	) {
		t.Fatalf("GetIndex(terminal) error = %v, want ErrNotFound", err)
	}
	var retained indexRecord
	if err := db.GORMDB().WithContext(ctx).
		Where("index_id = ?", operation.IndexID).
		Take(&retained).Error; err != nil {
		t.Fatalf("read retained terminal index: %v", err)
	}
	if retained.State != IndexStateDeleting ||
		uint64(retained.Version) != operation.DeletingVersion ||
		retained.UpdatedAtUnixMicro != deleting.UpdatedAt.UnixMicro() {
		t.Fatalf(
			"retained terminal index = %#v, deleting index = %#v",
			retained,
			deleting,
		)
	}
	var tombstone indexDeletionTombstoneRecord
	if err := db.GORMDB().WithContext(ctx).
		Where("index_id = ?", operation.IndexID).
		Take(&tombstone).Error; err != nil {
		t.Fatalf("read physical-deletion tombstone: %v", err)
	}
	if tombstone.Name != operation.IndexName ||
		uint64(tombstone.DeletedVersion) != operation.DeletingVersion ||
		tombstone.DeletedAtUnixMicro != completion.CompletedAt.UnixMicro() {
		t.Fatalf("tombstone = %#v, completion = %#v", tombstone, completion)
	}
	if _, err := db.GetIndexDeletionOperation(ctx, operation.ID); !errors.Is(
		err,
		ErrNotFound,
	) {
		t.Fatalf("GetIndexDeletionOperation() error = %v, want ErrNotFound", err)
	}
	if _, err := db.GetIndexDeletionMutationAttempt(
		ctx,
		operation.ID,
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf(
			"GetIndexDeletionMutationAttempt() error = %v, want ErrNotFound",
			err,
		)
	}
	if _, err := db.NextIndexDeletionOperation(ctx); !errors.Is(
		err,
		ErrNotFound,
	) {
		t.Fatalf("NextIndexDeletionOperation() error = %v, want ErrNotFound", err)
	}
	if _, err := db.CreateIndex(
		ctx,
		enabledIndex(operation.IndexName),
	); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf(
			"CreateIndex(reserved terminal name) error = %v, want ErrAlreadyExists",
			err,
		)
	}
	rows, err := db.SQLDB().QueryContext(ctx, `PRAGMA foreign_key_check`)
	if err != nil {
		t.Fatalf("foreign_key_check: %v", err)
	}
	foreignKeyViolation := rows.Next()
	iterationErr := rows.Err()
	closeErr := rows.Close()
	if iterationErr != nil {
		t.Fatalf("iterate foreign_key_check: %v", iterationErr)
	}
	if closeErr != nil {
		t.Fatalf("close foreign_key_check: %v", closeErr)
	}
	if foreignKeyViolation {
		t.Fatal("foreign_key_check found a terminal deletion violation")
	}

	got, err := db.GetIndexDataDeletionCompletion(ctx, operation.ID)
	if err != nil {
		t.Fatalf("GetIndexDataDeletionCompletion(): %v", err)
	}
	if got != completion {
		t.Fatalf("GetIndexDataDeletionCompletion() = %#v, want %#v", got, completion)
	}
	retried, err := db.CompleteIndexDataDeletion(
		ctx,
		attempt,
	)
	if err != nil {
		t.Fatalf("idempotent CompleteIndexDataDeletion(): %v", err)
	}
	if retried != completion {
		t.Fatalf("idempotent completion = %#v, want %#v", retried, completion)
	}
	changed := attempt
	changed.CorrelationID = "idxmut_wrong"
	if _, err := db.CompleteIndexDataDeletion(
		ctx,
		changed,
	); !errors.Is(err, ErrDependencyConflict) {
		t.Fatalf(
			"changed correlation completion error = %v, want ErrDependencyConflict",
			err,
		)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	db, err = Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	restarted, err := db.CompleteIndexDataDeletion(
		ctx,
		attempt,
	)
	if err != nil {
		t.Fatalf("CompleteIndexDataDeletion(after restart): %v", err)
	}
	if restarted != completion {
		t.Fatalf("restarted completion = %#v, want %#v", restarted, completion)
	}
}

func TestCompleteIndexDataDeletionSupportsFinalSQLiteVersion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	created, err := db.CreateIndex(ctx, enabledIndex("terminal-version-boundary"))
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
		archived.ID,
		math.MaxInt64-1,
		archived.Definition.Name,
	)
	if err != nil {
		t.Fatalf("BeginIndexDataDeletion(): %v", err)
	}
	attempt, err := db.EnsureIndexDeletionMutationAttempt(
		ctx,
		operation.ID,
		IndexDeletionMutationTarget{
			TenantID:  "tenant",
			Database:  "open_splunk",
			Table:     "events",
			TableUUID: "61234567-89ab-4cde-8fab-0123456789ab",
		},
	)
	if err != nil {
		t.Fatalf("EnsureIndexDeletionMutationAttempt(): %v", err)
	}
	completion, err := db.CompleteIndexDataDeletion(
		ctx,
		attempt,
	)
	if err != nil {
		t.Fatalf("CompleteIndexDataDeletion(): %v", err)
	}
	if completion.ArchivedVersion != math.MaxInt64-1 ||
		completion.DeletedVersion != math.MaxInt64 {
		t.Fatalf("boundary completion = %#v", completion)
	}
}

func TestCompleteIndexDataDeletionSupportsOpaqueSchemaValidIndexID(
	t *testing.T,
) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	created, err := db.CreateIndex(ctx, enabledIndex("terminal-opaque-id"))
	if err != nil {
		t.Fatalf("CreateIndex(): %v", err)
	}
	opaqueID := strings.Repeat("i", 256) + "\x00opaque"
	if _, err := db.SQLDB().ExecContext(ctx, `
		UPDATE indexes
		SET index_id = ?
		WHERE index_id = ?`,
		opaqueID,
		created.ID,
	); err != nil {
		t.Fatalf("seed opaque index ID: %v", err)
	}
	archived, err := db.SetIndexState(
		ctx,
		opaqueID,
		created.Version,
		IndexStateArchived,
	)
	if err != nil {
		t.Fatalf("archive opaque-ID index: %v", err)
	}
	operation, err := db.BeginIndexDataDeletion(
		ctx,
		opaqueID,
		archived.Version,
		archived.Definition.Name,
	)
	if err != nil {
		t.Fatalf("BeginIndexDataDeletion(): %v", err)
	}
	attempt, err := db.EnsureIndexDeletionMutationAttempt(
		ctx,
		operation.ID,
		IndexDeletionMutationTarget{
			TenantID:  "tenant",
			Database:  "open_splunk",
			Table:     "events",
			TableUUID: "11234567-89ab-4cde-8fab-1123456789ab",
		},
	)
	if err != nil {
		t.Fatalf("EnsureIndexDeletionMutationAttempt(): %v", err)
	}
	completion, err := db.CompleteIndexDataDeletion(ctx, attempt)
	if err != nil {
		t.Fatalf("CompleteIndexDataDeletion(): %v", err)
	}
	if completion.IndexID != opaqueID {
		t.Fatalf(
			"completion index ID byte length = %d, want opaque ID length %d",
			len(completion.IndexID),
			len(opaqueID),
		)
	}
}

func TestCompletedIndexDataDeletionMakesStaleOutstandingReadsNotFound(
	t *testing.T,
) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	operation := createIndexDeletionOperation(t, db, "terminal-stale-read")
	attempt, err := db.EnsureIndexDeletionMutationAttempt(
		ctx,
		operation.ID,
		IndexDeletionMutationTarget{
			TenantID:  "tenant",
			Database:  "open_splunk",
			Table:     "events",
			TableUUID: "31234567-89ab-4cde-8fab-1123456789ab",
		},
	)
	if err != nil {
		t.Fatalf("EnsureIndexDeletionMutationAttempt(): %v", err)
	}
	var operationRecord indexDeletionOperationRecord
	if err := db.GORMDB().WithContext(ctx).
		Where("deletion_operation_id = ?", operation.ID).
		Take(&operationRecord).Error; err != nil {
		t.Fatalf("read stale operation record: %v", err)
	}
	var attemptRecord indexDeletionMutationAttemptRecord
	if err := db.GORMDB().WithContext(ctx).
		Where("deletion_operation_id = ?", operation.ID).
		Take(&attemptRecord).Error; err != nil {
		t.Fatalf("read stale attempt record: %v", err)
	}
	if _, err := db.CompleteIndexDataDeletion(ctx, attempt); err != nil {
		t.Fatalf("CompleteIndexDataDeletion(): %v", err)
	}

	if _, err := validatedIndexDeletionOperation(
		db.GORMDB().WithContext(ctx),
		operationRecord,
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf(
			"validatedIndexDeletionOperation(stale) error = %v, want ErrNotFound",
			err,
		)
	}
	if _, err := validatedIndexDeletionMutationAttempt(
		db.GORMDB().WithContext(ctx),
		attemptRecord,
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf(
			"validatedIndexDeletionMutationAttempt(stale) error = %v, want ErrNotFound",
			err,
		)
	}
}

func TestCompleteIndexDataDeletionRollsBackEveryTerminalEffect(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	operation := createIndexDeletionOperation(t, db, "terminal-rollback")
	attempt, err := db.EnsureIndexDeletionMutationAttempt(
		ctx,
		operation.ID,
		IndexDeletionMutationTarget{
			TenantID:  "tenant",
			Database:  "open_splunk",
			Table:     "events",
			TableUUID: "71234567-89ab-4cde-8fab-0123456789ab",
		},
	)
	if err != nil {
		t.Fatalf("EnsureIndexDeletionMutationAttempt(): %v", err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `
		CREATE TRIGGER test_terminal_tombstone_failure
		BEFORE INSERT ON index_deletion_tombstones
		BEGIN
			SELECT RAISE(ABORT, 'injected terminal tombstone failure');
		END`,
	); err != nil {
		t.Fatalf("install terminal failure: %v", err)
	}

	if _, err := db.CompleteIndexDataDeletion(
		ctx,
		attempt,
	); err == nil {
		t.Fatal("CompleteIndexDataDeletion() unexpectedly succeeded")
	}
	if _, err := db.GetIndexDataDeletionCompletion(
		ctx,
		operation.ID,
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rolled-back completion error = %v, want ErrNotFound", err)
	}
	if got, err := db.GetIndexDeletionOperation(
		ctx,
		operation.ID,
	); err != nil || got != operation {
		t.Fatalf("operation after rollback = %#v, error=%v", got, err)
	}
	if got, err := db.GetIndexDeletionMutationAttempt(
		ctx,
		operation.ID,
	); err != nil || got != attempt {
		t.Fatalf("attempt after rollback = %#v, error=%v", got, err)
	}
	if index, err := db.GetIndex(
		ctx,
		operation.IndexID,
	); err != nil || index.State != IndexStateDeleting {
		t.Fatalf("index after rollback = %#v, error=%v", index, err)
	}
	var tombstones int64
	if err := db.GORMDB().WithContext(ctx).
		Model(&indexDeletionTombstoneRecord{}).
		Where("index_id = ?", operation.IndexID).
		Count(&tombstones).Error; err != nil {
		t.Fatalf("count tombstones after rollback: %v", err)
	}
	if tombstones != 0 {
		t.Fatalf("tombstones after rollback = %d, want 0", tombstones)
	}
}

func TestCompleteIndexDataDeletionRollsBackWhenOperationCleanupFails(
	t *testing.T,
) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	operation := createIndexDeletionOperation(t, db, "terminal-cleanup-rollback")
	attempt, err := db.EnsureIndexDeletionMutationAttempt(
		ctx,
		operation.ID,
		IndexDeletionMutationTarget{
			TenantID:  "tenant",
			Database:  "open_splunk",
			Table:     "events",
			TableUUID: "91234567-89ab-4cde-8fab-0123456789ab",
		},
	)
	if err != nil {
		t.Fatalf("EnsureIndexDeletionMutationAttempt(): %v", err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `
		CREATE TRIGGER test_terminal_operation_cleanup_failure
		BEFORE DELETE ON index_deletion_operations
		BEGIN
			SELECT RAISE(ABORT, 'injected operation cleanup failure');
		END`,
	); err != nil {
		t.Fatalf("install cleanup failure: %v", err)
	}

	if _, err := db.CompleteIndexDataDeletion(ctx, attempt); err == nil {
		t.Fatal("CompleteIndexDataDeletion() unexpectedly succeeded")
	}
	assertOutstandingIndexDeletion(t, db, operation, attempt)
}

func TestCompleteIndexDataDeletionRollsBackWhenAttemptCleanupFails(
	t *testing.T,
) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	operation := createIndexDeletionOperation(t, db, "terminal-attempt-rollback")
	attempt, err := db.EnsureIndexDeletionMutationAttempt(
		ctx,
		operation.ID,
		IndexDeletionMutationTarget{
			TenantID:  "tenant",
			Database:  "open_splunk",
			Table:     "events",
			TableUUID: "21234567-89ab-4cde-8fab-1123456789ab",
		},
	)
	if err != nil {
		t.Fatalf("EnsureIndexDeletionMutationAttempt(): %v", err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `
		CREATE TRIGGER test_terminal_attempt_cleanup_failure
		BEFORE DELETE ON index_deletion_mutation_attempts
		BEGIN
			SELECT RAISE(ABORT, 'injected attempt cleanup failure');
		END`,
	); err != nil {
		t.Fatalf("install attempt cleanup failure: %v", err)
	}

	if _, err := db.CompleteIndexDataDeletion(ctx, attempt); err == nil {
		t.Fatal("CompleteIndexDataDeletion() unexpectedly succeeded")
	}
	assertOutstandingIndexDeletion(t, db, operation, attempt)
	if _, err := db.SQLDB().ExecContext(
		ctx,
		`DROP TRIGGER test_terminal_attempt_cleanup_failure`,
	); err != nil {
		t.Fatalf("remove attempt cleanup failure: %v", err)
	}
	completion, err := db.CompleteIndexDataDeletion(ctx, attempt)
	if err != nil {
		t.Fatalf("CompleteIndexDataDeletion(retry): %v", err)
	}
	if completion.DeletionOperationID != operation.ID {
		t.Fatalf("retry completion = %#v, operation = %#v", completion, operation)
	}
}

func TestCompleteIndexDataDeletionConvergesConcurrently(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	operation := createIndexDeletionOperation(t, db, "terminal-concurrent")
	attempt, err := db.EnsureIndexDeletionMutationAttempt(
		ctx,
		operation.ID,
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

	const callers = 16
	start := make(chan struct{})
	results := make(chan IndexDataDeletionCompletion, callers)
	failures := make(chan error, callers)
	var wait sync.WaitGroup
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			completion, completeErr := db.CompleteIndexDataDeletion(
				ctx,
				attempt,
			)
			results <- completion
			failures <- completeErr
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(failures)
	for err := range failures {
		if err != nil {
			t.Errorf("concurrent CompleteIndexDataDeletion(): %v", err)
		}
	}
	var want IndexDataDeletionCompletion
	for result := range results {
		if want == (IndexDataDeletionCompletion{}) {
			want = result
			continue
		}
		if result != want {
			t.Errorf("concurrent completion = %#v, want %#v", result, want)
		}
	}
	if want == (IndexDataDeletionCompletion{}) {
		t.Fatal("no concurrent completion succeeded")
	}
	var completions, tombstones int64
	if err := db.GORMDB().WithContext(ctx).
		Model(&indexDataDeletionCompletionRecord{}).
		Count(&completions).Error; err != nil {
		t.Fatalf("count completions: %v", err)
	}
	if err := db.GORMDB().WithContext(ctx).
		Model(&indexDeletionTombstoneRecord{}).
		Count(&tombstones).Error; err != nil {
		t.Fatalf("count tombstones: %v", err)
	}
	if completions != 1 || tombstones != 1 {
		t.Fatalf(
			"terminal rows = completions %d tombstones %d, want 1/1",
			completions,
			tombstones,
		)
	}
}

func TestCompleteIndexDataDeletionRetryDoesNotReserveSQLiteWriter(
	t *testing.T,
) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	operation := createIndexDeletionOperation(t, db, "terminal-read-retry")
	attempt, err := db.EnsureIndexDeletionMutationAttempt(
		ctx,
		operation.ID,
		IndexDeletionMutationTarget{
			TenantID:  "tenant",
			Database:  "open_splunk",
			Table:     "events",
			TableUUID: "e1234567-89ab-4cde-8fab-0123456789ab",
		},
	)
	if err != nil {
		t.Fatalf("EnsureIndexDeletionMutationAttempt(): %v", err)
	}
	completion, err := db.CompleteIndexDataDeletion(ctx, attempt)
	if err != nil {
		t.Fatalf("CompleteIndexDataDeletion(): %v", err)
	}

	writer, err := db.SQLDB().Conn(ctx)
	if err != nil {
		t.Fatalf("acquire writer connection: %v", err)
	}
	if _, err := writer.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		if closeErr := writer.Close(); closeErr != nil {
			t.Fatalf(
				"reserve SQLite writer: %v; close connection: %v",
				err,
				closeErr,
			)
		}
		t.Fatalf("reserve SQLite writer: %v", err)
	}
	defer func() {
		if _, err := writer.ExecContext(
			context.Background(),
			`ROLLBACK`,
		); err != nil {
			t.Errorf("release SQLite writer: %v", err)
		}
		if err := writer.Close(); err != nil {
			t.Errorf("close writer connection: %v", err)
		}
	}()

	retryContext, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	retried, err := db.CompleteIndexDataDeletion(retryContext, attempt)
	if err != nil {
		t.Fatalf(
			"CompleteIndexDataDeletion(with reserved writer): %v",
			err,
		)
	}
	if retried != completion {
		t.Fatalf("read-only retry = %#v, want %#v", retried, completion)
	}
}

func TestIndexDataDeletionCompletionRelationshipReadPreservesCancellation(
	t *testing.T,
) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	operation := createIndexDeletionOperation(t, db, "terminal-read-canceled")
	attempt, err := db.EnsureIndexDeletionMutationAttempt(
		ctx,
		operation.ID,
		IndexDeletionMutationTarget{
			TenantID:  "tenant",
			Database:  "open_splunk",
			Table:     "events",
			TableUUID: "f1234567-89ab-4cde-8fab-0123456789ab",
		},
	)
	if err != nil {
		t.Fatalf("EnsureIndexDeletionMutationAttempt(): %v", err)
	}
	if _, err := db.CompleteIndexDataDeletion(ctx, attempt); err != nil {
		t.Fatalf("CompleteIndexDataDeletion(): %v", err)
	}
	var record indexDataDeletionCompletionRecord
	if err := db.GORMDB().WithContext(ctx).
		Where("deletion_operation_id = ?", operation.ID).
		Take(&record).Error; err != nil {
		t.Fatalf("read completion record: %v", err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := validatedIndexDataDeletionCompletion(
		db.GORMDB().WithContext(canceled),
		record,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf(
			"validatedIndexDataDeletionCompletion(canceled) error = %v, want context.Canceled",
			err,
		)
	}
}

func TestCompleteIndexDataDeletionValidatesExactAttempt(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	operation := createIndexDeletionOperation(t, db, "terminal-validation")
	attempt, err := db.EnsureIndexDeletionMutationAttempt(
		ctx,
		operation.ID,
		IndexDeletionMutationTarget{
			TenantID:  "tenant",
			Database:  "open_splunk",
			Table:     "events",
			TableUUID: "a1234567-89ab-4cde-8fab-0123456789ab",
		},
	)
	if err != nil {
		t.Fatalf("EnsureIndexDeletionMutationAttempt(): %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*IndexDeletionMutationAttempt)
		want   error
	}{
		{
			name: "unknown operation",
			mutate: func(value *IndexDeletionMutationAttempt) {
				value.DeletionOperationID = "idxdel_missing"
			},
			want: ErrNotFound,
		},
		{
			name: "invalid operation",
			mutate: func(value *IndexDeletionMutationAttempt) {
				value.DeletionOperationID = "../operation"
			},
			want: ErrInvalidArgument,
		},
		{
			name: "correlation drift",
			mutate: func(value *IndexDeletionMutationAttempt) {
				value.CorrelationID = "idxmut_drift"
			},
			want: ErrDependencyConflict,
		},
		{
			name: "index drift",
			mutate: func(value *IndexDeletionMutationAttempt) {
				value.IndexID = "idx_drift"
			},
			want: ErrDependencyConflict,
		},
		{
			name: "name drift",
			mutate: func(value *IndexDeletionMutationAttempt) {
				value.IndexName = "terminal-validation-drift"
			},
			want: ErrDependencyConflict,
		},
		{
			name: "tenant drift",
			mutate: func(value *IndexDeletionMutationAttempt) {
				value.Target.TenantID = "other"
			},
			want: ErrDependencyConflict,
		},
		{
			name: "table UUID drift",
			mutate: func(value *IndexDeletionMutationAttempt) {
				value.Target.TableUUID =
					"b1234567-89ab-4cde-8fab-0123456789ab"
			},
			want: ErrDependencyConflict,
		},
		{
			name: "created time drift",
			mutate: func(value *IndexDeletionMutationAttempt) {
				value.CreatedAt = value.CreatedAt.Add(time.Microsecond)
			},
			want: ErrDependencyConflict,
		},
		{
			name: "sub-microsecond time",
			mutate: func(value *IndexDeletionMutationAttempt) {
				value.CreatedAt = value.CreatedAt.Add(time.Nanosecond)
			},
			want: ErrInvalidArgument,
		},
		{
			name: "unsupported protocol",
			mutate: func(value *IndexDeletionMutationAttempt) {
				value.ProtocolVersion++
			},
			want: ErrInvalidArgument,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := attempt
			test.mutate(&changed)
			if _, err := db.CompleteIndexDataDeletion(
				ctx,
				changed,
			); !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := db.CompleteIndexDataDeletion(
		canceled,
		attempt,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf(
			"CompleteIndexDataDeletion(canceled) error = %v, want context.Canceled",
			err,
		)
	}
	if _, err := db.GetIndexDataDeletionCompletion(
		canceled,
		operation.ID,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf(
			"GetIndexDataDeletionCompletion(canceled) error = %v, want context.Canceled",
			err,
		)
	}
	if _, err := db.GetIndexDataDeletionCompletion(
		ctx,
		"../operation",
	); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf(
			"GetIndexDataDeletionCompletion(invalid) error = %v, want ErrInvalidArgument",
			err,
		)
	}
	assertOutstandingIndexDeletion(t, db, operation, attempt)
}

func TestIndexDataDeletionCompletionSchemaRejectsForgeryAndReplacement(
	t *testing.T,
) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	target := IndexDeletionMutationTarget{
		TenantID:  "tenant",
		Database:  "open_splunk",
		Table:     "events",
		TableUUID: "c1234567-89ab-4cde-8fab-0123456789ab",
	}
	directOperation := createIndexDeletionOperation(
		t,
		db,
		"terminal-direct-tombstone",
	)
	directAttempt, err := db.EnsureIndexDeletionMutationAttempt(
		ctx,
		directOperation.ID,
		target,
	)
	if err != nil {
		t.Fatalf("EnsureIndexDeletionMutationAttempt(direct): %v", err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `
		INSERT INTO index_deletion_tombstones (
			index_id,
			name,
			deleted_version,
			deleted_at_unix_micro
		) VALUES (?, ?, ?, ?)`,
		directOperation.IndexID,
		directOperation.IndexName,
		directOperation.DeletingVersion,
		directAttempt.CreatedAt.UnixMicro(),
	); err == nil {
		t.Fatal("physical tombstone without completion unexpectedly succeeded")
	}
	assertOutstandingIndexDeletion(t, db, directOperation, directAttempt)

	for name, mutate := range map[string]func(
		*indexDataDeletionCompletionRecord,
	){
		"correlation": func(record *indexDataDeletionCompletionRecord) {
			record.CorrelationID = "idxmut_forged"
		},
		"index name": func(record *indexDataDeletionCompletionRecord) {
			record.IndexName = "forged-name"
		},
		"versions": func(record *indexDataDeletionCompletionRecord) {
			record.ArchivedIndexVersion++
			record.DeletingIndexVersion++
		},
		"tenant": func(record *indexDataDeletionCompletionRecord) {
			record.TenantID = "other"
		},
		"table UUID": func(record *indexDataDeletionCompletionRecord) {
			record.ClickHouseTableUUID =
				"d1234567-89ab-4cde-8fab-0123456789ab"
		},
		"operation timestamp": func(record *indexDataDeletionCompletionRecord) {
			record.OperationCreatedAtUnixMicro--
		},
		"attempt timestamp": func(record *indexDataDeletionCompletionRecord) {
			record.AttemptCreatedAtUnixMicro--
		},
	} {
		t.Run(name, func(t *testing.T) {
			operation := createIndexDeletionOperation(
				t,
				db,
				"terminal-forgery-"+strings.ReplaceAll(name, " ", "-"),
			)
			attempt, err := db.EnsureIndexDeletionMutationAttempt(
				ctx,
				operation.ID,
				target,
			)
			if err != nil {
				t.Fatalf("EnsureIndexDeletionMutationAttempt(): %v", err)
			}
			record := completionRecordForTest(operation, attempt)
			mutate(&record)
			if err := db.GORMDB().WithContext(ctx).
				Create(&record).Error; err == nil {
				t.Fatal("forged completion unexpectedly succeeded")
			}
			assertOutstandingIndexDeletion(t, db, operation, attempt)
		})
	}

	operation := createIndexDeletionOperation(t, db, "terminal-immutable")
	attempt, err := db.EnsureIndexDeletionMutationAttempt(
		ctx,
		operation.ID,
		target,
	)
	if err != nil {
		t.Fatalf("EnsureIndexDeletionMutationAttempt(): %v", err)
	}
	completion, err := db.CompleteIndexDataDeletion(ctx, attempt)
	if err != nil {
		t.Fatalf("CompleteIndexDataDeletion(): %v", err)
	}
	for name, statement := range map[string]string{
		"update": `UPDATE index_data_deletion_completions
			SET tenant_id = 'other'
			WHERE deletion_operation_id = ?`,
		"delete": `DELETE FROM index_data_deletion_completions
			WHERE deletion_operation_id = ?`,
		"replace": `INSERT OR REPLACE INTO
			index_data_deletion_completions
			SELECT * FROM index_data_deletion_completions
			WHERE deletion_operation_id = ?`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := db.SQLDB().ExecContext(
				ctx,
				statement,
				operation.ID,
			); err == nil {
				t.Fatalf("%s unexpectedly succeeded", name)
			}
		})
	}
	if got, err := db.GetIndexDataDeletionCompletion(
		ctx,
		operation.ID,
	); err != nil || got != completion {
		t.Fatalf("completion after direct mutations = %#v, error=%v", got, err)
	}

	keepData, err := db.CreateIndex(ctx, enabledIndex("terminal-keep-data"))
	if err != nil {
		t.Fatalf("CreateIndex(KEEP_DATA): %v", err)
	}
	keepData, err = db.SetIndexState(
		ctx,
		keepData.ID,
		keepData.Version,
		IndexStateArchived,
	)
	if err != nil {
		t.Fatalf("archive KEEP_DATA index: %v", err)
	}
	if _, err := db.DeleteIndex(
		ctx,
		keepData.ID,
		keepData.Version,
		keepData.Definition.Name,
	); err != nil {
		t.Fatalf("DeleteIndex(KEEP_DATA): %v", err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `
		INSERT OR REPLACE INTO index_deletion_tombstones (
			index_id,
			name,
			deleted_version,
			deleted_at_unix_micro
		)
		SELECT
			index_id,
			name,
			deleted_version,
			deleted_at_unix_micro + 1
		FROM index_deletion_tombstones
		WHERE index_id = ?`,
		keepData.ID,
	); err == nil {
		t.Fatal("KEEP_DATA tombstone replacement unexpectedly succeeded")
	}
}

func completionRecordForTest(
	operation IndexDeletionOperation,
	attempt IndexDeletionMutationAttempt,
) indexDataDeletionCompletionRecord {
	return indexDataDeletionCompletionRecord{
		DeletionOperationID:         operation.ID,
		CorrelationID:               attempt.CorrelationID,
		IndexID:                     operation.IndexID,
		IndexName:                   operation.IndexName,
		ArchivedIndexVersion:        int64(operation.ArchivedVersion),
		DeletingIndexVersion:        int64(operation.DeletingVersion),
		TenantID:                    attempt.Target.TenantID,
		ClickHouseDatabase:          attempt.Target.Database,
		ClickHouseTable:             attempt.Target.Table,
		ClickHouseTableUUID:         attempt.Target.TableUUID,
		ProtocolVersion:             int64(attempt.ProtocolVersion),
		OperationCreatedAtUnixMicro: operation.CreatedAt.UnixMicro(),
		AttemptCreatedAtUnixMicro:   attempt.CreatedAt.UnixMicro(),
		CompletedAtUnixMicro:        attempt.CreatedAt.UnixMicro(),
	}
}

func assertOutstandingIndexDeletion(
	t *testing.T,
	db *DB,
	operation IndexDeletionOperation,
	attempt IndexDeletionMutationAttempt,
) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.GetIndexDataDeletionCompletion(
		ctx,
		operation.ID,
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("completion error = %v, want ErrNotFound", err)
	}
	if got, err := db.GetIndexDeletionOperation(
		ctx,
		operation.ID,
	); err != nil || got != operation {
		t.Fatalf("outstanding operation = %#v, error=%v", got, err)
	}
	if got, err := db.GetIndexDeletionMutationAttempt(
		ctx,
		operation.ID,
	); err != nil || got != attempt {
		t.Fatalf("outstanding attempt = %#v, error=%v", got, err)
	}
	var tombstones int64
	if err := db.GORMDB().WithContext(ctx).
		Model(&indexDeletionTombstoneRecord{}).
		Where("index_id = ?", operation.IndexID).
		Count(&tombstones).Error; err != nil {
		t.Fatalf("count outstanding tombstones: %v", err)
	}
	if tombstones != 0 {
		t.Fatalf("outstanding tombstones = %d, want 0", tombstones)
	}
}
