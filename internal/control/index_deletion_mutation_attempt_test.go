package control

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/protocolid"
)

func TestEnsureIndexDeletionMutationAttemptPersistsExactTargetAcrossRestart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "control.sqlite")
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	operation := createIndexDeletionOperation(t, db, "mutation-target")
	target := IndexDeletionMutationTarget{
		TenantID:  "tenant-a",
		Database:  "open_splunk",
		Table:     "events",
		TableUUID: "01234567-89ab-4cde-8fab-0123456789ab",
	}

	attempt, err := db.EnsureIndexDeletionMutationAttempt(ctx, operation.ID, target)
	if err != nil {
		t.Fatalf("EnsureIndexDeletionMutationAttempt(): %v", err)
	}
	if !protocolid.Valid(attempt.CorrelationID) ||
		attempt.DeletionOperationID != operation.ID ||
		attempt.IndexID != operation.IndexID ||
		attempt.IndexName != operation.IndexName ||
		attempt.Target != target ||
		attempt.ProtocolVersion != IndexDeletionMutationProtocolVersion ||
		attempt.CreatedAt.IsZero() ||
		attempt.CreatedAt.Before(operation.CreatedAt) {
		t.Fatalf("attempt = %#v, operation = %#v", attempt, operation)
	}

	retried, err := db.EnsureIndexDeletionMutationAttempt(ctx, operation.ID, target)
	if err != nil {
		t.Fatalf("idempotent EnsureIndexDeletionMutationAttempt(): %v", err)
	}
	if retried != attempt {
		t.Fatalf("idempotent attempt = %#v, want %#v", retried, attempt)
	}
	got, err := db.GetIndexDeletionMutationAttempt(ctx, operation.ID)
	if err != nil {
		t.Fatalf("GetIndexDeletionMutationAttempt(): %v", err)
	}
	if got != attempt {
		t.Fatalf("GetIndexDeletionMutationAttempt() = %#v, want %#v", got, attempt)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	db, err = Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	restarted, err := db.EnsureIndexDeletionMutationAttempt(ctx, operation.ID, target)
	if err != nil {
		t.Fatalf("EnsureIndexDeletionMutationAttempt(after restart): %v", err)
	}
	if restarted != attempt {
		t.Fatalf("restarted attempt = %#v, want %#v", restarted, attempt)
	}

	driftedTargets := []IndexDeletionMutationTarget{
		{TenantID: "tenant-b", Database: target.Database, Table: target.Table, TableUUID: target.TableUUID},
		{TenantID: target.TenantID, Database: "other_database", Table: target.Table, TableUUID: target.TableUUID},
		{TenantID: target.TenantID, Database: target.Database, Table: "other_table", TableUUID: target.TableUUID},
		{TenantID: target.TenantID, Database: target.Database, Table: target.Table, TableUUID: "11234567-89ab-4cde-8fab-0123456789ab"},
	}
	for _, drifted := range driftedTargets {
		if _, err := db.EnsureIndexDeletionMutationAttempt(
			ctx,
			operation.ID,
			drifted,
		); !errors.Is(err, ErrDependencyConflict) {
			t.Errorf("target drift %+v error = %v, want ErrDependencyConflict", drifted, err)
		}
	}
}

func TestEnsureIndexDeletionMutationAttemptConvergesConcurrently(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	operation := createIndexDeletionOperation(t, db, "mutation-concurrent")
	target := IndexDeletionMutationTarget{
		TenantID:  "tenant",
		Database:  "open_splunk",
		Table:     "events",
		TableUUID: "21234567-89ab-4cde-8fab-0123456789ab",
	}

	const callers = 24
	start := make(chan struct{})
	results := make(chan IndexDeletionMutationAttempt, callers)
	failures := make(chan error, callers)
	var wait sync.WaitGroup
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			attempt, err := db.EnsureIndexDeletionMutationAttempt(ctx, operation.ID, target)
			if err != nil {
				failures <- err
				return
			}
			results <- attempt
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(failures)
	for err := range failures {
		t.Errorf("concurrent EnsureIndexDeletionMutationAttempt(): %v", err)
	}
	var want IndexDeletionMutationAttempt
	for result := range results {
		if want == (IndexDeletionMutationAttempt{}) {
			want = result
			continue
		}
		if result != want {
			t.Errorf("concurrent attempt = %#v, want %#v", result, want)
		}
	}
	if want == (IndexDeletionMutationAttempt{}) {
		t.Fatal("no concurrent attempt succeeded")
	}
	var count int64
	if err := db.GORMDB().WithContext(ctx).
		Model(&indexDeletionMutationAttemptRecord{}).
		Count(&count).Error; err != nil {
		t.Fatalf("count mutation attempts: %v", err)
	}
	if count != 1 {
		t.Fatalf("mutation attempt count = %d, want 1", count)
	}
}

func TestIndexDeletionMutationAttemptValidatesInputsAndParent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	operation := createIndexDeletionOperation(t, db, "mutation-validation")
	valid := IndexDeletionMutationTarget{
		TenantID:  "tenant",
		Database:  "open_splunk",
		Table:     "events",
		TableUUID: "31234567-89ab-4cde-8fab-0123456789ab",
	}
	tests := []struct {
		name      string
		operation string
		target    IndexDeletionMutationTarget
		want      error
	}{
		{name: "missing operation", operation: "idxdel_missing", target: valid, want: ErrNotFound},
		{name: "invalid operation", operation: "../operation", target: valid, want: ErrInvalidArgument},
		{name: "empty tenant", operation: operation.ID, target: withMutationTarget(valid, func(target *IndexDeletionMutationTarget) { target.TenantID = "" }), want: ErrInvalidArgument},
		{name: "padded tenant", operation: operation.ID, target: withMutationTarget(valid, func(target *IndexDeletionMutationTarget) { target.TenantID = " tenant" }), want: ErrInvalidArgument},
		{name: "control tenant", operation: operation.ID, target: withMutationTarget(valid, func(target *IndexDeletionMutationTarget) { target.TenantID = "tenant\nother" }), want: ErrInvalidArgument},
		{name: "nul tenant", operation: operation.ID, target: withMutationTarget(valid, func(target *IndexDeletionMutationTarget) { target.TenantID = "tenant\x00other" }), want: ErrInvalidArgument},
		{name: "bad database", operation: operation.ID, target: withMutationTarget(valid, func(target *IndexDeletionMutationTarget) { target.Database = "open-splunk" }), want: ErrInvalidArgument},
		{name: "bad table", operation: operation.ID, target: withMutationTarget(valid, func(target *IndexDeletionMutationTarget) { target.Table = "events.table" }), want: ErrInvalidArgument},
		{name: "uppercase uuid", operation: operation.ID, target: withMutationTarget(valid, func(target *IndexDeletionMutationTarget) { target.TableUUID = "31234567-89AB-4CDE-8FAB-0123456789AB" }), want: ErrInvalidArgument},
		{name: "nil uuid", operation: operation.ID, target: withMutationTarget(valid, func(target *IndexDeletionMutationTarget) { target.TableUUID = "00000000-0000-0000-0000-000000000000" }), want: ErrInvalidArgument},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := db.EnsureIndexDeletionMutationAttempt(
				ctx,
				test.operation,
				test.target,
			); !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := db.EnsureIndexDeletionMutationAttempt(
		canceled,
		operation.ID,
		valid,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("EnsureIndexDeletionMutationAttempt(canceled) error = %v, want context.Canceled", err)
	}
	if _, err := db.GetIndexDeletionMutationAttempt(
		canceled,
		operation.ID,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetIndexDeletionMutationAttempt(canceled) error = %v, want context.Canceled", err)
	}
}

func TestIndexDeletionMutationAttemptSchemaPreventsMutationAndReplacement(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	firstOperation := createIndexDeletionOperation(t, db, "mutation-guard-first")
	secondOperation := createIndexDeletionOperation(t, db, "mutation-guard-second")
	nulUUIDOperation := createIndexDeletionOperation(t, db, "mutation-guard-nul-uuid")
	target := IndexDeletionMutationTarget{
		TenantID:  "tenant",
		Database:  "open_splunk",
		Table:     "events",
		TableUUID: "41234567-89ab-4cde-8fab-0123456789ab",
	}
	attempt, err := db.EnsureIndexDeletionMutationAttempt(ctx, firstOperation.ID, target)
	if err != nil {
		t.Fatalf("EnsureIndexDeletionMutationAttempt(): %v", err)
	}

	for name, statement := range map[string]string{
		"update": `UPDATE index_deletion_mutation_attempts
			SET tenant_id = 'other'
			WHERE deletion_operation_id = ?`,
		"delete": `DELETE FROM index_deletion_mutation_attempts
			WHERE deletion_operation_id = ?`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := db.SQLDB().ExecContext(ctx, statement, firstOperation.ID); err == nil {
				t.Fatalf("%s unexpectedly succeeded", name)
			}
		})
	}
	if _, err := db.SQLDB().ExecContext(ctx, `
		INSERT OR REPLACE INTO index_deletion_mutation_attempts (
			deletion_operation_id,
			correlation_id,
			tenant_id,
			clickhouse_database,
			clickhouse_table,
			clickhouse_table_uuid,
			protocol_version,
			created_at_unix_micro
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		secondOperation.ID,
		attempt.CorrelationID,
		target.TenantID,
		target.Database,
		target.Table,
		target.TableUUID,
		IndexDeletionMutationProtocolVersion,
		attempt.CreatedAt.UnixMicro(),
	); err == nil {
		t.Fatal("correlation identity replacement unexpectedly succeeded")
	}
	if _, err := db.SQLDB().ExecContext(ctx, `
		INSERT INTO index_deletion_mutation_attempts (
			deletion_operation_id,
			correlation_id,
			tenant_id,
			clickhouse_database,
			clickhouse_table,
			clickhouse_table_uuid,
			protocol_version,
			created_at_unix_micro
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		nulUUIDOperation.ID,
		"idxmut_nul_uuid",
		target.TenantID,
		target.Database,
		target.Table,
		target.TableUUID+"\x00suffix",
		IndexDeletionMutationProtocolVersion,
		nulUUIDOperation.CreatedAt.UnixMicro(),
	); err == nil {
		t.Fatal("table UUID with embedded NUL suffix unexpectedly succeeded")
	}

	got, err := db.GetIndexDeletionMutationAttempt(ctx, firstOperation.ID)
	if err != nil {
		t.Fatalf("GetIndexDeletionMutationAttempt(): %v", err)
	}
	if got != attempt {
		t.Fatalf("attempt after direct mutations = %#v, want %#v", got, attempt)
	}
	if _, err := db.GetIndexDeletionMutationAttempt(
		ctx,
		secondOperation.ID,
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second operation attempt error = %v, want ErrNotFound", err)
	}
}

func createIndexDeletionOperation(t *testing.T, db *DB, name string) IndexDeletionOperation {
	t.Helper()
	ctx := context.Background()
	index, err := db.CreateIndex(ctx, enabledIndex(name))
	if err != nil {
		t.Fatalf("CreateIndex(%q): %v", name, err)
	}
	index, err = db.SetIndexState(ctx, index.ID, index.Version, IndexStateArchived)
	if err != nil {
		t.Fatalf("archive index %q: %v", name, err)
	}
	operation, err := db.BeginIndexDataDeletion(
		ctx,
		index.ID,
		index.Version,
		index.Definition.Name,
	)
	if err != nil {
		t.Fatalf("BeginIndexDataDeletion(%q): %v", name, err)
	}
	return operation
}

func withMutationTarget(
	target IndexDeletionMutationTarget,
	mutate func(*IndexDeletionMutationTarget),
) IndexDeletionMutationTarget {
	mutate(&target)
	return target
}
