package featureaudit

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/featureops"
)

type fixedClock struct{ now time.Time }

func (clock fixedClock) Now() time.Time { return clock.now }

func TestStorePersistsImmutableEventsAcrossRestart(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "control.db")
	firstDatabase, err := control.Open(ctx, path)
	if err != nil {
		t.Fatalf("open first database: %v", err)
	}
	firstTime := time.Date(2026, time.August, 30, 8, 9, 10, 123456789, time.FixedZone("test", -7*60*60))
	firstStore, err := New(ctx, firstDatabase, Options{
		TenantID: "tenant-a",
		Clock:    fixedClock{now: firstTime},
	})
	if err != nil {
		t.Fatalf("open first store: %v", err)
	}
	firstEvent := featureops.Event{
		Feature:   featureops.FeatureDurableArtifacts,
		Operation: featureops.OperationShare,
		Outcome:   featureops.OutcomeSucceeded,
		Items:     2,
		Bytes:     3,
	}
	first, err := firstStore.Append(ctx, firstEvent)
	if err != nil {
		t.Fatalf("append first event: %v", err)
	}
	if first.Sequence != 1 || !first.OccurredAt.Equal(firstTime.UTC().Truncate(time.Microsecond)) {
		t.Fatalf("first record = %#v", first)
	}
	if err := firstDatabase.Close(); err != nil {
		t.Fatalf("close first database: %v", err)
	}

	secondDatabase, err := control.Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen database: %v", err)
	}
	t.Cleanup(func() { _ = secondDatabase.Close() })
	secondStore, err := New(ctx, secondDatabase, Options{
		TenantID: "tenant-a",
		// A wall-clock correction cannot make journal time move backward.
		Clock: fixedClock{now: firstTime.Add(-time.Hour)},
	})
	if err != nil {
		t.Fatalf("open second store: %v", err)
	}
	secondEvent := featureops.Event{
		Feature:   featureops.FeatureAlerts,
		Operation: featureops.OperationDelivery,
		Outcome:   featureops.OutcomeDelivered,
		Items:     1,
		Bytes:     512,
	}
	second, err := secondStore.Append(ctx, secondEvent)
	if err != nil {
		t.Fatalf("append second event: %v", err)
	}
	if second.Sequence != 2 || !second.OccurredAt.Equal(first.OccurredAt) {
		t.Fatalf("second record = %#v", second)
	}
	records, err := secondStore.List(ctx, 0, MaximumListPageSize)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(records) != 2 || records[0].Event != firstEvent || records[1].Event != secondEvent {
		t.Fatalf("records = %#v", records)
	}

	if _, err := secondDatabase.SQLDB().ExecContext(ctx, `
		UPDATE feature_operation_audit_events SET items = 99
		WHERE tenant_id = 'tenant-a' AND sequence = 1
	`); err == nil {
		t.Fatal("immutable event update succeeded")
	}
	if _, err := secondDatabase.SQLDB().ExecContext(ctx, `
		DELETE FROM feature_operation_audit_events
		WHERE tenant_id = 'tenant-a' AND sequence = 1
	`); err == nil {
		t.Fatal("immutable event delete succeeded")
	}
	if _, err := secondDatabase.SQLDB().ExecContext(ctx, `
		DELETE FROM feature_operation_audit_tenant_state
		WHERE tenant_id = 'tenant-a'
	`); err == nil {
		t.Fatal("immutable tenant state delete succeeded")
	}
}

func TestStoreInitializesTenantStateOnceOnFirstCommittedEvent(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	database, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	store, err := New(ctx, database, Options{
		TenantID: "tenant-a",
		Clock:    fixedClock{now: time.Date(2026, 8, 30, 1, 2, 3, 0, time.UTC)},
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	var stateRows int
	if err := database.SQLDB().QueryRowContext(ctx, `
		SELECT COUNT(*) FROM feature_operation_audit_tenant_state
		WHERE tenant_id = 'tenant-a'
	`).Scan(&stateRows); err != nil {
		t.Fatalf("count initial tenant state: %v", err)
	}
	if stateRows != 0 {
		t.Fatalf("initial tenant state rows = %d, want 0", stateRows)
	}
	event := featureops.Event{
		Feature: featureops.FeatureAlerts, Operation: featureops.OperationLifecycle,
		Outcome: featureops.OutcomeSucceeded,
	}
	if _, err := store.Append(ctx, event); err != nil {
		t.Fatalf("append first event: %v", err)
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		CREATE TRIGGER test_reject_redundant_feature_audit_state_insert
		BEFORE INSERT ON feature_operation_audit_tenant_state
		BEGIN SELECT RAISE(ABORT, 'redundant tenant-state initialization'); END
	`); err != nil {
		t.Fatalf("install redundant-insert guard: %v", err)
	}
	second, err := store.Append(ctx, event)
	if err != nil {
		t.Fatalf("append with initialized tenant state: %v", err)
	}
	if second.Sequence != 2 {
		t.Fatalf("second sequence = %d, want 2", second.Sequence)
	}
}

func TestStoreRejectsTamperedOrIncompleteJournalAtStartup(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	database, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	store, err := New(ctx, database, Options{
		TenantID: "tenant-a",
		Clock:    fixedClock{now: time.Date(2026, 8, 30, 1, 2, 3, 0, time.UTC)},
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err := store.Append(ctx, featureops.Event{
		Feature: featureops.FeatureScheduledReports, Operation: featureops.OperationRunOutcome,
		Outcome: featureops.OutcomeSucceeded,
	}); err != nil {
		t.Fatalf("append event: %v", err)
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		DROP TRIGGER feature_operation_audit_event_update_is_forbidden
	`); err != nil {
		t.Fatalf("remove immutability trigger: %v", err)
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE feature_operation_audit_events
		SET occurred_at_unix_micro = occurred_at_unix_micro + 1
		WHERE tenant_id = 'tenant-a' AND sequence = 1
	`); err != nil {
		t.Fatalf("tamper event: %v", err)
	}
	if reopened, err := New(ctx, database, Options{TenantID: "tenant-a"}); reopened != nil || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("New(tampered) = (%v, %v), want nil/ErrCorrupt", reopened, err)
	}
}

func TestStoreSerializesConcurrentSequenceAssignment(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	database, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	store, err := New(ctx, database, Options{
		TenantID: "tenant-a",
		Clock:    fixedClock{now: time.Date(2026, 8, 30, 1, 2, 3, 0, time.UTC)},
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	const workers = 32
	errorsByWorker := make([]error, workers)
	var wait sync.WaitGroup
	for worker := range workers {
		wait.Go(func() {
			_, errorsByWorker[worker] = store.Append(ctx, featureops.Event{
				Feature: featureops.FeatureAlerts, Operation: featureops.OperationLifecycle,
				Outcome: featureops.OutcomeSucceeded, Items: 1,
			})
		})
	}
	wait.Wait()
	for worker, err := range errorsByWorker {
		if err != nil {
			t.Fatalf("worker %d: %v", worker, err)
		}
	}
	records, err := store.List(ctx, 0, workers)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(records) != workers {
		t.Fatalf("records = %d, want %d", len(records), workers)
	}
	for index, record := range records {
		if record.Sequence != uint64(index+1) {
			t.Fatalf("record %d sequence = %d", index, record.Sequence)
		}
	}
}

func TestObserverReportsOnlyBoundedFailureCategory(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	database, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	var failures []Failure
	store, err := New(ctx, database, Options{
		TenantID: "tenant-a",
		Clock:    fixedClock{now: time.Date(2026, 8, 30, 1, 2, 3, 0, time.UTC)},
		OnFailure: func(failure Failure) {
			failures = append(failures, failure)
		},
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	store.Observe(featureops.Event{Feature: featureops.FeatureAlerts})
	if len(failures) != 1 || failures[0] != (Failure{Category: FailureInvalid}) {
		t.Fatalf("failures = %#v", failures)
	}
	if health := store.Health(); health != (Health{FailedEvents: 1, LastFailure: FailureInvalid}) {
		t.Fatalf("health = %#v", health)
	}
	records, err := store.List(ctx, 0, MaximumListPageSize)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("invalid observer event was persisted: %#v", records)
	}
}
