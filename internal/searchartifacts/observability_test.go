package searchartifacts

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/featureops"
)

func TestArtifactAdmissionAndRetentionObservationsAreAggregate(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 29, 10, 0, 0, 0, time.UTC)}
	store, _, _ := newTestStore(t, clock, DefaultMaximumBytes)
	metrics := featureops.NewMetrics()
	store.observer = metrics
	store.maximumJobs = 1

	job := testQueuedJob(t, "observed", clock.now)
	if err := store.Admit(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := store.Admit(ctx, testQueuedJob(t, "capacity", clock.now)); !errors.Is(err, ErrCapacity) {
		t.Fatalf("capacity admission error = %v", err)
	}
	completed := completeTestJob(job, clock.now, time.Hour)
	if err := store.Finalize(ctx, completed); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PersistResults(ctx, testAccess(), job.ID, &sourceLease{
		schema: *completed.Schema, rows: testRows(t), generation: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ShareExpected(ctx, testAccess(), job.ID, completed.Version-1); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale share error = %v", err)
	}
	shared, err := store.ShareExpected(ctx, testAccess(), job.ID, completed.Version)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpdateSettingsExpected(ctx, testAccess(), job.ID, Settings{
		Visibility: VisibilityPrivate, RetentionClass: RetentionManual,
		Lifetime: 10 * time.Minute,
	}, shared.Job.Version); err != nil {
		t.Fatal(err)
	}

	snapshot := metrics.Snapshot()
	assertFeatureCounter(t, snapshot, featureops.OperationAdmission, featureops.OutcomeSucceeded)
	assertFeatureCounter(t, snapshot, featureops.OperationAdmission, featureops.OutcomeCapacityRejected)
	assertFeatureCounter(t, snapshot, featureops.OperationShare, featureops.OutcomeConflict)
	assertFeatureCounter(t, snapshot, featureops.OperationShare, featureops.OutcomeSucceeded)
	assertFeatureCounter(t, snapshot, featureops.OperationRetentionChange, featureops.OutcomeSucceeded)
}

func TestStartupReconciliationAndCleanupObservationsAreDeterministic(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 29, 10, 0, 0, 0, time.UTC)}
	store, database, directory := newTestStore(t, clock, DefaultMaximumBytes)
	job := testQueuedJob(t, "interrupted-observed", clock.now)
	if err := store.Admit(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	orphanContents := []byte("orphan")
	if err := os.WriteFile(filepath.Join(directory, artifactName("orphan-observed")), orphanContents, 0o600); err != nil {
		t.Fatal(err)
	}
	metrics := featureops.NewMetrics()
	store, err := New(ctx, Config{
		DB: database.SQLDB(), Directory: directory, Clock: clock.Now,
		CleanupInterval: -1, TombstoneRetention: time.Minute, Observer: metrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	reconciled := metrics.Snapshot().Counter(
		featureops.FeatureDurableArtifacts,
		featureops.OperationReconciliation,
		featureops.OutcomeSucceeded,
	)
	if reconciled.Events != 1 || reconciled.Items != 2 || reconciled.Bytes != uint64(len(orphanContents)) {
		t.Fatalf("reconciliation counter = %#v", reconciled)
	}
	if _, err := database.SQLDB().ExecContext(ctx, `DELETE FROM durable_search_jobs WHERE id = ?`, job.ID); err != nil {
		t.Fatal(err)
	}

	completedJob := testQueuedJob(t, "cleanup-observed", clock.now)
	if err := store.Admit(ctx, completedJob); err != nil {
		t.Fatal(err)
	}
	completed := completeTestJob(completedJob, clock.now, time.Minute)
	if err := store.Finalize(ctx, completed); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PersistResults(ctx, testAccess(), completedJob.ID, &sourceLease{
		schema: *completed.Schema, rows: testRows(t), generation: 1,
	}); err != nil {
		t.Fatal(err)
	}
	clock.now = clock.now.Add(time.Minute)
	if err := store.Reap(ctx); err != nil {
		t.Fatal(err)
	}
	clock.now = clock.now.Add(time.Minute)
	if err := store.Reap(ctx); err != nil {
		t.Fatal(err)
	}
	cleanup := metrics.Snapshot().Counter(
		featureops.FeatureDurableArtifacts,
		featureops.OperationCleanup,
		featureops.OutcomeSucceeded,
	)
	if cleanup.Events != 2 || cleanup.Items != 2 || cleanup.Bytes == 0 {
		t.Fatalf("cleanup counter = %#v", cleanup)
	}
}

func assertFeatureCounter(
	t *testing.T,
	snapshot featureops.Snapshot,
	operation featureops.Operation,
	outcome featureops.Outcome,
) {
	t.Helper()
	got := snapshot.Counter(featureops.FeatureDurableArtifacts, operation, outcome)
	if got.Events != 1 || got.Items != 1 {
		t.Fatalf("%v/%v counter = %#v, want one event and item", operation, outcome, got)
	}
}
