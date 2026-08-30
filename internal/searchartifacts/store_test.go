package searchartifacts

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/featureops"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

type testClock struct{ now time.Time }

func (clock *testClock) Now() time.Time { return clock.now }

type sourceLease struct {
	schema     searchjobs.Schema
	rows       []searchjobs.ResultRow
	truncated  bool
	generation uint64
	next       int
}

type blockingSourceLease struct {
	*sourceLease
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (lease *blockingSourceLease) Next(ctx context.Context) (searchjobs.ResultRow, bool, error) {
	lease.once.Do(func() { close(lease.started) })
	select {
	case <-ctx.Done():
		return searchjobs.ResultRow{}, false, ctx.Err()
	case <-lease.release:
		return lease.sourceLease.Next(ctx)
	}
}

func (lease *sourceLease) Schema() searchjobs.Schema { return lease.schema }
func (lease *sourceLease) RowCount() uint64          { return uint64(len(lease.rows)) }
func (lease *sourceLease) RowCountExact() bool       { return true }
func (lease *sourceLease) ResultsTruncated() bool    { return lease.truncated }
func (lease *sourceLease) Generation() uint64        { return lease.generation }
func (lease *sourceLease) Close() error              { return nil }
func (lease *sourceLease) Next(ctx context.Context) (searchjobs.ResultRow, bool, error) {
	if err := ctx.Err(); err != nil {
		return searchjobs.ResultRow{}, false, err
	}
	if lease.next >= len(lease.rows) {
		return searchjobs.ResultRow{}, false, nil
	}
	row := lease.rows[lease.next]
	lease.next++
	return row, true, nil
}

func TestStorePersistsSharesAndSlidesCompletedArtifacts(t *testing.T) {
	ctx := context.Background()
	directory := filepath.Join(t.TempDir(), "search-artifacts")
	database, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	clock := &testClock{now: time.Date(2026, 8, 29, 10, 0, 0, 0, time.UTC)}
	store := openTestStore(t, database.SQLDB(), directory, clock, DefaultMaximumBytes)

	job := testQueuedJob(t, "durable", clock.now)
	if err := store.Admit(ctx, job); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	completed := completeTestJob(job, clock.now, 10*time.Minute)
	if err := store.Finalize(ctx, completed); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	rows := testRows(t)
	source := &sourceLease{schema: *completed.Schema, rows: rows, generation: 7}
	if _, err := store.PersistResults(ctx, testAccess(), job.ID, source); err != nil {
		t.Fatalf("PersistResults: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	clock.now = clock.now.Add(5 * time.Minute)
	store = openTestStore(t, database.SQLDB(), directory, clock, DefaultMaximumBytes)
	record, err := store.Share(ctx, testAccess(), job.ID)
	if err != nil {
		t.Fatalf("Share: %v", err)
	}
	if record.Visibility != VisibilityEveryone || record.RetentionClass != RetentionShared ||
		record.Lifetime != 7*24*time.Hour || !record.ExpiresAt.Equal(clock.now.Add(7*24*time.Hour)) {
		t.Fatalf("shared record = %#v", record)
	}

	clock.now = clock.now.Add(time.Hour)
	lease, err := store.Acquire(ctx, testAccess(), job.ID)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer func() { _ = lease.Close() }()
	if lease.Generation() != 7 || lease.RowCount() != uint64(len(rows)) || !lease.RowCountExact() {
		t.Fatalf("lease metadata = generation %d, rows %d, exact %v", lease.Generation(), lease.RowCount(), lease.RowCountExact())
	}
	assertLeaseRows(t, lease, rows)
	record, err = store.Get(ctx, testAccess(), job.ID, AccessInspect)
	if err != nil {
		t.Fatal(err)
	}
	if !record.LastAccessedAt.Equal(clock.now) || !record.ExpiresAt.Equal(clock.now.Add(7*24*time.Hour)) {
		t.Fatalf("sliding access record = %#v", record)
	}

	info, err := os.Stat(directory)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("artifact directory permissions = %#o", info.Mode().Perm())
	}
}

func TestUpdateSettingsExpectedReportsStaleVersionConflict(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 29, 10, 0, 0, 0, time.UTC)}
	store, _, _ := newTestStore(t, clock, DefaultMaximumBytes)
	job := testQueuedJob(t, "settings-conflict", clock.now)
	if err := store.Admit(ctx, job); err != nil {
		t.Fatal(err)
	}
	completed := completeTestJob(job, clock.now, time.Hour)
	if err := store.Finalize(ctx, completed); err != nil {
		t.Fatal(err)
	}
	persisted, err := store.PersistResults(ctx, testAccess(), job.ID, &sourceLease{
		schema: *completed.Schema, rows: testRows(t), generation: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if persisted.State != StateCompleted || persisted.Job.State != searchjobs.StateCompleted ||
		!persisted.ArtifactPresent {
		t.Fatalf("PersistResults returned incomplete publication = %#v", persisted)
	}
	_, err = store.ShareExpected(ctx, testAccess(), job.ID, completed.Version-1)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("ShareExpected stale version error = %v, want ErrConflict", err)
	}
}

func TestAcquireLoadsOutsideStoreMutexAndPinsReaping(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 29, 10, 0, 0, 0, time.UTC)}
	store, _, _ := newTestStore(t, clock, DefaultMaximumBytes)
	job := testQueuedJob(t, "concurrent-acquire", clock.now)
	if err := store.Admit(ctx, job); err != nil {
		t.Fatal(err)
	}
	completed := completeTestJob(job, clock.now, time.Minute)
	if err := store.Finalize(ctx, completed); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PersistResults(ctx, testAccess(), job.ID, &sourceLease{
		schema: *completed.Schema, rows: testRows(t), generation: 1,
	}); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	resume := make(chan struct{})
	store.load = func(ctx context.Context, source *os.File, jobID string, verification artifactVerification) (artifactMetadata, artifactRowSource, error) {
		close(started)
		select {
		case <-ctx.Done():
			return artifactMetadata{}, nil, ctx.Err()
		case <-resume:
		}
		return loadVerifiedArtifact(ctx, source, jobID, verification)
	}
	type acquireResult struct {
		lease ResultLease
		err   error
	}
	acquired := make(chan acquireResult, 1)
	go func() {
		lease, err := store.Acquire(ctx, testAccess(), job.ID)
		acquired <- acquireResult{lease: lease, err: err}
	}()
	<-started

	inspected := make(chan error, 1)
	go func() {
		_, err := store.Get(ctx, testAccess(), job.ID, AccessInspect)
		inspected <- err
	}()
	select {
	case err := <-inspected:
		if err != nil {
			t.Fatalf("Get during artifact decode: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("metadata read blocked behind artifact decode")
	}

	clock.now = clock.now.Add(2 * time.Minute)
	if err := store.Reap(ctx); err != nil {
		t.Fatal(err)
	}
	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Jobs != 1 || stats.ActiveLeases != 1 {
		t.Fatalf("stats while acquisition pinned = %#v", stats)
	}
	close(resume)
	result := <-acquired
	if result.err != nil {
		t.Fatalf("Acquire: %v", result.err)
	}
	if err := result.lease.Close(); err != nil {
		t.Fatal(err)
	}
	clock.now = clock.now.Add(time.Minute)
	if err := store.Reap(ctx); err != nil {
		t.Fatal(err)
	}
	stats, err = store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Jobs != 0 || stats.ActiveLeases != 0 {
		t.Fatalf("stats after lease release = %#v", stats)
	}
}

func TestExactExpiryRejectsNewReadsButExistingLeaseFinishes(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 29, 10, 0, 0, 0, time.UTC)}
	store, _, _ := newTestStore(t, clock, DefaultMaximumBytes)
	job := testQueuedJob(t, "lease-expiry", clock.now)
	if err := store.Admit(ctx, job); err != nil {
		t.Fatal(err)
	}
	completed := completeTestJob(job, clock.now, time.Minute)
	if err := store.Finalize(ctx, completed); err != nil {
		t.Fatal(err)
	}
	rows := testRows(t)
	if _, err := store.PersistResults(ctx, testAccess(), job.ID, &sourceLease{schema: *completed.Schema, rows: rows, generation: 1}); err != nil {
		t.Fatal(err)
	}
	lease, err := store.Acquire(ctx, testAccess(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	clock.now = clock.now.Add(time.Minute)
	if _, err := store.Acquire(ctx, testAccess(), job.ID); !errors.Is(err, ErrExpired) {
		t.Fatalf("Acquire at deadline error = %v", err)
	}
	assertLeaseRows(t, lease, rows)
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLaunchReturnsExpiredDefinitionWithoutRefreshingOrRestoringResults(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 29, 10, 0, 0, 0, time.UTC)}
	store, _, _ := newTestStore(t, clock, DefaultMaximumBytes)
	job := testQueuedJob(t, "expired-deep-link", clock.now)
	if err := store.Admit(ctx, job); err != nil {
		t.Fatal(err)
	}
	completed := completeTestJob(job, clock.now, time.Minute)
	if err := store.Finalize(ctx, completed); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PersistResults(ctx, testAccess(), job.ID, &sourceLease{
		schema: *completed.Schema, rows: testRows(t), generation: 1,
	}); err != nil {
		t.Fatal(err)
	}

	clock.now = completed.ExpiresAt
	record, err := store.Get(ctx, testAccess(), job.ID, AccessLaunch)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != StateExpired || record.Job.SPL != completed.SPL ||
		!record.ExpiresAt.Equal(completed.ExpiresAt) {
		t.Fatalf("expired launch record = %#v", record)
	}
	if _, err := store.Get(ctx, testAccess(), job.ID, AccessRefresh); !errors.Is(err, ErrExpired) {
		t.Fatalf("ordinary refresh at deadline error = %v, want ErrExpired", err)
	}
	if _, err := store.Acquire(ctx, testAccess(), job.ID); !errors.Is(err, ErrExpired) {
		t.Fatalf("result acquire at deadline error = %v, want ErrExpired", err)
	}
}

func TestReconcileMarksActiveJobsInterruptedAndRemovesOrphans(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 29, 10, 0, 0, 0, time.UTC)}
	store, database, directory := newTestStore(t, clock, DefaultMaximumBytes)
	job := testQueuedJob(t, "interrupted", clock.now)
	if err := store.Admit(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(directory, artifactName("orphan"))
	if err := os.WriteFile(orphan, []byte("orphan"), 0o600); err != nil {
		t.Fatal(err)
	}
	clock.now = clock.now.Add(time.Minute)
	store = openTestStore(t, database.SQLDB(), directory, clock, DefaultMaximumBytes)
	record, err := store.Get(ctx, testAccess(), job.ID, AccessInspect)
	if err != nil {
		t.Fatalf("Get interrupted: %v", err)
	}
	if record.State != StateInterrupted || record.Job.Failure == nil ||
		record.Job.Failure.Message != "search was interrupted by server restart" {
		t.Fatalf("interrupted record = %#v", record)
	}
	if _, err := os.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan artifact still exists: %v", err)
	}
}

func TestInterruptedJobRemainsReadableUntilItsExactExpiry(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)}
	store, database, directory := newTestStore(t, clock, DefaultMaximumBytes)
	job := testQueuedJob(t, "interrupted-retention", clock.now)
	job.RetentionLifetime = 30 * time.Minute
	if err := store.Admit(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openTestStore(t, database.SQLDB(), directory, clock, DefaultMaximumBytes)

	clock.now = clock.now.Add(DefaultTombstoneRetention + time.Minute)
	if err := store.Reap(ctx); err != nil {
		t.Fatal(err)
	}
	record, err := store.Get(ctx, testAccess(), job.ID, AccessInspect)
	if err != nil {
		t.Fatalf("Get before interrupted expiry: %v", err)
	}
	if record.State != StateInterrupted ||
		!record.ExpiresAt.Equal(time.Date(2026, 8, 29, 12, 30, 0, 0, time.UTC)) {
		t.Fatalf("interrupted record before expiry = %#v", record)
	}

	clock.now = record.ExpiresAt
	if _, err := store.Get(ctx, testAccess(), job.ID, AccessInspect); !errors.Is(err, ErrExpired) {
		t.Fatalf("Get at interrupted expiry error = %v, want ErrExpired", err)
	}
	if stats, err := store.Stats(ctx); err != nil || stats.Jobs != 1 {
		t.Fatalf("Stats at interrupted expiry = %#v, %v", stats, err)
	}
	clock.now = clock.now.Add(DefaultTombstoneRetention + time.Microsecond)
	if err := store.Reap(ctx); err != nil {
		t.Fatal(err)
	}
	if stats, err := store.Stats(ctx); err != nil || stats.Jobs != 0 {
		t.Fatalf("Stats after interrupted tombstone grace = %#v, %v", stats, err)
	}
}

func TestCapacityRejectsWithoutEvictingUnexpiredArtifact(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 29, 10, 0, 0, 0, time.UTC)}
	store, _, _ := newTestStore(t, clock, 256)
	metrics := featureops.NewMetrics()
	store.observer = metrics
	job := testQueuedJob(t, "capacity", clock.now)
	if err := store.Admit(ctx, job); err != nil {
		t.Fatal(err)
	}
	completed := completeTestJob(job, clock.now, time.Hour)
	if err := store.Finalize(ctx, completed); err != nil {
		t.Fatal(err)
	}
	large := searchjobs.ResultRow{Ordinal: 0, Values: []searchjobs.Value{searchjobs.StringValue(string(make([]byte, 512)))}}
	_, err := store.PersistResults(ctx, testAccess(), job.ID, &sourceLease{
		schema: *completed.Schema, rows: []searchjobs.ResultRow{large}, generation: 1,
	})
	if !errors.Is(err, ErrCapacity) {
		t.Fatalf("PersistResults error = %v", err)
	}
	record, err := store.Get(ctx, testAccess(), job.ID, AccessInspect)
	if err != nil {
		t.Fatal(err)
	}
	if record.ArtifactPresent {
		t.Fatal("capacity failure published a partial artifact")
	}
	if record.State != StateQueued {
		t.Fatalf("capacity failure exposed state = %v, want queued staging state", record.State)
	}
	failed := completed
	failed.State = searchjobs.StateFailed
	failed.Version++
	failed.Schema = nil
	failed.RowCount = 0
	failed.ResultBytes = 0
	failed.Failure = &searchjobs.Failure{
		Code: searchjobs.FailureStorageUnavailable, Message: "retained search results are unavailable",
	}
	if err := store.Finalize(context.Background(), failed); err != nil {
		t.Fatalf("terminal publication compensation error = %v", err)
	}
	record, err = store.Get(ctx, testAccess(), job.ID, AccessInspect)
	if err != nil || record.State != StateFailed || record.ArtifactPresent ||
		record.Job.Failure == nil || record.Job.Failure.Code != searchjobs.FailureStorageUnavailable {
		t.Fatalf("terminal publication compensation = %#v, %v", record, err)
	}
	rejected := metrics.Snapshot().Counter(
		featureops.FeatureDurableArtifacts,
		featureops.OperationAdmission,
		featureops.OutcomeCapacityRejected,
	)
	if rejected.Events != 1 || rejected.Items != 1 || rejected.Bytes <= 256 {
		t.Fatalf("artifact-capacity counter = %#v", rejected)
	}
}

func TestCompletedStateIsPublishedAtomicallyWithArtifact(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 29, 11, 0, 0, 0, time.UTC)}
	store, _, _ := newTestStore(t, clock, DefaultMaximumBytes)
	job := testQueuedJob(t, "atomic-completion", clock.now)
	if err := store.Admit(ctx, job); err != nil {
		t.Fatal(err)
	}
	completed := completeTestJob(job, clock.now, time.Hour)
	if err := store.Finalize(ctx, completed); err != nil {
		t.Fatal(err)
	}
	staged, err := store.Get(ctx, testAccess(), job.ID, AccessInspect)
	if err != nil {
		t.Fatal(err)
	}
	if staged.State != StateQueued || staged.Job.State != searchjobs.StateQueued || staged.ArtifactPresent {
		t.Fatalf("staged completion became observable = %#v", staged)
	}
	if _, err := store.PersistResults(ctx, testAccess(), job.ID, &sourceLease{
		schema: *completed.Schema, rows: testRows(t), generation: 1,
	}); err != nil {
		t.Fatal(err)
	}
	published, err := store.Get(ctx, testAccess(), job.ID, AccessInspect)
	if err != nil {
		t.Fatal(err)
	}
	if published.State != StateCompleted || !published.ArtifactPresent {
		t.Fatalf("published completion = %#v", published)
	}
	// Replaying terminal metadata after an ambiguous caller outcome must not
	// hide an artifact that was already published.
	if err := store.Finalize(ctx, completed); err != nil {
		t.Fatal(err)
	}
	replayed, err := store.Get(ctx, testAccess(), job.ID, AccessInspect)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.State != StateCompleted || !replayed.ArtifactPresent {
		t.Fatalf("replayed completion = %#v", replayed)
	}
}

func TestConcurrentReadersCannotObserveCompletedBeforeArtifactPublication(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 29, 11, 30, 0, 0, time.UTC)}
	store, _, _ := newTestStore(t, clock, DefaultMaximumBytes)
	job := testQueuedJob(t, "publication-barrier", clock.now)
	if err := store.Admit(ctx, job); err != nil {
		t.Fatal(err)
	}
	completed := completeTestJob(job, clock.now, time.Hour)
	if err := store.Finalize(ctx, completed); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	persisted := make(chan error, 1)
	rows := testRows(t)
	go func() {
		_, err := store.PersistResults(ctx, testAccess(), job.ID, &blockingSourceLease{
			sourceLease: &sourceLease{schema: *completed.Schema, rows: rows, generation: 1},
			started:     started,
			release:     release,
		})
		persisted <- err
	}()
	<-started
	duringPublication, err := store.Get(ctx, testAccess(), job.ID, AccessInspect)
	if err != nil {
		t.Fatal(err)
	}
	if duringPublication.State != StateQueued ||
		duringPublication.Job.State != searchjobs.StateQueued ||
		duringPublication.ArtifactPresent {
		t.Fatalf("reader crossed publication barrier: %#v", duringPublication)
	}
	close(release)
	if err := <-persisted; err != nil {
		t.Fatal(err)
	}
	afterPublication, err := store.Get(ctx, testAccess(), job.ID, AccessInspect)
	if err != nil {
		t.Fatal(err)
	}
	if afterPublication.State != StateCompleted ||
		afterPublication.Job.State != searchjobs.StateCompleted ||
		!afterPublication.ArtifactPresent {
		t.Fatalf("completed publication is incomplete: %#v", afterPublication)
	}
}

func TestDifferentJobsPublishArtifactsConcurrently(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 29, 11, 45, 0, 0, time.UTC)}
	store, _, _ := newTestStore(t, clock, DefaultMaximumBytes)
	release := make(chan struct{})
	results := make(chan error, 2)
	started := []chan struct{}{make(chan struct{}), make(chan struct{})}
	completedJobs := make([]searchjobs.Job, len(started))
	for index, id := range []string{"parallel-publication-a", "parallel-publication-b"} {
		job := testQueuedJob(t, id, clock.now.Add(time.Duration(index)*time.Second))
		if err := store.Admit(ctx, job); err != nil {
			t.Fatal(err)
		}
		completed := completeTestJob(job, job.CreatedAt, time.Hour)
		if err := store.Finalize(ctx, completed); err != nil {
			t.Fatal(err)
		}
		completedJobs[index] = completed
	}
	for index, completed := range completedJobs {
		rows := testRows(t)
		go func(started chan struct{}, job searchjobs.Job, rows []searchjobs.ResultRow) {
			_, err := store.PersistResults(ctx, testAccess(), job.ID, &blockingSourceLease{
				sourceLease: &sourceLease{
					schema: *job.Schema, rows: rows, generation: 1,
				},
				started: started,
				release: release,
			})
			results <- err
		}(started[index], completed, rows)
	}

	concurrent := true
	for _, signal := range started {
		select {
		case <-signal:
		case <-time.After(time.Second):
			concurrent = false
		}
	}
	close(release)
	for range started {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if !concurrent {
		t.Fatal("one artifact publication blocked another job's result stream")
	}
}

func TestExclusiveDirectoryLock(t *testing.T) {
	ctx := context.Background()
	directory := filepath.Join(t.TempDir(), "search-artifacts")
	database, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	clock := &testClock{now: time.Now()}
	first := openTestStore(t, database.SQLDB(), directory, clock, DefaultMaximumBytes)
	defer func() { _ = first.Close() }()
	_, err = New(ctx, Config{DB: database.SQLDB(), Directory: directory, Clock: clock.Now, CleanupInterval: -1})
	if !errors.Is(err, ErrDirectoryInUse) {
		t.Fatalf("second New error = %v", err)
	}
}

func newTestStore(t *testing.T, clock *testClock, maximumBytes uint64) (*Store, *control.DB, string) {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "search-artifacts")
	database, err := control.Open(context.Background(), filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return openTestStore(t, database.SQLDB(), directory, clock, maximumBytes), database, directory
}

func openTestStore(t *testing.T, database *sql.DB, directory string, clock *testClock, maximumBytes uint64) *Store {
	t.Helper()
	store, err := New(context.Background(), Config{
		DB:                 database,
		Directory:          directory,
		Clock:              clock.Now,
		MaximumBytes:       maximumBytes,
		CleanupInterval:    -1,
		TombstoneRetention: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func testQueuedJob(t *testing.T, id string, created time.Time) searchjobs.Job {
	t.Helper()
	rangeValue, err := searchtime.NewAbsoluteRange(created.Add(-time.Hour), created)
	if err != nil {
		t.Fatal(err)
	}
	return searchjobs.Job{
		ID:        id,
		Version:   1,
		OwnerID:   "owner",
		TenantID:  "tenant",
		SPL:       "index=main",
		TimeRange: rangeValue.Intent(),
		State:     searchjobs.StateQueued,
		CreatedAt: created,
	}
}

func completeTestJob(job searchjobs.Job, finished time.Time, lifetime time.Duration) searchjobs.Job {
	job.Version = 6
	job.State = searchjobs.StateCompleted
	job.StartedAt = finished
	job.FinishedAt = finished
	job.ExpiresAt = finished.Add(lifetime)
	job.Schema = &searchjobs.Schema{Columns: []searchjobs.Column{{Name: "value", Kind: searchjobs.ValueKindMixed}}}
	job.RowCount = 2
	return job
}

func testRows(t *testing.T) []searchjobs.ResultRow {
	t.Helper()
	decimal, err := searchjobs.DecimalValue("123.450")
	if err != nil {
		t.Fatal(err)
	}
	object, err := searchjobs.ObjectValue(searchjobs.ObjectField{Name: "nested", Value: searchjobs.BoolValue(true)})
	if err != nil {
		t.Fatal(err)
	}
	return []searchjobs.ResultRow{
		{Ordinal: 0, Values: []searchjobs.Value{searchjobs.ListValue(searchjobs.StringValue("hello"), decimal)}},
		{Ordinal: 1, Values: []searchjobs.Value{object}},
	}
}

func testAccess() searchjobs.AccessScope {
	return searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"}
}

func assertLeaseRows(t *testing.T, lease ResultLease, want []searchjobs.ResultRow) {
	t.Helper()
	var got []searchjobs.ResultRow
	for {
		row, ok, err := lease.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		got = append(got, row)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("leased rows = %#v, want %#v", got, want)
	}
}
