package searchartifacts

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestReapQueriesUseDedicatedMaintenanceIndexes(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 30, 14, 0, 0, 0, time.UTC)}
	_, database, _ := newTestStore(t, clock, DefaultMaximumBytes)
	queries := []struct {
		indexName string
		query     string
		arguments []any
	}{
		{indexName: "durable_search_jobs_expiry", query: `
			SELECT id FROM durable_search_jobs
			WHERE tombstoned_at_us IS NULL AND expires_at_us IS NOT NULL
				AND expires_at_us <= ? AND state <> ?
			ORDER BY expires_at_us, id
			LIMIT ?`, arguments: []any{toUnixMicro(clock.now), StateExpired, DefaultReapBatchSize}},
		{indexName: "durable_search_jobs_tombstone", query: `
			SELECT id, artifact_name, artifact_size_bytes
			FROM durable_search_jobs
			WHERE state = ? AND tombstoned_at_us IS NOT NULL AND tombstoned_at_us <= ?
			ORDER BY tombstoned_at_us, id
			LIMIT ?`, arguments: []any{StateExpired, toUnixMicro(clock.now), DefaultReapBatchSize}},
	}
	for _, test := range queries {
		rows, err := database.SQLDB().QueryContext(ctx, "EXPLAIN QUERY PLAN "+test.query,
			test.arguments...)
		if err != nil {
			t.Fatalf("explain %s: %v", test.indexName, err)
		}
		var plan strings.Builder
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				_ = rows.Close()
				t.Fatalf("scan %s plan: %v", test.indexName, err)
			}
			plan.WriteString(detail)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(plan.String(), test.indexName) {
			t.Fatalf("%s query plan does not use its index: %s", test.indexName, plan.String())
		}
	}
}

func TestCapacityCountersAndBoundedReapRemainExactAcrossRestart(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 30, 15, 0, 0, 0, time.UTC)}
	directory := filepath.Join(t.TempDir(), "artifacts")
	database, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	open := func() *Store {
		store, err := New(ctx, Config{
			DB: database.SQLDB(), Directory: directory, Clock: clock.Now,
			MaximumJobs: 3, MaximumBytes: DefaultMaximumBytes,
			CleanupInterval: -1, TombstoneRetention: time.Minute, ReapBatchSize: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		return store
	}
	store := open()
	defer func() { _ = store.Close() }()
	for _, id := range []string{"capacity-a", "capacity-b", "capacity-c"} {
		if err := store.Admit(ctx, testQueuedJob(t, id, clock.now)); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Admit(ctx, testQueuedJob(t, "capacity-d", clock.now)); !errors.Is(err, ErrCapacity) {
		t.Fatalf("fourth Admit() error = %v, want ErrCapacity", err)
	}
	if stats, err := store.Stats(ctx); err != nil || stats.Jobs != 3 || stats.ArtifactBytes != 0 {
		t.Fatalf("Stats before reap = %#v, %v", stats, err)
	}
	old := toUnixMicro(clock.now)
	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE durable_search_jobs
		SET state = ?, expires_at_us = ?, tombstoned_at_us = ?`, StateExpired, old, old); err != nil {
		t.Fatal(err)
	}
	clock.now = clock.now.Add(2 * time.Minute)
	if err := store.Reap(ctx); err != nil {
		t.Fatal(err)
	}
	if stats, err := store.Stats(ctx); err != nil || stats.Jobs != 2 {
		t.Fatalf("Stats after one-item reap = %#v, %v", stats, err)
	}
	if err := store.Admit(ctx, testQueuedJob(t, "capacity-d", clock.now)); err != nil {
		t.Fatalf("Admit after reap: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = open()
	if stats, err := store.Stats(ctx); err != nil || stats.Jobs != 3 || stats.ArtifactBytes != 0 {
		t.Fatalf("Stats after restart = %#v, %v", stats, err)
	}
}

func TestAdmissionRejectsWhenArtifactByteCapacityIsAlreadyExhausted(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 30, 16, 0, 0, 0, time.UTC)}
	store, _, _ := newTestStore(t, clock, DefaultMaximumBytes)
	first := testQueuedJob(t, "byte-capacity-first", clock.now)
	if err := store.Admit(ctx, first); err != nil {
		t.Fatal(err)
	}
	completed := completeTestJob(first, clock.now, time.Hour)
	if err := store.Finalize(ctx, completed); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PersistResults(ctx, testAccess(), first.ID, &sourceLease{
		schema: *completed.Schema, rows: testRows(t), generation: 1,
	}); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.maximumBytes = store.artifactBytes
	store.mu.Unlock()

	second := testQueuedJob(t, "byte-capacity-second", clock.now.Add(time.Second))
	if err := store.Admit(ctx, second); !errors.Is(err, ErrCapacity) {
		t.Fatalf("Admit at byte capacity error = %v, want ErrCapacity", err)
	}
	if _, err := store.Get(ctx, testAccess(), first.ID, AccessInspect); err != nil {
		t.Fatalf("existing unexpired artifact was evicted: %v", err)
	}
	if _, err := store.Get(ctx, testAccess(), second.ID, AccessInspect); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rejected job lookup error = %v, want ErrNotFound", err)
	}
}

type failingAdmissionJournal struct{ err error }

func (journal failingAdmissionJournal) Admit(context.Context, searchjobs.Job) error {
	return journal.err
}
func (failingAdmissionJournal) Finalize(context.Context, searchjobs.Job) error { return nil }

func TestCompositeAdmissionFailureCompensatesDurableArtifactRecord(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 30, 17, 0, 0, 0, time.UTC)}
	store, _, _ := newTestStore(t, clock, DefaultMaximumBytes)
	journal := searchjobs.NewCompositeJournal(store, failingAdmissionJournal{err: errors.New("projection unavailable")})
	job := testQueuedJob(t, "compensated-artifact", clock.now)
	job.RetentionLifetime = 19 * time.Minute
	if err := journal.Admit(ctx, job); err == nil {
		t.Fatal("composite admission unexpectedly succeeded")
	}
	record, err := store.Get(ctx, testAccess(), job.ID, AccessInspect)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != StateCanceled || record.ExpiresAt.IsZero() ||
		!record.ExpiresAt.Equal(clock.now.Add(19*time.Minute)) {
		t.Fatalf("compensated artifact record = %#v", record)
	}
}
