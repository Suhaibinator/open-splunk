package searchartifacts

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestAcquirePublishedBoundedRejectsTerminalAndUnauthorizedWithoutWaiting(t *testing.T) {
	for _, outcome := range []string{"failed", "canceled", "interrupted", "expired", "wrong-owner", "missing"} {
		t.Run(outcome, func(t *testing.T) {
			clock := &testClock{now: time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)}
			store, database, directory := newTestStore(t, clock, DefaultMaximumBytes)
			job := testQueuedJob(t, "publication-terminal", clock.now)
			if err := store.Admit(t.Context(), job); err != nil {
				t.Fatal(err)
			}
			access := testAccess()
			want := ErrNotReady
			switch outcome {
			case "failed", "canceled", "expired":
				terminal := completeTestJob(job, clock.now, time.Minute)
				terminal.State = searchjobs.StateFailed
				if outcome == "canceled" {
					terminal.State = searchjobs.StateCanceled
				}
				if err := store.Finalize(t.Context(), terminal); err != nil {
					t.Fatal(err)
				}
				if outcome == "expired" {
					clock.now = clock.now.Add(time.Minute)
					want = ErrExpired
				}
			case "interrupted":
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				store = openTestStore(t, database.SQLDB(), directory, clock, DefaultMaximumBytes)
			case "wrong-owner":
				access.OwnerID = "other"
				want = ErrNotFound
			case "missing":
				job.ID = "absent"
				want = ErrNotFound
			}
			// A retry would terminate at the context deadline instead of the exact
			// terminal/authorization error. No decode reservation is permissible.
			ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
			defer cancel()
			reserved := false
			lease, err := store.AcquirePublishedBounded(ctx, access, job.ID, func(uint64) (func(), bool) { reserved = true; return func() {}, true })
			if lease != nil {
				_ = lease.Close()
			}
			if !errors.Is(err, want) || lease != nil || reserved {
				t.Fatalf("acquire = %v, lease=%v reserved=%t; want %v", err, lease, reserved, want)
			}
		})
	}
}

func TestAcquirePublishedBoundedPendingHonorsOperationDeadline(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)}
	store, _, _ := newTestStore(t, clock, DefaultMaximumBytes)
	job := testQueuedJob(t, "publication-pending", clock.now)
	if err := store.Admit(t.Context(), job); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	lease, err := store.AcquirePublishedBounded(ctx, testAccess(), job.ID, func(uint64) (func(), bool) {
		t.Error("pending acquisition reserved decode bytes")
		return func() {}, true
	})
	if lease != nil {
		_ = lease.Close()
	}
	if !errors.Is(err, context.DeadlineExceeded) || lease != nil {
		t.Fatalf("pending acquire = %v, lease=%v", err, lease)
	}
}

func TestAcquirePublishedBoundedDoesNotRetryArtifactErrors(t *testing.T) {
	for _, injected := range []error{ErrCorrupt, ErrConflict} {
		t.Run(injected.Error(), func(t *testing.T) {
			clock := &testClock{now: time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)}
			store, _, _ := newTestStore(t, clock, DefaultMaximumBytes)
			job, _ := persistTestResults(t, store, clock, "publication-artifact-error", testRows(t))
			store.mu.Lock()
			clear(store.verified)
			store.mu.Unlock()
			calls := 0
			store.verify = func(context.Context, *os.File, artifactCatalogIdentity) (artifactVerification, error) {
				calls++
				return artifactVerification{}, injected
			}
			ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
			defer cancel()
			tracker := &reservationTracker{limit: 320 << 20}
			lease, err := store.AcquirePublishedBounded(ctx, testAccess(), job.ID, tracker.reserve)
			if lease != nil {
				_ = lease.Close()
			}
			if !errors.Is(err, injected) || calls != 1 || lease != nil || tracker.used != 0 {
				t.Fatalf("artifact acquisition = %v, calls=%d, lease=%v, charged=%d", err, calls, lease, tracker.used)
			}
		})
	}
}
