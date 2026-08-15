package searchjobs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

// preamblePath names one of the three reads that share lockEntryForAccess.
// Preview deliberately omits the closed-manager rejection the other two apply.
type preamblePath struct {
	name         string
	requiresOpen bool
	call         func(context.Context, *Manager, AccessScope, string) error
}

func preamblePaths() []preamblePath {
	return []preamblePath{
		{name: "preview", requiresOpen: false, call: func(_ context.Context, manager *Manager, access AccessScope, id string) error {
			_, err := manager.PreviewFor(access, id, 1)
			return err
		}},
		{name: "execution snapshot", requiresOpen: true, call: func(ctx context.Context, manager *Manager, access AccessScope, id string) error {
			_, err := manager.CompletedExecutionSnapshotFor(ctx, access, id)
			return err
		}},
		{name: "result lease", requiresOpen: true, call: func(ctx context.Context, manager *Manager, access AccessScope, id string) error {
			lease, err := manager.AcquireResultsFor(ctx, access, id)
			if lease != nil {
				_ = lease.Close()
			}
			return err
		}},
	}
}

func newPreambleManager(t *testing.T, clock *fakeClock, prefix string) (*Manager, string) {
	t.Helper()
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			return sink.AddRow([]Value{StringValue("row")})
		}),
		MaxPageSize:     4,
		DefaultPageSize: 1,
		RetentionTTL:    time.Minute,
		CleanupInterval: -1,
		Now:             clock.Now,
		NewID:           sequenceIDs(prefix),
	})
	created, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, manager, created.ID, StateCompleted)
	return manager, created.ID
}

// TestPreambleClosedManagerRejectionIsPathSpecific pins the asymmetry the
// extracted preamble encodes: only the two lease-bearing reads reject a closed
// manager, and that rejection precedes both the lookup and the tenant check.
func TestPreambleClosedManagerRejectionIsPathSpecific(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{now: time.Date(2026, time.July, 23, 13, 0, 0, 0, time.UTC)}
	manager, id := newPreambleManager(t, clock, "preamble-closed")
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	owner := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	stranger := AccessScope{TenantID: "other", OwnerID: "owner"}

	for _, path := range preamblePaths() {
		t.Run(path.name, func(t *testing.T) {
			// Close expires retained completed results, so the preview path
			// reports the entry state rather than shutdown.
			want := ErrExpired
			if path.requiresOpen {
				want = ErrClosed
			}
			if err := path.call(context.Background(), manager, owner, id); !errors.Is(err, want) {
				t.Fatalf("owner read on closed manager = %v, want %v", err, want)
			}
			// Closed outranks both a missing id and a foreign tenant.
			wantMissing := ErrNotFound
			if path.requiresOpen {
				wantMissing = ErrClosed
			}
			if err := path.call(context.Background(), manager, owner, "no-such-job"); !errors.Is(err, wantMissing) {
				t.Fatalf("missing id on closed manager = %v, want %v", err, wantMissing)
			}
			if err := path.call(context.Background(), manager, stranger, id); !errors.Is(err, wantMissing) {
				t.Fatalf("foreign tenant on closed manager = %v, want %v", err, wantMissing)
			}
		})
	}
}

// TestPreambleForeignTenantNeverExpiresEntry proves the tenant check runs
// before expiry resolution, so a cross-tenant probe cannot drive another
// tenant's job into StateExpired as an observable side effect.
func TestPreambleForeignTenantNeverExpiresEntry(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{now: time.Date(2026, time.July, 23, 13, 0, 0, 0, time.UTC)}
	manager, id := newPreambleManager(t, clock, "preamble-tenant")
	clock.Add(2 * time.Minute)
	entry := manager.lookup(id)

	for _, path := range preamblePaths() {
		for _, stranger := range []AccessScope{
			{TenantID: "other", OwnerID: "owner"},
			{TenantID: "tenant", OwnerID: "other"},
		} {
			err := path.call(context.Background(), manager, stranger, id)
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("%s foreign %+v = %v, want ErrNotFound", path.name, stranger, err)
			}
			entry.mu.RLock()
			state := entry.job.State
			entry.mu.RUnlock()
			if state != StateCompleted {
				t.Fatalf("%s foreign read expired the entry: state = %v", path.name, state)
			}
		}
	}
	owner := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	if _, err := manager.PreviewFor(owner, id, 1); !errors.Is(err, ErrExpired) {
		t.Fatalf("owner read after foreign probes = %v, want ErrExpired", err)
	}
}

// TestPreambleResolvesExpiryAfterEntryLockWait covers the lease and execution
// snapshot paths; preview already has an equivalent single-path regression in
// TestPreviewForRechecksExpiryAfterWaitingForEntryLock.
func TestPreambleResolvesExpiryAfterEntryLockWait(t *testing.T) {
	t.Parallel()

	for _, path := range preamblePaths()[1:] {
		t.Run(path.name, func(t *testing.T) {
			t.Parallel()

			clock := &fakeClock{now: time.Date(2026, time.July, 23, 13, 0, 0, 0, time.UTC)}
			manager, id := newPreambleManager(t, clock, "preamble-wait")
			entry := manager.lookup(id)
			entry.mu.Lock()

			result := make(chan error, 1)
			go func() {
				result <- path.call(
					context.Background(),
					manager,
					AccessScope{TenantID: "tenant", OwnerID: "owner"},
					id,
				)
			}()
			time.Sleep(10 * time.Millisecond)
			// The blocked reader sampled no clock yet; it must observe this one.
			clock.Add(2 * time.Minute)
			entry.mu.Unlock()

			select {
			case err := <-result:
				if !errors.Is(err, ErrExpired) {
					t.Fatalf("%s after lock wait = %v, want ErrExpired", path.name, err)
				}
			case <-time.After(3 * time.Second):
				t.Fatalf("%s did not return", path.name)
			}
		})
	}
}

var errPreambleProbe = errors.New("preamble probe cancellation")

// entryLockProbeContext reports cancellation only while the job entry lock is
// held, which is true for exactly the one contextError call the preamble makes
// between acquiring entry.mu and checking the tenant.
type entryLockProbeContext struct {
	context.Context
	entry *jobEntry
}

func (probe entryLockProbeContext) Err() error {
	if probe.entry.mu.TryLock() {
		probe.entry.mu.Unlock()
		return nil
	}
	return errPreambleProbe
}

// TestPreambleContextErrorPrecedesTenantCheck asserts the preamble reports
// cancellation before it reports ErrNotFound for a foreign tenant, so a
// canceled caller is never told the job does not exist.
func TestPreambleContextErrorPrecedesTenantCheck(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{now: time.Date(2026, time.July, 23, 13, 0, 0, 0, time.UTC)}
	manager, id := newPreambleManager(t, clock, "preamble-ctx")
	ctx := entryLockProbeContext{Context: context.Background(), entry: manager.lookup(id)}

	for _, path := range preamblePaths()[1:] {
		stranger := AccessScope{TenantID: "other", OwnerID: "owner"}
		if err := path.call(ctx, manager, stranger, id); !errors.Is(err, errPreambleProbe) {
			t.Fatalf("%s canceled foreign read = %v, want the probe cancellation", path.name, err)
		}
	}
}

// TestPreambleConcurrentReadsRaceCloseAndExpiry hammers all three paths while
// the clock crosses retention and the manager shuts down. Foreign scopes must
// never succeed, and no path may report an unclassified error.
func TestPreambleConcurrentReadsRaceCloseAndExpiry(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{now: time.Date(2026, time.July, 23, 13, 0, 0, 0, time.UTC)}
	manager, id := newPreambleManager(t, clock, "preamble-race")
	owner := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	stranger := AccessScope{TenantID: "other", OwnerID: "owner"}
	classified := []error{
		ErrExpired, ErrClosed, ErrNotFound, ErrResultsUnavailable,
		ErrResultsNotReady, ErrCapacity, context.Canceled,
	}
	accept := func(name string, err error) {
		for _, known := range classified {
			if err == nil || errors.Is(err, known) {
				return
			}
		}
		t.Errorf("%s returned unclassified error %v", name, err)
	}

	var group sync.WaitGroup
	for worker := range 12 {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			path := preamblePaths()[worker%3]
			for round := range 200 {
				if (worker+round)%3 == 0 {
					if err := path.call(context.Background(), manager, stranger, id); !errors.Is(err, ErrNotFound) &&
						!errors.Is(err, ErrClosed) {
						t.Errorf("%s foreign scope = %v, want ErrNotFound or ErrClosed", path.name, err)
						return
					}
					continue
				}
				accept(path.name, path.call(context.Background(), manager, owner, id))
			}
		}(worker)
	}
	group.Go(func() {
		for range 50 {
			clock.Add(2 * time.Second)
		}
	})
	time.Sleep(5 * time.Millisecond)
	if err := manager.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
	group.Wait()
}
