package export

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type cleanupRejectJournal struct {
	*memoryExportJournal
	fail      atomic.Bool
	clearOnly bool
}

func (journal *cleanupRejectJournal) Update(ctx context.Context, row DurableJob) error {
	if journal.fail.Load() && (!journal.clearOnly || row.ArtifactName == "") {
		return errors.New("injected durable cleanup outage")
	}
	return journal.memoryExportJournal.Update(ctx, row)
}

type trackedExportQuota struct {
	mu    sync.Mutex
	bytes uint64
}

func (quota *trackedExportQuota) ReserveExport(_ context.Context, _, _ string, bytes uint64) error {
	quota.mu.Lock()
	defer quota.mu.Unlock()
	quota.bytes = bytes
	return nil
}

func (quota *trackedExportQuota) ReleaseExport(_ context.Context, _ string) error {
	quota.mu.Lock()
	defer quota.mu.Unlock()
	quota.bytes = 0
	return nil
}

func (quota *trackedExportQuota) used() uint64 {
	quota.mu.Lock()
	defer quota.mu.Unlock()
	return quota.bytes
}

func TestRemoteCleanupJournalOutageRetainsPinnedIdentityAndQuota(t *testing.T) {
	for _, clearOnly := range []bool{false, true} {
		name := "expiry publication outage"
		if clearOnly {
			name = "identity clearing outage"
		}
		t.Run(name, func(t *testing.T) {
			journal := &cleanupRejectJournal{memoryExportJournal: newMemoryExportJournal(), clearOnly: clearOnly}
			blobs := &remoteTestBlobs{objects: map[string][]byte{}}
			quota := &trackedExportQuota{}
			clock := &exportTestClock{now: time.Now().UTC()}
			source := &exportTestSource{datasets: map[string]exportTestDataset{"search": {schema: basicExportSchema(), rows: basicExportRows()}}}
			manager := newExportTestManager(t, source, func(config *Config) {
				config.Journal, config.Blobs, config.RetentionQuota = journal, blobs, quota
				config.Now = clock.Now
			})
			created, err := manager.Create(t.Context(), testAccess, CreateRequest{SearchJobID: "search", Format: FormatCSV})
			if err != nil {
				t.Fatal(err)
			}
			waitExportState(t, manager, testAccess, created.ID, StateCompleted)
			entry := manager.jobs[created.ID]
			entry.mu.RLock()
			identity := *entry.remoteIdentity
			entry.mu.RUnlock()
			journal.fail.Store(true)
			clock.Advance(time.Hour)
			if err := manager.Cleanup(t.Context()); err == nil {
				t.Fatal("durable cleanup outage was not reported")
			}
			entry.mu.RLock()
			retained := entry.journalPending && entry.artifactPath == identity.Key && entry.remoteIdentity != nil && *entry.remoteIdentity == identity
			entry.mu.RUnlock()
			if !retained || quota.used() == 0 {
				t.Fatal("cleanup lost its exact pinned identity or quota reservation")
			}
			journal.mu.Lock()
			row := journal.rows[created.ID]
			journal.mu.Unlock()
			if row.ArtifactName == "" {
				t.Fatal("durable cleanup intent disappeared before clearing committed")
			}
			blobs.mu.Lock()
			objects := len(blobs.objects)
			blobs.mu.Unlock()
			if (!clearOnly && objects != 1) || (clearOnly && objects != 0) {
				t.Fatalf("physical objects=%d, clearing-only=%t", objects, clearOnly)
			}
			journal.fail.Store(false)
			if err := manager.Cleanup(t.Context()); err != nil {
				t.Fatal(err)
			}
			entry.mu.RLock()
			clean := !entry.journalPending && entry.artifactPath == "" && entry.remoteIdentity == nil
			entry.mu.RUnlock()
			journal.mu.Lock()
			row = journal.rows[created.ID]
			journal.mu.Unlock()
			if !clean || row.ArtifactName != "" || quota.used() != 0 {
				t.Fatal("recovered cleanup failed to clear metadata and release quota")
			}
		})
	}
}

func TestRemoteCancelJournalOutageStopsWorkAndRetainsCleanupQuota(t *testing.T) {
	journal := &cleanupRejectJournal{memoryExportJournal: newMemoryExportJournal()}
	blobs := &remoteTestBlobs{objects: map[string][]byte{}}
	quota := &trackedExportQuota{}
	started := make(chan struct{}, 1)
	source := &exportTestSource{datasets: map[string]exportTestDataset{"search": {
		schema: basicExportSchema(), rows: basicExportRows(), nextGate: make(chan struct{}), nextStarted: started,
	}}}
	manager := newExportTestManager(t, source, func(config *Config) {
		config.Journal, config.Blobs, config.RetentionQuota = journal, blobs, quota
	})
	created, err := manager.Create(t.Context(), testAccess, CreateRequest{SearchJobID: "search", Format: FormatCSV})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("export source did not start")
	}
	journal.fail.Store(true)
	if _, err := manager.Cancel(t.Context(), testAccess, created.ID); !errors.Is(err, ErrArtifactUnavailable) {
		t.Fatalf("cancel error=%v, want unavailable durable state", err)
	}
	failed := waitExportState(t, manager, testAccess, created.ID, StateFailed)
	entry := manager.jobs[created.ID]
	entry.mu.RLock()
	retained := entry.journalPending && entry.remoteIdentity != nil && entry.artifactPath != ""
	entry.mu.RUnlock()
	if !retained || quota.used() == 0 || failed.Artifact != nil {
		t.Fatal("failed cancellation lost private cleanup identity or storage reservation")
	}
	journal.fail.Store(false)
	if err := manager.Cleanup(t.Context()); err != nil {
		t.Fatal(err)
	}
	if quota.used() != 0 {
		t.Fatal("recovered cancellation retained its quota")
	}
}
