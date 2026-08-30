package searchartifacts

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPublishedArtifactCacheAvoidsRepeatedVerification(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 30, 13, 0, 0, 0, time.UTC)}
	store, _, _ := newTestStore(t, clock, DefaultMaximumBytes)
	job, rows := persistTestResults(t, store, clock, "cached-pages", testRows(t))

	store.mu.Lock()
	cached, ok := store.verified[job.ID]
	store.mu.Unlock()
	if !ok || cached.Catalog.JobID != job.ID || cached.Verification.Format != artifactFormatVersion {
		t.Fatalf("published cache entry = %#v, present = %t", cached, ok)
	}

	var verifications atomic.Int32
	originalVerify := store.verify
	store.verify = func(
		ctx context.Context,
		file *os.File,
		catalog artifactCatalogIdentity,
	) (artifactVerification, error) {
		verifications.Add(1)
		return originalVerify(ctx, file, catalog)
	}
	for range 3 {
		lease, err := store.Acquire(ctx, testAccess(), job.ID)
		if err != nil {
			t.Fatal(err)
		}
		assertLeaseRows(t, lease, rows)
		if err := lease.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if verifications.Load() != 0 {
		t.Fatalf("cache hits performed %d full verifications", verifications.Load())
	}
}

func TestArtifactCacheDetectsSameSizeTamperingWithRestoredMtime(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 30, 13, 15, 0, 0, time.UTC)}
	store, _, directory := newTestStore(t, clock, DefaultMaximumBytes)
	job, _ := persistTestResults(t, store, clock, "cached-tamper", testRows(t))
	path := filepath.Join(directory, artifactName(job.ID))
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	payload[len(payload)/2] ^= 1
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}

	var verifications atomic.Int32
	originalVerify := store.verify
	store.verify = func(
		ctx context.Context,
		file *os.File,
		catalog artifactCatalogIdentity,
	) (artifactVerification, error) {
		verifications.Add(1)
		return originalVerify(ctx, file, catalog)
	}
	if _, err := store.Acquire(ctx, testAccess(), job.ID); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Acquire(tampered) error = %v, want ErrCorrupt", err)
	}
	if verifications.Load() != 1 {
		t.Fatalf("tamper triggered %d verifications, want 1", verifications.Load())
	}
	store.mu.Lock()
	_, cached := store.verified[job.ID]
	store.mu.Unlock()
	if cached {
		t.Fatal("corrupt artifact remained cached")
	}
}

func TestConcurrentArtifactCacheMissesCoalesce(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 30, 13, 30, 0, 0, time.UTC)}
	store, _, _ := newTestStore(t, clock, DefaultMaximumBytes)
	job, _ := persistTestResults(t, store, clock, "coalesced-verification", testRows(t))
	store.mu.Lock()
	store.invalidateArtifactLocked(job.ID)
	store.mu.Unlock()

	originalVerify := store.verify
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var verifications atomic.Int32
	store.verify = func(
		ctx context.Context,
		file *os.File,
		catalog artifactCatalogIdentity,
	) (artifactVerification, error) {
		verifications.Add(1)
		once.Do(func() { close(started) })
		select {
		case <-ctx.Done():
			return artifactVerification{}, ctx.Err()
		case <-release:
		}
		return originalVerify(ctx, file, catalog)
	}

	const readers = 8
	type result struct {
		lease ResultLease
		err   error
	}
	results := make(chan result, readers)
	for range readers {
		go func() {
			lease, err := store.Acquire(ctx, testAccess(), job.ID)
			results <- result{lease: lease, err: err}
		}()
	}
	<-started
	deadline := time.Now().Add(3 * time.Second)
	for {
		store.mu.Lock()
		pins := store.pins[job.ID]
		store.mu.Unlock()
		if pins == readers {
			break
		}
		if time.Now().After(deadline) {
			close(release)
			t.Fatalf("only %d of %d readers reached verification", pins, readers)
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	for range readers {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		if err := result.lease.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if verifications.Load() != 1 {
		t.Fatalf("concurrent misses performed %d verifications, want 1", verifications.Load())
	}
}

func TestArtifactCacheWaiterCancellationDoesNotCancelVerification(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 30, 13, 40, 0, 0, time.UTC)}
	store, _, _ := newTestStore(t, clock, DefaultMaximumBytes)
	job, _ := persistTestResults(t, store, clock, "verification-waiter-cancel", testRows(t))
	store.mu.Lock()
	store.invalidateArtifactLocked(job.ID)
	store.mu.Unlock()

	originalVerify := store.verify
	started := make(chan struct{})
	release := make(chan struct{})
	store.verify = func(
		ctx context.Context,
		file *os.File,
		catalog artifactCatalogIdentity,
	) (artifactVerification, error) {
		close(started)
		select {
		case <-ctx.Done():
			return artifactVerification{}, ctx.Err()
		case <-release:
		}
		return originalVerify(ctx, file, catalog)
	}

	type result struct {
		lease ResultLease
		err   error
	}
	leader := make(chan result, 1)
	go func() {
		lease, err := store.Acquire(ctx, testAccess(), job.ID)
		leader <- result{lease: lease, err: err}
	}()
	<-started
	waiterCtx, cancel := context.WithCancel(ctx)
	waiter := make(chan error, 1)
	go func() {
		lease, err := store.Acquire(waiterCtx, testAccess(), job.ID)
		if lease != nil {
			_ = lease.Close()
		}
		waiter <- err
	}()
	deadline := time.Now().Add(3 * time.Second)
	for {
		store.mu.Lock()
		pins := store.pins[job.ID]
		store.mu.Unlock()
		if pins == 2 {
			break
		}
		if time.Now().After(deadline) {
			close(release)
			t.Fatalf("waiter did not join verification; active pins = %d", pins)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-waiter; !errors.Is(err, context.Canceled) {
		close(release)
		t.Fatalf("canceled waiter error = %v", err)
	}
	close(release)
	acquired := <-leader
	if acquired.err != nil {
		t.Fatal(acquired.err)
	}
	if err := acquired.lease.Close(); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	_, cached := store.verified[job.ID]
	store.mu.Unlock()
	if !cached {
		t.Fatal("leader verification was canceled with its waiter")
	}
}

func TestArtifactCacheUsesCatalogDigestAndReconcilesOnRestart(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 30, 13, 45, 0, 0, time.UTC)}
	store, database, directory := newTestStore(t, clock, DefaultMaximumBytes)
	job, _ := persistTestResults(t, store, clock, "catalog-cache", testRows(t))
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openTestStore(t, database.SQLDB(), directory, clock, DefaultMaximumBytes)
	store.mu.Lock()
	_, reconciled := store.verified[job.ID]
	store.mu.Unlock()
	if !reconciled {
		t.Fatal("startup reconciliation did not populate the artifact cache")
	}

	wrongDigest := sha256.Sum256([]byte("wrong artifact"))
	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE durable_search_jobs SET artifact_sha256 = ? WHERE id = ?`, wrongDigest[:], job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Acquire(ctx, testAccess(), job.ID); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Acquire(catalog digest changed) error = %v, want ErrCorrupt", err)
	}
}

func TestReapAndCloseInvalidateArtifactCache(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 30, 14, 0, 0, 0, time.UTC)}
	store, _, _ := newTestStore(t, clock, DefaultMaximumBytes)
	job, _ := persistTestResults(t, store, clock, "cache-lifecycle", testRows(t))
	retainedJob, _ := persistTestResults(t, store, clock, "cache-unrelated", testRows(t))
	if _, err := store.Share(ctx, testAccess(), retainedJob.ID); err != nil {
		t.Fatal(err)
	}
	clock.now = clock.now.Add(2 * time.Hour)
	if err := store.Reap(ctx); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	_, cachedAfterReap := store.verified[job.ID]
	_, unrelatedStillCached := store.verified[retainedJob.ID]
	store.mu.Unlock()
	if cachedAfterReap {
		t.Fatal("reaped artifact remained cached")
	}
	if !unrelatedStillCached {
		t.Fatal("reaping one artifact invalidated an unrelated cache entry")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	cached := len(store.verified)
	store.mu.Unlock()
	if cached != 0 {
		t.Fatalf("closed store retained %d cache entries", cached)
	}
}
