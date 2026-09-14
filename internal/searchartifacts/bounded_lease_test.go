package searchartifacts

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

type boundedLeaseForTest interface {
	ResultLease
	BoundedRead() bool
	NextBounded(context.Context) (searchjobs.ResultRow, bool, uint64, func(), error)
}

type reservationTracker struct {
	mu       sync.Mutex
	limit    uint64
	used     uint64
	peak     uint64
	releases int
}

func (tracker *reservationTracker) reserve(bytes uint64) (func(), bool) {
	tracker.mu.Lock()
	if bytes > tracker.limit-tracker.used {
		tracker.mu.Unlock()
		return nil, false
	}
	tracker.used += bytes
	tracker.peak = max(tracker.peak, tracker.used)
	tracker.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			tracker.mu.Lock()
			tracker.used -= bytes
			tracker.releases++
			tracker.mu.Unlock()
		})
	}, true
}

func TestAcquireBoundedReservesBeforeDecodeAndReleasesAtRowAndLeaseLifetimes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)}
	store, _, _ := newTestStore(t, clock, DefaultMaximumBytes)
	rows := []searchjobs.ResultRow{{Ordinal: 0, Values: []searchjobs.Value{
		searchjobs.StringValue("retained bounded row"),
	}}}
	job, _ := persistTestResults(t, store, clock, "bounded-acquire", rows)

	tracker := &reservationTracker{limit: 320 << 20}
	lease, err := store.AcquireBounded(ctx, testAccess(), job.ID, tracker.reserve)
	if err != nil {
		t.Fatal(err)
	}
	bounded, ok := lease.(boundedLeaseForTest)
	if !ok || !bounded.BoundedRead() {
		t.Fatalf("AcquireBounded returned %T without bounded reads", lease)
	}
	if _, _, err := lease.Next(ctx); !errors.Is(err, ErrInvalid) {
		t.Fatalf("ordinary Next on bounded lease error = %v, want ErrInvalid", err)
	}
	row, present, rowBytes, releaseRow, err := bounded.NextBounded(ctx)
	if err != nil || !present || row.Ordinal != 0 || rowBytes == 0 || releaseRow == nil {
		t.Fatalf("NextBounded = row %#v, present %t, bytes %d, release %v, err %v", row, present, rowBytes, releaseRow != nil, err)
	}
	tracker.mu.Lock()
	usedWithRow := tracker.used
	tracker.mu.Unlock()
	if usedWithRow <= rowBytes {
		t.Fatalf("reservation %d does not include acquire and row bytes %d", usedWithRow, rowBytes)
	}
	releaseRow()
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.used != 0 || tracker.peak == 0 || tracker.releases != 2 {
		t.Fatalf("reservation lifecycle = used %d peak %d releases %d", tracker.used, tracker.peak, tracker.releases)
	}
}

func TestAcquireBoundedRejectsCapacityBeforeArtifactDecode(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)}
	store, _, _ := newTestStore(t, clock, DefaultMaximumBytes)
	job, _ := persistTestResults(t, store, clock, "bounded-capacity", testRows(t))

	tracker := &reservationTracker{limit: 1}
	if _, err := store.AcquireBounded(ctx, testAccess(), job.ID, tracker.reserve); !errors.Is(err, ErrCapacity) {
		t.Fatalf("AcquireBounded error = %v, want ErrCapacity", err)
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.used != 0 || tracker.releases != 0 {
		t.Fatalf("failed reservation lifecycle = used %d releases %d", tracker.used, tracker.releases)
	}
}

func TestNextBoundedRejectsFrameBeforePayloadAllocation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)}
	store, _, _ := newTestStore(t, clock, DefaultMaximumBytes)
	job, _ := persistTestResults(t, store, clock, "bounded-row-capacity", testRows(t))

	calls := 0
	var acquireRelease func()
	lease, err := store.AcquireBounded(ctx, testAccess(), job.ID, func(_ uint64) (func(), bool) {
		calls++
		if calls != 1 {
			return nil, false
		}
		acquireRelease = func() {}
		return acquireRelease, true
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	bounded := lease.(boundedLeaseForTest)
	if _, present, _, release, err := bounded.NextBounded(ctx); !errors.Is(err, ErrCapacity) || present || release != nil {
		t.Fatalf("NextBounded capacity = present %t release %v err %v", present, release != nil, err)
	}
	if calls != 2 || acquireRelease == nil {
		t.Fatalf("reservation calls = %d, acquire release set = %t", calls, acquireRelease != nil)
	}
}

func TestNextBoundedRefusesMalformedFrameBeforeDecoderAndReleasesDecodeFailure(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)}
	store, database, directory := newTestStore(t, clock, DefaultMaximumBytes)
	job, _ := persistTestResults(t, store, clock, "bounded-malformed-row", testRows(t))
	path := filepath.Join(directory, artifactName(job.ID))
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	start, length := framedRowPayload(t, payload, 0)
	for index := range length {
		payload[start+index] = ' '
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE durable_search_jobs SET artifact_sha256 = ? WHERE id = ?`, digest[:], job.ID); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.invalidateArtifactLocked(job.ID)
	store.mu.Unlock()

	tracker := &reservationTracker{limit: 320 << 20}
	lease, err := store.AcquireBounded(ctx, testAccess(), job.ID, tracker.reserve)
	if err != nil {
		t.Fatal(err)
	}
	tracker.mu.Lock()
	tracker.limit = tracker.used
	tracker.mu.Unlock()
	bounded := lease.(boundedLeaseForTest)
	if _, present, _, release, err := bounded.NextBounded(ctx); !errors.Is(err, ErrCapacity) || present || release != nil {
		t.Fatalf("malformed row under rejected reservation = present %t release %v err %v", present, release != nil, err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}

	tracker = &reservationTracker{limit: 320 << 20}
	lease, err = store.AcquireBounded(ctx, testAccess(), job.ID, tracker.reserve)
	if err != nil {
		t.Fatal(err)
	}
	bounded = lease.(boundedLeaseForTest)
	if _, _, _, release, err := bounded.NextBounded(ctx); !errors.Is(err, ErrCorrupt) || release != nil {
		t.Fatalf("malformed row after reservation error = %v release=%v, want ErrCorrupt and internal release", err, release != nil)
	}
	tracker.mu.Lock()
	usedAfterDecodeFailure := tracker.used
	tracker.mu.Unlock()
	if usedAfterDecodeFailure == 0 {
		t.Fatal("decode failure released the lease-level reservation early")
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.used != 0 || tracker.releases < 2 {
		t.Fatalf("decode failure reservation lifecycle = used %d releases %d", tracker.used, tracker.releases)
	}
}

func TestAcquireBoundedLegacyReservesWholeArtifactAndReleasesMetadataFailure(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)}
	store, database, directory := newTestStore(t, clock, DefaultMaximumBytes)
	job := testQueuedJob(t, "bounded-legacy", clock.now)
	if err := store.Admit(ctx, job); err != nil {
		t.Fatal(err)
	}
	completed := completeTestJob(job, clock.now, time.Hour)
	if err := store.Finalize(ctx, completed); err != nil {
		t.Fatal(err)
	}
	rows := testRows(t)
	storedRows := make([]storedResultRow, len(rows))
	for index, row := range rows {
		storedRows[index], _ = storedRow(row)
	}
	payload, err := json.Marshal(storedArtifact{
		Version: legacyArtifactFormatVersion, JobID: job.ID, Generation: 8,
		Schema: *completed.Schema, Rows: storedRows,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	name := artifactName(job.ID)
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE durable_search_jobs
		SET state = ?, artifact_name = ?, artifact_sha256 = ?, artifact_size_bytes = ?
		WHERE id = ?`, StateCompleted, name, digest[:], len(payload), job.ID); err != nil {
		t.Fatal(err)
	}
	store = openTestStore(t, database.SQLDB(), directory, clock, DefaultMaximumBytes)
	tracker := &reservationTracker{limit: 320 << 20}
	lease, err := store.AcquireBounded(ctx, testAccess(), job.ID, tracker.reserve)
	if err != nil {
		t.Fatal(err)
	}
	minimum := uint64(len(payload))*boundedJSONDecodeMultiplier + boundedHeaderFixedBytes
	tracker.mu.Lock()
	peak := tracker.peak
	tracker.mu.Unlock()
	if peak < minimum {
		t.Fatalf("legacy peak reservation = %d, want at least %d", peak, minimum)
	}
	bounded := lease.(boundedLeaseForTest)
	if _, present, rowBytes, release, err := bounded.NextBounded(ctx); err != nil || !present || rowBytes != 0 || release != nil {
		t.Fatalf("legacy NextBounded = present %t bytes %d release %v err %v", present, rowBytes, release != nil, err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	tracker.mu.Lock()
	if tracker.used != 0 {
		tracker.mu.Unlock()
		t.Fatalf("legacy close retained %d bytes", tracker.used)
	}
	tracker.mu.Unlock()

	malformed := []byte(`{"version":1,"job_id":`)
	if err := os.WriteFile(path, malformed, 0o600); err != nil {
		t.Fatal(err)
	}
	digest = sha256.Sum256(malformed)
	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE durable_search_jobs SET artifact_sha256 = ?, artifact_size_bytes = ? WHERE id = ?`,
		digest[:], len(malformed), job.ID); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.invalidateArtifactLocked(job.ID)
	store.mu.Unlock()
	tracker = &reservationTracker{limit: 320 << 20}
	if _, err := store.AcquireBounded(ctx, testAccess(), job.ID, tracker.reserve); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("malformed legacy AcquireBounded error = %v, want ErrCorrupt", err)
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.used != 0 || tracker.releases != 1 {
		t.Fatalf("malformed legacy reservation lifecycle = used %d releases %d", tracker.used, tracker.releases)
	}
}
