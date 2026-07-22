package export

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func createCompletedDownloadJob(t *testing.T, manager *Manager, searchJobID string) Job {
	t.Helper()
	created, err := manager.Create(context.Background(), testAccess, CreateRequest{
		SearchJobID: searchJobID,
		Format:      FormatJSONLines,
	})
	if err != nil {
		t.Fatal(err)
	}
	return waitExportState(t, manager, testAccess, created.ID, StateCompleted)
}

func TestDownloadGrantIsScopedOpaqueHashedAndArtifactCapped(t *testing.T) {
	t.Parallel()
	clock := &exportTestClock{now: time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)}
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"search": {schema: basicExportSchema(), rows: basicExportRows()},
	}}
	manager := newExportTestManager(t, source, func(config *Config) {
		config.Now = clock.Now
		config.ArtifactTTL = 30 * time.Second
		config.DownloadGrantTTL = 2 * time.Minute
	})
	completed := createCompletedDownloadJob(t, manager, "search")
	wrongScope := searchjobs.AccessScope{TenantID: testAccess.TenantID, OwnerID: "other"}
	if _, err := manager.CreateDownloadGrant(context.Background(), wrongScope, completed.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("CreateDownloadGrant(cross-scope) = %v, want ErrNotFound", err)
	}

	grant, err := manager.CreateDownloadGrant(context.Background(), testAccess, completed.ID)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(grant.Token)
	if err != nil || len(decoded) != downloadTokenBytes {
		t.Fatalf("grant token = %q, decoded length %d, error %v", grant.Token, len(decoded), err)
	}
	if grant.ExpiresAt != completed.Artifact.ExpiresAt {
		t.Fatalf("grant expiration = %s, want artifact cap %s", grant.ExpiresAt, completed.Artifact.ExpiresAt)
	}
	digest := sha256.Sum256([]byte(grant.Token))
	manager.mu.RLock()
	stored, exists := manager.grants[digest]
	manager.mu.RUnlock()
	if !exists || stored.jobID != completed.ID {
		t.Fatalf("stored digest grant = (%#v, %t)", stored, exists)
	}
}

func TestDownloadGrantRedemptionIsAtomicAndSingleUse(t *testing.T) {
	t.Parallel()
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"search": {schema: basicExportSchema(), rows: basicExportRows()},
	}}
	manager := newExportTestManager(t, source, nil)
	completed := createCompletedDownloadJob(t, manager, "search")
	grant, err := manager.CreateDownloadGrant(context.Background(), testAccess, completed.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.RedeemDownload(context.Background(), "malformed"); err != ErrInvalidDownloadGrant {
		t.Fatalf("RedeemDownload(malformed) = %v, want exact ErrInvalidDownloadGrant", err)
	}
	if _, err := manager.RedeemDownload(context.Background(), strings.Repeat("x", 1<<20)); err != ErrInvalidDownloadGrant {
		t.Fatalf("RedeemDownload(oversized) = %v, want exact ErrInvalidDownloadGrant", err)
	}
	if _, err := manager.CreateDownloadGrant(context.Background(), testAccess, strings.Repeat("x", 1<<20)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("CreateDownloadGrant(oversized ID) = %v, want ErrNotFound", err)
	}
	unknown, _, err := newDownloadToken()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.RedeemDownload(context.Background(), unknown); err != ErrInvalidDownloadGrant {
		t.Fatalf("RedeemDownload(unknown) = %v, want exact ErrInvalidDownloadGrant", err)
	}

	const contenders = 32
	start := make(chan struct{})
	results := make(chan struct {
		lease ArtifactDownload
		err   error
	}, contenders)
	var wait sync.WaitGroup
	for range contenders {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			lease, redeemErr := manager.RedeemDownload(context.Background(), grant.Token)
			results <- struct {
				lease ArtifactDownload
				err   error
			}{lease: lease, err: redeemErr}
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	successes := 0
	for result := range results {
		if result.err == nil {
			successes++
			contents, readErr := io.ReadAll(result.lease)
			if readErr != nil || len(contents) == 0 {
				t.Fatalf("ReadAll() = %q, %v", contents, readErr)
			}
			if result.lease.Artifact() != *completed.Artifact {
				t.Fatalf("lease artifact = %#v, want %#v", result.lease.Artifact(), *completed.Artifact)
			}
			if closeErr := result.lease.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
		} else if !errors.Is(result.err, ErrInvalidDownloadGrant) {
			t.Fatalf("losing RedeemDownload() = %v, want ErrInvalidDownloadGrant", result.err)
		}
	}
	if successes != 1 {
		t.Fatalf("successful redemptions = %d, want 1", successes)
	}
	if _, err := manager.RedeemDownload(context.Background(), grant.Token); !errors.Is(err, ErrInvalidDownloadGrant) {
		t.Fatalf("replayed RedeemDownload() = %v, want ErrInvalidDownloadGrant", err)
	}
}

func TestDownloadGrantBoundsMetadataAndExpirationRecovery(t *testing.T) {
	t.Parallel()
	clock := &exportTestClock{now: time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)}
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"one": {schema: basicExportSchema(), rows: basicExportRows()},
		"two": {schema: basicExportSchema(), rows: basicExportRows()},
	}}
	manager := newExportTestManager(t, source, func(config *Config) {
		config.Now = clock.Now
		config.ArtifactTTL = time.Hour
		config.DownloadGrantTTL = time.Second
		config.MaxDownloadGrants = 2
		config.MaxDownloadGrantsPerJob = 1
	})
	one := createCompletedDownloadJob(t, manager, "one")
	two := createCompletedDownloadJob(t, manager, "two")

	manager.budgetMu.Lock()
	baselineMetadata := manager.totalMetadata
	manager.budgetMu.Unlock()
	grantBytes := downloadGrantMetadataFixed + uint64(len(one.ID))
	manager.maxTotalMetadata = baselineMetadata + grantBytes - 1
	if _, err := manager.CreateDownloadGrant(context.Background(), testAccess, one.ID); !errors.Is(err, ErrDownloadGrantCapacity) {
		t.Fatalf("CreateDownloadGrant(metadata short) = %v, want ErrDownloadGrantCapacity", err)
	}
	manager.maxTotalMetadata = baselineMetadata + 2*grantBytes
	first, err := manager.CreateDownloadGrant(context.Background(), testAccess, one.ID)
	if err != nil {
		t.Fatal(err)
	}
	manager.budgetMu.Lock()
	withFirst := manager.totalMetadata
	manager.budgetMu.Unlock()
	if withFirst != baselineMetadata+grantBytes {
		t.Fatalf("metadata with one grant = %d, want %d", withFirst, baselineMetadata+grantBytes)
	}
	if _, err := manager.CreateDownloadGrant(context.Background(), testAccess, one.ID); !errors.Is(err, ErrDownloadGrantCapacity) {
		t.Fatalf("CreateDownloadGrant(per-job full) = %v, want ErrDownloadGrantCapacity", err)
	}
	if _, err := manager.CreateDownloadGrant(context.Background(), testAccess, two.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.CreateDownloadGrant(context.Background(), testAccess, two.ID); !errors.Is(err, ErrDownloadGrantCapacity) {
		t.Fatalf("CreateDownloadGrant(global full) = %v, want ErrDownloadGrantCapacity", err)
	}

	clock.Advance(2 * time.Second)
	replacement, err := manager.CreateDownloadGrant(context.Background(), testAccess, one.ID)
	if err != nil {
		t.Fatalf("CreateDownloadGrant(after expiration purge) = %v", err)
	}
	if _, err := manager.RedeemDownload(context.Background(), first.Token); !errors.Is(err, ErrInvalidDownloadGrant) {
		t.Fatalf("expired RedeemDownload() = %v, want ErrInvalidDownloadGrant", err)
	}
	lease, err := manager.RedeemDownload(context.Background(), replacement.Token)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDownloadGrantReportsExistingJobStateErrors(t *testing.T) {
	t.Parallel()
	clock := &exportTestClock{now: time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)}
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"running": {
			schema:      basicExportSchema(),
			rows:        basicExportRows(),
			nextGate:    gate,
			nextStarted: started,
		},
		"queued": {schema: basicExportSchema()},
		"failed": {schema: basicExportSchema(), rows: basicExportRows(), rowCount: 2},
		"done":   {schema: basicExportSchema()},
	}}
	manager := newExportTestManager(t, source, func(config *Config) {
		config.Now = clock.Now
		config.MaxWorkers = 1
		config.ArtifactTTL = time.Second
	})
	running, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "running", Format: FormatCSV})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("running export did not start")
	}
	queued, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "queued", Format: FormatCSV})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{running.ID, queued.ID} {
		if _, err := manager.CreateDownloadGrant(context.Background(), testAccess, id); !errors.Is(err, ErrSourceNotReady) {
			t.Fatalf("CreateDownloadGrant(nonterminal %s) = %v, want ErrSourceNotReady", id, err)
		}
	}
	if _, err := manager.Cancel(context.Background(), testAccess, queued.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.CreateDownloadGrant(context.Background(), testAccess, queued.ID); !errors.Is(err, ErrSourceUnavailable) {
		t.Fatalf("CreateDownloadGrant(canceled) = %v, want ErrSourceUnavailable", err)
	}
	close(gate)
	waitExportState(t, manager, testAccess, running.ID, StateCompleted)

	failed, err := manager.Create(context.Background(), testAccess, CreateRequest{
		SearchJobID: "failed",
		Format:      FormatCSV,
		RowLimit:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitExportState(t, manager, testAccess, failed.ID, StateFailed)
	if _, err := manager.CreateDownloadGrant(context.Background(), testAccess, failed.ID); !errors.Is(err, ErrSourceUnavailable) {
		t.Fatalf("CreateDownloadGrant(failed) = %v, want ErrSourceUnavailable", err)
	}
	done := createCompletedDownloadJob(t, manager, "done")
	clock.Advance(2 * time.Second)
	if _, err := manager.CreateDownloadGrant(context.Background(), testAccess, done.ID); !errors.Is(err, ErrSourceExpired) {
		t.Fatalf("CreateDownloadGrant(expired) = %v, want ErrSourceExpired", err)
	}
	if _, err := manager.CreateDownloadGrant(context.Background(), testAccess, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("CreateDownloadGrant(missing) = %v, want ErrNotFound", err)
	}
}

func TestDownloadLeasePinsExpiredArtifactUntilFinalClose(t *testing.T) {
	t.Parallel()
	clock := &exportTestClock{now: time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)}
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"search": {schema: basicExportSchema(), rows: basicExportRows()},
	}}
	manager := newExportTestManager(t, source, func(config *Config) {
		config.Now = clock.Now
		config.ArtifactTTL = time.Second
		config.ExpiredRetention = time.Second
	})
	completed := createCompletedDownloadJob(t, manager, "search")
	firstGrant, err := manager.CreateDownloadGrant(context.Background(), testAccess, completed.ID)
	if err != nil {
		t.Fatal(err)
	}
	secondGrant, err := manager.CreateDownloadGrant(context.Background(), testAccess, completed.ID)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := manager.RedeemDownload(context.Background(), firstGrant.Token)
	if err != nil {
		t.Fatal(err)
	}
	artifactPath := filepath.Join(manager.artifactDir, completed.Artifact.FileName)
	clock.Advance(2 * time.Second)
	expired, err := manager.Get(context.Background(), testAccess, completed.ID)
	if err != nil || expired.State != StateExpired || expired.Artifact != nil {
		t.Fatalf("Get(after TTL) = (%#v, %v)", expired, err)
	}
	if _, err := os.Stat(artifactPath); err != nil {
		t.Fatalf("pinned artifact Stat() = %v", err)
	}
	if _, err := manager.RedeemDownload(context.Background(), secondGrant.Token); !errors.Is(err, ErrInvalidDownloadGrant) {
		t.Fatalf("artifact-expired RedeemDownload() = %v, want ErrInvalidDownloadGrant", err)
	}
	contents, err := io.ReadAll(lease)
	if err != nil || len(contents) == 0 {
		t.Fatalf("pinned ReadAll(after TTL) = %q, %v", contents, err)
	}
	if err := manager.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(artifactPath); err != nil {
		t.Fatalf("Cleanup removed pinned artifact: %v", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(artifactPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("artifact after final lease Close() Stat = %v, want not exist", err)
	}
	manager.budgetMu.Lock()
	retainedBytes := manager.totalBytes
	manager.budgetMu.Unlock()
	if retainedBytes != 0 {
		t.Fatalf("retained artifact bytes after final Close = %d", retainedBytes)
	}
}

func TestActiveDownloadRetainsMetadataAndHasSeparateCapacity(t *testing.T) {
	t.Parallel()
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"search": {schema: basicExportSchema(), rows: basicExportRows()},
	}}
	manager := newExportTestManager(t, source, func(config *Config) {
		config.MaxDownloadGrants = 1
		config.MaxDownloadGrantsPerJob = 1
		config.MaxActiveDownloads = 1
		config.MaxActiveDownloadsPerJob = 1
	})
	completed := createCompletedDownloadJob(t, manager, "search")
	manager.budgetMu.Lock()
	baseline := manager.totalMetadata
	manager.budgetMu.Unlock()
	grant, err := manager.CreateDownloadGrant(context.Background(), testAccess, completed.ID)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := manager.RedeemDownload(context.Background(), grant.Token)
	if err != nil {
		t.Fatal(err)
	}
	queuedGrant, err := manager.CreateDownloadGrant(context.Background(), testAccess, completed.ID)
	if err != nil {
		t.Fatalf("CreateDownloadGrant(while a download is active) = %v", err)
	}
	manager.budgetMu.Lock()
	whileOpen := manager.totalMetadata
	manager.budgetMu.Unlock()
	grantBytes := downloadGrantMetadataFixed + uint64(len(completed.ID))
	if whileOpen != baseline+2*grantBytes {
		t.Fatalf("open-lease metadata = %d, want %d", whileOpen, baseline+2*grantBytes)
	}
	if _, err := manager.CreateDownloadGrant(context.Background(), testAccess, completed.ID); !errors.Is(err, ErrDownloadGrantCapacity) {
		t.Fatalf("CreateDownloadGrant(outstanding grant) = %v, want ErrDownloadGrantCapacity", err)
	}
	if _, err := manager.RedeemDownload(context.Background(), queuedGrant.Token); !errors.Is(err, ErrDownloadGrantCapacity) {
		t.Fatalf("RedeemDownload(active limit) = %v, want ErrDownloadGrantCapacity", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	manager.budgetMu.Lock()
	afterClose := manager.totalMetadata
	manager.budgetMu.Unlock()
	if afterClose != baseline+grantBytes {
		t.Fatalf("metadata after lease close = %d, want %d", afterClose, baseline+grantBytes)
	}
	queuedLease, err := manager.RedeemDownload(context.Background(), queuedGrant.Token)
	if err != nil {
		t.Fatalf("RedeemDownload(after lease close) = %v", err)
	}
	concreteLease := queuedLease.(*DownloadLease)
	if err := queuedLease.Close(); err != nil {
		t.Fatal(err)
	}
	if concreteLease.manager != nil || concreteLease.entry != nil || concreteLease.jobID != "" || concreteLease.metadataBytes != 0 {
		t.Fatal("closed download lease retained manager-owned state")
	}
	manager.budgetMu.Lock()
	afterFinalClose := manager.totalMetadata
	manager.budgetMu.Unlock()
	if afterFinalClose != baseline {
		t.Fatalf("metadata after final lease close = %d, want %d", afterFinalClose, baseline)
	}
	manager.mu.RLock()
	entry := manager.jobs[completed.ID]
	activeDownloads := manager.activeDownloads
	heapEntries := len(manager.grantExpirations)
	manager.mu.RUnlock()
	entry.mu.RLock()
	downloadMapAllocated := entry.downloads != nil
	entry.mu.RUnlock()
	if activeDownloads != 0 || heapEntries != 0 || downloadMapAllocated {
		t.Fatalf("released download state = active %d heap %d mapAllocated %t", activeDownloads, heapEntries, downloadMapAllocated)
	}
}

func TestDownloadGrantHeapAndMapsReleaseChurnedStorage(t *testing.T) {
	t.Parallel()
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"search": {schema: basicExportSchema(), rows: basicExportRows()},
	}}
	manager := newExportTestManager(t, source, func(config *Config) {
		config.MaxDownloadGrants = 128
		config.MaxDownloadGrantsPerJob = 128
		config.MaxActiveDownloads = 1
		config.MaxActiveDownloadsPerJob = 1
	})
	completed := createCompletedDownloadJob(t, manager, "search")
	grants := make([]DownloadGrant, 100)
	for index := range grants {
		grant, err := manager.CreateDownloadGrant(context.Background(), testAccess, completed.ID)
		if err != nil {
			t.Fatal(err)
		}
		grants[index] = grant
	}
	for _, grant := range grants {
		lease, err := manager.RedeemDownload(context.Background(), grant.Token)
		if err != nil {
			t.Fatal(err)
		}
		if err := lease.Close(); err != nil {
			t.Fatal(err)
		}
	}
	manager.mu.RLock()
	grantCount := len(manager.grants)
	heapLength, heapCapacity := len(manager.grantExpirations), cap(manager.grantExpirations)
	highWater := manager.grantMapHighWater
	jobCounts := len(manager.grantsByJob)
	downloadCounts := len(manager.downloadsByJob)
	manager.mu.RUnlock()
	if grantCount != 0 || heapLength != 0 || heapCapacity != 0 || highWater != 0 || jobCounts != 0 || downloadCounts != 0 {
		t.Fatalf("churned grant storage = grants %d heap %d/%d highWater %d jobCounts %d downloadCounts %d",
			grantCount, heapLength, heapCapacity, highWater, jobCounts, downloadCounts)
	}
}

func TestRedeemCancellationWhileWaitingForManagerDoesNotConsumeGrant(t *testing.T) {
	t.Parallel()
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"search": {schema: basicExportSchema(), rows: basicExportRows()},
	}}
	manager := newExportTestManager(t, source, nil)
	completed := createCompletedDownloadJob(t, manager, "search")
	grant, err := manager.CreateDownloadGrant(context.Background(), testAccess, completed.ID)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	result := make(chan error, 1)
	manager.mu.Lock()
	go func() {
		close(started)
		_, redeemErr := manager.RedeemDownload(ctx, grant.Token)
		result <- redeemErr
	}()
	<-started
	cancel()
	manager.mu.Unlock()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("RedeemDownload(canceled while waiting) = %v, want context.Canceled", err)
	}
	lease, err := manager.RedeemDownload(context.Background(), grant.Token)
	if err != nil {
		t.Fatalf("RedeemDownload(after canceled waiter) = %v", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGrantAndArtifactExpiryAreCheckedAfterManagerLockWait(t *testing.T) {
	t.Parallel()
	clock := &exportTestClock{now: time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)}
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"search": {schema: basicExportSchema(), rows: basicExportRows()},
	}}
	manager := newExportTestManager(t, source, func(config *Config) {
		config.Now = clock.Now
		config.ArtifactTTL = time.Second
		config.DownloadGrantTTL = time.Second
	})
	completed := createCompletedDownloadJob(t, manager, "search")
	grant, err := manager.CreateDownloadGrant(context.Background(), testAccess, completed.ID)
	if err != nil {
		t.Fatal(err)
	}

	redeemStarted := make(chan struct{})
	createStarted := make(chan struct{})
	redeemed := make(chan error, 1)
	created := make(chan error, 1)
	manager.mu.Lock()
	go func() {
		close(redeemStarted)
		_, redeemErr := manager.RedeemDownload(context.Background(), grant.Token)
		redeemed <- redeemErr
	}()
	go func() {
		close(createStarted)
		_, createErr := manager.CreateDownloadGrant(context.Background(), testAccess, completed.ID)
		created <- createErr
	}()
	<-redeemStarted
	<-createStarted
	// Give the call an opportunity to reach the held lock; correctness must
	// still use the clock sampled only after that lock is acquired.
	time.Sleep(10 * time.Millisecond)
	clock.Advance(2 * time.Second)
	manager.mu.Unlock()
	if err := <-redeemed; err != ErrInvalidDownloadGrant {
		t.Fatalf("RedeemDownload(after lock-delayed expiry) = %v, want exact ErrInvalidDownloadGrant", err)
	}
	if err := <-created; !errors.Is(err, ErrSourceExpired) {
		t.Fatalf("CreateDownloadGrant(after lock-delayed expiry) = %v, want ErrSourceExpired", err)
	}
}

func TestGrantChecksAreRepeatedAfterEntryLockWait(t *testing.T) {
	t.Parallel()
	clock := &exportTestClock{now: time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)}
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"search": {schema: basicExportSchema(), rows: basicExportRows()},
	}}
	manager := newExportTestManager(t, source, func(config *Config) {
		config.Now = clock.Now
		config.ArtifactTTL = time.Second
		config.DownloadGrantTTL = time.Second
	})
	completed := createCompletedDownloadJob(t, manager, "search")
	grant, err := manager.CreateDownloadGrant(context.Background(), testAccess, completed.ID)
	if err != nil {
		t.Fatal(err)
	}
	manager.mu.RLock()
	entry := manager.jobs[completed.ID]
	manager.mu.RUnlock()
	if entry == nil {
		t.Fatal("completed entry is missing")
	}

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	canceledResult := make(chan error, 1)
	entry.mu.Lock()
	go func() {
		close(started)
		_, redeemErr := manager.RedeemDownload(ctx, grant.Token)
		canceledResult <- redeemErr
	}()
	<-started
	time.Sleep(10 * time.Millisecond)
	cancel()
	entry.mu.Unlock()
	if err := <-canceledResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("RedeemDownload(canceled behind entry lock) = %v, want context.Canceled", err)
	}

	createStarted := make(chan struct{})
	redeemStarted := make(chan struct{})
	created := make(chan error, 1)
	redeemed := make(chan error, 1)
	entry.mu.Lock()
	go func() {
		close(createStarted)
		_, createErr := manager.CreateDownloadGrant(context.Background(), testAccess, completed.ID)
		created <- createErr
	}()
	go func() {
		close(redeemStarted)
		_, redeemErr := manager.RedeemDownload(context.Background(), grant.Token)
		redeemed <- redeemErr
	}()
	<-createStarted
	<-redeemStarted
	time.Sleep(10 * time.Millisecond)
	clock.Advance(2 * time.Second)
	entry.mu.Unlock()
	if err := <-created; !errors.Is(err, ErrSourceExpired) {
		t.Fatalf("CreateDownloadGrant(expired behind entry lock) = %v, want ErrSourceExpired", err)
	}
	if err := <-redeemed; err != ErrInvalidDownloadGrant {
		t.Fatalf("RedeemDownload(expired behind entry lock) = %v, want exact ErrInvalidDownloadGrant", err)
	}
}

func TestRedeemDownloadRejectsArtifactSubstitutionWithoutExposingPaths(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		tamper func(*testing.T, string)
	}{
		{
			name: "size",
			tamper: func(t *testing.T, path string) {
				t.Helper()
				if err := os.Truncate(path, 0); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "same-size-replacement",
			tamper: func(t *testing.T, path string) {
				t.Helper()
				contents, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				replacement := filepath.Join(filepath.Dir(path), "replacement")
				if err := os.WriteFile(replacement, contents, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(replacement, path); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "symlink",
			tamper: func(t *testing.T, path string) {
				t.Helper()
				target := filepath.Join(filepath.Dir(path), "target")
				if err := os.WriteFile(target, []byte("not the artifact"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Base(target), path); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			source := &exportTestSource{datasets: map[string]exportTestDataset{
				"search": {schema: basicExportSchema(), rows: basicExportRows()},
			}}
			manager := newExportTestManager(t, source, nil)
			completed := createCompletedDownloadJob(t, manager, "search")
			grant, err := manager.CreateDownloadGrant(context.Background(), testAccess, completed.ID)
			if err != nil {
				t.Fatal(err)
			}
			artifactPath := filepath.Join(manager.artifactDir, completed.Artifact.FileName)
			test.tamper(t, artifactPath)
			if _, err := manager.RedeemDownload(context.Background(), grant.Token); err != ErrArtifactUnavailable {
				t.Fatalf("RedeemDownload(tampered) = %v, want exact path-free ErrArtifactUnavailable", err)
			}
			if _, err := manager.RedeemDownload(context.Background(), grant.Token); !errors.Is(err, ErrInvalidDownloadGrant) {
				t.Fatalf("RedeemDownload(replayed after tamper) = %v, want ErrInvalidDownloadGrant", err)
			}
		})
	}
}

func TestCanceledRedeemDoesNotConsumeGrantAndManagerCloseRevokesAndCloses(t *testing.T) {
	t.Parallel()
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"search": {schema: basicExportSchema(), rows: basicExportRows()},
	}}
	manager := newExportTestManager(t, source, nil)
	completed := createCompletedDownloadJob(t, manager, "search")
	grant, err := manager.CreateDownloadGrant(context.Background(), testAccess, completed.ID)
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := manager.RedeemDownload(canceled, grant.Token); !errors.Is(err, context.Canceled) {
		t.Fatalf("RedeemDownload(canceled) = %v, want context.Canceled", err)
	}
	lease, err := manager.RedeemDownload(context.Background(), grant.Token)
	if err != nil {
		t.Fatal(err)
	}
	revoked, err := manager.CreateDownloadGrant(context.Background(), testAccess, completed.ID)
	if err != nil {
		t.Fatal(err)
	}
	artifactDir := manager.artifactDir
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := lease.Read(make([]byte, 1)); err != ErrArtifactUnavailable {
		t.Fatalf("Read(after Manager.Close) = %v, want exact ErrArtifactUnavailable", err)
	}
	if _, err := manager.RedeemDownload(context.Background(), revoked.Token); err != ErrInvalidDownloadGrant {
		t.Fatalf("RedeemDownload(revoked) = %v, want exact ErrInvalidDownloadGrant", err)
	}
	if _, err := os.Stat(artifactDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("artifact session after Manager.Close Stat = %v, want not exist", err)
	}
}

func TestDownloadGrantConfigurationBounds(t *testing.T) {
	t.Parallel()
	source := &exportTestSource{datasets: map[string]exportTestDataset{}}
	tests := []Config{
		{DownloadGrantTTL: -time.Second},
		{DownloadGrantTTL: maximumDownloadGrantTTL + time.Nanosecond},
		{MaxDownloadGrants: -1},
		{MaxDownloadGrants: maximumDownloadGrants + 1},
		{MaxDownloadGrantsPerJob: -1},
		{MaxDownloadGrantsPerJob: maximumGrantsPerJob + 1},
		{MaxDownloadGrants: 1, MaxDownloadGrantsPerJob: 2},
		{MaxActiveDownloads: -1},
		{MaxActiveDownloads: maximumActiveDownloads + 1},
		{MaxActiveDownloadsPerJob: -1},
		{MaxActiveDownloadsPerJob: maximumActivePerJob + 1},
		{MaxActiveDownloads: 1, MaxActiveDownloadsPerJob: 2},
	}
	for _, config := range tests {
		config.Source = source
		config.ArtifactDir = t.TempDir()
		if manager, err := New(config); err == nil {
			_ = manager.Close()
			t.Errorf("New(%+v) unexpectedly succeeded", config)
		}
	}
}
