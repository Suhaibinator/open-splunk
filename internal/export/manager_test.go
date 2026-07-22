package export

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

var testAccess = searchjobs.AccessScope{TenantID: "tenant-a", OwnerID: "owner-a"}

type exportTestDataset struct {
	schema          searchjobs.Schema
	rows            []searchjobs.ResultRow
	rowCount        uint64
	rowCountInexact bool
	truncated       bool
	nextGate        <-chan struct{}
	nextExitGate    <-chan struct{}
	nextStarted     chan<- struct{}
	ignoreNextCtx   bool
	unboundContext  bool
	nextErr         error
	expectedScope   *searchjobs.AccessScope
}

type exportTestSource struct {
	mu           sync.Mutex
	datasets     map[string]exportTestDataset
	errors       map[string]error
	leases       []*exportTestLease
	acquires     int
	gate         <-chan struct{}
	started      chan<- struct{}
	beforeReturn func()
}

func (source *exportTestSource) AcquireResultsFor(ctx context.Context, access searchjobs.AccessScope, id string) (searchjobs.ResultLease, error) {
	if source.started != nil {
		select {
		case source.started <- struct{}{}:
		default:
		}
	}
	if source.gate != nil {
		select {
		case <-source.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	source.acquires++
	if err := source.errors[id]; err != nil {
		return nil, err
	}
	dataset, exists := source.datasets[id]
	if !exists {
		return nil, searchjobs.ErrNotFound
	}
	if dataset.expectedScope != nil && *dataset.expectedScope != access {
		return nil, searchjobs.ErrNotFound
	}
	rowCount := dataset.rowCount
	if rowCount == 0 {
		rowCount = uint64(len(dataset.rows))
	}
	lease := &exportTestLease{
		schema:          cloneTestSchema(dataset.schema),
		rows:            cloneTestRows(dataset.rows),
		rowCount:        rowCount,
		rowCountInexact: dataset.rowCountInexact,
		truncated:       dataset.truncated,
		nextGate:        dataset.nextGate,
		nextExitGate:    dataset.nextExitGate,
		nextStarted:     dataset.nextStarted,
		ignoreNextCtx:   dataset.ignoreNextCtx,
		nextErr:         dataset.nextErr,
		closedSignal:    make(chan struct{}),
	}
	if !dataset.unboundContext {
		stop := context.AfterFunc(ctx, func() { _ = lease.Close() })
		lease.mu.Lock()
		if lease.closed {
			lease.mu.Unlock()
			stop()
		} else {
			lease.stopContext = stop
			lease.mu.Unlock()
		}
	}
	source.leases = append(source.leases, lease)
	if source.beforeReturn != nil {
		source.beforeReturn()
	}
	return lease, nil
}

func (source *exportTestSource) closedLeases() int {
	source.mu.Lock()
	defer source.mu.Unlock()
	closed := 0
	for _, lease := range source.leases {
		if lease.closeCount.Load() != 0 {
			closed++
		}
	}
	return closed
}

type exportTestLease struct {
	mu              sync.Mutex
	schema          searchjobs.Schema
	rows            []searchjobs.ResultRow
	rowCount        uint64
	rowCountInexact bool
	truncated       bool
	next            int
	closed          bool
	nextGate        <-chan struct{}
	nextExitGate    <-chan struct{}
	nextStarted     chan<- struct{}
	ignoreNextCtx   bool
	nextErr         error
	closeOnce       sync.Once
	closeCount      atomic.Int32
	closedSignal    chan struct{}
	stopContext     func() bool
}

func (lease *exportTestLease) Schema() searchjobs.Schema { return cloneTestSchema(lease.schema) }
func (lease *exportTestLease) RowCount() uint64          { return lease.rowCount }
func (*exportTestLease) Generation() uint64              { return 1 }
func (lease *exportTestLease) ResultsTruncated() bool    { return lease.truncated }
func (lease *exportTestLease) RowCountExact() bool       { return !lease.rowCountInexact }

func (lease *exportTestLease) Next(ctx context.Context) (searchjobs.ResultRow, bool, error) {
	if lease.nextStarted != nil {
		select {
		case lease.nextStarted <- struct{}{}:
		default:
		}
	}
	if lease.nextGate != nil {
		if lease.ignoreNextCtx {
			select {
			case <-lease.nextGate:
			case <-lease.closedSignal:
			}
		} else {
			select {
			case <-lease.nextGate:
			case <-ctx.Done():
				return searchjobs.ResultRow{}, false, ctx.Err()
			case <-lease.closedSignal:
			}
		}
	}
	if lease.nextExitGate != nil {
		<-lease.nextExitGate
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.closed {
		return searchjobs.ResultRow{}, false, searchjobs.ErrResultLeaseClosed
	}
	if err := ctx.Err(); err != nil {
		return searchjobs.ResultRow{}, false, err
	}
	if lease.nextErr != nil {
		err := lease.nextErr
		lease.nextErr = nil
		return searchjobs.ResultRow{}, false, err
	}
	if lease.next >= len(lease.rows) {
		return searchjobs.ResultRow{}, false, nil
	}
	row := cloneTestRow(lease.rows[lease.next])
	lease.next++
	return row, true, nil
}

func (lease *exportTestLease) Close() error {
	lease.closeOnce.Do(func() {
		lease.mu.Lock()
		lease.closed = true
		close(lease.closedSignal)
		stop := lease.stopContext
		lease.stopContext = nil
		lease.mu.Unlock()
		if stop != nil {
			stop()
		}
		lease.closeCount.Add(1)
	})
	return nil
}

func cloneTestSchema(schema searchjobs.Schema) searchjobs.Schema {
	return searchjobs.Schema{Columns: append([]searchjobs.Column(nil), schema.Columns...)}
}

func cloneTestRows(rows []searchjobs.ResultRow) []searchjobs.ResultRow {
	result := make([]searchjobs.ResultRow, len(rows))
	for index, row := range rows {
		result[index] = cloneTestRow(row)
	}
	return result
}

func cloneTestRow(row searchjobs.ResultRow) searchjobs.ResultRow {
	return searchjobs.ResultRow{Ordinal: row.Ordinal, Values: append([]searchjobs.Value(nil), row.Values...)}
}

type exportTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *exportTestClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *exportTestClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(duration)
	clock.mu.Unlock()
}

func sequenceExportIDs(prefix string) func() string {
	var next atomic.Uint64
	return func() string { return fmt.Sprintf("%s-%d", prefix, next.Add(1)) }
}

func basicExportSchema() searchjobs.Schema {
	return searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "message", Kind: searchjobs.ValueKindString},
		{Name: "count", Kind: searchjobs.ValueKindUnsigned},
	}}
}

func basicExportRows() []searchjobs.ResultRow {
	return []searchjobs.ResultRow{
		{Ordinal: 0, Values: []searchjobs.Value{searchjobs.StringValue("first"), searchjobs.UnsignedValue(2)}},
		{Ordinal: 1, Values: []searchjobs.Value{searchjobs.StringValue("=second"), searchjobs.UnsignedValue(math.MaxUint64)}},
	}
}

func newExportTestManager(t *testing.T, source ResultSource, mutate func(*Config)) *Manager {
	t.Helper()
	config := Config{
		Source:          source,
		ArtifactDir:     t.TempDir(),
		CleanupInterval: -1,
		NewID:           sequenceExportIDs("export"),
	}
	if mutate != nil {
		mutate(&config)
	}
	manager, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Errorf("Close() = %v", err)
		}
	})
	return manager
}

func waitExportState(t *testing.T, manager *Manager, access searchjobs.AccessScope, id string, states ...State) Job {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		job, err := manager.Get(context.Background(), access, id)
		if err == nil && slices.Contains(states, job.State) {
			manager.mu.RLock()
			entry := manager.jobs[id]
			workerAcknowledged := true
			if entry != nil && (job.State == StateCompleted || job.State == StateFailed || job.State == StateCanceled || job.State == StateExpired) {
				entry.mu.RLock()
				workerAcknowledged = entry.workerDone && entry.leaseReleased
				entry.mu.RUnlock()
			}
			manager.mu.RUnlock()
			if workerAcknowledged {
				return job
			}
		}
		if err != nil && !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get(%q) = %v", id, err)
		}
		time.Sleep(time.Millisecond)
	}
	job, err := manager.Get(context.Background(), access, id)
	t.Fatalf("job %q did not reach %v; final = %#v, error = %v", id, states, job, err)
	return Job{}
}

func waitFor(t *testing.T, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

func TestManagerStreamsSelectedJSONLinesAndReturnsDetachedSnapshots(t *testing.T) {
	t.Parallel()
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"search-1": {schema: basicExportSchema(), rows: basicExportRows()},
	}}
	manager := newExportTestManager(t, source, nil)
	job, err := manager.Create(context.Background(), testAccess, CreateRequest{
		SearchJobID: "search-1",
		Format:      FormatJSONLines,
		Columns:     []string{"count", "message"},
	})
	if err != nil {
		t.Fatal(err)
	}
	job.Columns[0] = "mutated"
	completed := waitExportState(t, manager, testAccess, job.ID, StateCompleted)
	if completed.Artifact == nil || completed.Artifact.RowCount != 2 || completed.Progress.RowsWritten != 2 {
		t.Fatalf("completed job = %#v", completed)
	}
	want := "{\"count\":2,\"message\":\"first\"}\n" +
		"{\"count\":\"18446744073709551615\",\"message\":\"=second\"}\n"
	path := filepath.Join(manager.artifactDir, completed.Artifact.FileName)
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != want {
		t.Fatalf("artifact = %q, want %q", contents, want)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("artifact mode = %o, want 600", info.Mode().Perm())
	}
	dirInfo, err := os.Stat(manager.artifactDir)
	if err != nil {
		t.Fatal(err)
	}
	if dirInfo.Mode().Perm() != 0o700 {
		t.Fatalf("artifact directory mode = %o, want 700", dirInfo.Mode().Perm())
	}
	completed.Columns[0] = "changed"
	completed.Artifact.FileName = "changed"
	again, err := manager.Get(context.Background(), testAccess, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if again.Columns[0] != "count" || again.Artifact.FileName == "changed" {
		t.Fatalf("Get snapshot was not detached: %#v", again)
	}
	waitFor(t, func() bool { return source.closedLeases() == 1 }, "source lease release")
}

func TestManagerRowAndExactByteLimitsNeverPublishArtifacts(t *testing.T) {
	t.Parallel()
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"too-many": {schema: basicExportSchema(), rows: basicExportRows(), rowCount: 1},
		"too-big":  {schema: basicExportSchema(), rows: basicExportRows()[:1]},
	}}
	manager := newExportTestManager(t, source, nil)
	rowJob, err := manager.Create(context.Background(), testAccess, CreateRequest{
		SearchJobID: "too-many",
		Format:      FormatCSV,
		RowLimit:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	rowFailed := waitExportState(t, manager, testAccess, rowJob.ID, StateFailed)
	if rowFailed.Failure == nil || rowFailed.Failure.Code != FailureRowLimit || rowFailed.Artifact != nil {
		t.Fatalf("row-limited job = %#v", rowFailed)
	}
	byteJob, err := manager.Create(context.Background(), testAccess, CreateRequest{
		SearchJobID: "too-big",
		Format:      FormatJSONLines,
		ByteLimit:   10,
	})
	if err != nil {
		t.Fatal(err)
	}
	byteFailed := waitExportState(t, manager, testAccess, byteJob.ID, StateFailed)
	if byteFailed.Failure == nil || byteFailed.Failure.Code != FailureByteLimit || byteFailed.Artifact != nil {
		t.Fatalf("byte-limited job = %#v", byteFailed)
	}
	waitFor(t, func() bool {
		entries, err := os.ReadDir(manager.artifactDir)
		return err == nil && len(entries) == 0
	}, "failed export partial cleanup")
}

func TestManagerDoesNotPrecheckAnInexactSourceRowCount(t *testing.T) {
	t.Parallel()
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"inexact": {
			schema:          basicExportSchema(),
			rows:            basicExportRows()[:1],
			rowCount:        1_000_000,
			rowCountInexact: true,
		},
	}}
	manager := newExportTestManager(t, source, nil)
	created, err := manager.Create(context.Background(), testAccess, CreateRequest{
		SearchJobID: "inexact",
		Format:      FormatJSONLines,
		RowLimit:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	completed := waitExportState(t, manager, testAccess, created.ID, StateCompleted)
	if completed.Artifact == nil || completed.Artifact.RowCount != 1 {
		t.Fatalf("inexact-count export = %#v", completed)
	}
}

func TestManagerRejectsInvalidUTF8CSVWithoutPublishing(t *testing.T) {
	t.Parallel()
	schema := searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}}
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"invalid-utf8": {
			schema: schema,
			rows:   []searchjobs.ResultRow{{Values: []searchjobs.Value{searchjobs.StringValue(string([]byte{0xff}))}}},
		},
	}}
	manager := newExportTestManager(t, source, nil)
	created, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "invalid-utf8", Format: FormatCSV})
	if err != nil {
		t.Fatal(err)
	}
	failed := waitExportState(t, manager, testAccess, created.ID, StateFailed)
	if failed.Failure == nil || failed.Failure.Code != FailureInternal || failed.Artifact != nil {
		t.Fatalf("invalid-UTF-8 job = %#v", failed)
	}
	waitFor(t, func() bool {
		entries, readErr := os.ReadDir(manager.artifactDir)
		return readErr == nil && len(entries) == 0
	}, "invalid-UTF-8 partial cleanup")
}

func TestManagerCancellationIsScopedAndRemovesPartial(t *testing.T) {
	t.Parallel()
	nextGate := make(chan struct{})
	nextStarted := make(chan struct{}, 1)
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"blocked": {
			schema:         basicExportSchema(),
			rows:           basicExportRows(),
			nextGate:       nextGate,
			nextStarted:    nextStarted,
			ignoreNextCtx:  true,
			unboundContext: true,
		},
	}}
	manager := newExportTestManager(t, source, nil)
	job, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "blocked", Format: FormatCSV})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-nextStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not begin reading")
	}
	wrong := searchjobs.AccessScope{TenantID: testAccess.TenantID, OwnerID: "other"}
	if _, err := manager.Cancel(context.Background(), wrong, job.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-scope Cancel() = %v, want ErrNotFound", err)
	}
	if _, err := manager.Get(context.Background(), wrong, job.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-scope Get() = %v, want ErrNotFound", err)
	}
	canceled, err := manager.Cancel(context.Background(), testAccess, job.ID)
	if err != nil || canceled.State != StateCanceled {
		t.Fatalf("Cancel() = (%#v, %v)", canceled, err)
	}
	waitFor(t, func() bool { return source.closedLeases() == 1 }, "canceled lease release")
	waitFor(t, func() bool {
		entries, readErr := os.ReadDir(manager.artifactDir)
		return readErr == nil && len(entries) == 0
	}, "canceled partial cleanup")
}

func TestManagerCanceledQueuedAndRunningJobsWaitForWorkerAcknowledgement(t *testing.T) {
	t.Parallel()
	clock := &exportTestClock{now: time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)}
	runningGate := make(chan struct{})
	runningExitGate := make(chan struct{})
	runningStarted := make(chan struct{}, 1)
	schema := searchjobs.Schema{Columns: []searchjobs.Column{{Name: "x", Kind: searchjobs.ValueKindString}}}
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"running": {
			schema:         schema,
			rows:           []searchjobs.ResultRow{{Values: []searchjobs.Value{searchjobs.StringValue("value")}}},
			nextGate:       runningGate,
			nextExitGate:   runningExitGate,
			nextStarted:    runningStarted,
			ignoreNextCtx:  true,
			unboundContext: true,
		},
		"queued": {schema: schema},
		"after":  {schema: schema},
	}}
	manager := newExportTestManager(t, source, func(config *Config) {
		config.MaxWorkers = 1
		config.MaxQueued = 4
		config.MaxJobs = 2
		config.ArtifactTTL = time.Nanosecond
		config.ExpiredRetention = time.Nanosecond
		config.Now = clock.Now
	})
	running, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "running", Format: FormatCSV})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-runningStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("running export did not block inside result iteration")
	}
	queued, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "queued", Format: FormatCSV})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Cancel(context.Background(), testAccess, queued.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Cancel(context.Background(), testAccess, running.ID); err != nil {
		t.Fatal(err)
	}
	manager.mu.RLock()
	runningEntry := manager.jobs[running.ID]
	queuedEntry := manager.jobs[queued.ID]
	manager.mu.RUnlock()
	if runningEntry == nil || queuedEntry == nil {
		t.Fatal("canceled entries disappeared before cleanup")
	}
	runningEntry.mu.RLock()
	runningPath := runningEntry.tempPath
	runningBytes := runningEntry.accountedBytes
	runningDone := runningEntry.workerDone
	runningLeaseReleased := runningEntry.leaseReleased
	runningEntry.mu.RUnlock()
	if runningPath == "" || runningBytes == 0 || runningDone || runningLeaseReleased {
		t.Fatalf("running private state = path %q bytes %d done %t leaseReleased %t", runningPath, runningBytes, runningDone, runningLeaseReleased)
	}
	if _, err := os.Stat(runningPath); err != nil {
		t.Fatalf("running temp file before cleanup: %v", err)
	}
	manager.budgetMu.Lock()
	metadataBefore := manager.totalMetadata
	artifactsBefore := manager.totalBytes
	manager.budgetMu.Unlock()
	clock.Advance(time.Hour)
	if err := manager.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, job := range []Job{running, queued} {
		retained, err := manager.Get(context.Background(), testAccess, job.ID)
		if err != nil {
			t.Fatalf("Get(canceled %s before worker acknowledgement) = %v", job.ID, err)
		}
		if retained.State != StateCanceled {
			t.Fatalf("canceled %s state = %s, want canceled", job.ID, retained.State)
		}
	}
	if _, err := os.Stat(runningPath); err != nil {
		t.Fatalf("Cleanup unlinked active writer temp file: %v", err)
	}
	manager.budgetMu.Lock()
	metadataAfter := manager.totalMetadata
	artifactsAfter := manager.totalBytes
	manager.budgetMu.Unlock()
	if metadataAfter != metadataBefore || artifactsAfter != artifactsBefore {
		t.Fatalf("premature cleanup accounting = metadata %d/%d artifact %d/%d", metadataAfter, metadataBefore, artifactsAfter, artifactsBefore)
	}
	if _, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "after", Format: FormatCSV}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("Create(before worker acknowledgement) = %v, want ErrCapacity", err)
	}
	close(runningExitGate)
	waitFor(t, func() bool {
		runningEntry.mu.RLock()
		runningFinished := runningEntry.workerDone && runningEntry.leaseReleased
		runningEntry.mu.RUnlock()
		queuedEntry.mu.RLock()
		queuedFinished := queuedEntry.workerDone && queuedEntry.leaseReleased
		queuedEntry.mu.RUnlock()
		return runningFinished && queuedFinished
	}, "running and queued worker acknowledgement")
	if err := manager.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(runningPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("acknowledged running temp Stat() = %v, want not exist", err)
	}
	manager.budgetMu.Lock()
	remainingMetadata := manager.totalMetadata
	remainingArtifacts := manager.totalBytes
	manager.budgetMu.Unlock()
	if remainingMetadata != 0 || remainingArtifacts != 0 {
		t.Fatalf("accounting after acknowledged cleanup = metadata %d artifact %d", remainingMetadata, remainingArtifacts)
	}
	after, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "after", Format: FormatCSV})
	if err != nil {
		t.Fatalf("Create(after worker acknowledgement) = %v", err)
	}
	waitExportState(t, manager, testAccess, after.ID, StateCompleted)
}

func TestManagerMapsSourceErrorsAndReleasesInvalidColumnLease(t *testing.T) {
	t.Parallel()
	source := &exportTestSource{
		datasets: map[string]exportTestDataset{
			"columns": {schema: basicExportSchema(), rows: basicExportRows()},
		},
		errors: map[string]error{
			"missing":  searchjobs.ErrNotFound,
			"active":   searchjobs.ErrResultsNotReady,
			"expired":  searchjobs.ErrExpired,
			"failed":   searchjobs.ErrResultsUnavailable,
			"capacity": searchjobs.ErrCapacity,
			"opaque":   errors.New("secret storage detail"),
		},
	}
	manager := newExportTestManager(t, source, nil)
	tests := []struct {
		id   string
		want error
	}{
		{id: "missing", want: ErrNotFound},
		{id: "active", want: ErrSourceNotReady},
		{id: "expired", want: ErrSourceExpired},
		{id: "failed", want: ErrSourceUnavailable},
		{id: "capacity", want: ErrCapacity},
		{id: "opaque", want: ErrSourceUnavailable},
	}
	for _, test := range tests {
		if _, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: test.id, Format: FormatCSV}); !errors.Is(err, test.want) {
			t.Errorf("Create(%q) = %v, want %v", test.id, err, test.want)
		}
	}
	if _, err := manager.Create(context.Background(), testAccess, CreateRequest{
		SearchJobID: "columns",
		Format:      FormatCSV,
		Columns:     []string{"missing"},
	}); !errors.Is(err, ErrInvalidColumns) {
		t.Fatalf("Create(invalid columns) = %v, want ErrInvalidColumns", err)
	}
	waitFor(t, func() bool { return source.closedLeases() == 1 }, "invalid-column lease release")
}

func TestManagerRejectsTruncatedRetainedSourceBeforeAdmission(t *testing.T) {
	t.Parallel()
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"truncated": {
			schema:    basicExportSchema(),
			rows:      basicExportRows(),
			truncated: true,
		},
	}}
	manager := newExportTestManager(t, source, nil)
	if _, err := manager.Create(context.Background(), testAccess, CreateRequest{
		SearchJobID: "truncated",
		Format:      FormatJSONLines,
	}); !errors.Is(err, ErrSourceTruncated) {
		t.Fatalf("Create(truncated source) = %v, want ErrSourceTruncated", err)
	}
	waitFor(t, func() bool { return source.closedLeases() == 1 }, "truncated source lease release")
	manager.mu.RLock()
	jobs := len(manager.jobs)
	reservations := manager.reservations
	manager.mu.RUnlock()
	manager.budgetMu.Lock()
	artifactBytes, metadataBytes := manager.totalBytes, manager.totalMetadata
	manager.budgetMu.Unlock()
	if jobs != 0 || reservations != 0 || artifactBytes != 0 || metadataBytes != 0 {
		t.Fatalf("truncated-source admission leaked jobs=%d reservations=%d artifact=%d metadata=%d", jobs, reservations, artifactBytes, metadataBytes)
	}
}

func TestManagerCallerCancellationPrecedesTruncatedSourceError(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	source := &exportTestSource{
		datasets: map[string]exportTestDataset{
			"truncated": {schema: basicExportSchema(), truncated: true},
		},
		beforeReturn: cancel,
	}
	manager := newExportTestManager(t, source, nil)
	if _, err := manager.Create(ctx, testAccess, CreateRequest{
		SearchJobID: "truncated",
		Format:      FormatJSONLines,
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Create(canceled truncated source) = %v, want context.Canceled", err)
	}
	waitFor(t, func() bool { return source.closedLeases() == 1 }, "canceled truncated source lease release")
}

func TestManagerShutdownPrecedesTruncatedSourceError(t *testing.T) {
	t.Parallel()
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"truncated": {schema: basicExportSchema(), truncated: true},
	}}
	manager := newExportTestManager(t, source, nil)
	source.beforeReturn = func() {
		manager.mu.Lock()
		manager.closed = true
		manager.mu.Unlock()
	}
	if _, err := manager.Create(context.Background(), testAccess, CreateRequest{
		SearchJobID: "truncated",
		Format:      FormatJSONLines,
	}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Create(closed truncated source) = %v, want ErrClosed", err)
	}
	waitFor(t, func() bool { return source.closedLeases() == 1 }, "closed truncated source lease release")
}

func TestManagerBoundsResolvedDefaultColumnSelection(t *testing.T) {
	t.Parallel()
	columns := make([]searchjobs.Column, maximumColumns+1)
	for index := range columns {
		columns[index] = searchjobs.Column{Name: fmt.Sprintf("field_%d", index), Kind: searchjobs.ValueKindString}
	}
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"wide": {schema: searchjobs.Schema{Columns: columns}},
	}}
	manager := newExportTestManager(t, source, nil)
	if _, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "wide", Format: FormatCSV}); !errors.Is(err, ErrInvalidColumns) {
		t.Fatalf("Create(wide default schema) = %v, want ErrInvalidColumns", err)
	}
	waitFor(t, func() bool { return source.closedLeases() == 1 }, "wide-schema lease release")
}

func TestManagerRejectsInvalidAccessScopeBeforeAdmission(t *testing.T) {
	t.Parallel()
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"search": {schema: basicExportSchema()},
	}}
	manager := newExportTestManager(t, source, nil)
	invalidScopes := []searchjobs.AccessScope{
		{TenantID: strings.Repeat("t", maximumAccessIDBytes+1), OwnerID: "owner"},
		{TenantID: string([]byte{0xff}), OwnerID: "owner"},
		{TenantID: "tenant", OwnerID: strings.Repeat("o", maximumAccessIDBytes+1)},
		{TenantID: "tenant", OwnerID: string([]byte{0xff})},
	}
	for _, access := range invalidScopes {
		if _, err := manager.Create(context.Background(), access, CreateRequest{SearchJobID: "search", Format: FormatCSV}); !errors.Is(err, ErrInvalidRequest) {
			t.Errorf("Create(invalid access) = %v, want ErrInvalidRequest", err)
		}
	}
	source.mu.Lock()
	acquires := source.acquires
	source.mu.Unlock()
	if acquires != 0 {
		t.Fatalf("source acquisitions for invalid access = %d", acquires)
	}
	manager.budgetMu.Lock()
	artifacts, metadata := manager.totalBytes, manager.totalMetadata
	manager.budgetMu.Unlock()
	if artifacts != 0 || metadata != 0 {
		t.Fatalf("invalid-access accounting = artifact %d metadata %d", artifacts, metadata)
	}
}

func TestManagerMetadataBudgetIncludesRetainedIdentifiersAndPaths(t *testing.T) {
	t.Parallel()
	access := searchjobs.AccessScope{
		TenantID: strings.Repeat("t", maximumAccessIDBytes),
		OwnerID:  strings.Repeat("o", maximumAccessIDBytes),
	}
	searchJobID := strings.Repeat("s", maximumSearchIDBytes)
	schema := searchjobs.Schema{Columns: []searchjobs.Column{{Name: "x", Kind: searchjobs.ValueKindString}}}
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		searchJobID: {schema: schema},
	}}
	manager := newExportTestManager(t, source, nil)
	expected, err := requestedMetadataBytes(manager.artifactDir, access, searchJobID, []string{"x"})
	if err != nil {
		t.Fatal(err)
	}
	withoutDirectory, err := requestedMetadataBytes("", access, searchJobID, []string{"x"})
	if err != nil {
		t.Fatal(err)
	}
	if expected-withoutDirectory != 2*uint64(len(manager.artifactDir)) {
		t.Fatalf("path metadata delta = %d, want %d", expected-withoutDirectory, 2*len(manager.artifactDir))
	}
	manager.maxTotalMetadata = expected - 1
	if _, err := manager.Create(context.Background(), access, CreateRequest{
		SearchJobID: searchJobID,
		Format:      FormatCSV,
		Columns:     []string{"x"},
	}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("Create(one byte below retained metadata) = %v, want ErrCapacity", err)
	}
	source.mu.Lock()
	acquires := source.acquires
	source.mu.Unlock()
	if acquires != 0 {
		t.Fatalf("source acquisitions before metadata admission = %d", acquires)
	}
	manager.maxTotalMetadata = expected
	created, err := manager.Create(context.Background(), access, CreateRequest{
		SearchJobID: searchJobID,
		Format:      FormatCSV,
		Columns:     []string{"x"},
	})
	if err != nil {
		t.Fatalf("Create(at exact retained metadata bound) = %v", err)
	}
	waitExportState(t, manager, access, created.ID, StateCompleted)
	manager.budgetMu.Lock()
	retained := manager.totalMetadata
	manager.budgetMu.Unlock()
	if retained != expected {
		t.Fatalf("retained metadata = %d, want %d", retained, expected)
	}
}

func TestManagerAggregateByteBudgetReservesReconcilesAndRecovers(t *testing.T) {
	t.Parallel()
	schema := searchjobs.Schema{Columns: []searchjobs.Column{{Name: "x", Kind: searchjobs.ValueKindString}}}
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"small":   {schema: schema},
		"blocked": {schema: schema, rows: []searchjobs.ResultRow{{Values: []searchjobs.Value{searchjobs.StringValue("x")}}}, nextGate: gate, nextStarted: started},
		"last":    {schema: schema},
	}}
	manager := newExportTestManager(t, source, func(config *Config) {
		config.MaxWorkers = 1
		config.DefaultByteLimit = 16
		config.MaximumByteLimit = 16
		config.MaxTotalBytes = 20
	})
	first, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "small", Format: FormatCSV})
	if err != nil {
		t.Fatal(err)
	}
	waitExportState(t, manager, testAccess, first.ID, StateCompleted)
	manager.budgetMu.Lock()
	if manager.totalBytes != 2 { // "x\n"
		t.Fatalf("reconciled bytes = %d, want 2", manager.totalBytes)
	}
	manager.budgetMu.Unlock()
	second, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "blocked", Format: FormatCSV})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("budgeted export did not start")
	}
	if _, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "last", Format: FormatCSV}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("Create(over aggregate byte budget) = %v, want ErrCapacity", err)
	}
	if _, err := manager.Cancel(context.Background(), testAccess, second.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		manager.budgetMu.Lock()
		defer manager.budgetMu.Unlock()
		return manager.totalBytes == 2
	}, "canceled reservation release")
	last, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "last", Format: FormatCSV})
	if err != nil {
		t.Fatalf("Create(after reservation release) = %v", err)
	}
	waitExportState(t, manager, testAccess, last.ID, StateCompleted)
}

func TestManagerMetadataBudgetPessimisticallyReservesAndReconciles(t *testing.T) {
	t.Parallel()
	schema := searchjobs.Schema{Columns: []searchjobs.Column{{Name: "x", Kind: searchjobs.ValueKindString}}}
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	source := &exportTestSource{
		datasets: map[string]exportTestDataset{
			"first":  {schema: schema},
			"second": {schema: schema},
		},
		gate:    gate,
		started: started,
	}
	manager := newExportTestManager(t, source, nil)
	firstMetadata, err := resolvedMetadataBytes(manager.artifactDir, testAccess, "first", schema.Columns)
	if err != nil {
		t.Fatal(err)
	}
	secondReservation, err := requestedMetadataBytes(manager.artifactDir, testAccess, "second", nil)
	if err != nil {
		t.Fatal(err)
	}
	manager.maxTotalMetadata = firstMetadata + secondReservation
	firstResult := make(chan struct {
		job Job
		err error
	}, 1)
	go func() {
		job, createErr := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "first", Format: FormatCSV})
		firstResult <- struct {
			job Job
			err error
		}{job: job, err: createErr}
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("first source acquisition did not begin")
	}
	if _, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "second", Format: FormatCSV}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("Create(during pessimistic reservation) = %v, want ErrCapacity", err)
	}
	close(gate)
	var first Job
	select {
	case result := <-firstResult:
		if result.err != nil {
			t.Fatal(result.err)
		}
		first = result.job
	case <-time.After(5 * time.Second):
		t.Fatal("first Create did not return")
	}
	second, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "second", Format: FormatCSV})
	if err != nil {
		t.Fatalf("Create(after metadata reconciliation) = %v", err)
	}
	waitExportState(t, manager, testAccess, first.ID, StateCompleted)
	waitExportState(t, manager, testAccess, second.ID, StateCompleted)
	manager.budgetMu.Lock()
	secondMetadata, err := resolvedMetadataBytes(manager.artifactDir, testAccess, "second", schema.Columns)
	if err != nil {
		t.Fatal(err)
	}
	if manager.totalMetadata != firstMetadata+secondMetadata {
		t.Fatalf("reconciled metadata = %d, want %d", manager.totalMetadata, firstMetadata+secondMetadata)
	}
	manager.budgetMu.Unlock()
	source.mu.Lock()
	acquires := source.acquires
	source.mu.Unlock()
	if acquires != 2 {
		t.Fatalf("source acquisitions = %d, want 2; capacity rejection must occur before acquisition", acquires)
	}
}

func TestManagerMetadataBudgetRemainsChargedUntilTombstoneRemoval(t *testing.T) {
	t.Parallel()
	clock := &exportTestClock{now: time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)}
	schema := searchjobs.Schema{Columns: []searchjobs.Column{{Name: "x", Kind: searchjobs.ValueKindString}}}
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"fails": {
			schema: schema,
			rows: []searchjobs.ResultRow{
				{Values: []searchjobs.Value{searchjobs.StringValue("one")}},
				{Values: []searchjobs.Value{searchjobs.StringValue("two")}},
			},
		},
		"after": {schema: schema},
	}}
	manager := newExportTestManager(t, source, func(config *Config) {
		config.ArtifactTTL = time.Minute
		config.ExpiredRetention = time.Minute
		config.Now = clock.Now
	})
	metadataBytes, err := requestedMetadataBytes(manager.artifactDir, testAccess, "fails", []string{"x"})
	if err != nil {
		t.Fatal(err)
	}
	manager.maxTotalMetadata = metadataBytes
	failed, err := manager.Create(context.Background(), testAccess, CreateRequest{
		SearchJobID: "fails",
		Format:      FormatCSV,
		Columns:     []string{"x"},
		RowLimit:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitExportState(t, manager, testAccess, failed.ID, StateFailed)
	manager.budgetMu.Lock()
	retainedMetadata := manager.totalMetadata
	retainedArtifacts := manager.totalBytes
	manager.budgetMu.Unlock()
	if retainedMetadata != metadataBytes || retainedArtifacts != 0 {
		t.Fatalf("terminal accounting = metadata %d artifact %d, want %d and 0", retainedMetadata, retainedArtifacts, metadataBytes)
	}
	if _, err := manager.Create(context.Background(), testAccess, CreateRequest{
		SearchJobID: "after",
		Format:      FormatCSV,
		Columns:     []string{"x"},
	}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("Create(while failed tombstone retained) = %v, want ErrCapacity", err)
	}
	clock.Advance(2 * time.Minute)
	if err := manager.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.budgetMu.Lock()
	remaining := manager.totalMetadata
	manager.budgetMu.Unlock()
	if remaining != 0 {
		t.Fatalf("metadata after tombstone deletion = %d, want 0", remaining)
	}
	after, err := manager.Create(context.Background(), testAccess, CreateRequest{
		SearchJobID: "after",
		Format:      FormatCSV,
		Columns:     []string{"x"},
	})
	if err != nil {
		t.Fatalf("Create(after tombstone deletion) = %v", err)
	}
	waitExportState(t, manager, testAccess, after.ID, StateCompleted)
}

func TestMetadataEstimatesDetectOverflow(t *testing.T) {
	t.Parallel()
	if _, ok := checkedAddUint64(^uint64(0), 1); ok {
		t.Fatal("checkedAddUint64 accepted overflow")
	}
	if result, ok := checkedAddUint64(^uint64(0)-1, 1); !ok || result != ^uint64(0) {
		t.Fatalf("checkedAddUint64(max-1, 1) = %d, %t", result, ok)
	}
	maximumAccess := searchjobs.AccessScope{
		TenantID: strings.Repeat("t", maximumAccessIDBytes),
		OwnerID:  strings.Repeat("o", maximumAccessIDBytes),
	}
	defaultEstimate, err := requestedMetadataBytes("", maximumAccess, strings.Repeat("s", maximumSearchIDBytes), nil)
	if err != nil || defaultEstimate != maximumJobMetadataExcludingDirectory {
		t.Fatalf("default metadata estimate = %d, %v, want %d", defaultEstimate, err, maximumJobMetadataExcludingDirectory)
	}
}

func TestManagerQueuedCancellationReleasesByteReservation(t *testing.T) {
	t.Parallel()
	schema := searchjobs.Schema{Columns: []searchjobs.Column{{Name: "x", Kind: searchjobs.ValueKindString}}}
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"running": {schema: schema, rows: []searchjobs.ResultRow{{Values: []searchjobs.Value{searchjobs.StringValue("x")}}}, nextGate: gate, nextStarted: started},
		"queued":  {schema: schema},
	}}
	manager := newExportTestManager(t, source, func(config *Config) {
		config.MaxWorkers = 1
		config.DefaultByteLimit = 16
		config.MaximumByteLimit = 16
		config.MaxTotalBytes = 32
	})
	running, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "running", Format: FormatCSV})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("first job did not start")
	}
	queued, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "queued", Format: FormatCSV})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Cancel(context.Background(), testAccess, queued.ID); err != nil {
		t.Fatal(err)
	}
	manager.budgetMu.Lock()
	if manager.totalBytes != 16 {
		t.Fatalf("bytes after queued cancellation = %d, want 16", manager.totalBytes)
	}
	manager.budgetMu.Unlock()
	if _, err := manager.Cancel(context.Background(), testAccess, running.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		manager.budgetMu.Lock()
		defer manager.budgetMu.Unlock()
		return manager.totalBytes == 0
	}, "running reservation release")
}

func TestManagerQueueAndJobCapacityRecoverAfterCleanup(t *testing.T) {
	t.Parallel()
	clock := &exportTestClock{now: time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)}
	firstGate := make(chan struct{})
	firstStarted := make(chan struct{}, 1)
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"first":  {schema: basicExportSchema(), rows: basicExportRows()[:1], nextGate: firstGate, nextStarted: firstStarted},
		"second": {schema: basicExportSchema(), rows: basicExportRows()[:1]},
		"third":  {schema: basicExportSchema(), rows: basicExportRows()[:1]},
	}}
	manager := newExportTestManager(t, source, func(config *Config) {
		config.MaxWorkers = 1
		config.MaxQueued = 1
		config.MaxJobs = 2
		config.ArtifactTTL = time.Minute
		config.ExpiredRetention = time.Minute
		config.Now = clock.Now
	})
	first, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "first", Format: FormatCSV})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("first export did not start")
	}
	second, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "second", Format: FormatCSV})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "third", Format: FormatCSV}); !errors.Is(err, ErrCapacity) && !errors.Is(err, ErrQueueFull) {
		t.Fatalf("Create(over capacity) = %v", err)
	}
	if _, err := manager.Cancel(context.Background(), testAccess, first.ID); err != nil {
		t.Fatal(err)
	}
	waitExportState(t, manager, testAccess, second.ID, StateCompleted)
	clock.Advance(2 * time.Minute)
	if err := manager.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The first cleanup expires terminal jobs. A second retention interval
	// removes their tombstones and releases the retained-job budget.
	clock.Advance(time.Minute)
	if err := manager.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	third, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "third", Format: FormatCSV})
	if err != nil {
		t.Fatalf("Create(after cleanup) = %v", err)
	}
	waitExportState(t, manager, testAccess, third.ID, StateCompleted)
}

func TestManagerArtifactTTLAndTombstoneCleanup(t *testing.T) {
	t.Parallel()
	clock := &exportTestClock{now: time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)}
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"search": {schema: basicExportSchema(), rows: basicExportRows()[:1]},
	}}
	manager := newExportTestManager(t, source, func(config *Config) {
		config.ArtifactTTL = time.Minute
		config.ExpiredRetention = 2 * time.Minute
		config.Now = clock.Now
	})
	created, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "search", Format: FormatCSV})
	if err != nil {
		t.Fatal(err)
	}
	completed := waitExportState(t, manager, testAccess, created.ID, StateCompleted)
	path := filepath.Join(manager.artifactDir, completed.Artifact.FileName)
	clock.Advance(time.Minute)
	if err := manager.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	expired, err := manager.Get(context.Background(), testAccess, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if expired.State != StateExpired || expired.Artifact != nil {
		t.Fatalf("expired job = %#v", expired)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired artifact Stat() = %v", err)
	}
	clock.Advance(2 * time.Minute)
	if err := manager.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Get(context.Background(), testAccess, created.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(removed tombstone) = %v, want ErrNotFound", err)
	}
}

func TestManagerDeletionFailureRetainsRetryHandleAndTombstone(t *testing.T) {
	t.Parallel()
	clock := &exportTestClock{now: time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)}
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"search": {schema: basicExportSchema(), rows: basicExportRows()[:1]},
	}}
	manager := newExportTestManager(t, source, func(config *Config) {
		config.ArtifactTTL = time.Minute
		config.ExpiredRetention = time.Minute
		config.Now = clock.Now
	})
	created, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "search", Format: FormatCSV})
	if err != nil {
		t.Fatal(err)
	}
	completed := waitExportState(t, manager, testAccess, created.ID, StateCompleted)
	path := filepath.Join(manager.artifactDir, completed.Artifact.FileName)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	blocker := filepath.Join(path, "blocker")
	if err := os.WriteFile(blocker, []byte("retain"), 0o600); err != nil {
		t.Fatal(err)
	}
	clock.Advance(3 * time.Minute)
	if err := manager.Cleanup(context.Background()); err == nil {
		t.Fatal("Cleanup() unexpectedly hid injected deletion failure")
	}
	expired, err := manager.Get(context.Background(), testAccess, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if expired.State != StateExpired || expired.Artifact != nil {
		t.Fatalf("job after failed deletion = %#v", expired)
	}
	manager.mu.RLock()
	entry := manager.jobs[created.ID]
	manager.mu.RUnlock()
	if entry == nil {
		t.Fatal("tombstone was removed despite retained artifact path")
	}
	entry.mu.RLock()
	retainedPath := entry.artifactPath
	retainedBytes := entry.accountedBytes
	entry.mu.RUnlock()
	if retainedPath != path || retainedBytes == 0 {
		t.Fatalf("private retry state = path %q bytes %d", retainedPath, retainedBytes)
	}
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := manager.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Get(context.Background(), testAccess, created.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(after successful retry) = %v, want ErrNotFound", err)
	}
	manager.budgetMu.Lock()
	if manager.totalBytes != 0 {
		t.Fatalf("bytes after successful deletion = %d", manager.totalBytes)
	}
	manager.budgetMu.Unlock()
}

func TestManagerAtomicPublicationDoesNotReplaceUnexpectedDestination(t *testing.T) {
	t.Parallel()
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"search": {schema: basicExportSchema(), rows: basicExportRows()[:1], nextGate: gate, nextStarted: started},
	}}
	manager := newExportTestManager(t, source, func(config *Config) { config.NewID = func() string { return "atomic-id" } })
	created, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "search", Format: FormatCSV})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("export did not reach blocked result read")
	}
	destination := manager.artifactPath(created.ID, FormatCSV)
	if err := os.WriteFile(destination, []byte("sentinel"), 0o600); err != nil {
		t.Fatal(err)
	}
	close(gate)
	failed := waitExportState(t, manager, testAccess, created.ID, StateFailed)
	if failed.Failure == nil || failed.Failure.Code != FailureStorageUnavailable || failed.Artifact != nil {
		t.Fatalf("collision job = %#v", failed)
	}
	contents, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "sentinel" {
		t.Fatalf("unexpected destination was replaced: %q", contents)
	}
}

func TestManagerRetriesPostPublishTemporaryUnlink(t *testing.T) {
	t.Parallel()
	clock := &exportTestClock{now: time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)}
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"search": {schema: basicExportSchema(), rows: basicExportRows()[:1]},
	}}
	manager := newExportTestManager(t, source, func(config *Config) {
		config.ArtifactTTL = time.Minute
		config.Now = clock.Now
	})
	originalRemove := manager.removePath
	var failedOnce atomic.Bool
	manager.removePath = func(path string) error {
		if strings.HasSuffix(path, ".partial") && failedOnce.CompareAndSwap(false, true) {
			return errors.New("injected temporary unlink failure")
		}
		return originalRemove(path)
	}
	created, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "search", Format: FormatCSV})
	if err != nil {
		t.Fatal(err)
	}
	waitExportState(t, manager, testAccess, created.ID, StateCompleted)
	manager.mu.RLock()
	entry := manager.jobs[created.ID]
	manager.mu.RUnlock()
	entry.mu.RLock()
	tempPath := entry.tempPath
	artifactPath := entry.artifactPath
	entry.mu.RUnlock()
	if tempPath == "" || artifactPath == "" {
		t.Fatalf("post-link retry paths = temp %q artifact %q", tempPath, artifactPath)
	}
	if _, err := os.Stat(tempPath); err != nil {
		t.Fatalf("retained temporary link: %v", err)
	}
	clock.Advance(time.Minute)
	if err := manager.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{tempPath, artifactPath} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("Stat(%q) after retry = %v", path, err)
		}
	}
	manager.budgetMu.Lock()
	if manager.totalBytes != 0 {
		t.Fatalf("bytes after both links removed = %d", manager.totalBytes)
	}
	manager.budgetMu.Unlock()
}

func TestManagerCreateReturnsCallerCancellationDuringAcquisition(t *testing.T) {
	t.Parallel()
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	source := &exportTestSource{
		datasets: map[string]exportTestDataset{"search": {schema: basicExportSchema()}},
		gate:     gate,
		started:  started,
	}
	manager := newExportTestManager(t, source, nil)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := manager.Create(ctx, testAccess, CreateRequest{SearchJobID: "search", Format: FormatCSV})
		result <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("source acquisition did not begin")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Create(canceled acquisition) = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled Create did not return")
	}
	manager.budgetMu.Lock()
	if manager.totalBytes != 0 {
		t.Fatalf("reservation after canceled acquisition = %d", manager.totalBytes)
	}
	if manager.totalMetadata != 0 {
		t.Fatalf("metadata reservation after canceled acquisition = %d", manager.totalMetadata)
	}
	manager.budgetMu.Unlock()
}

func TestManagerCloseCancelsWorkRemovesFilesAndRejectsAdmission(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	nextGate := make(chan struct{})
	nextStarted := make(chan struct{}, 1)
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"blocked": {schema: basicExportSchema(), rows: basicExportRows(), nextGate: nextGate, nextStarted: nextStarted},
	}}
	manager, err := New(Config{
		Source:          source,
		ArtifactDir:     directory,
		CleanupInterval: -1,
		NewID:           sequenceExportIDs("close"),
	})
	if err != nil {
		t.Fatal(err)
	}
	job, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "blocked", Format: FormatCSV})
	if err != nil {
		t.Fatal(err)
	}
	_ = job
	select {
	case <-nextStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not start")
	}
	closeResult := make(chan error, 1)
	go func() { closeResult <- manager.Close() }()
	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		// Ensure a broken implementation can unwind instead of leaking the
		// blocked worker for the remainder of the test process.
		close(nextGate)
		t.Fatal("Close did not close the underlying lease before waiting for workers")
	}
	manager.budgetMu.Lock()
	if manager.totalBytes != 0 || manager.totalMetadata != 0 {
		t.Fatalf("budgets after Close = artifact %d metadata %d", manager.totalBytes, manager.totalMetadata)
	}
	manager.budgetMu.Unlock()
	if err := manager.Close(); err != nil {
		t.Fatalf("second Close() = %v", err)
	}
	if _, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "blocked", Format: FormatCSV}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Create(after Close) = %v, want ErrClosed", err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("artifact directory after Close = %v", entries)
	}
	if source.closedLeases() != 1 {
		t.Fatalf("closed leases = %d, want 1", source.closedLeases())
	}
}

func TestManagerConcurrentAndRepeatedCloseReturnsStableRemovalError(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"search": {schema: basicExportSchema(), rows: basicExportRows()[:1]},
	}}
	manager, err := New(Config{Source: source, ArtifactDir: base, CleanupInterval: -1, NewID: func() string { return "close-error" }})
	if err != nil {
		t.Fatal(err)
	}
	created, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "search", Format: FormatCSV})
	if err != nil {
		t.Fatal(err)
	}
	completed := waitExportState(t, manager, testAccess, created.ID, StateCompleted)
	path := filepath.Join(manager.artifactDir, completed.Artifact.FileName)
	removeFailure := errors.New("injected close removal failure")
	originalRemove := manager.removePath
	manager.removePath = func(candidate string) error {
		if candidate == path {
			return removeFailure
		}
		return originalRemove(candidate)
	}
	const callers = 8
	errorsSeen := make(chan error, callers)
	var group sync.WaitGroup
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			errorsSeen <- manager.Close()
		}()
	}
	group.Wait()
	close(errorsSeen)
	var first string
	for closeErr := range errorsSeen {
		if closeErr == nil {
			t.Fatal("Close() unexpectedly returned nil")
		}
		if !errors.Is(closeErr, removeFailure) {
			t.Fatalf("Close() = %v, want injected removal failure", closeErr)
		}
		if first == "" {
			first = closeErr.Error()
		} else if closeErr.Error() != first {
			t.Fatalf("Close errors differ: %q vs %q", closeErr.Error(), first)
		}
	}
	if repeated := manager.Close(); repeated == nil || repeated.Error() != first {
		t.Fatalf("repeated Close() = %v, want %q", repeated, first)
	}
	entries, err := os.ReadDir(base)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("session directory survived Close: %v", entries)
	}
}

func TestManagerCloseRacingSynchronousAcquisitionDoesNotLeak(t *testing.T) {
	t.Parallel()
	acquireGate := make(chan struct{})
	acquireStarted := make(chan struct{}, 1)
	source := &exportTestSource{
		datasets: map[string]exportTestDataset{"search": {schema: basicExportSchema(), rows: basicExportRows()}},
		gate:     acquireGate,
		started:  acquireStarted,
	}
	manager, err := New(Config{Source: source, ArtifactDir: t.TempDir(), CleanupInterval: -1, NewID: sequenceExportIDs("acquire-close")})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, createErr := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "search", Format: FormatCSV})
		result <- createErr
	}()
	select {
	case <-acquireStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("source acquisition did not begin")
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case createErr := <-result:
		if !errors.Is(createErr, ErrClosed) {
			t.Fatalf("Create racing Close = %v, want ErrClosed", createErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Create did not return after Close")
	}
}

func TestManagerValidatesConfigurationAndGeneratedIDs(t *testing.T) {
	t.Parallel()
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"search": {schema: basicExportSchema(), rows: basicExportRows()},
	}}
	invalidConfigs := []Config{
		{Source: source, MaxWorkers: -1},
		{Source: source, MaxWorkers: maximumWorkers + 1},
		{Source: source, MaxQueued: maximumQueued + 1},
		{Source: source, MaxJobs: maximumJobs + 1},
		{Source: source, DefaultRowLimit: 2, MaximumRowLimit: 1},
		{Source: source, MaximumRowLimit: hardMaximumRowLimit + 1},
		{Source: source, DefaultByteLimit: 2, MaximumByteLimit: 1},
		{Source: source, MaximumByteLimit: hardMaximumByteLimit + 1},
		{Source: source, DefaultByteLimit: 1, MaximumByteLimit: 2, MaxTotalBytes: 1},
		{Source: source, MaxTotalBytes: hardMaximumTotalBytes + 1},
		{Source: source, MaxTotalMetadataBytes: metadataBaseBytes - 1},
		{Source: source, MaxTotalMetadataBytes: metadataBaseBytes},
		{Source: source, MaxTotalMetadataBytes: hardMaximumMetadata + 1},
		{Source: source, ArtifactTTL: -1},
	}
	for index, config := range invalidConfigs {
		config.ArtifactDir = t.TempDir()
		if manager, err := New(config); err == nil {
			_ = manager.Close()
			t.Errorf("New(invalid config %d) unexpectedly succeeded", index)
		}
	}
	manager := newExportTestManager(t, source, func(config *Config) { config.NewID = func() string { return "../unsafe" } })
	if _, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "search", Format: FormatCSV}); !errors.Is(err, ErrInvalidID) {
		t.Fatalf("Create(unsafe ID) = %v, want ErrInvalidID", err)
	}
}

func TestManagerConcurrentInspectionAndCancellation(t *testing.T) {
	nextGate := make(chan struct{})
	source := &exportTestSource{datasets: make(map[string]exportTestDataset)}
	for index := range 32 {
		id := fmt.Sprintf("search-%d", index)
		source.datasets[id] = exportTestDataset{schema: basicExportSchema(), rows: basicExportRows(), nextGate: nextGate}
	}
	manager := newExportTestManager(t, source, func(config *Config) {
		config.MaxWorkers = 8
		config.MaxQueued = 64
		config.MaxJobs = 64
		config.DefaultByteLimit = 1 << 20
		config.MaximumByteLimit = 1 << 20
		config.MaxTotalBytes = 64 << 20
	})
	jobs := make([]Job, 0, 32)
	for index := range 32 {
		job, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: fmt.Sprintf("search-%d", index), Format: FormatJSONLines})
		if err != nil {
			t.Fatal(err)
		}
		jobs = append(jobs, job)
	}
	var group sync.WaitGroup
	for _, job := range jobs {
		job := job
		group.Add(2)
		go func() {
			defer group.Done()
			for range 20 {
				_, err := manager.Get(context.Background(), testAccess, job.ID)
				if err != nil && !errors.Is(err, ErrNotFound) {
					t.Errorf("Get(%s) = %v", job.ID, err)
					return
				}
			}
		}()
		go func() {
			defer group.Done()
			_, err := manager.Cancel(context.Background(), testAccess, job.ID)
			if err != nil && !errors.Is(err, ErrNotCancelable) {
				t.Errorf("Cancel(%s) = %v", job.ID, err)
			}
		}()
	}
	group.Wait()
	close(nextGate)
	waitFor(t, func() bool { return source.closedLeases() == len(jobs) }, "all concurrent leases to close")
}
