package export

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

type integrationSearchExecutor func(context.Context, clickhouse.CompiledQuery, searchjobs.ResultSink) error

func (execute integrationSearchExecutor) Execute(ctx context.Context, query clickhouse.CompiledQuery, sink searchjobs.ResultSink) error {
	return execute(ctx, query, sink)
}

type integrationSnapshotter func(context.Context) (uint64, error)

func (snapshot integrationSnapshotter) VisibilityCutoff(ctx context.Context) (uint64, error) {
	return snapshot(ctx)
}

type integrationClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *integrationClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *integrationClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(duration)
	clock.mu.Unlock()
}

// gatedSearchSource delegates acquisition and all result access to the real
// search job manager. The gate only makes the otherwise very fast immutable
// lease deterministic enough to expire its source while export still owns it.
type gatedSearchSource struct {
	manager      *searchjobs.Manager
	nextGate     <-chan struct{}
	acquired     chan struct{}
	nextStarted  chan struct{}
	leaseClosed  chan struct{}
	acquiredOnce sync.Once
	nextOnce     sync.Once
	closedOnce   sync.Once
}

func (source *gatedSearchSource) AcquireResultsFor(ctx context.Context, access searchjobs.AccessScope, id string) (searchjobs.ResultLease, error) {
	lease, err := source.manager.AcquireResultsFor(ctx, access, id)
	if err != nil {
		return nil, err
	}
	source.acquiredOnce.Do(func() { close(source.acquired) })
	return &gatedSearchLease{
		ResultLease: lease,
		source:      source,
		closed:      make(chan struct{}),
	}, nil
}

type gatedSearchLease struct {
	searchjobs.ResultLease
	source    *gatedSearchSource
	closed    chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func (lease *gatedSearchLease) Next(ctx context.Context) (searchjobs.ResultRow, bool, error) {
	lease.source.nextOnce.Do(func() { close(lease.source.nextStarted) })
	select {
	case <-lease.source.nextGate:
	case <-lease.closed:
	case <-ctx.Done():
		return searchjobs.ResultRow{}, false, ctx.Err()
	}
	return lease.ResultLease.Next(ctx)
}

func (lease *gatedSearchLease) Close() error {
	lease.closeOnce.Do(func() {
		close(lease.closed)
		lease.closeErr = lease.ResultLease.Close()
		lease.source.closedOnce.Do(func() { close(lease.source.leaseClosed) })
	})
	return lease.closeErr
}

func TestManagerExportsPinnedSearchJobSnapshotAcrossSourceExpiry(t *testing.T) {
	t.Parallel()

	access := searchjobs.AccessScope{TenantID: "tenant-integration", OwnerID: "owner-integration"}
	wrongAccess := searchjobs.AccessScope{TenantID: "other-tenant", OwnerID: access.OwnerID}
	clock := &integrationClock{now: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)}
	payload, err := searchjobs.ObjectValue(searchjobs.ObjectField{
		Name: "items",
		Value: searchjobs.ListValue(
			searchjobs.UnsignedValue(math.MaxUint64),
			searchjobs.BytesValue([]byte("x")),
		),
	})
	if err != nil {
		t.Fatal(err)
	}
	schema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "message", Kind: searchjobs.ValueKindString},
		{Name: "payload", Kind: searchjobs.ValueKindObject, Nullable: true},
	}}
	rows := [][]searchjobs.Value{
		{searchjobs.StringValue("first"), payload},
		{searchjobs.StringValue("second"), searchjobs.NullValue()},
	}
	searchManager, err := searchjobs.New(searchjobs.Config{
		Executor: integrationSearchExecutor(func(_ context.Context, query clickhouse.CompiledQuery, sink searchjobs.ResultSink) error {
			if !slices.Equal(query.OutputFields, []string{"message", "payload"}) {
				return fmt.Errorf("compiled fields = %v", query.OutputFields)
			}
			if err := sink.SetSchema(schema); err != nil {
				return err
			}
			for _, row := range rows {
				if err := sink.AddRow(row); err != nil {
					return err
				}
			}
			return nil
		}),
		Snapshotter: integrationSnapshotter(func(context.Context) (uint64, error) {
			return 17, nil
		}),
		MaxConcurrent:    1,
		RetentionTTL:     time.Second,
		ExpiredRetention: time.Second,
		CleanupInterval:  -1,
		Now:              clock.Now,
		NewID:            func() string { return "integration-search-1" },
		CursorKey:        []byte("0123456789abcdef0123456789abcdef"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := searchManager.Close(); err != nil {
			t.Errorf("close search manager: %v", err)
		}
	})

	searchJob, err := searchManager.Create(context.Background(), searchjobs.CreateRequest{
		SPL:               "index=main | table message payload",
		OwnerID:           access.OwnerID,
		TenantID:          access.TenantID,
		AuthorizedIndexes: []string{"main"},
		RequestedIndexes:  []string{"main"},
		Earliest:          clock.Now().Add(-time.Hour),
		Latest:            clock.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForIntegrationSearchState(t, searchManager, access, searchJob.ID, searchjobs.StateCompleted)

	nextGate := make(chan struct{})
	var releaseGate sync.Once
	releaseNext := func() { releaseGate.Do(func() { close(nextGate) }) }
	source := &gatedSearchSource{
		manager:     searchManager,
		nextGate:    nextGate,
		acquired:    make(chan struct{}),
		nextStarted: make(chan struct{}),
		leaseClosed: make(chan struct{}),
	}
	var exportSequence atomic.Uint64
	exportManager, err := New(Config{
		Source:          source,
		ArtifactDir:     t.TempDir(),
		MaxWorkers:      1,
		CleanupInterval: -1,
		NewID: func() string {
			return fmt.Sprintf("integration-export-%d", exportSequence.Add(1))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		releaseNext()
		if err := exportManager.Close(); err != nil {
			t.Errorf("close export manager: %v", err)
		}
	})

	request := CreateRequest{
		SearchJobID: searchJob.ID,
		Format:      FormatJSONLines,
		Columns:     []string{"payload", "message"},
		JSONLines:   JSONLinesOptions{IncludeTypeMetadata: true},
	}
	if _, err := exportManager.Create(context.Background(), wrongAccess, request); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Create(cross-scope) = %v, want ErrNotFound", err)
	}
	created, err := exportManager.Create(context.Background(), access, request)
	if err != nil {
		t.Fatal(err)
	}
	waitForIntegrationSignal(t, source.acquired, "export source acquisition")
	waitForIntegrationSignal(t, source.nextStarted, "export source iteration")
	if _, err := exportManager.Get(context.Background(), wrongAccess, created.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(cross-scope) = %v, want ErrNotFound", err)
	}

	clock.Advance(2 * time.Second)
	if changed := searchManager.Cleanup(); changed == 0 {
		t.Fatal("Cleanup() did not expire the pinned search result")
	}
	expired, err := searchManager.GetFor(access, searchJob.ID)
	if err != nil {
		t.Fatal(err)
	}
	if expired.State != searchjobs.StateExpired || expired.Schema != nil {
		t.Fatalf("expired search job = %#v", expired)
	}
	if lease, err := searchManager.AcquireResultsFor(context.Background(), access, searchJob.ID); !errors.Is(err, searchjobs.ErrExpired) || lease != nil {
		t.Fatalf("AcquireResultsFor(expired) = (%v, %v), want nil, ErrExpired", lease, err)
	}

	releaseNext()
	completed := waitForIntegrationExportState(t, exportManager, access, created.ID, StateCompleted)
	waitForIntegrationSignal(t, source.leaseClosed, "export source lease release")
	if completed.Artifact == nil || completed.Artifact.RowCount != 2 || completed.Progress.RowsWritten != 2 {
		t.Fatalf("completed export = %#v", completed)
	}
	if !slices.Equal(completed.Columns, []string{"payload", "message"}) {
		t.Fatalf("completed columns = %v", completed.Columns)
	}
	want := "{\"payload\":{\"$type\":\"object\",\"$value\":{\"items\":{\"$type\":\"list\",\"$value\":[{\"$type\":\"unsigned\",\"$value\":\"18446744073709551615\"},{\"$type\":\"bytes\",\"$value\":\"eA==\"}]}}},\"message\":{\"$type\":\"string\",\"$value\":\"first\"}}\n" +
		"{\"payload\":{\"$type\":\"null\",\"$value\":null},\"message\":{\"$type\":\"string\",\"$value\":\"second\"}}\n"
	contents, err := os.ReadFile(filepath.Join(exportManager.artifactDir, completed.Artifact.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != want {
		t.Fatalf("exported JSONL = %q, want %q", contents, want)
	}
	if completed.Artifact.SizeBytes != uint64(len(want)) || completed.Progress.BytesWritten != uint64(len(want)) {
		t.Fatalf("exported byte counts = artifact %d progress %d, want %d", completed.Artifact.SizeBytes, completed.Progress.BytesWritten, len(want))
	}

	clock.Advance(2 * time.Second)
	if changed := searchManager.Cleanup(); changed == 0 {
		t.Fatal("Cleanup() did not remove the unpinned search tombstone")
	}
	if _, err := searchManager.GetFor(access, searchJob.ID); !errors.Is(err, searchjobs.ErrNotFound) {
		t.Fatalf("GetFor(after lease release and cleanup) = %v, want ErrNotFound", err)
	}
}

func waitForIntegrationSearchState(t *testing.T, manager *searchjobs.Manager, access searchjobs.AccessScope, id string, want searchjobs.State) searchjobs.Job {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		job, err := manager.GetFor(access, id)
		if err != nil {
			t.Fatalf("GetFor(%q) = %v", id, err)
		}
		if job.State == want {
			return job
		}
		switch job.State {
		case searchjobs.StateCompleted, searchjobs.StateFailed, searchjobs.StateCanceled, searchjobs.StateExpired:
			t.Fatalf("search job %q reached %v, want %v; failure=%#v", id, job.State, want, job.Failure)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("search job %q did not reach %v", id, want)
	return searchjobs.Job{}
}

func waitForIntegrationExportState(t *testing.T, manager *Manager, access searchjobs.AccessScope, id string, want State) Job {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		job, err := manager.Get(context.Background(), access, id)
		if err != nil {
			t.Fatalf("Get(%q) = %v", id, err)
		}
		if job.State == want {
			return job
		}
		switch job.State {
		case StateCompleted, StateFailed, StateCanceled, StateExpired:
			t.Fatalf("export job %q reached %v, want %v; failure=%#v", id, job.State, want, job.Failure)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("export job %q did not reach %v", id, want)
	return Job{}
}

func waitForIntegrationSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}
