package export

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestReexecutionLeaseRejectsConcurrentCallsRetainedByReturningExecutor(t *testing.T) {
	t.Parallel()

	searches, schema, access := newReexecutionTestSearches()
	const callbackCount = 32
	releaseCallbacks := make(chan struct{})
	callbackResults := make(chan error, callbackCount*2)
	executorReturned := make(chan struct{})
	executor := reexecutionTestExecutor(func(
		_ context.Context,
		_ clickhouse.CompiledQuery,
		sink searchjobs.ResultSink,
	) error {
		if err := sink.SetSchema(schema); err != nil {
			return err
		}
		for index := range callbackCount {
			go func(value int) {
				<-releaseCallbacks
				callbackResults <- sink.AddRow([]searchjobs.Value{searchjobs.SignedValue(int64(value))})
				callbackResults <- sink.SetSchema(schema)
			}(index)
		}
		close(executorReturned)
		return nil
	})
	source := newReexecutionTestSource(t, searches, executor, nil)
	lease, err := source.AcquireResultsFor(context.Background(), access, searches.job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, nextErr := lease.Next(context.Background()); ok || nextErr != nil {
		t.Fatalf("Next(returning executor) = ok %t err %v", ok, nextErr)
	}
	<-executorReturned
	close(releaseCallbacks)
	for range callbackCount * 2 {
		if callbackErr := <-callbackResults; !errors.Is(callbackErr, errReexecutionSinkClosed) ||
			!errors.Is(callbackErr, searchjobs.ErrInvalidResult) {
			t.Fatalf("retained callback error = %v, want fixed invalid-result closure", callbackErr)
		}
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSchemaCardinalityPreflightRejectsOversizedSourceBeforeInspection(t *testing.T) {
	t.Parallel()

	columns := make([]searchjobs.Column, maximumColumns+1)
	for index := range columns {
		columns[index] = searchjobs.Column{Name: fmt.Sprintf("field_%04d", index), Kind: searchjobs.ValueKindString}
	}
	// An invalid first column distinguishes the cardinality gate from the
	// per-column map/validation loop: the size error must win.
	columns[0].Name = ""
	schema := searchjobs.Schema{Columns: columns}
	if validSourceSchemaCardinality(schema) {
		t.Fatal("oversized schema passed the shared cardinality preflight")
	}
	if _, err := selectColumns(schema, nil); !errors.Is(err, ErrInvalidColumns) {
		t.Fatalf("selectColumns(oversized invalid schema) = %v, want ErrInvalidColumns", err)
	}
	if schemaMatchesCompiledQuery(schema, clickhouse.CompiledQuery{}) {
		t.Fatal("oversized schema matched a compiled query")
	}
}

type hardeningSummaryLease struct {
	*exportTestLease
	summaryCalls atomic.Int32
}

//nolint:unparam // Both results are required by knowledgeSnapshotSummaryProvider.
func (lease *hardeningSummaryLease) knowledgeSnapshotSummary() (*opensplunk.KnowledgeSnapshotSummary, error) {
	lease.summaryCalls.Add(1)
	return nil, nil
}

type hardeningStaticSource struct {
	lease searchjobs.ResultLease
	err   error
}

func (source *hardeningStaticSource) AcquireResultsFor(
	context.Context,
	searchjobs.AccessScope,
	string,
) (searchjobs.ResultLease, error) {
	return source.lease, source.err
}

type hardeningTypedNilSource struct{}

func (*hardeningTypedNilSource) AcquireResultsFor(
	context.Context,
	searchjobs.AccessScope,
	string,
) (searchjobs.ResultLease, error) {
	panic("typed-nil result source was dereferenced")
}

func TestNewManagerRejectsTypedNilResultSource(t *testing.T) {
	t.Parallel()

	var source *hardeningTypedNilSource
	manager, err := New(Config{Source: source})
	if err == nil || manager != nil {
		t.Fatalf("New(typed-nil source) = (%v, %v), want nil manager and error", manager, err)
	}
}

func TestManagerRejectsOversizedSchemaBeforeKnowledgeProjection(t *testing.T) {
	t.Parallel()

	columns := make([]searchjobs.Column, maximumColumns+1)
	for index := range columns {
		columns[index] = searchjobs.Column{Name: fmt.Sprintf("field_%d", index), Kind: searchjobs.ValueKindString}
	}
	lease := &hardeningSummaryLease{exportTestLease: &exportTestLease{
		schema:       searchjobs.Schema{Columns: columns},
		closedSignal: make(chan struct{}),
	}}
	manager := newExportTestManager(t, &hardeningStaticSource{lease: lease}, nil)
	if _, err := manager.Create(context.Background(), testAccess, CreateRequest{
		SearchJobID: "oversized",
		Format:      FormatCSV,
	}); !errors.Is(err, ErrInvalidColumns) {
		t.Fatalf("Create(oversized schema) = %v, want ErrInvalidColumns", err)
	}
	if lease.summaryCalls.Load() != 0 {
		t.Fatalf("knowledge summary calls = %d, want zero", lease.summaryCalls.Load())
	}
	if lease.closeCount.Load() != 1 {
		t.Fatalf("lease close calls = %d, want one", lease.closeCount.Load())
	}
	assertHardeningManagerAdmissionReleased(t, manager)
}

type hardeningTypedNilLease struct{}

func (*hardeningTypedNilLease) Schema() searchjobs.Schema { panic("typed-nil lease was dereferenced") }
func (*hardeningTypedNilLease) RowCount() uint64          { panic("typed-nil lease was dereferenced") }
func (*hardeningTypedNilLease) RowCountExact() bool       { panic("typed-nil lease was dereferenced") }
func (*hardeningTypedNilLease) ResultsTruncated() bool    { panic("typed-nil lease was dereferenced") }
func (*hardeningTypedNilLease) Generation() uint64        { panic("typed-nil lease was dereferenced") }
func (*hardeningTypedNilLease) Next(context.Context) (searchjobs.ResultRow, bool, error) {
	panic("typed-nil lease was dereferenced")
}
func (*hardeningTypedNilLease) Close() error { panic("typed-nil lease was closed") }

func TestManagerCreateRejectsTypedNilLeaseOnSourceSuccessWithoutLeak(t *testing.T) {
	t.Parallel()

	var lease *hardeningTypedNilLease
	manager := newExportTestManager(t, &hardeningStaticSource{lease: lease}, nil)
	if _, err := manager.Create(context.Background(), testAccess, CreateRequest{
		SearchJobID: "typed-nil-success",
		Format:      FormatCSV,
	}); !errors.Is(err, ErrSourceUnavailable) {
		t.Fatalf("Create(typed-nil success) = %v, want ErrSourceUnavailable", err)
	}
	assertHardeningManagerAdmissionReleased(t, manager)
}

func TestManagerCreateIgnoresTypedNilLeaseOnSourceErrorWithoutLeak(t *testing.T) {
	t.Parallel()

	var lease *hardeningTypedNilLease
	manager := newExportTestManager(t, &hardeningStaticSource{
		lease: lease,
		err:   searchjobs.ErrResultsNotReady,
	}, nil)
	if _, err := manager.Create(context.Background(), testAccess, CreateRequest{
		SearchJobID: "typed-nil-error",
		Format:      FormatCSV,
	}); !errors.Is(err, ErrSourceNotReady) {
		t.Fatalf("Create(typed-nil error) = %v, want ErrSourceNotReady", err)
	}
	assertHardeningManagerAdmissionReleased(t, manager)
}

func assertHardeningManagerAdmissionReleased(t *testing.T, manager *Manager) {
	t.Helper()
	manager.mu.RLock()
	reservations := manager.reservations
	jobs := len(manager.jobs)
	reservedIDs := len(manager.reservedIDs)
	manager.mu.RUnlock()
	manager.budgetMu.Lock()
	totalBytes := manager.totalBytes
	totalMetadata := manager.totalMetadata
	manager.budgetMu.Unlock()
	if reservations != 0 || jobs != 0 || reservedIDs != 0 || totalBytes != 0 || totalMetadata != 0 {
		t.Fatalf(
			"leaked admission: reservations=%d jobs=%d reserved_ids=%d bytes=%d metadata=%d",
			reservations,
			jobs,
			reservedIDs,
			totalBytes,
			totalMetadata,
		)
	}
}
