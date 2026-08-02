package clickhouse

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
)

func TestIndexStatisticsReadAdmissionRejectsRetiredScopeBeforeNativeWork(t *testing.T) {
	t.Parallel()

	registry := indexread.NewRegistry()
	request := indexStatisticsValidRequest()
	if err := registry.Retire(
		context.Background(),
		request.TenantID,
		request.IndexName,
	); err != nil {
		t.Fatalf("Retire(): %v", err)
	}
	connection := &indexStatisticsScriptConnection{}
	reader, err := newIndexStatisticsReader(connection, IndexStatisticsConfig{
		ReadAdmission: registry,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result, readErr := reader.GetIndexStatistics(
		context.Background(),
		request,
	); !errors.Is(readErr, indexread.ErrUnavailable) || result != (IndexStatisticsResult{}) {
		t.Fatalf("GetIndexStatistics(retired) = (%#v, %v), want zero and ErrUnavailable", result, readErr)
	}
	if calls := connection.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("native query calls = %d after rejected read, want 0", len(calls))
	}
}

func TestIndexStatisticsReadAdmissionCancelsAndJoinsActiveNativeRead(t *testing.T) {
	t.Parallel()

	registry := indexread.NewRegistry()
	request := indexStatisticsValidRequest()
	queryEntered := make(chan struct{})
	connection := &indexStatisticsScriptConnection{
		steps: []indexStatisticsScriptStep{{
			rowForContext: func(ctx context.Context) driver.Row {
				return &indexStatisticsBlockingRow{ctx: ctx, entered: queryEntered}
			},
		}},
	}
	reader, err := newIndexStatisticsReader(connection, IndexStatisticsConfig{
		ReadAdmission: registry,
	})
	if err != nil {
		t.Fatal(err)
	}
	readDone := make(chan error, 1)
	go func() {
		_, readErr := reader.GetIndexStatistics(context.Background(), request)
		readDone <- readErr
	}()
	select {
	case <-queryEntered:
	case <-time.After(time.Second):
		t.Fatal("statistics query did not start")
	}
	retirementDone := make(chan error, 1)
	go func() {
		retirementDone <- registry.Retire(
			context.Background(),
			request.TenantID,
			request.IndexName,
		)
	}()
	select {
	case readErr := <-readDone:
		if !errors.Is(readErr, indexread.ErrUnavailable) {
			t.Fatalf("GetIndexStatistics() error = %v, want ErrUnavailable", readErr)
		}
	case <-time.After(time.Second):
		t.Fatal("retirement did not cancel active statistics query")
	}
	select {
	case retireErr := <-retirementDone:
		if retireErr != nil {
			t.Fatalf("Retire(): %v", retireErr)
		}
	case <-time.After(time.Second):
		t.Fatal("retirement did not join the statistics read lease")
	}
}

func TestIndexStatisticsBatchAdmissionIsAtomicAcrossEveryRequestedIndex(t *testing.T) {
	t.Parallel()

	request := indexStatisticsValidBatchRequest()
	wantErr := errors.New("test batch admission rejected")
	admission := &indexStatisticsRecordingAdmission{err: wantErr}
	connection := &indexStatisticsBatchConnection{}
	reader, err := newIndexStatisticsReader(connection, IndexStatisticsConfig{
		ReadAdmission: admission,
	})
	if err != nil {
		t.Fatal(err)
	}
	results, readErr := reader.GetIndexStatisticsBatch(context.Background(), request)
	if !errors.Is(readErr, wantErr) || results != nil {
		t.Fatalf("GetIndexStatisticsBatch() = (%#v, %v), want nil and admission error", results, readErr)
	}
	tenantID, indexNames, calls := admission.snapshot()
	wantNames := []string{"bravo", "empty", "alpha"}
	if calls != 1 || tenantID != request.TenantID || !slices.Equal(indexNames, wantNames) {
		t.Fatalf("admission = calls %d scope %q/%v, want 1 %q/%v", calls, tenantID, indexNames, request.TenantID, wantNames)
	}
	queryCalls, rowCalls := connection.snapshotCalls()
	if len(queryCalls) != 0 || len(rowCalls) != 0 {
		t.Fatalf("native query calls = %d/%d after rejected batch, want 0/0", len(queryCalls), len(rowCalls))
	}
}

func TestIndexStatisticsReaderRequiresReadAdmission(t *testing.T) {
	t.Parallel()

	var admission *indexStatisticsRecordingAdmission
	for _, test := range []struct {
		name      string
		admission indexread.Admission
	}{
		{name: "nil"},
		{name: "typed nil", admission: admission},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader, err := newIndexStatisticsReader(
				&indexStatisticsScriptConnection{},
				IndexStatisticsConfig{ReadAdmission: test.admission},
			)
			if err == nil || reader != nil {
				t.Fatalf("newIndexStatisticsReader(read admission) = (%v, %v), want nil and error", reader, err)
			}
		})
	}
}

func TestIndexStatisticsAdmissionDoesNotOccupyNativeOperationGate(t *testing.T) {
	t.Parallel()

	admission := &indexStatisticsBlockingAdmission{
		entered: make(chan struct{}),
		proceed: make(chan struct{}),
	}
	connection := &indexStatisticsScriptConnection{
		steps: []indexStatisticsScriptStep{{
			row: &indexStatisticsScriptRow{values: []any{
				uint64(0), (*time.Time)(nil), (*time.Time)(nil),
			}},
		}},
	}
	reader, err := newIndexStatisticsReader(connection, IndexStatisticsConfig{
		ReadAdmission: admission,
	})
	if err != nil {
		t.Fatal(err)
	}
	readDone := make(chan error, 1)
	go func() {
		_, readErr := reader.GetIndexStatistics(
			context.Background(),
			indexStatisticsValidRequest(),
		)
		readDone <- readErr
	}()
	select {
	case <-admission.entered:
	case <-time.After(time.Second):
		t.Fatal("statistics read did not enter catalog admission")
	}
	if occupied := len(reader.operation); occupied != 0 {
		t.Fatalf("native operation gate occupancy during catalog admission = %d, want 0", occupied)
	}
	close(admission.proceed)
	select {
	case readErr := <-readDone:
		if readErr != nil {
			t.Fatalf("GetIndexStatistics(): %v", readErr)
		}
	case <-time.After(time.Second):
		t.Fatal("statistics read did not finish after admission")
	}
}

func TestIndexStatisticsBatchAdmissionDoesNotOccupyNativeOperationGate(t *testing.T) {
	t.Parallel()

	admission := &indexStatisticsBlockingAdmission{
		entered: make(chan struct{}),
		proceed: make(chan struct{}),
	}
	connection := &indexStatisticsBatchConnection{
		querySteps: []indexStatisticsBatchQueryStep{{
			rows: &indexStatisticsBatchRows{},
		}},
	}
	reader, err := newIndexStatisticsReader(connection, IndexStatisticsConfig{
		ReadAdmission: admission,
	})
	if err != nil {
		t.Fatal(err)
	}
	readDone := make(chan error, 1)
	go func() {
		_, readErr := reader.GetIndexStatisticsBatch(
			context.Background(),
			indexStatisticsValidBatchRequest(),
		)
		readDone <- readErr
	}()
	select {
	case <-admission.entered:
	case <-time.After(time.Second):
		t.Fatal("statistics batch did not enter catalog admission")
	}
	if occupied := len(reader.operation); occupied != 0 {
		t.Fatalf("native operation gate occupancy during batch admission = %d, want 0", occupied)
	}
	close(admission.proceed)
	select {
	case readErr := <-readDone:
		if readErr != nil {
			t.Fatalf("GetIndexStatisticsBatch(): %v", readErr)
		}
	case <-time.After(time.Second):
		t.Fatal("statistics batch did not finish after admission")
	}
}

type indexStatisticsRecordingAdmission struct {
	mu         sync.Mutex
	tenantID   string
	indexNames []string
	calls      int
	err        error
}

type indexStatisticsBlockingAdmission struct {
	entered chan struct{}
	proceed chan struct{}
}

func (admission *indexStatisticsBlockingAdmission) Acquire(
	ctx context.Context,
	_ string,
	_ []string,
) (context.Context, func(), error) {
	close(admission.entered)
	select {
	case <-admission.proceed:
		return ctx, func() {}, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (admission *indexStatisticsRecordingAdmission) Acquire(
	ctx context.Context,
	tenantID string,
	indexNames []string,
) (context.Context, func(), error) {
	admission.mu.Lock()
	admission.calls++
	admission.tenantID = tenantID
	admission.indexNames = slices.Clone(indexNames)
	err := admission.err
	admission.mu.Unlock()
	if err != nil {
		return nil, nil, err
	}
	return ctx, func() {}, nil
}

func (admission *indexStatisticsRecordingAdmission) snapshot() (string, []string, int) {
	admission.mu.Lock()
	defer admission.mu.Unlock()
	return admission.tenantID, slices.Clone(admission.indexNames), admission.calls
}
