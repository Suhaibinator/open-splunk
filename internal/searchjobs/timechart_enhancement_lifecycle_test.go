package searchjobs

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestComposedTimechartBoundsSurvivePagingAndPinnedSnapshot(t *testing.T) {
	t.Parallel()

	schema := Schema{Columns: []Column{
		{Name: "_time", Kind: ValueKindTime},
		{Name: "count", Kind: ValueKindUnsigned},
	}}
	base := time.Date(2026, time.September, 10, 12, 0, 0, 125_000_000, time.UTC)
	wantBounds := []TimeBucketBounds{
		{Earliest: "2026-09-10T12:00:00.125Z", Latest: "2026-09-10T12:00:00.375Z"},
		{Earliest: "2026-09-10T12:00:00.375Z", Latest: "2026-09-10T12:00:00.625Z"},
		{Earliest: "2026-09-10T12:00:00.625Z", Latest: "2026-09-10T12:00:00.875Z"},
	}
	executor := executorFunc(func(_ context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
		if !query.HasValidExecutionSeal() || !query.RequiresAtomicResult() {
			return errors.New("composed timechart lost sealed atomic execution")
		}
		bucketSink, ok := sink.(TimeBucketResultSink)
		if !ok {
			return errors.New("manager sink does not accept time bucket metadata")
		}
		if err := sink.SetSchema(schema); err != nil {
			return err
		}
		for index, bounds := range wantBounds {
			if err := bucketSink.AddRowWithTimeBucket([]Value{
				TimeValue(base.Add(time.Duration(index) * 250 * time.Millisecond)),
				UnsignedValue(uint64(index + 1)),
			}, bounds); err != nil {
				return err
			}
		}
		return nil
	})
	manager := newTestManager(t, Config{
		Executor:        executor,
		DefaultPageSize: 2,
		MaxPageSize:     2,
		CleanupInterval: -1,
		NewID:           sequenceIDs("composed-timechart-bounds"),
	})
	request := validRequest()
	request.SPL = `index=main | timechart span=250ms count | where count > 0 | sort 0 +_time`
	created, err := manager.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("Create composed timechart: %v", err)
	}
	waitForState(t, manager, created.ID, StateCompleted)
	access := AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID}

	first, err := manager.ResultsFor(access, created.ID, PageRequest{Limit: 2})
	if err != nil {
		t.Fatalf("ResultsFor first page: %v", err)
	}
	if first.Complete || first.NextCursor == "" || len(first.Rows) != 2 {
		t.Fatalf("first page = %#v", first)
	}
	second, err := manager.ResultsFor(access, created.ID, PageRequest{
		Limit:  2,
		Cursor: first.NextCursor,
	})
	if err != nil {
		t.Fatalf("ResultsFor second page: %v", err)
	}
	if !second.Complete || second.NextCursor != "" || len(second.Rows) != 1 {
		t.Fatalf("second page = %#v", second)
	}
	gotRows := append(first.Rows, second.Rows...)
	for index, row := range gotRows {
		if row.TimeBucket == nil || *row.TimeBucket != wantBounds[index] {
			t.Fatalf("paged row %d bounds = %#v, want %#v", index, row.TimeBucket, wantBounds[index])
		}
	}

	// Mutating a detached page must not affect the immutable retained generation
	// used by export and replay consumers.
	first.Rows[0].TimeBucket.Earliest = "mutated"
	lease, err := manager.AcquireResultsFor(context.Background(), access, created.ID)
	if err != nil {
		t.Fatalf("AcquireResultsFor: %v", err)
	}
	defer lease.Close()
	if !reflect.DeepEqual(lease.Schema(), schema) || lease.RowCount() != 3 || !lease.RowCountExact() {
		t.Fatalf("lease metadata = schema %#v rows %d exact %t", lease.Schema(), lease.RowCount(), lease.RowCountExact())
	}
	for index, want := range wantBounds {
		row, ok, nextErr := lease.Next(context.Background())
		if nextErr != nil || !ok || row.TimeBucket == nil || *row.TimeBucket != want {
			t.Fatalf("lease row %d = (%#v, %t, %v), want bounds %#v", index, row, ok, nextErr, want)
		}
	}
}

func TestCanceledComposedTimechartPublishesNoBoundedPrefix(t *testing.T) {
	started := make(chan struct{})
	executor := executorFunc(func(ctx context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
		if !query.HasValidExecutionSeal() || !query.RequiresAtomicResult() {
			return errors.New("composed timechart lost sealed atomic execution")
		}
		bucketSink, ok := sink.(TimeBucketResultSink)
		if !ok {
			return errors.New("manager sink does not accept time bucket metadata")
		}
		if err := sink.SetSchema(Schema{Columns: []Column{
			{Name: "_time", Kind: ValueKindTime},
			{Name: "count", Kind: ValueKindUnsigned},
		}}); err != nil {
			return err
		}
		if err := bucketSink.AddRowWithTimeBucket([]Value{
			TimeValue(time.Date(2026, time.September, 10, 12, 0, 0, 0, time.UTC)),
			UnsignedValue(1),
		}, TimeBucketBounds{
			Earliest: "2026-09-10T12:00:00Z",
			Latest:   "2026-09-10T12:00:00.25Z",
		}); err != nil {
			return err
		}
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	manager := newTestManager(t, Config{
		Executor:        executor,
		CleanupInterval: -1,
		NewID:           sequenceIDs("canceled-composed-timechart"),
	})
	request := validRequest()
	request.SPL = `index=main | timechart span=250ms count | head 1`
	created, err := manager.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("Create composed timechart: %v", err)
	}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("executor did not stage bounded timechart row")
	}
	access := AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID}
	if _, previewErr := manager.PreviewFor(access, created.ID, 1); !errors.Is(previewErr, ErrResultsNotReady) {
		t.Fatalf("PreviewFor staged atomic row = %v, want ErrResultsNotReady", previewErr)
	}
	if err := manager.CancelFor(access, created.ID); err != nil {
		t.Fatalf("CancelFor: %v", err)
	}
	canceled := waitForState(t, manager, created.ID, StateCanceled)
	if canceled.Schema != nil || canceled.RowCount != 0 || canceled.ResultBytes != 0 || canceled.ResultsTruncated {
		t.Fatalf("canceled timechart retained a public prefix: %#v", canceled)
	}
	if _, resultErr := manager.ResultsFor(access, created.ID, PageRequest{Limit: 1}); !errors.Is(resultErr, ErrResultsUnavailable) {
		t.Fatalf("ResultsFor canceled timechart = %v, want ErrResultsUnavailable", resultErr)
	}
}
