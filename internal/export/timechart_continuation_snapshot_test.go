package export

import (
	"context"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

func TestContinuationExportUsesResolvedSnapshotWithoutReexecution(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	start := now.Add(-time.Hour)
	access := searchjobs.AccessScope{TenantID: "tenant-a", OwnerID: "owner-a"}
	bounds := searchjobs.TimeBucketBounds{Earliest: start.Format(time.RFC3339Nano), Latest: start.Add(time.Hour).Format(time.RFC3339Nano)}
	schema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "_time", Kind: searchjobs.ValueKindTime},
		{Name: "west coast", Kind: searchjobs.ValueKindUnsigned},
	}}
	manager, err := searchjobs.New(searchjobs.Config{
		Executor: reexecutionTestExecutor(func(ctx context.Context, query clickhouse.CompiledQuery, sink searchjobs.ResultSink) error {
			final, err := query.ContinueContext(ctx, []clickhouse.RelationColumn{
				{Name: "_time", Type: "DateTime64(9, 'UTC')"},
				{Name: "west coast", Type: "UInt64"},
			}, [][]any{{start, uint64(7)}})
			if err != nil {
				return err
			}
			if err := sink.(searchjobs.CompiledResultSink).SetCompiledQuery(final); err != nil {
				return err
			}
			if err := sink.SetSchema(schema); err != nil {
				return err
			}
			return sink.(searchjobs.TimeBucketResultSink).AddRowWithTimeBucket(
				[]searchjobs.Value{searchjobs.TimeValue(start), searchjobs.UnsignedValue(7)}, bounds)
		}),
		Snapshotter:     integrationSnapshotter(func(context.Context) (uint64, error) { return 42, nil }),
		MaxConcurrent:   1,
		MaxRows:         10,
		RetentionTTL:    time.Hour,
		CleanupInterval: -1,
		Now:             func() time.Time { return now },
		NewID:           func() string { return "continuation-export" },
		CursorKey:       []byte("continuation-export-snapshot-cursor-key-at-least-32-bytes"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	rangeValue, err := searchtime.NewAbsoluteRange(start, now)
	if err != nil {
		t.Fatal(err)
	}
	created, err := manager.Create(ctx, searchjobs.CreateRequest{
		SPL:     "index=main | timechart span=1h count by host | table _time 'west coast'",
		OwnerID: access.OwnerID, TenantID: access.TenantID,
		AuthorizedIndexes: []string{"main"}, RequestedIndexes: []string{"main"}, TimeRange: rangeValue,
	})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		job, err := manager.GetFor(access, created.ID)
		if err != nil {
			t.Fatal(err)
		}
		if job.State.Terminal() {
			if job.State != searchjobs.StateCompleted {
				t.Fatalf("search state = %s, failure = %#v", job.State, job.Failure)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("search did not finish")
		}
		time.Sleep(time.Millisecond)
	}
	source := newReexecutionTestSource(t, manager, reexecutionTestExecutor(func(context.Context, clickhouse.CompiledQuery, searchjobs.ResultSink) error {
		t.Error("export rescanned events instead of reading the resolved snapshot")
		return searchjobs.ErrInvalidResult
	}), nil)
	lease, err := source.AcquireResultsFor(ctx, access, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Close() })
	if !lease.RowCountExact() || lease.RowCount() != 1 || lease.ResultsTruncated() || !equalResultSchemas(lease.Schema(), schema) {
		t.Fatal("export changed the retained result contract")
	}
	row, ok, err := lease.Next(ctx)
	if err != nil || !ok || row.TimeBucket == nil || *row.TimeBucket != bounds {
		t.Fatalf("export row = %#v, ok = %t, error = %v", row, ok, err)
	}
	if count, valid := row.Values[1].Unsigned(); !valid || count != 7 {
		t.Fatalf("export count = %d, valid = %t", count, valid)
	}
	if _, ok, err := lease.Next(ctx); err != nil || ok {
		t.Fatalf("export end = %t, %v", ok, err)
	}
}
