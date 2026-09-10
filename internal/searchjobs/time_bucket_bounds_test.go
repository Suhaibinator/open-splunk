package searchjobs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestManagerRetainsExactTimeBucketBoundsAcrossPagesAndLeases(t *testing.T) {
	t.Parallel()

	boundary := time.Date(2026, time.September, 10, 8, 9, 10, 123_456_789, time.UTC)
	bounds := TimeBucketBounds{
		Earliest: "2026-09-10T08:09:10.123456789Z",
		Latest:   "2026-09-10T08:09:10.12345679Z",
	}
	schema := Schema{Columns: []Column{
		{Name: "_time", Kind: ValueKindTime},
		{Name: "count", Kind: ValueKindUnsigned},
	}}
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(schema); err != nil {
				return err
			}
			timeBucketSink, ok := sink.(TimeBucketResultSink)
			if !ok {
				t.Fatal("production result sink does not expose TimeBucketResultSink")
			}
			return timeBucketSink.AddRowWithTimeBucket(
				[]Value{TimeValue(boundary), UnsignedValue(3)},
				bounds,
			)
		}),
		CleanupInterval: -1,
		NewID:           sequenceIDs("exact-time-bucket"),
	})
	request := withSPL(validRequest(), "index=main | timechart span=1s count")
	request.TimeRange = mustAbsoluteTimeRange(boundary, boundary.Add(time.Second))
	created, err := manager.Create(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	completed := waitForState(t, manager, created.ID, StateCompleted)
	if completed.ResultBytes < uint64(len(bounds.Earliest)+len(bounds.Latest)) {
		t.Fatalf("result bytes %d do not account for exact bounds", completed.ResultBytes)
	}

	page, err := manager.Results(created.ID, PageRequest{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Rows) != 1 || page.Rows[0].TimeBucket == nil || *page.Rows[0].TimeBucket != bounds {
		t.Fatalf("paged time bucket = %#v, want %#v", page.Rows, bounds)
	}
	page.Rows[0].TimeBucket.Earliest = "mutated"

	lease, err := manager.AcquireResultsFor(context.Background(), AccessScope{
		TenantID: request.TenantID,
		OwnerID:  request.OwnerID,
	}, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	leased, ok, err := lease.Next(context.Background())
	if err != nil || !ok || leased.TimeBucket == nil || *leased.TimeBucket != bounds {
		t.Fatalf("leased time bucket = %#v, %t, %v", leased.TimeBucket, ok, err)
	}
}

func TestTimeBucketBoundsRejectMalformedOrUnboundMetadata(t *testing.T) {
	t.Parallel()

	stamp := time.Date(2026, time.September, 10, 8, 9, 10, 123_456_789, time.UTC)
	validBounds := TimeBucketBounds{
		Earliest: "2026-09-10T08:09:10.123456789Z",
		Latest:   "2026-09-10T08:09:11.123456789Z",
	}
	tests := []struct {
		name    string
		columns []Column
		values  []Value
		bounds  TimeBucketBounds
	}{
		{
			name:    "missing time column",
			columns: []Column{{Name: "count", Kind: ValueKindUnsigned}},
			values:  []Value{UnsignedValue(1)},
			bounds:  validBounds,
		},
		{
			name:    "mismatched time cell",
			columns: []Column{{Name: "_time", Kind: ValueKindTime}},
			values:  []Value{TimeValue(stamp.Add(time.Nanosecond))},
			bounds:  validBounds,
		},
		{
			name:    "non canonical offset",
			columns: []Column{{Name: "_time", Kind: ValueKindTime}},
			values:  []Value{TimeValue(stamp)},
			bounds:  TimeBucketBounds{Earliest: "2026-09-10T08:09:10.123456789+00:00", Latest: validBounds.Latest},
		},
		{
			name:    "non canonical fractional precision",
			columns: []Column{{Name: "_time", Kind: ValueKindTime}},
			values:  []Value{TimeValue(stamp)},
			bounds:  TimeBucketBounds{Earliest: "2026-09-10T08:09:10.1234567890Z", Latest: validBounds.Latest},
		},
		{
			name:    "empty interval",
			columns: []Column{{Name: "_time", Kind: ValueKindTime}},
			values:  []Value{TimeValue(stamp)},
			bounds:  TimeBucketBounds{Earliest: validBounds.Earliest, Latest: validBounds.Earliest},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := measureTimeBucketBounds(test.columns, test.values, &test.bounds, 0, 0)
			if !errors.Is(err, ErrInvalidResult) {
				t.Fatalf("measureTimeBucketBounds() error = %v, want ErrInvalidResult", err)
			}
		})
	}
}
