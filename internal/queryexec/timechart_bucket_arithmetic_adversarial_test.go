package queryexec

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// TestCheckedBucketBoundaryRejectsEveryOverflowShape probes the arithmetic
// guard both publishers and the readers share.
func TestCheckedBucketBoundaryRejectsEveryOverflowShape(t *testing.T) {
	t.Parallel()

	maxInt64 := int64(math.MaxInt64)
	for _, testCase := range []struct {
		name       string
		first      int64
		span       int64
		multiplier uint64
		want       int64
		wantOK     bool
	}{
		{name: "zero span", first: 0, span: 0, multiplier: 1},
		{name: "negative span", first: 0, span: -60, multiplier: 1},
		{name: "product overflows int64", first: 0, span: 86400, multiplier: uint64(maxInt64)/86400 + 1},
		{name: "product exactly fits", first: 0, span: 86400, multiplier: uint64(maxInt64) / 86400,
			want: (maxInt64 / 86400) * 86400, wantOK: true},
		{name: "sum overflows int64", first: maxInt64 - 59, span: 60, multiplier: 1},
		{name: "sum lands on MaxInt64", first: maxInt64 - 60, span: 60, multiplier: 1, want: maxInt64, wantOK: true},
		{name: "negative origin absorbs the offset", first: -maxInt64, span: 60, multiplier: 1,
			want: -maxInt64 + 60, wantOK: true},
		{name: "zero multiplier is identity", first: maxInt64, span: 86400, multiplier: 0,
			want: maxInt64, wantOK: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			got, ok := checkedBucketBoundary(testCase.first, testCase.span, testCase.multiplier)
			if ok != testCase.wantOK || got != testCase.want {
				t.Fatalf("checkedBucketBoundary = %d, %v; want %d, %v", got, ok, testCase.want, testCase.wantOK)
			}
		})
	}
}

// TestExecutorRejectsAnOverflowingBucketOriginBeforeQuerying pushes the whole
// fixed timechart through Execute with an origin so late that the last bucket
// cannot be represented. The contract check must fire before ClickHouse is
// asked anything.
func TestExecutorRejectsAnOverflowingBucketOriginBeforeQuerying(t *testing.T) {
	t.Parallel()

	spanSeconds := int64(5 * 60)
	origin := time.Unix((int64(math.MaxInt64)/spanSeconds)*spanSeconds, 0).UTC()
	connection := &fakeQueryConnection{rows: fixedCountFieldTimechartRows([]uint64{0}, []uint8{1})}
	sink := &fakeSink{}
	err := mustExecutor(t, connection).Execute(
		context.Background(),
		fixedCountFieldTimechartQuery(origin, 3, "eligible_values"),
		sink,
	)
	if !errors.Is(err, searchjobs.ErrInvalidResult) || connection.query != "" || sink.setCalls != 0 {
		t.Fatalf("err=%v query=%q setCalls=%d", err, connection.query, sink.setCalls)
	}
}

// TestExecutorRejectsBucketCountsOutsideTheClosedRange pins both ends of the
// compiled bucket-count contract that every grid publisher relies on.
func TestExecutorRejectsBucketCountsOutsideTheClosedRange(t *testing.T) {
	t.Parallel()

	origin := time.Date(2026, time.August, 15, 0, 0, 0, 0, time.UTC)
	for _, bucketCount := range []uint64{0, maximumTimechartBuckets + 1} {
		connection := &fakeQueryConnection{rows: fixedCountFieldTimechartRows([]uint64{0}, []uint8{1})}
		err := mustExecutor(t, connection).Execute(
			context.Background(),
			fixedCountFieldTimechartQuery(origin, bucketCount, "eligible_values"),
			&fakeSink{},
		)
		if !errors.Is(err, searchjobs.ErrInvalidResult) || connection.query != "" {
			t.Fatalf("bucket count %d: err=%v query=%q", bucketCount, err, connection.query)
		}
	}
	// The cap itself must remain publishable end to end.
	counts := make([]uint64, maximumTimechartBuckets)
	presence := make([]uint8, maximumTimechartBuckets)
	for index := range counts {
		presence[index] = 1
	}
	counts[maximumTimechartBuckets-1] = 3
	sink := &fakeSink{}
	if err := mustExecutor(t, &fakeQueryConnection{
		rows: fixedCountFieldTimechartRows(counts, presence),
	}).Execute(
		context.Background(),
		fixedCountFieldTimechartQuery(origin, maximumTimechartBuckets, "eligible_values"),
		sink,
	); err != nil {
		t.Fatalf("maximum grid Execute: %v", err)
	}
	if uint64(len(sink.rows)) != maximumTimechartBuckets {
		t.Fatalf("maximum grid rows = %d", len(sink.rows))
	}
	last := sink.rows[len(sink.rows)-1]
	bucket, bucketOK := last[0].Time()
	want := origin.Add(time.Duration(maximumTimechartBuckets-1) * 5 * time.Minute)
	if !bucketOK || !bucket.Equal(want) {
		t.Fatalf("last bucket = %#v, want %v", last[0], want)
	}
	if count, ok := last[1].Unsigned(); !ok || count != 3 {
		t.Fatalf("last count = %#v", last[1])
	}
}
