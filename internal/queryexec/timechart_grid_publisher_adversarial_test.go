package queryexec

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// unsignedCells builds a fixed grid of the requested width.
func unsignedCells(count int) []uint64 { return make([]uint64, count) }

// TestPublishFixedGridGuardsBucketArithmeticBeforeTheSchema walks the boundary
// of the overflow guard the generic publisher applies to the last ordinal. The
// guard runs before SetSchema, so a rejected grid must leave the sink untouched
// rather than half published. The empty grid is the interesting corner: the
// guard multiplies len(cells)-1, so zero cells must not underflow into a
// gigantic multiplier.
func TestPublishFixedGridGuardsBucketArithmeticBeforeTheSchema(t *testing.T) {
	t.Parallel()

	maxInt64 := int64(math.MaxInt64)
	for _, testCase := range []struct {
		name      string
		firstUnix int64
		cells     int
		wantErr   bool
	}{
		{name: "last bucket lands exactly on MaxInt64", firstUnix: maxInt64 - 2, cells: 3},
		{name: "last bucket exceeds MaxInt64 by one", firstUnix: maxInt64 - 2, cells: 4, wantErr: true},
		{name: "empty grid skips the guard entirely", firstUnix: maxInt64, cells: 0},
		{name: "single bucket at MaxInt64", firstUnix: maxInt64, cells: 1},
		{name: "two buckets from MaxInt64 overflow", firstUnix: maxInt64, cells: 2, wantErr: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			sink := &fakeSink{}
			err := publishFixedGrid(
				context.Background(),
				sink,
				time.Unix(testCase.firstUnix, 0).UTC(),
				time.Second,
				searchjobs.Column{Name: "count", Kind: searchjobs.ValueKindUnsigned},
				unsignedCells(testCase.cells),
				searchjobs.UnsignedValue,
			)
			if testCase.wantErr {
				if !errors.Is(err, searchjobs.ErrInvalidResult) || sink.setCalls != 0 || len(sink.rows) != 0 {
					t.Fatalf("err=%v setCalls=%d rows=%d, want a rejected grid", err, sink.setCalls, len(sink.rows))
				}
				return
			}
			if err != nil || sink.setCalls != 1 || len(sink.rows) != testCase.cells {
				t.Fatalf("err=%v setCalls=%d rows=%d, want %d rows", err, sink.setCalls, len(sink.rows), testCase.cells)
			}
		})
	}
}

// TestPublishWideGridStopsAfterTheSchemaWhenNoSeriesSurvive covers the
// empty-columns short-circuit on both instantiations of the shared publisher.
// The buffered rows deliberately exist: publishing them against a one-column
// schema would emit ragged rows.
func TestPublishWideGridStopsAfterTheSchemaWhenNoSeriesSurvive(t *testing.T) {
	t.Parallel()

	bucket := time.Unix(0, 0).UTC()
	countSink := &fakeSink{}
	if err := publishTimechart(context.Background(), countSink, bufferedTimechart{
		rows: []timechartRow{{bucket: bucket}, {bucket: bucket.Add(time.Minute)}},
	}); err != nil {
		t.Fatalf("publishTimechart: %v", err)
	}
	valueSink := &fakeSink{}
	if err := publishValueTimechart(context.Background(), valueSink, bufferedValueTimechart{
		rows: []timechartValueRow{{bucket: bucket}, {bucket: bucket.Add(time.Minute)}},
	}); err != nil {
		t.Fatalf("publishValueTimechart: %v", err)
	}
	for name, sink := range map[string]*fakeSink{"count": countSink, "value": valueSink} {
		if sink.setCalls != 1 || len(sink.schema.Columns) != 1 ||
			sink.schema.Columns[0].Name != "_time" || len(sink.rows) != 0 {
			t.Fatalf("%s grid schema=%#v calls=%d rows=%d", name, sink.schema, sink.setCalls, len(sink.rows))
		}
	}
}

// TestPublishWideGridKeepsTheNullabilityOfEachInstantiation pins the two series
// column contracts the generic publisher now shares, plus the NULL mapping of a
// buffered numeric cell that never received a value.
func TestPublishWideGridKeepsTheNullabilityOfEachInstantiation(t *testing.T) {
	t.Parallel()

	bucket := time.Unix(0, 0).UTC()
	countSink := &fakeSink{}
	if err := publishTimechart(context.Background(), countSink, bufferedTimechart{
		columns: []string{"alpha"},
		rows:    []timechartRow{{bucket: bucket, cells: []uint64{7}}},
	}); err != nil {
		t.Fatalf("publishTimechart: %v", err)
	}
	if countSink.schema.Columns[1].Nullable ||
		countSink.schema.Columns[1].Kind != searchjobs.ValueKindUnsigned {
		t.Fatalf("count series column = %#v", countSink.schema.Columns[1])
	}
	if value, ok := countSink.rows[0][1].Unsigned(); !ok || value != 7 {
		t.Fatalf("count cell = %#v", countSink.rows[0][1])
	}

	valueSink := &fakeSink{}
	if err := publishValueTimechart(context.Background(), valueSink, bufferedValueTimechart{
		columns: []string{"alpha", "beta"},
		rows: []timechartValueRow{{
			bucket: bucket,
			cells:  []nullableFloat64{{value: 1.5, valid: true}, {value: 99, valid: false}},
		}},
	}); err != nil {
		t.Fatalf("publishValueTimechart: %v", err)
	}
	for index := 1; index <= 2; index++ {
		if !valueSink.schema.Columns[index].Nullable ||
			valueSink.schema.Columns[index].Kind != searchjobs.ValueKindDouble {
			t.Fatalf("value series column %d = %#v", index, valueSink.schema.Columns[index])
		}
	}
	if value, ok := valueSink.rows[0][1].Double(); !ok || value != 1.5 {
		t.Fatalf("present cell = %#v", valueSink.rows[0][1])
	}
	if !valueSink.rows[0][2].IsNull() {
		t.Fatalf("absent cell = %#v, want NULL even though it carries 99", valueSink.rows[0][2])
	}
}

// TestGridPublishersRefuseToStartOnACanceledContext and a failing SetSchema:
// neither publisher may emit rows once the prologue fails.
func TestGridPublishersRefuseToStartOnACanceledContext(t *testing.T) {
	t.Parallel()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	fixedSink := &fakeSink{}
	if err := publishFixedGrid(
		canceled, fixedSink, time.Unix(0, 0).UTC(), time.Minute,
		searchjobs.Column{Name: "count", Kind: searchjobs.ValueKindUnsigned},
		unsignedCells(3), searchjobs.UnsignedValue,
	); !errors.Is(err, context.Canceled) || fixedSink.setCalls != 0 {
		t.Fatalf("fixed grid err=%v setCalls=%d", err, fixedSink.setCalls)
	}
	wideSink := &fakeSink{}
	if err := publishTimechart(canceled, wideSink, bufferedTimechart{
		columns: []string{"alpha"},
		rows:    []timechartRow{{bucket: time.Unix(0, 0).UTC(), cells: []uint64{1}}},
	}); !errors.Is(err, context.Canceled) || wideSink.setCalls != 0 {
		t.Fatalf("wide grid err=%v setCalls=%d", err, wideSink.setCalls)
	}

	failing := errors.New("schema rejected")
	rejecting := &fakeSink{setErr: failing}
	if err := publishFixedGrid(
		context.Background(), rejecting, time.Unix(0, 0).UTC(), time.Minute,
		searchjobs.Column{Name: "count", Kind: searchjobs.ValueKindUnsigned},
		unsignedCells(3), searchjobs.UnsignedValue,
	); !errors.Is(err, failing) || len(rejecting.rows) != 0 {
		t.Fatalf("fixed grid schema failure err=%v rows=%d", err, len(rejecting.rows))
	}
	rejecting = &fakeSink{setErr: failing}
	if err := publishTimechart(context.Background(), rejecting, bufferedTimechart{
		columns: []string{"alpha"},
		rows:    []timechartRow{{bucket: time.Unix(0, 0).UTC(), cells: []uint64{1}}},
	}); !errors.Is(err, failing) || len(rejecting.rows) != 0 {
		t.Fatalf("wide grid schema failure err=%v rows=%d", err, len(rejecting.rows))
	}
}
