package queryexec

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// BenchmarkExactTimechartTransport isolates the occupancy adapter at the maximum
// 10,000-bucket grid size. Fixtures and caller scan destinations are reused;
// each iteration captures a new complete occupancy sequence.
func BenchmarkExactTimechartTransport(b *testing.B) {
	const buckets = 10000
	first := time.Unix(0, 0).UTC()
	source := &fakeRows{data: make([][]any, buckets)}
	for index := range source.data {
		source.data[index] = []any{uint64(index), first.Add(time.Duration(index) * time.Millisecond), uint8(1), uint64(1)}
	}
	var ordinal, count uint64
	var bucket time.Time
	destinations := []any{&ordinal, &bucket, &count}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		source.index = 0
		wrapped := &timechartGridRows{Rows: source, present: make([]uint8, 0, buckets)}
		for wrapped.Next() {
			if err := wrapped.Scan(destinations...); err != nil {
				b.Fatal(err)
			}
		}
		if err := wrapped.Err(); err != nil {
			b.Fatal(err)
		}
		if len(wrapped.present) != buckets || ordinal != buckets-1 || count != 1 {
			b.Fatal("incomplete exact-grid transport")
		}
	}
	b.ReportMetric(buckets, "buckets/op")
}

// BenchmarkExactTimechartPublication measures complete production validation,
// occupancy capture, and exact bounds publication for 10,000 subsecond buckets.
func BenchmarkExactTimechartPublication(b *testing.B) {
	const buckets = 10000
	first := time.Unix(0, 0).UTC()
	boundaries := make([]time.Time, buckets+1)
	for index := range boundaries {
		boundaries[index] = first.Add(time.Duration(index) * time.Millisecond)
	}
	rows := benchmarkTimechartRows(buckets, 10)
	calendarTimechartRows(rows, boundaries[:buckets])
	rows.columns = slices.Insert(rows.columns, 2, clickhouse.TimechartBucketPresentColumn)
	rows.types = slices.Insert(rows.types, 2, driver.ColumnType(fakeColumnType{name: clickhouse.TimechartBucketPresentColumn, databaseType: "UInt8", scanType: reflect.TypeFor[uint8]()}))
	for index := range rows.data {
		rows.data[index] = slices.Insert(rows.data[index], 2, any(uint8(1)))
	}
	settings, err := querySettings(Config{})
	if err != nil {
		b.Fatal(err)
	}
	executor := &Executor{connection: &benchmarkRowsConnection{template: rows}, settings: mustValidatedSettings(b, settings), expandTimechartGroupLimit: true, newQueryID: func() (string, error) { return "exact-timechart-publication-benchmark", nil }}
	query := timechartQuery(first, buckets)
	query.Timechart.ExactGrid, query.Timechart.Calendar = true, true
	query.Timechart.Span = time.Millisecond
	query.Timechart.Boundaries = boundaries
	query.Timechart.Continuous, query.Timechart.IncludePartial = true, true
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		sink := &benchmarkExactTimechartSink{}
		sink.wantColumns, sink.wantRows = 11, buckets
		if err := executor.Execute(context.Background(), query, sink); err != nil {
			b.Fatal(err)
		}
		if sink.rows != buckets || sink.bounds != buckets || sink.schemaCalls != 1 {
			b.Fatalf("incomplete exact publication: %+v", sink)
		}
	}
	b.ReportMetric(buckets, "buckets/op")
}

type benchmarkExactTimechartSink struct {
	benchmarkTimechartSink
	bounds int
}

func (sink *benchmarkExactTimechartSink) AddRowWithTimeBucket(values []searchjobs.Value, bounds searchjobs.TimeBucketBounds) error {
	if bounds.Earliest == "" || bounds.Latest == "" {
		return fmt.Errorf("missing exact bucket bounds")
	}
	sink.bounds++
	return sink.AddRow(values)
}
