package queryexec

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// BenchmarkTimechartPublication is the deterministic before/after runtime
// benchmark for buffered split-timechart validation and publication. It uses
// the production Executor with a repeatable driver transport, leaving database
// execution to the opt-in ClickHouse integration suite. Fixture construction is
// outside the timed region; buffering, validation, schema construction, and
// row publication are measured.
//
// Run the paired sample with:
//
//	go test ./internal/queryexec -run '^$' \
//	  -bench '^BenchmarkTimechartPublication$' -benchtime=100x -count=5 -benchmem
func BenchmarkTimechartPublication(b *testing.B) {
	b.ReportAllocs()
	for _, shape := range []struct {
		buckets int
		series  int
	}{
		{buckets: 100, series: 10},
		{buckets: 1_000, series: 10},
	} {
		name := fmt.Sprintf("buckets-%05d/series-%03d", shape.buckets, shape.series)
		rows := benchmarkTimechartRows(shape.buckets, shape.series)
		connection := &benchmarkRowsConnection{template: rows}
		settings, err := querySettings(Config{})
		if err != nil {
			b.Fatalf("create %s query settings: %v", name, err)
		}
		executor := &Executor{
			connection:                connection,
			settings:                  mustValidatedSettings(b, settings),
			expandTimechartGroupLimit: true,
			newQueryID:                func() (string, error) { return "timechart-publication-benchmark", nil },
		}
		query := timechartQuery(time.Unix(0, 0).UTC(), uint64(shape.buckets))

		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				sink := &benchmarkTimechartSink{
					wantColumns: shape.series + 1,
					wantRows:    shape.buckets,
				}
				if executeErr := executor.Execute(context.Background(), query, sink); executeErr != nil {
					b.Fatalf("execute: %v", executeErr)
				}
				if sink.schemaCalls != 1 || sink.rows != shape.buckets {
					b.Fatalf(
						"published schema/rows = %d/%d, want 1/%d",
						sink.schemaCalls,
						sink.rows,
						shape.buckets,
					)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(shape.buckets*shape.series), "cells/op")
		})
	}
}

type benchmarkRowsConnection struct {
	template *fakeRows
}

func (connection *benchmarkRowsConnection) Query(
	context.Context,
	string,
	...any,
) (driver.Rows, error) {
	return &fakeRows{
		columns: connection.template.columns,
		data:    connection.template.data,
		types:   connection.template.types,
	}, nil
}

type benchmarkTimechartSink struct {
	rows        int
	schemaCalls int
	wantColumns int
	wantRows    int
}

func (sink *benchmarkTimechartSink) SetSchema(schema searchjobs.Schema) error {
	sink.schemaCalls++
	if len(schema.Columns) != sink.wantColumns {
		return fmt.Errorf("schema columns = %d, want %d", len(schema.Columns), sink.wantColumns)
	}
	return nil
}

func (sink *benchmarkTimechartSink) AddRow(values []searchjobs.Value) error {
	sink.rows++
	if sink.rows > sink.wantRows {
		return fmt.Errorf("published more than %d rows", sink.wantRows)
	}
	if len(values) != sink.wantColumns {
		return fmt.Errorf("row width = %d, want %d", len(values), sink.wantColumns)
	}
	return nil
}

func benchmarkTimechartRows(bucketCount, seriesCount int) *fakeRows {
	names := make([]string, seriesCount)
	for index := range names {
		names[index] = fmt.Sprintf("0:series-%03d", index)
	}
	counts := make([][]uint64, bucketCount)
	for bucket := range counts {
		counts[bucket] = make([]uint64, seriesCount)
		for series := range counts[bucket] {
			counts[bucket][series] = uint64((bucket + 1) * (series + 1))
		}
	}
	return timechartOrdinalRows(names, counts)
}
