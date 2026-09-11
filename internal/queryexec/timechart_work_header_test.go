package queryexec

import (
	"context"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestObservedTimechartWorkHeaderValidation(t *testing.T) {
	compiled := compileReadAdmissionQuery(t, `index=target | eval payload="a,b" | makemv delim="," payload | mvexpand payload | timechart span=1s fixedrange=false count`)
	fields := []string{"_time", "timechart_observed_work", "timechart_observed_input_row"}
	if !slices.Equal(compiled.OutputFields, fields) {
		t.Fatalf("observed work transport fields=%v want=%v", compiled.OutputFields, fields)
	}
	columns := []clickhouse.RelationColumn{{Name: fields[0], Type: "DateTime64(9, 'UTC')"}, {Name: fields[1], Type: "UInt64"}, {Name: fields[2], Type: "UInt64"}}
	first := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	header := []any{first, uint64(15000), uint64(0)}
	eventRow := []any{first.Add(time.Minute), uint64(15000), uint64(1)}
	for _, testCase := range []struct {
		name string
		rows [][]any
	}{
		{"missing header", [][]any{eventRow}},
		{"missing whole transport", nil},
		{"repeated header", [][]any{header, eventRow, header}},
		{"invalid row flag", [][]any{header, {first, uint64(15000), uint64(2)}}},
		{"signed row flag", [][]any{{first, uint64(15000), int64(0)}}},
		{"late inconsistent counter", [][]any{header, {first.Add(time.Minute), uint64(14999), uint64(1)}}},
		{"header counter exceeds ceiling", [][]any{{first, uint64(15001), uint64(0)}}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := compiled.ContinueWithTimeBucketsContext(context.Background(), columns, testCase.rows, nil); err == nil {
				t.Fatal("invalid observed work transport accepted")
			}
		})
	}
	t.Run("header excluded from extent", func(t *testing.T) {
		next, err := compiled.ContinueWithTimeBucketsContext(context.Background(), columns, [][]any{header, eventRow}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if next.HasEmptyTimechartInput() || next.Timechart == nil || !next.Timechart.FirstBucket.Equal(first.Add(time.Minute)) || next.Timechart.BucketCount != 1 {
			t.Fatalf("header altered observed extent: empty=%t grid=%+v", next.HasEmptyTimechartInput(), next.Timechart)
		}
	})
	t.Run("header preserves work without fabricating an event", func(t *testing.T) {
		next, err := compiled.ContinueWithTimeBucketsContext(context.Background(), columns, [][]any{header}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !next.HasEmptyTimechartInput() {
			t.Fatal("header fabricated observed input")
		}
		sink := &timechartWorkReceiptSink{}
		connection := &fakeQueryConnection{}
		if err := mustExecutor(t, connection).Execute(context.Background(), next, sink); err != nil {
			t.Fatal(err)
		}
		if connection.query != "" || sink.workCalls != 1 || sink.work != 15000 || len(sink.rows) != 0 {
			t.Fatalf("empty fastpath lost work or queried source: query=%q receipt calls=%d work=%d rows=%d", connection.query, sink.workCalls, sink.work, len(sink.rows))
		}
	})
}

func TestOrdinaryRowsDoNotInterpretUndeclaredTimechartWorkFields(t *testing.T) {
	compiled := compileReadAdmissionQuery(t, `index=target | eval timechart_observed_work=15001, timechart_observed_input_row=0 | table _time timechart_observed_work timechart_observed_input_row`)
	first := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := &fakeRows{
		columns: compiled.OutputFields,
		types: []driver.ColumnType{
			fakeColumnType{name: "_time", databaseType: "DateTime64(9, 'UTC')", scanType: reflect.TypeFor[time.Time]()},
			fakeColumnType{name: "timechart_observed_work", databaseType: "UInt64", scanType: reflect.TypeFor[uint64]()},
			fakeColumnType{name: "timechart_observed_input_row", databaseType: "UInt64", scanType: reflect.TypeFor[uint64]()},
		},
		data: [][]any{{first, uint64(15001), uint64(0)}},
	}
	sink := &fakeSink{}
	if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(context.Background(), compiled, sink); err != nil {
		t.Fatal(err)
	}
	if len(sink.rows) != 1 || len(sink.schema.Columns) != 3 || len(sink.rows[0]) != 3 {
		t.Fatalf("ordinary user fields were hidden: schema=%+v rows=%+v", sink.schema, sink.rows)
	}
	work, ok := sink.rows[0][1].Unsigned()
	if !ok || work != 15001 {
		t.Fatal("undeclared user work value was changed")
	}
}
