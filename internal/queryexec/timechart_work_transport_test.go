package queryexec

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func workTransportQuery(t *testing.T) clickhouse.CompiledQuery {
	t.Helper()
	earliest := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	parsed, err := spl.Parse(`index=gradethis | eval payload="x,y" | makemv delim="," payload | mvexpand payload | timechart span=1s partial=false count`)
	if err != nil {
		t.Fatal(err)
	}
	cutoff := uint64(1)
	logical, err := plan.Build(parsed, plan.Scope{TenantID: "tenant", AuthorizedIndexes: []string{"gradethis"}, Earliest: earliest, Latest: earliest.Add(2 * time.Second), SearchStart: earliest.Add(time.Hour), IndexTimeCutoff: earliest.Add(time.Hour), VisibilityCutoff: &cutoff, SearchTimezone: "UTC"})
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := (clickhouse.Compiler{}).Compile(logical)
	if err != nil {
		t.Fatal(err)
	}
	if !compiled.HasTimechartWorkReceipt() {
		t.Fatal("missing native work receipt contract")
	}
	return compiled
}

func workTransportRows(query clickhouse.CompiledQuery) *fakeRows {
	columns := []string{clickhouse.TimechartOrdinalColumn, clickhouse.TimechartBucketColumn, clickhouse.TimechartBucketPresentColumn, clickhouse.TimechartCountColumn, clickhouse.TimechartWorkRowsColumn}
	kinds := []string{"UInt64", "DateTime64(9, 'UTC')", "UInt8", "UInt64", "UInt64"}
	scans := []reflect.Type{reflect.TypeFor[uint64](), reflect.TypeFor[time.Time](), reflect.TypeFor[uint8](), reflect.TypeFor[uint64](), reflect.TypeFor[uint64]()}
	types := make([]driver.ColumnType, len(columns))
	for i := range types {
		types[i] = fakeColumnType{name: columns[i], databaseType: kinds[i], scanType: scans[i]}
	}
	return &fakeRows{columns: columns, types: types, data: [][]any{{uint64(0), query.Timechart.Boundaries[0], uint8(1), uint64(1), uint64(8000)}, {uint64(1), query.Timechart.Boundaries[1], uint8(0), uint64(0), uint64(8000)}}}
}

type workTransportSink struct {
	fakeSink
	work      uint64
	workCalls int
}

func (sink *workTransportSink) SetTimechartWork(work uint64) error {
	sink.work = work
	sink.workCalls++
	return nil
}

func TestTimechartWorkTransportValidatesBeforePublication(t *testing.T) {
	query := workTransportQuery(t)
	for _, test := range []struct {
		name   string
		mutate func(*fakeRows)
		limit  bool
	}{
		{"late inconsistent", func(rows *fakeRows) { rows.data[1][4] = uint64(7999) }, false},
		{"limit", func(rows *fakeRows) { rows.data[1][4] = uint64(15001) }, true},
		{"wrong column", func(rows *fakeRows) { rows.columns[4] = "count" }, false},
		{"wrong type", func(rows *fakeRows) {
			rows.types[4] = fakeColumnType{name: rows.columns[4], databaseType: "Nullable(UInt64)", scanType: reflect.TypeFor[*uint64]()}
		}, false},
		{"late close error", func(rows *fakeRows) { rows.closeErr = errors.New("late stream failure") }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			rows := workTransportRows(query)
			test.mutate(rows)
			sink := &workTransportSink{}
			err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(context.Background(), query, sink)
			if err == nil || (test.limit && !errors.Is(err, searchjobs.ErrExecutionLimit)) {
				t.Fatalf("error=%v", err)
			}
			if sink.setCalls != 0 || len(sink.rows) != 0 || sink.workCalls != 0 {
				t.Fatal("invalid late receipt published schema, rows or work")
			}
		})
	}
	rows := workTransportRows(query)
	sink := &workTransportSink{}
	if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(context.Background(), query, sink); err != nil {
		t.Fatal(err)
	}
	if sink.work != 8000 || sink.workCalls != 1 || len(sink.rows) != 2 || len(sink.schema.Columns) != 2 {
		t.Fatalf("work=%d calls=%d rows=%d columns=%d", sink.work, sink.workCalls, len(sink.rows), len(sink.schema.Columns))
	}
}
