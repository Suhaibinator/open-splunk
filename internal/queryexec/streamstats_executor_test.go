package queryexec

import (
	"context"
	"reflect"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestExecutorPublishesStreamStatsAsRowPreservingNullableUnsignedCount(t *testing.T) {
	t.Parallel()

	rows := &fakeRows{
		columns: []string{"event_id", "service", "prior"},
		types: []driver.ColumnType{
			fakeColumnType{
				name:         "event_id",
				databaseType: "String",
				scanType:     reflect.TypeOf(""),
			},
			fakeColumnType{
				name:         "service",
				databaseType: "Nullable(String)",
				scanType:     reflect.TypeOf((*string)(nil)),
				nullable:     true,
			},
			fakeColumnType{
				name:         "prior",
				databaseType: "Nullable(UInt64)",
				scanType:     reflect.TypeOf((*uint64)(nil)),
				nullable:     true,
			},
		},
		data: [][]any{
			{"event-1", "api", uint64(0)},
			{"event-2", "worker", uint64(0)},
			{"event-3", nil, nil},
			{"event-4", "api", uint64(1)},
		},
	}
	sink := &fakeSink{}
	query := clickhouse.CompiledQuery{
		SQL:          "SELECT event_id, service, prior FROM streamstats_result",
		OutputFields: []string{"event_id", "service", "prior"},
	}
	if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(),
		query,
		sink,
	); err != nil {
		t.Fatalf("Execute(streamstats result): %v", err)
	}

	wantSchema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "event_id", Kind: searchjobs.ValueKindString},
		{Name: "service", Kind: searchjobs.ValueKindString, Nullable: true},
		{Name: "prior", Kind: searchjobs.ValueKindUnsigned, Nullable: true},
	}}
	if sink.setCalls != 1 || !reflect.DeepEqual(sink.schema, wantSchema) {
		t.Fatalf("streamstats schema = %#v after %d calls, want %#v", sink.schema, sink.setCalls, wantSchema)
	}
	if len(sink.rows) != len(rows.data) {
		t.Fatalf("streamstats published %d rows, want one for each of %d inputs", len(sink.rows), len(rows.data))
	}
	for index, want := range []uint64{0, 0} {
		got, ok := sink.rows[index][2].Unsigned()
		if !ok || got != want {
			t.Fatalf("prior count at row %d = (%d, %v), want %d", index, got, ok, want)
		}
	}
	if !sink.rows[2][1].IsNull() || !sink.rows[2][2].IsNull() {
		t.Fatalf("missing BY row = %#v, want retained row with null group and count", sink.rows[2])
	}
	if got, ok := sink.rows[3][2].Unsigned(); !ok || got != 1 {
		t.Fatalf("second api prior count = (%d, %v), want 1", got, ok)
	}
	if !rows.closed {
		t.Fatal("streamstats result rows were not closed")
	}
}
