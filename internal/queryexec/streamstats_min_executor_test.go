package queryexec

import (
	"context"
	"reflect"
	"slices"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestExecutorPublishesStreamStatsMinimumMixedValuesWithoutDroppingRows(t *testing.T) {
	t.Parallel()

	invalidUTF8 := string([]byte{0xff, 0x00})
	rows := &fakeRows{
		columns: []string{"event_id", "service", "prior_min"},
		types: []driver.ColumnType{
			fakeColumnType{
				name:         "event_id",
				databaseType: "String",
				scanType:     reflect.TypeFor[string](),
			},
			fakeColumnType{
				name:         "service",
				databaseType: "Nullable(String)",
				scanType:     reflect.TypeFor[*string](),
				nullable:     true,
			},
			fakeColumnType{
				name:         "prior_min",
				databaseType: "Dynamic",
				scanType:     reflect.TypeFor[any](),
			},
		},
		data: [][]any{
			{"event-1", "api", chcol.NewDynamicWithType(float64(2.5), "Float64")},
			{"event-2", "worker", chcol.NewDynamicWithType("10", "String")},
			{"event-3", nil, chcol.NewDynamicWithType(nil, "")},
			{"event-4", "api", chcol.NewDynamicWithType(invalidUTF8, "String")},
		},
	}
	sink := &fakeSink{}
	query := clickhouse.CompiledQuery{
		SQL:          "SELECT event_id, service, prior_min FROM streamstats_minimum_result",
		OutputFields: []string{"event_id", "service", "prior_min"},
	}
	if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(),
		query,
		sink,
	); err != nil {
		t.Fatalf("Execute(streamstats minimum result): %v", err)
	}

	wantSchema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "event_id", Kind: searchjobs.ValueKindString},
		{Name: "service", Kind: searchjobs.ValueKindString, Nullable: true},
		{Name: "prior_min", Kind: searchjobs.ValueKindMixed, Nullable: true},
	}}
	if sink.setCalls != 1 || !reflect.DeepEqual(sink.schema, wantSchema) {
		t.Fatalf(
			"streamstats minimum schema = %#v after %d calls, want %#v",
			sink.schema,
			sink.setCalls,
			wantSchema,
		)
	}
	if len(sink.rows) != len(rows.data) {
		t.Fatalf(
			"streamstats minimum published %d rows, want one for each of %d inputs",
			len(sink.rows),
			len(rows.data),
		)
	}
	if got, ok := sink.rows[0][2].Double(); !ok || got != 2.5 {
		t.Fatalf("numeric minimum = %v/%v, want Double(2.5)", got, ok)
	}
	if got, ok := sink.rows[1][2].String(); !ok || got != "10" {
		t.Fatalf("lexical minimum = %q/%v, want String(10)", got, ok)
	}
	if !sink.rows[2][1].IsNull() || !sink.rows[2][2].IsNull() {
		t.Fatalf("missing BY row = %#v, want retained row with null minimum", sink.rows[2])
	}
	if got, ok := sink.rows[3][2].Bytes(); !ok || !slices.Equal(got, []byte{0xff, 0x00}) {
		t.Fatalf("binary minimum = %x/%v, want Bytes(ff00)", got, ok)
	}
	if !rows.closed {
		t.Fatal("streamstats minimum result rows were not closed")
	}
}
