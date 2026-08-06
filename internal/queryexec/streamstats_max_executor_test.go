package queryexec

import (
	"context"
	"math/big"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestExecutorPublishesStreamStatsMaximumMixedValuesWithoutDroppingRows(t *testing.T) {
	t.Parallel()

	wideInteger, ok := new(big.Int).SetString("999999999999999999990", 10)
	if !ok {
		t.Fatal("parse wide streamstats maximum fixture")
	}
	invalidUTF8 := string([]byte{0xff, 0x00})
	rows := &fakeRows{
		columns: []string{"event_id", "service", "running_max"},
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
				name:         "running_max",
				databaseType: "Dynamic",
				scanType:     reflect.TypeOf((*any)(nil)).Elem(),
			},
		},
		data: [][]any{
			{"event-1", "api", chcol.NewDynamicWithType(float64(9.25), "Float64")},
			{"event-2", "worker", chcol.NewDynamicWithType("zulu", "String")},
			{"event-3", nil, chcol.NewDynamicWithType(nil, "")},
			{"event-4", "api", chcol.NewDynamicWithType(invalidUTF8, "String")},
			{"event-5", "worker", chcol.NewDynamicWithType(wideInteger, "Int256")},
		},
	}

	anchor := time.Date(2026, time.August, 4, 12, 0, 0, 0, time.UTC)
	query := queryIntegrationCompileSearchRange(
		t,
		`index=main | table event_id,service,status | streamstats max(status) AS status | rename status AS running_max | table event_id,service,running_max`,
		anchor,
		anchor.Add(-time.Hour),
		anchor.Add(time.Hour),
	)
	if !slices.Equal(query.OutputFields, []string{"event_id", "service", "running_max"}) {
		t.Fatalf("compiled streamstats maximum fields = %v", query.OutputFields)
	}

	sink := &fakeSink{}
	if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(),
		query,
		sink,
	); err != nil {
		t.Fatalf("Execute(streamstats maximum result): %v", err)
	}

	wantSchema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "event_id", Kind: searchjobs.ValueKindString},
		{Name: "service", Kind: searchjobs.ValueKindString, Nullable: true},
		{Name: "running_max", Kind: searchjobs.ValueKindMixed, Nullable: true},
	}}
	if sink.setCalls != 1 || !reflect.DeepEqual(sink.schema, wantSchema) {
		t.Fatalf(
			"streamstats maximum schema = %#v after %d calls, want %#v",
			sink.schema,
			sink.setCalls,
			wantSchema,
		)
	}
	if len(sink.rows) != len(rows.data) {
		t.Fatalf(
			"streamstats maximum published %d rows, want one for each of %d inputs",
			len(sink.rows),
			len(rows.data),
		)
	}
	if got, ok := sink.rows[0][2].Double(); !ok || got != 9.25 {
		t.Fatalf("numeric maximum = %v/%v, want Double(9.25)", got, ok)
	}
	if got, ok := sink.rows[1][2].String(); !ok || got != "zulu" {
		t.Fatalf("lexical maximum = %q/%v, want String(zulu)", got, ok)
	}
	if !sink.rows[2][1].IsNull() || !sink.rows[2][2].IsNull() {
		t.Fatalf("missing BY row = %#v, want retained row with null maximum", sink.rows[2])
	}
	if got, ok := sink.rows[3][2].Bytes(); !ok || !slices.Equal(got, []byte{0xff, 0x00}) {
		t.Fatalf("binary maximum = %x/%v, want Bytes(ff00)", got, ok)
	}
	if got, ok := sink.rows[4][2].Decimal(); !ok || got != wideInteger.String() {
		t.Fatalf("wide numeric maximum = %q/%v, want Decimal(%s)", got, ok, wideInteger)
	}
	if !rows.closed {
		t.Fatal("streamstats maximum result rows were not closed")
	}
}
