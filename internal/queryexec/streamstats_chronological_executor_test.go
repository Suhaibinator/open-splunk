package queryexec

import (
	"context"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestExecutorPublishesStackedStreamStatsChronologicalMixedValuesWithoutDroppingRows(t *testing.T) {
	t.Parallel()

	bytesEnvelope := map[string]string{
		extendedTypeKey:  "bytes/v1",
		extendedValueKey: "/wA",
	}
	rows := &fakeRows{
		columns: []string{"event_id", "service", "first_seen", "last_seen"},
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
				name:         "first_seen",
				databaseType: "Dynamic",
				scanType:     reflect.TypeFor[any](),
			},
			fakeColumnType{
				name:         "last_seen",
				databaseType: "Dynamic",
				scanType:     reflect.TypeFor[any](),
			},
		},
		data: [][]any{
			{
				"event-1",
				"api",
				chcol.NewDynamicWithType("alpha", "String"),
				chcol.NewDynamicWithType("zulu", "String"),
			},
			{
				"event-2",
				"worker",
				chcol.NewDynamicWithType(nil, ""),
				chcol.NewDynamicWithType(nil, ""),
			},
			{
				"event-3",
				nil,
				chcol.NewDynamicWithType(bytesEnvelope, "Map(String, String)"),
				chcol.NewDynamicWithType("omega", "String"),
			},
		},
	}

	anchor := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	query := queryIntegrationCompileSearchRange(
		t,
		`index=main | table _time,event_id,service,status | streamstats earliest(status) AS first_seen | streamstats latest(status) AS last_seen | table event_id,service,first_seen,last_seen`,
		anchor,
		anchor.Add(-time.Hour),
		anchor.Add(time.Hour),
	)
	if !slices.Equal(
		query.OutputFields,
		[]string{"event_id", "service", "first_seen", "last_seen"},
	) {
		t.Fatalf("compiled streamstats chronological fields = %v", query.OutputFields)
	}

	sink := &fakeSink{}
	if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(),
		query,
		sink,
	); err != nil {
		t.Fatalf("Execute(streamstats chronological result): %v", err)
	}

	wantSchema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "event_id", Kind: searchjobs.ValueKindString},
		{Name: "service", Kind: searchjobs.ValueKindString, Nullable: true},
		{Name: "first_seen", Kind: searchjobs.ValueKindMixed, Nullable: true},
		{Name: "last_seen", Kind: searchjobs.ValueKindMixed, Nullable: true},
	}}
	if sink.setCalls != 1 || !reflect.DeepEqual(sink.schema, wantSchema) {
		t.Fatalf(
			"streamstats chronological schema = %#v after %d calls, want %#v",
			sink.schema,
			sink.setCalls,
			wantSchema,
		)
	}
	if len(sink.rows) != len(rows.data) {
		t.Fatalf(
			"streamstats chronological published %d rows, want one for each of %d inputs",
			len(sink.rows),
			len(rows.data),
		)
	}
	if got, ok := sink.rows[0][2].String(); !ok || got != "alpha" {
		t.Fatalf("earliest lexical value = %q/%v, want String(alpha)", got, ok)
	}
	if got, ok := sink.rows[0][3].String(); !ok || got != "zulu" {
		t.Fatalf("latest lexical value = %q/%v, want String(zulu)", got, ok)
	}
	if !sink.rows[1][2].IsNull() || !sink.rows[1][3].IsNull() {
		t.Fatalf("empty chronological frame = %#v, want nullable Dynamic values", sink.rows[1])
	}
	if !sink.rows[2][1].IsNull() {
		t.Fatalf("missing group field = %#v, want retained row with null service", sink.rows[2])
	}
	if got, ok := sink.rows[2][2].Bytes(); !ok || !slices.Equal(got, []byte{0xff, 0x00}) {
		t.Fatalf("earliest binary value = %x/%v, want Bytes(ff00)", got, ok)
	}
	if got, ok := sink.rows[2][3].String(); !ok || got != "omega" {
		t.Fatalf("latest lexical value = %q/%v, want String(omega)", got, ok)
	}
	if !rows.closed {
		t.Fatal("streamstats chronological result rows were not closed")
	}
}
