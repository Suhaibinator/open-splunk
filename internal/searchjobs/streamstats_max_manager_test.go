package searchjobs

import (
	"context"
	"reflect"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestManagerRetainsReplacingAndStackedStreamStatsMaximumResults(t *testing.T) {
	t.Parallel()

	wideMaximum, err := DecimalValue("999999999999999999990")
	if err != nil {
		t.Fatal(err)
	}
	schema := Schema{Columns: []Column{
		{Name: "event_id", Kind: ValueKindString},
		{Name: "service", Kind: ValueKindString, Nullable: true},
		{Name: "status", Kind: ValueKindMixed, Nullable: true},
		{Name: "max_so_far", Kind: ValueKindMixed, Nullable: true},
	}}
	wantRows := [][]Value{
		{StringValue("event-1"), StringValue("api"), NullValue(), NullValue()},
		{StringValue("event-2"), StringValue("worker"), NullValue(), NullValue()},
		{StringValue("event-3"), NullValue(), NullValue(), NullValue()},
		{StringValue("event-4"), StringValue("api"), DoubleValue(9.25), DoubleValue(9.25)},
		{StringValue("event-5"), StringValue("worker"), StringValue("zulu"), StringValue("zulu")},
		{StringValue("event-6"), StringValue("api"), wideMaximum, StringValue("zulu")},
		{StringValue("event-7"), StringValue("api"), BytesValue([]byte{0xff, 0x00}), BytesValue([]byte{0xff, 0x00})},
	}
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(
			_ context.Context,
			query clickhouse.CompiledQuery,
			sink ResultSink,
		) error {
			if !slices.Equal(query.OutputFields, []string{"event_id", "service", "status", "max_so_far"}) {
				t.Fatalf("compiled streamstats maximum fields = %v", query.OutputFields)
			}
			if query.Timechart != nil || query.Chart != nil || query.SparseFields {
				t.Fatalf("streamstats maximum declared a non-tabular transport: %#v", query)
			}
			if err := sink.SetSchema(schema); err != nil {
				return err
			}
			for _, row := range wantRows {
				if err := sink.AddRow(row); err != nil {
					return err
				}
			}
			return nil
		}),
		CleanupInterval: -1,
		NewID:           sequenceIDs("streamstats-maximum-row-preserving"),
	})
	created, err := manager.Create(context.Background(), withSPL(
		validRequest(),
		"index=main | table event_id,service,status | streamstats current=f window=2 global=f max(status) AS status BY service | streamstats max(status) AS max_so_far | table event_id,service,status,max_so_far",
	))
	if err != nil {
		t.Fatalf("Create(streamstats maximum): %v", err)
	}
	completed := waitForState(t, manager, created.ID, StateCompleted)
	if completed.RowCount != uint64(len(wantRows)) || completed.ResultsTruncated ||
		completed.Schema == nil || !reflect.DeepEqual(*completed.Schema, schema) {
		t.Fatalf(
			"completed streamstats maximum result = rows %d truncated %v schema %#v",
			completed.RowCount,
			completed.ResultsTruncated,
			completed.Schema,
		)
	}

	page, err := manager.Results(created.ID, PageRequest{Limit: len(wantRows)})
	if err != nil {
		t.Fatalf("Results(streamstats maximum): %v", err)
	}
	if page.TotalRows != uint64(len(wantRows)) || len(page.Rows) != len(wantRows) ||
		!page.Complete || !reflect.DeepEqual(page.Schema, schema) {
		t.Fatalf("streamstats maximum page = %#v", page)
	}
	wantEventIDs := []string{
		"event-1", "event-2", "event-3", "event-4", "event-5", "event-6", "event-7",
	}
	wantServices := []string{"api", "worker", "", "api", "worker", "api", "api"}
	for index, row := range page.Rows {
		if row.Ordinal != uint64(index) || len(row.Values) != 4 {
			t.Fatalf("streamstats maximum row %d = %#v", index, row)
		}
		if got, ok := row.Values[0].String(); !ok || got != wantEventIDs[index] {
			t.Fatalf("streamstats maximum event_id at row %d = %q/%v", index, got, ok)
		}
		if index == 2 {
			if !row.Values[1].IsNull() {
				t.Fatalf("streamstats maximum service at row %d = %#v, want null", index, row.Values[1])
			}
		} else if got, ok := row.Values[1].String(); !ok || got != wantServices[index] {
			t.Fatalf("streamstats maximum service at row %d = %q/%v", index, got, ok)
		}
	}
	for index := 0; index < 3; index++ {
		if !page.Rows[index].Values[2].IsNull() || !page.Rows[index].Values[3].IsNull() {
			t.Fatalf("empty maximum frame at row %d = %#v", index, page.Rows[index])
		}
	}
	if maximum, ok := page.Rows[3].Values[2].Double(); !ok || maximum != 9.25 {
		t.Fatalf("numeric maximum = %v/%v, want Double(9.25)", maximum, ok)
	}
	if maximum, ok := page.Rows[3].Values[3].Double(); !ok || maximum != 9.25 {
		t.Fatalf("stacked numeric maximum = %v/%v, want Double(9.25)", maximum, ok)
	}
	if maximum, ok := page.Rows[4].Values[2].String(); !ok || maximum != "zulu" {
		t.Fatalf("lexical maximum = %q/%v, want String(zulu)", maximum, ok)
	}
	if maximum, ok := page.Rows[4].Values[3].String(); !ok || maximum != "zulu" {
		t.Fatalf("stacked lexical maximum = %q/%v, want String(zulu)", maximum, ok)
	}
	if maximum, ok := page.Rows[5].Values[2].Decimal(); !ok || maximum != "999999999999999999990" {
		t.Fatalf("wide replacement maximum = %q/%v", maximum, ok)
	}
	if maximum, ok := page.Rows[5].Values[3].String(); !ok || maximum != "zulu" {
		t.Fatalf("lexical maximum should dominate later numeric value = %q/%v", maximum, ok)
	}
	if maximum, ok := page.Rows[6].Values[2].Bytes(); !ok || !slices.Equal(maximum, []byte{0xff, 0x00}) {
		t.Fatalf("binary replacement maximum = %x/%v, want Bytes(ff00)", maximum, ok)
	}
	if maximum, ok := page.Rows[6].Values[3].Bytes(); !ok || !slices.Equal(maximum, []byte{0xff, 0x00}) {
		t.Fatalf("stacked raw-byte lexical maximum = %x/%v, want Bytes(ff00)", maximum, ok)
	}
}
