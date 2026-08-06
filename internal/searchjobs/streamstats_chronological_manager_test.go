package searchjobs

import (
	"context"
	"reflect"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestManagerPublishesReplacingAndStackedStreamStatsChronologicalResults(t *testing.T) {
	t.Parallel()

	schema := Schema{Columns: []Column{
		{Name: "event_id", Kind: ValueKindString},
		{Name: "service", Kind: ValueKindString, Nullable: true},
		{Name: "status", Kind: ValueKindMixed, Nullable: true},
		{Name: "last_seen", Kind: ValueKindMixed, Nullable: true},
	}}
	wantRows := [][]Value{
		{StringValue("event-1"), StringValue("api"), StringValue("alpha"), StringValue("zulu")},
		{StringValue("event-2"), StringValue("worker"), BytesValue([]byte{0xff, 0x00}), StringValue("zulu")},
		{StringValue("event-3"), NullValue(), NullValue(), NullValue()},
	}
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(
			_ context.Context,
			query clickhouse.CompiledQuery,
			sink ResultSink,
		) error {
			if !slices.Equal(
				query.OutputFields,
				[]string{"event_id", "service", "status", "last_seen"},
			) {
				t.Fatalf("compiled streamstats chronological fields = %v", query.OutputFields)
			}
			if query.Timechart != nil || query.Chart != nil || query.SparseFields {
				t.Fatalf("streamstats chronological declared a non-tabular transport: %#v", query)
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
		NewID:           sequenceIDs("streamstats-chronological-row-preserving"),
	})
	created, err := manager.Create(context.Background(), withSPL(
		validRequest(),
		"index=main | table _time,event_id,service,status | streamstats current=f window=2 global=f earliest(status) AS status BY service | streamstats latest(status) AS last_seen | table event_id,service,status,last_seen",
	))
	if err != nil {
		t.Fatalf("Create(streamstats chronological): %v", err)
	}
	completed := waitForState(t, manager, created.ID, StateCompleted)
	if completed.RowCount != uint64(len(wantRows)) || completed.ResultsTruncated ||
		completed.Schema == nil || !reflect.DeepEqual(*completed.Schema, schema) {
		t.Fatalf(
			"completed streamstats chronological result = rows %d truncated %v schema %#v",
			completed.RowCount,
			completed.ResultsTruncated,
			completed.Schema,
		)
	}

	page, err := manager.Results(created.ID, PageRequest{Limit: 10})
	if err != nil {
		t.Fatalf("Results(streamstats chronological): %v", err)
	}
	if page.TotalRows != uint64(len(wantRows)) ||
		!reflect.DeepEqual(page.Schema, schema) ||
		!page.Complete || page.NextCursor != "" || len(page.Rows) != len(wantRows) {
		t.Fatalf("streamstats chronological page = %#v", page)
	}
	for index, row := range page.Rows {
		if row.Ordinal != uint64(index) || len(row.Values) != 4 {
			t.Fatalf("streamstats chronological row %d = %#v", index, row)
		}
	}
	if eventID, ok := page.Rows[0].Values[0].String(); !ok || eventID != "event-1" {
		t.Fatalf("first streamstats chronological event ID = %q/%v", eventID, ok)
	}
	if service, ok := page.Rows[0].Values[1].String(); !ok || service != "api" {
		t.Fatalf("first streamstats chronological service = %q/%v", service, ok)
	}
	if first, ok := page.Rows[0].Values[2].String(); !ok || first != "alpha" {
		t.Fatalf("first streamstats chronological earliest = %q/%v", first, ok)
	}
	if last, ok := page.Rows[0].Values[3].String(); !ok || last != "zulu" {
		t.Fatalf("first streamstats chronological latest = %q/%v", last, ok)
	}
	if value, ok := page.Rows[1].Values[2].Bytes(); !ok || !slices.Equal(value, []byte{0xff, 0x00}) {
		t.Fatalf("second streamstats chronological earliest = %x/%v", value, ok)
	}
	if last, ok := page.Rows[1].Values[3].String(); !ok || last != "zulu" {
		t.Fatalf("second streamstats chronological latest = %q/%v", last, ok)
	}
	if eventID, ok := page.Rows[2].Values[0].String(); !ok || eventID != "event-3" ||
		!page.Rows[2].Values[1].IsNull() ||
		!page.Rows[2].Values[2].IsNull() ||
		!page.Rows[2].Values[3].IsNull() {
		t.Fatalf("null streamstats chronological row = %#v", page.Rows[2])
	}
}
