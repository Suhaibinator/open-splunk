package searchjobs

import (
	"context"
	"reflect"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestManagerRetainsRowPreservingStreamStatsMinimumMixedResults(t *testing.T) {
	t.Parallel()

	schema := Schema{Columns: []Column{
		{Name: "event_id", Kind: ValueKindString},
		{Name: "service", Kind: ValueKindString, Nullable: true},
		{Name: "prior_min", Kind: ValueKindMixed, Nullable: true},
	}}
	wantRows := [][]Value{
		{StringValue("event-1"), StringValue("api"), NullValue()},
		{StringValue("event-2"), StringValue("worker"), NullValue()},
		{StringValue("event-3"), NullValue(), NullValue()},
		{StringValue("event-4"), StringValue("api"), DoubleValue(2.5)},
		{StringValue("event-5"), StringValue("worker"), StringValue("lexical")},
		{StringValue("event-6"), StringValue("api"), BytesValue([]byte{0xff, 0x00})},
	}
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(
			_ context.Context,
			query clickhouse.CompiledQuery,
			sink ResultSink,
		) error {
			if !slices.Equal(query.OutputFields, []string{"event_id", "service", "prior_min"}) {
				t.Fatalf("compiled streamstats minimum fields = %v", query.OutputFields)
			}
			if query.Timechart != nil || query.Chart != nil || query.SparseFields {
				t.Fatalf("streamstats minimum declared a non-tabular transport: %#v", query)
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
		NewID:           sequenceIDs("streamstats-minimum-row-preserving"),
	})
	created, err := manager.Create(context.Background(), withSPL(
		validRequest(),
		"index=main | table event_id,service,status | streamstats current=f window=2 global=f min(status) AS prior_min BY service | table event_id,service,prior_min",
	))
	if err != nil {
		t.Fatalf("Create(streamstats minimum): %v", err)
	}
	completed := waitForState(t, manager, created.ID, StateCompleted)
	if completed.RowCount != uint64(len(wantRows)) || completed.ResultsTruncated ||
		completed.Schema == nil || !reflect.DeepEqual(*completed.Schema, schema) {
		t.Fatalf(
			"completed streamstats minimum result = rows %d truncated %v schema %#v",
			completed.RowCount,
			completed.ResultsTruncated,
			completed.Schema,
		)
	}

	page, err := manager.Results(created.ID, PageRequest{Limit: len(wantRows)})
	if err != nil {
		t.Fatalf("Results(streamstats minimum): %v", err)
	}
	if page.TotalRows != uint64(len(wantRows)) || len(page.Rows) != len(wantRows) ||
		!page.Complete || !reflect.DeepEqual(page.Schema, schema) {
		t.Fatalf("streamstats minimum page = %#v", page)
	}
	wantEventIDs := []string{"event-1", "event-2", "event-3", "event-4", "event-5", "event-6"}
	wantServices := []string{"api", "worker", "", "api", "worker", "api"}
	for index, row := range page.Rows {
		if row.Ordinal != uint64(index) || len(row.Values) != 3 {
			t.Fatalf("streamstats minimum row %d = %#v", index, row)
		}
		if got, ok := row.Values[0].String(); !ok || got != wantEventIDs[index] {
			t.Fatalf("streamstats minimum event_id at row %d = %q/%v", index, got, ok)
		}
		if index == 2 {
			if !row.Values[1].IsNull() {
				t.Fatalf("streamstats minimum service at row %d = %#v, want null", index, row.Values[1])
			}
		} else if got, ok := row.Values[1].String(); !ok || got != wantServices[index] {
			t.Fatalf("streamstats minimum service at row %d = %q/%v", index, got, ok)
		}
	}
	if !page.Rows[2].Values[1].IsNull() || !page.Rows[2].Values[2].IsNull() {
		t.Fatalf("missing BY row = %#v, want retained row with null group and minimum", page.Rows[2])
	}
	for index := 0; index < 3; index++ {
		if !page.Rows[index].Values[2].IsNull() {
			t.Fatalf("empty prior minimum at row %d = %#v, want null", index, page.Rows[index].Values[2])
		}
	}
	if minimum, ok := page.Rows[3].Values[2].Double(); !ok || minimum != 2.5 {
		t.Fatalf("numeric minimum = %v/%v, want Double(2.5)", minimum, ok)
	}
	if minimum, ok := page.Rows[4].Values[2].String(); !ok || minimum != "lexical" {
		t.Fatalf("lexical minimum = %q/%v, want String(lexical)", minimum, ok)
	}
	if minimum, ok := page.Rows[5].Values[2].Bytes(); !ok || !slices.Equal(minimum, []byte{0xff, 0x00}) {
		t.Fatalf("binary minimum = %x/%v, want Bytes(ff00)", minimum, ok)
	}
}
