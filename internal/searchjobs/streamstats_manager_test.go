package searchjobs

import (
	"context"
	"reflect"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestManagerRetainsRowPreservingStreamStatsCountFieldSchemaAndResults(t *testing.T) {
	t.Parallel()

	schema := Schema{Columns: []Column{
		{Name: "event_id", Kind: ValueKindString},
		{Name: "service", Kind: ValueKindString, Nullable: true},
		{Name: "prior", Kind: ValueKindUnsigned, Nullable: true},
	}}
	rows := [][]Value{
		{StringValue("event-1"), StringValue("api"), UnsignedValue(0)},
		{StringValue("event-2"), StringValue("worker"), UnsignedValue(0)},
		{StringValue("event-3"), NullValue(), NullValue()},
		{StringValue("event-4"), StringValue("api"), UnsignedValue(1)},
	}
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(
			_ context.Context,
			query clickhouse.CompiledQuery,
			sink ResultSink,
		) error {
			if !slices.Equal(query.OutputFields, []string{"event_id", "service", "prior"}) {
				t.Fatalf("compiled streamstats fields = %v", query.OutputFields)
			}
			if query.Timechart != nil || query.Chart != nil || query.SparseFields {
				t.Fatalf("streamstats declared a non-tabular transport: %#v", query)
			}
			if err := sink.SetSchema(schema); err != nil {
				return err
			}
			for _, row := range rows {
				if err := sink.AddRow(row); err != nil {
					return err
				}
			}
			return nil
		}),
		CleanupInterval: -1,
		NewID:           sequenceIDs("streamstats-row-preserving"),
	})
	created, err := manager.Create(context.Background(), withSPL(
		validRequest(),
		"index=main | table event_id,service | streamstats current=f window=2 global=f count(event_id) AS prior BY service",
	))
	if err != nil {
		t.Fatalf("Create(streamstats): %v", err)
	}
	completed := waitForState(t, manager, created.ID, StateCompleted)
	if completed.RowCount != uint64(len(rows)) || completed.ResultsTruncated ||
		completed.Schema == nil || !reflect.DeepEqual(*completed.Schema, schema) {
		t.Fatalf("completed streamstats result = rows %d truncated %v schema %#v", completed.RowCount, completed.ResultsTruncated, completed.Schema)
	}

	page, err := manager.Results(created.ID, PageRequest{Limit: len(rows)})
	if err != nil {
		t.Fatalf("Results(streamstats): %v", err)
	}
	if page.TotalRows != uint64(len(rows)) || len(page.Rows) != len(rows) || !page.Complete ||
		!reflect.DeepEqual(page.Schema, schema) {
		t.Fatalf("streamstats page = %#v", page)
	}
	wantEventIDs := []string{"event-1", "event-2", "event-3", "event-4"}
	wantServices := []string{"api", "worker", "", "api"}
	wantPrior := []uint64{0, 0, 0, 1}
	for index, row := range page.Rows {
		if row.Ordinal != uint64(index) || len(row.Values) != 3 {
			t.Fatalf("streamstats row %d = %#v", index, row)
		}
		if got, ok := row.Values[0].String(); !ok || got != wantEventIDs[index] {
			t.Fatalf("streamstats event_id at row %d = (%q, %v)", index, got, ok)
		}
		if index == 2 {
			if !row.Values[1].IsNull() || !row.Values[2].IsNull() {
				t.Fatalf("missing BY row = %#v, want retained null group and count", row)
			}
			continue
		}
		if got, ok := row.Values[1].String(); !ok || got != wantServices[index] {
			t.Fatalf("streamstats service at row %d = (%q, %v)", index, got, ok)
		}
		if got, ok := row.Values[2].Unsigned(); !ok || got != wantPrior[index] {
			t.Fatalf("streamstats prior count at row %d = (%d, %v)", index, got, ok)
		}
	}
}
