package searchjobs

import (
	"context"
	"fmt"
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

func TestManagerRetainsRowPreservingStreamStatsSumSchemaAndResults(t *testing.T) {
	t.Parallel()

	schema := Schema{Columns: []Column{
		{Name: "event_id", Kind: ValueKindString},
		{Name: "service", Kind: ValueKindString, Nullable: true},
		{Name: "prior_total", Kind: ValueKindDouble, Nullable: true},
	}}
	rows := [][]Value{
		{StringValue("event-1"), StringValue("api"), NullValue()},
		{StringValue("event-2"), StringValue("worker"), NullValue()},
		{StringValue("event-3"), NullValue(), NullValue()},
		{StringValue("event-4"), StringValue("api"), DoubleValue(2.5)},
	}
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(
			_ context.Context,
			query clickhouse.CompiledQuery,
			sink ResultSink,
		) error {
			if !slices.Equal(query.OutputFields, []string{"event_id", "service", "prior_total"}) {
				t.Fatalf("compiled streamstats sum fields = %v", query.OutputFields)
			}
			if query.Timechart != nil || query.Chart != nil || query.SparseFields {
				t.Fatalf("streamstats sum declared a non-tabular transport: %#v", query)
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
		NewID:           sequenceIDs("streamstats-sum-row-preserving"),
	})
	created, err := manager.Create(context.Background(), withSPL(
		validRequest(),
		"index=main | table event_id,service,status | streamstats current=f window=2 global=f sum(status) AS prior_total BY service | table event_id,service,prior_total",
	))
	if err != nil {
		t.Fatalf("Create(streamstats sum): %v", err)
	}
	completed := waitForState(t, manager, created.ID, StateCompleted)
	if completed.RowCount != uint64(len(rows)) || completed.ResultsTruncated ||
		completed.Schema == nil || !reflect.DeepEqual(*completed.Schema, schema) {
		t.Fatalf(
			"completed streamstats sum result = rows %d truncated %v schema %#v",
			completed.RowCount,
			completed.ResultsTruncated,
			completed.Schema,
		)
	}

	page, err := manager.Results(created.ID, PageRequest{Limit: len(rows)})
	if err != nil {
		t.Fatalf("Results(streamstats sum): %v", err)
	}
	if page.TotalRows != uint64(len(rows)) || len(page.Rows) != len(rows) || !page.Complete ||
		!reflect.DeepEqual(page.Schema, schema) {
		t.Fatalf("streamstats sum page = %#v", page)
	}
	wantEventIDs := []string{"event-1", "event-2", "event-3", "event-4"}
	wantServices := []string{"api", "worker", "", "api"}
	for index, row := range page.Rows {
		if row.Ordinal != uint64(index) || len(row.Values) != 3 {
			t.Fatalf("streamstats sum row %d = %#v", index, row)
		}
		if got, ok := row.Values[0].String(); !ok || got != wantEventIDs[index] {
			t.Fatalf("streamstats sum event_id at row %d = (%q, %v)", index, got, ok)
		}
		if index == 2 {
			if !row.Values[1].IsNull() || !row.Values[2].IsNull() {
				t.Fatalf("missing BY row = %#v, want retained null group and sum", row)
			}
			continue
		}
		if got, ok := row.Values[1].String(); !ok || got != wantServices[index] {
			t.Fatalf("streamstats sum service at row %d = (%q, %v)", index, got, ok)
		}
		if index < 3 {
			if !row.Values[2].IsNull() {
				t.Fatalf("streamstats sum prior total at row %d = %#v, want null", index, row.Values[2])
			}
			continue
		}
		if got, ok := row.Values[2].Double(); !ok || got != 2.5 {
			t.Fatalf("streamstats sum prior total at row %d = (%v, %v)", index, got, ok)
		}
	}
}

func TestManagerRetainsRowPreservingStreamStatsAverageSchemaAndResults(t *testing.T) {
	t.Parallel()

	schema := Schema{Columns: []Column{
		{Name: "event_id", Kind: ValueKindString},
		{Name: "service", Kind: ValueKindString, Nullable: true},
		{Name: "prior_mean", Kind: ValueKindDouble, Nullable: true},
	}}
	rows := [][]Value{
		{StringValue("event-1"), StringValue("api"), NullValue()},
		{StringValue("event-2"), NullValue(), NullValue()},
		{StringValue("event-3"), StringValue("api"), DoubleValue(2.5)},
	}
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(
			_ context.Context,
			query clickhouse.CompiledQuery,
			sink ResultSink,
		) error {
			if !slices.Equal(query.OutputFields, []string{"event_id", "service", "prior_mean"}) {
				t.Fatalf("compiled streamstats average fields = %v", query.OutputFields)
			}
			if query.Timechart != nil || query.Chart != nil || query.SparseFields {
				t.Fatalf("streamstats average declared a non-tabular transport: %#v", query)
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
		NewID:           sequenceIDs("streamstats-average-row-preserving"),
	})
	created, err := manager.Create(context.Background(), withSPL(
		validRequest(),
		"index=main | table event_id,service,status | streamstats current=f window=2 global=f avg(status) AS prior_mean BY service | table event_id,service,prior_mean",
	))
	if err != nil {
		t.Fatalf("Create(streamstats average): %v", err)
	}
	completed := waitForState(t, manager, created.ID, StateCompleted)
	if completed.RowCount != uint64(len(rows)) || completed.ResultsTruncated ||
		completed.Schema == nil || !reflect.DeepEqual(*completed.Schema, schema) {
		t.Fatalf(
			"completed streamstats average result = rows %d truncated %v schema %#v",
			completed.RowCount,
			completed.ResultsTruncated,
			completed.Schema,
		)
	}
	page, err := manager.Results(created.ID, PageRequest{Limit: len(rows)})
	if err != nil {
		t.Fatalf("Results(streamstats average): %v", err)
	}
	if page.TotalRows != uint64(len(rows)) || len(page.Rows) != len(rows) ||
		!page.Complete || !reflect.DeepEqual(page.Schema, schema) {
		t.Fatalf("streamstats average page = %#v", page)
	}
	for index, row := range page.Rows {
		if row.Ordinal != uint64(index) || len(row.Values) != 3 {
			t.Fatalf("streamstats average row %d = %#v", index, row)
		}
		wantEventID := fmt.Sprintf("event-%d", index+1)
		if got, ok := row.Values[0].String(); !ok || got != wantEventID {
			t.Fatalf("streamstats average event_id at row %d = (%q, %v)", index, got, ok)
		}
		if index == 1 {
			if !row.Values[1].IsNull() || !row.Values[2].IsNull() {
				t.Fatalf("missing BY row = %#v, want retained null group and average", row)
			}
			continue
		}
		if got, ok := row.Values[1].String(); !ok || got != "api" {
			t.Fatalf("streamstats average service at row %d = (%q, %v)", index, got, ok)
		}
		if index == 0 {
			if !row.Values[2].IsNull() {
				t.Fatalf("streamstats average prior mean at row %d = %#v, want null", index, row.Values[2])
			}
			continue
		}
		if got, ok := row.Values[2].Double(); !ok || got != 2.5 {
			t.Fatalf("streamstats average prior mean at row %d = (%v, %v)", index, got, ok)
		}
	}
}
