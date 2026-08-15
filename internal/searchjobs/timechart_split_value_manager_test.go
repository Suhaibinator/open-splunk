package searchjobs

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestValidateSplitNumericTimechartSchema(t *testing.T) {
	t.Parallel()

	output := clickhouse.TimechartOutput{
		Mode:          clickhouse.TimechartModeRuntimeWideValue,
		FirstBucket:   time.Unix(0, 0).UTC(),
		Span:          time.Minute,
		BucketCount:   2,
		MaxSeries:     12,
		MaxLabelBytes: 256,
		ValueKind:     clickhouse.TimechartValueKindAverage,
	}
	valid := Schema{Columns: []Column{
		{Name: "_time", Kind: ValueKindTime},
		{Name: "api", Kind: ValueKindDouble, Nullable: true},
		{Name: "NULL", Kind: ValueKindDouble, Nullable: true},
	}}
	for _, kind := range []clickhouse.TimechartValueKind{
		clickhouse.TimechartValueKindPercentile,
		clickhouse.TimechartValueKindSum,
		clickhouse.TimechartValueKindAverage,
	} {
		got := output
		got.ValueKind = kind
		if err := ValidateTimechartSchema(valid, []string{"_time"}, got); err != nil {
			t.Fatalf("ValidateTimechartSchema(valid %v): %v", kind, err)
		}
	}

	for _, test := range []struct {
		name   string
		mutate func(*Schema, *clickhouse.TimechartOutput)
	}{
		{name: "unsigned series", mutate: func(schema *Schema, _ *clickhouse.TimechartOutput) { schema.Columns[1].Kind = ValueKindUnsigned }},
		{name: "nonnullable series", mutate: func(schema *Schema, _ *clickhouse.TimechartOutput) { schema.Columns[1].Nullable = false }},
		{name: "multivalue series", mutate: func(schema *Schema, _ *clickhouse.TimechartOutput) { schema.Columns[1].Multivalue = true }},
		{name: "unset aggregate", mutate: func(_ *Schema, got *clickhouse.TimechartOutput) {
			got.ValueKind = clickhouse.TimechartValueKindInvalid
		}},
		{name: "unknown aggregate", mutate: func(_ *Schema, got *clickhouse.TimechartOutput) {
			got.ValueKind = clickhouse.TimechartValueKind(255)
		}},
		{name: "declared value field", mutate: func(_ *Schema, got *clickhouse.TimechartOutput) { got.ValueField = "ignored" }},
		{name: "zero series bound", mutate: func(schema *Schema, got *clickhouse.TimechartOutput) {
			schema.Columns = schema.Columns[:1]
			got.MaxSeries = 0
		}},
		{name: "oversized series bound", mutate: func(_ *Schema, got *clickhouse.TimechartOutput) { got.MaxSeries = 13 }},
		{name: "zero label bound", mutate: func(schema *Schema, got *clickhouse.TimechartOutput) {
			schema.Columns = schema.Columns[:1]
			got.MaxLabelBytes = 0
		}},
		{name: "oversized label bound", mutate: func(_ *Schema, got *clickhouse.TimechartOutput) { got.MaxLabelBytes = 257 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			schema := Schema{Columns: append([]Column(nil), valid.Columns...)}
			got := output
			test.mutate(&schema, &got)
			if err := ValidateTimechartSchema(schema, []string{"_time"}, got); !errors.Is(err, ErrInvalidResult) {
				t.Fatalf("ValidateTimechartSchema error = %v, want ErrInvalidResult", err)
			}
		})
	}
}

func TestSplitNumericTimechartResultSinkPreservesNullAndNonfiniteValues(t *testing.T) {
	t.Parallel()

	output := clickhouse.TimechartOutput{
		Mode:          clickhouse.TimechartModeRuntimeWideValue,
		FirstBucket:   time.Unix(0, 0).UTC(),
		Span:          time.Minute,
		BucketCount:   2,
		MaxSeries:     12,
		MaxLabelBytes: 256,
		ValueKind:     clickhouse.TimechartValueKindSum,
	}
	manager := &Manager{
		maxRows:       4,
		maxBytes:      1 << 20,
		maxTotalBytes: 1 << 20,
		maxPageBytes:  1 << 20,
	}
	entry := &jobEntry{job: Job{State: StateRunning}}
	sink := &resultSink{
		manager:        manager,
		entry:          entry,
		ctx:            context.Background(),
		expectedFields: []string{"_time"},
		timechart:      &output,
	}
	schema := Schema{Columns: []Column{
		{Name: "_time", Kind: ValueKindTime},
		{Name: "api", Kind: ValueKindDouble, Nullable: true},
		{Name: "OTHER", Kind: ValueKindDouble, Nullable: true},
	}}
	if err := sink.SetSchema(schema); err != nil {
		t.Fatalf("SetSchema: %v", err)
	}
	if err := sink.AddRow([]Value{
		TimeValue(output.FirstBucket),
		DoubleValue(math.Inf(1)),
		NullValue(),
	}); err != nil {
		t.Fatalf("AddRow(first): %v", err)
	}
	if err := sink.AddRow([]Value{
		TimeValue(output.FirstBucket.Add(output.Span)),
		DoubleValue(math.NaN()),
		DoubleValue(math.Inf(-1)),
	}); err != nil {
		t.Fatalf("AddRow(second): %v", err)
	}
	if len(entry.rows) != 2 || !entry.rows[0].Values[2].IsNull() {
		t.Fatalf("retained rows = %#v", entry.rows)
	}
	positive, positiveOK := entry.rows[0].Values[1].Double()
	notANumber, nanOK := entry.rows[1].Values[1].Double()
	negative, negativeOK := entry.rows[1].Values[2].Double()
	if !positiveOK || !math.IsInf(positive, 1) ||
		!nanOK || !math.IsNaN(notANumber) ||
		!negativeOK || !math.IsInf(negative, -1) {
		t.Fatalf("nonfinite values were not preserved: +Inf=%v/%v NaN=%v/%v -Inf=%v/%v", positive, positiveOK, notANumber, nanOK, negative, negativeOK)
	}
}

func TestManagerDetachesSplitPercentileTimechartMetadataFromExecutor(t *testing.T) {
	t.Parallel()

	schema := Schema{Columns: []Column{
		{Name: "_time", Kind: ValueKindTime},
		{Name: "api", Kind: ValueKindDouble, Nullable: true},
	}}
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(
			_ context.Context,
			query clickhouse.CompiledQuery,
			sink ResultSink,
		) error {
			if query.Timechart == nil ||
				query.Timechart.Mode != clickhouse.TimechartModeRuntimeWideValue ||
				query.Timechart.ValueKind != clickhouse.TimechartValueKindPercentile ||
				query.Timechart.ValueField != "" ||
				len(query.OutputFields) != 1 || query.OutputFields[0] != "_time" {
				t.Fatalf("compiled split percentile timechart = fields %v metadata %#v", query.OutputFields, query.Timechart)
			}
			query.Timechart.ValueKind = clickhouse.TimechartValueKind(255)
			query.Timechart.MaxSeries = 1
			return sink.SetSchema(schema)
		}),
		CleanupInterval: -1,
		NewID:           sequenceIDs("split-percentile-timechart-detachment"),
	})
	created, err := manager.Create(
		context.Background(),
		withSPL(
			validRequest(),
			"index=main | timechart span=5m p95(duration) AS ignored BY service",
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	failed := waitForState(t, manager, created.ID, StateFailed)
	if failed.Failure == nil || failed.Failure.Code != FailureInternal || failed.Schema != nil {
		t.Fatalf("mutated execution authority published result = %#v", failed)
	}
}
