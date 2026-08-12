package searchjobs

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestValidateTimechartSchemaEnforcesFixedPercentileContract(t *testing.T) {
	t.Parallel()

	output := clickhouse.TimechartOutput{
		Mode:       clickhouse.TimechartModeFixedValue,
		MaxSeries:  1,
		ValueField: "p95_ms",
		ValueKind:  clickhouse.TimechartValueKindPercentile,
	}
	valid := Schema{Columns: []Column{
		{Name: "_time", Kind: ValueKindTime},
		{Name: "p95_ms", Kind: ValueKindDouble, Nullable: true},
	}}
	if err := ValidateTimechartSchema(
		valid,
		[]string{"_time", "p95_ms"},
		output,
	); err != nil {
		t.Fatalf("valid fixed percentile timechart schema: %v", err)
	}
	longValueField := strings.Repeat("a", 200) + "." + strings.Repeat("b", 200)
	longOutput := output
	longOutput.ValueField = longValueField
	longSchema := Schema{Columns: []Column{
		valid.Columns[0],
		{Name: longValueField, Kind: ValueKindDouble, Nullable: true},
	}}
	if err := ValidateTimechartSchema(
		longSchema,
		[]string{"_time", longValueField},
		longOutput,
	); err != nil {
		t.Fatalf("valid long dotted fixed percentile schema: %v", err)
	}

	for _, test := range []struct {
		name     string
		schema   Schema
		expected []string
		output   clickhouse.TimechartOutput
	}{
		{name: "missing value", schema: Schema{Columns: valid.Columns[:1]}, expected: []string{"_time", "p95_ms"}, output: output},
		{name: "extra column", schema: Schema{Columns: append(append([]Column(nil), valid.Columns...), Column{Name: "extra", Kind: ValueKindDouble, Nullable: true})}, expected: []string{"_time", "p95_ms"}, output: output},
		{name: "wrong compiler fields", schema: valid, expected: []string{"_time", "other"}, output: output},
		{name: "wrong declared value field", schema: valid, expected: []string{"_time", "p95_ms"}, output: func() clickhouse.TimechartOutput { got := output; got.ValueField = "other"; return got }()},
		{name: "empty declared value field", schema: valid, expected: []string{"_time", "p95_ms"}, output: func() clickhouse.TimechartOutput { got := output; got.ValueField = ""; return got }()},
		{name: "time collision", schema: valid, expected: []string{"_time", "p95_ms"}, output: func() clickhouse.TimechartOutput { got := output; got.ValueField = "_time"; return got }()},
		{name: "wrong series bound", schema: valid, expected: []string{"_time", "p95_ms"}, output: func() clickhouse.TimechartOutput { got := output; got.MaxSeries = 2; return got }()},
		{name: "dynamic label bound", schema: valid, expected: []string{"_time", "p95_ms"}, output: func() clickhouse.TimechartOutput { got := output; got.MaxLabelBytes = 1; return got }()},
		{name: "invalid utf8 value field", schema: Schema{Columns: []Column{valid.Columns[0], {Name: string([]byte{0xff}), Kind: ValueKindDouble, Nullable: true}}}, expected: []string{"_time", string([]byte{0xff})}, output: func() clickhouse.TimechartOutput { got := output; got.ValueField = string([]byte{0xff}); return got }()},
		{name: "private value field", schema: Schema{Columns: []Column{valid.Columns[0], {Name: "__OS_private", Kind: ValueKindDouble, Nullable: true}}}, expected: []string{"_time", "__OS_private"}, output: func() clickhouse.TimechartOutput { got := output; got.ValueField = "__OS_private"; return got }()},
		{name: "oversized value field", schema: Schema{Columns: []Column{valid.Columns[0], {Name: strings.Repeat("x", 257), Kind: ValueKindDouble, Nullable: true}}}, expected: []string{"_time", strings.Repeat("x", 257)}, output: func() clickhouse.TimechartOutput {
			got := output
			got.ValueField = strings.Repeat("x", 257)
			return got
		}()},
		{name: "wrong time name", schema: Schema{Columns: []Column{{Name: "time", Kind: ValueKindTime}, valid.Columns[1]}}, expected: []string{"_time", "p95_ms"}, output: output},
		{name: "nullable time", schema: Schema{Columns: []Column{{Name: "_time", Kind: ValueKindTime, Nullable: true}, valid.Columns[1]}}, expected: []string{"_time", "p95_ms"}, output: output},
		{name: "multivalue time", schema: Schema{Columns: []Column{{Name: "_time", Kind: ValueKindTime, Multivalue: true}, valid.Columns[1]}}, expected: []string{"_time", "p95_ms"}, output: output},
		{name: "wrong value name", schema: Schema{Columns: []Column{valid.Columns[0], {Name: "other", Kind: ValueKindDouble, Nullable: true}}}, expected: []string{"_time", "p95_ms"}, output: output},
		{name: "wrong value kind", schema: Schema{Columns: []Column{valid.Columns[0], {Name: "p95_ms", Kind: ValueKindSigned, Nullable: true}}}, expected: []string{"_time", "p95_ms"}, output: output},
		{name: "nonnullable value", schema: Schema{Columns: []Column{valid.Columns[0], {Name: "p95_ms", Kind: ValueKindDouble}}}, expected: []string{"_time", "p95_ms"}, output: output},
		{name: "multivalue value", schema: Schema{Columns: []Column{valid.Columns[0], {Name: "p95_ms", Kind: ValueKindDouble, Nullable: true, Multivalue: true}}}, expected: []string{"_time", "p95_ms"}, output: output},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if err := ValidateTimechartSchema(
				test.schema,
				test.expected,
				test.output,
			); !errors.Is(err, ErrInvalidResult) {
				t.Fatalf("ValidateTimechartSchema() = %v, want ErrInvalidResult", err)
			}
		})
	}
}

func TestManagerDetachesFixedPercentileTimechartMetadataFromExecutor(t *testing.T) {
	t.Parallel()

	schema := Schema{Columns: []Column{
		{Name: "_time", Kind: ValueKindTime},
		{Name: "p95_ms", Kind: ValueKindDouble, Nullable: true},
	}}
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(
			_ context.Context,
			query clickhouse.CompiledQuery,
			sink ResultSink,
		) error {
			if query.Timechart == nil ||
				query.Timechart.Mode != clickhouse.TimechartModeFixedValue ||
				query.Timechart.ValueKind != clickhouse.TimechartValueKindPercentile ||
				query.Timechart.ValueField != "p95_ms" {
				t.Fatalf("compiled fixed percentile timechart = %#v", query.Timechart)
			}
			query.Timechart.ValueField = "mutated"
			return sink.SetSchema(schema)
		}),
		CleanupInterval: -1,
		NewID:           sequenceIDs("fixed-percentile-timechart-detachment"),
	})
	created, err := manager.Create(
		context.Background(),
		withSPL(
			validRequest(),
			"index=main | timechart span=5m p95(duration) AS p95_ms",
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
