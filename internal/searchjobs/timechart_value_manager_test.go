package searchjobs

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestValidateTimechartSchemaEnforcesFixedValueKinds(t *testing.T) {
	t.Parallel()

	schema := Schema{Columns: []Column{
		{Name: "_time", Kind: ValueKindTime},
		{Name: "metric", Kind: ValueKindDouble, Nullable: true},
	}}
	for _, kind := range []clickhouse.TimechartValueKind{
		clickhouse.TimechartValueKindSum,
		clickhouse.TimechartValueKindAverage,
	} {
		output := clickhouse.TimechartOutput{
			Mode:       clickhouse.TimechartModeFixedValue,
			MaxSeries:  1,
			ValueField: "metric",
			ValueKind:  kind,
		}
		if err := ValidateTimechartSchema(schema, []string{"_time", "metric"}, output); err != nil {
			t.Fatalf("ValidateTimechartSchema(%v): %v", kind, err)
		}
	}

	invalid := clickhouse.TimechartOutput{
		Mode:       clickhouse.TimechartModeFixedValue,
		MaxSeries:  1,
		ValueField: "metric",
		ValueKind:  clickhouse.TimechartValueKind(255),
	}
	if err := ValidateTimechartSchema(
		schema,
		[]string{"_time", "metric"},
		invalid,
	); !errors.Is(err, ErrInvalidResult) {
		t.Fatalf("invalid fixed value kind error = %v, want ErrInvalidResult", err)
	}
}

func TestManagerDetachesFixedSumAndAverageMetadataFromExecutor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		spl   string
		field string
		kind  clickhouse.TimechartValueKind
	}{
		{
			name:  "sum",
			spl:   "index=main | timechart span=5m sum(bytes) AS total_bytes",
			field: "total_bytes",
			kind:  clickhouse.TimechartValueKindSum,
		},
		{
			name:  "average",
			spl:   "index=main | timechart span=5m avg(latency) AS mean_latency",
			field: "mean_latency",
			kind:  clickhouse.TimechartValueKindAverage,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			schema := Schema{Columns: []Column{
				{Name: "_time", Kind: ValueKindTime},
				{Name: test.field, Kind: ValueKindDouble, Nullable: true},
			}}
			manager := newTestManager(t, Config{
				Executor: executorFunc(func(
					_ context.Context,
					query clickhouse.CompiledQuery,
					sink ResultSink,
				) error {
					if query.Timechart == nil ||
						query.Timechart.Mode != clickhouse.TimechartModeFixedValue ||
						query.Timechart.ValueField != test.field ||
						query.Timechart.ValueKind != test.kind {
						t.Fatalf("compiled fixed value timechart = %#v", query.Timechart)
					}
					query.Timechart.ValueField = "mutated"
					query.Timechart.ValueKind = clickhouse.TimechartValueKind(255)
					return sink.SetSchema(schema)
				}),
				CleanupInterval: -1,
				NewID:           sequenceIDs("fixed-" + test.name + "-timechart-detachment"),
			})
			created, err := manager.Create(
				context.Background(),
				withSPL(validRequest(), test.spl),
			)
			if err != nil {
				t.Fatal(err)
			}
			completed := waitForState(t, manager, created.ID, StateCompleted)
			if completed.Schema == nil || !reflect.DeepEqual(*completed.Schema, schema) {
				t.Fatalf("completed schema = %#v, want %#v", completed.Schema, schema)
			}
		})
	}
}
