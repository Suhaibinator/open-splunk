package searchinspection

import (
	"context"
	"slices"
	"testing"
)

func TestProjectLogicalPlanProjectsEventStatsNumericAggregateInputsAndOutput(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		function string
		input    string
		output   string
	}{
		{"sum", "sum", "http.bytes", "total_bytes"},
		{"average", "avg", "http.duration_ms", "mean_ms"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			snapshot := validInspectionSnapshot()
			snapshot.SPL = "index=" + snapshot.EffectiveIndexes[0] +
				" | table _time,host," + test.input +
				" | eventstats " + test.function + "(" + test.input + ") AS " + test.output + " BY host"
			logical, err := buildInspectionAuthoredPlan(snapshot)
			if err != nil {
				t.Fatalf("BuildExecutionPlan: %v", err)
			}
			projected, err := projectLogicalPlan(
				context.Background(),
				logical,
				snapshot.SPL,
			)
			if err != nil {
				t.Fatalf("projectLogicalPlan: %v", err)
			}

			stage := projected.Stages[len(projected.Stages)-1]
			if stage.Operator != "EventAggregate" ||
				!slices.Equal(stage.InputFields, []string{"host", test.input}) ||
				!slices.Equal(stage.OutputFields, []string{test.output}) {
				t.Fatalf("eventstats %s stage = %#v", test.name, stage)
			}
			if !slices.Equal(
				projected.ReferencedFields,
				[]string{"_time", "host", test.input, "index"},
			) || projected.Output.Kind != OutputKindStatic ||
				!slices.Equal(
					projected.Output.Fields,
					[]string{"_time", "host", test.input, test.output},
				) || projected.Output.MaxDynamicFields != 0 {
				t.Fatalf("eventstats %s projection = %#v", test.name, projected)
			}
		})
	}
}
