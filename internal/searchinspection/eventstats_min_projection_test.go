package searchinspection

import (
	"context"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/searchsnapshot"
)

func TestProjectLogicalPlanProjectsEventStatsFieldAggregateInputAndOutput(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name        string
		function    string
		input       string
		output      string
		stageInputs []string
	}{
		{
			name: "minimum", function: "min", input: "http.latency",
			output: "minimum_latency", stageInputs: []string{"host", "http.latency"},
		},
		{
			name: "maximum", function: "max", input: "http.latency",
			output: "maximum_latency", stageInputs: []string{"host", "http.latency"},
		},
		{
			name: "earliest", function: "earliest", input: "http.status",
			output: "first_status", stageInputs: []string{"_time", "host", "http.status"},
		},
		{
			name: "latest", function: "latest", input: "http.status",
			output: "last_status", stageInputs: []string{"_time", "host", "http.status"},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			snapshot := validInspectionSnapshot()
			snapshot.SPL = "index=" + snapshot.EffectiveIndexes[0] +
				" | table _time,host," + test.input +
				" | eventstats " + test.function +
				"(" + test.input + ") AS " + test.output + " BY host"
			logical, err := searchsnapshot.BuildExecutionPlan(snapshot)
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
				!slices.Equal(stage.InputFields, test.stageInputs) ||
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
