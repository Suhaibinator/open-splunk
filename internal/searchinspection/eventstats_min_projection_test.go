package searchinspection

import (
	"context"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/searchsnapshot"
)

func TestProjectLogicalPlanProjectsEventStatsExtremaInputAndOutput(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		function string
		output   string
	}{
		{name: "minimum", function: "min", output: "minimum_latency"},
		{name: "maximum", function: "max", output: "maximum_latency"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			snapshot := validInspectionSnapshot()
			snapshot.SPL = "index=" + snapshot.EffectiveIndexes[0] +
				" | table _time,host,http.latency" +
				" | eventstats " + test.function +
				"(http.latency) AS " + test.output + " BY host"
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
				!slices.Equal(stage.InputFields, []string{"host", "http.latency"}) ||
				!slices.Equal(stage.OutputFields, []string{test.output}) {
				t.Fatalf("eventstats %s stage = %#v", test.name, stage)
			}
			if !slices.Equal(
				projected.ReferencedFields,
				[]string{"_time", "host", "http.latency", "index"},
			) || projected.Output.Kind != OutputKindStatic ||
				!slices.Equal(
					projected.Output.Fields,
					[]string{"_time", "host", "http.latency", test.output},
				) || projected.Output.MaxDynamicFields != 0 {
				t.Fatalf("eventstats %s projection = %#v", test.name, projected)
			}
		})
	}
}
