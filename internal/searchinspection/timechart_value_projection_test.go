package searchinspection

import (
	"context"
	"slices"
	"testing"
)

func TestProjectLogicalPlanProjectsStaticTimechartSumAndAverage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		spl    string
		input  string
		output string
	}{
		{
			name:   "sum",
			spl:    " | timechart span=5m sum(bytes) AS total_bytes",
			input:  "bytes",
			output: "total_bytes",
		},
		{
			name:   "average",
			spl:    " | timechart span=5m avg(http.duration) AS mean_latency",
			input:  "http.duration",
			output: "mean_latency",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			snapshot := validInspectionSnapshot()
			snapshot.SPL = "index=" + snapshot.EffectiveIndexes[0] + test.spl
			logical, err := buildInspectionAuthoredPlan(snapshot)
			if err != nil {
				t.Fatalf("BuildExecutionPlan: %v", err)
			}
			projected, err := projectLogicalPlan(context.Background(), logical, snapshot.SPL)
			if err != nil {
				t.Fatalf("projectLogicalPlan: %v", err)
			}
			stage := projected.Stages[len(projected.Stages)-1]
			if stage.Operator != "Timechart" ||
				!slices.Equal(stage.InputFields, []string{"_time", test.input}) ||
				!slices.Equal(stage.OutputFields, []string{"_time", test.output}) {
				t.Fatalf("timechart stage = %#v", stage)
			}
			if !slices.Equal(
				projected.ReferencedFields,
				[]string{"_time", test.input, "index"},
			) || projected.Output.Kind != OutputKindStatic ||
				!slices.Equal(projected.Output.Fields, []string{"_time", test.output}) ||
				projected.Output.MaxDynamicFields != 0 {
				t.Fatalf("projection = %#v", projected)
			}
		})
	}
}
