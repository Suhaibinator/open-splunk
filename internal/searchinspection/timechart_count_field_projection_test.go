package searchinspection

import (
	"context"
	"slices"
	"testing"
)

func TestProjectLogicalPlanProjectsFixedTimechartCountFieldSchemas(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		suffix string
		output string
	}{
		{
			name:   "canonical output",
			suffix: " | timechart span=5m count(http.status)",
			output: "count(http.status)",
		},
		{
			name:   "aliased output",
			suffix: " | timechart span=5m count(http.status) AS populated",
			output: "populated",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			snapshot := validInspectionSnapshot()
			snapshot.SPL = "index=" + snapshot.EffectiveIndexes[0] + test.suffix
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
			if stage.Operator != "Timechart" ||
				!slices.Equal(stage.InputFields, []string{"_time", "http.status"}) ||
				!slices.Equal(stage.OutputFields, []string{"_time", test.output}) {
				t.Fatalf("timechart stage = %#v", stage)
			}
			if !slices.Equal(
				projected.ReferencedFields,
				[]string{"_time", "http.status", "index"},
			) || projected.Output.Kind != OutputKindStatic ||
				!slices.Equal(projected.Output.Fields, []string{"_time", test.output}) ||
				projected.Output.MaxDynamicFields != 0 {
				t.Fatalf("projection = %#v", projected)
			}
		})
	}
}

func TestProjectLogicalPlanProjectsSplitTimechartCountFieldSchema(t *testing.T) {
	t.Parallel()

	snapshot := validInspectionSnapshot()
	snapshot.SPL = "index=" + snapshot.EffectiveIndexes[0] +
		" | timechart span=5m count(http.status) AS ignored BY Service"
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
	if stage.Operator != "Timechart" ||
		!slices.Equal(stage.InputFields, []string{"Service", "_time", "http.status"}) ||
		!slices.Equal(stage.OutputFields, []string{"_time"}) {
		t.Fatalf("timechart stage = %#v", stage)
	}
	if !slices.Equal(
		projected.ReferencedFields,
		[]string{"Service", "_time", "http.status", "index"},
	) || projected.Output.Kind != OutputKindDynamic ||
		!slices.Equal(projected.Output.Fields, []string{"_time"}) ||
		projected.Output.MaxDynamicFields != 12 {
		t.Fatalf("projection = %#v", projected)
	}
}
