package searchinspection

import (
	"context"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/searchsnapshot"
)

func TestProjectLogicalPlanProjectsStaticTimechartPercentile(t *testing.T) {
	t.Parallel()

	snapshot := validInspectionSnapshot()
	snapshot.SPL = "index=" + snapshot.EffectiveIndexes[0] +
		" | timechart span=5m p95(http.duration) AS latency_p95"
	logical, err := searchsnapshot.BuildExecutionPlan(snapshot)
	if err != nil {
		t.Fatalf("BuildExecutionPlan: %v", err)
	}
	projected, err := projectLogicalPlan(context.Background(), logical, snapshot.SPL)
	if err != nil {
		t.Fatalf("projectLogicalPlan: %v", err)
	}
	stage := projected.Stages[len(projected.Stages)-1]
	if stage.Operator != "Timechart" ||
		!slices.Equal(stage.InputFields, []string{"_time", "http.duration"}) ||
		!slices.Equal(stage.OutputFields, []string{"_time", "latency_p95"}) {
		t.Fatalf("timechart stage = %#v", stage)
	}
	if !slices.Equal(projected.ReferencedFields, []string{"_time", "http.duration", "index"}) ||
		projected.Output.Kind != OutputKindStatic ||
		!slices.Equal(projected.Output.Fields, []string{"_time", "latency_p95"}) ||
		projected.Output.MaxDynamicFields != 0 {
		t.Fatalf("projection = %#v", projected)
	}
}
