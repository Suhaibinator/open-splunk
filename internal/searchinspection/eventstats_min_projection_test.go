package searchinspection

import (
	"context"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/searchsnapshot"
)

func TestProjectLogicalPlanProjectsEventStatsMinimumInputAndOutput(t *testing.T) {
	t.Parallel()

	snapshot := validInspectionSnapshot()
	snapshot.SPL = "index=" + snapshot.EffectiveIndexes[0] +
		" | table _time,host,http.latency" +
		" | eventstats min(http.latency) AS minimum_latency BY host"
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
		!slices.Equal(stage.OutputFields, []string{"minimum_latency"}) {
		t.Fatalf("eventstats minimum stage = %#v", stage)
	}
	if !slices.Equal(
		projected.ReferencedFields,
		[]string{"_time", "host", "http.latency", "index"},
	) || projected.Output.Kind != OutputKindStatic ||
		!slices.Equal(
			projected.Output.Fields,
			[]string{"_time", "host", "http.latency", "minimum_latency"},
		) || projected.Output.MaxDynamicFields != 0 {
		t.Fatalf("eventstats minimum projection = %#v", projected)
	}
}
