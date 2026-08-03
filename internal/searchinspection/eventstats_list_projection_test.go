package searchinspection

import (
	"context"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/searchsnapshot"
)

func TestProjectLogicalPlanProjectsEventStatsListInputsAndFixedOutput(t *testing.T) {
	t.Parallel()

	snapshot := validInspectionSnapshot()
	snapshot.SPL = "index=" + snapshot.EffectiveIndexes[0] +
		" | table _time,host,user" +
		" | eventstats list(user) AS recent_users BY host"
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
		!slices.Equal(stage.InputFields, []string{"host", "user"}) ||
		!slices.Equal(stage.OutputFields, []string{"recent_users"}) {
		t.Fatalf("eventstats list stage = %#v", stage)
	}
	if !slices.Equal(
		projected.ReferencedFields,
		[]string{"_time", "host", "index", "user"},
	) || projected.Output.Kind != OutputKindStatic ||
		!slices.Equal(
			projected.Output.Fields,
			[]string{"_time", "host", "user", "recent_users"},
		) || projected.Output.MaxDynamicFields != 0 {
		t.Fatalf("eventstats list projection = %#v", projected)
	}
}
