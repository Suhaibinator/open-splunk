package searchinspection

import (
	"context"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/searchsnapshot"
)

func TestProjectLogicalPlanProjectsEventStatsDistinctCountInputsAndOutput(
	t *testing.T,
) {
	t.Parallel()

	snapshot := validInspectionSnapshot()
	snapshot.SPL = "index=" + snapshot.EffectiveIndexes[0] +
		" | table _time,host,user" +
		" | eventstats dc(user) AS unique_users BY host"
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
		!slices.Equal(stage.OutputFields, []string{"unique_users"}) {
		t.Fatalf("eventstats dc stage = %#v", stage)
	}
	if !slices.Equal(
		projected.ReferencedFields,
		[]string{"_time", "host", "index", "user"},
	) || projected.Output.Kind != OutputKindStatic ||
		!slices.Equal(
			projected.Output.Fields,
			[]string{"_time", "host", "user", "unique_users"},
		) || projected.Output.MaxDynamicFields != 0 {
		t.Fatalf("eventstats dc projection = %#v", projected)
	}
}
