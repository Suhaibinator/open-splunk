package searchinspection

import (
	"context"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/searchsnapshot"
)

func TestProjectLogicalPlanProjectsEventStatsSumInputsAndOutput(t *testing.T) {
	t.Parallel()

	snapshot := validInspectionSnapshot()
	snapshot.SPL = "index=" + snapshot.EffectiveIndexes[0] +
		" | table _time,host,http.bytes" +
		" | eventstats sum(http.bytes) AS total_bytes BY host"
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
		!slices.Equal(stage.InputFields, []string{"host", "http.bytes"}) ||
		!slices.Equal(stage.OutputFields, []string{"total_bytes"}) {
		t.Fatalf("eventstats sum stage = %#v", stage)
	}
	if !slices.Equal(
		projected.ReferencedFields,
		[]string{"_time", "host", "http.bytes", "index"},
	) || projected.Output.Kind != OutputKindStatic ||
		!slices.Equal(
			projected.Output.Fields,
			[]string{"_time", "host", "http.bytes", "total_bytes"},
		) || projected.Output.MaxDynamicFields != 0 {
		t.Fatalf("eventstats sum projection = %#v", projected)
	}
}
