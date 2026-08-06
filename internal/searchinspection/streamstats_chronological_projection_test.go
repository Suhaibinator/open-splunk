package searchinspection

import (
	"context"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestProjectLogicalPlanDescribesReplacingAndStackedChronologicalStreamAggregates(t *testing.T) {
	t.Parallel()

	const source = "index=main | table _time,event_id,user,status | streamstats current=f window=2 global=f earliest(status) AS status BY user | streamstats latest(status) AS last_seen"
	sourceRange := testSourceRange()
	user, err := plan.ResolveField("user", sourceRange)
	if err != nil {
		t.Fatalf("ResolveField(user): %v", err)
	}
	status, err := plan.ResolveField("status", sourceRange)
	if err != nil {
		t.Fatalf("ResolveField(status): %v", err)
	}
	logical := &plan.Query{
		OutputFields: []string{"event_id", "user", "status", "last_seen"},
		Operators: []plan.Operator{
			&plan.Scan{Range: sourceRange},
			&plan.StreamAggregate{
				GroupBy: []plan.FieldRef{user},
				Measure: plan.AggregateMeasure{
					Function: plan.AggregateFunctionEarliest,
					Input:    status,
					Output:   "status",
				},
				IncludeCurrent: false,
				WindowRows:     2,
				Global:         false,
				Range:          sourceRange,
			},
			&plan.StreamAggregate{
				Measure: plan.AggregateMeasure{
					Function: plan.AggregateFunctionLatest,
					Input:    status,
					Output:   "last_seen",
				},
				IncludeCurrent: true,
				Global:         true,
				Range:          sourceRange,
			},
		},
	}

	projected, err := projectLogicalPlan(context.Background(), logical, source)
	if err != nil {
		t.Fatalf("projectLogicalPlan(stacked streamstats chronological): %v", err)
	}
	if len(projected.Stages) != 3 {
		t.Fatalf("stacked chronological stages = %#v", projected.Stages)
	}
	replacement := projected.Stages[1]
	if replacement.Operator != "StreamAggregate" ||
		!slices.Equal(replacement.InputFields, []string{"_time", "status", "user"}) ||
		!slices.Equal(replacement.OutputFields, []string{"status"}) {
		t.Fatalf("replacement earliest stream aggregate stage = %#v", replacement)
	}
	stacked := projected.Stages[2]
	if stacked.Operator != "StreamAggregate" ||
		!slices.Equal(stacked.InputFields, []string{"_time", "status"}) ||
		!slices.Equal(stacked.OutputFields, []string{"last_seen"}) {
		t.Fatalf("stacked latest stream aggregate stage = %#v", stacked)
	}
	if !slices.Equal(projected.ReferencedFields, []string{"_time", "status", "user"}) {
		t.Fatalf(
			"stacked chronological referenced fields = %v, want immutable time, de-duplicated measure, and BY fields",
			projected.ReferencedFields,
		)
	}
	if projected.Output.Kind != OutputKindStatic ||
		!slices.Equal(
			projected.Output.Fields,
			[]string{"event_id", "user", "status", "last_seen"},
		) || projected.Output.MaxDynamicFields != 0 {
		t.Fatalf(
			"stacked chronological stream aggregate output = %#v, want row-preserving static schema",
			projected.Output,
		)
	}
}
