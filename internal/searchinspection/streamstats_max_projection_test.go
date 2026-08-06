package searchinspection

import (
	"context"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestProjectLogicalPlanDescribesReplacingAndStackedMaximumStreamAggregates(t *testing.T) {
	t.Parallel()

	const source = "index=main | table event_id,user,status | streamstats current=f window=2 global=f max(status) AS status BY user | streamstats max(status) AS max_so_far"
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
		OutputFields: []string{"event_id", "user", "status", "max_so_far"},
		Operators: []plan.Operator{
			&plan.Scan{Range: sourceRange},
			&plan.StreamAggregate{
				GroupBy: []plan.FieldRef{user},
				Measure: plan.AggregateMeasure{
					Function: plan.AggregateFunctionMaximum,
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
					Function: plan.AggregateFunctionMaximum,
					Input:    status,
					Output:   "max_so_far",
				},
				IncludeCurrent: true,
				Global:         true,
				Range:          sourceRange,
			},
		},
	}

	projected, err := projectLogicalPlan(context.Background(), logical, source)
	if err != nil {
		t.Fatalf("projectLogicalPlan(stacked streamstats maximum): %v", err)
	}
	if len(projected.Stages) != 3 {
		t.Fatalf("stacked maximum stages = %#v", projected.Stages)
	}
	replacement := projected.Stages[1]
	if replacement.Operator != "StreamAggregate" ||
		!slices.Equal(replacement.InputFields, []string{"status", "user"}) ||
		!slices.Equal(replacement.OutputFields, []string{"status"}) {
		t.Fatalf("replacement maximum stream aggregate stage = %#v", replacement)
	}
	stacked := projected.Stages[2]
	if stacked.Operator != "StreamAggregate" ||
		!slices.Equal(stacked.InputFields, []string{"status"}) ||
		!slices.Equal(stacked.OutputFields, []string{"max_so_far"}) {
		t.Fatalf("stacked maximum stream aggregate stage = %#v", stacked)
	}
	if !slices.Equal(projected.ReferencedFields, []string{"status", "user"}) {
		t.Fatalf(
			"stacked maximum referenced fields = %v, want de-duplicated measure and BY fields",
			projected.ReferencedFields,
		)
	}
	if projected.Output.Kind != OutputKindStatic ||
		!slices.Equal(
			projected.Output.Fields,
			[]string{"event_id", "user", "status", "max_so_far"},
		) || projected.Output.MaxDynamicFields != 0 {
		t.Fatalf(
			"stacked maximum stream aggregate output = %#v, want row-preserving static schema",
			projected.Output,
		)
	}
}
