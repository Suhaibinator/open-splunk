package searchinspection

import (
	"context"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestProjectLogicalPlanDescribesMinimumStreamAggregateReferences(t *testing.T) {
	t.Parallel()

	const source = "index=main | table event_id,user,status | streamstats current=f window=2 global=f min(status) AS prior_min BY user"
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
		OutputFields: []string{"event_id", "user", "status", "prior_min"},
		Operators: []plan.Operator{
			&plan.Scan{Range: sourceRange},
			&plan.StreamAggregate{
				GroupBy: []plan.FieldRef{user},
				Measure: plan.AggregateMeasure{
					Function: plan.AggregateFunctionMinimum,
					Input:    status,
					Output:   "prior_min",
				},
				IncludeCurrent: false,
				WindowRows:     2,
				Global:         false,
				Range:          sourceRange,
			},
		},
	}

	projected, err := projectLogicalPlan(context.Background(), logical, source)
	if err != nil {
		t.Fatalf("projectLogicalPlan(streamstats minimum): %v", err)
	}
	stage := projected.Stages[1]
	if stage.Operator != "StreamAggregate" ||
		!slices.Equal(stage.InputFields, []string{"status", "user"}) ||
		!slices.Equal(stage.OutputFields, []string{"prior_min"}) {
		t.Fatalf("minimum stream aggregate stage = %#v", stage)
	}
	if !slices.Equal(projected.ReferencedFields, []string{"status", "user"}) {
		t.Fatalf(
			"minimum stream aggregate referenced fields = %v, want measure and BY fields",
			projected.ReferencedFields,
		)
	}
	if projected.Output.Kind != OutputKindStatic ||
		!slices.Equal(
			projected.Output.Fields,
			[]string{"event_id", "user", "status", "prior_min"},
		) || projected.Output.MaxDynamicFields != 0 {
		t.Fatalf(
			"minimum stream aggregate output = %#v, want row-preserving static schema",
			projected.Output,
		)
	}
}
