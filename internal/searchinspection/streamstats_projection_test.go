package searchinspection

import (
	"context"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestProjectLogicalPlanDescribesRowPreservingStreamAggregate(t *testing.T) {
	t.Parallel()

	source := "index=main | table event_id,user,status | streamstats current=f window=2 global=f count(status) AS prior BY user"
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
		OutputFields: []string{"event_id", "user", "status", "prior"},
		Operators: []plan.Operator{
			&plan.Scan{Range: sourceRange},
			&plan.StreamAggregate{
				GroupBy: []plan.FieldRef{user},
				Measure: plan.AggregateMeasure{
					Function: plan.AggregateFunctionCountValues,
					Input:    status,
					Output:   "prior",
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
		t.Fatalf("projectLogicalPlan: %v", err)
	}
	stage := projected.Stages[1]
	if stage.Operator != "StreamAggregate" ||
		!slices.Equal(stage.InputFields, []string{"status", "user"}) ||
		!slices.Equal(stage.OutputFields, []string{"prior"}) {
		t.Fatalf("stream aggregate stage = %#v", stage)
	}
	if !slices.Equal(projected.ReferencedFields, []string{"status", "user"}) {
		t.Fatalf("referenced fields = %v, want measure and BY fields", projected.ReferencedFields)
	}
	if projected.Output.Kind != OutputKindStatic ||
		!slices.Equal(projected.Output.Fields, []string{"event_id", "user", "status", "prior"}) ||
		projected.Output.MaxDynamicFields != 0 {
		t.Fatalf("output = %#v, want row-preserving static schema", projected.Output)
	}
}

func TestValidateResultAcceptsStreamAggregateOperator(t *testing.T) {
	t.Parallel()

	result := validResultForValidation(t)
	result.Plan.Stages[0].Operator = "StreamAggregate"
	if err := ValidateResult(result); err != nil {
		t.Fatalf("ValidateResult: %v", err)
	}
}
