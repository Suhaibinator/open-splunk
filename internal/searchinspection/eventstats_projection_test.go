package searchinspection

import (
	"context"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestProjectLogicalPlanDescribesEventAggregate(t *testing.T) {
	t.Parallel()

	source := "index=main | eventstats count AS events BY host"
	sourceRange := testSourceRange()
	host, err := plan.ResolveField("host", sourceRange)
	if err != nil {
		t.Fatalf("ResolveField(host): %v", err)
	}
	logical := &plan.Query{
		Operators: []plan.Operator{
			&plan.Scan{Range: sourceRange},
			&plan.EventAggregate{
				GroupBy: []plan.FieldRef{host},
				Measure: plan.AggregateMeasure{
					Function: plan.AggregateFunctionCountRows,
					Output:   "events",
				},
				Range: sourceRange,
			},
		},
	}
	projected, err := projectLogicalPlan(
		context.Background(),
		logical,
		source,
	)
	if err != nil {
		t.Fatalf("projectLogicalPlan: %v", err)
	}
	stage := projected.Stages[1]
	if stage.Operator != "EventAggregate" ||
		!slices.Equal(stage.InputFields, []string{"host"}) ||
		!slices.Equal(stage.OutputFields, []string{"events"}) {
		t.Fatalf("event aggregate stage = %#v", stage)
	}
	if !slices.Equal(projected.ReferencedFields, []string{"host"}) {
		t.Fatalf(
			"referenced fields = %v",
			projected.ReferencedFields,
		)
	}
	if projected.Output.Kind != OutputKindOpen {
		t.Fatalf("output = %#v, want open event schema", projected.Output)
	}
}

func TestValidateResultAcceptsEventAggregateOperator(t *testing.T) {
	t.Parallel()

	result := validResultForValidation(t)
	result.Plan.Stages[0].Operator = "EventAggregate"
	if err := ValidateResult(result); err != nil {
		t.Fatalf("ValidateResult: %v", err)
	}
}
