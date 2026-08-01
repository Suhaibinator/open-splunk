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

func TestProjectLogicalPlanDescribesConditionalEventAggregateInputs(t *testing.T) {
	t.Parallel()

	source := "index=main | eventstats count(eval(status>=500)) AS errors BY service"
	sourceRange := testSourceRange()
	status, err := plan.ResolveField("status", sourceRange)
	if err != nil {
		t.Fatalf("ResolveField(status): %v", err)
	}
	service, err := plan.ResolveField("service", sourceRange)
	if err != nil {
		t.Fatalf("ResolveField(service): %v", err)
	}
	logical := &plan.Query{
		Operators: []plan.Operator{
			&plan.Scan{Range: sourceRange},
			&plan.EventAggregate{
				GroupBy: []plan.FieldRef{service},
				Measure: plan.AggregateMeasure{
					Function: plan.AggregateFunctionCountPredicate,
					Predicate: &plan.EvalComparisonExpression{
						Left: &plan.ScalarFieldExpression{
							Field: status,
							Range: sourceRange,
						},
						Op: plan.ComparisonOpGreaterEqual,
						Right: &plan.ScalarLiteralExpression{
							Value: plan.Value{
								Kind:  plan.ValueKindInt64,
								Int64: 500,
							},
							Range: sourceRange,
						},
						Range: sourceRange,
					},
					Output: "errors",
				},
				Range: sourceRange,
			},
		},
	}
	projected, err := projectLogicalPlan(context.Background(), logical, source)
	if err != nil {
		t.Fatalf("projectLogicalPlan: %v", err)
	}
	stage := projected.Stages[1]
	if stage.Operator != "EventAggregate" ||
		!slices.Equal(stage.InputFields, []string{"service", "status"}) ||
		!slices.Equal(stage.OutputFields, []string{"errors"}) {
		t.Fatalf("conditional event aggregate stage = %#v", stage)
	}
	if !slices.Equal(projected.ReferencedFields, []string{"service", "status"}) {
		t.Fatalf("referenced fields = %v", projected.ReferencedFields)
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
