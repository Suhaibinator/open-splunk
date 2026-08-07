package searchinspection

import (
	"context"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestProjectLogicalPlanDescribesConditionalStreamAggregateInputs(t *testing.T) {
	t.Parallel()

	const source = "index=main | streamstats current=f window=3 global=f count(eval(status>=500)) AS prior_errors BY service"
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
			&plan.StreamAggregate{
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
					Output: "prior_errors",
				},
				IncludeCurrent: false,
				WindowRows:     3,
				Global:         false,
				Range:          sourceRange,
			},
		},
	}

	projected, err := projectLogicalPlan(context.Background(), logical, source)
	if err != nil {
		t.Fatalf("projectLogicalPlan(streamstats count(eval)): %v", err)
	}
	stage := projected.Stages[1]
	if stage.Operator != "StreamAggregate" ||
		!slices.Equal(stage.InputFields, []string{"service", "status"}) ||
		!slices.Equal(stage.OutputFields, []string{"prior_errors"}) {
		t.Fatalf("conditional stream aggregate stage = %#v", stage)
	}
	if !slices.Equal(projected.ReferencedFields, []string{"service", "status"}) {
		t.Fatalf(
			"conditional stream aggregate referenced fields = %v, want predicate and BY fields",
			projected.ReferencedFields,
		)
	}
}
