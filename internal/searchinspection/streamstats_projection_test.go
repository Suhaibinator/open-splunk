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
	sourceRange := *result.Plan.Stages[0].SourceRange
	result.Plan.Stages = append(result.Plan.Stages, PlanStage{
		Index:       1,
		Operator:    "StreamAggregate",
		SourceRange: &sourceRange,
	})
	if err := ValidateResult(result); err != nil {
		t.Fatalf("ValidateResult: %v", err)
	}
}

func TestProjectLogicalPlanDescribesNumericStreamAggregate(t *testing.T) {
	t.Parallel()

	source := "index=main | table event_id,user,status | streamstats current=f window=2 global=f sum(status) AS prior_total BY user"
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
		OutputFields: []string{"event_id", "user", "status", "prior_total"},
		Operators: []plan.Operator{
			&plan.Scan{Range: sourceRange},
			&plan.StreamAggregate{
				GroupBy: []plan.FieldRef{user},
				Measure: plan.AggregateMeasure{
					Function: plan.AggregateFunctionSum,
					Input:    status,
					Output:   "prior_total",
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
		!slices.Equal(stage.OutputFields, []string{"prior_total"}) {
		t.Fatalf("numeric stream aggregate stage = %#v", stage)
	}
	if !slices.Equal(projected.ReferencedFields, []string{"status", "user"}) {
		t.Fatalf(
			"numeric stream aggregate referenced fields = %v, want measure and BY fields",
			projected.ReferencedFields,
		)
	}
	if projected.Output.Kind != OutputKindStatic ||
		!slices.Equal(
			projected.Output.Fields,
			[]string{"event_id", "user", "status", "prior_total"},
		) || projected.Output.MaxDynamicFields != 0 {
		t.Fatalf("numeric stream aggregate output = %#v, want row-preserving static schema", projected.Output)
	}

	logical.Operators[1].(*plan.StreamAggregate).Measure.Function = plan.AggregateFunctionAverage
	averageProjected, err := projectLogicalPlan(context.Background(), logical, source)
	if err != nil {
		t.Fatalf("projectLogicalPlan(streamstats average): %v", err)
	}
	averageStage := averageProjected.Stages[1]
	if averageStage.Operator != "StreamAggregate" ||
		!slices.Equal(averageStage.InputFields, []string{"status", "user"}) ||
		!slices.Equal(averageStage.OutputFields, []string{"prior_total"}) ||
		!slices.Equal(averageProjected.ReferencedFields, []string{"status", "user"}) ||
		averageProjected.Output.Kind != OutputKindStatic ||
		!slices.Equal(
			averageProjected.Output.Fields,
			[]string{"event_id", "user", "status", "prior_total"},
		) {
		t.Fatalf("average stream aggregate projection = %#v", averageProjected)
	}
}
