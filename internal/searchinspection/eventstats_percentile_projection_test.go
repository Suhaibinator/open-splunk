package searchinspection

import (
	"context"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestProjectLogicalPlanProjectsEventStatsPercentileInputAndOutput(t *testing.T) {
	t.Parallel()

	source := "index=main | eventstats p95(http.duration) AS p95_duration BY host"
	sourceRange := testSourceRange()
	host, err := plan.ResolveField("host", sourceRange)
	if err != nil {
		t.Fatalf("ResolveField(host): %v", err)
	}
	duration, err := plan.ResolveField("http.duration", sourceRange)
	if err != nil {
		t.Fatalf("ResolveField(http.duration): %v", err)
	}
	logical := &plan.Query{
		OutputFields: []string{"_time", "host", "http.duration", "p95_duration"},
		Operators: []plan.Operator{
			&plan.Scan{Range: sourceRange},
			&plan.EventAggregate{
				GroupBy: []plan.FieldRef{host},
				Measure: plan.AggregateMeasure{
					Function:   plan.AggregateFunctionPercentile,
					Input:      duration,
					Percentile: 95,
					Output:     "p95_duration",
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
		!slices.Equal(stage.InputFields, []string{"host", "http.duration"}) ||
		!slices.Equal(stage.OutputFields, []string{"p95_duration"}) {
		t.Fatalf("eventstats percentile stage = %#v", stage)
	}
	if !slices.Equal(projected.ReferencedFields, []string{"host", "http.duration"}) {
		t.Fatalf("referenced fields = %v", projected.ReferencedFields)
	}
	if projected.Output.Kind != OutputKindStatic ||
		!slices.Equal(
			projected.Output.Fields,
			[]string{"_time", "host", "http.duration", "p95_duration"},
		) || projected.Output.MaxDynamicFields != 0 {
		t.Fatalf("output = %#v, want static percentile-enriched schema", projected.Output)
	}
}
