package plan

import (
	"fmt"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildStatsNumericFamiliesAndMeanPreserveIRAndOutputs(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | stats mean(value) range(value) sumsq(value) `+
				`stdev(value) stdevp(value) var(value) varp(value)`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	wantOutputs := []string{
		"mean(value)", "range(value)", "sumsq(value)", "stdev(value)",
		"stdevp(value)", "var(value)", "varp(value)",
	}
	if !slices.Equal(logical.OutputFields, wantOutputs) {
		t.Fatalf("outputs = %v, want %v", logical.OutputFields, wantOutputs)
	}
	aggregate := logical.Operators[len(logical.Operators)-1].(*Aggregate)
	wantFunctions := []AggregateFunction{
		AggregateFunctionAverage,
		AggregateFunctionRange,
		AggregateFunctionSumSquares,
		AggregateFunctionStandardDeviationSample,
		AggregateFunctionStandardDeviationPopulation,
		AggregateFunctionVarianceSample,
		AggregateFunctionVariancePopulation,
	}
	for index, measure := range aggregate.Measures {
		if measure.Function != wantFunctions[index] ||
			measure.Input.Name != "value" ||
			measure.InputExpression != nil ||
			measure.Predicate != nil ||
			measure.Output != wantOutputs[index] {
			t.Errorf("measure[%d] = %#v", index, measure)
		}
	}
}

func TestBuildStatsNumericEvalInputsProduceScalarIR(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call       string
		function   AggregateFunction
		percentile uint8
	}{
		{call: "p95", function: AggregateFunctionPercentile, percentile: 95},
		{call: "sum", function: AggregateFunctionSum},
		{call: "avg", function: AggregateFunctionAverage},
		{call: "mean", function: AggregateFunctionAverage},
		{call: "range", function: AggregateFunctionRange},
		{call: "sumsq", function: AggregateFunctionSumSquares},
		{call: "stdev", function: AggregateFunctionStandardDeviationSample},
		{call: "stdevp", function: AggregateFunctionStandardDeviationPopulation},
		{call: "var", function: AggregateFunctionVarianceSample},
		{call: "varp", function: AggregateFunctionVariancePopulation},
	}
	for _, test := range tests {
		t.Run(test.call, func(t *testing.T) {
			t.Parallel()

			logical, err := Build(
				mustParse(
					t,
					fmt.Sprintf(
						`index=gradethis | stats %s(eval(if(status>=500, bytes*price, tonumber(fallback)))) AS result`,
						test.call,
					),
				),
				testScope([]string{"gradethis"}, nil),
			)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			operator := logical.Operators[len(logical.Operators)-1].(*Aggregate)
			measure := operator.Measures[0]
			if measure.Function != test.function ||
				measure.Percentile != test.percentile ||
				measure.Input.Name != "" || measure.Input.Canonical ||
				measure.Input.Path != nil || measure.Input.Range != (spl.Range{}) ||
				measure.InputExpression == nil ||
				measure.Predicate != nil ||
				measure.Output != "result" {
				t.Fatalf("measure = %#v", measure)
			}
			if _, ok := measure.InputExpression.(*ScalarIfExpression); !ok {
				t.Fatalf("input expression = %T, want *ScalarIfExpression", measure.InputExpression)
			}
			analysis, analysisErr := Analyze(logical)
			if analysisErr != nil {
				t.Fatalf("Analyze: %v", analysisErr)
			}
			for _, field := range []string{"bytes", "fallback", "price", "status"} {
				if !slices.Contains(analysis.ReferencedFields, field) {
					t.Errorf("referenced fields = %v, missing %q", analysis.ReferencedFields, field)
				}
			}
		})
	}
}

func TestBuildStatsNumericEvalRejectsForgedMetadata(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	sourceRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Offset: 5, Line: 1, Column: 6},
	}
	validExpression := func() spl.ScalarExpr {
		return &spl.ScalarBinaryExpr{
			Op: spl.ScalarBinaryOpAdd,
			Left: &spl.ScalarFieldExpr{
				Field: "bytes",
				Range: sourceRange,
			},
			Right: &spl.ScalarLiteralExpr{
				Value: spl.Literal{
					Kind:  spl.LiteralKindInteger,
					Text:  "1",
					Range: sourceRange,
				},
				Range: sourceRange,
			},
			Range: sourceRange,
		}
	}
	valid := spl.StatsAggregate{
		Function:        spl.AggregateFunctionSum,
		InputExpression: validExpression(),
		Alias:           "total",
		ExplicitAlias:   true,
		Range:           sourceRange,
		AliasRange:      sourceRange,
	}
	var typedNil *spl.ScalarFieldExpr
	tests := []struct {
		name   string
		mutate func(*spl.StatsAggregate)
	}{
		{
			name: "typed nil expression",
			mutate: func(aggregate *spl.StatsAggregate) {
				aggregate.InputExpression = typedNil
			},
		},
		{
			name: "exact field and expression",
			mutate: func(aggregate *spl.StatsAggregate) {
				aggregate.Input = "bytes"
				aggregate.InputRange = sourceRange
			},
		},
		{
			name: "predicate and expression",
			mutate: func(aggregate *spl.StatsAggregate) {
				aggregate.Predicate = &spl.WhereScalarPredicateExpr{
					Value: &spl.ScalarCallExpr{
						Function: spl.ScalarFunctionIsNull,
						Arguments: []spl.ScalarExpr{
							&spl.ScalarFieldExpr{Field: "bytes", Range: sourceRange},
						},
						Range: sourceRange,
					},
					Range: sourceRange,
				}
			},
		},
		{
			name: "implicit alias",
			mutate: func(aggregate *spl.StatsAggregate) {
				aggregate.ExplicitAlias = false
			},
		},
		{
			name: "field occurrence count function",
			mutate: func(aggregate *spl.StatsAggregate) {
				aggregate.Function = spl.AggregateFunctionCountValues
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			aggregate := valid
			test.mutate(&aggregate)
			query := &spl.Query{
				Search: base.Search,
				Commands: []spl.Command{&spl.StatsCommand{
					Aggregates: []spl.StatsAggregate{aggregate},
					Range:      sourceRange,
				}},
				Range: base.Range,
			}
			_, err := Build(query, testScope([]string{"gradethis"}, nil))
			assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_STATS_AGGREGATE")
		})
	}
}

func TestBuildStatsNumericEvalRevalidatesSharedArithmeticBudget(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	field := &spl.ScalarFieldExpr{Field: "value"}
	measures := make([]spl.StatsAggregate, spl.MaximumStatsMeasures)
	for index := range measures {
		var expression spl.ScalarExpr = field
		for range 17 {
			expression = &spl.ScalarBinaryExpr{
				Op:    spl.ScalarBinaryOpAdd,
				Left:  expression,
				Right: field,
			}
		}
		measures[index] = spl.StatsAggregate{
			Function:        spl.AggregateFunctionSum,
			InputExpression: expression,
			Alias:           fmt.Sprintf("total_%d", index),
			ExplicitAlias:   true,
		}
	}
	query := &spl.Query{
		Search: base.Search,
		Commands: []spl.Command{&spl.StatsCommand{
			Aggregates: measures,
		}},
		Range: base.Range,
	}
	_, err := Build(query, testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_QUERY_TOO_COMPLEX")
}

func TestBuildStatsNumericEvalPreservesReservedOpenSchemaBoundary(t *testing.T) {
	t.Parallel()

	_, err := Build(
		mustParse(t, `index=gradethis | stats sum(eval(fields+1)) AS total`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_AMBIGUOUS_STATS_FIELD")

	if _, err := Build(
		mustParse(
			t,
			`index=gradethis | stats count AS fields | stats sum(eval(fields+1)) AS total`,
		),
		testScope([]string{"gradethis"}, nil),
	); err != nil {
		t.Fatalf("Build with closed schema: %v", err)
	}
}

func TestAnalyzeStatsNumericEvalRejectsForgedPlanMetadata(t *testing.T) {
	t.Parallel()

	input := mustResolveEventAggregateField(t, "value")
	scalarInput := &ScalarFieldExpression{Field: input}
	tests := []AggregateMeasure{
		{
			Function:        AggregateFunctionSum,
			Input:           input,
			InputExpression: scalarInput,
			Output:          "total",
		},
		{
			Function:        AggregateFunctionCountValues,
			InputExpression: scalarInput,
			Output:          "occurrences",
		},
		{
			Function:        AggregateFunctionCountPredicate,
			InputExpression: scalarInput,
			Predicate: &ScalarPredicateExpression{Value: &ScalarCallExpression{
				Function: ScalarFunctionIsNull,
				Arguments: []ScalarExpression{
					scalarInput,
				},
			}},
			Output: "matches",
		},
	}
	for index, measure := range tests {
		_, err := Analyze(&Query{Operators: []Operator{&Aggregate{
			Measures: []AggregateMeasure{measure},
		}}})
		if err == nil {
			t.Fatalf("case %d Analyze succeeded for forged measure %#v", index, measure)
		}
	}
}
