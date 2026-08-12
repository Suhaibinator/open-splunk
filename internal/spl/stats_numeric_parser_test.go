package spl

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestParseStatsNumericFamiliesAndMeanPreserveCanonicalDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		function AggregateFunction
		alias    string
	}{
		{name: "avg", function: AggregateFunctionAverage, alias: "avg(value)"},
		{name: "mean", function: AggregateFunctionAverage, alias: "mean(value)"},
		{name: "range", function: AggregateFunctionRange, alias: "range(value)"},
		{name: "sumsq", function: AggregateFunctionSumSquares, alias: "sumsq(value)"},
		{name: "stdev", function: AggregateFunctionStandardDeviationSample, alias: "stdev(value)"},
		{name: "stdevp", function: AggregateFunctionStandardDeviationPopulation, alias: "stdevp(value)"},
		{name: "var", function: AggregateFunctionVarianceSample, alias: "var(value)"},
		{name: "varp", function: AggregateFunctionVariancePopulation, alias: "varp(value)"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			source := "| stats " + strings.ToUpper(test.name) + "(value)"
			query, err := Parse(source)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			aggregate := query.Commands[0].(*StatsCommand).Aggregates[0]
			if aggregate.Function != test.function ||
				aggregate.Input != "value" ||
				aggregate.InputRange == (Range{}) ||
				aggregate.InputExpression != nil ||
				aggregate.Predicate != nil ||
				aggregate.Alias != test.alias ||
				aggregate.ExplicitAlias {
				t.Fatalf("aggregate = %#v", aggregate)
			}
		})
	}
}

func TestParseStatsNumericEvalInputsUseV02ScalarGrammar(t *testing.T) {
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
		test := test
		t.Run(test.call, func(t *testing.T) {
			t.Parallel()

			const expression = `if(status>=500, bytes*price, tonumber(fallback))`
			source := fmt.Sprintf("| stats %s(eval(%s)) AS result", test.call, expression)
			query, err := Parse(source)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			aggregate := query.Commands[0].(*StatsCommand).Aggregates[0]
			if aggregate.Function != test.function ||
				aggregate.Percentile != test.percentile ||
				aggregate.Input != "" ||
				aggregate.InputRange != (Range{}) ||
				aggregate.InputExpression == nil ||
				aggregate.Predicate != nil ||
				aggregate.Alias != "result" ||
				!aggregate.ExplicitAlias {
				t.Fatalf("aggregate = %#v", aggregate)
			}
			if got := source[aggregate.InputExpression.SourceRange().Start.Offset:aggregate.InputExpression.SourceRange().End.Offset]; got != expression {
				t.Fatalf("input expression range = %q, want %q", got, expression)
			}
			if _, ok := aggregate.InputExpression.(*ScalarIfExpr); !ok {
				t.Fatalf("input expression = %T, want *ScalarIfExpr", aggregate.InputExpression)
			}
		})
	}
}

func TestParseStatsNumericEvalInputsPreserveCountBoundary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		source  string
		code    string
		message string
	}{
		{
			name:   "empty expression",
			source: `| stats sum(eval()) AS total`,
			code:   "SPL_EXPECTED_SCALAR_EXPRESSION",
		},
		{
			name:   "Boolean comparison stays conditional-count syntax",
			source: `| stats sum(eval(status=500)) AS total`,
			code:   "SPL_EXPECTED_RIGHT_PAREN",
		},
		{
			name:   "field occurrence count remains exact-field only",
			source: `| stats c(eval(bytes+1)) AS occurrences`,
			code:   "SPL_EXPECTED_RIGHT_PAREN",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := Parse(test.source)
			if err == nil {
				t.Fatal("Parse succeeded, want error")
			}
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) {
				t.Fatalf("error = %T %v, want *Diagnostic", err, err)
			}
			if diagnostic.Code != test.code {
				t.Fatalf("diagnostic = %#v, want code %q", diagnostic, test.code)
			}
			if test.message != "" && !strings.Contains(diagnostic.Message, test.message) {
				t.Fatalf("message = %q, want substring %q", diagnostic.Message, test.message)
			}
		})
	}
}

func TestParseStatsNumericEvalInputsShareArithmeticBudget(t *testing.T) {
	t.Parallel()

	expression := strings.Join(repeatString("value", 18), "+")
	measures := make([]string, MaximumStatsMeasures)
	for index := range measures {
		measures[index] = fmt.Sprintf(
			"sum(eval(%s)) AS total_%d",
			expression,
			index,
		)
	}
	_, err := Parse("| stats " + strings.Join(measures, " "))
	if err == nil {
		t.Fatal("Parse succeeded above arithmetic budget")
	}
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
		t.Fatalf("diagnostic = %#v, want SPL_QUERY_TOO_COMPLEX", err)
	}
}

func TestParseStatsCountEvalRemainsBooleanAndDisjointFromScalarInput(t *testing.T) {
	t.Parallel()

	query, err := Parse(`| stats count(eval(status>=500)) AS errors`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	aggregate := query.Commands[0].(*StatsCommand).Aggregates[0]
	if aggregate.Function != AggregateFunctionCountPredicate ||
		aggregate.Predicate == nil || aggregate.InputExpression != nil {
		t.Fatalf("conditional count = %#v", aggregate)
	}
}
