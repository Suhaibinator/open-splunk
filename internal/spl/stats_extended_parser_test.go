package spl

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
)

func TestParseStatsExtendedFamiliesPreserveDistinctFunctionsAndDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call       string
		function   AggregateFunction
		percentile uint8
		alias      string
	}{
		{call: "estdc", function: AggregateFunctionEstimatedDistinctCount, alias: "estdc(value)"},
		{call: "estdc_error", function: AggregateFunctionEstimatedDistinctCountError, alias: "estdc_error(value)"},
		{call: "exactperc095", function: AggregateFunctionExactPercentile, percentile: 95, alias: "exactperc95(value)"},
		{call: "upperperc050", function: AggregateFunctionUpperPercentile, percentile: 50, alias: "upperperc50(value)"},
		{call: "median", function: AggregateFunctionMedian, alias: "median(value)"},
		{call: "mode", function: AggregateFunctionMode, alias: "mode(value)"},
		{call: "first", function: AggregateFunctionFirst, alias: "first(value)"},
		{call: "last", function: AggregateFunctionLast, alias: "last(value)"},
		{call: "earliest_time", function: AggregateFunctionEarliestTime, alias: "earliest_time(value)"},
		{call: "latest_time", function: AggregateFunctionLatestTime, alias: "latest_time(value)"},
		{call: "rate", function: AggregateFunctionRate, alias: "rate(value)"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.call, func(t *testing.T) {
			t.Parallel()

			query, err := Parse("| stats " + strings.ToUpper(test.call) + "(value)")
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			aggregate := query.Commands[0].(*StatsCommand).Aggregates[0]
			if aggregate.Function != test.function ||
				aggregate.Percentile != test.percentile ||
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

func TestParseStatsPercentileFamiliesRemainIntegerOneThroughNinetyNinePendingOracle(t *testing.T) {
	t.Parallel()

	// The pinned reference pages disagree about broader suffixes and algorithms.
	// This test records the deliberately conservative contract until an oracle
	// resolves whether decimal/endpoint forms belong in a later compatibility tier.
	for _, call := range []string{
		"p0", "p100", "perc0", "perc100",
		"exactperc", "exactperc0", "exactperc100", "exactperc256",
		"exactperc-1", "exactperc50.5", "exactpercfoo",
		"upperperc", "upperperc0", "upperperc100", "upperperc256",
		"upperperc-1", "upperperc50.5", "upperpercfoo",
	} {
		call := call
		t.Run(call, func(t *testing.T) {
			t.Parallel()

			_, err := Parse("| stats " + call + "(value)")
			if err == nil {
				t.Fatal("Parse succeeded, want bounded-suffix diagnostic")
			}
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_UNSUPPORTED_STATS_AGGREGATE" {
				t.Fatalf("diagnostic = %#v, want SPL_UNSUPPORTED_STATS_AGGREGATE", err)
			}
		})
	}

	for _, call := range []string{"p1", "perc99", "exactperc001", "exactperc099", "upperperc001", "upperperc099"} {
		if _, err := Parse("| stats " + call + "(value)"); err != nil {
			t.Errorf("Parse(%s): %v", call, err)
		}
	}
}

func TestParseStatsEveryFieldTakingFamilyAcceptsScalarEvalWithExplicitAlias(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call       string
		function   AggregateFunction
		percentile uint8
	}{
		{call: "p95", function: AggregateFunctionPercentile, percentile: 95},
		{call: "exactperc95", function: AggregateFunctionExactPercentile, percentile: 95},
		{call: "upperperc95", function: AggregateFunctionUpperPercentile, percentile: 95},
		{call: "median", function: AggregateFunctionMedian},
		{call: "sum", function: AggregateFunctionSum},
		{call: "avg", function: AggregateFunctionAverage},
		{call: "mean", function: AggregateFunctionAverage},
		{call: "range", function: AggregateFunctionRange},
		{call: "sumsq", function: AggregateFunctionSumSquares},
		{call: "stdev", function: AggregateFunctionStandardDeviationSample},
		{call: "stdevp", function: AggregateFunctionStandardDeviationPopulation},
		{call: "var", function: AggregateFunctionVarianceSample},
		{call: "varp", function: AggregateFunctionVariancePopulation},
		{call: "dc", function: AggregateFunctionDistinctCount},
		{call: "distinct_count", function: AggregateFunctionDistinctCount},
		{call: "estdc", function: AggregateFunctionEstimatedDistinctCount},
		{call: "estdc_error", function: AggregateFunctionEstimatedDistinctCountError},
		{call: "values", function: AggregateFunctionValues},
		{call: "list", function: AggregateFunctionList},
		{call: "min", function: AggregateFunctionMinimum},
		{call: "max", function: AggregateFunctionMaximum},
		{call: "mode", function: AggregateFunctionMode},
		{call: "first", function: AggregateFunctionFirst},
		{call: "last", function: AggregateFunctionLast},
		{call: "earliest", function: AggregateFunctionEarliest},
		{call: "latest", function: AggregateFunctionLatest},
		{call: "earliest_time", function: AggregateFunctionEarliestTime},
		{call: "latest_time", function: AggregateFunctionLatestTime},
		{call: "rate", function: AggregateFunctionRate},
	}
	for _, test := range tests {
		test := test
		t.Run(test.call, func(t *testing.T) {
			t.Parallel()

			const expression = `if(isnull(value), coalesce(fallback, 0), value+1)`
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
			got := source[aggregate.InputExpression.SourceRange().Start.Offset:aggregate.InputExpression.SourceRange().End.Offset]
			if got != expression {
				t.Fatalf("input expression range = %q, want %q", got, expression)
			}
		})
	}
}

func TestParseStatsGenericEvalBoundaryKeepsCountDisjoint(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		source  string
		code    string
		message string
	}{
		{
			name:   "field occurrence count stays exact-field only",
			source: `| stats c(eval(value)) AS result`,
			code:   "SPL_EXPECTED_RIGHT_PAREN",
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := Parse(test.source)
			if err == nil {
				t.Fatal("Parse succeeded, want error")
			}
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != test.code {
				t.Fatalf("diagnostic = %#v, want %s", err, test.code)
			}
			if test.message != "" && !strings.Contains(diagnostic.Message, test.message) {
				t.Fatalf("message = %q, want substring %q", diagnostic.Message, test.message)
			}
		})
	}
}

func TestParseStatsFieldTakingEvalAcceptsBooleanScalarResults(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		call     string
		function AggregateFunction
	}{
		{call: "values", function: AggregateFunctionValues},
		{call: "dc", function: AggregateFunctionDistinctCount},
		{call: "mode", function: AggregateFunctionMode},
		{call: "sum", function: AggregateFunctionSum},
	} {
		test := test
		t.Run(test.call, func(t *testing.T) {
			t.Parallel()

			query, err := Parse(
				fmt.Sprintf(`| stats %s(eval(isnull(value))) AS flags`, test.call),
			)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			aggregate := query.Commands[0].(*StatsCommand).Aggregates[0]
			call, ok := aggregate.InputExpression.(*ScalarCallExpr)
			if aggregate.Function != test.function ||
				!aggregate.ExplicitAlias || aggregate.Alias != "flags" ||
				aggregate.Predicate != nil || !ok ||
				call.Function != ScalarFunctionIsNull {
				t.Fatalf("aggregate = %#v, expression = %#v", aggregate, call)
			}
		})
	}

	query, err := Parse(`| stats count(eval(isnull(value))) AS missing`)
	if err != nil {
		t.Fatalf("Parse conditional count: %v", err)
	}
	count := query.Commands[0].(*StatsCommand).Aggregates[0]
	if count.Function != AggregateFunctionCountPredicate ||
		count.Predicate == nil || count.InputExpression != nil {
		t.Fatalf("conditional count = %#v", count)
	}
}

func TestStatsExtendedAggregateAndGenericEvalSuggestions(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		want   []string
	}{
		{source: "| stats est", want: []string{"estdc", "estdc_error"}},
		{source: "| stats exactp", want: []string{"exactperc50", "exactperc95"}},
		{source: "| stats upperp", want: []string{"upperperc50", "upperperc95"}},
		{source: "| stats earliest_", want: []string{"earliest_time"}},
		{source: "| stats latest_", want: []string{"latest_time"}},
	} {
		result := Suggest(test.source, len(test.source), 20)
		if result.Diagnostic != nil {
			t.Fatalf("Suggest(%q): %v", test.source, result.Diagnostic)
		}
		if labels := suggestionLabels(result.Suggestions); !slices.Equal(labels, test.want) {
			t.Errorf("Suggest(%q) labels = %v, want %v", test.source, labels, test.want)
		}
	}

	const scalarSource = "| stats mode(eval(value+to"
	scalar := Suggest(scalarSource, len(scalarSource), 20)
	if scalar.Diagnostic != nil {
		t.Fatalf("Suggest generic eval: %v", scalar.Diagnostic)
	}
	if scalar.Context.FunctionClass != SuggestionFunctionClassScalar ||
		!scalar.Context.Allows(SuggestionKindFunction) ||
		!scalar.Context.Allows(SuggestionKindField) ||
		!scalar.Context.AllowsQuotedScalarFields {
		t.Fatalf("generic eval context = %#v", scalar.Context)
	}
	if labels := suggestionLabels(scalar.Suggestions); !slices.Equal(
		labels,
		[]string{"tonumber", "tostring"},
	) {
		t.Fatalf("generic eval labels = %v, want tonumber/tostring", labels)
	}

	exact := Suggest("| stats mode(fi", len("| stats mode(fi"), 20)
	if exact.Diagnostic != nil {
		t.Fatalf("Suggest exact input: %v", exact.Diagnostic)
	}
	if !exact.Context.Allows(SuggestionKindField) ||
		exact.Context.Allows(SuggestionKindFunction) ||
		!exact.Context.AllowsQuotedScalarFields {
		t.Fatalf("exact input context = %#v", exact.Context)
	}
}

func TestStatsOnlyExtendedSuggestionsDoNotLeakIntoRelatedCommands(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		"| eventstats med",
		"| streamstats med",
		"| chart med",
		"| timechart span=5m med",
	} {
		result := Suggest(source, len(source), 20)
		if result.Diagnostic != nil {
			t.Fatalf("Suggest(%q): %v", source, result.Diagnostic)
		}
		if slices.Contains(suggestionLabels(result.Suggestions), "median") {
			t.Errorf("Suggest(%q) leaked stats-only median", source)
		}
	}
}
