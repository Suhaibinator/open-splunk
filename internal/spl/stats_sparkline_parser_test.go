package spl

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
)

func TestParseStatsSparklinePreservesNestedMetadataRangesAndOrder(t *testing.T) {
	t.Parallel()

	const source = `index=main | stats partitions=1 sparkline(AvG(latency),6H) AS trend count BY host dedup_splitvals=false`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*StatsCommand)
	if len(command.Aggregates) != 2 || len(command.GroupBy) != 1 {
		t.Fatalf("stats command = %#v", command)
	}
	aggregate := command.Aggregates[0]
	if aggregate.Function != AggregateFunctionInvalid || aggregate.Sparkline == nil ||
		aggregate.Sparkline.Function != AggregateFunctionAverage ||
		aggregate.Sparkline.Input != "latency" || aggregate.Alias != "trend" ||
		!aggregate.ExplicitAlias {
		t.Fatalf("sparkline aggregate = %#v", aggregate)
	}
	if aggregate.Sparkline.Span.Kind != SparklineSpanKindExplicit ||
		aggregate.Sparkline.Span.Magnitude != 6 ||
		aggregate.Sparkline.Span.Unit != SparklineSpanUnitHour {
		t.Fatalf("sparkline span = %#v", aggregate.Sparkline.Span)
	}
	for _, test := range []struct {
		sourceRange Range
		want        string
	}{
		{aggregate.Range, `sparkline(AvG(latency),6H) AS trend`},
		{aggregate.Sparkline.Range, `sparkline(AvG(latency),6H)`},
		{aggregate.Sparkline.InputRange, `latency`},
		{aggregate.Sparkline.Span.Range, `6H`},
		{aggregate.AliasRange, `trend`},
	} {
		assertSourceRangeText(t, source, test.sourceRange, test.want)
	}
	if command.Aggregates[1].Function != AggregateFunctionCount ||
		command.Aggregates[1].Sparkline != nil || command.Aggregates[1].Alias != "count" {
		t.Fatalf("ordinary measure after sparkline = %#v", command.Aggregates[1])
	}
}

func TestParseStatsSparklineAcceptsScopedAndUnscopedCountForms(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		source    string
		function  AggregateFunction
		input     string
		automatic bool
	}{
		{name: "bare inner", source: `| stats sparkline(count)`, function: AggregateFunctionCount, automatic: true},
		{name: "empty call", source: `| stats sparkline(count())`, function: AggregateFunctionCount, automatic: true},
		{name: "scoped count", source: `| stats sparkline(count(status),30m)`, function: AggregateFunctionCountValues, input: "status"},
		{name: "scoped abbreviation", source: `| stats sparkline(c(status),30m)`, function: AggregateFunctionCountValues, input: "status"},
		{name: "legacy shorthand", source: `| stats sparkline count`, function: AggregateFunctionCount, automatic: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			query, err := Parse(test.source)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			aggregate := query.Commands[0].(*StatsCommand).Aggregates[0]
			if aggregate.Sparkline == nil ||
				aggregate.Sparkline.Function != test.function ||
				aggregate.Sparkline.Input != test.input ||
				aggregate.Alias != "sparkline" || aggregate.ExplicitAlias {
				t.Fatalf("sparkline = %#v", aggregate)
			}
			if test.automatic != (aggregate.Sparkline.Span.Kind == SparklineSpanKindAutomatic) {
				t.Fatalf("span = %#v", aggregate.Sparkline.Span)
			}
			if aggregate.Sparkline.Span.Kind == SparklineSpanKindAutomatic &&
				aggregate.Sparkline.Span != (SparklineSpan{Kind: SparklineSpanKindAutomatic}) {
				t.Fatalf("automatic span contains authored metadata: %#v", aggregate.Sparkline.Span)
			}
		})
	}
}

func TestParseStatsSparklinePreservesWildcardInputAndAliasMetadata(t *testing.T) {
	t.Parallel()

	const source = `| stats sparkline(AvG(*lay),5m) AS trend_* sparkline(dc(http_*))`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	aggregates := query.Commands[0].(*StatsCommand).Aggregates
	if len(aggregates) != 2 {
		t.Fatalf("aggregates = %#v", aggregates)
	}

	aliased := aggregates[0]
	if aliased.Sparkline == nil ||
		aliased.Sparkline.Function != AggregateFunctionAverage ||
		aliased.Sparkline.InputGlob == nil ||
		aliased.Sparkline.InputGlob.Pattern != "*lay" ||
		aliased.Sparkline.Input != "" || aliased.Sparkline.InputRange != (Range{}) ||
		aliased.AliasGlob == nil || aliased.AliasGlob.Pattern != "trend_*" ||
		!aliased.ExplicitAlias {
		t.Fatalf("aliased sparkline = %#v", aliased)
	}
	for _, test := range []struct {
		sourceRange Range
		want        string
	}{
		{aliased.Range, `sparkline(AvG(*lay),5m) AS trend_*`},
		{aliased.Sparkline.Range, `sparkline(AvG(*lay),5m)`},
		{aliased.Sparkline.InputGlob.Range, `*lay`},
		{aliased.AliasGlob.Range, `trend_*`},
	} {
		assertSourceRangeText(t, source, test.sourceRange, test.want)
	}

	implicit := aggregates[1]
	if implicit.Sparkline == nil ||
		implicit.Sparkline.Function != AggregateFunctionDistinctCount ||
		implicit.Sparkline.InputGlob == nil ||
		implicit.Sparkline.InputGlob.Pattern != "http_*" ||
		implicit.AliasGlob != nil || implicit.Alias != "sparkline" ||
		implicit.ExplicitAlias || implicit.AliasRange != implicit.Sparkline.Range {
		t.Fatalf("unaliased sparkline = %#v", implicit)
	}
}

func TestParseStatsSparklineSupportsDocumentedInnerFunctionInventory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		function AggregateFunction
	}{
		{"c", AggregateFunctionCountValues},
		{"count", AggregateFunctionCountValues},
		{"dc", AggregateFunctionDistinctCount},
		{"mean", AggregateFunctionAverage},
		{"avg", AggregateFunctionAverage},
		{"stdev", AggregateFunctionStandardDeviationSample},
		{"stdevp", AggregateFunctionStandardDeviationPopulation},
		{"var", AggregateFunctionVarianceSample},
		{"varp", AggregateFunctionVariancePopulation},
		{"sum", AggregateFunctionSum},
		{"sumsq", AggregateFunctionSumSquares},
		{"min", AggregateFunctionMinimum},
		{"max", AggregateFunctionMaximum},
		{"range", AggregateFunctionRange},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			query, err := Parse(`| stats sparkline(` + test.name + `(value)) AS trend`)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			sparkline := query.Commands[0].(*StatsCommand).Aggregates[0].Sparkline
			if sparkline == nil || sparkline.Function != test.function || sparkline.Input != "value" {
				t.Fatalf("sparkline = %#v", sparkline)
			}
		})
	}
}

func TestParseStatsSparklinePreservesEveryDocumentedSpanUnit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		span      string
		magnitude uint64
		unit      SparklineSpanUnit
	}{
		{"250us", 250, SparklineSpanUnitMicrosecond},
		{"125MS", 125, SparklineSpanUnitMillisecond},
		{"25cs", 25, SparklineSpanUnitCentisecond},
		{"5ds", 5, SparklineSpanUnitDecisecond},
		{"2seconds", 2, SparklineSpanUnitSecond},
		{"3mins", 3, SparklineSpanUnitMinute},
		{"4hours", 4, SparklineSpanUnitHour},
		{"5days", 5, SparklineSpanUnitDay},
		{"6months", 6, SparklineSpanUnitMonth},
	}
	for _, test := range tests {
		t.Run(test.span, func(t *testing.T) {
			t.Parallel()

			query, err := Parse(`| stats sparkline(sum(value),` + test.span + `)`)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			span := query.Commands[0].(*StatsCommand).Aggregates[0].Sparkline.Span
			if span.Kind != SparklineSpanKindExplicit ||
				span.Magnitude != test.magnitude || span.Unit != test.unit {
				t.Fatalf("span = %#v", span)
			}
		})
	}
}

func TestParseStatsSparklineRejectsUnsupportedShapesAndInvalidSpans(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		source  string
		code    string
		message string
	}{
		{name: "unsupported inner", source: `| stats sparkline(median(value))`, code: "SPL_UNSUPPORTED_STATS_AGGREGATE", message: "not supported"},
		{name: "distinct long alias excluded", source: `| stats sparkline(distinct_count(value))`, code: "SPL_UNSUPPORTED_STATS_AGGREGATE"},
		{name: "unscoped sum", source: `| stats sparkline(sum)`, code: "SPL_UNSUPPORTED_STATS_SYNTAX", message: "requires one exact field"},
		{name: "unscoped c", source: `| stats sparkline(c)`, code: "SPL_UNSUPPORTED_STATS_SYNTAX"},
		{name: "eval input excluded", source: `| stats sparkline(sum(eval(value+1)))`, code: "SPL_EXPECTED_RIGHT_PAREN"},
		{name: "legacy only count", source: `| stats sparkline avg(value)`, code: "SPL_UNSUPPORTED_STATS_SYNTAX", message: "legacy"},
		{name: "missing span", source: `| stats sparkline(count,)`, code: "SPL_INVALID_ARGUMENT"},
		{name: "zero span", source: `| stats sparkline(count,0s)`, code: "SPL_INVALID_ARGUMENT"},
		{name: "unitless span", source: `| stats sparkline(count,5)`, code: "SPL_INVALID_ARGUMENT"},
		{name: "unknown span unit", source: `| stats sparkline(count,1w)`, code: "SPL_UNSUPPORTED_STATS_SYNTAX"},
		{name: "nondividing milliseconds", source: `| stats sparkline(count,3ms)`, code: "SPL_INVALID_ARGUMENT", message: "divide one second"},
		{name: "whole second in milliseconds", source: `| stats sparkline(count,1000ms)`, code: "SPL_INVALID_ARGUMENT", message: "less than one second"},
		{name: "overflow span", source: `| stats sparkline(count,18446744073709551616s)`, code: "SPL_NUMBER_OUT_OF_RANGE"},
		{name: "span assignment excluded", source: `| stats sparkline(count,span=5m)`, code: "SPL_INVALID_ARGUMENT"},
		{name: "extra argument", source: `| stats sparkline(count,5m,6m)`, code: "SPL_EXPECTED_RIGHT_PAREN"},
		{name: "wildcard alias excluded", source: `| stats sparkline(count) AS trend*`, code: "SPL_UNSUPPORTED_STATS_AGGREGATE"},
		{name: "wildcard exact alias", source: `| stats sparkline(avg(*lay)) AS trend`, code: "SPL_UNSUPPORTED_STATS_AGGREGATE", message: "wc-field"},
		{name: "wildcard alias capture mismatch", source: `| stats sparkline(avg(*lay)) AS trend_*_*`, code: "SPL_UNSUPPORTED_STATS_AGGREGATE", message: "each input capture"},
		{name: "quoted wildcard alias", source: `| stats sparkline(avg(*lay)) AS "trend_*"`, code: "SPL_UNSUPPORTED_STATS_AGGREGATE", message: "unquoted wc-field"},
	} {
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

func TestStatsSparklineSharesMeasureCapAndCoexistsWithBYAndOptions(t *testing.T) {
	t.Parallel()

	measures := make([]string, MaximumStatsMeasures)
	for index := range measures {
		if index%2 == 0 {
			measures[index] = fmt.Sprintf("sparkline(count) AS trend_%d", index)
		} else {
			measures[index] = fmt.Sprintf("sum(value) AS total_%d", index)
		}
	}
	source := `| stats allnum=true ` + strings.Join(measures, " ") +
		` BY host,service dedup_splitvals=false`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse at measure cap: %v", err)
	}
	command := query.Commands[0].(*StatsCommand)
	if len(command.Aggregates) != MaximumStatsMeasures || len(command.GroupBy) != 2 ||
		!command.Options.AllNumericSpecified ||
		!command.Options.DeduplicateSplitValuesSpecified {
		t.Fatalf("command = %#v", command)
	}

	_, err = Parse(`| stats ` + strings.Join(append(measures, `sparkline(count) AS overflow`), " "))
	if err == nil {
		t.Fatal("Parse above measure cap succeeded")
	}
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
		t.Fatalf("overflow diagnostic = %#v", err)
	}
}

func TestStatsSparklineSuggestionsUseNestedFunctionInventory(t *testing.T) {
	t.Parallel()

	root := Suggest(`| stats spa`, len(`| stats spa`), 20)
	if root.Diagnostic != nil ||
		!slices.Equal(suggestionLabels(root.Suggestions), []string{"sparkline"}) {
		t.Fatalf("root suggestions = %#v", root)
	}

	innerSource := `| stats sparkline(`
	inner := Suggest(innerSource, len(innerSource), 30)
	if inner.Diagnostic != nil ||
		inner.Context.FunctionClass != SuggestionFunctionClassAggregate ||
		!slices.Equal(inner.Context.FunctionNames, statsSparklineFunctionNames()) {
		t.Fatalf("inner context = %#v", inner.Context)
	}
	for _, excluded := range []string{"median", "distinct_count", "values", "rate"} {
		if slices.Contains(suggestionLabels(inner.Suggestions), excluded) {
			t.Errorf("inner suggestions include unsupported %q", excluded)
		}
	}

	functionSource := `| stats sparkline(av`
	function := Suggest(functionSource, len(functionSource), 20)
	if function.Diagnostic != nil ||
		!slices.Equal(suggestionLabels(function.Suggestions), []string{"avg"}) {
		t.Fatalf("function suggestions = %#v", function)
	}

	legacySource := `| stats sparkline co`
	legacy := Suggest(legacySource, len(legacySource), 20)
	if legacy.Diagnostic != nil ||
		!slices.Equal(suggestionLabels(legacy.Suggestions), []string{"count"}) {
		t.Fatalf("legacy suggestions = %#v", legacy)
	}

	fieldSource := `| stats sparkline(avg(`
	field := Suggest(fieldSource, len(fieldSource), 20)
	if field.Diagnostic != nil || !field.Context.Allows(SuggestionKindField) ||
		field.Context.Allows(SuggestionKindFunction) {
		t.Fatalf("field context = %#v", field.Context)
	}

	aliasSource := `| stats sparkline(avg(value)) A`
	alias := Suggest(aliasSource, len(aliasSource), 20)
	if alias.Diagnostic != nil ||
		!slices.Contains(suggestionLabels(alias.Suggestions), "AS") {
		t.Fatalf("alias suggestions = %#v", alias)
	}
}
