package plan

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildStatsExtendedFamiliesPreserveDistinctIRDefaultsAndSuffixes(t *testing.T) {
	t.Parallel()

	source := `index=gradethis | stats ` +
		`estdc(value) estdc_error(value) exactperc095(value) upperperc050(value) ` +
		`median(value) mode(value) first(value) last(value) earliest_time(value) ` +
		`latest_time(value) rate(value)`
	logical, err := Build(
		mustParse(t, source),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	wantOutputs := []string{
		"estdc(value)", "estdc_error(value)", "exactperc95(value)",
		"upperperc50(value)", "median(value)", "mode(value)", "first(value)",
		"last(value)", "earliest_time(value)", "latest_time(value)", "rate(value)",
	}
	if !slices.Equal(logical.OutputFields, wantOutputs) {
		t.Fatalf("outputs = %v, want %v", logical.OutputFields, wantOutputs)
	}
	aggregate := logical.Operators[len(logical.Operators)-1].(*Aggregate)
	wantFunctions := []AggregateFunction{
		AggregateFunctionEstimatedDistinctCount,
		AggregateFunctionEstimatedDistinctCountError,
		AggregateFunctionExactPercentile,
		AggregateFunctionUpperPercentile,
		AggregateFunctionMedian,
		AggregateFunctionMode,
		AggregateFunctionFirst,
		AggregateFunctionLast,
		AggregateFunctionEarliestTime,
		AggregateFunctionLatestTime,
		AggregateFunctionRate,
	}
	for index, measure := range aggregate.Measures {
		wantPercentile := uint8(0)
		switch measure.Function {
		case AggregateFunctionExactPercentile:
			wantPercentile = 95
		case AggregateFunctionUpperPercentile:
			wantPercentile = 50
		}
		if measure.Function != wantFunctions[index] ||
			measure.Input.Name != "value" ||
			measure.InputExpression != nil ||
			measure.Predicate != nil ||
			measure.Percentile != wantPercentile ||
			measure.Output != wantOutputs[index] {
			t.Errorf("measure[%d] = %#v", index, measure)
		}
	}
	analysis, analysisErr := Analyze(logical)
	if analysisErr != nil {
		t.Fatalf("Analyze: %v", analysisErr)
	}
	if !slices.Equal(analysis.ReferencedFields, []string{"_time", "index", "value"}) {
		t.Fatalf("referenced fields = %v, want _time/index/value", analysis.ReferencedFields)
	}
}

func TestBuildStatsEveryFieldTakingFamilyAcceptsScalarIR(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call         string
		function     AggregateFunction
		percentile   uint8
		requiresTime bool
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
		{call: "earliest", function: AggregateFunctionEarliest, requiresTime: true},
		{call: "latest", function: AggregateFunctionLatest, requiresTime: true},
		{call: "earliest_time", function: AggregateFunctionEarliestTime, requiresTime: true},
		{call: "latest_time", function: AggregateFunctionLatestTime, requiresTime: true},
		{call: "rate", function: AggregateFunctionRate, requiresTime: true},
	}
	for _, test := range tests {
		test := test
		t.Run(test.call, func(t *testing.T) {
			t.Parallel()

			logical, err := Build(
				mustParse(
					t,
					fmt.Sprintf(
						`index=gradethis | stats %s(eval(coalesce(value, fallback)+1)) AS result`,
						test.call,
					),
				),
				testScope([]string{"gradethis"}, nil),
			)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			measure := logical.Operators[len(logical.Operators)-1].(*Aggregate).Measures[0]
			if measure.Function != test.function ||
				measure.Percentile != test.percentile ||
				measure.Input.Name != "" || measure.Input.Canonical ||
				measure.Input.Path != nil || measure.Input.Range != (spl.Range{}) ||
				measure.InputExpression == nil ||
				measure.Predicate != nil ||
				measure.Output != "result" {
				t.Fatalf("measure = %#v", measure)
			}
			if _, ok := measure.InputExpression.(*ScalarBinaryExpression); !ok {
				t.Fatalf("input expression = %T, want *ScalarBinaryExpression", measure.InputExpression)
			}
			analysis, analysisErr := Analyze(logical)
			if analysisErr != nil {
				t.Fatalf("Analyze: %v", analysisErr)
			}
			wantFields := []string{"fallback", "index", "value"}
			if test.requiresTime {
				wantFields = []string{"_time", "fallback", "index", "value"}
			}
			if !slices.Equal(analysis.ReferencedFields, wantFields) {
				t.Fatalf("referenced fields = %v, want %v", analysis.ReferencedFields, wantFields)
			}
		})
	}
}

func TestBuildStatsBooleanScalarEvalPreservesTypedIRForValueAndNumericFamilies(t *testing.T) {
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

			logical, err := Build(
				mustParse(
					t,
					fmt.Sprintf(
						`index=gradethis | stats %s(eval(isnull(value))) AS flags`,
						test.call,
					),
				),
				testScope([]string{"gradethis"}, nil),
			)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			measure := logical.Operators[len(logical.Operators)-1].(*Aggregate).Measures[0]
			call, ok := measure.InputExpression.(*ScalarCallExpression)
			if measure.Function != test.function || measure.Predicate != nil ||
				!ok || call.Function != ScalarFunctionIsNull {
				t.Fatalf("measure = %#v, expression = %#v", measure, call)
			}
			analysis, analysisErr := Analyze(logical)
			if analysisErr != nil {
				t.Fatalf("Analyze: %v", analysisErr)
			}
			for _, field := range []string{"index", "value"} {
				if !slices.Contains(analysis.ReferencedFields, field) {
					t.Errorf("referenced fields = %v, missing %q", analysis.ReferencedFields, field)
				}
			}
		})
	}
}

func TestBuildStatsTimeFamiliesRequireCanonicalTimeButFirstAndLastDoNot(t *testing.T) {
	t.Parallel()

	for _, call := range []string{"earliest", "latest", "earliest_time", "latest_time", "rate"} {
		call := call
		for _, input := range []string{"value", "eval(value)"} {
			input := input
			t.Run(call+"/"+input, func(t *testing.T) {
				t.Parallel()

				suffix := ""
				if strings.HasPrefix(input, "eval(") {
					suffix = " AS result"
				}
				_, err := Build(
					mustParse(t, fmt.Sprintf(
						`index=gradethis | fields - _time | stats %s(%s)%s`,
						call,
						input,
						suffix,
					)),
					testScope([]string{"gradethis"}, nil),
				)
				assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_STATS_TIME_FIELD")
			})
		}
	}

	for _, call := range []string{"first", "last"} {
		for _, form := range []string{
			fmt.Sprintf("%s(value)", call),
			fmt.Sprintf("%s(eval(value)) AS result", call),
		} {
			if _, err := Build(
				mustParse(t, `index=gradethis | fields - _time | stats `+form),
				testScope([]string{"gradethis"}, nil),
			); err != nil {
				t.Errorf("Build(%s): %v", form, err)
			}
		}
	}
}

func TestBuildStatsExtendedFamiliesRejectForgedSuffixAndInputMetadata(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	sourceRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Offset: 5, Line: 1, Column: 6},
	}
	fieldExpression := func() spl.ScalarExpr {
		return &spl.ScalarFieldExpr{Field: "value", Range: sourceRange}
	}
	valid := func(function spl.AggregateFunction) spl.StatsAggregate {
		return spl.StatsAggregate{
			Function:   function,
			Input:      "value",
			InputRange: sourceRange,
			Alias:      "result",
			AliasRange: sourceRange,
			Range:      sourceRange,
		}
	}
	tests := []spl.StatsAggregate{}
	for _, function := range []spl.AggregateFunction{
		spl.AggregateFunctionExactPercentile,
		spl.AggregateFunctionUpperPercentile,
	} {
		for _, percentile := range []uint8{0, 100} {
			aggregate := valid(function)
			aggregate.Percentile = percentile
			tests = append(tests, aggregate)
		}
	}
	medianWithSuffix := valid(spl.AggregateFunctionMedian)
	medianWithSuffix.Percentile = 50
	tests = append(tests, medianWithSuffix)
	medianWithBothInputs := valid(spl.AggregateFunctionMedian)
	medianWithBothInputs.InputExpression = fieldExpression()
	medianWithBothInputs.ExplicitAlias = true
	tests = append(tests, medianWithBothInputs)
	medianWithoutInput := valid(spl.AggregateFunctionMedian)
	medianWithoutInput.Input = ""
	medianWithoutInput.InputRange = spl.Range{}
	tests = append(tests, medianWithoutInput)
	countValuesWithExpression := valid(spl.AggregateFunctionCountValues)
	countValuesWithExpression.Input = ""
	countValuesWithExpression.InputRange = spl.Range{}
	countValuesWithExpression.InputExpression = fieldExpression()
	countValuesWithExpression.ExplicitAlias = true
	tests = append(tests, countValuesWithExpression)
	modeWithoutExplicitAlias := valid(spl.AggregateFunctionMode)
	modeWithoutExplicitAlias.Input = ""
	modeWithoutExplicitAlias.InputRange = spl.Range{}
	modeWithoutExplicitAlias.InputExpression = fieldExpression()
	tests = append(tests, modeWithoutExplicitAlias)
	var typedNil *spl.ScalarFieldExpr
	modeWithTypedNil := valid(spl.AggregateFunctionMode)
	modeWithTypedNil.Input = ""
	modeWithTypedNil.InputRange = spl.Range{}
	modeWithTypedNil.InputExpression = typedNil
	modeWithTypedNil.ExplicitAlias = true
	tests = append(tests, modeWithTypedNil)

	for index, aggregate := range tests {
		query := &spl.Query{
			Search: base.Search,
			Commands: []spl.Command{&spl.StatsCommand{
				Aggregates: []spl.StatsAggregate{aggregate},
				Range:      sourceRange,
			}},
			Range: base.Range,
		}
		_, err := Build(query, testScope([]string{"gradethis"}, nil))
		if err == nil {
			t.Fatalf("case %d Build succeeded for forged aggregate %#v", index, aggregate)
		}
		assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_STATS_AGGREGATE")
	}
}

func TestAnalyzeStatsExtendedFamiliesRejectForgedSuffixMetadata(t *testing.T) {
	t.Parallel()

	input := mustResolveEventAggregateField(t, "value")
	scalarInput := &ScalarFieldExpression{Field: input}
	var typedNil *ScalarFieldExpression
	forgedPath := input
	forgedPath.Path = []string{"attacker"}
	forgedCanonical := input
	forgedCanonical.Canonical = true
	invalid := []AggregateMeasure{
		{Function: AggregateFunctionExactPercentile, Input: input, Output: "result"},
		{Function: AggregateFunctionExactPercentile, Input: input, Percentile: 100, Output: "result"},
		{Function: AggregateFunctionUpperPercentile, Input: input, Output: "result"},
		{Function: AggregateFunctionUpperPercentile, Input: input, Percentile: 100, Output: "result"},
		{Function: AggregateFunctionMedian, Input: input, Percentile: 50, Output: "result"},
		{Function: AggregateFunctionCountValues, InputExpression: scalarInput, Output: "result"},
		{Function: AggregateFunctionMode, InputExpression: typedNil, Output: "result"},
		{Function: AggregateFunctionMode, Input: forgedPath, Output: "result"},
		{Function: AggregateFunctionMode, Input: forgedCanonical, Output: "result"},
		{Function: AggregateFunctionMode, Input: input, Output: "__os_private"},
		{Function: AggregateFunctionInvalid, Input: input, Output: "result"},
	}
	for index, measure := range invalid {
		_, err := Analyze(&Query{Operators: []Operator{&Aggregate{
			Measures: []AggregateMeasure{measure},
		}}})
		if err == nil {
			t.Fatalf("case %d Analyze succeeded for forged measure %#v", index, measure)
		}
	}
}

func TestAnalyzeStatsAggregateRevalidatesResourceAndCollisionContracts(t *testing.T) {
	t.Parallel()

	tooManyMeasures := make([]AggregateMeasure, spl.MaximumStatsMeasures+1)
	for index := range tooManyMeasures {
		tooManyMeasures[index] = AggregateMeasure{
			Function: AggregateFunctionCountRows,
			Output:   fmt.Sprintf("count_%d", index),
		}
	}
	tooManyGroups := make([]FieldRef, spl.MaximumStatsGroupFields+1)
	for index := range tooManyGroups {
		tooManyGroups[index] = mustResolveEventAggregateField(
			t,
			fmt.Sprintf("group_%d", index),
		)
	}
	group := mustResolveEventAggregateField(t, "group")
	for _, operator := range []*Aggregate{
		{},
		{Measures: tooManyMeasures},
		{GroupBy: tooManyGroups, Measures: []AggregateMeasure{{Function: AggregateFunctionCountRows, Output: "count"}}},
		{Measures: []AggregateMeasure{
			{Function: AggregateFunctionCountRows, Output: "count"},
			{Function: AggregateFunctionCountRows, Output: "count"},
		}},
		{GroupBy: []FieldRef{group, group}, Measures: []AggregateMeasure{{Function: AggregateFunctionCountRows, Output: "count"}}},
		{GroupBy: []FieldRef{group}, Measures: []AggregateMeasure{{Function: AggregateFunctionCountRows, Output: "group"}}},
	} {
		if _, err := Analyze(&Query{Operators: []Operator{operator}}); err == nil {
			t.Fatalf("Analyze accepted forged aggregate %#v", operator)
		}
	}
}

func TestBuildStatsExtendedEvalPreservesReservedOpenSchemaBoundary(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | stats mode(eval(coalesce(fields, "missing"))) AS result`,
		`index=gradethis | stats estdc(fields)`,
	} {
		_, err := Build(
			mustParse(t, source),
			testScope([]string{"gradethis"}, nil),
		)
		assertDiagnosticCode(t, err, "SPL_AMBIGUOUS_STATS_FIELD")
	}

	if _, err := Build(
		mustParse(t, `index=gradethis | stats count AS fields | stats mode(eval(fields)) AS result`),
		testScope([]string{"gradethis"}, nil),
	); err != nil {
		t.Fatalf("Build with closed schema: %v", err)
	}
}

func TestBuildRelatedCommandsRejectStatsOnlyExtendedEnums(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	sourceRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Offset: 5, Line: 1, Column: 6},
	}
	aggregate := spl.StatsAggregate{
		Function:   spl.AggregateFunctionMedian,
		Input:      "value",
		InputRange: sourceRange,
		Alias:      "result",
		AliasRange: sourceRange,
		Range:      sourceRange,
	}
	commands := []struct {
		command spl.Command
		code    string
	}{
		{
			command: &spl.EventStatsCommand{Aggregate: aggregate, Range: sourceRange},
			code:    "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
		},
		{
			command: &spl.StreamStatsCommand{
				Aggregate: aggregate,
				Current:   true,
				Global:    true,
				Range:     sourceRange,
			},
			code: "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE",
		},
		{
			command: &spl.TimechartCommand{Aggregate: aggregate, Range: sourceRange},
			code:    "SPL_UNSUPPORTED_TIMECHART_AGGREGATE",
		},
		{
			command: &spl.ChartCommand{Aggregate: aggregate, Range: sourceRange},
			code:    "SPL_UNSUPPORTED_CHART_AGGREGATE",
		},
	}
	for _, test := range commands {
		query := &spl.Query{
			Search:   base.Search,
			Commands: []spl.Command{test.command},
			Range:    base.Range,
		}
		_, err := Build(query, testScope([]string{"gradethis"}, nil))
		if err == nil {
			t.Fatalf("Build(%T) succeeded for stats-only function", test.command)
		}
		var diagnostic *Diagnostic
		if !errors.As(err, &diagnostic) || diagnostic.Code != test.code {
			t.Fatalf("Build(%T) diagnostic = %#v, want %s", test.command, err, test.code)
		}
	}
}
