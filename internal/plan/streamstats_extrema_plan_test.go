package plan

import (
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildStreamStatsExtremaProduceResolvedRowPreservingOperators(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name           string
		function       AggregateFunction
		source         string
		input          string
		output         string
		includeCurrent bool
		windowRows     uint64
		global         bool
		groups         []string
	}{
		{
			name: "minimum canonical", function: AggregateFunctionMinimum,
			source: `index=gradethis | streamstats min(Payload.Value)`, input: "Payload.Value",
			output: "min(Payload.Value)", includeCurrent: true, global: true,
		},
		{
			name: "minimum prior grouped", function: AggregateFunctionMinimum,
			source: `index=gradethis | streamstats current=f window=2 global=f min(value) AS prior_min BY service`,
			input:  "value", output: "prior_min", windowRows: 2, groups: []string{"service"},
		},
		{
			name: "maximum canonical", function: AggregateFunctionMaximum,
			source: `index=gradethis | streamstats max(Payload.Value)`, input: "Payload.Value",
			output: "max(Payload.Value)", includeCurrent: true, global: true,
		},
		{
			name: "maximum prior grouped", function: AggregateFunctionMaximum,
			source: `index=gradethis | streamstats current=f window=2 global=f max(value) AS prior_max BY service`,
			input:  "value", output: "prior_max", windowRows: 2, groups: []string{"service"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical, err := Build(mustParse(t, test.source), testScope([]string{"gradethis"}, nil))
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			operator, ok := logical.Operators[len(logical.Operators)-1].(*StreamAggregate)
			if !ok || operator.Measure.Function != test.function ||
				operator.Measure.Input.Name != test.input ||
				operator.Measure.Input.Range == (spl.Range{}) ||
				len(operator.Measure.Input.Path) == 0 ||
				operator.Measure.Output != test.output ||
				operator.IncludeCurrent != test.includeCurrent ||
				operator.WindowRows != test.windowRows ||
				operator.Global != test.global {
				t.Fatalf("streamstats extremum operator = %#v", logical.Operators[len(logical.Operators)-1])
			}
			if len(operator.GroupBy) != len(test.groups) {
				t.Fatalf("groups = %#v, want %v", operator.GroupBy, test.groups)
			}
			for index, want := range test.groups {
				if operator.GroupBy[index].Name != want || operator.GroupBy[index].Range == (spl.Range{}) {
					t.Fatalf("group %d = %#v, want resolved %q", index, operator.GroupBy[index], want)
				}
			}

			analysis, analyzeErr := Analyze(logical)
			if analyzeErr != nil {
				t.Fatalf("Analyze: %v", analyzeErr)
			}
			wantReferences := append([]string{"index", test.input}, test.groups...)
			slices.Sort(wantReferences)
			wantReferences = slices.Compact(wantReferences)
			if !slices.Equal(analysis.ReferencedFields, wantReferences) {
				t.Fatalf("referenced fields = %v, want %v", analysis.ReferencedFields, wantReferences)
			}
		})
	}
}

func TestBuildStreamStatsExtremaPreserveReplacementAndCanonicalTime(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name, replacementSource, ordinaryTimeSource, replacedTimeSource string
	}{
		{
			name:               "minimum",
			replacementSource:  `index=gradethis | table _time,value,service | streamstats min(value) AS value BY service`,
			ordinaryTimeSource: `index=gradethis | streamstats min(value) AS running_min | timechart span=5m count BY level`,
			replacedTimeSource: `index=gradethis | streamstats min(value) AS _time | timechart span=5m count BY level`,
		},
		{
			name:               "maximum",
			replacementSource:  `index=gradethis | table _time,value,service | streamstats max(value) AS value BY service`,
			ordinaryTimeSource: `index=gradethis | streamstats max(value) AS running_max | timechart span=5m count BY level`,
			replacedTimeSource: `index=gradethis | streamstats max(value) AS _time | timechart span=5m count BY level`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical, err := Build(mustParse(t, test.replacementSource), testScope([]string{"gradethis"}, nil))
			if err != nil {
				t.Fatalf("Build alias replacement: %v", err)
			}
			if !slices.Equal(logical.OutputFields, []string{"_time", "value", "service"}) {
				t.Fatalf("output fields = %v", logical.OutputFields)
			}
			operator := logical.Operators[len(logical.Operators)-1].(*StreamAggregate)
			if operator.Measure.Input.Name != "value" || operator.Measure.Output != "value" {
				t.Fatalf("alias replacement operator = %#v", operator)
			}

			if _, err := Build(mustParse(t, test.ordinaryTimeSource), testScope([]string{"gradethis"}, nil)); err != nil {
				t.Fatalf("ordinary output invalidated canonical time: %v", err)
			}
			_, err = Build(mustParse(t, test.replacedTimeSource), testScope([]string{"gradethis"}, nil))
			assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_TIMECHART_TIME_FIELD")
		})
	}
}

func TestBuildStreamStatsExtremaProtectOpenFieldsPayload(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name, functionName, alias string
	}{
		{name: "minimum", functionName: "min", alias: "low"},
		{name: "maximum", functionName: "max", alias: "high"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			for _, source := range []string{
				`index=gradethis | streamstats ` + test.functionName + `(fields) AS ` + test.alias,
				`index=gradethis | streamstats ` + test.functionName + `(value) AS fields`,
				`index=gradethis | streamstats ` + test.functionName + `(value) AS ` + test.alias + ` BY fields`,
			} {
				_, err := Build(mustParse(t, source), testScope([]string{"gradethis"}, nil))
				assertDiagnosticCode(t, err, "SPL_AMBIGUOUS_STREAMSTATS_FIELD")
			}

			closed, err := Build(
				mustParse(t, `index=gradethis | table fields,host | streamstats `+test.functionName+`(fields) AS `+test.alias+` BY host`),
				testScope([]string{"gradethis"}, nil),
			)
			if err != nil {
				t.Fatalf("Build closed fields: %v", err)
			}
			if !slices.Equal(closed.OutputFields, []string{"fields", "host", test.alias}) {
				t.Fatalf("closed fields output = %v", closed.OutputFields)
			}
		})
	}
}

func TestBuildStreamStatsMaximumRejectsForgedASTMetadata(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	fieldRange := spl.Range{
		Start: spl.Position{Offset: 1, Line: 1, Column: 2},
		End:   spl.Position{Offset: 8, Line: 1, Column: 9},
	}
	valid := spl.StatsAggregate{
		Function: spl.AggregateFunctionMaximum, Input: "value", InputRange: fieldRange,
		Alias: "max(value)", Range: fieldRange, AliasRange: fieldRange,
	}
	build := func(aggregate spl.StatsAggregate) error {
		_, err := Build(&spl.Query{
			Search: base.Search,
			Commands: []spl.Command{&spl.StreamStatsCommand{
				Aggregate: aggregate, Current: true, Global: true, Range: fieldRange,
			}},
			Range: base.Range,
		}, testScope([]string{"gradethis"}, nil))
		return err
	}

	for _, test := range []struct {
		name     string
		mutate   func(*spl.StatsAggregate)
		wantCode string
	}{
		{"missing input", func(aggregate *spl.StatsAggregate) { aggregate.Input = "" }, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE"},
		{"missing input range", func(aggregate *spl.StatsAggregate) { aggregate.InputRange = spl.Range{} }, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE"},
		{"predicate metadata", func(aggregate *spl.StatsAggregate) { aggregate.Predicate = &spl.WhereComparisonExpr{} }, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE"},
		{"percentile metadata", func(aggregate *spl.StatsAggregate) { aggregate.Percentile = 99 }, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE"},
		{"forged canonical default", func(aggregate *spl.StatsAggregate) { aggregate.Alias = "max(other)" }, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE"},
		{"invalid explicit output", func(aggregate *spl.StatsAggregate) {
			aggregate.Alias, aggregate.ExplicitAlias = "high value", true
		}, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX"},
		{"reserved explicit output", func(aggregate *spl.StatsAggregate) {
			aggregate.Alias, aggregate.ExplicitAlias = "__os_streamstats_private", true
		}, "SPL_RESERVED_FIELD"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			aggregate := valid
			test.mutate(&aggregate)
			assertDiagnosticCode(t, build(aggregate), test.wantCode)
		})
	}
}

func TestAnalyzeStreamStatsExtremaAcceptOnlyCanonicalContracts(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name, output, otherOutput, otherFieldOutput string
		function                                    AggregateFunction
	}{
		{
			name: "minimum", function: AggregateFunctionMinimum, output: "min(value)",
			otherOutput: "max(value)", otherFieldOutput: "min(other)",
		},
		{
			name: "maximum", function: AggregateFunctionMaximum, output: "max(value)",
			otherOutput: "min(value)", otherFieldOutput: "max(other)",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			input := mustResolveStreamStatsField(t, "value")
			valid := func() *StreamAggregate {
				return &StreamAggregate{
					Measure:        AggregateMeasure{Function: test.function, Input: input, Output: test.output},
					IncludeCurrent: true,
					Global:         true,
				}
			}
			analysis, err := Analyze(&Query{Operators: []Operator{valid()}})
			if err != nil {
				t.Fatalf("Analyze valid extremum: %v", err)
			}
			if !slices.Equal(analysis.ReferencedFields, []string{"value"}) {
				t.Fatalf("references = %v", analysis.ReferencedFields)
			}

			for _, mutate := range []func(*StreamAggregate){
				func(operator *StreamAggregate) { operator.Measure.Input = FieldRef{} },
				func(operator *StreamAggregate) {
					operator.Measure.Input = mustResolveStreamStatsField(t, "value,other")
				},
				func(operator *StreamAggregate) { operator.Measure.Output = test.otherOutput },
				func(operator *StreamAggregate) { operator.Measure.Output = test.otherFieldOutput },
				func(operator *StreamAggregate) { operator.Measure.Predicate = &ComparisonExpression{} },
				func(operator *StreamAggregate) { operator.Measure.Percentile = 50 },
			} {
				operator := valid()
				mutate(operator)
				if _, err := Analyze(&Query{Operators: []Operator{operator}}); err == nil {
					t.Fatalf("Analyze accepted forged extremum contract: %#v", operator)
				}
			}
		})
	}
}
