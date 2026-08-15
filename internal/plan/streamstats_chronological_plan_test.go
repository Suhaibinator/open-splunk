package plan

import (
	"errors"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildStreamStatsChronologicalProducesResolvedOperators(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name           string
		source         string
		function       AggregateFunction
		input          string
		output         string
		includeCurrent bool
		windowRows     uint64
		global         bool
		groups         []string
	}{
		{
			name:           "earliest canonical output",
			source:         `index=gradethis | streamstats earliest(Payload.Value)`,
			function:       AggregateFunctionEarliest,
			input:          "Payload.Value",
			output:         "earliest(Payload.Value)",
			includeCurrent: true,
			global:         true,
		},
		{
			name:       "latest bounded prior group",
			source:     `index=gradethis | streamstats current=f window=2 global=f latest(value) AS prior_latest BY service`,
			function:   AggregateFunctionLatest,
			input:      "value",
			output:     "prior_latest",
			windowRows: 2,
			groups:     []string{"service"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical, err := Build(
				mustParse(t, test.source),
				testScope([]string{"gradethis"}, nil),
			)
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
				t.Fatalf("streamstats chronological operator = %#v", logical.Operators[len(logical.Operators)-1])
			}
			if len(operator.GroupBy) != len(test.groups) {
				t.Fatalf("groups = %#v, want %v", operator.GroupBy, test.groups)
			}
			for index, want := range test.groups {
				if operator.GroupBy[index].Name != want ||
					operator.GroupBy[index].Range == (spl.Range{}) {
					t.Fatalf("group %d = %#v, want resolved %q", index, operator.GroupBy[index], want)
				}
			}

			analysis, analyzeErr := Analyze(logical)
			if analyzeErr != nil {
				t.Fatalf("Analyze: %v", analyzeErr)
			}
			wantReferences := append([]string{"_time", "index", test.input}, test.groups...)
			slices.Sort(wantReferences)
			wantReferences = slices.Compact(wantReferences)
			if !slices.Equal(analysis.ReferencedFields, wantReferences) {
				t.Fatalf("referenced fields = %v, want %v", analysis.ReferencedFields, wantReferences)
			}
		})
	}
}

func TestBuildStreamStatsChronologicalRequiresCanonicalTime(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | fields - _time | streamstats earliest(status)`,
		`index=gradethis | table host,status | streamstats latest(status)`,
		`index=gradethis | eval _time=1 | streamstats earliest(status)`,
		`index=gradethis | rex "(?<_time>\d+)" | streamstats latest(status)`,
		`index=gradethis | spath input=_raw output=_time path=observed_at | streamstats earliest(status)`,
		`index=gradethis | rename _time AS observed_at | streamstats latest(status)`,
		`index=gradethis | bin _time span=5m | streamstats earliest(status)`,
		`index=gradethis | stats count BY host | streamstats latest(host)`,
		`index=gradethis | streamstats count AS _time | streamstats earliest(status)`,
	} {
		query := mustParse(t, source)
		_, err := Build(query, testScope([]string{"gradethis"}, nil))
		assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_STREAMSTATS_TIME_FIELD")
		var diagnostic *Diagnostic
		if !errors.As(err, &diagnostic) {
			t.Fatalf("Build(%q) error = %T, want *Diagnostic", source, err)
		}
		command := query.Commands[len(query.Commands)-1].(*spl.StreamStatsCommand)
		if diagnostic.Range != command.Aggregate.Range {
			t.Errorf(
				"Build(%q) diagnostic range = %#v, want aggregate %#v",
				source,
				diagnostic.Range,
				command.Aggregate.Range,
			)
		}
		if !slices.Contains(
			diagnostic.Suggestions,
			"run streamstats earliest or latest before removing, replacing, or transforming _time",
		) {
			t.Errorf("Build(%q) suggestions = %v", source, diagnostic.Suggestions)
		}
	}
}

func TestBuildStreamStatsChronologicalAcceptsEventPreservingPipelines(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis level=error | streamstats earliest(status)`,
		`index=gradethis | search level=error | where status>=500 | streamstats latest(status)`,
		`index=gradethis | sort -_time | head 20 | streamstats earliest(status)`,
		`index=gradethis | sort _time | tail 20 | streamstats latest(status)`,
		`index=gradethis | dedup 2 host | streamstats earliest(status) BY host`,
		`index=gradethis | table _time,host,status | streamstats latest(status) BY host`,
		`index=gradethis | bin _time span=5m AS bucket_time | streamstats earliest(status)`,
	} {
		if _, err := Build(
			mustParse(t, source),
			testScope([]string{"gradethis"}, nil),
		); err != nil {
			t.Errorf("Build(%q): %v", source, err)
		}
	}
}

func TestBuildStreamStatsChronologicalRejectsForgedASTMetadata(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	fieldRange := spl.Range{
		Start: spl.Position{Offset: 1, Line: 1, Column: 2},
		End:   spl.Position{Offset: 8, Line: 1, Column: 9},
	}
	for _, testFunction := range []struct {
		name     string
		function spl.AggregateFunction
	}{
		{name: "earliest", function: spl.AggregateFunctionEarliest},
		{name: "latest", function: spl.AggregateFunctionLatest},
	} {
		for _, test := range []struct {
			name   string
			mutate func(*spl.StatsAggregate)
		}{
			{"missing input", func(aggregate *spl.StatsAggregate) { aggregate.Input = "" }},
			{"missing input range", func(aggregate *spl.StatsAggregate) { aggregate.InputRange = spl.Range{} }},
			{"predicate metadata", func(aggregate *spl.StatsAggregate) { aggregate.Predicate = &spl.WhereComparisonExpr{} }},
			{"percentile metadata", func(aggregate *spl.StatsAggregate) { aggregate.Percentile = 95 }},
			{"forged default", func(aggregate *spl.StatsAggregate) { aggregate.Alias = testFunction.name + "(other)" }},
		} {
			t.Run(testFunction.name+"/"+test.name, func(t *testing.T) {
				t.Parallel()

				aggregate := spl.StatsAggregate{
					Function:   testFunction.function,
					Input:      "value",
					InputRange: fieldRange,
					Alias:      testFunction.name + "(value)",
					Range:      fieldRange,
					AliasRange: fieldRange,
				}
				test.mutate(&aggregate)
				_, err := Build(&spl.Query{
					Search: base.Search,
					Commands: []spl.Command{&spl.StreamStatsCommand{
						Aggregate: aggregate,
						Current:   true,
						Global:    true,
						Range:     fieldRange,
					}},
					Range: base.Range,
				}, testScope([]string{"gradethis"}, nil))
				assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE")
			})
		}
	}
}

func TestStreamStatsChronologicalPlanContractRejectsForgery(t *testing.T) {
	t.Parallel()

	input := mustResolveStreamStatsField(t, "value")
	host := mustResolveStreamStatsField(t, "host")
	for _, testFunction := range []struct {
		name     string
		function AggregateFunction
	}{
		{name: "earliest", function: AggregateFunctionEarliest},
		{name: "latest", function: AggregateFunctionLatest},
	} {
		t.Run(testFunction.name, func(t *testing.T) {
			t.Parallel()

			valid := func() *StreamAggregate {
				return &StreamAggregate{
					GroupBy: []FieldRef{host},
					Measure: AggregateMeasure{
						Function: testFunction.function,
						Input:    input,
						Output:   testFunction.name + "(value)",
					},
					IncludeCurrent: true,
					Global:         true,
				}
			}
			validQuery := &Query{Operators: []Operator{&Scan{}, valid()}}
			analysis, err := Analyze(validQuery)
			if err != nil {
				t.Fatalf("Analyze valid chronological stream aggregate: %v", err)
			}
			if !slices.Equal(analysis.ReferencedFields, []string{"_time", "host", "value"}) {
				t.Fatalf("referenced fields = %v", analysis.ReferencedFields)
			}
			if err := ValidateFieldAnalysisEligibility(validQuery); err != nil {
				t.Fatalf("field analysis: %v", err)
			}
			if err := ValidateTimelineEligibility(validQuery); err != nil {
				t.Fatalf("timeline: %v", err)
			}

			for _, test := range []struct {
				name   string
				mutate func(*StreamAggregate)
			}{
				{"missing input", func(operator *StreamAggregate) { operator.Measure.Input = FieldRef{} }},
				{"unresolved input", func(operator *StreamAggregate) { operator.Measure.Input = FieldRef{Name: input.Name} }},
				{"forged path", func(operator *StreamAggregate) { operator.Measure.Input.Path = []string{"attacker"} }},
				{"canonical input", func(operator *StreamAggregate) { operator.Measure.Input.Canonical = true }},
				{"wrong default", func(operator *StreamAggregate) { operator.Measure.Output = testFunction.name + "(other)" }},
				{"predicate", func(operator *StreamAggregate) { operator.Measure.Predicate = &ComparisonExpression{} }},
				{"percentile", func(operator *StreamAggregate) { operator.Measure.Percentile = 50 }},
			} {
				t.Run(test.name, func(t *testing.T) {
					t.Parallel()

					operator := valid()
					test.mutate(operator)
					query := &Query{Operators: []Operator{&Scan{}, operator}}
					for _, validator := range []struct {
						name string
						fn   func(*Query) error
					}{
						{"analyze", func(query *Query) error { _, err := Analyze(query); return err }},
						{"field analysis", ValidateFieldAnalysisEligibility},
						{"timeline", ValidateTimelineEligibility},
					} {
						if err := validator.fn(query); err == nil {
							t.Fatalf("%s accepted forged operator %#v", validator.name, operator)
						}
					}
				})
			}
		})
	}
}
