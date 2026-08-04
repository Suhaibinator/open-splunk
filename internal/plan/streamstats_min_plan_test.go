package plan

import (
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildStreamStatsMinimumProducesResolvedRowPreservingOperator(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name           string
		source         string
		input          string
		output         string
		includeCurrent bool
		windowRows     uint64
		global         bool
		groups         []string
	}{
		{
			name:           "canonical output",
			source:         `index=gradethis | streamstats min(Payload.Value)`,
			input:          "Payload.Value",
			output:         "min(Payload.Value)",
			includeCurrent: true,
			global:         true,
		},
		{
			name:           "prior grouped row window",
			source:         `index=gradethis | streamstats current=f window=2 global=f min(value) AS prior_min BY service`,
			input:          "value",
			output:         "prior_min",
			includeCurrent: false,
			windowRows:     2,
			groups:         []string{"service"},
		},
	} {
		test := test
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
			if !ok || operator.Measure.Function != AggregateFunctionMinimum ||
				operator.Measure.Input.Name != test.input ||
				operator.Measure.Input.Range == (spl.Range{}) ||
				len(operator.Measure.Input.Path) == 0 ||
				operator.Measure.Output != test.output ||
				operator.IncludeCurrent != test.includeCurrent ||
				operator.WindowRows != test.windowRows ||
				operator.Global != test.global {
				t.Fatalf("streamstats minimum operator = %#v", logical.Operators[len(logical.Operators)-1])
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

func TestBuildStreamStatsMinimumResolvesInputBeforeReplacementAndPreservesSchema(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | table _time,value,service | streamstats min(value) AS value BY service`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build alias replacement: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"_time", "value", "service"}) {
		t.Fatalf("output fields = %v", logical.OutputFields)
	}
	operator := logical.Operators[len(logical.Operators)-1].(*StreamAggregate)
	if operator.Measure.Input.Name != "value" || operator.Measure.Output != "value" {
		t.Fatalf("minimum alias replacement operator = %#v", operator)
	}

	if _, err := Build(
		mustParse(t, `index=gradethis | streamstats min(value) AS running_min | timechart span=5m count BY level`),
		testScope([]string{"gradethis"}, nil),
	); err != nil {
		t.Fatalf("ordinary minimum output invalidated canonical time: %v", err)
	}
	_, err = Build(
		mustParse(t, `index=gradethis | streamstats min(value) AS _time | timechart span=5m count BY level`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_TIMECHART_TIME_FIELD")
}

func TestBuildStreamStatsMinimumProtectsOpenFieldsPayload(t *testing.T) {
	t.Parallel()

	_, err := Build(
		mustParse(t, `index=gradethis | streamstats min(fields) AS low`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_AMBIGUOUS_STREAMSTATS_FIELD")

	closed, err := Build(
		mustParse(t, `index=gradethis | table fields,host | streamstats min(fields) AS low BY host`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build closed fields minimum: %v", err)
	}
	if !slices.Equal(closed.OutputFields, []string{"fields", "host", "low"}) {
		t.Fatalf("closed fields output = %v", closed.OutputFields)
	}
}

func TestAnalyzeStreamStatsMinimumAcceptsOnlyCanonicalContract(t *testing.T) {
	t.Parallel()

	input := mustResolveStreamStatsField(t, "value")
	valid := func() *StreamAggregate {
		return &StreamAggregate{
			Measure: AggregateMeasure{
				Function: AggregateFunctionMinimum,
				Input:    input,
				Output:   "min(value)",
			},
			IncludeCurrent: true,
			Global:         true,
		}
	}
	analysis, err := Analyze(&Query{Operators: []Operator{valid()}})
	if err != nil {
		t.Fatalf("Analyze valid minimum: %v", err)
	}
	if !slices.Equal(analysis.ReferencedFields, []string{"value"}) {
		t.Fatalf("minimum references = %v", analysis.ReferencedFields)
	}

	for _, mutate := range []func(*StreamAggregate){
		func(operator *StreamAggregate) { operator.Measure.Input = FieldRef{} },
		func(operator *StreamAggregate) {
			operator.Measure.Input = mustResolveStreamStatsField(t, "value,other")
		},
		func(operator *StreamAggregate) { operator.Measure.Output = "min(other)" },
		func(operator *StreamAggregate) { operator.Measure.Predicate = &ComparisonExpression{} },
		func(operator *StreamAggregate) { operator.Measure.Percentile = 50 },
	} {
		operator := valid()
		mutate(operator)
		if _, err := Analyze(&Query{Operators: []Operator{operator}}); err == nil {
			t.Fatalf("Analyze accepted forged minimum contract: %#v", operator)
		}
	}
}
