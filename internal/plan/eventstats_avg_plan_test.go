package plan

import (
	"errors"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildEventStatsAverageProducesSingularRowPreservingMeasure(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | eventstats avg(http.duration) AS mean_duration BY host, status`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	eventAggregate, ok := logical.Operators[len(logical.Operators)-1].(*EventAggregate)
	if !ok {
		t.Fatalf("last operator = %T, want *EventAggregate", logical.Operators[len(logical.Operators)-1])
	}
	measure := eventAggregate.Measure
	if eventAggregate.LogicalName() != "EventAggregate" ||
		eventAggregate.SourceRange() != eventAggregate.Range ||
		measure.Function != AggregateFunctionAverage ||
		measure.Input.Name != "http.duration" ||
		measure.Input.Canonical ||
		!slices.Equal(measure.Input.Path, []string{"http", "duration"}) ||
		measure.Input.Range == (spl.Range{}) ||
		measure.Predicate != nil ||
		measure.Percentile != 0 ||
		measure.Output != "mean_duration" {
		t.Fatalf("event aggregate = %#v", eventAggregate)
	}
	if len(eventAggregate.GroupBy) != 2 ||
		eventAggregate.GroupBy[0].Name != "host" ||
		eventAggregate.GroupBy[1].Name != "status" {
		t.Fatalf("event aggregate groups = %#v, want resolved host/status", eventAggregate.GroupBy)
	}

	analysis, err := Analyze(logical)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if !slices.Equal(
		analysis.ReferencedFields,
		[]string{"host", "http.duration", "index", "status"},
	) {
		t.Fatalf("referenced fields = %v", analysis.ReferencedFields)
	}
	if err := ValidateFieldAnalysisEligibility(logical); err != nil {
		t.Fatalf("field analysis eligibility: %v", err)
	}
	if err := ValidateTimelineEligibility(logical); err != nil {
		t.Fatalf("timeline eligibility: %v", err)
	}
}

func TestBuildEventStatsAverageUsesSharedFieldAndOutputContract(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | table _time,mean_ms,host,duration_ms`+
				` | eventstats avg(duration_ms) AS mean_ms BY host`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build known schema: %v", err)
	}
	if !slices.Equal(
		logical.OutputFields,
		[]string{"_time", "mean_ms", "host", "duration_ms"},
	) {
		t.Fatalf("output fields = %v", logical.OutputFields)
	}

	_, err = Build(
		mustParse(t, `index=gradethis | eventstats avg(fields) AS mean`),
		testScope([]string{"gradethis"}, nil),
	)
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) ||
		diagnostic.Code != "SPL_AMBIGUOUS_EVENTSTATS_FIELD" ||
		diagnostic.Range == (spl.Range{}) {
		t.Fatalf("reserved fields error = %#v, want source-located diagnostic", err)
	}

	overwrittenTime, err := Build(
		mustParse(t, `index=gradethis | eventstats avg(duration_ms) AS _time`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build _time replacement: %v", err)
	}
	assertDiagnosticCode(
		t,
		ValidateTimelineEligibility(overwrittenTime),
		"SPL_UNSUPPORTED_TIMELINE_TIME_FIELD",
	)
}

func TestAnalyzeEventStatsAverageAcceptsResolvedInputAndRejectsForgedMetadata(
	t *testing.T,
) {
	t.Parallel()

	input := mustResolveEventAggregateField(t, "http.duration")
	valid := AggregateMeasure{
		Function: AggregateFunctionAverage,
		Input:    input,
		Output:   "mean_duration",
	}
	analysis, err := Analyze(&Query{Operators: []Operator{&EventAggregate{
		GroupBy: []FieldRef{mustResolveEventAggregateField(t, "host")},
		Measure: valid,
	}}})
	if err != nil {
		t.Fatalf("Analyze valid average: %v", err)
	}
	if !slices.Equal(analysis.ReferencedFields, []string{"host", "http.duration"}) {
		t.Fatalf("referenced fields = %v", analysis.ReferencedFields)
	}

	for _, test := range []struct {
		name   string
		mutate func(*AggregateMeasure)
	}{
		{"missing input", func(measure *AggregateMeasure) { measure.Input = FieldRef{} }},
		{"forged path", func(measure *AggregateMeasure) { measure.Input.Path = []string{"attacker"} }},
		{"predicate", func(measure *AggregateMeasure) { measure.Predicate = &ComparisonExpression{} }},
		{"private output", func(measure *AggregateMeasure) { measure.Output = "__os_eventstats_private" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			measure := valid
			test.mutate(&measure)
			query := &Query{Operators: []Operator{
				&Scan{},
				&EventAggregate{Measure: measure},
			}}
			if _, err := Analyze(query); err == nil {
				t.Fatalf("Analyze accepted forged average measure %#v", measure)
			}
			if err := ValidateFieldAnalysisEligibility(query); err == nil {
				t.Fatalf("field analysis accepted forged average measure %#v", measure)
			}
			if err := ValidateTimelineEligibility(query); err == nil {
				t.Fatalf("timeline accepted forged average measure %#v", measure)
			}
		})
	}
}
