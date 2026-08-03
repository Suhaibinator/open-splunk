package plan

import (
	"errors"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildEventStatsDistinctCountProducesSingularRowPreservingMeasure(
	t *testing.T,
) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | eventstats dc(http.user) AS unique_users BY host, status`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	eventAggregate, ok := logical.Operators[len(logical.Operators)-1].(*EventAggregate)
	if !ok {
		t.Fatalf(
			"last operator = %T, want *EventAggregate",
			logical.Operators[len(logical.Operators)-1],
		)
	}
	measure := eventAggregate.Measure
	if eventAggregate.LogicalName() != "EventAggregate" ||
		eventAggregate.SourceRange() != eventAggregate.Range ||
		measure.Function != AggregateFunctionDistinctCount ||
		measure.Input.Name != "http.user" ||
		measure.Input.Canonical ||
		!slices.Equal(measure.Input.Path, []string{"http", "user"}) ||
		measure.Input.Range == (spl.Range{}) ||
		measure.Predicate != nil ||
		measure.Percentile != 0 ||
		measure.Output != "unique_users" {
		t.Fatalf("event aggregate = %#v", eventAggregate)
	}
	if len(eventAggregate.GroupBy) != 2 ||
		eventAggregate.GroupBy[0].Name != "host" ||
		eventAggregate.GroupBy[1].Name != "status" {
		t.Fatalf("event aggregate groups = %#v, want host/status", eventAggregate.GroupBy)
	}

	analysis, err := Analyze(logical)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if !slices.Equal(
		analysis.ReferencedFields,
		[]string{"host", "http.user", "index", "status"},
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

func TestBuildEventStatsDistinctCountRejectsForgedAggregateMetadata(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	fieldRange := spl.Range{
		Start: spl.Position{Offset: 1, Line: 1, Column: 2},
		End:   spl.Position{Offset: 7, Line: 1, Column: 8},
	}
	valid := spl.StatsAggregate{
		Function:      spl.AggregateFunctionDistinctCount,
		Input:         "user",
		InputRange:    fieldRange,
		Alias:         "unique_users",
		ExplicitAlias: true,
		Range:         fieldRange,
		AliasRange:    fieldRange,
	}
	for _, test := range []struct {
		name   string
		mutate func(*spl.StatsAggregate)
	}{
		{"missing input", func(aggregate *spl.StatsAggregate) { aggregate.Input = "" }},
		{"missing input range", func(aggregate *spl.StatsAggregate) { aggregate.InputRange = spl.Range{} }},
		{"predicate metadata", func(aggregate *spl.StatsAggregate) { aggregate.Predicate = &spl.WhereComparisonExpr{} }},
		{"percentile metadata", func(aggregate *spl.StatsAggregate) { aggregate.Percentile = 95 }},
		{"implicit alias", func(aggregate *spl.StatsAggregate) { aggregate.ExplicitAlias = false }},
		{"empty output", func(aggregate *spl.StatsAggregate) { aggregate.Alias = "" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			aggregate := valid
			test.mutate(&aggregate)
			query := &spl.Query{
				Search: base.Search,
				Commands: []spl.Command{&spl.EventStatsCommand{
					Aggregate: aggregate,
				}},
				Range: base.Range,
			}
			_, err := Build(query, testScope([]string{"gradethis"}, nil))
			assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE")
		})
	}
}

func TestAnalyzeEventStatsDistinctCountAcceptsResolvedInputAndRejectsForgedMetadata(
	t *testing.T,
) {
	t.Parallel()

	input := mustResolveEventAggregateField(t, "http.user")
	valid := AggregateMeasure{
		Function: AggregateFunctionDistinctCount,
		Input:    input,
		Output:   "unique_users",
	}
	operator := &EventAggregate{
		GroupBy: []FieldRef{mustResolveEventAggregateField(t, "host")},
		Measure: valid,
	}
	analysis, err := Analyze(&Query{Operators: []Operator{operator}})
	if err != nil {
		t.Fatalf("Analyze valid distinct count: %v", err)
	}
	if !slices.Equal(analysis.ReferencedFields, []string{"host", "http.user"}) {
		t.Fatalf("referenced fields = %v", analysis.ReferencedFields)
	}

	for _, test := range []struct {
		name   string
		mutate func(*AggregateMeasure)
	}{
		{"missing input", func(measure *AggregateMeasure) { measure.Input = FieldRef{} }},
		{"unresolved input", func(measure *AggregateMeasure) { measure.Input = FieldRef{Name: input.Name} }},
		{"forged path", func(measure *AggregateMeasure) { measure.Input.Path = []string{"attacker"} }},
		{"forged canonical bit", func(measure *AggregateMeasure) { measure.Input.Canonical = true }},
		{"predicate metadata", func(measure *AggregateMeasure) { measure.Predicate = &ComparisonExpression{} }},
		{"percentile metadata", func(measure *AggregateMeasure) { measure.Percentile = 95 }},
		{"private output", func(measure *AggregateMeasure) { measure.Output = "__os_eventstats_private" }},
		{"unsupported function", func(measure *AggregateMeasure) { measure.Function = AggregateFunctionList }},
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
				t.Fatalf("Analyze accepted forged distinct count measure %#v", measure)
			}
			if err := ValidateFieldAnalysisEligibility(query); err == nil {
				t.Fatalf("field analysis accepted forged distinct count measure %#v", measure)
			}
			if err := ValidateTimelineEligibility(query); err == nil {
				t.Fatalf("timeline accepted forged distinct count measure %#v", measure)
			}
		})
	}
}

func TestBuildEventStatsDistinctCountProtectsReservedOpenSchemaFields(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | eventstats dc(fields) AS unique_values`,
		`index=gradethis | eventstats dc(user) AS fields`,
		`index=gradethis | eventstats dc(user) AS unique_values BY fields`,
	} {
		_, err := Build(
			mustParse(t, source),
			testScope([]string{"gradethis"}, nil),
		)
		var diagnostic *Diagnostic
		if !errors.As(err, &diagnostic) ||
			diagnostic.Code != "SPL_AMBIGUOUS_EVENTSTATS_FIELD" ||
			diagnostic.Range == (spl.Range{}) {
			t.Fatalf("Build(%q) error = %#v, want source-located diagnostic", source, err)
		}
	}
}
