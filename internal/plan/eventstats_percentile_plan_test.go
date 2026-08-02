package plan

import (
	"errors"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildEventStatsPercentileProducesBoundedRowPreservingMeasure(
	t *testing.T,
) {
	t.Parallel()

	query := mustParse(
		t,
		`index=gradethis | eventstats p95(http.duration) AS p95_duration BY host, status`,
	)
	logical, err := Build(query, testScope([]string{"gradethis"}, nil))
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
	if measure.Function != AggregateFunctionPercentile ||
		measure.Percentile != 95 ||
		measure.Input.Name != "http.duration" ||
		measure.Input.Canonical ||
		!slices.Equal(measure.Input.Path, []string{"http", "duration"}) ||
		measure.Input.Range == (spl.Range{}) ||
		measure.Predicate != nil ||
		measure.Output != "p95_duration" {
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

func TestBuildEventStatsPercentileValidatesLevelAndMetadata(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	fieldRange := spl.Range{
		Start: spl.Position{Offset: 1, Line: 1, Column: 2},
		End:   spl.Position{Offset: 7, Line: 1, Column: 8},
	}
	valid := spl.StatsAggregate{
		Function:      spl.AggregateFunctionPercentile,
		Input:         "duration",
		InputRange:    fieldRange,
		Percentile:    95,
		Alias:         "p95_duration",
		ExplicitAlias: true,
		Range:         fieldRange,
		AliasRange:    fieldRange,
	}
	for _, level := range []uint8{1, 99} {
		aggregate := valid
		aggregate.Percentile = level
		query := &spl.Query{
			Search: base.Search,
			Commands: []spl.Command{&spl.EventStatsCommand{
				Aggregate: aggregate,
			}},
			Range: base.Range,
		}
		logical, err := Build(query, testScope([]string{"gradethis"}, nil))
		if err != nil {
			t.Fatalf("Build percentile %d: %v", level, err)
		}
		measure := logical.Operators[len(logical.Operators)-1].(*EventAggregate).Measure
		if measure.Function != AggregateFunctionPercentile ||
			measure.Percentile != level {
			t.Fatalf("percentile %d measure = %#v", level, measure)
		}
	}

	for _, test := range []struct {
		name   string
		mutate func(*spl.StatsAggregate)
	}{
		{"zero level", func(aggregate *spl.StatsAggregate) { aggregate.Percentile = 0 }},
		{"level above range", func(aggregate *spl.StatsAggregate) { aggregate.Percentile = 100 }},
		{"missing input", func(aggregate *spl.StatsAggregate) { aggregate.Input = "" }},
		{"missing input range", func(aggregate *spl.StatsAggregate) { aggregate.InputRange = spl.Range{} }},
		{"predicate metadata", func(aggregate *spl.StatsAggregate) { aggregate.Predicate = &spl.WhereComparisonExpr{} }},
		{"implicit alias", func(aggregate *spl.StatsAggregate) { aggregate.ExplicitAlias = false }},
		{"empty output", func(aggregate *spl.StatsAggregate) { aggregate.Alias = "" }},
		{"non-percentile function", func(aggregate *spl.StatsAggregate) { aggregate.Function = spl.AggregateFunctionAverage }},
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

func TestBuildEventStatsPercentileUsesSharedOutputAndReservedFieldContract(
	t *testing.T,
) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | table _time,p95_duration,host,duration`+
				` | eventstats p95(duration) AS p95_duration BY host`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build replacement: %v", err)
	}
	if !slices.Equal(
		logical.OutputFields,
		[]string{"_time", "p95_duration", "host", "duration"},
	) {
		t.Fatalf("output fields = %v", logical.OutputFields)
	}

	for _, source := range []string{
		`index=gradethis | eventstats p95(fields) AS p95_duration`,
		`index=gradethis | eventstats p95(duration) AS fields`,
		`index=gradethis | eventstats p95(duration) AS p95_duration BY fields`,
	} {
		_, err := Build(
			mustParse(t, source),
			testScope([]string{"gradethis"}, nil),
		)
		var diagnostic *Diagnostic
		if !errors.As(err, &diagnostic) ||
			diagnostic.Code != "SPL_AMBIGUOUS_EVENTSTATS_FIELD" ||
			diagnostic.Range == (spl.Range{}) {
			t.Fatalf("Build(%q) error = %#v, want source-located reserved-field diagnostic", source, err)
		}
	}

	closedSchema, err := Build(
		mustParse(
			t,
			`index=gradethis | table fields,host`+
				` | eventstats p95(fields) AS p95_fields BY host`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build closed-schema reserved-name input: %v", err)
	}
	if !slices.Equal(
		closedSchema.OutputFields,
		[]string{"fields", "host", "p95_fields"},
	) {
		t.Fatalf("closed-schema output fields = %v", closedSchema.OutputFields)
	}

	overwrittenTime, err := Build(
		mustParse(
			t,
			`index=gradethis | eventstats p95(duration) AS _time`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build _time replacement: %v", err)
	}
	if err := ValidateFieldAnalysisEligibility(overwrittenTime); err != nil {
		t.Fatalf("field analysis after _time replacement: %v", err)
	}
	assertDiagnosticCode(
		t,
		ValidateTimelineEligibility(overwrittenTime),
		"SPL_UNSUPPORTED_TIMELINE_TIME_FIELD",
	)
}

func TestEventStatsPercentilePlanContractRejectsForgedMetadata(t *testing.T) {
	t.Parallel()

	input := mustResolveEventAggregateField(t, "http.duration")
	host := mustResolveEventAggregateField(t, "host")
	valid := AggregateMeasure{
		Function:   AggregateFunctionPercentile,
		Input:      input,
		Percentile: 95,
		Output:     "p95_duration",
	}
	operator := &EventAggregate{GroupBy: []FieldRef{host}, Measure: valid}
	analysis, err := Analyze(&Query{Operators: []Operator{operator}})
	if err != nil {
		t.Fatalf("Analyze valid percentile: %v", err)
	}
	if !slices.Equal(analysis.ReferencedFields, []string{"host", "http.duration"}) {
		t.Fatalf("referenced fields = %v", analysis.ReferencedFields)
	}

	for _, test := range []struct {
		name   string
		mutate func(*AggregateMeasure)
	}{
		{"zero level", func(measure *AggregateMeasure) { measure.Percentile = 0 }},
		{"level above range", func(measure *AggregateMeasure) { measure.Percentile = 100 }},
		{"missing input", func(measure *AggregateMeasure) { measure.Input = FieldRef{} }},
		{"unresolved input", func(measure *AggregateMeasure) { measure.Input = FieldRef{Name: input.Name} }},
		{"forged path", func(measure *AggregateMeasure) { measure.Input.Path = []string{"attacker"} }},
		{"forged canonical bit", func(measure *AggregateMeasure) { measure.Input.Canonical = true }},
		{"predicate metadata", func(measure *AggregateMeasure) { measure.Predicate = &ComparisonExpression{} }},
		{"private output", func(measure *AggregateMeasure) { measure.Output = "__os_eventstats_private" }},
		{"wrong function", func(measure *AggregateMeasure) { measure.Function = AggregateFunctionAverage }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			measure := valid
			test.mutate(&measure)
			query := &Query{Operators: []Operator{
				&Scan{},
				&EventAggregate{Measure: measure},
			}}
			for _, validate := range []struct {
				name string
				fn   func(*Query) error
			}{
				{name: "analyze", fn: func(query *Query) error {
					_, err := Analyze(query)
					return err
				}},
				{name: "field analysis", fn: ValidateFieldAnalysisEligibility},
				{name: "timeline", fn: ValidateTimelineEligibility},
			} {
				if err := validate.fn(query); err == nil {
					t.Fatalf("%s accepted forged percentile measure %#v", validate.name, measure)
				}
			}
		})
	}
}
