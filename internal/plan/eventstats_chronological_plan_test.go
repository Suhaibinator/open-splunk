package plan

import (
	"errors"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildEventStatsChronologicalAggregatesProduceRowPreservingMeasures(
	t *testing.T,
) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		function   string
		plan       AggregateFunction
		output     string
		wantFields []string
		references []string
	}{
		{
			name:       "earliest appends output",
			function:   "earliest",
			plan:       AggregateFunctionEarliest,
			output:     "first_status",
			wantFields: []string{"_time", "host", "http.status", "first_status"},
			references: []string{"_time", "host", "http.status", "index"},
		},
		{
			name:       "latest replaces output in place",
			function:   "latest",
			plan:       AggregateFunctionLatest,
			output:     "last_status",
			wantFields: []string{"_time", "last_status", "host", "http.status"},
			references: []string{"_time", "host", "http.status", "index", "last_status"},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			initialFields := "_time,host,http.status"
			if test.function == "latest" {
				initialFields = "_time,last_status,host,http.status"
			}
			source := `index=gradethis | table ` + initialFields +
				` | eventstats ` + test.function + `(http.status) AS ` + test.output +
				` BY host`
			logical, err := Build(
				mustParse(t, source),
				testScope([]string{"gradethis"}, nil),
			)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			if !slices.Equal(logical.OutputFields, test.wantFields) {
				t.Fatalf("output fields = %v, want %v", logical.OutputFields, test.wantFields)
			}
			eventAggregate, ok := logical.Operators[len(logical.Operators)-1].(*EventAggregate)
			if !ok {
				t.Fatalf("last operator = %T, want *EventAggregate", logical.Operators[len(logical.Operators)-1])
			}
			measure := eventAggregate.Measure
			if measure.Function != test.plan ||
				measure.Input.Name != "http.status" ||
				measure.Input.Canonical ||
				!slices.Equal(measure.Input.Path, []string{"http", "status"}) ||
				measure.Input.Range == (spl.Range{}) ||
				measure.Predicate != nil ||
				measure.Percentile != 0 ||
				measure.Output != test.output ||
				len(eventAggregate.GroupBy) != 1 ||
				eventAggregate.GroupBy[0].Name != "host" {
				t.Fatalf("event aggregate = %#v", eventAggregate)
			}

			analysis, analyzeErr := Analyze(logical)
			if analyzeErr != nil {
				t.Fatalf("Analyze: %v", analyzeErr)
			}
			if !slices.Equal(
				analysis.ReferencedFields,
				test.references,
			) {
				t.Fatalf("referenced fields = %v", analysis.ReferencedFields)
			}
			if eligibilityErr := ValidateFieldAnalysisEligibility(logical); eligibilityErr != nil {
				t.Fatalf("field analysis eligibility: %v", eligibilityErr)
			}
			if timelineErr := ValidateTimelineEligibility(logical); timelineErr != nil {
				t.Fatalf("timeline eligibility: %v", timelineErr)
			}
		})
	}
}

func TestBuildEventStatsChronologicalAggregatesRequireCanonicalTime(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | fields - _time | eventstats earliest(status) AS first_status`,
		`index=gradethis | table host,status | eventstats latest(status) AS last_status`,
		`index=gradethis | eval _time=1 | eventstats earliest(status) AS first_status`,
		`index=gradethis | rex "(?<_time>\d+)" | eventstats latest(status) AS last_status`,
		`index=gradethis | spath input=_raw output=_time path=observed_at | eventstats earliest(status) AS first_status`,
		`index=gradethis | rename _time AS observed_at | eventstats latest(status) AS last_status`,
		`index=gradethis | bin _time span=5m | eventstats earliest(status) AS first_status`,
		`index=gradethis | stats count BY host | eventstats latest(host) AS last_host`,
	} {
		query := mustParse(t, source)
		_, err := Build(query, testScope([]string{"gradethis"}, nil))
		assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_EVENTSTATS_TIME_FIELD")
		var diagnostic *Diagnostic
		if !errors.As(err, &diagnostic) {
			t.Fatalf("Build(%q) error = %T, want *Diagnostic", source, err)
		}
		command := query.Commands[len(query.Commands)-1].(*spl.EventStatsCommand)
		if diagnostic.Range != command.Aggregate.Range {
			t.Errorf("Build(%q) diagnostic range = %#v, want aggregate %#v", source, diagnostic.Range, command.Aggregate.Range)
		}
		if !slices.Contains(
			diagnostic.Suggestions,
			"run eventstats earliest or latest before removing, replacing, or transforming _time",
		) {
			t.Errorf("Build(%q) suggestions = %v", source, diagnostic.Suggestions)
		}
	}
}

func TestBuildEventStatsChronologicalAggregatesAcceptEventPreservingPipelines(
	t *testing.T,
) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis level=error | eventstats earliest(status) AS first_status`,
		`index=gradethis | search level=error | where status>=500 | eventstats latest(status) AS last_status`,
		`index=gradethis | sort -_time | head 20 | eventstats earliest(status) AS first_status`,
		`index=gradethis | sort _time | tail 20 | eventstats latest(status) AS last_status`,
		`index=gradethis | dedup 2 host | eventstats earliest(status) AS first_status BY host`,
		`index=gradethis | table _time,host,status | eventstats latest(status) AS last_status BY host`,
		`index=gradethis | bin _time span=5m AS bucket_time | eventstats earliest(status) AS first_status`,
	} {
		if _, err := Build(
			mustParse(t, source),
			testScope([]string{"gradethis"}, nil),
		); err != nil {
			t.Errorf("Build(%q): %v", source, err)
		}
	}
}

func TestBuildEventStatsChronologicalAggregatesUseSharedFieldAndOutputContract(
	t *testing.T,
) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | eventstats earliest(fields) AS first_value`,
		`index=gradethis | eventstats latest(status) AS fields`,
		`index=gradethis | eventstats earliest(status) AS first_status BY fields`,
	} {
		_, err := Build(
			mustParse(t, source),
			testScope([]string{"gradethis"}, nil),
		)
		assertDiagnosticCode(t, err, "SPL_AMBIGUOUS_EVENTSTATS_FIELD")
	}

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | table _time,fields,host`+
				` | eventstats latest(fields) AS last_value BY host`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build closed-schema fields input: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"_time", "fields", "host", "last_value"}) {
		t.Fatalf("output fields = %v", logical.OutputFields)
	}

	overwrittenTime, err := Build(
		mustParse(t, `index=gradethis | eventstats earliest(status) AS _time`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build chronological _time output: %v", err)
	}
	assertDiagnosticCode(
		t,
		ValidateTimelineEligibility(overwrittenTime),
		"SPL_UNSUPPORTED_TIMELINE_TIME_FIELD",
	)
}

func TestBuildEventStatsChronologicalAggregatesRejectForgedMetadata(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	fieldRange := spl.Range{
		Start: spl.Position{Offset: 1, Line: 1, Column: 2},
		End:   spl.Position{Offset: 7, Line: 1, Column: 8},
	}
	for _, testFunction := range []struct {
		name     string
		function spl.AggregateFunction
	}{
		{name: "earliest", function: spl.AggregateFunctionEarliest},
		{name: "latest", function: spl.AggregateFunctionLatest},
	} {
		testFunction := testFunction
		for _, test := range []struct {
			name   string
			mutate func(*spl.StatsAggregate)
		}{
			{"missing input", func(aggregate *spl.StatsAggregate) { aggregate.Input = "" }},
			{"missing input range", func(aggregate *spl.StatsAggregate) { aggregate.InputRange = spl.Range{} }},
			{"predicate metadata", func(aggregate *spl.StatsAggregate) { aggregate.Predicate = &spl.WhereComparisonExpr{} }},
			{"percentile metadata", func(aggregate *spl.StatsAggregate) { aggregate.Percentile = 95 }},
			{"implicit alias", func(aggregate *spl.StatsAggregate) { aggregate.ExplicitAlias = false }},
		} {
			test := test
			t.Run(testFunction.name+"/"+test.name, func(t *testing.T) {
				t.Parallel()

				aggregate := spl.StatsAggregate{
					Function:      testFunction.function,
					Input:         "status",
					InputRange:    fieldRange,
					Alias:         "observed_status",
					ExplicitAlias: true,
					Range:         fieldRange,
					AliasRange:    fieldRange,
				}
				test.mutate(&aggregate)
				query := &spl.Query{
					Search: base.Search,
					Commands: []spl.Command{&spl.EventStatsCommand{
						Aggregate: aggregate,
						Range:     fieldRange,
					}},
					Range: base.Range,
				}
				_, err := Build(query, testScope([]string{"gradethis"}, nil))
				assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE")
			})
		}
	}
}

func TestEventStatsChronologicalPlanContractAcceptsResolvedFieldsAndRejectsForgery(
	t *testing.T,
) {
	t.Parallel()

	input := mustResolveEventAggregateField(t, "http.status")
	host := mustResolveEventAggregateField(t, "host")
	for _, testFunction := range []struct {
		name     string
		function AggregateFunction
	}{
		{name: "earliest", function: AggregateFunctionEarliest},
		{name: "latest", function: AggregateFunctionLatest},
	} {
		testFunction := testFunction
		t.Run(testFunction.name, func(t *testing.T) {
			t.Parallel()

			valid := AggregateMeasure{
				Function: testFunction.function,
				Input:    input,
				Output:   "observed_status",
			}
			validQuery := &Query{Operators: []Operator{
				&Scan{},
				&EventAggregate{GroupBy: []FieldRef{host}, Measure: valid},
			}}
			analysis, err := Analyze(validQuery)
			if err != nil {
				t.Fatalf("Analyze valid measure: %v", err)
			}
			if !slices.Equal(
				analysis.ReferencedFields,
				[]string{"_time", "host", "http.status"},
			) {
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
				mutate func(*AggregateMeasure)
			}{
				{"missing input", func(measure *AggregateMeasure) { measure.Input = FieldRef{} }},
				{"unresolved input", func(measure *AggregateMeasure) { measure.Input = FieldRef{Name: input.Name} }},
				{"forged path", func(measure *AggregateMeasure) { measure.Input.Path = []string{"attacker"} }},
				{"canonical input", func(measure *AggregateMeasure) { measure.Input.Canonical = true }},
				{"predicate", func(measure *AggregateMeasure) { measure.Predicate = &ComparisonExpression{} }},
				{"percentile", func(measure *AggregateMeasure) { measure.Percentile = 50 }},
				{"empty output", func(measure *AggregateMeasure) { measure.Output = "" }},
				{"private output", func(measure *AggregateMeasure) { measure.Output = "__os_eventstats_private" }},
			} {
				test := test
				t.Run(test.name, func(t *testing.T) {
					t.Parallel()

					measure := valid
					test.mutate(&measure)
					query := &Query{Operators: []Operator{
						&Scan{},
						&EventAggregate{GroupBy: []FieldRef{host}, Measure: measure},
					}}
					for _, validator := range []struct {
						name string
						fn   func(*Query) error
					}{
						{"analyze", func(query *Query) error { _, err := Analyze(query); return err }},
						{"field analysis", ValidateFieldAnalysisEligibility},
						{"timeline", ValidateTimelineEligibility},
					} {
						if err := validator.fn(query); err == nil {
							t.Fatalf("%s accepted forged measure %#v", validator.name, measure)
						}
					}
				})
			}
		})
	}
}
