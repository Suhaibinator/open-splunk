package plan

import (
	"errors"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildEventStatsCountProducesRowPreservingAggregate(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | eventstats count AS events BY host, status`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(logical.OutputFields) != 0 {
		t.Fatalf(
			"open event output fields = %v, want an open schema",
			logical.OutputFields,
		)
	}
	eventAggregate, ok :=
		logical.Operators[len(logical.Operators)-1].(*EventAggregate)
	if !ok {
		t.Fatalf(
			"last operator = %T, want *EventAggregate",
			logical.Operators[len(logical.Operators)-1],
		)
	}
	if eventAggregate.LogicalName() != "EventAggregate" ||
		eventAggregate.SourceRange() != eventAggregate.Range ||
		len(eventAggregate.GroupBy) != 2 ||
		eventAggregate.GroupBy[0].Name != "host" ||
		eventAggregate.GroupBy[1].Name != "status" ||
		eventAggregate.Measure.Function != AggregateFunctionCountRows ||
		eventAggregate.Measure.Input.Name != "" ||
		eventAggregate.Measure.Input.Canonical ||
		len(eventAggregate.Measure.Input.Path) != 0 ||
		eventAggregate.Measure.Input.Range != (spl.Range{}) ||
		eventAggregate.Measure.Predicate != nil ||
		eventAggregate.Measure.Percentile != 0 ||
		eventAggregate.Measure.Output != "events" {
		t.Fatalf("event aggregate = %#v", eventAggregate)
	}
}

func TestBuildEventStatsCountFieldProducesRowPreservingAggregate(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | eventstats count(status) AS populated BY host`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	eventAggregate, ok :=
		logical.Operators[len(logical.Operators)-1].(*EventAggregate)
	if !ok {
		t.Fatalf(
			"last operator = %T, want *EventAggregate",
			logical.Operators[len(logical.Operators)-1],
		)
	}
	if eventAggregate.Measure.Function != AggregateFunctionCountValues ||
		eventAggregate.Measure.Input.Name != "status" ||
		eventAggregate.Measure.Input.Range == (spl.Range{}) ||
		eventAggregate.Measure.Predicate != nil ||
		eventAggregate.Measure.Percentile != 0 ||
		eventAggregate.Measure.Output != "populated" ||
		len(eventAggregate.GroupBy) != 1 ||
		eventAggregate.GroupBy[0].Name != "host" {
		t.Fatalf("event aggregate = %#v", eventAggregate)
	}

	analysis, err := Analyze(logical)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if !slices.Equal(
		analysis.ReferencedFields,
		[]string{"host", "index", "status"},
	) {
		t.Fatalf(
			"referenced fields = %v, want [host index status]",
			analysis.ReferencedFields,
		)
	}
}

func TestBuildEventStatsCountUpsertsKnownOutputSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		want   []string
	}{
		{
			name: "append",
			source: `index=gradethis | table _time,host,message` +
				` | eventstats count AS events BY host`,
			want: []string{"_time", "host", "message", "events"},
		},
		{
			name: "replace in place",
			source: `index=gradethis | table _time,events,host` +
				` | eventstats count AS events BY host`,
			want: []string{"_time", "events", "host"},
		},
		{
			name: "preserve statistics relation",
			source: `index=gradethis | stats count BY host` +
				` | eventstats count AS groups`,
			want: []string{"host", "count", "groups"},
		},
		{
			name: "field count append",
			source: `index=gradethis | table _time,host,status` +
				` | eventstats count(status) AS populated BY host`,
			want: []string{"_time", "host", "status", "populated"},
		},
		{
			name: "field count replace in place",
			source: `index=gradethis | table _time,populated,host,status` +
				` | eventstats count(status) AS populated BY host`,
			want: []string{"_time", "populated", "host", "status"},
		},
	}
	for _, test := range tests {
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
			if !slices.Equal(logical.OutputFields, test.want) {
				t.Fatalf(
					"output fields = %v, want %v",
					logical.OutputFields,
					test.want,
				)
			}
		})
	}
}

func TestBuildEventStatsCountEnforcesReservedOpenSchemaBoundary(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | eventstats count AS fields`,
		`index=gradethis | eventstats count BY fields`,
		`index=gradethis | eventstats count(fields) AS populated`,
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
			`index=gradethis | table fields,host`+
				` | eventstats count AS fields BY host`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build closed schema: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"fields", "host"}) {
		t.Fatalf("closed output fields = %v", logical.OutputFields)
	}

	logical, err = Build(
		mustParse(
			t,
			`index=gradethis | table fields,host`+
				` | eventstats count(fields) AS populated BY host`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build closed-schema field count: %v", err)
	}
	if !slices.Equal(
		logical.OutputFields,
		[]string{"fields", "host", "populated"},
	) {
		t.Fatalf(
			"closed field-count output fields = %v",
			logical.OutputFields,
		)
	}
}

func TestBuildEventStatsCountInvalidatesCanonicalTimeOnlyWhenOverwritten(
	t *testing.T,
) {
	t.Parallel()

	if _, err := Build(
		mustParse(
			t,
			`index=gradethis | eventstats count AS events`+
				` | timechart span=5m count BY level`,
		),
		testScope([]string{"gradethis"}, nil),
	); err != nil {
		t.Fatalf("non-time eventstats output invalidated canonical time: %v", err)
	}

	_, err := Build(
		mustParse(
			t,
			`index=gradethis | eventstats count AS _time`+
				` | timechart span=5m count BY level`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_TIMECHART_TIME_FIELD")
}

func TestBuildEventStatsIndexOverwriteStopsAuthorizationReferences(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | eventstats count AS index`+
				` | search index=secret`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build overwritten index search: %v", err)
	}
	if !slices.Equal(logical.EffectiveIndexes, []string{"gradethis"}) {
		t.Fatalf("effective indexes = %v", logical.EffectiveIndexes)
	}

	_, err = Build(
		mustParse(
			t,
			`index=gradethis | eventstats count AS events`+
				` | search index=secret`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_INDEX_FORBIDDEN")
}

func TestBuildEventStatsCountRejectsForgedMetadata(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	fieldRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Line: 1, Column: 5},
	}
	valid := spl.StatsAggregate{
		Function: spl.AggregateFunctionCount,
		Alias:    "count",
	}
	tests := []struct {
		name      string
		aggregate spl.StatsAggregate
	}{
		{
			name: "count field without source range or alias",
			aggregate: spl.StatsAggregate{
				Function: spl.AggregateFunctionCountValues,
				Input:    "status",
				Alias:    "count",
			},
		},
		{
			name: "count field without explicit alias",
			aggregate: spl.StatsAggregate{
				Function:   spl.AggregateFunctionCountValues,
				Input:      "status",
				InputRange: fieldRange,
				Alias:      "populated",
			},
		},
		{
			name: "count field without input",
			aggregate: spl.StatsAggregate{
				Function:      spl.AggregateFunctionCountValues,
				Alias:         "populated",
				ExplicitAlias: true,
			},
		},
		{
			name: "unsupported function",
			aggregate: spl.StatsAggregate{
				Function:      spl.AggregateFunctionCountPredicate,
				Alias:         "matches",
				ExplicitAlias: true,
			},
		},
		{
			name: "input metadata",
			aggregate: spl.StatsAggregate{
				Function:   spl.AggregateFunctionCount,
				Input:      "status",
				InputRange: fieldRange,
				Alias:      "count",
			},
		},
		{
			name: "predicate metadata",
			aggregate: spl.StatsAggregate{
				Function: spl.AggregateFunctionCount,
				Predicate: &spl.WhereComparisonExpr{
					Left:  &spl.ScalarFieldExpr{Field: "status"},
					Op:    spl.CompareOpEqual,
					Right: &spl.ScalarLiteralExpr{},
				},
				Alias: "count",
			},
		},
		{
			name: "percentile metadata",
			aggregate: spl.StatsAggregate{
				Function:   spl.AggregateFunctionCount,
				Percentile: 95,
				Alias:      "count",
			},
		},
		{
			name: "implicit custom alias",
			aggregate: spl.StatsAggregate{
				Function: spl.AggregateFunctionCount,
				Alias:    "events",
			},
		},
		{
			name: "empty alias",
			aggregate: spl.StatsAggregate{
				Function: spl.AggregateFunctionCount,
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			query := &spl.Query{
				Search: base.Search,
				Commands: []spl.Command{&spl.EventStatsCommand{
					Aggregate: test.aggregate,
				}},
				Range: base.Range,
			}
			_, err := Build(
				query,
				testScope([]string{"gradethis"}, nil),
			)
			assertDiagnosticCode(
				t,
				err,
				"SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
			)
		})
	}

	customAlias := valid
	customAlias.Alias = "events"
	customAlias.ExplicitAlias = true
	query := &spl.Query{
		Search: base.Search,
		Commands: []spl.Command{&spl.EventStatsCommand{
			Aggregate: customAlias,
			GroupBy: []spl.StatsGroupField{
				{Name: "host"},
				{Name: "host"},
			},
		}},
		Range: base.Range,
	}
	_, err := Build(query, testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_DUPLICATE_FIELD")
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) ||
		diagnostic.Message != `eventstats grouping field "host" is repeated` {
		t.Fatalf("eventstats duplicate-group error = %v", err)
	}

	query.Commands = []spl.Command{&spl.StatsCommand{
		Aggregates: []spl.StatsAggregate{{
			Function: spl.AggregateFunctionCount,
			Alias:    "count",
		}},
		GroupBy: []spl.StatsGroupField{
			{Name: "host"},
			{Name: "host"},
		},
	}}
	_, err = Build(query, testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_DUPLICATE_FIELD")
	if !errors.As(err, &diagnostic) ||
		diagnostic.Message != `stats grouping field "host" is repeated` {
		t.Fatalf("stats duplicate-group error = %v", err)
	}

	query.Commands = []spl.Command{&spl.EventStatsCommand{
		Aggregate: customAlias,
		GroupBy: make(
			[]spl.StatsGroupField,
			spl.MaximumStatsGroupFields+1,
		),
	}}
	for index := range query.Commands[0].(*spl.EventStatsCommand).GroupBy {
		query.Commands[0].(*spl.EventStatsCommand).GroupBy[index].Name =
			"field_" + string(rune('a'+index))
	}
	_, err = Build(query, testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_QUERY_TOO_COMPLEX")
}

func TestAnalyzeEventAggregateReadsGroupsAndCountFieldAndValidatesContract(
	t *testing.T,
) {
	t.Parallel()

	host := mustResolveEventAggregateField(t, "host")
	status := mustResolveEventAggregateField(t, "status")
	operator := &EventAggregate{
		GroupBy: []FieldRef{host, status},
		Measure: AggregateMeasure{
			Function: AggregateFunctionCountRows,
			Output:   "events",
		},
	}
	analysis, err := Analyze(&Query{Operators: []Operator{operator}})
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if !slices.Equal(
		analysis.ReferencedFields,
		[]string{"host", "status"},
	) {
		t.Fatalf("referenced fields = %v", analysis.ReferencedFields)
	}

	fieldOperator := &EventAggregate{
		GroupBy: []FieldRef{host},
		Measure: AggregateMeasure{
			Function: AggregateFunctionCountValues,
			Input:    status,
			Output:   "populated",
		},
	}
	analysis, err = Analyze(&Query{Operators: []Operator{fieldOperator}})
	if err != nil {
		t.Fatalf("Analyze count(field): %v", err)
	}
	if !slices.Equal(
		analysis.ReferencedFields,
		[]string{"host", "status"},
	) {
		t.Fatalf(
			"count(field) referenced fields = %v",
			analysis.ReferencedFields,
		)
	}

	malformed := []struct {
		name     string
		operator *EventAggregate
	}{
		{
			name: "missing output",
			operator: &EventAggregate{
				Measure: AggregateMeasure{
					Function: AggregateFunctionCountRows,
				},
			},
		},
		{
			name: "wrong function",
			operator: &EventAggregate{
				Measure: AggregateMeasure{
					Function: AggregateFunctionSum,
					Output:   "events",
				},
			},
		},
		{
			name: "count field missing input",
			operator: &EventAggregate{
				Measure: AggregateMeasure{
					Function: AggregateFunctionCountValues,
					Output:   "populated",
				},
			},
		},
		{
			name: "count field malformed input",
			operator: &EventAggregate{
				Measure: AggregateMeasure{
					Function: AggregateFunctionCountValues,
					Input:    FieldRef{Name: "status"},
					Output:   "populated",
				},
			},
		},
		{
			name: "input metadata",
			operator: &EventAggregate{
				Measure: AggregateMeasure{
					Function: AggregateFunctionCountRows,
					Input:    FieldRef{Name: "status"},
					Output:   "events",
				},
			},
		},
		{
			name: "empty input path metadata",
			operator: &EventAggregate{
				Measure: AggregateMeasure{
					Function: AggregateFunctionCountRows,
					Input:    FieldRef{Path: []string{}},
					Output:   "events",
				},
			},
		},
		{
			name: "percentile metadata",
			operator: &EventAggregate{
				Measure: AggregateMeasure{
					Function:   AggregateFunctionCountRows,
					Percentile: 1,
					Output:     "events",
				},
			},
		},
		{
			name: "empty group",
			operator: &EventAggregate{
				GroupBy: []FieldRef{{}},
				Measure: AggregateMeasure{
					Function: AggregateFunctionCountRows,
					Output:   "events",
				},
			},
		},
		{
			name: "duplicate group",
			operator: &EventAggregate{
				GroupBy: []FieldRef{host, host},
				Measure: AggregateMeasure{
					Function: AggregateFunctionCountRows,
					Output:   "events",
				},
			},
		},
		{
			name: "malformed group metadata",
			operator: &EventAggregate{
				GroupBy: []FieldRef{{Name: "host"}},
				Measure: AggregateMeasure{
					Function: AggregateFunctionCountRows,
					Output:   "events",
				},
			},
		},
		{
			name: "invalid output",
			operator: &EventAggregate{
				Measure: AggregateMeasure{
					Function: AggregateFunctionCountRows,
					Output:   "__os_eventstats_private",
				},
			},
		},
	}
	for _, test := range malformed {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if _, err := Analyze(
				&Query{Operators: []Operator{test.operator}},
			); err == nil {
				t.Fatalf(
					"malformed event aggregate was accepted: %#v",
					test.operator,
				)
			}
		})
	}

	var typedNil *EventAggregate
	if _, err := Analyze(
		&Query{Operators: []Operator{typedNil}},
	); err == nil {
		t.Fatal("typed-nil EventAggregate was accepted")
	}
}

func TestEventAggregateEligibilityRejectsForgedContracts(t *testing.T) {
	t.Parallel()

	host := mustResolveEventAggregateField(t, "host")
	validMeasure := AggregateMeasure{
		Function: AggregateFunctionCountRows,
		Output:   "events",
	}
	var typedNil *EventAggregate
	tests := []struct {
		name     string
		operator Operator
	}{
		{
			name: "wrong function",
			operator: &EventAggregate{
				Measure: AggregateMeasure{
					Function: AggregateFunctionSum,
					Output:   "events",
				},
			},
		},
		{
			name: "count field missing input",
			operator: &EventAggregate{
				Measure: AggregateMeasure{
					Function: AggregateFunctionCountValues,
					Output:   "events",
				},
			},
		},
		{
			name: "duplicate group",
			operator: &EventAggregate{
				GroupBy: []FieldRef{host, host},
				Measure: validMeasure,
			},
		},
		{
			name: "malformed group reference",
			operator: &EventAggregate{
				GroupBy: []FieldRef{{Name: "host"}},
				Measure: validMeasure,
			},
		},
		{
			name: "invalid output",
			operator: &EventAggregate{
				Measure: AggregateMeasure{
					Function: AggregateFunctionCountRows,
					Output:   "__os_eventstats_private",
				},
			},
		},
		{
			name:     "typed nil",
			operator: typedNil,
		},
	}
	validators := []struct {
		name     string
		validate func(*Query) error
		wantCode string
	}{
		{
			name:     "field analysis",
			validate: ValidateFieldAnalysisEligibility,
			wantCode: fieldAnalysisPipelineDiagnosticCode,
		},
		{
			name:     "timeline",
			validate: ValidateTimelineEligibility,
			wantCode: timelinePipelineDiagnosticCode,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			for _, validator := range validators {
				validator := validator
				t.Run(validator.name, func(t *testing.T) {
					t.Parallel()

					query := &Query{Operators: []Operator{
						&Scan{},
						test.operator,
					}}
					var diagnostic *Diagnostic
					err := validator.validate(query)
					if !errors.As(err, &diagnostic) ||
						diagnostic.Code != validator.wantCode {
						t.Fatalf(
							"%s error = %v, want %s",
							validator.name,
							err,
							validator.wantCode,
						)
					}
				})
			}
		})
	}
}

func mustResolveEventAggregateField(
	t *testing.T,
	name string,
) FieldRef {
	t.Helper()

	field, err := ResolveField(name, spl.Range{})
	if err != nil {
		t.Fatalf("ResolveField(%q): %v", name, err)
	}
	return field
}

func TestEventAggregatePreservesEventAnalysisAndCanonicalTimeline(
	t *testing.T,
) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | eventstats count AS events BY host`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := ValidateFieldAnalysisEligibility(logical); err != nil {
		t.Fatalf("field analysis eligibility: %v", err)
	}
	if err := ValidateTimelineEligibility(logical); err != nil {
		t.Fatalf("timeline eligibility: %v", err)
	}

	fieldCount, err := Build(
		mustParse(
			t,
			`index=gradethis | eventstats count(status) AS populated BY host`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build count(field): %v", err)
	}
	if err := ValidateFieldAnalysisEligibility(fieldCount); err != nil {
		t.Fatalf("field analysis eligibility for count(field): %v", err)
	}
	if err := ValidateTimelineEligibility(fieldCount); err != nil {
		t.Fatalf("timeline eligibility for count(field): %v", err)
	}

	overwritten, err := Build(
		mustParse(
			t,
			`index=gradethis | eventstats count AS _time`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build _time overwrite: %v", err)
	}
	if err := ValidateFieldAnalysisEligibility(overwritten); err != nil {
		t.Fatalf("field analysis after _time overwrite: %v", err)
	}
	var diagnostic *Diagnostic
	err = ValidateTimelineEligibility(overwritten)
	if !errors.As(err, &diagnostic) ||
		diagnostic.Code != "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD" {
		t.Fatalf("timeline error = %v", err)
	}
}
