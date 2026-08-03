package plan

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildEventStatsValuesProducesSingularRowPreservingMeasure(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | eventstats values(http.User) AS unique_users BY host, Host`,
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
		measure.Function != AggregateFunctionValues ||
		measure.Input.Name != "http.User" ||
		measure.Input.Canonical ||
		!slices.Equal(measure.Input.Path, []string{"http", "User"}) ||
		measure.Input.Range == (spl.Range{}) ||
		measure.Predicate != nil ||
		measure.Percentile != 0 ||
		measure.Output != "unique_users" {
		t.Fatalf("event aggregate = %#v", eventAggregate)
	}
	if len(eventAggregate.GroupBy) != 2 ||
		eventAggregate.GroupBy[0].Name != "host" ||
		eventAggregate.GroupBy[1].Name != "Host" ||
		eventAggregate.GroupBy[0].Range == (spl.Range{}) ||
		eventAggregate.GroupBy[1].Range == (spl.Range{}) {
		t.Fatalf("event aggregate groups = %#v, want source-resolved host/Host", eventAggregate.GroupBy)
	}

	analysis, err := Analyze(logical)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if !slices.Equal(
		analysis.ReferencedFields,
		[]string{"Host", "host", "http.User", "index"},
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

func TestBuildEventStatsValuesUpsertsKnownOutputSchema(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
		want   []string
	}{
		{
			name: "append",
			source: `index=gradethis | table _time,host,user` +
				` | eventstats values(user) AS unique_users BY host`,
			want: []string{"_time", "host", "user", "unique_users"},
		},
		{
			name: "replace in place",
			source: `index=gradethis | table _time,unique_users,host,user` +
				` | eventstats values(user) AS unique_users BY host`,
			want: []string{"_time", "unique_users", "host", "user"},
		},
		{
			name: "preserve statistics relation",
			source: `index=gradethis | stats count AS events BY host` +
				` | eventstats values(host) AS observed_hosts`,
			want: []string{"host", "events", "observed_hosts"},
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
			if !slices.Equal(logical.OutputFields, test.want) {
				t.Fatalf("output fields = %v, want %v", logical.OutputFields, test.want)
			}
		})
	}
}

func TestBuildEventStatsValuesProtectsReservedOpenSchemaFields(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | eventstats values(fields) AS unique_values`,
		`index=gradethis | eventstats values(user) AS fields`,
		`index=gradethis | eventstats values(user) AS unique_values BY fields`,
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

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | table fields,host`+
				` | eventstats values(fields) AS unique_values BY host`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build closed-schema input: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"fields", "host", "unique_values"}) {
		t.Fatalf("closed input output fields = %v", logical.OutputFields)
	}

	logical, err = Build(
		mustParse(
			t,
			`index=gradethis | table fields,host,user`+
				` | eventstats values(user) AS fields BY host`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build closed-schema output: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"fields", "host", "user"}) {
		t.Fatalf("closed output fields = %v", logical.OutputFields)
	}
}

func TestBuildEventStatsValuesPreservesTimeAndIndexProvenance(t *testing.T) {
	t.Parallel()

	if _, err := Build(
		mustParse(
			t,
			`index=gradethis | eventstats values(user) AS unique_users`+
				` | timechart span=5m count BY level`,
		),
		testScope([]string{"gradethis"}, nil),
	); err != nil {
		t.Fatalf("ordinary values output invalidated canonical time: %v", err)
	}

	_, err := Build(
		mustParse(
			t,
			`index=gradethis | eventstats values(user) AS _time`+
				` | timechart span=5m count BY level`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_TIMECHART_TIME_FIELD")

	overwrittenTime, err := Build(
		mustParse(t, `index=gradethis | eventstats values(user) AS _time`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build direct _time overwrite: %v", err)
	}
	if err := ValidateFieldAnalysisEligibility(overwrittenTime); err != nil {
		t.Fatalf("field analysis after _time overwrite: %v", err)
	}
	assertDiagnosticCode(
		t,
		ValidateTimelineEligibility(overwrittenTime),
		"SPL_UNSUPPORTED_TIMELINE_TIME_FIELD",
	)

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | eventstats values(user) AS index`+
				` | search index=secret`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build overwritten index search: %v", err)
	}
	if !slices.Equal(logical.EffectiveIndexes, []string{"gradethis"}) {
		t.Fatalf("effective indexes = %v, want [gradethis]", logical.EffectiveIndexes)
	}

	_, err = Build(
		mustParse(
			t,
			`index=gradethis | eventstats values(user) AS unique_users`+
				` | search index=secret`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_INDEX_FORBIDDEN")
}

func TestBuildEventStatsValuesRejectsForgedAggregateMetadata(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	fieldRange := spl.Range{
		Start: spl.Position{Offset: 1, Line: 1, Column: 2},
		End:   spl.Position{Offset: 7, Line: 1, Column: 8},
	}
	valid := spl.StatsAggregate{
		Function:      spl.AggregateFunctionValues,
		Input:         "user",
		InputRange:    fieldRange,
		Alias:         "unique_users",
		ExplicitAlias: true,
		Range:         fieldRange,
		AliasRange:    fieldRange,
	}
	for _, test := range []struct {
		name     string
		mutate   func(*spl.StatsAggregate)
		wantCode string
	}{
		{"missing input", func(aggregate *spl.StatsAggregate) { aggregate.Input = "" }, "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE"},
		{"missing input range", func(aggregate *spl.StatsAggregate) { aggregate.InputRange = spl.Range{} }, "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE"},
		{"predicate metadata", func(aggregate *spl.StatsAggregate) { aggregate.Predicate = &spl.WhereComparisonExpr{} }, "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE"},
		{"percentile metadata", func(aggregate *spl.StatsAggregate) { aggregate.Percentile = 95 }, "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE"},
		{"implicit alias", func(aggregate *spl.StatsAggregate) { aggregate.ExplicitAlias = false }, "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE"},
		{"empty output", func(aggregate *spl.StatsAggregate) { aggregate.Alias = "" }, "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE"},
		{"private output", func(aggregate *spl.StatsAggregate) { aggregate.Alias = "__os_eventstats_private" }, "SPL_RESERVED_FIELD"},
		{"unsupported list function", func(aggregate *spl.StatsAggregate) { aggregate.Function = spl.AggregateFunctionList }, "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE"},
	} {
		test := test
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
			assertDiagnosticCode(t, err, test.wantCode)
		})
	}
}

func TestBuildEventStatsValuesRejectsLongAliasAndForgedGroups(t *testing.T) {
	t.Parallel()

	longAlias := strings.Repeat("x", maxFieldNameBytes+1)
	query := mustParse(
		t,
		"index=gradethis | eventstats values(user) AS "+longAlias,
	)
	_, err := Build(query, testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_QUERY_TOO_COMPLEX")
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) {
		t.Fatalf("long alias error = %#v, want *Diagnostic", err)
	}
	command := query.Commands[0].(*spl.EventStatsCommand)
	if diagnostic.Range != command.Aggregate.AliasRange {
		t.Fatalf(
			"long alias diagnostic range = %#v, want alias range %#v",
			diagnostic.Range,
			command.Aggregate.AliasRange,
		)
	}

	base := mustParse(t, `index=gradethis`)
	fieldRange := spl.Range{
		Start: spl.Position{Offset: 1, Line: 1, Column: 2},
		End:   spl.Position{Offset: 7, Line: 1, Column: 8},
	}
	valid := spl.StatsAggregate{
		Function:      spl.AggregateFunctionValues,
		Input:         "user",
		InputRange:    fieldRange,
		Alias:         "unique_users",
		ExplicitAlias: true,
		Range:         fieldRange,
		AliasRange:    fieldRange,
	}
	for _, test := range []struct {
		name     string
		groups   []spl.StatsGroupField
		wantCode string
	}{
		{
			name: "duplicate",
			groups: []spl.StatsGroupField{
				{Name: "host", Range: fieldRange},
				{Name: "host", Range: fieldRange},
			},
			wantCode: "SPL_DUPLICATE_FIELD",
		},
		{
			name:     "over limit",
			groups:   make([]spl.StatsGroupField, spl.MaximumStatsGroupFields+1),
			wantCode: "SPL_QUERY_TOO_COMPLEX",
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			groups := slices.Clone(test.groups)
			for index := range groups {
				if groups[index].Name == "" {
					groups[index] = spl.StatsGroupField{
						Name:  "field_" + string(rune('a'+index)),
						Range: fieldRange,
					}
				}
			}
			query := &spl.Query{
				Search: base.Search,
				Commands: []spl.Command{&spl.EventStatsCommand{
					Aggregate: valid,
					GroupBy:   groups,
				}},
				Range: base.Range,
			}
			_, err := Build(query, testScope([]string{"gradethis"}, nil))
			assertDiagnosticCode(t, err, test.wantCode)
		})
	}
}

func TestAnalyzeEventStatsValuesAcceptsResolvedFieldsAndRejectsForgery(t *testing.T) {
	t.Parallel()

	input := mustResolveEventAggregateField(t, "http.User")
	host := mustResolveEventAggregateField(t, "host")
	valid := AggregateMeasure{
		Function: AggregateFunctionValues,
		Input:    input,
		Output:   "unique_users",
	}
	operator := &EventAggregate{
		GroupBy: []FieldRef{host},
		Measure: valid,
	}
	analysis, err := Analyze(&Query{Operators: []Operator{operator}})
	if err != nil {
		t.Fatalf("Analyze valid values: %v", err)
	}
	if !slices.Equal(analysis.ReferencedFields, []string{"host", "http.User"}) {
		t.Fatalf("referenced fields = %v, want [host http.User]", analysis.ReferencedFields)
	}

	for _, test := range []struct {
		name   string
		mutate func(*AggregateMeasure)
	}{
		{"missing input", func(measure *AggregateMeasure) { measure.Input = FieldRef{} }},
		{"unresolved input", func(measure *AggregateMeasure) { measure.Input = FieldRef{Name: input.Name} }},
		{"forged input path", func(measure *AggregateMeasure) { measure.Input.Path = []string{"attacker"} }},
		{"forged canonical input", func(measure *AggregateMeasure) { measure.Input.Canonical = true }},
		{"predicate metadata", func(measure *AggregateMeasure) { measure.Predicate = &ComparisonExpression{} }},
		{"percentile metadata", func(measure *AggregateMeasure) { measure.Percentile = 50 }},
		{"empty output", func(measure *AggregateMeasure) { measure.Output = "" }},
		{"private output", func(measure *AggregateMeasure) { measure.Output = "__os_eventstats_private" }},
		{"unsupported list function", func(measure *AggregateMeasure) { measure.Function = AggregateFunctionList }},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			measure := valid
			test.mutate(&measure)
			assertEventStatsValuesPlanValidatorsReject(
				t,
				&EventAggregate{GroupBy: []FieldRef{host}, Measure: measure},
			)
		})
	}

	for _, test := range []struct {
		name   string
		groups []FieldRef
	}{
		{"unresolved group", []FieldRef{{Name: "host"}}},
		{"forged group path", []FieldRef{{Name: "host", Path: []string{"attacker"}}}},
		{"duplicate group", []FieldRef{host, host}},
		{"over group limit", make([]FieldRef, spl.MaximumStatsGroupFields+1)},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			groups := slices.Clone(test.groups)
			for index := range groups {
				if groups[index].Name == "" {
					groups[index] = mustResolveEventAggregateField(
						t,
						"field_"+string(rune('a'+index)),
					)
				}
			}
			assertEventStatsValuesPlanValidatorsReject(
				t,
				&EventAggregate{GroupBy: groups, Measure: valid},
			)
		})
	}
}

func assertEventStatsValuesPlanValidatorsReject(
	t *testing.T,
	operator *EventAggregate,
) {
	t.Helper()

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
		query := &Query{Operators: []Operator{&Scan{}, operator}}
		if err := validate.fn(query); err == nil {
			t.Fatalf("%s accepted forged values operator %#v", validate.name, operator)
		}
	}
}
