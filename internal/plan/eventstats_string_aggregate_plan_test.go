package plan

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

type eventStatsStringAggregatePlanCase struct {
	name        string
	call        string
	output      string
	function    AggregateFunction
	splFunction spl.AggregateFunction
}

func eventStatsStringAggregatePlanCases() []eventStatsStringAggregatePlanCase {
	return []eventStatsStringAggregatePlanCase{
		eventStatsValuesPlanCase(),
		eventStatsListPlanCase(),
	}
}

func TestBuildEventStatsStringAggregatesProduceSingularRowPreservingMeasure(t *testing.T) {
	t.Parallel()

	for _, test := range eventStatsStringAggregatePlanCases() {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical, err := Build(
				mustParse(
					t,
					fmt.Sprintf(
						"index=gradethis | eventstats %s(http.User) AS %s BY host, Host",
						test.call,
						test.output,
					),
				),
				testScope([]string{"gradethis"}, nil),
			)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			if logical.DynamicOutput != nil || len(logical.OutputFields) != 0 {
				t.Fatalf(
					"open event schema = fields %v dynamic %#v, want preserved open schema",
					logical.OutputFields,
					logical.DynamicOutput,
				)
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
				measure.Function != test.function ||
				measure.Input.Name != "http.User" ||
				measure.Input.Canonical ||
				!slices.Equal(measure.Input.Path, []string{"http", "User"}) ||
				measure.Input.Range == (spl.Range{}) ||
				measure.Predicate != nil ||
				measure.Percentile != 0 ||
				measure.Output != test.output {
				t.Fatalf("event aggregate = %#v", eventAggregate)
			}
			if len(eventAggregate.GroupBy) != 2 ||
				eventAggregate.GroupBy[0].Name != "host" ||
				eventAggregate.GroupBy[1].Name != "Host" ||
				eventAggregate.GroupBy[0].Range == (spl.Range{}) ||
				eventAggregate.GroupBy[1].Range == (spl.Range{}) {
				t.Fatalf(
					"event aggregate groups = %#v, want source-resolved host/Host",
					eventAggregate.GroupBy,
				)
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
		})
	}
}

func TestBuildEventStatsStringAggregatesUpsertFixedOutputSchema(t *testing.T) {
	t.Parallel()

	for _, aggregate := range eventStatsStringAggregatePlanCases() {
		t.Run(aggregate.name, func(t *testing.T) {
			t.Parallel()

			for _, test := range []struct {
				name   string
				source string
				want   []string
			}{
				{
					name: "append multivalue output",
					source: fmt.Sprintf(
						"index=gradethis | table _time,host,user | eventstats %s(user) AS %s BY host",
						aggregate.call,
						aggregate.output,
					),
					want: []string{"_time", "host", "user", aggregate.output},
				},
				{
					name: "replace alias in place",
					source: fmt.Sprintf(
						"index=gradethis | table _time,%s,host,user | eventstats %s(user) AS %s BY host",
						aggregate.output,
						aggregate.call,
						aggregate.output,
					),
					want: []string{"_time", aggregate.output, "host", "user"},
				},
				{
					name: "preserve statistics relation",
					source: fmt.Sprintf(
						"index=gradethis | stats count AS events BY host | eventstats %s(host) AS observed_hosts",
						aggregate.call,
					),
					want: []string{"host", "events", "observed_hosts"},
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
					if !slices.Equal(logical.OutputFields, test.want) || logical.DynamicOutput != nil {
						t.Fatalf(
							"output = fields %v dynamic %#v, want fixed %v",
							logical.OutputFields,
							logical.DynamicOutput,
							test.want,
						)
					}
				})
			}
		})
	}
}

func TestBuildEventStatsStringAggregatesProtectReservedOpenSchemaFields(t *testing.T) {
	t.Parallel()

	for _, test := range eventStatsStringAggregatePlanCases() {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			for _, source := range []string{
				fmt.Sprintf("index=gradethis | eventstats %s(fields) AS %s", test.call, test.output),
				fmt.Sprintf("index=gradethis | eventstats %s(user) AS fields", test.call),
				fmt.Sprintf(
					"index=gradethis | eventstats %s(user) AS %s BY fields",
					test.call,
					test.output,
				),
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
					fmt.Sprintf(
						"index=gradethis | table fields,host | eventstats %s(fields) AS %s BY host",
						test.call,
						test.output,
					),
				),
				testScope([]string{"gradethis"}, nil),
			)
			if err != nil {
				t.Fatalf("Build closed-schema input: %v", err)
			}
			if !slices.Equal(logical.OutputFields, []string{"fields", "host", test.output}) {
				t.Fatalf("closed input output fields = %v", logical.OutputFields)
			}

			logical, err = Build(
				mustParse(
					t,
					fmt.Sprintf(
						"index=gradethis | table fields,host,user | eventstats %s(user) AS fields BY host",
						test.call,
					),
				),
				testScope([]string{"gradethis"}, nil),
			)
			if err != nil {
				t.Fatalf("Build closed-schema output: %v", err)
			}
			if !slices.Equal(logical.OutputFields, []string{"fields", "host", "user"}) {
				t.Fatalf("closed output fields = %v", logical.OutputFields)
			}
		})
	}
}

func TestBuildEventStatsStringAggregatesPreserveTimeAndIndexProvenance(t *testing.T) {
	t.Parallel()

	for _, test := range eventStatsStringAggregatePlanCases() {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if _, err := Build(
				mustParse(
					t,
					fmt.Sprintf(
						"index=gradethis | eventstats %s(user) AS %s | timechart span=5m count BY level",
						test.call,
						test.output,
					),
				),
				testScope([]string{"gradethis"}, nil),
			); err != nil {
				t.Fatalf("ordinary %s output invalidated canonical time: %v", test.call, err)
			}

			_, err := Build(
				mustParse(
					t,
					fmt.Sprintf(
						"index=gradethis | eventstats %s(user) AS _time | timechart span=5m count BY level",
						test.call,
					),
				),
				testScope([]string{"gradethis"}, nil),
			)
			assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_TIMECHART_TIME_FIELD")

			overwrittenTime, err := Build(
				mustParse(
					t,
					fmt.Sprintf("index=gradethis | eventstats %s(user) AS _time", test.call),
				),
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
					fmt.Sprintf(
						"index=gradethis | eventstats %s(user) AS index | search index=secret",
						test.call,
					),
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
					fmt.Sprintf(
						"index=gradethis | eventstats %s(user) AS %s | search index=secret",
						test.call,
						test.output,
					),
				),
				testScope([]string{"gradethis"}, nil),
			)
			assertDiagnosticCode(t, err, "SPL_INDEX_FORBIDDEN")
		})
	}
}

func TestBuildEventStatsStringAggregatesRejectForgedAggregateMetadata(t *testing.T) {
	t.Parallel()

	for _, aggregate := range eventStatsStringAggregatePlanCases() {
		t.Run(aggregate.name, func(t *testing.T) {
			t.Parallel()

			base := mustParse(t, `index=gradethis`)
			fieldRange := eventStatsStringAggregateTestRange()
			valid := spl.StatsAggregate{
				Function:      aggregate.splFunction,
				Input:         "user",
				InputRange:    fieldRange,
				Alias:         aggregate.output,
				ExplicitAlias: true,
				Range:         fieldRange,
				AliasRange:    fieldRange,
			}
			validQuery := &spl.Query{
				Search: base.Search,
				Commands: []spl.Command{&spl.EventStatsCommand{
					Aggregate: valid,
				}},
				Range: base.Range,
			}
			if _, err := Build(validQuery, testScope([]string{"gradethis"}, nil)); err != nil {
				t.Fatalf("Build forged-valid %s AST: %v", aggregate.call, err)
			}

			for _, test := range []struct {
				name     string
				mutate   func(*spl.StatsAggregate)
				wantCode string
			}{
				{"missing input", func(value *spl.StatsAggregate) { value.Input = "" }, "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE"},
				{"missing input range", func(value *spl.StatsAggregate) { value.InputRange = spl.Range{} }, "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE"},
				{"predicate metadata", func(value *spl.StatsAggregate) { value.Predicate = &spl.WhereComparisonExpr{} }, "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE"},
				{"percentile metadata", func(value *spl.StatsAggregate) { value.Percentile = 95 }, "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE"},
				{"implicit alias", func(value *spl.StatsAggregate) { value.ExplicitAlias = false }, "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE"},
				{"empty output", func(value *spl.StatsAggregate) { value.Alias = "" }, "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE"},
				{"private output", func(value *spl.StatsAggregate) { value.Alias = "__os_eventstats_private" }, "SPL_RESERVED_FIELD"},
			} {
				t.Run(test.name, func(t *testing.T) {
					t.Parallel()

					forged := valid
					test.mutate(&forged)
					query := &spl.Query{
						Search: base.Search,
						Commands: []spl.Command{&spl.EventStatsCommand{
							Aggregate: forged,
						}},
						Range: base.Range,
					}
					_, err := Build(query, testScope([]string{"gradethis"}, nil))
					assertDiagnosticCode(t, err, test.wantCode)
				})
			}
		})
	}
}

func TestBuildEventStatsStringAggregatesRejectLongAliasAndForgedGroups(t *testing.T) {
	t.Parallel()

	for _, aggregate := range eventStatsStringAggregatePlanCases() {
		t.Run(aggregate.name, func(t *testing.T) {
			t.Parallel()

			longAlias := strings.Repeat("x", maxFieldNameBytes+1)
			query := mustParse(
				t,
				fmt.Sprintf(
					"index=gradethis | eventstats %s(user) AS %s",
					aggregate.call,
					longAlias,
				),
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
			fieldRange := eventStatsStringAggregateTestRange()
			valid := spl.StatsAggregate{
				Function:      aggregate.splFunction,
				Input:         "user",
				InputRange:    fieldRange,
				Alias:         aggregate.output,
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
		})
	}
}

func TestAnalyzeEventStatsStringAggregatesAcceptResolvedFieldsAndRejectForgery(
	t *testing.T,
) {
	t.Parallel()

	for _, aggregate := range eventStatsStringAggregatePlanCases() {
		t.Run(aggregate.name, func(t *testing.T) {
			t.Parallel()

			input := mustResolveEventAggregateField(t, "http.User")
			host := mustResolveEventAggregateField(t, "host")
			valid := AggregateMeasure{
				Function: aggregate.function,
				Input:    input,
				Output:   aggregate.output,
			}
			operator := &EventAggregate{
				GroupBy: []FieldRef{host},
				Measure: valid,
			}
			analysis, err := Analyze(&Query{Operators: []Operator{operator}})
			if err != nil {
				t.Fatalf("Analyze valid %s: %v", aggregate.call, err)
			}
			if !slices.Equal(analysis.ReferencedFields, []string{"host", "http.User"}) {
				t.Fatalf("referenced fields = %v, want [host http.User]", analysis.ReferencedFields)
			}

			for _, test := range []struct {
				name   string
				mutate func(*AggregateMeasure)
			}{
				{"missing input", func(value *AggregateMeasure) { value.Input = FieldRef{} }},
				{"unresolved input", func(value *AggregateMeasure) { value.Input = FieldRef{Name: input.Name} }},
				{"forged input path", func(value *AggregateMeasure) { value.Input.Path = []string{"attacker"} }},
				{"forged canonical input", func(value *AggregateMeasure) { value.Input.Canonical = true }},
				{"predicate metadata", func(value *AggregateMeasure) { value.Predicate = &ComparisonExpression{} }},
				{"percentile metadata", func(value *AggregateMeasure) { value.Percentile = 50 }},
				{"empty output", func(value *AggregateMeasure) { value.Output = "" }},
				{"private output", func(value *AggregateMeasure) { value.Output = "__os_eventstats_private" }},
			} {
				t.Run(test.name, func(t *testing.T) {
					t.Parallel()

					measure := valid
					test.mutate(&measure)
					assertEventStatsStringAggregatePlanValidatorsReject(
						t,
						aggregate.call,
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
					assertEventStatsStringAggregatePlanValidatorsReject(
						t,
						aggregate.call,
						&EventAggregate{GroupBy: groups, Measure: valid},
					)
				})
			}
		})
	}
}

func eventStatsStringAggregateTestRange() spl.Range {
	return spl.Range{
		Start: spl.Position{Offset: 1, Line: 1, Column: 2},
		End:   spl.Position{Offset: 7, Line: 1, Column: 8},
	}
}

func assertEventStatsStringAggregatePlanValidatorsReject(
	t *testing.T,
	call string,
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
			t.Fatalf("%s accepted forged %s operator %#v", validate.name, call, operator)
		}
	}
}
