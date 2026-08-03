package spl

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
)

type eventStatsStringAggregateParserCase struct {
	name            string
	call            string
	mixedCaseCall   string
	function        AggregateFunction
	alias           string
	mixedCaseAlias  string
	suggestionAlias string
	unsupportedCall string
}

func eventStatsStringAggregateParserCases() []eventStatsStringAggregateParserCase {
	return []eventStatsStringAggregateParserCase{
		eventStatsValuesParserCase(),
		eventStatsListParserCase(),
	}
}

func TestParseEventStatsStringAggregatesAcceptExactFieldsAndPreserveSourceRanges(
	t *testing.T,
) {
	t.Parallel()

	for _, test := range eventStatsStringAggregateParserCases() {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			source := fmt.Sprintf(
				"index=main\n| EvEnTsTaTs %s(http.User) aS %s bY Host, source",
				test.mixedCaseCall,
				test.mixedCaseAlias,
			)
			query, err := Parse(source)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if len(query.Commands) != 1 {
				t.Fatalf("command count = %d, want 1", len(query.Commands))
			}
			command, ok := query.Commands[0].(*EventStatsCommand)
			if !ok {
				t.Fatalf("command = %T, want *EventStatsCommand", query.Commands[0])
			}
			aggregate := command.Aggregate
			if aggregate.Function != test.function ||
				aggregate.Input != "http.User" ||
				aggregate.Predicate != nil ||
				aggregate.Percentile != 0 ||
				aggregate.Alias != test.mixedCaseAlias ||
				!aggregate.ExplicitAlias {
				t.Fatalf(
					"aggregate = %#v, want %s(http.User) AS %s",
					aggregate,
					test.call,
					test.mixedCaseAlias,
				)
			}
			if len(command.GroupBy) != 2 ||
				command.GroupBy[0].Name != "Host" ||
				command.GroupBy[1].Name != "source" {
				t.Fatalf("group fields = %#v, want case-preserved Host/source", command.GroupBy)
			}

			commandText := fmt.Sprintf(
				"EvEnTsTaTs %s(http.User) aS %s bY Host, source",
				test.mixedCaseCall,
				test.mixedCaseAlias,
			)
			assertSourceRangeText(t, source, command.SourceRange(), commandText)
			assertSourceRangeText(
				t,
				source,
				aggregate.Range,
				fmt.Sprintf("%s(http.User) aS %s", test.mixedCaseCall, test.mixedCaseAlias),
			)
			assertSourceRangeText(t, source, aggregate.InputRange, "http.User")
			assertSourceRangeText(t, source, aggregate.AliasRange, test.mixedCaseAlias)
			assertSourceRangeText(t, source, command.GroupBy[0].Range, "Host")
			assertSourceRangeText(t, source, command.GroupBy[1].Range, "source")
		})
	}
}

func TestParseEventStatsStringAggregatesAcceptGlobalAndCaseDistinctGroups(t *testing.T) {
	t.Parallel()

	for _, test := range eventStatsStringAggregateParserCases() {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			globalSource := fmt.Sprintf(
				"index=main | eventstats %s(user) AS %s",
				test.call,
				test.alias,
			)
			query, err := Parse(globalSource)
			if err != nil {
				t.Fatalf("Parse global %s: %v", test.call, err)
			}
			command := query.Commands[0].(*EventStatsCommand)
			if len(command.GroupBy) != 0 ||
				command.Aggregate.Function != test.function ||
				command.Aggregate.Input != "user" ||
				command.Aggregate.Alias != test.alias ||
				!command.Aggregate.ExplicitAlias {
				t.Fatalf("command = %#v, want global %s(user) AS %s", command, test.call, test.alias)
			}

			groupedSource := globalSource + " BY host,Host"
			query, err = Parse(groupedSource)
			if err != nil {
				t.Fatalf("Parse case-distinct groups: %v", err)
			}
			command = query.Commands[0].(*EventStatsCommand)
			if len(command.GroupBy) != 2 ||
				command.GroupBy[0].Name != "host" ||
				command.GroupBy[1].Name != "Host" {
				t.Fatalf("groups = %#v, want case-distinct host/Host", command.GroupBy)
			}
		})
	}
}

func TestParseEventStatsStringAggregatesRequireExplicitExactAlias(t *testing.T) {
	t.Parallel()

	for _, test := range eventStatsStringAggregateParserCases() {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			for _, suffix := range []string{"", " BY host"} {
				source := fmt.Sprintf("index=main | eventstats %s(user)%s", test.call, suffix)
				_, err := Parse(source)
				var diagnostic *Diagnostic
				if !errors.As(err, &diagnostic) {
					t.Fatalf("Parse(%q) error = %#v, want *Diagnostic", source, err)
				}
				wantMessage := fmt.Sprintf(
					"eventstats %s(field) requires AS followed by an output field name",
					test.call,
				)
				if diagnostic.Code != "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX" ||
					diagnostic.Message != wantMessage {
					t.Fatalf("Parse(%q) diagnostic = %#v", source, diagnostic)
				}
				assertSourceRangeText(
					t,
					source,
					diagnostic.Range,
					fmt.Sprintf("%s(user)", test.call),
				)
				wantSuggestion := fmt.Sprintf(
					"eventstats %s(field) AS %s",
					test.call,
					test.suggestionAlias,
				)
				if !slices.Contains(diagnostic.Suggestions, wantSuggestion) {
					t.Fatalf(
						"Parse(%q) suggestions = %v, want %q",
						source,
						diagnostic.Suggestions,
						wantSuggestion,
					)
				}
			}
		})
	}
}

func TestParseEventStatsStringAggregatesRejectUnsupportedShapes(t *testing.T) {
	t.Parallel()

	for _, aggregate := range eventStatsStringAggregateParserCases() {
		aggregate := aggregate
		t.Run(aggregate.name, func(t *testing.T) {
			t.Parallel()

			prefix := "index=main | eventstats "
			call := aggregate.call
			alias := aggregate.alias
			for _, test := range []struct {
				name      string
				source    string
				code      string
				rangeText string
			}{
				{"missing call", prefix + call + " AS " + alias, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", call},
				{"empty input", prefix + call + "() AS " + alias, "SPL_EXPECTED_FIELD", ")"},
				{"quoted input", prefix + call + `("user") AS ` + alias, "SPL_EXPECTED_FIELD", `"user"`},
				{"wildcard input", prefix + call + "(*) AS " + alias, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "*"},
				{"prefix wildcard input", prefix + call + "(user*) AS " + alias, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "user*"},
				{"eval input", prefix + call + "(eval(user)) AS " + alias, "SPL_EXPECTED_RIGHT_PAREN", "("},
				{"multiple inputs", prefix + call + "(user,host) AS " + alias, "SPL_EXPECTED_RIGHT_PAREN", ","},
				{"missing alias value", prefix + call + "(user) AS", "SPL_EXPECTED_FIELD", ""},
				{"wildcard output", prefix + call + "(user) AS result_*", "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "result_*"},
				{"quoted output", prefix + call + `(user) AS "result"`, "SPL_EXPECTED_FIELD", `"result"`},
				{"space separated second measure", prefix + call + "(user) AS " + alias + " count", "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "count"},
				{"comma separated second measure", prefix + call + "(user) AS " + alias + ", count", "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", ","},
				{"leading unsupported option", prefix + "allnum=true " + call + "(user) AS " + alias, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "allnum"},
				{"trailing unsupported option", prefix + call + "(user) AS " + alias + " allnum=true", "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "allnum"},
			} {
				test := test
				t.Run(test.name, func(t *testing.T) {
					t.Parallel()

					_, err := Parse(test.source)
					var diagnostic *Diagnostic
					if !errors.As(err, &diagnostic) {
						t.Fatalf("Parse error = %#v, want *Diagnostic", err)
					}
					if diagnostic.Code != test.code {
						t.Fatalf(
							"code = %q, want %q (diagnostic: %v)",
							diagnostic.Code,
							test.code,
							diagnostic,
						)
					}
					if test.rangeText != "" {
						assertSourceRangeText(t, test.source, diagnostic.Range, test.rangeText)
					}
				})
			}
		})
	}
}

func TestParseEventStatsStringAggregatesBoundExactGroupingFields(t *testing.T) {
	t.Parallel()

	groups := make([]string, 0, MaximumStatsGroupFields+1)
	for index := 0; index <= MaximumStatsGroupFields; index++ {
		groups = append(groups, fmt.Sprintf("field%d", index))
	}

	for _, aggregate := range eventStatsStringAggregateParserCases() {
		aggregate := aggregate
		t.Run(aggregate.name, func(t *testing.T) {
			t.Parallel()

			prefix := fmt.Sprintf(
				"index=main | eventstats %s(user) AS %s BY ",
				aggregate.call,
				aggregate.alias,
			)
			boundedSource := prefix + strings.Join(groups[:MaximumStatsGroupFields], ",")
			query, err := Parse(boundedSource)
			if err != nil {
				t.Fatalf("Parse maximum groups: %v", err)
			}
			if got := len(query.Commands[0].(*EventStatsCommand).GroupBy); got != MaximumStatsGroupFields {
				t.Fatalf("group fields = %d, want %d", got, MaximumStatsGroupFields)
			}

			for _, test := range []struct {
				name      string
				source    string
				code      string
				rangeText string
			}{
				{
					name:      "over limit",
					source:    prefix + strings.Join(groups, ","),
					code:      "SPL_QUERY_TOO_COMPLEX",
					rangeText: fmt.Sprintf("field%d", MaximumStatsGroupFields),
				},
				{"empty", strings.TrimSuffix(prefix, " "), "SPL_EXPECTED_FIELD", ""},
				{"duplicate exact field", prefix + "host,host", "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "host"},
				{"wildcard", prefix + "host*", "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "host*"},
				{"quoted", prefix + `"host"`, "SPL_EXPECTED_FIELD", `"host"`},
			} {
				test := test
				t.Run(test.name, func(t *testing.T) {
					t.Parallel()

					_, err := Parse(test.source)
					var diagnostic *Diagnostic
					if !errors.As(err, &diagnostic) || diagnostic.Code != test.code {
						t.Fatalf("Parse error = %#v, want %s", err, test.code)
					}
					if test.rangeText != "" {
						assertSourceRangeText(t, test.source, diagnostic.Range, test.rangeText)
					}
				})
			}
		})
	}
}

func TestEventStatsDiagnosticsAdvertiseStringAggregateForms(t *testing.T) {
	t.Parallel()

	for _, test := range eventStatsStringAggregateParserCases() {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := Parse(`index=main | eventstats`)
			var diagnostic *Diagnostic
			acceptedForm := fmt.Sprintf("%s(field) AS output", test.call)
			if !errors.As(err, &diagnostic) ||
				diagnostic.Code != "SPL_EXPECTED_AGGREGATE" ||
				!strings.Contains(diagnostic.Message, acceptedForm) {
				t.Fatalf("diagnostic = %#v, want %s accepted form", err, acceptedForm)
			}

			_, err = Parse(fmt.Sprintf(
				"index=main | eventstats %s(user) AS %s",
				test.unsupportedCall,
				test.alias,
			))
			wantSuggestion := fmt.Sprintf(
				"eventstats %s(field) AS %s BY group",
				test.call,
				test.suggestionAlias,
			)
			if !errors.As(err, &diagnostic) ||
				!slices.Contains(diagnostic.Suggestions, wantSuggestion) {
				t.Fatalf(
					"unsupported aggregate diagnostic = %#v, want %q",
					err,
					wantSuggestion,
				)
			}
		})
	}
}

func TestClassifyResultShapeTreatsEventStatsStringAggregatesAsRowPreserving(t *testing.T) {
	t.Parallel()

	for _, test := range eventStatsStringAggregateParserCases() {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			query, err := Parse(fmt.Sprintf(
				"index=main | stats count AS events BY host | eventstats %s(host) AS observed_hosts",
				test.call,
			))
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if got := ClassifyResultShape(query); got != (ResultShape{Kind: ResultKindStatistics}) {
				t.Fatalf("ClassifyResultShape = %#v, want preserved statistics relation", got)
			}
		})
	}
}
