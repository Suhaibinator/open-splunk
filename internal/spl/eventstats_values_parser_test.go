package spl

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
)

func TestParseEventStatsValuesAcceptsExactFieldAndPreservesSourceRanges(
	t *testing.T,
) {
	t.Parallel()

	const source = "index=main\n| EvEnTsTaTs VaLuEs(http.User) aS uniqueUsers bY Host, source"
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
	if aggregate.Function != AggregateFunctionValues ||
		aggregate.Input != "http.User" ||
		aggregate.Predicate != nil ||
		aggregate.Percentile != 0 ||
		aggregate.Alias != "uniqueUsers" ||
		!aggregate.ExplicitAlias {
		t.Fatalf("aggregate = %#v, want values(http.User) AS uniqueUsers", aggregate)
	}
	if len(command.GroupBy) != 2 ||
		command.GroupBy[0].Name != "Host" ||
		command.GroupBy[1].Name != "source" {
		t.Fatalf("group fields = %#v, want case-preserved Host/source", command.GroupBy)
	}
	assertSourceRangeText(
		t,
		source,
		command.SourceRange(),
		"EvEnTsTaTs VaLuEs(http.User) aS uniqueUsers bY Host, source",
	)
	assertSourceRangeText(t, source, aggregate.Range, "VaLuEs(http.User) aS uniqueUsers")
	assertSourceRangeText(t, source, aggregate.InputRange, "http.User")
	assertSourceRangeText(t, source, aggregate.AliasRange, "uniqueUsers")
	assertSourceRangeText(t, source, command.GroupBy[0].Range, "Host")
	assertSourceRangeText(t, source, command.GroupBy[1].Range, "source")
}

func TestParseEventStatsValuesAcceptsGlobalFormAndCaseDistinctGroups(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | eventstats values(user) AS unique_users`)
	if err != nil {
		t.Fatalf("Parse global values: %v", err)
	}
	command := query.Commands[0].(*EventStatsCommand)
	if len(command.GroupBy) != 0 ||
		command.Aggregate.Function != AggregateFunctionValues ||
		command.Aggregate.Input != "user" ||
		command.Aggregate.Alias != "unique_users" ||
		!command.Aggregate.ExplicitAlias {
		t.Fatalf("command = %#v, want global values(user) AS unique_users", command)
	}

	query, err = Parse(
		`index=main | eventstats values(user) AS unique_users BY host,Host`,
	)
	if err != nil {
		t.Fatalf("Parse case-distinct groups: %v", err)
	}
	command = query.Commands[0].(*EventStatsCommand)
	if len(command.GroupBy) != 2 ||
		command.GroupBy[0].Name != "host" ||
		command.GroupBy[1].Name != "Host" {
		t.Fatalf("groups = %#v, want case-distinct host/Host", command.GroupBy)
	}
}

func TestParseEventStatsValuesRequiresExplicitExactAlias(t *testing.T) {
	t.Parallel()

	for _, suffix := range []string{"", " BY host"} {
		source := "index=main | eventstats values(user)" + suffix
		_, err := Parse(source)
		var diagnostic *Diagnostic
		if !errors.As(err, &diagnostic) {
			t.Fatalf("Parse(%q) error = %#v, want *Diagnostic", source, err)
		}
		if diagnostic.Code != "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX" ||
			diagnostic.Message != "eventstats values(field) requires AS followed by an output field name" {
			t.Fatalf("Parse(%q) diagnostic = %#v", source, diagnostic)
		}
		assertSourceRangeText(t, source, diagnostic.Range, "values(user)")
		if !slices.Contains(
			diagnostic.Suggestions,
			"eventstats values(field) AS distinct_values",
		) {
			t.Fatalf("Parse(%q) suggestions = %v, want values form", source, diagnostic.Suggestions)
		}
	}
}

func TestParseEventStatsValuesRejectsUnsupportedShapes(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		source    string
		code      string
		rangeText string
	}{
		{"missing call", `index=main | eventstats values AS unique_users`, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "values"},
		{"empty input", `index=main | eventstats values() AS unique_users`, "SPL_EXPECTED_FIELD", ")"},
		{"quoted input", `index=main | eventstats values("user") AS unique_users`, "SPL_EXPECTED_FIELD", `"user"`},
		{"wildcard input", `index=main | eventstats values(*) AS unique_users`, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "*"},
		{"prefix wildcard input", `index=main | eventstats values(user*) AS unique_users`, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "user*"},
		{"eval input", `index=main | eventstats values(eval(user)) AS unique_users`, "SPL_EXPECTED_RIGHT_PAREN", "("},
		{"multiple inputs", `index=main | eventstats values(user,host) AS unique_users`, "SPL_EXPECTED_RIGHT_PAREN", ","},
		{"wildcard output", `index=main | eventstats values(user) AS unique_*`, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "unique_*"},
		{"quoted output", `index=main | eventstats values(user) AS "unique_users"`, "SPL_EXPECTED_FIELD", `"unique_users"`},
		{"space separated second measure", `index=main | eventstats values(user) AS unique_users count`, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "count"},
		{"comma separated second measure", `index=main | eventstats values(user) AS unique_users, count`, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", ","},
		{"unsupported option", `index=main | eventstats values(user) AS unique_users allnum=true`, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "allnum"},
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
				t.Fatalf("code = %q, want %q (diagnostic: %v)", diagnostic.Code, test.code, diagnostic)
			}
			assertSourceRangeText(t, test.source, diagnostic.Range, test.rangeText)
		})
	}
}

func TestParseEventStatsValuesBoundsExactGroupingFields(t *testing.T) {
	t.Parallel()

	groups := make([]string, 0, MaximumStatsGroupFields+1)
	for index := 0; index <= MaximumStatsGroupFields; index++ {
		groups = append(groups, fmt.Sprintf("field%d", index))
	}
	boundedSource := "index=main | eventstats values(user) AS unique_users BY " +
		strings.Join(groups[:MaximumStatsGroupFields], ",")
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
			name: "over limit",
			source: "index=main | eventstats values(user) AS unique_users BY " +
				strings.Join(groups, ","),
			code:      "SPL_QUERY_TOO_COMPLEX",
			rangeText: fmt.Sprintf("field%d", MaximumStatsGroupFields),
		},
		{"empty", `index=main | eventstats values(user) AS unique_users BY`, "SPL_EXPECTED_FIELD", ""},
		{"duplicate exact field", `index=main | eventstats values(user) AS unique_users BY host,host`, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "host"},
		{"wildcard", `index=main | eventstats values(user) AS unique_users BY host*`, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "host*"},
		{"quoted", `index=main | eventstats values(user) AS unique_users BY "host"`, "SPL_EXPECTED_FIELD", `"host"`},
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
}

func TestEventStatsDiagnosticsAdvertiseValuesForm(t *testing.T) {
	t.Parallel()

	_, err := Parse(`index=main | eventstats`)
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) ||
		diagnostic.Code != "SPL_EXPECTED_AGGREGATE" ||
		!strings.Contains(diagnostic.Message, "values(field) AS output") {
		t.Fatalf("diagnostic = %#v, want values(field) accepted form", err)
	}

	_, err = Parse(`index=main | eventstats value(user) AS unique_users`)
	if !errors.As(err, &diagnostic) ||
		!slices.Contains(
			diagnostic.Suggestions,
			"eventstats values(field) AS distinct_values BY group",
		) {
		t.Fatalf("unsupported aggregate diagnostic = %#v, want values suggestion", err)
	}
}

func TestClassifyResultShapeTreatsEventStatsValuesAsRowPreserving(t *testing.T) {
	t.Parallel()

	query, err := Parse(
		`index=main | stats count AS events BY host` +
			` | eventstats values(host) AS observed_hosts`,
	)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := ClassifyResultShape(query); got != (ResultShape{Kind: ResultKindStatistics}) {
		t.Fatalf("ClassifyResultShape = %#v, want preserved statistics relation", got)
	}
}
