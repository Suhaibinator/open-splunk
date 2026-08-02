package spl

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
)

func TestParseEventStatsDistinctCountAcceptsExactFieldAndPreservesSourceRanges(
	t *testing.T,
) {
	t.Parallel()

	const source = "index=main\n| EvEnTsTaTs Dc(http.user) aS uniqueUsers bY Host, source"
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
	if aggregate.Function != AggregateFunctionDistinctCount ||
		aggregate.Input != "http.user" ||
		aggregate.Predicate != nil ||
		aggregate.Percentile != 0 ||
		aggregate.Alias != "uniqueUsers" ||
		!aggregate.ExplicitAlias {
		t.Fatalf("aggregate = %#v, want dc(http.user) AS uniqueUsers", aggregate)
	}
	if len(command.GroupBy) != 2 ||
		command.GroupBy[0].Name != "Host" ||
		command.GroupBy[1].Name != "source" {
		t.Fatalf("group fields = %#v, want Host/source", command.GroupBy)
	}
	assertSourceRangeText(
		t,
		source,
		command.SourceRange(),
		"EvEnTsTaTs Dc(http.user) aS uniqueUsers bY Host, source",
	)
	assertSourceRangeText(t, source, aggregate.Range, "Dc(http.user) aS uniqueUsers")
	assertSourceRangeText(t, source, aggregate.InputRange, "http.user")
	assertSourceRangeText(t, source, aggregate.AliasRange, "uniqueUsers")
	assertSourceRangeText(t, source, command.GroupBy[0].Range, "Host")
	assertSourceRangeText(t, source, command.GroupBy[1].Range, "source")
}

func TestParseEventStatsDistinctCountAcceptsMaximumGroupingFields(t *testing.T) {
	t.Parallel()

	groups := make([]string, 0, MaximumStatsGroupFields)
	for index := 0; index < MaximumStatsGroupFields; index++ {
		groups = append(groups, fmt.Sprintf("field%d", index))
	}
	source := "index=main | eventstats dc(user) AS unique_users BY " +
		strings.Join(groups, ",")
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*EventStatsCommand)
	if len(command.GroupBy) != MaximumStatsGroupFields {
		t.Fatalf(
			"group fields = %d, want %d",
			len(command.GroupBy),
			MaximumStatsGroupFields,
		)
	}
}

func TestParseEventStatsDistinctCountAcceptsGlobalForm(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | eventstats dc(user) AS unique_users`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*EventStatsCommand)
	if len(command.GroupBy) != 0 ||
		command.Aggregate.Function != AggregateFunctionDistinctCount ||
		command.Aggregate.Input != "user" ||
		command.Aggregate.Alias != "unique_users" ||
		!command.Aggregate.ExplicitAlias {
		t.Fatalf("command = %#v, want global dc(user) AS unique_users", command)
	}
}

func TestParseEventStatsDistinctCountRequiresExplicitAlias(t *testing.T) {
	t.Parallel()

	for _, suffix := range []string{"", " BY host"} {
		source := "index=main | eventstats dc(user)" + suffix
		_, err := Parse(source)
		var diagnostic *Diagnostic
		if !errors.As(err, &diagnostic) {
			t.Fatalf("Parse(%q) error = %#v, want *Diagnostic", source, err)
		}
		if diagnostic.Code != "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX" ||
			diagnostic.Message != "eventstats dc(field) requires AS followed by an output field name" {
			t.Fatalf("Parse(%q) diagnostic = %#v", source, diagnostic)
		}
		assertSourceRangeText(t, source, diagnostic.Range, "dc(user)")
		if !slices.Contains(
			diagnostic.Suggestions,
			"eventstats dc(field) AS distinct_values",
		) {
			t.Fatalf(
				"Parse(%q) suggestions = %v, want dc form",
				source,
				diagnostic.Suggestions,
			)
		}
	}
}

func TestParseEventStatsDistinctCountRejectsUnsupportedShapes(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		source    string
		code      string
		rangeText string
	}{
		{"long spelling", `index=main | eventstats distinct_count(user) AS unique_users`, "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE", "distinct_count"},
		{"missing call", `index=main | eventstats dc AS unique_users`, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "dc"},
		{"empty input", `index=main | eventstats dc() AS unique_users`, "SPL_EXPECTED_FIELD", ")"},
		{"quoted input", `index=main | eventstats dc("user") AS unique_users`, "SPL_EXPECTED_FIELD", `"user"`},
		{"wildcard input", `index=main | eventstats dc(*) AS unique_users`, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "*"},
		{"prefix wildcard input", `index=main | eventstats dc(user*) AS unique_users`, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "user*"},
		{"eval input", `index=main | eventstats dc(eval(user)) AS unique_users`, "SPL_EXPECTED_RIGHT_PAREN", "("},
		{"multiple inputs", `index=main | eventstats dc(user,host) AS unique_users`, "SPL_EXPECTED_RIGHT_PAREN", ","},
		{"second measure", `index=main | eventstats dc(user) AS unique_users count`, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "count"},
	} {
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
			assertSourceRangeText(t, test.source, diagnostic.Range, test.rangeText)
		})
	}
}

func TestEventStatsDiagnosticsAdvertiseDistinctCountForm(t *testing.T) {
	t.Parallel()

	_, err := Parse(`index=main | eventstats`)
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) {
		t.Fatalf("Parse error = %#v, want *Diagnostic", err)
	}
	if diagnostic.Code != "SPL_EXPECTED_AGGREGATE" ||
		!strings.Contains(diagnostic.Message, "dc(field) AS output") {
		t.Fatalf("diagnostic = %#v, want dc(field) accepted form", diagnostic)
	}

	_, err = Parse(`index=main | eventstats distinct_count(user) AS unique_users`)
	if !errors.As(err, &diagnostic) ||
		!slices.Contains(
			diagnostic.Suggestions,
			"eventstats dc(field) AS distinct_values BY group",
		) {
		t.Fatalf("unsupported aggregate diagnostic = %#v, want dc suggestion", err)
	}
}
