package spl

import (
	"errors"
	"slices"
	"testing"
)

func TestParseEventStatsSumAcceptsExactFieldAndPreservesSourceRanges(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name          string
		source        string
		input         string
		output        string
		groupNames    []string
		commandText   string
		aggregateText string
	}{
		{
			name:          "global",
			source:        `index=main | eventstats sum(bytes) AS total_bytes`,
			input:         "bytes",
			output:        "total_bytes",
			commandText:   "eventstats sum(bytes) AS total_bytes",
			aggregateText: "sum(bytes) AS total_bytes",
		},
		{
			name:          "grouped case insensitive function and keywords",
			source:        "index=main\n| EvEnTsTaTs SuM(http.bytes) aS totalBytes bY Host, source",
			input:         "http.bytes",
			output:        "totalBytes",
			groupNames:    []string{"Host", "source"},
			commandText:   "EvEnTsTaTs SuM(http.bytes) aS totalBytes bY Host, source",
			aggregateText: "SuM(http.bytes) aS totalBytes",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			query, err := Parse(test.source)
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
			if aggregate.Function != AggregateFunctionSum ||
				aggregate.Input != test.input ||
				aggregate.InputRange == (Range{}) ||
				aggregate.Predicate != nil ||
				aggregate.Percentile != 0 ||
				aggregate.Alias != test.output ||
				!aggregate.ExplicitAlias {
				t.Fatalf("aggregate = %#v, want sum(%s) AS %s", aggregate, test.input, test.output)
			}
			if len(command.GroupBy) != len(test.groupNames) {
				t.Fatalf("group fields = %#v, want %v", command.GroupBy, test.groupNames)
			}
			for index, want := range test.groupNames {
				if command.GroupBy[index].Name != want {
					t.Fatalf("group field %d = %q, want %q", index, command.GroupBy[index].Name, want)
				}
				assertSourceRangeText(t, test.source, command.GroupBy[index].Range, want)
			}
			assertSourceRangeText(t, test.source, command.SourceRange(), test.commandText)
			assertSourceRangeText(t, test.source, aggregate.Range, test.aggregateText)
			assertSourceRangeText(t, test.source, aggregate.InputRange, test.input)
			assertSourceRangeText(t, test.source, aggregate.AliasRange, test.output)
		})
	}
}

func TestParseEventStatsSumRequiresExplicitAlias(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eventstats sum(bytes)`,
		`index=main | eventstats sum(bytes) BY host`,
	} {
		_, err := Parse(source)
		var diagnostic *Diagnostic
		if !errors.As(err, &diagnostic) {
			t.Fatalf("Parse(%q) error = %#v, want *Diagnostic", source, err)
		}
		if diagnostic.Code != "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX" ||
			diagnostic.Message != "eventstats sum(field) requires AS followed by an output field name" {
			t.Fatalf("Parse(%q) diagnostic = %#v", source, diagnostic)
		}
		assertSourceRangeText(t, source, diagnostic.Range, "sum(bytes)")
		if !slices.Contains(diagnostic.Suggestions, "eventstats sum(field) AS total") {
			t.Fatalf("Parse(%q) suggestions = %v, want sum alias form", source, diagnostic.Suggestions)
		}
	}
}

func TestParseEventStatsSumRejectsUnsupportedInputsAndMultipleMeasures(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name      string
		source    string
		code      string
		rangeText string
	}{
		{
			name:      "missing input call",
			source:    `index=main | eventstats sum AS total`,
			code:      "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
			rangeText: "sum",
		},
		{
			name:      "empty input",
			source:    `index=main | eventstats sum() AS total`,
			code:      "SPL_EXPECTED_FIELD",
			rangeText: ")",
		},
		{
			name:      "multiple inputs",
			source:    `index=main | eventstats sum(bytes,other) AS total`,
			code:      "SPL_EXPECTED_RIGHT_PAREN",
			rangeText: ",",
		},
		{
			name:      "eval input",
			source:    `index=main | eventstats sum(eval(bytes)) AS total`,
			code:      "SPL_EXPECTED_RIGHT_PAREN",
			rangeText: "(",
		},
		{
			name:      "wildcard input",
			source:    `index=main | eventstats sum(*) AS total`,
			code:      "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
			rangeText: "*",
		},
		{
			name:      "prefix wildcard input",
			source:    `index=main | eventstats sum(byte*) AS total`,
			code:      "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
			rangeText: "byte*",
		},
		{
			name:      "quoted input",
			source:    `index=main | eventstats sum("bytes") AS total`,
			code:      "SPL_EXPECTED_FIELD",
			rangeText: `"bytes"`,
		},
		{
			name:      "space separated second measure",
			source:    `index=main | eventstats sum(bytes) AS total count`,
			code:      "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
			rangeText: "count",
		},
		{
			name:      "comma separated second measure",
			source:    `index=main | eventstats sum(bytes) AS total, count`,
			code:      "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
			rangeText: ",",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := Parse(test.source)
			if err == nil {
				t.Fatal("Parse succeeded, want error")
			}
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) {
				t.Fatalf("error = %T, want *Diagnostic", err)
			}
			if diagnostic.Code != test.code {
				t.Fatalf("code = %q, want %q (diagnostic: %v)", diagnostic.Code, test.code, diagnostic)
			}
			assertSourceRangeText(t, test.source, diagnostic.Range, test.rangeText)
		})
	}
}
