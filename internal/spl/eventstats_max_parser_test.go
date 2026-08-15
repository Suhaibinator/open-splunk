package spl

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestParseEventStatsMaximumRejectsUnsupportedInputsAndMultipleMeasures(
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
			source:    `index=main | eventstats max AS maximum`,
			code:      "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
			rangeText: "max",
		},
		{
			name:      "empty input",
			source:    `index=main | eventstats max() AS maximum`,
			code:      "SPL_EXPECTED_FIELD",
			rangeText: ")",
		},
		{
			name:      "multiple inputs",
			source:    `index=main | eventstats max(latency,other) AS maximum`,
			code:      "SPL_EXPECTED_RIGHT_PAREN",
			rangeText: ",",
		},
		{
			name:      "eval input",
			source:    `index=main | eventstats max(eval(latency)) AS maximum`,
			code:      "SPL_EXPECTED_RIGHT_PAREN",
			rangeText: "(",
		},
		{
			name:      "wildcard input",
			source:    `index=main | eventstats max(*) AS maximum`,
			code:      "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
			rangeText: "*",
		},
		{
			name:      "prefix wildcard input",
			source:    `index=main | eventstats max(latency*) AS maximum`,
			code:      "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
			rangeText: "latency*",
		},
		{
			name:      "quoted input",
			source:    `index=main | eventstats max("latency") AS maximum`,
			code:      "SPL_EXPECTED_FIELD",
			rangeText: `"latency"`,
		},
		{
			name:      "wildcard output",
			source:    `index=main | eventstats max(latency) AS max*`,
			code:      "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
			rangeText: "max*",
		},
		{
			name:      "quoted output",
			source:    `index=main | eventstats max(latency) AS "maximum"`,
			code:      "SPL_EXPECTED_FIELD",
			rangeText: `"maximum"`,
		},
		{
			name:      "space separated second measure",
			source:    `index=main | eventstats max(latency) AS maximum count`,
			code:      "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
			rangeText: "count",
		},
		{
			name:      "comma separated second measure",
			source:    `index=main | eventstats max(latency) AS maximum, count`,
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

func TestParseEventStatsMaximumBoundsGroupingFields(t *testing.T) {
	t.Parallel()

	buildSource := func(groupFields int) string {
		var source strings.Builder
		source.WriteString("index=main | eventstats max(latency) AS maximum BY ")
		for index := range groupFields {
			if index > 0 {
				source.WriteString(", ")
			}
			fmt.Fprintf(&source, "field%d", index)
		}
		return source.String()
	}

	boundedSource := buildSource(MaximumStatsGroupFields)
	query, err := Parse(boundedSource)
	if err != nil {
		t.Fatalf("Parse(%d grouping fields): %v", MaximumStatsGroupFields, err)
	}
	command := query.Commands[0].(*EventStatsCommand)
	if len(command.GroupBy) != MaximumStatsGroupFields {
		t.Fatalf("grouping fields = %d, want %d", len(command.GroupBy), MaximumStatsGroupFields)
	}

	overflowSource := buildSource(MaximumStatsGroupFields + 1)
	_, err = Parse(overflowSource)
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
		t.Fatalf("diagnostic = %#v, want SPL_QUERY_TOO_COMPLEX", err)
	}
	assertSourceRangeText(
		t,
		overflowSource,
		diagnostic.Range,
		fmt.Sprintf("field%d", MaximumStatsGroupFields),
	)
}
