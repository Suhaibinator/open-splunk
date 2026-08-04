package spl

import (
	"errors"
	"strings"
	"testing"
)

func TestParseStreamStatsMinimumField(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name, source, input, alias string
		explicitAlias              bool
		current                    bool
		window                     uint64
		global                     bool
		groups                     []string
	}{
		{
			name:    "canonical default preserves input spelling",
			source:  `index=main | streamstats MiN(Payload.Value)`,
			input:   "Payload.Value",
			alias:   "min(Payload.Value)",
			current: true,
			global:  true,
		},
		{
			name:          "prior bounded group window",
			source:        `index=main | streamstats current=f window=3 global=f min(payload.value) AS prior_min BY service`,
			input:         "payload.value",
			alias:         "prior_min",
			explicitAlias: true,
			window:        3,
			groups:        []string{"service"},
		},
		{
			name:   "trailing options",
			source: `index=main | streamstats min(value) BY service current=false window=0 global=t`,
			input:  "value",
			alias:  "min(value)",
			global: true,
			groups: []string{"service"},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			query, err := Parse(test.source)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			command, ok := query.Commands[0].(*StreamStatsCommand)
			if !ok {
				t.Fatalf("command = %T, want *StreamStatsCommand", query.Commands[0])
			}
			if command.Aggregate.Function != AggregateFunctionMinimum ||
				command.Aggregate.Input != test.input ||
				command.Aggregate.InputRange == (Range{}) ||
				command.Aggregate.Predicate != nil ||
				command.Aggregate.Percentile != 0 ||
				command.Aggregate.Alias != test.alias ||
				command.Aggregate.ExplicitAlias != test.explicitAlias ||
				command.Current != test.current ||
				command.Window != test.window ||
				command.Global != test.global {
				t.Fatalf("streamstats min(field) command = %#v", command)
			}
			if len(command.GroupBy) != len(test.groups) {
				t.Fatalf("groups = %#v, want %v", command.GroupBy, test.groups)
			}
			for index, want := range test.groups {
				if command.GroupBy[index].Name != want {
					t.Fatalf("group %d = %#v, want %q", index, command.GroupBy[index], want)
				}
			}
			assertSourceRangeText(t, test.source, command.Aggregate.InputRange, test.input)
			if test.explicitAlias {
				assertSourceRangeText(t, test.source, command.Aggregate.AliasRange, test.alias)
			} else {
				functionStart := strings.Index(strings.ToLower(test.source), "min(")
				assertSourceRangeText(
					t,
					test.source,
					command.Aggregate.AliasRange,
					test.source[functionStart:functionStart+len("min(")+len(test.input)+1],
				)
			}
		})
	}
}

func TestParseStreamStatsMinimumRejectsNonExactShapes(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name, source, code, rangeText string
	}{
		{"bare minimum", `index=main | streamstats min`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", "min"},
		{"empty minimum", `index=main | streamstats min()`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", "min"},
		{"eval minimum", `index=main | streamstats min(eval(value>0))`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", "min"},
		{"wildcard minimum", `index=main | streamstats min(value*)`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "value*"},
		{"quoted minimum", `index=main | streamstats min("value")`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", `"value"`},
		{"multiple minimum inputs", `index=main | streamstats min(value, other)`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", ","},
		{"missing minimum close", `index=main | streamstats min(value`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", ""},
		{"minimum synonym", `index=main | streamstats minimum(value)`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", "minimum"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := Parse(test.source)
			if err == nil {
				t.Fatal("Parse succeeded, want error")
			}
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != test.code {
				t.Fatalf("diagnostic = %#v, want %s", err, test.code)
			}
			assertSourceRangeText(t, test.source, diagnostic.Range, test.rangeText)
		})
	}
}
