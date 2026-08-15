package spl

import (
	"errors"
	"strings"
	"testing"
)

func TestParseStreamStatsExtremaField(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name, functionName, source, input, alias string
		function                                 AggregateFunction
		explicitAlias                            bool
		current                                  bool
		window                                   uint64
		global                                   bool
		groups                                   []string
	}{
		{
			name: "minimum canonical", functionName: "min", function: AggregateFunctionMinimum,
			source: `index=main | streamstats MiN(Payload.Value)`, input: "Payload.Value",
			alias: "min(Payload.Value)", current: true, global: true,
		},
		{
			name: "minimum bounded group", functionName: "min", function: AggregateFunctionMinimum,
			source: `index=main | streamstats current=f window=3 global=f min(payload.value) AS prior_min BY service`,
			input:  "payload.value", alias: "prior_min", explicitAlias: true, window: 3,
			groups: []string{"service"},
		},
		{
			name: "minimum trailing options", functionName: "min", function: AggregateFunctionMinimum,
			source: `index=main | streamstats min(value) BY service current=false window=0 global=t`,
			input:  "value", alias: "min(value)", global: true, groups: []string{"service"},
		},
		{
			name: "maximum canonical", functionName: "max", function: AggregateFunctionMaximum,
			source: `index=main | streamstats MaX(Payload.Value)`, input: "Payload.Value",
			alias: "max(Payload.Value)", current: true, global: true,
		},
		{
			name: "maximum bounded group", functionName: "max", function: AggregateFunctionMaximum,
			source: `index=main | streamstats current=f window=3 global=f MAX(payload.value) AS prior_max BY service`,
			input:  "payload.value", alias: "prior_max", explicitAlias: true, window: 3,
			groups: []string{"service"},
		},
		{
			name: "maximum trailing options", functionName: "max", function: AggregateFunctionMaximum,
			source: `index=main | streamstats max(value) BY service current=false window=0 global=t`,
			input:  "value", alias: "max(value)", global: true, groups: []string{"service"},
		},
	} {
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
			if command.Aggregate.Function != test.function ||
				command.Aggregate.Input != test.input ||
				command.Aggregate.InputRange == (Range{}) ||
				command.Aggregate.Predicate != nil ||
				command.Aggregate.Percentile != 0 ||
				command.Aggregate.Alias != test.alias ||
				command.Aggregate.ExplicitAlias != test.explicitAlias ||
				command.Current != test.current ||
				command.Window != test.window ||
				command.Global != test.global {
				t.Fatalf("streamstats %s(field) command = %#v", test.functionName, command)
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
				return
			}
			functionStart := strings.Index(strings.ToLower(test.source), test.functionName+"(")
			assertSourceRangeText(
				t,
				test.source,
				command.Aggregate.AliasRange,
				test.source[functionStart:functionStart+len(test.functionName)+len(test.input)+2],
			)
		})
	}
}

func TestParseStreamStatsExtremaRejectsNonExactShapes(t *testing.T) {
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
		{"bare maximum", `index=main | streamstats max`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", "max"},
		{"empty maximum", `index=main | streamstats max()`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", "max"},
		{"eval maximum", `index=main | streamstats max(eval(value>0))`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", "max"},
		{"wildcard maximum", `index=main | streamstats max(value*)`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "value*"},
		{"quoted maximum", `index=main | streamstats max("value")`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", `"value"`},
		{"multiple maximum inputs", `index=main | streamstats max(value, other)`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", ","},
		{"missing maximum close", `index=main | streamstats max(value`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", ""},
		{"maximum synonym", `index=main | streamstats maximum(value)`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", "maximum"},
		{"wildcard maximum alias", `index=main | streamstats max(value) AS high*`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "high*"},
		{"maximum alias after by", `index=main | streamstats max(value) BY host AS high`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "AS"},
	} {
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
