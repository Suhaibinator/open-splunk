package spl

import (
	"errors"
	"slices"
	"testing"
)

func TestParseStreamStatsChronologicalFields(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name, source, input, alias, aliasSource string
		function                                AggregateFunction
		explicitAlias                           bool
		current                                 bool
		window                                  uint64
		global                                  bool
		groups                                  []string
	}{
		{
			name:        "case-insensitive earliest with canonical default",
			source:      `index=main | streamstats EaRlIeSt(Payload.Value)`,
			input:       "Payload.Value",
			alias:       "earliest(Payload.Value)",
			aliasSource: "EaRlIeSt(Payload.Value)",
			function:    AggregateFunctionEarliest,
			current:     true,
			global:      true,
		},
		{
			name:          "bounded prior latest by group",
			source:        `index=main | streamstats current=f window=3 global=f LATEST(value) AS prior_latest BY service`,
			input:         "value",
			alias:         "prior_latest",
			aliasSource:   "prior_latest",
			function:      AggregateFunctionLatest,
			explicitAlias: true,
			window:        3,
			groups:        []string{"service"},
		},
		{
			name:        "trailing options retain existing contract",
			source:      `index=main | streamstats earliest(status) BY host current=false window=0 global=t`,
			input:       "status",
			alias:       "earliest(status)",
			aliasSource: "earliest(status)",
			function:    AggregateFunctionEarliest,
			global:      true,
			groups:      []string{"host"},
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
				t.Fatalf("streamstats chronological command = %#v", command)
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
			assertSourceRangeText(t, test.source, command.Aggregate.AliasRange, test.aliasSource)
		})
	}
}

func TestParseStreamStatsChronologicalRejectsBroaderShapes(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name, source, code, rangeText string
	}{
		{"bare earliest", `index=main | streamstats earliest`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", "earliest"},
		{"empty latest", `index=main | streamstats latest()`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", "latest"},
		{"eval earliest", `index=main | streamstats earliest(eval(value))`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", "earliest"},
		{"wildcard latest", `index=main | streamstats latest(value*)`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "value*"},
		{"quoted earliest", `index=main | streamstats earliest("value")`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", `"value"`},
		{"multiple latest inputs", `index=main | streamstats latest(value, other)`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", ","},
		{"missing earliest close", `index=main | streamstats earliest(value`, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE", ""},
		{"earliest alias after by", `index=main | streamstats earliest(value) BY host AS first`, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX", "AS"},
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

func TestAnalyzeStreamStatsChronologicalSuggestions(t *testing.T) {
	t.Parallel()

	for _, function := range []string{"earliest", "latest"} {
		call := `| streamstats ` + function + `(`
		context, diagnostic := AnalyzeSuggestionContext(call, len(call))
		if diagnostic != nil {
			t.Fatalf("AnalyzeSuggestionContext(%q): %v", call, diagnostic)
		}
		if !slices.Equal(context.Kinds, []SuggestionKind{SuggestionKindField}) {
			t.Fatalf("context for %q = %#v, want field", call, context)
		}
	}
}
