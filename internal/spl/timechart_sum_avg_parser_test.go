package spl

import (
	"errors"
	"slices"
	"testing"
)

func TestParseTimechartSumAndAverageCanonicalizesAndLocatesAggregate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		source            string
		function          AggregateFunction
		input             string
		output            string
		explicitAlias     bool
		wantAggregateText string
		wantAliasText     string
	}{
		{
			name:              "sum default output",
			source:            `index=main | timechart span=30s SUM(bytes)`,
			function:          AggregateFunctionSum,
			input:             "bytes",
			output:            "sum(bytes)",
			wantAggregateText: "SUM(bytes)",
			wantAliasText:     "SUM(bytes)",
		},
		{
			name:              "average exact alias",
			source:            `index=main | TIMECHART SPAN=2H AvG(http.duration) AS mean_ms`,
			function:          AggregateFunctionAverage,
			input:             "http.duration",
			output:            "mean_ms",
			explicitAlias:     true,
			wantAggregateText: "AvG(http.duration) AS mean_ms",
			wantAliasText:     "mean_ms",
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
			command, ok := query.Commands[0].(*TimechartCommand)
			if !ok {
				t.Fatalf("command = %T, want *TimechartCommand", query.Commands[0])
			}
			aggregate := command.Aggregate
			if aggregate.Function != test.function || aggregate.Percentile != 0 ||
				aggregate.Input != test.input || aggregate.Alias != test.output ||
				aggregate.ExplicitAlias != test.explicitAlias || command.SplitBy != nil {
				t.Fatalf("timechart = %#v", command)
			}
			assertSourceRangeText(t, test.source, aggregate.Range, test.wantAggregateText)
			assertSourceRangeText(t, test.source, aggregate.InputRange, test.input)
			assertSourceRangeText(t, test.source, aggregate.AliasRange, test.wantAliasText)
			wantCommand := test.source[len("index=main | "):]
			assertSourceRangeText(t, test.source, command.SourceRange(), wantCommand)
		})
	}
}

func TestParseTimechartSumAndAverageRejectsUnsupportedShapesAtSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		source    string
		code      string
		locatedAt string
	}{
		{"split sum", `index=main | timechart span=5m sum(bytes) BY service`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", "BY"},
		{"split average", `index=main | timechart span=5m avg(latency) BY service`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", "BY"},
		{"wildcard input", `index=main | timechart span=5m sum(byte*)`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", "byte*"},
		{"quoted input", `index=main | timechart span=5m avg("latency")`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", `"latency"`},
		{"missing input", `index=main | timechart span=5m sum()`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", ")"},
		{"missing closing parenthesis", `index=main | timechart span=5m avg(latency`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", ""},
		{"multiple inputs", `index=main | timechart span=5m sum(bytes,other)`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", ","},
		{"eval input", `index=main | timechart span=5m avg(eval(latency>0))`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", "eval"},
		{"missing call", `index=main | timechart span=5m sum`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", "sum"},
		{"second aggregate", `index=main | timechart span=5m sum(bytes) avg(latency)`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", "avg"},
		{"quoted alias", `index=main | timechart span=5m avg(latency) AS "mean"`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", `"mean"`},
		{"wildcard alias", `index=main | timechart span=5m sum(bytes) AS total_*`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", "total_*"},
		{"missing alias", `index=main | timechart span=5m avg(latency) AS`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", ""},
		{"unsupported aggregate", `index=main | timechart span=5m min(latency)`, "SPL_UNSUPPORTED_TIMECHART_AGGREGATE", "min"},
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
			if !errors.As(err, &diagnostic) || diagnostic.Code != test.code {
				t.Fatalf("diagnostic = %#v, want %s", err, test.code)
			}
			assertSourceRangeText(t, test.source, diagnostic.Range, test.locatedAt)
			if len(diagnostic.Suggestions) == 0 {
				t.Fatalf("diagnostic has no timechart suggestion: %#v", diagnostic)
			}
		})
	}
}

func TestTimechartSumAndAverageSuggestionContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		source        string
		kinds         []SuggestionKind
		functionNames []string
		keywords      []string
	}{
		{
			source:        `| timechart span=5m a`,
			kinds:         []SuggestionKind{SuggestionKindFunction},
			functionNames: []string{"count", "p50", "p95", "sum", "avg"},
		},
		{
			source: `| timechart span=5m sum(`,
			kinds:  []SuggestionKind{SuggestionKindField},
		},
		{
			source:   `| timechart span=5m avg(latency) `,
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"AS"},
		},
		{
			source: `| timechart span=5m sum(bytes) AS t`,
			kinds:  []SuggestionKind{SuggestionKindField},
		},
		{
			source: `| timechart span=5m avg(latency) BY `,
		},
	}
	for _, test := range tests {
		context, diagnostic := AnalyzeSuggestionContext(test.source, len(test.source))
		if diagnostic != nil {
			t.Fatalf("AnalyzeSuggestionContext(%q): %v", test.source, diagnostic)
		}
		if !slices.Equal(context.Kinds, test.kinds) ||
			!slices.Equal(context.FunctionNames, test.functionNames) ||
			!slices.Equal(context.Keywords, test.keywords) {
			t.Errorf("context for %q = %#v", test.source, context)
		}
	}
}

func TestClassifySumAndAverageTimechartsAsStaticTimeSeries(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | timechart span=5m sum(bytes)`,
		`index=main | timechart span=5m avg(latency) AS mean`,
	} {
		query, err := Parse(source)
		if err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
		if got := ClassifyResultShape(query); got != (ResultShape{Kind: ResultKindTimeSeries}) {
			t.Fatalf("ClassifyResultShape(%q) = %#v, want static time series", source, got)
		}
	}
}

func TestSuggestTimechartSumAndAverageExamples(t *testing.T) {
	t.Parallel()

	for source, want := range map[string][]string{
		`| timechart span=5m a`: {"avg"},
		`| timechart span=5m s`: {"sum"},
	} {
		result := Suggest(source, len(source), 20)
		if result.Diagnostic != nil {
			t.Fatalf("Suggest(%q): %v", source, result.Diagnostic)
		}
		if labels := suggestionLabels(result.Suggestions); !slices.Equal(labels, want) {
			t.Fatalf("suggestions for %q = %v, want %v", source, labels, want)
		}
	}
}
