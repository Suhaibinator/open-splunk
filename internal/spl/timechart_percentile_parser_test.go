package spl

import (
	"errors"
	"slices"
	"testing"
)

func TestParseTimechartPercentileCanonicalizesAndLocatesAggregate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		source            string
		percentile        uint8
		input             string
		output            string
		explicitAlias     bool
		wantAggregateText string
		wantAliasText     string
	}{
		{
			name:              "short spelling",
			source:            `index=main | timechart span=30s P1(latency)`,
			percentile:        1,
			input:             "latency",
			output:            "perc1(latency)",
			wantAggregateText: "P1(latency)",
			wantAliasText:     "P1(latency)",
		},
		{
			name:              "leading zero canonicalization",
			source:            `index=main | timechart span=5m p050(http.duration)`,
			percentile:        50,
			input:             "http.duration",
			output:            "perc50(http.duration)",
			wantAggregateText: "p050(http.duration)",
			wantAliasText:     "p050(http.duration)",
		},
		{
			name:              "long spelling and exact alias",
			source:            `index=main | TIMECHART SPAN=2H PeRc99(latency) AS exact_output`,
			percentile:        99,
			input:             "latency",
			output:            "exact_output",
			explicitAlias:     true,
			wantAggregateText: "PeRc99(latency) AS exact_output",
			wantAliasText:     "exact_output",
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
			if aggregate.Function != AggregateFunctionPercentile ||
				aggregate.Percentile != test.percentile ||
				aggregate.Input != test.input ||
				aggregate.Alias != test.output ||
				aggregate.ExplicitAlias != test.explicitAlias ||
				command.SplitBy != nil {
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

func TestParseTimechartPercentileRejectsUnsupportedShapesAtSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		source    string
		code      string
		locatedAt string
	}{
		{"split percentile", `index=main | timechart span=5m p95(latency) BY service`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", "BY"},
		{"wildcard input", `index=main | timechart span=5m p95(lat*)`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", "lat*"},
		{"quoted input", `index=main | timechart span=5m p95("latency")`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", `"latency"`},
		{"missing input", `index=main | timechart span=5m p95()`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", ")"},
		{"missing closing parenthesis", `index=main | timechart span=5m p95(latency`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", ""},
		{"multiple inputs", `index=main | timechart span=5m p95(latency,bytes)`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", ","},
		{"eval input", `index=main | timechart span=5m p95(eval(latency>0))`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", "eval"},
		{"missing call", `index=main | timechart span=5m p95`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", "p95"},
		{"out of range low", `index=main | timechart span=5m p0(latency)`, "SPL_UNSUPPORTED_TIMECHART_AGGREGATE", "p0"},
		{"out of range high", `index=main | timechart span=5m perc100(latency)`, "SPL_UNSUPPORTED_TIMECHART_AGGREGATE", "perc100"},
		{"fractional suffix", `index=main | timechart span=5m p99.5(latency)`, "SPL_UNSUPPORTED_TIMECHART_AGGREGATE", "p99.5"},
		{"other aggregate", `index=main | timechart span=5m min(latency)`, "SPL_UNSUPPORTED_TIMECHART_AGGREGATE", "min"},
		{"second aggregate", `index=main | timechart span=5m p95(latency) p99(latency)`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", "p99"},
		{"quoted alias", `index=main | timechart span=5m p95(latency) AS "p95_latency"`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", `"p95_latency"`},
		{"wildcard alias", `index=main | timechart span=5m p95(latency) AS p95_*`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", "p95_*"},
		{"missing alias", `index=main | timechart span=5m p95(latency) AS`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", ""},
		{"count alias remains unsupported", `index=main | timechart span=5m count AS total`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX", "AS"},
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

func TestTimechartPercentileSuggestionContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		source        string
		kinds         []SuggestionKind
		functionNames []string
		keywords      []string
	}{
		{
			source:        `| timechart span=5m p`,
			kinds:         []SuggestionKind{SuggestionKindFunction},
			functionNames: []string{"count", "p50", "p95", "sum", "avg"},
		},
		{
			source: `| timechart span=5m p95(`,
			kinds:  []SuggestionKind{SuggestionKindField},
		},
		{
			source:   `| timechart span=5m p95(latency) `,
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"AS"},
		},
		{
			source: `| timechart span=5m p95(latency) AS p`,
			kinds:  []SuggestionKind{SuggestionKindField},
		},
		{
			source: `| timechart span=5m p95(latency) BY `,
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

func TestClassifyPercentileTimechartAsStaticTimeSeries(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | timechart span=5m p95(latency) AS p95_latency`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := ClassifyResultShape(query); got != (ResultShape{Kind: ResultKindTimeSeries}) {
		t.Fatalf("ClassifyResultShape = %#v, want static time series", got)
	}
}

func TestSuggestTimechartPercentileExamples(t *testing.T) {
	t.Parallel()

	source := `| timechart span=5m p`
	result := Suggest(source, len(source), 20)
	if result.Diagnostic != nil {
		t.Fatalf("Suggest: %v", result.Diagnostic)
	}
	if labels := suggestionLabels(result.Suggestions); !slices.Equal(labels, []string{"p50", "p95"}) {
		t.Fatalf("suggestions = %v, want p50/p95", labels)
	}
}
