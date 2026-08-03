package spl

import (
	"errors"
	"slices"
	"testing"
)

func TestParseChartPercentileCanonicalizesBothAxisSpellingsAndRanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		source        string
		percentile    uint8
		input         string
		output        string
		aggregateText string
		row           string
		column        string
		overSpelled   bool
	}{
		{
			name:          "short spelling OVER BY",
			source:        `index=main | chart P1(latency) OVER path BY service`,
			percentile:    1,
			input:         "latency",
			output:        "perc1(latency)",
			aggregateText: "P1(latency)",
			row:           "path",
			column:        "service",
			overSpelled:   true,
		},
		{
			name:          "leading-zero long spelling BY",
			source:        `index=main | CHART PeRc050(http.duration) BY endpoint, region`,
			percentile:    50,
			input:         "http.duration",
			output:        "perc50(http.duration)",
			aggregateText: "PeRc050(http.duration)",
			row:           "endpoint",
			column:        "region",
		},
		{
			name:          "upper boundary whitespace BY",
			source:        `index=main | chart perc99(metric) BY endpoint service`,
			percentile:    99,
			input:         "metric",
			output:        "perc99(metric)",
			aggregateText: "perc99(metric)",
			row:           "endpoint",
			column:        "service",
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
			command, ok := query.Commands[0].(*ChartCommand)
			if !ok {
				t.Fatalf("command = %T, want *ChartCommand", query.Commands[0])
			}
			aggregate := command.Aggregate
			if aggregate.Function != AggregateFunctionPercentile ||
				aggregate.Percentile != test.percentile ||
				aggregate.Input != test.input || aggregate.Alias != test.output ||
				aggregate.ExplicitAlias || aggregate.Predicate != nil ||
				command.Over.Name != test.row || command.SplitBy.Name != test.column ||
				command.OverSpelledOver != test.overSpelled {
				t.Fatalf("chart = %#v", command)
			}
			assertSourceRangeText(t, test.source, aggregate.Range, test.aggregateText)
			assertSourceRangeText(t, test.source, aggregate.InputRange, test.input)
			assertSourceRangeText(t, test.source, aggregate.AliasRange, test.aggregateText)
			assertSourceRangeText(t, test.source, command.Over.Range, test.row)
			assertSourceRangeText(t, test.source, command.SplitBy.Range, test.column)
			assertSourceRangeText(
				t,
				test.source,
				command.SourceRange(),
				test.source[len("index=main | "):],
			)
		})
	}
}

func TestParseChartPercentilePreservesAxisMatchingMeasureForPlanner(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | chart p95(latency) OVER latency BY service`,
		`index=main | chart perc50(service) BY endpoint, service`,
	} {
		query, err := Parse(source)
		if err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
		command := query.Commands[0].(*ChartCommand)
		if command.Aggregate.Input != command.Over.Name &&
			command.Aggregate.Input != command.SplitBy.Name {
			t.Fatalf("Parse(%q) lost the axis-matching measure: %#v", source, command)
		}
	}
}

func TestParseChartPercentileRejectsUnsupportedSurfaceAtSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		source    string
		code      string
		locatedAt string
	}{
		{"zero percentile", `index=main | chart p0(latency) OVER path BY service`, "SPL_UNSUPPORTED_CHART_AGGREGATE", "p0"},
		{"hundredth percentile", `index=main | chart perc100(latency) OVER path BY service`, "SPL_UNSUPPORTED_CHART_AGGREGATE", "perc100"},
		{"decimal suffix", `index=main | chart p99.5(latency) OVER path BY service`, "SPL_UNSUPPORTED_CHART_AGGREGATE", "p99.5"},
		{"missing suffix", `index=main | chart perc(latency) OVER path BY service`, "SPL_UNSUPPORTED_CHART_AGGREGATE", "perc"},
		{"malformed suffix", `index=main | chart pfoo(latency) OVER path BY service`, "SPL_UNSUPPORTED_CHART_AGGREGATE", "pfoo"},
		{"two argument spelling", `index=main | chart perc(latency,95) OVER path BY service`, "SPL_UNSUPPORTED_CHART_AGGREGATE", "perc"},
		{"wildcard input", `index=main | chart p95(lat*) OVER path BY service`, "SPL_UNSUPPORTED_CHART_SYNTAX", "lat*"},
		{"quoted input", `index=main | chart perc50("latency") OVER path BY service`, "SPL_UNSUPPORTED_CHART_SYNTAX", `"latency"`},
		{"missing input", `index=main | chart p95() OVER path BY service`, "SPL_UNSUPPORTED_CHART_SYNTAX", ")"},
		{"eval input", `index=main | chart p95(eval(latency>0)) OVER path BY service`, "SPL_UNSUPPORTED_CHART_SYNTAX", "eval"},
		{"multiple inputs", `index=main | chart p95(latency,bytes) OVER path BY service`, "SPL_UNSUPPORTED_CHART_SYNTAX", ","},
		{"missing call", `index=main | chart p95 OVER path BY service`, "SPL_UNSUPPORTED_CHART_SYNTAX", "p95"},
		{"missing closing parenthesis", `index=main | chart p95(latency OVER path BY service`, "SPL_UNSUPPORTED_CHART_SYNTAX", "OVER"},
		{"explicit alias", `index=main | chart p95(latency) AS latency_p95 OVER path BY service`, "SPL_UNSUPPORTED_CHART_AGGREGATE", "AS"},
		{"second aggregate", `index=main | chart p95(latency) p99(latency) OVER path BY service`, "SPL_UNSUPPORTED_CHART_AGGREGATE", "p99"},
		{"comma aggregate", `index=main | chart p95(latency), sum(bytes) OVER path BY service`, "SPL_UNSUPPORTED_CHART_AGGREGATE", ","},
		{"unsupported function", `index=main | chart exactperc95(latency) OVER path BY service`, "SPL_UNSUPPORTED_CHART_AGGREGATE", "exactperc95"},
		{"aggregate option", `index=main | chart agg=p95 p95(latency) OVER path BY service`, "SPL_UNSUPPORTED_CHART_AGGREGATE", "agg"},
		{"option remains unsupported", `index=main | chart limit=10 p95(latency) OVER path BY service`, "SPL_UNSUPPORTED_CHART_OPTION", "limit"},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := Parse(test.source)
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != test.code {
				t.Fatalf("Parse(%q) diagnostic = %#v, want %s", test.source, err, test.code)
			}
			assertSourceRangeText(t, test.source, diagnostic.Range, test.locatedAt)
			if !slices.Contains(
				diagnostic.Suggestions,
				"chart p95(field) OVER row BY column",
			) {
				t.Fatalf("diagnostic has no percentile chart suggestion: %#v", diagnostic)
			}
		})
	}
}

func TestChartPercentileSuggestionContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		source        string
		kinds         []SuggestionKind
		functionNames []string
		keywords      []string
	}{
		{
			source:        `| chart p`,
			kinds:         []SuggestionKind{SuggestionKindFunction},
			functionNames: []string{"count", "p50", "p95", "sum", "avg"},
		},
		{source: `| chart p95(`, kinds: []SuggestionKind{SuggestionKindField}},
		{source: `| chart perc050(http.`, kinds: []SuggestionKind{SuggestionKindField}},
		{
			source:   `| chart p95(latency) `,
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"OVER", "BY"},
		},
		{source: `| chart p95(latency) OVER `, kinds: []SuggestionKind{SuggestionKindField}},
		{source: `| chart perc50(latency) OVER path BY `, kinds: []SuggestionKind{SuggestionKindField}},
		{source: `| chart p95(latency) BY `, kinds: []SuggestionKind{SuggestionKindField}},
	}

	for _, test := range tests {
		test := test
		t.Run(test.source, func(t *testing.T) {
			t.Parallel()

			context, diagnostic := AnalyzeSuggestionContext(test.source, len(test.source))
			if diagnostic != nil {
				t.Fatalf("AnalyzeSuggestionContext(%q): %v", test.source, diagnostic)
			}
			if !slices.Equal(context.Kinds, test.kinds) ||
				!slices.Equal(context.FunctionNames, test.functionNames) ||
				!slices.Equal(context.Keywords, test.keywords) {
				t.Fatalf("context for %q = %#v", test.source, context)
			}
		})
	}
}

func TestSuggestChartPercentileExamples(t *testing.T) {
	t.Parallel()

	source := `| chart p`
	result := Suggest(source, len(source), 20)
	if result.Diagnostic != nil {
		t.Fatalf("Suggest: %v", result.Diagnostic)
	}
	if labels := suggestionLabels(result.Suggestions); !slices.Equal(labels, []string{"p50", "p95"}) {
		t.Fatalf("suggestions = %v, want p50/p95", labels)
	}
}
