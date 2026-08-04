package spl

import (
	"errors"
	"slices"
	"testing"
)

func TestParseChartCountFieldCanonicalizesAndPreservesExactInput(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name        string
		source      string
		input       string
		row         string
		column      string
		overSpelled bool
	}{
		{
			name:        "OVER BY spelling",
			source:      `index=main | CHART CoUnT(Http.Status) OVER endpoint BY service`,
			input:       "Http.Status",
			row:         "endpoint",
			column:      "service",
			overSpelled: true,
		},
		{
			name:   "comma separated BY spelling",
			source: `index=main | chart count(payload) BY route, service`,
			input:  "payload",
			row:    "route",
			column: "service",
		},
	} {
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
			if aggregate.Function != AggregateFunctionCountValues ||
				aggregate.Input != test.input || aggregate.Predicate != nil ||
				aggregate.Percentile != 0 ||
				aggregate.Alias != "count("+test.input+")" ||
				aggregate.ExplicitAlias || command.Over.Name != test.row ||
				command.SplitBy.Name != test.column ||
				command.OverSpelledOver != test.overSpelled {
				t.Fatalf("chart = %#v", command)
			}
			assertSourceRangeText(t, test.source, aggregate.InputRange, test.input)
			assertSourceRangeText(
				t,
				test.source,
				aggregate.AliasRange,
				test.source[aggregate.Range.Start.Offset:aggregate.Range.End.Offset],
			)
		})
	}
}

func TestParseChartCountFieldPreservesBareCount(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | chart count OVER path BY service`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	aggregate := query.Commands[0].(*ChartCommand).Aggregate
	if aggregate.Function != AggregateFunctionCount || aggregate.Input != "" ||
		aggregate.Alias != "count" || aggregate.ExplicitAlias {
		t.Fatalf("bare count = %#v", aggregate)
	}
}

func TestParseChartCountFieldRejectsNonExactAndAliasedShapes(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		source    string
		code      string
		locatedAt string
	}{
		{"empty call", `index=main | chart count() OVER path BY service`, "SPL_UNSUPPORTED_CHART_SYNTAX", ")"},
		{"wildcard input", `index=main | chart count(stat*) OVER path BY service`, "SPL_UNSUPPORTED_CHART_SYNTAX", "stat*"},
		{"quoted input", `index=main | chart count("status") OVER path BY service`, "SPL_UNSUPPORTED_CHART_SYNTAX", `"status"`},
		{"single quoted input", `index=main | chart count('status') OVER path BY service`, "SPL_UNSUPPORTED_CHART_SYNTAX", `'status'`},
		{"backtick quoted input", "index=main | chart count(`status`) OVER path BY service", "SPL_UNSUPPORTED_CHART_SYNTAX", "`status`"},
		{"eval input", `index=main | chart count(eval(status=500)) OVER path BY service`, "SPL_UNSUPPORTED_CHART_SYNTAX", "eval"},
		{"multiple inputs", `index=main | chart count(status, host) OVER path BY service`, "SPL_UNSUPPORTED_CHART_SYNTAX", ","},
		{"missing close", `index=main | chart count(status OVER path BY service`, "SPL_UNSUPPORTED_CHART_SYNTAX", "OVER"},
		{"shorthand", `index=main | chart c(status) OVER path BY service`, "SPL_UNSUPPORTED_CHART_AGGREGATE", "c"},
		{"explicit alias", `index=main | chart count(status) AS populated OVER path BY service`, "SPL_UNSUPPORTED_CHART_AGGREGATE", "AS"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := Parse(test.source)
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != test.code {
				t.Fatalf("Parse(%q) diagnostic = %#v, want %s", test.source, err, test.code)
			}
			assertSourceRangeText(t, test.source, diagnostic.Range, test.locatedAt)
		})
	}
}

func TestChartCountFieldSuggestionContextAndResultShape(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source   string
		prefix   string
		kinds    []SuggestionKind
		keywords []string
	}{
		{source: `| chart count(`, kinds: []SuggestionKind{SuggestionKindField}},
		{source: `| chart count(Http.`, prefix: "Http.", kinds: []SuggestionKind{SuggestionKindField}},
		{
			source:   `| chart count(status) `,
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"OVER", "BY"},
		},
	} {
		context, diagnostic := AnalyzeSuggestionContext(test.source, len(test.source))
		if diagnostic != nil {
			t.Fatalf("AnalyzeSuggestionContext(%q): %v", test.source, diagnostic)
		}
		if context.Prefix != test.prefix || !slices.Equal(context.Kinds, test.kinds) ||
			!slices.Equal(context.Keywords, test.keywords) {
			t.Fatalf("suggestion context for %q = %#v", test.source, context)
		}
	}

	query, err := Parse(`index=main | chart count(status) OVER path BY service`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := ClassifyResultShape(query); got != (ResultShape{
		Kind:                ResultKindStatistics,
		RuntimeNamedColumns: true,
	}) {
		t.Fatalf("result shape = %#v", got)
	}
}
