package spl

import (
	"errors"
	"testing"
)

func TestParseChartSumAndAveragePreservesExactMeasureAndRanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		source        string
		function      AggregateFunction
		input         string
		output        string
		aggregateText string
		row           string
		column        string
		overSpelled   bool
	}{
		{
			name:          "sum OVER BY",
			source:        `index=main | chart SuM(bytes) OVER path BY status_class`,
			function:      AggregateFunctionSum,
			input:         "bytes",
			output:        "sum(bytes)",
			aggregateText: "SuM(bytes)",
			row:           "path",
			column:        "status_class",
			overSpelled:   true,
		},
		{
			name:          "avg comma-separated BY",
			source:        `index=main | CHART AvG(http.duration) BY service, region`,
			function:      AggregateFunctionAverage,
			input:         "http.duration",
			output:        "avg(http.duration)",
			aggregateText: "AvG(http.duration)",
			row:           "service",
			column:        "region",
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
			if aggregate.Function != test.function || aggregate.Input != test.input ||
				aggregate.Alias != test.output || aggregate.ExplicitAlias ||
				aggregate.Predicate != nil || aggregate.Percentile != 0 ||
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

func TestParseChartNumericMeasureMayMatchEitherAxis(t *testing.T) {
	t.Parallel()

	// Parsing preserves all three exact fields. The planner owns the semantic
	// restriction: a numeric measure cannot also be the row axis, while using
	// the column field as the measure is well-defined and remains supported.
	for _, source := range []string{
		`index=main | chart sum(bytes) OVER bytes BY service`,
		`index=main | chart avg(service) OVER endpoint BY service`,
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

func TestParseChartSumAndAverageRejectsUnsupportedInputShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		source    string
		code      string
		locatedAt string
	}{
		{"wildcard input", `index=main | chart sum(byte*) OVER path BY level`, "SPL_UNSUPPORTED_CHART_SYNTAX", "byte*"},
		{"quoted input", `index=main | chart avg("latency") OVER path BY level`, "SPL_UNSUPPORTED_CHART_SYNTAX", `"latency"`},
		{"missing input", `index=main | chart sum() OVER path BY level`, "SPL_UNSUPPORTED_CHART_SYNTAX", ")"},
		{"missing closing parenthesis", `index=main | chart avg(latency OVER path BY level`, "SPL_UNSUPPORTED_CHART_SYNTAX", "OVER"},
		{"multiple inputs", `index=main | chart sum(bytes, other) OVER path BY level`, "SPL_UNSUPPORTED_CHART_SYNTAX", ","},
		{"eval input", `index=main | chart avg(eval(latency>0)) OVER path BY level`, "SPL_UNSUPPORTED_CHART_SYNTAX", "eval"},
		{"missing call", `index=main | chart sum OVER path BY level`, "SPL_UNSUPPORTED_CHART_SYNTAX", "sum"},
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
		})
	}
}

func TestParseChartSumAndAverageRejectsAdditionalAggregatesAliasesAndOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		source    string
		code      string
		locatedAt string
	}{
		{"unsupported function", `index=main | chart max(bytes) OVER path BY level`, "SPL_UNSUPPORTED_CHART_AGGREGATE", "max"},
		{"second aggregate", `index=main | chart sum(bytes) avg(latency) OVER path BY level`, "SPL_UNSUPPORTED_CHART_AGGREGATE", "avg"},
		{"comma-separated aggregates", `index=main | chart avg(latency), sum(bytes) OVER path BY level`, "SPL_UNSUPPORTED_CHART_AGGREGATE", ","},
		{"sum alias", `index=main | chart sum(bytes) AS total OVER path BY level`, "SPL_UNSUPPORTED_CHART_AGGREGATE", "AS"},
		{"unsupported average spelling", `index=main | chart average(latency) AS mean OVER path BY level`, "SPL_UNSUPPORTED_CHART_AGGREGATE", "average"},
		{"option before aggregate", `index=main | chart limit=10 sum(bytes) OVER path BY level`, "SPL_UNSUPPORTED_CHART_OPTION", "limit"},
		{"option after aggregate", `index=main | chart avg(latency) limit=10 OVER path BY level`, "SPL_UNSUPPORTED_CHART_OPTION", "limit"},
		{"option after axes", `index=main | chart sum(bytes) OVER path BY level useother=true`, "SPL_UNSUPPORTED_CHART_OPTION", "useother"},
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
		})
	}
}
