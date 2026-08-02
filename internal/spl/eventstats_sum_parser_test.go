package spl

import (
	"errors"
	"slices"
	"testing"
)

func TestParseEventStatsFieldAggregatesAcceptExactFieldAndPreserveSourceRanges(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name          string
		function      AggregateFunction
		functionName  string
		source        string
		input         string
		output        string
		groupNames    []string
		commandText   string
		aggregateText string
	}{
		{
			name:          "minimum global",
			function:      AggregateFunctionMinimum,
			functionName:  "min",
			source:        `index=main | eventstats min(latency_ms) AS minimum_latency`,
			input:         "latency_ms",
			output:        "minimum_latency",
			commandText:   "eventstats min(latency_ms) AS minimum_latency",
			aggregateText: "min(latency_ms) AS minimum_latency",
		},
		{
			name:          "minimum grouped case insensitive function and keywords",
			function:      AggregateFunctionMinimum,
			functionName:  "min",
			source:        "index=main\n| EvEnTsTaTs MiN(http.latency) aS floorLatency bY Host, source",
			input:         "http.latency",
			output:        "floorLatency",
			groupNames:    []string{"Host", "source"},
			commandText:   "EvEnTsTaTs MiN(http.latency) aS floorLatency bY Host, source",
			aggregateText: "MiN(http.latency) aS floorLatency",
		},
		{
			name:          "sum global",
			function:      AggregateFunctionSum,
			functionName:  "sum",
			source:        `index=main | eventstats sum(bytes) AS total_bytes`,
			input:         "bytes",
			output:        "total_bytes",
			commandText:   "eventstats sum(bytes) AS total_bytes",
			aggregateText: "sum(bytes) AS total_bytes",
		},
		{
			name:          "sum grouped case insensitive function and keywords",
			function:      AggregateFunctionSum,
			functionName:  "sum",
			source:        "index=main\n| EvEnTsTaTs SuM(http.bytes) aS totalBytes bY Host, source",
			input:         "http.bytes",
			output:        "totalBytes",
			groupNames:    []string{"Host", "source"},
			commandText:   "EvEnTsTaTs SuM(http.bytes) aS totalBytes bY Host, source",
			aggregateText: "SuM(http.bytes) aS totalBytes",
		},
		{
			name:          "average global",
			function:      AggregateFunctionAverage,
			functionName:  "avg",
			source:        `index=main | eventstats avg(duration_ms) AS mean_ms`,
			input:         "duration_ms",
			output:        "mean_ms",
			commandText:   "eventstats avg(duration_ms) AS mean_ms",
			aggregateText: "avg(duration_ms) AS mean_ms",
		},
		{
			name:          "average grouped case insensitive function and keywords",
			function:      AggregateFunctionAverage,
			functionName:  "avg",
			source:        "index=main\n| EvEnTsTaTs AvG(http.duration) aS meanDuration bY Host, source",
			input:         "http.duration",
			output:        "meanDuration",
			groupNames:    []string{"Host", "source"},
			commandText:   "EvEnTsTaTs AvG(http.duration) aS meanDuration bY Host, source",
			aggregateText: "AvG(http.duration) aS meanDuration",
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
			if aggregate.Function != test.function ||
				aggregate.Input != test.input ||
				aggregate.InputRange == (Range{}) ||
				aggregate.Predicate != nil ||
				aggregate.Percentile != 0 ||
				aggregate.Alias != test.output ||
				!aggregate.ExplicitAlias {
				t.Fatalf(
					"aggregate = %#v, want %s(%s) AS %s",
					aggregate,
					test.functionName,
					test.input,
					test.output,
				)
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

func TestParseEventStatsFieldAggregatesRequireExplicitAlias(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		function   string
		input      string
		suggestion string
	}{
		{"minimum", "min", "latency_ms", "eventstats min(field) AS minimum"},
		{"sum", "sum", "bytes", "eventstats sum(field) AS total"},
		{"average", "avg", "duration_ms", "eventstats avg(field) AS mean"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			for _, suffix := range []string{"", " BY host"} {
				source := "index=main | eventstats " + test.function + "(" + test.input + ")" + suffix
				_, err := Parse(source)
				var diagnostic *Diagnostic
				if !errors.As(err, &diagnostic) {
					t.Fatalf("Parse(%q) error = %#v, want *Diagnostic", source, err)
				}
				wantMessage := "eventstats " + test.function +
					"(field) requires AS followed by an output field name"
				if diagnostic.Code != "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX" ||
					diagnostic.Message != wantMessage {
					t.Fatalf("Parse(%q) diagnostic = %#v", source, diagnostic)
				}
				assertSourceRangeText(
					t,
					source,
					diagnostic.Range,
					test.function+"("+test.input+")",
				)
				if !slices.Contains(diagnostic.Suggestions, test.suggestion) {
					t.Fatalf(
						"Parse(%q) suggestions = %v, want %q",
						source,
						diagnostic.Suggestions,
						test.suggestion,
					)
				}
			}
		})
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
