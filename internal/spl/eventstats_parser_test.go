package spl

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestParseEventStatsCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		source     string
		alias      string
		explicit   bool
		groupNames []string
	}{
		{
			name:   "global default alias",
			source: `index=main | eventstats count`,
			alias:  "count",
		},
		{
			name:       "grouped explicit alias",
			source:     "index=main\n| EvEnTsTaTs CoUnT AS event_count BY host, source",
			alias:      "event_count",
			explicit:   true,
			groupNames: []string{"host", "source"},
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
			if command.Name() != "eventstats" {
				t.Fatalf("command name = %q, want eventstats", command.Name())
			}
			if command.Aggregate.Function != AggregateFunctionCount ||
				command.Aggregate.Input != "" ||
				command.Aggregate.Predicate != nil ||
				command.Aggregate.Alias != test.alias ||
				command.Aggregate.ExplicitAlias != test.explicit {
				t.Fatalf("aggregate = %#v, want argument-free count AS %q", command.Aggregate, test.alias)
			}
			if command.SourceRange() != command.Range {
				t.Fatalf("SourceRange() = %#v, want %#v", command.SourceRange(), command.Range)
			}
			if len(command.GroupBy) != len(test.groupNames) {
				t.Fatalf("group fields = %#v, want %v", command.GroupBy, test.groupNames)
			}
			for index, want := range test.groupNames {
				if command.GroupBy[index].Name != want {
					t.Fatalf("group field %d = %q, want %q", index, command.GroupBy[index].Name, want)
				}
			}
		})
	}
}

func TestParseEventStatsCountFieldRequiresExplicitAlias(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		source     string
		input      string
		alias      string
		groupNames []string
	}{
		{
			name:   "global",
			source: `index=main | eventstats count(status) AS populated`,
			input:  "status",
			alias:  "populated",
		},
		{
			name:       "grouped case insensitive function",
			source:     "index=main\n| EvEnTsTaTs CoUnT(productId) AS products BY host, source",
			input:      "productId",
			alias:      "products",
			groupNames: []string{"host", "source"},
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
			command, ok := query.Commands[0].(*EventStatsCommand)
			if !ok {
				t.Fatalf("command = %T, want *EventStatsCommand", query.Commands[0])
			}
			if command.Aggregate.Function != AggregateFunctionCountValues ||
				command.Aggregate.Input != test.input ||
				command.Aggregate.InputRange == (Range{}) ||
				command.Aggregate.Predicate != nil ||
				command.Aggregate.Percentile != 0 ||
				command.Aggregate.Alias != test.alias ||
				!command.Aggregate.ExplicitAlias {
				t.Fatalf(
					"aggregate = %#v, want count(%s) AS %s",
					command.Aggregate,
					test.input,
					test.alias,
				)
			}
			if len(command.GroupBy) != len(test.groupNames) {
				t.Fatalf("group fields = %#v, want %v", command.GroupBy, test.groupNames)
			}
			for index, want := range test.groupNames {
				if command.GroupBy[index].Name != want {
					t.Fatalf("group field %d = %q, want %q", index, command.GroupBy[index].Name, want)
				}
			}
		})
	}
}

func TestParseEventStatsCountPreservesSourceRanges(t *testing.T) {
	t.Parallel()

	source := "index=main\n| eventstats count AS event_count BY host, source"
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*EventStatsCommand)
	assertSourceRangeText(t, source, command.Range, "eventstats count AS event_count BY host, source")
	assertSourceRangeText(t, source, command.Aggregate.Range, "count AS event_count")
	assertSourceRangeText(t, source, command.Aggregate.AliasRange, "event_count")
	assertSourceRangeText(t, source, command.GroupBy[0].Range, "host")
	assertSourceRangeText(t, source, command.GroupBy[1].Range, "source")
}

func TestParseEventStatsCountFieldPreservesSourceRanges(t *testing.T) {
	t.Parallel()

	source := "index=main\n| eventstats count(status) AS populated BY host"
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*EventStatsCommand)
	assertSourceRangeText(t, source, command.Range, "eventstats count(status) AS populated BY host")
	assertSourceRangeText(t, source, command.Aggregate.Range, "count(status) AS populated")
	assertSourceRangeText(t, source, command.Aggregate.InputRange, "status")
	assertSourceRangeText(t, source, command.Aggregate.AliasRange, "populated")
	assertSourceRangeText(t, source, command.GroupBy[0].Range, "host")
}

func TestParseEventStatsRejectsUnsupportedSurfaceAtSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		source    string
		code      string
		rangeText string
	}{
		{
			name:      "empty call",
			source:    `index=main | eventstats count()`,
			code:      "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
			rangeText: "(",
		},
		{
			name:      "other function",
			source:    "index=main\n| eventstats dc(user)",
			code:      "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
			rangeText: "dc",
		},
		{
			name:      "comma separated second measure",
			source:    `index=main | eventstats count, count`,
			code:      "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
			rangeText: ",",
		},
		{
			name:      "space separated second measure",
			source:    `index=main | eventstats count sum(bytes)`,
			code:      "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
			rangeText: "sum",
		},
		{
			name:      "command option",
			source:    `index=main | eventstats allnum=true count`,
			code:      "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
			rangeText: "allnum",
		},
		{
			name:      "wildcard alias",
			source:    `index=main | eventstats count AS total*`,
			code:      "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
			rangeText: "total*",
		},
		{
			name:      "quoted alias",
			source:    `index=main | eventstats count AS "total"`,
			code:      "SPL_EXPECTED_FIELD",
			rangeText: `"total"`,
		},
		{
			name:      "wildcard group field",
			source:    `index=main | eventstats count BY host*`,
			code:      "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
			rangeText: "host*",
		},
		{
			name:      "alias after BY",
			source:    `index=main | eventstats count BY host AS total`,
			code:      "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
			rangeText: "AS",
		},
		{
			name:      "trailing group comma",
			source:    `index=main | eventstats count BY host,`,
			code:      "SPL_EXPECTED_FIELD",
			rangeText: "",
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

func TestParseEventStatsRejectsUnsupportedCountFieldForms(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		source    string
		code      string
		rangeText string
	}{
		{
			name:      "missing required alias",
			source:    `index=main | eventstats count(status)`,
			code:      "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
			rangeText: "count(status)",
		},
		{
			name:      "missing required alias before BY",
			source:    `index=main | eventstats count(status) BY host`,
			code:      "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
			rangeText: "count(status)",
		},
		{
			name:      "short alias",
			source:    `index=main | eventstats c(status) AS populated`,
			code:      "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
			rangeText: "c",
		},
		{
			name:      "short alias empty call",
			source:    `index=main | eventstats c() AS populated`,
			code:      "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
			rangeText: "c",
		},
		{
			name:      "wildcard",
			source:    `index=main | eventstats count(*) AS populated`,
			code:      "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
			rangeText: "*",
		},
		{
			name:      "prefix wildcard",
			source:    `index=main | eventstats count(status*) AS populated`,
			code:      "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
			rangeText: "status*",
		},
		{
			name:      "quoted field",
			source:    `index=main | eventstats count("status") AS populated`,
			code:      "SPL_EXPECTED_FIELD",
			rangeText: `"status"`,
		},
		{
			name:      "multiple fields",
			source:    `index=main | eventstats count(status,host) AS populated`,
			code:      "SPL_EXPECTED_RIGHT_PAREN",
			rangeText: ",",
		},
		{
			name:      "missing right parenthesis",
			source:    `index=main | eventstats count(status AS populated`,
			code:      "SPL_EXPECTED_RIGHT_PAREN",
			rangeText: "AS",
		},
		{
			name:      "second measure after field count",
			source:    `index=main | eventstats count(status) AS populated count`,
			code:      "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
			rangeText: "count",
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

func TestParseAggregateGroupFieldsKeepCommandSpecificDiagnostics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		source      string
		code        string
		message     string
		suggestions []string
	}{
		{
			name:    "stats leading comma",
			source:  `index=main | stats count BY , host`,
			code:    "SPL_EXPECTED_FIELD",
			message: "expected a stats grouping field",
		},
		{
			name:    "eventstats leading comma",
			source:  `index=main | eventstats count BY , host`,
			code:    "SPL_EXPECTED_FIELD",
			message: "expected an eventstats grouping field",
		},
		{
			name:        "stats alias after BY",
			source:      `index=main | stats count BY host AS total`,
			code:        "SPL_UNSUPPORTED_STATS_SYNTAX",
			message:     "a stats aggregate alias must appear before the BY clause",
			suggestions: []string{"stats count AS total BY field"},
		},
		{
			name:    "eventstats alias after BY",
			source:  `index=main | eventstats count BY host AS total`,
			code:    "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
			message: "an eventstats aggregate alias must appear before the BY clause",
			suggestions: []string{
				"eventstats count",
				"eventstats count AS event_count BY group",
				"eventstats count(field) AS occurrences BY group",
				"eventstats count(eval(field=value)) AS matches BY group",
			},
		},
		{
			name:    "eventstats wildcard field",
			source:  `index=main | eventstats count BY host*`,
			code:    "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
			message: "wildcard eventstats grouping fields are not supported",
			suggestions: []string{
				"eventstats count",
				"eventstats count AS event_count BY group",
				"eventstats count(field) AS occurrences BY group",
				"eventstats count(eval(field=value)) AS matches BY group",
			},
		},
		{
			name:    "stats trailing comma",
			source:  `index=main | stats count BY host,`,
			code:    "SPL_EXPECTED_FIELD",
			message: "stats BY requires at least one field",
		},
		{
			name:    "eventstats trailing comma",
			source:  `index=main | eventstats count BY host,`,
			code:    "SPL_EXPECTED_FIELD",
			message: "eventstats BY requires at least one field",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := Parse(test.source)
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) {
				t.Fatalf("error = %T, want *Diagnostic", err)
			}
			if diagnostic.Code != test.code {
				t.Fatalf("code = %q, want %q", diagnostic.Code, test.code)
			}
			if diagnostic.Message != test.message {
				t.Fatalf("message = %q, want %q", diagnostic.Message, test.message)
			}
			if strings.Join(diagnostic.Suggestions, "\x00") != strings.Join(test.suggestions, "\x00") {
				t.Fatalf("suggestions = %q, want %q", diagnostic.Suggestions, test.suggestions)
			}
		})
	}
}

func TestParseStatsGroupFieldsStillAcceptWildcards(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | stats count BY host*`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*StatsCommand)
	if len(command.GroupBy) != 1 || command.GroupBy[0].Name != "host*" {
		t.Fatalf("group fields = %#v, want host*", command.GroupBy)
	}
}

func TestParseEventStatsRequiresCountAliasAndGroupField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		code   string
	}{
		{name: "missing aggregate", source: `index=main | eventstats`, code: "SPL_EXPECTED_AGGREGATE"},
		{name: "missing alias", source: `index=main | eventstats count AS`, code: "SPL_EXPECTED_FIELD"},
		{name: "missing group", source: `index=main | eventstats count BY`, code: "SPL_EXPECTED_FIELD"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := Parse(test.source)
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != test.code {
				t.Fatalf("diagnostic = %#v, want %s", err, test.code)
			}
		})
	}
}

func TestParseEventStatsBoundsGroupingFields(t *testing.T) {
	t.Parallel()

	var source strings.Builder
	source.WriteString("index=main | eventstats count BY ")
	for index := 0; index <= MaximumStatsGroupFields; index++ {
		if index > 0 {
			source.WriteString(", ")
		}
		fmt.Fprintf(&source, "field%d", index)
	}

	_, err := Parse(source.String())
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
		t.Fatalf("diagnostic = %#v, want SPL_QUERY_TOO_COMPLEX", err)
	}
	assertSourceRangeText(t, source.String(), diagnostic.Range, fmt.Sprintf("field%d", MaximumStatsGroupFields))
}

func TestAnalyzeEventStatsSuggestionContextStaysWithinBoundedGrammar(t *testing.T) {
	t.Parallel()

	aggregateSource := "| eventstats c"
	aggregate, diagnostic := AnalyzeSuggestionContext(aggregateSource, len(aggregateSource))
	if diagnostic != nil {
		t.Fatalf("AnalyzeSuggestionContext(%q): %v", aggregateSource, diagnostic)
	}
	if aggregate.FunctionClass != SuggestionFunctionClassAggregate {
		t.Fatalf("function class = %q, want aggregate", aggregate.FunctionClass)
	}
	candidates := StaticSuggestionCandidates(aggregate)
	if len(candidates) != 1 || candidates[0].Label != "count" {
		t.Fatalf("aggregate suggestions = %v, want only count", candidates)
	}

	tests := []struct {
		source string
		kind   SuggestionKind
		words  []string
	}{
		{source: "| eventstats count A", kind: SuggestionKindKeyword, words: []string{"AS", "BY"}},
		{source: "| eventstats count(", kind: SuggestionKindField},
		{source: "| eventstats count(status)", kind: SuggestionKindKeyword, words: []string{"AS"}},
		{source: "| eventstats count(eval(status=500))", kind: SuggestionKindKeyword, words: []string{"AS"}},
		{source: "| eventstats count AS event_", kind: SuggestionKindField},
		{source: "| eventstats count AS events B", kind: SuggestionKindKeyword, words: []string{"BY"}},
		{source: "| eventstats count(status) AS populated B", kind: SuggestionKindKeyword, words: []string{"BY"}},
		{source: "| eventstats count BY ho", kind: SuggestionKindField},
	}
	for _, test := range tests {
		context, contextDiagnostic := AnalyzeSuggestionContext(test.source, len(test.source))
		if contextDiagnostic != nil {
			t.Fatalf("AnalyzeSuggestionContext(%q): %v", test.source, contextDiagnostic)
		}
		if len(context.Kinds) != 1 || context.Kinds[0] != test.kind {
			t.Fatalf("AnalyzeSuggestionContext(%q) kinds = %v, want [%s]", test.source, context.Kinds, test.kind)
		}
		if strings.Join(context.Keywords, ",") != strings.Join(test.words, ",") {
			t.Fatalf("AnalyzeSuggestionContext(%q) keywords = %v, want %v", test.source, context.Keywords, test.words)
		}
	}
}

func assertSourceRangeText(t *testing.T, source string, sourceRange Range, want string) {
	t.Helper()
	if sourceRange.Start.Offset < 0 ||
		sourceRange.End.Offset < sourceRange.Start.Offset ||
		sourceRange.End.Offset > len(source) {
		t.Fatalf("invalid source range %#v for %d-byte source", sourceRange, len(source))
	}
	if got := source[sourceRange.Start.Offset:sourceRange.End.Offset]; got != want {
		t.Fatalf("range %#v text = %q, want %q", sourceRange, got, want)
	}
}
