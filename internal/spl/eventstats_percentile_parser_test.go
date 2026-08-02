package spl

import (
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func TestParseEventStatsPercentileFamilyPreservesValidatedSuffixAndRanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		source     string
		percentile uint8
		input      string
		output     string
		groups     []string
		command    string
	}{
		{
			name:       "short minimum",
			source:     "index=main\n| EvEnTsTaTs P1(latency) AS low",
			percentile: 1,
			input:      "latency",
			output:     "low",
			command:    "EvEnTsTaTs P1(latency) AS low",
		},
		{
			name:       "long maximum grouped",
			source:     "index=main | eventstats PeRc99(duration_ms) AS p99_ms BY service, host",
			percentile: 99,
			input:      "duration_ms",
			output:     "p99_ms",
			groups:     []string{"service", "host"},
			command:    "eventstats PeRc99(duration_ms) AS p99_ms BY service, host",
		},
		{
			name:       "leading zero canonical value",
			source:     "index=main | eventstats p050(bytes) AS median BY index",
			percentile: 50,
			input:      "bytes",
			output:     "median",
			groups:     []string{"index"},
			command:    "eventstats p050(bytes) AS median BY index",
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
				t.Fatalf("commands = %#v, want one", query.Commands)
			}
			command, ok := query.Commands[0].(*EventStatsCommand)
			if !ok {
				t.Fatalf("command = %T, want *EventStatsCommand", query.Commands[0])
			}
			aggregate := command.Aggregate
			if aggregate.Function != AggregateFunctionPercentile ||
				aggregate.Percentile != test.percentile ||
				aggregate.Input != test.input ||
				aggregate.Alias != test.output ||
				!aggregate.ExplicitAlias ||
				aggregate.Predicate != nil {
				t.Fatalf("aggregate = %#v", aggregate)
			}
			assertSourceRangeText(t, test.source, aggregate.InputRange, test.input)
			assertSourceRangeText(t, test.source, aggregate.AliasRange, test.output)
			aggregateText := test.command[strings.IndexByte(test.command, ' ')+1:]
			if by := strings.Index(aggregateText, " BY "); by >= 0 {
				aggregateText = aggregateText[:by]
			}
			assertSourceRangeText(
				t,
				test.source,
				aggregate.Range,
				aggregateText,
			)
			assertSourceRangeText(t, test.source, command.Range, test.command)

			groups := make([]string, len(command.GroupBy))
			for index, group := range command.GroupBy {
				groups[index] = group.Name
				assertSourceRangeText(t, test.source, group.Range, group.Name)
			}
			if !slices.Equal(groups, test.groups) {
				t.Fatalf("groups = %v, want %v", groups, test.groups)
			}
		})
	}
}

func TestParseEventStatsPercentileRequiresExplicitAlias(t *testing.T) {
	t.Parallel()

	for _, suffix := range []string{"", " BY service"} {
		suffix := suffix
		t.Run(strconv.Quote(suffix), func(t *testing.T) {
			t.Parallel()

			_, err := Parse("index=main | eventstats p95(duration_ms)" + suffix)
			if err == nil {
				t.Fatal("Parse succeeded, want error")
			}
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) {
				t.Fatalf("Parse error = %T, want *Diagnostic", err)
			}
			if diagnostic.Code != "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX" ||
				diagnostic.Message !=
					"eventstats p95(field) requires AS followed by an output field name" {
				t.Fatalf("diagnostic = %#v", diagnostic)
			}
			if !slices.Contains(
				diagnostic.Suggestions,
				"eventstats p95(field) AS p95_value",
			) {
				t.Fatalf("suggestions = %v", diagnostic.Suggestions)
			}
			assertSourceRangeText(
				t,
				"index=main | eventstats p95(duration_ms)"+suffix,
				diagnostic.Range,
				"p95(duration_ms)",
			)
		})
	}
}

func TestParseEventStatsPercentileRejectsUnsupportedSurface(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		code   string
		at     string
	}{
		{"zero short", `index=main | eventstats p0(latency) AS value`, "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE", "p0"},
		{"hundred short", `index=main | eventstats p100(latency) AS value`, "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE", "p100"},
		{"zero long", `index=main | eventstats perc0(latency) AS value`, "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE", "perc0"},
		{"hundred long", `index=main | eventstats perc100(latency) AS value`, "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE", "perc100"},
		{"missing suffix short", `index=main | eventstats p(latency) AS value`, "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE", "p"},
		{"missing suffix long", `index=main | eventstats perc(latency) AS value`, "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE", "perc"},
		{"decimal suffix", `index=main | eventstats perc99.5(latency) AS value`, "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE", "perc99.5"},
		{"percentile spelling", `index=main | eventstats percentile95(latency) AS value`, "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE", "percentile95"},
		{"upper percentile", `index=main | eventstats upperperc95(latency) AS value`, "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE", "upperperc95"},
		{"exact percentile", `index=main | eventstats exactperc95(latency) AS value`, "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE", "exactperc95"},
		{"two argument percentile", `index=main | eventstats perc(latency,95) AS value`, "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE", "perc"},
		{"missing call", `index=main | eventstats p95 AS value`, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "p95"},
		{"empty input", `index=main | eventstats p95() AS value`, "SPL_EXPECTED_FIELD", ")"},
		{"quoted input", `index=main | eventstats p95("latency") AS value`, "SPL_EXPECTED_FIELD", `"latency"`},
		{"wildcard input", `index=main | eventstats p95(*) AS value`, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "*"},
		{"prefix wildcard input", `index=main | eventstats p95(lat*) AS value`, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "lat*"},
		{"eval input", `index=main | eventstats p95(eval(latency)) AS value`, "SPL_EXPECTED_RIGHT_PAREN", "("},
		{"multiple inputs", `index=main | eventstats p95(latency,bytes) AS value`, "SPL_EXPECTED_RIGHT_PAREN", ","},
		{"missing alias", `index=main | eventstats p95(latency) AS`, "SPL_EXPECTED_FIELD", ""},
		{"quoted alias", `index=main | eventstats p95(latency) AS "value"`, "SPL_EXPECTED_FIELD", `"value"`},
		{"wildcard alias", `index=main | eventstats p95(latency) AS value*`, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "value*"},
		{"second measure", `index=main | eventstats p95(latency) AS value count`, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "count"},
		{"comma measure", `index=main | eventstats p95(latency) AS value, count`, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", ","},
		{"option", `index=main | eventstats allnum=true p95(latency) AS value`, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "allnum"},
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
				t.Fatalf("Parse error = %T, want *Diagnostic", err)
			}
			if diagnostic.Code != test.code {
				t.Fatalf("diagnostic = %#v, want code %s", diagnostic, test.code)
			}
			if test.at != "" {
				assertSourceRangeText(t, test.source, diagnostic.Range, test.at)
			}
		})
	}
}

func TestParseEventStatsPercentileEnforcesGroupBound(t *testing.T) {
	t.Parallel()

	var source strings.Builder
	source.WriteString("index=main | eventstats p95(latency) AS p95_latency BY ")
	for index := 0; index < MaximumStatsGroupFields; index++ {
		if index > 0 {
			source.WriteByte(',')
		}
		source.WriteString("field")
		source.WriteString(strconv.Itoa(index))
	}
	query, err := Parse(source.String())
	if err != nil {
		t.Fatalf("Parse(exact group bound): %v", err)
	}
	command := query.Commands[0].(*EventStatsCommand)
	if len(command.GroupBy) != MaximumStatsGroupFields {
		t.Fatalf("group fields = %d, want %d", len(command.GroupBy), MaximumStatsGroupFields)
	}

	source.WriteString(",overflow")
	_, err = Parse(source.String())
	if err == nil {
		t.Fatal("Parse succeeded, want group bound error")
	}
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
		t.Fatalf("diagnostic = %#v", diagnostic)
	}
}

func TestEventStatsPercentileSuggestionsAreFieldThenAliasThenGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		source string
		kind   SuggestionKind
		words  []string
	}{
		{source: "| eventstats ", kind: SuggestionKindFunction, words: []string{"p50", "p95"}},
		{source: "| eventstats p95(", kind: SuggestionKindField},
		{source: "| eventstats perc95(", kind: SuggestionKindField},
		{source: "| eventstats p95(latency)", kind: SuggestionKindKeyword, words: []string{"AS"}},
		{source: "| eventstats perc95(latency)", kind: SuggestionKindKeyword, words: []string{"AS"}},
		{source: "| eventstats P95(latency)", kind: SuggestionKindKeyword, words: []string{"AS"}},
		{source: "| eventstats p95(latency) AS p95_", kind: SuggestionKindField},
		{source: "| eventstats p95(latency) AS p95_latency B", kind: SuggestionKindKeyword, words: []string{"BY"}},
	}

	for _, test := range tests {
		test := test
		t.Run(test.source, func(t *testing.T) {
			t.Parallel()

			context, diagnostic := AnalyzeSuggestionContext(
				test.source,
				len(test.source),
			)
			if diagnostic != nil {
				t.Fatalf("AnalyzeSuggestionContext: %v", diagnostic)
			}
			if !context.Allows(test.kind) {
				t.Fatalf("context = %#v, want kind %s", context, test.kind)
			}
			for _, word := range test.words {
				var values []string
				switch test.kind {
				case SuggestionKindFunction:
					values = context.FunctionNames
				case SuggestionKindKeyword:
					values = context.Keywords
				}
				if !slices.Contains(values, word) {
					t.Fatalf("context = %#v, want %q", context, word)
				}
			}
		})
	}
}

func TestClassifyResultShapePreservesEventsForEventStatsPercentile(t *testing.T) {
	t.Parallel()

	query, err := Parse(
		"index=main | eventstats p95(duration_ms) AS p95_ms BY service",
	)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := ClassifyResultShape(query); got != (ResultShape{Kind: ResultKindEvents}) {
		t.Fatalf("result shape = %#v, want events", got)
	}
}
