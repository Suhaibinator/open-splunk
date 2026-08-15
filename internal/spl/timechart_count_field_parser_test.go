package spl

import (
	"errors"
	"slices"
	"testing"
)

func TestParseTimechartCountFieldCanonicalizesAndPreservesExactFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		source            string
		input             string
		output            string
		explicitAlias     bool
		split             string
		wantAggregateText string
		wantAliasText     string
	}{
		{
			name:              "canonical default output",
			source:            `index=main | timechart span=30s COUNT(Http.Status)`,
			input:             "Http.Status",
			output:            "count(Http.Status)",
			wantAggregateText: "COUNT(Http.Status)",
			wantAliasText:     "COUNT(Http.Status)",
		},
		{
			name:              "case insensitive keywords with exact alias and split",
			source:            `index=main | TiMeChArT SpAn=2H CoUnT(http.duration) aS populated bY Service`,
			input:             "http.duration",
			output:            "populated",
			explicitAlias:     true,
			split:             "Service",
			wantAggregateText: "CoUnT(http.duration) aS populated",
			wantAliasText:     "populated",
		},
	}
	for _, test := range tests {
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
			if aggregate.Function != AggregateFunctionCountValues ||
				aggregate.Input != test.input || aggregate.Predicate != nil ||
				aggregate.Percentile != 0 || aggregate.Alias != test.output ||
				aggregate.ExplicitAlias != test.explicitAlias {
				t.Fatalf("aggregate = %#v", aggregate)
			}
			assertSourceRangeText(t, test.source, aggregate.Range, test.wantAggregateText)
			assertSourceRangeText(t, test.source, aggregate.InputRange, test.input)
			assertSourceRangeText(t, test.source, aggregate.AliasRange, test.wantAliasText)
			if test.split == "" {
				if command.SplitBy != nil {
					t.Fatalf("split = %#v, want nil", command.SplitBy)
				}
			} else {
				if command.SplitBy == nil || command.SplitBy.Name != test.split {
					t.Fatalf("split = %#v, want %q", command.SplitBy, test.split)
				}
				assertSourceRangeText(t, test.source, command.SplitBy.Range, test.split)
			}
		})
	}
}

func TestParseTimechartCountFieldRejectsShorthandAndNonExactShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		code   string
	}{
		{"empty call", `index=main | timechart span=5m count()`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX"},
		{"bare shorthand", `index=main | timechart span=5m c`, "SPL_UNSUPPORTED_TIMECHART_AGGREGATE"},
		{"shorthand call", `index=main | timechart span=5m c(status)`, "SPL_UNSUPPORTED_TIMECHART_AGGREGATE"},
		{"eval input", `index=main | timechart span=5m count(eval(status="ok"))`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX"},
		{"wildcard input", `index=main | timechart span=5m count(stat*)`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX"},
		{"quoted input", `index=main | timechart span=5m count("status")`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX"},
		{"multiple inputs", `index=main | timechart span=5m count(status,host)`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX"},
		{"missing closing parenthesis", `index=main | timechart span=5m count(status`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX"},
		{"missing alias", `index=main | timechart span=5m count(status) AS`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX"},
		{"quoted alias", `index=main | timechart span=5m count(status) AS "seen"`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX"},
		{"wildcard alias", `index=main | timechart span=5m count(status) AS seen*`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX"},
		{"reserved alias", `index=main | timechart span=5m count(status) AS BY service`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX"},
		{"second alias", `index=main | timechart span=5m count(status) AS seen AS other`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX"},
		{"missing split", `index=main | timechart span=5m count(status) BY`, "SPL_EXPECTED_FIELD"},
		{"quoted split", `index=main | timechart span=5m count(status) BY "service"`, "SPL_EXPECTED_FIELD"},
		{"wildcard split", `index=main | timechart span=5m count(status) BY service*`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX"},
		{"second split", `index=main | timechart span=5m count(status) BY service host`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX"},
	}
	for _, test := range tests {
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
		})
	}
}

func TestTimechartCountFieldSuggestionContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		source   string
		prefix   string
		kinds    []SuggestionKind
		keywords []string
	}{
		{source: `| timechart span=5m count(`, kinds: []SuggestionKind{SuggestionKindField}},
		{source: `| timechart span=5m count(Http.`, prefix: "Http.", kinds: []SuggestionKind{SuggestionKindField}},
		{
			source:   `| timechart span=5m count(status) `,
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"AS", "BY"},
		},
		{source: `| timechart span=5m count(status) AS p`, prefix: "p", kinds: []SuggestionKind{SuggestionKindField}},
		{
			source:   `| timechart span=5m count(status) AS populated `,
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"BY"},
		},
		{source: `| timechart span=5m count(status) BY `, kinds: []SuggestionKind{SuggestionKindField}},
	}
	for _, test := range tests {
		t.Run(test.source, func(t *testing.T) {
			t.Parallel()

			context, diagnostic := AnalyzeSuggestionContext(test.source, len(test.source))
			if diagnostic != nil {
				t.Fatalf("AnalyzeSuggestionContext: %v", diagnostic)
			}
			if context.Prefix != test.prefix ||
				!slices.Equal(context.Kinds, test.kinds) ||
				!slices.Equal(context.Keywords, test.keywords) {
				t.Fatalf("context = %#v, want prefix=%q kinds=%v keywords=%v",
					context, test.prefix, test.kinds, test.keywords)
			}
		})
	}
}

func TestSuggestTimechartCountFieldUsesCaseSensitiveFieldPrefix(t *testing.T) {
	t.Parallel()

	source := `| timechart span=5m count(Http.`
	context, diagnostic := AnalyzeSuggestionContext(source, len(source))
	if diagnostic != nil {
		t.Fatalf("AnalyzeSuggestionContext: %v", diagnostic)
	}
	candidates := []SuggestionCandidate{
		{Kind: SuggestionKindField, Label: "Http.Status", Insertion: "Http.Status"},
		{Kind: SuggestionKindField, Label: "http.status", Insertion: "http.status"},
	}
	if labels := suggestionLabels(RankSuggestionCandidates(context, candidates, 20)); !slices.Equal(labels, []string{"Http.Status"}) {
		t.Fatalf("field suggestions = %v, want exact-case Http.Status", labels)
	}
}

func TestClassifyTimechartCountFieldResultShape(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		want   ResultShape
	}{
		{
			source: `index=main | timechart span=5m count(status)`,
			want:   ResultShape{Kind: ResultKindTimeSeries},
		},
		{
			source: `index=main | timechart span=5m count(status) AS populated BY service`,
			want:   ResultShape{Kind: ResultKindTimeSeries, RuntimeNamedColumns: true},
		},
	} {
		query, err := Parse(test.source)
		if err != nil {
			t.Fatalf("Parse(%q): %v", test.source, err)
		}
		if got := ClassifyResultShape(query); got != test.want {
			t.Fatalf("ClassifyResultShape(%q) = %#v, want %#v", test.source, got, test.want)
		}
	}
}
