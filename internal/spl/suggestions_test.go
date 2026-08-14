package spl

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestAnalyzeSuggestionContextRejectsInvalidSourceAndCursor(t *testing.T) {
	t.Parallel()

	source := "München | st"
	tests := []struct {
		name   string
		source string
		cursor int
		code   string
	}{
		{name: "negative cursor", source: source, cursor: -1, code: "SPL_INVALID_CURSOR"},
		{name: "cursor past source", source: source, cursor: len(source) + 1, code: "SPL_INVALID_CURSOR"},
		{name: "cursor in UTF-8 rune", source: source, cursor: 2, code: "SPL_INVALID_CURSOR"},
		{name: "invalid UTF-8", source: string([]byte{'|', ' ', 0xff}), cursor: 2, code: "SPL_INVALID_UTF8"},
		{name: "NUL", source: "| st\x00", cursor: 4, code: "SPL_INVALID_SOURCE"},
		{
			name:   "source byte bound",
			source: strings.Repeat("a", MaximumSuggestionSourceBytes+1),
			cursor: MaximumSuggestionSourceBytes,
			code:   "SPL_QUERY_TOO_COMPLEX",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			context, diagnostic := AnalyzeSuggestionContext(test.source, test.cursor)
			if diagnostic == nil || diagnostic.Code != test.code {
				t.Fatalf("diagnostic = %#v, want %s", diagnostic, test.code)
			}
			if len(context.Kinds) != 0 {
				t.Fatalf("context kinds = %v, want none", context.Kinds)
			}
		})
	}

	context, diagnostic := AnalyzeSuggestionContext(source, len(source))
	if diagnostic != nil {
		t.Fatalf("valid UTF-8 boundary: %v", diagnostic)
	}
	if context.Prefix != "st" || !context.Allows(SuggestionKindCommand) {
		t.Fatalf("valid boundary context = %#v", context)
	}
	if !utf8.ValidString(source) {
		t.Fatal("test source is not valid UTF-8")
	}
}

func TestAnalyzeSuggestionContextReplacesWholeFragment(t *testing.T) {
	t.Parallel()

	source := "index=main | staZZ"
	context, diagnostic := AnalyzeSuggestionContext(source, 16)
	if diagnostic != nil {
		t.Fatalf("AnalyzeSuggestionContext: %v", diagnostic)
	}
	if context.Prefix != "sta" {
		t.Fatalf("prefix = %q, want sta", context.Prefix)
	}
	if got, want := context.Replacement, (Range{
		Start: Position{Offset: 13, Line: 1, Column: 14},
		End:   Position{Offset: 18, Line: 1, Column: 19},
	}); got != want {
		t.Fatalf("replacement = %#v, want %#v", got, want)
	}
	result := Suggest(source, 16, 0)
	if result.Diagnostic != nil {
		t.Fatalf("Suggest: %v", result.Diagnostic)
	}
	if len(result.Suggestions) == 0 || result.Suggestions[0].Label != "stats" {
		t.Fatalf("suggestions = %#v, want stats first", result.Suggestions)
	}
	if result.Suggestions[0].Replacement != context.Replacement {
		t.Fatalf("suggestion replacement = %#v, want %#v", result.Suggestions[0].Replacement, context.Replacement)
	}

	sortSource := "* | sort -_tiZZ"
	sortContext, diagnostic := AnalyzeSuggestionContext(sortSource, 13)
	if diagnostic != nil {
		t.Fatalf("AnalyzeSuggestionContext(sort): %v", diagnostic)
	}
	if sortContext.Prefix != "_ti" ||
		sortContext.Replacement.Start.Offset != 10 ||
		sortContext.Replacement.End.Offset != len(sortSource) {
		t.Fatalf("sort context = %#v, want sign-preserving _tiZZ replacement", sortContext)
	}
}

func TestAnalyzeSuggestionContextUsesLexerPipeQuoteAndEscapeRules(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval x="a|b" | st`,
		`index=main | eval x="a\"|b" | st`,
		`index=main | eval x=outside\| st`,
	} {
		context, diagnostic := AnalyzeSuggestionContext(source, len(source))
		if diagnostic != nil {
			t.Fatalf("AnalyzeSuggestionContext(%q): %v", source, diagnostic)
		}
		if context.Prefix != "st" || !context.Allows(SuggestionKindCommand) {
			t.Fatalf("context for %q = %#v, want command st", source, context)
		}
		if context.PipelinePrefixEnd != strings.LastIndex(source, "|") {
			t.Fatalf("pipeline prefix end for %q = %d, want final real pipe at %d",
				source, context.PipelinePrefixEnd, strings.LastIndex(source, "|"))
		}
	}

	closed := `* | eval x="a|b"`
	context, diagnostic := AnalyzeSuggestionContext(closed, strings.Index(closed, "|b"))
	if diagnostic != nil {
		t.Fatalf("closed quoted value: %v", diagnostic)
	}
	if len(context.Kinds) != 0 {
		t.Fatalf("closed quoted value context = %#v, want no suggestions", context)
	}

	for _, source := range []string{
		`* | eval x="abc`,
		`* | eval x="abc\`,
	} {
		context, diagnostic := AnalyzeSuggestionContext(source, len(source))
		if diagnostic == nil || diagnostic.Code != "SPL_UNTERMINATED_STRING" {
			t.Fatalf("AnalyzeSuggestionContext(%q) diagnostic = %#v", source, diagnostic)
		}
		if len(context.Kinds) != 0 {
			t.Fatalf("unterminated string context = %#v, want none", context)
		}
	}
}

func TestAnalyzeSuggestionContextKeepsBaseSearchOutOfScalarNormalization(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		"eval=status+",
		"where=status/",
		"stats=count+",
	} {
		context, diagnostic := AnalyzeSuggestionContext(source, len(source))
		if diagnostic != nil {
			t.Fatalf("AnalyzeSuggestionContext(%q): %v", source, diagnostic)
		}
		if context.Prefix != source[strings.IndexByte(source, '=')+1:] ||
			context.Replacement.Start.Offset != strings.IndexByte(source, '=')+1 ||
			context.Replacement.End.Offset != len(source) ||
			context.AllowsQuotedScalarFields {
			t.Fatalf("base-search context for %q = %#v", source, context)
		}
	}
}

func TestAnalyzeSuggestionContextExposesParseablePipelinePrefix(t *testing.T) {
	t.Parallel()

	source := `index=main | eval x="a|b" | stats count BY ho`
	context, diagnostic := AnalyzeSuggestionContext(source, len(source))
	if diagnostic != nil {
		t.Fatalf("AnalyzeSuggestionContext: %v", diagnostic)
	}
	wantEnd := strings.LastIndex(source, "|")
	if context.PipelinePrefixEnd != wantEnd {
		t.Fatalf("pipeline prefix end = %d, want %d", context.PipelinePrefixEnd, wantEnd)
	}
	if _, err := Parse(source[:context.PipelinePrefixEnd]); err != nil {
		t.Fatalf("preceding pipeline is not parseable: %v", err)
	}

	base, diagnostic := AnalyzeSuggestionContext("index=ma", len("index=ma"))
	if diagnostic != nil {
		t.Fatalf("AnalyzeSuggestionContext(base): %v", diagnostic)
	}
	if base.PipelinePrefixEnd != 0 {
		t.Fatalf("base pipeline prefix end = %d, want 0", base.PipelinePrefixEnd)
	}
}

func TestAnalyzeSuggestionContextIncompleteGrammar(t *testing.T) {
	t.Parallel()

	tests := []struct {
		source        string
		prefix        string
		kinds         []SuggestionKind
		functionClass SuggestionFunctionClass
		keywords      []string
	}{
		{source: "| st", prefix: "st", kinds: []SuggestionKind{SuggestionKindCommand}},
		{
			source:        "| eval x=to",
			prefix:        "to",
			kinds:         []SuggestionKind{SuggestionKindFunction, SuggestionKindField},
			functionClass: SuggestionFunctionClassScalar,
		},
		{
			source:        "| where is",
			prefix:        "is",
			kinds:         []SuggestionKind{SuggestionKindFunction, SuggestionKindField, SuggestionKindKeyword},
			functionClass: SuggestionFunctionClassScalar,
			keywords:      []string{"NOT"},
		},
		{
			source:        "| eval ratio=duration_ms+to",
			prefix:        "to",
			kinds:         []SuggestionKind{SuggestionKindFunction, SuggestionKindField},
			functionClass: SuggestionFunctionClassScalar,
		},
		{
			source:        "| where duration_ms/to",
			prefix:        "to",
			kinds:         []SuggestionKind{SuggestionKindFunction, SuggestionKindField},
			functionClass: SuggestionFunctionClassScalar,
		},
		{
			source:        "| where status==to",
			prefix:        "to",
			kinds:         []SuggestionKind{SuggestionKindFunction, SuggestionKindField},
			functionClass: SuggestionFunctionClassScalar,
		},
		{
			source:   "| where status I",
			prefix:   "I",
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"AND", "OR", "IN", "NOT"},
		},
		{
			source:   "| where status NOT I",
			prefix:   "I",
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"IN"},
		},
		{
			source:        "| stats count(eval(duration_ms+to",
			prefix:        "to",
			kinds:         []SuggestionKind{SuggestionKindFunction, SuggestionKindField},
			functionClass: SuggestionFunctionClassScalar,
		},
		{
			source:   "| eventstats count(eval(status I",
			prefix:   "I",
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"AND", "OR", "IN", "NOT"},
		},
		{
			source:   "| streamstats count(eval(status NOT I",
			prefix:   "I",
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"IN"},
		},
		{
			source:        "| stats d",
			prefix:        "d",
			kinds:         []SuggestionKind{SuggestionKindFunction, SuggestionKindKeyword},
			keywords:      []string{"partitions=", "allnum=", "delim="},
			functionClass: SuggestionFunctionClassAggregate,
		},
		{source: "| stats dc(tr", prefix: "tr", kinds: []SuggestionKind{SuggestionKindField}},
		{
			source:   "| rename host A",
			prefix:   "A",
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"AS"},
		},
		{
			source:        "| stats count B",
			prefix:        "B",
			kinds:         []SuggestionKind{SuggestionKindFunction, SuggestionKindKeyword},
			keywords:      []string{"AS", "BY", "dedup_splitvals="},
			functionClass: SuggestionFunctionClassAggregate,
		},
		{
			source:   "| chart count O",
			prefix:   "O",
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"OVER", "BY"},
		},
		{
			source:   "| chart count B",
			prefix:   "B",
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"OVER", "BY"},
		},
		{
			source:   "| chart count ",
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"OVER", "BY"},
		},
		{
			source: "| chart count OVER st",
			prefix: "st",
			kinds:  []SuggestionKind{SuggestionKindField},
		},
		{
			source:   "| chart count OVER status B",
			prefix:   "B",
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"BY"},
		},
		{
			source:        "| timechart span=5m co",
			prefix:        "co",
			kinds:         []SuggestionKind{SuggestionKindFunction},
			functionClass: SuggestionFunctionClassAggregate,
		},
		{
			source:   "| timechart span=5m count B",
			prefix:   "B",
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"BY"},
		},
		{
			source:   "| timechart span=5m count ",
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"BY"},
		},
		{source: "index=ma", prefix: "ma", kinds: []SuggestionKind{SuggestionKindIndex}},
		{source: "| table tr", prefix: "tr", kinds: []SuggestionKind{SuggestionKindField}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.source, func(t *testing.T) {
			t.Parallel()
			context, diagnostic := AnalyzeSuggestionContext(test.source, len(test.source))
			if diagnostic != nil {
				t.Fatalf("AnalyzeSuggestionContext: %v", diagnostic)
			}
			if context.Prefix != test.prefix ||
				!slices.Equal(context.Kinds, test.kinds) ||
				context.FunctionClass != test.functionClass ||
				!slices.Equal(context.Keywords, test.keywords) {
				t.Fatalf("context = %#v, want prefix=%q kinds=%v functionClass=%q keywords=%v",
					context, test.prefix, test.kinds, test.functionClass, test.keywords)
			}
		})
	}
}

func TestAnalyzeSuggestionContextV02OperatorsReplaceOnlyTheActiveFragment(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		"| eval ratio=duration_ms+toSuffix",
		"| eval ratio=duration_ms-toSuffix",
		"| eval ratio=duration_ms*toSuffix",
		"| eval ratio=duration_ms/toSuffix",
		"| eval ratio=duration_ms%toSuffix",
		"| where status==toSuffix",
		"| eval ratio=duration_ms+'HTTP Status'+toSuffix",
		"| stats count(eval(duration_ms+toSuffix",
		"| eventstats count(eval(duration_ms/toSuffix",
		"| streamstats count(eval(duration_ms%toSuffix",
	} {
		source := source
		t.Run(source, func(t *testing.T) {
			t.Parallel()
			fragmentStart := strings.LastIndex(source, "toSuffix")
			cursor := fragmentStart + len("to")
			context, diagnostic := AnalyzeSuggestionContext(source, cursor)
			if diagnostic != nil {
				t.Fatalf("AnalyzeSuggestionContext(): %v", diagnostic)
			}
			if context.Prefix != "to" ||
				!slices.Equal(context.Kinds, []SuggestionKind{
					SuggestionKindFunction,
					SuggestionKindField,
				}) ||
				context.FunctionClass != SuggestionFunctionClassScalar ||
				!context.AllowsQuotedScalarFields ||
				context.Replacement.Start.Offset != fragmentStart ||
				context.Replacement.End.Offset != fragmentStart+len("toSuffix") {
				t.Fatalf("context = %#v, want trailing toSuffix replacement", context)
			}
			result := Suggest(source, cursor, 20)
			if result.Diagnostic != nil {
				t.Fatalf("Suggest(): %v", result.Diagnostic)
			}
			if labels := suggestionLabels(result.Suggestions); !slices.Equal(
				labels,
				[]string{"tonumber", "tostring"},
			) {
				t.Fatalf("labels = %v, want tonumber/tostring", labels)
			}
		})
	}
}

func TestAnalyzeSuggestionContextReplacesFragmentInsideQuotedFieldComposite(t *testing.T) {
	t.Parallel()

	source := "| eval x='HTTP Status'+staZZ"
	fragmentStart := strings.Index(source, "staZZ")
	cursor := fragmentStart + len("sta")
	context, diagnostic := AnalyzeSuggestionContext(source, cursor)
	if diagnostic != nil {
		t.Fatalf("AnalyzeSuggestionContext(): %v", diagnostic)
	}
	if context.Prefix != "sta" ||
		context.Replacement.Start.Offset != fragmentStart ||
		context.Replacement.End.Offset != fragmentStart+len("staZZ") ||
		!context.AllowsQuotedScalarFields ||
		!slices.Equal(context.Kinds, []SuggestionKind{
			SuggestionKindFunction,
			SuggestionKindField,
		}) {
		t.Fatalf("context = %#v, want exact staZZ scalar fragment", context)
	}
}

func TestAnalyzeSuggestionContextDeliberatelyOffersNoValues(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`host=web`,
		`| eval x=123`,
		`| head 12`,
		`| transaction `,
	} {
		context, diagnostic := AnalyzeSuggestionContext(source, len(source))
		if diagnostic != nil {
			t.Fatalf("AnalyzeSuggestionContext(%q): %v", source, diagnostic)
		}
		if len(context.Kinds) != 0 {
			t.Fatalf("context for %q = %#v, want no value suggestions", source, context)
		}
	}

	for _, source := range []string{`| where !`, `| where )`} {
		context, diagnostic := AnalyzeSuggestionContext(source, len(source))
		if diagnostic == nil {
			t.Fatalf("AnalyzeSuggestionContext(%q) unexpectedly succeeded: %#v", source, context)
		}
		if len(context.Kinds) != 0 {
			t.Fatalf("malformed context = %#v, want none", context)
		}
	}
}

func TestAnalyzeSuggestionContextIgnoresMalformedSuffixAfterSafeCursor(t *testing.T) {
	t.Parallel()

	source := "| staZZ | where !"
	result := Suggest(source, 5, 20)
	if result.Diagnostic != nil {
		t.Fatalf("Suggest: %v", result.Diagnostic)
	}
	if labels := suggestionLabels(result.Suggestions); !slices.Equal(labels, []string{"stats"}) {
		t.Fatalf("labels = %v, want stats despite malformed suffix", labels)
	}
	if got, want := result.Context.Replacement.Start.Offset, 2; got != want {
		t.Fatalf("replacement start = %d, want %d", got, want)
	}
	if got, want := result.Context.Replacement.End.Offset, 7; got != want {
		t.Fatalf("replacement end = %d, want %d", got, want)
	}
}

func TestAnalyzeSuggestionContextBoundsTokensAndPipelineStages(t *testing.T) {
	t.Parallel()

	for name, source := range map[string]string{
		"tokens": strings.Repeat("a ", maxSPLTokens+1),
		"stages": strings.Repeat("| ", maxPipelineCommands+1),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			context, diagnostic := AnalyzeSuggestionContext(source, len(source))
			if diagnostic == nil || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
				t.Fatalf("diagnostic = %#v, want SPL_QUERY_TOO_COMPLEX", diagnostic)
			}
			if len(context.Kinds) != 0 {
				t.Fatalf("context = %#v, want none", context)
			}
		})
	}
}

func TestAnalyzeSuggestionContextRangesStayOnUTF8Boundaries(t *testing.T) {
	t.Parallel()

	sources := []string{
		"",
		"München | st",
		"index=main\n| eval label=first.\" \".last\n| stats dc(trace_id) BY host",
		`index=main | eval x="a\"|b" | sort -_time`,
		`| where isnotnull(trace_id) AND message!="閉じる"`,
		`| eval x="unterminated\`,
		`| where !`,
	}
	for _, source := range sources {
		source := source
		t.Run(source, func(t *testing.T) {
			t.Parallel()
			for cursor := 0; cursor <= len(source); cursor++ {
				context, diagnostic := AnalyzeSuggestionContext(source, cursor)
				isBoundary := cursor == len(source) || utf8.RuneStart(source[cursor])
				if !isBoundary {
					if diagnostic == nil || diagnostic.Code != "SPL_INVALID_CURSOR" {
						t.Fatalf("cursor %d diagnostic = %#v, want SPL_INVALID_CURSOR", cursor, diagnostic)
					}
					continue
				}
				if diagnostic != nil {
					continue
				}
				start := context.Replacement.Start.Offset
				end := context.Replacement.End.Offset
				if start < 0 || start > cursor || end < cursor || end > len(source) {
					t.Fatalf("cursor %d context range = [%d,%d), source bytes = %d", cursor, start, end, len(source))
				}
				if (start < len(source) && !utf8.RuneStart(source[start])) ||
					(end < len(source) && !utf8.RuneStart(source[end])) {
					t.Fatalf("cursor %d context range = [%d,%d), want UTF-8 boundaries", cursor, start, end)
				}
				if got := source[start:cursor]; got != context.Prefix {
					t.Fatalf("cursor %d prefix = %q, source range = %q", cursor, context.Prefix, got)
				}
				if context.PipelinePrefixEnd < 0 || context.PipelinePrefixEnd > len(source) ||
					(context.PipelinePrefixEnd < len(source) &&
						!utf8.RuneStart(source[context.PipelinePrefixEnd])) {
					t.Fatalf("cursor %d pipeline prefix end = %d", cursor, context.PipelinePrefixEnd)
				}
			}
		})
	}
}

func TestAnalyzeSuggestionContextTracksConsumedCommandOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		source   string
		kinds    []SuggestionKind
		keywords []string
	}{
		{
			source:   `| rex "(?<x>.)" f`,
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"max_match="},
		},
		{
			source: `| top 20 l`,
			kinds:  []SuggestionKind{SuggestionKindField},
		},
		{
			source: `| top 20 `,
			kinds:  []SuggestionKind{SuggestionKindField},
		},
		{
			source: `| top host, so`,
			kinds:  []SuggestionKind{SuggestionKindField},
		},
		{
			source: `| rare limit=5 `,
			kinds:  []SuggestionKind{SuggestionKindField},
		},
		{
			source: `| rare limit=5 host, `,
			kinds:  []SuggestionKind{SuggestionKindField},
		},
		{
			source:   `| bin foo span=5m sp`,
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"AS"},
		},
		{
			source:   `| spath path=x pa`,
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"input=", "output="},
		},
		{
			source:   `| rex field=message ma`,
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"max_match="},
		},
		{
			source:   `| bin foo sp`,
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"span="},
		},
		{
			source: `| bin span=5m fo`,
			kinds:  []SuggestionKind{SuggestionKindField},
		},
		{
			source:   `| spath path=x out`,
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"input=", "output="},
		},
		{
			source:   `| streamstats current=false count `,
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"AS", "BY", "window=", "global="},
		},
		{
			source: `| streamstats current=false count(`,
			kinds:  []SuggestionKind{SuggestionKindField},
		},
		{
			source: `| streamstats current=false sum(`,
			kinds:  []SuggestionKind{SuggestionKindField},
		},
		{
			source: `| streamstats current=false avg(`,
			kinds:  []SuggestionKind{SuggestionKindField},
		},
		{
			source: `| streamstats current=false min(`,
			kinds:  []SuggestionKind{SuggestionKindField},
		},
		{
			source:   `| streamstats sum `,
			kinds:    nil,
			keywords: nil,
		},
		{
			source:   `| streamstats avg `,
			kinds:    nil,
			keywords: nil,
		},
		{
			source:   `| streamstats min `,
			kinds:    nil,
			keywords: nil,
		},
		{
			source:   `| streamstats count(status) `,
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"AS", "BY", "current=", "window=", "global="},
		},
		{
			source:   `| streamstats count(status) AS populated `,
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"BY", "current=", "window=", "global="},
		},
		{
			source:   `| streamstats count AS sum `,
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"BY", "current=", "window=", "global="},
		},
		{
			source:   `| streamstats count AS avg `,
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"BY", "current=", "window=", "global="},
		},
		{
			source:   `| streamstats sum(bytes) `,
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"AS", "BY", "current=", "window=", "global="},
		},
		{
			source:   `| streamstats sum(bytes) AS running_bytes `,
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"BY", "current=", "window=", "global="},
		},
		{
			source:   `| streamstats avg(bytes) AS running_mean `,
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"BY", "current=", "window=", "global="},
		},
		{
			source:   `| streamstats min(bytes) AS running_minimum `,
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"BY", "current=", "window=", "global="},
		},
		{
			source:   `| streamstats count BY host `,
			kinds:    []SuggestionKind{SuggestionKindField, SuggestionKindKeyword},
			keywords: []string{"current=", "window=", "global="},
		},
		{
			source:   `| streamstats count BY sum `,
			kinds:    []SuggestionKind{SuggestionKindField, SuggestionKindKeyword},
			keywords: []string{"current=", "window=", "global="},
		},
		{
			source:   `| streamstats count BY avg `,
			kinds:    []SuggestionKind{SuggestionKindField, SuggestionKindKeyword},
			keywords: []string{"current=", "window=", "global="},
		},
		{
			source:   `| streamstats count BY host current=false `,
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"window=", "global="},
		},
		{
			source:   `| streamstats count BY host current=false window=2 global=false `,
			kinds:    nil,
			keywords: nil,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.source, func(t *testing.T) {
			t.Parallel()
			context, diagnostic := AnalyzeSuggestionContext(test.source, len(test.source))
			if diagnostic != nil {
				t.Fatalf("AnalyzeSuggestionContext: %v", diagnostic)
			}
			if !slices.Equal(context.Kinds, test.kinds) ||
				!slices.Equal(context.Keywords, test.keywords) {
				t.Fatalf(
					"context = %#v, want kinds=%v keywords=%v",
					context,
					test.kinds,
					test.keywords,
				)
			}
		})
	}
}

func TestAnalyzeStreamStatsSuggestionsAdvertiseSupportedAggregates(t *testing.T) {
	t.Parallel()

	source := `| streamstats `
	context, diagnostic := AnalyzeSuggestionContext(source, len(source))
	if diagnostic != nil {
		t.Fatalf("AnalyzeSuggestionContext: %v", diagnostic)
	}
	if !slices.Equal(
		context.FunctionNames,
		[]string{"count", "sum", "avg", "min", "max", "earliest", "latest"},
	) {
		t.Fatalf(
			"streamstats functions = %v, want [count sum avg min max earliest latest]",
			context.FunctionNames,
		)
	}
}

func TestAnalyzeStreamStatsSuggestionsPreserveFieldsNamedMinimum(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source   string
		kinds    []SuggestionKind
		keywords []string
	}{
		{
			source:   `| streamstats count AS min `,
			kinds:    []SuggestionKind{SuggestionKindKeyword},
			keywords: []string{"BY", "current=", "window=", "global="},
		},
		{
			source: `| streamstats count BY min `,
			kinds: []SuggestionKind{
				SuggestionKindField,
				SuggestionKindKeyword,
			},
			keywords: []string{"current=", "window=", "global="},
		},
	} {
		context, diagnostic := AnalyzeSuggestionContext(test.source, len(test.source))
		if diagnostic != nil {
			t.Fatalf("AnalyzeSuggestionContext(%q): %v", test.source, diagnostic)
		}
		if !slices.Equal(context.Kinds, test.kinds) ||
			!slices.Equal(context.Keywords, test.keywords) {
			t.Fatalf(
				"streamstats context for %q = %#v, want kinds=%v keywords=%v",
				test.source,
				context,
				test.kinds,
				test.keywords,
			)
		}
	}
}

func TestAnalyzeSuggestionContextBoundsFrequencyFieldLists(t *testing.T) {
	t.Parallel()

	fieldNames := make([]string, MaximumFrequencyFields)
	for index := range fieldNames {
		fieldNames[index] = fmt.Sprintf("field%d", index)
	}
	tests := []struct {
		name       string
		fieldNames []string
		wantField  bool
	}{
		{
			name:       "below ceiling",
			fieldNames: fieldNames[:MaximumFrequencyFields-1],
			wantField:  true,
		},
		{
			name:       "at ceiling",
			fieldNames: fieldNames,
		},
		{
			name:       "duplicate tuple",
			fieldNames: []string{"host", "host"},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			source := "| top " + strings.Join(test.fieldNames, ",") + ", "
			context, diagnostic := AnalyzeSuggestionContext(source, len(source))
			if diagnostic != nil {
				t.Fatalf("AnalyzeSuggestionContext: %v", diagnostic)
			}
			if got := context.Allows(SuggestionKindField); got != test.wantField {
				t.Fatalf("field suggestion = %t, want %t; context = %#v", got, test.wantField, context)
			}
		})
	}
}

func TestAnalyzeSuggestionContextRejectsInvalidFrequencyLimits(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
	}{
		{name: "negative positional", source: `| top -1, `},
		{name: "overflow positional", source: `| rare 18446744073709551616, `},
		{name: "overflow named", source: `| top limit=18446744073709551616 `},
		{name: "malformed named", source: `| rare limit=lots, `},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			context, diagnostic := AnalyzeSuggestionContext(test.source, len(test.source))
			if diagnostic != nil {
				t.Fatalf("AnalyzeSuggestionContext: %v", diagnostic)
			}
			if context.Allows(SuggestionKindField) || len(context.Kinds) != 0 {
				t.Fatalf("context = %#v, want invalid limit to suppress field continuation", context)
			}
		})
	}
}

func TestRankSuggestionCandidatesExcludesFrequencyGeneratedAndCommittedFields(t *testing.T) {
	t.Parallel()

	source := `| top host, `
	context, diagnostic := AnalyzeSuggestionContext(source, len(source))
	if diagnostic != nil {
		t.Fatalf("AnalyzeSuggestionContext: %v", diagnostic)
	}
	labels := []string{
		"host", "Host",
		"count", "Count",
		"percent", "Percent",
		"BY", "by", "By",
		"Bypass", "source",
	}
	candidates := make([]SuggestionCandidate, 0, len(labels))
	for _, label := range labels {
		candidates = append(candidates, SuggestionCandidate{
			Kind:      SuggestionKindField,
			Label:     label,
			Insertion: label,
		})
	}

	got := suggestionLabels(RankSuggestionCandidates(context, candidates, 20))
	want := []string{"Bypass", "Count", "Host", "Percent", "source"}
	if !slices.Equal(got, want) {
		t.Fatalf("labels = %v, want exact exclusions with only BY folded: %v", got, want)
	}
}

func TestRankSuggestionCandidatesExcludesLaterFrequencyTupleFields(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		source     string
		cursor     int
		candidates []string
		want       []string
	}{
		{
			name:       "active field replacement expects comma",
			source:     `| top ho,host | where !`,
			cursor:     len(`| top ho`),
			candidates: []string{"host", "hostname"},
			want:       []string{"hostname"},
		},
		{
			name:       "empty field slot accepts suffix field",
			source:     `| top host, source | where !`,
			cursor:     len(`| top host,`),
			candidates: []string{"source", "sourcetype"},
			want:       []string{"sourcetype"},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			context, diagnostic := AnalyzeSuggestionContext(test.source, test.cursor)
			if diagnostic != nil {
				t.Fatalf("AnalyzeSuggestionContext: %v", diagnostic)
			}
			candidates := make([]SuggestionCandidate, 0, len(test.candidates))
			for _, label := range test.candidates {
				candidates = append(candidates, SuggestionCandidate{
					Kind:      SuggestionKindField,
					Label:     label,
					Insertion: label,
				})
			}
			got := suggestionLabels(RankSuggestionCandidates(context, candidates, 20))
			if !slices.Equal(got, test.want) {
				t.Fatalf("labels = %v, want %v; context=%#v", got, test.want, context)
			}
		})
	}
}

func TestRankSuggestionCandidatesIsPrefixOnlyStableAndBounded(t *testing.T) {
	t.Parallel()

	context := SuggestionContext{
		Kinds:       []SuggestionKind{SuggestionKindFunction, SuggestionKindField},
		Prefix:      "tr",
		Replacement: Range{Start: Position{Offset: 7, Line: 1, Column: 8}, End: Position{Offset: 9, Line: 1, Column: 10}},
	}
	candidates := []SuggestionCandidate{
		{Kind: SuggestionKindField, Label: "trace_id", Insertion: "trace_id", Priority: 1},
		{Kind: SuggestionKindField, Label: "TraceUpper", Insertion: "TraceUpper", Priority: 100},
		{Kind: SuggestionKindField, Label: "other_trace", Insertion: "other_trace", Priority: 100},
		{Kind: SuggestionKindFunction, Label: "trim", Insertion: "trim(", Priority: 1},
		{Kind: SuggestionKindField, Label: "trace_id", Insertion: "trace_id", Priority: 999},
	}
	got := RankSuggestionCandidates(context, candidates, 100)
	if labels := suggestionLabels(got); !slices.Equal(labels, []string{"trim", "trace_id"}) {
		t.Fatalf("labels = %v, want prefix-only case-aware stable results", labels)
	}
	if got[0].Relevance != 0.75 || got[1].Relevance != 0.75 {
		t.Fatalf("relevance = %v, %v, want fixed prefix tier", got[0].Relevance, got[1].Relevance)
	}
	if got[0].Replacement != context.Replacement || got[1].Replacement != context.Replacement {
		t.Fatalf("replacement drifted: %#v", got)
	}

	exactContext := context
	exactContext.Prefix = "trim"
	exact := RankSuggestionCandidates(exactContext, candidates, 100)
	if len(exact) != 1 || exact[0].Label != "trim" || exact[0].Relevance != 1 {
		t.Fatalf("exact results = %#v", exact)
	}

	many := make([]SuggestionCandidate, 150)
	for index := range many {
		many[index] = SuggestionCandidate{
			Kind:      SuggestionKindField,
			Label:     fmt.Sprintf("field_%03d", index),
			Insertion: fmt.Sprintf("field_%03d", index),
		}
	}
	fieldContext := SuggestionContext{Kinds: []SuggestionKind{SuggestionKindField}}
	if got := RankSuggestionCandidates(fieldContext, many, 0); len(got) != DefaultSuggestionLimit {
		t.Fatalf("default result count = %d, want %d", len(got), DefaultSuggestionLimit)
	}
	if got := RankSuggestionCandidates(fieldContext, many, MaximumSuggestionLimit+50); len(got) != MaximumSuggestionLimit {
		t.Fatalf("hard-bounded result count = %d, want %d", len(got), MaximumSuggestionLimit)
	}
	first := suggestionLabels(RankSuggestionCandidates(fieldContext, many, 100))
	for iteration := 0; iteration < 100; iteration++ {
		if next := suggestionLabels(RankSuggestionCandidates(fieldContext, slices.Clone(many), 100)); !slices.Equal(next, first) {
			t.Fatalf("iteration %d order differs", iteration)
		}
	}
}

func TestRankSuggestionCandidatesFoldsIndexButNotFieldPrefixes(t *testing.T) {
	t.Parallel()

	candidates := []SuggestionCandidate{
		{Kind: SuggestionKindIndex, Label: "main", Insertion: "main"},
		{Kind: SuggestionKindField, Label: "MainField", Insertion: "MainField"},
	}
	indexContext := SuggestionContext{
		Kinds:  []SuggestionKind{SuggestionKindIndex},
		Prefix: "MA",
	}
	if labels := suggestionLabels(RankSuggestionCandidates(indexContext, candidates, 20)); !slices.Equal(labels, []string{"main"}) {
		t.Fatalf("index labels = %v, want case-folded main", labels)
	}
	fieldContext := SuggestionContext{
		Kinds:  []SuggestionKind{SuggestionKindField},
		Prefix: "ma",
	}
	if suggestions := RankSuggestionCandidates(fieldContext, candidates, 20); len(suggestions) != 0 {
		t.Fatalf("field suggestions = %#v, want case-sensitive rejection", suggestions)
	}
}

func TestStaticSuggestionsUseSharedCatalogAndContextFilters(t *testing.T) {
	t.Parallel()

	command := Suggest("| ST", len("| ST"), 20)
	if command.Diagnostic != nil {
		t.Fatalf("Suggest(command): %v", command.Diagnostic)
	}
	if labels := suggestionLabels(command.Suggestions); !slices.Equal(labels, []string{"strcat", "stats", "streamstats"}) {
		t.Fatalf("command labels = %v, want strcat/stats/streamstats", labels)
	}

	scalar := Suggest("| eval x=to", len("| eval x=to"), 20)
	if scalar.Diagnostic != nil {
		t.Fatalf("Suggest(scalar): %v", scalar.Diagnostic)
	}
	if labels := suggestionLabels(scalar.Suggestions); !slices.Equal(labels, []string{"tonumber", "tostring"}) {
		t.Fatalf("scalar labels = %v, want tonumber/tostring", labels)
	}

	arithmetic := Suggest("| eval ratio=duration_ms+to", len("| eval ratio=duration_ms+to"), 20)
	if arithmetic.Diagnostic != nil {
		t.Fatalf("Suggest(arithmetic): %v", arithmetic.Diagnostic)
	}
	if labels := suggestionLabels(arithmetic.Suggestions); !slices.Equal(labels, []string{"tonumber", "tostring"}) {
		t.Fatalf("arithmetic labels = %v, want tonumber/tostring", labels)
	}

	membership := Suggest("| where status I", len("| where status I"), 20)
	if membership.Diagnostic != nil {
		t.Fatalf("Suggest(membership): %v", membership.Diagnostic)
	}
	if labels := suggestionLabels(membership.Suggestions); !slices.Equal(labels, []string{"IN"}) {
		t.Fatalf("membership labels = %v, want IN", labels)
	}
	if got := membership.Suggestions[0].Insertion; got != "IN (" {
		t.Fatalf("membership insertion = %q, want IN (", got)
	}

	notMembership := Suggest("| where status NOT I", len("| where status NOT I"), 20)
	if notMembership.Diagnostic != nil {
		t.Fatalf("Suggest(not membership): %v", notMembership.Diagnostic)
	}
	if labels := suggestionLabels(notMembership.Suggestions); !slices.Equal(labels, []string{"IN"}) {
		t.Fatalf("not-membership labels = %v, want IN", labels)
	}

	for _, source := range []string{
		"| stats count(eval(status I",
		"| eventstats count(eval(status I",
		"| streamstats count(eval(status I",
	} {
		result := Suggest(source, len(source), 20)
		if result.Diagnostic != nil {
			t.Fatalf("Suggest(%q): %v", source, result.Diagnostic)
		}
		if labels := suggestionLabels(result.Suggestions); !slices.Equal(labels, []string{"IN"}) {
			t.Fatalf("nested membership labels for %q = %v, want IN", source, labels)
		}
		if !result.Context.AllowsQuotedScalarFields {
			t.Fatalf("nested membership context for %q did not allow quoted scalar fields", source)
		}
	}

	aggregate := Suggest("| stats d", len("| stats d"), 20)
	if aggregate.Diagnostic != nil {
		t.Fatalf("Suggest(aggregate): %v", aggregate.Diagnostic)
	}
	if labels := suggestionLabels(aggregate.Suggestions); !slices.Equal(labels, []string{"dc", "distinct_count", "delim="}) {
		t.Fatalf("aggregate labels = %v, want dc/distinct_count/delim", labels)
	}

	timechart := Suggest("| timechart span=5m co", len("| timechart span=5m co"), 20)
	if timechart.Diagnostic != nil {
		t.Fatalf("Suggest(timechart): %v", timechart.Diagnostic)
	}
	if labels := suggestionLabels(timechart.Suggestions); !slices.Equal(labels, []string{"count"}) {
		t.Fatalf("timechart labels = %v, want count", labels)
	}

	keyword := Suggest("| rename host A", len("| rename host A"), 20)
	if keyword.Diagnostic != nil {
		t.Fatalf("Suggest(keyword): %v", keyword.Diagnostic)
	}
	if labels := suggestionLabels(keyword.Suggestions); !slices.Equal(labels, []string{"AS"}) {
		t.Fatalf("keyword labels = %v, want AS", labels)
	}
}

func TestCompletionCatalogCoversSupportedFixedCommandsAndFunctions(t *testing.T) {
	t.Parallel()

	wantCommands := []string{
		"search", "where", "eval", "rename", "fields", "table", "sort",
		"dedup", "rex", "spath", "bin", "bucket", "head", "tail", "regex",
		"reverse", "accum", "strcat", "addinfo", "fillnull", "addtotals", "delta",
		"makemv", "mvexpand", "stats", "eventstats", "streamstats", "top", "rare",
		"timechart", "chart",
	}
	gotCommands := make([]string, 0, len(completionCatalog.Commands))
	for _, command := range completionCatalog.Commands {
		gotCommands = append(gotCommands, command.Name)
		if command.Insertion == "" || command.Detail == "" {
			t.Fatalf("command metadata incomplete: %#v", command)
		}
		if command.Name == "timechart" {
			if command.Insertion != "timechart span=5m count" {
				t.Fatalf("timechart insertion = %q, want static count form", command.Insertion)
			}
			if command.Detail != "Chart row counts, field occurrence counts, percentiles, sums, or averages over fixed time buckets; every supported aggregate may split BY one field." {
				t.Fatalf("timechart detail = %q, want aggregate/split description", command.Detail)
			}
		}
		if command.Name == "chart" {
			if command.Insertion != "chart count OVER status BY level" {
				t.Fatalf("chart insertion = %q, want bounded two-axis form", command.Insertion)
			}
			if command.Detail != "Build a bounded two-dimensional pivot of row counts, exact-field occurrence counts, percentiles, sums, or averages." {
				t.Fatalf("chart detail = %q, want supported aggregate description", command.Detail)
			}
		}
		if command.Name == "eventstats" {
			if command.Insertion !=
				"eventstats values(user) AS users BY service" {
				t.Fatalf(
					"eventstats insertion = %q, want values form",
					command.Insertion,
				)
			}
			if !strings.Contains(command.Detail, "true-only count(eval(predicate))") ||
				!strings.Contains(command.Detail, "exact distinct count") ||
				!strings.Contains(command.Detail, "canonical distinct-values list") ||
				!strings.Contains(command.Detail, "order-preserving list") ||
				!strings.Contains(command.Detail, "pN/percN percentile") ||
				!strings.Contains(command.Detail, "minimum") ||
				!strings.Contains(command.Detail, "maximum") ||
				!strings.Contains(command.Detail, "chronologically earliest") ||
				!strings.Contains(command.Detail, "chronologically latest") ||
				!strings.Contains(command.Detail, "numeric sum") ||
				!strings.Contains(command.Detail, "numeric average") {
				t.Fatalf(
					"eventstats detail = %q, want conditional-count, exact-distinct, values, order-preserving list, percentile, minimum, maximum, chronological, sum, and average description",
					command.Detail,
				)
			}
		}
	}
	if !slices.Equal(gotCommands, wantCommands) {
		t.Fatalf("catalog commands = %v, want %v", gotCommands, wantCommands)
	}

	wantFunctions := []string{
		"count", "sparkline", "p50", "p95", "exactperc50", "exactperc95", "upperperc50", "upperperc95",
		"c", "dc", "distinct_count", "estdc", "estdc_error", "values", "list", "min", "max",
		"median", "mode", "first", "last", "earliest", "latest", "earliest_time",
		"latest_time", "rate", "sum", "avg", "mean", "range", "sumsq", "stdev",
		"stdevp", "var", "varp", "if", "case", "in", "now", "strftime",
		"strptime", "relative_time", "tonumber", "tostring", "replace", "isnull",
		"isnotnull", "coalesce", "lower", "upper", "len", "length", "round",
		"ceil", "ceiling", "floor", "mvcount", "mvsort", "match", "like", "substr",
	}
	gotFunctions := make([]string, 0, len(completionCatalog.Functions))
	for _, function := range completionCatalog.Functions {
		gotFunctions = append(gotFunctions, function.Name)
		if function.Insertion == "" || function.Detail == "" {
			t.Fatalf("function metadata incomplete: %#v", function)
		}
	}
	if !slices.Equal(gotFunctions, wantFunctions) {
		t.Fatalf("catalog functions = %v, want %v", gotFunctions, wantFunctions)
	}
}

func TestStatsNumericAggregateAndEvalInputSuggestions(t *testing.T) {
	t.Parallel()

	aggregates := Suggest("| stats st", len("| stats st"), 20)
	if aggregates.Diagnostic != nil {
		t.Fatalf("Suggest aggregate: %v", aggregates.Diagnostic)
	}
	if labels := suggestionLabels(aggregates.Suggestions); !slices.Equal(
		labels,
		[]string{"stdev", "stdevp"},
	) {
		t.Fatalf("aggregate labels = %v, want stdev/stdevp", labels)
	}

	scalar := Suggest(
		"| stats sum(eval(duration_ms+to",
		len("| stats sum(eval(duration_ms+to"),
		20,
	)
	if scalar.Diagnostic != nil {
		t.Fatalf("Suggest scalar input: %v", scalar.Diagnostic)
	}
	if scalar.Context.FunctionClass != SuggestionFunctionClassScalar ||
		!scalar.Context.Allows(SuggestionKindFunction) ||
		!scalar.Context.Allows(SuggestionKindField) ||
		!scalar.Context.AllowsQuotedScalarFields {
		t.Fatalf("scalar input context = %#v", scalar.Context)
	}
	if labels := suggestionLabels(scalar.Suggestions); !slices.Equal(
		labels,
		[]string{"tonumber", "tostring"},
	) {
		t.Fatalf("scalar input labels = %v, want tonumber/tostring", labels)
	}

	exact := Suggest("| stats range(fi", len("| stats range(fi"), 20)
	if exact.Diagnostic != nil {
		t.Fatalf("Suggest exact input: %v", exact.Diagnostic)
	}
	if !exact.Context.Allows(SuggestionKindField) ||
		exact.Context.Allows(SuggestionKindFunction) ||
		!exact.Context.AllowsQuotedScalarFields {
		t.Fatalf("exact input context = %#v", exact.Context)
	}
}

func suggestionLabels(suggestions []Suggestion) []string {
	labels := make([]string, len(suggestions))
	for index, suggestion := range suggestions {
		labels[index] = suggestion.Label
	}
	return labels
}

func maximumScalarSuggestionFixture() (string, int) {
	var source strings.Builder
	source.WriteString("| eval result=")
	for range (maxSPLTokens - 5) / 2 {
		source.WriteString("value+")
	}
	source.WriteString("toSuffix")
	value := source.String()
	return value, strings.LastIndex(value, "toSuffix") + len("to")
}

func TestAnalyzeSuggestionContextV02MaximumScalarShapeBoundsAllocations(t *testing.T) {
	source, cursor := maximumScalarSuggestionFixture()
	var context SuggestionContext
	var diagnostic *Diagnostic
	allocations := testing.AllocsPerRun(50, func() {
		context, diagnostic = AnalyzeSuggestionContext(source, cursor)
	})
	if diagnostic != nil {
		t.Fatalf("AnalyzeSuggestionContext(): %v", diagnostic)
	}
	if context.Prefix != "to" ||
		context.Replacement.Start.Offset != strings.LastIndex(source, "toSuffix") {
		t.Fatalf("context = %#v, want maximum-shape trailing fragment", context)
	}
	// The append-only normalizer needs one bounded token backing array. Leave
	// headroom for runtime bookkeeping while catching fragment-by-fragment
	// slice reconstruction, which scales allocations with authored operators.
	if allocations > 32 {
		t.Fatalf("maximum-shape allocations = %.0f, want at most 32", allocations)
	}
}

func BenchmarkAnalyzeSuggestionContextV02MaximumScalarShape(b *testing.B) {
	source, cursor := maximumScalarSuggestionFixture()
	b.ReportAllocs()
	b.SetBytes(int64(len(source)))
	b.ResetTimer()
	for range b.N {
		context, diagnostic := AnalyzeSuggestionContext(source, cursor)
		if diagnostic != nil {
			b.Fatalf("AnalyzeSuggestionContext(): %v", diagnostic)
		}
		if context.Prefix != "to" ||
			context.Replacement.Start.Offset != strings.LastIndex(source, "toSuffix") {
			b.Fatalf("context = %#v, want maximum-shape trailing fragment", context)
		}
	}
}
