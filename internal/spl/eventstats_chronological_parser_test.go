package spl

import (
	"errors"
	"slices"
	"testing"
)

func TestParseEventStatsChronologicalAggregatesRequireBoundedExactSyntax(
	t *testing.T,
) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		source    string
		code      string
		rangeText string
	}{
		{"earliest missing input call", `index=main | eventstats earliest AS first`, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "earliest"},
		{"latest empty input", `index=main | eventstats latest() AS last`, "SPL_EXPECTED_FIELD", ")"},
		{"earliest multiple inputs", `index=main | eventstats earliest(left,right) AS first`, "SPL_EXPECTED_RIGHT_PAREN", ","},
		{"latest eval input", `index=main | eventstats latest(eval(status)) AS last`, "SPL_EXPECTED_RIGHT_PAREN", "("},
		{"earliest wildcard input", `index=main | eventstats earliest(*) AS first`, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "*"},
		{"latest prefix wildcard input", `index=main | eventstats latest(status*) AS last`, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "status*"},
		{"earliest quoted input", `index=main | eventstats earliest("status") AS first`, "SPL_EXPECTED_FIELD", `"status"`},
		{"latest wildcard output", `index=main | eventstats latest(status) AS last*`, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "last*"},
		{"earliest quoted output", `index=main | eventstats earliest(status) AS "first"`, "SPL_EXPECTED_FIELD", `"first"`},
		{"latest second measure", `index=main | eventstats latest(status) AS last count`, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "count"},
		{"earliest comma second measure", `index=main | eventstats earliest(status) AS first, count`, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", ","},
		{"latest option", `index=main | eventstats latest(status) AS last allnum=true`, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "allnum"},
	} {
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

func TestParseEventStatsChronologicalAggregatesRejectDuplicateGroups(t *testing.T) {
	t.Parallel()

	var diagnostic *Diagnostic
	duplicateSource := `index=main | eventstats latest(status) AS last_status BY host,host`
	_, err := Parse(duplicateSource)
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX" {
		t.Fatalf("duplicate diagnostic = %#v", err)
	}
	assertSourceRangeText(t, duplicateSource, diagnostic.Range, "host")
}

func TestEventStatsChronologicalSuggestionsFollowBoundedGrammar(t *testing.T) {
	t.Parallel()

	root, diagnostic := AnalyzeSuggestionContext("| eventstats ", len("| eventstats "))
	if diagnostic != nil {
		t.Fatalf("AnalyzeSuggestionContext(root): %v", diagnostic)
	}
	if !root.Allows(SuggestionKindFunction) ||
		!slices.Contains(root.FunctionNames, "earliest") ||
		!slices.Contains(root.FunctionNames, "latest") ||
		!slices.Contains(root.FunctionNames, "values") ||
		!slices.Contains(root.FunctionNames, "list") ||
		slices.Contains(root.FunctionNames, "distinct_count") {
		t.Fatalf("eventstats functions = %v", root.FunctionNames)
	}

	for _, test := range []struct {
		source   string
		kind     SuggestionKind
		keywords []string
	}{
		{"| eventstats earliest(", SuggestionKindField, nil},
		{"| eventstats latest(status)", SuggestionKindKeyword, []string{"AS"}},
		{"| eventstats earliest(status) AS first_", SuggestionKindField, nil},
		{"| eventstats latest(status) AS last_status B", SuggestionKindKeyword, []string{"BY"}},
		{"| eventstats earliest(status) AS first_status BY ho", SuggestionKindField, nil},
	} {
		context, contextDiagnostic := AnalyzeSuggestionContext(test.source, len(test.source))
		if contextDiagnostic != nil {
			t.Fatalf("AnalyzeSuggestionContext(%q): %v", test.source, contextDiagnostic)
		}
		if !context.Allows(test.kind) || !slices.Equal(context.Keywords, test.keywords) {
			t.Fatalf("AnalyzeSuggestionContext(%q) = %#v", test.source, context)
		}
	}
}
