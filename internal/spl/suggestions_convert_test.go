package spl

import (
	"slices"
	"testing"
)

func TestConvertSuggestionsStayInsideCommandGrammar(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		source string
		kind   SuggestionKind
		labels []string
	}{
		{`| con`, SuggestionKindCommand, []string{"convert"}},
		{`| convert ct`, SuggestionKindKeyword, []string{"ctime"}},
		{`| convert ti`, SuggestionKindKeyword, []string{"timeformat="}},
		{`| convert timeformat="%Y-%m-%d" mk`, SuggestionKindKeyword, []string{"mktime"}},
		{`| convert ctime(`, SuggestionKindField, nil},
		{`| convert ctime(_ti`, SuggestionKindField, nil},
		{`| convert ctime(_time) A`, SuggestionKindKeyword, []string{"AS", "auto"}},
		{`| convert ctime(_time) AS `, SuggestionKindField, nil},
		{`| convert ctime(_time) AS rendered, du`, SuggestionKindKeyword, []string{"dur2sec"}},
		{`| convert num(bytes*) A`, SuggestionKindKeyword, []string{"auto"}},
		{`| convert none(bytes) A`, SuggestionKindKeyword, []string{"auto"}},
		{`| convert none(bytes) num(`, SuggestionKindField, nil},
	} {
		t.Run(test.source, func(t *testing.T) {
			result := Suggest(test.source, len(test.source), 100)
			if result.Diagnostic != nil || !slices.Equal(result.Context.Kinds, []SuggestionKind{test.kind}) {
				t.Fatalf("context=%+v diagnostic=%v", result.Context, result.Diagnostic)
			}
			if test.labels != nil && !slices.Equal(suggestionLabels(result.Suggestions), test.labels) {
				t.Fatalf("labels=%v, want %v", suggestionLabels(result.Suggestions), test.labels)
			}
		})
	}
	for _, source := range []string{`| convert timeformat=`, `| convert ctime(_time `, `| convert num(bytes*) AS `, `| convert none(bytes) AS `} {
		context, diagnostic := AnalyzeSuggestionContext(source, len(source))
		if diagnostic != nil || len(context.Kinds) != 0 {
			t.Fatalf("%q context=%+v diagnostic=%v", source, context, diagnostic)
		}
	}
}

func TestConversionFunctionsNeverAppearAsScalarSuggestions(t *testing.T) {
	t.Parallel()
	for _, source := range []string{`| eval result=`, `| where `} {
		result := Suggest(source, len(source), MaximumSuggestionLimit)
		if result.Diagnostic != nil {
			t.Fatal(result.Diagnostic)
		}
		for _, suggestion := range result.Suggestions {
			if containsASCIIFold(convertSuggestionFunctionNames, suggestion.Label) {
				t.Fatalf("%q advertised conversion %q as scalar", source, suggestion.Label)
			}
		}
	}
}
