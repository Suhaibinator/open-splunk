package spl

import (
	"slices"
	"testing"
)

func TestStatsWildcardSuggestionContexts(t *testing.T) {
	t.Parallel()

	active, diagnostic := AnalyzeSuggestionContext(
		`| stats avg(*la`,
		len(`| stats avg(*la`),
	)
	if diagnostic != nil {
		t.Fatalf("AnalyzeSuggestionContext(active): %v", diagnostic)
	}
	if !active.Allows(SuggestionKindField) || active.Prefix != "la" {
		t.Fatalf("active wildcard context = %#v", active)
	}

	for _, source := range []string{`| stats avg(*lay) `, `| stats avg `} {
		context, err := AnalyzeSuggestionContext(source, len(source))
		if err != nil {
			t.Fatalf("AnalyzeSuggestionContext(%q): %v", source, err)
		}
		if !context.Allows(SuggestionKindFunction) ||
			!context.Allows(SuggestionKindKeyword) ||
			!slices.Contains(context.Keywords, "AS") ||
			!slices.Contains(context.Keywords, "BY") {
			t.Fatalf("wildcard completion context for %q = %#v", source, context)
		}
	}

	wildcardAlias := `| stats avg(*) AS `
	context, err := AnalyzeSuggestionContext(wildcardAlias, len(wildcardAlias))
	if err != nil {
		t.Fatalf("AnalyzeSuggestionContext(alias): %v", err)
	}
	if !context.Allows(SuggestionKindField) {
		t.Fatalf("wildcard AS context = %#v, want wc-field authoring", context)
	}
}
