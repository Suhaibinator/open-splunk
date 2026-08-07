package knowledge

import (
	"bytes"
	"context"
	"testing"
)

func FuzzNormalizePattern(f *testing.F) {
	for _, seed := range []string{
		"host-*", "api-??", `literal\*`, `literal\?`, `literal\\`, "東京-?", "a***b",
		"\t trimmed \r", "bad\x00control", `trailing\`, string([]byte{0xff}),
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, source string) {
		pattern, err := NormalizePattern(source)
		if err != nil {
			return
		}
		reparsed, err := NormalizePattern(pattern.String())
		if err != nil {
			t.Fatalf("canonical pattern %q did not reparse: %v", pattern.String(), err)
		}
		if reparsed.String() != pattern.String() || reparsed.IsLiteral() != pattern.IsLiteral() {
			t.Fatalf("normalization is not idempotent: %#v then %#v", pattern, reparsed)
		}
		if literal, ok := pattern.Literal(); ok {
			reparsedLiteral, reparsedOK := reparsed.Literal()
			if !reparsedOK || reparsedLiteral != literal {
				t.Fatalf("literal changed from %q to %q", literal, reparsedLiteral)
			}
		}
	})
}

func FuzzSelectorMatchReference(f *testing.F) {
	for _, seed := range []struct{ pattern, value string }{
		{"*", ""}, {"?", "é"}, {"??", "😀x"}, {"a*b", "a\nb"},
		{`\*`, "*"}, {"東京*", "東京😀"}, {"API-*", "api-1"}, {"a***b", "ab"},
	} {
		f.Add(seed.pattern, seed.value)
	}
	f.Fuzz(func(t *testing.T, source, value string) {
		pattern, err := NormalizePattern(source)
		if err != nil || len(value) > MaximumSelectorRuntimeValueBytes {
			return
		}
		selector, err := CompileSelector(SelectorSpec{Dimensions: []DimensionSpec{{
			Dimension: DimensionHost, Patterns: []string{source},
		}}})
		if err != nil {
			return
		}
		matched, _, err := selector.Match(context.Background(), EventMetadata{Host: StringMetadata(value)}, DefaultRuntimeBudget())
		if err != nil {
			return
		}
		if want := referenceGlobMatch(pattern.tokens, []rune(value)); matched != want {
			t.Fatalf("pattern %q value %q = %t, want %t", source, value, matched, want)
		}
	})
}

func FuzzSelectorMultiPatternORReference(f *testing.F) {
	for _, seed := range []struct{ first, second, value string }{
		{"a*", "*z", "ab"},
		{"?", "??", "é"},
		{`\*`, "*", "literal"},
		{"a***", "a*", "abc"},
		{"東京*", "*😀", "東京😀"},
	} {
		f.Add(seed.first, seed.second, seed.value)
	}
	f.Fuzz(func(t *testing.T, firstSource, secondSource, value string) {
		first, firstErr := NormalizePattern(firstSource)
		second, secondErr := NormalizePattern(secondSource)
		if firstErr != nil || secondErr != nil || len(value) > MaximumSelectorRuntimeValueBytes {
			return
		}
		selector, err := CompileSelector(SelectorSpec{Dimensions: []DimensionSpec{{
			Dimension: DimensionHost,
			Patterns:  []string{firstSource, secondSource},
		}}})
		if err != nil {
			return
		}
		matched, _, err := selector.Match(
			context.Background(),
			EventMetadata{Host: StringMetadata(value)},
			DefaultRuntimeBudget(),
		)
		if err != nil {
			return
		}
		want := referenceGlobMatch(first.tokens, []rune(value)) ||
			referenceGlobMatch(second.tokens, []rune(value))
		if matched != want {
			t.Fatalf("patterns %q/%q value %q = %t, want %t", firstSource, secondSource, value, matched, want)
		}
	})
}

func FuzzSelectorCanonicalOrder(f *testing.F) {
	for _, seed := range []struct{ first, second string }{
		{"a*", "b?"}, {"exact", "exact"}, {"a***", "a*"}, {`\*`, `\?`}, {"東京", "😀*"},
	} {
		f.Add(seed.first, seed.second)
	}
	f.Fuzz(func(t *testing.T, first, second string) {
		one, err := CompileSelector(SelectorSpec{Dimensions: []DimensionSpec{{
			Dimension: DimensionSource, Patterns: []string{first, second},
		}}})
		if err != nil {
			return
		}
		two, err := CompileSelector(SelectorSpec{Dimensions: []DimensionSpec{{
			Dimension: DimensionSource, Patterns: []string{second, first, first},
		}}})
		if err != nil {
			return
		}
		if !bytes.Equal(one.CanonicalBytes(), two.CanonicalBytes()) {
			t.Fatalf("canonical order depends on source ordering: %x != %x", one.CanonicalBytes(), two.CanonicalBytes())
		}
	})
}
