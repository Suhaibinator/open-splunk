package tokenconstraint

import (
	"slices"
	"strings"
	"testing"
)

func TestNormalizeValidatesDeduplicatesAndSortsExactPatterns(t *testing.T) {
	t.Parallel()

	got, err := Normalize([]string{"^z$", "^a$", "^z$", " host "})
	if err != nil {
		t.Fatalf("Normalize(): %v", err)
	}
	want := []string{" host ", "^a$", "^z$"}
	if !slices.Equal(got, want) {
		t.Fatalf("Normalize() = %q, want %q", got, want)
	}
	if err := ValidateNormalized(got); err != nil {
		t.Fatalf("ValidateNormalized(): %v", err)
	}
	manyDuplicates := make([]string, MaximumPatternsPerDimension+1)
	for index := range manyDuplicates {
		manyDuplicates[index] = "^same$"
	}
	deduplicated, err := Normalize(manyDuplicates)
	if err != nil {
		t.Fatalf("Normalize(many duplicates): %v", err)
	}
	if !slices.Equal(deduplicated, []string{"^same$"}) {
		t.Fatalf("Normalize(many duplicates) = %q", deduplicated)
	}
	for _, invalid := range [][]string{
		{"^z$", "^a$"},
		{"^a$", "^a$"},
	} {
		if err := ValidateNormalized(invalid); err == nil {
			t.Fatalf("ValidateNormalized(%q) unexpectedly succeeded", invalid)
		}
	}
}

func TestNormalizeRejectsCompiledProgramOverComplexityLimit(t *testing.T) {
	t.Parallel()

	// Each counted repetition is compact source text but expands to one rune
	// instruction per repetition in the RE2 program.
	pattern := strings.Repeat("a{1000}", 5)
	if len(pattern) > MaximumPatternBytes {
		t.Fatalf("test pattern has %d bytes", len(pattern))
	}
	if _, err := Normalize([]string{pattern}); err == nil {
		t.Fatal("Normalize() unexpectedly accepted an oversized RE2 program")
	}
}

func TestNormalizeRejectsAggregateProgramOverComplexityLimit(t *testing.T) {
	t.Parallel()

	patterns := make([]string, 5)
	for index := range patterns {
		unit := string(rune('a'+index)) + "{1000}"
		patterns[index] = strings.Repeat(unit, 4)
		instructions, err := compiledInstructionCount(patterns[index])
		if err != nil {
			t.Fatalf("compile test pattern %d: %v", index, err)
		}
		if instructions > MaximumPatternInstructions {
			t.Fatalf(
				"test pattern %d has %d instructions, exceeds per-pattern limit %d",
				index,
				instructions,
				MaximumPatternInstructions,
			)
		}
	}
	if _, err := Normalize(patterns); err == nil {
		t.Fatal("Normalize() unexpectedly accepted an oversized aggregate RE2 program")
	}
}
