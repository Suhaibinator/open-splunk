package knowledge

import (
	"context"
	"strings"
	"testing"
)

func TestSelectorImpliesConservativeDimensionProof(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		source SelectorSpec
		target SelectorSpec
		want   bool
	}{
		{name: "both unrestricted", want: true},
		{name: "target unrestricted", source: implicationDimensionSelector("main", "api-*"), want: true},
		{name: "target universal", source: implicationDimensionSelector("main"), target: implicationDimensionSelector("*"), want: true},
		{name: "unrestricted source does not imply universal", target: implicationDimensionSelector("*"), want: false},
		{name: "source unrestricted", target: implicationDimensionSelector("main"), want: false},
		{name: "literal exact", source: implicationDimensionSelector("main"), target: implicationDimensionSelector("main"), want: true},
		{name: "literal matched by wildcard", source: implicationDimensionSelector("api-01"), target: implicationDimensionSelector("api-??"), want: true},
		{name: "literal outside wildcard", source: implicationDimensionSelector("worker-01"), target: implicationDimensionSelector("api-*"), want: false},
		{name: "identical wildcard", source: implicationDimensionSelector("api-*"), target: implicationDimensionSelector("api-*"), want: true},
		{
			name:   "nontrivial wildcard containment fails closed",
			source: implicationDimensionSelector("api-?"),
			target: implicationDimensionSelector("api-*"),
			want:   false,
		},
		{
			name: "dimensions independent",
			source: SelectorSpec{Dimensions: []DimensionSpec{
				{Dimension: DimensionIndex, Patterns: []string{"main"}},
				{Dimension: DimensionHost, Patterns: []string{"api-01"}},
			}},
			target: SelectorSpec{Dimensions: []DimensionSpec{
				{Dimension: DimensionIndex, Patterns: []string{"main", "audit"}},
				{Dimension: DimensionHost, Patterns: []string{"api-*"}},
			}},
			want: true,
		},
		{
			name: "one dimension disproves",
			source: SelectorSpec{Dimensions: []DimensionSpec{
				{Dimension: DimensionIndex, Patterns: []string{"main"}},
				{Dimension: DimensionHost, Patterns: []string{"worker"}},
			}},
			target: SelectorSpec{Dimensions: []DimensionSpec{
				{Dimension: DimensionIndex, Patterns: []string{"main"}},
				{Dimension: DimensionHost, Patterns: []string{"api-*"}},
			}},
			want: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			source, err := CompileSelector(test.source)
			if err != nil {
				t.Fatal(err)
			}
			target, err := CompileSelector(test.target)
			if err != nil {
				t.Fatal(err)
			}
			if got := SelectorImplies(source, target); got != test.want {
				t.Fatalf("SelectorImplies() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestSelectorImpliesUniversalTargetPreservesMissingAndNullSemantics(t *testing.T) {
	t.Parallel()
	source, err := CompileSelector(SelectorSpec{})
	if err != nil {
		t.Fatal(err)
	}
	target, err := CompileSelector(implicationDimensionSelector("*"))
	if err != nil {
		t.Fatal(err)
	}
	for _, metadata := range []EventMetadata{
		{Host: MissingMetadata()},
		{Host: NullMetadata()},
	} {
		sourceMatch, _, sourceErr := source.Match(context.Background(), metadata, DefaultRuntimeBudget())
		targetMatch, _, targetErr := target.Match(context.Background(), metadata, DefaultRuntimeBudget())
		if sourceErr != nil || targetErr != nil || !sourceMatch || targetMatch {
			t.Fatalf("runtime match = source:%t/%v target:%t/%v", sourceMatch, sourceErr, targetMatch, targetErr)
		}
	}
}

func TestGlobPatternMatchesLiteralMaximumShapeAllocatesNothing(t *testing.T) {
	pattern, err := NormalizePattern(strings.Repeat("*a", MaximumSelectorPatternBytes/2) + "*")
	if err != nil {
		t.Fatal(err)
	}
	literal := strings.Repeat("a", MaximumSelectorPatternBytes)
	if !globPatternMatchesLiteral(pattern, literal) {
		t.Fatal("maximum-shape pattern did not match")
	}
	if allocations := testing.AllocsPerRun(100, func() {
		if !globPatternMatchesLiteral(pattern, literal) {
			panic("maximum-shape pattern did not match")
		}
	}); allocations != 0 {
		t.Fatalf("allocations per maximum-shape match = %v, want 0", allocations)
	}
}

func TestGlobPatternMatchesLiteralAgreesWithReference(t *testing.T) {
	patterns := enumerateSelectorTestStrings(
		[]string{"a", "é", "😀", "?", "*", `\*`, `\?`, `\\`},
		2,
		false,
	)
	patterns = append(patterns, "a**?é", `\**`, `?\\*`, "😀*?a")
	values := enumerateSelectorTestStrings(
		[]string{"a", "é", "😀", "*", "?", `\`},
		3,
		true,
	)
	for _, source := range patterns {
		pattern, err := NormalizePattern(source)
		if err != nil {
			t.Fatalf("NormalizePattern(%q): %v", source, err)
		}
		for _, value := range values {
			got := globPatternMatchesLiteral(pattern, value)
			want := referenceGlobMatch(pattern.tokens, []rune(value))
			if got != want {
				t.Fatalf("globPatternMatchesLiteral(%q, %q) = %t, want %t", pattern.String(), value, got, want)
			}
		}
	}
}

func enumerateSelectorTestStrings(atoms []string, maximum int, includeEmpty bool) []string {
	values := make([]string, 0)
	if includeEmpty {
		values = append(values, "")
	}
	var visit func(string, int)
	visit = func(prefix string, remaining int) {
		if remaining == 0 {
			return
		}
		for _, atom := range atoms {
			value := prefix + atom
			values = append(values, value)
			visit(value, remaining-1)
		}
	}
	visit("", maximum)
	return values
}

func TestSelectorImpliesRejectsNilAuthority(t *testing.T) {
	t.Parallel()
	selector, err := CompileSelector(SelectorSpec{})
	if err != nil {
		t.Fatal(err)
	}
	if SelectorImplies(nil, selector) || SelectorImplies(selector, nil) {
		t.Fatal("nil selector was treated as trusted authority")
	}
}

func TestSelectorsProvablyDisjointConservativeProof(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		left, right SelectorSpec
		want        bool
	}{
		{name: "both unrestricted"},
		{name: "one unrestricted", left: implicationDimensionSelector("main")},
		{name: "distinct literals", left: implicationDimensionSelector("main"), right: implicationDimensionSelector("audit"), want: true},
		{name: "same literal", left: implicationDimensionSelector("main"), right: implicationDimensionSelector("main")},
		{name: "literal outside glob", left: implicationDimensionSelector("worker-01"), right: implicationDimensionSelector("api-*"), want: true},
		{name: "literal inside glob", left: implicationDimensionSelector("api-01"), right: implicationDimensionSelector("api-*"), want: false},
		{name: "ambiguous wildcard languages", left: implicationDimensionSelector("api-?"), right: implicationDimensionSelector("worker-*"), want: false},
		{
			name: "one disjoint dimension proves selectors disjoint",
			left: SelectorSpec{Dimensions: []DimensionSpec{
				{Dimension: DimensionIndex, Patterns: []string{"main"}},
				{Dimension: DimensionHost, Patterns: []string{"shared"}},
			}},
			right: SelectorSpec{Dimensions: []DimensionSpec{
				{Dimension: DimensionIndex, Patterns: []string{"audit"}},
				{Dimension: DimensionHost, Patterns: []string{"shared"}},
			}},
			want: true,
		},
		{
			name:  "every alternative pair must be disjoint",
			left:  implicationDimensionSelector("main", "audit"),
			right: implicationDimensionSelector("other", "audit"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			left, err := CompileSelector(test.left)
			if err != nil {
				t.Fatal(err)
			}
			right, err := CompileSelector(test.right)
			if err != nil {
				t.Fatal(err)
			}
			if got := SelectorsProvablyDisjoint(left, right); got != test.want ||
				SelectorsProvablyDisjoint(right, left) != test.want {
				t.Fatalf("SelectorsProvablyDisjoint() = %t, want symmetric %t", got, test.want)
			}
		})
	}
	if SelectorsProvablyDisjoint(nil, &Selector{}) || SelectorsProvablyDisjoint(&Selector{}, nil) {
		t.Fatal("nil selector was treated as trusted disjoint authority")
	}
}

func implicationDimensionSelector(patterns ...string) SelectorSpec {
	return SelectorSpec{Dimensions: []DimensionSpec{{Dimension: DimensionHost, Patterns: patterns}}}
}
