package knowledge

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"
)

func TestNormalizePatternPinsClosedGlobGrammar(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source      string
		canonical   string
		literal     string
		wantLiteral bool
	}{
		{source: " api-?? ", canonical: "api-??"},
		{source: "a***b", canonical: "a*b"},
		{source: `literal\*star`, canonical: `literal\*star`, literal: "literal*star", wantLiteral: true},
		{source: `literal\?question`, canonical: `literal\?question`, literal: "literal?question", wantLiteral: true},
		{source: `literal\\slash`, canonical: `literal\\slash`, literal: `literal\slash`, wantLiteral: true},
		{source: "東京", canonical: "東京", literal: "東京", wantLiteral: true},
		{source: "\u00a0host\u00a0", canonical: "\u00a0host\u00a0", literal: "\u00a0host\u00a0", wantLiteral: true},
	} {
		test := test
		t.Run(test.source, func(t *testing.T) {
			t.Parallel()
			pattern, err := NormalizePattern(test.source)
			if err != nil {
				t.Fatal(err)
			}
			if pattern.String() != test.canonical || pattern.IsLiteral() != test.wantLiteral {
				t.Fatalf("pattern = (%q, literal=%t), want (%q, %t)", pattern.String(), pattern.IsLiteral(), test.canonical, test.wantLiteral)
			}
			literal, ok := pattern.Literal()
			if ok != test.wantLiteral || ok && literal != test.literal {
				t.Fatalf("Literal() = %q, %t; want %q, %t", literal, ok, test.literal, test.wantLiteral)
			}
		})
	}

	for _, source := range []string{"", " \t ", `trailing\`, `bad\a`, "a\x00b", "a\x7fb", "a\u0080b", "a\u0085b", "a\u009fb", string([]byte{0xff})} {
		if _, err := NormalizePattern(source); !errors.Is(err, ErrInvalidSelector) {
			t.Errorf("NormalizePattern(%q) error = %v, want ErrInvalidSelector", source, err)
		}
	}
	maximum := strings.Repeat("x", MaximumSelectorPatternBytes)
	if pattern, err := NormalizePattern(maximum); err != nil || pattern.String() != maximum {
		t.Fatalf("maximum pattern = %d bytes, %v", len(pattern.String()), err)
	}
	if _, err := NormalizePattern(maximum + "x"); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("oversized pattern error = %v", err)
	}
}

func TestSelectorContractLimitConstantsArePinned(t *testing.T) {
	t.Parallel()

	if MaximumSelectorDimensions != 4 ||
		MaximumSelectorPatternsPerDimension != 16 ||
		MaximumSelectorPatterns != 64 ||
		MaximumSelectorPatternBytes != 255 ||
		MaximumSelectorNormalizedBytes != 8<<10 ||
		MaximumSelectorWildcardWorkUnits != 1<<10 ||
		MaximumSelectorRuntimeValueBytes != 1<<20 ||
		MaximumSelectorRuntimeEventBytes != 4<<20 ||
		MaximumSelectorRuntimeQueryUnits != 1<<30 ||
		SelectorMatcherTransitionUnits != 8 {
		t.Fatalf("selector compatibility limits changed")
	}
}

func TestSelectorGlobIsAnchoredCaseSensitiveAndUnicodeScalarAware(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		pattern string
		value   string
		want    bool
	}{
		{pattern: "api", value: "api", want: true},
		{pattern: "api", value: "API"},
		{pattern: "api", value: "xapi"},
		{pattern: "api", value: "apix"},
		{pattern: "?", value: "é", want: true},
		{pattern: "?", value: "😀", want: true},
		{pattern: "?", value: "é"},
		{pattern: "??", value: "é", want: true},
		{pattern: "a*b", value: "a\n東京\nb", want: true},
		{pattern: `\*`, value: "*", want: true},
		{pattern: `\?`, value: "?", want: true},
	} {
		test := test
		t.Run(fmt.Sprintf("%q/%q", test.pattern, test.value), func(t *testing.T) {
			t.Parallel()
			selector := mustCompileSelector(t, SelectorSpec{Dimensions: []DimensionSpec{{
				Dimension: DimensionHost,
				Patterns:  []string{test.pattern},
			}}})
			matched, _, err := selector.Match(context.Background(), EventMetadata{Host: StringMetadata(test.value)}, DefaultRuntimeBudget())
			if err != nil || matched != test.want {
				t.Fatalf("Match() = %t, %v; want %t", matched, err, test.want)
			}
		})
	}
}

func TestCompileSelectorCanonicalizesOrderDuplicatesAndCallerMemory(t *testing.T) {
	t.Parallel()

	hostPatterns := []string{"z*", "alpha", "a***", "alpha"}
	dimensions := []DimensionSpec{
		{Dimension: DimensionSource, Patterns: []string{"/var/*", "/tmp/??"}},
		{Dimension: DimensionHost, Patterns: hostPatterns},
	}
	first := mustCompileSelector(t, SelectorSpec{Dimensions: dimensions})
	second := mustCompileSelector(t, SelectorSpec{Dimensions: []DimensionSpec{
		{Dimension: DimensionHost, Patterns: []string{"a*", "alpha", "z*"}},
		{Dimension: DimensionSource, Patterns: []string{"/tmp/??", "/var/*"}},
	}})
	if !bytes.Equal(first.CanonicalBytes(), second.CanonicalBytes()) {
		t.Fatalf("canonical selectors differ:\n%x\n%x", first.CanonicalBytes(), second.CanonicalBytes())
	}
	if got, want := first.Patterns(DimensionHost), []string{"a*", "alpha", "z*"}; !slices.Equal(got, want) {
		t.Fatalf("host patterns = %#v, want %#v", got, want)
	}

	hostPatterns[0] = "mutated"
	dimensions[0].Patterns[0] = "mutated"
	patterns := first.Patterns(DimensionHost)
	patterns[0] = "mutated"
	canonical := first.CanonicalBytes()
	canonical[0] ^= 0xff
	if !slices.Equal(first.Patterns(DimensionHost), []string{"a*", "alpha", "z*"}) ||
		!bytes.Equal(first.CanonicalBytes(), second.CanonicalBytes()) {
		t.Fatal("compiled selector exposed caller or returned mutable backing state")
	}

	stats := first.Stats()
	if stats.Dimensions != 2 || stats.Patterns != 5 || stats.NormalizedBytes != uint64(len(first.CanonicalBytes())) ||
		stats.WildcardWorkUnits == 0 {
		t.Fatalf("compile stats = %+v", stats)
	}
}

func TestCompileSelectorPinsMixedKindByteOrderAndCanonicalDuplicateCollapse(t *testing.T) {
	t.Parallel()

	selector := mustCompileSelector(t, SelectorSpec{Dimensions: []DimensionSpec{{
		Dimension: DimensionHost,
		Patterns:  []string{`\*`, "*", "a***", "a*", `\*`},
	}}})
	if got, want := selector.Patterns(DimensionHost), []string{"*", `\*`, "a*"}; !slices.Equal(got, want) {
		t.Fatalf("mixed canonical patterns = %#v, want byte order %#v", got, want)
	}
	if got, want := selector.Stats().Patterns, uint64(3); got != want {
		t.Fatalf("canonical pattern count = %d, want %d", got, want)
	}
}

func TestSelectorANDORAndMissingNullSemantics(t *testing.T) {
	t.Parallel()

	selector := mustCompileSelector(t, SelectorSpec{Dimensions: []DimensionSpec{
		{Dimension: DimensionIndex, Patterns: []string{"main", "audit"}},
		{Dimension: DimensionHost, Patterns: []string{"api-*"}},
	}})
	for _, test := range []struct {
		name  string
		event EventMetadata
		want  bool
	}{
		{name: "first OR branch", event: EventMetadata{Index: StringMetadata("main"), Host: StringMetadata("api-1")}, want: true},
		{name: "second OR branch", event: EventMetadata{Index: StringMetadata("audit"), Host: StringMetadata("api-2")}, want: true},
		{name: "AND failure", event: EventMetadata{Index: StringMetadata("other"), Host: StringMetadata("api-1")}},
		{name: "missing constrained", event: EventMetadata{Index: MissingMetadata(), Host: StringMetadata("api-1")}},
		{name: "null constrained", event: EventMetadata{Index: NullMetadata(), Host: StringMetadata("api-1")}},
		{name: "missing other constrained", event: EventMetadata{Index: StringMetadata("main"), Host: MissingMetadata()}},
		{name: "null other constrained", event: EventMetadata{Index: StringMetadata("main"), Host: NullMetadata()}},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			matched, _, err := selector.Match(context.Background(), test.event, DefaultRuntimeBudget())
			if err != nil || matched != test.want {
				t.Fatalf("Match() = %t, %v; want %t", matched, err, test.want)
			}
		})
	}

	unrestricted := mustCompileSelector(t, SelectorSpec{})
	for _, event := range []EventMetadata{{}, {Index: NullMetadata()}, {Host: StringMetadata(string([]byte{0xff}))}} {
		matched, remaining, err := unrestricted.Match(context.Background(), event, DefaultRuntimeBudget())
		if err != nil || !matched || remaining.Remaining() != DefaultRuntimeBudget().Remaining() {
			t.Fatalf("unrestricted Match(%+v) = %t, %+v, %v", event, matched, remaining.Remaining(), err)
		}
	}
	empty := mustCompileSelector(t, SelectorSpec{Dimensions: []DimensionSpec{{Dimension: DimensionHost, Patterns: []string{"*"}}}})
	matched, _, err := empty.Match(context.Background(), EventMetadata{Host: StringMetadata("")}, DefaultRuntimeBudget())
	if err != nil || !matched {
		t.Fatalf("star did not match present empty string: %t, %v", matched, err)
	}
}

func TestSelectorUsesLiteralFastPathAndOneCombinedWildcardProgram(t *testing.T) {
	t.Parallel()

	exact := mustCompileSelector(t, SelectorSpec{Dimensions: []DimensionSpec{{
		Dimension: DimensionHost,
		Patterns:  []string{"one", "two", "three"},
	}}})
	exactDimension := exact.dimensions[DimensionHost-1]
	if exactDimension.wildcard != nil || len(exactDimension.exact) != 3 {
		t.Fatalf("exact plan = %+v", exactDimension)
	}

	patterns := make([]string, MaximumSelectorPatternsPerDimension-1)
	for index := range patterns {
		patterns[index] = fmt.Sprintf("service-%02d-*", index)
	}
	mixed := mustCompileSelector(t, SelectorSpec{Dimensions: []DimensionSpec{{
		Dimension: DimensionHost,
		Patterns:  append(patterns, "exact"),
	}}})
	dimension := mixed.dimensions[DimensionHost-1]
	if dimension.wildcard == nil || len(dimension.wildcard.patterns) != len(patterns) || len(dimension.exact) != 1 {
		t.Fatalf("mixed plan did not retain one exact map plus one combined program: %+v", dimension)
	}
	// compiledDimension has one combined program pointer, not a program slice. This
	// structural assertion prevents regressions to sequential per-pattern scans.
	if reflect.TypeOf(dimension.wildcard).String() != "*knowledge.globProgram" {
		t.Fatalf("wildcard program type = %T", dimension.wildcard)
	}
	matched, _, err := mixed.Match(context.Background(), EventMetadata{Host: StringMetadata("service-14-worker")}, DefaultRuntimeBudget())
	if err != nil || !matched {
		t.Fatalf("combined program Match() = %t, %v", matched, err)
	}
}

func TestSelectorRuntimeProgramPinsRE2AndConservativeAssessment(t *testing.T) {
	t.Parallel()

	selector := mustCompileSelector(t, SelectorSpec{Dimensions: []DimensionSpec{{
		Dimension: DimensionHost,
		Patterns:  []string{`literal\*star`, "a*b", "?"},
	}}})
	program, ok := selector.RuntimeProgram(DimensionHost)
	if !ok {
		t.Fatal("RuntimeProgram(host) is absent")
	}
	if !slices.Equal(program.ExactLiterals, []string{"literal*star"}) {
		t.Fatalf("exact literals = %#v", program.ExactLiterals)
	}
	if want := `(?s)^(?:.|a.*b)$`; program.WildcardRE2 != want {
		t.Fatalf("wildcard RE2 = %q, want %q", program.WildcardRE2, want)
	}
	if program.Assessment != (MatcherTransitionAssessment{
		Initial: 2, PerInputByte: 14, Final: 6,
	}) {
		t.Fatalf("assessment = %+v", program.Assessment)
	}
	compiled := regexp.MustCompile(program.WildcardRE2)
	for _, test := range []struct {
		value string
		want  bool
	}{
		{value: "x", want: true},
		{value: "😀", want: true},
		{value: "a\n東京\nb", want: true},
		{value: "literal*star"},
		{value: "ab-extra"},
	} {
		if got := compiled.MatchString(test.value); got != test.want {
			t.Errorf("RE2(%q) = %t, want %t", test.value, got, test.want)
		}
	}

	// The returned literal inventory is detached from the selector.
	program.ExactLiterals[0] = "mutated"
	again, ok := selector.RuntimeProgram(DimensionHost)
	if !ok || !slices.Equal(again.ExactLiterals, []string{"literal*star"}) {
		t.Fatalf("detached runtime program = %#v, %t", again, ok)
	}
	if _, ok := selector.RuntimeProgram(DimensionIndex); ok {
		t.Fatal("unrestricted index returned a runtime program")
	}
}

func TestMatcherTransitionAssessmentBoundsActualCombinedNFAWork(t *testing.T) {
	t.Parallel()

	selector := mustCompileSelector(t, SelectorSpec{Dimensions: []DimensionSpec{{
		Dimension: DimensionHost,
		Patterns:  []string{"*", "a*b", "??", "東*😀", "*z"},
	}}})
	dimension := selector.dimensions[DimensionHost-1]
	for _, value := range []string{"", "a", "ab", "a\n東京\nb", "é😀", "東abc😀", strings.Repeat("x", 4096)} {
		bound, err := dimension.assessment.UpperBound(uint64(len(value)))
		if err != nil {
			t.Fatalf("UpperBound(%d): %v", len(value), err)
		}
		_, actual, matchErr := dimension.wildcard.match(context.Background(), value, bound)
		if matchErr != nil {
			t.Fatalf("match(%q) exceeded bound %d after %d: %v", value, bound, actual, matchErr)
		}
		if actual > bound {
			t.Fatalf("match(%q) transitions = %d, bound %d", value, actual, bound)
		}
	}
	if _, err := (MatcherTransitionAssessment{
		Initial: ^uint64(0), PerInputByte: 1,
	}).UpperBound(1); !errors.Is(err, ErrRuntimeLimit) {
		t.Fatalf("overflow assessment error = %v", err)
	}
}

func TestSelectorRuntimeProgramRE2MatchesClosedGlobReference(t *testing.T) {
	t.Parallel()

	sources := []string{"a*b", "file?.txt", `\**`, "a(b)?", "[x]*", "東*😀"}
	selector := mustCompileSelector(t, SelectorSpec{Dimensions: []DimensionSpec{{
		Dimension: DimensionHost, Patterns: sources,
	}}})
	program, ok := selector.RuntimeProgram(DimensionHost)
	if !ok || program.WildcardRE2 == "" {
		t.Fatalf("runtime program = %#v, %t", program, ok)
	}
	compiled := regexp.MustCompile(program.WildcardRE2)
	patterns := make([]Pattern, 0, len(sources))
	for _, source := range sources {
		pattern, err := NormalizePattern(source)
		if err != nil {
			t.Fatalf("NormalizePattern(%q): %v", source, err)
		}
		patterns = append(patterns, pattern)
	}
	for _, value := range []string{
		"", "a", "ab", "a\n東京\nb", "file1.txt", "file😀.txt", "file12.txt",
		"*", "*suffix", "a(b)x", "[x]", "[x]tail", "東\n😀", "東abc😀", "other",
	} {
		want := false
		for _, pattern := range patterns {
			if referenceGlobMatch(pattern.tokens, []rune(value)) {
				want = true
				break
			}
		}
		if got := compiled.MatchString(value); got != want {
			t.Errorf("combined RE2(%q) = %t, reference %t; regex %q", value, got, want, program.WildcardRE2)
		}
	}
}

func TestSelectorRuntimeChargeUsesExactFastPathOrFixedWildcardBound(t *testing.T) {
	t.Parallel()

	mixed := mustCompileSelector(t, SelectorSpec{Dimensions: []DimensionSpec{{
		Dimension: DimensionHost, Patterns: []string{"exact", "a*"},
	}}})
	for _, test := range []struct {
		name        string
		value       string
		wantMatch   bool
		wantInput   uint64
		wantMatcher uint64
		wantUnits   uint64
	}{
		{name: "exact hit", value: "exact", wantMatch: true, wantInput: 5, wantUnits: 5},
		{name: "wildcard hit", value: "a", wantMatch: true, wantInput: 1, wantMatcher: 11, wantUnits: 89},
		{name: "wildcard miss", value: "z", wantInput: 1, wantMatcher: 11, wantUnits: 89},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			matched, charged, err := mixed.Match(
				context.Background(), EventMetadata{Host: StringMetadata(test.value)}, DefaultRuntimeBudget(),
			)
			if err != nil || matched != test.wantMatch {
				t.Fatalf("Match(%q) = %t, %v", test.value, matched, err)
			}
			if got := charged.Charge(); got != (RuntimeCharge{
				InputBytes: test.wantInput, MatcherTransitionUpperBound: test.wantMatcher, QueryUnits: test.wantUnits,
			}) {
				t.Fatalf("charge(%q) = %+v", test.value, got)
			}
		})
	}

	literals := mustCompileSelector(t, SelectorSpec{Dimensions: []DimensionSpec{{
		Dimension: DimensionHost, Patterns: []string{"exact"},
	}}})
	matched, charged, err := literals.Match(
		context.Background(), EventMetadata{Host: StringMetadata("miss")}, DefaultRuntimeBudget(),
	)
	if err != nil || matched || charged.Charge() != (RuntimeCharge{InputBytes: 4, QueryUnits: 4}) {
		t.Fatalf("literal-only miss = %t charge=%+v err=%v", matched, charged.Charge(), err)
	}
}

func TestSelectorRuntimeChargeUsesUTF8BytesAndDoesNotCoerceMissingOrNull(t *testing.T) {
	t.Parallel()

	selector := mustCompileSelector(t, SelectorSpec{Dimensions: []DimensionSpec{{
		Dimension: DimensionHost, Patterns: []string{"?"},
	}}})
	for _, test := range []struct {
		value       string
		wantMatch   bool
		wantInput   uint64
		wantMatcher uint64
		wantUnits   uint64
	}{
		{value: "é", wantMatch: true, wantInput: 2, wantMatcher: 11, wantUnits: 90},
		{value: "éx", wantInput: 3, wantMatcher: 15, wantUnits: 123},
	} {
		matched, charged, err := selector.Match(
			context.Background(), EventMetadata{Host: StringMetadata(test.value)}, DefaultRuntimeBudget(),
		)
		if err != nil || matched != test.wantMatch {
			t.Fatalf("Match(%q) = %t, %v", test.value, matched, err)
		}
		if got := charged.Charge(); got != (RuntimeCharge{
			InputBytes: test.wantInput, MatcherTransitionUpperBound: test.wantMatcher, QueryUnits: test.wantUnits,
		}) {
			t.Fatalf("charge(%q) = %+v", test.value, got)
		}
	}

	universal := mustCompileSelector(t, SelectorSpec{Dimensions: []DimensionSpec{{
		Dimension: DimensionHost, Patterns: []string{"*"},
	}}})
	initial := DefaultRuntimeBudget()
	for _, value := range []MetadataValue{MissingMetadata(), NullMetadata()} {
		matched, returned, err := universal.Match(
			context.Background(), EventMetadata{Host: value}, initial,
		)
		if err != nil || matched || returned.Charge() != (RuntimeCharge{}) || returned.Remaining() != initial.Remaining() {
			t.Fatalf("universal Match(kind=%d) = %t charge=%+v remaining=%+v err=%v", value.Kind(), matched, returned.Charge(), returned.Remaining(), err)
		}
	}
}

func TestSelectorRuntimeChargePinsCompleteDimensionOrder(t *testing.T) {
	t.Parallel()

	selector := mustCompileSelector(t, SelectorSpec{Dimensions: []DimensionSpec{
		{Dimension: DimensionIndex, Patterns: []string{"i"}},
		{Dimension: DimensionHost, Patterns: []string{"h"}},
		{Dimension: DimensionSource, Patterns: []string{"s"}},
		{Dimension: DimensionSourcetype, Patterns: []string{"t*"}},
	}})
	matched, charged, err := selector.Match(context.Background(), EventMetadata{
		Index: StringMetadata("i"), Host: StringMetadata("h"), Source: StringMetadata("s"), Sourcetype: StringMetadata("type"),
	}, DefaultRuntimeBudget())
	if err != nil || !matched || charged.Charge() != (RuntimeCharge{
		InputBytes: 7, MatcherTransitionUpperBound: 32, QueryUnits: 263,
	}) {
		t.Fatalf("full ordered match = %t charge=%+v err=%v", matched, charged.Charge(), err)
	}

	matched, charged, err = selector.Match(context.Background(), EventMetadata{
		Index:      StringMetadata("i"),
		Host:       StringMetadata("h"),
		Source:     StringMetadata("x"),
		Sourcetype: StringMetadata(strings.Repeat("x", MaximumSelectorRuntimeValueBytes+1)),
	}, DefaultRuntimeBudget())
	if err != nil || matched || charged.Charge() != (RuntimeCharge{InputBytes: 3, QueryUnits: 3}) {
		t.Fatalf("source short-circuit = %t charge=%+v err=%v", matched, charged.Charge(), err)
	}
}

func TestCompileSelectorRejectsDimensionPatternByteAndWorkLimits(t *testing.T) {
	t.Parallel()

	five := make([]DimensionSpec, MaximumSelectorDimensions+1)
	for index := range five {
		five[index] = DimensionSpec{Dimension: DimensionIndex, Patterns: []string{"x"}}
	}
	tooManyPerDimension := make([]string, MaximumSelectorPatternsPerDimension+1)
	for index := range tooManyPerDimension {
		tooManyPerDimension[index] = fmt.Sprintf("p-%02d", index)
	}

	for _, test := range []struct {
		name string
		spec SelectorSpec
		err  error
	}{
		{name: "dimensions", spec: SelectorSpec{Dimensions: five}, err: ErrResourceLimit},
		{name: "unknown", spec: SelectorSpec{Dimensions: []DimensionSpec{{Dimension: 99, Patterns: []string{"x"}}}}, err: ErrInvalidSelector},
		{name: "duplicate", spec: SelectorSpec{Dimensions: []DimensionSpec{{Dimension: DimensionHost}, {Dimension: DimensionHost}}}, err: ErrInvalidSelector},
		{name: "patterns per dimension", spec: SelectorSpec{Dimensions: []DimensionSpec{{Dimension: DimensionHost, Patterns: tooManyPerDimension}}}, err: ErrResourceLimit},
		{name: "pattern bytes", spec: SelectorSpec{Dimensions: []DimensionSpec{{Dimension: DimensionHost, Patterns: []string{strings.Repeat("x", MaximumSelectorPatternBytes+1)}}}}, err: ErrResourceLimit},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := CompileSelector(test.spec); !errors.Is(err, test.err) {
				t.Fatalf("CompileSelector() error = %v, want %v", err, test.err)
			}
		})
	}

	total := make([]DimensionSpec, MaximumSelectorDimensions)
	for dimension := range total {
		patterns := make([]string, MaximumSelectorPatternsPerDimension)
		for index := range patterns {
			patterns[index] = fmt.Sprintf("d%d-%02d", dimension, index)
		}
		total[dimension] = DimensionSpec{Dimension: Dimension(dimension + 1), Patterns: patterns}
	}
	maximumPatterns := mustCompileSelector(t, SelectorSpec{Dimensions: total})
	if maximumPatterns.Stats().Patterns != MaximumSelectorPatterns {
		t.Fatalf("aggregate patterns = %d, want %d", maximumPatterns.Stats().Patterns, MaximumSelectorPatterns)
	}

	byteHeavy := make([]DimensionSpec, 4)
	for dimension := range byteHeavy {
		patterns := make([]string, 16)
		for index := range patterns {
			prefix := fmt.Sprintf("%d-%02d-", dimension, index)
			patterns[index] = prefix + strings.Repeat("x", 255-len(prefix))
		}
		byteHeavy[dimension] = DimensionSpec{Dimension: Dimension(dimension + 1), Patterns: patterns}
	}
	if _, err := CompileSelector(SelectorSpec{Dimensions: byteHeavy}); !errors.Is(err, ErrResourceLimit) || !strings.Contains(err.Error(), "canonical representation") {
		t.Fatalf("normalized byte error = %v", err)
	}

	workHeavy := make([]string, MaximumSelectorPatternsPerDimension)
	for index := range workHeavy {
		prefix := fmt.Sprintf("%02d-", index)
		workHeavy[index] = prefix + strings.Repeat("?", 64)
	}
	if _, err := CompileSelector(SelectorSpec{Dimensions: []DimensionSpec{{
		Dimension: DimensionHost, Patterns: workHeavy,
	}}}); !errors.Is(err, ErrResourceLimit) || !strings.Contains(err.Error(), "wildcard work") {
		t.Fatalf("wildcard work error = %v", err)
	}

	exactWorkBoundary := make([]string, MaximumSelectorPatternsPerDimension)
	for index := range exactWorkBoundary {
		prefix := fmt.Sprintf("%02d", index)
		exactWorkBoundary[index] = prefix + strings.Repeat("x", 64-len(prefix))
	}
	boundary := mustCompileSelector(t, SelectorSpec{Dimensions: []DimensionSpec{{
		Dimension: DimensionHost, Patterns: exactWorkBoundary,
	}}})
	if boundary.Stats().WildcardWorkUnits != MaximumSelectorWildcardWorkUnits {
		t.Fatalf("boundary wildcard work = %d, want %d", boundary.Stats().WildcardWorkUnits, MaximumSelectorWildcardWorkUnits)
	}
	charged, err := ChargeSnapshotSelectorWork(0, boundary)
	if err != nil || charged != MaximumSelectorWildcardWorkUnits {
		t.Fatalf("snapshot work boundary = %d, %v", charged, err)
	}
	one := mustCompileSelector(t, SelectorSpec{Dimensions: []DimensionSpec{{
		Dimension: DimensionHost, Patterns: []string{"x"},
	}}})
	if _, err := ChargeSnapshotSelectorWork(charged, one); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("snapshot aggregate overflow error = %v", err)
	}
}

func TestRuntimeBudgetChargesInputsAndRejectsInvalidOrExhaustedWork(t *testing.T) {
	t.Parallel()

	budget, err := NewRuntimeBudget(RuntimeLimits{QueryUnits: 64})
	if err != nil {
		t.Fatal(err)
	}
	charged, err := budget.ChargeInput("é")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := charged.Remaining(), (RuntimeRemaining{QueryUnits: 62, EventBytes: MaximumSelectorRuntimeEventBytes - 2}); got != want {
		t.Fatalf("remaining = %+v, want %+v", got, want)
	}
	if budget.Remaining() != (RuntimeRemaining{QueryUnits: 64, EventBytes: MaximumSelectorRuntimeEventBytes}) {
		t.Fatal("immutable budget receiver was mutated")
	}
	if charged.Charge() != (RuntimeCharge{InputBytes: 2, QueryUnits: 2}) {
		t.Fatalf("input charge = %+v", charged.Charge())
	}
	for _, test := range []struct {
		name  string
		value string
		err   error
	}{
		{name: "input exhausted", value: strings.Repeat("x", 63), err: ErrRuntimeLimit},
		{name: "invalid UTF-8", value: string([]byte{0xff}), err: ErrInvalidSelector},
		{name: "per value", value: strings.Repeat("x", MaximumSelectorRuntimeValueBytes+1), err: ErrRuntimeLimit},
	} {
		if _, chargeErr := charged.ChargeInput(test.value); !errors.Is(chargeErr, test.err) {
			t.Errorf("%s error = %v, want %v", test.name, chargeErr, test.err)
		}
	}
	for _, limits := range []RuntimeLimits{
		{}, {QueryUnits: MaximumSelectorRuntimeQueryUnits + 1},
	} {
		if _, err := NewRuntimeBudget(limits); !errors.Is(err, ErrRuntimeLimit) {
			t.Errorf("NewRuntimeBudget(%+v) error = %v", limits, err)
		}
	}
}

func TestRuntimeBudgetPinsValueEventAndCumulativeQueryBoundaries(t *testing.T) {
	t.Parallel()

	value := strings.Repeat("x", MaximumSelectorRuntimeValueBytes)
	budget := DefaultRuntimeBudget()
	if got := budget.Remaining(); got != (RuntimeRemaining{
		QueryUnits: MaximumSelectorRuntimeQueryUnits,
		EventBytes: MaximumSelectorRuntimeEventBytes,
	}) {
		t.Fatalf("default runtime capacity = %+v", got)
	}
	var err error
	for count := range MaximumSelectorRuntimeEventBytes / MaximumSelectorRuntimeValueBytes {
		budget, err = budget.ChargeInput(value)
		if err != nil {
			t.Fatalf("event boundary charge %d: %v", count, err)
		}
	}
	if budget.Remaining().EventBytes != 0 || budget.Charge().InputBytes != MaximumSelectorRuntimeEventBytes {
		t.Fatalf("full event charge = %+v remaining=%+v", budget.Charge(), budget.Remaining())
	}
	if _, err := budget.ChargeInput(""); err != nil {
		t.Fatalf("present empty string should cost zero bytes: %v", err)
	}
	if _, err := budget.ChargeInput("x"); !errors.Is(err, ErrRuntimeLimit) {
		t.Fatalf("event overflow error = %v", err)
	}
	budget, err = budget.BeginEvent()
	if err != nil {
		t.Fatal(err)
	}
	budget, err = budget.ChargeInput(value)
	if err != nil {
		t.Fatal(err)
	}
	if budget.Charge().InputBytes != 5<<20 || budget.Remaining().EventBytes != 3<<20 {
		t.Fatalf("cumulative query charge after next event = %+v remaining=%+v", budget.Charge(), budget.Remaining())
	}

	selector := mustCompileSelector(t, SelectorSpec{Dimensions: []DimensionSpec{{
		Dimension: DimensionHost, Patterns: []string{"a*"},
	}}})
	small, err := NewRuntimeBudget(RuntimeLimits{QueryUnits: 178})
	if err != nil {
		t.Fatal(err)
	}
	matched, small, err := selector.Match(context.Background(), EventMetadata{Host: StringMetadata("a")}, small)
	if err != nil || !matched || small.Charge() != (RuntimeCharge{
		InputBytes: 1, MatcherTransitionUpperBound: 11, QueryUnits: 89,
	}) {
		t.Fatalf("first cumulative match = %t charge=%+v err=%v", matched, small.Charge(), err)
	}
	small, err = small.BeginEvent()
	if err != nil {
		t.Fatal(err)
	}
	matched, small, err = selector.Match(context.Background(), EventMetadata{Host: StringMetadata("a")}, small)
	if err != nil || !matched || small.Charge().QueryUnits != 178 {
		t.Fatalf("query boundary match = %t charge=%+v err=%v", matched, small.Charge(), err)
	}
	small, err = small.BeginEvent()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := selector.Match(context.Background(), EventMetadata{Host: StringMetadata("a")}, small); !errors.Is(err, ErrRuntimeLimit) {
		t.Fatalf("exhausted cumulative query error = %v", err)
	}
}

type cancelOnSecondDoneContext struct {
	context.Context
	cancel     context.CancelFunc
	doneChecks int
}

func (ctx *cancelOnSecondDoneContext) Done() <-chan struct{} {
	ctx.doneChecks++
	if ctx.doneChecks == 2 {
		ctx.cancel()
	}
	return ctx.Context.Done()
}

func TestSelectorLargeMatchChecksCancellationPeriodically(t *testing.T) {
	t.Parallel()

	selector := mustCompileSelector(t, SelectorSpec{Dimensions: []DimensionSpec{{
		Dimension: DimensionHost,
		Patterns:  []string{"*z"},
	}}})
	base, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ctx := &cancelOnSecondDoneContext{Context: base, cancel: cancel}

	matched, charged, err := selector.Match(ctx, EventMetadata{
		Host: StringMetadata(strings.Repeat("a", MaximumSelectorRuntimeValueBytes)),
	}, DefaultRuntimeBudget())
	if matched || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled large match = %t, %v", matched, err)
	}
	if ctx.doneChecks != 2 {
		t.Fatalf("deadline checks = %d, want deterministic periodic second check", ctx.doneChecks)
	}
	program, ok := selector.RuntimeProgram(DimensionHost)
	if !ok {
		t.Fatal("host runtime program is absent")
	}
	want, err := program.Assessment.UpperBound(MaximumSelectorRuntimeValueBytes)
	if err != nil {
		t.Fatal(err)
	}
	if got := charged.Charge().MatcherTransitionUpperBound; got != want {
		t.Fatalf("charged transition bound before cancellation = %d, want %d", got, want)
	}
}

func TestEmptyStarChargesCombinedMatcherInitializationClosureAndTerminalWork(t *testing.T) {
	t.Parallel()

	selector := mustCompileSelector(t, SelectorSpec{Dimensions: []DimensionSpec{{
		Dimension: DimensionHost, Patterns: []string{"*"},
	}}})
	budget, err := NewRuntimeBudget(RuntimeLimits{QueryUnits: 64})
	if err != nil {
		t.Fatal(err)
	}
	for event := range 2 {
		matched, next, matchErr := selector.Match(context.Background(), EventMetadata{Host: StringMetadata("")}, budget)
		if matchErr != nil || !matched {
			t.Fatalf("empty star event %d = %t, %v", event, matched, matchErr)
		}
		budget, err = next.BeginEvent()
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := budget.Charge(); got != (RuntimeCharge{
		MatcherTransitionUpperBound: 8,
		QueryUnits:                  64,
	}) {
		t.Fatalf("two empty-star charges = %+v", got)
	}
	matched, exhausted, err := selector.Match(context.Background(), EventMetadata{Host: StringMetadata("")}, budget)
	if !errors.Is(err, ErrRuntimeLimit) || matched || exhausted.Charge() != budget.Charge() {
		t.Fatalf("exhausted empty-star match = %t charge=%+v err=%v", matched, exhausted.Charge(), err)
	}
}

func TestSelectorMatchChargesOnlyInspectedConstrainedDimensions(t *testing.T) {
	t.Parallel()

	selector := mustCompileSelector(t, SelectorSpec{Dimensions: []DimensionSpec{
		{Dimension: DimensionIndex, Patterns: []string{"main"}},
		{Dimension: DimensionHost, Patterns: []string{"api-*"}},
	}})
	initial := DefaultRuntimeBudget()
	matched, remaining, err := selector.Match(context.Background(), EventMetadata{
		Index: StringMetadata("other"),
		Host:  StringMetadata(strings.Repeat("x", MaximumSelectorRuntimeValueBytes+1)),
	}, initial)
	if err != nil || matched {
		t.Fatalf("early nonmatch = %t, %v", matched, err)
	}
	if remaining.Charge().InputBytes != uint64(len("other")) {
		t.Fatalf("early nonmatch charged unexpected input: before=%+v after=%+v", initial.Remaining(), remaining.Remaining())
	}

	_, returned, err := selector.Match(context.Background(), EventMetadata{
		Index: StringMetadata("main"),
		Host:  StringMetadata(strings.Repeat("x", MaximumSelectorRuntimeValueBytes+1)),
	}, initial)
	if !errors.Is(err, ErrRuntimeLimit) {
		t.Fatalf("oversized inspected host error = %v", err)
	}
	if returned.Charge().InputBytes != uint64(len("main")) {
		t.Fatalf("failed charge did not return last committed budget: %+v", returned.Remaining())
	}
}

func TestCombinedGlobMatchesReferenceAcrossAdversarialCorpus(t *testing.T) {
	t.Parallel()

	random := rand.New(rand.NewSource(20260806))
	alphabet := []rune{'a', 'b', 'A', 'é', '東', '😀', '\n'}
	patterns := []string{"", "*", "?", "a*", "*b", "?é?", "東*😀", `\*`, `\\`, "a**?b"}
	for range 500 {
		var pattern strings.Builder
		length := random.Intn(8) + 1
		for range length {
			switch random.Intn(8) {
			case 0:
				pattern.WriteByte('*')
			case 1:
				pattern.WriteByte('?')
			default:
				pattern.WriteRune(alphabet[random.Intn(len(alphabet)-1)])
			}
		}
		patterns = append(patterns, pattern.String())
	}
	values := []string{"", "a", "A", "é", "😀", "é", "a\nb", "東abc😀", "*", `\`}
	for range 500 {
		var value strings.Builder
		for index := 0; index < random.Intn(10); index++ {
			value.WriteRune(alphabet[random.Intn(len(alphabet))])
		}
		values = append(values, value.String())
	}

	for _, source := range patterns {
		pattern, err := NormalizePattern(source)
		if source == "" {
			if err == nil {
				t.Fatal("empty pattern unexpectedly normalized")
			}
			continue
		}
		if err != nil {
			t.Fatalf("NormalizePattern(%q): %v", source, err)
		}
		selector := mustCompileSelector(t, SelectorSpec{Dimensions: []DimensionSpec{{Dimension: DimensionHost, Patterns: []string{source}}}})
		for _, value := range values {
			got, _, matchErr := selector.Match(context.Background(), EventMetadata{Host: StringMetadata(value)}, DefaultRuntimeBudget())
			want := referenceGlobMatch(pattern.tokens, []rune(value))
			if matchErr != nil || got != want {
				t.Fatalf("pattern %q value %q = %t, %v; reference %t", source, value, got, matchErr, want)
			}
		}
	}
}

func TestSelectorIsSafeForConcurrentMatchingAndDetachedReads(t *testing.T) {
	selector := mustCompileSelector(t, SelectorSpec{Dimensions: []DimensionSpec{
		{Dimension: DimensionIndex, Patterns: []string{"main"}},
		{Dimension: DimensionHost, Patterns: []string{"api-*", "worker-??"}},
	}})
	wantCanonical := selector.CanonicalBytes()

	var wait sync.WaitGroup
	for worker := range 64 {
		worker := worker
		wait.Go(func() {
			for range 500 {
				host := fmt.Sprintf("api-%d", worker)
				matched, _, err := selector.Match(context.Background(), EventMetadata{
					Index: StringMetadata("main"), Host: StringMetadata(host),
				}, DefaultRuntimeBudget())
				if err != nil || !matched {
					t.Errorf("concurrent Match() = %t, %v", matched, err)
					return
				}
				canonicalCopy := selector.CanonicalBytes()
				canonicalCopy[len(canonicalCopy)-1] ^= byte(worker + 1)
			}
		})
	}
	wait.Wait()
	if !bytes.Equal(selector.CanonicalBytes(), wantCanonical) {
		t.Fatal("concurrent detached reads mutated selector")
	}
}

func referenceGlobMatch(tokens []globToken, value []rune) bool {
	type point struct{ token, value int }
	memo := make(map[point]bool)
	seen := make(map[point]bool)
	var visit func(int, int) bool
	visit = func(tokenIndex, valueIndex int) bool {
		key := point{token: tokenIndex, value: valueIndex}
		if seen[key] {
			return memo[key]
		}
		seen[key] = true
		matched := false
		if tokenIndex == len(tokens) {
			matched = valueIndex == len(value)
		} else {
			switch token := tokens[tokenIndex]; token.kind {
			case globLiteral:
				matched = valueIndex < len(value) && value[valueIndex] == token.literal && visit(tokenIndex+1, valueIndex+1)
			case globOne:
				matched = valueIndex < len(value) && visit(tokenIndex+1, valueIndex+1)
			case globMany:
				matched = visit(tokenIndex+1, valueIndex) || valueIndex < len(value) && visit(tokenIndex, valueIndex+1)
			}
		}
		memo[key] = matched
		return matched
	}
	return visit(0, 0)
}

func mustCompileSelector(t *testing.T, spec SelectorSpec) *Selector {
	t.Helper()
	selector, err := CompileSelector(spec)
	if err != nil {
		t.Fatal(err)
	}
	return selector
}

func TestMetadataValueKindsRemainDistinct(t *testing.T) {
	t.Parallel()

	values := []MetadataValue{MissingMetadata(), NullMetadata(), StringMetadata("")}
	for index, value := range values {
		if value.Kind() != ValueKind(index) {
			t.Fatalf("value %d kind = %d", index, value.Kind())
		}
	}
	if _, ok := values[0].String(); ok {
		t.Fatal("missing value exposed a string")
	}
	if _, ok := values[1].String(); ok {
		t.Fatal("null value exposed a string")
	}
	if text, ok := values[2].String(); !ok || text != "" {
		t.Fatalf("empty string = %q, %t", text, ok)
	}
}

func TestReferenceMatcherTreatsUTF8AsScalars(t *testing.T) {
	t.Parallel()

	pattern, err := NormalizePattern("??")
	if err != nil {
		t.Fatal(err)
	}
	if !referenceGlobMatch(pattern.tokens, []rune("é😀")) || referenceGlobMatch(pattern.tokens, []rune("é")) {
		t.Fatal("reference matcher scalar boundary is invalid")
	}
	if utf8.RuneCountInString("é😀") != 2 {
		t.Fatal("test fixture does not contain two Unicode scalar values")
	}
}
