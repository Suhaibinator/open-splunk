package asciifold

import (
	"math/rand"
	"strings"
	"testing"
)

// TestFoldLeavesEveryNonLetterByteAlone walks all 256 byte values so the fold
// window can never silently widen past 'A'..'Z'.
func TestFoldLeavesEveryNonLetterByteAlone(t *testing.T) {
	t.Parallel()

	for value := range 256 {
		character := byte(value)
		want := character
		if character >= 'A' && character <= 'Z' {
			want = character + 32
		}
		if got := Fold(character); got != want {
			t.Fatalf("Fold(%#x) = %#x, want %#x", character, got, want)
		}
	}
	// The bytes bracketing both letter ranges, NUL, and DEL are the ones a
	// range comparison is most likely to capture by accident.
	for _, character := range []byte{0x00, '@', '[', '`', '{', 0x7f, 0x80, 0xff} {
		if Fold(character) != character {
			t.Fatalf("Fold(%#x) folded a non-letter byte", character)
		}
	}
}

// TestMatcherHandlesNulAndDelBytes keeps binary payload bytes exact and
// scannable rather than terminating or folding the pattern.
func TestMatcherHandlesNulAndDelBytes(t *testing.T) {
	t.Parallel()

	value := "head\x00MID\x7ftail"
	tests := []struct {
		pattern string
		want    bool
	}{
		{pattern: "\x00mid\x7f", want: true},
		{pattern: "\x00MID\x7F", want: true},
		{pattern: "head\x00", want: true},
		{pattern: "\x7ftail", want: true},
		{pattern: "head\x00mid", want: true},
		{pattern: "headmid", want: false},
		{pattern: "head\x00tail", want: false},
		{pattern: "mid\x7ftail\x00", want: false},
		{pattern: "\x00\x00", want: false},
	}
	for _, test := range tests {
		matcher := New(test.pattern)
		if got := matcher.Contains(value); got != test.want {
			t.Fatalf("Contains(%q, %q) = %t, want %t", value, test.pattern, got, test.want)
		}
		got, err := matcher.ContainsFunc(value, 8, func() error { return nil })
		if err != nil {
			t.Fatalf("ContainsFunc(%q) error = %v", test.pattern, err)
		}
		if got != test.want {
			t.Fatalf("ContainsFunc(%q, %q) = %t, want %t", value, test.pattern, got, test.want)
		}
	}
}

// TestMatcherLengthEdges pins the pattern/value length boundary, including the
// exact-length match that the len(pattern) > len(value) short circuit brackets
// and the empty-value case where ContainsFunc has no such short circuit.
func TestMatcherLengthEdges(t *testing.T) {
	t.Parallel()

	for _, length := range []int{1, 2, 1023, 1024, 1025, 4096, 4097} {
		exact := strings.Repeat("aB", length/2) + strings.Repeat("c", length%2)
		matcher := New(exact)
		if !matcher.Contains(strings.ToUpper(exact)) {
			t.Fatalf("length %d: equal-length value did not match", length)
		}
		if matcher.Contains(exact[:len(exact)-1]) {
			t.Fatalf("length %d: value one byte short matched", length)
		}
		if !matcher.Contains("x" + exact + "x") {
			t.Fatalf("length %d: embedded value did not match", length)
		}
		if matcher.Contains("") {
			t.Fatalf("length %d: empty value matched a nonempty pattern", length)
		}
		got, err := matcher.ContainsFunc("", 4096, func() error { return nil })
		if err != nil || got {
			t.Fatalf("length %d: ContainsFunc(\"\") = (%t, %v), want (false, nil)", length, got, err)
		}
	}
}

// TestMatcherLongPatternFailureTable drives the failure table far past any
// fixed 1024-byte scratch buffer with a highly periodic pattern, which is the
// input class that makes KMP walk its prefix links repeatedly.
func TestMatcherLongPatternFailureTable(t *testing.T) {
	t.Parallel()

	for _, length := range []int{1025, 2048, 8193} {
		pattern := strings.Repeat("Ab", length/2) + "Z"
		matcher := New(pattern)
		// A value made only of the pattern's period never completes it.
		if matcher.Contains(strings.Repeat("ab", 4*length)) {
			t.Fatalf("length %d: periodic prefix without the terminator matched", length)
		}
		// The same value plus the terminator must match on the final byte.
		if !matcher.Contains(strings.Repeat("ab", 4*length) + "z") {
			t.Fatalf("length %d: terminated periodic value did not match", length)
		}
		// A near miss one byte before the terminator must still fail.
		if matcher.Contains(strings.Repeat("ab", 4*length) + "y") {
			t.Fatalf("length %d: near-miss terminator matched", length)
		}
	}
}

// TestContainsFuncCheckCadence pins the documented cadence: one check at every
// everyNBytes boundary starting at index 0, plus one before a non-match.
func TestContainsFuncCheckCadence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		valueLength int
		everyNBytes int
		wantChecks  int
	}{
		{valueLength: 0, everyNBytes: 1, wantChecks: 1},
		{valueLength: 1, everyNBytes: 1, wantChecks: 2},
		{valueLength: 8, everyNBytes: 1, wantChecks: 9},
		{valueLength: 8, everyNBytes: 2, wantChecks: 5},
		{valueLength: 9, everyNBytes: 2, wantChecks: 6},
		{valueLength: 8, everyNBytes: 4096, wantChecks: 2},
		{valueLength: 4096, everyNBytes: 4096, wantChecks: 2},
		{valueLength: 4097, everyNBytes: 4096, wantChecks: 3},
	}
	matcher := New("never-matches-anywhere")
	for _, test := range tests {
		checks := 0
		got, err := matcher.ContainsFunc(strings.Repeat("x", test.valueLength), test.everyNBytes, func() error {
			checks++
			return nil
		})
		if got || err != nil {
			t.Fatalf("ContainsFunc(%d bytes) = (%t, %v), want (false, nil)", test.valueLength, got, err)
		}
		if checks != test.wantChecks {
			t.Fatalf("ContainsFunc(%d bytes, every %d) made %d checks, want %d",
				test.valueLength, test.everyNBytes, checks, test.wantChecks)
		}
	}
}

// TestMatcherLongPatternMatchesNaiveReference differentially fuzzes long
// patterns over a tiny alphabet, where random long patterns actually recur in
// the value and exercise deep prefix-link walks.
func TestMatcherLongPatternMatchesNaiveReference(t *testing.T) {
	t.Parallel()

	random := rand.New(rand.NewSource(0x5eed))
	alphabet := []byte("aAbB\x00\x7f")
	randomString := func(length int) string {
		value := make([]byte, length)
		for index := range value {
			value[index] = alphabet[random.Intn(len(alphabet))]
		}
		return string(value)
	}
	for iteration := range 3_000 {
		value := randomString(random.Intn(512))
		pattern := randomString(1 + random.Intn(1200))
		if random.Intn(2) == 0 && len(value) > 0 {
			// Splice the pattern into the value so long matches actually occur.
			offset := random.Intn(len(value))
			value = value[:offset] + pattern + value[offset:]
		}
		matcher := New(pattern)
		want := strings.Contains(foldReference(value), foldReference(pattern))
		if got := matcher.Contains(value); got != want {
			t.Fatalf("iteration %d: Contains(len %d, pattern len %d) = %t, want %t",
				iteration, len(value), len(pattern), got, want)
		}
		got, err := matcher.ContainsFunc(value, 16, func() error { return nil })
		if err != nil {
			t.Fatalf("iteration %d: ContainsFunc error = %v", iteration, err)
		}
		if got != want {
			t.Fatalf("iteration %d: ContainsFunc disagreed with Contains", iteration)
		}
	}
}
