package asciifold

import (
	"context"
	"errors"
	"math/rand"
	"strings"
	"testing"
)

func TestMatcherContains(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   string
		pattern string
		want    bool
	}{
		{name: "empty", value: "anything", pattern: "", want: true},
		{name: "ascii folded prefix", value: "INDEX=main", pattern: "index=", want: true},
		{name: "ascii folded suffix", value: "index=main ERROR", pattern: "error", want: true},
		{name: "overlap", value: "aaaaab", pattern: "aaab", want: true},
		{name: "longer", value: "short", pattern: "longer", want: false},
		{name: "missing", value: "index=main", pattern: "needle", want: false},
		{name: "non ascii exact", value: "café ERROR", pattern: "fé error", want: true},
		{name: "non ascii is not folded", value: "CAFÉ", pattern: "café", want: false},
		{name: "folded pattern", value: "prefix index=main suffix", pattern: "INDEX=MAIN", want: true},
		{name: "non ascii value", value: "error=Café", pattern: "Café", want: true},
		{name: "non ascii pattern", value: "error=Café", pattern: "CAFÉ", want: false},
		{name: "long overlap", value: strings.Repeat("a", 4096) + "b", pattern: "aaaaab", want: true},
		{name: "long overlap missing", value: strings.Repeat("a", 4096), pattern: "aaaaab", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			matcher := New(test.pattern)
			if got := matcher.Contains(test.value); got != test.want {
				t.Fatalf("Contains(%q, %q) = %v, want %v", test.value, test.pattern, got, test.want)
			}
		})
	}
}

func TestNilMatcherContainsEverything(t *testing.T) {
	t.Parallel()

	var matcher *Matcher
	if !matcher.Contains("anything") {
		t.Fatal("nil matcher rejected a value")
	}
}

func TestMatcherMatchesNaiveReference(t *testing.T) {
	t.Parallel()

	random := rand.New(rand.NewSource(0xc0ffee))
	alphabet := []rune("abcXYZ012 _=-|éÉλ")
	randomString := func(maximum int) string {
		length := random.Intn(maximum + 1)
		value := make([]rune, length)
		for index := range value {
			value[index] = alphabet[random.Intn(len(alphabet))]
		}
		return string(value)
	}
	for iteration := range 10_000 {
		value := randomString(96)
		pattern := randomString(16)
		matcher := New(pattern)
		want := strings.Contains(foldReference(value), foldReference(pattern))
		if got := matcher.Contains(value); got != want {
			t.Fatalf("iteration %d: Contains(%q, %q) = %t, want %t", iteration, value, pattern, got, want)
		}
		matched, err := matcher.ContainsFunc(value, 4096, func() error { return nil })
		if err != nil {
			t.Fatalf("iteration %d: ContainsFunc error = %v", iteration, err)
		}
		if matched != want {
			t.Fatalf("iteration %d: ContainsFunc(%q, %q) = %t, want %t", iteration, value, pattern, matched, want)
		}
	}
}

func TestMatcherAdversarialFallback(t *testing.T) {
	t.Parallel()

	// Patterns are unbounded: 255 bytes is the admin text filter ceiling and
	// 8 KiB the longest scanned description.
	matcher := New("b" + strings.Repeat("a", 255-1))
	if matcher.Contains(strings.Repeat("a", 8<<10)) {
		t.Fatal("adversarial near-match was accepted")
	}
}

func TestContainsFuncReportsCheckError(t *testing.T) {
	t.Parallel()

	canceling := &cancelAfterChecksContext{after: 3}
	matcher := New("never-matches")
	if _, err := matcher.ContainsFunc(
		strings.Repeat("x", 32<<10),
		4096,
		canceling.Err,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-value cancellation error = %v, want context.Canceled", err)
	}
}

func TestContainsFuncChecksBeforeReturningNonMatch(t *testing.T) {
	t.Parallel()

	canceling := &cancelAfterChecksContext{after: 1}
	matcher := New("never-matches")
	if _, err := matcher.ContainsFunc("", 4096, canceling.Err); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-return cancellation error = %v, want context.Canceled", err)
	}
}

func TestContainsFuncEmptyPatternSkipsCheck(t *testing.T) {
	t.Parallel()

	matched, err := matcher0().ContainsFunc("anything", 4096, func() error {
		return context.Canceled
	})
	if err != nil || !matched {
		t.Fatalf("empty pattern ContainsFunc = (%t, %v), want (true, nil)", matched, err)
	}
}

func matcher0() *Matcher {
	matcher := New("")
	return &matcher
}

func foldReference(value string) string {
	folded := []byte(value)
	for index, character := range folded {
		if character >= 'A' && character <= 'Z' {
			folded[index] = character + ('a' - 'A')
		}
	}
	return string(folded)
}

type cancelAfterChecksContext struct {
	checks int
	after  int
}

func (canceling *cancelAfterChecksContext) Err() error {
	canceling.checks++
	if canceling.checks >= canceling.after {
		return context.Canceled
	}
	return nil
}
