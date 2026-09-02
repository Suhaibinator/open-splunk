package splwildcard

import (
	"errors"
	"strings"
	"testing"
)

// likeFuzzProbes are subjects both the authored and the normalized pattern
// are matched against with a reference LIKE matcher. Subjects derived from
// the pattern itself are added per input so every accepted pattern is also
// checked on strings it must match.
var likeFuzzProbes = []string{
	"",
	"api",
	"api-gateway",
	"%",
	"_",
	`\`,
	`a\%b`,
	`\q`,
	"a\nb",
	"名前¥",
	"aaaaaaaaaaaaaaaaaaaaaaaa",
}

func FuzzCompileLikePattern(f *testing.F) {
	for _, seed := range []string{
		"",
		"api",
		"%api%",
		"%%%api%%",
		"a_c",
		`\%\_\\`,
		`\q%`,
		`%\%%`,
		`\`,
		`a\`,
		"_¥",
		"%%",
		"%_%_%",
		"bad\x00pattern",
		"\xff",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, pattern string) {
		first, firstErr := CompileLikePattern(pattern)
		second, secondErr := CompileLikePattern(pattern)
		if (firstErr == nil) != (secondErr == nil) ||
			(firstErr != nil && firstErr.Error() != secondErr.Error()) ||
			first != second {
			t.Fatalf("CompileLikePattern changed across identical input: first=%#v/%v second=%#v/%v",
				first, firstErr, second, secondErr)
		}
		if firstErr != nil {
			if first != (LikePattern{}) {
				t.Fatalf("error published a partial pattern: %#v", first)
			}
			invalid := errors.Is(firstErr, ErrInvalidLikePattern)
			tooLarge := errors.Is(firstErr, ErrLikePatternTooLarge)
			if invalid == tooLarge {
				t.Fatalf("error %v must match exactly one published sentinel", firstErr)
			}
			if IsLikeComplexityError(firstErr) != tooLarge {
				t.Fatalf("IsLikeComplexityError(%v) = %t, want %t", firstErr, !tooLarge, tooLarge)
			}
			return
		}

		if len(first.Pattern) > MaximumLikePatternBytes || len(first.Pattern) > len(pattern) {
			t.Fatalf("normalized %q (%d bytes) is longer than the %d-byte authored pattern or the %d-byte limit",
				first.Pattern, len(first.Pattern), len(pattern), MaximumLikePatternBytes)
		}
		if first.WorkUnits < 1 || first.WorkUnits > MaximumLikePatternWorkUnits {
			t.Fatalf("work units %d outside [1, %d]", first.WorkUnits, MaximumLikePatternWorkUnits)
		}
		authoredTokens, ok := likeFuzzTokenize(pattern)
		if !ok {
			t.Fatalf("accepted pattern %q has a dangling escape", pattern)
		}
		normalizedTokens, ok := likeFuzzTokenize(first.Pattern)
		if !ok {
			t.Fatalf("normalized pattern %q has a dangling escape", first.Pattern)
		}
		if got, want := likeFuzzWorkUnits(normalizedTokens), first.WorkUnits; got != want {
			t.Fatalf("normalized %q charges %d work units, metadata says %d", first.Pattern, got, want)
		}
		for index := 1; index < len(normalizedTokens); index++ {
			if normalizedTokens[index].kind == likeFuzzAnySequence &&
				normalizedTokens[index-1].kind == likeFuzzAnySequence {
				t.Fatalf("normalized %q still carries adjacent %% wildcards", first.Pattern)
			}
		}

		// Normalization is a fixed point on the token stream and must not
		// change the language: the authored and normalized patterns accept
		// exactly the same subjects, including ones built from the pattern.
		again, err := CompileLikePattern(first.Pattern)
		if err != nil {
			t.Fatalf("normalized pattern %q does not recompile: %v", first.Pattern, err)
		}
		if again != first {
			t.Fatalf("normalization is not idempotent:\nfirst:  %#v\nsecond: %#v", first, again)
		}
		probes := append(likeFuzzSubjects(authoredTokens), likeFuzzProbes...)
		for _, probe := range probes {
			want := likeFuzzMatch(authoredTokens, []rune(probe))
			got := likeFuzzMatch(normalizedTokens, []rune(probe))
			if got != want {
				t.Fatalf("normalized %q and authored %q disagree on %q: got %t want %t",
					first.Pattern, pattern, probe, got, want)
			}
		}
		for _, probe := range likeFuzzSubjects(authoredTokens) {
			if !likeFuzzMatch(authoredTokens, []rune(probe)) {
				t.Fatalf("pattern %q rejects %q derived from itself", pattern, probe)
			}
		}
	})
}

type likeFuzzTokenKind uint8

const (
	likeFuzzLiteral likeFuzzTokenKind = iota + 1
	likeFuzzAnyOne
	likeFuzzAnySequence
)

type likeFuzzToken struct {
	kind likeFuzzTokenKind
	rune rune
}

// likeFuzzTokenize applies ClickHouse's LIKE escape rules independently of
// the compiler: a backslash escapes %, _, and itself, and before any other
// rune it is an ordinary literal backslash.
func likeFuzzTokenize(pattern string) ([]likeFuzzToken, bool) {
	tokens := make([]likeFuzzToken, 0, len(pattern))
	runes := []rune(pattern)
	for index := 0; index < len(runes); index++ {
		current := runes[index]
		switch current {
		case '\\':
			if index+1 >= len(runes) {
				return nil, false
			}
			next := runes[index+1]
			if next != '%' && next != '_' && next != '\\' {
				tokens = append(tokens, likeFuzzToken{kind: likeFuzzLiteral, rune: '\\'})
			}
			tokens = append(tokens, likeFuzzToken{kind: likeFuzzLiteral, rune: next})
			index++
		case '%':
			tokens = append(tokens, likeFuzzToken{kind: likeFuzzAnySequence})
		case '_':
			tokens = append(tokens, likeFuzzToken{kind: likeFuzzAnyOne})
		default:
			tokens = append(tokens, likeFuzzToken{kind: likeFuzzLiteral, rune: current})
		}
	}
	return tokens, true
}

func likeFuzzWorkUnits(tokens []likeFuzzToken) int {
	if len(tokens) == 0 {
		return 1
	}
	return len(tokens)
}

// likeFuzzSubjects builds subjects the pattern must accept: every wildcard
// is instantiated with the shortest and with a longer witness.
func likeFuzzSubjects(tokens []likeFuzzToken) []string {
	var shortest, longer strings.Builder
	for _, token := range tokens {
		switch token.kind {
		case likeFuzzLiteral:
			shortest.WriteRune(token.rune)
			longer.WriteRune(token.rune)
		case likeFuzzAnyOne:
			shortest.WriteRune('x')
			longer.WriteRune('¥')
		case likeFuzzAnySequence:
			longer.WriteString("zz\n%")
		}
	}
	return []string{shortest.String(), longer.String()}
}

// likeFuzzMatch is a reference LIKE matcher over runes with the usual
// greedy-with-backtracking wildcard semantics.
func likeFuzzMatch(tokens []likeFuzzToken, subject []rune) bool {
	tokenIndex, subjectIndex := 0, 0
	starToken, starSubject := -1, 0
	for subjectIndex < len(subject) {
		if tokenIndex < len(tokens) {
			switch token := tokens[tokenIndex]; token.kind {
			case likeFuzzAnySequence:
				starToken = tokenIndex
				starSubject = subjectIndex
				tokenIndex++
				continue
			case likeFuzzAnyOne:
				tokenIndex++
				subjectIndex++
				continue
			case likeFuzzLiteral:
				if token.rune == subject[subjectIndex] {
					tokenIndex++
					subjectIndex++
					continue
				}
			}
		}
		if starToken < 0 {
			return false
		}
		starSubject++
		tokenIndex = starToken + 1
		subjectIndex = starSubject
	}
	for tokenIndex < len(tokens) && tokens[tokenIndex].kind == likeFuzzAnySequence {
		tokenIndex++
	}
	return tokenIndex == len(tokens)
}

func TestLikeFuzzReferenceMatcher(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		pattern string
		subject string
		want    bool
	}{
		{pattern: "", subject: "", want: true},
		{pattern: "", subject: "a", want: false},
		{pattern: "%", subject: "", want: true},
		{pattern: "%", subject: "anything\n", want: true},
		{pattern: "a_c", subject: "abc", want: true},
		{pattern: "a_c", subject: "ac", want: false},
		{pattern: "a_c", subject: "a¥c", want: true},
		{pattern: "%api%", subject: "the api gateway", want: true},
		{pattern: "%api%", subject: "the ap gateway", want: false},
		{pattern: "a%b%c", subject: "axxbxxc", want: true},
		{pattern: "a%b%c", subject: "axxcxxb", want: false},
		{pattern: `\%`, subject: "%", want: true},
		{pattern: `\%`, subject: "a", want: false},
		{pattern: `\_`, subject: "_", want: true},
		{pattern: `\_`, subject: "a", want: false},
		{pattern: `\\`, subject: `\`, want: true},
		{pattern: `\q`, subject: `\q`, want: true},
		{pattern: `\q`, subject: `q`, want: false},
		{pattern: "%%a", subject: "xxa", want: true},
		{pattern: "a%%", subject: "a", want: true},
	} {
		tokens, ok := likeFuzzTokenize(test.pattern)
		if !ok {
			t.Fatalf("tokenize %q failed", test.pattern)
		}
		if got := likeFuzzMatch(tokens, []rune(test.subject)); got != test.want {
			t.Errorf("match(%q, %q) = %t, want %t", test.pattern, test.subject, got, test.want)
		}
	}
	if _, ok := likeFuzzTokenize(`a\`); ok {
		t.Fatal("tokenize accepted a dangling escape")
	}
}
