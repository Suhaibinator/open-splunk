package splregex

import (
	"errors"
	"regexp"
	"strings"
	"testing"
)

const routePattern = `/(\d+|[0-9a-fA-F-]{36}|[0-9a-fA-F]{24,})(?=/|$)`

func TestCompileReplacePathPattern(t *testing.T) {
	t.Parallel()
	compiled, err := CompileReplacePattern(routePattern)
	if err != nil {
		t.Fatal(err)
	}
	if !compiled.PathSegment || compiled.ProgramWorkUnits == 0 {
		t.Fatalf("descriptor = %#v", compiled)
	}
	program := regexp.MustCompile(compiled.Pattern)
	for _, tc := range []struct{ input, want string }{
		{"/123", "123"}, {"/123x", ""}, {"123", ""}, {"/123/456", ""}, {"/123\n", ""},
		{"/abcdefabcdefabcdefabcdef", "abcdefabcdefabcdefabcdef"},
		{"/12345678-1234-1234-1234-123456789abc", "12345678-1234-1234-1234-123456789abc"},
	} {
		captures := program.FindStringSubmatch(tc.input)
		if tc.want == "" {
			if captures != nil {
				t.Errorf("unexpected match %q: %q", tc.input, captures)
			}
			continue
		}
		if len(captures) != 2 || captures[1] != tc.want {
			t.Errorf("captures for %q = %q", tc.input, captures)
		}
	}
	for _, pattern := range []string{`/(a|ab)(?=/|$)`, `/([[:digit:]]+)(?=/|$)`, `/([^/\n]+)(?=/|$)`, `/(\Q(?=\E)(?=/|$)`} {
		if _, err := CompileReplacePattern(pattern); err != nil {
			t.Errorf("%q: %v", pattern, err)
		}
	}
}

func TestReplacePatternDiagnostics(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ pattern, code string }{
		{`(?=secret)`, "SPL_UNSUPPORTED_PCRE"}, {`a(?!b)`, "SPL_UNSUPPORTED_PCRE"},
		{`(?<=a)b`, "SPL_UNSUPPORTED_PCRE"}, {`(.)\1`, "SPL_UNSUPPORTED_PCRE"},
		{`/(.*)(?=/|$)`, "SPL_UNSUPPORTED_PCRE"}, {`/([^/]+)(?=/|$)`, "SPL_UNSUPPORTED_PCRE"},
		{`/(a*)(?=/|$)`, "SPL_UNSUPPORTED_PCRE"}, {`/a|b(?=/|$)`, "SPL_UNSUPPORTED_PCRE"},
		{`/(?i:a)(?=/|$)`, "SPL_UNSUPPORTED_PCRE"}, {`/(^a)(?=/|$)`, "SPL_UNSUPPORTED_PCRE"},
		{`/(a/b)(?=/|$)`, "SPL_UNSUPPORTED_PCRE"}, {`/(a)(?=/|$)x`, "SPL_UNSUPPORTED_PCRE"},
		{`[`, "SPL_UNSUPPORTED_REGEX"}, {`a*`, "SPL_UNSUPPORTED_REGEX"}, {``, "SPL_UNSUPPORTED_REGEX"},
		{"/" + strings.Repeat("x", MaximumReplacePathPatternBytes) + "(?=/|$)", "SPL_QUERY_TOO_COMPLEX"},
		{`/(a{1000}b{1000}c{1000}d{1000}e{1000})(?=/|$)`, "SPL_QUERY_TOO_COMPLEX"},
	} {
		_, err := CompileReplacePattern(tc.pattern)
		if err == nil {
			t.Errorf("accepted %q", tc.pattern)
			continue
		}
		code, _ := ReplacePatternDiagnostic(err)
		if code != tc.code {
			t.Errorf("%q: %s (%v), want %s", tc.pattern, code, err, tc.code)
		}
	}
	for _, pattern := range []string{`\(\?=secret\)`, `[(?=]+`, `\Q(?=literal)\E`, `a(?i)b`} {
		compiled, err := CompileReplacePattern(pattern)
		if err != nil || compiled.PathSegment || compiled.Pattern != pattern {
			t.Errorf("ordinary %q: %#v, %v", pattern, compiled, err)
		}
	}
	if _, err := CompileReplacePattern(`a*`); !errors.Is(err, ErrMayMatchEmpty) {
		t.Fatal(err)
	}
}

func TestReplacePathPatternBounds(t *testing.T) {
	t.Parallel()
	// Literal normalization has a fixed wrapper; find and exercise its exact
	// byte boundary, including normalized growth, without expanding repetitions.
	largest := 0
	for n := MaximumReplacePathPatternBytes - 32; n <= MaximumReplacePathPatternBytes; n++ {
		_, err := CompileReplacePattern("/" + strings.Repeat("a", n) + "(?=/|$)")
		if err == nil {
			largest = n
		} else if !errors.Is(err, ErrReplacePathTooComplex) {
			t.Fatal(err)
		}
	}
	if largest == 0 {
		t.Fatal("no near-boundary pattern admitted")
	}
	for _, tc := range []struct {
		n      int
		reject bool
	}{{largest, false}, {largest + 1, true}} {
		_, err := CompileReplacePattern("/" + strings.Repeat("a", tc.n) + "(?=/|$)")
		if (err != nil) != tc.reject {
			t.Fatalf("boundary %d: %v", tc.n, err)
		}
	}
}

func TestReplacePathCharacterClassSyntax(t *testing.T) {
	t.Parallel()
	for _, pattern := range []string{
		`/[]|]+(?=/|$)`,
		`/[]()]+(?=/|$)`,
		`/[^]/\n]+(?=/|$)`,
		`/[\]|]+(?=/|$)`,
		`/[[:digit:]|()]+(?=/|$)`,
		`/(?P<id>[0-9]+)(?=/|$)`,
		`/(?<id>[0-9]+)(?=/|$)`,
	} {
		compiled, err := CompileReplacePattern(pattern)
		if err != nil || !compiled.PathSegment {
			t.Errorf("valid path %q: %#v, %v", pattern, compiled, err)
		}
	}
	for _, pattern := range []string{
		`/[](]a|b[])](?=/|$)`,
		`/[[:digit:](]a|b[[:digit:])](?=/|$)`,
	} {
		if _, err := CompileReplacePattern(pattern); !errors.Is(err, ErrUnsupportedReplacePCRE) {
			t.Errorf("top-level alternative %q: %v", pattern, err)
		}
	}
	for _, pattern := range []string{`[](?=]+`, `[^](?=]+`, `[\](?=]+`, `[[:digit:](?=]+`} {
		if hasReplacePCRE(pattern) {
			t.Errorf("literal assertion characters treated as PCRE: %q", pattern)
		}
	}
}

func TestReplacePathMalformedBodyDiagnostics(t *testing.T) {
	t.Parallel()
	for _, pattern := range []string{`/((a)(?=/|$)`, `/[abc(?=/|$)`, `/a{2,1}(?=/|$)`} {
		_, err := CompileReplacePattern(pattern)
		code, message := ReplacePatternDiagnostic(err)
		if err == nil || code != "SPL_UNSUPPORTED_REGEX" || !strings.Contains(message, "invalid syntax") {
			t.Errorf("malformed %q: %s, %s, %v", pattern, code, message, err)
		}
	}
	for _, pattern := range []string{`/a(?=b)(?=/|$)`, `/(a)\1(?=/|$)`, `/(?i:a)(?=/|$)`} {
		_, err := CompileReplacePattern(pattern)
		if !errors.Is(err, ErrUnsupportedReplacePCRE) {
			t.Errorf("unsupported body %q: %v", pattern, err)
		}
	}
}

func TestReplacePossessiveDiagnostics(t *testing.T) {
	t.Parallel()
	for _, body := range []string{`a++`, `a*+`, `a?+`, `a{2}+`, `a{2,}+`, `a{2,3}+`, `\(?+`} {
		for _, pattern := range []string{body, "/" + body + "(?=/|$)"} {
			_, err := CompileReplacePattern(pattern)
			code, _ := ReplacePatternDiagnostic(err)
			if err == nil || code != "SPL_UNSUPPORTED_PCRE" {
				t.Errorf("possessive %q: %s, %v", pattern, code, err)
			}
		}
	}
	for _, pattern := range []string{`\Q++\E`, `\x{0061}+`, strings.Repeat("a", MaximumReplacePathPatternBytes) + `\x{0061}+`, `[+?*{}]+`, `\*+`, `\++`, `\?+`, `\{2\}+`, `a{,2}+`, `a{2,x}+`} {
		compiled, err := CompileReplacePattern(pattern)
		if err != nil || compiled.PathSegment || compiled.Pattern != pattern || hasReplacePCRE(pattern) {
			t.Errorf("ordinary pattern %q: %#v, %v", pattern, compiled, err)
		}
	}
	if hasReplacePCRE(`(?+`) {
		t.Error("group prefix incorrectly recognized as possessive quantifier")
	}
}

func TestReplaceMalformedTokenRuns(t *testing.T) {
	t.Parallel()
	for _, pattern := range []string{
		"[" + strings.Repeat("[:", 10000),
		strings.Repeat(`\x{`, 10000),
	} {
		_, err := CompileReplacePattern(pattern)
		code, message := ReplacePatternDiagnostic(err)
		if err == nil || code != "SPL_UNSUPPORTED_REGEX" || !strings.Contains(message, "invalid syntax") {
			t.Fatalf("malformed token run: %s, %s, %v", code, message, err)
		}
	}
}
