package splregex

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateReplacePattern(t *testing.T) {
	t.Parallel()

	for _, pattern := range []string{`ms$`, `^(\d{1,2})/(\d{1,2})/`, `a+`, `[^/]+`} {
		if err := ValidateReplacePattern(pattern); err != nil {
			t.Errorf("ValidateReplacePattern(%q): %v", pattern, err)
		}
	}
	for _, pattern := range []string{"", `a*`, `a?`, `^`, `\b`, `(?:x|)`} {
		if err := ValidateReplacePattern(pattern); !errors.Is(err, ErrMayMatchEmpty) {
			t.Errorf("ValidateReplacePattern(%q) = %v, want ErrMayMatchEmpty", pattern, err)
		}
	}
	if err := ValidateReplacePattern(`(?=secret)`); err == nil || errors.Is(err, ErrMayMatchEmpty) {
		t.Fatalf("invalid lookahead error = %v", err)
	}
}

func TestCompileReplacePatternPreservesAuthoredPatternAndBoundsWork(t *testing.T) {
	t.Parallel()

	for _, pattern := range []string{
		`ms$`, `^(\d{1,2})/(\d{1,2})/`, `(?ims)^(word).+$`,
		`\Q(a){12}\E`, `(?:ab|cd){8}`, `(?P<word>\w+)`,
		strings.Repeat("a{1000}", 4) + "a{94}",
	} {
		compiled, err := CompileReplacePattern(pattern)
		if err != nil {
			t.Fatalf("CompileReplacePattern(%q): %v", pattern, err)
		}
		if compiled.Pattern != pattern || compiled.ProgramWorkUnits <= 0 ||
			compiled.ProgramWorkUnits > MaximumMatchProgramWorkUnits {
			t.Fatalf("CompileReplacePattern(%q) = %#v", pattern, compiled)
		}
	}
}

func TestCompileReplacePatternRejectsExpandedProgramsAndOversizedText(t *testing.T) {
	t.Parallel()

	for _, pattern := range []string{
		strings.Repeat("a{1000}", 4) + "a{95}",
		`(?:ab|cd){900}`,
		`(abc){900}`,
		`(?:` + strings.Repeat("a", 40) + `b{10}){100}`,
		strings.Repeat("x", MaximumMatchPatternBytes+1),
		strings.Repeat("x", MaximumMatchPatternBytes-len("(?-s)")+1),
	} {
		compiled, err := CompileReplacePattern(pattern)
		if !errors.Is(err, ErrReplacePatternTooLarge) || compiled != (ReplacePattern{}) {
			t.Fatalf("CompileReplacePattern(%d bytes) = %#v, %v", len(pattern), compiled, err)
		}
		if err := ValidateReplacePattern(pattern); !errors.Is(err, ErrReplacePatternTooLarge) {
			t.Fatalf("ValidateReplacePattern(%d bytes) = %v", len(pattern), err)
		}
	}
	for _, pattern := range []string{"a\x00", "a\xff", `(?=a)`, `a*`} {
		if _, err := CompileReplacePattern(pattern); err == nil || errors.Is(err, ErrReplacePatternTooLarge) {
			t.Fatalf("invalid/empty-match pattern returned %v", err)
		}
	}
}
