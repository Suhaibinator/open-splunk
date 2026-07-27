package splregex

import (
	"errors"
	"regexp"
	"strings"
	"testing"
)

func TestCompileMatchPatternAcceptsZeroWidthAndPinsDotAllOff(t *testing.T) {
	t.Parallel()

	for _, pattern := range []string{"", "^$", "a*", `(?i)error|warn`, `\bapi\b`} {
		compiled, err := CompileMatchPattern(pattern)
		if err != nil {
			t.Errorf("CompileMatchPattern(%q): %v", pattern, err)
			continue
		}
		if !strings.HasPrefix(compiled.Pattern, "(?-s)") {
			t.Errorf("CompileMatchPattern(%q) = %q, want explicit dot-all disable", pattern, compiled.Pattern)
		}
		if compiled.ProgramWorkUnits < 2 ||
			compiled.ProgramWorkUnits > MaximumMatchProgramWorkUnits {
			t.Errorf(
				"CompileMatchPattern(%q) work = %d, want bounded positive work",
				pattern,
				compiled.ProgramWorkUnits,
			)
		}
	}
}

func TestCompileMatchPatternRejectsUnsupportedAndUnboundedPatterns(t *testing.T) {
	t.Parallel()

	for _, pattern := range []string{`(?=secret)`, `(.)\1`, "[", string([]byte{0xff}), "x\x00y"} {
		if _, err := CompileMatchPattern(pattern); !errors.Is(err, ErrInvalidMatchPattern) {
			t.Errorf("CompileMatchPattern(%q) = %v, want ErrInvalidMatchPattern", pattern, err)
		}
	}
	for _, pattern := range []string{
		strings.Repeat("x", MaximumMatchPatternBytes+1),
		strings.Repeat("x", MaximumMatchPatternBytes-len("(?-s)")+1),
	} {
		if _, err := CompileMatchPattern(pattern); !errors.Is(err, ErrMatchPatternTooLarge) {
			t.Errorf("CompileMatchPattern(%d bytes) = %v, want ErrMatchPatternTooLarge", len(pattern), err)
		}
	}
}

func TestCompileMatchPatternRejectsCountedRepeatProgramBombs(t *testing.T) {
	t.Parallel()

	for _, pattern := range []string{
		strings.Repeat("a{1000}", 5),
		strings.Repeat("(a){1000}", 5),
	} {
		if len(pattern) > MaximumMatchPatternBytes {
			t.Fatalf("adversarial pattern unexpectedly exceeds text bound: %d", len(pattern))
		}
		if _, err := CompileMatchPattern(pattern); !errors.Is(err, ErrMatchPatternTooLarge) {
			t.Errorf(
				"CompileMatchPattern(%q) = %v, want ErrMatchPatternTooLarge",
				pattern,
				err,
			)
		}
	}
}

func TestCompileMatchPatternPreservesDollarAndStrictEndSemantics(t *testing.T) {
	t.Parallel()

	dollar, err := CompileMatchPattern(`ERROR$`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(dollar.Pattern, `\n`) ||
		!strings.Contains(dollar.Pattern, `\z`) {
		t.Fatalf("dollar normalization = %q, want optional final newline plus strict end", dollar.Pattern)
	}
	if !regexp.MustCompile(dollar.Pattern).MatchString("ERROR\n") {
		t.Fatalf("dollar normalization %q does not match before a final newline", dollar.Pattern)
	}
	strict, err := CompileMatchPattern(`ERROR\z`)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strict.Pattern, `\n`) {
		t.Fatalf("strict-end normalization = %q, want no final-newline alternative", strict.Pattern)
	}
	if regexp.MustCompile(strict.Pattern).MatchString("ERROR\n") {
		t.Fatalf("strict-end normalization %q matched before a final newline", strict.Pattern)
	}
	multiline, err := CompileMatchPattern(`(?m)ERROR$`)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(multiline.Pattern, `\z`) {
		t.Fatalf("multiline-dollar normalization = %q, want line-end assertion", multiline.Pattern)
	}
	if !regexp.MustCompile(multiline.Pattern).MatchString("ERROR\nnext") {
		t.Fatalf("multiline-dollar normalization %q did not match line end", multiline.Pattern)
	}
}
