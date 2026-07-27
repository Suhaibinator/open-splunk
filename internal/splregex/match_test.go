package splregex

import (
	"errors"
	"strings"
	"testing"
)

func TestCompileMatchPatternAcceptsZeroWidthAndPinsDotAllOff(t *testing.T) {
	t.Parallel()

	for _, pattern := range []string{"", "^$", "a*", `(?i)error|warn`, `\bapi\b`} {
		normalized, err := CompileMatchPattern(pattern)
		if err != nil {
			t.Errorf("CompileMatchPattern(%q): %v", pattern, err)
			continue
		}
		if !strings.HasPrefix(normalized, "(?-s)") {
			t.Errorf("CompileMatchPattern(%q) = %q, want explicit dot-all disable", pattern, normalized)
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
