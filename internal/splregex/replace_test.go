package splregex

import (
	"errors"
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
