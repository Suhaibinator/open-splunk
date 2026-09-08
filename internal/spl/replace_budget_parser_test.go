package spl

import (
	"errors"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/splregex"
)

func TestParseReplaceRejectsPatternResourceLimitsAtLiteral(t *testing.T) {
	t.Parallel()

	for _, pattern := range []string{
		`(?:ab|cd){900}`,
		`(abc){900}`,
		strings.Repeat("x", splregex.MaximumMatchPatternBytes+1),
	} {
		for _, expression := range []string{
			`replace(message, "` + pattern + `", "")`,
			`lower(RePlAcE(message, "` + pattern + `", ""))`,
		} {
			source := `index=main | eval value=` + expression
			_, err := Parse(source)
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
				t.Fatalf("Parse error = %v, want SPL_QUERY_TOO_COMPLEX", err)
			}
			if got := source[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset]; got != `"`+pattern+`"` {
				t.Fatalf("diagnostic points to %q, want pattern literal", got)
			}
		}
	}
}
