package spl

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/splregex"
)

func TestParseReplacePath(t *testing.T) {
	t.Parallel()
	const source = `index=gradethis msg="Request summary statistics" | eval route=replace(path, "/(\d+|[0-9a-fA-F-]{36}|[0-9a-fA-F]{24,})(?=/|$)", "/:id")`
	if _, err := Parse(source); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ pattern, code, text string }{
		{`(?=secret)`, "SPL_UNSUPPORTED_PCRE", "remains unsupported"},
		{`a*`, "SPL_UNSUPPORTED_REGEX", "empty substring"},
		{`[`, "SPL_UNSUPPORTED_REGEX", "invalid syntax"},
	} {
		query := `index=main | eval x=replace(path, "` + tc.pattern + `", "x")`
		_, err := Parse(query)
		var diagnostic *Diagnostic
		if !errors.As(err, &diagnostic) || diagnostic.Code != tc.code || !strings.Contains(diagnostic.Message, tc.text) {
			t.Fatalf("%s: %v", tc.pattern, err)
		}
		if got := query[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset]; got != `"`+tc.pattern+`"` {
			t.Fatalf("range = %q", got)
		}
	}
}

func TestParseReplacePathQueryProgramBudget(t *testing.T) {
	t.Parallel()
	const pattern = `/(a{1000})(?=/|$)`
	compiled, err := splregex.CompileReplacePattern(pattern)
	if err != nil {
		t.Fatal(err)
	}
	calls := splregex.MaximumReplacePathQueryProgramWorkUnits / compiled.ProgramWorkUnits
	source := "index=main"
	for i := 0; i <= calls; i++ {
		source += fmt.Sprintf(` | eval x%d=replace(path, "%s", "x")`, i, pattern)
		_, err := Parse(source)
		if i < calls && err != nil {
			t.Fatal(err)
		}
		if i == calls {
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
				t.Fatalf("query budget: %v", err)
			}
		}
	}
}
