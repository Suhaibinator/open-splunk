package spl

import (
	"errors"
	"testing"
)

func TestParseEventStatsAverageRejectsDistinctUnsupportedShapes(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		source    string
		code      string
		rangeText string
	}{
		{"eval input", `index=main | eventstats avg(eval(duration_ms)) AS mean`, "SPL_EXPECTED_RIGHT_PAREN", "("},
		{"wildcard input", `index=main | eventstats avg(*) AS mean`, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "*"},
		{"multiple inputs", `index=main | eventstats avg(duration_ms,other) AS mean`, "SPL_EXPECTED_RIGHT_PAREN", ","},
		{"second measure", `index=main | eventstats avg(duration_ms) AS mean count`, "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX", "count"},
		{"average spelling", `index=main | eventstats average(duration_ms) AS mean`, "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE", "average"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := Parse(test.source)
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) {
				t.Fatalf("Parse error = %#v, want *Diagnostic", err)
			}
			if diagnostic.Code != test.code {
				t.Fatalf("code = %q, want %q (diagnostic: %v)", diagnostic.Code, test.code, diagnostic)
			}
			assertSourceRangeText(t, test.source, diagnostic.Range, test.rangeText)
		})
	}
}
