package splwildcard

import (
	"errors"
	"strings"
	"testing"
)

func TestCompileLikePatternPreservesEscapesAndCollapsesPercentRuns(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		pattern string
		want    string
		work    int
	}{
		{pattern: "", want: "", work: 1},
		{pattern: "api", want: "api", work: 3},
		{pattern: "%%%api%%", want: "%api%", work: 5},
		{pattern: `\%\_\\`, want: `\%\_\\`, work: 3},
		{pattern: `\q%`, want: `\q%`, work: 3},
		{pattern: "_¥", want: "_¥", work: 2},
	} {
		test := test
		t.Run(test.pattern, func(t *testing.T) {
			t.Parallel()
			compiled, err := CompileLikePattern(test.pattern)
			if err != nil {
				t.Fatalf("CompileLikePattern(%q): %v", test.pattern, err)
			}
			if compiled.Pattern != test.want || compiled.WorkUnits != test.work {
				t.Fatalf(
					"CompileLikePattern(%q) = %#v, want pattern %q work %d",
					test.pattern,
					compiled,
					test.want,
					test.work,
				)
			}
		})
	}
}

func TestCompileLikePatternRejectsInvalidAndOversizedText(t *testing.T) {
	t.Parallel()

	for _, pattern := range []string{"bad\x00pattern", "bad\xffpattern", "bad\\"} {
		if _, err := CompileLikePattern(pattern); !errors.Is(err, ErrInvalidLikePattern) {
			t.Errorf(
				"CompileLikePattern(%q) = %v, want ErrInvalidLikePattern",
				pattern,
				err,
			)
		}
	}
	if _, err := CompileLikePattern(
		strings.Repeat("x", MaximumLikePatternBytes+1),
	); !errors.Is(err, ErrLikePatternTooLarge) {
		t.Fatalf("oversized pattern error = %v, want ErrLikePatternTooLarge", err)
	}
}
