package splregex

import (
	"errors"
	"regexp"
	"strings"
	"testing"
)

func TestCompileExtractionPatternReturnsOrderedNamedCaptureIndexes(t *testing.T) {
	t.Parallel()

	compiled, err := CompileExtractionPattern(
		`^(?:request=)([A-Z]+)\s+(?<method>[A-Z]+)\s+(?P<path>\S+)$`,
	)
	if err != nil {
		t.Fatalf("CompileExtractionPattern: %v", err)
	}
	want := []NamedCapture{
		{Name: "method", Group: 2},
		{Name: "path", Group: 3},
	}
	if len(compiled.Captures) != len(want) {
		t.Fatalf("captures = %#v, want %#v", compiled.Captures, want)
	}
	for index := range want {
		if compiled.Captures[index] != want[index] {
			t.Fatalf("capture %d = %#v, want %#v", index, compiled.Captures[index], want[index])
		}
	}
	if compiled.GroupCount != 3 {
		t.Fatalf("group count = %d, want 3", compiled.GroupCount)
	}
	if !strings.HasPrefix(compiled.Pattern, "(?-s)") {
		t.Fatalf("normalized pattern = %q, want explicit non-dotall prefix", compiled.Pattern)
	}
	if _, err := regexp.Compile(compiled.Pattern); err != nil {
		t.Fatalf("normalized pattern is not valid RE2: %v", err)
	}
}

func TestCompileExtractionPatternValidatesDialectAndCaptureContract(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		want    error
	}{
		{name: "no named capture", pattern: `status=(\d+)`, want: ErrNoNamedCapture},
		{name: "duplicate named capture", pattern: `(?<value>x)|(?P<value>y)`, want: ErrDuplicateNamedCapture},
		{name: "lookahead", pattern: `(?<value>x)(?=y)`, want: ErrInvalidExtractionPattern},
		{name: "backreference", pattern: `(?<value>x)\1`, want: ErrInvalidExtractionPattern},
		{name: "nul", pattern: "(?<value>x)\x00", want: ErrInvalidExtractionPattern},
		{name: "invalid utf8", pattern: "(?<value>" + string([]byte{0xff}) + ")", want: ErrInvalidExtractionPattern},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := CompileExtractionPattern(test.pattern)
			if !errors.Is(err, test.want) {
				t.Fatalf("CompileExtractionPattern(%q) error = %v, want %v", test.pattern, err, test.want)
			}
		})
	}
}

func TestCompileExtractionPatternBoundsPatternAndCaptureWork(t *testing.T) {
	t.Parallel()

	tooLong := `(?<value>` + strings.Repeat("x", MaximumExtractionPatternBytes) + `)`
	if _, err := CompileExtractionPattern(tooLong); !errors.Is(err, ErrExtractionPatternTooLarge) {
		t.Fatalf("long pattern error = %v, want ErrExtractionPatternTooLarge", err)
	}

	var captures strings.Builder
	for index := 0; index <= MaximumExtractionCaptureGroups; index++ {
		captures.WriteString("(?<f")
		captures.WriteString(string(rune('a' + index)))
		captures.WriteString(">x)")
	}
	if _, err := CompileExtractionPattern(captures.String()); !errors.Is(err, ErrTooManyExtractionCaptures) {
		t.Fatalf("capture-limit error = %v, want ErrTooManyExtractionCaptures", err)
	}
}

func TestCompileExtractionPatternAllowsEmptyWholeMatches(t *testing.T) {
	t.Parallel()

	compiled, err := CompileExtractionPattern(`(?<empty>^$)`)
	if err != nil {
		t.Fatalf("CompileExtractionPattern: %v", err)
	}
	match := regexp.MustCompile(compiled.Pattern).FindStringSubmatch("")
	if len(match) != 2 || match[1] != "" {
		t.Fatalf("empty match = %#v, want one participating empty capture", match)
	}
}
