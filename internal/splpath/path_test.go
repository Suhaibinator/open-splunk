package splpath

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestParseJSONPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want []Step
	}{
		{
			name: "object path",
			path: "vendorProductSet.product.desc",
			want: []Step{{Key: "vendorProductSet"}, {Key: "product"}, {Key: "desc"}},
		},
		{
			name: "zero based array indexes",
			path: "outer{0}.items{9}.value",
			want: []Step{
				{Key: "outer", HasIndex: true, Index: 0},
				{Key: "items", HasIndex: true, Index: 9},
				{Key: "value"},
			},
		},
		{
			name: "maximum array index",
			path: "items{2147483646}",
			want: []Step{{Key: "items", HasIndex: true, Index: math.MaxInt32 - 1}},
		},
		{
			name: "unicode and spaces",
			path: "résumé.display name",
			want: []Step{{Key: "résumé"}, {Key: "display name"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseJSON(test.path)
			if err != nil {
				t.Fatalf("ParseJSON(%q): %v", test.path, err)
			}
			if len(got) != len(test.want) {
				t.Fatalf("ParseJSON(%q) steps = %#v, want %#v", test.path, got, test.want)
			}
			for index := range got {
				if got[index] != test.want[index] {
					t.Fatalf("ParseJSON(%q) step %d = %#v, want %#v", test.path, index, got[index], test.want[index])
				}
			}
		})
	}
}

func TestParseJSONPathRejectsUnsupportedOrMalformedPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		kind ErrorKind
	}{
		{name: "empty", path: "", kind: ErrorKindInvalid},
		{name: "leading separator", path: ".value", kind: ErrorKindInvalid},
		{name: "trailing separator", path: "value.", kind: ErrorKindInvalid},
		{name: "empty step", path: "one..two", kind: ErrorKindInvalid},
		{name: "array wildcard", path: "items{}.value", kind: ErrorKindUnsupported},
		{name: "star wildcard", path: "items{*}.value", kind: ErrorKindUnsupported},
		{name: "key wildcard", path: "wild*.value", kind: ErrorKindUnsupported},
		{name: "XML attribute", path: "item{@id}", kind: ErrorKindUnsupported},
		{name: "negative index", path: "items{-1}", kind: ErrorKindUnsupported},
		{name: "leading-zero index", path: "items{00}", kind: ErrorKindInvalid},
		{name: "index suffix", path: "items{1}tail", kind: ErrorKindInvalid},
		{name: "multiple indexes", path: "items{1}{2}", kind: ErrorKindInvalid},
		{name: "unclosed index", path: "items{1", kind: ErrorKindInvalid},
		{name: "unopened index", path: "items1}", kind: ErrorKindInvalid},
		{name: "path escape", path: `literal\.dot`, kind: ErrorKindUnsupported},
		{name: "control character", path: "one.\x1ftwo", kind: ErrorKindInvalid},
		{name: "array index overflow", path: "items{2147483647}", kind: ErrorKindTooComplex},
		{name: "invalid UTF-8", path: string([]byte{'a', '.', 0xff}), kind: ErrorKindInvalid},
		{
			name: "key byte ceiling",
			path: strings.Repeat("x", MaximumKeyBytes+1),
			kind: ErrorKindTooComplex,
		},
		{
			name: "path byte ceiling",
			path: strings.Repeat("a.", MaximumPathBytes/2) + "a",
			kind: ErrorKindTooComplex,
		},
		{
			name: "array selector ceiling",
			path: strings.TrimSuffix(strings.Repeat("items{0}.", MaximumArraySelectors+1), "."),
			kind: ErrorKindTooComplex,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseJSON(test.path)
			var pathErr *Error
			if !errors.As(err, &pathErr) {
				t.Fatalf("ParseJSON(%q) error = %v, want *Error", test.path, err)
			}
			if pathErr.Kind != test.kind {
				t.Fatalf("ParseJSON(%q) kind = %v, want %v (error %v)", test.path, pathErr.Kind, test.kind, err)
			}
			if pathErr.Offset < 0 || pathErr.Offset > len(test.path) {
				t.Fatalf("ParseJSON(%q) offset = %d, want inside source", test.path, pathErr.Offset)
			}
		})
	}
}

func TestParseJSONPathBoundsLocationSteps(t *testing.T) {
	t.Parallel()

	accepted := strings.TrimSuffix(strings.Repeat("a.", MaximumPathSteps), ".")
	steps, err := ParseJSON(accepted)
	if err != nil {
		t.Fatalf("ParseJSON(%d steps): %v", MaximumPathSteps, err)
	}
	if len(steps) != MaximumPathSteps {
		t.Fatalf("step count = %d, want %d", len(steps), MaximumPathSteps)
	}

	rejected := accepted + ".overflow"
	_, err = ParseJSON(rejected)
	var pathErr *Error
	if !errors.As(err, &pathErr) || pathErr.Kind != ErrorKindTooComplex {
		t.Fatalf("ParseJSON(%d steps) error = %v, want complexity error", MaximumPathSteps+1, err)
	}
}

func FuzzParseJSONPath(f *testing.F) {
	for _, seed := range []string{
		"payload.value",
		"outer{0}.items{9}.value",
		"résumé.display name",
		"",
		".",
		"items{*}",
		string([]byte{'a', '.', 0xff}),
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, path string) {
		steps, err := ParseJSON(path)
		if err != nil {
			var pathErr *Error
			if !errors.As(err, &pathErr) {
				t.Fatalf("ParseJSON(%q) returned unexpected error type %T", path, err)
			}
			return
		}
		if path == "" || !utf8.ValidString(path) || len(path) > MaximumPathBytes {
			t.Fatalf("ParseJSON(%q) accepted an invalid top-level path", path)
		}
		if len(steps) == 0 || len(steps) > MaximumPathSteps {
			t.Fatalf("ParseJSON(%q) returned %d steps", path, len(steps))
		}

		var canonical strings.Builder
		arraySelectors := 0
		for index, step := range steps {
			if index > 0 {
				canonical.WriteByte('.')
			}
			if step.Key == "" || len(step.Key) > MaximumKeyBytes {
				t.Fatalf("ParseJSON(%q) returned invalid step %#v", path, step)
			}
			canonical.WriteString(step.Key)
			if step.HasIndex {
				arraySelectors++
				if step.Index > MaximumArrayIndex {
					t.Fatalf("ParseJSON(%q) returned out-of-range step %#v", path, step)
				}
				canonical.WriteByte('{')
				canonical.WriteString(strconv.FormatUint(step.Index, 10))
				canonical.WriteByte('}')
			}
		}
		if arraySelectors > MaximumArraySelectors {
			t.Fatalf("ParseJSON(%q) returned %d array selectors", path, arraySelectors)
		}
		if canonical.String() != path {
			t.Fatalf("ParseJSON(%q) canonicalized unexpectedly to %q", path, canonical.String())
		}
	})
}
