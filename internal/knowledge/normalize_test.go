package knowledge

import (
	"errors"
	"strings"
	"testing"
)

func TestNormalizeNamePinsASCIIWhitespaceControlsAndBinaryCase(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
		want   string
	}{
		{name: "plain", source: "Revenue", want: "Revenue"},
		{name: "pinned ASCII trim", source: "\t\n\v\f\r Revenue \t", want: "Revenue"},
		{name: "binary case lower", source: "revenue", want: "revenue"},
		{name: "non-ASCII whitespace is data", source: "\u00a0Revenue\u00a0", want: "\u00a0Revenue\u00a0"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := NormalizeName(test.source)
			if err != nil || got.String() != test.want {
				t.Fatalf("NormalizeName(%q) = %q, %v; want %q", test.source, got.String(), err, test.want)
			}
		})
	}

	upper, err := NormalizeName("Revenue")
	if err != nil {
		t.Fatal(err)
	}
	lower, err := NormalizeName("revenue")
	if err != nil {
		t.Fatal(err)
	}
	if upper == lower {
		t.Fatal("binary-distinct names compared equal")
	}
}

func TestNormalizeNameRejectsMalformedAndBoundedText(t *testing.T) {
	t.Parallel()

	for _, source := range []string{"", " \t\r\n ", "a\x00b", "a\x1fb", "a\x7fb", "a\u0080b", "a\u0085b", "a\u009fb", string([]byte{0xff})} {
		if _, err := NormalizeName(source); !errors.Is(err, ErrInvalidText) {
			t.Errorf("NormalizeName(%q) error = %v, want ErrInvalidText", source, err)
		}
	}
	maximum := strings.Repeat("x", MaximumObjectNameBytes)
	if got, err := NormalizeName(maximum); err != nil || got.String() != maximum {
		t.Fatalf("maximum name = %d bytes, %v", len(got.String()), err)
	}
	if _, err := NormalizeName(maximum + "x"); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("oversized name error = %v, want ErrResourceLimit", err)
	}
}

func TestNormalizeFieldDestinationUsesEventFieldPathAndRootAuthority(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		want   string
	}{
		{source: " status ", want: "status"},
		{source: `http.literal\.dot`, want: `http.literal\.dot`},
		{source: `nested.slash\\key`, want: `nested.slash\\key`},
		{source: "İndex.value", want: "İndex.value"},
	} {
		got, err := NormalizeFieldDestination(test.source)
		if err != nil || got.String() != test.want {
			t.Errorf("NormalizeFieldDestination(%q) = %q, %v; want %q", test.source, got.String(), err, test.want)
		}
	}

	for _, source := range []string{
		"index", "INDEX.child", "tenant_id", "Tenant_ID.child", "fields.child",
		"__os_private", "__OS_FUTURE.child", ".status", "status.", "a..b", `a\q`,
		"a\u0080b", "a\u0085b", "a\u009fb",
	} {
		if _, err := NormalizeFieldDestination(source); !errors.Is(err, ErrInvalidFieldDestination) {
			t.Errorf("reserved/malformed destination %q error = %v", source, err)
		}
	}
	maximum := strings.Repeat("x", MaximumFieldDestinationBytes)
	if got, err := NormalizeFieldDestination(maximum); err != nil || got.String() != maximum {
		t.Fatalf("maximum field destination = %d bytes, %v", len(got.String()), err)
	}
	if _, err := NormalizeFieldDestination(maximum + "x"); !errors.Is(err, ErrInvalidFieldDestination) || !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("oversized field destination error = %v", err)
	}

	sixteen := strings.Repeat("a.", 15) + "leaf"
	seventeen := strings.Repeat("a.", 16) + "leaf"
	eighteen := strings.Repeat("a.", 17) + "leaf"
	for segments, source := range map[int]string{16: sixteen, 17: seventeen} {
		got, err := NormalizeFieldDestination(source)
		if err != nil || got.String() != source {
			t.Errorf("%d-segment destination = %q, %v", segments, got.String(), err)
		}
	}
	if _, err := NormalizeFieldDestination(eighteen); !errors.Is(err, ErrInvalidFieldDestination) {
		t.Fatalf("18-segment destination error = %v, want ErrInvalidFieldDestination", err)
	}
}

func TestNormalizedScalarTypesExposeNoMutableBackingState(t *testing.T) {
	t.Parallel()

	name, err := NormalizeName("Stable")
	if err != nil {
		t.Fatal(err)
	}
	destination, err := NormalizeFieldDestination("stable.path")
	if err != nil {
		t.Fatal(err)
	}
	nameCopy := name
	destinationCopy := destination
	if nameCopy != name || destinationCopy != destination {
		t.Fatal("normalized immutable scalar copy changed identity")
	}
}
