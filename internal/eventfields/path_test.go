package eventfields

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestDynamicPathCodecsRoundTripLiteralDotsBackslashesAndPercents(t *testing.T) {
	t.Parallel()

	segments := []string{`literal.dot`, `slash\key`, `percent%2Ekey`, "leaf"}
	normalized := NormalizeDynamicPath(segments)
	if normalized != `literal\.dot.slash\\key.percent%2Ekey.leaf` {
		t.Fatalf("normalized path = %q", normalized)
	}
	normalizedBytes := 0
	for _, segment := range segments {
		normalizedBytes = NormalizedDynamicPathBytes(normalizedBytes, segment)
	}
	if normalizedBytes != len(normalized) {
		t.Fatalf("normalized byte count = %d, want %d", normalizedBytes, len(normalized))
	}
	decoded, err := ParseNormalizedDynamicPath(normalized)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, segments) {
		t.Fatalf("decoded path = %#v, want %#v", decoded, segments)
	}

	physical := "literal%2Edot.slash\\key.percent%252Ekey.leaf"
	physicalSegments := make([]string, len(segments))
	for index, segment := range segments {
		physicalSegments[index] = EncodePhysicalPathSegment(segment)
	}
	if got := strings.Join(physicalSegments, "."); got != physical {
		t.Fatalf("physical path = %q, want %q", got, physical)
	}
	if got, err := NormalizePhysicalDynamicPath(physical); err != nil || got != normalized {
		t.Fatalf("NormalizePhysicalDynamicPath() = %q, %v, want %q", got, err, normalized)
	}
}

func TestDynamicPathCodecsRejectMalformedPaths(t *testing.T) {
	t.Parallel()

	for _, path := range []string{"", ".a", "a.", "a..b", `a\q`, `a\`} {
		if _, err := ParseNormalizedDynamicPath(path); err == nil {
			t.Errorf("ParseNormalizedDynamicPath(%q) unexpectedly succeeded", path)
		}
	}
	for _, path := range []string{"", ".a", "a.", "a..b", "a%2", "a%2e", "a%00"} {
		if _, err := NormalizePhysicalDynamicPath(path); err == nil {
			t.Errorf("NormalizePhysicalDynamicPath(%q) unexpectedly succeeded", path)
		}
	}
}

func TestParseNormalizedDynamicPathBoundsEscapedDotCapacity(t *testing.T) {
	t.Parallel()

	segment := strings.Repeat(".", MaximumDynamicPathSegmentBytes)
	decoded, err := ParseNormalizedDynamicPath(NormalizeDynamicPath([]string{segment}))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, []string{segment}) {
		t.Fatalf("decoded path = %#v", decoded)
	}
	if cap(decoded) > MaximumDynamicPathSegments {
		t.Fatalf("decoded path capacity = %d, maximum = %d", cap(decoded), MaximumDynamicPathSegments)
	}
}

func TestParseStoredFieldNamesValidatesWholePresenceSet(t *testing.T) {
	t.Parallel()

	names := []string{`a.b`, `literal\.dot`, `nested.slash\\key`}
	got, err := ParseStoredFieldNames(names)
	if err != nil {
		t.Fatalf("ParseStoredFieldNames(valid): %v", err)
	}
	want := [][]string{{"a", "b"}, {"literal.dot"}, {"nested", `slash\key`}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded paths = %#v, want %#v", got, want)
	}

	for _, invalid := range []struct {
		name  string
		names []string
	}{
		{"unsorted", []string{"b", "a"}},
		{"duplicate", []string{"a", "a"}},
		{"reserved root", []string{"tenant_id"}},
		{"ancestor collision", []string{"a", "a.b"}},
	} {
		t.Run(invalid.name, func(t *testing.T) {
			t.Parallel()
			if _, err := ParseStoredFieldNames(invalid.names); err == nil {
				t.Fatalf("ParseStoredFieldNames(%#v) unexpectedly succeeded", invalid.names)
			}
		})
	}
}

func TestParseStoredFieldNamesRejectsAggregateMetadataOverLimit(t *testing.T) {
	t.Parallel()

	parent := strings.Repeat(".", MaximumDynamicPathSegmentBytes)
	prefix := make([]string, MaximumDynamicPathSegments-1)
	for index := range prefix {
		prefix[index] = parent
	}
	names := make([]string, 140)
	for index := range names {
		segments := append(append([]string(nil), prefix...), fmt.Sprintf("leaf%04d", index))
		names[index] = NormalizeDynamicPath(segments)
	}
	if _, err := ParseStoredFieldNames(names); err == nil {
		t.Fatal("ParseStoredFieldNames accepted aggregate metadata over its byte limit")
	}
}
