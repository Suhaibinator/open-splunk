package eventfields

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestParseStoredContainerMetadataSupportsCurrentVersion(t *testing.T) {
	t.Parallel()

	names := []string{
		"__os_private.leaf",
		"_time.child",
		"field_types",
		`literal\.root.slash\\key`,
		"tenant_id",
	}
	wantPaths := [][]string{
		{"__os_private", "leaf"},
		{"_time", "child"},
		{"field_types"},
		{"literal.root", `slash\key`},
		{"tenant_id"},
	}

	typeCodes := []uint8{
		uint8(StoredValueTypeString),
		uint8(StoredValueTypeObject),
		uint8(StoredValueTypeNull),
		uint8(StoredValueTypeBytes),
		uint8(StoredValueTypeList),
	}
	current, err := ParseStoredContainerMetadata(
		names,
		typeCodes,
		CurrentFieldMetadataVersion,
	)
	if err != nil {
		t.Fatalf("ParseStoredContainerMetadata(v1): %v", err)
	}
	wantTypes := []StoredValueType{
		StoredValueTypeString,
		StoredValueTypeObject,
		StoredValueTypeNull,
		StoredValueTypeBytes,
		StoredValueTypeList,
	}
	if !reflect.DeepEqual(current.Paths, wantPaths) ||
		!reflect.DeepEqual(current.Types, wantTypes) {
		t.Fatalf("v1 metadata = %#v, want %#v/%#v", current, wantPaths, wantTypes)
	}

	names[0] = "caller-mutated"
	typeCodes[0] = uint8(StoredValueTypeBool)
	if current.Paths[0][0] != "__os_private" ||
		current.Types[0] != StoredValueTypeString {
		t.Fatalf("parsed metadata aliases caller input: %#v", current)
	}

	empty, err := ParseStoredContainerMetadata(
		[]string{},
		[]uint8{},
		CurrentFieldMetadataVersion,
	)
	if err != nil {
		t.Fatalf("ParseStoredContainerMetadata(empty v1): %v", err)
	}
	if empty.Types == nil || len(empty.Paths) != 0 || len(empty.Types) != 0 {
		t.Fatalf("empty v1 metadata = %#v, want non-nil empty types", empty)
	}
}

func TestParseStoredContainerMetadataRejectsVersionsAlignmentAndTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		names   []string
		types   []uint8
		version uint8
	}{
		{name: "old version", names: []string{"a"}, types: []uint8{uint8(StoredValueTypeString)}},
		{name: "v1 missing type", names: []string{"a"}, version: CurrentFieldMetadataVersion},
		{name: "v1 extra type", types: []uint8{uint8(StoredValueTypeString)}, version: CurrentFieldMetadataVersion},
		{name: "v1 unspecified type", names: []string{"a"}, types: []uint8{0}, version: CurrentFieldMetadataVersion},
		{name: "v1 unknown type", names: []string{"a"}, types: []uint8{uint8(StoredValueTypeDecimal) + 1}, version: CurrentFieldMetadataVersion},
		{name: "unknown version", version: 2},
		{name: "maximum unknown version", version: 255},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := ParseStoredContainerMetadata(
				test.names,
				test.types,
				test.version,
			); err == nil {
				t.Fatal("ParseStoredContainerMetadata unexpectedly succeeded")
			}
		})
	}
}

func TestParseStoredContainerMetadataRejectsInvalidRelativePaths(t *testing.T) {
	t.Parallel()

	tooDeep := strings.Repeat("a.", MaximumDynamicPathSegments) + "a"
	tests := []struct {
		name  string
		names []string
	}{
		{name: "empty path", names: []string{""}},
		{name: "invalid UTF-8", names: []string{string([]byte{0xff})}},
		{name: "invalid escape", names: []string{`a\q`}},
		{name: "empty segment", names: []string{"a..b"}},
		{name: "control character", names: []string{"a.\nb"}},
		{name: "overlong segment", names: []string{strings.Repeat("x", MaximumDynamicPathSegmentBytes+1)}},
		{name: "too deep", names: []string{tooDeep}},
		{name: "unsorted", names: []string{"b", "a"}},
		{name: "duplicate", names: []string{"a", "a"}},
		{name: "ancestor collision", names: []string{"a", "a.b"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			types := make([]uint8, len(test.names))
			for index := range types {
				types[index] = uint8(StoredValueTypeString)
			}
			if _, err := ParseStoredContainerMetadata(
				test.names,
				types,
				CurrentFieldMetadataVersion,
			); err == nil {
				t.Fatal("ParseStoredContainerMetadata unexpectedly succeeded")
			}
		})
	}
}

func TestParseStoredContainerMetadataEnforcesCountAndAggregateByteBounds(t *testing.T) {
	t.Parallel()

	overCountNames := make([]string, MaximumStoredFieldsPerEvent+1)
	overCountTypes := make([]uint8, len(overCountNames))
	for index := range overCountNames {
		overCountNames[index] = fmt.Sprintf("field%04d", index)
		overCountTypes[index] = uint8(StoredValueTypeString)
	}
	if _, err := ParseStoredContainerMetadata(
		overCountNames,
		overCountTypes,
		CurrentFieldMetadataVersion,
	); err == nil {
		t.Fatal("ParseStoredContainerMetadata accepted too many fields")
	}

	parent := strings.Repeat(".", MaximumDynamicPathSegmentBytes)
	prefix := make([]string, MaximumDynamicPathSegments-1)
	for index := range prefix {
		prefix[index] = parent
	}
	overBytesNames := make([]string, 140)
	overBytesTypes := make([]uint8, len(overBytesNames))
	for index := range overBytesNames {
		segments := append(append([]string(nil), prefix...), fmt.Sprintf("leaf%04d", index))
		overBytesNames[index] = NormalizeDynamicPath(segments)
		overBytesTypes[index] = uint8(StoredValueTypeString)
	}
	if _, err := ParseStoredContainerMetadata(
		overBytesNames,
		overBytesTypes,
		CurrentFieldMetadataVersion,
	); err == nil {
		t.Fatal("ParseStoredContainerMetadata accepted aggregate names over the byte limit")
	}
}
