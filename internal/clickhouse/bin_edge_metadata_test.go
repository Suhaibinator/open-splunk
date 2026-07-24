package clickhouse

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
)

// binEdgeMetadataStoredTypeCode renders one semantic type code exactly as the
// compiler emits it, so an assertion cannot silently drift if the numeric code
// of a stored type ever changes.
func binEdgeMetadataStoredTypeCode(value eventfields.StoredValueType) string {
	return fmt.Sprintf("toUInt8(%d)", uint8(value))
}

func binEdgeMetadataRequire(t *testing.T, sql, label string, required ...string) {
	t.Helper()
	for _, want := range required {
		if !strings.Contains(sql, want) {
			t.Fatalf("%s is missing %q:\n%s", label, want, sql)
		}
	}
}

func binEdgeMetadataRequireBindable(t *testing.T, sql string, args []any, label string) {
	t.Helper()
	if got, want := strings.Count(sql, "?"), len(args); got != want {
		t.Fatalf("%s placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", label, got, want, sql, args)
	}
}

// TestBinEdgeMetadataPreservesEvalDestinationSemanticType proves that a missing
// source keeps the destination's exact semantic type, not merely its presence.
// A destination whose type collapsed to the source's classification would make
// later field catalogs and summaries describe a value the row never held.
func TestBinEdgeMetadataPreservesEvalDestinationSemanticType(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | eval band="kept" | bin metric span=10 AS band | table metric band`)
	binEdgeMetadataRequire(t, compiled.SQL, "eval-made destination",
		// The value branch reads the prior destination column, never the source.
		`= 'None', CAST("band" AS Dynamic)`,
		// Presence and type both fall back to the destination's own metadata.
		`if("__os_numeric_bin_exists_3" != 0, 1, ifNull(1, 0))) AS "__os_numeric_bin_output_exists_3"`,
		`multiIf(isNull("band"), CAST(? AS UInt8), isValidUTF8("band"), CAST(? AS UInt8), CAST(? AS UInt8))))`+
			` AS "__os_numeric_bin_output_type_3"`,
	)
	binEdgeMetadataRequireBindable(t, compiled.SQL, compiled.Args, "eval-made destination")
	// The destination's fixed String classification is bound exactly and in
	// occurrence order, so a preserved value is never reported with the
	// bucket's numeric type.
	wantPrefix := []any{
		uint64(10),
		eventfields.CurrentFieldMetadataVersion,
		"metric",
		uint8(eventfields.StoredValueTypeNull),
		uint8(eventfields.StoredValueTypeString),
		uint8(eventfields.StoredValueTypeBytes),
	}
	if len(compiled.Args) < len(wantPrefix) || !reflect.DeepEqual(compiled.Args[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("Dynamic bin argument prefix = %#v, want %#v", compiled.Args, wantPrefix)
	}
}

// TestBinEdgeMetadataPreservesRexDestinationPrivateAliases covers the reviewer's
// untested branch: a destination created by rex carries private exists/type
// aliases rather than stored metadata, and bin must consume those aliases.
func TestBinEdgeMetadataPreservesRexDestinationPrivateAliases(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | rex field=_raw "(?<band>[a-z]+)" | bin metric span=10 AS band | table metric band`,
	)
	binEdgeMetadataRequire(t, compiled.SQL, "rex-made destination",
		`= 'None', CAST("band" AS Dynamic)`,
		`ifNull((("__os_rex_exists_2_0") OR (arrayExists(name -> startsWith(name, ?), "__os_field_names"))), 0)))`+
			` AS "__os_numeric_bin_output_exists_4"`,
		`"__os_rex_type_2_0")) AS "__os_numeric_bin_output_type_4"`,
	)
	binEdgeMetadataRequireBindable(t, compiled.SQL, compiled.Args, "rex-made destination")
	// The rex stage's own private columns must still be defined by that stage
	// and read again by the bin stage; a pruned alias would make the query
	// reference a column the inner relation no longer projects.
	for _, alias := range []string{`"__os_rex_exists_2_0"`, `"__os_rex_type_2_0"`} {
		if got := strings.Count(compiled.SQL, alias); got < 2 {
			t.Fatalf("rex private alias %s occurs %d times, want a definition and a bin-stage read:\n%s",
				alias, got, compiled.SQL)
		}
	}
}

// TestBinEdgeMetadataAfterRexClassifiesFromRexPrivateMetadata proves that
// binning a rex output reads that stage's private presence and semantic type
// instead of re-deriving a stale stored-document classification.
func TestBinEdgeMetadataAfterRexClassifiesFromRexPrivateMetadata(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | rex field=_raw "(?<cap>[0-9]+)" | bin cap span=10 | table cap`)
	binEdgeMetadataRequire(t, compiled.SQL, "bin after rex",
		`toUInt8(ifNull("__os_rex_exists_2_0", 0)) AS "__os_numeric_bin_exists_4"`,
		`toUInt8("__os_rex_type_2_0") AS "__os_numeric_bin_type_4"`,
		`toUInt8("__os_numeric_bin_type_4" = `+binEdgeMetadataStoredTypeCode(eventfields.StoredValueTypeObject)+
			`) AS "__os_numeric_bin_parent_4"`,
		// The bucketed value still reads the rex output column, not the
		// immutable stored document the capture shadowed.
		`dynamicType("cap") AS "__os_numeric_bin_physical_type_4"`,
	)
	// The sorted-array position probe belongs to the direct stored path only.
	if strings.Contains(compiled.SQL, "arrayFirstIndex(") {
		t.Fatalf("bin after rex used the direct stored-path probe:\n%s", compiled.SQL)
	}
	binEdgeMetadataRequireBindable(t, compiled.SQL, compiled.Args, "bin after rex")
}

// TestBinEdgeMetadataAfterEvalCopyResolvesSourcePathMetadata proves that an
// eval copy is always materialized, so bin must classify it from the copied
// leaf's stored semantic type rather than from a presence probe that eval
// deliberately rewrote to a constant.
func TestBinEdgeMetadataAfterEvalCopyResolvesSourcePathMetadata(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | eval copy=metric | bin copy span=10 | table copy`)
	binEdgeMetadataRequire(t, compiled.SQL, "bin after eval copy",
		`toUInt8(ifNull(1, 0)) AS "__os_numeric_bin_exists_3"`,
		`indexOf("__os_field_names", ?)`,
		`dynamicType("copy") AS "__os_numeric_bin_physical_type_3"`,
	)
	if strings.Contains(compiled.SQL, "arrayFirstIndex(") {
		t.Fatalf("bin after eval used the direct stored-path probe:\n%s", compiled.SQL)
	}
	binEdgeMetadataRequireBindable(t, compiled.SQL, compiled.Args, "bin after eval copy")
	// The copied leaf's own path resolves the type; the eval output name must
	// never be looked up in the immutable document.
	if got := countArgument(compiled.Args, "metric"); got != 2 {
		t.Fatalf("copied leaf path argument count = %d, want 2: %#v", got, compiled.Args)
	}
	if got := countArgument(compiled.Args, "metric."); got != 1 {
		t.Fatalf("copied leaf descendant argument count = %d, want 1: %#v", got, compiled.Args)
	}
	if got := countArgument(compiled.Args, "copy"); got != 0 {
		t.Fatalf("eval output name leaked into stored metadata lookups: %#v", compiled.Args)
	}
}

// TestBinEdgeMetadataChainedDestinationsReusePriorBinMetadata proves that a
// second bin writing the same destination preserves the first bin's output,
// including its calculated semantic type, when its own source is missing.
func TestBinEdgeMetadataChainedDestinationsReusePriorBinMetadata(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | bin left span=10 AS band | bin right span=7 AS band | table band`)
	binEdgeMetadataRequire(t, compiled.SQL, "chained bin destinations",
		`= 'None', CAST("band" AS Dynamic)`,
		`ifNull("__os_numeric_bin_output_exists_2", 0))) AS "__os_numeric_bin_output_exists_3"`,
		`"__os_numeric_bin_output_type_2")) AS "__os_numeric_bin_output_type_3"`,
	)
	binEdgeMetadataRequireBindable(t, compiled.SQL, compiled.Args, "chained bin destinations")
	// Each stage owns its span so one bin can never capture the other's WITH
	// alias through ClickHouse's nested-subquery alias inheritance.
	if got := countArgument(compiled.Args, uint64(10)); got != 1 {
		t.Fatalf("first span argument count = %d, want 1: %#v", got, compiled.Args)
	}
	if got := countArgument(compiled.Args, uint64(7)); got != 1 {
		t.Fatalf("second span argument count = %d, want 1: %#v", got, compiled.Args)
	}
}

// TestBinEdgeMetadataDottedDescendantUsesExactLeafPath proves that a flattened
// object's descendant is classified by its own normalized leaf path, and that
// the destination it writes is a distinct top-level field.
func TestBinEdgeMetadataDottedDescendantUsesExactLeafPath(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | bin obj.child span=10 AS band | table obj.child band`)
	binEdgeMetadataRequire(t, compiled.SQL, "dotted descendant bin",
		`CAST(? AS String) AS "__os_numeric_bin_path_2"`,
		`dynamicType("__os_fields"."obj"."child") AS "__os_numeric_bin_physical_type_2"`,
		`dynamicElement("__os_fields"."obj"."child", 'String')`,
	)
	binEdgeMetadataRequireBindable(t, compiled.SQL, compiled.Args, "dotted descendant bin")
	if got := countArgument(compiled.Args, "obj.child"); got != 1 {
		t.Fatalf("normalized leaf path argument count = %d, want 1: %#v", got, compiled.Args)
	}
	// The parent path must never be probed on the descendant's behalf.
	if got := countArgument(compiled.Args, "obj"); got != 0 {
		t.Fatalf("flattened parent path leaked into the descendant probe: %#v", compiled.Args)
	}
	if !slices.Equal(compiled.OutputFields, []string{"obj.child", "band"}) {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}
}

// TestBinEdgeMetadataFieldCatalogConsumesBinPrivateMetadata proves that the
// field catalog reads the bin's calculated presence and semantic type for the
// destination instead of the immutable document's stored metadata, which never
// described the bucketed value.
func TestBinEdgeMetadataFieldCatalogConsumesBinPrivateMetadata(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis | bin metric span=10 AS band`)
	catalog, err := (Compiler{}).CompileFieldCatalog(logical, FieldCatalogSpec{MaximumFields: 64})
	if err != nil {
		t.Fatalf("CompileFieldCatalog: %v", err)
	}
	binEdgeMetadataRequire(t, catalog.SQL, "bin field catalog",
		`toUInt8(ifNull("__os_numeric_bin_output_exists_2", 0))`,
		`"__os_numeric_bin_output_type_2" AS `,
	)
	binEdgeMetadataRequireBindable(t, catalog.SQL, catalog.Args, "bin field catalog")
	// The retained source keeps its own stored metadata and is shadowed out of
	// the dynamic-leaf enumeration so it cannot be profiled twice.
	names := catalogStringArguments(catalog.Args)
	if !slices.Contains(names, "band") || !slices.Contains(names, "metric") {
		t.Fatalf("catalog shadow names = %v, want both the destination and the retained source", names)
	}

	summary, err := (Compiler{}).CompileFieldSummary(logical, FieldSummarySpec{
		FieldName:             "band",
		MaximumValues:         10,
		MaximumDistinctValues: 100,
		MaximumValueBytes:     4_096,
	})
	if err != nil {
		t.Fatalf("CompileFieldSummary: %v", err)
	}
	binEdgeMetadataRequire(t, summary.SQL, "bin field summary",
		`"__os_numeric_bin_output_exists_2"`,
		`"__os_numeric_bin_output_type_2"`,
	)
	binEdgeMetadataRequireBindable(t, summary.SQL, summary.Args, "bin field summary")
}

// TestBinEdgeMetadataRejectsFixedNonNumericDestinationsAndSources keeps the
// compile-time boundary exact: a runtime-typed event field is now accepted, but
// a pipeline type already known to be non-numeric is still refused before
// execution.
func TestBinEdgeMetadataRejectsFixedNonNumericDestinationsAndSources(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | eval band="text" | bin band span=10`,
		`index=gradethis | eval band=true | bin band span=10`,
		`index=gradethis | table message | rex field=message "(?<cap>[0-9]+)" | bin cap span=10`,
	} {
		logical := buildPlan(t, source)
		if _, err := (Compiler{}).Compile(logical); err == nil {
			t.Fatalf("compiled %q, want SPL_UNSUPPORTED_BIN_FIELD_TYPE", source)
		} else if !strings.Contains(err.Error(), "fixed numeric field or a runtime-typed event field") {
			t.Fatalf("compile %q error = %v", source, err)
		}
	}
}

// TestBinEdgeMetadataNeverAdmitsATaggedDecimalArm replaces a weak
// existing check. Asserting only that one particular `IN (...)` spelling is
// absent would still pass if the tagged-decimal envelope were admitted through
// any other syntax, so assert the tag and its semantic code are absent outright
// while the three genuinely admitted envelopes are each classified once.
func TestBinEdgeMetadataNeverAdmitsATaggedDecimalArm(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | bin metric span=10 AS band | table metric band`)
	if strings.Contains(compiled.SQL, "decimal/v1") {
		t.Fatalf("tagged decimal envelope reached the Dynamic bin classifier:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, binEdgeMetadataStoredTypeCode(eventfields.StoredValueTypeDecimal)) {
		t.Fatalf("tagged decimal semantic code reached the Dynamic bin classifier:\n%s", compiled.SQL)
	}
	for _, admitted := range []struct {
		tag  string
		code eventfields.StoredValueType
	}{
		{"'bytes/v1'", eventfields.StoredValueTypeBytes},
		{"'timestamp/v1'", eventfields.StoredValueTypeTimestamp},
		{"'duration/v1'", eventfields.StoredValueTypeDuration},
	} {
		if got := strings.Count(compiled.SQL, admitted.tag); got != 1 {
			t.Fatalf("tag %s is classified %d times, want once:\n%s", admitted.tag, got, compiled.SQL)
		}
		if !strings.Contains(compiled.SQL, binEdgeMetadataStoredTypeCode(admitted.code)) {
			t.Fatalf("tag %s has no aligned semantic-type guard:\n%s", admitted.tag, compiled.SQL)
		}
	}
	// Containers and multivalue data have no arm at all: they reach the
	// sanitized marker through the classifier's final fallback.
	if strings.Contains(compiled.SQL, binEdgeMetadataStoredTypeCode(eventfields.StoredValueTypeList)) {
		t.Fatalf("multivalue data gained a classification arm:\n%s", compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, UnsupportedNumericBinValueMarker); got != 1 {
		t.Fatalf("sanitized marker appears %d times, want exactly one fallback:\n%s", got, compiled.SQL)
	}
}
