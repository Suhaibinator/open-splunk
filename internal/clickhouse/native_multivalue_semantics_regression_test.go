package clickhouse

import (
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
)

func TestNativeMVLogicalPresenceAndStoredTypeUseSealedSidecar(t *testing.T) {
	t.Parallel()

	field := fieldState{
		valueSQL:                     `"members"`,
		existsSQL:                    `has("row", ?)`,
		existsArgs:                   []any{"members"},
		optionalMultivaluePresentSQL: `"members_present" != 0`,
		kind:                         fieldKindDynamicArray,
	}

	logicalSQL, logicalArgs := logicalFieldPresenceSQL(field)
	if logicalSQL != field.optionalMultivaluePresentSQL ||
		len(logicalArgs) != 1 || logicalArgs[0] != "members" {
		t.Fatalf("logical presence = %q args %#v, want sealed sidecar and shared args", logicalSQL, logicalArgs)
	}
	if strings.Contains(logicalSQL, "notEmpty") || strings.Contains(logicalSQL, "isNotNull") {
		t.Fatalf("logical presence inferred from physical array shape: %s", logicalSQL)
	}

	existsSQL, existsArgs := knownFieldPresenceSQL(field)
	if existsSQL != field.existsSQL || len(existsArgs) != 1 {
		t.Fatalf("field occurrence = %q args %#v, want independent existence", existsSQL, existsArgs)
	}

	typeSQL, typeArgs, err := knownFieldStoredTypeSQL(field)
	if err != nil {
		t.Fatalf("knownFieldStoredTypeSQL: %v", err)
	}
	if !strings.Contains(typeSQL, field.optionalMultivaluePresentSQL) ||
		strings.Contains(typeSQL, `isNull("members")`) {
		t.Fatalf("native MV stored type does not use the list sidecar: %s", typeSQL)
	}
	wantTail := []any{
		uint8(eventfields.StoredValueTypeList),
		uint8(eventfields.StoredValueTypeNull),
	}
	if len(typeArgs) != 3 || typeArgs[0] != "members" ||
		typeArgs[1] != wantTail[0] || typeArgs[2] != wantTail[1] {
		t.Fatalf("native MV stored type args = %#v, want existence then %#v", typeArgs, wantTail)
	}
}

func TestCompileNativeMVFieldSummaryKeepsMissingNullAndPresentEmptyDistinct(t *testing.T) {
	t.Parallel()

	summary := compileFieldSummary(
		t,
		buildPlan(t, `index=gradethis | eval ranged=mvindex(split("x", ","), 9, 10)`),
		fieldSummaryTestSpec("ranged"),
	)
	for _, required := range []string{
		`tupleElement("__os_eval_mv_state_`,
		`, 1) != 0`,
		`, 2) != 0`,
		`CAST(? AS UInt8), CAST(? AS UInt8)`,
		quoteIdentifier(FieldSummaryNullCountColumn),
		quoteIdentifier(FieldSummaryMissingCountColumn),
	} {
		if !strings.Contains(summary.SQL, required) {
			t.Fatalf("native MV field summary SQL missing %q:\n%s", required, summary.SQL)
		}
	}
	if strings.Contains(summary.SQL, `isNull("ranged")`) {
		t.Fatalf("native MV field summary classified physical Array nullability:\n%s", summary.SQL)
	}
	if got, want := strings.Count(summary.SQL, "?"), len(summary.Args); got != want {
		t.Fatalf("field summary placeholders = %d, args = %d\nSQL: %s\nargs: %#v", got, want, summary.SQL, summary.Args)
	}
}

func TestCompileNativeMVPresencePredicatesAndFillNullAvoidPhysicalArrayTests(t *testing.T) {
	t.Parallel()

	search := compileSPL(
		t,
		`index=gradethis | eval empty=split("", "") | search empty=* | table empty`,
	)
	if strings.Contains(search.SQL, `isNotNull("empty")`) ||
		strings.Contains(search.SQL, `notEmpty("empty")`) {
		t.Fatalf("field=* inferred native MV presence from physical array shape:\n%s", search.SQL)
	}
	if !strings.Contains(search.SQL, `tupleElement("__os_eval_mv_state_`) ||
		!strings.Contains(search.SQL, `, 2) != 0`) {
		t.Fatalf("field=* did not consume the sealed list-presence sidecar:\n%s", search.SQL)
	}

	nullTests := compileSPL(
		t,
		`index=gradethis | eval empty=split("", ""), null_mv=mvindex(split("x", ","), 9, 10) | eval empty_null=if(isnull(empty), 1, 0), null_null=if(isnull(null_mv), 1, 0) | table empty_null,null_null`,
	)
	if strings.Contains(nullTests.SQL, `isNotNull("empty")`) ||
		strings.Contains(nullTests.SQL, `notEmpty("empty")`) ||
		strings.Contains(nullTests.SQL, `isNotNull("null_mv")`) ||
		strings.Contains(nullTests.SQL, `notEmpty("null_mv")`) {
		t.Fatalf("isnull inferred native MV presence from physical arrays:\n%s", nullTests.SQL)
	}

	filled := compileSPL(
		t,
		`index=gradethis | eval empty=split("", ""), null_mv=mvindex(split("x", ","), 9, 10) | fillnull value="fallback" empty null_mv | table empty,null_mv`,
	)
	for _, field := range []string{"empty", "null_mv"} {
		if strings.Contains(filled.SQL, `isNotNull("`+field+`")`) ||
			strings.Contains(filled.SQL, `notEmpty("`+field+`")`) {
			t.Fatalf("fillnull inferred %s presence from physical array shape:\n%s", field, filled.SQL)
		}
		if !strings.Contains(filled.SQL, `, CAST("`+field+`" AS Dynamic), CAST(CAST(? AS String) AS Dynamic))`) {
			t.Fatalf("fillnull did not preserve present native MV %s behind a sidecar predicate:\n%s", field, filled.SQL)
		}
	}
	if got, want := strings.Count(filled.SQL, "?"), len(filled.Args); got != want {
		t.Fatalf("fillnull placeholders = %d, args = %d\nSQL: %s\nargs: %#v", got, want, filled.SQL, filled.Args)
	}
}

func TestCompileNativeMVTextTransformsComposePreserveStateAndStayBounded(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval values=mvappend("B", "a"), lowered=lower(values), sorted=mvsort(lowered) | table lowered,sorted`,
	)
	for _, required := range []string{
		`lowerUTF8(assumeNotNull(dynamicElement(element, 'String')))`,
		`arrayExists(element -> dynamicType(element) != 'String'`,
		`arraySort(arrayMap(element -> assumeNotNull(dynamicElement(element, 'String'))`,
		UnsupportedNativeMVValueMarker,
		NativeMVPayloadLimitMarker,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("native MV lower/mvsort SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if len(compiled.OptionalMultivalueOutputs) != 2 {
		t.Fatalf("native MV transformed outputs lost tri-state transport: %#v", compiled.OptionalMultivalueOutputs)
	}
	if !compiled.RequiresAtomicResult() {
		t.Fatal("native MV text transforms with runtime guards are not atomic")
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("native MV transforms placeholders = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}

	stringsOnly := compileSPL(
		t,
		`index=gradethis | eval values=split("b,a", ","), normalized=upper(values), sorted=mvsort(normalized) | table sorted`,
	)
	if len(stringsOnly.OptionalMultivalueOutputs) != 1 {
		t.Fatalf("Array(String) transform lost tri-state transport: %#v", stringsOnly.OptionalMultivalueOutputs)
	}
	if !strings.Contains(stringsOnly.SQL, NativeMVPayloadLimitMarker) {
		t.Fatalf("Unicode String MV case mapping lacks output payload guard:\n%s", stringsOnly.SQL)
	}

	presenceOnly := compileSPL(
		t,
		`index=gradethis | eval values=mvappend("B", "a"), probe=if(isnull(lower(values)), 1, 0) | table probe`,
	)
	for _, required := range []string{
		`ignore(validated_mv)`,
		`lowerUTF8(assumeNotNull(dynamicElement(element, 'String')))`,
		NativeMVPayloadLimitMarker,
		`ignore("probe") = 0`,
	} {
		if !strings.Contains(presenceOnly.SQL, required) {
			t.Fatalf("presence-only native MV transform pruned guard %q:\n%s", required, presenceOnly.SQL)
		}
	}
}

func TestCompileNativeMVCountsExcludeExplicitNullMembers(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval values=mvappend(null, "x", null), member_count=mvcount(values) | stats count(values) AS occurrences max(member_count) AS maximum`,
	)
	cardinality := `arrayCount(element -> dynamicType(element) != 'None', "values")`
	if got := strings.Count(compiled.SQL, cardinality); got < 2 {
		t.Fatalf("mvcount/stats count(field) null-excluding cardinality occurrences = %d, want at least 2:\n%s", got, compiled.SQL)
	}
	if strings.Contains(compiled.SQL, `toUInt64(length("values"))`) {
		t.Fatalf("native MV count still uses raw physical length:\n%s", compiled.SQL)
	}
}
