package clickhouse

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileFieldSummaryValidatesEffectiveBounds(t *testing.T) {
	t.Parallel()

	if MaximumFieldSummaryValues != 100 ||
		MaximumFieldSummaryDistinctValues != 10_000 ||
		MaximumFieldSummaryValueBytes != 256<<10 {
		t.Fatalf(
			"hard limits = (%d, %d, %d), want (100, 10000, 262144)",
			MaximumFieldSummaryValues,
			MaximumFieldSummaryDistinctValues,
			MaximumFieldSummaryValueBytes,
		)
	}

	logical := buildPlan(t, `index=gradethis`)
	valid := fieldSummaryTestSpec("status")
	tests := []struct {
		name   string
		mutate func(*FieldSummarySpec)
	}{
		{name: "zero values", mutate: func(spec *FieldSummarySpec) { spec.MaximumValues = 0 }},
		{name: "too many values", mutate: func(spec *FieldSummarySpec) { spec.MaximumValues = MaximumFieldSummaryValues + 1 }},
		{name: "zero distinct", mutate: func(spec *FieldSummarySpec) { spec.MaximumDistinctValues = 0 }},
		{name: "too many distinct", mutate: func(spec *FieldSummarySpec) {
			spec.MaximumDistinctValues = MaximumFieldSummaryDistinctValues + 1
		}},
		{name: "values exceed distinct", mutate: func(spec *FieldSummarySpec) {
			spec.MaximumValues = 2
			spec.MaximumDistinctValues = 1
		}},
		{name: "zero bytes", mutate: func(spec *FieldSummarySpec) { spec.MaximumValueBytes = 0 }},
		{name: "too many bytes", mutate: func(spec *FieldSummarySpec) {
			spec.MaximumValueBytes = MaximumFieldSummaryValueBytes + 1
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			spec := valid
			test.mutate(&spec)
			if _, err := (Compiler{}).CompileFieldSummary(logical, spec); err == nil {
				t.Fatalf("CompileFieldSummary(%#v) succeeded", spec)
			}
		})
	}
}

func TestCompileFieldSummaryIsImmutableDeterministicAndParameterized(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis | eval copied=status | rename copied AS target | table target`)
	before := buildPlan(t, `index=gradethis | eval copied=status | rename copied AS target | table target`)
	spec := FieldSummarySpec{
		FieldName:             "target",
		MaximumValues:         73,
		MaximumDistinctValues: 997,
		MaximumValueBytes:     4_096,
	}
	first := compileFieldSummary(t, logical, spec)
	second := compileFieldSummary(t, logical, spec)

	if !reflect.DeepEqual(logical, before) {
		t.Fatalf("CompileFieldSummary mutated plan\nbefore: %#v\nafter:  %#v", before, logical)
	}
	if first.SQL != second.SQL || !reflect.DeepEqual(first.Args, second.Args) ||
		first.Spec != second.Spec || first.FieldKnown != second.FieldKnown {
		t.Fatalf("recompilation differed\nfirst:  %#v\nsecond: %#v", first, second)
	}
	if got, want := strings.Count(first.SQL, "?"), len(first.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nargs: %#v\nSQL: %s", got, want, first.Args, first.SQL)
	}
	if !containsArgument(first.Args, spec.FieldName) ||
		!containsArgument(first.Args, uint64(spec.MaximumValueBytes)) {
		t.Fatalf("effective field/byte bounds are not bound: %#v", first.Args)
	}
	if first.Spec != spec || !first.FieldKnown {
		t.Fatalf("compiled contract = %#v, want known %#v", first, spec)
	}
}

func TestCompileFieldSummaryFixedTransportAndExactGroups(t *testing.T) {
	t.Parallel()

	compiled := compileFieldSummary(
		t,
		buildPlan(t, `index=gradethis`),
		fieldSummaryTestSpec("status"),
	)
	for _, column := range []string{
		FieldSummaryRowKindColumn,
		FieldSummaryFieldNameColumn,
		FieldSummaryObservedTypesColumn,
		FieldSummaryEventCountColumn,
		FieldSummaryNullCountColumn,
		FieldSummaryMissingCountColumn,
		FieldSummaryTotalEventCountColumn,
		FieldSummaryValueTypeColumn,
		FieldSummaryEncodedValueColumn,
		FieldSummaryValueCountColumn,
		FieldSummaryMetadataInvalidColumn,
		FieldSummaryUnsupportedColumn,
		FieldSummaryOversizedColumn,
	} {
		if !strings.Contains(compiled.SQL, quoteIdentifier(column)) {
			t.Errorf("fixed output column %q is missing", column)
		}
	}
	for _, fragment := range []string{
		"toUInt8(0) AS " + quoteIdentifier(FieldSummaryRowKindColumn),
		"toUInt8(1) AS " + quoteIdentifier(FieldSummaryRowKindColumn),
		"CAST(? AS String) AS " + quoteIdentifier(FieldSummaryFieldNameColumn),
		"CAST([], 'Array(UInt8)') AS " + quoteIdentifier(FieldSummaryObservedTypesColumn),
		"toUInt64(0) AS " + quoteIdentifier(FieldSummaryEventCountColumn),
		"toUInt8(0) AS " + quoteIdentifier(FieldSummaryMetadataInvalidColumn),
		"GROUP BY " + quoteIdentifier(fieldSummaryGroupType) + ", " + quoteIdentifier(fieldSummaryGroupEncoded),
		"ORDER BY " + quoteIdentifier(FieldSummaryRowKindColumn) + " ASC, " +
			quoteIdentifier(FieldSummaryValueTypeColumn) + " ASC, " +
			quoteIdentifier(FieldSummaryEncodedValueColumn) + " ASC",
	} {
		if !strings.Contains(compiled.SQL, fragment) {
			t.Errorf("summary SQL is missing %q", fragment)
		}
	}
	for _, name := range []string{
		fieldSummaryRowsCTE,
		fieldSummaryTotalsCTE,
	} {
		if fragment := quoteIdentifier(name) + " AS MATERIALIZED ("; !strings.Contains(compiled.SQL, fragment) {
			t.Errorf("shared summary CTE %q is not materialized", name)
		}
	}
	for _, name := range []string{
		fieldSummarySourceCTE,
		fieldSummaryTypedCTE,
		fieldSummaryEncodedCTE,
		fieldSummaryGroupsCTE,
	} {
		if fragment := quoteIdentifier(name) + " AS MATERIALIZED ("; strings.Contains(compiled.SQL, fragment) {
			t.Errorf("single-use summary CTE %q is unnecessarily materialized", name)
		}
		if fragment := quoteIdentifier(name) + " AS ("; !strings.Contains(compiled.SQL, fragment) {
			t.Errorf("single-use summary CTE %q is missing", name)
		}
	}
	if strings.Contains(compiled.SQL, " LIMIT ") {
		t.Fatalf("field summary added SQL truncation instead of emitting every exact group:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, "GROUP BY Dynamic") ||
		strings.Contains(compiled.SQL, "notEmpty("+quoteIdentifier(fieldSummaryEncoded)+")") {
		t.Fatalf("summary grouped Dynamic directly or excluded the empty string:\n%s", compiled.SQL)
	}
	if !strings.Contains(compiled.SQL,
		quoteIdentifier(fieldSummaryStoredType)+" != toUInt8("+
			"1)") {
		t.Fatalf("explicit null values are not excluded from groups:\n%s", compiled.SQL)
	}
}

func TestCompileFieldSummaryUsesFinalFieldSemantics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		source    string
		field     string
		wantKnown bool
		notFound  bool
		fragments []string
	}{
		{
			name: "open dynamic is runtime known", source: `index=gradethis`,
			field: "status", wantKnown: false,
		},
		{
			name: "canonical is known", source: `index=gradethis`,
			field: "_raw", wantKnown: true,
		},
		{
			name: "include closes schema", source: `index=gradethis | fields status`,
			field: "status", wantKnown: true,
		},
		{
			name: "eval output", source: `index=gradethis | eval copied=status | table copied`,
			field: "copied", wantKnown: true,
		},
		{
			name: "rename destination", source: `index=gradethis | rename status AS component | table component`,
			field: "component", wantKnown: true,
		},
		{
			name: "rename removes source", source: `index=gradethis | rename status AS component | table component`,
			field: "status", notFound: true,
		},
		{
			name: "rex output", source: `index=gradethis | rex field=duration "^(?<duration_value>\d+)" | table duration_value`,
			field: "duration_value", wantKnown: true,
			fragments: []string{"extractGroups(", `"__os_rex_exists_`},
		},
		{
			name:   "numeric bin output",
			source: `index=gradethis | eval signed=-11 | bin signed span=10 AS band | table band`,
			field:  "band", wantKnown: true,
			fragments: []string{UnsupportedNumericBinValueMarker, `accurateCastOrNull(`},
		},
		{
			name: "exclude blocks exact", source: `index=gradethis | fields - status`,
			field: "status", notFound: true,
		},
		{
			name: "table projects away", source: `index=gradethis | table host`,
			field: "status", notFound: true,
		},
		{
			name: "dedup and head remain final", source: `index=gradethis | dedup 2 status | head 5`,
			field: "status", wantKnown: false,
			fragments: []string{"LIMIT ? BY", "LIMIT ?"},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled, err := (Compiler{}).CompileFieldSummary(
				buildPlan(t, test.source),
				fieldSummaryTestSpec(test.field),
			)
			if test.notFound {
				if !errors.Is(err, ErrFieldSummaryNotFound) {
					t.Fatalf("CompileFieldSummary() error = %#v, want ErrFieldSummaryNotFound", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("CompileFieldSummary: %v", err)
			}
			if compiled.FieldKnown != test.wantKnown {
				t.Errorf("FieldKnown = %t, want %t", compiled.FieldKnown, test.wantKnown)
			}
			for _, fragment := range test.fragments {
				if !strings.Contains(compiled.SQL, fragment) {
					t.Errorf("final-relation fragment %q is missing:\n%s", fragment, compiled.SQL)
				}
			}
		})
	}
}

func TestCompileFieldSummaryValidatesPreservedRexDynamicEncodings(t *testing.T) {
	t.Parallel()

	compiled := compileFieldSummary(
		t,
		buildPlan(t, `index=gradethis | rex "status=(?<status>\d+)" | table status`),
		fieldSummaryTestSpec("status"),
	)
	for _, fragment := range []string{
		`"__os_rex_type_`,
		`'bytes/v1' AND match(`,
		`'timestamp/v1' AND match(`,
		`'duration/v1' AND match(`,
		`'decimal/v1' AND match(`,
		`IN (toUInt8(10), toUInt8(11)), 1`,
	} {
		if !strings.Contains(compiled.SQL, fragment) {
			t.Errorf("mixed rex field summary is missing %q:\n%s", fragment, compiled.SQL)
		}
	}
}

func TestCompileFieldSummaryBindsExactEscapedFieldAndRejectsInjection(t *testing.T) {
	t.Parallel()

	escaped := `labels.kubernetes\.io/app`
	compiled := compileFieldSummary(
		t,
		buildPlan(t, `index=gradethis`),
		fieldSummaryTestSpec(escaped),
	)
	if !containsArgument(compiled.Args, escaped) {
		t.Fatalf("exact normalized path is not bound: %#v", compiled.Args)
	}
	if strings.Contains(compiled.SQL, escaped) {
		t.Fatalf("escaped logical path was interpolated into SQL:\n%s", compiled.SQL)
	}

	injection := `status") OR 1=1 --`
	injected := compileFieldSummary(
		t,
		buildPlan(t, `index=gradethis`),
		fieldSummaryTestSpec(injection),
	)
	if !containsArgument(injected.Args, injection) {
		t.Fatalf("hostile exact field name is not bound: %#v", injected.Args)
	}
	if strings.Contains(injected.SQL, injection) {
		t.Fatalf("hostile logical field name was interpolated into SQL:\n%s", injected.SQL)
	}

	lower := compileFieldSummary(t, buildPlan(t, `index=gradethis`), fieldSummaryTestSpec("status"))
	upper := compileFieldSummary(t, buildPlan(t, `index=gradethis`), fieldSummaryTestSpec("Status"))
	if reflect.DeepEqual(lower.Args, upper.Args) {
		t.Fatalf("case-sensitive field spellings compiled identically: %#v", lower.Args)
	}
}

func TestCompileFieldSummaryValidatesMetadataPhysicalTypesAndContainers(t *testing.T) {
	t.Parallel()

	compiled := compileFieldSummary(
		t,
		buildPlan(t, `index=gradethis`),
		fieldSummaryTestSpec("status"),
	)
	for _, fragment := range []string{
		quoteIdentifier(internalFieldMetadataVersionColumn) + " != ?",
		"length(" + quoteIdentifier(internalFieldNamesColumn) + ") > ?",
		"length(" + quoteIdentifier(internalFieldTypesColumn) + ") > ?",
		"length(" + quoteIdentifier(internalFieldNamesColumn) + ") != length(" +
			quoteIdentifier(internalFieldTypesColumn) + ")",
		quoteIdentifier(internalFieldNamesColumn) + " != arraySort(arrayDistinct(" +
			quoteIdentifier(internalFieldNamesColumn) + "))",
		"arrayExists(field_name -> empty(field_name) OR NOT isValidUTF8(field_name) OR length(field_name) > ?, " +
			quoteIdentifier(internalFieldNamesColumn) + ")",
		"arrayExists(stored_type -> stored_type < ? OR stored_type > ?, " +
			quoteIdentifier(internalFieldTypesColumn) + ")",
		quoteIdentifier(fieldSummaryRowInvalid) + " != 0",
		"'Map(String, String)'",
		"'bytes/v1'",
		"'timestamp/v1'",
		"'duration/v1'",
		"'decimal/v1'",
		"length(" + quoteIdentifier(fieldSummaryEncoded) + ") > CAST(? AS UInt64)",
	} {
		if !strings.Contains(compiled.SQL, fragment) {
			t.Errorf("guard fragment %q is missing", fragment)
		}
	}
	if strings.Contains(compiled.SQL, "toUnixTimestamp64Nano") {
		t.Fatalf("dynamic timestamps were narrowed through Unix nanoseconds:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, "{") {
		t.Fatalf("dynamic guard contains a brace that clickhouse-go can misread as a native query parameter:\n%s", compiled.SQL)
	}
	if !strings.Contains(compiled.SQL,
		quoteIdentifier(fieldSummaryRowUnsupported)+" != 0") ||
		!strings.Contains(compiled.SQL,
			quoteIdentifier(fieldSummaryRowInvalid)+" != 0") {
		t.Fatalf("valid containers and corrupt scalar metadata do not have distinct controls:\n%s", compiled.SQL)
	}
}

func TestCompileFieldSummaryUsesReversibleFixedEncodings(t *testing.T) {
	t.Parallel()

	raw := compileFieldSummary(t, buildPlan(t, `index=gradethis`), fieldSummaryTestSpec("_raw"))
	for _, fragment := range []string{
		"isValidUTF8(",
		"replaceRegexpOne(base64Encode(toString(",
		"'=+$', '')",
	} {
		if !strings.Contains(raw.SQL, fragment) {
			t.Errorf("_raw encoding is missing %q:\n%s", fragment, raw.SQL)
		}
	}

	timestamp := compileFieldSummary(t, buildPlan(t, `index=gradethis`), fieldSummaryTestSpec("_time"))
	if !strings.Contains(timestamp.SQL,
		"concat(replaceOne(toString(toDateTime64(") ||
		!strings.Contains(timestamp.SQL, ", 9, 'UTC')), ' ', 'T'), 'Z')") {
		t.Fatalf("fixed timestamp is not canonical UTC text:\n%s", timestamp.SQL)
	}
	if strings.Contains(timestamp.SQL, "formatDateTime(") {
		t.Fatalf("fixed timestamp uses precision-losing formatDateTime %%f:\n%s", timestamp.SQL)
	}
	if strings.Contains(timestamp.SQL, "toUnixTimestamp64Nano") {
		t.Fatalf("fixed timestamp was narrowed through Unix nanoseconds:\n%s", timestamp.SQL)
	}
}

func TestCompileFieldSummaryArgumentTypesAreFixed(t *testing.T) {
	t.Parallel()

	spec := fieldSummaryTestSpec("status")
	compiled := compileFieldSummary(t, buildPlan(t, `index=gradethis`), spec)
	want := []any{
		"tenant-1",
		"gradethis",
		"2026-07-21 00:00:00.000000000",
		"2026-07-22 00:00:00.000000000",
		"2026-07-22 00:00:01.000",
		uint64(73),
		"gradethis",
		"status",
		"status.",
		"status",
		"status",
		"status.",
		uint8(eventfields.StoredValueTypeObject),
		uint8(eventfields.StoredValueTypeNull),
		uint8(0),
		uint64(spec.MaximumValueBytes),
		eventfields.CurrentFieldMetadataVersion,
		uint64(eventfields.MaximumStoredFieldsPerEvent),
		uint64(eventfields.MaximumStoredFieldsPerEvent),
		uint64(eventfields.MaximumNormalizedFieldNameBytes),
		uint8(eventfields.StoredValueTypeNull),
		uint8(eventfields.StoredValueTypeDecimal),
		"status",
	}
	if !reflect.DeepEqual(compiled.Args, want) {
		t.Fatalf("args = %#v\nwant = %#v", compiled.Args, want)
	}
	if strings.Count(compiled.SQL, "?") != len(want) {
		t.Fatalf("placeholder count = %d, want %d", strings.Count(compiled.SQL, "?"), len(want))
	}
}

func TestCompileFieldSummaryRejectsTransformingFinalRelations(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | stats count BY status`,
		`index=gradethis | timechart span=1m count BY status`,
	} {
		_, err := (Compiler{}).CompileFieldSummary(
			buildPlan(t, source),
			fieldSummaryTestSpec("status"),
		)
		if err == nil || errors.Is(err, ErrFieldSummaryNotFound) {
			t.Errorf("CompileFieldSummary(%q) error = %#v, want event-analysis eligibility error", source, err)
		}
	}
}

func fieldSummaryTestSpec(fieldName string) FieldSummarySpec {
	return FieldSummarySpec{
		FieldName:             fieldName,
		MaximumValues:         10,
		MaximumDistinctValues: 1_000,
		MaximumValueBytes:     4_096,
	}
}

func compileFieldSummary(t *testing.T, logical *plan.Query, spec FieldSummarySpec) CompiledFieldSummary {
	t.Helper()
	compiled, err := (Compiler{}).CompileFieldSummary(logical, spec)
	if err != nil {
		t.Fatalf("CompileFieldSummary: %v", err)
	}
	if compiled.Spec.FieldName != spec.FieldName {
		t.Fatalf("compiled spec field = %q, want %q", compiled.Spec.FieldName, spec.FieldName)
	}
	return compiled
}
