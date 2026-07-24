package clickhouse

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileFieldCatalogValidatesBound(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis`)
	for _, maximum := range []uint32{0, 10_001} {
		if _, err := (Compiler{}).CompileFieldCatalog(logical, FieldCatalogSpec{MaximumFields: maximum}); err == nil {
			t.Fatalf("CompileFieldCatalog(MaximumFields=%d) succeeded", maximum)
		}
	}
}

func TestCompileFieldCatalogPreservesImmutableScanScope(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis`)
	before := buildPlan(t, `index=gradethis`)
	compiled := compileFieldCatalog(t, logical, 100)
	if !reflect.DeepEqual(logical, before) {
		t.Fatalf("CompileFieldCatalog mutated plan\nbefore: %#v\nafter:  %#v", before, logical)
	}

	for _, predicate := range []string{
		`"tenant_id" = ?`,
		`"index_name" IN (?)`,
		`"event_time" >= parseDateTime64BestEffort(?, 9, 'UTC')`,
		`"event_time" < parseDateTime64BestEffort(?, 9, 'UTC')`,
		`"index_time" <= parseDateTime64BestEffort(?, 3, 'UTC')`,
		`"visibility_seq" <= ?`,
	} {
		if strings.Count(compiled.SQL, predicate) != 1 {
			t.Fatalf("security predicate %q count != 1:\n%s", predicate, compiled.SQL)
		}
	}
	if strings.Count(compiled.SQL, `FROM "open_splunk"."events"`) != 1 {
		t.Fatalf("catalog must contain exactly one physical source scan:\n%s", compiled.SQL)
	}
	wantScope := []any{
		"tenant-1",
		"gradethis",
		"2026-07-21 00:00:00.000000000",
		"2026-07-22 00:00:00.000000000",
		"2026-07-22 00:00:01.000",
		uint64(73),
	}
	if len(compiled.Args) < len(wantScope) || !reflect.DeepEqual(compiled.Args[:len(wantScope)], wantScope) {
		t.Fatalf("scan scope args = %#v, want prefix %#v", compiled.Args, wantScope)
	}
}

func TestCompileFieldCatalogIsDeterministicAndParameterized(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis | eval copied=status | rename copied AS target${x} | table target${x},literal\.dot`)
	first := compileFieldCatalog(t, logical, 73)
	second := compileFieldCatalog(t, logical, 73)
	if first.SQL != second.SQL || !reflect.DeepEqual(first.Args, second.Args) || first.Spec != second.Spec {
		t.Fatalf("recompilation differed\nfirst:  %#v\nsecond: %#v", first, second)
	}
	if got, want := strings.Count(first.SQL, "?"), len(first.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nargs: %#v\nSQL: %s", got, want, first.Args, first.SQL)
	}
	if strings.Contains(first.SQL, "target${x}") || strings.Contains(first.SQL, `literal\.dot`) {
		t.Fatalf("logical field name was interpolated into catalog SQL:\n%s", first.SQL)
	}
	if !containsArgument(first.Args, "target${x}") || !containsArgument(first.Args, `literal\.dot`) {
		t.Fatalf("logical field names were not bound: %#v", first.Args)
	}
	if got := first.Args[len(first.Args)-1]; got != uint64(74) {
		t.Fatalf("profile limit arg = %#v, want MaximumFields+1", got)
	}
}

func TestCompileFieldCatalogFixedResultContract(t *testing.T) {
	t.Parallel()

	compiled := compileFieldCatalog(t, buildPlan(t, `index=gradethis`), 50)
	if strings.Count(compiled.SQL, " AS MATERIALIZED (") != 1 {
		t.Fatalf("materialized source CTE count != 1:\n%s", compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, "FROM "+quoteIdentifier(fieldCatalogSourceCTE)); got != 2 {
		t.Fatalf("materialized final relation read count = %d, want one known/header pass plus one dynamic pass:\n%s", got, compiled.SQL)
	}
	for _, column := range []string{
		FieldCatalogRowKindColumn,
		FieldCatalogNameColumn,
		FieldCatalogObservedTypesColumn,
		FieldCatalogEventCountColumn,
		FieldCatalogNullCountColumn,
		FieldCatalogMissingCountColumn,
		FieldCatalogTotalEventsColumn,
		FieldCatalogInvalidColumn,
	} {
		if !strings.Contains(compiled.SQL, quoteIdentifier(column)) {
			t.Fatalf("fixed output column %q is missing:\n%s", column, compiled.SQL)
		}
	}
	for _, fragment := range []string{
		"toUInt8(0) AS " + quoteIdentifier(FieldCatalogRowKindColumn),
		"toUInt8(1) AS " + quoteIdentifier(FieldCatalogRowKindColumn),
		"arraySort(groupUniqArrayIf(",
		"countIf(\"__os_field_catalog_present_",
		"LIMIT ?",
		"ORDER BY " + quoteIdentifier(FieldCatalogRowKindColumn) + " ASC, " + quoteIdentifier(FieldCatalogNameColumn) + " ASC",
	} {
		if !strings.Contains(compiled.SQL, fragment) {
			t.Fatalf("catalog SQL is missing %q:\n%s", fragment, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, "GROUP BY Dynamic") || strings.Contains(compiled.SQL, `GROUP BY "__os_fields"`) {
		t.Fatalf("catalog grouped a Dynamic value:\n%s", compiled.SQL)
	}
}

func TestCompileFieldCatalogEmitsHeaderForEmptyKnownDomain(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis`)
	logical.Operators = append(logical.Operators, &plan.Project{Mode: plan.ProjectModeTable})
	compiled := compileFieldCatalog(t, logical, 5)
	if !strings.Contains(compiled.SQL, "UNION ALL") ||
		!strings.Contains(compiled.SQL, "toUInt8(0) AS "+quoteIdentifier(FieldCatalogRowKindColumn)) {
		t.Fatalf("empty domain cannot return its header:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, `CAST(? AS String) AS "__os_field_catalog_profile_name"`) {
		t.Fatalf("empty exact schema unexpectedly emitted a known profile:\n%s", compiled.SQL)
	}
}

func TestCompileFieldCatalogProjectionAndShadowSemantics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		source          string
		wantKnown       []string
		wantAbsentKnown []string
		wantShadowed    []string
		wantPrefixes    []string
		wantDynamic     bool
		wantTypePath    string
	}{
		{
			name: "include closes schema", source: `index=gradethis | fields status`,
			wantKnown: []string{"_raw", "_time", "status"}, wantDynamic: false, wantTypePath: "status",
		},
		{
			name: "exclude blocks exact", source: `index=gradethis | fields - status`,
			wantAbsentKnown: []string{"status"}, wantShadowed: []string{"status"}, wantDynamic: true,
		},
		{
			name: "table closes schema", source: `index=gradethis | table status`,
			wantKnown: []string{"status"}, wantAbsentKnown: []string{"_raw", "_time"}, wantDynamic: false, wantTypePath: "status",
		},
		{
			name: "rename moves dynamic type", source: `index=gradethis | rename logger AS component | table component`,
			wantKnown: []string{"component"}, wantShadowed: []string{"component", "logger"},
			wantPrefixes: []string{"component", "logger"}, wantDynamic: false, wantTypePath: "logger",
		},
		{
			name: "eval shadows stored destination", source: `index=gradethis | eval status=logger | table status`,
			wantKnown: []string{"status"}, wantShadowed: []string{"status"}, wantDynamic: false, wantTypePath: "logger",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled := compileFieldCatalog(t, buildPlan(t, test.source), 100)
			known := catalogStringArguments(compiled.Args)
			for _, name := range test.wantKnown {
				if !slices.Contains(known, name) {
					t.Errorf("known names = %v, missing %q", known, name)
				}
			}
			for _, name := range test.wantAbsentKnown {
				if slices.Contains(known, name) {
					t.Errorf("known names = %v, unexpectedly include %q", known, name)
				}
			}
			shadows, prefixes, dynamic, ok := catalogDynamicControlArguments(compiled.Args)
			if !ok {
				t.Fatalf("dynamic control arguments missing: %#v", compiled.Args)
			}
			if dynamic != test.wantDynamic {
				t.Errorf("allowDynamic = %t, want %t", dynamic, test.wantDynamic)
			}
			for _, name := range test.wantShadowed {
				if !slices.Contains(shadows, name) {
					t.Errorf("shadow set = %v, missing %q", shadows, name)
				}
			}
			for _, name := range test.wantPrefixes {
				if !slices.Contains(prefixes, name) {
					t.Errorf("blocked prefixes = %v, missing %q", prefixes, name)
				}
			}
			if test.wantTypePath != "" && !containsArgument(compiled.Args, test.wantTypePath) {
				t.Errorf("metadata type path %q not bound: %#v", test.wantTypePath, compiled.Args)
			}
		})
	}
}

func TestCompileFieldCatalogAnalyzesRexFinalRelationAndPresence(t *testing.T) {
	t.Parallel()

	compiled := compileFieldCatalog(
		t,
		buildPlan(t, `index=gradethis | rex field=duration "^(?<duration_value>\d+)(?<duration_unit>ms|µs)$" | table duration_value, duration_unit`),
		20,
	)
	if strings.Count(compiled.SQL, "extractGroups(") != 1 ||
		!strings.Contains(compiled.SQL, `"__os_rex_exists_`) {
		t.Fatalf("field catalog lost rex value or presence semantics:\n%s", compiled.SQL)
	}
	known := catalogStringArguments(compiled.Args)
	for _, field := range []string{"duration_value", "duration_unit"} {
		if !slices.Contains(known, field) {
			t.Fatalf("known fields = %v, missing rex output %q", known, field)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}

func TestCompileFieldCatalogBindsEscapedLogicalAndPhysicalNames(t *testing.T) {
	t.Parallel()

	compiled := compileFieldCatalog(t, buildPlan(t, `index=gradethis | table labels.kubernetes\.io/app,foo?bar`), 10)
	for _, name := range []string{`labels.kubernetes\.io/app`, "foo?bar"} {
		if !containsArgument(compiled.Args, name) {
			t.Errorf("logical name %q is not bound: %#v", name, compiled.Args)
		}
		if strings.Contains(compiled.SQL, name) {
			t.Errorf("logical name %q was interpolated into SQL:\n%s", name, compiled.SQL)
		}
	}
	if !containsArgument(compiled.Args, `labels.kubernetes\.io/app`) || !containsArgument(compiled.Args, "foo?bar") {
		t.Fatalf("normalized metadata paths are not bound: %#v", compiled.Args)
	}
}

func TestCompileFieldCatalogMarksInvalidMetadataWithoutGuessing(t *testing.T) {
	t.Parallel()

	compiled := compileFieldCatalog(t, buildPlan(t, `index=gradethis | table status`), 10)
	for _, fragment := range []string{
		quoteIdentifier(internalFieldMetadataVersionColumn) + " != ?",
		"length(" + quoteIdentifier(internalFieldNamesColumn) + ") > ?",
		"length(" + quoteIdentifier(internalFieldTypesColumn) + ") > ?",
		"length(" + quoteIdentifier(internalFieldNamesColumn) + ") != length(" + quoteIdentifier(internalFieldTypesColumn) + ")",
		quoteIdentifier(internalFieldNamesColumn) + " != arraySort(arrayDistinct(" + quoteIdentifier(internalFieldNamesColumn) + "))",
		"arrayExists(field_name -> empty(field_name) OR NOT isValidUTF8(field_name) OR length(field_name) > ?, " + quoteIdentifier(internalFieldNamesColumn) + ")",
		"arrayExists(stored_type -> stored_type < ? OR stored_type > ?, " + quoteIdentifier(internalFieldTypesColumn) + ")",
		"indexOf(" + quoteIdentifier(internalFieldNamesColumn) + ", ?)",
		"arrayZip(arraySlice(" + quoteIdentifier(internalFieldNamesColumn),
	} {
		if !strings.Contains(compiled.SQL, fragment) {
			t.Fatalf("metadata guard is missing %q:\n%s", fragment, compiled.SQL)
		}
	}
	for _, want := range []any{
		eventfields.CurrentFieldMetadataVersion,
		uint64(eventfields.MaximumStoredFieldsPerEvent),
		uint64(eventfields.MaximumNormalizedFieldNameBytes),
		uint8(eventfields.StoredValueTypeNull),
		uint8(eventfields.StoredValueTypeDecimal),
	} {
		if !containsArgument(compiled.Args, want) {
			t.Errorf("metadata guard arg %#v is missing: %#v", want, compiled.Args)
		}
	}
	if strings.Contains(compiled.SQL, "dynamicType("+quoteIdentifier("status")+")") {
		t.Fatalf("projected dynamic field guessed its semantic type:\n%s", compiled.SQL)
	}
}

func TestCompileFieldCatalogPreservesKnownScalarTypeCodes(t *testing.T) {
	t.Parallel()

	compiled := compileFieldCatalog(t, buildPlan(t,
		`index=gradethis | eval signed=-7,unsigned=18446744073709551615,ratio=1.25,ok=true,text="x",nil=null | table signed,unsigned,ratio,ok,text,nil,_time`), 20)
	for _, code := range []eventfields.StoredValueType{
		eventfields.StoredValueTypeNull,
		eventfields.StoredValueTypeString,
		eventfields.StoredValueTypeSint64,
		eventfields.StoredValueTypeUint64,
		eventfields.StoredValueTypeDouble,
		eventfields.StoredValueTypeBool,
		eventfields.StoredValueTypeTimestamp,
	} {
		if !containsArgument(compiled.Args, uint8(code)) {
			t.Errorf("stored type code %d missing: %#v", code, compiled.Args)
		}
	}
}

func TestCompileFieldCatalogAnalyzesNumericBinFinalType(t *testing.T) {
	t.Parallel()

	compiled := compileFieldCatalog(
		t,
		buildPlan(t, `index=gradethis | eval signed=-11 | bin signed span=10 AS band | table band`),
		10,
	)
	if !strings.Contains(compiled.SQL, UnsupportedNumericBinValueMarker) ||
		!containsArgument(compiled.Args, uint8(eventfields.StoredValueTypeSint64)) {
		t.Fatalf("numeric-bin catalog lost its guarded Int64 final type:\n%s\nargs: %#v", compiled.SQL, compiled.Args)
	}
	if known := catalogStringArguments(compiled.Args); !slices.Contains(known, "band") {
		t.Fatalf("known catalog fields = %v, want band", known)
	}
}

func TestCompileFieldCatalogAssignsNullTypeToMissingDynamicEvalInputs(t *testing.T) {
	t.Parallel()

	compiled := compileFieldCatalog(t, buildPlan(t, `index=gradethis | eval copied=status | table copied`), 10)
	for _, want := range []string{
		"multiIf(indexOf(" + quoteIdentifier(internalFieldNamesColumn) + ", ?) != 0, arrayElement(" + quoteIdentifier(internalFieldTypesColumn),
		"isNull(\"copied\"), CAST(? AS UInt8), CAST(? AS UInt8))",
	} {
		if strings.Contains(compiled.SQL, want) {
			continue
		}
		t.Fatalf("dynamic eval has no missing-to-null semantic type fallback:\n%s", compiled.SQL)
	}
	if got := countArgument(compiled.Args, "status"); got < 2 {
		t.Fatalf("dynamic eval metadata path occurrence count = %d, want at least 2: %#v", got, compiled.Args)
	}
}

func TestCompileFieldCatalogRecognizesProvenDynamicObjectParents(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
		value  string
	}{
		{name: "table", source: `index=gradethis | table parent`, value: "parent"},
		{name: "eval copy", source: `index=gradethis | eval copied=parent | table copied`, value: "copied"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled := compileFieldCatalog(t, buildPlan(t, test.source), 10)
			for _, fragment := range []string{
				`arrayExists(name -> startsWith(name, ?), "__os_field_names")`,
				`isNull("` + test.value + `"), CAST(? AS UInt8), CAST(? AS UInt8))`,
			} {
				if !strings.Contains(compiled.SQL, fragment) {
					t.Fatalf("object-parent analysis is missing %q:\n%s", fragment, compiled.SQL)
				}
			}
			if !containsArgument(compiled.Args, uint8(eventfields.StoredValueTypeObject)) {
				t.Fatalf("object semantic code is not bound: %#v", compiled.Args)
			}
		})
	}
}

func TestCompileFieldCatalogDistinguishesBinaryRawFromUTF8String(t *testing.T) {
	t.Parallel()

	compiled := compileFieldCatalog(t, buildPlan(t, `index=gradethis | table _raw`), 10)
	if !strings.Contains(compiled.SQL, `isValidUTF8("_raw")`) ||
		!containsArgument(compiled.Args, uint8(eventfields.StoredValueTypeBytes)) {
		t.Fatalf("_raw semantic type does not distinguish binary bytes:\n%s\nargs: %#v", compiled.SQL, compiled.Args)
	}
}

func TestCompileFieldCatalogRejectsTransformingAndForgedPlans(t *testing.T) {
	t.Parallel()

	tests := []*plan.Query{
		buildPlan(t, `index=gradethis | stats count by status`),
		{Operators: []plan.Operator{buildPlan(t, `index=gradethis`).Operators[0], &plan.Aggregate{}}},
		{Operators: []plan.Operator{buildPlan(t, `index=gradethis`).Operators[0], (*plan.Project)(nil)}},
	}
	for _, logical := range tests {
		_, err := (Compiler{}).CompileFieldCatalog(logical, FieldCatalogSpec{MaximumFields: 10})
		diagnostic, ok := err.(*plan.Diagnostic)
		if !ok || diagnostic.Code != "SPL_UNSUPPORTED_FIELD_ANALYSIS_PIPELINE" {
			t.Errorf("CompileFieldCatalog(%#v) error = %#v", logical.Operators, err)
		}
	}
}

func compileFieldCatalog(t *testing.T, logical *plan.Query, maximum uint32) CompiledFieldCatalog {
	t.Helper()
	compiled, err := (Compiler{}).CompileFieldCatalog(logical, FieldCatalogSpec{MaximumFields: maximum})
	if err != nil {
		t.Fatalf("CompileFieldCatalog: %v", err)
	}
	return compiled
}

func containsArgument(arguments []any, want any) bool {
	return slices.ContainsFunc(arguments, func(argument any) bool { return reflect.DeepEqual(argument, want) })
}

func countArgument(arguments []any, want any) int {
	count := 0
	for _, argument := range arguments {
		if reflect.DeepEqual(argument, want) {
			count++
		}
	}
	return count
}

func catalogStringArguments(arguments []any) []string {
	names := make([]string, 0)
	for _, argument := range arguments {
		name, ok := argument.(string)
		if !ok || argument == "" {
			continue
		}
		names = append(names, name)
	}
	return names
}

func catalogDynamicControlArguments(arguments []any) (shadows, prefixes []string, allow bool, ok bool) {
	for index := range arguments {
		if index+2 >= len(arguments) {
			continue
		}
		var shadowsOK, prefixesOK, allowOK bool
		shadows, shadowsOK = arguments[index].([]string)
		prefixes, prefixesOK = arguments[index+1].([]string)
		allow, allowOK = arguments[index+2].(bool)
		if shadowsOK && prefixesOK && allowOK {
			return shadows, prefixes, allow, true
		}
	}
	return nil, nil, false, false
}
