package clickhouse

import (
	"errors"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileFieldSuggestionsValidatesBounds(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis`)
	tests := []FieldSuggestionSpec{
		{MaximumFields: 0},
		{MaximumFields: MaximumFieldSuggestions + 1},
		{Prefix: string([]byte{0xff}), MaximumFields: 1},
		{Prefix: "sta\x00tus", MaximumFields: 1},
		{Prefix: "sta\x01tus", MaximumFields: 1},
		{Prefix: "sta\u0085tus", MaximumFields: 1},
		{Prefix: strings.Repeat("x", eventfields.MaximumNormalizedFieldNameBytes+1), MaximumFields: 1},
	}
	for _, spec := range tests {
		if _, err := (Compiler{}).CompileFieldSuggestions(logical, spec); err == nil {
			t.Fatalf("CompileFieldSuggestions(%#v) succeeded", spec)
		}
	}
}

func TestCompileFieldSuggestionsPreservesImmutableScopedEventScan(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis | where status="ok"`)
	before := buildPlan(t, `index=gradethis | where status="ok"`)
	compiled := compileFieldSuggestions(t, logical, FieldSuggestionSpec{Prefix: "sta", MaximumFields: 20})
	if !reflect.DeepEqual(logical, before) {
		t.Fatalf("CompileFieldSuggestions mutated plan\nbefore: %#v\nafter:  %#v", before, logical)
	}

	for _, predicate := range []string{
		`"tenant_id" = ?`,
		`"index_name" IN (?)`,
		`"event_time" >= parseDateTime64BestEffort(?, 9, 'UTC')`,
		`"event_time" < parseDateTime64BestEffort(?, 9, 'UTC')`,
		`"index_time" <= parseDateTime64BestEffort(?, 3, 'UTC')`,
		`"expires_at" > parseDateTime64BestEffort(?, 3, 'UTC')`,
		`"visibility_seq" <= ?`,
	} {
		if strings.Count(compiled.SQL, predicate) != 1 {
			t.Fatalf("security predicate %q count != 1:\n%s", predicate, compiled.SQL)
		}
	}
	if strings.Count(compiled.SQL, `FROM "open_splunk"."events"`) != 1 {
		t.Fatalf("suggestions must contain exactly one physical source scan:\n%s", compiled.SQL)
	}
	wantScope := []any{
		"tenant-1",
		"gradethis",
		"2026-07-21 00:00:00.000000000",
		"2026-07-22 00:00:00.000000000",
		"2026-07-22 00:00:01.000",
		"2026-07-22 00:00:01.000",
		uint64(73),
	}
	if len(compiled.Args) < len(wantScope) || !reflect.DeepEqual(compiled.Args[:len(wantScope)], wantScope) {
		t.Fatalf("scan scope args = %#v, want prefix %#v", compiled.Args, wantScope)
	}
}

func TestCompileFieldSuggestionsIsDeterministicParameterizedAndNameOnly(t *testing.T) {
	t.Parallel()

	spec := FieldSuggestionSpec{Prefix: `target${x}`, MaximumFields: 73}
	logical := buildPlan(t, `index=gradethis | eval copied=status | rename copied AS target${x}`)
	first := compileFieldSuggestions(t, logical, spec)
	second := compileFieldSuggestions(t, logical, spec)
	if first.SQL != second.SQL || !reflect.DeepEqual(first.Args, second.Args) || first.Spec != second.Spec {
		t.Fatalf("recompilation differed\nfirst:  %#v\nsecond: %#v", first, second)
	}
	if got, want := strings.Count(first.SQL, "?"), len(first.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nargs: %#v\nSQL: %s", got, want, first.Args, first.SQL)
	}
	if strings.Contains(first.SQL, spec.Prefix) {
		t.Fatalf("prefix was interpolated into suggestion SQL:\n%s", first.SQL)
	}
	if !containsArgument(first.Args, spec.Prefix) {
		t.Fatalf("prefix was not bound: %#v", first.Args)
	}
	if got := first.Args[len(first.Args)-1]; got != uint64(spec.MaximumFields)+1 {
		t.Fatalf("row limit arg = %#v, want MaximumFields+1", got)
	}
	for _, forbidden := range []string{
		FieldCatalogObservedTypesColumn,
		FieldCatalogEventCountColumn,
		FieldCatalogNullCountColumn,
		FieldCatalogMissingCountColumn,
		FieldCatalogTotalEventsColumn,
		"groupUniqArray(toUInt8(",
	} {
		if strings.Contains(first.SQL, forbidden) {
			t.Fatalf("name-only suggestion SQL contains %q:\n%s", forbidden, first.SQL)
		}
	}
}

func TestCompileFieldSuggestionsPushesExactPrefixBeforeDynamicGrouping(t *testing.T) {
	t.Parallel()

	compiled := compileFieldSuggestions(
		t,
		buildPlan(t, `index=gradethis`),
		FieldSuggestionSpec{Prefix: "HTTP", MaximumFields: 20},
	)
	prefix := strings.Index(
		compiled.SQL,
		"startsWith("+quoteIdentifier(fieldSuggestionDynamicName)+", CAST(? AS String))",
	)
	grouping := strings.Index(
		compiled.SQL,
		"GROUP BY "+quoteIdentifier(fieldSuggestionDynamicName),
	)
	if prefix < 0 || grouping < 0 || prefix >= grouping {
		t.Fatalf("case-sensitive prefix was not pushed before grouping:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, "lowerUTF8(") || strings.Contains(compiled.SQL, "ILIKE") {
		t.Fatalf("suggestion prefix became case-insensitive:\n%s", compiled.SQL)
	}
	if got := countArgument(compiled.Args, "HTTP"); got != 1 {
		t.Fatalf("prefix argument count = %d, want exactly one: %#v", got, compiled.Args)
	}
	boundedSlice := "least(length(" + quoteIdentifier(internalFieldNamesColumn) +
		"), length(" + quoteIdentifier(internalFieldTypesColumn) +
		"), CAST(? AS UInt64))"
	if got := strings.Count(compiled.SQL, boundedSlice); got != 2 {
		t.Fatalf("bounded aligned metadata slice count = %d, want 2:\n%s", got, compiled.SQL)
	}
	if got := countArgument(
		compiled.Args,
		uint64(eventfields.MaximumStoredFieldsPerEvent),
	); got < 4 {
		t.Fatalf("stored field bound argument count = %d, want metadata guards plus two slices: %#v", got, compiled.Args)
	}
}

func TestCompileFieldSuggestionsValidatesAllMetadataBeforePublishingNames(t *testing.T) {
	t.Parallel()

	compiled := compileFieldSuggestions(
		t,
		buildPlan(t, `index=gradethis`),
		FieldSuggestionSpec{Prefix: "no-match", MaximumFields: 10},
	)
	for _, fragment := range []string{
		"arraySlice(" + quoteIdentifier(internalFieldNamesColumn) + ", 1, CAST(? AS UInt64)) AS " +
			quoteIdentifier(fieldSuggestionBoundedNames),
		"arrayMap(field_name -> left(field_name, CAST(? AS UInt64)), " +
			quoteIdentifier(fieldSuggestionBoundedNames) + ") AS " +
			quoteIdentifier(fieldSuggestionCheckedNames),
		"arraySlice(" + quoteIdentifier(internalFieldTypesColumn) + ", 1, CAST(? AS UInt64)) AS " +
			quoteIdentifier(fieldSuggestionBoundedTypes),
		quoteIdentifier(internalFieldMetadataVersionColumn) + " != ?",
		"length(" + quoteIdentifier(internalFieldNamesColumn) + ") > ?",
		"length(" + quoteIdentifier(internalFieldTypesColumn) + ") > ?",
		"length(" + quoteIdentifier(internalFieldNamesColumn) + ") != length(" + quoteIdentifier(internalFieldTypesColumn) + ")",
		quoteIdentifier(fieldSuggestionCheckedNames) + " != arraySort(arrayDistinct(" +
			quoteIdentifier(fieldSuggestionCheckedNames) + "))",
		"NOT match(field_name, CAST(? AS String))",
		"splitByChar('.', replaceAll(replaceAll(field_name, CAST(? AS String), 'x'), CAST(? AS String), 'x'))",
		"arrayExists(stored_type -> stored_type < ? OR stored_type > ?, " +
			quoteIdentifier(fieldSuggestionBoundedTypes) + ")",
		"toUInt8(0) AS " + quoteIdentifier(FieldSuggestionRowKindColumn),
		"toUInt8(1) AS " + quoteIdentifier(FieldSuggestionRowKindColumn),
	} {
		if !strings.Contains(compiled.SQL, fragment) {
			t.Fatalf("metadata/control guard is missing %q:\n%s", fragment, compiled.SQL)
		}
	}
	for _, want := range []any{
		eventfields.CurrentFieldMetadataVersion,
		uint64(eventfields.MaximumStoredFieldsPerEvent),
		uint64(eventfields.MaximumNormalizedFieldNameBytes),
		uint64(eventfields.MaximumDynamicPathSegmentBytes),
		uint8(eventfields.StoredValueTypeNull),
		uint8(eventfields.StoredValueTypeDecimal),
		fieldSuggestionNormalizedNamePattern,
		fieldSuggestionEscapedBackslash,
		fieldSuggestionEscapedDot,
	} {
		if !containsArgument(compiled.Args, want) {
			t.Errorf("metadata guard arg %#v is missing: %#v", want, compiled.Args)
		}
	}
}

func TestFieldSuggestionNormalizedNamePatternMatchesDurableParser(t *testing.T) {
	t.Parallel()

	pattern, err := regexp.Compile(fieldSuggestionNormalizedNamePattern)
	if err != nil {
		t.Fatalf("compile normalized-name pattern: %v", err)
	}
	corpus := []string{
		"a",
		`a\.b`,
		`a\\b`,
		`labels.kubernetes\.io/app`,
		"µservice",
		`space and "quotes"|()`,
		strings.Repeat("x", eventfields.MaximumDynamicPathSegmentBytes),
		strings.Repeat("x", eventfields.MaximumDynamicPathSegmentBytes) + ".b",
		strings.Repeat("a.", eventfields.MaximumDynamicPathSegments-1) + "a",
		"",
		`a\q`,
		`a\`,
		`.a`,
		`a.`,
		`a..b`,
		"a\x01b",
		"a\u0085b",
		strings.Repeat("a.", eventfields.MaximumDynamicPathSegments) + "a",
	}
	for _, name := range corpus {
		_, parseErr := eventfields.ParseNormalizedDynamicPath(name)
		if got, want := pattern.MatchString(name), parseErr == nil; got != want {
			t.Errorf("normalized-name pattern match for %q = %t, parser valid = %t", name, got, want)
		}
	}
}

func TestCompileFieldSuggestionsHonorsVisibleShadowsAndBlockedPrefixes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		source       string
		prefix       string
		wantKnown    []string
		wantShadowed []string
		wantPrefixes []string
		wantDynamic  bool
		absentKnown  []string
	}{
		{
			name: "open schema", source: `index=gradethis`, prefix: "s",
			wantKnown: []string{"service", "severity", "source", "sourcetype"}, wantDynamic: true,
		},
		{
			name: "include closes schema", source: `index=gradethis | fields status`, prefix: "s",
			wantKnown: []string{"status"}, wantDynamic: false,
			absentKnown: []string{"service", "severity", "source", "sourcetype"},
		},
		{
			name: "exclude blocks exact", source: `index=gradethis | fields - status`, prefix: "sta",
			wantShadowed: []string{"status"}, wantDynamic: true,
		},
		{
			name:   "rename blocks source and destination descendants",
			source: `index=gradethis | rename logger AS component`, prefix: "comp",
			wantKnown: []string{"component"}, wantShadowed: []string{"component", "logger"},
			wantPrefixes: []string{"component", "logger"}, wantDynamic: true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled := compileFieldSuggestions(
				t,
				buildPlan(t, test.source),
				FieldSuggestionSpec{Prefix: test.prefix, MaximumFields: 100},
			)
			known, shadows, prefixes, dynamic, ok := fieldSuggestionControlArguments(compiled.Args)
			if !ok {
				t.Fatalf("suggestion control arguments missing: %#v", compiled.Args)
			}
			for _, name := range test.wantKnown {
				if !slices.Contains(known, name) {
					t.Errorf("known names = %v, missing %q", known, name)
				}
			}
			for _, name := range test.absentKnown {
				if slices.Contains(known, name) {
					t.Errorf("known names = %v, unexpectedly include %q", known, name)
				}
			}
			for _, name := range test.wantShadowed {
				if !slices.Contains(shadows, name) {
					t.Errorf("shadow set = %v, missing %q", shadows, name)
				}
			}
			for _, prefix := range test.wantPrefixes {
				if !slices.Contains(prefixes, prefix) {
					t.Errorf("blocked prefixes = %v, missing %q", prefixes, prefix)
				}
			}
			if dynamic != test.wantDynamic {
				t.Errorf("allowDynamic = %t, want %t", dynamic, test.wantDynamic)
			}
		})
	}
}

func TestCompileFieldSuggestionsRejectsTransformingAndForgedPlans(t *testing.T) {
	t.Parallel()

	tests := []*plan.Query{
		buildPlan(t, `index=gradethis | stats count by status`),
		{Operators: []plan.Operator{buildPlan(t, `index=gradethis`).Operators[0], &plan.Aggregate{}}},
		{Operators: []plan.Operator{buildPlan(t, `index=gradethis`).Operators[0], (*plan.Project)(nil)}},
	}
	for _, logical := range tests {
		_, err := (Compiler{}).CompileFieldSuggestions(
			logical,
			FieldSuggestionSpec{MaximumFields: 10},
		)
		diagnostic := &plan.Diagnostic{}
		if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_UNSUPPORTED_FIELD_ANALYSIS_PIPELINE" {
			t.Errorf("CompileFieldSuggestions(%#v) error = %#v", logical.Operators, err)
		}
	}
}

func compileFieldSuggestions(
	t *testing.T,
	logical *plan.Query,
	spec FieldSuggestionSpec,
) CompiledFieldSuggestions {
	t.Helper()
	compiled, err := (Compiler{}).CompileFieldSuggestions(logical, spec)
	if err != nil {
		t.Fatalf("CompileFieldSuggestions: %v", err)
	}
	return compiled
}

func fieldSuggestionControlArguments(
	arguments []any,
) (known, shadows, prefixes []string, allow bool, ok bool) {
	for index := range arguments {
		if index+3 >= len(arguments) {
			continue
		}
		var knownOK, shadowsOK, prefixesOK, allowOK bool
		shadows, shadowsOK = arguments[index].([]string)
		prefixes, prefixesOK = arguments[index+1].([]string)
		allow, allowOK = arguments[index+2].(bool)
		known, knownOK = arguments[index+3].([]string)
		if knownOK && shadowsOK && prefixesOK && allowOK {
			return known, shadows, prefixes, allow, true
		}
	}
	return nil, nil, nil, false, false
}
