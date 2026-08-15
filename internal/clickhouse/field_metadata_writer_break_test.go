package clickhouse

import (
	"reflect"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// fieldMetadataBreakSidecars is a hostile sidecar set: a root whose name is a
// prefix of another root, one at the ceiling length, and one whose SQL carries
// characters the writers must never interpolate unquoted.
func fieldMetadataBreakSidecars() []fieldSuggestionSidecar {
	return []fieldSuggestionSidecar{
		{
			root:               "payload",
			relativeNamesSQL:   quoteIdentifier("a_names"),
			relativeTypesSQL:   quoteIdentifier("a_types"),
			metadataVersionSQL: quoteIdentifier("a_version"),
		},
		{
			root:               "payload.inner",
			relativeNamesSQL:   quoteIdentifier("b_names"),
			relativeTypesSQL:   quoteIdentifier("b_types"),
			metadataVersionSQL: quoteIdentifier("b_version"),
		},
	}
}

// TestFieldSuggestionMetadataCTEBindsExactlyWhatItPlaceholds pins the extracted
// metadata-validation writer's only implicit contract: the placeholders it
// emits and the values it appends must stay in lockstep, and it must append to
// — never replace — the caller's argument prefix. Both live callers pass a
// non-empty prefix (the scan scope), so a lost prefix would silently rebind
// tenant isolation.
func TestFieldSuggestionMetadataCTEBindsExactlyWhatItPlaceholds(t *testing.T) {
	t.Parallel()

	roots := []string{"host", "source", "sourcetype"}
	prefix := []any{"tenant-1", uint64(73)}

	var sql strings.Builder
	args := writeFieldSuggestionMetadataCTE(&sql, append([]any(nil), prefix...), roots)
	text := sql.String()

	if got, want := strings.Count(text, "?"), len(args)-len(prefix); got != want {
		t.Fatalf("metadata CTE placeholders = %d, appended arguments = %d:\n%s", got, want, text)
	}
	if !reflect.DeepEqual(args[:len(prefix)], prefix) {
		t.Fatalf("metadata CTE clobbered the caller prefix: %#v", args[:len(prefix)])
	}
	// The reserved-root array is bound, never spliced into the text.
	for _, root := range roots {
		if strings.Contains(text, "'"+root+"'") {
			t.Fatalf("reserved root %q was interpolated into the CTE:\n%s", root, text)
		}
	}
	bound := args[len(prefix):]
	if !reflect.DeepEqual(bound[len(bound)-3], roots) {
		t.Fatalf("reserved roots bound at the wrong position: %#v", bound)
	}
	if bound[len(bound)-2] != uint8(eventfields.StoredValueTypeNull) ||
		bound[len(bound)-1] != uint8(eventfields.StoredValueTypeDecimal) {
		t.Fatalf("stored-type bounds are not the final two binds: %#v", bound)
	}

	// The writer is a pure function of its identifier-free shape: a second
	// call with different roots must emit byte-identical text.
	var other strings.Builder
	otherArgs := writeFieldSuggestionMetadataCTE(&other, nil, []string{"different"})
	if other.String() != text {
		t.Fatalf("metadata CTE text depends on the bound roots:\n%s\n\n%s", text, other.String())
	}
	if len(otherArgs) != len(bound) {
		t.Fatalf("metadata CTE bind count = %d, want %d", len(otherArgs), len(bound))
	}
}

// TestFieldSuggestionMetadataCTEIsSharedByBothMaterializedCallers proves the
// suggestion catalog and the stats wildcard inventory really do emit one block.
// Both promise the same atomic poisoning semantics; divergent text would let a
// corrupt event invalidate one surface and not the other.
func TestFieldSuggestionMetadataCTEIsSharedByBothMaterializedCallers(t *testing.T) {
	t.Parallel()

	var sql strings.Builder
	writeFieldSuggestionMetadataCTE(&sql, nil, nil)
	shared := sql.String()

	suggestions := compileFieldSuggestions(
		t,
		buildPlan(t, `index=gradethis | where status="ok"`),
		FieldSuggestionSpec{Prefix: "sta", MaximumFields: 20},
	)
	if !strings.Contains(suggestions.SQL, shared) {
		t.Fatalf("field suggestions no longer emit the shared metadata CTE:\n%s", suggestions.SQL)
	}

	parsed, err := spl.Parse(`index=gradethis | stats avg(met.a*tail*) AS value_**`)
	if err != nil {
		t.Fatal(err)
	}
	preparation, err := plan.PrepareStatsWildcard(parsed, testChartScope())
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := (Compiler{}).CompileStatsWildcardInventory(preparation.Prefix(), preparation.Request())
	if err != nil {
		t.Fatalf("CompileStatsWildcardInventory: %v", err)
	}
	if !strings.Contains(inventory.SQL, shared) {
		t.Fatalf("stats wildcard inventory no longer emits the shared metadata CTE:\n%s", inventory.SQL)
	}
	fieldMetadataBreakAssertBalanced(t, "field suggestions", suggestions.SQL, suggestions.Args)
	fieldMetadataBreakAssertBalanced(t, "stats wildcard inventory", inventory.SQL, inventory.Args)
}

func fieldMetadataBreakAssertBalanced(t *testing.T, name, sql string, args []any) {
	t.Helper()
	if got, want := strings.Count(sql, "?"), len(args); got != want {
		t.Fatalf("%s placeholders = %d, args = %d:\n%s", name, got, want, sql)
	}
}

// TestPrerequisiteFieldSuggestionPreludeIsIdenticalForBothCallers pins the
// extracted prerequisite preamble. The two prerequisite finalizers differ only
// in the observation stage they append afterwards, so given the same relation
// and sidecars their preludes and bind values must be indistinguishable.
func TestPrerequisiteFieldSuggestionPreludeIsIdenticalForBothCallers(t *testing.T) {
	t.Parallel()

	relation := compiledRelation{sql: `SELECT 1 AS "x"`, depth: 1}
	sidecars := fieldMetadataBreakSidecars()
	roots := []string{"host", "source"}
	known := []string{"alpha", "beta"}

	var first, second strings.Builder
	firstArgs := writePrerequisiteFieldSuggestionPrelude(&first, []any{"scope"}, relation, known, roots, sidecars)
	secondArgs := writePrerequisiteFieldSuggestionPrelude(&second, []any{"scope"}, relation, known, roots, sidecars)
	if first.String() != second.String() || !reflect.DeepEqual(firstArgs, secondArgs) {
		t.Fatal("the prerequisite prelude is not deterministic")
	}
	text := first.String()
	if got, want := strings.Count(text, "?"), len(firstArgs)-1; got != want {
		t.Fatalf("prelude placeholders = %d, appended arguments = %d:\n%s", got, want, text)
	}
	if firstArgs[0] != "scope" {
		t.Fatalf("prelude clobbered the caller prefix: %#v", firstArgs)
	}
	if !strings.HasPrefix(text, "WITH "+quoteIdentifier(fieldSuggestionSourceCTE)+" AS ("+relation.sql+"), ") {
		t.Fatalf("prelude no longer opens with the single source CTE:\n%s", text)
	}
	// The sidecar roots cross the boundary as one bound array, and the writer
	// keeps them sorted so two roots sharing a prefix stay distinguishable.
	if !reflect.DeepEqual(firstArgs[len(firstArgs)-1], fieldSuggestionSidecarRoots(sidecars)) {
		t.Fatalf("prelude sidecar roots = %#v", firstArgs[len(firstArgs)-1])
	}
	if strings.Contains(text, "'payload.inner'") {
		t.Fatalf("a sidecar root was interpolated into the prelude:\n%s", text)
	}

	// A prelude with no sidecars must still balance, and must still bind the
	// empty root array rather than dropping the placeholder.
	var empty strings.Builder
	emptyArgs := writePrerequisiteFieldSuggestionPrelude(&empty, nil, relation, nil, roots, nil)
	if got, want := strings.Count(empty.String(), "?"), len(emptyArgs); got != want {
		t.Fatalf("sidecar-free prelude placeholders = %d, args = %d:\n%s", got, want, empty.String())
	}
}

// TestAlignedFieldMetadataPredicateBindsSixValuesEverywhere pins the shared
// aligned-metadata predicate. It emits six placeholders and no closing paren,
// so every caller owns both the paren and the six binds; any caller that
// disagrees shifts the entire remaining bind list of its query.
func TestAlignedFieldMetadataPredicateBindsSixValuesEverywhere(t *testing.T) {
	t.Parallel()

	var sql strings.Builder
	writeAlignedFieldMetadataInvalidPredicate(&sql)
	predicate := sql.String()
	if got := strings.Count(predicate, "?"); got != 6 {
		t.Fatalf("aligned metadata predicate placeholders = %d, want 6:\n%s", got, predicate)
	}
	if strings.Count(predicate, "(") == strings.Count(predicate, ")") {
		t.Fatalf("predicate closed its own paren; callers append one:\n%s", predicate)
	}
	q := quoteIdentifier
	cursor := 0
	for _, ordered := range []string{
		q(internalFieldMetadataVersionColumn) + " != ?",
		"length(" + q(internalFieldNamesColumn) + ") > ?",
		"length(" + q(internalFieldTypesColumn) + ") > ?",
		"length(" + q(internalFieldNamesColumn) + ") != length(" + q(internalFieldTypesColumn) + ")",
		"arrayExists(field_name -> empty(field_name) OR NOT isValidUTF8(field_name) OR length(field_name) > ?, " +
			q(internalFieldNamesColumn) + ")",
		q(internalFieldNamesColumn) + " != arraySort(arrayDistinct(" + q(internalFieldNamesColumn) + "))",
		// Deliberately unterminated: the caller owns the closing paren.
		"arrayExists(stored_type -> stored_type < ? OR stored_type > ?, " + q(internalFieldTypesColumn),
	} {
		// Positions must increase: the six binds are positional, so a
		// reordered clause silently rebinds the whole predicate.
		offset := strings.Index(predicate[cursor:], ordered)
		if offset < 0 {
			t.Fatalf("aligned metadata predicate lost or reordered %q:\n%s", ordered, predicate)
		}
		cursor += offset + len(ordered)
	}

	// Every live surface that embeds the predicate must stay balanced, on the
	// materialized path and on the prerequisite path alike.
	for _, source := range []string{
		`index=gradethis | where status="ok"`,
		`index=gradethis | eventstats min(payload) AS low BY host`,
	} {
		logical := buildPlan(t, source)
		catalog, err := (Compiler{}).CompileFieldCatalog(logical, FieldCatalogSpec{MaximumFields: 20})
		if err != nil {
			t.Fatalf("CompileFieldCatalog(%q): %v", source, err)
		}
		summary, err := (Compiler{}).CompileFieldSummary(buildPlan(t, source), fieldSummaryTestSpec("host"))
		if err != nil {
			t.Fatalf("CompileFieldSummary(%q): %v", source, err)
		}
		fieldMetadataBreakAssertBalanced(t, source+" catalog", catalog.SQL, catalog.Args)
		fieldMetadataBreakAssertBalanced(t, source+" summary", summary.SQL, summary.Args)
		for _, sql := range []string{catalog.SQL, summary.SQL} {
			if !strings.Contains(sql, predicate) {
				t.Fatalf("%q stopped using the shared aligned metadata predicate:\n%s", source, sql)
			}
		}
	}
}
