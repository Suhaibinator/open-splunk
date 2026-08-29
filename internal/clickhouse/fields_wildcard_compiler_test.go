package clickhouse

import (
	"slices"
	"strings"
	"testing"
)

func TestCompileFieldsWildcardFiltersBoundedSparseInventory(t *testing.T) {
	t.Parallel()

	included := compileSPL(t, `index=gradethis | fields + host, error*`)
	if !included.SparseFields || !included.SparseFieldsSubset ||
		!slices.Equal(included.OutputFields, []string{"host", "_time", "_raw", "fields"}) {
		t.Fatalf("included contract = sparse %t/%t outputs %v", included.SparseFields, included.SparseFieldsSubset, included.OutputFields)
	}
	for _, required := range []string{
		`arrayFilter((field_name, field_type) -> (`,
		`arrayFilter((field_type, field_name) -> (`,
		`match(field_name, ?)`,
		`AS "__os_field_names"`,
		`AS "__os_field_types"`,
	} {
		if !strings.Contains(included.SQL, required) {
			t.Fatalf("wildcard inclusion misses %q:\n%s", required, included.SQL)
		}
	}
	if got, want := strings.Count(included.SQL, "?"), len(included.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d", got, want)
	}

	excluded := compileSPL(t, `index=gradethis | fields - _*, error_secret`)
	if !excluded.SparseFields || !excluded.SparseFieldsSubset ||
		slices.Contains(excluded.OutputFields, "_time") ||
		slices.Contains(excluded.OutputFields, "_raw") {
		t.Fatalf("excluded contract = sparse %t/%t outputs %v", excluded.SparseFields, excluded.SparseFieldsSubset, excluded.OutputFields)
	}
	if !strings.Contains(excluded.SQL, `NOT (field_name = ? OR match(field_name, ?))`) {
		t.Fatalf("wildcard exclusion is not applied to dynamic metadata:\n%s", excluded.SQL)
	}
	for _, argument := range excluded.Args {
		if text, ok := argument.(string); ok && strings.Contains(strings.ToLower(text), "__os_") {
			t.Fatalf("private namespace entered wildcard arguments: %#v", excluded.Args)
		}
	}

	broadExclusion := compileSPL(t, `index=gradethis | fields - *`)
	if !slices.Contains(broadExclusion.OutputFields, "_time") ||
		!slices.Contains(broadExclusion.OutputFields, "_raw") ||
		!strings.Contains(broadExclusion.SQL, `NOT startsWith(field_name, '_')`) {
		t.Fatalf("broad wildcard removed internal fields: %v", broadExclusion.OutputFields)
	}
}

func TestCompileFieldsWildcardComposesWithDownstreamDynamicReads(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | fields error* | stats count BY error_code`)
	if !strings.Contains(compiled.SQL, `has("__os_field_names", ?)`) ||
		!strings.Contains(compiled.SQL, `"__os_fields"."error_code"`) {
		t.Fatalf("downstream matching dynamic field was not resolved through filtered metadata:\n%s", compiled.SQL)
	}
	if strings.Count(compiled.SQL, "?") != len(compiled.Args) {
		t.Fatalf("placeholder mismatch: %d != %d", strings.Count(compiled.SQL, "?"), len(compiled.Args))
	}

	exactDynamic := compileSPL(t, `index=gradethis | fields custom, error* | stats count(custom) AS populated`)
	if !strings.Contains(exactDynamic.SQL, `has("__os_field_names", ?)`) ||
		!strings.Contains(exactDynamic.SQL, `"__os_fields"."custom" AS "custom"`) {
		t.Fatalf("mixed exact/wildcard projection lost exact dynamic value or presence:\n%s", exactDynamic.SQL)
	}

	chainedExactDynamic := compileSPL(t, `index=gradethis | fields logger,status* | fields logger,error* | stats count(logger) AS populated`)
	if !strings.Contains(chainedExactDynamic.SQL, `has("__os_field_names", ?)`) ||
		!strings.Contains(chainedExactDynamic.SQL, `"__os_fields"."logger" AS "logger"`) {
		t.Fatalf("chained mixed projections lost exact dynamic presence:\n%s", chainedExactDynamic.SQL)
	}

	canonical := compileSPL(t, `index=gradethis | fields event_id, error* | stats count(event_id) AS populated`)
	if strings.Contains(canonical.SQL, `has("__os_field_names", ?) AND isNotNull("event_id")`) ||
		!strings.Contains(canonical.SQL, `toUInt64((1) AND isNotNull("event_id"))`) {
		t.Fatalf("mixed exact/wildcard projection lost canonical exact presence:\n%s", canonical.SQL)
	}

	severity := compileSPL(t, `index=gradethis | fields + sev* | table severity`)
	if !slices.Equal(severity.OutputFields, []string{"severity"}) ||
		!strings.Contains(severity.SQL, `"severity"`) {
		t.Fatalf("pattern did not select non-default canonical severity: outputs=%v\n%s", severity.OutputFields, severity.SQL)
	}
	preservedSeverity := compileSPL(t, `index=gradethis | fields - host | table severity`)
	if !slices.Equal(preservedSeverity.OutputFields, []string{"severity"}) ||
		!strings.Contains(preservedSeverity.SQL, `"severity"`) {
		t.Fatalf("exclusion dropped a non-public canonical field: outputs=%v\n%s", preservedSeverity.OutputFields, preservedSeverity.SQL)
	}

	objectParent := compileSPL(t, `index=gradethis | fields parent,status* | stats count BY parent`)
	if !strings.Contains(objectParent.SQL, `startsWith(field_name, concat(?, '.'))`) ||
		!slices.Contains(objectParent.Args, any("parent.")) {
		t.Fatalf("mixed exact selector lost Dynamic object descendants:\n%s\nargs: %#v", objectParent.SQL, objectParent.Args)
	}

	calculated := compileSPL(t, `index=gradethis | eval copied=status | fields copied,error* | stats count(copied) AS populated`)
	if !strings.Contains(calculated.SQL, `AS "__os_fields_exists_`) ||
		!strings.Contains(calculated.SQL, `AS "__os_fields_descendant_`) ||
		strings.Count(calculated.SQL, "?") != len(calculated.Args) {
		t.Fatalf("calculated Dynamic presence was not frozen across wildcard filtering:\n%s\nargs: %#v", calculated.SQL, calculated.Args)
	}
	statusIndex := slices.Index(calculated.Args, any("status"))
	tenantIndex := slices.Index(calculated.Args, any("tenant-1"))
	if statusIndex < 0 || tenantIndex < 0 || statusIndex >= tenantIndex {
		t.Fatalf("projection sidecar arguments are out of SQL order: %#v", calculated.Args)
	}
}

func TestCompileFieldsWildcardDoesNotResurfaceHiddenDynamicValues(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | fields + error* | sort secret | table event_id`,
		`index=gradethis | fields - secret* | where secret_token="x" | table event_id`,
	} {
		compiled := compileSPL(t, source)
		if compiled.SparseFields || compiled.SparseFieldsSubset {
			t.Fatalf("%q retained stale sparse contract: %t/%t", source, compiled.SparseFields, compiled.SparseFieldsSubset)
		}
		for _, hidden := range []string{
			`"__os_fields"."secret"`,
			`"__os_fields"."secret_token"`,
		} {
			if strings.Contains(compiled.SQL, hidden) {
				t.Fatalf("%q resurfaced hidden dynamic value %s:\n%s", source, hidden, compiled.SQL)
			}
		}
	}

	exactParent := compileSPL(t, `index=gradethis | fields - parent | table parent.child`)
	if !slices.Equal(exactParent.OutputFields, []string{"parent.child"}) ||
		!strings.Contains(exactParent.SQL, `"__os_fields"."parent"."child"`) {
		t.Fatalf("exact exclusion incorrectly blocked a distinct dotted child: outputs=%v\n%s", exactParent.OutputFields, exactParent.SQL)
	}
}

func TestCompileFieldsWildcardComposesPriorTombstonesAndShadows(t *testing.T) {
	t.Parallel()

	chained := compileSPL(t, `index=gradethis | fields - secret | fields + *`)
	if !chained.SparseFields || !chained.SparseFieldsSubset ||
		!slices.Contains(chained.Args, any("secret")) {
		t.Fatalf("chained exclusion contract = sparse %t/%t args %#v", chained.SparseFields, chained.SparseFieldsSubset, chained.Args)
	}

	for _, source := range []string{
		`index=gradethis | rename error_code AS error_new | fields error*`,
		`index=gradethis | eval error_code="new" | fields error*`,
	} {
		compiled := compileSPL(t, source)
		if !compiled.SparseFields || !compiled.SparseFieldsSubset ||
			!slices.Contains(compiled.Args, any("error_code")) {
			t.Fatalf("%q did not deny the shadowed raw path: sparse %t/%t args %#v\n%s", source, compiled.SparseFields, compiled.SparseFieldsSubset, compiled.Args, compiled.SQL)
		}
	}
}

func TestCompileFieldsWildcardPresenceSidecarRetainsTypePathThroughCopies(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis | eval first=status | fields first,error* | eval second=first | table second`)
	if _, err := (Compiler{}).CompileFieldCatalog(logical, FieldCatalogSpec{MaximumFields: 10}); err != nil {
		t.Fatalf("CompileFieldCatalog after wildcard presence sidecar copy: %v", err)
	}
}
