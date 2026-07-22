package clickhouse

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestCompileGradeThisEventSearchIsScopedAndParameterized(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis trace_id="secret-value" | sort _time | table _time level layer logger message | head 20`)
	for _, required := range []string{
		`FROM "open_splunk"."events"`,
		`"tenant_id" = ?`,
		`"index_name" IN (?)`,
		`"event_time" >= ?`,
		`"event_time" < ?`,
		`"index_time" <= ?`,
		`ORDER BY "_time" ASC NULLS LAST`,
		`LIMIT ?`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, "secret-value") || strings.Contains(compiled.SQL, "gradethis") {
		t.Fatalf("SQL contains user value: %s", compiled.SQL)
	}
	if !slices.Equal(compiled.OutputFields, []string{"_time", "level", "layer", "logger", "message"}) {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}
	if got := compiled.Args[0]; got != "tenant-1" {
		t.Fatalf("first argument = %#v", got)
	}
	if got := compiled.Args[1]; got != "gradethis" {
		t.Fatalf("index argument = %#v", got)
	}
	if got := compiled.Args[len(compiled.Args)-1]; got != uint64(20) {
		t.Fatalf("last argument = %#v, want head limit", got)
	}
}

func TestCompilePreservesSearchORPrecedence(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `level=ERROR OR level=WARN index=gradethis`)
	if !strings.Contains(compiled.SQL, `((isNotNull("level") AND ifNull(lowerUTF8(toString("level")) = lowerUTF8(?), 0)) OR (isNotNull("level") AND ifNull(lowerUTF8(toString("level")) = lowerUTF8(?), 0))) AND (1 AND ifNull("index" = ?, 0))`) {
		t.Fatalf("unexpected predicate grouping:\n%s", compiled.SQL)
	}
}

func TestCompileDynamicNumericComparisonKeepsTypedArgument(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis status>=500`)
	if !strings.Contains(compiled.SQL, `has("__os_field_names", ?) AND ifNull("__os_fields"."status" >= ?, 0)`) {
		t.Fatalf("unexpected dynamic comparison:\n%s", compiled.SQL)
	}
	if got := compiled.Args[len(compiled.Args)-1]; got != int64(500) {
		t.Fatalf("numeric argument = %#v (%T), want int64", got, got)
	}
}

func TestCompileFieldNotEqualRequiresExistence(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis status!=500`)
	if !strings.Contains(compiled.SQL, `has("__os_field_names", ?) AND NOT ifNull("__os_fields"."status" = ?, 0)`) {
		t.Fatalf("!= does not enforce presence:\n%s", compiled.SQL)
	}
}

func TestCompileNOTComparisonIncludesMissingField(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis NOT status=500`)
	if !strings.Contains(compiled.SQL, `NOT ((has("__os_field_names", ?) AND ifNull("__os_fields"."status" = ?, 0)))`) {
		t.Fatalf("NOT comparison grouping is unsafe:\n%s", compiled.SQL)
	}
}

func TestCompileWildcardUsesAnchoredEscapedRegexParameter(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `message="error.*"`)
	if strings.Contains(compiled.SQL, "error") {
		t.Fatalf("SQL contains wildcard value: %s", compiled.SQL)
	}
	want := `(?i)^error\..*$`
	if got := compiled.Args[len(compiled.Args)-1]; got != want {
		t.Fatalf("wildcard regex = %#v, want %#v", got, want)
	}
}

func TestCompileTailReversesAndRestoresOrder(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | sort -_time | tail 3`)
	if !strings.Contains(compiled.SQL, `ORDER BY "_time" ASC NULLS LAST LIMIT ?`) || !strings.Contains(compiled.SQL, `ORDER BY "_time" DESC NULLS LAST`) {
		t.Fatalf("tail did not reverse and restore order:\n%s", compiled.SQL)
	}
}

func TestCompileRejectsUntrustedPhysicalIdentifier(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis`)
	_, err := (Compiler{Database: `open_splunk; DROP DATABASE open_splunk`, Table: "events"}).Compile(logical)
	if err == nil {
		t.Fatal("Compile succeeded with invalid database identifier")
	}
}

func TestQuoteIdentifierEscapesEveryDoubleQuote(t *testing.T) {
	t.Parallel()

	if got, want := quoteIdentifier(`a"b`), `"a""b"`; got != want {
		t.Fatalf("quoteIdentifier = %q, want %q", got, want)
	}
}

func TestProjectionDoesNotExposeInternalColumns(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | fields - trace_id`)
	for _, output := range compiled.OutputFields {
		if strings.HasPrefix(output, "__os_") || output == "fields" || output == "trace_id" {
			t.Fatalf("unexpected public output field %q in %v", output, compiled.OutputFields)
		}
	}
}

func TestCompiledPlaceholderCountMatchesArguments(t *testing.T) {
	t.Parallel()

	queries := []string{
		`index=gradethis trace_id="abc"`,
		`index=gradethis status>=500 | table status | search status!=503`,
		`index=gradethis | sort 25 -status | tail 3`,
		`"connection*refused" | fields _time,message`,
	}
	for _, source := range queries {
		compiled := compileSPL(t, source)
		if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
			t.Fatalf("%q placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", source, got, want, compiled.SQL, compiled.Args)
		}
	}
}

func compileSPL(t *testing.T, source string) CompiledQuery {
	t.Helper()
	logical := buildPlan(t, source)
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return compiled
}

func buildPlan(t *testing.T, source string) *plan.Query {
	t.Helper()
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          "tenant-1",
		AuthorizedIndexes: []string{"gradethis"},
		Earliest:          time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC),
		Latest:            time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC),
		IndexTimeCutoff:   time.Date(2026, 7, 22, 0, 0, 1, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return logical
}
