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
		`ORDER BY "__os_order_`,
		`ASC NULLS LAST`,
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
	if !strings.Contains(compiled.SQL, `((1 AND ifNull(lowerUTF8(toString("level")) = lowerUTF8(?), 0)) OR (1 AND ifNull(lowerUTF8(toString("level")) = lowerUTF8(?), 0))) AND (1 AND ifNull("index" = ?, 0))`) {
		t.Fatalf("unexpected predicate grouping:\n%s", compiled.SQL)
	}
}

func TestCompileDynamicNumericComparisonUsesFailureFreeNumericCoercion(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis status>=500`)
	if !strings.Contains(compiled.SQL, `has("__os_field_names", ?) AND ifNull(toFloat64OrNull(toString("__os_fields"."status")) >= toFloat64OrNull(?), 0)`) {
		t.Fatalf("unexpected dynamic comparison:\n%s", compiled.SQL)
	}
	if got := compiled.Args[len(compiled.Args)-1]; got != "500" {
		t.Fatalf("numeric argument = %#v (%T), want source string", got, got)
	}
}

func TestCompileStringFieldWithNumericLookingLiteralCannotTypeMismatch(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis host=500`)
	if !strings.Contains(compiled.SQL, `lowerUTF8(toString("host")) = lowerUTF8(?)`) {
		t.Fatalf("host comparison is not string-safe:\n%s", compiled.SQL)
	}
	if got := compiled.Args[len(compiled.Args)-1]; got != "500" {
		t.Fatalf("host argument = %#v (%T), want string", got, got)
	}
}

func TestCompileDynamicEqualityRetainsLiteralTypeIntent(t *testing.T) {
	t.Parallel()

	integer := compileSPL(t, `index=gradethis ratio=1`)
	floating := compileSPL(t, `index=gradethis ratio=1.0`)
	if !strings.Contains(integer.SQL, `dynamicType("__os_fields"."ratio") IN (`) {
		t.Fatalf("integer equality has no Dynamic type guard:\n%s", integer.SQL)
	}
	if !strings.Contains(floating.SQL, `startsWith(dynamicType("__os_fields"."ratio"), 'Float')`) {
		t.Fatalf("floating equality has no Dynamic type guard:\n%s", floating.SQL)
	}
	if integer.SQL == floating.SQL {
		t.Fatal("integer and floating equality compiled identically")
	}
	if integer.Args[len(integer.Args)-1] != "1" || floating.Args[len(floating.Args)-1] != "1.0" {
		t.Fatalf("source lexemes lost: integer=%#v floating=%#v", integer.Args, floating.Args)
	}
}

func TestCompileFieldNotEqualRequiresExistence(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis status!=500`)
	if !strings.Contains(compiled.SQL, `has("__os_field_names", ?) AND NOT ifNull((dynamicType("__os_fields"."status") IN (`) {
		t.Fatalf("!= does not enforce presence:\n%s", compiled.SQL)
	}
}

func TestCompileNOTComparisonIncludesMissingField(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis NOT status=500`)
	if !strings.Contains(compiled.SQL, `NOT ((has("__os_field_names", ?) AND ifNull((dynamicType("__os_fields"."status") IN (`) {
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

func TestCompileQuestionMarkIsLiteralAndFreeWildcardIsTokenScoped(t *testing.T) {
	t.Parallel()

	question := compileSPL(t, `message="what?"`)
	if strings.Contains(question.SQL, "match(") {
		t.Fatalf("question mark unexpectedly activated wildcard matching:\n%s", question.SQL)
	}
	if got := question.Args[len(question.Args)-1]; got != "what?" {
		t.Fatalf("question-mark argument = %#v", got)
	}

	wildcard := compileSPL(t, `error*`)
	if got, want := wildcard.Args[len(wildcard.Args)-1], `(?i)(?:^|[^[:alnum:]_])error[[:alnum:]_]*(?:$|[^[:alnum:]_])`; got != want {
		t.Fatalf("free wildcard regex = %#v, want %#v", got, want)
	}
	if strings.Contains(wildcard.Args[len(wildcard.Args)-1].(string), `^error`) {
		t.Fatal("free wildcard was anchored to the complete raw event")
	}
}

func TestCompileTailReturnsReverseOrderAndInvertsNullPlacement(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | sort -_time | tail 3`)
	if !strings.Contains(compiled.SQL, `ASC NULLS FIRST LIMIT ?`) {
		t.Fatalf("tail did not reverse direction and null placement:\n%s", compiled.SQL)
	}
	lastOrder := strings.LastIndex(compiled.SQL, "ORDER BY")
	if lastOrder < 0 || !strings.Contains(compiled.SQL[lastOrder:], `ASC NULLS FIRST`) || strings.Contains(compiled.SQL[lastOrder:], `DESC NULLS LAST`) {
		t.Fatalf("tail restored forward order instead of returning reverse order:\n%s", compiled.SQL)
	}
}

func TestCompilePreservesSortOrderThroughProjection(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | sort status | fields - status | tail 10`)
	if !strings.Contains(compiled.SQL, `"__os_order_`) {
		t.Fatalf("sort key was not materialized as a private column:\n%s", compiled.SQL)
	}
	if slices.Contains(compiled.OutputFields, "status") {
		t.Fatalf("excluded sort field leaked into output: %v", compiled.OutputFields)
	}
}

func TestCompileSortDefaultIsBoundedAndExplicitZeroIsUnlimited(t *testing.T) {
	t.Parallel()

	bounded := compileSPL(t, `index=gradethis | sort -_time`)
	if got := bounded.Args[len(bounded.Args)-1]; got != uint64(10_000) {
		t.Fatalf("default sort limit = %#v, want 10000; args=%#v", got, bounded.Args)
	}
	unlimited := compileSPL(t, `index=gradethis | sort 0 -_time`)
	for _, argument := range unlimited.Args {
		if argument == uint64(10_000) {
			t.Fatalf("explicit sort 0 retained default limit: %#v", unlimited.Args)
		}
	}
}

func TestCompileFieldWildcardExistenceTruthTable(t *testing.T) {
	t.Parallel()

	present := compileSPL(t, `index=gradethis status=*`)
	if !strings.Contains(present.SQL, `has("__os_field_names", ?) AND isNotNull("__os_fields"."status")`) {
		t.Fatalf("field=* does not require a present non-null value:\n%s", present.SQL)
	}
	notPresent := compileSPL(t, `index=gradethis status!=*`)
	if !strings.Contains(notPresent.SQL, `AND 0`) {
		t.Fatalf("field!=* should match no events:\n%s", notPresent.SQL)
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
