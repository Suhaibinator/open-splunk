package clickhouse

import (
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"slices"
	"strconv"
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
		`"event_time" >= parseDateTime64BestEffort(?, 9, 'UTC')`,
		`"event_time" < parseDateTime64BestEffort(?, 9, 'UTC')`,
		`"index_time" <= parseDateTime64BestEffort(?, 3, 'UTC')`,
		`"visibility_seq" <= ?`,
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

func TestCompileTimeBoundsUseExplicitDateTime64StringParameters(t *testing.T) {
	t.Parallel()

	parsed, err := spl.Parse(`index=gradethis`)
	if err != nil {
		t.Fatal(err)
	}
	zone := time.FixedZone("fixture", 9*60*60+30*60)
	visibility := uint64(73)
	earliest := time.Date(1960, 1, 2, 3, 4, 5, 123456789, zone)
	latest := time.Date(2262, 4, 11, 23, 47, 16, 854775807, time.UTC)
	cutoff := time.Date(2026, 7, 22, 11, 47, 38, 687883000, zone)
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          "tenant-1",
		AuthorizedIndexes: []string{"gradethis"},
		Earliest:          earliest,
		Latest:            latest,
		IndexTimeCutoff:   cutoff,
		VisibilityCutoff:  &visibility,
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	want := []any{
		"tenant-1",
		"gradethis",
		"1960-01-01 17:34:05.123456789",
		"2262-04-11 23:47:16.854775807",
		"2026-07-22 02:17:38.687",
		uint64(73),
		"gradethis",
	}
	if !reflect.DeepEqual(compiled.Args, want) {
		t.Fatalf("compiled args = %#v, want %#v", compiled.Args, want)
	}
	for _, argument := range compiled.Args {
		if _, inferredDateTime := argument.(time.Time); inferredDateTime {
			t.Fatalf("bare time.Time argument retained: %#v", compiled.Args)
		}
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
	for _, required := range []string{
		`has("__os_field_names", ?) AND ifNull(multiIf((dynamicType("__os_fields"."status") IN (`,
		`accurateCastOrNull(toString("__os_fields"."status"), 'Int256')) >= accurateCastOrNull(?, 'Int256')`,
		`reinterpretAsInt256(bitNot(`,
		`'decimal/v1'`,
		`toFloat64OrNull(?)`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("dynamic comparison SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := compiled.Args[len(compiled.Args)-2:]; !reflect.DeepEqual(got, []any{"500", "500"}) {
		t.Fatalf("numeric argument occurrences = %#v, want source strings", got)
	}
}

func TestDynamicTaggedDecimalExactIntegerLoweringHandlesExponentSyntax(t *testing.T) {
	t.Parallel()

	exact := dynamicTaggedDecimalIntegralSQL(compiledScalar{
		valueSQL:       `"value"`,
		dynamicTypeSQL: `dynamicType("value")`,
		kind:           fieldKindDynamic,
	})
	// Integral exponent spellings are ordinary exact Decimals, not a Float64
	// compatibility case. In particular, 9007199254740992e0 must remain
	// distinct from an adjacent Dynamic(Int256) bucket above 2^53.
	exponentAware := strings.Contains(exact, "[eE]") ||
		strings.Contains(exact, "positionCaseInsensitive") ||
		(strings.Contains(exact, "lowerUTF8") && strings.Contains(exact, "'e'")) ||
		(strings.Contains(exact, "'e'") && strings.Contains(exact, "'E'"))
	if !exponentAware {
		t.Fatalf("exact tagged-Decimal integer lowering has no exponent path:\n%s", exact)
	}
	for _, bound := range []string{exactNumericBinMaxInt256, exactNumericBinMinMagnitude} {
		if !strings.Contains(exact, bound) {
			t.Fatalf("exact tagged-Decimal integer lowering lost signed Int256 bound %q:\n%s", bound, exact)
		}
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

func TestCompileBaseSearchComparesTaggedDecimalValues(t *testing.T) {
	t.Parallel()

	equality := compileSPL(t, `index=gradethis decimal_value=123.45`)
	relational := compileSPL(t, `index=gradethis decimal_value>100`)
	for name, compiled := range map[string]CompiledQuery{"equality": equality, "relational": relational} {
		for _, required := range []string{`Map(String, String)`, `'decimal/v1'`, `open_splunk_value`, `ifNotFinite(toFloat64OrNull(`} {
			if !strings.Contains(compiled.SQL, required) {
				t.Fatalf("%s tagged-decimal SQL missing %q:\n%s", name, required, compiled.SQL)
			}
		}
		if placeholders := strings.Count(compiled.SQL, "?"); placeholders != len(compiled.Args) {
			t.Fatalf("%s placeholder count = %d, args = %d: %#v", name, placeholders, len(compiled.Args), compiled.Args)
		}
	}
}

func TestCompileFieldNotEqualRequiresExistence(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis status!=500`)
	if !strings.Contains(compiled.SQL, `has("__os_field_names", ?) AND NOT ifNull(multiIf((dynamicType("__os_fields"."status") IN (`) {
		t.Fatalf("!= does not enforce presence:\n%s", compiled.SQL)
	}
}

func TestCompileNOTComparisonIncludesMissingField(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis NOT status=500`)
	if !strings.Contains(compiled.SQL, `NOT ((has("__os_field_names", ?) AND ifNull(multiIf((dynamicType("__os_fields"."status") IN (`) {
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

func TestCompileDedupUsesOrderedLimitByAndPrivateScalarKeys(t *testing.T) {
	t.Parallel()

	baseline := compileSPL(t, `index=gradethis`)
	compiled := compileSPL(t, `index=gradethis | dedup 2 status, host`)
	if !slices.Equal(compiled.OutputFields, baseline.OutputFields) {
		t.Fatalf("dedup changed output schema: got %v want %v", compiled.OutputFields, baseline.OutputFields)
	}
	for _, required := range []string{
		`AS "__os_dedup_present_`,
		`AS "__os_dedup_supported_`,
		`AS "__os_dedup_key_`,
		`max(CAST(("__os_dedup_present_`,
		`OVER () AS "__os_dedup_any_unsupported_`,
		UnsupportedDedupValueMarker,
		`SELECT * EXCEPT ("__os_dedup_present_`,
		`ORDER BY "__os_sort_time" DESC NULLS LAST, "__os_sort_event_id" DESC NULLS LAST LIMIT ? BY "__os_dedup_key_`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("dedup SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, "argMax") || strings.Contains(compiled.SQL, "GROUP BY") {
		t.Fatalf("dedup must use LIMIT BY rather than aggregation:\n%s", compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
	if got := compiled.Args[:2]; !reflect.DeepEqual(got, []any{"status", "status."}) {
		t.Fatalf("dynamic key arguments = %#v, want [status status.]", got)
	}
	if got := compiled.Args[len(compiled.Args)-1]; got != uint64(2) {
		t.Fatalf("dedup count argument = %#v, want 2", got)
	}
	outerProjectionEnd := strings.Index(compiled.SQL, " FROM (")
	if outerProjectionEnd < 0 || strings.Contains(compiled.SQL[:outerProjectionEnd], "__os_dedup_") {
		t.Fatalf("private dedup columns leaked into public projection:\n%s", compiled.SQL)
	}
}

func TestCompileRepeatedDedupPrunesEachStagesPrivateHelpers(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | dedup status | dedup host`)
	if got := strings.Count(compiled.SQL, `SELECT * EXCEPT (`); got != 2 {
		t.Fatalf("repeated dedup has %d helper-pruning projections, want 2:\n%s", got, compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}

func TestCompileDedupHonorsPriorSortAndProjectionBoundaries(t *testing.T) {
	t.Parallel()

	sorted := compileSPL(t, `index=gradethis | sort 0 +_time | dedup event_id`)
	limitBy := strings.LastIndex(sorted.SQL, " LIMIT ? BY ")
	if limitBy < 0 {
		t.Fatalf("dedup LIMIT BY missing:\n%s", sorted.SQL)
	}
	dedupOrder := strings.LastIndex(sorted.SQL[:limitBy], "ORDER BY ")
	if dedupOrder < 0 || !strings.Contains(sorted.SQL[dedupOrder:limitBy], `"__os_order_2_0" ASC NULLS LAST`) {
		t.Fatalf("dedup did not reuse the prior materialized sort order:\n%s", sorted.SQL)
	}

	removed := compileSPL(t, `index=gradethis | fields host | dedup status`)
	if strings.Contains(removed.SQL, `"__os_fields"."status"`) ||
		!strings.Contains(removed.SQL, `toUInt8(0) AS "__os_dedup_present_`) {
		t.Fatalf("dedup resurrected a projected-away key:\n%s", removed.SQL)
	}
}

func TestCompileEvalFieldCopiesPreserveFlattenedObjectProvenance(t *testing.T) {
	t.Parallel()

	direct := compileSPL(t, `index=gradethis | eval copied=object_parent | dedup copied`)
	for _, required := range []string{
		`arrayExists(name -> startsWith(name, ?), "__os_field_names")`,
		`OVER () AS "__os_dedup_any_unsupported_`,
		UnsupportedDedupValueMarker,
	} {
		if !strings.Contains(direct.SQL, required) {
			t.Fatalf("direct eval copy lost flattened-object provenance %q:\n%s", required, direct.SQL)
		}
	}
	if got := direct.Args[0]; got != "object_parent." {
		t.Fatalf("direct eval descendant argument = %#v, want object_parent.; args=%#v", got, direct.Args)
	}
	if got, want := strings.Count(direct.SQL, "?"), len(direct.Args); got != want {
		t.Fatalf("direct eval placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, direct.SQL, direct.Args)
	}
	if !slices.Contains(direct.OutputFields, "copied") || slices.Contains(direct.OutputFields, internalFieldNamesColumn) {
		t.Fatalf("direct eval public output fields = %v", direct.OutputFields)
	}

	chained := compileSPL(t, `index=gradethis | eval first=object_parent, copied=first | stats count BY copied`)
	for _, required := range []string{
		`arrayExists(name -> startsWith(name, ?), "__os_field_names")`,
		`OVER () AS "__os_stats_by_any_unsupported"`,
		UnsupportedStatsByValueMarker,
	} {
		if !strings.Contains(chained.SQL, required) {
			t.Fatalf("chained eval copy lost flattened-object provenance %q:\n%s", required, chained.SQL)
		}
	}
	if got := chained.Args[len(chained.Args)-1]; got != "object_parent." {
		t.Fatalf("chained eval descendant argument = %#v, want object_parent.; args=%#v", got, chained.Args)
	}
	if got := chained.Args[0]; got != "object_parent." {
		t.Fatalf("chained eval validation argument = %#v, want object_parent.; args=%#v", got, chained.Args)
	}
	if got, want := strings.Count(chained.SQL, "?"), len(chained.Args); got != want {
		t.Fatalf("chained eval placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, chained.SQL, chained.Args)
	}
	if !slices.Equal(chained.OutputFields, []string{"copied", "count"}) {
		t.Fatalf("chained eval stats output fields = %v", chained.OutputFields)
	}

	multiKey := compileSPL(t, `index=gradethis | eval copied=object_parent | stats count BY copied, absent`)
	validation := strings.Index(multiKey.SQL, `OVER () AS "__os_stats_by_any_unsupported"`)
	eligibility := strings.Index(multiKey.SQL, `WHERE if("__os_stats_by_any_unsupported" != 0`)
	if validation < 0 || eligibility < 0 || validation >= eligibility {
		t.Fatalf("multi-key stats did not validate before eligibility filtering:\n%s", multiKey.SQL)
	}
	if got, want := strings.Count(multiKey.SQL, "?"), len(multiKey.Args); got != want {
		t.Fatalf("multi-key stats placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, multiKey.SQL, multiKey.Args)
	}

	scalar := compileSPL(t, `index=gradethis | eval copied="ordinary" | dedup copied`)
	if strings.Contains(scalar.SQL, `arrayExists(name -> startsWith(name, ?), "__os_field_names")`) ||
		strings.Contains(scalar.SQL, `AS "__os_dedup_supported_`) {
		t.Fatalf("ordinary scalar eval acquired Dynamic descendant guards:\n%s", scalar.SQL)
	}
	if got, want := strings.Count(scalar.SQL, "?"), len(scalar.Args); got != want {
		t.Fatalf("scalar eval placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, scalar.SQL, scalar.Args)
	}

	projectedAway := compileSPL(t, `index=gradethis | fields host | eval copied=object_parent | dedup copied`)
	if strings.Contains(projectedAway.SQL, `"__os_fields"."object_parent"`) ||
		strings.Contains(projectedAway.SQL, `arrayExists(name -> startsWith(name, ?), "__os_field_names")`) {
		t.Fatalf("eval resurrected projected-away object provenance:\n%s", projectedAway.SQL)
	}
}

func TestCompileDedupSupportsTransformingAndDownstreamPipelines(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | stats count BY service | sort 0 -count | dedup count | table service, count`)
	if !slices.Equal(compiled.OutputFields, []string{"service", "count"}) {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}
	if !strings.Contains(compiled.SQL, `LIMIT ? BY "__os_dedup_key_`) ||
		!strings.Contains(compiled.SQL, `"count" AS "__os_dedup_key_`) {
		t.Fatalf("post-stats dedup did not retain its fixed scalar key:\n%s", compiled.SQL)
	}
	if got := compiled.Args[len(compiled.Args)-1]; got != uint64(1) {
		t.Fatalf("dedup count argument = %#v, want 1", got)
	}
}

func TestCompileDedupAllowsClosedSchemaFieldNamedFields(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | stats count AS fields | dedup fields`)
	if !slices.Equal(compiled.OutputFields, []string{"fields"}) ||
		!strings.Contains(compiled.SQL, `"fields" AS "__os_dedup_key_`) ||
		strings.Contains(compiled.SQL, `"__os_fields"."fields"`) {
		t.Fatalf("closed-schema fields key compiled incorrectly; output=%v\n%s", compiled.OutputFields, compiled.SQL)
	}
}

func TestCompileDedupRejectsDirectPlanWithAmbiguousEventFieldsPayload(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis`)
	fieldRange := spl.Range{
		Start: spl.Position{Offset: 7, Line: 1, Column: 8},
		End:   spl.Position{Offset: 13, Line: 1, Column: 14},
	}
	fields, err := plan.ResolveField("fields", fieldRange)
	if err != nil {
		t.Fatal(err)
	}
	logical.Operators = append(logical.Operators, &plan.Deduplicate{
		Count: 1,
		Keys:  []plan.FieldRef{fields},
	})
	_, err = (Compiler{}).Compile(logical)
	diagnostic, ok := err.(*plan.Diagnostic)
	if !ok || diagnostic.Code != "SPL_AMBIGUOUS_DEDUP_FIELD" {
		t.Fatalf("Compile error = %#v, want SPL_AMBIGUOUS_DEDUP_FIELD", err)
	}
	if diagnostic.Range != fieldRange {
		t.Fatalf("diagnostic range = %#v, want key range %#v", diagnostic.Range, fieldRange)
	}
}

func TestCompileFieldWildcardExistenceTruthTable(t *testing.T) {
	t.Parallel()

	present := compileSPL(t, `index=gradethis status=*`)
	if !strings.Contains(present.SQL, `(has("__os_field_names", ?)) AND isNotNull("__os_fields"."status")`) ||
		!strings.Contains(present.SQL, `OR (arrayExists(name -> startsWith(name, ?), "__os_field_names"))`) {
		t.Fatalf("field=* does not include non-null leaves and flattened object parents:\n%s", present.SQL)
	}
	notPresent := compileSPL(t, `index=gradethis status!=*`)
	if !strings.Contains(notPresent.SQL, `AND 0`) {
		t.Fatalf("field!=* should match no events:\n%s", notPresent.SQL)
	}
}

func TestCompileStatsCountUsesTransformingSchemaAndSplunkNullGrouping(t *testing.T) {
	t.Parallel()

	global := compileSPL(t, `index=gradethis | stats count`)
	if !slices.Equal(global.OutputFields, []string{"count"}) {
		t.Fatalf("global output fields = %v", global.OutputFields)
	}
	if !strings.Contains(global.SQL, `count() AS "count"`) || strings.Contains(global.SQL, `GROUP BY`) {
		t.Fatalf("unexpected global count SQL:\n%s", global.SQL)
	}

	grouped := compileSPL(t, `index=gradethis | stats count AS events by level, status`)
	if !slices.Equal(grouped.OutputFields, []string{"level", "status", "events"}) {
		t.Fatalf("grouped output fields = %v", grouped.OutputFields)
	}
	for _, required := range []string{
		`SELECT "__os_group_0" AS "level", "__os_group_1" AS "status", "events"`,
		`"level" AS "__os_group_0"`,
		`AS "__os_group_value_1"`,
		`"__os_group_value_1" AS "__os_group_1"`,
		`count() AS "events"`,
		`OVER () AS "__os_stats_by_any_unsupported"`,
		`(1 AND isNotNull("level"))`,
		`(has("__os_field_names", ?) AND isNotNull("__os_fields"."status"))`,
		`arrayExists(name -> startsWith(name, ?), "__os_field_names")`,
		`GROUP BY "level", "__os_group_value_1"`,
		`if("__os_stats_by_any_unsupported" != 0, throwIf(toUInt8(1)`,
		`ORDER BY "__os_group_0" ASC NULLS LAST, "__os_group_1" ASC NULLS LAST`,
	} {
		if !strings.Contains(grouped.SQL, required) {
			t.Fatalf("grouped stats SQL missing %q:\n%s", required, grouped.SQL)
		}
	}
	outerProjectionEnd := strings.Index(grouped.SQL, " FROM (")
	if outerProjectionEnd < 0 || strings.Contains(grouped.SQL[:outerProjectionEnd], internalSortTimeColumn) {
		t.Fatalf("event sort helper leaked into aggregate projection:\n%s", grouped.SQL)
	}
	if got, want := strings.Count(grouped.SQL, "?"), len(grouped.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, grouped.SQL, grouped.Args)
	}
	if got := grouped.Args[len(grouped.Args)-2:]; !reflect.DeepEqual(got, []any{"status", "status."}) {
		t.Fatalf("dynamic presence arguments = %#v, want [status status.]", got)
	}
	if got := grouped.Args[:2]; !reflect.DeepEqual(got, []any{"status", "status."}) {
		t.Fatalf("dynamic validation arguments = %#v, want [status status.]", got)
	}
	if !strings.Contains(grouped.SQL, UnsupportedStatsByValueMarker) ||
		!strings.Contains(grouped.SQL, `dynamicElement("__os_fields"."status", 'Map(String, String)')`) ||
		strings.Contains(grouped.SQL, `IN ('None',`) ||
		strings.Contains(grouped.SQL, `throwIf(CAST(dynamicType(`) {
		t.Fatalf("dynamic stats group is not guarded as scalar-only:\n%s", grouped.SQL)
	}
}

func TestCompileTimeBinUsesOneStreamingProjection(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis message="Request metrics" | bin _time span=5m | table _time message`)
	if compiled.Timechart != nil {
		t.Fatalf("bin unexpectedly produced timechart metadata: %#v", compiled.Timechart)
	}
	if !slices.Equal(compiled.OutputFields, []string{"_time", "message"}) {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}
	for _, required := range []string{
		`SELECT * REPLACE (fromUnixTimestamp64Nano(`,
		`intDiv(reinterpretAsInt64("_time"), ?) - if(reinterpretAsInt64("_time") < 0 AND reinterpretAsInt64("_time") % ? != 0, 1, 0)`,
		`) * ?`,
		`AS "_time") FROM (`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("bin SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("scoped storage scan occurs %d times, want once:\n%s", got, compiled.SQL)
	}
	if strings.Contains(compiled.SQL, " GROUP BY ") || strings.Contains(compiled.SQL, " MATERIALIZED ") {
		t.Fatalf("streaming bin introduced transforming/materialized work:\n%s", compiled.SQL)
	}
	span := int64(5 * time.Minute)
	if got := compiled.Args[:3]; !reflect.DeepEqual(got, []any{span, span, span}) {
		t.Fatalf("bin prefix args = %#v, want three nanosecond spans; all=%#v", got, compiled.Args)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}

func TestCompileNumericBinUsesExactStreamingArithmetic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		source     string
		output     string
		numberType string
		guarded    bool
		required   []string
	}{
		{
			name:       "signed integer",
			source:     `index=gradethis | eval latency=-11 | bin latency span=10 | table latency`,
			output:     "latency",
			numberType: "Int64",
			guarded:    true,
			required: []string{
				`toInt128("latency")`,
				`intDiv(`,
				`%`,
				`accurateCastOrNull(`,
				`'Int64'`,
			},
		},
		{
			name:       "unsigned integer",
			source:     `index=gradethis | stats count | bin count span=10 | table count`,
			output:     "count",
			numberType: "UInt64",
			required: []string{
				`toUInt64("count")`,
				`intDiv(`,
				` AS UInt64)`,
			},
		},
		{
			name:       "floating point",
			source:     `index=gradethis | eval latency=-11.5 | bin latency span=10 | table latency`,
			output:     "latency",
			numberType: "Float64",
			guarded:    true,
			required: []string{
				`floor(`,
				`toFloat64(`,
				`isFinite(`,
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			compiled := compileSPL(t, test.source)
			if !slices.Equal(compiled.OutputFields, []string{test.output}) {
				t.Fatalf("output fields = %v", compiled.OutputFields)
			}
			for _, required := range test.required {
				if !strings.Contains(compiled.SQL, required) {
					t.Fatalf("%s numeric bin SQL missing %q:\n%s", test.numberType, required, compiled.SQL)
				}
			}
			if got := strings.Contains(compiled.SQL, UnsupportedNumericBinValueMarker); got != test.guarded {
				t.Fatalf("numeric bin marker present = %t, want %t:\n%s", got, test.guarded, compiled.SQL)
			}
			if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
				t.Fatalf("scoped storage scan occurs %d times, want once:\n%s", got, compiled.SQL)
			}
			for _, forbidden := range []string{" GROUP BY ", " MATERIALIZED ", " OVER ("} {
				if strings.Contains(compiled.SQL, forbidden) {
					t.Fatalf("streaming numeric bin introduced %q:\n%s", forbidden, compiled.SQL)
				}
			}
			if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
				t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
			}
		})
	}
}

func TestCompileDynamicNumericBinDispatchesByStoredAndRuntimeType(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | bin metric span=10 AS band | table event_id metric band`)
	if !slices.Equal(compiled.OutputFields, []string{"event_id", "metric", "band"}) {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}
	for _, required := range []string{
		`dynamicType("__os_fields"."metric")`,
		`dynamicElement("__os_fields"."metric", 'Int64')`,
		`dynamicElement("__os_fields"."metric", 'UInt64')`,
		`dynamicElement("__os_fields"."metric", 'Float64')`,
		`dynamicElement("__os_fields"."metric", 'String')`,
		`accurateCastOrNull(`,
		`toInt128(`,
		`toInt256(`,
		`intDiv(`,
		`floor(`,
		`trimBoth(`,
		`toUInt256(`,
		`reinterpretAsInt256(`,
		`'^([+]|-|)[0-9]+$'`,
		`'decimal/v1'`,
		`'bytes/v1'`,
		`'timestamp/v1'`,
		`'duration/v1'`,
		UnsupportedNumericBinValueMarker,
		`AS "__os_numeric_bin_exists_2"`,
		`AS "__os_numeric_bin_type_2"`,
		`AS "__os_numeric_bin_output_exists_2"`,
		`AS "__os_numeric_bin_output_type_2"`,
		`toUInt8("__os_field_metadata_version" = ?) AS "__os_numeric_bin_metadata_version_2"`,
		// Stored metadata written before the current aligned version is never
		// interpreted heuristically, so those rows keep their value instead of
		// failing the search as an out-of-range numeric one.
		`"__os_numeric_bin_metadata_version_2" = 0, CAST("__os_fields"."metric" AS Dynamic)`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("Dynamic numeric bin SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, `'decimal/v1' IN (`) {
		t.Fatalf("tagged decimal was admitted as a pass-through type:\n%s", compiled.SQL)
	}
	// Numeric text becomes the number it spells. A bucket written back as text
	// would be invisible to every downstream numeric predicate.
	if strings.Contains(compiled.SQL, `CAST(toString(`) {
		t.Fatalf("numeric-string bucket was written back as text:\n%s", compiled.SQL)
	}
	// Ordinary text spelled 'NaN' or 'inf' is not force-classified as numeric
	// so that it can never fail an otherwise successful search.
	if strings.Contains(compiled.SQL, `'infinity'`) {
		t.Fatalf("non-finite spellings were pulled into the numeric-string context:\n%s", compiled.SQL)
	}
	// The driver reads a `{name:type}` sequence as a native query parameter, so
	// generated expressions must not introduce braces of their own.
	if strings.ContainsAny(compiled.SQL, "{}") {
		t.Fatalf("Dynamic numeric bin SQL introduced a brace the driver can bind:\n%s", compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("scoped storage scan occurs %d times, want once:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, "arrayFirstIndex("); got != 1 {
		t.Fatalf("Dynamic metadata position is calculated %d times, want once:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `dynamicType("__os_fields"."metric")`); got != 1 {
		t.Fatalf("Dynamic physical type is calculated %d times, want once:\n%s", got, compiled.SQL)
	}
	for _, forbidden := range []string{" GROUP BY ", " MATERIALIZED ", " OVER (", " JOIN "} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("streaming Dynamic numeric bin introduced %q:\n%s", forbidden, compiled.SQL)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
	if got := countArgument(compiled.Args, uint64(10)); got != 1 {
		t.Fatalf("span argument count = %d, want 1: %#v", got, compiled.Args)
	}
}

func TestCompileDynamicNumericBinCarriesSparseMetadataDownstream(t *testing.T) {
	t.Parallel()

	openSchema := compileSPL(t, `index=gradethis | bin metric span=10 AS band`)
	if !slices.Contains(openSchema.OutputFields, "metric") ||
		!slices.Contains(openSchema.OutputFields, "band") ||
		slices.Contains(openSchema.OutputFields, "fields") {
		t.Fatalf(
			"open-schema Dynamic bin fields = %v, want retained source/alias without stale fields payload",
			openSchema.OutputFields,
		)
	}
	if strings.Contains(openSchema.SQL, `"__os_fields" AS "fields"`) {
		t.Fatalf("Dynamic numeric bin exposed a stale immutable fields payload:\n%s", openSchema.SQL)
	}

	compiled := compileSPL(
		t,
		`index=gradethis | bin metric span=10 AS band | where band>=20 | table metric band`,
	)
	for _, required := range []string{
		`"__os_numeric_bin_output_exists_2"`,
		`"__os_numeric_bin_output_type_2"`,
		`dynamicType("__os_filter_bound_3_1")`,
		`AS "band"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("downstream Dynamic numeric bin SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if !slices.Equal(compiled.OutputFields, []string{"metric", "band"}) {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}
	for _, output := range compiled.OutputFields {
		if strings.HasPrefix(output, "__os_numeric_bin_") {
			t.Fatalf("private Dynamic bin metadata leaked into output: %v", compiled.OutputFields)
		}
	}

	collision := compileSPL(
		t,
		`index=gradethis | eval band="stale" | bin metric span=10 AS band | table metric band`,
	)
	if !strings.Contains(collision.SQL, `REPLACE (`) ||
		!strings.Contains(collision.SQL, `AS "band"`) {
		t.Fatalf("Dynamic numeric bin did not overwrite its AS destination:\n%s", collision.SQL)
	}
}

func TestCompileDynamicNumericBinKeepsPriorDestinationWithoutASource(t *testing.T) {
	t.Parallel()

	calculated := compileSPL(
		t,
		`index=gradethis | eval band="stale" | bin metric span=10 AS band | table metric band`,
	)
	for _, required := range []string{
		// An event without the source keeps the destination's prior value,
		// semantic type, and presence instead of losing them to a null write.
		`"__os_numeric_bin_exists_3" = 0 AND "__os_numeric_bin_parent_3" = 0 AND ` +
			`"__os_numeric_bin_physical_type_3" = 'None', CAST("band" AS Dynamic)`,
		`toUInt8(ifNull(1, 0)) AS "__os_numeric_bin_previous_exists_3"`,
		`toUInt8(if("__os_numeric_bin_exists_3" != 0, 1, "__os_numeric_bin_previous_exists_3"))` +
			` AS "__os_numeric_bin_output_exists_3"`,
	} {
		if !strings.Contains(calculated.SQL, required) {
			t.Fatalf("Dynamic numeric bin destroyed its prior destination, missing %q:\n%s", required, calculated.SQL)
		}
	}

	stored := compileSPL(t, `index=gradethis | bin metric span=10 AS band | table metric band`)
	if !strings.Contains(stored.SQL, `= 'None', CAST("__os_fields"."band" AS Dynamic)`) {
		t.Fatalf("Dynamic numeric bin discarded a stored destination value:\n%s", stored.SQL)
	}
	if got, want := strings.Count(stored.SQL, "?"), len(stored.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, stored.SQL, stored.Args)
	}

	replaced := compileSPL(t, `index=gradethis | bin metric span=10 | table metric`)
	if !strings.Contains(replaced.SQL, `= 'None', CAST(NULL AS Dynamic)`) {
		t.Fatalf("source-replacing Dynamic numeric bin did not stay sparse:\n%s", replaced.SQL)
	}
}

func TestCompileDynamicNumericBinClassifiesFromCurrentStageMetadata(t *testing.T) {
	t.Parallel()

	// A stored descendant probe describes the immutable document, so a field
	// that a later stage overwrote with a scalar must not be classified as a
	// flattened object parent.
	compiled := compileSPL(
		t,
		`index=gradethis | rex field=_raw "(?<cap>[0-9]+)" | bin cap span=10 AS band | table event_id cap band`,
	)
	if !strings.Contains(
		compiled.SQL,
		`toUInt8("__os_numeric_bin_type_4" = toUInt8(11)) AS "__os_numeric_bin_parent_4"`,
	) {
		t.Fatalf("Dynamic numeric bin did not derive its container decision from the stage type:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, `"__os_field_names") AS "__os_numeric_bin_parent_4"`) {
		t.Fatalf("Dynamic numeric bin reused a stale stored descendant probe:\n%s", compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}

func TestCompileConsecutiveDynamicNumericBinsKeepPrivateStateStageLocal(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | bin left span=10 AS first | bin right span=7 AS second | table first second`,
	)
	for _, required := range []string{
		`"__os_numeric_bin_exists_2"`,
		`"__os_numeric_bin_type_2"`,
		`"__os_numeric_bin_span_2"`,
		`"__os_numeric_bin_span_3"`,
		`"__os_numeric_bin_exists_3"`,
		`"__os_numeric_bin_type_3"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("consecutive Dynamic numeric bins are missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := countArgument(compiled.Args, uint64(10)); got != 1 {
		t.Fatalf("span 10 argument count = %d, want 1: %#v", got, compiled.Args)
	}
	if got := countArgument(compiled.Args, uint64(7)); got != 1 {
		t.Fatalf("span 7 argument count = %d, want 1: %#v", got, compiled.Args)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}

func TestCompileNumericBinPreservesSourceAndOutputSchemaWithAS(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | eval latency=-11 | bin latency span=10 AS band | table latency band`)
	if !slices.Equal(compiled.OutputFields, []string{"latency", "band"}) {
		t.Fatalf("output fields = %v, want source and alias", compiled.OutputFields)
	}
	if !strings.Contains(compiled.SQL, `SELECT *, `) ||
		!strings.Contains(compiled.SQL, `AS "band"`) {
		t.Fatalf("new alias is not emitted as an added projection:\n%s", compiled.SQL)
	}

	overwritten := compileSPL(t, `index=gradethis | stats count BY level | bin count span=10 AS level | table level count`)
	if !slices.Equal(overwritten.OutputFields, []string{"level", "count"}) {
		t.Fatalf("collision output fields = %v", overwritten.OutputFields)
	}
	if !strings.Contains(overwritten.SQL, `REPLACE (`) || !strings.Contains(overwritten.SQL, `AS "level"`) {
		t.Fatalf("existing destination is not overwritten:\n%s", overwritten.SQL)
	}

	openSchema := compileSPL(t, `index=gradethis | bin severity span=10 AS band`)
	if slices.Contains(openSchema.OutputFields, "fields") ||
		!slices.Contains(openSchema.OutputFields, "severity") ||
		!slices.Contains(openSchema.OutputFields, "band") {
		t.Fatalf("open-schema output fields = %v, want source/alias without stale fields payload", openSchema.OutputFields)
	}
	if strings.Contains(openSchema.SQL, `"__os_fields" AS "fields"`) {
		t.Fatalf("numeric bin exposed an immutable fields payload after a calculated alias:\n%s", openSchema.SQL)
	}
}

func TestCompileConsecutiveNumericBinsKeepStageLocalSpans(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval signed=-11,unsigned=18446744073709551615 | bin signed span=10 | bin unsigned span=7 | table signed unsigned`,
	)
	if got, want := strings.Count(compiled.SQL, UnsupportedNumericBinValueMarker), 1; got != want {
		t.Fatalf("numeric bin guard count = %d, want %d:\n%s", got, want, compiled.SQL)
	}
	if got := countArgument(compiled.Args, uint64(10)); got != 1 {
		t.Fatalf("span 10 argument count = %d, want 1: %#v", got, compiled.Args)
	}
	if got := countArgument(compiled.Args, uint64(7)); got != 1 {
		t.Fatalf("span 7 argument count = %d, want 1: %#v", got, compiled.Args)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}

func TestCompileTimeBinASRetainsCanonicalSource(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | bin _time span=5m AS bucket_time | table _time bucket_time`)
	if !slices.Equal(compiled.OutputFields, []string{"_time", "bucket_time"}) {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}
	if !strings.Contains(compiled.SQL, `AS "bucket_time"`) {
		t.Fatalf("time bin alias is missing:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, `REPLACE (`) {
		t.Fatalf("time bin alias replaced its canonical source:\n%s", compiled.SQL)
	}
	if result := compileSPL(t, `index=gradethis | bin _time span=5m AS bucket_time | timechart span=5m count BY level`); result.Timechart == nil {
		t.Fatal("time bin AS unexpectedly invalidated the canonical source time")
	}
}

func TestCompileNumericBinRejectsUnsupportedFieldKinds(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | bin host span=10`,
		`index=gradethis | eval enabled=true | bin enabled span=10`,
	} {
		logical := buildPlan(t, source)
		_, err := (Compiler{}).Compile(logical)
		var diagnostic *plan.Diagnostic
		if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_UNSUPPORTED_BIN_FIELD_TYPE" {
			t.Fatalf("Compile(%q) error = %v, want SPL_UNSUPPORTED_BIN_FIELD_TYPE", source, err)
		}
	}
}

func TestCompileNumericBinRejectsForgedPlans(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*plan.NumericBucket)
	}{
		{name: "zero span", mutate: func(bucket *plan.NumericBucket) { bucket.Span = 0 }},
		{name: "oversized span", mutate: func(bucket *plan.NumericBucket) {
			bucket.Span = plan.MaximumNumericBinSpan + 1
		}},
		{name: "wrong input metadata", mutate: func(bucket *plan.NumericBucket) {
			bucket.Input.Path = []string{"forged"}
		}},
		{name: "wrong output metadata", mutate: func(bucket *plan.NumericBucket) {
			bucket.Output.Path = []string{"forged"}
		}},
		{name: "time input", mutate: func(bucket *plan.NumericBucket) {
			bucket.Input = plan.FieldRef{Name: "_time", Canonical: true}
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical := buildPlan(t, `index=gradethis | eval latency=-11 | bin latency span=10`)
			bucket := logical.Operators[len(logical.Operators)-1].(*plan.NumericBucket)
			test.mutate(bucket)
			if _, err := (Compiler{}).Compile(logical); err == nil {
				t.Fatal("Compile() succeeded for forged numeric bucket")
			}
		})
	}
}

func TestCompileBucketAliasMatchesBin(t *testing.T) {
	t.Parallel()

	bin := compileSPL(t, `index=gradethis | bin _time span=5m | stats count BY _time`)
	bucket := compileSPL(t, `index=gradethis | bucket span=5m _time | stats count BY _time`)
	if bin.SQL != bucket.SQL || !reflect.DeepEqual(bin.Args, bucket.Args) ||
		!slices.Equal(bin.OutputFields, bucket.OutputFields) {
		t.Fatalf("bucket alias diverged\nbin: %#v\nbucket: %#v", bin, bucket)
	}
}

func TestCompileProjectionPreservesCanonicalTimeProvenance(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | fields message | bin _time span=5m`,
		`index=gradethis | fields - host | bin _time span=5m`,
		`index=gradethis | table _time message | bin _time span=5m`,
		`index=gradethis | fields level | timechart span=5m count BY level`,
		`index=gradethis | fields - host | timechart span=5m count BY level`,
		`index=gradethis | table _time level | timechart span=5m count BY level`,
	} {
		if compiled := compileSPL(t, source); compiled.SQL == "" {
			t.Fatalf("projected canonical time did not compile for %q", source)
		}
	}
}

func TestCompileTimeBinUsesMathematicalPreEpochFloor(t *testing.T) {
	t.Parallel()

	parsed, err := spl.Parse(`index=gradethis | bucket span=5m _time | table _time`)
	if err != nil {
		t.Fatal(err)
	}
	visibility := uint64(1)
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          "tenant-1",
		AuthorizedIndexes: []string{"gradethis"},
		Earliest:          time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC),
		Latest:            time.Date(1970, 1, 1, 0, 0, 0, 1, time.UTC),
		IndexTimeCutoff:   time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC),
		VisibilityCutoff:  &visibility,
	})
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(compiled.SQL,
		`intDiv(reinterpretAsInt64("_time"), ?) - if(reinterpretAsInt64("_time") < 0 AND reinterpretAsInt64("_time") % ? != 0, 1, 0)`) {
		t.Fatalf("pre-epoch floor correction is missing:\n%s", compiled.SQL)
	}
}

func TestCompileTimeBinRejectsBucketBeforeDateTime64Minimum(t *testing.T) {
	t.Parallel()

	parsed, err := spl.Parse(`index=gradethis | bin _time span=7h`)
	if err != nil {
		t.Fatal(err)
	}
	earliest := MinimumSearchTime()
	visibility := uint64(1)
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          "tenant-1",
		AuthorizedIndexes: []string{"gradethis"},
		Earliest:          earliest,
		Latest:            earliest.Add(time.Second),
		IndexTimeCutoff:   time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC),
		VisibilityCutoff:  &visibility,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = (Compiler{}).Compile(logical)
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_UNSUPPORTED_BIN_TIME_RANGE" {
		t.Fatalf("Compile() error = %v, want lower-bound bin diagnostic", err)
	}
}

func TestCompileTimeBinRejectsForgedPlans(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*plan.TimeBucket)
	}{
		{name: "zero span", mutate: func(bucket *plan.TimeBucket) { bucket.Span = 0 }},
		{name: "subsecond span", mutate: func(bucket *plan.TimeBucket) { bucket.Span = time.Millisecond }},
		{name: "day span", mutate: func(bucket *plan.TimeBucket) { bucket.Span = 24 * time.Hour }},
		{name: "oversized span", mutate: func(bucket *plan.TimeBucket) { bucket.Span = 25 * time.Hour }},
		{name: "wrong field", mutate: func(bucket *plan.TimeBucket) {
			bucket.Field = plan.FieldRef{Name: "status", Path: []string{"status"}}
		}},
		{name: "forged metadata", mutate: func(bucket *plan.TimeBucket) {
			bucket.Field.Path = []string{"forged"}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logical := buildPlan(t, `index=gradethis | bin _time span=5m`)
			bucket := logical.Operators[len(logical.Operators)-1].(*plan.TimeBucket)
			test.mutate(bucket)
			if _, err := (Compiler{}).Compile(logical); err == nil {
				t.Fatal("Compile() succeeded for forged time bucket")
			}
		})
	}

	transformed := buildPlan(t, `index=gradethis | stats count`)
	timeField, err := plan.ResolveField("_time", spl.Range{})
	if err != nil {
		t.Fatal(err)
	}
	transformed.Operators = append(transformed.Operators, &plan.TimeBucket{
		Field:  timeField,
		Output: timeField,
		Span:   5 * time.Minute,
	})
	if _, err := (Compiler{}).Compile(transformed); err == nil {
		t.Fatal("Compile() accepted a time bucket after transformed rows")
	}

	bucketed := buildPlan(t, `index=gradethis | bin _time span=5m`)
	timechart := buildPlan(t, `index=gradethis | timechart span=5m count BY level`)
	bucketed.Operators = append(bucketed.Operators, timechart.Operators[len(timechart.Operators)-1])
	bucketed.DynamicOutput = timechart.DynamicOutput
	bucketed.OutputFields = nil
	if _, err := (Compiler{}).Compile(bucketed); err == nil {
		t.Fatal("Compile() accepted timechart after binned canonical time")
	}
}

func TestCompileTimechartUsesOneScopedScanAndPrivateWideTransport(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis message="Request metrics" status>=500 | timechart span=5m count by path`)
	if !slices.Equal(compiled.OutputFields, []string{"_time"}) {
		t.Fatalf("public fixed fields = %v", compiled.OutputFields)
	}
	if compiled.Timechart == nil {
		t.Fatal("compiled timechart metadata is missing")
	}
	if compiled.Timechart.FirstBucket != time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC) ||
		compiled.Timechart.Span != 5*time.Minute || compiled.Timechart.BucketCount != 288 ||
		compiled.Timechart.MaxSeries != 12 || compiled.Timechart.MaxLabelBytes != 256 {
		t.Fatalf("compiled timechart metadata = %#v", compiled.Timechart)
	}
	for _, required := range []string{
		`"__os_timechart_source" AS (`,
		`"__os_timechart_prepared" AS (SELECT *, toUInt8(if("__os_tc_present" != 0, 0, arrayExists(`,
		`"__os_timechart_classified" AS (`,
		`"__os_timechart_canonicalized" AS (`,
		`"__os_timechart_group_counts" AS MATERIALIZED`,
		`"__os_timechart_top" AS MATERIALIZED`,
		`"__os_timechart_normalization_collisions" AS (`,
		`LIMIT 10`,
		`sumIf("__os_tc_count", "__os_tc_kind" = 3)`,
		`HAVING uniqExact("__os_tc_label") > 1`,
		`concat('VALUE', "__os_tc_label")`,
		`"__os_tc_sort_label"`,
		`arrayMap(item -> item.3`,
		`mapFromArrays(`,
		`FROM numbers(?)`,
		`AS "` + TimechartOrdinalColumn + `"`,
		`AS "` + TimechartNamesColumn + `"`,
		`AS "` + TimechartCountsColumn + `"`,
		`AS "` + TimechartInvalidColumn + `"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("timechart SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("scoped storage scan occurs %d times, want once:\n%s", got, compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
	if got := compiled.Args[0]; got != "path" {
		t.Fatalf("dynamic exact-presence argument = %#v, want path before nested scan", got)
	}
	if got := compiled.Args[len(compiled.Args)-5]; got != "path." {
		t.Fatalf("dynamic descendant argument = %#v, want path. after nested scan", got)
	}
	spanNanoseconds := int64(5 * time.Minute)
	wantTail := []any{spanNanoseconds, spanNanoseconds, int64(5_948_640), uint64(288)}
	if got := compiled.Args[len(compiled.Args)-4:]; !reflect.DeepEqual(got, wantTail) {
		t.Fatalf("grid arguments = %#v, want %#v", got, wantTail)
	}
}

func TestCompileTimechartUsesMathematicalPreEpochFloor(t *testing.T) {
	t.Parallel()

	parsed, err := spl.Parse(`index=gradethis | timechart span=5m count by level`)
	if err != nil {
		t.Fatal(err)
	}
	visibility := uint64(1)
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          "tenant-1",
		AuthorizedIndexes: []string{"gradethis"},
		Earliest:          time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC),
		Latest:            time.Date(1970, 1, 1, 0, 0, 0, 1, time.UTC),
		IndexTimeCutoff:   time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC),
		VisibilityCutoff:  &visibility,
	})
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		`reinterpretAsInt64("__os_tc_event_time")`,
		`intDiv("__os_tc_ticks", ?) - if("__os_tc_ticks" < 0 AND "__os_tc_ticks" % ? != 0, 1, 0)`,
		`AS "__os_tc_bucket_number"`,
		`toUInt64(number) AS "` + TimechartOrdinalColumn + `"`,
		`toInt64(?) + toInt64(number) AS "__os_tc_bucket_number"`,
		`LEFT JOIN "__os_timechart_bucket_maps" ON "__os_timechart_bucket_maps"."__os_tc_bucket_number" = "__os_timechart_grid"."__os_tc_bucket_number"`,
		`ORDER BY "__os_timechart_grid"."` + TimechartOrdinalColumn + `" ASC`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("pre-epoch SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, "toStartOfInterval") || strings.Contains(compiled.SQL, "fromUnixTimestamp64Nano") {
		t.Fatalf("timechart converted bucket transport through DateTime64:\n%s", compiled.SQL)
	}
	if compiled.Timechart == nil || compiled.Timechart.FirstBucket != time.Date(1969, 12, 31, 23, 55, 0, 0, time.UTC) || compiled.Timechart.BucketCount != 2 {
		t.Fatalf("pre-epoch metadata = %#v", compiled.Timechart)
	}
	wantTail := []any{int64(5 * time.Minute), int64(5 * time.Minute), int64(-1), uint64(2)}
	if got := compiled.Args[len(compiled.Args)-4:]; !reflect.DeepEqual(got, wantTail) {
		t.Fatalf("pre-epoch bucket arguments = %#v, want %#v", got, wantTail)
	}
}

func TestCompileTimechartKeepsAlignedBucketBeforeDateTime64MinimumAsInteger(t *testing.T) {
	t.Parallel()

	parsed, err := spl.Parse(`index=gradethis | timechart span=5h count by level`)
	if err != nil {
		t.Fatal(err)
	}
	earliest := MinimumSearchTime()
	visibility := uint64(1)
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          "tenant-1",
		AuthorizedIndexes: []string{"gradethis"},
		Earliest:          earliest,
		Latest:            earliest.Add(time.Second),
		IndexTimeCutoff:   time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC),
		VisibilityCutoff:  &visibility,
	})
	if err != nil {
		t.Fatal(err)
	}

	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatal(err)
	}
	if compiled.Timechart == nil || !compiled.Timechart.FirstBucket.Before(MinimumSearchTime()) {
		t.Fatalf("first bucket = %#v, want aligned bucket before DateTime64 minimum", compiled.Timechart)
	}
	if strings.Contains(compiled.SQL, "fromUnixTimestamp64Nano") ||
		strings.Contains(compiled.SQL, `parseDateTime64BestEffort(?)`) {
		t.Fatalf("timechart transported an aligned bucket through DateTime64:\n%s", compiled.SQL)
	}
	firstBucketNumber := compiled.Timechart.FirstBucket.Unix() / int64((5*time.Hour)/time.Second)
	wantTail := []any{int64(5 * time.Hour), int64(5 * time.Hour), firstBucketNumber, uint64(1)}
	if got := compiled.Args[len(compiled.Args)-4:]; !reflect.DeepEqual(got, wantTail) {
		t.Fatalf("lower-bound bucket arguments = %#v, want %#v", got, wantTail)
	}
}

func TestCompileTimechartRejectsSignedBucketGridOverflow(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis | timechart span=5m count by level`)
	operator := logical.Operators[len(logical.Operators)-1].(*plan.Timechart)
	operator.Span = time.Second
	operator.FirstBucket = time.Unix(math.MaxInt64, 0).UTC()
	operator.BucketCount = 2

	if _, err := (Compiler{}).Compile(logical); err == nil || !strings.Contains(err.Error(), "bucket grid overflows") {
		t.Fatalf("Compile() error = %v, want signed bucket-grid overflow rejection", err)
	}
}

func TestCompileTimechartRejectsUnixGridEndOverflow(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis | timechart span=5m count by level`)
	operator := logical.Operators[len(logical.Operators)-1].(*plan.Timechart)
	const spanSeconds = int64((5 * time.Minute) / time.Second)
	operator.FirstBucket = time.Unix(math.MaxInt64-math.MaxInt64%spanSeconds, 0).UTC()
	operator.BucketCount = 2

	if _, err := (Compiler{}).Compile(logical); err == nil || !strings.Contains(err.Error(), "bucket grid overflows") {
		t.Fatalf("Compile() error = %v, want Unix grid-end overflow rejection", err)
	}
}

func TestCompileStatsDetectsFlattenedObjectParentsWithEscapedPaths(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		field string
		want  []any
	}{
		{field: `literal\.dot`, want: []any{`literal\.dot`, `literal\.dot.`}},
		{field: `literal\\slash`, want: []any{`literal\\slash`, `literal\\slash.`}},
	} {
		compiled := compileSPL(t, `index=gradethis | stats count by `+test.field)
		if !strings.Contains(compiled.SQL, `arrayExists(name -> startsWith(name, ?), "__os_field_names")`) {
			t.Fatalf("flattened-object parent detection is missing for %q:\n%s", test.field, compiled.SQL)
		}
		if got := compiled.Args[len(compiled.Args)-2:]; !reflect.DeepEqual(got, test.want) {
			t.Fatalf("escaped dynamic presence arguments for %q = %#v, want %#v", test.field, got, test.want)
		}
	}
}

func TestCompileStatsAliasesReservedEventNames(t *testing.T) {
	t.Parallel()

	for _, alias := range []string{"fields", "_raw"} {
		compiled := compileSPL(t, `index=gradethis | stats count AS `+alias)
		if !slices.Equal(compiled.OutputFields, []string{alias}) {
			t.Fatalf("alias %q output fields = %v", alias, compiled.OutputFields)
		}
		wantPrefix := `SELECT "` + alias + `" FROM (`
		if !strings.HasPrefix(compiled.SQL, wantPrefix) {
			t.Fatalf("alias %q final projection does not select its aggregate output:\n%s", alias, compiled.SQL)
		}
	}
}

func TestCompileStatsHonorsProjectionBoundaries(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | fields host | stats count BY status`,
		`index=gradethis | table host | stats count BY status`,
		`index=gradethis | fields - status | stats count BY status`,
		`index=gradethis | fields host | fields - host | stats count BY status`,
	} {
		compiled := compileSPL(t, source)
		if !slices.Equal(compiled.OutputFields, []string{"status", "count"}) {
			t.Fatalf("%q output fields = %v", source, compiled.OutputFields)
		}
		if !strings.Contains(compiled.SQL, `SELECT "__os_group_0" AS "status", "count"`) ||
			!strings.Contains(compiled.SQL, `CAST(NULL AS Nullable(String)) AS "__os_group_0"`) ||
			!strings.Contains(compiled.SQL, `(0 AND isNotNull(CAST(NULL AS Nullable(String))))`) {
			t.Fatalf("%q did not compile an empty typed aggregate:\n%s", source, compiled.SQL)
		}
		if strings.Contains(compiled.SQL, `"__os_fields"."status"`) {
			t.Fatalf("%q resurrected the projected-away dynamic field:\n%s", source, compiled.SQL)
		}
	}

	retained := compileSPL(t, `index=gradethis | fields status | stats count BY status`)
	if !strings.Contains(retained.SQL, `"__os_fields"."status" AS "status"`) ||
		!strings.Contains(retained.SQL, `AS "__os_group_value_0"`) ||
		!strings.Contains(retained.SQL, `GROUP BY "__os_group_value_0"`) ||
		!strings.Contains(retained.SQL, `dynamicType("__os_fields"."status")`) ||
		strings.Contains(retained.SQL, `CAST(NULL AS Nullable(String)) AS "status"`) {
		t.Fatalf("explicitly retained field was not grouped:\n%s", retained.SQL)
	}
	if strings.Contains(retained.SQL, `AS "__os_group_supported_0"`) {
		t.Fatalf("unused dynamic support alias was materialized:\n%s", retained.SQL)
	}
}

func TestCompileSearchHonorsProjectionBoundaries(t *testing.T) {
	t.Parallel()

	removed := compileSPL(t, `index=gradethis | fields host | search status=500`)
	if !strings.Contains(removed.SQL, `WHERE 0`) || strings.Contains(removed.SQL, `"__os_fields"."status"`) {
		t.Fatalf("search resurrected a projected-away dynamic field:\n%s", removed.SQL)
	}

	retained := compileSPL(t, `index=gradethis | fields status | search status=500`)
	if !strings.Contains(retained.SQL, `"__os_fields"."status" AS "status"`) ||
		!strings.Contains(retained.SQL, `dynamicType("__os_fields"."status")`) ||
		strings.Contains(retained.SQL, `dynamicType("status")`) {
		t.Fatalf("search lost a retained dynamic field's type:\n%s", retained.SQL)
	}
}

func TestCompileRenameDynamicFieldFeedsDownstreamPipeline(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | rename logger AS component | where component="api" | table component`)
	if !slices.Equal(compiled.OutputFields, []string{"component"}) {
		t.Fatalf("output fields = %v, want [component]", compiled.OutputFields)
	}
	if !strings.Contains(compiled.SQL, `"__os_fields"."logger" AS "component"`) {
		t.Fatalf("rename did not alias the dynamic source:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, `"__os_fields"."component"`) {
		t.Fatalf("downstream pipeline resurrected the pre-rename dynamic target:\n%s", compiled.SQL)
	}
	if !slices.Contains(compiled.Args, any("logger")) {
		t.Fatalf("rename source existence argument missing: %#v", compiled.Args)
	}
}

func TestCompileRenameSuppressesStalePublicFieldsPayload(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | rename logger AS component`)
	if slices.Contains(compiled.OutputFields, "fields") {
		t.Fatalf("stale raw fields payload remained public: %v", compiled.OutputFields)
	}
	if !slices.Contains(compiled.OutputFields, "component") {
		t.Fatalf("renamed destination is absent: %v", compiled.OutputFields)
	}
	// The private document must survive the rename stage so an unrelated
	// dynamic field can still be selected later.
	downstream := compileSPL(t, `index=gradethis | rename logger AS component | table component, path`)
	if !slices.Equal(downstream.OutputFields, []string{"component", "path"}) ||
		!strings.Contains(downstream.SQL, `"__os_fields"."path" AS "path"`) {
		t.Fatalf("unrelated dynamic field was not preserved; output=%v\n%s", downstream.OutputFields, downstream.SQL)
	}
	descendants := compileSPL(t, `index=gradethis | rename logger AS component | table logger.child, component.child, path`)
	if strings.Contains(descendants.SQL, `"__os_fields"."logger"."child"`) ||
		strings.Contains(descendants.SQL, `"__os_fields"."component"."child"`) ||
		!strings.Contains(descendants.SQL, `"__os_fields"."path" AS "path"`) {
		t.Fatalf("rename leaked stale source/target descendants or blocked an unrelated field:\n%s", descendants.SQL)
	}
}

func TestCompileRenameTombstonesSurviveExactEvalRedefinition(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | rename logger AS component | eval component="replacement" | table logger.child, component.child`)
	for _, stalePath := range []string{
		`"__os_fields"."logger"."child"`,
		`"__os_fields"."component"."child"`,
	} {
		if strings.Contains(compiled.SQL, stalePath) {
			t.Fatalf("eval resurrected renamed descendant %q:\n%s", stalePath, compiled.SQL)
		}
	}
	if !slices.Equal(compiled.OutputFields, []string{"logger.child", "component.child"}) ||
		strings.Count(compiled.SQL, `CAST(NULL AS Nullable(String))`) < 2 {
		t.Fatalf("renamed descendants were not retained as missing columns; output=%v\n%s", compiled.OutputFields, compiled.SQL)
	}
}

func TestCompileRenameKeepsCanonicalScanPredicatesAuthoritative(t *testing.T) {
	t.Parallel()

	calculatedIndex := compileSPL(t, `index=gradethis | table path | rename path AS index | search index="/manager" | table index`)
	for _, predicate := range []string{
		`"tenant_id" = ?`,
		`"index_name" IN (?)`,
		`"event_time" >= parseDateTime64BestEffort(?, 9, 'UTC')`,
		`"event_time" < parseDateTime64BestEffort(?, 9, 'UTC')`,
		`"index_time" <= parseDateTime64BestEffort(?, 3, 'UTC')`,
		`"visibility_seq" <= ?`,
	} {
		if !strings.Contains(calculatedIndex.SQL, predicate) {
			t.Fatalf("calculated index rename lost scan predicate %q:\n%s", predicate, calculatedIndex.SQL)
		}
	}
	if len(calculatedIndex.Args) < 2 || calculatedIndex.Args[0] != "tenant-1" || calculatedIndex.Args[1] != "gradethis" {
		t.Fatalf("calculated index changed physical scope args: %#v", calculatedIndex.Args)
	}

	calculatedTime := compileSPL(t, `index=gradethis | table path | rename path AS _time | search _time="/manager" | table _time`)
	if !strings.Contains(calculatedTime.SQL, `"event_time" >= parseDateTime64BestEffort(?, 9, 'UTC')`) ||
		!strings.Contains(calculatedTime.SQL, `"path" AS "_time"`) {
		t.Fatalf("calculated _time changed the immutable scan range or lost its value:\n%s", calculatedTime.SQL)
	}
}

func TestCompileRenameBlocksOldNameAndPreservesLeftToRightPairs(t *testing.T) {
	t.Parallel()

	blocked := compileSPL(t, `index=gradethis | rename logger AS component | search logger=api | table component`)
	if !strings.Contains(blocked.SQL, `WHERE 0`) {
		t.Fatalf("search resurrected renamed source:\n%s", blocked.SQL)
	}

	chained := compileSPL(t, `index=gradethis | rename path AS route, route AS endpoint | table endpoint`)
	if !slices.Equal(chained.OutputFields, []string{"endpoint"}) ||
		!strings.Contains(chained.SQL, `"__os_fields"."path" AS "route"`) ||
		!strings.Contains(chained.SQL, `"route" AS "endpoint"`) {
		t.Fatalf("left-to-right rename was not preserved; output=%v\n%s", chained.OutputFields, chained.SQL)
	}
}

func TestCompileRenameOverwriteAndMissingSourceSemantics(t *testing.T) {
	t.Parallel()

	overwrite := compileSPL(t, `index=gradethis | stats count by logger | rename logger AS count`)
	if !slices.Equal(overwrite.OutputFields, []string{"count"}) || !strings.Contains(overwrite.SQL, `"__os_group_0" AS "count"`) {
		t.Fatalf("known target overwrite output=%v\n%s", overwrite.OutputFields, overwrite.SQL)
	}

	missingToExisting := compileSPL(t, `index=gradethis | stats count by logger | rename absent AS count`)
	if !slices.Equal(missingToExisting.OutputFields, []string{"logger", "count"}) ||
		!strings.Contains(missingToExisting.SQL, `CAST(NULL AS Nullable(String)) AS "count"`) {
		t.Fatalf("missing source did not null existing target; output=%v\n%s", missingToExisting.OutputFields, missingToExisting.SQL)
	}

	missingToMissing := compileSPL(t, `index=gradethis | stats count by logger | rename absent AS unknown`)
	if !slices.Equal(missingToMissing.OutputFields, []string{"logger", "count"}) || strings.Contains(missingToMissing.SQL, ` AS "unknown"`) {
		t.Fatalf("missing-to-missing rename was not a no-op; output=%v\n%s", missingToMissing.OutputFields, missingToMissing.SQL)
	}

	dynamicDestination := compileSPL(t, `index=gradethis | fields - logger | rename logger AS path | table path`)
	if !slices.Equal(dynamicDestination.OutputFields, []string{"path"}) ||
		!strings.Contains(dynamicDestination.SQL, `CAST(NULL AS Nullable(String)) AS "path"`) ||
		strings.Contains(dynamicDestination.SQL, `"__os_fields"."path" AS "path"`) {
		t.Fatalf("missing source did not remove a potentially stored dynamic target; output=%v\n%s", dynamicDestination.OutputFields, dynamicDestination.SQL)
	}

	blockedSource := compileSPL(t, `index=gradethis | rename logger AS component | rename logger AS path | table component, path`)
	if !slices.Equal(blockedSource.OutputFields, []string{"component", "path"}) ||
		!strings.Contains(blockedSource.SQL, `CAST(NULL AS Nullable(String)) AS "path"`) ||
		strings.Contains(blockedSource.SQL, `"__os_fields"."path" AS "path"`) {
		t.Fatalf("blocked source resurrected a stored dynamic target; output=%v\n%s", blockedSource.OutputFields, blockedSource.SQL)
	}
}

func TestCompileRenameDriverMetacharactersRemainQuoted(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | table foo?bar | rename foo?bar AS target${x}`)
	if !slices.Equal(compiled.OutputFields, []string{"target${x}"}) {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}
	for _, unsafe := range []string{`"foo?bar"`, `"target${x}"`} {
		if strings.Contains(compiled.SQL, unsafe) {
			t.Fatalf("compiled SQL retained unsafe binder-shaped identifier %q:\n%s", unsafe, compiled.SQL)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d: %#v\n%s", got, want, compiled.Args, compiled.SQL)
	}
}

func TestCompileStatsCountSQLGolden(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | stats count AS events by level`)
	want, err := os.ReadFile("testdata/stats_count_by.golden.sql")
	if err != nil {
		t.Fatalf("read golden SQL: %v", err)
	}
	if got := compiled.SQL; got != strings.TrimSpace(string(want)) {
		t.Fatalf("compiled SQL differs from golden\ngot:\n%s\n\nwant:\n%s", got, want)
	}
}

func TestCompileStatsSupportsDownstreamPipeline(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | stats count AS events by level | search events>1 | sort -events | head 20 | table level, events`)
	if !slices.Equal(compiled.OutputFields, []string{"level", "events"}) {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}
	for _, required := range []string{
		`count() AS "events"`,
		`"__os_group_0" AS "level"`,
		`toInt256("events") > accurateCastOrNull(?, 'Int256')`,
		`"events" AS "__os_order_`,
		` DESC NULLS LAST`,
		` LIMIT ?`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("downstream stats SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
	if got := compiled.Args[len(compiled.Args)-3:]; !reflect.DeepEqual(got, []any{"1", uint64(10_000), uint64(20)}) {
		t.Fatalf("downstream args = %#v", got)
	}
	if strings.Contains(compiled.SQL, `"__os_sort_event_id" AS "__os_order_`) ||
		!strings.Contains(compiled.SQL, `"__os_group_0" AS "__os_order_5_tie_0"`) {
		t.Fatalf("post-stats sort did not use the grouping tuple as its stable tie-breaker:\n%s", compiled.SQL)
	}
}

func TestCompileStatsSupportsImmediateLimitsAndRepeatedAggregation(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | stats count by level | head 2`,
		`index=gradethis | stats count by level | tail 2`,
		`index=gradethis | stats count AS events by level | stats count`,
		`index=gradethis | stats count | head 1 | table count`,
	} {
		compiled := compileSPL(t, source)
		if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
			t.Fatalf("%q placeholder count = %d, args = %d\nSQL: %s", source, got, want, compiled.SQL)
		}
		if strings.Contains(compiled.SQL, `"__os_sort_event_id" AS "__os_order_`) {
			t.Fatalf("%q reused event identity after stats:\n%s", source, compiled.SQL)
		}
	}
}

func TestCompileTopCalculatesPercentBeforeDeterministicLimit(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | top limit=20 message`)
	if !slices.Equal(compiled.OutputFields, []string{"message", "count", "percent"}) {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}
	for _, required := range []string{
		`count() AS "count"`,
		`sum("count") OVER ()`,
		`AS "percent"`,
		`ORDER BY`,
		`DESC NULLS LAST`,
		`LIMIT ?`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("top SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := compiled.Args[len(compiled.Args)-1]; got != uint64(20) {
		t.Fatalf("top limit argument = %#v, want 20", got)
	}
	if strings.Contains(compiled.SQL, "_tie_") {
		t.Fatalf("top repeated its explicit group field as a contradictory tie key:\n%s", compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}

func TestCompileTopLimitZeroAndDownstreamPipeline(t *testing.T) {
	t.Parallel()

	unlimited := compileSPL(t, `index=gradethis | top limit=0 message`)
	if strings.Contains(unlimited.SQL, "LIMIT ?") {
		t.Fatalf("top limit=0 emitted a SQL limit:\n%s", unlimited.SQL)
	}

	downstream := compileSPL(t, `index=gradethis | top message | search percent>=10 | sort -percent | table message, count, percent`)
	if !slices.Equal(downstream.OutputFields, []string{"message", "count", "percent"}) ||
		!strings.Contains(downstream.SQL, `toFloat64("percent") >= toFloat64OrNull(?)`) {
		t.Fatalf("post-top pipeline output=%v\nSQL: %s", downstream.OutputFields, downstream.SQL)
	}
}

func TestCompileRareCalculatesPercentBeforeDeterministicLimit(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | rare limit=2 message`)
	if !slices.Equal(compiled.OutputFields, []string{"message", "count", "percent"}) {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}
	for _, required := range []string{
		`count() AS "count"`,
		`sum("count") OVER ()`,
		`AS "percent"`,
		`ASC NULLS LAST`,
		`DESC NULLS LAST`,
		`LIMIT ?`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("rare SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := compiled.Args[len(compiled.Args)-1]; got != uint64(2) {
		t.Fatalf("rare limit argument = %#v, want 2", got)
	}
	if strings.Contains(compiled.SQL, "_tie_") {
		t.Fatalf("rare repeated its explicit group field as a contradictory tie key:\n%s", compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}

func TestCompileRareLimitZeroAndDownstreamPipeline(t *testing.T) {
	t.Parallel()

	unlimited := compileSPL(t, `index=gradethis | rare limit=0 message`)
	if strings.Contains(unlimited.SQL, "LIMIT ?") {
		t.Fatalf("rare limit=0 emitted a SQL limit:\n%s", unlimited.SQL)
	}

	downstream := compileSPL(t, `index=gradethis | rare message | search percent>=10 | sort -percent | table message, count, percent`)
	if !slices.Equal(downstream.OutputFields, []string{"message", "count", "percent"}) ||
		!strings.Contains(downstream.SQL, `toFloat64("percent") >= toFloat64OrNull(?)`) {
		t.Fatalf("post-rare pipeline output=%v\nSQL: %s", downstream.OutputFields, downstream.SQL)
	}
}

func TestCompilePostStatsProjectionPreservesDeclaredSchemaAndAliases(t *testing.T) {
	t.Parallel()

	tabled := compileSPL(t, `index=gradethis | stats count by level | table missing, count, level`)
	if !slices.Equal(tabled.OutputFields, []string{"missing", "count", "level"}) ||
		!strings.Contains(tabled.SQL, `CAST(NULL AS Nullable(String)) AS "missing"`) {
		t.Fatalf("post-stats table schema = %v\nSQL: %s", tabled.OutputFields, tabled.SQL)
	}

	fieldsAlias := compileSPL(t, `index=gradethis | stats count AS fields | fields - missing`)
	if !slices.Equal(fieldsAlias.OutputFields, []string{"fields"}) {
		t.Fatalf("aggregate fields alias was dropped: %v\nSQL: %s", fieldsAlias.OutputFields, fieldsAlias.SQL)
	}

	global := compileSPL(t, `index=gradethis | stats count | table count`)
	if strings.Contains(global.SQL, `"count" AS "count", "count"`) {
		t.Fatalf("global count was projected twice:\n%s", global.SQL)
	}
}

func TestCompilePostStatsMissingSortUsesAggregateIdentity(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | stats count by level | sort host`)
	if !strings.Contains(compiled.SQL, `CAST(NULL AS Nullable(String)) AS "__os_order_`) ||
		!strings.Contains(compiled.SQL, `"__os_group_0" AS "__os_order_`) ||
		strings.Contains(compiled.SQL, `"host" AS "__os_order_`) {
		t.Fatalf("missing post-stats sort key was not lowered safely:\n%s", compiled.SQL)
	}
}

func TestCompileDynamicStatsGroupRetainsNumericAwareSort(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | stats count by status | sort status`)
	if !strings.Contains(compiled.SQL, `tuple(if(isNull("__os_group_0")`) ||
		!strings.Contains(compiled.SQL, `toFloat64OrNull(toString("__os_group_0"))`) ||
		!strings.Contains(compiled.SQL, `accurateCastOrNull(toString("__os_group_0"), 'Int256')`) {
		t.Fatalf("dynamic stats group lost numeric-aware downstream sort:\n%s", compiled.SQL)
	}
}

func TestCompileDynamicSortUsesExactIntegralTieBreaker(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | sort wide_sort | table event_id`)
	for _, required := range []string{
		`dynamicType("__os_fields"."wide_sort")`,
		`accurateCastOrNull(toString("__os_fields"."wide_sort"), 'Int256')`,
		`ifNotFinite(`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("dynamic sort SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
}

func TestCompilePostStatsIndexAliasUsesAggregateType(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | stats count AS index | search index=1`)
	if !strings.Contains(compiled.SQL, `toInt256("index") = accurateCastOrNull(?, 'Int256')`) {
		t.Fatalf("aggregate alias index retained physical index comparison semantics:\n%s", compiled.SQL)
	}
}

func TestCompileWhereAfterStatsUsesTypedPostAggregatePredicate(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | stats count by status | where count>1 AND status!=500`)
	for _, required := range []string{
		`toInt256("count") > accurateCastOrNull(CAST(? AS Int64), 'Int256')`,
		`"__os_group_0"`,
		`AND`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("where SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, "status!=500") {
		t.Fatalf("where source leaked into SQL:\n%s", compiled.SQL)
	}
}

func TestCompileWhereKeepsEvalStringAndFieldComparisonSemantics(t *testing.T) {
	t.Parallel()

	caseSensitive := compileSPL(t, `index=gradethis | where host="API"`)
	if !strings.Contains(caseSensitive.SQL, `toString("host") = CAST(? AS String)`) || strings.Contains(caseSensitive.SQL, "lowerUTF8") {
		t.Fatalf("where string comparison is not case-sensitive:\n%s", caseSensitive.SQL)
	}

	fieldToField := compileSPL(t, `index=gradethis | where host=source`)
	if !strings.Contains(fieldToField.SQL, `toString("host") = toString("source")`) {
		t.Fatalf("where field comparison was not preserved:\n%s", fieldToField.SQL)
	}

	missingUnderNot := compileSPL(t, `index=gradethis | where NOT absent=1`)
	if !strings.Contains(missingUnderNot.SQL, `CAST(NULL AS Nullable(Bool))`) ||
		!strings.Contains(missingUnderNot.SQL, `NOT (`) {
		t.Fatalf("where NOT lost three-valued missing semantics:\n%s", missingUnderNot.SQL)
	}
}

func TestCompileWhereRejectsFixedBoolCoercion(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | eval one=1 | where one=true`,
		`index=gradethis event_id=one | stats count | where count=true`,
	} {
		compiled := compileSPL(t, source)
		if !strings.Contains(compiled.SQL, `CAST(NULL AS Nullable(Bool))`) {
			t.Fatalf("%q retained numeric/Bool coercion:\n%s", source, compiled.SQL)
		}
	}
}

func TestCompileWhereRejectsOrderedBoolComparisons(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | where true>false`,
		`index=gradethis | eval flag=true | where flag>false`,
		`index=gradethis | where dynamic_flag>false`,
	} {
		compiled := compileSPL(t, source)
		if !strings.Contains(compiled.SQL, `CAST(NULL AS Nullable(Bool))`) ||
			strings.Contains(compiled.SQL, `> CAST(? AS Bool)`) || strings.Contains(compiled.SQL, `Bool') >`) {
			t.Fatalf("%q retained ordered Bool comparison:\n%s", source, compiled.SQL)
		}
	}

	dynamicPair := compileSPL(t, `index=gradethis | where dynamic_flag>other_flag`)
	if strings.Contains(dynamicPair.SQL, `dynamicElement("__os_fields"."dynamic_flag", 'Bool') >`) ||
		!strings.Contains(dynamicPair.SQL, `dynamicType("__os_fields"."dynamic_flag") = 'Bool'`) {
		t.Fatalf("dynamic Bool pair retained ordered comparison:\n%s", dynamicPair.SQL)
	}
}

func TestCompileMaterializedNullOutputsRemainPresent(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | eval x=tonumber("bad") | search x=null`,
		`index=gradethis | stats p95(absent) AS p | search p=null`,
	} {
		compiled := compileSPL(t, source)
		if !strings.Contains(compiled.SQL, `1 AND isNull(`) {
			t.Fatalf("%q conflated a materialized null with a missing field:\n%s", source, compiled.SQL)
		}
	}
}

func TestCompileBaseSearchOrdersStringsLexically(t *testing.T) {
	t.Parallel()

	canonical := compileSPL(t, `index=gradethis host>"a"`)
	if !strings.Contains(canonical.SQL, `lowerUTF8(toString("host")) > lowerUTF8(?)`) {
		t.Fatalf("canonical string ordering is not lexical:\n%s", canonical.SQL)
	}
	dynamic := compileSPL(t, `index=gradethis category>"alpha"`)
	for _, required := range []string{
		`dynamicType("__os_fields"."category") = 'String'`,
		`lowerUTF8(dynamicElement("__os_fields"."category", 'String')) > lowerUTF8(?)`,
	} {
		if !strings.Contains(dynamic.SQL, required) {
			t.Fatalf("dynamic string ordering SQL missing %q:\n%s", required, dynamic.SQL)
		}
	}
}

func TestCompileWhereUsesRuntimeDynamicTypesAndOccurrenceOrderedArguments(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | where unsigned>18446744073709551614`)
	for _, required := range []string{
		`multiIf((dynamicType("__os_fields"."unsigned") IN (`,
		`reinterpretAsInt256(bitNot(`,
		`coalesce(`,
		`accurateCastOrNull(toString("__os_fields"."unsigned"), 'Int256')) > accurateCastOrNull(CAST(? AS UInt64), 'Int256')`,
		`'decimal/v1'`,
		`toFloat64(CAST(? AS UInt64))`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("dynamic integer SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if placeholders := strings.Count(compiled.SQL, "?"); placeholders != len(compiled.Args) {
		t.Fatalf("placeholder count = %d, args = %d: %#v\n%s", placeholders, len(compiled.Args), compiled.Args, compiled.SQL)
	}
	literalCount := 0
	for _, argument := range compiled.Args {
		if argument == uint64(18_446_744_073_709_551_614) {
			literalCount++
		}
	}
	if literalCount != 2 {
		t.Fatalf("wide integer argument occurrences = %d, want 2: %#v", literalCount, compiled.Args)
	}

	fieldToField := compileSPL(t, `index=gradethis | where left>right`)
	if !strings.Contains(fieldToField.SQL, `accurateCastOrNull(toString("__os_fields"."left"), 'Int256')) > coalesce(`) ||
		!strings.Contains(fieldToField.SQL, `accurateCastOrNull(toString("__os_fields"."right"), 'Int256'))`) ||
		!strings.Contains(fieldToField.SQL, `dynamicElement("__os_fields"."left", 'String') > dynamicElement("__os_fields"."right", 'String')`) {
		t.Fatalf("dynamic field comparison is not runtime typed:\n%s", fieldToField.SQL)
	}
}

func TestCompileWhereTreatsCanonicalTimeAsEpochSeconds(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | where _time>1700000000`)
	if !strings.Contains(compiled.SQL, `toFloat64(toUnixTimestamp64Nano("_time")) / 1000000000`) {
		t.Fatalf("where time comparison is not epoch based:\n%s", compiled.SQL)
	}
}

func TestCompileRexUsesOneParameterizedExtractionForAllCaptures(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | rex "method=(?<method>[A-Z]+)\s+path=(?P<path>\S+)" | table method, path`)
	if !slices.Equal(compiled.OutputFields, []string{"method", "path"}) {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}
	if got := strings.Count(compiled.SQL, "extractGroups("); got != 1 {
		t.Fatalf("extractGroups calls = %d, want 1:\n%s", got, compiled.SQL)
	}
	for _, required := range []string{
		`isValidUTF8("_raw")`,
		`CAST([], 'Array(String)')`,
		`arraySum(value -> toUInt64(length(value)),`,
		RexCaptureLimitMarker,
		`arrayElement(`,
		`AS "method"`,
		`AS "path"`,
		`CAST(`,
		` AS Dynamic)`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("rex SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, `AS MATERIALIZED`) {
		t.Fatalf("rex introduced a full-relation materialization fence:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, `(?P<`) || strings.Contains(compiled.SQL, `method=(`) {
		t.Fatalf("regex was interpolated into SQL:\n%s", compiled.SQL)
	}
	patterns := 0
	for _, argument := range compiled.Args {
		if pattern, ok := argument.(string); ok && strings.HasPrefix(pattern, "(?-s)") &&
			strings.Contains(pattern, "method") {
			patterns++
		}
	}
	if patterns != 1 {
		t.Fatalf("bound normalized regex occurrences = %d, want 1: %#v", patterns, compiled.Args)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
	for _, output := range compiled.OutputFields {
		if strings.HasPrefix(output, "__os_rex_") {
			t.Fatalf("private rex helper leaked into output: %v", compiled.OutputFields)
		}
	}
}

func TestCompileRexCarriesQueryWideCaptureBudgetAcrossProjection(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | rex "(?<first>.+)" | table _raw, first | rex "(?<second>.+)" | table first, second`,
	)
	if strings.Count(compiled.SQL, "extractGroups(") != 2 ||
		strings.Contains(compiled.SQL, " AS MATERIALIZED (") ||
		strings.Count(compiled.SQL, RexCaptureLimitMarker) != 2 ||
		!strings.Contains(compiled.SQL, `) + arraySum(value -> toUInt64(length(value)),`) {
		t.Fatalf("rex capture budget was not streamed and accumulated across table:\n%s", compiled.SQL)
	}
}

func TestCompileRexPreservesDestinationsOnNoMatchAndUpdatesSimultaneously(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | rex field=status "^(?<status>\d+)-(?<source>.*)$" | table status, source`)
	if strings.Count(compiled.SQL, "extractGroups(") != 1 ||
		!strings.Contains(compiled.SQL, `"__os_fields"."status"`) ||
		!strings.Contains(compiled.SQL, `"source"`) ||
		!strings.Contains(compiled.SQL, `"__os_rex_exists_`) ||
		!strings.Contains(compiled.SQL, `arrayElement("__os_rex_groups_2", 1)`) ||
		!strings.Contains(compiled.SQL, `arrayElement("__os_rex_groups_2", 2)`) ||
		!strings.Contains(compiled.SQL, `"__os_rex_type_2_0"`) ||
		!strings.Contains(compiled.SQL, `"__os_rex_type_2_1"`) {
		t.Fatalf("rex collision/simultaneous projection is incomplete:\n%s", compiled.SQL)
	}
	if !strings.Contains(compiled.SQL, `notEmpty("__os_rex_groups_`) ||
		!strings.Contains(compiled.SQL, `if("__os_rex_matched_`) {
		t.Fatalf("rex output is not conditional on a whole-pattern match:\n%s", compiled.SQL)
	}
}

func TestCompileRexDoesNotResurrectProjectedSourceAndFeedsDownstreamCommands(t *testing.T) {
	t.Parallel()

	projected := compileSPL(t, `index=gradethis | table message | rex field=duration "(?<value>\d+)" | table value`)
	if strings.Contains(projected.SQL, `"__os_fields"."duration"`) ||
		slices.Contains(projected.Args, "duration") {
		t.Fatalf("projected source was resurrected:\nSQL: %s\nargs: %#v", projected.SQL, projected.Args)
	}

	downstream := compileSPL(t, `index=gradethis | rex "method=(?<method>[A-Z]+)" | where method="POST" | stats count BY method`)
	if !slices.Equal(downstream.OutputFields, []string{"method", "count"}) ||
		!strings.Contains(downstream.SQL, `dynamicElement("method", 'String')`) {
		t.Fatalf("downstream rex field was not resolved as current Dynamic data; output=%v\n%s", downstream.OutputFields, downstream.SQL)
	}
}

func TestCompileRexRejectsForgedInvalidPlans(t *testing.T) {
	t.Parallel()

	base := buildPlan(t, `index=gradethis`)
	raw, err := plan.ResolveField("_raw", spl.Range{})
	if err != nil {
		t.Fatal(err)
	}
	value, err := plan.ResolveField("value", spl.Range{})
	if err != nil {
		t.Fatal(err)
	}
	privateOutput := plan.FieldRef{Name: "__os_fields", Path: []string{"__os_fields"}}
	tests := []plan.Operator{
		&plan.Extract{},
		&plan.Extract{Input: raw, Pattern: `(?-s)(?P<value>x)`, Captures: []plan.ExtractCapture{{Output: value, Group: 0}}},
		&plan.Extract{Input: raw, Pattern: `(?-s)(?P<value>x)`, Captures: []plan.ExtractCapture{{Output: value, Group: 2}}},
		&plan.Extract{Input: raw, Pattern: `(?=x)`, Captures: []plan.ExtractCapture{{Output: value, Group: 1}}},
		&plan.Extract{
			Input: plan.FieldRef{Name: "_raw"}, Pattern: `(?-s)(?P<value>x)`,
			Captures: []plan.ExtractCapture{{Output: value, Group: 1}},
		},
		&plan.Extract{
			Input: raw, Pattern: `(?-s)(?P<__os_fields>x)`,
			Captures: []plan.ExtractCapture{{Output: privateOutput, Group: 1}},
		},
		&plan.Extract{
			Input: raw, Pattern: `(?-s)(?P<value>x)`,
			Captures: []plan.ExtractCapture{{Output: value, Group: 1}, {Output: value, Group: 1}},
		},
	}
	for index, operator := range tests {
		candidate := *base
		candidate.Operators = append(append([]plan.Operator(nil), base.Operators...), operator)
		if _, err := (Compiler{}).Compile(&candidate); err == nil {
			t.Fatalf("forged rex plan %d unexpectedly compiled", index)
		}
	}
}

func TestCompileEvalReplaceToNumberIsNullableAndParameterized(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | eval duration_ms=tonumber(replace(duration, "ms$", "")) | table duration_ms`)
	if !slices.Equal(compiled.OutputFields, []string{"duration_ms"}) {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}
	for _, required := range []string{
		`dynamicElement("__os_fields"."duration", 'String')`,
		`replaceRegexpAll(`,
		`toFloat64OrNull(`,
		`ifNotFinite(`,
		`AS "duration_ms"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("eval SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, "ms$") {
		t.Fatalf("regex literal leaked into SQL:\n%s", compiled.SQL)
	}
	wantPrefix := []any{"duration", "ms$", ""}
	if len(compiled.Args) < len(wantPrefix) || !reflect.DeepEqual(compiled.Args[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("eval arg prefix = %#v, want %#v\nall args: %#v", compiled.Args, wantPrefix, compiled.Args)
	}
}

func TestCompileEvalAssignmentsAreSequentialAndOverwriteWithoutDuplicateColumns(t *testing.T) {
	t.Parallel()

	sequential := compileSPL(t, `index=gradethis | eval first=replace(duration, "ms$", ""), second=tonumber(first) | table second`)
	if strings.Count(sequential.SQL, `AS "first"`) == 0 || !strings.Contains(sequential.SQL, `"first"`) ||
		!strings.Contains(sequential.SQL, `AS "second"`) {
		t.Fatalf("sequential eval aliases are incomplete:\n%s", sequential.SQL)
	}

	overwrite := compileSPL(t, `index=gradethis | eval message=replace(message, "old", "new") | table message`)
	if !strings.Contains(overwrite.SQL, `SELECT * REPLACE (`) || strings.Contains(overwrite.SQL, `*, replaceRegexpAll`) {
		t.Fatalf("existing field was not deliberately replaced:\n%s", overwrite.SQL)
	}
}

func TestCompileEvalLiteralsRetainNativeTypesAndCalculatedIndexSemantics(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | eval signed=-7,unsigned=18446744073709551615,ratio=1.25,ok=true,text="x" | table signed,unsigned,ratio,ok,text`)
	for _, required := range []string{
		`CAST(? AS Int64) AS "signed"`,
		`CAST(? AS UInt64) AS "unsigned"`,
		`CAST(? AS Float64) AS "ratio"`,
		`CAST(? AS Bool) AS "ok"`,
		`CAST(? AS String) AS "text"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("typed eval SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if placeholders := strings.Count(compiled.SQL, "?"); placeholders != len(compiled.Args) {
		t.Fatalf("placeholder count = %d, args = %d: %#v", placeholders, len(compiled.Args), compiled.Args)
	}

	calculatedIndex := compileSPL(t, `index=gradethis | eval index=1 | search index=1`)
	if !strings.Contains(calculatedIndex.SQL, `toInt256("index") = accurateCastOrNull(?, 'Int256')`) {
		t.Fatalf("calculated index retained physical selector semantics:\n%s", calculatedIndex.SQL)
	}
}

func TestCompileEvalRejectsRegexOutsideSafeRE2Subset(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis`)
	output, err := plan.ResolveField("value", spl.Range{})
	if err != nil {
		t.Fatal(err)
	}
	message, err := plan.ResolveField("message", spl.Range{})
	if err != nil {
		t.Fatal(err)
	}
	for _, pattern := range []string{"(?=secret)", "a*"} {
		candidate := *logical
		candidate.Operators = append(append([]plan.Operator(nil), logical.Operators...), &plan.Extend{Assignments: []plan.ExtendAssignment{{
			Output: output,
			Expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionReplace,
				Arguments: []plan.ScalarExpression{
					&plan.ScalarFieldExpression{Field: message},
					&plan.ScalarLiteralExpression{Value: plan.Value{Kind: plan.ValueKindString, String: pattern}},
					&plan.ScalarLiteralExpression{Value: plan.Value{Kind: plan.ValueKindString, String: ""}},
				},
			},
		}}})
		_, err = (Compiler{}).Compile(&candidate)
		if err == nil || !strings.Contains(err.Error(), "regular expression") {
			t.Fatalf("Compile pattern %q error = %v, want safe regex diagnostic", pattern, err)
		}
	}
}

func TestCompileStatsP95UsesBoundedNullableAggregate(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | eval duration_ms=tonumber(replace(duration, "ms$", "")) | stats count p95(duration_ms) AS p95_ms BY path | where p95_ms>500`)
	if !slices.Equal(compiled.OutputFields, []string{"path", "count", "p95_ms"}) {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}
	for _, required := range []string{
		`count() AS "count"`,
		`quantileGKOrNull(100, 0.95)(ifNotFinite(toFloat64("duration_ms"), CAST(NULL AS Nullable(Float64)))) AS "p95_ms"`,
		`toFloat64("p95_ms") > toFloat64(CAST(? AS Int64))`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("p95 SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
}

func TestCompileStatsP95SupportsTimeAndTaggedDecimalAndRejectsNonFiniteText(t *testing.T) {
	t.Parallel()

	timePercentile := compileSPL(t, `index=gradethis | stats p95(_time) AS p95_time`)
	if !strings.Contains(timePercentile.SQL, `toFloat64(toUnixTimestamp64Nano("_time")) / 1000000000`) {
		t.Fatalf("time percentile is not epoch based:\n%s", timePercentile.SQL)
	}

	decimalPercentile := compileSPL(t, `index=gradethis | stats p95(decimal_value) AS p95_decimal`)
	for _, required := range []string{`'decimal/v1'`, `ifNotFinite(toFloat64OrNull(`, `Map(String, String)`} {
		if !strings.Contains(decimalPercentile.SQL, required) {
			t.Fatalf("decimal percentile SQL missing %q:\n%s", required, decimalPercentile.SQL)
		}
	}
}

func TestCompileStatsP95DoesNotResurrectProjectedInput(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | fields - duration | stats count p95(duration) AS p95_ms BY path`)
	if !strings.Contains(compiled.SQL, `quantileGKOrNull(100, 0.95)(CAST(NULL AS Nullable(Float64))) AS "p95_ms"`) {
		t.Fatalf("projected percentile input was not retained as null:\n%s", compiled.SQL)
	}
}

func TestCompileStatsSumAndAverageUseBoundedNumericArraysWithoutRowExpansion(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | stats count sum(amount) AS total avg(amount) AS mean BY service`)
	if !slices.Equal(compiled.OutputFields, []string{"service", "count", "total", "mean"}) {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}
	for _, required := range []string{
		`dynamicElement("__os_fields"."amount", 'Array(Dynamic)')`,
		`AS "__os_measure_values_0"`,
		`sum(length("__os_measure_values_0"))`,
		`sum(arraySum("__os_measure_values_0"))`,
		`AS "total"`,
		`AS "mean"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("sum/avg SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(strings.ToUpper(compiled.SQL), "ARRAY JOIN") {
		t.Fatalf("sum/avg expanded event rows and would corrupt count:\n%s", compiled.SQL)
	}
	if strings.Count(compiled.SQL, `dynamicElement("__os_fields"."amount", 'Array(Dynamic)')`) != 1 ||
		strings.Contains(compiled.SQL, `__os_measure_values_1`) {
		t.Fatalf("sum/avg did not reuse one numeric conversion for the same input:\n%s", compiled.SQL)
	}
}

func TestCompileStatsCountValuesUsesSharedCardinalityWithoutRowExpansion(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats count count(user) AS users count(user) AS users_again count(other) AS others BY service`,
	)
	if !slices.Equal(compiled.OutputFields, []string{"service", "count", "users", "users_again", "others"}) {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}
	for _, required := range []string{
		`dynamicElement("__os_fields"."user", 'Array(Dynamic)')`,
		`arrayCount(element -> dynamicType(element) != 'None'`,
		`AS "__os_measure_count_0"`,
		`AS "__os_measure_count_1"`,
		`toUInt64(sum(toUInt128("__os_measure_count_0"))) AS "users"`,
		`toUInt64(sum(toUInt128("__os_measure_count_0"))) AS "users_again"`,
		`toUInt64(sum(toUInt128("__os_measure_count_1"))) AS "others"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("count(field) SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(strings.ToUpper(compiled.SQL), "ARRAY JOIN") {
		t.Fatalf("count(field) expanded event rows and would corrupt sibling count:\n%s", compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `AS "__os_measure_count_0"`); got != 1 ||
		strings.Contains(compiled.SQL, `__os_measure_count_2`) {
		t.Fatalf("count(field) did not reuse one cardinality conversion per input:\n%s", compiled.SQL)
	}
}

func TestCompileStatsCountValuesSupportsFixedMultivalueAndProjectedInput(t *testing.T) {
	t.Parallel()

	fixed := compileSPL(
		t,
		`index=gradethis | stats values(user) AS users | stats count(users) AS total`,
	)
	if !strings.Contains(fixed.SQL, `toUInt64(length("users")) AS "__os_measure_count_0"`) ||
		!strings.Contains(fixed.SQL, `toUInt64(sum(toUInt128("__os_measure_count_0"))) AS "total"`) {
		t.Fatalf("count(field) did not count fixed multivalue members:\n%s", fixed.SQL)
	}

	projected := compileSPL(
		t,
		`index=gradethis | fields service | stats count(user) AS users BY service`,
	)
	if !strings.Contains(projected.SQL, `toUInt64(0) AS "__os_measure_count_0"`) ||
		strings.Contains(projected.SQL, `"__os_fields"."user"`) {
		t.Fatalf("count(field) resurrected a projected-away input:\n%s", projected.SQL)
	}

	downstream := compileSPL(
		t,
		`index=gradethis | stats count(user) AS users | search users=18446744073709551615`,
	)
	if !strings.Contains(downstream.SQL, `toUInt64(sum(toUInt128("__os_measure_count_0"))) AS "users"`) ||
		!strings.Contains(downstream.SQL, `toInt256("users")`) {
		t.Fatalf("count(field) did not publish a UInt64 measure downstream:\n%s", downstream.SQL)
	}
}

func TestCompileStatsCountValuesCountsFlattenedObjectAsOneOccurrence(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | stats count(object_parent) AS objects`)
	for _, required := range []string{
		`has("__os_field_names", ?)`,
		`arrayExists(name -> startsWith(name, ?), "__os_field_names")`,
		`AS "__os_measure_count_0"`,
		`toUInt64(sum(toUInt128("__os_measure_count_0"))) AS "objects"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("count(object parent) SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, UnsupportedStatsMeasureValueMarker) {
		t.Fatalf("count(field) rejected a present container it can count without interpretation:\n%s", compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}

	for _, source := range []string{
		`index=gradethis | eval copied=object_parent | stats count(copied) AS objects`,
		`index=gradethis | rename object_parent AS copied | stats count(copied) AS objects`,
	} {
		propagated := compileSPL(t, source)
		if !strings.Contains(propagated.SQL, `arrayExists(name -> startsWith(name, ?), "__os_field_names")`) ||
			!strings.Contains(propagated.SQL, `dynamicType("copied") != 'None'`) ||
			!strings.Contains(propagated.SQL, `toUInt64(sum(toUInt128("__os_measure_count_0"))) AS "objects"`) ||
			strings.Contains(propagated.SQL, UnsupportedStatsMeasureValueMarker) {
			t.Fatalf("%q lost flattened-object occurrence provenance:\n%s", source, propagated.SQL)
		}
	}
}

func TestCompileStatsCountValuesCountsStaticNullAndExactTableBoundaries(t *testing.T) {
	t.Parallel()

	nullValue := compileSPL(t, `index=gradethis | eval n=null | stats count(n) AS occurrences`)
	if !strings.Contains(nullValue.SQL, `toUInt64((1) AND isNotNull("n")) AS "__os_measure_count_0"`) ||
		!strings.Contains(nullValue.SQL, `toUInt64(sum(toUInt128("__os_measure_count_0"))) AS "occurrences"`) {
		t.Fatalf("count(null) did not retain a zero-valued UInt64 aggregate:\n%s", nullValue.SQL)
	}

	tabled := compileSPL(
		t,
		`index=gradethis | stats count AS existing | table missing | stats count(missing) AS occurrences`,
	)
	if !strings.Contains(tabled.SQL, `toUInt64((0) AND isNotNull("missing")) AS "__os_measure_count_0"`) ||
		strings.Contains(tabled.SQL, `"__os_fields"."missing"`) {
		t.Fatalf("count(table-projected missing field) resurrected storage:\n%s", tabled.SQL)
	}
}

func TestCompileStatsDistinctCountUsesExactStringArraysWithoutRowExpansion(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | stats count dc(user) AS users distinct_count(user) AS users_again BY service`)
	if !slices.Equal(compiled.OutputFields, []string{"service", "count", "users", "users_again"}) {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}
	sentinel := strconv.FormatUint(MaximumStatsDistinctValuesPerGroup+1, 10)
	for _, required := range []string{
		`dynamicElement("__os_fields"."user", 'Array(Dynamic)')`,
		`AS "__os_measure_strings_0"`,
		`groupUniqArrayArray(` + sentinel + `)("__os_measure_strings_0")`,
		`AS "__os_dc_cardinality_0"`,
		`max(toUInt8("__os_dc_cardinality_0" > toUInt64(`,
		`OVER () AS "__os_stats_dc_any_overflow"`,
		`WHERE throwIf(toUInt8("__os_stats_dc_any_overflow" != 0)`,
		UnsupportedStatsDistinctLimitMarker,
		UnsupportedStatsMeasureValueMarker,
		`"__os_dc_cardinality_0" AS "users"`,
		`"__os_dc_cardinality_0" AS "users_again"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("dc SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(strings.ToUpper(compiled.SQL), "ARRAY JOIN") {
		t.Fatalf("dc expanded event rows and would corrupt count:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, "uniqExact") {
		t.Fatalf("dc uses an unbounded distinct aggregate state:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, "arrayExists(element -> dynamicType(element)") {
		t.Fatalf("dc scans each multivalue separately for conversion and validation:\n%s", compiled.SQL)
	}
	if strings.Count(compiled.SQL, `AS "__os_measure_strings_0"`) != 1 ||
		strings.Contains(compiled.SQL, `__os_measure_strings_1`) {
		t.Fatalf("dc did not reuse one string conversion for the same input:\n%s", compiled.SQL)
	}
	if strings.Index(compiled.SQL, `OVER () AS "__os_stats_dc_any_overflow"`) >
		strings.Index(compiled.SQL, `WHERE throwIf(toUInt8("__os_stats_dc_any_overflow" != 0)`) {
		t.Fatalf("dc overflow validation is not materialized before publication:\n%s", compiled.SQL)
	}
}

func TestCompileStatsDistinctCountValidatesAllGroupsBeforeDownstreamLimit(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats dc(user) AS users BY service | sort 0 +service | head 1`,
	)
	window := strings.Index(compiled.SQL, `OVER () AS "__os_stats_dc_any_overflow"`)
	validation := strings.Index(compiled.SQL, `WHERE throwIf(toUInt8("__os_stats_dc_any_overflow" != 0)`)
	limit := strings.LastIndex(compiled.SQL, "LIMIT ?")
	if window < 0 || validation < 0 || limit < 0 || window > validation || validation > limit {
		t.Fatalf("dc group-wide overflow barrier does not precede downstream LIMIT:\n%s", compiled.SQL)
	}
}

func TestCompileStatsDistinctCountProjectedInputIsZeroAndResultIsUInt64(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | fields service | stats dc(user) AS users BY service | where users>0`)
	sentinel := strconv.FormatUint(MaximumStatsDistinctValuesPerGroup+1, 10)
	for _, required := range []string{
		`groupUniqArrayArray(` + sentinel + `)(CAST([], 'Array(String)'))`,
		`toInt256("users") > accurateCastOrNull(CAST(? AS Int64), 'Int256')`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("projected dc SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
}

func TestCompileStatsValuesUsesOneBoundedExactSetWithLexicalPublication(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats count values(user) AS users dc(user) AS user_count values(user) AS users_again BY service`,
	)
	if !slices.Equal(compiled.OutputFields, []string{"service", "count", "users", "user_count", "users_again"}) {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}
	sentinel := strconv.FormatUint(MaximumStatsValuesPerGroup+1, 10)
	maximumBytes := strconv.FormatUint(MaximumStatsValuesBytesPerGroup, 10)
	for _, required := range []string{
		`dynamicElement("__os_fields"."user", 'Array(Dynamic)')`,
		`AS "__os_measure_strings_0"`,
		`groupUniqArrayArray(` + sentinel + `)("__os_measure_strings_0") AS "__os_exact_strings_0"`,
		`length("__os_exact_strings_0") > toUInt64(`,
		`arrayFold((bytes, value) -> bytes + toUInt128(length(value)), "__os_exact_strings_0", toUInt128(0)) > toUInt128(` + maximumBytes + `)`,
		`arraySort("__os_exact_strings_0") AS "__os_sorted_exact_strings_0"`,
		`OVER () AS "__os_stats_values_any_overflow"`,
		`OVER () AS "__os_stats_values_total_elements"`,
		`OVER () AS "__os_stats_values_bytes_any_overflow"`,
		`OVER () AS "__os_stats_values_total_bytes"`,
		`throwIf(toUInt8("__os_stats_values_any_overflow" != 0 OR "__os_stats_values_total_elements" > toUInt128(`,
		`throwIf(toUInt8("__os_stats_values_bytes_any_overflow" != 0 OR "__os_stats_values_total_bytes" > toUInt128(`,
		StatsValuesLimitMarker,
		StatsValuesBytesLimitMarker,
		`"__os_sorted_exact_strings_0" AS "users"`,
		`toUInt64(length("__os_exact_strings_0")) AS "user_count"`,
		`"__os_sorted_exact_strings_0" AS "users_again"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("values SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(strings.ToUpper(compiled.SQL), "ARRAY JOIN") {
		t.Fatalf("values expanded event rows and would corrupt count:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, "uniqExact") {
		t.Fatalf("values uses an unbounded distinct aggregate state:\n%s", compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `groupUniqArrayArray(`+sentinel+`)("__os_measure_strings_0")`); got != 1 {
		t.Fatalf("values/dc compiled %d exact aggregate states, want one:\n%s", got, compiled.SQL)
	}
	if strings.Count(compiled.SQL, `AS "__os_measure_strings_0"`) != 1 ||
		strings.Contains(compiled.SQL, `__os_measure_strings_1`) {
		t.Fatalf("values/dc did not reuse one string conversion for the same input:\n%s", compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `arraySort("__os_exact_strings_0")`); got != 1 {
		t.Fatalf("values aliases compiled %d lexical sorts, want one:\n%s", got, compiled.SQL)
	}

	dcOnly := compileSPL(t, `index=gradethis | stats dc(user) AS users`)
	dcSentinel := strconv.FormatUint(MaximumStatsDistinctValuesPerGroup+1, 10)
	if !strings.Contains(dcOnly.SQL, `groupUniqArrayArray(`+dcSentinel+`)`) ||
		!strings.Contains(dcOnly.SQL, `)) AS "__os_dc_cardinality_0"`) ||
		strings.Contains(dcOnly.SQL, `AS "__os_exact_strings_0"`) ||
		!strings.Contains(dcOnly.SQL, UnsupportedStatsDistinctLimitMarker) {
		t.Fatalf("dc-only query lost its scalar exact-cardinality path:\n%s", dcOnly.SQL)
	}
}

func TestCompileStatsValuesValidatesEveryGroupBeforeDownstreamLimit(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats values(user) AS users BY service | sort 0 +service | head 1`,
	)
	valuesWindow := strings.Index(compiled.SQL, `OVER () AS "__os_stats_values_any_overflow"`)
	bytesWindow := strings.Index(compiled.SQL, `OVER () AS "__os_stats_values_bytes_any_overflow"`)
	valuesValidation := strings.Index(compiled.SQL, `throwIf(toUInt8("__os_stats_values_any_overflow" != 0 OR`)
	bytesValidation := strings.Index(compiled.SQL, `throwIf(toUInt8("__os_stats_values_bytes_any_overflow" != 0 OR`)
	limit := strings.LastIndex(compiled.SQL, "LIMIT ?")
	if valuesWindow < 0 || bytesWindow < 0 || valuesValidation < 0 || bytesValidation < 0 ||
		limit < 0 || valuesWindow > valuesValidation || bytesWindow > bytesValidation ||
		valuesValidation > limit || bytesValidation > limit {
		t.Fatalf("values group-wide overflow barrier does not precede downstream LIMIT:\n%s", compiled.SQL)
	}
}

func TestCompileStatsValuesProjectedInputIsEmptyAndCanFeedStringStats(t *testing.T) {
	t.Parallel()

	projected := compileSPL(t, `index=gradethis | fields service | stats values(user) AS users BY service`)
	sentinel := strconv.FormatUint(MaximumStatsValuesPerGroup+1, 10)
	if !strings.Contains(projected.SQL, `groupUniqArrayArray(`+sentinel+`)(CAST([], 'Array(String)'))`) {
		t.Fatalf("projected values did not remain an empty list:\n%s", projected.SQL)
	}

	repeated := compileSPL(
		t,
		`index=gradethis | stats values(user) AS users | stats dc(users) AS count_values values(users) AS repeated`,
	)
	if !slices.Equal(repeated.OutputFields, []string{"count_values", "repeated"}) {
		t.Fatalf("repeated values output = %v", repeated.OutputFields)
	}
	if !strings.Contains(repeated.SQL, `"__os_sorted_exact_strings_0" AS "repeated"`) {
		t.Fatalf("values result was not accepted as a top-level multivalue stats input:\n%s", repeated.SQL)
	}
}

func TestCompileStatsValuesPreservesLogicalMultivalueTypeDownstream(t *testing.T) {
	t.Parallel()

	search := compileSPL(
		t,
		`index=gradethis | stats values(user) AS users | search users=ALICE OR users="a*" OR users!=nobody`,
	)
	for _, required := range []string{
		`arrayExists(element -> isValidUTF8(element) AND lowerUTF8(element) = lowerUTF8(?), "users")`,
		`arrayExists(element -> isValidUTF8(element) AND match(element, ?), "users")`,
		`notEmpty("users")`,
	} {
		if !strings.Contains(search.SQL, required) {
			t.Fatalf("multivalue search SQL missing %q:\n%s", required, search.SQL)
		}
	}

	renamed := compileSPL(
		t,
		`index=gradethis | stats values(user) AS users | rename users AS people | search people=* | table people`,
	)
	if !strings.Contains(renamed.SQL, `notEmpty("people")`) ||
		strings.Contains(renamed.SQL, `notEmpty("users") AND isNotNull("people")`) {
		t.Fatalf("renamed values presence was not rebound to the public array:\n%s", renamed.SQL)
	}

	copied := compileSPL(
		t,
		`index=gradethis | stats values(user) AS users | eval people=users | search people=* | table people`,
	)
	if !strings.Contains(copied.SQL, `notEmpty("people")`) {
		t.Fatalf("eval-copied values presence was not rebound to the public array:\n%s", copied.SQL)
	}

	numeric := compileSPL(
		t,
		`index=gradethis | stats values(metric) AS metrics | stats sum(metrics) AS total avg(metrics) AS mean`,
	)
	if !strings.Contains(
		numeric.SQL,
		`arrayMap(element -> ifNotFinite(toFloat64OrNull(element), CAST(NULL AS Nullable(Float64))), "metrics")`,
	) {
		t.Fatalf("sum/avg did not flatten a fixed values result:\n%s", numeric.SQL)
	}
}

func TestCompileStatsValuesRejectsUnpinnedScalarMultivalueConsumers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		source   string
		code     string
		wantText string
	}{
		{name: "where", source: `index=gradethis | stats values(user) AS users | where users="alice"`, code: "SPL_UNSUPPORTED_MULTIVALUE_USAGE", wantText: `users="alice"`},
		{name: "ordered search", source: `index=gradethis | stats values(user) AS users | search users>"alice"`, code: "SPL_UNSUPPORTED_MULTIVALUE_USAGE", wantText: `users>"alice"`},
		{name: "sort", source: `index=gradethis | stats values(user) AS users | sort users`, code: "SPL_UNSUPPORTED_MULTIVALUE_USAGE", wantText: "users"},
		{name: "dedup", source: `index=gradethis | stats values(user) AS users | dedup users`, code: "SPL_UNSUPPORTED_MULTIVALUE_USAGE", wantText: "users"},
		{name: "stats BY", source: `index=gradethis | stats values(user) AS users | stats count BY users`, code: "SPL_UNSUPPORTED_MULTIVALUE_USAGE", wantText: "users"},
		{name: "p95", source: `index=gradethis | stats values(user) AS users | stats p95(users)`, code: "SPL_UNSUPPORTED_MULTIVALUE_USAGE", wantText: "users"},
		{name: "replace", source: `index=gradethis | stats values(user) AS users | eval x=replace(users,"a","b")`, code: "SPL_UNSUPPORTED_MULTIVALUE_USAGE", wantText: `replace(users,"a","b")`},
		{name: "tonumber", source: `index=gradethis | stats values(user) AS users | eval x=tonumber(users)`, code: "SPL_UNSUPPORTED_MULTIVALUE_USAGE", wantText: "tonumber(users)"},
		{name: "rex", source: `index=gradethis | stats values(user) AS users | rex field=users "(?<x>.+)"`, code: "SPL_UNSUPPORTED_MULTIVALUE_USAGE", wantText: "users"},
		{name: "spath", source: `index=gradethis | stats values(user) AS users | spath input=users output=x path=a`, code: "SPL_UNSUPPORTED_MULTIVALUE_USAGE", wantText: "users"},
		{name: "top", source: `index=gradethis | stats values(user) AS users | top users`, code: "SPL_UNSUPPORTED_MULTIVALUE_USAGE", wantText: "users"},
		{name: "bin", source: `index=gradethis | stats values(user) AS users | bin users span=10`, code: "SPL_UNSUPPORTED_BIN_FIELD_TYPE", wantText: "users"},
		{name: "chart row", source: `index=gradethis | stats values(user) AS users | chart count OVER users BY missing`, code: "SPL_UNSUPPORTED_MULTIVALUE_USAGE", wantText: "users"},
		{name: "chart column", source: `index=gradethis | stats values(user) AS users count AS events | chart count OVER events BY users`, code: "SPL_UNSUPPORTED_MULTIVALUE_USAGE", wantText: "users"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := (Compiler{}).Compile(buildPlan(t, test.source))
			diagnostic, ok := err.(*plan.Diagnostic)
			if !ok || diagnostic.Code != test.code {
				t.Fatalf("Compile() error = %#v, want source-located %s", err, test.code)
			}
			if got := test.source[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset]; got != test.wantText {
				t.Fatalf("Compile() diagnostic text = %q, want %q (%#v)", got, test.wantText, diagnostic.Range)
			}
		})
	}
}

func TestCompileRejectsForgedAggregateBoundsAndReservedFieldsInput(t *testing.T) {
	t.Parallel()

	base := buildPlan(t, `index=gradethis`)
	field, err := plan.ResolveField("user", spl.Range{})
	if err != nil {
		t.Fatal(err)
	}
	fields, err := plan.ResolveField("fields", spl.Range{})
	if err != nil {
		t.Fatal(err)
	}
	measures := make([]plan.AggregateMeasure, spl.MaximumStatsMeasures+1)
	for index := range measures {
		measures[index] = plan.AggregateMeasure{
			Function: plan.AggregateFunctionDistinctCount,
			Input:    field,
			Output:   fmt.Sprintf("dc_%d", index),
		}
	}
	groups := make([]plan.FieldRef, spl.MaximumStatsGroupFields+1)
	for index := range groups {
		groups[index], err = plan.ResolveField(fmt.Sprintf("group_%d", index), spl.Range{})
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, aggregate := range []*plan.Aggregate{
		{Measures: measures},
		{GroupBy: groups, Measures: []plan.AggregateMeasure{{Function: plan.AggregateFunctionCountRows, Output: "count"}}},
		{Measures: []plan.AggregateMeasure{{
			Function: plan.AggregateFunctionDistinctCount,
			Input:    fields,
			Output:   "dc_fields",
		}}},
		{Measures: []plan.AggregateMeasure{{
			Function: plan.AggregateFunctionValues,
			Input:    fields,
			Output:   "values_fields",
		}}},
		{Measures: []plan.AggregateMeasure{{
			Function: plan.AggregateFunctionCountValues,
			Input:    fields,
			Output:   "count_fields",
		}}},
		{GroupBy: []plan.FieldRef{fields}, Measures: []plan.AggregateMeasure{{
			Function: plan.AggregateFunctionCountRows,
			Output:   "count",
		}}},
		{Measures: []plan.AggregateMeasure{{
			Function:   plan.AggregateFunctionCountRows,
			Input:      field,
			Percentile: 0.95,
			Output:     "count",
		}}},
		{Measures: []plan.AggregateMeasure{{
			Function: plan.AggregateFunctionCountRows,
			Input: plan.FieldRef{
				Range: spl.Range{Start: spl.Position{Offset: 1}},
			},
			Output: "count",
		}}},
		{Measures: []plan.AggregateMeasure{{
			Function: plan.AggregateFunctionCountValues,
			Output:   "count_user",
		}}},
		{Measures: []plan.AggregateMeasure{{
			Function:   plan.AggregateFunctionCountValues,
			Input:      field,
			Percentile: 0.95,
			Output:     "count_user",
		}}},
		{Measures: []plan.AggregateMeasure{{
			Function: plan.AggregateFunctionCountValues,
			Input: plan.FieldRef{
				Name:      "user",
				Canonical: true,
				Path:      []string{"other"},
			},
			Output: "count_user",
		}}},
		{Measures: []plan.AggregateMeasure{{
			Function:   plan.AggregateFunctionDistinctCount,
			Input:      field,
			Percentile: 0.95,
			Output:     "dc_user",
		}}},
		{Measures: []plan.AggregateMeasure{{
			Function:   plan.AggregateFunctionValues,
			Input:      field,
			Percentile: 0.95,
			Output:     "values_user",
		}}},
		{Measures: []plan.AggregateMeasure{{
			Function: plan.AggregateFunctionValues,
			Output:   "values_user",
		}}},
		{Measures: []plan.AggregateMeasure{{
			Function: plan.AggregateFunctionMinimum,
			Input:    fields,
			Output:   "min_fields",
		}}},
		{Measures: []plan.AggregateMeasure{{
			Function: plan.AggregateFunctionMaximum,
			Output:   "max_user",
		}}},
		{Measures: []plan.AggregateMeasure{{
			Function:   plan.AggregateFunctionMinimum,
			Input:      field,
			Percentile: 0.95,
			Output:     "min_user",
		}}},
		{Measures: []plan.AggregateMeasure{{
			Function: plan.AggregateFunctionMaximum,
			Input: plan.FieldRef{
				Name:      "user",
				Canonical: true,
				Path:      []string{"other"},
			},
			Output: "max_user",
		}}},
	} {
		candidate := *base
		candidate.Operators = append(append([]plan.Operator(nil), base.Operators...), aggregate)
		if _, err := (Compiler{}).Compile(&candidate); err == nil {
			t.Fatalf("Compile accepted forged aggregate %#v", aggregate)
		}
	}
}

func TestCompileStatsNumericInputCachingPreservesPreAggregateArgumentOrder(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | stats sum(request.amount) avg(other.amount) sum(request.amount) AS repeated`)
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
	if len(compiled.Args) < 2 || compiled.Args[0] != "request.amount" || compiled.Args[1] != "other.amount" {
		t.Fatalf("pre-aggregate args = %#v, want request.amount then other.amount before scan args", compiled.Args)
	}
	if strings.Count(compiled.SQL, `AS "__os_measure_values_0"`) != 1 ||
		strings.Count(compiled.SQL, `AS "__os_measure_values_1"`) != 1 ||
		strings.Contains(compiled.SQL, `__os_measure_values_2`) {
		t.Fatalf("numeric input cache aliases are not stable:\n%s", compiled.SQL)
	}
}

func TestCompileStatsMinAndMaxShareOneRuntimeNormalization(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats min(metric) AS low max(metric) AS high min(metric) AS low_again`,
	)
	if !slices.Equal(compiled.OutputFields, []string{"low", "high", "low_again"}) {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}
	for _, required := range []string{
		`AS "__os_measure_strings_0"`,
		`AS "__os_measure_extrema_0"`,
		`argMinArray(arrayMap(candidate -> tupleElement(candidate, 1), "__os_measure_extrema_0")`,
		`argMaxArray(arrayMap(candidate -> tupleElement(candidate, 1), "__os_measure_extrema_0")`,
		`AS "__os_stats_extrema_type_0"`,
		`AS "__os_stats_extrema_type_1"`,
		`AS "__os_stats_extrema_type_2"`,
		`isValidUTF8(value)`,
		`length(value) <= ` + strconv.Itoa(MaximumExactNumericBinTextBytes),
		`ifNotFinite(toFloat64OrNull(`,
		`'^([+]|-|)(([0-9]+([.][0-9]*|))|([.][0-9]+))([eE]([+]|-|)[0-9]+|)$'`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("min/max SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Count(compiled.SQL, `AS "__os_measure_strings_0"`) != 1 ||
		strings.Count(compiled.SQL, `AS "__os_measure_extrema_0"`) != 1 ||
		strings.Contains(compiled.SQL, `__os_measure_extrema_1`) {
		t.Fatalf("min/max did not share one normalization for the same input:\n%s", compiled.SQL)
	}
	if strings.Contains(strings.ToUpper(compiled.SQL), "ARRAY JOIN") {
		t.Fatalf("min/max expanded event rows:\n%s", compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}

	withValues := compileSPL(
		t,
		`index=gradethis | stats min(metric) AS low values(metric) AS all_values`,
	)
	if strings.Count(withValues.SQL, `AS "__os_measure_strings_0"`) != 1 ||
		strings.Contains(withValues.SQL, `__os_measure_strings_1`) {
		t.Fatalf("min and values did not share one canonical String input:\n%s", withValues.SQL)
	}
}

func TestCompileStatsMinAndMaxUseNativeFixedScalarExtrema(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats min(severity) AS low max(_time) AS latest max(level) AS highest`,
	)
	for _, required := range []string{
		`minIfOrNull("severity", (1) AND isNotNull("severity")) AS "low"`,
		`maxIfOrNull("_time", (1) AND isNotNull("_time")) AS "latest"`,
		`argMaxArray(arrayMap(candidate -> tupleElement(candidate, 1), "__os_measure_extrema_0")`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("fixed min/max SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, `toFloat64("severity")`) ||
		strings.Contains(compiled.SQL, `toFloat64("_time")`) {
		t.Fatalf("fixed extrema lost native precision:\n%s", compiled.SQL)
	}
}

func TestCompileStatsRuntimeExtremaRemainTypedDownstream(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | stats min(metric) AS low | rename low AS first | table first`,
		`index=gradethis | stats max(metric) AS high | eval copied=high | table copied`,
		`index=gradethis | stats min(metric) AS low | bin low span=10 | table low`,
		`index=gradethis | stats max(metric) AS high | stats min(high) AS repeated`,
	} {
		compiled := compileSPL(t, source)
		if !strings.Contains(compiled.SQL, `__os_stats_extrema_type_0`) {
			t.Fatalf("runtime extrema lost private semantic type in %q:\n%s", source, compiled.SQL)
		}
	}

	binned := compileSPL(
		t,
		`index=gradethis | stats min(metric) AS low | bin low span=10 | table low`,
	)
	if !strings.Contains(binned.SQL, `toUInt8(1) AS "__os_numeric_bin_metadata_version_`) ||
		strings.Contains(
			binned.SQL,
			`toUInt8("__os_field_metadata_version" = ?) AS "__os_numeric_bin_metadata_version_`,
		) {
		t.Fatalf("transformed extrema bin reused unavailable event metadata:\n%s", binned.SQL)
	}
}

func TestCompileStatsExtremaProjectedInputStaysMissing(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | fields service | stats min(user) AS low max(user) AS high BY service`,
	)
	if !strings.Contains(compiled.SQL, `CAST([], 'Array(String)')`) ||
		!strings.Contains(compiled.SQL, `argMinArray(`) ||
		!strings.Contains(compiled.SQL, `argMaxArray(`) {
		t.Fatalf("projected extrema did not aggregate an empty candidate set:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, `"__os_fields"."user"`) {
		t.Fatalf("projected extrema resurrected the private event field:\n%s", compiled.SQL)
	}
}

func TestCompileStatsRuntimeExtremaRetainContainerGuard(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | stats min(payload) AS low max(payload) AS high`)
	for _, required := range []string{
		UnsupportedStatsMeasureValueMarker,
		`dynamicElement("__os_fields"."payload", 'Array(Dynamic)')`,
		`startsWith(name, ?)`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("runtime extrema SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d", got, want)
	}
}

func TestCompileStatsSumAndAveragePreserveComputedNonFiniteResults(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | stats sum(amount) AS total avg(amount) AS mean`)
	for _, required := range []string{
		`if(sum(length("__os_measure_values_0")) = 0, CAST(NULL AS Nullable(Float64)), toFloat64(sum(arraySum("__os_measure_values_0")))) AS "total"`,
		`if(sum(length("__os_measure_values_0")) = 0, CAST(NULL AS Nullable(Float64)), toFloat64(sum(arraySum("__os_measure_values_0"))) / toFloat64(sum(length("__os_measure_values_0")))) AS "mean"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("sum/avg SQL missing non-finite-preserving expression %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, `ifNotFinite(toFloat64(sum(arraySum(`) {
		t.Fatalf("computed non-finite sum/avg was converted to null:\n%s", compiled.SQL)
	}
}

func TestCompileStatsSumAndAverageAliasesCanReplaceConvenienceColumns(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | stats sum(amount) AS fields avg(amount) AS _raw`)
	if !slices.Equal(compiled.OutputFields, []string{"fields", "_raw"}) ||
		!strings.Contains(compiled.SQL, `AS "fields"`) || !strings.Contains(compiled.SQL, `AS "_raw"`) {
		t.Fatalf("sum/avg aliases output=%v\nSQL: %s", compiled.OutputFields, compiled.SQL)
	}
}

func TestCompileStatsSumAndAverageSupportTimeDownstreamAndRepeatedAggregation(t *testing.T) {
	t.Parallel()

	timeSum := compileSPL(t, `index=gradethis | stats sum(_time) AS total avg(_time) AS mean`)
	if strings.Count(timeSum.SQL, `toFloat64(toUnixTimestamp64Nano("_time")) / 1000000000`) != 1 {
		t.Fatalf("time sum/avg did not share one epoch conversion:\n%s", timeSum.SQL)
	}

	downstream := compileSPL(t, `index=gradethis | stats sum(amount) AS total BY service | where total>30`)
	if !strings.Contains(downstream.SQL, `toFloat64("total") > toFloat64(CAST(? AS Int64))`) {
		t.Fatalf("downstream sum predicate is not numeric:\n%s", downstream.SQL)
	}

	repeated := compileSPL(t, `index=gradethis | stats sum(amount) AS total BY service | stats avg(total) AS mean`)
	if !slices.Equal(repeated.OutputFields, []string{"mean"}) || strings.Count(repeated.SQL, `sum(arraySum(`) != 2 {
		t.Fatalf("repeated sum/avg output=%v\nSQL: %s", repeated.OutputFields, repeated.SQL)
	}
}

func TestCompileStatsSumAndAverageDoNotResurrectProjectedInput(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | fields service | stats count sum(amount) AS total avg(amount) AS mean BY service`)
	if strings.Count(compiled.SQL, `CAST([], 'Array(Float64)') AS "__os_measure_values_`) != 1 {
		t.Fatalf("projected numeric inputs were not materialized as empty arrays:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, `fields.amount`) {
		t.Fatalf("projected numeric input was resurrected:\n%s", compiled.SQL)
	}
}

func TestCompileFixedNumericComparisonsPreserveOutOfRangeOrdering(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis severity<300`,
		`index=gradethis | stats count AS events | search events>-1`,
		`index=gradethis | stats count AS events | search events=18446744073709551615`,
	} {
		compiled := compileSPL(t, source)
		if !strings.Contains(compiled.SQL, `toInt256(`) || !strings.Contains(compiled.SQL, `accurateCastOrNull(?, 'Int256')`) {
			t.Fatalf("%q lost exact wide-integer comparison:\n%s", source, compiled.SQL)
		}
	}
	float := compileSPL(t, `index=gradethis | stats count AS events | search events=1.0`)
	if !strings.Contains(float.SQL, `toFloat64("events") = toFloat64OrNull(?)`) {
		t.Fatalf("floating aggregate comparison was not numerically coerced:\n%s", float.SQL)
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

func TestQuoteIdentifierEscapesSQLAndDriverBindMetacharacters(t *testing.T) {
	t.Parallel()

	if got, want := quoteIdentifier(`a"b\c?d$1{e:f}`), `"a\x22b\x5Cc\x3Fd\x241\x7Be:f\x7D"`; got != want {
		t.Fatalf("quoteIdentifier = %q, want %q", got, want)
	}
	for _, marker := range []string{"?", "$1", "{e:f}"} {
		if strings.Contains(quoteIdentifier(`a"b\c?d$1{e:f}`), marker) {
			t.Fatalf("quoted identifier retained driver marker %q", marker)
		}
	}
}

func TestCompileDriverMetacharacterFieldNamesRemainParameterized(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis foo?bar="value" | eval result?=1 | stats count AS total$1 BY brace{x:y}`)
	for _, marker := range []string{`"foo?bar"`, `"result?"`, `"total$1"`, `"brace{x:y}"`} {
		if strings.Contains(compiled.SQL, marker) {
			t.Fatalf("compiled SQL retained unsafe identifier %q:\n%s", marker, compiled.SQL)
		}
	}
	if placeholders := strings.Count(compiled.SQL, "?"); placeholders != len(compiled.Args) {
		t.Fatalf("placeholder count = %d, args = %d: %#v\n%s", placeholders, len(compiled.Args), compiled.Args, compiled.SQL)
	}
	if !slices.Equal(compiled.OutputFields, []string{"brace{x:y}", "total$1"}) {
		t.Fatalf("logical output fields = %v", compiled.OutputFields)
	}
}

func TestCompileRejectsOversizedGeneratedSQL(t *testing.T) {
	t.Parallel()

	segment := strings.Repeat("?", 245)
	field := strings.Repeat(segment+".", 14) + segment
	var source strings.Builder
	source.WriteString("index=gradethis | where ")
	for index := 0; index < 4; index++ {
		if index > 0 {
			source.WriteString(" AND ")
		}
		source.WriteString(field)
		source.WriteString("=1")
	}
	logical := buildPlan(t, source.String())
	var err error
	_, err = (Compiler{}).Compile(logical)
	diagnostic, ok := err.(*plan.Diagnostic)
	if !ok || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
		t.Fatalf("Compile error = %#v, want SPL_QUERY_TOO_COMPLEX", err)
	}
}

func TestCompileBoundsGeneratedRelationalDepth(t *testing.T) {
	t.Parallel()

	evalPipeline := func(finalAssignments int) string {
		var source strings.Builder
		for command := 0; command < 62; command++ {
			if command > 0 {
				source.WriteByte(' ')
			}
			source.WriteString("| eval f")
			source.WriteString(strconv.Itoa(command))
			source.WriteString("=1")
		}
		source.WriteString(" | eval ")
		for assignment := 0; assignment < finalAssignments; assignment++ {
			if assignment > 0 {
				source.WriteByte(',')
			}
			source.WriteString("tail")
			source.WriteString(strconv.Itoa(assignment))
			source.WriteString("=1")
		}
		if source.Len() >= 16<<10 {
			t.Fatalf("relational-depth fixture is %d bytes, want it below the parser ceiling", source.Len())
		}
		return source.String()
	}

	t.Run("exact boundary", func(t *testing.T) {
		source := evalPipeline(32)
		parsed, err := spl.Parse(source)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		if len(parsed.Commands) != 63 {
			t.Fatalf("pipeline commands = %d, want 63", len(parsed.Commands))
		}
		logical, err := plan.Build(parsed, testChartScope())
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		compiled, err := (Compiler{}).Compile(logical)
		if err != nil {
			t.Fatalf("Compile(exact relational-depth boundary): %v", err)
		}
		if compiled.relationalDepth != maximumCompiledRelationalDepth {
			t.Fatalf(
				"reported relational depth = %d, want %d",
				compiled.relationalDepth,
				maximumCompiledRelationalDepth,
			)
		}
		if depth := strings.Count(compiled.SQL, "SELECT "); depth != maximumCompiledRelationalDepth {
			t.Fatalf("generated relational depth = %d, want %d", depth, maximumCompiledRelationalDepth)
		}
	})

	t.Run("one over is source located", func(t *testing.T) {
		source := evalPipeline(33)
		parsed, err := spl.Parse(source)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		overflowRange := parsed.Commands[len(parsed.Commands)-1].SourceRange()
		logical, err := plan.Build(parsed, testChartScope())
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		_, err = (Compiler{}).Compile(logical)
		var diagnostic *plan.Diagnostic
		if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
			t.Fatalf("Compile(one over relational-depth boundary) error = %#v, want SPL_QUERY_TOO_COMPLEX", err)
		}
		if diagnostic.Range != overflowRange {
			t.Fatalf("relational-depth diagnostic range = %#v, want final eval range %#v", diagnostic.Range, overflowRange)
		}
		if !strings.Contains(diagnostic.Message, "96 relational levels") {
			t.Fatalf("relational-depth diagnostic message = %q, want it to name 96 relational levels", diagnostic.Message)
		}
	})

	t.Run("full parser command budget remains compatible", func(t *testing.T) {
		var source strings.Builder
		for command := 0; command < 64; command++ {
			if command > 0 {
				source.WriteByte(' ')
			}
			source.WriteString("| head 1")
		}
		parsed, err := spl.Parse(source.String())
		if err != nil {
			t.Fatalf("Parse(64-command pipeline): %v", err)
		}
		if len(parsed.Commands) != 64 {
			t.Fatalf("pipeline commands = %d, want the full 64-command budget", len(parsed.Commands))
		}
		logical, err := plan.Build(parsed, testChartScope())
		if err != nil {
			t.Fatalf("Build(64-command pipeline): %v", err)
		}
		compiled, err := (Compiler{}).Compile(logical)
		if err != nil {
			t.Fatalf("Compile(64-command pipeline): %v", err)
		}
		if depth := strings.Count(compiled.SQL, "SELECT "); depth != 66 {
			t.Fatalf("64-command generated relational depth = %d, want 66", depth)
		}
	})
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

func TestCompileScanAliasesPersistedFieldMetadataWithoutPublicExposure(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis`)
	for _, alias := range []string{
		`"field_types" AS "__os_field_types"`,
		`"field_metadata_version" AS "__os_field_metadata_version"`,
	} {
		if strings.Count(compiled.SQL, alias) != 1 {
			t.Fatalf("compiled SQL must contain one scan alias %q:\n%s", alias, compiled.SQL)
		}
	}
	for _, output := range compiled.OutputFields {
		if output == internalFieldTypesColumn || output == internalFieldMetadataVersionColumn {
			t.Fatalf("persisted field metadata leaked into public outputs: %v", compiled.OutputFields)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d: %#v\n%s", got, want, compiled.Args, compiled.SQL)
	}
}

func TestEventPipelinesPreservePersistedFieldMetadataForAnalysis(t *testing.T) {
	t.Parallel()

	const explicitPrivateProjection = `"__os_fields", "__os_field_names", "__os_field_types", "__os_field_metadata_version", "__os_raw_encoding", "__os_sort_time", "__os_sort_event_id"`
	tests := []struct {
		name          string
		source        string
		stageFragment string
	}{
		{name: "include", source: `index=gradethis | fields status`, stageFragment: explicitPrivateProjection},
		{name: "exclude", source: `index=gradethis | fields - trace_id`, stageFragment: explicitPrivateProjection},
		{name: "table", source: `index=gradethis | table status`, stageFragment: explicitPrivateProjection},
		{name: "rename", source: `index=gradethis | rename status AS code`, stageFragment: explicitPrivateProjection},
		{name: "eval", source: `index=gradethis | eval code=status`, stageFragment: `SELECT *, "__os_fields"."status" AS "code"`},
		{name: "dedup", source: `index=gradethis | dedup status`, stageFragment: `SELECT *, toUInt8(`},
		{name: "head", source: `index=gradethis | head 5`, stageFragment: `SELECT * FROM (`},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			logical := buildPlan(t, test.source)
			compiled, err := (Compiler{}).compileEventAnalysis(logical, func(
				relation compiledRelation,
				_ compileState,
				args []any,
				_ *plan.Scan,
				_ int,
			) (CompiledQuery, error) {
				probeSQL := "SELECT " + quoteIdentifier(internalFieldTypesColumn) + ", " +
					quoteIdentifier(internalFieldMetadataVersionColumn) + " FROM (" + relation.sql + ") AS " +
					quoteIdentifier("__os_metadata_probe")
				relation = relation.selectFrom(probeSQL, relation.ownerRange)
				return withCompiledRelationalDepth(CompiledQuery{
					SQL:  relation.sql,
					Args: args,
				}, relation.depth, relation.ownerRange), nil
			})
			if err != nil {
				t.Fatalf("compileEventAnalysis: %v", err)
			}
			if !strings.Contains(compiled.SQL, test.stageFragment) {
				t.Fatalf("compiled SQL does not preserve metadata through %s stage; missing %q:\n%s", test.name, test.stageFragment, compiled.SQL)
			}
			if !strings.HasPrefix(compiled.SQL, `SELECT "__os_field_types", "__os_field_metadata_version" FROM (`) {
				t.Fatalf("analysis finalizer cannot address persisted metadata after %s:\n%s", test.name, compiled.SQL)
			}
			if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
				t.Fatalf("placeholder count = %d, args = %d: %#v\n%s", got, want, compiled.Args, compiled.SQL)
			}
		})
	}
}

func TestCompiledPlaceholderCountMatchesArguments(t *testing.T) {
	t.Parallel()

	queries := []string{
		`index=gradethis trace_id="abc"`,
		`index=gradethis status>=500 | table status | search status!=503`,
		`index=gradethis | sort 25 -status | tail 3`,
		`"connection*refused" | fields _time,message`,
		`index=gradethis | top limit=20 message | search percent>1`,
		`index=gradethis | stats count by status | where count>1`,
	}
	for _, source := range queries {
		compiled := compileSPL(t, source)
		if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
			t.Fatalf("%q placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", source, got, want, compiled.SQL, compiled.Args)
		}
	}
}

func TestCompileRejectsTypedNilOperatorsWithoutPanicking(t *testing.T) {
	t.Parallel()

	var (
		nilScan        *plan.Scan
		nilFilter      *plan.Filter
		nilProject     *plan.Project
		nilExtend      *plan.Extend
		nilExtract     *plan.Extract
		nilRename      *plan.Rename
		nilAggregate   *plan.Aggregate
		nilTimechart   *plan.Timechart
		nilWindow      *plan.Window
		nilSort        *plan.Sort
		nilDeduplicate *plan.Deduplicate
		nilLimit       *plan.Limit
	)
	tests := []struct {
		name     string
		operator plan.Operator
		first    bool
	}{
		{name: "scan", operator: nilScan, first: true},
		{name: "filter", operator: nilFilter},
		{name: "project", operator: nilProject},
		{name: "extend", operator: nilExtend},
		{name: "extract", operator: nilExtract},
		{name: "rename", operator: nilRename},
		{name: "aggregate", operator: nilAggregate},
		{name: "timechart", operator: nilTimechart},
		{name: "window", operator: nilWindow},
		{name: "sort", operator: nilSort},
		{name: "deduplicate", operator: nilDeduplicate},
		{name: "limit", operator: nilLimit},
	}
	base := buildPlan(t, `index=gradethis`)
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			operators := []plan.Operator{base.Operators[0], test.operator}
			if test.first {
				operators = []plan.Operator{test.operator}
			}
			if _, err := (Compiler{}).Compile(&plan.Query{Operators: operators}); err == nil {
				t.Fatal("Compile() accepted a typed-nil operator")
			}
		})
	}
}

func TestAnalysisFinalizerCannotBypassTerminalTimechart(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis | timechart span=5m count BY level`)
	called := false
	_, err := (Compiler{}).compileWithFinalizer(logical, func(compiledRelation, compileState, []any, *plan.Scan, int) (CompiledQuery, error) {
		called = true
		return CompiledQuery{}, nil
	}, false)
	if err == nil || called {
		t.Fatalf("compileWithFinalizer() error = %v, finalizer called = %t", err, called)
	}
}

func TestAnalysisFinalizerCannotBypassTerminalChart(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis | chart count OVER path BY level`)
	called := false
	_, err := (Compiler{}).compileWithFinalizer(logical, func(compiledRelation, compileState, []any, *plan.Scan, int) (CompiledQuery, error) {
		called = true
		return CompiledQuery{}, nil
	}, false)
	if err == nil || called {
		t.Fatalf("compileWithFinalizer() error = %v, finalizer called = %t", err, called)
	}
}

func TestCompileChartUsesOneScopedScanAndBoundedPivotTransport(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis message="Request metrics" | chart count OVER path BY status_class`)
	if !slices.Equal(compiled.OutputFields, []string{"path"}) {
		t.Fatalf("public fixed fields = %v, want the row field only", compiled.OutputFields)
	}
	if compiled.Timechart != nil {
		t.Fatalf("chart declared a timechart contract: %#v", compiled.Timechart)
	}
	if compiled.Chart == nil {
		t.Fatal("compiled chart metadata is missing")
	}
	want := ChartOutput{
		RowField: "path", RowKind: ChartRowKindString, RowDatabaseType: "String",
		RowLimit: 10_000, MaxSeries: 12, MaxLabelBytes: 256,
	}
	if *compiled.Chart != want {
		t.Fatalf("compiled chart metadata = %#v, want %#v", *compiled.Chart, want)
	}
	for _, required := range []string{
		`"__os_chart_source" AS (`,
		`"__os_chart_prepared" AS (SELECT *, toUInt8((("__os_ch_row_exact" != 0 AND isNotNull("__os_ch_row_value")) OR arrayExists(`,
		`"__os_chart_kinded" AS (SELECT *, multiIf(`,
		`"__os_chart_classified" AS (`,
		`FROM "__os_chart_kinded")`,
		`"__os_chart_canonicalized" AS (`,
		`"__os_chart_label_totals" AS MATERIALIZED`,
		`"__os_chart_group_counts" AS MATERIALIZED`,
		`WHERE "__os_ch_row_eligible" != 0 GROUP BY "__os_ch_row", "__os_ch_kind", "__os_ch_encoded"`,
		`"__os_chart_top" AS MATERIALIZED`,
		`"__os_chart_row_domain" AS MATERIALIZED`,
		`"__os_chart_normalization_collisions" AS (`,
		`"__os_chart_column_check" AS (`,
		`ORDER BY "__os_ch_count" DESC, "__os_ch_label" ASC LIMIT 10`,
		`maxOrDefault("__os_ch_kind" = 3)`,
		`max("__os_ch_row_invalid") > 0`,
		`HAVING uniqExact("__os_ch_label") > 1`,
		`concat('VALUE', "__os_ch_label")`,
		`"__os_ch_sort_label"`,
		`arrayMap(item -> item.3`,
		`mapFromArrays(`,
		`row_number() OVER (ORDER BY`,
		`AS "` + ChartOrdinalColumn + `"`,
		`AS "` + ChartRowColumn + `"`,
		`AS "` + ChartNamesColumn + `"`,
		`AS "` + ChartCountsColumn + `"`,
		`AS "` + ChartInvalidColumn + `"`,
		`WHERE throwIf("__os_chart_row_domain"."` + ChartOrdinalColumn + `" >= 10000, '` + ChartRowLimitMarker + `') = 0`,
		`ORDER BY "__os_chart_row_domain"."` + ChartOrdinalColumn + `" ASC`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("chart SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	// Chart has no fixed axis, so it must never borrow timechart's grid.
	for _, forbidden := range []string{"FROM numbers(", "__os_timechart", "__os_tc_"} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("chart SQL contains %q:\n%s", forbidden, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("scoped storage scan occurs %d times, want once:\n%s", got, compiled.SQL)
	}
	// Exactly two aggregations read the scanned rows: the one-dimensional label
	// aggregate that chooses the column domain, then the row-keyed aggregate
	// whose column axis is already collapsed to it. Every later stage reads one
	// of those materialized aggregates instead of the events again.
	if got := strings.Count(compiled.SQL, `FROM "__os_chart_canonicalized"`); got != 2 {
		t.Fatalf("row-level aggregation occurs %d times, want twice:\n%s", got, compiled.SQL)
	}
	// A scalar subquery over a materialized CTE is evaluated during analysis,
	// before the temporary table exists, so each occurrence re-runs the whole
	// scoped scan. Every reference must be an ordinary relation reference.
	for _, materialized := range []string{
		`"__os_chart_label_totals"`,
		`"__os_chart_group_counts"`,
		`"__os_chart_top"`,
		`"__os_chart_normalization_collisions"`,
		`"__os_chart_column_check"`,
		`"__os_chart_row_domain"`,
	} {
		for _, scalar := range []string{
			"(SELECT count() FROM " + materialized,
			"(SELECT count() FROM (SELECT 1 FROM " + materialized,
		} {
			if strings.Contains(compiled.SQL, scalar) {
				t.Fatalf("chart SQL evaluates %s through a scalar subquery:\n%s", materialized, compiled.SQL)
			}
		}
	}
	// The row-keyed aggregation groups on the already-encoded column domain, so
	// its state count is rows x public series rather than rows x raw labels.
	if strings.Contains(compiled.SQL, `count() AS "__os_ch_count" FROM "__os_chart_canonicalized" WHERE "__os_ch_row_eligible" != 0 GROUP BY "__os_ch_row", "__os_ch_kind", "__os_ch_label"`) {
		t.Fatalf("chart SQL groups the row axis on the raw column label:\n%s", compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
	if got := compiled.Args[0]; got != "path" {
		t.Fatalf("row exact-presence argument = %#v, want path before nested scan", got)
	}
	if got := compiled.Args[1]; got != "status_class" {
		t.Fatalf("column exact-presence argument = %#v, want status_class before nested scan", got)
	}
	tail := compiled.Args[len(compiled.Args)-3:]
	if !reflect.DeepEqual(tail, []any{"path.", "status_class.", "path"}) {
		t.Fatalf("trailing arguments = %#v, want descendant probes then the reserved row-column name", tail)
	}
}

// TestCompileChartClassifiesColumnValuesIndependentOfRowPresence pins the
// atomic column-axis rejection against its own presence, the same rule
// compileAggregate applies to every BY key: an unsupported column value must
// fail the whole command even when the same input row omits the row field.
func TestCompileChartClassifiesColumnValuesIndependentOfRowPresence(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | chart count OVER path BY status_class`)
	// The kind is computed over the unfiltered prepared relation ...
	if !strings.Contains(compiled.SQL, `AS "__os_ch_kind" FROM "__os_chart_prepared")`) {
		t.Fatalf("column classification is not row-independent:\n%s", compiled.SQL)
	}
	// ... and no stage between the scan and the kind expression drops rows.
	if strings.Contains(compiled.SQL, `FROM "__os_chart_prepared" WHERE`) {
		t.Fatalf("the classification input is filtered by row eligibility:\n%s", compiled.SQL)
	}
	// The atomic flag reads a signal derived from every classified input row,
	// not only from the row-keyed aggregate.
	for _, required := range []string{
		`"__os_chart_column_check" AS (SELECT toUInt8(maxOrDefault("__os_ch_kind" = 3)) AS "__os_ch_column_invalid" FROM "__os_chart_label_totals")`,
		`"__os_chart_column_check"."__os_ch_column_invalid" != 0`,
		`CROSS JOIN "__os_chart_column_check"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("chart SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d", got, want)
	}
}

// TestCompileChartOverAndByFormsAreIdentical pins S2: the two accepted
// spellings are the same pivot, so they must compile byte for byte.
func TestCompileChartOverAndByFormsAreIdentical(t *testing.T) {
	t.Parallel()

	over := compileSPL(t, `index=gradethis | chart count OVER path BY level`)
	by := compileSPL(t, `index=gradethis | chart count BY path, level`)
	if over.SQL != by.SQL || !reflect.DeepEqual(over.Args, by.Args) || !reflect.DeepEqual(over.Chart, by.Chart) {
		t.Fatalf("OVER and BY forms diverge:\n%s\n%s", over.SQL, by.SQL)
	}
}

// TestCompileChartRowColumnMatchesStatsGroupColumn pins R1/D5/D6: the first
// output column is exactly the group column stats BY publishes for the same
// field, including the deliberate asymmetry that numeric values are legal row
// labels but fatal column labels.
func TestCompileChartRowColumnMatchesStatsGroupColumn(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name         string
		source       string
		kind         ChartRowKind
		databaseType string
		required     string
	}{
		{
			name:         "fixed string column",
			source:       `index=gradethis | chart count OVER level BY path`,
			kind:         ChartRowKindString,
			databaseType: "String",
			required:     `CAST(assumeNotNull("__os_ch_row_value") AS String) AS "__os_ch_row"`,
		},
		{
			name:         "runtime typed event field",
			source:       `index=gradethis | chart count OVER path BY level`,
			kind:         ChartRowKindString,
			databaseType: "String",
			// Runtime scalars converge on the same lexical key that stats BY uses,
			// so integer 500 and string "500" are one row.
			required: `dynamicElement("__os_ch_row_value", 'Map(String, String)')[concat(char(0), 'open_splunk_value')]`,
		},
		{
			name:         "fixed numeric column keeps its own scalar type",
			source:       `index=gradethis | bin severity span=10 | chart count OVER severity BY level`,
			kind:         ChartRowKindUnsigned,
			databaseType: "UInt8",
			required:     `CAST(assumeNotNull("__os_ch_row_value") AS UInt8) AS "__os_ch_row"`,
		},
		{
			name:         "binned timestamp column",
			source:       `index=gradethis | bin _time span=5m AS bucket_time | chart count OVER bucket_time BY level`,
			kind:         ChartRowKindTime,
			databaseType: "DateTime64(9, 'UTC')",
			required:     `CAST(assumeNotNull("__os_ch_row_value") AS DateTime64(9, 'UTC')) AS "__os_ch_row"`,
		},
		{
			name:         "numeric stats output",
			source:       `index=gradethis | stats count BY level | chart count OVER count BY level`,
			kind:         ChartRowKindUnsigned,
			databaseType: "UInt64",
			required:     `CAST(assumeNotNull("__os_ch_row_value") AS UInt64) AS "__os_ch_row"`,
		},
		{
			// stats count BY _raw publishes a Mixed, nullable column because
			// _raw may carry non-UTF-8 bytes, so the pivot's first column
			// declares the same kind over the same String transport.
			name:         "canonical raw column is the Mixed group column",
			source:       `index=gradethis | chart count OVER _raw BY level`,
			kind:         ChartRowKindMixed,
			databaseType: "String",
			required:     `CAST(assumeNotNull("__os_ch_row_value") AS String) AS "__os_ch_row"`,
		},
		{
			// A statically null column is the String group column stats BY
			// publishes: no present, non-null value, so no rows — never an
			// unclassified planning failure.
			name:         "static null literal row axis",
			source:       `index=gradethis | eval n=null | chart count OVER n BY level`,
			kind:         ChartRowKindString,
			databaseType: "String",
			required:     `CAST(assumeNotNull("__os_ch_row_value") AS String) AS "__os_ch_row"`,
		},
		{
			name:         "boolean row axis",
			source:       `index=gradethis | eval flag=true | chart count OVER flag BY level`,
			kind:         ChartRowKindBool,
			databaseType: "Bool",
			required:     `CAST(assumeNotNull("__os_ch_row_value") AS Bool) AS "__os_ch_row"`,
		},
		{
			name:         "signed row axis",
			source:       `index=gradethis | eval offset=-3 | chart count OVER offset BY level`,
			kind:         ChartRowKindSigned,
			databaseType: "Int64",
			required:     `CAST(assumeNotNull("__os_ch_row_value") AS Int64) AS "__os_ch_row"`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled := compileSPL(t, test.source)
			if compiled.Chart == nil || compiled.Chart.RowKind != test.kind || compiled.Chart.RowDatabaseType != test.databaseType {
				t.Fatalf("chart row contract = %#v, want kind %d type %q", compiled.Chart, test.kind, test.databaseType)
			}
			if !strings.Contains(compiled.SQL, test.required) {
				t.Fatalf("chart SQL missing %q:\n%s", test.required, compiled.SQL)
			}
		})
	}
}

// TestCompileChartOrdersRowsLikeAutomaticSort pins O1: rows follow the exact
// order sort 0 +<row field> produces on the published column, so a lexical
// row axis is ordered numerically first and a fixed numeric axis is not.
// TestCompileWideOperatorsDeclareMaterializedCTEs pins the execution
// requirement the bounded runtime-wide lowerings take on by reading their
// aggregates through CTEs declared AS MATERIALIZED. ClickHouse honors that
// declaration only while enable_materialized_cte is on; with it off it inlines
// every reference, re-running the whole scoped scan once per reference and
// exposing an analyzer defect that drops a column whenever a search predicate
// shares expressions with the operator's own projections — for instance a
// comparison on the field that also names the column axis. Carrying the setting
// in the query text keeps the SQL correct on any connection.
func TestCompileWideOperatorsDeclareMaterializedCTEs(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | chart count OVER path BY level`,
		`index=gradethis level="error" | chart count OVER path BY level`,
		`index=gradethis | timechart span=5m count BY level`,
		`index=gradethis level="error" | timechart span=5m count BY level`,
	} {
		compiled := compileSPL(t, source)
		if !strings.Contains(compiled.SQL, " AS MATERIALIZED (") {
			t.Fatalf("Compile(%q) has no materialized aggregate:\n%s", source, compiled.SQL)
		}
		if !strings.HasSuffix(compiled.SQL, " SETTINGS enable_materialized_cte = 1") {
			t.Fatalf("Compile(%q) does not declare the materialized-CTE requirement:\n%s", source, compiled.SQL)
		}
	}
}

func TestCompileChartOrdersRowsLikeAutomaticSort(t *testing.T) {
	t.Parallel()

	lexical := compileSPL(t, `index=gradethis | chart count OVER path BY level`)
	if !strings.Contains(lexical.SQL, `row_number() OVER (ORDER BY tuple(if(isNull("__os_ch_row")`) ||
		!strings.Contains(lexical.SQL, `accurateCastOrNull(toString("__os_ch_row"), 'Int256')`) {
		t.Fatalf("runtime-typed row axis is not ordered numerically:\n%s", lexical.SQL)
	}
	fixed := compileSPL(t, `index=gradethis | bin severity span=10 | chart count OVER severity BY level`)
	if !strings.Contains(fixed.SQL, `row_number() OVER (ORDER BY "__os_ch_row" ASC)`) {
		t.Fatalf("fixed numeric row axis is not ordered by its own value:\n%s", fixed.SQL)
	}
}

// TestCompileChartRejectsNonStringColumnFields pins C2 and the D6 asymmetry:
// the same field type is a legal row axis and a rejected column axis.
func TestCompileChartRejectsNonStringColumnFields(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | chart count OVER level BY severity`,
		`index=gradethis | bin severity span=10 | chart count OVER level BY severity`,
		`index=gradethis | bin _time span=5m AS bucket_time | chart count OVER level BY bucket_time`,
		`index=gradethis | stats count BY level | chart count OVER level BY count`,
	} {
		_, err := (Compiler{}).Compile(buildPlan(t, source))
		diagnostic, ok := err.(*plan.Diagnostic)
		if !ok || diagnostic.Code != "SPL_UNSUPPORTED_CHART_FIELD_TYPE" ||
			diagnostic.Message != "chart column fields currently support strings plus missing and null values" ||
			!slices.Contains(diagnostic.Suggestions, "convert the column field to a string before chart") {
			t.Fatalf("Compile(%q) error = %#v, want a located chart column-type diagnostic", source, err)
		}
		if diagnostic.Range.Start.Offset == 0 {
			t.Fatalf("Compile(%q) diagnostic is not located at the column field: %#v", source, diagnostic.Range)
		}
	}

	// The same fields are legal row axes.
	for _, source := range []string{
		`index=gradethis | bin severity span=10 | chart count OVER severity BY level`,
		`index=gradethis | bin _time span=5m AS bucket_time | chart count OVER bucket_time BY level`,
	} {
		if compiled := compileSPL(t, source); compiled.Chart == nil {
			t.Fatalf("Compile(%q) did not produce a chart contract", source)
		}
	}
}

// TestCompileChartRejectsReservedConvenienceColumn pins S8 for an open event
// schema, where the fields payload is not an ordinary field. The planner
// rejects it first; the compiler repeats the check so a forged plan cannot
// reach the pivot transport with an ambiguous axis.
func TestCompileChartRejectsReservedConvenienceColumn(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | chart count OVER fields BY level`,
		`index=gradethis | chart count OVER level BY fields`,
	} {
		parsed, err := spl.Parse(source)
		if err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
		_, err = plan.Build(parsed, testChartScope())
		diagnostic, ok := err.(*plan.Diagnostic)
		if !ok || diagnostic.Code != "SPL_UNSUPPORTED_CHART_FIELD_TYPE" ||
			!strings.Contains(diagnostic.Message, "reserved fields payload") {
			t.Fatalf("Build(%q) error = %#v, want the reserved-payload rejection", source, err)
		}
	}

	// The compiler repeats every reserved-name rule against a plan that never
	// saw them, so a forged plan cannot publish a colliding public schema.
	for _, forged := range []struct {
		name    string
		axis    string
		message string
	}{
		{"reserved payload", "fields", "reserved fields payload"},
		{"reserved null series", "NULL", "reserved chart series names"},
		{"reserved other series", "OTHER", "reserved chart series names"},
	} {
		t.Run(forged.name, func(t *testing.T) {
			t.Parallel()
			logical := buildPlan(t, `index=gradethis | chart count OVER path BY level`)
			chart := logical.Operators[len(logical.Operators)-1].(*plan.Chart)
			chart.Over = plan.FieldRef{Name: forged.axis, Path: []string{forged.axis}}
			logical.DynamicOutput.FixedFields = []string{forged.axis}
			_, err := (Compiler{}).Compile(logical)
			diagnostic, ok := err.(*plan.Diagnostic)
			if !ok || diagnostic.Code != "SPL_UNSUPPORTED_CHART_FIELD_TYPE" ||
				!strings.Contains(diagnostic.Message, forged.message) {
				t.Fatalf("Compile() error = %#v, want %q", err, forged.message)
			}
		})
	}

	// A closed upstream schema that declares an ordinary fields column may use it.
	compiled := compileSPL(t, `index=gradethis | stats count BY level, path | rename level AS fields | chart count OVER fields BY path`)
	if compiled.Chart == nil || compiled.Chart.RowField != "fields" {
		t.Fatalf("closed-schema fields column was rejected: %#v", compiled.Chart)
	}
}

// TestCompileChartMissingAxesFollowStatsSemantics pins I2 and I3: a removed
// column field becomes the documented NULL series, and a removed row field
// emits the declared one-column schema with no groups.
func TestCompileChartMissingAxesFollowStatsSemantics(t *testing.T) {
	t.Parallel()

	missingColumn := compileSPL(t, `index=gradethis | table path level | fields - level | chart count OVER path BY status`)
	if !strings.Contains(missingColumn.SQL, `CAST(NULL AS Nullable(String)) AS "__os_ch_value"`) ||
		!strings.Contains(missingColumn.SQL, `toUInt8(0) AS "__os_ch_present"`) {
		t.Fatalf("removed column field did not become a typed NULL series:\n%s", missingColumn.SQL)
	}
	missingRow := compileSPL(t, `index=gradethis | table path level | fields - path | chart count OVER path BY level`)
	if !strings.Contains(missingRow.SQL, `CAST(NULL AS Nullable(String)) AS "__os_ch_row_value"`) ||
		!strings.Contains(missingRow.SQL, `toUInt8(0) AS "__os_ch_row_exact"`) {
		t.Fatalf("removed row field did not become an empty group domain:\n%s", missingRow.SQL)
	}
	if !slices.Equal(missingRow.OutputFields, []string{"path"}) {
		t.Fatalf("removed row field changed the declared schema: %v", missingRow.OutputFields)
	}
}

// TestCompileChartRevalidatesTheBoundedContract proves the backend does not
// trust a forged plan: every bound the planner carries as data is re-checked
// before any SQL is emitted.
func TestCompileChartRevalidatesTheBoundedContract(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		corrupt func(*plan.Query, *plan.Chart)
		want    string
	}{
		{"row limit raised", func(_ *plan.Query, chart *plan.Chart) { chart.RowLimit = 10_001 }, "bounded defaults are invalid"},
		{"row limit removed", func(_ *plan.Query, chart *plan.Chart) { chart.RowLimit = 0 }, "bounded defaults are invalid"},
		{"series limit raised", func(_ *plan.Query, chart *plan.Chart) { chart.SeriesLimit = 11 }, "bounded defaults are invalid"},
		{"usenull disabled", func(_ *plan.Query, chart *plan.Chart) { chart.IncludeNull = false }, "bounded defaults are invalid"},
		{"useother disabled", func(_ *plan.Query, chart *plan.Chart) { chart.IncludeOther = false }, "bounded defaults are invalid"},
		{"null label renamed", func(_ *plan.Query, chart *plan.Chart) { chart.NullLabel = "none" }, "bounded defaults are invalid"},
		{"axes collapsed", func(_ *plan.Query, chart *plan.Chart) { chart.SplitBy = chart.Over }, "bounded defaults are invalid"},
		{
			"aggregate replaced",
			func(_ *plan.Query, chart *plan.Chart) { chart.Function = plan.AggregateFunctionSum },
			"count operator is required",
		},
		{
			"declared schema widened",
			func(query *plan.Query, _ *plan.Chart) { query.DynamicOutput.MaxSeries = 24 },
			"bounded defaults are invalid",
		},
		{
			"declared prefix renamed",
			func(query *plan.Query, _ *plan.Chart) { query.DynamicOutput.FixedFields = []string{"_time"} },
			"dynamic output contract is invalid",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			logical := buildPlan(t, `index=gradethis | chart count OVER path BY level`)
			chart := logical.Operators[len(logical.Operators)-1].(*plan.Chart)
			test.corrupt(logical, chart)
			if _, err := (Compiler{}).Compile(logical); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Compile() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestCompileChartRejectsNonTerminalOperator(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis | chart count OVER path BY level`)
	logical.Operators = append(logical.Operators, &plan.Limit{Count: 5})
	if _, err := (Compiler{}).Compile(logical); err == nil || !strings.Contains(err.Error(), "operator must be terminal") {
		t.Fatalf("Compile() error = %v, want terminal-operator rejection", err)
	}
}

// TestCompileChartStaysInsideTheCompiledByteCeiling proves the extra pivot
// stages do not silently push a realistic pipeline past the expansion budget.
func TestCompileChartStaysInsideTheCompiledByteCeiling(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis message="Request metrics" status>=500
| rex field=path "^/api/v1/(?<area>[^/?]+)"
| eval duration_ms=tonumber(replace(duration, "ms$", ""))
| bin duration_ms span=100 AS latency_band
| chart count OVER latency_band BY area`)
	if len(compiled.SQL) > maxCompiledQueryBytes {
		t.Fatalf("compiled chart pipeline is %d bytes, ceiling is %d", len(compiled.SQL), maxCompiledQueryBytes)
	}
	if compiled.Chart == nil {
		t.Fatal("deep chart pipeline lost its chart contract")
	}
}

func TestAnalysisFinalizerCannotBypassCompiledQueryByteLimit(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis`)
	_, err := (Compiler{}).compileEventAnalysis(logical, func(relation compiledRelation, _ compileState, _ []any, _ *plan.Scan, _ int) (CompiledQuery, error) {
		return withCompiledRelationalDepth(
			CompiledQuery{SQL: strings.Repeat("x", maxCompiledQueryBytes+1)},
			relation.depth+1,
			relation.ownerRange,
		), nil
	})
	diagnostic, ok := err.(*plan.Diagnostic)
	if !ok || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
		t.Fatalf("compileEventAnalysis() error = %#v, want SPL_QUERY_TOO_COMPLEX", err)
	}
}

func TestAnalysisFinalizerMustReportCompiledRelationalDepth(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis`)
	_, err := (Compiler{}).compileEventAnalysis(logical, func(compiledRelation, compileState, []any, *plan.Scan, int) (CompiledQuery, error) {
		return CompiledQuery{SQL: "SELECT 1"}, nil
	})
	if err == nil || !strings.Contains(err.Error(), "relational depth was not reported") {
		t.Fatalf("compileEventAnalysis() error = %v, want missing relational-depth evidence", err)
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
	logical, err := plan.Build(parsed, testChartScope())
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return logical
}

func testChartScope() plan.Scope {
	return plan.Scope{
		TenantID:          "tenant-1",
		AuthorizedIndexes: []string{"gradethis"},
		Earliest:          time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC),
		Latest:            time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC),
		IndexTimeCutoff:   time.Date(2026, 7, 22, 0, 0, 1, 0, time.UTC),
		VisibilityCutoff:  uint64Pointer(73),
	}
}

func uint64Pointer(value uint64) *uint64 { return &value }
