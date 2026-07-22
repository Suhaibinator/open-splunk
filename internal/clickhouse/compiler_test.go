package clickhouse

import (
	"os"
	"reflect"
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
		`has("__os_field_names", ?) AND ifNull(multiIf(dynamicType("__os_fields"."status") IN (`,
		`accurateCastOrNull(toString("__os_fields"."status"), 'Int256') >= accurateCastOrNull(?, 'Int256')`,
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
	if !strings.Contains(compiled.SQL, `has("__os_field_names", ?) AND NOT ifNull(multiIf(dynamicType("__os_fields"."status") IN (`) {
		t.Fatalf("!= does not enforce presence:\n%s", compiled.SQL)
	}
}

func TestCompileNOTComparisonIncludesMissingField(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis NOT status=500`)
	if !strings.Contains(compiled.SQL, `NOT ((has("__os_field_names", ?) AND ifNull(multiIf(dynamicType("__os_fields"."status") IN (`) {
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
	if !strings.Contains(present.SQL, `has("__os_field_names", ?) AND isNotNull("__os_fields"."status")`) {
		t.Fatalf("field=* does not require a present non-null value:\n%s", present.SQL)
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
		`AS "` + TimechartBucketColumn + `"`,
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
	if got := compiled.Args[len(compiled.Args)-3]; got != "path." {
		t.Fatalf("dynamic descendant argument = %#v, want path. after nested scan", got)
	}
	wantTail := []any{"2026-07-21 00:00:00.000000000", uint64(288)}
	if got := compiled.Args[len(compiled.Args)-2:]; !reflect.DeepEqual(got, wantTail) {
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
		`"__os_tc_ticks" < 0 AND "__os_tc_ticks" % 300000000000 != 0`,
		`fromUnixTimestamp64Nano(`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("pre-epoch SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, "toStartOfInterval") {
		t.Fatalf("timechart used origin-restricted bucketing:\n%s", compiled.SQL)
	}
	if compiled.Timechart == nil || compiled.Timechart.FirstBucket != time.Date(1969, 12, 31, 23, 55, 0, 0, time.UTC) || compiled.Timechart.BucketCount != 2 {
		t.Fatalf("pre-epoch metadata = %#v", compiled.Timechart)
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
		`multiIf(dynamicType("__os_fields"."unsigned") IN (`,
		`accurateCastOrNull(toString("__os_fields"."unsigned"), 'Int256') > accurateCastOrNull(CAST(? AS UInt64), 'Int256')`,
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
	if !strings.Contains(fieldToField.SQL, `accurateCastOrNull(toString("__os_fields"."left"), 'Int256') > accurateCastOrNull(toString("__os_fields"."right"), 'Int256')`) ||
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
		VisibilityCutoff:  uint64Pointer(73),
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return logical
}

func uint64Pointer(value uint64) *uint64 { return &value }
