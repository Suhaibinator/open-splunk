package clickhouse

import (
	"errors"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileEventStatsMinimumFoldsOneBoundedDynamicInputToScalarWinners(
	t *testing.T,
) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis source="eventstats-min-fixture" | sort 0 +event_id | eventstats min(eventstats_value) AS low | where low=2 | table event_id low`,
	)
	if !slices.Equal(compiled.OutputFields, []string{"event_id", "low"}) {
		t.Fatalf("eventstats min output fields = %#v", compiled.OutputFields)
	}

	resultInputAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_eventstats_result_input_",
	)
	barrierAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_eventstats_result_",
	)
	rowsAlias := eventStatsPrivateAlias(t, compiled.SQL, "__os_eventstats_rows_")
	measureAlias := eventStatsPrivateAlias(t, compiled.SQL, "__os_eventstats_measure_")
	publishedAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_eventstats_published_value_",
	)
	typeAlias := eventStatsPrivateAlias(t, compiled.SQL, "__os_eventstats_extrema_type_")
	validationAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_eventstats_validation_",
	)
	sentinel := `LIMIT ` + strconv.FormatUint(MaximumEventStatsInputRows+1, 10)
	for _, required := range []string{
		resultInputAlias + ` AS MATERIALIZED (`,
		barrierAlias + ` AS (SELECT * FROM ` + resultInputAlias + `)`,
		sentinel,
		`dynamicType("__os_fields"."eventstats_value")`,
		`dynamicElement(__os_eventstats_extrema_field_value, 'Array(Dynamic)')`,
		`arrayFold((__os_eventstats_extrema_state, element) ->`,
		`__os_eventstats_extrema_candidate`,
		`'decimal/v1'`,
		`length(toString(element)) <= ` +
			strconv.Itoa(MaximumExactNumericOrderingInputTextBytes),
		`length(dynamicElement(element, 'Map(String, String)')` +
			`[concat(char(0), 'open_splunk_value')]) <= ` +
			strconv.Itoa(MaximumExactNumericOrderingTextBytes),
		`argMinOrNullIf(tuple(tupleElement(` + measureAlias + `, 2), tupleElement(` +
			measureAlias + `, 3), tupleElement(` + measureAlias + `, 4)), tupleElement(` +
			measureAlias + `, 1), tupleElement(` + measureAlias + `, 5) != 0) OVER ()`,
		`count() OVER ()`,
		`toUInt8(toUInt8(tupleElement(` + measureAlias + `, 6))) AS ` + validationAlias,
		`toUInt8(` + rowsAlias + `.` + validationAlias + `) AS ` + validationAlias,
		`SELECT toUInt8((` + validationAlias + ` != 0)) AS ` +
			quoteIdentifier("__os_chronological_invalid") + ` FROM ` + barrierAlias,
		` AS ` + publishedAlias,
		`.` + publishedAlias + ` AS "low"`,
		`AS "low"`,
		` AS ` + typeAlias,
		UnsupportedStatsMeasureValueMarker,
		EventStatsInputLimitMarker,
		`ORDER BY`,
		materializedCTESettingsSQL,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("eventstats min SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, resultInputAlias+` AS MATERIALIZED (`); got != 1 {
		t.Fatalf("bounded eventstats min result-input definitions = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, ` AS MATERIALIZED (`); got != 1 {
		t.Fatalf("eventstats min materialized CTEs = %d, want only result input:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, sentinel); got != 1 {
		t.Fatalf("eventstats min sentinel limits = %d, want 1:\n%s", got, compiled.SQL)
	}
	barrierAt := strings.Index(compiled.SQL, barrierAlias+` AS (`)
	unsupportedAt := strings.Index(compiled.SQL, UnsupportedStatsMeasureValueMarker)
	if barrierAt < 0 || unsupportedAt <= barrierAt {
		t.Fatalf(
			"eventstats min final validation does not consume the result barrier: barrier=%d final throw=%d:\n%s",
			barrierAt,
			unsupportedAt,
			compiled.SQL,
		)
	}
	if got := strings.Count(compiled.SQL, ` AS `+validationAlias); got != 2 {
		t.Fatalf("eventstats min validation projections = %d, want row-local plus result projection:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `arrayFold(`); got != 1 {
		t.Fatalf("eventstats min Dynamic member folds = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `argMinOrNullIf(`); got != 1 {
		t.Fatalf("eventstats min aggregate count = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, ` AS `+publishedAlias); got != 1 {
		t.Fatalf("eventstats min published-value definitions = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("eventstats min physical scan count = %d, want 1:\n%s", got, compiled.SQL)
	}
	for _, forbidden := range []string{
		"ARRAY JOIN",
		"arrayJoin(",
		"groupArray(",
		"groupUniqArray(",
		"argMinArray(",
		"Array(Tuple(String, String))",
		"arrayFilter(element ->",
		"arrayExists(element ->",
		"arrayMap(__os_stats_extrema_input ->",
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("eventstats min SQL contains row-multiplying %q:\n%s", forbidden, compiled.SQL)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf(
			"eventstats min placeholder count = %d, args = %d\nSQL: %s\nargs: %#v",
			got,
			want,
			compiled.SQL,
			compiled.Args,
		)
	}
}

func TestCompileEventStatsMinimumReusesScalarStringExtremaAndTypeMetadata(
	t *testing.T,
) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eventstats min(service) AS low | eval copied=low | table event_id copied`,
	)
	measureAlias := eventStatsPrivateAlias(t, compiled.SQL, "__os_eventstats_measure_")
	typeAlias := eventStatsPrivateAlias(t, compiled.SQL, "__os_eventstats_extrema_type_")
	for _, required := range []string{
		`CAST(toString("service") AS Nullable(String))`,
		`if(isNotNull(`,
		`argMinOrNullIf(tuple(tupleElement(` + measureAlias + `, 2), tupleElement(` +
			measureAlias + `, 3), tupleElement(` + measureAlias + `, 4)), tupleElement(` +
			measureAlias + `, 1), tupleElement(` + measureAlias + `, 5) != 0)`,
		` AS ` + typeAlias,
		`AS "low"`,
		`AS "copied"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("scalar String eventstats min SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	for _, forbidden := range []string{
		`argMinArray(`,
		`Array(Tuple(String, String))`,
		`toFloat64("service")`,
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("scalar String eventstats min retained %q:\n%s", forbidden, compiled.SQL)
		}
	}
	if strings.Count(compiled.SQL, `argMinOrNullIf(`) != 1 {
		t.Fatalf("scalar String eventstats min duplicated aggregate state:\n%s", compiled.SQL)
	}
	if strings.Count(compiled.SQL, ` AS `+measureAlias) != 1 {
		t.Fatalf("scalar String eventstats min duplicated its candidate:\n%s", compiled.SQL)
	}
	if !slices.Equal(compiled.OutputFields, []string{"event_id", "copied"}) {
		t.Fatalf("copied eventstats min output fields = %#v", compiled.OutputFields)
	}
}

func TestCompileEventStatsMinimumKeepsNativeFixedNumericAndTimeTypes(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		field     string
		output    string
		forbidden []string
	}{
		{
			name:      "UInt8 severity",
			field:     "severity",
			output:    "lowest_severity",
			forbidden: []string{`toFloat64("severity")`, `argMinArray(`, `argMinOrNullIf(`},
		},
		{
			name:      "DateTime64 time",
			field:     "_time",
			output:    "first_time",
			forbidden: []string{`toFloat64("_time")`, `toUnixTimestamp64Nano(`, `argMinArray(`, `argMinOrNullIf(`},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			compiled := compileSPL(
				t,
				`index=gradethis | eventstats min(`+test.field+`) AS `+test.output+
					` | table event_id `+test.output,
			)
			if !strings.Contains(compiled.SQL, `minIfOrNull(`) ||
				!strings.Contains(compiled.SQL, `"`+test.field+`"`) ||
				!strings.Contains(compiled.SQL, `AS "`+test.output+`"`) {
				t.Fatalf("fixed eventstats min lost native %s lowering:\n%s", test.field, compiled.SQL)
			}
			for _, forbidden := range test.forbidden {
				if strings.Contains(compiled.SQL, forbidden) {
					t.Fatalf("fixed eventstats min contains %q:\n%s", forbidden, compiled.SQL)
				}
			}
			if strings.Count(compiled.SQL, `minIfOrNull(`) != 1 {
				t.Fatalf("fixed eventstats min aggregate count != 1:\n%s", compiled.SQL)
			}
		})
	}
}

func TestCompileEventStatsMinimumKeepsSearchScopeBelowTheInputFence(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis source="eventstats-min-fixture" host="web" | eventstats min(eventstats_value) AS low | table event_id low`,
	)
	resultInputAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_eventstats_result_input_",
	)
	preparedAlias := eventStatsPrivateAlias(t, compiled.SQL, "__os_eventstats_prepared_")
	inputStart := strings.Index(compiled.SQL, resultInputAlias+` AS MATERIALIZED (`)
	if inputStart < 0 {
		t.Fatalf("eventstats min materialized result input is missing:\n%s", compiled.SQL)
	}
	sentinel := `LIMIT ` + strconv.FormatUint(MaximumEventStatsInputRows+1, 10)
	limitOffset := strings.Index(compiled.SQL[inputStart:], sentinel)
	if limitOffset < 0 {
		t.Fatalf("eventstats min input fence is missing:\n%s", compiled.SQL)
	}
	boundedInput := compiled.SQL[inputStart : inputStart+limitOffset]
	scanAt := strings.Index(boundedInput, `FROM "open_splunk"."events"`)
	whereAt := strings.Index(boundedInput, `WHERE "tenant_id" = ?`)
	if scanAt < 0 || whereAt <= scanAt {
		t.Fatalf(
			"eventstats min source scan or scope is outside the bounded input: scan=%d where=%d\n%s",
			scanAt,
			whereAt,
			compiled.SQL,
		)
	}
	for _, predicate := range []string{
		`WHERE "tenant_id" = ? AND "index_name" IN (?)`,
		`"event_time" >= parseDateTime64BestEffort(?, 9, 'UTC')`,
		`"event_time" < parseDateTime64BestEffort(?, 9, 'UTC')`,
		`"index_time" <= parseDateTime64BestEffort(?, 3, 'UTC')`,
		`"expires_at" > parseDateTime64BestEffort(?, 3, 'UTC')`,
		`"visibility_seq" <= ?`,
		`lowerUTF8(toString("source")) = lowerUTF8(?)`,
		`lowerUTF8(toString("host")) = lowerUTF8(?)`,
	} {
		if !strings.Contains(boundedInput, predicate) {
			t.Fatalf(
				"eventstats min scope predicate %q escaped the bounded input:\n%s",
				predicate,
				compiled.SQL,
			)
		}
	}
	if !strings.Contains(
		compiled.SQL[inputStart+limitOffset:],
		sentinel+`) AS `+preparedAlias,
	) {
		t.Fatalf("eventstats min prepared window does not consume the bounded input:\n%s", compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, sentinel); got != 1 {
		t.Fatalf("eventstats min scope sentinel limits = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("eventstats min scope physical scans = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d", got, want)
	}
}

func TestCompileEventStatsMinimumUsesOneGroupedAggregateAndPreservesPresence(
	t *testing.T,
) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eventstats min(eventstats_value) AS low BY eventstats_group | search low=* | sort 0 +event_id | table event_id eventstats_group low`,
	)
	resultInputAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_eventstats_result_input_",
	)
	barrierAlias := eventStatsPrivateAlias(t, compiled.SQL, "__os_eventstats_result_")
	rowsAlias := eventStatsPrivateAlias(t, compiled.SQL, "__os_eventstats_rows_")
	existsAlias := eventStatsPrivateAlias(t, compiled.SQL, "__os_eventstats_exists_")
	eligibleAlias := eventStatsPrivateAlias(t, compiled.SQL, "__os_eventstats_eligible_")
	groupAlias := eventStatsPrivateAlias(t, compiled.SQL, "__os_eventstats_group_")
	measureAlias := eventStatsPrivateAlias(t, compiled.SQL, "__os_eventstats_measure_")
	publishedAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_eventstats_published_value_",
	)
	typeAlias := eventStatsPrivateAlias(t, compiled.SQL, "__os_eventstats_extrema_type_")
	validationAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_eventstats_validation_",
	)
	for _, required := range []string{
		resultInputAlias + ` AS MATERIALIZED (`,
		barrierAlias + ` AS (SELECT * FROM ` + resultInputAlias + `)`,
		`arrayFold((__os_eventstats_extrema_state, element) ->`,
		`argMinOrNullIf(tuple(tupleElement(` + measureAlias + `, 2), tupleElement(` +
			measureAlias + `, 3), tupleElement(` + measureAlias + `, 4)), tupleElement(` +
			measureAlias + `, 1), tupleElement(` + measureAlias + `, 5) != 0) OVER (` +
			`PARTITION BY ` + eligibleAlias + `, ` + groupAlias + `)`,
		`tupleElement(` + groupAlias + `, 2) != 0`,
		`tupleElement(` + groupAlias + `, 3) != 0`,
		`tupleElement(` + measureAlias + `, 6)`,
		` AS ` + validationAlias,
		` AS ` + publishedAlias,
		`if(` + rowsAlias + `.` + eligibleAlias + ` != 0, ` + rowsAlias + `.` +
			publishedAlias + `, CAST(NULL AS Dynamic)) AS "low"`,
		existsAlias,
		` AS ` + typeAlias,
		`AS "low"`,
		`ORDER BY`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("grouped eventstats min SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `arrayFold(`); got != 1 {
		t.Fatalf("grouped eventstats min folds = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `argMinOrNullIf(`); got != 1 {
		t.Fatalf("grouped eventstats min aggregates = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `PARTITION BY `); got != 1 {
		t.Fatalf("grouped eventstats min window partitions = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, ` AS MATERIALIZED (`); got != 1 {
		t.Fatalf("grouped eventstats min materialized CTEs = %d, want result input only:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("grouped eventstats min physical scans = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, ` AS `+groupAlias); got != 1 {
		t.Fatalf("grouped eventstats min group classifiers = %d, want 1:\n%s", got, compiled.SQL)
	}
	eligibilityProjectionAt := strings.Index(compiled.SQL, ` AS `+eligibleAlias)
	gateAt := strings.Index(compiled.SQL, `if(`+eligibleAlias+` != 0, arrayFold(`)
	foldAt := strings.Index(compiled.SQL, `arrayFold(`)
	if eligibilityProjectionAt < 0 || gateAt <= eligibilityProjectionAt || foldAt != gateAt+len(`if(`+eligibleAlias+` != 0, `) {
		t.Fatalf(
			"group eligibility does not gate Dynamic member traversal: projection=%d gate=%d fold=%d\n%s",
			eligibilityProjectionAt,
			gateAt,
			foldAt,
			compiled.SQL,
		)
	}
	for _, forbidden := range []string{
		"ARRAY JOIN",
		"arrayJoin(",
		"groupArray(",
		"argMinArray(",
		"Array(Tuple(String, String))",
		"arrayFilter(element ->",
		"arrayExists(element ->",
		" GROUP BY ",
		" LEFT JOIN ",
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("grouped eventstats min contains row-multiplying %q:\n%s", forbidden, compiled.SQL)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d", got, want)
	}
}

func TestCompileEventStatsMinimumOrdersMeasureBeforeMultiKeyClassifiers(
	t *testing.T,
) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eventstats min(measure_value) AS low BY first_group second_group | where low=2 | table event_id low`,
	)
	wantPrefix := []any{
		"measure_value",
		"measure_value.",
		"first_group",
		"first_group.",
		"second_group",
		"second_group.",
	}
	if len(compiled.Args) < len(wantPrefix) ||
		!slices.Equal(compiled.Args[:len(wantPrefix)], wantPrefix) {
		t.Fatalf(
			"grouped eventstats min argument prefix = %#v, want %#v",
			compiled.Args,
			wantPrefix,
		)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d", got, want)
	}
}

func TestCompileEventStatsMinimumTreatsProjectedInputAsMissingAndResolvesAliasesFirst(
	t *testing.T,
) {
	t.Parallel()

	projected := compileSPL(
		t,
		`index=gradethis | fields event_id | eventstats min(eventstats_value) AS low | table event_id low`,
	)
	if !strings.Contains(projected.SQL, `CAST([], 'Array(String)')`) ||
		!strings.Contains(projected.SQL, `argMinArray(`) {
		t.Fatalf("projected eventstats min did not aggregate an empty candidate set:\n%s", projected.SQL)
	}
	if strings.Contains(projected.SQL, `"__os_fields"."eventstats_value"`) ||
		slices.Contains(projected.Args, any("eventstats_value")) ||
		slices.Contains(projected.Args, any("eventstats_value.")) {
		t.Fatalf(
			"projected-away eventstats min input was rebound from storage:\nSQL: %s\nargs: %#v",
			projected.SQL,
			projected.Args,
		)
	}

	replaced := compileSPL(
		t,
		`index=gradethis | eventstats min(eventstats_value) AS eventstats_value | table event_id eventstats_value`,
	)
	if !slices.Equal(replaced.OutputFields, []string{"event_id", "eventstats_value"}) ||
		!strings.Contains(replaced.SQL, `AS "eventstats_value"`) {
		t.Fatalf("eventstats min alias replacement lost its public output:\n%s", replaced.SQL)
	}
	wantPrefix := []any{"eventstats_value", "eventstats_value."}
	if len(replaced.Args) < len(wantPrefix) ||
		!slices.Equal(replaced.Args[:len(wantPrefix)], wantPrefix) {
		t.Fatalf(
			"eventstats min alias replacement input args = %#v, want prefix %#v",
			replaced.Args,
			wantPrefix,
		)
	}
}

func TestCompileEventStatsMinimumCannotPruneUnsupportedContainerValidation(
	t *testing.T,
) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eventstats min(payload) AS discarded | table event_id | search definitely_missing=value`,
	)
	if !slices.Equal(compiled.OutputFields, []string{"event_id"}) {
		t.Fatalf("discarded eventstats min output fields = %#v", compiled.OutputFields)
	}
	resultInputAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_eventstats_result_input_",
	)
	barrierAlias := eventStatsPrivateAlias(t, compiled.SQL, "__os_eventstats_result_")
	validationAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_eventstats_validation_",
	)
	for _, required := range []string{
		resultInputAlias + ` AS MATERIALIZED (`,
		barrierAlias + ` AS (SELECT * FROM ` + resultInputAlias + `)`,
		` AS ` + validationAlias,
		`SELECT toUInt8((` + validationAlias + ` != 0)) AS ` +
			quoteIdentifier("__os_chronological_invalid") + ` FROM ` + barrierAlias,
		UnsupportedStatsMeasureValueMarker,
		EventStatsInputLimitMarker,
		`WHERE 0`,
		`UNION ALL`,
		materializedCTESettingsSQL,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("eventstats min validation envelope missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.LastIndex(compiled.SQL, `UNION ALL`) < strings.LastIndex(compiled.SQL, `WHERE 0`) {
		t.Fatalf("always-false downstream filter escaped final extrema validation:\n%s", compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("validation envelope rescans events %d times, want 1:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, ` AS MATERIALIZED (`); got != 1 {
		t.Fatalf("validation envelope materialized CTEs = %d, want result input only:\n%s", got, compiled.SQL)
	}
	for _, forbidden := range []string{
		"ARRAY JOIN",
		"arrayJoin(",
		"groupArray(",
		" GROUP BY ",
		" LEFT JOIN ",
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("validation envelope contains row-multiplying %q:\n%s", forbidden, compiled.SQL)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d", got, want)
	}
}

func TestCompileEventStatsMinimumAnalysisKeepsOneMaterializedLeaf(t *testing.T) {
	t.Parallel()

	logical := buildEventStatsMinimumPlan(
		t,
		`index=gradethis | eventstats min(payload) AS low BY host`,
	)
	compiled, err := (Compiler{}).CompileFieldSuggestions(
		logical,
		FieldSuggestionSpec{Prefix: "lo", MaximumFields: 10},
	)
	if err != nil {
		t.Fatalf("CompileFieldSuggestions: %v", err)
	}
	resultInputAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_eventstats_result_input_",
	)
	barrierAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_eventstats_result_",
	)
	finalAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_chronological_final_input_",
	)
	validationAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_chronological_validation_",
	)
	for _, required := range []string{
		resultInputAlias + ` AS MATERIALIZED (`,
		barrierAlias + ` AS (SELECT * FROM ` + resultInputAlias + `)`,
		quoteIdentifier(fieldSuggestionSourceCTE) + ` AS (`,
		finalAlias + ` AS (`,
		validationAlias + ` AS (`,
		`UNION ALL`,
		UnsupportedStatsMeasureValueMarker,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("eventstats analysis SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, ` AS MATERIALIZED (`); got != 1 {
		t.Fatalf(
			"eventstats analysis materialized CTEs = %d, want only result input:\n%s",
			got,
			compiled.SQL,
		)
	}
	if got := strings.Count(compiled.SQL, `arrayFold(`); got != 1 {
		t.Fatalf("eventstats analysis folds = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `argMinOrNullIf(`); got != 1 {
		t.Fatalf("eventstats analysis window minima = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(
		compiled.SQL,
		`LIMIT `+strconv.FormatUint(MaximumEventStatsInputRows+1, 10),
	); got != 1 {
		t.Fatalf("eventstats analysis sentinel limits = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d", got, want)
	}
}

func TestEventAnalysisFinalizationPolicyOnlyInlinesPrerequisiteGraphs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		barriers        []compiledChronologicalBarrier
		wantMaterialize bool
		wantOrder       bool
	}{
		{name: "standalone", wantMaterialize: true, wantOrder: true},
		{
			name:            "ordinary chronological barrier",
			barriers:        []compiledChronologicalBarrier{{name: `"ordinary"`}},
			wantMaterialize: true,
		},
		{
			name: "eventstats prerequisite barrier",
			barriers: []compiledChronologicalBarrier{{
				name:                    `"eventstats"`,
				prerequisiteDefinitions: []string{`"input" AS MATERIALIZED (SELECT 1)`},
			}},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			policy := eventAnalysisFinalizationPolicyFor(test.barriers)
			if policy.materializeSharedCTEs != test.wantMaterialize ||
				policy.includeResultOrder != test.wantOrder {
				t.Fatalf("policy = %#v", policy)
			}
		})
	}
}

func TestCompileStackedEventStatsMinimumAnalysisKeepsFirstBoundedLeaf(
	t *testing.T,
) {
	t.Parallel()

	logical := buildPlan(
		t,
		`index=gradethis | eventstats min(first_payload) AS low`+
			` | eventstats min(second_payload) AS lower`,
	)
	compiled, err := (Compiler{}).CompileFieldSuggestions(
		logical,
		FieldSuggestionSpec{Prefix: "low", MaximumFields: 10},
	)
	if err != nil {
		t.Fatalf("CompileFieldSuggestions: %v", err)
	}
	inputDefinitions := regexp.MustCompile(
		`"__os_eventstats_result_input_[0-9]+" AS (?:MATERIALIZED )?\(`,
	).FindAllString(compiled.SQL, -1)
	if len(inputDefinitions) != 2 ||
		!strings.Contains(inputDefinitions[0], " AS MATERIALIZED (") ||
		strings.Contains(inputDefinitions[1], " AS MATERIALIZED (") {
		t.Fatalf("stacked eventstats input definitions = %#v\n%s", inputDefinitions, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, ` AS MATERIALIZED (`); got != 1 {
		t.Fatalf("stacked eventstats materialized CTEs = %d, want first leaf only", got)
	}
	if got := strings.Count(
		compiled.SQL,
		`LIMIT `+strconv.FormatUint(MaximumEventStatsInputRows+1, 10),
	); got != 2 {
		t.Fatalf("stacked eventstats sentinel limits = %d, want 2:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `arrayFold(`); got != 2 {
		t.Fatalf("stacked eventstats folds = %d, want 2:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `argMinOrNullIf(`); got != 2 {
		t.Fatalf("stacked eventstats window minima = %d, want 2:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("stacked eventstats physical scans = %d, want 1:\n%s", got, compiled.SQL)
	}
	firstAt := slices.Index(compiled.Args, any("first_payload"))
	secondAt := slices.Index(compiled.Args, any("second_payload"))
	if firstAt < 0 || secondAt <= firstAt {
		t.Fatalf("stacked eventstats args are out of order: %#v", compiled.Args)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d", got, want)
	}
}

func TestCompileMixedEventStatsAnalysisKeepsOneFlatMaterializedLeaf(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name                       string
		source                     string
		wantFirstInputMaterialized bool
		wantSentinelLimits         int
		argumentOrder              []string
	}{
		{
			name: "count then minimum",
			source: `index=gradethis | eventstats count AS peers` +
				` | eventstats min(payload) AS low`,
			wantFirstInputMaterialized: true,
			wantSentinelLimits:         2,
		},
		{
			name: "minimum then count",
			source: `index=gradethis | eventstats min(payload) AS low` +
				` | eventstats count AS peers`,
			wantFirstInputMaterialized: true,
			wantSentinelLimits:         2,
		},
		{
			name: "fenced conditional count then minimum",
			source: `index=gradethis | spath input=_raw output=selected path=needle` +
				` | eventstats count(eval(selected="wanted")) AS hits` +
				` | eventstats min(payload) AS low`,
			wantSentinelLimits: 3,
			argumentOrder:      []string{"needle", "wanted", "payload"},
		},
		{
			name: "minimum then fenced conditional count",
			source: `index=gradethis | eventstats min(payload) AS low` +
				` | spath input=_raw output=selected path=needle` +
				` | eventstats count(eval(selected="wanted")) AS hits`,
			wantFirstInputMaterialized: true,
			wantSentinelLimits:         3,
			argumentOrder:              []string{"payload", "needle", "wanted"},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical := buildPlan(t, test.source)
			compiled, err := (Compiler{}).CompileFieldSuggestions(
				logical,
				FieldSuggestionSpec{Prefix: "p", MaximumFields: 10},
			)
			if err != nil {
				t.Fatalf("CompileFieldSuggestions: %v", err)
			}
			inputDefinitions := regexp.MustCompile(
				`"__os_eventstats_(?:input|result_input)_[0-9]+" AS (?:MATERIALIZED )?\(`,
			).FindAllString(compiled.SQL, -1)
			if len(inputDefinitions) != 2 ||
				strings.Contains(inputDefinitions[0], " AS MATERIALIZED (") !=
					test.wantFirstInputMaterialized ||
				strings.Contains(inputDefinitions[1], " AS MATERIALIZED (") {
				t.Fatalf(
					"mixed eventstats input definitions = %#v\n%s",
					inputDefinitions,
					compiled.SQL,
				)
			}
			if got := strings.Count(compiled.SQL, ` AS MATERIALIZED (`); got != 1 {
				t.Fatalf(
					"mixed eventstats materialized CTEs = %d, want first leaf only:\n%s",
					got,
					compiled.SQL,
				)
			}
			if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
				t.Fatalf("mixed eventstats physical scans = %d, want 1:\n%s", got, compiled.SQL)
			}
			if got := strings.Count(
				compiled.SQL,
				`LIMIT `+strconv.FormatUint(MaximumEventStatsInputRows+1, 10),
			); got != test.wantSentinelLimits {
				t.Fatalf(
					"mixed eventstats sentinel limits = %d, want %d:\n%s",
					got,
					test.wantSentinelLimits,
					compiled.SQL,
				)
			}
			if got := strings.Count(compiled.SQL, `arrayFold(`); got != 1 {
				t.Fatalf("mixed eventstats minimum folds = %d, want 1:\n%s", got, compiled.SQL)
			}
			if got := strings.Count(compiled.SQL, `argMinOrNullIf(`); got != 1 {
				t.Fatalf("mixed eventstats window minima = %d, want 1:\n%s", got, compiled.SQL)
			}
			if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
				t.Fatalf("placeholder count = %d, args = %d", got, want)
			}
			previous := -1
			for _, argument := range test.argumentOrder {
				position := slices.Index(compiled.Args, any(argument))
				if position <= previous {
					t.Fatalf(
						"mixed eventstats argument %q at %d after %d: %#v",
						argument,
						position,
						previous,
						compiled.Args,
					)
				}
				previous = position
			}
		})
	}
}

func TestCompileEventStatsMinimumRejectsForgedPlanBoundaries(t *testing.T) {
	t.Parallel()

	base := buildEventStatsMinimumPlan(
		t,
		`index=gradethis | eventstats min(eventstats_value) AS low`,
	)
	if _, err := (Compiler{}).Compile(base); err != nil {
		t.Fatalf("Compile(resolved eventstats min plan): %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*plan.EventAggregate)
	}{
		{
			name: "predicate",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Predicate = &plan.ComparisonExpression{}
			},
		},
		{
			name: "percentile",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Percentile = 50
			},
		},
		{
			name: "missing input",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Input = plan.FieldRef{}
			},
		},
		{
			name: "malformed input",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Input = plan.FieldRef{Name: "eventstats_value"}
			},
		},
		{
			name: "private output",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Output = "__os_eventstats_min_private"
			},
		},
		{
			name: "canonical input with forged empty path",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Input = plan.FieldRef{
					Name:      "severity",
					Canonical: true,
					Path:      []string{},
					Range:     operator.Measure.Input.Range,
				}
			},
		},
		{
			name: "canonical group with forged empty path",
			mutate: func(operator *plan.EventAggregate) {
				operator.GroupBy = []plan.FieldRef{{
					Name:      "_time",
					Canonical: true,
					Path:      []string{},
					Range:     operator.Measure.Input.Range,
				}}
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical, operator := cloneEventAggregatePlan(t, base)
			test.mutate(operator)
			if _, err := (Compiler{}).Compile(logical); err == nil {
				t.Fatal("Compile() accepted forged eventstats min measure")
			}
		})
	}

	reserved, operator := cloneEventAggregatePlan(t, base)
	fields := operator.Measure.Input
	fields.Name = "fields"
	fields.Path = []string{"fields"}
	operator.Measure.Input = fields
	_, err := (Compiler{}).Compile(reserved)
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) ||
		diagnostic.Code != "SPL_AMBIGUOUS_EVENTSTATS_FIELD" {
		t.Fatalf(
			"open fields eventstats min error = %#v, want SPL_AMBIGUOUS_EVENTSTATS_FIELD",
			err,
		)
	}
}

func TestCompileEventStatsStackBoundsDeferredGraphAmplification(t *testing.T) {
	t.Parallel()

	compileOrdinary := func(logical *plan.Query) error {
		_, err := (Compiler{}).Compile(logical)
		return err
	}
	for _, test := range []struct {
		name           string
		aggregate      string
		grouped        bool
		acceptedStages int
	}{
		{
			name:           "global count field stack",
			aggregate:      "count(payload)",
			acceptedStages: 7,
		},
		{
			name:           "grouped count stack",
			aggregate:      "count",
			grouped:        true,
			acceptedStages: 4,
		},
		{
			name:           "global validating minimum stack",
			aggregate:      "min(payload)",
			acceptedStages: 4,
		},
		{
			name:           "grouped validating minimum stack",
			aggregate:      "min(payload)",
			grouped:        true,
			acceptedStages: 2,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			accepted := eventStatsAmplificationStackSource(
				`index=gradethis`,
				test.aggregate,
				test.acceptedStages,
				test.grouped,
			)
			rejected := eventStatsAmplificationStackSource(
				`index=gradethis`,
				test.aggregate,
				test.acceptedStages+1,
				test.grouped,
			)
			requireEventStatsAmplificationBoundary(
				t,
				accepted,
				rejected,
				compileOrdinary,
			)
		})
	}
}

func TestCompileEventStatsGlobalRowCountStackHasLinearFanout(t *testing.T) {
	t.Parallel()

	const stages = 12
	compiled := compileSPL(
		t,
		eventStatsAmplificationStackSource(
			`index=gradethis`,
			"count",
			stages,
			false,
		),
	)
	if got := strings.Count(compiled.SQL, "count() OVER ()"); got != stages {
		t.Fatalf("global row-count windows = %d, want %d", got, stages)
	}
	if strings.Contains(compiled.SQL, `"__os_eventstats_total_`) ||
		strings.Contains(compiled.SQL, "CROSS JOIN") {
		t.Fatalf("global row-count stack retained aggregate fanout:\n%s", compiled.SQL)
	}
}

func TestCompileEventStatsGraphAmplificationChargesPrerequisiteBarriers(t *testing.T) {
	t.Parallel()

	compileOrdinary := func(logical *plan.Query) error {
		_, err := (Compiler{}).Compile(logical)
		return err
	}
	for _, test := range []struct {
		name     string
		accepted string
		rejected string
	}{
		{
			name: "calculated predicate fence keeps the full first-stage fanout",
			accepted: eventStatsAmplificationStackSource(
				`index=gradethis | spath input=_raw output=selected path=value`+
					` | eventstats count(eval(selected="wanted")) AS conditional_0`,
				"count(payload)",
				6,
				false,
			),
			rejected: eventStatsAmplificationStackSource(
				`index=gradethis | spath input=_raw output=selected path=value`+
					` | eventstats count(eval(selected="wanted")) AS conditional_0`,
				"count(payload)",
				7,
				false,
			),
		},
		{
			name: "chronological barrier before eventstats",
			accepted: eventStatsAmplificationStackSource(
				`index=gradethis | stats earliest(payload) AS first`,
				"count(payload)",
				5,
				false,
			),
			rejected: eventStatsAmplificationStackSource(
				`index=gradethis | stats earliest(payload) AS first`,
				"count(payload)",
				6,
				false,
			),
		},
		{
			name: "chronological barrier after eventstats",
			accepted: eventStatsAmplificationStackSource(
				`index=gradethis`,
				"count(payload)",
				5,
				false,
			) + ` | stats earliest(payload) AS first`,
			rejected: eventStatsAmplificationStackSource(
				`index=gradethis`,
				"count(payload)",
				6,
				false,
			) + ` | stats earliest(payload) AS first`,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			requireEventStatsAmplificationBoundary(
				t,
				test.accepted,
				test.rejected,
				compileOrdinary,
			)
		})
	}
}

func TestCompileEventStatsGraphAmplificationChargesAnalysisFanout(t *testing.T) {
	t.Parallel()

	fourGroupedCounts := eventStatsAmplificationStackSource(
		`index=gradethis`,
		"count",
		4,
		true,
	)
	logical := buildPlan(t, fourGroupedCounts)
	for _, test := range []struct {
		name    string
		compile func(*plan.Query) error
	}{
		{name: "field suggestions", compile: func(query *plan.Query) error {
			_, err := (Compiler{}).CompileFieldSuggestions(
				query,
				FieldSuggestionSpec{Prefix: "a", MaximumFields: 10},
			)
			return err
		}},
		{name: "field catalog", compile: func(query *plan.Query) error {
			_, err := (Compiler{}).CompileFieldCatalog(
				query,
				FieldCatalogSpec{MaximumFields: 10},
			)
			return err
		}},
	} {
		if err := test.compile(logical); err != nil {
			t.Fatalf("Compile %s at prerequisite amplification boundary: %v", test.name, err)
		}
	}

	fiveGroupedCounts := eventStatsAmplificationStackSource(
		`index=gradethis`,
		"count",
		5,
		true,
	)
	logical = buildPlan(t, fiveGroupedCounts)
	_, err := (Compiler{}).CompileFieldSuggestions(
		logical,
		FieldSuggestionSpec{Prefix: "a", MaximumFields: 10},
	)
	requireEventStatsAmplificationDiagnostic(t, logical, err)
	logical = buildPlan(t, fiveGroupedCounts)
	_, err = (Compiler{}).CompileFieldCatalog(
		logical,
		FieldCatalogSpec{MaximumFields: 10},
	)
	requireEventStatsAmplificationDiagnostic(t, logical, err)

	twoGroupedMinimums := eventStatsAmplificationStackSource(
		`index=gradethis`,
		"min(payload)",
		2,
		true,
	)
	if _, err := (Compiler{}).CompileFieldCatalog(
		buildPlan(t, twoGroupedMinimums),
		FieldCatalogSpec{MaximumFields: 10},
	); err != nil {
		t.Fatalf("CompileFieldCatalog at prerequisite minimum boundary: %v", err)
	}
	threeGroupedMinimums := eventStatsAmplificationStackSource(
		`index=gradethis`,
		"min(payload)",
		3,
		true,
	)
	logical = buildPlan(t, threeGroupedMinimums)
	_, err = (Compiler{}).CompileFieldCatalog(
		logical,
		FieldCatalogSpec{MaximumFields: 10},
	)
	requireEventStatsAmplificationDiagnostic(t, logical, err)
}

func TestCompileEventStatsGraphAmplificationChargesTerminalWideFanout(t *testing.T) {
	t.Parallel()

	compileOrdinary := func(logical *plan.Query) error {
		_, err := (Compiler{}).Compile(logical)
		return err
	}
	for _, test := range []struct {
		name           string
		terminal       string
		acceptedStages int
	}{
		{
			name:           "chart reads its event source once",
			terminal:       ` | chart count OVER host BY source`,
			acceptedStages: 7,
		},
		{
			name:           "chart count field reads its event source once",
			terminal:       ` | chart count(amplification_0) OVER host BY source`,
			acceptedStages: 7,
		},
		{
			name:           "timechart reads its event source once",
			terminal:       ` | timechart span=1m count`,
			acceptedStages: 7,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			accepted := eventStatsAmplificationStackSource(
				`index=gradethis`,
				"count(payload)",
				test.acceptedStages,
				false,
			) + test.terminal
			rejected := eventStatsAmplificationStackSource(
				`index=gradethis`,
				"count(payload)",
				test.acceptedStages+1,
				false,
			) + test.terminal
			requireEventStatsAmplificationBoundary(
				t,
				accepted,
				rejected,
				compileOrdinary,
			)
		})
	}
}

func eventStatsAmplificationStackSource(
	prefix string,
	aggregate string,
	additionalStages int,
	grouped bool,
) string {
	var source strings.Builder
	source.WriteString(prefix)
	for index := 0; index < additionalStages; index++ {
		source.WriteString(` | eventstats `)
		source.WriteString(aggregate)
		source.WriteString(` AS amplification_`)
		source.WriteString(strconv.Itoa(index))
		if grouped {
			source.WriteString(` BY host`)
		}
	}
	return source.String()
}

func requireEventStatsAmplificationBoundary(
	t *testing.T,
	acceptedSource string,
	rejectedSource string,
	compile func(*plan.Query) error,
) {
	t.Helper()
	if err := compile(buildPlan(t, acceptedSource)); err != nil {
		t.Fatalf("compile at amplification boundary: %v", err)
	}
	logical := buildPlan(t, rejectedSource)
	requireEventStatsAmplificationDiagnostic(t, logical, compile(logical))
}

func requireEventStatsAmplificationDiagnostic(
	t *testing.T,
	logical *plan.Query,
	err error,
) {
	t.Helper()
	var last *plan.EventAggregate
	for _, operator := range logical.Operators {
		if eventAggregate, ok := operator.(*plan.EventAggregate); ok {
			last = eventAggregate
		}
	}
	if last == nil {
		t.Fatal("amplification fixture has no EventAggregate")
	}
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
		t.Fatalf(
			"compile over amplification boundary error = %#v, want SPL_QUERY_TOO_COMPLEX",
			err,
		)
	}
	if diagnostic.Range != last.Range {
		t.Fatalf(
			"amplification diagnostic range = %#v, want %#v",
			diagnostic.Range,
			last.Range,
		)
	}
	if !strings.Contains(
		diagnostic.Message,
		strconv.FormatUint(MaximumEventStatsGraphAmplification, 10),
	) {
		t.Fatalf(
			"amplification diagnostic = %q, want fixed limit",
			diagnostic.Message,
		)
	}
}

func buildEventStatsMinimumPlan(t *testing.T, source string) *plan.Query {
	t.Helper()
	logical := buildPlan(t, source)
	var eventAggregate *plan.EventAggregate
	for _, operator := range logical.Operators {
		candidate, ok := operator.(*plan.EventAggregate)
		if !ok {
			continue
		}
		if eventAggregate != nil {
			t.Fatal("test query has more than one EventAggregate")
		}
		eventAggregate = candidate
	}
	if eventAggregate == nil {
		t.Fatal("test query has no EventAggregate")
	}
	if eventAggregate.Measure.Function != plan.AggregateFunctionMinimum {
		t.Fatalf(
			"test EventAggregate function = %v, want minimum",
			eventAggregate.Measure.Function,
		)
	}
	return logical
}
