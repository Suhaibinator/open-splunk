package clickhouse

import (
	"errors"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestCompileStreamStatsMinimumFoldsOneBoundedDynamicInputToRunningWinners(
	t *testing.T,
) {
	t.Parallel()

	logical := buildPlan(
		t,
		`index=gradethis source="streamstats-min-fixture" | sort 0 +event_id`+
			` | streamstats min(streamstats_value) AS running_min`+
			` | where running_min=2 | table event_id running_min`,
	)
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(streamstats minimum): %v", err)
	}
	measure := eventStatsPrivateAlias(t, compiled.SQL, "__os_streamstats_measure_")
	typeColumn := eventStatsPrivateAlias(t, compiled.SQL, "__os_streamstats_extrema_type_")
	validation := eventStatsPrivateAlias(t, compiled.SQL, "__os_streamstats_validation_")
	resultInput := eventStatsPrivateAlias(t, compiled.SQL, "__os_streamstats_result_input_")
	sentinel := `LIMIT ` + strconv.FormatUint(MaximumStreamStatsInputRows+1, 10)
	for _, required := range []string{
		`arrayFold((__os_eventstats_extrema_state, element) ->`,
		`dynamicElement(__os_eventstats_extrema_field_value, 'Array(Dynamic)')`,
		` AS ` + measure,
		`argMinOrNullIf(tuple(tupleElement(` + measure + `, 2), tupleElement(` +
			measure + `, 3), tupleElement(` + measure + `, 4)), tupleElement(` +
			measure + `, 1), tupleElement(` + measure + `, 5) != 0) OVER (`,
		`ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`,
		` AS ` + typeColumn,
		` AS ` + validation,
		resultInput + ` AS MATERIALIZED (`,
		`SELECT toUInt8((` + validation + ` != 0)) AS "__os_chronological_invalid"`,
		`AS "running_min"`,
		sentinel,
		StreamStatsInputLimitMarker,
		UnsupportedStatsMeasureValueMarker,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("streamstats minimum SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `arrayFold(`); got != 1 {
		t.Fatalf("streamstats minimum row folds = %d, want one:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `argMinOrNullIf(`); got != 1 {
		t.Fatalf("streamstats minimum window aggregates = %d, want one:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, sentinel); got != 1 {
		t.Fatalf("streamstats minimum sentinel limits = %d, want one:\n%s", got, compiled.SQL)
	}
	if regexp.MustCompile(
		`"__os_streamstats_input_[0-9]+" AS MATERIALIZED`,
	).MatchString(compiled.SQL) {
		t.Fatalf("Dynamic streamstats minimum materialized its prepared input instead of its completed result:\n%s", compiled.SQL)
	}
	for _, forbidden := range []string{
		"ARRAY JOIN", "arrayJoin(", "groupArray(", "argMinArray(",
		"Array(Tuple(String, String))", "arrayFilter(element ->",
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("streamstats minimum contains forbidden %q:\n%s", forbidden, compiled.SQL)
		}
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsMinimumPinsExactFrames(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name, options, frame string
	}{
		{name: "unbounded current", frame: `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`},
		{name: "unbounded prior", options: `current=false`, frame: `ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING`},
		{name: "one current row", options: `window=1`, frame: `ROWS BETWEEN CURRENT ROW AND CURRENT ROW`},
		{name: "one prior row", options: `current=false window=1`, frame: `ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING`},
		{name: "three current rows", options: `window=3`, frame: `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW`},
		{name: "three prior rows", options: `current=false window=3`, frame: `ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING`},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical := buildPlan(
				t,
				`index=gradethis | streamstats `+test.options+
					` min(streamstats_value) AS running_min`,
			)
			compiled, err := (Compiler{}).Compile(logical)
			if err != nil {
				t.Fatalf("Compile(streamstats minimum frame): %v", err)
			}
			if strings.Count(compiled.SQL, `argMinOrNullIf(`) != 1 ||
				strings.Count(compiled.SQL, test.frame) != 1 {
				t.Fatalf("streamstats minimum frame is not exact:\n%s", compiled.SQL)
			}
			assertBoundedStreamStatsSQL(t, compiled)
		})
	}
}

func TestCompileStreamStatsMinimumKeepsNativeFixedNumericBooleanAndTimeTypes(
	t *testing.T,
) {
	t.Parallel()

	for _, test := range []struct {
		field, output string
	}{
		{field: "severity", output: "lowest_severity"},
		{field: "_time", output: "first_time"},
	} {
		test := test
		t.Run(test.field, func(t *testing.T) {
			t.Parallel()

			logical := buildPlan(
				t,
				`index=gradethis | streamstats min(`+test.field+`) AS `+test.output,
			)
			compiled, err := (Compiler{}).Compile(logical)
			if err != nil {
				t.Fatalf("Compile(fixed streamstats minimum): %v", err)
			}
			if !strings.Contains(compiled.SQL, `minIfOrNull(`) ||
				!strings.Contains(compiled.SQL, ` OVER (`) ||
				!strings.Contains(compiled.SQL, `AS "`+test.output+`"`) {
				t.Fatalf("fixed streamstats minimum lost native window lowering:\n%s", compiled.SQL)
			}
			for _, forbidden := range []string{
				`toFloat64("` + test.field + `")`, `argMinOrNullIf(`,
				`argMinArray(`, `CAST(NULL AS Dynamic)`,
			} {
				if strings.Contains(compiled.SQL, forbidden) {
					t.Fatalf("fixed streamstats minimum contains %q:\n%s", forbidden, compiled.SQL)
				}
			}
			assertBoundedStreamStatsSQL(t, compiled)
		})
	}
}

func TestCompileStreamStatsMinimumUsesScalarStringMixedCandidateAndMetadata(
	t *testing.T,
) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | streamstats min(service) AS low`+
			` | eval copied=low | table event_id low copied`,
	)
	measure := eventStatsPrivateAlias(t, compiled.SQL, "__os_streamstats_measure_")
	stringScratch := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_streamstats_extrema_string_",
	)
	numberScratch := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_streamstats_extrema_number_",
	)
	typeColumn := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_streamstats_extrema_type_",
	)
	for _, required := range []string{
		`CAST(toString("service") AS Nullable(String))`,
		`if(isNotNull(`,
		`SELECT * EXCEPT (` + stringScratch + `, ` + numberScratch + `), `,
		`argMinOrNullIf(tuple(tupleElement(` + measure + `, 2), tupleElement(` +
			measure + `, 3), tupleElement(` + measure + `, 4)), tupleElement(` +
			measure + `, 1), tupleElement(` + measure + `, 5) != 0) OVER (`,
		` AS ` + typeColumn,
		`AS "low"`,
		`AS "copied"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("scalar String streamstats minimum missing %q:\n%s", required, compiled.SQL)
		}
	}
	for _, forbidden := range []string{
		`arrayFold(`,
		`argMinArray(`,
		`Array(Tuple(String, String))`,
		`toFloat64("service")`,
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("scalar String streamstats minimum contains %q:\n%s", forbidden, compiled.SQL)
		}
	}
	if strings.Count(compiled.SQL, `argMinOrNullIf(`) != 1 ||
		strings.Count(compiled.SQL, ` AS `+measure) != 1 ||
		strings.Count(compiled.SQL, ` AS `+typeColumn) != 1 {
		t.Fatalf("scalar String streamstats minimum duplicated candidate, window, or metadata:\n%s", compiled.SQL)
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsMinimumKeepsComputedBooleanNative(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval selected=false`+
			` | streamstats min(selected) AS lowest BY service | table event_id lowest`,
	)
	for _, required := range []string{
		`CAST(? AS Bool) AS "selected"`,
		`minIfOrNull(tupleElement("__os_streamstats_measure_`,
		`CAST(NULL AS Nullable(Bool))`,
		`AS "lowest"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("computed Bool streamstats minimum missing %q:\n%s", required, compiled.SQL)
		}
	}
	for _, forbidden := range []string{
		`argMinOrNullIf(`,
		`arrayFold(`,
		`dynamicType("selected")`,
		`toFloat64("selected")`,
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("computed Bool streamstats minimum contains %q:\n%s", forbidden, compiled.SQL)
		}
	}
	if !slices.Contains(compiled.Args, any(false)) {
		t.Fatalf("computed Bool streamstats minimum lost its bound false literal: %#v", compiled.Args)
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsMinimumReducesFixedMultivalueOnceWithoutExpansion(
	t *testing.T,
) {
	t.Parallel()

	logical := buildPlan(
		t,
		`index=gradethis | stats values(host) AS hosts`+
			` | streamstats min(hosts) AS running_min | table hosts running_min`,
	)
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(fixed-multivalue streamstats minimum): %v", err)
	}
	measure := eventStatsPrivateAlias(t, compiled.SQL, "__os_streamstats_measure_")
	for _, required := range []string{
		`arrayFold((__os_streamstats_extrema_state, value) ->`,
		`"hosts"`,
		` AS ` + measure,
		`argMinOrNullIf(`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("fixed-multivalue streamstats minimum missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `groupUniqArray`); got != 1 {
		t.Fatalf("fixed-multivalue collectors = %d, want upstream values only:\n%s", got, compiled.SQL)
	}
	for _, forbidden := range []string{"ARRAY JOIN", "arrayJoin(", "groupArray("} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("fixed-multivalue streamstats minimum contains %q:\n%s", forbidden, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("fixed-multivalue physical scans = %d, want one:\n%s", got, compiled.SQL)
	}
}

func TestCompileStreamStatsMinimumGroupedPresenceAndPoisonValidationAreIndependent(
	t *testing.T,
) {
	t.Parallel()

	logical := buildPlan(
		t,
		`index=gradethis | streamstats window=2 global=false`+
			` min(streamstats_value) AS running_min BY user`+
			` | search running_min=* | table event_id running_min`,
	)
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(grouped streamstats minimum): %v", err)
	}
	measure := eventStatsPrivateAlias(t, compiled.SQL, "__os_streamstats_measure_")
	for _, required := range []string{
		`PARTITION BY "__os_streamstats_eligible_`,
		`argMinOrNullIf(`,
		`tupleElement(` + measure + `, 6)`,
		`"__os_streamstats_eligible_`,
		`CAST(NULL AS Dynamic)`,
		`"__os_streamstats_exists_`,
		UnsupportedStatsByValueMarker,
		UnsupportedStatsMeasureValueMarker,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("grouped streamstats minimum missing %q:\n%s", required, compiled.SQL)
		}
	}
	wantPrefix := []any{"streamstats_value", "streamstats_value.", "user", "user."}
	if len(compiled.Args) <= len(wantPrefix) ||
		!slices.Equal(compiled.Args[:len(wantPrefix)], wantPrefix) ||
		compiled.Args[len(wantPrefix)] != "tenant-1" {
		t.Fatalf("grouped minimum argument prefix = %#v, want %#v then tenant", compiled.Args, wantPrefix)
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsMinimumCannotPruneDynamicPoisonValidation(
	t *testing.T,
) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | streamstats min(payload) AS discarded`+
			` | table event_id | search definitely_missing=value`,
	)
	if !slices.Equal(compiled.OutputFields, []string{"event_id"}) {
		t.Fatalf("discarded streamstats minimum output fields = %#v", compiled.OutputFields)
	}
	barrier := eventStatsPrivateAlias(t, compiled.SQL, "__os_streamstats_result_")
	validation := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_streamstats_validation_",
	)
	for _, required := range []string{
		` AS ` + validation,
		`SELECT toUInt8((` + validation + ` != 0)) AS ` +
			quoteIdentifier("__os_chronological_invalid") + ` FROM ` + barrier,
		UnsupportedStatsMeasureValueMarker,
		StreamStatsInputLimitMarker,
		`WHERE 0`,
		`UNION ALL`,
		materializedCTESettingsSQL,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("hidden streamstats minimum validation missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.LastIndex(compiled.SQL, `UNION ALL`) <
		strings.LastIndex(compiled.SQL, `WHERE 0`) {
		t.Fatalf("always-false filter escaped streamstats minimum validation:\n%s", compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, ` AS MATERIALIZED (`); got != 1 {
		t.Fatalf("hidden streamstats minimum materialized CTEs = %d, want one:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `arrayFold(`); got != 1 {
		t.Fatalf("hidden streamstats minimum row folds = %d, want one:\n%s", got, compiled.SQL)
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsMinimumGatesDynamicFoldOnCompleteBYTuple(
	t *testing.T,
) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | streamstats min(payload) AS low BY required_group`+
			` | table event_id low`,
	)
	eligible := eventStatsPrivateAlias(t, compiled.SQL, "__os_streamstats_eligible_")
	measure := eventStatsPrivateAlias(t, compiled.SQL, "__os_streamstats_measure_")
	gate := `if(` + eligible + ` != 0, arrayFold(`
	eligibilityProjectionAt := strings.Index(compiled.SQL, ` AS `+eligible)
	gateAt := strings.Index(compiled.SQL, gate)
	if eligibilityProjectionAt < 0 || gateAt < 0 || gateAt >= eligibilityProjectionAt {
		t.Fatalf(
			"streamstats minimum Dynamic gate is not outside its classified BY source: projection=%d gate=%d\n%s",
			eligibilityProjectionAt,
			gateAt,
			compiled.SQL,
		)
	}
	for _, required := range []string{
		`PARTITION BY ` + eligible,
		`tupleElement(` + measure + `, 6)`,
		UnsupportedStatsByValueMarker,
		UnsupportedStatsMeasureValueMarker,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("gated streamstats minimum missing %q:\n%s", required, compiled.SQL)
		}
	}
	wantPrefix := []any{"payload", "payload.", "required_group", "required_group."}
	if len(compiled.Args) <= len(wantPrefix) ||
		!slices.Equal(compiled.Args[:len(wantPrefix)], wantPrefix) ||
		compiled.Args[len(wantPrefix)] != "tenant-1" {
		t.Fatalf("gated streamstats minimum args = %#v, want %#v then tenant", compiled.Args, wantPrefix)
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsMinimumTreatsProjectedInputAsEmpty(t *testing.T) {
	t.Parallel()

	logical := buildPlan(
		t,
		`index=gradethis | fields event_id`+
			` | streamstats min(streamstats_value) AS running_min`+
			` | table event_id running_min`,
	)
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(projected streamstats minimum): %v", err)
	}
	if !strings.Contains(compiled.SQL, eventStatsExtremaEmptyRowStateSQL("0")) ||
		!strings.Contains(compiled.SQL, `CAST(NULL AS Dynamic)`) {
		t.Fatalf("projected streamstats minimum did not stay empty:\n%s", compiled.SQL)
	}
	if slices.Contains(compiled.Args, any("streamstats_value")) ||
		slices.Contains(compiled.Args, any("streamstats_value.")) {
		t.Fatalf("projected minimum rebound hidden input: %#v", compiled.Args)
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsMinimumResolvesReplacementBeforeOrderSnapshot(
	t *testing.T,
) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | sort 0 +streamstats_value`+
			` | streamstats min(streamstats_value) AS streamstats_value`+
			` | where streamstats_value=2 | table event_id streamstats_value`,
	)
	measure := eventStatsPrivateAlias(t, compiled.SQL, "__os_streamstats_measure_")
	if !strings.Contains(compiled.SQL, ` AS `+measure) ||
		!strings.Contains(compiled.SQL, ` AS "streamstats_value"`) ||
		!regexp.MustCompile(
			`"__os_order_[0-9]+_0" AS "__os_streamstats_order_[0-9]+_0"`,
		).MatchString(compiled.SQL) {
		t.Fatalf("streamstats minimum replacement lost its input or order snapshot:\n%s", compiled.SQL)
	}
	wantPrefix := []any{"streamstats_value", "streamstats_value."}
	if len(compiled.Args) < len(wantPrefix) ||
		!slices.Equal(compiled.Args[:len(wantPrefix)], wantPrefix) {
		t.Fatalf(
			"streamstats minimum replacement args = %#v, want prefix %#v",
			compiled.Args,
			wantPrefix,
		)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStackedStreamStatsMinimumPropagatesStoredTypes(t *testing.T) {
	t.Parallel()

	logical := buildPlan(
		t,
		`index=gradethis | streamstats min(first_payload) AS low`+
			` | streamstats min(low) AS lower | eval copied=lower`+
			` | table event_id low lower copied`,
	)
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(stacked streamstats minimum): %v", err)
	}
	typePattern := regexp.MustCompile(`"__os_streamstats_extrema_type_[0-9]+"`)
	seenTypes := make(map[string]struct{})
	var typeColumns []string
	for _, match := range typePattern.FindAllString(compiled.SQL, -1) {
		if _, seen := seenTypes[match]; seen {
			continue
		}
		seenTypes[match] = struct{}{}
		typeColumns = append(typeColumns, match)
	}
	if len(typeColumns) != 2 {
		t.Fatalf("stacked streamstats minimum type columns = %#v, want two\n%s", typeColumns, compiled.SQL)
	}
	if strings.Count(compiled.SQL, `argMinOrNullIf(`) != 2 ||
		strings.Count(compiled.SQL, `arrayFold(`) != 2 ||
		!strings.Contains(compiled.SQL, `AS "copied"`) {
		t.Fatalf("stacked streamstats minimum lost a winner, fold, or copied output:\n%s", compiled.SQL)
	}
	for _, typeColumn := range typeColumns {
		if strings.Count(compiled.SQL, ` AS `+typeColumn) != 1 {
			t.Fatalf("stacked streamstats minimum type %s is not defined once:\n%s", typeColumn, compiled.SQL)
		}
	}
	if slices.Contains(compiled.Args, any("low")) ||
		slices.Contains(compiled.Args, any("low.")) {
		t.Fatalf("stacked streamstats minimum rebound its first output from storage: %#v", compiled.Args)
	}
	assertBoundedStreamStatsSQL(t, compiled)

	summary, err := (Compiler{}).CompileFieldSummary(
		logical,
		FieldSummarySpec{
			FieldName:             "lower",
			MaximumValues:         10,
			MaximumDistinctValues: 10,
			MaximumValueBytes:     64,
		},
	)
	if err != nil {
		t.Fatalf("CompileFieldSummary(stacked streamstats minimum): %v", err)
	}
	if !strings.Contains(
		summary.SQL,
		typeColumns[1]+` AS "__os_field_summary_stored_type"`,
	) {
		t.Fatalf(
			"stacked streamstats minimum summary did not consume the final stored type %s:\n%s",
			typeColumns[1],
			summary.SQL,
		)
	}
	if got, want := strings.Count(summary.SQL, "?"), len(summary.Args); got != want {
		t.Fatalf("summary placeholders = %d, args = %d\nSQL: %s\nargs: %#v", got, want, summary.SQL, summary.Args)
	}
}

func TestStreamStatsMinimumValidationOnlyGraphAmplificationBoundary(t *testing.T) {
	t.Parallel()

	owner := spl.Range{
		Start: spl.Position{Offset: 7, Line: 1, Column: 8},
		End:   spl.Position{Offset: 21, Line: 1, Column: 22},
	}
	barriers := make([]compiledChronologicalBarrier, 60)
	for index := range barriers {
		barriers[index] = compiledChronologicalBarrier{
			validationColumns: []string{quoteIdentifier("validation")},
			fanout:            1,
			ownerRange:        owner,
		}
	}
	if err := validateChronologicalGraphAmplification(
		barriers[:59],
		eventStatsCatalogSourceFanout,
		owner,
	); err != nil {
		t.Fatalf("validation-only graph at 128 bounded-leaf reads: %v", err)
	}
	err := validateChronologicalGraphAmplification(
		barriers,
		eventStatsCatalogSourceFanout,
		owner,
	)
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
		t.Fatalf("validation-only graph over boundary error = %#v", err)
	}
	if diagnostic.Range != owner ||
		!strings.Contains(
			diagnostic.Message,
			strconv.FormatUint(MaximumEventStatsGraphAmplification, 10),
		) {
		t.Fatalf("validation-only graph diagnostic = %#v, want range and fixed limit", diagnostic)
	}
}

func TestCompileStreamStatsMinimumCanonicalDefaultAndDefensiveValidation(t *testing.T) {
	t.Parallel()

	logical := buildPlan(
		t,
		`index=gradethis | streamstats min(Payload.Items)`,
	)
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(canonical streamstats minimum): %v", err)
	}
	if !slices.Contains(compiled.OutputFields, "min(Payload.Items)") ||
		!strings.Contains(compiled.SQL, `AS "min(Payload.Items)"`) {
		t.Fatalf("minimum canonical output missing: fields=%#v\n%s", compiled.OutputFields, compiled.SQL)
	}

	valid := func() *plan.StreamAggregate {
		return &plan.StreamAggregate{
			Measure: plan.AggregateMeasure{
				Function: plan.AggregateFunctionMinimum,
				Input:    mustResolveStreamStatsField(t, "status"),
				Output:   "low",
			},
			IncludeCurrent: true,
			Global:         true,
		}
	}
	for _, test := range []struct {
		name   string
		mutate func(*plan.StreamAggregate)
	}{
		{"missing input", func(operator *plan.StreamAggregate) { operator.Measure.Input = plan.FieldRef{} }},
		{"forged canonical input", func(operator *plan.StreamAggregate) { operator.Measure.Input.Canonical = true }},
		{"forged input path", func(operator *plan.StreamAggregate) { operator.Measure.Input.Path = []string{"attacker"} }},
		{"mismatched default", func(operator *plan.StreamAggregate) { operator.Measure.Output = "min(other)" }},
		{"maximum function", func(operator *plan.StreamAggregate) { operator.Measure.Function = plan.AggregateFunctionMaximum }},
		{"predicate", func(operator *plan.StreamAggregate) { operator.Measure.Predicate = &plan.BooleanExpression{} }},
		{"percentile", func(operator *plan.StreamAggregate) { operator.Measure.Percentile = 50 }},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			op := valid()
			test.mutate(op)
			if _, compileErr := (Compiler{}).Compile(appendStreamStatsOperator(
				buildPlan(t, `index=gradethis`),
				op,
			)); compileErr == nil {
				t.Fatal("Compile accepted forged streamstats minimum metadata")
			}
		})
	}

	openFields := valid()
	openFields.Measure.Input = mustResolveStreamStatsField(t, "fields")
	_, err = (Compiler{}).Compile(appendStreamStatsOperator(
		buildPlan(t, `index=gradethis`),
		openFields,
	))
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_AMBIGUOUS_STREAMSTATS_FIELD" {
		t.Fatalf("open fields minimum error = %#v", err)
	}
}
