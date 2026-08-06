package clickhouse

import (
	"errors"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestCompileStreamStatsMaximumUsesDirectionCorrectRunningWinner(t *testing.T) {
	t.Parallel()

	logical := buildPlan(
		t,
		`index=gradethis | sort 0 +event_id`+
			` | streamstats max(streamstats_value) AS running_max`,
	)
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(streamstats maximum): %v", err)
	}
	if !strings.Contains(compiled.SQL, `argMaxOrNullIf(`) ||
		strings.Contains(compiled.SQL, `argMinOrNullIf(`) ||
		!strings.Contains(compiled.SQL, `AS "running_max"`) {
		t.Fatalf("streamstats maximum is not direction-correct:\n%s", compiled.SQL)
	}
	if strings.Count(compiled.SQL, `arrayFold(`) != 1 ||
		strings.Count(compiled.SQL, `argMaxOrNullIf(`) != 1 ||
		strings.Count(compiled.SQL, `FROM "open_splunk"."events"`) != 1 {
		t.Fatalf("streamstats maximum lost its one-fold, one-window, one-scan shape:\n%s", compiled.SQL)
	}
	for _, forbidden := range []string{
		"ARRAY JOIN", "arrayJoin(", "groupArray(", "argMaxArray(",
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("streamstats maximum contains forbidden %q:\n%s", forbidden, compiled.SQL)
		}
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsMaximumPinsEveryFrameShape(t *testing.T) {
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
			compiled := compileSPL(
				t,
				`index=gradethis | streamstats `+test.options+
					` max(streamstats_value) AS running_max`,
			)
			if strings.Count(compiled.SQL, `argMaxOrNullIf(`) != 1 ||
				strings.Count(compiled.SQL, test.frame) != 1 {
				t.Fatalf("streamstats maximum frame is not exact:\n%s", compiled.SQL)
			}
			assertBoundedStreamStatsSQL(t, compiled)
		})
	}
}

func TestCompileStreamStatsMaximumPreservesFixedScalarAndMultivalueTypes(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name, source, required string
		forbidden              []string
	}{
		{
			name:     "fixed numeric",
			source:   `index=gradethis | streamstats max(severity) AS highest`,
			required: `maxIfOrNull(tupleElement("__os_streamstats_measure_`,
			forbidden: []string{
				`argMaxOrNullIf(`, `arrayFold(`, `toFloat64("severity")`,
			},
		},
		{
			name:     "fixed time",
			source:   `index=gradethis | streamstats max(_time) AS last_time`,
			required: `maxIfOrNull(tupleElement("__os_streamstats_measure_`,
			forbidden: []string{
				`argMaxOrNullIf(`, `arrayFold(`, `CAST(NULL AS Dynamic)`,
			},
		},
		{
			name:     "computed bool",
			source:   `index=gradethis | eval selected=true | streamstats max(selected) AS any_selected BY service`,
			required: `CAST(NULL AS Nullable(Bool))`,
			forbidden: []string{
				`argMaxOrNullIf(`, `arrayFold(`, `dynamicType("selected")`,
			},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled := compileSPL(t, test.source)
			if !strings.Contains(compiled.SQL, test.required) ||
				!strings.Contains(compiled.SQL, ` OVER (`) {
				t.Fatalf("%s maximum lost native lowering:\n%s", test.name, compiled.SQL)
			}
			for _, forbidden := range test.forbidden {
				if strings.Contains(compiled.SQL, forbidden) {
					t.Fatalf("%s maximum contains %q:\n%s", test.name, forbidden, compiled.SQL)
				}
			}
			assertBoundedStreamStatsSQL(t, compiled)
		})
	}

	stringMaximum := compileSPL(
		t,
		`index=gradethis | streamstats max(service) AS high`+
			` | eval copied=high | table event_id high copied`,
	)
	measure := eventStatsPrivateAlias(t, stringMaximum.SQL, "__os_streamstats_measure_")
	typeColumn := eventStatsPrivateAlias(t, stringMaximum.SQL, "__os_streamstats_extrema_type_")
	for _, required := range []string{
		`CAST(toString("service") AS Nullable(String))`,
		`argMaxOrNullIf(tuple(tupleElement(` + measure,
		` AS ` + typeColumn,
		`AS "high"`,
		`AS "copied"`,
	} {
		if !strings.Contains(stringMaximum.SQL, required) {
			t.Fatalf("scalar String maximum missing %q:\n%s", required, stringMaximum.SQL)
		}
	}
	if strings.Contains(stringMaximum.SQL, `arrayFold(`) ||
		strings.Count(stringMaximum.SQL, `argMaxOrNullIf(`) != 1 {
		t.Fatalf("scalar String maximum lost scalar one-window lowering:\n%s", stringMaximum.SQL)
	}

	multivalue := compileSPL(
		t,
		`index=gradethis | stats values(host) AS hosts`+
			` | streamstats max(hosts) AS running_max | table hosts running_max`,
	)
	for _, required := range []string{
		`arrayFold((__os_streamstats_extrema_state, value) ->`,
		`argMaxOrNullIf(`,
		`AS "running_max"`,
	} {
		if !strings.Contains(multivalue.SQL, required) {
			t.Fatalf("fixed multivalue maximum missing %q:\n%s", required, multivalue.SQL)
		}
	}
	if strings.Count(multivalue.SQL, `groupUniqArray`) != 1 ||
		strings.Count(multivalue.SQL, `FROM "open_splunk"."events"`) != 1 {
		t.Fatalf("fixed multivalue maximum duplicated collection or scan:\n%s", multivalue.SQL)
	}
	for _, forbidden := range []string{"ARRAY JOIN", "arrayJoin(", "groupArray("} {
		if strings.Contains(multivalue.SQL, forbidden) {
			t.Fatalf("fixed multivalue maximum contains %q:\n%s", forbidden, multivalue.SQL)
		}
	}
}

func TestCompileStreamStatsMaximumSeparatesGroupEligibilityFromPoisonValidation(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | streamstats window=2 global=false`+
			` max(payload) AS running_max BY required_group`+
			` | search running_max=* | table event_id running_max`,
	)
	measure := eventStatsPrivateAlias(t, compiled.SQL, "__os_streamstats_measure_")
	eligible := eventStatsPrivateAlias(t, compiled.SQL, "__os_streamstats_eligible_")
	for _, required := range []string{
		`PARTITION BY ` + eligible,
		`argMaxOrNullIf(`,
		`tupleElement(` + measure + `, 6)`,
		`CAST(NULL AS Dynamic)`,
		UnsupportedStatsByValueMarker,
		UnsupportedStatsMeasureValueMarker,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("grouped maximum missing %q:\n%s", required, compiled.SQL)
		}
	}
	gate := `if(` + eligible + ` != 0, arrayFold(`
	if gateAt, eligibilityAt := strings.Index(compiled.SQL, gate),
		strings.Index(compiled.SQL, ` AS `+eligible); gateAt < 0 ||
		eligibilityAt < 0 || gateAt >= eligibilityAt {
		t.Fatalf("Dynamic maximum fold is not gated outside BY classification:\n%s", compiled.SQL)
	}
	wantPrefix := []any{"payload", "payload.", "required_group", "required_group."}
	if len(compiled.Args) <= len(wantPrefix) ||
		!slices.Equal(compiled.Args[:len(wantPrefix)], wantPrefix) ||
		compiled.Args[len(wantPrefix)] != "tenant-1" {
		t.Fatalf("grouped maximum args = %#v, want %#v then tenant", compiled.Args, wantPrefix)
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsMaximumValidationSurvivesProjectionAndEmptyOutput(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | streamstats max(payload) AS discarded`+
			` | table event_id | search definitely_missing=value`,
	)
	if !slices.Equal(compiled.OutputFields, []string{"event_id"}) {
		t.Fatalf("discarded maximum output fields = %#v", compiled.OutputFields)
	}
	barrier := eventStatsPrivateAlias(t, compiled.SQL, "__os_streamstats_result_")
	validation := eventStatsPrivateAlias(t, compiled.SQL, "__os_streamstats_validation_")
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
			t.Fatalf("hidden maximum validation missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.LastIndex(compiled.SQL, `UNION ALL`) < strings.LastIndex(compiled.SQL, `WHERE 0`) ||
		strings.Count(compiled.SQL, ` AS MATERIALIZED (`) != 1 ||
		strings.Count(compiled.SQL, `arrayFold(`) != 1 {
		t.Fatalf("hidden maximum validation can be pruned or duplicates work:\n%s", compiled.SQL)
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsMaximumProjectedInputStackingAndReplacement(t *testing.T) {
	t.Parallel()

	projected := compileSPL(
		t,
		`index=gradethis | fields event_id`+
			` | streamstats max(streamstats_value) AS running_max`+
			` | table event_id running_max`,
	)
	if !strings.Contains(projected.SQL, eventStatsExtremaEmptyRowStateSQL("0")) ||
		!strings.Contains(projected.SQL, `CAST(NULL AS Dynamic)`) ||
		slices.Contains(projected.Args, any("streamstats_value")) ||
		slices.Contains(projected.Args, any("streamstats_value.")) {
		t.Fatalf("projected-away maximum rebound or became nonempty:\n%s\nargs=%#v", projected.SQL, projected.Args)
	}

	replaced := compileSPL(
		t,
		`index=gradethis | sort 0 +streamstats_value`+
			` | streamstats max(streamstats_value) AS streamstats_value`+
			` | table event_id streamstats_value`,
	)
	if !regexp.MustCompile(
		`"__os_order_[0-9]+_0" AS "__os_streamstats_order_[0-9]+_0"`,
	).MatchString(replaced.SQL) || !strings.Contains(replaced.SQL, `AS "streamstats_value"`) {
		t.Fatalf("maximum replacement lost its incoming order snapshot:\n%s", replaced.SQL)
	}

	stacked := compileSPL(
		t,
		`index=gradethis | streamstats max(first_payload) AS high`+
			` | streamstats max(high) AS higher | eval copied=higher`+
			` | table event_id high higher copied`,
	)
	typePattern := regexp.MustCompile(`"__os_streamstats_extrema_type_[0-9]+"`)
	seen := make(map[string]struct{})
	for _, match := range typePattern.FindAllString(stacked.SQL, -1) {
		seen[match] = struct{}{}
	}
	if len(seen) != 2 || strings.Count(stacked.SQL, `argMaxOrNullIf(`) != 2 ||
		strings.Count(stacked.SQL, `arrayFold(`) != 2 ||
		!strings.Contains(stacked.SQL, `AS "copied"`) ||
		slices.Contains(stacked.Args, any("high")) ||
		slices.Contains(stacked.Args, any("high.")) {
		t.Fatalf("stacked maximum lost type provenance or rebound storage:\n%s\nargs=%#v", stacked.SQL, stacked.Args)
	}
	assertBoundedStreamStatsSQL(t, stacked)
}

func TestCompileStreamStatsMaximumPreservesTypedAndRawBytesAcrossStacking(t *testing.T) {
	t.Parallel()

	stacked := compileSPL(
		t,
		`index=gradethis | streamstats max(_raw) AS high`+
			` | streamstats max(high) AS higher | table high higher`,
	)
	for _, required := range []string{
		`'bytes/v1'`,
		`tryBase64Decode(`,
		`modulo(length(__os_raw_base64_payload), 4) = 2`,
		`modulo(length(__os_raw_base64_payload), 4) = 3`,
		`replaceRegexpOne(base64Encode(`,
		`= toUInt8(` + strconv.Itoa(int(eventfields.StoredValueTypeBytes)) + `)`,
	} {
		if !strings.Contains(stacked.SQL, required) {
			t.Fatalf("stacked Bytes maximum is missing %q:\n%s", required, stacked.SQL)
		}
	}
	if strings.Contains(stacked.SQL, `tryBase64Decode(__os_stats_extrema_dynamic_lexical)`) ||
		strings.Count(stacked.SQL, `argMaxOrNullIf(`) != 2 ||
		strings.Count(stacked.SQL, `arrayFold(`) != 1 ||
		strings.Count(stacked.SQL, `FROM "open_splunk"."events"`) != 1 {
		t.Fatalf("stacked Bytes maximum lost padded one-fold lowering:\n%s", stacked.SQL)
	}
	assertBoundedStreamStatsSQL(t, stacked)
}

func TestCompileStreamStatsMaximumCanonicalDefaultAndDefensiveValidation(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | streamstats max(Payload.Items)`)
	if !slices.Contains(compiled.OutputFields, "max(Payload.Items)") ||
		!strings.Contains(compiled.SQL, `AS "max(Payload.Items)"`) {
		t.Fatalf("maximum canonical output missing: fields=%#v\n%s", compiled.OutputFields, compiled.SQL)
	}

	valid := func() *plan.StreamAggregate {
		return &plan.StreamAggregate{
			Measure: plan.AggregateMeasure{
				Function: plan.AggregateFunctionMaximum,
				Input:    mustResolveStreamStatsField(t, "status"),
				Output:   "high",
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
		{"mismatched default", func(operator *plan.StreamAggregate) { operator.Measure.Output = "max(other)" }},
		{"predicate", func(operator *plan.StreamAggregate) { operator.Measure.Predicate = &plan.BooleanExpression{} }},
		{"percentile", func(operator *plan.StreamAggregate) { operator.Measure.Percentile = 50 }},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			op := valid()
			test.mutate(op)
			if _, err := (Compiler{}).Compile(appendStreamStatsOperator(
				buildPlan(t, `index=gradethis`),
				op,
			)); err == nil {
				t.Fatal("Compile accepted forged streamstats maximum metadata")
			}
		})
	}

	openFields := valid()
	openFields.Measure.Input = mustResolveStreamStatsField(t, "fields")
	_, err := (Compiler{}).Compile(appendStreamStatsOperator(
		buildPlan(t, `index=gradethis`),
		openFields,
	))
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_AMBIGUOUS_STREAMSTATS_FIELD" {
		t.Fatalf("open fields maximum error = %#v", err)
	}
}

func TestStreamStatsMaximumValidationGraphPinsAmplificationBoundary(t *testing.T) {
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
		t.Fatalf("maximum validation graph at 128 bounded-leaf reads: %v", err)
	}
	err := validateChronologicalGraphAmplification(
		barriers,
		eventStatsCatalogSourceFanout,
		owner,
	)
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" ||
		diagnostic.Range != owner ||
		!strings.Contains(diagnostic.Message, strconv.FormatUint(MaximumEventStatsGraphAmplification, 10)) {
		t.Fatalf("maximum validation graph over boundary diagnostic = %#v", err)
	}
}
