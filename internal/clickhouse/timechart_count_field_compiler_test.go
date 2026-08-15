package clickhouse

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	timechartCountValueAlias      = `"__os_tc_measure_count"`
	timechartCountRowAlias        = `"__os_tc_row_count"`
	timechartOccurrenceCountAlias = `"__os_tc_occurrence_count"`
	timechartCollapsedRowAlias    = `"__os_tc_collapsed_row_count"`
	timechartCollapsedCountAlias  = `"__os_tc_collapsed_count"`
	timechartSeriesScoreAlias     = `"__os_tc_series_score"`
	timechartSeriesRankAlias      = `"__os_tc_series_rank"`
)

func TestCompileFixedTimechartCountFieldUsesOneBoundedOccurrenceContribution(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis message="Request metrics" | timechart span=5m count(counted_value) AS eligible_values`,
	)

	if !slices.Equal(compiled.OutputFields, []string{"_time", "eligible_values"}) {
		t.Fatalf("public fields = %#v, want fixed aliased schema", compiled.OutputFields)
	}
	if compiled.Timechart == nil ||
		compiled.Timechart.Mode != TimechartModeFixedFieldCount ||
		compiled.Timechart.ValueField != "eligible_values" ||
		compiled.Timechart.ValueKind != TimechartValueKindInvalid ||
		compiled.Timechart.MaxSeries != 1 ||
		compiled.Timechart.MaxLabelBytes != 0 {
		t.Fatalf("compiled fixed count(field) metadata = %#v", compiled.Timechart)
	}

	for _, required := range []string{
		`"__os_timechart_source" AS (`,
		`AS ` + timechartCountValueAlias,
		`arrayCount(element -> dynamicType(element) != 'None'`,
		`"__os_timechart_group_counts" AS MATERIALIZED (`,
		`toUInt64(sum(toUInt128(` + timechartCountValueAlias + `))) AS "` + TimechartCountColumn + `"`,
		`"__os_timechart_input_presence" AS (`,
		`toUInt8(count() > 0) AS "` + TimechartInputPresentColumn + `"`,
		`FROM "__os_timechart_group_counts"`,
		`AS "` + TimechartInputPresentColumn + `"`,
		`FROM numbers(?)`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("fixed timechart count(field) SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	assertTimechartCountFieldShape(t, compiled)

	wantArgumentPrefix := []any{"counted_value", "counted_value."}
	if len(compiled.Args) < len(wantArgumentPrefix) ||
		!reflect.DeepEqual(compiled.Args[:len(wantArgumentPrefix)], wantArgumentPrefix) {
		t.Fatalf("count(field) argument prefix = %#v, want %#v", compiled.Args, wantArgumentPrefix)
	}
}

func TestCompileFixedTimechartCountFieldTreatsProjectedInputAsZero(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | fields _time | timechart span=5m count(projected_value) AS eligible_values`,
	)

	if !strings.Contains(compiled.SQL, `toUInt64(0) AS `+timechartCountValueAlias) {
		t.Fatalf("projected count(field) input was not normalized to zero:\n%s", compiled.SQL)
	}
	if slices.Contains(compiled.Args, any("projected_value")) ||
		slices.Contains(compiled.Args, any("projected_value.")) {
		t.Fatalf("projected count(field) input was rebound from storage: %#v", compiled.Args)
	}
	if !strings.Contains(compiled.SQL, `AS "`+TimechartInputPresentColumn+`"`) {
		t.Fatalf("projected count(field) lost independent row-presence transport:\n%s", compiled.SQL)
	}
	assertTimechartCountFieldShape(t, compiled)
}

func TestCompileSplitTimechartCountFieldRanksOccurrencesButKeepsRowDomain(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis message="Request metrics" | timechart span=5m count(counted_value) BY service`,
	)

	if !slices.Equal(compiled.OutputFields, []string{"_time"}) ||
		compiled.Timechart == nil ||
		compiled.Timechart.Mode != TimechartModeRuntimeWide ||
		compiled.Timechart.MaxSeries != 12 ||
		compiled.Timechart.MaxLabelBytes != MaximumTimechartLabelBytes ||
		compiled.Timechart.ValueField != "" ||
		compiled.Timechart.ValueKind != TimechartValueKindInvalid {
		t.Fatalf("compiled split count(field) = fields %#v metadata %#v", compiled.OutputFields, compiled.Timechart)
	}

	for _, required := range []string{
		`AS ` + timechartCountValueAlias,
		`count() AS ` + timechartCountRowAlias,
		`toUInt64(sum(toUInt128(` + timechartCountValueAlias + `))) AS ` + timechartOccurrenceCountAlias,
		`sum(toUInt128(` + timechartOccurrenceCountAlias + `)) OVER (PARTITION BY "__os_tc_kind", "__os_tc_label") AS ` + timechartSeriesScoreAlias,
		`dense_rank() OVER (PARTITION BY "__os_tc_kind" ORDER BY ` + timechartSeriesScoreAlias + ` DESC, "__os_tc_label" ASC) AS ` + timechartSeriesRankAlias,
		`"__os_tc_kind" = 0 AND ` + timechartSeriesRankAlias + ` <= 10`,
		`sum(` + timechartCountRowAlias + `) AS ` + timechartCollapsedRowAlias,
		`toUInt64(sum(toUInt128(` + timechartOccurrenceCountAlias + `))) AS ` + timechartCollapsedCountAlias,
		`sumIf(` + timechartCountRowAlias + `, "__os_tc_kind" = 3)`,
		`uniqExact("__os_tc_label") OVER (PARTITION BY "__os_tc_kind"`,
		`maxIf("__os_tc_collision_cardinality", "__os_tc_kind" = 0) > 1`,
		`FROM "__os_timechart_collapsed" WHERE "__os_tc_encoded" != '' AND ` + timechartCollapsedRowAlias + ` > 0`,
		`arrayPushBack(groupArrayIf("__os_tc_encoded", "__os_tc_encoded" != ''), CAST('' AS String))`,
		`toUInt64(max("__os_tc_invalid" != 0 OR "__os_tc_collision" != 0))`,
		`mapFromArrays(`,
		`AS "` + TimechartCountsColumn + `"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("split timechart count(field) SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	for relation, want := range map[string]int{
		`FROM "__os_timechart_group_counts"`: 1,
		`FROM "__os_timechart_scored"`:       1,
		`FROM "__os_timechart_ranked"`:       1,
		`FROM "__os_timechart_collapsed"`:    2,
	} {
		if got := strings.Count(compiled.SQL, relation); got != want {
			t.Fatalf("split count(field) relation %q occurs %d times, want %d:\n%s", relation, got, want, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, ` AS MATERIALIZED (`); got != 1 {
		t.Fatalf("split count(field) materialized CTE count = %d, want collapsed only:\n%s", got, compiled.SQL)
	}

	scored := timechartCTESection(
		t,
		compiled.SQL,
		"__os_timechart_scored",
		"__os_timechart_ranked",
	)
	if strings.Contains(scored, `sum(toUInt128(`+timechartCountRowAlias+`)) OVER`) ||
		!strings.Contains(scored, `sum(toUInt128(`+timechartOccurrenceCountAlias+`)) OVER`) {
		t.Fatalf("series scoring is not occurrence-based:\n%s", scored)
	}
	collapsed := timechartCTESection(
		t,
		compiled.SQL,
		"__os_timechart_collapsed",
		"__os_timechart_domain_rows",
	)
	if !strings.Contains(collapsed, `sumIf(`+timechartCountRowAlias+`, "__os_tc_kind" = 3)`) {
		t.Fatalf("split validation is not row-based:\n%s", collapsed)
	}
	if strings.Contains(compiled.SQL, `WHERE `+timechartCountValueAlias+` > 0`) ||
		strings.Contains(compiled.SQL, `WHERE `+timechartCountValueAlias+` != 0`) {
		t.Fatalf("zero-occurrence rows were removed from the split domain:\n%s", compiled.SQL)
	}
	assertTimechartCountFieldShape(t, compiled)
}

func TestCompileSplitTimechartCountFieldKeepsProjectedInputInRowDomain(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | fields _time service | timechart span=5m count(projected_value) BY service`,
	)

	if !strings.Contains(compiled.SQL, `toUInt64(0) AS `+timechartCountValueAlias) {
		t.Fatalf("projected split measure was not normalized to zero:\n%s", compiled.SQL)
	}
	if slices.Contains(compiled.Args, any("projected_value")) ||
		slices.Contains(compiled.Args, any("projected_value.")) {
		t.Fatalf("projected split measure was rebound from storage: %#v", compiled.Args)
	}
	for _, rowDomainFragment := range []string{
		`count() AS ` + timechartCountRowAlias,
		`FROM "__os_timechart_collapsed" WHERE "__os_tc_encoded" != '' AND ` + timechartCollapsedRowAlias + ` > 0`,
		`sumIf(` + timechartCountRowAlias + `, "__os_tc_kind" = 3)`,
	} {
		if !strings.Contains(compiled.SQL, rowDomainFragment) {
			t.Fatalf("projected split count(field) lost row domain %q:\n%s", rowDomainFragment, compiled.SQL)
		}
	}
	assertTimechartCountFieldShape(t, compiled)
}

func TestCompileFixedTimechartCountFieldAfterStreamStatsPreservesTransportEnvelope(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | streamstats count AS running | timechart span=5m count(duration) AS populated`,
	)
	if compiled.Timechart == nil ||
		compiled.Timechart.Mode != TimechartModeFixedFieldCount ||
		!slices.Equal(compiled.OutputFields, []string{"_time", "populated"}) {
		t.Fatalf("streamstats count(field) output contract = %#v", compiled)
	}

	wantProjection := `SELECT "` + TimechartOrdinalColumn + `", "` +
		TimechartCountColumn + `", "` + TimechartInputPresentColumn +
		`" FROM "__os_chronological_final_input_`
	if !strings.Contains(compiled.SQL, wantProjection) {
		t.Fatalf(
			"fixed count(field) chronological envelope is missing %q:\n%s",
			wantProjection,
			compiled.SQL,
		)
	}
	rowCountProjection := `SELECT "` + TimechartOrdinalColumn + `", "` +
		TimechartCountColumn + `" FROM "__os_chronological_final_input_`
	if strings.Contains(compiled.SQL, rowCountProjection) {
		t.Fatalf("fixed count(field) chronological envelope lost input presence:\n%s", compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("streamstats count(field) physical scans = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf(
			"placeholder count = %d, args = %d\nSQL: %s\nargs: %#v",
			got,
			want,
			compiled.SQL,
			compiled.Args,
		)
	}
}

func TestCompileTimechartCountFieldRevalidatesForgedPlan(t *testing.T) {
	t.Parallel()

	compileSPL(
		t,
		`index=gradethis | timechart span=5m count(counted_value) AS eligible_values`,
	)

	for _, test := range []struct {
		name   string
		mutate func(*plan.Query, *plan.Timechart)
	}{
		{
			name: "missing input",
			mutate: func(_ *plan.Query, operator *plan.Timechart) {
				operator.Measure.Input = plan.FieldRef{}
			},
		},
		{
			name: "noncanonical input",
			mutate: func(_ *plan.Query, operator *plan.Timechart) {
				operator.Measure.Input.Canonical = true
			},
		},
		{
			name: "forged input path",
			mutate: func(_ *plan.Query, operator *plan.Timechart) {
				operator.Measure.Input.Path = []string{"attacker"}
			},
		},
		{
			name: "predicate metadata",
			mutate: func(_ *plan.Query, operator *plan.Timechart) {
				operator.Measure.Predicate = &plan.BooleanExpression{}
			},
		},
		{
			name: "percentile metadata",
			mutate: func(_ *plan.Query, operator *plan.Timechart) {
				operator.Measure.Percentile = 50
			},
		},
		{
			name: "empty output",
			mutate: func(query *plan.Query, operator *plan.Timechart) {
				operator.Measure.Output = ""
				query.OutputFields[1] = ""
			},
		},
		{
			name: "time output collision",
			mutate: func(query *plan.Query, operator *plan.Timechart) {
				operator.Measure.Output = "_time"
				query.OutputFields[1] = "_time"
			},
		},
		{
			name: "fixed output disagreement",
			mutate: func(query *plan.Query, _ *plan.Timechart) {
				query.OutputFields[1] = "attacker"
			},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			logical, operator := parsedTimechartCountFieldPlan(
				t,
				`index=gradethis | timechart span=5m count(counted_value) AS eligible_values`,
			)
			test.mutate(logical, operator)
			if _, err := (Compiler{}).Compile(logical); err == nil {
				t.Fatal("Compile accepted forged timechart count(field) metadata")
			}
		})
	}

	t.Run("reserved open fields payload", func(t *testing.T) {
		t.Parallel()

		logical, operator := parsedTimechartCountFieldPlan(
			t,
			`index=gradethis | timechart span=5m count(counted_value) AS eligible_values`,
		)
		fields, err := plan.ResolveField("fields", spl.Range{})
		if err != nil {
			t.Fatalf("ResolveField fields: %v", err)
		}
		operator.Measure.Input = fields
		if _, err := (Compiler{}).Compile(logical); err == nil {
			t.Fatal("Compile accepted the open event fields payload as count(field) input")
		}
	})

	t.Run("input and split collision", func(t *testing.T) {
		t.Parallel()

		logical, operator := parsedTimechartCountFieldPlan(
			t,
			`index=gradethis | timechart span=5m count(counted_value) BY service`,
		)
		operator.Measure.Input = operator.Split.Field
		if _, err := (Compiler{}).Compile(logical); err == nil {
			t.Fatal("Compile accepted the same count(field) input and split field")
		}
	})

	t.Run("missing split output contract", func(t *testing.T) {
		t.Parallel()

		logical, _ := parsedTimechartCountFieldPlan(
			t,
			`index=gradethis | timechart span=5m count(counted_value) BY service`,
		)
		logical.DynamicOutput = nil
		if _, err := (Compiler{}).Compile(logical); err == nil {
			t.Fatal("Compile accepted split count(field) without its bounded dynamic output")
		}
	})
}

func assertTimechartCountFieldShape(
	t *testing.T,
	compiled CompiledQuery,
) {
	t.Helper()
	assertCountFieldPhysicalShape(t, compiled, timechartCountValueAlias, "timechart")

	if compiled.Timechart != nil && compiled.Timechart.Mode == TimechartModeRuntimeWide {
		firstOrdinalNames := `if("__os_timechart_grid"."` + TimechartOrdinalColumn +
			`" = 0, "__os_timechart_domain".names, CAST([], 'Array(String)')) AS "` +
			TimechartNamesColumn + `"`
		if got := strings.Count(compiled.SQL, firstOrdinalNames); got != 1 {
			t.Fatalf(
				"runtime count(field) ordinal-zero names projections = %d, want 1:\n%s",
				got,
				compiled.SQL,
			)
		}
		if strings.Contains(
			compiled.SQL,
			`, "__os_timechart_domain".names AS "`+TimechartNamesColumn+`"`,
		) {
			t.Fatalf("runtime count(field) repeats names on every bucket:\n%s", compiled.SQL)
		}
	}
}

func assertCountFieldPhysicalShape(
	t *testing.T,
	compiled CompiledQuery,
	measureAlias string,
	subject string,
) {
	t.Helper()

	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("%s scoped storage scan occurs %d times, want once:\n%s", subject, got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `AS `+measureAlias); got != 1 {
		t.Fatalf("%s per-row countValue normalization definitions = %d, want one:\n%s", subject, got, compiled.SQL)
	}
	upperSQL := strings.ToUpper(compiled.SQL)
	if strings.Contains(upperSQL, "ARRAY JOIN") ||
		strings.Contains(upperSQL, "ARRAYJOIN(") {
		t.Fatalf("%s count(field) expanded rows:\n%s", subject, compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}

func parsedTimechartCountFieldPlan(
	t *testing.T,
	source string,
) (*plan.Query, *plan.Timechart) {
	t.Helper()

	logical := buildPlan(t, source)
	operator, ok := logical.Operators[len(logical.Operators)-1].(*plan.Timechart)
	if !ok {
		t.Fatalf("last logical operator = %T, want *plan.Timechart", logical.Operators[len(logical.Operators)-1])
	}
	if operator.Measure.Function != plan.AggregateFunctionCountValues {
		t.Fatalf(
			"parsed timechart measure = %d, want count(field)",
			operator.Measure.Function,
		)
	}
	return logical, operator
}

func timechartCTESection(t *testing.T, sql, name, next string) string {
	t.Helper()

	startMarker := `"` + name + `" AS `
	start := strings.Index(sql, startMarker)
	if start < 0 {
		t.Fatalf("timechart SQL has no %s CTE:\n%s", name, sql)
	}
	endMarker := `, "` + next + `" AS `
	endOffset := strings.Index(sql[start:], endMarker)
	if endOffset < 0 {
		t.Fatalf("timechart SQL has no %s CTE after %s:\n%s", next, name, sql)
	}
	return sql[start : start+endOffset]
}
