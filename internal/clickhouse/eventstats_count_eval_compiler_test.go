package clickhouse

import (
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileEventStatsCountEvalMaterializesTrueOnlyMeasure(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eventstats count(eval(source="api")) AS matches | where matches=1 | table event_id matches`,
	)
	for _, required := range []string{
		`toUInt64(ifNull(`,
		`AS "__os_eventstats_measure_`,
		`toUInt64(sum(toUInt128("__os_eventstats_measure_`,
		`AS "matches"`,
		EventStatsInputLimitMarker,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("conditional eventstats SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("physical event scan count = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `source = ?`); got != 0 {
		t.Fatalf("predicate was lowered as a row filter (%d matches):\n%s", got, compiled.SQL)
	}
	if compiled.Args[0] != "api" || compiled.Args[len(compiled.Args)-1] != int64(1) {
		t.Fatalf("conditional eventstats arguments = %#v, want predicate first and downstream filter last", compiled.Args)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}

func TestCompileEventStatsCountEvalGroupsAndKeepsNullableOutput(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eventstats count(eval(event_id="wanted")) AS matches BY bucket | table event_id matches`,
	)
	for _, required := range []string{
		`toUInt64(ifNull(`,
		`toUInt64(sum(toUInt128("__os_eventstats_measure_`,
		`GROUP BY "__os_eventstats_group_0"`,
		`LEFT JOIN`,
		`CAST(NULL AS Nullable(UInt64))`,
		`"__os_eventstats_exists_`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("grouped conditional eventstats SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	wantPrefix := []any{
		"wanted",
		"bucket",
		"bucket.",
		"bucket",
		"bucket.",
	}
	if len(compiled.Args) < len(wantPrefix) ||
		!slices.Equal(compiled.Args[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("conditional/group argument prefix = %#v, want %#v", compiled.Args, wantPrefix)
	}
}

func TestCompileEventStatsCountEvalTreatsNullPredicateAsZero(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eventstats count(eval(if(event_id="null-row", null, if(event_id="match-row", true, false))=true)) AS matches | table event_id matches`,
	)
	if !strings.Contains(compiled.SQL, `toUInt64(ifNull(`) {
		t.Fatalf("nullable conditional eventstats measure is not null-safe:\n%s", compiled.SQL)
	}
	if !slices.Equal(compiled.Args[:2], []any{"null-row", "match-row"}) {
		t.Fatalf("nullable predicate arguments = %#v, want branch order", compiled.Args)
	}
}

func TestCompileEventStatsCountEvalFencesCalculatedPredicateField(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | spath input=_raw output=selected path=value | eventstats count(eval(selected="wanted")) AS matches | table event_id matches`,
	)
	for _, required := range []string{
		`__os_eventstats_predicate_input_`,
		`AS MATERIALIZED (`,
		`ARRAY JOIN`,
		`__os_stats_predicate_bound_`,
		`AS "__os_eventstats_measure_`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("calculated conditional eventstats SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(strings.ToUpper(compiled.SQL), "ARRAY JOIN"); got != 1 {
		t.Fatalf("calculated predicate fences = %d, want 1:\n%s", got, compiled.SQL)
	}
	cteStart := strings.Index(compiled.SQL, `WITH "__os_eventstats_predicate_input_`)
	if cteStart < 0 {
		t.Fatalf("conditional eventstats predicate CTE is missing:\n%s", compiled.SQL)
	}
	cteEndOffset := strings.Index(compiled.SQL[cteStart:], `) SELECT * FROM "__os_eventstats_predicate_input_`)
	if cteEndOffset < 0 {
		t.Fatalf("conditional eventstats predicate CTE boundary is missing:\n%s", compiled.SQL)
	}
	boundedPredicateSQL := compiled.SQL[cteStart : cteStart+cteEndOffset]
	sentinel := `LIMIT ` + strconv.FormatUint(MaximumEventStatsInputRows+1, 10)
	if !strings.Contains(boundedPredicateSQL, sentinel) {
		t.Fatalf(
			"predicate MATERIALIZED CTE is not bounded to the sentinel rows before eventstats input:\n%s",
			compiled.SQL,
		)
	}

	downstream := compileSPL(
		t,
		`index=gradethis | spath input=_raw output=selected path=value | eventstats count(eval(selected="wanted")) AS matches | where selected="wanted" | table event_id matches`,
	)
	if !strings.Contains(downstream.SQL, `__os_stats_predicate_bound_`) {
		t.Fatalf("downstream conditional eventstats SQL has no predicate fence:\n%s", downstream.SQL)
	}
	lastWhere := strings.LastIndex(downstream.SQL, ` WHERE `)
	if lastWhere < 0 {
		t.Fatalf("downstream conditional eventstats SQL has no filter:\n%s", downstream.SQL)
	}
	downstreamFilterSQL := downstream.SQL[lastWhere:]
	if !strings.Contains(downstreamFilterSQL, `dynamicType("selected")`) {
		t.Fatalf("downstream filter did not rebind calculated field to its public column:\n%s", downstream.SQL)
	}
	if strings.Contains(downstreamFilterSQL, `__os_stats_predicate_bound_`) {
		t.Fatalf("private predicate alias leaked into downstream compilation:\n%s", downstream.SQL)
	}
}

func TestCompileEventStatsCountEvalMaterializesRepeatedExactNumericKeysOnce(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eventstats count(eval(source="api" AND ratio>9007199254740992.75 AND ratio<9007199254740994)) AS matches | table event_id matches`,
	)
	for _, required := range []string{
		`AS "__os_eventstats_exact_key_`,
		`AS "__os_eventstats_exact_numeric_`,
		`["__os_eventstats_exact_key_`,
		`"__os_eventstats_exact_numeric_`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("repeated exact-numeric eventstats SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, ` AS "__os_eventstats_exact_key_`); got != 1 {
		t.Fatalf("exact numeric key definitions = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, ` AS "__os_eventstats_exact_numeric_`); got != 1 {
		t.Fatalf("exact numeric eligibility definitions = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `"__os_eventstats_exact_key_`); got < 3 {
		t.Fatalf("exact numeric key references = %d, want definition plus both comparisons:\n%s", got, compiled.SQL)
	}
	if len(compiled.Args) == 0 || compiled.Args[0] != "api" {
		t.Fatalf("conditional eventstats arguments = %#v, want predicate argument order preserved", compiled.Args)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}

func TestCompileEventStatsCountEvalComposesCalculatedFenceWithExactNumericKeys(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | spath input=_raw output=selected path=value | eventstats count(eval(selected>9007199254740992 AND selected<9007199254740994)) AS matches | table event_id matches`,
	)
	for _, required := range []string{
		`ARRAY JOIN`,
		`__os_stats_predicate_bound_`,
		`AS "__os_eventstats_exact_key_`,
		`AS "__os_eventstats_exact_numeric_`,
		`LIMIT ` + strconv.FormatUint(MaximumEventStatsInputRows+1, 10),
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("calculated exact-numeric eventstats SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(strings.ToUpper(compiled.SQL), "ARRAY JOIN"); got != 1 {
		t.Fatalf("calculated exact-numeric predicate fences = %d, want 1:\n%s", got, compiled.SQL)
	}
}

func TestCompileEventStatsCountEvalTreatsProjectedFieldAsMissing(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | fields event_id | eventstats count(eval(isnotnull(probe))) AS matches | table event_id matches`,
	)
	if strings.Contains(compiled.SQL, `dynamicElement("fields", 'probe')`) {
		t.Fatalf("projected-away predicate resurrected probe from storage:\n%s", compiled.SQL)
	}
	if !strings.Contains(compiled.SQL, `toUInt64(ifNull(`) ||
		!strings.Contains(compiled.SQL, `AS "matches"`) {
		t.Fatalf("projected conditional eventstats measure missing:\n%s", compiled.SQL)
	}
}

func TestCompileEventStatsCountEvalRejectsForgedPlans(t *testing.T) {
	t.Parallel()

	validPredicate := func() plan.Expression {
		return &plan.EvalComparisonExpression{
			Left: &plan.ScalarFieldExpression{
				Field: plan.FieldRef{Name: "source", Canonical: true},
			},
			Op: plan.ComparisonOpEqual,
			Right: &plan.ScalarLiteralExpression{
				Value: plan.Value{Kind: plan.ValueKindString, String: "api"},
			},
		}
	}
	field := plan.FieldRef{Name: "source", Canonical: true}
	var typedNil *plan.EvalComparisonExpression
	tests := []struct {
		name   string
		mutate func(*plan.EventAggregate)
	}{
		{
			name: "missing predicate",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Predicate = nil
			},
		},
		{
			name: "typed nil predicate",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Predicate = typedNil
			},
		},
		{
			name: "input metadata",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Input = field
			},
		},
		{
			name: "empty input path metadata",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Input.Path = []string{}
			},
		},
		{
			name: "percentile metadata",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Percentile = 95
			},
		},
		{
			name: "base search predicate",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Predicate = &plan.TextExpression{Value: "unsafe"}
			},
		},
		{
			name: "non Boolean scalar predicate",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Predicate = &plan.ScalarPredicateExpression{
					Value: &plan.ScalarFieldExpression{Field: field},
				}
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			base := buildPlan(t, `index=gradethis | eventstats count AS matches`)
			logical, operator := cloneEventAggregatePlan(t, base)
			operator.Measure.Function = plan.AggregateFunctionCountPredicate
			operator.Measure.Predicate = validPredicate()
			test.mutate(operator)
			_, err := (Compiler{}).Compile(logical)
			if err == nil || errors.Is(err, nil) {
				t.Fatal("Compile succeeded for forged conditional eventstats measure")
			}
		})
	}
}

func TestCompileEventStatsCountEvalRejectsReservedOpenFieldsPayload(t *testing.T) {
	t.Parallel()

	base := buildPlan(t, `index=gradethis | eventstats count AS matches`)
	logical, operator := cloneEventAggregatePlan(t, base)
	fields, err := plan.ResolveField("fields", operator.Range)
	if err != nil {
		t.Fatal(err)
	}
	operator.Measure.Function = plan.AggregateFunctionCountPredicate
	operator.Measure.Predicate = &plan.EvalComparisonExpression{
		Left: &plan.ScalarFieldExpression{Field: fields},
		Op:   plan.ComparisonOpEqual,
		Right: &plan.ScalarLiteralExpression{
			Value: plan.Value{Kind: plan.ValueKindString, String: "unsafe"},
		},
	}
	_, err = (Compiler{}).Compile(logical)
	diagnostic := &plan.Diagnostic{}
	if !errors.As(err, &diagnostic) ||
		diagnostic.Code != "SPL_AMBIGUOUS_EVENTSTATS_FIELD" {
		t.Fatalf("Compile error = %#v, want reserved eventstats fields diagnostic", err)
	}
}
