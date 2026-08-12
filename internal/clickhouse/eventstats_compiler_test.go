package clickhouse

import (
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestCompileEventStatsGlobalCountPreservesRowsAndBoundsInput(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | eventstats count AS total | table event_id total`)
	for _, required := range []string{
		` AS MATERIALIZED (`,
		`LIMIT ` + strconv.FormatUint(MaximumEventStatsInputRows+1, 10),
		`count() OVER ()`,
		EventStatsInputLimitMarker,
		`"__os_chronological_validation_`,
		`UNION ALL`,
		`AS "total"`,
		materializedCTESettingsSQL,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("eventstats SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	for _, forbidden := range []string{
		"ARRAY JOIN",
		"GROUP BY",
		"groupArray(",
		`"__os_eventstats_total_`,
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("global eventstats SQL contains %q:\n%s", forbidden, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `"__os_eventstats_input_`); got != 2 {
		t.Fatalf("global eventstats input consumers = %d, want definition plus one window read:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("physical event scan count = %d, want 1:\n%s", got, compiled.SQL)
	}
	if !slices.Equal(compiled.OutputFields, []string{"event_id", "total"}) {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}
}

func TestCompileEventStatsGroupedCountReusesStatsByScalarKeys(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eventstats count AS peers BY status host | table event_id status host peers`,
	)
	for _, required := range []string{
		` AS MATERIALIZED (`,
		`GROUP BY "__os_eventstats_group_0", "__os_eventstats_group_1"`,
		`LEFT JOIN`,
		`"__os_eventstats_exists_`,
		`dynamicType(`,
		UnsupportedStatsByValueMarker,
		EventStatsInputLimitMarker,
		materializedCTESettingsSQL,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("grouped eventstats SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	for _, forbidden := range []string{"ARRAY JOIN", "groupArray(", "groupUniqArray"} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("grouped eventstats SQL contains %q:\n%s", forbidden, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("physical event scan count = %d, want 1:\n%s", got, compiled.SQL)
	}
	if !slices.Equal(compiled.OutputFields, []string{"event_id", "status", "host", "peers"}) {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}
}

func TestCompileEventStatsGroupedCountKeepsMaterializedArgumentOrder(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eventstats count AS peers BY first_group second_group | table event_id peers`,
	)
	wantPrefix := []any{
		"first_group",
		"first_group.",
		"second_group",
		"second_group.",
		"first_group",
		"first_group.",
		"second_group",
		"second_group.",
	}
	if len(compiled.Args) < len(wantPrefix) ||
		!slices.Equal(compiled.Args[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("group-key argument prefix = %#v, want %#v", compiled.Args, wantPrefix)
	}
	if got := compiled.Args[len(wantPrefix)]; got != "tenant-1" {
		t.Fatalf("first nested scan argument = %#v, want tenant-1", got)
	}
}

func TestCompileEventStatsCountComposesAndReplacesAliases(t *testing.T) {
	t.Parallel()

	afterStats := compileSPL(
		t,
		`index=gradethis | stats count BY level | eventstats count AS groups | where groups>1 | table level count groups`,
	)
	if !slices.Equal(afterStats.OutputFields, []string{"level", "count", "groups"}) {
		t.Fatalf("statistics output fields = %v", afterStats.OutputFields)
	}
	if strings.Count(afterStats.SQL, `count()`) < 2 {
		t.Fatalf("stats and eventstats counts were not both compiled:\n%s", afterStats.SQL)
	}

	replaced := compileSPL(
		t,
		`index=gradethis | eventstats count AS level | table event_id level`,
	)
	if !slices.Equal(replaced.OutputFields, []string{"event_id", "level"}) {
		t.Fatalf("replacement output fields = %v", replaced.OutputFields)
	}
}

func TestCompileEventStatsDeferredStageWrapsTerminalWideOutputs(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
		valid  func(CompiledQuery) bool
	}{
		{
			name: "chart",
			source: `index=gradethis | eventstats count AS peers` +
				` | chart count OVER path BY level`,
			valid: func(compiled CompiledQuery) bool {
				return compiled.Chart != nil && compiled.Timechart == nil
			},
		},
		{
			name: "timechart",
			source: `index=gradethis | eventstats count AS peers` +
				` | timechart span=5m count BY path`,
			valid: func(compiled CompiledQuery) bool {
				return compiled.Timechart != nil && compiled.Chart == nil
			},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			compiled := compileSPL(t, test.source)
			if !test.valid(compiled) {
				t.Fatalf("%s output contract is missing", test.name)
			}
			for _, required := range []string{
				`"__os_eventstats_input_`,
				`"__os_eventstats_result_`,
				` AS MATERIALIZED (`,
				materializedCTESettingsSQL,
			} {
				if !strings.Contains(compiled.SQL, required) {
					t.Fatalf("%s SQL missing %q:\n%s", test.name, required, compiled.SQL)
				}
			}
			if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
				t.Fatalf("%s physical scans = %d, want 1:\n%s", test.name, got, compiled.SQL)
			}
			if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
				t.Fatalf("%s placeholders = %d, args = %d", test.name, got, want)
			}
		})
	}
}

func TestCompileEventStatsGroupedCountRejectsMultivalueKeys(t *testing.T) {
	t.Parallel()

	logical := buildPlan(
		t,
		`index=gradethis | stats values(host) AS hosts | eventstats count BY hosts`,
	)
	_, err := (Compiler{}).Compile(logical)
	diagnostic := &plan.Diagnostic{}
	if !errors.As(err, &diagnostic) ||
		diagnostic.Code != "SPL_UNSUPPORTED_MULTIVALUE_USAGE" ||
		!strings.Contains(diagnostic.Message, "eventstats BY") {
		t.Fatalf("Compile() error = %#v, want eventstats multivalue diagnostic", err)
	}
}

func TestCompileEventStatsDefensivelyRejectsForgedOperators(t *testing.T) {
	t.Parallel()

	base := buildPlan(t, `index=gradethis | eventstats count AS total`)

	tests := []struct {
		name   string
		mutate func(*plan.EventAggregate)
	}{
		{
			name: "missing output",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Output = ""
			},
		},
		{
			name: "wrong function",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Function = plan.AggregateFunctionCountValues
			},
		},
		{
			name: "input metadata",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Input = plan.FieldRef{Name: "host", Canonical: true}
			},
		},
		{
			name: "empty input path metadata",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Input.Path = []string{}
			},
		},
		{
			name: "predicate metadata",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Predicate = &plan.ComparisonExpression{
					Field: plan.FieldRef{Name: "host", Canonical: true},
					Op:    plan.ComparisonOpEqual,
					Value: plan.Value{Kind: plan.ValueKindString, String: "x"},
				}
			},
		},
		{
			name: "percentile metadata",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Percentile = 50
			},
		},
		{
			name: "too many groups",
			mutate: func(operator *plan.EventAggregate) {
				operator.GroupBy = make([]plan.FieldRef, spl.MaximumStatsGroupFields+1)
				for index := range operator.GroupBy {
					operator.GroupBy[index] = plan.FieldRef{
						Name: "field" + strconv.Itoa(index),
						Path: []string{"field" + strconv.Itoa(index)},
					}
				}
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			logical, operator := cloneEventAggregatePlan(t, base)
			test.mutate(operator)
			if _, err := (Compiler{}).Compile(logical); err == nil {
				t.Fatal("Compile() succeeded for forged eventstats operator")
			}
		})
	}
}

func cloneEventAggregatePlan(
	t *testing.T,
	base *plan.Query,
) (*plan.Query, *plan.EventAggregate) {
	t.Helper()
	logical := *base
	logical.Operators = append([]plan.Operator(nil), base.Operators...)
	for index, operator := range logical.Operators {
		eventAggregate, ok := operator.(*plan.EventAggregate)
		if !ok {
			continue
		}
		copyOperator := *eventAggregate
		copyOperator.GroupBy = append(
			[]plan.FieldRef(nil),
			eventAggregate.GroupBy...,
		)
		logical.Operators[index] = &copyOperator
		return &logical, &copyOperator
	}
	t.Fatal("logical plan has no EventAggregate")
	return nil, nil
}

func TestCompileEventStatsDefensivelyRejectsOpenFieldsPayload(t *testing.T) {
	t.Parallel()

	for _, mutate := range []func(*plan.EventAggregate){
		func(operator *plan.EventAggregate) { operator.Measure.Output = "fields" },
		func(operator *plan.EventAggregate) {
			operator.GroupBy = []plan.FieldRef{{Name: "fields", Path: []string{"fields"}}}
		},
	} {
		logical := buildPlan(t, `index=gradethis | eventstats count AS total`)
		operator := logical.Operators[len(logical.Operators)-1].(*plan.EventAggregate)
		mutate(operator)
		_, err := (Compiler{}).Compile(logical)
		diagnostic := &plan.Diagnostic{}
		if !errors.As(err, &diagnostic) ||
			diagnostic.Code != "SPL_AMBIGUOUS_EVENTSTATS_FIELD" {
			t.Fatalf("Compile() error = %#v, want reserved eventstats fields diagnostic", err)
		}
	}
}
