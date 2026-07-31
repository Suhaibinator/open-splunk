package clickhouse

import (
	"errors"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestCompileEventStatsCountFieldUsesBoundedOccurrenceInput(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eventstats count(eventstats_value) AS occurrences | table event_id occurrences`,
	)
	measureAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_eventstats_measure_",
	)
	valueAlias := strings.Replace(
		measureAlias,
		"__os_eventstats_measure_",
		"__os_eventstats_value_count_",
		1,
	)
	inputCountAlias := strings.Replace(
		measureAlias,
		"__os_eventstats_measure_",
		"__os_eventstats_input_count_",
		1,
	)
	totalRowAlias := strings.Replace(
		measureAlias,
		"__os_eventstats_measure_",
		"__os_eventstats_total_row_",
		1,
	)
	for _, required := range []string{
		`AS ` + measureAlias,
		`toUInt64(sum(toUInt128(` + measureAlias + `))) AS ` + valueAlias,
		`AS ` + inputCountAlias,
		totalRowAlias + `.` + valueAlias + ` AS "occurrences"`,
		`arrayCount(element -> dynamicType(element) != 'None'`,
		EventStatsInputLimitMarker,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("eventstats count(field) SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `count()`); got != 1 {
		t.Fatalf("eventstats count(field) row guards = %d, want 1:\n%s", got, compiled.SQL)
	}
	for _, forbidden := range []string{"ARRAY JOIN", "GROUP BY", "groupArray("} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("global eventstats count(field) SQL contains %q:\n%s", forbidden, compiled.SQL)
		}
	}
	wantPrefix := []any{"eventstats_value", "eventstats_value."}
	if len(compiled.Args) < len(wantPrefix) ||
		!slices.Equal(compiled.Args[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("count(field) argument prefix = %#v, want %#v", compiled.Args, wantPrefix)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}

func TestCompileEventStatsCountFieldGroupsIndependentlyFromOccurrence(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eventstats count(eventstats_value) AS occurrences BY eventstats_group | table event_id occurrences`,
	)
	measureAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_eventstats_measure_",
	)
	groupCountAlias := strings.Replace(
		measureAlias,
		"__os_eventstats_measure_",
		"__os_eventstats_group_count_",
		1,
	)
	eligibleAlias := strings.Replace(
		measureAlias,
		"__os_eventstats_measure_",
		"__os_eventstats_eligible_",
		1,
	)
	existsAlias := strings.Replace(
		measureAlias,
		"__os_eventstats_measure_",
		"__os_eventstats_exists_",
		1,
	)
	for _, required := range []string{
		`toUInt64(sum(toUInt128(` + measureAlias + `))) AS ` + groupCountAlias,
		eligibleAlias + ` != 0`,
		`GROUP BY "__os_eventstats_group_0"`,
		`LEFT JOIN`,
		existsAlias,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("grouped eventstats count(field) SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(
		compiled.SQL,
		`toUInt64(sum(toUInt128(`+measureAlias+`)))`,
	); got != 1 {
		t.Fatalf(
			"grouped eventstats count(field) occurrence sums = %d, want 1:\n%s",
			got,
			compiled.SQL,
		)
	}
	groupMeasure := `toUInt64(sum(toUInt128(` + measureAlias + `))) AS ` + groupCountAlias
	groupMeasureAt := strings.Index(compiled.SQL, groupMeasure)
	groupWhereAt := strings.Index(compiled.SQL[groupMeasureAt:], " WHERE ")
	groupByAt := strings.Index(compiled.SQL[groupMeasureAt:], " GROUP BY ")
	if groupMeasureAt < 0 || groupWhereAt < 0 || groupByAt < 0 ||
		strings.Contains(
			compiled.SQL[groupMeasureAt+groupWhereAt:groupMeasureAt+groupByAt],
			measureAlias,
		) {
		t.Fatalf("measure presence incorrectly filtered zero-valued groups:\n%s", compiled.SQL)
	}
	for _, forbidden := range []string{"ARRAY JOIN", "groupArray(", "groupUniqArray"} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("grouped eventstats count(field) SQL contains %q:\n%s", forbidden, compiled.SQL)
		}
	}
	wantPrefix := []any{
		"eventstats_value",
		"eventstats_value.",
		"eventstats_group",
		"eventstats_group.",
		"eventstats_group",
		"eventstats_group.",
	}
	if len(compiled.Args) < len(wantPrefix) ||
		!slices.Equal(compiled.Args[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("measure/group argument prefix = %#v, want %#v", compiled.Args, wantPrefix)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}

func TestCompileEventStatsCountFieldTreatsProjectedInputAsMissing(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | fields event_id | eventstats count(eventstats_value) AS occurrences | table event_id occurrences`,
	)
	measureAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_eventstats_measure_",
	)
	if !strings.Contains(
		compiled.SQL,
		`toUInt64(0) AS `+measureAlias,
	) {
		t.Fatalf("projected count(field) input was not fixed at zero:\n%s", compiled.SQL)
	}
	if slices.Contains(compiled.Args, any("eventstats_value")) ||
		slices.Contains(compiled.Args, any("eventstats_value.")) {
		t.Fatalf("projected-away count(field) input was rebound from storage: %#v", compiled.Args)
	}
}

func TestCompileEventStatsCountFieldUsesFixedMultivalueCardinality(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats values(host) AS hosts | eventstats count(hosts) AS occurrences | table hosts occurrences`,
	)
	measureAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_eventstats_measure_",
	)
	if !strings.Contains(
		compiled.SQL,
		`toUInt64(length("hosts")) AS `+measureAlias,
	) {
		t.Fatalf("fixed multivalue count(field) did not use cardinality:\n%s", compiled.SQL)
	}
	if strings.Contains(strings.ToUpper(compiled.SQL), "ARRAY JOIN") {
		t.Fatalf("fixed multivalue count(field) expanded rows:\n%s", compiled.SQL)
	}
}

func TestCompileEventStatsCountFieldCountsCalculatedHomogeneousArrays(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval lowered=lower(eventstats_letters) | eventstats count(lowered) AS occurrences | table event_id occurrences`,
	)
	for _, required := range []string{
		`dynamicType("lowered") = 'Array(Dynamic)'`,
		`startsWith(dynamicType("lowered"), 'Array(')`,
		`ifNull(toUInt64(length("lowered")), toUInt64(0))`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf(
				"calculated homogeneous-array count SQL missing %q:\n%s",
				required,
				compiled.SQL,
			)
		}
	}
}

func TestCompileEventStatsCountFieldResolvesInputBeforeAliasReplacement(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eventstats count(status) AS status | table event_id status`,
	)
	measureAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_eventstats_measure_",
	)
	if !strings.Contains(compiled.SQL, `AS `+measureAlias) ||
		!strings.Contains(compiled.SQL, `AS "status"`) {
		t.Fatalf("count(field) alias replacement lost its upstream input:\n%s", compiled.SQL)
	}
	wantPrefix := []any{"status", "status."}
	if len(compiled.Args) < len(wantPrefix) ||
		!slices.Equal(compiled.Args[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("alias replacement input args = %#v, want %#v", compiled.Args, wantPrefix)
	}
}

func TestCompileEventStatsCountFieldProtectsOpenFieldsPayload(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis | eventstats count AS occurrences`)
	setEventStatsCountFieldInput(t, logical, "fields")
	_, err := (Compiler{}).Compile(logical)
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) ||
		diagnostic.Code != "SPL_AMBIGUOUS_EVENTSTATS_FIELD" {
		t.Fatalf("open fields count input error = %#v, want SPL_AMBIGUOUS_EVENTSTATS_FIELD", err)
	}

	closed := compileSPL(
		t,
		`index=gradethis | stats count AS fields | eventstats count(fields) AS occurrences | table fields occurrences`,
	)
	if !slices.Equal(closed.OutputFields, []string{"fields", "occurrences"}) {
		t.Fatalf("closed fields count output = %#v", closed.OutputFields)
	}
}

func TestCompileEventStatsCountFieldRejectsForgedMetadata(t *testing.T) {
	t.Parallel()

	base := buildPlan(
		t,
		`index=gradethis | eventstats count(eventstats_value) AS occurrences`,
	)
	tests := []struct {
		name   string
		mutate func(*plan.EventAggregate)
	}{
		{
			name: "predicate",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Predicate = &plan.ComparisonExpression{
					Field: plan.FieldRef{Name: "host", Canonical: true},
					Op:    plan.ComparisonOpEqual,
					Value: plan.Value{Kind: plan.ValueKindString, String: "x"},
				}
			},
		},
		{
			name: "percentile",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Percentile = 50
			},
		},
		{
			name: "noncanonical input metadata",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Input.Canonical = true
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
				t.Fatal("Compile() accepted forged count(field) metadata")
			}
		})
	}
}

func setEventStatsCountFieldInput(
	t *testing.T,
	logical *plan.Query,
	inputName string,
) {
	t.Helper()
	input, err := plan.ResolveField(inputName, spl.Range{})
	if err != nil {
		t.Fatalf("resolve count(field) input: %v", err)
	}
	for _, operator := range logical.Operators {
		eventAggregate, ok := operator.(*plan.EventAggregate)
		if !ok {
			continue
		}
		eventAggregate.Measure.Function = plan.AggregateFunctionCountValues
		eventAggregate.Measure.Input = input
		return
	}
	t.Fatal("logical plan has no EventAggregate")
}

func eventStatsPrivateAlias(t *testing.T, sql, prefix string) string {
	t.Helper()
	pattern := regexp.MustCompile(`"` + regexp.QuoteMeta(prefix) + `[0-9]+"`)
	alias := pattern.FindString(sql)
	if alias == "" {
		t.Fatalf("eventstats SQL has no private %q alias:\n%s", prefix, sql)
	}
	return alias
}
