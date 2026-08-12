package clickhouse

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileStatsOrderAndTimeKeepsPipelineAndEventOrderSeparate(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | sort 0 +metric | stats `+
			`first(service) AS pipeline_first last(service) AS pipeline_last `+
			`earliest_time(service) AS event_earliest latest_time(service) AS event_latest `+
			`rate(metric) AS per_second`,
	)
	if !slices.Equal(compiled.OutputFields, []string{
		"pipeline_first",
		"pipeline_last",
		"event_earliest",
		"event_latest",
		"per_second",
	}) {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}

	// first/last consume the row number formed from the current pipeline sort.
	// The occurrence-time functions and rate instead consume the immutable
	// event-time/event-id key, so a user sort cannot silently redefine time.
	for _, required := range []string{
		`row_number() OVER (ORDER BY`,
		`AS "__os_list_row_ordinal"`,
		`tuple("__os_sort_time", "__os_sort_event_id", "__os_sort_visibility_seq", "__os_sort_source_identity") AS "__os_chronological_row_key"`,
		`argMinOrNullIf(tupleElement("__os_chronological_candidates_0", 1), tuple("__os_list_row_ordinal",`,
		`argMaxOrNullIf(tupleElement("__os_chronological_candidates_0", 2), tuple("__os_list_row_ordinal",`,
		`argMinOrNullIf(ifNotFinite(toFloat64(toUnixTimestamp64Nano("_time")) / 1000000000, CAST(NULL AS Nullable(Float64))), "__os_chronological_row_key",`,
		`argMaxOrNullIf(ifNotFinite(toFloat64(toUnixTimestamp64Nano("_time")) / 1000000000, CAST(NULL AS Nullable(Float64))), "__os_chronological_row_key",`,
		`argMinOrNullIf(arrayElementOrNull("__os_measure_values_0", 1), "__os_chronological_row_key",`,
		`argMaxOrNullIf(arrayElementOrNull("__os_measure_values_0", -1), "__os_chronological_row_key",`,
		`countIf(isNotNull(arrayElementOrNull("__os_measure_values_0", 1))`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("order/time SQL is missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(strings.ToUpper(compiled.SQL), "ARRAY JOIN") {
		t.Fatalf("order/time stats unexpectedly expanded rows:\n%s", compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d", got, want)
	}
}

func TestCompileStatsOrderAndTimeEvalInputsUseTheSameOrderingContracts(t *testing.T) {
	t.Parallel()

	const lexicalExpression = `lower(service)`
	compiled := compileSPL(
		t,
		`index=gradethis | sort 0 +metric | stats `+
			`first(eval(`+lexicalExpression+`)) AS pipeline_first `+
			`last(eval(`+lexicalExpression+`)) AS pipeline_last `+
			`earliest_time(eval(`+lexicalExpression+`)) AS event_earliest `+
			`latest_time(eval(`+lexicalExpression+`)) AS event_latest `+
			`rate(eval(metric+5)) AS shifted_rate`,
	)
	if !slices.Equal(compiled.OutputFields, []string{
		"pipeline_first",
		"pipeline_last",
		"event_earliest",
		"event_latest",
		"shifted_rate",
	}) {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}
	if got := strings.Count(
		compiled.SQL,
		` AS "__os_measure_numeric_expression_value_0"`,
	); got != 1 {
		t.Fatalf("shared lexical eval materializations = %d, want 1:\n%s", got, compiled.SQL)
	}
	for _, required := range []string{
		`row_number() OVER (ORDER BY`,
		`"__os_list_row_ordinal"`,
		`"__os_chronological_row_key"`,
		`"__os_measure_numeric_expression_1"`,
		`countIf(isNotNull(arrayElementOrNull("__os_measure_numeric_expression_1", 1))`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("eval order/time SQL is missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, "__os_measure_numeric_expression_value_2") ||
		strings.Contains(strings.ToUpper(compiled.SQL), "ARRAY JOIN") {
		t.Fatalf("eval order/time inputs were duplicated or expanded:\n%s", compiled.SQL)
	}
}

func TestCompileStatsTimeFunctionsRequireCanonicalEventTime(t *testing.T) {
	t.Parallel()

	service := mustResolveChronologicalField(t, "service")
	metric := mustResolveChronologicalField(t, "metric")
	for _, function := range []string{
		"earliest_time(service)",
		"latest_time(service)",
		"rate(metric)",
	} {
		function := function
		t.Run(function, func(t *testing.T) {
			t.Parallel()

			logical := buildPlan(t, `index=gradethis | stats `+function)
			aggregateIndex := len(logical.Operators) - 1
			if aggregateIndex < 1 {
				t.Fatalf("logical operator count = %d", len(logical.Operators))
			}
			forged := make([]plan.Operator, 0, len(logical.Operators)+1)
			forged = append(forged, logical.Operators[:aggregateIndex]...)
			forged = append(
				forged,
				&plan.Project{
					Mode:   plan.ProjectModeTable,
					Fields: []plan.FieldRef{service, metric},
				},
				logical.Operators[aggregateIndex],
			)
			logical.Operators = forged
			_, err := (Compiler{}).Compile(logical)
			var diagnostic *plan.Diagnostic
			if !errors.As(err, &diagnostic) ||
				diagnostic.Code != "SPL_UNSUPPORTED_STATS_TIME_FIELD" {
				t.Fatalf(
					"Compile(%s without _time) error = %#v, want SPL_UNSUPPORTED_STATS_TIME_FIELD",
					function,
					err,
				)
			}
		})
	}
}
