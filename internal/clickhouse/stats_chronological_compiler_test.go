package clickhouse

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestCompileStatsChronologicalRejectsForgedCanonicalTimeProvenance(t *testing.T) {
	t.Parallel()

	service := mustResolveChronologicalField(t, "service")
	canonicalTime := mustResolveChronologicalField(t, "_time")

	for _, test := range []struct {
		name     string
		upstream plan.Operator
	}{
		{
			name: "missing canonical time",
			upstream: &plan.Project{
				Mode:   plan.ProjectModeTable,
				Fields: []plan.FieldRef{service},
			},
		},
		{
			name: "modified canonical time",
			upstream: &plan.TimeBucket{
				Field:  canonicalTime,
				Output: canonicalTime,
				Span:   5 * time.Minute,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical := buildPlan(t, `index=gradethis | stats earliest(service) AS first_seen`)
			if len(logical.Operators) < 2 {
				t.Fatalf("logical operator count = %d, want upstream and aggregate", len(logical.Operators))
			}
			aggregateIndex := len(logical.Operators) - 1
			if _, ok := logical.Operators[aggregateIndex].(*plan.Aggregate); !ok {
				t.Fatalf("terminal operator = %T, want aggregate", logical.Operators[aggregateIndex])
			}
			forged := make([]plan.Operator, 0, len(logical.Operators)+1)
			forged = append(forged, logical.Operators[:aggregateIndex]...)
			forged = append(forged, test.upstream, logical.Operators[aggregateIndex])
			logical.Operators = forged

			_, err := (Compiler{}).Compile(logical)
			var diagnostic *plan.Diagnostic
			if !errors.As(err, &diagnostic) ||
				diagnostic.Code != "SPL_UNSUPPORTED_STATS_TIME_FIELD" {
				t.Fatalf(
					"Compile(forged chronological plan) error = %#v, want SPL_UNSUPPORTED_STATS_TIME_FIELD",
					err,
				)
			}
		})
	}
}

func TestCompileStatsChronologicalScalarUsesImmutableEventOrderAndSharesStates(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | sort 0 +service | stats earliest(service) AS first_seen latest(service) AS last_seen earliest(service) AS first_again`,
	)
	if !slices.Equal(compiled.OutputFields, []string{"first_seen", "last_seen", "first_again"}) {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}

	const rowKey = `tuple("__os_sort_time", "__os_sort_event_id", "__os_sort_visibility_seq", "__os_sort_source_identity") AS "__os_chronological_row_key"`
	if strings.Count(compiled.SQL, rowKey) != 1 {
		t.Fatalf("chronological row key is not materialized exactly once:\n%s", compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, "argMinOrNullIf("); got != 1 {
		t.Fatalf("argMinOrNullIf state count = %d, want one shared earliest state:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, "argMaxOrNullIf("); got != 1 {
		t.Fatalf("argMaxOrNullIf state count = %d, want one latest state:\n%s", got, compiled.SQL)
	}

	for _, function := range []string{"argMinOrNullIf(", "argMaxOrNullIf("} {
		call := chronologicalAggregateCall(t, compiled.SQL, function)
		const scalarKey = `tuple("__os_chronological_row_key", toUInt64(1))`
		if strings.Count(call, scalarKey) != 1 {
			t.Fatalf("%s ordering key is not the immutable row key plus scalar ordinal:\n%s", function, call)
		}
		if strings.Contains(call, `"__os_order_`) {
			t.Fatalf("%s depends on the user sort order:\n%s", function, call)
		}
	}

	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}

func TestCompileStatsChronologicalMultivalueIsStreamingAndValidationCannotBePruned(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats earliest(payload) AS first_seen latest(payload) AS last_seen earliest(payload) AS first_again`,
	)
	if got := strings.Count(compiled.SQL, "argMinOrNullArray("); got != 1 {
		t.Fatalf("argMinOrNullArray state count = %d, want one shared earliest state:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, "argMaxOrNullArray("); got != 1 {
		t.Fatalf("argMaxOrNullArray state count = %d, want one latest state:\n%s", got, compiled.SQL)
	}
	for _, function := range []string{"argMinOrNullArray(", "argMaxOrNullArray("} {
		call := chronologicalAggregateCall(t, compiled.SQL, function)
		for _, required := range []string{
			`"__os_chronological_row_key"`,
			"arrayEnumerate(",
		} {
			if !strings.Contains(call, required) {
				t.Fatalf("%s call is missing %q:\n%s", function, required, call)
			}
		}
		if !strings.Contains(call, "toUInt64(member_ordinal)") &&
			!strings.Contains(call, "toUInt64(member_index)") {
			t.Fatalf("%s call does not append the one-based member ordinal to its key:\n%s", function, call)
		}
		if strings.Contains(call, `"__os_order_`) {
			t.Fatalf("%s depends on a pipeline-order key:\n%s", function, call)
		}
	}

	const validation = `"__os_stats_chronological_any_unsupported_0"`
	if strings.Count(compiled.SQL, ` AS `+validation) != 1 ||
		strings.Contains(compiled.SQL, `"__os_stats_chronological_any_unsupported_1"`) {
		t.Fatalf("same-input validation was not shared:\n%s", compiled.SQL)
	}
	if !strings.Contains(compiled.SQL, "max(toUInt8(") ||
		!strings.Contains(compiled.SQL, UnsupportedStatsMeasureValueMarker) ||
		!strings.Contains(compiled.SQL, `WHERE throwIf(toUInt8(`+validation+` != 0), '`+
			UnsupportedStatsMeasureValueMarker+`') = 0`) {
		t.Fatalf("chronological validation is not forced after aggregation:\n%s", compiled.SQL)
	}

	upperSQL := strings.ToUpper(compiled.SQL)
	for _, forbidden := range []string{
		" OVER (",
		"ARRAY JOIN",
		"ARRAYJOIN(",
		"GROUPARRAY",
	} {
		if strings.Contains(upperSQL, forbidden) {
			t.Fatalf("chronological aggregation retained unbounded/order-dependent %q:\n%s", forbidden, compiled.SQL)
		}
	}

	projected := compileSPL(
		t,
		`index=gradethis | stats earliest(payload) AS discarded BY service | table service`,
	)
	if !slices.Equal(projected.OutputFields, []string{"service"}) ||
		!strings.Contains(projected.SQL, ` AS `+validation) ||
		!strings.Contains(projected.SQL, `WHERE throwIf(toUInt8(`+validation+` != 0), '`+
			UnsupportedStatsMeasureValueMarker+`') = 0`) {
		t.Fatalf("downstream projection could prune chronological validation:\n%s", projected.SQL)
	}

	distinctInputs := compileSPL(
		t,
		`index=gradethis | stats earliest(payload) AS first_payload latest(other) AS last_other`,
	)
	for _, distinctValidation := range []string{
		` AS "__os_stats_chronological_any_unsupported_0"`,
		` AS "__os_stats_chronological_any_unsupported_1"`,
	} {
		if strings.Count(distinctInputs.SQL, distinctValidation) != 1 {
			t.Fatalf("distinct inputs did not receive separate validation state %q:\n%s", distinctValidation, distinctInputs.SQL)
		}
	}
}

func TestCompileStatsChronologicalMaximumAliasesKeepConstantAggregateState(t *testing.T) {
	t.Parallel()

	var source strings.Builder
	source.WriteString(`index=gradethis | stats `)
	for measure := 0; measure < spl.MaximumStatsMeasures; measure++ {
		if measure > 0 {
			source.WriteByte(' ')
		}
		function := "earliest"
		if measure%2 != 0 {
			function = "latest"
		}
		fmt.Fprintf(&source, "%s(service) AS result_%d", function, measure)
	}

	compiled := compileSPL(t, source.String())
	if len(compiled.OutputFields) != spl.MaximumStatsMeasures {
		t.Fatalf(
			"chronological output fields = %d, want %d",
			len(compiled.OutputFields),
			spl.MaximumStatsMeasures,
		)
	}
	if strings.Count(compiled.SQL, "argMinOrNullIf(") != 1 ||
		strings.Count(compiled.SQL, "argMaxOrNullIf(") != 1 {
		t.Fatalf("maximum aliases duplicated chronological aggregate state:\n%s", compiled.SQL)
	}
	if strings.Count(
		compiled.SQL,
		`tuple("__os_sort_time", "__os_sort_event_id", "__os_sort_visibility_seq", "__os_sort_source_identity") AS "__os_chronological_row_key"`,
	) != 1 {
		t.Fatalf("maximum aliases duplicated the immutable row key:\n%s", compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d", got, want)
	}
}

func mustResolveChronologicalField(t *testing.T, name string) plan.FieldRef {
	t.Helper()
	field, err := plan.ResolveField(name, spl.Range{})
	if err != nil {
		t.Fatalf("ResolveField(%q): %v", name, err)
	}
	return field
}

func chronologicalAggregateCall(t *testing.T, sql string, function string) string {
	t.Helper()

	start := strings.Index(sql, function)
	if start < 0 {
		t.Fatalf("SQL is missing %s:\n%s", function, sql)
	}
	depth := 0
	for index := start + len(function) - 1; index < len(sql); index++ {
		switch sql[index] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return sql[start : index+1]
			}
		}
	}
	t.Fatalf("SQL contains an unterminated %s call:\n%s", function, sql)
	return ""
}
