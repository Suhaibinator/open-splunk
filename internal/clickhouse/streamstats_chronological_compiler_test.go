package clickhouse

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileStreamStatsChronologicalUsesPipelineFramesAndImmutableChronology(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | sort 0 -event_id`+
			` | streamstats window=3 global=false earliest(payload) AS first_seen BY service`+
			` | streamstats current=false latest(payload) AS prior_last`+
			` | table event_id first_seen prior_last`,
	)
	if !slices.Equal(compiled.OutputFields, []string{"event_id", "first_seen", "prior_last"}) {
		t.Fatalf("streamstats chronological output fields = %#v", compiled.OutputFields)
	}
	for _, function := range []string{"argMinOrNullIf(", "argMaxOrNullIf("} {
		call := chronologicalAggregateCall(t, compiled.SQL, function)
		if !strings.Contains(call, `tupleElement("__os_streamstats_measure_`) {
			t.Fatalf("%s does not consume its materialized bounded measure:\n%s", function, call)
		}
		if strings.Contains(call, `"__os_streamstats_order_`) {
			t.Fatalf("%s uses pipeline order as chronological winner order:\n%s", function, call)
		}
	}
	for _, identity := range []string{
		`"__os_sort_time"`,
		`"__os_sort_event_id"`,
		`"__os_sort_visibility_seq"`,
		`"__os_sort_source_identity"`,
	} {
		if !strings.Contains(compiled.SQL, identity) {
			t.Fatalf("streamstats chronology is missing immutable identity %s:\n%s", identity, compiled.SQL)
		}
	}
	for _, frame := range []string{
		"ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
		"ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING",
	} {
		if !strings.Contains(compiled.SQL, frame) {
			t.Fatalf("streamstats chronological SQL is missing frame %q:\n%s", frame, compiled.SQL)
		}
	}
	upperSQL := strings.ToUpper(compiled.SQL)
	for _, forbidden := range []string{
		"ARRAY JOIN",
		"ARRAYJOIN(",
		"GROUPARRAY",
		"ARRAYFOLD(",
	} {
		if strings.Contains(upperSQL, forbidden) {
			t.Fatalf("streamstats chronological lowering retained %q:\n%s", forbidden, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("streamstats chronological physical scans = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}

func TestCompileStreamStatsChronologicalOnlySelectsRequestedDirection(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		function  string
		required  []string
		forbidden []string
	}{
		{
			name:     "earliest",
			function: "earliest",
			required: []string{"arrayFirstIndex(", "argMinOrNullIf("},
			forbidden: []string{
				"arrayLastIndex(", "argMaxOrNullIf(", "arrayFold(", "groupArray",
			},
		},
		{
			name:     "latest",
			function: "latest",
			required: []string{"arrayLastIndex(", "argMaxOrNullIf("},
			forbidden: []string{
				"arrayFirstIndex(", "argMinOrNullIf(", "arrayFold(", "groupArray",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			compiled := compileSPL(
				t,
				`index=gradethis | streamstats `+test.function+
					`(payload) AS selected | table event_id selected`,
			)
			for _, required := range test.required {
				if !strings.Contains(compiled.SQL, required) {
					t.Fatalf("%s lowering is missing %q:\n%s", test.name, required, compiled.SQL)
				}
			}
			for _, forbidden := range test.forbidden {
				if strings.Contains(compiled.SQL, forbidden) {
					t.Fatalf("%s lowering retained unused %q:\n%s", test.name, forbidden, compiled.SQL)
				}
			}
			if got := strings.Count(compiled.SQL, test.required[1]); got != 1 {
				t.Fatalf("%s window states = %d, want one:\n%s", test.name, got, compiled.SQL)
			}
		})
	}
}

func TestCompileStreamStatsChronologicalRetainsAtomicValidation(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | streamstats earliest(payload) AS discarded | table event_id`,
		`index=gradethis | streamstats latest(payload) AS discarded BY service` +
			` | search definitely_missing=value | table event_id`,
	} {
		compiled := compileSPL(t, source)
		for _, required := range []string{
			"AS MATERIALIZED",
			"UNION ALL",
			UnsupportedStatsMeasureValueMarker,
			`"__os_streamstats_validation_`,
		} {
			if !strings.Contains(compiled.SQL, required) {
				t.Fatalf("streamstats chronological validation is missing %q:\n%s", required, compiled.SQL)
			}
		}
		if strings.Contains(source, "definitely_missing") &&
			strings.LastIndex(compiled.SQL, "UNION ALL") < strings.LastIndex(compiled.SQL, "WHERE 0") {
			t.Fatalf("downstream empty result can prune chronological validation:\n%s", compiled.SQL)
		}
	}
}

func TestChronologicalPublicationPreservesInvalidUTF8AsBytes(t *testing.T) {
	t.Parallel()

	valueSQL := chronologicalPublishedValueSQL(`"winner"`)
	for _, required := range []string{
		`isValidUTF8(assumeNotNull("winner"))`,
		`'bytes/v1'`,
		`base64Encode(assumeNotNull("winner"))`,
		`CAST(NULL AS Dynamic)`,
	} {
		if !strings.Contains(valueSQL, required) {
			t.Fatalf("chronological publication is missing %q: %s", required, valueSQL)
		}
	}
	typeSQL := chronologicalPublishedTypeSQL(`"winner"`)
	if !strings.Contains(typeSQL, `isValidUTF8(assumeNotNull("winner"))`) {
		t.Fatalf("chronological stored type does not classify UTF-8: %s", typeSQL)
	}
}

func TestCompileStreamStatsChronologicalRejectsForgedCanonicalTime(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis | streamstats min(payload) AS selected`)
	operatorIndex := len(logical.Operators) - 1
	operator, ok := logical.Operators[operatorIndex].(*plan.StreamAggregate)
	if !ok {
		t.Fatalf("terminal operator = %T, want *plan.StreamAggregate", logical.Operators[operatorIndex])
	}
	operator.Measure.Function = plan.AggregateFunctionEarliest
	logical.Operators = append(
		append([]plan.Operator(nil), logical.Operators[:operatorIndex]...),
		&plan.Project{
			Mode:   plan.ProjectModeTable,
			Fields: []plan.FieldRef{operator.Measure.Input},
		},
		operator,
	)

	_, err := (Compiler{}).Compile(logical)
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) ||
		diagnostic.Code != "SPL_UNSUPPORTED_STREAMSTATS_TIME_FIELD" {
		t.Fatalf("missing canonical time error = %#v", err)
	}
	if diagnostic.Range != operator.Measure.Input.Range {
		t.Fatalf("missing canonical time range = %#v, want %#v", diagnostic.Range, operator.Measure.Input.Range)
	}
}
