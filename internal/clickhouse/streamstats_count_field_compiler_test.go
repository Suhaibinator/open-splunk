package clickhouse

import (
	"errors"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileStreamStatsCountFieldUsesOneBoundedOccurrenceContribution(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | streamstats count(status) AS populated | table event_id populated`,
	)
	measure := streamStatsCountFieldPrivateAlias(t, compiled.SQL)
	for _, required := range []string{
		`arrayCount(element -> dynamicType(element) != 'None'`,
		` AS ` + measure,
		`toUInt64(ifNull(sum(toUInt128(` + measure + `)) OVER (`,
		`toUInt128(0))) AS "__os_streamstats_value_`,
		`count() OVER () AS "__os_streamstats_input_count_`,
		`AS "populated"`,
		StreamStatsInputLimitMarker,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("streamstats count(field) SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, measure); got != 2 {
		t.Fatalf("streamstats count(field) contribution alias occurs %d times, want definition and window use:\n%s", got, compiled.SQL)
	}
	wantPrefix := []any{"status", "status."}
	if len(compiled.Args) <= len(wantPrefix) ||
		!slices.Equal(compiled.Args[:len(wantPrefix)], wantPrefix) ||
		compiled.Args[len(wantPrefix)] != "tenant-1" {
		t.Fatalf("streamstats count(field) argument prefix = %#v, want %#v then tenant", compiled.Args, wantPrefix)
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsCountFieldPublishesCanonicalDefaultName(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | streamstats COUNT(Payload.Items)`,
	)
	if !slices.Contains(compiled.OutputFields, "count(Payload.Items)") {
		t.Fatalf(
			"streamstats count(field) output fields = %#v, want canonical default name",
			compiled.OutputFields,
		)
	}
	if !strings.Contains(compiled.SQL, `AS "count(Payload.Items)"`) {
		t.Fatalf("streamstats count(field) did not quote its default output:\n%s", compiled.SQL)
	}
	if len(compiled.Args) < 2 ||
		!slices.Equal(compiled.Args[:2], []any{"Payload.Items", "Payload.Items."}) {
		t.Fatalf("streamstats count(field) input spelling args = %#v", compiled.Args)
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsCountFieldPinsRowWindowsAndZeroPriorFrame(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, options, frame string
	}{
		{
			name:  "unbounded current",
			frame: `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`,
		},
		{
			name:    "unbounded prior",
			options: `current=false`,
			frame:   `ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING`,
		},
		{
			name:    "two current rows",
			options: `window=2`,
			frame:   `ROWS BETWEEN 1 PRECEDING AND CURRENT ROW`,
		},
		{
			name:    "two prior rows",
			options: `current=false window=2`,
			frame:   `ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING`,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			source := `index=gradethis | streamstats ` + test.options +
				` count(status) AS populated | table event_id populated`
			compiled := compileSPL(t, source)
			measure := streamStatsCountFieldPrivateAlias(t, compiled.SQL)
			window := `sum(toUInt128(` + measure + `)) OVER (`
			if strings.Count(compiled.SQL, window) != 1 ||
				strings.Count(compiled.SQL, test.frame) != 1 ||
				!strings.Contains(compiled.SQL, `ifNull(`+window) {
				t.Fatalf("streamstats count(field) frame is not exact:\n%s", compiled.SQL)
			}
			assertBoundedStreamStatsSQL(t, compiled)
		})
	}
}

func TestCompileStreamStatsCountFieldTreatsProjectedInputAsMissing(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | fields event_id | streamstats count(status) AS populated | table event_id populated`,
	)
	measure := streamStatsCountFieldPrivateAlias(t, compiled.SQL)
	if !strings.Contains(compiled.SQL, `toUInt64(0) AS `+measure) {
		t.Fatalf("projected streamstats count(field) input was not fixed at zero:\n%s", compiled.SQL)
	}
	if slices.Contains(compiled.Args, any("status")) ||
		slices.Contains(compiled.Args, any("status.")) {
		t.Fatalf("projected streamstats count(field) rebound hidden input: %#v", compiled.Args)
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsCountFieldUsesFixedMultivalueCardinality(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats values(host) AS hosts | streamstats count(hosts) AS populated | table hosts populated`,
	)
	measure := streamStatsCountFieldPrivateAlias(t, compiled.SQL)
	if !strings.Contains(compiled.SQL, `toUInt64(length("hosts")) AS `+measure) {
		t.Fatalf("fixed multivalue streamstats count(field) did not use cardinality:\n%s", compiled.SQL)
	}
	// The upstream stats values(host) deliberately owns one bounded
	// groupUniqArrayArray. The streamstats stage must consume that fixed array
	// directly without introducing another collection or expanding rows.
	if got := strings.Count(compiled.SQL, "groupUniqArray"); got != 1 {
		t.Fatalf("fixed multivalue pipeline has %d distinct collectors, want the one upstream stats collector:\n%s", got, compiled.SQL)
	}
	for _, forbidden := range []string{"ARRAY JOIN", "arrayJoin(", "groupArray("} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("fixed multivalue streamstats SQL contains row expansion or a new collection %q:\n%s", forbidden, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("fixed multivalue streamstats physical event scans = %d, want one:\n%s", got, compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("fixed multivalue streamstats placeholders = %d, args = %d:\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}

func TestCompileStreamStatsCountFieldResolvesInputBeforeAliasReplacement(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | sort 0 +status | streamstats count(status) AS status | table event_id status`,
	)
	measure := streamStatsCountFieldPrivateAlias(t, compiled.SQL)
	if !strings.Contains(compiled.SQL, ` AS `+measure) ||
		!strings.Contains(compiled.SQL, ` AS "status"`) {
		t.Fatalf("streamstats count(field) alias replacement lost the upstream input:\n%s", compiled.SQL)
	}
	if len(compiled.Args) < 2 ||
		!slices.Equal(compiled.Args[:2], []any{"status", "status."}) {
		t.Fatalf("streamstats alias replacement args = %#v", compiled.Args)
	}
	orderCapture := regexp.MustCompile(`AS "__os_streamstats_order_[0-9]+_0"`)
	if !orderCapture.MatchString(compiled.SQL) {
		t.Fatalf("streamstats field alias replacement lost its incoming order snapshot:\n%s", compiled.SQL)
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsCountFieldKeepsMeasureIndependentFromGroupPresence(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | streamstats window=2 global=false count(status) AS populated BY user | table event_id populated`,
	)
	measure := streamStatsCountFieldPrivateAlias(t, compiled.SQL)
	for _, required := range []string{
		`PARTITION BY "__os_streamstats_eligible_`,
		`sum(toUInt128(` + measure + `)) OVER (`,
		`CAST(NULL AS Nullable(UInt64))`,
		`"__os_streamstats_exists_`,
		UnsupportedStatsByValueMarker,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("grouped streamstats count(field) SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	wantPrefix := []any{"user", "user.", "status", "status."}
	if len(compiled.Args) <= len(wantPrefix) ||
		!slices.Equal(compiled.Args[:len(wantPrefix)], wantPrefix) ||
		compiled.Args[len(wantPrefix)] != "tenant-1" {
		t.Fatalf("group/measure argument prefix = %#v, want %#v then tenant", compiled.Args, wantPrefix)
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsCountFieldProtectsOpenFieldsPayload(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis | streamstats count AS populated`)
	operator := logical.Operators[len(logical.Operators)-1].(*plan.StreamAggregate)
	operator.Measure.Function = plan.AggregateFunctionCountValues
	operator.Measure.Input = mustResolveStreamStatsField(t, "fields")
	_, err := (Compiler{}).Compile(logical)
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) ||
		diagnostic.Code != "SPL_AMBIGUOUS_STREAMSTATS_FIELD" {
		t.Fatalf("open fields streamstats input error = %#v", err)
	}

	closed := compileSPL(
		t,
		`index=gradethis | stats count AS fields | streamstats count(fields) AS populated | table fields populated`,
	)
	if !slices.Equal(closed.OutputFields, []string{"fields", "populated"}) {
		t.Fatalf("closed fields streamstats output = %#v", closed.OutputFields)
	}
}

func TestCompileStreamStatsCountFieldRejectsForgedMetadata(t *testing.T) {
	t.Parallel()

	validInput := mustResolveStreamStatsField(t, "status")
	valid := func() *plan.StreamAggregate {
		return &plan.StreamAggregate{
			Measure: plan.AggregateMeasure{
				Function: plan.AggregateFunctionCountValues,
				Input:    validInput,
				Output:   "populated",
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
		{"quoted input", func(operator *plan.StreamAggregate) {
			operator.Measure.Input = mustResolveStreamStatsField(t, "'status'")
		}},
		{"comma input", func(operator *plan.StreamAggregate) {
			operator.Measure.Input = mustResolveStreamStatsField(t, "status,host")
		}},
		{"whitespace input", func(operator *plan.StreamAggregate) {
			operator.Measure.Input = mustResolveStreamStatsField(t, "status host")
		}},
		{"parenthesized input", func(operator *plan.StreamAggregate) {
			operator.Measure.Input = mustResolveStreamStatsField(t, "status(host)")
		}},
		{"mismatched default output", func(operator *plan.StreamAggregate) {
			operator.Measure.Output = "count(other)"
		}},
		{"whitespace output", func(operator *plan.StreamAggregate) {
			operator.Measure.Output = "populated value"
		}},
		{"comma group", func(operator *plan.StreamAggregate) {
			operator.GroupBy = []plan.FieldRef{
				mustResolveStreamStatsField(t, "host,status"),
			}
		}},
		{"predicate", func(operator *plan.StreamAggregate) { operator.Measure.Predicate = &plan.BooleanExpression{} }},
		{"percentile", func(operator *plan.StreamAggregate) { operator.Measure.Percentile = 50 }},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			operator := valid()
			test.mutate(operator)
			if _, err := (Compiler{}).Compile(appendStreamStatsOperator(
				buildPlan(t, `index=gradethis`),
				operator,
			)); err == nil {
				t.Fatal("Compile accepted forged streamstats count(field) metadata")
			}
		})
	}
}

func streamStatsCountFieldPrivateAlias(t *testing.T, sql string) string {
	t.Helper()
	alias := regexp.MustCompile(`"__os_streamstats_measure_[0-9]+"`).FindString(sql)
	if alias == "" {
		t.Fatalf("streamstats count(field) SQL has no private measure alias:\n%s", sql)
	}
	return alias
}
