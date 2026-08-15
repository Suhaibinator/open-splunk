package clickhouse

import (
	"errors"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileStreamStatsCountEvalUsesOneTrueOnlyUInt128Window(t *testing.T) {
	t.Parallel()

	operator := streamStatsCountEvalOperator(
		streamStatsCountEvalStringComparison(t),
		"matches",
	)
	compiled, err := (Compiler{}).Compile(appendStreamStatsOperator(
		buildPlan(t, `index=gradethis | table event_id,source | sort 0 +event_id`),
		operator,
	))
	if err != nil {
		t.Fatalf("Compile(streamstats count(eval)): %v", err)
	}

	measure := streamStatsCountFieldPrivateAlias(t, compiled.SQL)
	window := `toUInt64(ifNull(sum(toUInt128(` + measure + `)) OVER (`
	for _, required := range []string{
		`toUInt64(ifNull(`,
		` AS ` + measure,
		window,
		`ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`,
		`count() OVER () AS "__os_streamstats_input_count_`,
		`AS "matches"`,
		StreamStatsInputLimitMarker,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("conditional streamstats SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, window); got != 1 {
		t.Fatalf("conditional streamstats additive windows = %d, want one:\n%s", got, compiled.SQL)
	}
	if strings.Contains(compiled.SQL, ` WHERE lowerUTF8(toString("source"))`) {
		t.Fatalf("conditional streamstats predicate became a row filter:\n%s", compiled.SQL)
	}
	if len(compiled.Args) < 2 || compiled.Args[0] != "api" || compiled.Args[1] != "tenant-1" {
		t.Fatalf("conditional streamstats argument prefix = %#v, want api then tenant", compiled.Args)
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsCountEvalPinsFramesAndGroupedPresence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		includeCurrent bool
		windowRows     uint64
		grouped        bool
		frame          string
	}{
		{
			name:           "complete current prefix",
			includeCurrent: true,
			frame:          `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`,
		},
		{
			name:  "complete prior prefix",
			frame: `ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING`,
		},
		{
			name:           "bounded current frame",
			includeCurrent: true,
			windowRows:     3,
			frame:          `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW`,
		},
		{
			name:       "bounded grouped prior frame",
			windowRows: 3,
			grouped:    true,
			frame:      `ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING`,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			operator := streamStatsCountEvalOperator(
				streamStatsCountEvalStringComparison(t),
				"matches",
			)
			operator.IncludeCurrent = test.includeCurrent
			operator.WindowRows = test.windowRows
			if test.grouped {
				operator.GroupBy = []plan.FieldRef{
					mustResolveStreamStatsField(t, "user"),
				}
				operator.Global = false
			}
			compiled, err := (Compiler{}).Compile(appendStreamStatsOperator(
				buildPlan(t, `index=gradethis | table event_id,source,user | sort 0 +event_id`),
				operator,
			))
			if err != nil {
				t.Fatalf("Compile(streamstats count(eval) frame): %v", err)
			}
			measure := streamStatsCountFieldPrivateAlias(t, compiled.SQL)
			window := `sum(toUInt128(` + measure + `)) OVER (`
			if strings.Count(compiled.SQL, window) != 1 ||
				strings.Count(compiled.SQL, test.frame) != 1 {
				t.Fatalf("conditional streamstats frame is not exact:\n%s", compiled.SQL)
			}
			if test.grouped {
				for _, required := range []string{
					`PARTITION BY "__os_streamstats_eligible_`,
					`CAST(NULL AS Nullable(UInt64))`,
					`"__os_streamstats_exists_`,
					UnsupportedStatsByValueMarker,
				} {
					if !strings.Contains(compiled.SQL, required) {
						t.Fatalf("grouped conditional streamstats SQL missing %q:\n%s", required, compiled.SQL)
					}
				}
				wantPrefix := []any{"api", "user", "user."}
				if len(compiled.Args) <= len(wantPrefix) ||
					!slices.Equal(compiled.Args[:len(wantPrefix)], wantPrefix) ||
					compiled.Args[len(wantPrefix)] != "tenant-1" {
					t.Fatalf("group/predicate argument prefix = %#v, want %#v then tenant", compiled.Args, wantPrefix)
				}
			}
			assertBoundedStreamStatsSQL(t, compiled)
		})
	}
}

func TestCompileStreamStatsCountEvalReadsIncomingSameNameBeforeReplacement(t *testing.T) {
	t.Parallel()

	operator := streamStatsCountEvalOperator(
		streamStatsCountEvalStringComparison(t),
		"source",
	)
	compiled, err := (Compiler{}).Compile(appendStreamStatsOperator(
		buildPlan(t, `index=gradethis | table event_id,source | sort 0 +source`),
		operator,
	))
	if err != nil {
		t.Fatalf("Compile(streamstats count(eval) replacement): %v", err)
	}
	measure := streamStatsCountFieldPrivateAlias(t, compiled.SQL)
	measureDefinition := strings.Index(compiled.SQL, ` AS `+measure)
	publication := strings.LastIndex(compiled.SQL, ` AS "source"`)
	if measureDefinition < 0 || publication <= measureDefinition ||
		!strings.Contains(compiled.SQL[:measureDefinition], `toString("source") = CAST(? AS String)`) {
		t.Fatalf("conditional streamstats did not read the incoming source before replacing it:\n%s", compiled.SQL)
	}
	orderSnapshot := regexp.MustCompile(
		`"source" AS "(__os_order_[0-9]+_0)"`,
	).FindStringSubmatch(compiled.SQL)
	if len(orderSnapshot) != 2 || !regexp.MustCompile(
		`"`+regexp.QuoteMeta(orderSnapshot[1])+`" AS "__os_streamstats_order_[0-9]+_0"`,
	).MatchString(compiled.SQL) {
		t.Fatalf("conditional streamstats did not snapshot the incoming same-name order:\n%s", compiled.SQL)
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsCountEvalMaterializesRepeatedExactNumericKeyOnce(t *testing.T) {
	t.Parallel()

	ratio := mustResolveStreamStatsField(t, "ratio")
	predicate := &plan.BooleanExpression{
		Op: plan.BooleanOpAnd,
		Left: &plan.EvalComparisonExpression{
			Left:  &plan.ScalarFieldExpression{Field: ratio},
			Op:    plan.ComparisonOpGreater,
			Right: streamStatsCountEvalIntegerLiteral(1),
		},
		Right: &plan.EvalComparisonExpression{
			Left:  &plan.ScalarFieldExpression{Field: ratio},
			Op:    plan.ComparisonOpLess,
			Right: streamStatsCountEvalIntegerLiteral(10),
		},
	}
	compiled, err := (Compiler{}).Compile(appendStreamStatsOperator(
		buildPlan(t, `index=gradethis`),
		streamStatsCountEvalOperator(predicate, "matches"),
	))
	if err != nil {
		t.Fatalf("Compile(repeated exact-numeric streamstats predicate): %v", err)
	}
	for _, prefix := range []string{
		` AS "__os_streamstats_exact_key_`,
		` AS "__os_streamstats_exact_numeric_`,
	} {
		if got := strings.Count(compiled.SQL, prefix); got != 1 {
			t.Fatalf("exact-numeric definition %q count = %d, want one:\n%s", prefix, got, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `"__os_streamstats_exact_key_`); got < 3 {
		t.Fatalf("exact-numeric key references = %d, want definition plus both comparisons:\n%s", got, compiled.SQL)
	}
	wantPrefix := []any{"ratio", int64(1), "ratio", int64(10)}
	if len(compiled.Args) <= len(wantPrefix) ||
		!slices.Equal(compiled.Args[:len(wantPrefix)], wantPrefix) ||
		compiled.Args[len(wantPrefix)] != "tenant-1" {
		t.Fatalf("exact-numeric predicate args = %#v, want %#v then tenant", compiled.Args, wantPrefix)
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsCountEvalFencesCalculatedPredicateOnce(t *testing.T) {
	t.Parallel()

	selected := mustResolveStreamStatsField(t, "selected")
	predicate := &plan.BooleanExpression{
		Op: plan.BooleanOpAnd,
		Left: &plan.EvalComparisonExpression{
			Left:  &plan.ScalarFieldExpression{Field: selected},
			Op:    plan.ComparisonOpGreater,
			Right: streamStatsCountEvalIntegerLiteral(1),
		},
		Right: &plan.EvalComparisonExpression{
			Left:  &plan.ScalarFieldExpression{Field: selected},
			Op:    plan.ComparisonOpLess,
			Right: streamStatsCountEvalIntegerLiteral(10),
		},
	}
	logical := buildPlan(
		t,
		`index=gradethis | spath input=_raw output=selected path=value | table event_id,matches`,
	)
	extractIndex := -1
	for index, candidate := range logical.Operators {
		if _, ok := candidate.(*plan.ExtractJSON); ok {
			extractIndex = index
			break
		}
	}
	if extractIndex < 0 {
		t.Fatal("fixture plan has no JSON extraction")
	}
	logical.Operators = insertPlanOperator(
		logical.Operators,
		extractIndex+1,
		streamStatsCountEvalOperator(predicate, "matches"),
	)
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(calculated streamstats predicate): %v", err)
	}
	for _, required := range []string{
		`ARRAY JOIN`,
		`__os_stats_predicate_bound_`,
		` AS "__os_streamstats_exact_key_`,
		` AS "__os_streamstats_exact_numeric_`,
		`LIMIT ` + strconv.FormatUint(MaximumStreamStatsInputRows+1, 10),
		`AS "matches"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("calculated conditional streamstats SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, "ARRAY JOIN"); got != 1 {
		t.Fatalf("calculated predicate singleton fences = %d, want one:\n%s", got, compiled.SQL)
	}
	postLimitFence := regexp.MustCompile(
		`LIMIT ` + strconv.FormatUint(MaximumStreamStatsInputRows+1, 10) +
			`\) AS "__os_streamstats_predicate_binding_source_[^"]+" ARRAY JOIN`,
	)
	if !postLimitFence.MatchString(compiled.SQL) {
		t.Fatalf("calculated predicate fence executes before the bounded sentinel input:\n%s", compiled.SQL)
	}
	if !regexp.MustCompile(
		`ARRAY JOIN \[[^\]]+\] AS "__os_stats_predicate_bound_[^"]+"`,
	).MatchString(compiled.SQL) {
		t.Fatalf("calculated predicate fence is not a singleton array:\n%s", compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, ` AS "__os_streamstats_exact_key_`); got != 1 {
		t.Fatalf("calculated exact-numeric key definitions = %d, want one:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("calculated conditional streamstats scans = %d, want one:\n%s", got, compiled.SQL)
	}
	for _, forbidden := range []string{" LEFT JOIN ", " RIGHT JOIN ", " FULL JOIN ", "groupArray("} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("calculated conditional streamstats contains %q:\n%s", forbidden, compiled.SQL)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("calculated predicate placeholders = %d, args = %d:\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}

func TestCompileStreamStatsCountEvalTreatsNullAndMissingAsZero(t *testing.T) {
	t.Parallel()

	probe := mustResolveStreamStatsField(t, "probe")
	predicate := &plan.ScalarPredicateExpression{
		Value: &plan.ScalarCallExpression{
			Function: plan.ScalarFunctionIsNotNull,
			Arguments: []plan.ScalarExpression{
				&plan.ScalarFieldExpression{Field: probe},
			},
		},
	}
	compiled, err := (Compiler{}).Compile(appendStreamStatsOperator(
		buildPlan(t, `index=gradethis | fields event_id`),
		streamStatsCountEvalOperator(predicate, "matches"),
	))
	if err != nil {
		t.Fatalf("Compile(missing streamstats predicate): %v", err)
	}
	if !strings.Contains(compiled.SQL, `toUInt64(ifNull(`) ||
		!strings.Contains(compiled.SQL, `toUInt128("__os_streamstats_measure_`) {
		t.Fatalf("missing conditional streamstats contribution is not null-safe and additive:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, `"__os_fields"."probe"`) ||
		slices.Contains(compiled.Args, any("probe")) ||
		slices.Contains(compiled.Args, any("probe.")) {
		t.Fatalf("missing conditional streamstats predicate resurrected probe: args=%#v\n%s", compiled.Args, compiled.SQL)
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsCountEvalRejectsForgedMetadataAndComplexity(t *testing.T) {
	t.Parallel()

	validPredicate := func() plan.Expression {
		return streamStatsCountEvalStringComparison(t)
	}
	input := mustResolveStreamStatsField(t, "source")
	var typedNil *plan.EvalComparisonExpression
	cyclic := &plan.NotExpression{}
	cyclic.Operand = cyclic
	tests := []struct {
		name   string
		mutate func(*plan.StreamAggregate)
	}{
		{"missing predicate", func(operator *plan.StreamAggregate) { operator.Measure.Predicate = nil }},
		{"typed nil predicate", func(operator *plan.StreamAggregate) { operator.Measure.Predicate = typedNil }},
		{"input metadata", func(operator *plan.StreamAggregate) { operator.Measure.Input = input }},
		{"empty input path metadata", func(operator *plan.StreamAggregate) { operator.Measure.Input.Path = []string{} }},
		{"percentile metadata", func(operator *plan.StreamAggregate) { operator.Measure.Percentile = 95 }},
		{"base search predicate", func(operator *plan.StreamAggregate) {
			operator.Measure.Predicate = &plan.TextExpression{Value: "unsafe"}
		}},
		{"non Boolean scalar", func(operator *plan.StreamAggregate) {
			operator.Measure.Predicate = &plan.ScalarPredicateExpression{
				Value: &plan.ScalarFieldExpression{Field: input},
			}
		}},
		{"predicate cycle", func(operator *plan.StreamAggregate) { operator.Measure.Predicate = cyclic }},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			operator := streamStatsCountEvalOperator(validPredicate(), "matches")
			test.mutate(operator)
			_, err := (Compiler{}).Compile(appendStreamStatsOperator(
				buildPlan(t, `index=gradethis`),
				operator,
			))
			if err == nil {
				t.Fatal("Compile accepted forged conditional streamstats metadata")
			}
			if test.name == "predicate cycle" {
				var diagnostic *plan.Diagnostic
				if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
					t.Fatalf("predicate cycle error = %#v, want SPL_QUERY_TOO_COMPLEX", err)
				}
			}
		})
	}
}

func TestCompileStreamStatsCountEvalProtectsOpenFieldsPayload(t *testing.T) {
	t.Parallel()

	fields := mustResolveStreamStatsField(t, "fields")
	operator := streamStatsCountEvalOperator(
		&plan.EvalComparisonExpression{
			Left: &plan.ScalarFieldExpression{Field: fields},
			Op:   plan.ComparisonOpEqual,
			Right: &plan.ScalarLiteralExpression{
				Value: plan.Value{Kind: plan.ValueKindString, String: "unsafe"},
			},
		},
		"matches",
	)
	_, err := (Compiler{}).Compile(appendStreamStatsOperator(
		buildPlan(t, `index=gradethis`),
		operator,
	))
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) ||
		diagnostic.Code != "SPL_AMBIGUOUS_STREAMSTATS_FIELD" {
		t.Fatalf("open fields conditional streamstats error = %#v, want SPL_AMBIGUOUS_STREAMSTATS_FIELD", err)
	}
}

func streamStatsCountEvalOperator(
	predicate plan.Expression,
	output string,
) *plan.StreamAggregate {
	return &plan.StreamAggregate{
		Measure: plan.AggregateMeasure{
			Function:  plan.AggregateFunctionCountPredicate,
			Predicate: predicate,
			Output:    output,
		},
		IncludeCurrent: true,
		Global:         true,
	}
}

func streamStatsCountEvalStringComparison(
	t *testing.T,
) plan.Expression {
	t.Helper()
	return &plan.EvalComparisonExpression{
		Left: &plan.ScalarFieldExpression{
			Field: mustResolveStreamStatsField(t, "source"),
		},
		Op: plan.ComparisonOpEqual,
		Right: &plan.ScalarLiteralExpression{
			Value: plan.Value{Kind: plan.ValueKindString, String: "api"},
		},
	}
}

func streamStatsCountEvalIntegerLiteral(value int64) plan.ScalarExpression {
	return &plan.ScalarLiteralExpression{
		Value: plan.Value{
			Kind:       plan.ValueKindInt64,
			Int64:      value,
			SourceText: strconv.FormatInt(value, 10),
		},
	}
}
