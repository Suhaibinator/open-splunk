package clickhouse

import (
	"context"
	"errors"
	"math"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

// TestExpressionV02AgainstClickHouse is the release's focused production-
// shaped arithmetic/membership gate. It is opt-in because it starts the
// repository's digest-pinned ClickHouse image and applies real migrations.
func TestExpressionV02AgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}
	image, err := testsupport.ResolvePinnedClickHouseImage(
		os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"),
	)
	if err != nil {
		t.Fatalf("resolve pinned ClickHouse image: %v", err)
	}
	t.Logf("ClickHouse image: %s", image)

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	connection, store := chartEdgeStartClickHouse(t, ctx)
	indexTime := time.Date(2026, time.August, 11, 12, 0, 0, 0, time.UTC)

	arithmeticEvent := expressionV02StoredEvent(
		"arithmetic",
		"expression-v02",
		indexTime,
		typedField("dyn_int", typedSint(5)),
		typedField("dyn_float", typedDouble(2.5)),
		typedField("dyn_string", typedString("12.5")),
		typedField("dyn_decimal", typedDecimal("3.25")),
		typedField("dyn_null", typedNull()),
		typedField("dyn_bool", typedBool(true)),
		typedField("status", typedString("unused")),
	)
	membershipMatch := expressionV02StoredEvent(
		"membership-match",
		"expression-v02",
		indexTime,
		typedField("status", typedString("A")),
		typedField("numeric_status", typedSint(2)),
	)
	membershipCaseMismatch := expressionV02StoredEvent(
		"membership-case-mismatch",
		"expression-v02",
		indexTime,
		typedField("status", typedString("a")),
		typedField("numeric_status", typedString("2")),
	)
	membershipNull := expressionV02StoredEvent(
		"membership-null",
		"expression-v02",
		indexTime,
		typedField("status", typedNull()),
		typedField("numeric_status", typedNull()),
	)

	compile, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx,
		t,
		store,
		indexTime,
		"expression-v02",
		"expression-v02-batch",
		241,
		arithmeticEvent,
		membershipMatch,
		membershipCaseMismatch,
		membershipNull,
	)

	t.Run("fixed and Dynamic arithmetic edge matrix", func(t *testing.T) {
		compiled := compile(
			`index=expression-v02 event_id="arithmetic"` +
				` | eval literal_result=2+3*4, int_result=dyn_int+1, float_result=dyn_float*2, string_result=dyn_string/2, decimal_result=dyn_decimal+0.75, null_result=dyn_null+1, bool_result=dyn_bool+1, zero_divisor=1/0, negative_zero=0.0/-2.0, negative_remainder=-5%2, positive_negative_remainder=5%-2, overflow_result=1e308*1e308, nan_result=(1e308*1e308)-(1e308*1e308)` +
				` | table literal_result,int_result,float_result,string_result,decimal_result,null_result,bool_result,zero_divisor,negative_zero,negative_remainder,positive_negative_remainder,overflow_result,nan_result`,
		)
		if !compiled.RequiresAtomicResult() {
			t.Fatal("arithmetic query did not retain atomic-result evidence")
		}
		if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
			t.Fatalf("arithmetic storage scans = %d, want 1:\n%s", got, compiled.SQL)
		}
		if strings.Contains(strings.ToUpper(compiled.SQL), " ARRAY JOIN ") {
			t.Fatalf("arithmetic generated row expansion:\n%s", compiled.SQL)
		}
		actions := explainCompiledQuery(t, queryContext, connection, "EXPLAIN actions=1 ", compiled)
		if strings.Contains(actions, "ArrayJoin") {
			t.Fatalf("arithmetic physical actions expand rows:\n%s", actions)
		}

		var literalResult, intResult, floatResult, stringResult, decimalResult float64
		var nullResult, boolResult, zeroDivisor *float64
		var negativeZero, negativeRemainder, positiveNegativeRemainder float64
		var overflowResult, nanResult float64
		if queryErr := connection.QueryRow(
			queryContext,
			compiled.SQL,
			compiled.Args...,
		).Scan(
			&literalResult,
			&intResult,
			&floatResult,
			&stringResult,
			&decimalResult,
			&nullResult,
			&boolResult,
			&zeroDivisor,
			&negativeZero,
			&negativeRemainder,
			&positiveNegativeRemainder,
			&overflowResult,
			&nanResult,
		); queryErr != nil {
			t.Fatalf("execute arithmetic edge matrix: %v\nSQL: %s\nargs: %#v",
				queryErr, compiled.SQL, compiled.Args)
		}
		if literalResult != 14 || intResult != 6 || floatResult != 5 ||
			stringResult != 6.25 || decimalResult != 4 {
			t.Fatalf(
				"finite arithmetic = literal:%v int:%v float:%v string:%v decimal:%v",
				literalResult,
				intResult,
				floatResult,
				stringResult,
				decimalResult,
			)
		}
		if nullResult != nil || boolResult != nil || zeroDivisor != nil {
			t.Fatalf("null arithmetic = null:%v bool:%v zero:%v", nullResult, boolResult, zeroDivisor)
		}
		if negativeZero != 0 || !math.Signbit(negativeZero) ||
			negativeRemainder != -1 || positiveNegativeRemainder != 1 {
			t.Fatalf(
				"signed arithmetic = negative-zero:%v/sign:%v remainders:%v,%v",
				negativeZero,
				math.Signbit(negativeZero),
				negativeRemainder,
				positiveNegativeRemainder,
			)
		}
		if !math.IsInf(overflowResult, 1) || !math.IsNaN(nanResult) {
			t.Fatalf("non-finite arithmetic = overflow:%v nan:%v", overflowResult, nanResult)
		}
	})

	t.Run("fixed String arithmetic is rejected before execution", func(t *testing.T) {
		parsed, parseErr := spl.Parse(
			`index=expression-v02 | eval fixed_string="12" | eval bad=fixed_string+1`,
		)
		if parseErr != nil {
			t.Fatalf("parse fixed String arithmetic fixture: %v", parseErr)
		}
		logical, buildErr := plan.Build(parsed, expressionV02IntegrationScope(indexTime, 1))
		var rejection error
		if buildErr != nil {
			rejection = buildErr
		} else {
			_, rejection = (Compiler{}).Compile(logical)
		}
		var diagnostic *plan.Diagnostic
		if !errors.As(rejection, &diagnostic) ||
			diagnostic.Code != "SPL_UNSUPPORTED_ARITHMETIC_VALUE_TYPE" {
			t.Fatalf("fixed String arithmetic rejection = %v", rejection)
		}
	})

	t.Run("membership equality and null truth table", func(t *testing.T) {
		queryIDs := func(source string) []string {
			t.Helper()
			compiled := compile(source + ` | sort event_id | table event_id`)
			if !compiled.RequiresAtomicResult() {
				t.Fatal("membership query did not retain atomic-result evidence")
			}
			if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
				t.Fatalf("membership storage scans = %d, want 1:\n%s", got, compiled.SQL)
			}
			if strings.Contains(strings.ToUpper(compiled.SQL), " ARRAY JOIN ") {
				t.Fatalf("membership generated row expansion:\n%s", compiled.SQL)
			}
			actions := explainCompiledQuery(
				t,
				queryContext,
				connection,
				"EXPLAIN actions=1 ",
				compiled,
			)
			if strings.Contains(actions, "ArrayJoin") {
				t.Fatalf("membership physical actions expand rows:\n%s", actions)
			}
			rows, queryErr := connection.Query(
				queryContext,
				compiled.SQL,
				compiled.Args...,
			)
			if queryErr != nil {
				t.Fatalf("execute membership query %q: %v", source, queryErr)
			}
			defer func() { _ = rows.Close() }()
			var eventIDs []string
			for rows.Next() {
				var eventID string
				if scanErr := rows.Scan(&eventID); scanErr != nil {
					t.Fatalf("scan membership row: %v", scanErr)
				}
				eventIDs = append(eventIDs, eventID)
			}
			if rowsErr := rows.Err(); rowsErr != nil {
				t.Fatalf("iterate membership rows: %v", rowsErr)
			}
			return eventIDs
		}

		if got := queryIDs(
			`index=expression-v02 | where status IN ("A", null)`,
		); !reflect.DeepEqual(got, []string{"membership-match"}) {
			t.Fatalf("match-before-null membership = %v", got)
		}
		if got := queryIDs(
			`index=expression-v02 | where status NOT IN ("A", null)`,
		); len(got) != 0 {
			t.Fatalf("NOT IN null membership = %v, want none", got)
		}
		if got := queryIDs(
			`index=expression-v02 | where status NOT IN ("A", "B")`,
		); !reflect.DeepEqual(got, []string{"arithmetic", "membership-case-mismatch"}) {
			t.Fatalf("case-sensitive NOT IN membership = %v", got)
		}
		if got := queryIDs(
			`index=expression-v02 | where numeric_status IN (2)`,
		); !reflect.DeepEqual(got, []string{"membership-match"}) {
			t.Fatalf("numeric membership = %v", got)
		}
		if got := queryIDs(
			`index=expression-v02 | eval nan_value=round((1e308*1e308)-(1e308*1e308)) | where nan_value=nan_value`,
		); len(got) != 0 {
			t.Fatalf("rounded arithmetic NaN equality matched events = %v", got)
		}
	})

	t.Run("membership predicate consumers preserve Dynamic null and NOT semantics", func(t *testing.T) {
		compileConsumer := func(t *testing.T, source string) CompiledQuery {
			t.Helper()
			compiled := compile(source)
			if !compiled.RequiresAtomicResult() {
				t.Fatal("membership consumer did not retain atomic-result evidence")
			}
			if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
				t.Fatalf("membership consumer storage scans = %d, want 1:\n%s", got, compiled.SQL)
			}
			if strings.Contains(strings.ToUpper(compiled.SQL), " ARRAY JOIN ") ||
				strings.Contains(compiled.SQL, "status IN (") {
				t.Fatalf("membership consumer used backend IN or row expansion:\n%s", compiled.SQL)
			}
			actions := explainCompiledQuery(
				t,
				queryContext,
				connection,
				"EXPLAIN actions=1 ",
				compiled,
			)
			if strings.Contains(actions, "ArrayJoin") {
				t.Fatalf("membership consumer physical actions expand rows:\n%s", actions)
			}
			return compiled
		}

		t.Run("if and case", func(t *testing.T) {
			compiled := compileConsumer(t,
				`index=expression-v02`+
					` | eval if_class=if(status IN ("A", null), "match", "other"),`+
					`case_class=case(status NOT IN ("A", "B"), "not", status IN ("A", null), "match")`+
					` | stats count(eval(if_class="match")) AS if_match`+
					` count(eval(if_class="other")) AS if_other`+
					` count(eval(case_class="match")) AS case_match`+
					` count(eval(case_class="not")) AS case_not`+
					` count(eval(isnull(case_class))) AS case_null`,
			)
			var ifMatch, ifOther, caseMatch, caseNot, caseNull uint64
			if err := connection.QueryRow(
				queryContext,
				compiled.SQL,
				compiled.Args...,
			).Scan(&ifMatch, &ifOther, &caseMatch, &caseNot, &caseNull); err != nil {
				t.Fatalf("execute if/case membership: %v", err)
			}
			if ifMatch != 1 || ifOther != 3 || caseMatch != 1 || caseNot != 2 || caseNull != 1 {
				t.Fatalf(
					"if/case membership = if %d/%d case %d/%d/%d, want 1/3 and 1/2/1",
					ifMatch,
					ifOther,
					caseMatch,
					caseNot,
					caseNull,
				)
			}
		})

		t.Run("stats count eval", func(t *testing.T) {
			compiled := compileConsumer(t,
				`index=expression-v02`+
					` | stats count(eval(status IN ("A", null))) AS matches`+
					` count(eval(status NOT IN ("A", null))) AS misses`+
					` count(eval(numeric_status IN (2))) AS numeric_matches`,
			)
			var matches, misses, numericMatches uint64
			if err := connection.QueryRow(
				queryContext,
				compiled.SQL,
				compiled.Args...,
			).Scan(&matches, &misses, &numericMatches); err != nil {
				t.Fatalf("execute stats membership: %v", err)
			}
			if matches != 1 || misses != 0 || numericMatches != 1 {
				t.Fatalf("stats membership = %d/%d/%d, want 1/0/1", matches, misses, numericMatches)
			}
		})

		t.Run("eventstats count eval", func(t *testing.T) {
			compiled := compileConsumer(t,
				`index=expression-v02`+
					` | eventstats count(eval(status IN ("A", null))) AS matches`+
					` | sort event_id | table event_id,matches`,
			)
			rows, err := connection.Query(queryContext, compiled.SQL, compiled.Args...)
			if err != nil {
				t.Fatalf("execute eventstats membership: %v", err)
			}
			defer func() { _ = rows.Close() }()
			var got []string
			for rows.Next() {
				var eventID string
				var matches uint64
				if err := rows.Scan(&eventID, &matches); err != nil {
					t.Fatalf("scan eventstats membership: %v", err)
				}
				if matches != 1 {
					t.Fatalf("eventstats membership for %q = %d, want 1", eventID, matches)
				}
				got = append(got, eventID)
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("iterate eventstats membership: %v", err)
			}
			if !reflect.DeepEqual(got, []string{
				"arithmetic",
				"membership-case-mismatch",
				"membership-match",
				"membership-null",
			}) {
				t.Fatalf("eventstats membership rows = %v", got)
			}
		})

		t.Run("streamstats count eval", func(t *testing.T) {
			compiled := compileConsumer(t,
				`index=expression-v02 | sort event_id`+
					` | streamstats count(eval(status IN ("A", null))) AS matches`+
					` | table event_id,matches`,
			)
			rows, err := connection.Query(queryContext, compiled.SQL, compiled.Args...)
			if err != nil {
				t.Fatalf("execute streamstats membership: %v", err)
			}
			defer func() { _ = rows.Close() }()
			var gotMatches []uint64
			for rows.Next() {
				var eventID string
				var matches uint64
				if err := rows.Scan(&eventID, &matches); err != nil {
					t.Fatalf("scan streamstats membership: %v", err)
				}
				gotMatches = append(gotMatches, matches)
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("iterate streamstats membership: %v", err)
			}
			if !reflect.DeepEqual(gotMatches, []uint64{0, 0, 1, 1}) {
				t.Fatalf("streamstats membership counts = %v, want [0 0 1 1]", gotMatches)
			}
		})
	})

	t.Run("maximum authored shapes execute with one scan", func(t *testing.T) {
		arithmeticSources := []struct {
			name        string
			source      string
			wantPresent uint64
		}{
			{
				name: "fixed",
				source: strings.Replace(
					expressionV02ArithmeticBenchmarkSource("severity", 256),
					"index=gradethis", "index=expression-v02", 1,
				),
				wantPresent: 4,
			},
			{
				name: "Dynamic",
				source: strings.Replace(
					expressionV02ArithmeticBenchmarkSource("dyn_int", 256),
					"index=gradethis", "index=expression-v02", 1,
				),
				wantPresent: 1,
			},
		}
		for _, fixture := range arithmeticSources {
			fixture := fixture
			t.Run(fixture.name+" arithmetic", func(t *testing.T) {
				compiled := compile(fixture.source)
				if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
					t.Fatalf("maximum arithmetic scans = %d, want 1", got)
				}
				actions := explainCompiledQuery(t, queryContext, connection, "EXPLAIN actions=1 ", compiled)
				if strings.Contains(actions, "ArrayJoin") {
					t.Fatalf("maximum arithmetic physical actions expand rows:\n%s", actions)
				}
				var rows, present uint64
				var sum *float64
				query := `SELECT count(), countIf(isNotNull("result")), sumOrNull("result") FROM (` + compiled.SQL + `)`
				if err := connection.QueryRow(queryContext, query, compiled.Args...).Scan(&rows, &present, &sum); err != nil {
					t.Fatalf("execute maximum arithmetic: %v", err)
				}
				if rows != 4 || present != fixture.wantPresent || sum == nil {
					t.Fatalf("maximum arithmetic result = rows %d present %d sum %v", rows, present, sum)
				}
			})
		}

		membershipSource := strings.ReplaceAll(
			strings.Replace(
				expressionV02MembershipBenchmarkSource(32),
				"index=gradethis", "index=expression-v02", 1,
			),
			"status", "dyn_int",
		)
		compiled := compile(membershipSource)
		if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
			t.Fatalf("maximum membership scans = %d, want 1", got)
		}
		actions := explainCompiledQuery(t, queryContext, connection, "EXPLAIN actions=1 ", compiled)
		if strings.Contains(actions, "ArrayJoin") {
			t.Fatalf("maximum membership physical actions expand rows:\n%s", actions)
		}
		var matching uint64
		if err := connection.QueryRow(
			queryContext,
			`SELECT count() FROM (`+compiled.SQL+`)`,
			compiled.Args...,
		).Scan(&matching); err != nil {
			t.Fatalf("execute maximum membership: %v", err)
		}
		if matching != 1 {
			t.Fatalf("maximum membership matches = %d, want 1", matching)
		}
	})

	t.Run("folded arithmetic preserves every operator and exceptional result", func(t *testing.T) {
		// More than maximumDirectArithmeticOperators forces the bounded RPN
		// lowering. Keep the whole query below the public 256-operator budget
		// while exercising every RPN token, nullable propagation, and division's
		// intentional negative-zero exception against the real backend.
		noops65 := strings.Repeat("+0", 65)
		noops64 := strings.Repeat("+0", 64)
		source := `index=expression-v02 event_id="arithmetic" | eval ` +
			`mixed=10.0` + noops65 + `+(1*2)-(8/4)+(7%4)+-1,` +
			`negative_zero_fold=(0.0` + noops64 + `)/-2.0,` +
			`null_fold=1.0` + noops64 + `/0 | ` +
			`table mixed,negative_zero_fold,null_fold`
		compiled := compile(source)
		if !strings.Contains(compiled.SQL, "arrayFold(") {
			t.Fatalf("maximum arithmetic did not select bounded fold lowering:\n%s", compiled.SQL)
		}
		if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
			t.Fatalf("folded arithmetic scans = %d, want 1", got)
		}
		var mixed, negativeZero float64
		var nullResult *float64
		if err := connection.QueryRow(
			queryContext,
			compiled.SQL,
			compiled.Args...,
		).Scan(&mixed, &negativeZero, &nullResult); err != nil {
			t.Fatalf("execute folded arithmetic: %v", err)
		}
		if mixed != 12 {
			t.Fatalf("folded mixed arithmetic = %v, want 12", mixed)
		}
		if negativeZero != 0 || !math.Signbit(negativeZero) {
			t.Fatalf("folded division zero = %v/sign:%v, want negative zero", negativeZero, math.Signbit(negativeZero))
		}
		if nullResult != nil {
			t.Fatalf("folded zero divisor = %v, want null", nullResult)
		}
	})

	t.Run("malformed semantic tag fails without payload disclosure", func(t *testing.T) {
		insertMalformedDecimalIntegrationFixture(
			ctx,
			t,
			store,
			connection,
			arithmeticEvent.Event.EventTime.AsTime(),
			"expression-v02",
			"malformed-expression",
			"expression-v02-malformed",
		)
		visibilityCutoff, cutoffErr := store.VisibilityCutoff(ctx)
		if cutoffErr != nil {
			t.Fatalf("capture malformed expression visibility: %v", cutoffErr)
		}
		compiled := compileIntegrationSPLForIndex(
			t,
			`index=expression-v02 | eval result=malformed+1 | sort event_id | table event_id,result`,
			indexTime.Add(10*time.Second),
			visibilityCutoff,
			"expression-v02",
		)
		rows, queryErr := connection.Query(queryContext, compiled.SQL, compiled.Args...)
		if queryErr == nil {
			defer func() { _ = rows.Close() }()
			for rows.Next() {
				var eventID string
				var result *float64
				if scanErr := rows.Scan(&eventID, &result); scanErr != nil {
					queryErr = scanErr
					break
				}
			}
			if queryErr == nil {
				queryErr = rows.Err()
			}
		}
		if queryErr == nil ||
			!strings.Contains(queryErr.Error(), UnsupportedExpressionValueMarker) {
			t.Fatalf("malformed semantic value error = %v", queryErr)
		}
		if strings.Contains(queryErr.Error(), "malformed-secret-1e") {
			t.Fatalf("malformed semantic value disclosed payload: %v", queryErr)
		}
	})
}

func expressionV02StoredEvent(
	eventID string,
	index string,
	indexTime time.Time,
	fields ...*opensplunkv1.TypedObjectField,
) *ingest.StoredEvent {
	event := testStoredEvent(eventID, index, indexTime)
	event.Event.Source = "expression-v02"
	event.Event.Fields = typedObjectValue(fields...)
	return event
}

func expressionV02IntegrationScope(indexTime time.Time, visibility uint64) plan.Scope {
	return plan.Scope{
		TenantID:          "tenant",
		AuthorizedIndexes: []string{"expression-v02"},
		Earliest:          indexTime.Add(-time.Hour),
		Latest:            indexTime.Add(time.Hour),
		SearchStart:       indexTime.Add(time.Second),
		SearchTimezone:    "UTC",
		IndexTimeCutoff:   indexTime.Add(time.Second),
		VisibilityCutoff:  uint64PointerForIntegration(visibility),
	}
}
