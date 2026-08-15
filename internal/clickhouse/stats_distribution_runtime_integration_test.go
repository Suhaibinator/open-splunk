package clickhouse

import (
	"context"
	"math"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

// TestStatsDistributionAggregatesAgainstClickHouse proves the helper syntax,
// result types, and local edge contract against the digest-pinned production
// ClickHouse version. It does not promote OracleRequired lowerings to Splunk
// semantic parity.
func TestStatsDistributionAggregatesAgainstClickHouse(t *testing.T) {
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
	mustLower := func(
		function plan.AggregateFunction,
		percentile uint8,
	) statsDistributionAggregateLowering {
		t.Helper()
		lowering, ok := statsDistributionArrayAggregateSQL(function, percentile, "a")
		if !ok {
			t.Fatalf("lower distribution function %v percentile %d", function, percentile)
		}
		return lowering
	}

	t.Run("exact nearest-rank percentile", func(t *testing.T) {
		exact50 := mustLower(plan.AggregateFunctionExactPercentile, 50)
		exact95 := mustLower(plan.AggregateFunctionExactPercentile, 95)
		source := `values('a Array(Float64)', ([1.,2.]), ([3.,4.,5.,6.,7.,8.,9.,10.]))`
		var p50, p95 *float64
		var p50Type string
		if queryErr := connection.QueryRow(
			ctx,
			"SELECT "+exact50.SQL+", "+exact95.SQL+", toTypeName("+exact50.SQL+") FROM "+source,
		).Scan(&p50, &p95, &p50Type); queryErr != nil {
			t.Fatalf("execute exact percentile helper: %v", queryErr)
		}
		assertDistributionFloat(t, "exactperc50", p50, 5)
		assertDistributionFloat(t, "exactperc95", p95, 10)
		if p50Type != "Nullable(Float64)" {
			t.Fatalf("exact percentile type = %q", p50Type)
		}

		for _, test := range []struct {
			name   string
			source string
			want   *float64
		}{
			{"empty array", `values('a Array(Float64)', (CAST([], 'Array(Float64)')))`, nil},
			{"no rows", `(SELECT CAST([], 'Array(Float64)') AS a FROM numbers(0))`, nil},
			{"singleton", `values('a Array(Float64)', ([7.]))`, new(float64(7))},
		} {
			t.Run(test.name, func(t *testing.T) {
				var got *float64
				if queryErr := connection.QueryRow(
					ctx,
					"SELECT "+exact50.SQL+" FROM "+test.source,
				).Scan(&got); queryErr != nil {
					t.Fatalf("execute exact percentile edge: %v", queryErr)
				}
				assertDistributionNullableFloat(t, "exact percentile", got, test.want)
			})
		}
	})

	t.Run("bounded upper percentile and median", func(t *testing.T) {
		upper50 := mustLower(plan.AggregateFunctionUpperPercentile, 50)
		median := mustLower(plan.AggregateFunctionMedian, 0)
		small := `values('a Array(Float64)', ([1.,2.,3.,4.,5.,6.,7.,8.,9.,10.]))`
		var smallUpper, smallMedian *float64
		var upperType, medianType string
		if queryErr := connection.QueryRow(
			ctx,
			"SELECT "+upper50.SQL+", "+median.SQL+", "+
				"toTypeName("+upper50.SQL+"), toTypeName("+median.SQL+") FROM "+small,
		).Scan(&smallUpper, &smallMedian, &upperType, &medianType); queryErr != nil {
			t.Fatalf("execute upper percentile/median helper: %v", queryErr)
		}
		assertDistributionFloat(t, "small upperperc50", smallUpper, 5)
		assertDistributionFloat(t, "small median", smallMedian, 5)
		if upperType != "Nullable(Float64)" || medianType != "Nullable(Float64)" {
			t.Fatalf("upper/median types = %q/%q", upperType, medianType)
		}

		large := `(SELECT arrayMap(value -> toFloat64(value + 1), range(toUInt64(2001))) AS a)`
		ordinary50 := `arrayElementOrNull(quantilesGKOrNullArray(100, 0.5)(a), 1)`
		var largeUpper, largeOrdinary *float64
		if queryErr := connection.QueryRow(
			ctx,
			"SELECT "+upper50.SQL+", "+ordinary50+" FROM "+large,
		).Scan(&largeUpper, &largeOrdinary); queryErr != nil {
			t.Fatalf("execute large upper percentile helper: %v", queryErr)
		}
		if largeUpper == nil || largeOrdinary == nil || *largeUpper < *largeOrdinary {
			t.Fatalf("large upper/ordinary percentile = %v/%v", largeUpper, largeOrdinary)
		}

		for _, lowering := range []statsDistributionAggregateLowering{upper50, median} {
			var empty, singleton *float64
			if queryErr := connection.QueryRow(
				ctx,
				"SELECT "+lowering.SQL+" FROM values('a Array(Float64)', (CAST([], 'Array(Float64)')))",
			).Scan(&empty); queryErr != nil {
				t.Fatalf("execute empty bounded percentile: %v", queryErr)
			}
			if queryErr := connection.QueryRow(
				ctx,
				"SELECT "+lowering.SQL+" FROM values('a Array(Float64)', ([7.]))",
			).Scan(&singleton); queryErr != nil {
				t.Fatalf("execute singleton bounded percentile: %v", queryErr)
			}
			assertDistributionNullableFloat(t, "empty bounded percentile", empty, nil)
			assertDistributionFloat(t, "singleton bounded percentile", singleton, 7)
		}
	})

	t.Run("bounded estimated distinct count and provisional error", func(t *testing.T) {
		estimate := mustLower(plan.AggregateFunctionEstimatedDistinctCount, 0)
		errorRatio := mustLower(plan.AggregateFunctionEstimatedDistinctCountError, 0)
		small := `values('a Array(String)', (['1','1.0','01','1']))`
		var smallEstimate uint64
		var smallError float64
		var estimateType, errorType string
		if queryErr := connection.QueryRow(
			ctx,
			"SELECT "+estimate.SQL+", "+errorRatio.SQL+", "+
				"toTypeName("+estimate.SQL+"), toTypeName("+errorRatio.SQL+") FROM "+small,
		).Scan(&smallEstimate, &smallError, &estimateType, &errorType); queryErr != nil {
			t.Fatalf("execute small estimated distinct helper: %v", queryErr)
		}
		if smallEstimate != 3 || smallError != 0 ||
			estimateType != "UInt64" || errorType != "Float64" {
			t.Fatalf(
				"small estimated distinct = %d/%v types %q/%q",
				smallEstimate,
				smallError,
				estimateType,
				errorType,
			)
		}

		for _, test := range []struct {
			name string
			data string
			want uint64
		}{
			{"empty", `CAST([], 'Array(String)')`, 0},
			{"singleton", `['only']`, 1},
		} {
			t.Run(test.name, func(t *testing.T) {
				var got uint64
				if queryErr := connection.QueryRow(
					ctx,
					"SELECT "+estimate.SQL+" FROM (SELECT "+test.data+" AS a)",
				).Scan(&got); queryErr != nil {
					t.Fatalf("execute estimated distinct edge: %v", queryErr)
				}
				if got != test.want {
					t.Fatalf("estimated distinct = %d, want %d", got, test.want)
				}
			})
		}

		large := `(SELECT arrayMap(value -> toString(value), range(toUInt64(10000))) AS a)`
		var largeEstimate uint64
		var largeError float64
		if queryErr := connection.QueryRow(
			ctx,
			"SELECT "+estimate.SQL+", "+errorRatio.SQL+" FROM "+large,
		).Scan(&largeEstimate, &largeError); queryErr != nil {
			t.Fatalf("execute large estimated distinct helper: %v", queryErr)
		}
		if largeEstimate < 9500 || largeEstimate > 10500 ||
			math.Abs(largeError-0.002872621298570349) > 1e-18 {
			t.Fatalf("large estimated distinct/error = %d/%v", largeEstimate, largeError)
		}
	})

	t.Run("exact mode and deterministic provisional tie", func(t *testing.T) {
		mode := mustLower(plan.AggregateFunctionMode, 0)
		var tied *string
		var tiedType string
		if queryErr := connection.QueryRow(
			ctx,
			"SELECT "+mode.SQL+", toTypeName("+mode.SQL+") "+
				"FROM values('a Array(String)', (['b','a']), (['b','a','c']))",
		).Scan(&tied, &tiedType); queryErr != nil {
			t.Fatalf("execute tied mode helper: %v", queryErr)
		}
		if tied == nil || *tied != "a" || tiedType != "Nullable(String)" {
			t.Fatalf("tied mode/type = %v/%q", tied, tiedType)
		}

		var lexicalNumeric *string
		if queryErr := connection.QueryRow(
			ctx,
			"SELECT "+mode.SQL+" FROM values('a Array(String)', (['1','1.0','01','1.0','01']))",
		).Scan(&lexicalNumeric); queryErr != nil {
			t.Fatalf("execute lexical numeric mode helper: %v", queryErr)
		}
		if lexicalNumeric == nil || *lexicalNumeric != "01" {
			t.Fatalf("lexical numeric tied mode = %v, want 01", lexicalNumeric)
		}

		for _, test := range []struct {
			name string
			data string
			want *string
		}{
			{"empty", `CAST([], 'Array(String)')`, nil},
			{"singleton", `['only']`, new("only")},
		} {
			t.Run(test.name, func(t *testing.T) {
				var got *string
				if queryErr := connection.QueryRow(
					ctx,
					"SELECT "+mode.SQL+" FROM (SELECT "+test.data+" AS a)",
				).Scan(&got); queryErr != nil {
					t.Fatalf("execute mode edge: %v", queryErr)
				}
				if test.want == nil {
					if got != nil {
						t.Fatalf("mode = %q, want null", *got)
					}
				} else if got == nil || *got != *test.want {
					t.Fatalf("mode = %v, want %q", got, *test.want)
				}
			})
		}
	})

	t.Run("compiled SPL over stored Dynamic fields", func(t *testing.T) {
		labels := []string{"b", "a", "b", "a", "c", "a", "b", "a", "b", "c"}
		fixtureTime := time.Date(2026, time.August, 11, 19, 0, 0, 0, time.UTC)
		events := make([]*ingest.StoredEvent, 0, len(labels)+1)
		for index, label := range labels {
			event := testStoredEvent(
				"distribution-"+label+"-"+string(rune('a'+index)),
				"stats-distribution",
				fixtureTime,
			)
			event.Event.Source = "stats-distribution"
			event.Event.Fields = typedObjectValue(
				typedField("metric", typedSint(int64(index+1))),
				typedField("label", typedString(label)),
			)
			events = append(events, event)
		}
		invalid := testStoredEvent(
			"distribution-invalid",
			"stats-distribution",
			fixtureTime,
		)
		invalid.Event.Source = "stats-distribution"
		invalid.Event.Fields = typedObjectValue(
			typedField("metric", typedString("bad")),
			typedField("label", typedNull()),
		)
		events = append(events, invalid)

		compile, queryContext := storeScalarFunctionIntegrationFixtures(
			ctx,
			t,
			store,
			fixtureTime,
			"stats-distribution",
			"stats-distribution-batch",
			352,
			events...,
		)
		compiled := compile(
			`index=stats-distribution source="stats-distribution" | stats ` +
				`exactperc50(metric) AS exact_value upperperc50(metric) AS upper_value ` +
				`median(metric) AS median_value estdc(label) AS estimated_labels ` +
				`estdc_error(label) AS label_error mode(label) AS common_label`,
		)
		var exactValue, upperValue, medianValue *float64
		var estimatedLabels uint64
		var labelError float64
		var commonLabel *string
		if queryErr := connection.QueryRow(
			queryContext,
			compiled.SQL,
			compiled.Args...,
		).Scan(
			&exactValue,
			&upperValue,
			&medianValue,
			&estimatedLabels,
			&labelError,
			&commonLabel,
		); queryErr != nil {
			t.Fatalf("execute compiled distribution SPL: %v\nSQL: %s\nargs: %#v", queryErr, compiled.SQL, compiled.Args)
		}
		assertDistributionFloat(t, "compiled exactperc50", exactValue, 5)
		assertDistributionFloat(t, "compiled upperperc50", upperValue, 5)
		assertDistributionFloat(t, "compiled median", medianValue, 5)
		if estimatedLabels != 3 || labelError != 0 || commonLabel == nil || *commonLabel != "a" {
			t.Fatalf(
				"compiled string distributions = estdc:%d error:%v mode:%v",
				estimatedLabels,
				labelError,
				commonLabel,
			)
		}

		empty := compile(
			`index=stats-distribution source="absent" | stats ` +
				`exactperc50(metric) AS exact_value upperperc50(metric) AS upper_value ` +
				`median(metric) AS median_value estdc(label) AS estimated_labels ` +
				`estdc_error(label) AS label_error mode(label) AS common_label`,
		)
		exactValue, upperValue, medianValue, commonLabel = nil, nil, nil, nil
		estimatedLabels, labelError = 1, 1
		if queryErr := connection.QueryRow(
			queryContext,
			empty.SQL,
			empty.Args...,
		).Scan(
			&exactValue,
			&upperValue,
			&medianValue,
			&estimatedLabels,
			&labelError,
			&commonLabel,
		); queryErr != nil {
			t.Fatalf("execute empty compiled distribution SPL: %v\nSQL: %s\nargs: %#v", queryErr, empty.SQL, empty.Args)
		}
		if exactValue != nil || upperValue != nil || medianValue != nil ||
			estimatedLabels != 0 || labelError != 0 || commonLabel != nil {
			t.Fatalf(
				"empty compiled distributions = exact:%v upper:%v median:%v estdc:%d error:%v mode:%v",
				exactValue,
				upperValue,
				medianValue,
				estimatedLabels,
				labelError,
				commonLabel,
			)
		}
	})
}

func assertDistributionFloat(t *testing.T, name string, got *float64, want float64) {
	t.Helper()
	assertDistributionNullableFloat(t, name, got, new(want))
}

func assertDistributionNullableFloat(t *testing.T, name string, got, want *float64) {
	t.Helper()
	if want == nil {
		if got != nil {
			t.Fatalf("%s = %v, want null", name, *got)
		}
		return
	}
	if got == nil || math.IsNaN(*got) || math.IsInf(*got, 0) || math.Abs(*got-*want) > 1e-12 {
		t.Fatalf("%s = %v, want %v", name, got, *want)
	}
}
