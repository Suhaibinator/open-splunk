package clickhouse

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

func TestStatsSparklineHelpersAgainstClickHouse(t *testing.T) {
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
	connection, _ := chartEdgeStartClickHouse(t, ctx)

	const partition = `group_key, bucket`
	lowering := func(function plan.AggregateFunction, input string) string {
		t.Helper()
		result, ok := statsSparklineWindowAggregateSQL(function, input, partition)
		if !ok {
			t.Fatalf("lower sparkline function %v", function)
		}
		return result.SQL
	}
	columns := []string{
		lowering(plan.AggregateFunctionCountRows, "") + " AS row_count",
		lowering(plan.AggregateFunctionCountValues, "occurrences") + " AS value_count",
		lowering(plan.AggregateFunctionDistinctCount, "string_values") + " AS distinct_count",
		lowering(plan.AggregateFunctionAverage, "numeric_values") + " AS average",
		lowering(plan.AggregateFunctionStandardDeviationSample, "numeric_values") + " AS stdev",
		lowering(plan.AggregateFunctionStandardDeviationPopulation, "numeric_values") + " AS stdevp",
		lowering(plan.AggregateFunctionVarianceSample, "numeric_values") + " AS variance",
		lowering(plan.AggregateFunctionVariancePopulation, "numeric_values") + " AS variancep",
		lowering(plan.AggregateFunctionSum, "numeric_values") + " AS total",
		lowering(plan.AggregateFunctionSumSquares, "numeric_values") + " AS sumsq",
		lowering(plan.AggregateFunctionMinimum, "string_values") + " AS minimum",
		lowering(plan.AggregateFunctionMaximum, "string_values") + " AS maximum",
		lowering(plan.AggregateFunctionRange, "numeric_values") + " AS value_range",
	}
	source := `SELECT
tupleElement(row, 1) AS group_key,
tupleElement(row, 2) AS bucket,
tupleElement(row, 3) AS numeric_values,
tupleElement(row, 4) AS string_values,
tupleElement(row, 5) AS occurrences
FROM (
SELECT arrayJoin([
tuple('g', toInt64(0), CAST([1.0, 2.0], 'Array(Float64)'), CAST(['a', 'b'], 'Array(String)'), toUInt64(2)),
tuple('g', toInt64(0), CAST([3.0], 'Array(Float64)'), CAST(['b'], 'Array(String)'), toUInt64(1)),
tuple('g', toInt64(1), CAST([], 'Array(Float64)'), CAST([], 'Array(String)'), toUInt64(0))
]) AS row
)`
	query := "SELECT DISTINCT group_key, bucket, " + strings.Join(columns, ", ") +
		" FROM (" + source + ") ORDER BY bucket"
	rows, queryErr := connection.Query(ctx, query)
	if queryErr != nil {
		t.Fatalf("execute sparkline window helpers: %v\nSQL: %s", queryErr, query)
	}
	defer rows.Close()

	type result struct {
		group                  string
		bucket                 int64
		rowCount               uint64
		valueCount             uint64
		distinctCount          uint64
		average, stdev, stdevp *float64
		variance, variancep    *float64
		total, sumsq           *float64
		minimum, maximum       *string
		valueRange             *float64
	}
	var got []result
	for rows.Next() {
		var item result
		if scanErr := rows.Scan(
			&item.group,
			&item.bucket,
			&item.rowCount,
			&item.valueCount,
			&item.distinctCount,
			&item.average,
			&item.stdev,
			&item.stdevp,
			&item.variance,
			&item.variancep,
			&item.total,
			&item.sumsq,
			&item.minimum,
			&item.maximum,
			&item.valueRange,
		); scanErr != nil {
			t.Fatalf("scan sparkline helper row: %v", scanErr)
		}
		got = append(got, item)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		t.Fatalf("iterate sparkline helper rows: %v", rowsErr)
	}
	if len(got) != 2 {
		t.Fatalf("sparkline helper rows = %#v, want two buckets", got)
	}
	first := got[0]
	if first.group != "g" || first.bucket != 0 || first.rowCount != 2 ||
		first.valueCount != 3 || first.distinctCount != 2 {
		t.Fatalf("first bucket identity/counts = %#v", first)
	}
	assertSparklineFloat := func(name string, got *float64, want float64) {
		t.Helper()
		if got == nil || math.Abs(*got-want) > 1e-12 {
			t.Fatalf("%s = %v, want %.15g", name, got, want)
		}
	}
	assertSparklineFloat("average", first.average, 2)
	assertSparklineFloat("stdev", first.stdev, 1)
	assertSparklineFloat("stdevp", first.stdevp, math.Sqrt(2.0/3.0))
	assertSparklineFloat("variance", first.variance, 1)
	assertSparklineFloat("variancep", first.variancep, 2.0/3.0)
	assertSparklineFloat("sum", first.total, 6)
	assertSparklineFloat("sumsq", first.sumsq, 14)
	assertSparklineFloat("range", first.valueRange, 2)
	if first.minimum == nil || *first.minimum != "a" ||
		first.maximum == nil || *first.maximum != "b" {
		t.Fatalf("first bucket extrema = %v/%v, want a/b", first.minimum, first.maximum)
	}
	second := got[1]
	if second.bucket != 1 || second.rowCount != 1 || second.valueCount != 0 ||
		second.distinctCount != 0 || second.average != nil || second.stdev != nil ||
		second.stdevp != nil || second.variance != nil || second.variancep != nil ||
		second.total != nil || second.sumsq != nil || second.minimum != nil ||
		second.maximum != nil || second.valueRange != nil {
		t.Fatalf("empty-value bucket = %#v", second)
	}

	recordsSQL, ok := statsSparklineBucketRecordsSQL(
		"bucket",
		"value",
		spl.MaximumStatsSparklinePoints,
	)
	if !ok {
		t.Fatal("lower bucket records")
	}
	spec := statsSparklineBucketSpec{
		FirstBucket:                    10,
		BucketCount:                    3,
		MaximumPoints:                  100,
		MaximumEncodedElements:         101,
		MarkerAccountingOracleRequired: true,
	}
	publishSQL, ok := statsSparklinePublishSQL(
		"records",
		spec,
		statsSparklineMissingZero,
	)
	if !ok {
		t.Fatal("lower sparkline publication")
	}
	recordSource := `SELECT tupleElement(row, 1) AS bucket, tupleElement(row, 2) AS value
FROM (SELECT arrayJoin([
tuple(toInt64(10), CAST(2 AS Nullable(Float64))),
tuple(toInt64(10), CAST(2 AS Nullable(Float64))),
tuple(toInt64(12), CAST(4 AS Nullable(Float64)))
]) AS row)`
	publishQuery := "SELECT " + publishSQL + " FROM (SELECT " + recordsSQL +
		" AS records FROM (" + recordSource + "))"
	var encoded []string
	if scanErr := connection.QueryRow(ctx, publishQuery).Scan(&encoded); scanErr != nil {
		t.Fatalf("execute sparkline publication: %v\nSQL: %s", scanErr, publishQuery)
	}
	wantEncoded := []string{statsSparklineMarker, "2", "0", "4"}
	if fmt.Sprint(encoded) != fmt.Sprint(wantEncoded) {
		t.Fatalf("encoded sparkline = %#v, want %#v", encoded, wantEncoded)
	}

	// ClickHouse independently caps one repeat() call below our 8 MiB cell
	// budget. Exercise the aggregate publication budget with nine legal 1 MiB
	// bucket values instead of depending on that unrelated function ceiling.
	byteSpec := spec
	byteSpec.BucketCount = 9
	bytePublishSQL, ok := statsSparklinePublishSQL(
		"records",
		byteSpec,
		statsSparklineMissingEmpty,
	)
	if !ok {
		t.Fatal("lower oversized sparkline publication")
	}
	oversizedBytesQuery := "SELECT " + bytePublishSQL + " FROM (SELECT " +
		"arrayMap(ordinal -> tuple(toInt64(10) + toInt64(ordinal), " +
		"repeat('x', toUInt64(1000000))), range(toUInt64(9))) AS records)"
	var rejected []string
	bytesErr := connection.QueryRow(ctx, oversizedBytesQuery).Scan(&rejected)
	if bytesErr == nil || !strings.Contains(
		bytesErr.Error(),
		StatsSparklineBytesLimitMarker,
	) {
		t.Fatalf(
			"oversized sparkline bytes error = %v, want %q",
			bytesErr,
			StatsSparklineBytesLimitMarker,
		)
	}

	tooManyPointsQuery := "SELECT " + publishSQL + " FROM (SELECT " +
		"arrayMap(ordinal -> tuple(toInt64(ordinal), toUInt64(ordinal)), " +
		"range(toUInt64(101))) AS records)"
	pointsErr := connection.QueryRow(ctx, tooManyPointsQuery).Scan(&rejected)
	if pointsErr == nil || !strings.Contains(pointsErr.Error(), StatsSparklineLimitMarker) {
		t.Fatalf(
			"oversized sparkline points error = %v, want %q",
			pointsErr,
			StatsSparklineLimitMarker,
		)
	}

	monthSpec, specErr := statsSparklineBucketSpecFor(
		plan.SparklineSpan{
			Kind:      plan.SparklineSpanKindExplicit,
			Magnitude: 2,
			Unit:      plan.SparklineSpanUnitMonth,
		},
		time.Date(2026, time.March, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, time.May, 1, 0, 0, 0, 0, time.UTC),
		spl.MaximumStatsSparklinePoints,
		`"_time"`,
		"UTC",
	)
	if specErr != nil {
		t.Fatalf("month bucket spec: %v", specErr)
	}
	monthQuery := "SELECT " + monthSpec.BucketSQL +
		" FROM (SELECT toDateTime64('2026-03-17 12:00:00', 9, 'UTC') AS \"_time\")"
	var monthBucket int64
	if scanErr := connection.QueryRow(
		ctx,
		monthQuery,
		monthSpec.BucketArgs...,
	).Scan(&monthBucket); scanErr != nil {
		t.Fatalf("execute month bucket: %v\nSQL: %s", scanErr, monthQuery)
	}
	if monthBucket != monthSpec.FirstBucket {
		t.Fatalf("month bucket = %d, want %d", monthBucket, monthSpec.FirstBucket)
	}

	const civilTimezone = "America/New_York"
	daySpec, specErr := statsSparklineBucketSpecFor(
		plan.SparklineSpan{
			Kind:      plan.SparklineSpanKindExplicit,
			Magnitude: 1,
			Unit:      plan.SparklineSpanUnitDay,
		},
		time.Date(2026, time.March, 8, 5, 0, 0, 0, time.UTC),
		time.Date(2026, time.March, 9, 4, 0, 0, 0, time.UTC),
		spl.MaximumStatsSparklinePoints,
		`"_time"`,
		civilTimezone,
	)
	if specErr != nil {
		t.Fatalf("civil day bucket spec: %v", specErr)
	}
	dayQuery := "SELECT groupArray(bucket) FROM (SELECT " + daySpec.BucketSQL +
		" AS bucket FROM (SELECT arrayJoin([" +
		"toDateTime64('2026-03-08 05:30:00', 9, 'UTC'), " +
		"toDateTime64('2026-03-09 03:30:00', 9, 'UTC')]) AS \"_time\") " +
		"ORDER BY \"_time\")"
	var dayBuckets []int64
	if scanErr := connection.QueryRow(
		ctx,
		dayQuery,
		daySpec.BucketArgs...,
	).Scan(&dayBuckets); scanErr != nil {
		t.Fatalf("execute civil day buckets: %v\nSQL: %s", scanErr, dayQuery)
	}
	if len(dayBuckets) != 2 || dayBuckets[0] != daySpec.FirstBucket ||
		dayBuckets[1] != daySpec.FirstBucket {
		t.Fatalf("civil DST day buckets = %#v, want [%d %d]", dayBuckets, daySpec.FirstBucket, daySpec.FirstBucket)
	}

	civilMonthSpec, specErr := statsSparklineBucketSpecFor(
		plan.SparklineSpan{
			Kind:      plan.SparklineSpanKindExplicit,
			Magnitude: 1,
			Unit:      plan.SparklineSpanUnitMonth,
		},
		time.Date(2026, time.February, 1, 4, 30, 0, 0, time.UTC),
		time.Date(2026, time.February, 1, 6, 30, 0, 0, time.UTC),
		spl.MaximumStatsSparklinePoints,
		`"_time"`,
		civilTimezone,
	)
	if specErr != nil {
		t.Fatalf("civil month bucket spec: %v", specErr)
	}
	monthBoundaryQuery := "SELECT groupArray(bucket) FROM (SELECT " +
		civilMonthSpec.BucketSQL + " AS bucket FROM (SELECT arrayJoin([" +
		"toDateTime64('2026-02-01 04:30:00', 9, 'UTC'), " +
		"toDateTime64('2026-02-01 05:30:00', 9, 'UTC')]) AS \"_time\") " +
		"ORDER BY \"_time\")"
	var monthBuckets []int64
	if scanErr := connection.QueryRow(
		ctx,
		monthBoundaryQuery,
		civilMonthSpec.BucketArgs...,
	).Scan(&monthBuckets); scanErr != nil {
		t.Fatalf("execute civil month buckets: %v\nSQL: %s", scanErr, monthBoundaryQuery)
	}
	if len(monthBuckets) != 2 || monthBuckets[0] != civilMonthSpec.FirstBucket ||
		monthBuckets[1] != civilMonthSpec.FirstBucket+1 {
		t.Fatalf("civil month boundary buckets = %#v, want [%d %d]", monthBuckets, civilMonthSpec.FirstBucket, civilMonthSpec.FirstBucket+1)
	}
}
