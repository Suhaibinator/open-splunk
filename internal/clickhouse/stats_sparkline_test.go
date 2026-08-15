package clickhouse

import (
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestStatsSparklineBucketSpecExplicitFixed(t *testing.T) {
	t.Parallel()

	earliest := time.Date(2026, time.August, 11, 0, 0, 30, 0, time.UTC)
	latest := earliest.Add(10*time.Minute + time.Nanosecond)
	spec, err := statsSparklineBucketSpecFor(
		plan.SparklineSpan{
			Kind:      plan.SparklineSpanKindExplicit,
			Magnitude: 5,
			Unit:      plan.SparklineSpanUnitMinute,
		},
		earliest,
		latest,
		spl.MaximumStatsSparklinePoints,
		`"_time"`,
		"UTC",
	)
	if err != nil {
		t.Fatalf("statsSparklineBucketSpecFor: %v", err)
	}
	wantSQL := `intDiv(reinterpretAsInt64("_time"), ?) - if(reinterpretAsInt64("_time") < 0 AND reinterpretAsInt64("_time") % ? != 0, 1, 0)`
	if spec.BucketSQL != wantSQL {
		t.Fatalf("bucket SQL = %q, want %q", spec.BucketSQL, wantSQL)
	}
	wantArgs := []any{int64(5 * time.Minute), int64(5 * time.Minute)}
	if !reflect.DeepEqual(spec.BucketArgs, wantArgs) {
		t.Fatalf("bucket args = %#v, want %#v", spec.BucketArgs, wantArgs)
	}
	if spec.BucketCount != 3 || spec.Automatic {
		t.Fatalf("bucket count/automatic = %d/%v, want 3/false", spec.BucketCount, spec.Automatic)
	}
	if spec.MaximumPoints != 100 || spec.MaximumEncodedElements != 101 ||
		spec.AlignmentOracleRequired {
		t.Fatalf("resource/oracle metadata = %#v", spec)
	}
}

func TestStatsSparklineBucketSpecAutomaticDefaultSteps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		rangeFor  time.Duration
		unit      plan.SparklineSpanUnit
		magnitude uint64
	}{
		{"one minute selects one second", time.Minute, plan.SparklineSpanUnitSecond, 1},
		{"seven days selects one day", 7 * 24 * time.Hour, plan.SparklineSpanUnitDay, 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			earliest := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)
			spec, err := statsSparklineBucketSpecFor(
				plan.SparklineSpan{Kind: plan.SparklineSpanKindAutomatic},
				earliest,
				earliest.Add(test.rangeFor),
				spl.MaximumStatsSparklinePoints,
				`"_time"`,
				"UTC",
			)
			if err != nil {
				t.Fatalf("automatic bucket spec: %v", err)
			}
			if !spec.Automatic || spec.Span.Unit != test.unit ||
				spec.Span.Magnitude != test.magnitude || spec.BucketCount > 100 {
				t.Fatalf("automatic bucket spec = %#v", spec)
			}
		})
	}
}

func TestStatsSparklineBucketSpecAutomaticUsesPinnedStepOrder(t *testing.T) {
	t.Parallel()

	want := []plan.SparklineSpan{
		{Kind: plan.SparklineSpanKindExplicit, Magnitude: 1, Unit: plan.SparklineSpanUnitSecond},
		{Kind: plan.SparklineSpanKindExplicit, Magnitude: 5, Unit: plan.SparklineSpanUnitSecond},
		{Kind: plan.SparklineSpanKindExplicit, Magnitude: 10, Unit: plan.SparklineSpanUnitSecond},
		{Kind: plan.SparklineSpanKindExplicit, Magnitude: 30, Unit: plan.SparklineSpanUnitSecond},
		{Kind: plan.SparklineSpanKindExplicit, Magnitude: 1, Unit: plan.SparklineSpanUnitMinute},
		{Kind: plan.SparklineSpanKindExplicit, Magnitude: 5, Unit: plan.SparklineSpanUnitMinute},
		{Kind: plan.SparklineSpanKindExplicit, Magnitude: 10, Unit: plan.SparklineSpanUnitMinute},
		{Kind: plan.SparklineSpanKindExplicit, Magnitude: 30, Unit: plan.SparklineSpanUnitMinute},
		{Kind: plan.SparklineSpanKindExplicit, Magnitude: 1, Unit: plan.SparklineSpanUnitHour},
		{Kind: plan.SparklineSpanKindExplicit, Magnitude: 1, Unit: plan.SparklineSpanUnitDay},
		{Kind: plan.SparklineSpanKindExplicit, Magnitude: 1, Unit: plan.SparklineSpanUnitMonth},
	}
	if got := statsSparklineAutomaticSteps[:]; !reflect.DeepEqual(got, want) {
		t.Fatalf("automatic steps = %#v, want %#v", got, want)
	}
}

func TestStatsSparklineBucketSpecMonthAndLimits(t *testing.T) {
	t.Parallel()

	earliest := time.Date(2026, time.January, 15, 0, 0, 0, 0, time.UTC)
	spec, err := statsSparklineBucketSpecFor(
		plan.SparklineSpan{
			Kind:      plan.SparklineSpanKindExplicit,
			Magnitude: 1,
			Unit:      plan.SparklineSpanUnitMonth,
		},
		earliest,
		time.Date(2026, time.April, 1, 0, 0, 0, 0, time.UTC),
		spl.MaximumStatsSparklinePoints,
		`"_time"`,
		"UTC",
	)
	if err != nil {
		t.Fatalf("month bucket spec: %v", err)
	}
	if spec.BucketCount != 3 || !strings.Contains(spec.BucketSQL, "toYear") ||
		!strings.Contains(spec.BucketSQL, "toMonth") ||
		!reflect.DeepEqual(spec.BucketArgs, []any{"UTC", "UTC", int64(1)}) ||
		spec.AlignmentOracleRequired {
		t.Fatalf("month bucket spec = %#v", spec)
	}

	_, err = statsSparklineBucketSpecFor(
		plan.SparklineSpan{
			Kind:      plan.SparklineSpanKindExplicit,
			Magnitude: 1,
			Unit:      plan.SparklineSpanUnitSecond,
		},
		earliest,
		earliest.Add(101*time.Second),
		spl.MaximumStatsSparklinePoints,
		`"_time"`,
		"UTC",
	)
	if err == nil || !strings.Contains(err.Error(), "maximum is 100") {
		t.Fatalf("101-point explicit span error = %v", err)
	}
}

func TestStatsSparklineBucketSpecUsesSearchTimezoneForCivilSpans(t *testing.T) {
	t.Parallel()

	const timezone = "America/New_York"
	day, err := statsSparklineBucketSpecFor(
		plan.SparklineSpan{
			Kind:      plan.SparklineSpanKindExplicit,
			Magnitude: 1,
			Unit:      plan.SparklineSpanUnitDay,
		},
		// The 2026 spring-forward local day is only 23 elapsed hours. Both
		// half-open boundaries are local midnight despite different offsets.
		time.Date(2026, time.March, 8, 5, 0, 0, 0, time.UTC),
		time.Date(2026, time.March, 9, 4, 0, 0, 0, time.UTC),
		spl.MaximumStatsSparklinePoints,
		`"_time"`,
		timezone,
	)
	if err != nil {
		t.Fatalf("civil day bucket spec: %v", err)
	}
	if day.BucketCount != 1 || day.AlignmentOracleRequired ||
		!strings.Contains(day.BucketSQL, "toDaysSinceYearZero") ||
		!reflect.DeepEqual(day.BucketArgs, []any{timezone, int64(1)}) {
		t.Fatalf("civil day bucket spec = %#v", day)
	}

	month, err := statsSparklineBucketSpecFor(
		plan.SparklineSpan{
			Kind:      plan.SparklineSpanKindExplicit,
			Magnitude: 1,
			Unit:      plan.SparklineSpanUnitMonth,
		},
		// These instants share one UTC date but straddle local January/February.
		time.Date(2026, time.February, 1, 4, 30, 0, 0, time.UTC),
		time.Date(2026, time.February, 1, 6, 30, 0, 0, time.UTC),
		spl.MaximumStatsSparklinePoints,
		`"_time"`,
		timezone,
	)
	if err != nil {
		t.Fatalf("civil month bucket spec: %v", err)
	}
	if month.BucketCount != 2 || month.AlignmentOracleRequired ||
		!reflect.DeepEqual(
			month.BucketArgs,
			[]any{timezone, timezone, int64(1)},
		) {
		t.Fatalf("civil month bucket spec = %#v", month)
	}

	multiDay, err := statsSparklineBucketSpecFor(
		plan.SparklineSpan{
			Kind:      plan.SparklineSpanKindExplicit,
			Magnitude: 2,
			Unit:      plan.SparklineSpanUnitDay,
		},
		time.Date(2026, time.March, 8, 5, 0, 0, 0, time.UTC),
		time.Date(2026, time.March, 10, 4, 0, 0, 0, time.UTC),
		spl.MaximumStatsSparklinePoints,
		`"_time"`,
		timezone,
	)
	if err != nil || !multiDay.AlignmentOracleRequired {
		t.Fatalf("multi-day anchor metadata = %#v, err=%v", multiDay, err)
	}
}

func TestStatsSparklineBucketSpecRejectsForgedMetadata(t *testing.T) {
	t.Parallel()

	earliest := time.Date(2026, time.August, 11, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		span    plan.SparklineSpan
		maximum uint16
		timeSQL string
	}{
		{"invalid kind", plan.SparklineSpan{}, 100, `"_time"`},
		{"automatic with magnitude", plan.SparklineSpan{Kind: plan.SparklineSpanKindAutomatic, Magnitude: 1}, 100, `"_time"`},
		{"explicit zero", plan.SparklineSpan{Kind: plan.SparklineSpanKindExplicit, Unit: plan.SparklineSpanUnitSecond}, 100, `"_time"`},
		{"fixed overflow", plan.SparklineSpan{Kind: plan.SparklineSpanKindExplicit, Magnitude: math.MaxUint64, Unit: plan.SparklineSpanUnitHour}, 100, `"_time"`},
		{"wrong maximum", plan.SparklineSpan{Kind: plan.SparklineSpanKindAutomatic}, 99, `"_time"`},
		{"missing time SQL", plan.SparklineSpan{Kind: plan.SparklineSpanKindAutomatic}, 100, ""},
		{"invalid timezone", plan.SparklineSpan{Kind: plan.SparklineSpanKindAutomatic}, 100, `"_time"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			timezone := "UTC"
			if test.name == "invalid timezone" {
				timezone = "Local"
			}
			if _, err := statsSparklineBucketSpecFor(
				test.span,
				earliest,
				earliest.Add(time.Hour),
				test.maximum,
				test.timeSQL,
				timezone,
			); err == nil {
				t.Fatal("forged sparkline metadata was accepted")
			}
		})
	}
}

func TestStatsSparklineWindowAggregateSQL(t *testing.T) {
	t.Parallel()

	const (
		numeric   = `"numeric_values"`
		stringsIn = `"string_values"`
		count     = `"occurrence_count"`
		partition = `"group", "bucket"`
		over      = ` OVER (PARTITION BY "group", "bucket")`
	)
	tests := []struct {
		name     string
		function plan.AggregateFunction
		input    string
		wantSQL  string
		wantKind statsSparklineInputKind
		result   statsSparklineResultKind
		oracle   bool
	}{
		{"count", plan.AggregateFunctionCountRows, "", `toUInt64(count()` + over + `)`, statsSparklineInputNone, statsSparklineResultUInt64, false},
		{"count values", plan.AggregateFunctionCountValues, count, `toUInt64(sum(toUInt128("occurrence_count"))` + over + `)`, statsSparklineInputOccurrenceCount, statsSparklineResultUInt64, false},
		{"average", plan.AggregateFunctionAverage, numeric, `avgOrNullArray("numeric_values")` + over, statsSparklineInputFloat64Array, statsSparklineResultNullableFloat64, false},
		{"stdev", plan.AggregateFunctionStandardDeviationSample, numeric, `if(countArray("numeric_values")` + over + ` = 1, CAST(0 AS Nullable(Float64)), stddevSampStableOrNullArray("numeric_values")` + over + `)`, statsSparklineInputFloat64Array, statsSparklineResultNullableFloat64, false},
		{"stdevp", plan.AggregateFunctionStandardDeviationPopulation, numeric, `stddevPopStableOrNullArray("numeric_values")` + over, statsSparklineInputFloat64Array, statsSparklineResultNullableFloat64, false},
		{"var", plan.AggregateFunctionVarianceSample, numeric, `if(countArray("numeric_values")` + over + ` = 1, CAST(0 AS Nullable(Float64)), varSampStableOrNullArray("numeric_values")` + over + `)`, statsSparklineInputFloat64Array, statsSparklineResultNullableFloat64, false},
		{"varp", plan.AggregateFunctionVariancePopulation, numeric, `varPopStableOrNullArray("numeric_values")` + over, statsSparklineInputFloat64Array, statsSparklineResultNullableFloat64, false},
		{"sum", plan.AggregateFunctionSum, numeric, `sumOrNullArray("numeric_values")` + over, statsSparklineInputFloat64Array, statsSparklineResultNullableFloat64, false},
		{"sumsq", plan.AggregateFunctionSumSquares, numeric, `sumOrNullArray(arrayMap(value -> value * value, "numeric_values"))` + over, statsSparklineInputFloat64Array, statsSparklineResultNullableFloat64, false},
		{"minimum", plan.AggregateFunctionMinimum, stringsIn, `minOrNullArray("string_values")` + over, statsSparklineInputStringArray, statsSparklineResultNullableString, true},
		{"maximum", plan.AggregateFunctionMaximum, stringsIn, `maxOrNullArray("string_values")` + over, statsSparklineInputStringArray, statsSparklineResultNullableString, true},
		{"range", plan.AggregateFunctionRange, numeric, `maxOrNullArray("numeric_values")` + over + ` - minOrNullArray("numeric_values")` + over, statsSparklineInputFloat64Array, statsSparklineResultNullableFloat64, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, ok := statsSparklineWindowAggregateSQL(test.function, test.input, partition)
			if !ok {
				t.Fatalf("function %v was rejected", test.function)
			}
			if got.SQL != test.wantSQL || got.Input != test.wantKind ||
				got.Result != test.result || got.OracleRequired != test.oracle {
				t.Fatalf("lowering = %#v, want SQL %q, input %v, result %v, oracle %v", got, test.wantSQL, test.wantKind, test.result, test.oracle)
			}
		})
	}
}

func TestStatsSparklineWindowDistinctCountIsExactAndBounded(t *testing.T) {
	t.Parallel()

	got, ok := statsSparklineWindowAggregateSQL(
		plan.AggregateFunctionDistinctCount,
		`"strings"`,
		`"group", "bucket"`,
	)
	if !ok {
		t.Fatal("dc was rejected")
	}
	for _, required := range []string{
		`groupUniqArrayArray(100001)("strings") OVER (PARTITION BY "group", "bucket")`,
		`cardinality > 100000`,
		ExactDistinctLimitMarker,
	} {
		if !strings.Contains(got.SQL, required) {
			t.Fatalf("dc SQL missing %q: %s", required, got.SQL)
		}
	}
	if got.MaximumDistinctPerBucket != MaximumStatsDistinctValuesPerGroup ||
		got.StateBound != statsSparklineStateBoundLinearDistinct {
		t.Fatalf("dc resource metadata = %#v", got)
	}
}

func TestStatsSparklineWindowAggregateSQLRejectsInvalidContracts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		function  plan.AggregateFunction
		input     string
		partition string
	}{
		{plan.AggregateFunctionInvalid, `"values"`, `"bucket"`},
		{plan.AggregateFunctionCountRows, `"values"`, `"bucket"`},
		{plan.AggregateFunctionSum, "", `"bucket"`},
		{plan.AggregateFunctionSum, `"values"`, ""},
		{plan.AggregateFunction(255), `"values"`, `"bucket"`},
	}
	for _, test := range tests {
		if got, ok := statsSparklineWindowAggregateSQL(
			test.function,
			test.input,
			test.partition,
		); ok || got != (statsSparklineAggregateLowering{}) {
			t.Fatalf("invalid contract = %#v, %v", got, ok)
		}
	}
}

func TestStatsSparklineBucketRecordsAndPublication(t *testing.T) {
	t.Parallel()

	records, ok := statsSparklineBucketRecordsSQL(
		`"bucket"`,
		`"value"`,
		spl.MaximumStatsSparklinePoints,
	)
	if !ok {
		t.Fatal("bucket record lowering was rejected")
	}
	wantRecords := `groupUniqArray(101)(tuple(toInt64("bucket"), "value"))`
	if records != wantRecords {
		t.Fatalf("records SQL = %q, want %q", records, wantRecords)
	}

	spec := statsSparklineBucketSpec{
		FirstBucket:            -1,
		BucketCount:            3,
		MaximumPoints:          100,
		MaximumEncodedElements: 101,
	}
	published, ok := statsSparklinePublishSQL(`"records"`, spec, statsSparklineMissingZero)
	if !ok {
		t.Fatal("sparkline publication was rejected")
	}
	for _, required := range []string{
		`CAST(['##__SPARKLINE__##'], 'Array(String)')`,
		`range(toUInt64(3))`,
		`toInt64(-1)`,
		`indexOf(arrayMap(record -> tupleElement(record, 1), "records"), bucket)`,
		`length("records") > 100`,
		StatsSparklineLimitMarker,
		`arrayFold((bytes, value) -> bytes + toUInt128(length(value)), series, toUInt128(0)) > toUInt128(8388608)`,
		StatsSparklineBytesLimitMarker,
	} {
		if !strings.Contains(published, required) {
			t.Fatalf("publication SQL missing %q:\n%s", required, published)
		}
	}
	if strings.Contains(published, "arraySlice") {
		t.Fatalf("publication silently truncates: %s", published)
	}
}

func TestStatsSparklinePublicationRejectsInvalidContracts(t *testing.T) {
	t.Parallel()

	valid := statsSparklineBucketSpec{
		FirstBucket:            0,
		BucketCount:            1,
		MaximumPoints:          100,
		MaximumEncodedElements: 101,
	}
	if got, ok := statsSparklineBucketRecordsSQL("", `"value"`, 100); ok || got != "" {
		t.Fatalf("empty bucket SQL = %q, %v", got, ok)
	}
	for _, test := range []struct {
		records string
		spec    statsSparklineBucketSpec
		missing statsSparklineMissingValue
	}{
		{"", valid, statsSparklineMissingZero},
		{`"records"`, statsSparklineBucketSpec{}, statsSparklineMissingZero},
		{`"records"`, valid, statsSparklineMissingInvalid},
	} {
		if got, ok := statsSparklinePublishSQL(test.records, test.spec, test.missing); ok || got != "" {
			t.Fatalf("invalid publication = %q, %v", got, ok)
		}
	}
}
