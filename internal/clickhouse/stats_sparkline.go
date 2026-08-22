package clickhouse

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"fortio.org/safecast"

	"github.com/Suhaibinator/open-splunk/internal/ianatimezone"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	// statsSparklineMarker is Splunk's transport marker at element zero of a
	// sparkline multivalue. Numeric or lexical bucket values follow it.
	statsSparklineMarker = "##__SPARKLINE__##"

	// StatsSparklineLimitMarker classifies a forged or unexpectedly amplified
	// bucket record set. Normal compilation proves at most MaximumPoints
	// distinct buckets before this defensive runtime guard is emitted.
	StatsSparklineLimitMarker = "open-splunk: stats sparkline exceeds the supported limit"

	// MaximumStatsSparklineBytesPerCell prevents one lexical min/max sparkline
	// from retaining roughly 100 maximum-sized source strings. It reuses the
	// existing stricter typed-multivalue publication budget; the executor's
	// result-byte limit independently bounds the complete multi-row result.
	MaximumStatsSparklineBytesPerCell uint64 = MaximumStatsValuesBytesPerResult
	// StatsSparklineBytesLimitMarker distinguishes payload amplification from a
	// forged excess-bucket state at the executor boundary.
	StatsSparklineBytesLimitMarker = "open-splunk: stats sparkline bytes exceed the supported limit"
)

// statsSparklineBucketSpec is the compiler-owned, search-range-specific time
// grid for one sparkline. BucketSQL returns a signed, consecutive bucket key.
// The public cell contains one additional transport marker. Official Splunk
// documentation does not say whether that marker consumes one of the
// sparkline_maxsize elements, so the physical +1 remains explicitly marked as
// oracle-required rather than silently reducing the documented 100 bins.
type statsSparklineBucketSpec struct {
	Span                    plan.SparklineSpan
	Automatic               bool
	BucketSQL               string
	BucketArgs              []any
	FirstBucket             int64
	BucketCount             uint16
	MaximumPoints           uint16
	MaximumEncodedElements  uint16
	AlignmentOracleRequired bool
}

var statsSparklineAutomaticSteps = [...]plan.SparklineSpan{
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

// statsSparklineBucketSpecFor resolves an authored span against the half-open
// scan range. Automatic selection uses the exact pinned Splunk 10.0 default
// step list and picks the first aligned step producing no more than
// MaximumPoints bins. An explicit span that exceeds the same ceiling fails
// closed; it is never silently truncated.
//
// timeSQL is compiler-owned SQL for the canonical DateTime64(9, 'UTC') _time
// column. Subday spans use the documented epoch grid. Day and month spans use
// searchTimezone civil boundaries, including DST transitions; only the anchor
// of a multi-day or multi-month grid remains oracle-required.
func statsSparklineBucketSpecFor(
	span plan.SparklineSpan,
	earliest time.Time,
	latest time.Time,
	maximumPoints uint16,
	timeSQL string,
	searchTimezone string,
) (statsSparklineBucketSpec, error) {
	if maximumPoints != spl.MaximumStatsSparklinePoints {
		return statsSparklineBucketSpec{}, fmt.Errorf(
			"compile ClickHouse stats sparkline: maximum points must be %d",
			spl.MaximumStatsSparklinePoints,
		)
	}
	if timeSQL == "" {
		return statsSparklineBucketSpec{}, errors.New(
			"compile ClickHouse stats sparkline: canonical time SQL is required",
		)
	}
	location, err := ianatimezone.Load(searchTimezone)
	if err != nil {
		return statsSparklineBucketSpec{}, errors.New(
			"compile ClickHouse stats sparkline: search timezone is invalid",
		)
	}
	earliest = earliest.Round(0).UTC()
	latest = latest.Round(0).UTC()
	if !earliest.Before(latest) ||
		!SupportsSearchTimeRange(earliest, latest) {
		return statsSparklineBucketSpec{}, errors.New(
			"compile ClickHouse stats sparkline: search time range is invalid",
		)
	}

	switch span.Kind {
	case plan.SparklineSpanKindAutomatic:
		if span.Magnitude != 0 || span.Unit != plan.SparklineSpanUnitInvalid {
			return statsSparklineBucketSpec{}, errors.New(
				"compile ClickHouse stats sparkline: automatic span metadata is invalid",
			)
		}
		for _, candidate := range statsSparklineAutomaticSteps {
			spec, count, err := statsSparklineExplicitBucketSpec(
				candidate,
				earliest,
				latest,
				maximumPoints,
				timeSQL,
				searchTimezone,
				location,
			)
			if err != nil {
				return statsSparklineBucketSpec{}, err
			}
			if count <= uint64(maximumPoints) {
				spec.Automatic = true
				return spec, nil
			}
		}
		return statsSparklineBucketSpec{}, fmt.Errorf(
			"compile ClickHouse stats sparkline: automatic span cannot fit within %d points",
			maximumPoints,
		)
	case plan.SparklineSpanKindExplicit:
		spec, count, err := statsSparklineExplicitBucketSpec(
			span,
			earliest,
			latest,
			maximumPoints,
			timeSQL,
			searchTimezone,
			location,
		)
		if err != nil {
			return statsSparklineBucketSpec{}, err
		}
		if count > uint64(maximumPoints) {
			return statsSparklineBucketSpec{}, fmt.Errorf(
				"compile ClickHouse stats sparkline: explicit span produces %d points, maximum is %d",
				count,
				maximumPoints,
			)
		}
		return spec, nil
	default:
		return statsSparklineBucketSpec{}, errors.New(
			"compile ClickHouse stats sparkline: span kind is invalid",
		)
	}
}

func statsSparklineExplicitBucketSpec(
	span plan.SparklineSpan,
	earliest time.Time,
	latest time.Time,
	maximumPoints uint16,
	timeSQL string,
	searchTimezone string,
	location *time.Location,
) (statsSparklineBucketSpec, uint64, error) {
	if span.Kind != plan.SparklineSpanKindExplicit || span.Magnitude == 0 ||
		span.Unit == plan.SparklineSpanUnitInvalid {
		return statsSparklineBucketSpec{}, 0, errors.New(
			"compile ClickHouse stats sparkline: explicit span is invalid",
		)
	}

	base := statsSparklineBucketSpec{
		Span:                   span,
		MaximumPoints:          maximumPoints,
		MaximumEncodedElements: maximumPoints + 1,
	}
	lastIncluded := latest.Add(-time.Nanosecond)
	if span.Unit == plan.SparklineSpanUnitDay {
		if span.Magnitude > math.MaxInt64 {
			return statsSparklineBucketSpec{}, 0, errors.New(
				"compile ClickHouse stats sparkline: day span is outside the supported range",
			)
		}
		magnitude := safecast.MustConv[int64](span.Magnitude)
		firstDay := statsSparklineCivilDayIndex(earliest, location)
		lastDay := statsSparklineCivilDayIndex(lastIncluded, location)
		base.FirstBucket = firstDay / magnitude
		lastBucket := lastDay / magnitude
		count := safecast.MustConv[uint64](lastBucket-base.FirstBucket) + 1
		localDate := "toDate32(toTimeZone(" + timeSQL + ", ?))"
		base.BucketSQL = "intDiv(toInt64(toDaysSinceYearZero(" + localDate + ")), ?)"
		base.BucketArgs = []any{searchTimezone, magnitude}
		// Local-midnight alignment is documented. Splunk does not document
		// which civil day anchors a multi-day grid.
		base.AlignmentOracleRequired = magnitude > 1
		if count <= math.MaxUint16 {
			base.BucketCount = safecast.MustConv[uint16](count)
		}
		return base, count, nil
	}
	if span.Unit == plan.SparklineSpanUnitMonth {
		if span.Magnitude > math.MaxInt64 {
			return statsSparklineBucketSpec{}, 0, errors.New(
				"compile ClickHouse stats sparkline: month span is outside the supported range",
			)
		}
		magnitude := safecast.MustConv[int64](span.Magnitude)
		firstMonth := statsSparklineMonthIndex(earliest, location)
		lastMonth := statsSparklineMonthIndex(lastIncluded, location)
		base.FirstBucket = firstMonth / magnitude
		lastBucket := lastMonth / magnitude
		count := safecast.MustConv[uint64](lastBucket-base.FirstBucket) + 1
		localizedYear := "toYear(toTimeZone(" + timeSQL + ", ?))"
		localizedMonth := "toMonth(toTimeZone(" + timeSQL + ", ?))"
		monthIndexSQL := "(toInt64(" + localizedYear + ") * 12 + " +
			"toInt64(" + localizedMonth + ") - 1)"
		base.BucketSQL = "intDiv(" + monthIndexSQL + ", ?)"
		base.BucketArgs = []any{searchTimezone, searchTimezone, magnitude}
		// Local month boundaries are documented; the anchor for a grid of
		// multiple months is not.
		base.AlignmentOracleRequired = magnitude > 1
		if count <= math.MaxUint16 {
			base.BucketCount = safecast.MustConv[uint16](count)
		}
		return base, count, nil
	}

	unitNanoseconds, ok := statsSparklineFixedUnitNanoseconds(span.Unit)
	if !ok {
		return statsSparklineBucketSpec{}, 0, errors.New(
			"compile ClickHouse stats sparkline: fixed span is outside the supported range",
		)
	}
	maximumMagnitude := safecast.MustConv[uint64](math.MaxInt64 / unitNanoseconds)
	if span.Magnitude > maximumMagnitude {
		return statsSparklineBucketSpec{}, 0, errors.New(
			"compile ClickHouse stats sparkline: fixed span is outside the supported range",
		)
	}
	spanNanoseconds := safecast.MustConv[int64](span.Magnitude) * unitNanoseconds
	base.FirstBucket = statsSparklineFloorQuotient(earliest.UnixNano(), spanNanoseconds)
	lastBucket := statsSparklineFloorQuotient(lastIncluded.UnixNano(), spanNanoseconds)
	count := safecast.MustConv[uint64](lastBucket-base.FirstBucket) + 1
	ticks := "reinterpretAsInt64(" + timeSQL + ")"
	base.BucketSQL = epochFloorBucketNumberSQL(ticks)
	base.BucketArgs = []any{spanNanoseconds, spanNanoseconds}
	if count <= math.MaxUint16 {
		base.BucketCount = safecast.MustConv[uint16](count)
	}
	return base, count, nil
}

func statsSparklineFixedUnitNanoseconds(unit plan.SparklineSpanUnit) (int64, bool) {
	switch unit {
	case plan.SparklineSpanUnitMicrosecond:
		return int64(time.Microsecond), true
	case plan.SparklineSpanUnitMillisecond:
		return int64(time.Millisecond), true
	case plan.SparklineSpanUnitCentisecond:
		return int64(10 * time.Millisecond), true
	case plan.SparklineSpanUnitDecisecond:
		return int64(100 * time.Millisecond), true
	case plan.SparklineSpanUnitSecond:
		return int64(time.Second), true
	case plan.SparklineSpanUnitMinute:
		return int64(time.Minute), true
	case plan.SparklineSpanUnitHour:
		return int64(time.Hour), true
	case plan.SparklineSpanUnitDay:
		return int64(24 * time.Hour), true
	default:
		return 0, false
	}
}

func statsSparklineFloorQuotient(value, divisor int64) int64 {
	quotient := value / divisor
	if value < 0 && value%divisor != 0 {
		quotient--
	}
	return quotient
}

func statsSparklineCivilDayIndex(value time.Time, location *time.Location) int64 {
	local := value.In(location)
	// ClickHouse's toDaysSinceYearZero uses the proleptic Gregorian day
	// ordinal. 1970-01-01 is ordinal 719528, and a UTC midnight constructed
	// from the local civil components gives the exact signed Unix-day delta.
	const unixEpochCivilDay int64 = 719528
	civilUTC := time.Date(
		local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, time.UTC,
	)
	return unixEpochCivilDay + civilUTC.Unix()/int64(24*time.Hour/time.Second)
}

func statsSparklineMonthIndex(value time.Time, location *time.Location) int64 {
	value = value.In(location)
	return int64(value.Year())*12 + int64(value.Month()) - 1
}

type statsSparklineInputKind uint8

const (
	statsSparklineInputInvalid statsSparklineInputKind = iota
	statsSparklineInputNone
	statsSparklineInputOccurrenceCount
	statsSparklineInputFloat64Array
	statsSparklineInputStringArray
)

type statsSparklineResultKind uint8

const (
	statsSparklineResultInvalid statsSparklineResultKind = iota
	statsSparklineResultUInt64
	statsSparklineResultNullableFloat64
	statsSparklineResultNullableString
)

type statsSparklineStateBound uint8

const (
	statsSparklineStateBoundInvalid statsSparklineStateBound = iota
	statsSparklineStateBoundConstant
	statsSparklineStateBoundLinearDistinct
)

type statsSparklineAggregateLowering struct {
	SQL                      string
	Input                    statsSparklineInputKind
	Result                   statsSparklineResultKind
	StateBound               statsSparklineStateBound
	OracleRequired           bool
	MaximumDistinctPerBucket uint64
}

// statsSparklineWindowAggregateSQL produces the per-(stats BY tuple,time-bin)
// window expression that must be materialized before the ordinary stats GROUP
// BY. inputSQL is one compiler-owned normalized row expression selected by the
// returned Input kind. partitionSQL is the compiler-owned BY aliases followed
// by the signed bucket alias.
//
// The outer stats aggregate can then retain one distinct (bucket,value) record
// per bucket while ordinary scalar measures are reduced normally. This avoids
// a second scan and permits sparkline and ordinary stats measures to coexist.
func statsSparklineWindowAggregateSQL(
	function plan.AggregateFunction,
	inputSQL string,
	partitionSQL string,
) (statsSparklineAggregateLowering, bool) {
	if partitionSQL == "" {
		return statsSparklineAggregateLowering{}, false
	}
	over := " OVER (PARTITION BY " + partitionSQL + ")"
	requireInput := func(kind statsSparklineInputKind) bool {
		return kind == statsSparklineInputNone || inputSQL != ""
	}

	switch function {
	case plan.AggregateFunctionCountRows:
		if inputSQL != "" {
			return statsSparklineAggregateLowering{}, false
		}
		return statsSparklineAggregateLowering{
			SQL:        "toUInt64(count()" + over + ")",
			Input:      statsSparklineInputNone,
			Result:     statsSparklineResultUInt64,
			StateBound: statsSparklineStateBoundConstant,
		}, true
	case plan.AggregateFunctionCountValues:
		if !requireInput(statsSparklineInputOccurrenceCount) {
			return statsSparklineAggregateLowering{}, false
		}
		return statsSparklineAggregateLowering{
			SQL:        "toUInt64(sum(toUInt128(" + inputSQL + "))" + over + ")",
			Input:      statsSparklineInputOccurrenceCount,
			Result:     statsSparklineResultUInt64,
			StateBound: statsSparklineStateBoundConstant,
		}, true
	case plan.AggregateFunctionDistinctCount:
		if !requireInput(statsSparklineInputStringArray) {
			return statsSparklineAggregateLowering{}, false
		}
		maximum := uint64(MaximumStatsDistinctValuesPerGroup)
		set := "groupUniqArrayArray(" + strconv.FormatUint(maximum+1, 10) + ")(" +
			inputSQL + ")" + over
		cardinality := "toUInt64(length(" + set + "))"
		bounded := "arrayElement(arrayMap(cardinality -> cardinality + " +
			"toUInt64(throwIf(toUInt8(cardinality > " +
			strconv.FormatUint(maximum, 10) + "), '" + ExactDistinctLimitMarker +
			"')), [" + cardinality + "]), 1)"
		return statsSparklineAggregateLowering{
			SQL:                      bounded,
			Input:                    statsSparklineInputStringArray,
			Result:                   statsSparklineResultUInt64,
			StateBound:               statsSparklineStateBoundLinearDistinct,
			MaximumDistinctPerBucket: maximum,
		}, true
	case plan.AggregateFunctionMinimum, plan.AggregateFunctionMaximum:
		if !requireInput(statsSparklineInputStringArray) {
			return statsSparklineAggregateLowering{}, false
		}
		name := "minOrNullArray"
		if function == plan.AggregateFunctionMaximum {
			name = "maxOrNullArray"
		}
		return statsSparklineAggregateLowering{
			SQL:        name + "(" + inputSQL + ")" + over,
			Input:      statsSparklineInputStringArray,
			Result:     statsSparklineResultNullableString,
			StateBound: statsSparklineStateBoundConstant,
			// The bytewise lexical lowering is deterministic, but Splunk's
			// mixed numeric/string ordering remains O-min-max-order.
			OracleRequired: true,
		}, true
	}

	if inputSQL == "" {
		return statsSparklineAggregateLowering{}, false
	}
	sql, supported := numericArrayAggregateOverSQL(function, inputSQL, over)
	if !supported {
		return statsSparklineAggregateLowering{}, false
	}
	return statsSparklineAggregateLowering{
		SQL:        sql,
		Input:      statsSparklineInputFloat64Array,
		Result:     statsSparklineResultNullableFloat64,
		StateBound: statsSparklineStateBoundConstant,
	}, true
}

// statsSparklineBucketRecordsSQL retains one distinct value per signed bucket.
// The +1 capacity is a sentinel; statsSparklinePublishSQL rejects it instead
// of truncating. valueSQL is the already-materialized window result.
func statsSparklineBucketRecordsSQL(
	bucketSQL string,
	valueSQL string,
	maximumPoints uint16,
) (string, bool) {
	if bucketSQL == "" || valueSQL == "" ||
		maximumPoints != spl.MaximumStatsSparklinePoints {
		return "", false
	}
	return "groupUniqArray(" + strconv.FormatUint(uint64(maximumPoints)+1, 10) +
		")(tuple(toInt64(" + bucketSQL + "), " + valueSQL + "))", true
}

type statsSparklineMissingValue uint8

const (
	statsSparklineMissingInvalid statsSparklineMissingValue = iota
	statsSparklineMissingZero
	statsSparklineMissingEmpty
)

// statsSparklinePublishSQL converts bounded private bucket records into the
// existing Array(String) multivalue transport and prepends Splunk's marker.
// Count sparklines use MissingZero. The representation of an empty numeric or
// lexical bucket is not pinned by the official pages, so callers using
// MissingEmpty must keep that behavior oracle-required in compatibility data.
func statsSparklinePublishSQL(
	recordsSQL string,
	spec statsSparklineBucketSpec,
	missing statsSparklineMissingValue,
) (string, bool) {
	if recordsSQL == "" || spec.BucketCount == 0 ||
		spec.MaximumPoints != spl.MaximumStatsSparklinePoints ||
		spec.BucketCount > spec.MaximumPoints ||
		spec.MaximumEncodedElements != spec.MaximumPoints+1 {
		return "", false
	}
	missingString := ""
	switch missing {
	case statsSparklineMissingZero:
		missingString = "0"
	case statsSparklineMissingEmpty:
		missingString = ""
	default:
		return "", false
	}

	first := strconv.FormatInt(spec.FirstBucket, 10)
	count := strconv.FormatUint(uint64(spec.BucketCount), 10)
	maximum := strconv.FormatUint(uint64(spec.MaximumPoints), 10)
	grid := "arrayMap(ordinal -> toInt64(" + first + ") + toInt64(ordinal), " +
		"range(toUInt64(" + count + ")))"
	keys := "arrayMap(record -> tupleElement(record, 1), " + recordsSQL + ")"
	index := "indexOf(" + keys + ", bucket)"
	value := "tupleElement(arrayElement(" + recordsSQL + ", " + index + "), 2)"
	encoded := "if(" + index + " = 0 OR isNull(" + value + "), '" +
		missingString + "', toString(assumeNotNull(" + value + ")))"
	values := "arrayMap(bucket -> " + encoded + ", " + grid + ")"
	series := "arrayConcat(CAST(['" + statsSparklineMarker +
		"'], 'Array(String)'), " + values + ")"
	// arrayMap binds the large series expression once while preserving an
	// Array(String) result from the validation side effects. The payload guard
	// includes the marker itself; marker accounting against the point-count
	// ceiling remains the separate oracle above.
	return "arrayElement(arrayMap(series -> arrayResize(series, length(series) + " +
		"toUInt64(throwIf(toUInt8(length(" + recordsSQL + ") > " + maximum +
		"), '" + StatsSparklineLimitMarker + "')) + " +
		"toUInt64(throwIf(toUInt8(arrayFold((bytes, value) -> bytes + " +
		"toUInt128(length(value)), series, toUInt128(0)) > toUInt128(" +
		strconv.FormatUint(MaximumStatsSparklineBytesPerCell, 10) + ")), '" +
		StatsSparklineBytesLimitMarker + "'))), [" + series + "]), 1)", true
}
