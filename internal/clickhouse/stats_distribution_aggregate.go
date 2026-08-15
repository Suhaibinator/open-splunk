package clickhouse

import (
	"fmt"
	"strconv"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

const (
	statsDistributionGKAccuracy            = 100
	statsDistributionDistinctPrecision     = 17
	statsDistributionSplunkApproxThreshold = 1000
	statsDistributionDistinctRelativeError = "0.002872621298570349"
)

type statsDistributionStateBound uint8

const (
	_ statsDistributionStateBound = iota
	// The aggregate state has a fixed upper bound independent of input
	// cardinality. Multiple constant-size states may still be present.
	statsDistributionStateBoundConstant
	// The aggregate retains every eligible input value. The query-wide
	// ClickHouse memory limit is the hard resource boundary.
	statsDistributionStateBoundLinearValues
	// The aggregate retains one entry per distinct input value. The query-wide
	// ClickHouse memory limit is the hard resource boundary.
	statsDistributionStateBoundLinearDistinct
)

type statsDistributionResultKind uint8

const (
	statsDistributionResultInvalid statsDistributionResultKind = iota
	statsDistributionResultNullableFloat64
	statsDistributionResultUInt64
	statsDistributionResultFloat64
	statsDistributionResultNullableString
)

// statsDistributionAggregateLowering carries the semantic and resource
// contract with the SQL so an approximate or oracle-dependent implementation
// cannot be mistaken for confirmed SPL parity at the call site.
type statsDistributionAggregateLowering struct {
	SQL           string
	Result        statsDistributionResultKind
	StateBound    statsDistributionStateBound
	Exact         bool
	Deterministic bool
}

type statsExactModeWithSemanticBytesLowering struct {
	ValueSQL         string
	SemanticBytesSQL string
}

// statsDistributionArrayAggregateSQL lowers a distribution aggregate over a
// compiler-owned normalized array expression. Exact/upper percentiles and
// median require Array(Float64); estdc, estdc_error, and mode require
// Array(String). The caller owns that type distinction.
//
// Exact percentile follows Splunk's documented nearest-rank rule explicitly.
// It is intentionally linear-memory, just like Splunk exactperc, and therefore
// relies on the production query memory ceiling. Mode uses an exact sumMap
// frequency state and is linear in distinct strings. The remaining states are
// bounded, and none of these lowerings is represented as confirmed Splunk
// parity: each still requires oracle confirmation.
func statsDistributionArrayAggregateSQL(
	function plan.AggregateFunction,
	percentile uint8,
	inputSQL string,
) (statsDistributionAggregateLowering, bool) {
	if inputSQL == "" {
		return statsDistributionAggregateLowering{}, false
	}

	switch function {
	case plan.AggregateFunctionExactPercentile:
		if !statsDistributionPercentileIsValid(percentile) {
			return statsDistributionAggregateLowering{}, false
		}
		return statsDistributionAggregateLowering{
			SQL:           statsExactNearestRankArraySQL(percentile, inputSQL),
			Result:        statsDistributionResultNullableFloat64,
			StateBound:    statsDistributionStateBoundLinearValues,
			Exact:         true,
			Deterministic: true,
		}, true
	case plan.AggregateFunctionUpperPercentile:
		if !statsDistributionPercentileIsValid(percentile) {
			return statsDistributionAggregateLowering{}, false
		}
		return statsDistributionAggregateLowering{
			SQL:           statsApproximateUpperPercentileArraySQL(percentile, inputSQL),
			Result:        statsDistributionResultNullableFloat64,
			StateBound:    statsDistributionStateBoundConstant,
			Exact:         false,
			Deterministic: false,
		}, true
	case plan.AggregateFunctionMedian:
		if percentile != 0 {
			return statsDistributionAggregateLowering{}, false
		}
		return statsDistributionAggregateLowering{
			SQL:           statsApproximateMedianArraySQL(inputSQL),
			Result:        statsDistributionResultNullableFloat64,
			StateBound:    statsDistributionStateBoundConstant,
			Exact:         false,
			Deterministic: false,
		}, true
	case plan.AggregateFunctionEstimatedDistinctCount:
		if percentile != 0 {
			return statsDistributionAggregateLowering{}, false
		}
		return statsDistributionAggregateLowering{
			SQL:           statsEstimatedDistinctArraySQL(inputSQL),
			Result:        statsDistributionResultUInt64,
			StateBound:    statsDistributionStateBoundConstant,
			Exact:         false,
			Deterministic: true,
		}, true
	case plan.AggregateFunctionEstimatedDistinctCountError:
		if percentile != 0 {
			return statsDistributionAggregateLowering{}, false
		}
		return statsDistributionAggregateLowering{
			SQL:           statsEstimatedDistinctErrorArraySQL(inputSQL),
			Result:        statsDistributionResultFloat64,
			StateBound:    statsDistributionStateBoundConstant,
			Exact:         false,
			Deterministic: true,
		}, true
	case plan.AggregateFunctionMode:
		if percentile != 0 {
			return statsDistributionAggregateLowering{}, false
		}
		return statsDistributionAggregateLowering{
			SQL:           statsExactModeArraySQL(inputSQL),
			Result:        statsDistributionResultNullableString,
			StateBound:    statsDistributionStateBoundLinearDistinct,
			Exact:         true,
			Deterministic: true,
		}, true
	default:
		return statsDistributionAggregateLowering{}, false
	}
}

func statsExactNearestRankArraySQL(percentile uint8, inputSQL string) string {
	level := statsDistributionPercentileLevelSQL(percentile)
	return "arrayElementOrNull(arraySort(groupArrayArray(" + inputSQL + ")), " +
		"toUInt64(ceil(toFloat64(countArray(" + inputSQL + ")) * " + level + ")))"
}

// statsApproximateUpperPercentileArraySQL is a bounded provisional mapping,
// not a claim that Greenwald-Khanna reproduces Splunk's proprietary radix-tree
// digest. It selects the ordinary level through 1000 estimated distinct values
// and the +1% rank-error edge above that threshold. A pinned Splunk oracle must
// calibrate or replace it before semantic parity is declared.
func statsApproximateUpperPercentileArraySQL(percentile uint8, inputSQL string) string {
	lowerLevel := statsDistributionPercentileLevelSQL(percentile)
	upperLevel := statsDistributionPercentileLevelSQL(percentile + 1)
	quantiles := "quantilesGKOrNullArray(" +
		strconv.Itoa(statsDistributionGKAccuracy) + ", " +
		lowerLevel + ", " + upperLevel + ")(" + inputSQL + ")"
	estimatedDistinct := statsEstimatedDistinctArraySQL(inputSQL)
	return "arrayElementOrNull(" + quantiles + ", if(" + estimatedDistinct + " <= " +
		strconv.Itoa(statsDistributionSplunkApproxThreshold) + ", 1, 2))"
}

func statsApproximateMedianArraySQL(inputSQL string) string {
	return "arrayElementOrNull(quantilesGKOrNullArray(" +
		strconv.Itoa(statsDistributionGKAccuracy) + ", 0.5)(" + inputSQL + "), 1)"
}

func statsEstimatedDistinctArraySQL(inputSQL string) string {
	return "uniqCombined64Array(" +
		strconv.Itoa(statsDistributionDistinctPrecision) + ")(" + inputSQL + ")"
}

// statsEstimatedDistinctErrorArraySQL reports zero in the documented exact
// small-cardinality region and the HLL17 relative standard error above it.
// ClickHouse does not expose Splunk's sketch error state; this value is a
// provisional algorithm bound and requires a Splunk oracle before parity is
// claimed.
func statsEstimatedDistinctErrorArraySQL(inputSQL string) string {
	return "if(" + statsEstimatedDistinctArraySQL(inputSQL) + " < " +
		strconv.Itoa(statsDistributionSplunkApproxThreshold) +
		", toFloat64(0), toFloat64(" + statsDistributionDistinctRelativeError + "))"
}

// sumMap returns bytewise-sorted keys and their exact frequencies. Selecting
// the first maximum therefore makes ties deterministic and bytewise-low. SPL
// documents no mode tie rule, so that tie policy remains oracle-required.
func statsExactModeArraySQL(inputSQL string) string {
	frequencies := "sumMap(" + inputSQL +
		", arrayMap(_ -> toUInt64(1), " + inputSQL + "))"
	keys := "tupleElement(" + frequencies + ", 1)"
	counts := "tupleElement(" + frequencies + ", 2)"
	return "arrayElementOrNull(" + keys +
		", indexOf(" + counts + ", arrayMax(" + counts + ")))"
}

// statsExactModeWithSemanticBytesSQL selects the winner from an exact tuple
// key of (payload, semantic-Bytes bit). Hex is an order-preserving encoding of
// arbitrary String bytes, and '/' sorts before every hex digit, so appending
// "/0" or "/1" keeps sumMap's deterministic order payload-first and type-bit
// second (String before Bytes for the same payload). This makes type identity
// part of mode's value domain and carries the selected winner's bit, rather
// than an aggregate-wide OR that could taint an unrelated winning String.
func statsExactModeWithSemanticBytesSQL(
	valuesSQL string,
	semanticBytesSQL string,
) statsExactModeWithSemanticBytesLowering {
	keysInput := "arrayMap((value, semantic_bytes) -> concat(hex(value), " +
		"CAST('/' AS String), if(semantic_bytes != 0, " +
		"CAST('1' AS String), CAST('0' AS String))), " +
		valuesSQL + ", " + semanticBytesSQL + ")"
	frequencies := "sumMap(" + keysInput +
		", arrayMap(_ -> toUInt64(1), " + valuesSQL + "))"
	keys := "tupleElement(" + frequencies + ", 1)"
	counts := "tupleElement(" + frequencies + ", 2)"
	winner := "arrayElementOrNull(" + keys +
		", indexOf(" + counts + ", arrayMax(" + counts + ")))"
	return statsExactModeWithSemanticBytesLowering{
		ValueSQL: "unhex(substring(" + winner + ", 1, length(" + winner +
			") - 2))",
		SemanticBytesSQL: "toUInt8(ifNull(endsWith(" + winner + ", '/1'), 0))",
	}
}

func statsDistributionPercentileIsValid(percentile uint8) bool {
	return percentile >= 1 && percentile <= 99
}

func statsDistributionPercentileLevelSQL(percentile uint8) string {
	if percentile == 100 {
		return "1"
	}
	if percentile%10 == 0 {
		return "0." + strconv.Itoa(int(percentile/10))
	}
	return fmt.Sprintf("0.%02d", percentile)
}
