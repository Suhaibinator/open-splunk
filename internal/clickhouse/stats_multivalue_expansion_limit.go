package clickhouse

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	// MaximumStatsMultivalueByCombinationsPerEvent bounds the Cartesian row
	// amplification produced by one source event's multivalue stats BY fields.
	// This is an Open Splunk resource ceiling, not a claim about Splunk's own
	// implementation limit. The guard runs against the effective arrays after
	// null-member filtering and optional dedup_splitvals processing.
	MaximumStatsMultivalueByCombinationsPerEvent uint64 = 10_000

	// StatsMultivalueByExpansionLimitMarker lets the executor classify the
	// deliberate pre-ARRAY-JOIN guard without retaining generated SQL or field
	// contents in the public failure.
	StatsMultivalueByExpansionLimitMarker = "open-splunk: stats multivalue BY expansion exceeds the per-event limit"
)

func statsMultivalueByExpansionMaximumSQL() string {
	return "toUInt64(" + strconv.FormatUint(
		MaximumStatsMultivalueByCombinationsPerEvent,
		10,
	) + ")"
}

// statsMultivalueByExpansionProductSQL returns the row-local, overflow-safe
// Cartesian cardinality of the already-materialized arrays that staged stats
// BY ARRAY JOINs will consume. The fold saturates at MAX+1 instead of
// multiplying beyond the cap. Zero dominates even a prior saturation sentinel
// because an empty or missing later BY dimension makes the event produce no
// Cartesian rows at all.
func statsMultivalueByExpansionProductSQL(
	expansions []compiledStatsGroupExpansion,
) (string, error) {
	if len(expansions) == 0 {
		return "", fmt.Errorf(
			"compile ClickHouse stats multivalue BY expansion guard: no expansions",
		)
	}

	cardinalities := make([]string, len(expansions))
	for index, expansion := range expansions {
		if expansion.valuesAlias == "" || expansion.valueAlias == "" {
			return "", fmt.Errorf(
				"compile ClickHouse stats multivalue BY expansion guard: expansion %d is invalid",
				index,
			)
		}
		cardinalities[index] = "toUInt64(length(" + expansion.valuesAlias + "))"
	}

	maximum := statsMultivalueByExpansionMaximumSQL()
	sentinel := "toUInt64(" + strconv.FormatUint(
		MaximumStatsMultivalueByCombinationsPerEvent+1,
		10,
	) + ")"
	return "arrayFold((product, cardinality) -> multiIf(" +
		"product = toUInt64(0) OR cardinality = toUInt64(0), toUInt64(0), " +
		"product > " + maximum + " OR cardinality > intDiv(" + maximum +
		", greatest(product, toUInt64(1))), " + sentinel + ", " +
		"product * cardinality), [" + strings.Join(cardinalities, ", ") +
		"], toUInt64(1))", nil
}

func statsMultivalueByExpansionGuardSQL(
	productAlias, anyOverLimitAlias string,
) (string, error) {
	if productAlias == "" || anyOverLimitAlias == "" {
		return "", fmt.Errorf(
			"compile ClickHouse stats multivalue BY expansion guard: aliases are invalid",
		)
	}
	// Reject an empty final domain in this same pre-expansion relation. Letting
	// it reach staged joins could still amplify earlier dimensions before a
	// later empty dimension removes the row. The windowed violation flag poisons
	// every retained source row when any event exceeds the cap, so downstream
	// limits cannot hide a violating event.
	return "if(" + anyOverLimitAlias + " != 0, throwIf(toUInt8(1), '" +
		StatsMultivalueByExpansionLimitMarker + "') = 0, " + productAlias +
		" != toUInt64(0))", nil
}
