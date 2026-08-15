package clickhouse

import (
	"fmt"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// maximumStatsPartitionsMaxThreadsHint is deliberately independent of the
// documented partitions_limit=100. ClickHouse's max_threads setting applies
// to the complete query, not one stats reduction stage. Open Splunk therefore
// uses partitions only as a bounded whole-query execution hint and never lets
// it raise the executor's existing four-thread ceiling. This does not claim
// Splunk reduce-stage parity or any partition-dependent ordering semantics.
const maximumStatsPartitionsMaxThreadsHint uint8 = 4

// effectiveStatsOptions returns the bounded defaults for legacy/internal
// Aggregate plans that predate the stats-only options seam. A non-nil options
// value is still validated defensively by the compiler because callers can
// forge logical plans without passing through the SPL builder.
func effectiveStatsOptions(operator *plan.Aggregate) (plan.StatsOptions, error) {
	defaults := plan.StatsOptions{
		Partitions: spl.DefaultStatsPartitions,
		Delimiter:  spl.DefaultStatsDelimiter,
	}
	if operator == nil || operator.StatsOptions == nil {
		return defaults, nil
	}
	options := *operator.StatsOptions
	if options.Partitions < 1 || options.Partitions > spl.MaximumStatsPartitions {
		return plan.StatsOptions{}, fmt.Errorf(
			"compile ClickHouse aggregate: stats partitions must be between 1 and %d",
			spl.MaximumStatsPartitions,
		)
	}
	if !utf8.ValidString(options.Delimiter) {
		return plan.StatsOptions{}, fmt.Errorf(
			"compile ClickHouse aggregate: stats delimiter must be valid UTF-8",
		)
	}
	return options, nil
}

func effectiveStatsPartitionsMaxThreadsHint(operator *plan.Aggregate) (uint8, error) {
	options, err := effectiveStatsOptions(operator)
	if err != nil {
		return 0, err
	}
	hint := min(options.Partitions, maximumStatsPartitionsMaxThreadsHint)
	return hint, nil
}

// mergeStatsPartitionsMaxThreadsHint applies a deterministic policy when a
// query has more than one stats stage: the smallest stage hint caps the whole
// ClickHouse query. This is the only safe query-global approximation because
// it can never grant a stage more concurrency than that stage requested.
func mergeStatsPartitionsMaxThreadsHint(current, next uint8) uint8 {
	if current == 0 || next < current {
		return next
	}
	return current
}

// statsAllNumericInvalidSQL returns a row-local UInt8 violation marker for the
// documented allnum=true policy. Missing and null values do not participate;
// present empty strings and every other nonnumeric scalar/member do. The
// aggregate compiler takes max(marker) independently in each BY group, so one
// invalid value suppresses only that group's numerical result.
func statsAllNumericInvalidSQL(field fieldState) (string, []any) {
	existsSQL := field.existsSQL
	if existsSQL == "" {
		existsSQL = "1"
	}
	args := append([]any(nil), field.existsArgs...)
	if field.alwaysNull || field.kind == fieldKindInvalid {
		return "toUInt8(0)", args
	}

	switch field.kind {
	case fieldKindNumber, fieldKindTime:
		return "toUInt8(0)", args
	case fieldKindString:
		return "toUInt8((" + existsSQL + ") AND isNotNull(" + field.valueSQL +
			") AND isNull(" + finiteFloatOrNullSQL(field.valueSQL) + "))", args
	case fieldKindBool:
		return "toUInt8((" + existsSQL + ") AND isNotNull(" + field.valueSQL + "))", args
	case fieldKindStringArray:
		return "toUInt8((" + existsSQL + ") AND arrayExists(element -> isNull(" +
			finiteFloatOrNullSQL("element") + "), " + field.valueSQL + "))", args
	case fieldKindDynamic:
		typeSQL := dynamicTypeExpression(field)
		scalarInvalid := "(" + typeSQL + " != 'None' AND isNull(" +
			dynamicFiniteFloatOrNullSQL(field.valueSQL, typeSQL) + "))"
		arrayValues := "dynamicElement(" + field.valueSQL + ", 'Array(Dynamic)')"
		arrayInvalid := "arrayExists(element -> dynamicType(element) != 'None' AND isNull(" +
			dynamicFiniteFloatOrNullSQL("element", "dynamicType(element)") + "), " +
			arrayValues + ")"
		valueInvalid := "if(" + typeSQL + " = 'Array(Dynamic)', " + arrayInvalid +
			", " + scalarInvalid + ")"
		if field.descendantSQL != "" {
			valueInvalid = "((" + field.descendantSQL + ") OR " + valueInvalid + ")"
			args = append(args, field.descendantArgs...)
		}
		return "toUInt8((" + existsSQL + ") AND " + valueInvalid + ")", args
	default:
		return "toUInt8(0)", args
	}
}

func statsAllNumericResultSQL(valueSQL, invalidAlias string) string {
	if invalidAlias == "" {
		return valueSQL
	}
	return "if(max(" + invalidAlias + ") != 0, CAST(NULL AS Nullable(Float64)), " +
		valueSQL + ")"
}

func statsUsesAllNumericPolicy(function plan.AggregateFunction) bool {
	switch function {
	case plan.AggregateFunctionPercentile,
		plan.AggregateFunctionExactPercentile,
		plan.AggregateFunctionUpperPercentile,
		plan.AggregateFunctionMedian,
		plan.AggregateFunctionSum,
		plan.AggregateFunctionAverage,
		plan.AggregateFunctionRange,
		plan.AggregateFunctionSumSquares,
		plan.AggregateFunctionStandardDeviationSample,
		plan.AggregateFunctionStandardDeviationPopulation,
		plan.AggregateFunctionVarianceSample,
		plan.AggregateFunctionVariancePopulation,
		plan.AggregateFunctionRate:
		return true
	default:
		return false
	}
}
