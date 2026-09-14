package clickhouse

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

// lowerStaticTimechartRelation turns the private fixed transport into a typed
// relation. A whole-result barrier consumes its validation column even when a
// downstream head or false predicate empties the ordinary branch.
func lowerStaticTimechartRelation(compiled CompiledQuery, previous compileState, operator *plan.Timechart, stage int) (compiledRelation, compileState, []any) {
	q := quoteIdentifier
	name := q(fmt.Sprintf("__os_timechart_relation_%d", stage))
	timeColumn := q("_time")
	valueColumn := q(operator.Measure.Output)
	ordinal := q(TimechartOrdinalColumn)
	physicalValue := q(TimechartCountColumn)
	kind := fieldKindNumber
	numberType := "UInt64"
	invalid := "toUInt8(0)"
	present := "sum(" + physicalValue + ") OVER () > 0"
	if compiled.Timechart.Mode == TimechartModeFixedValue {
		physicalValue = q(TimechartValueColumn)
		numberType = "Float64"
		// Sum and average may legitimately overflow to IEEE non-finite
		// results. Match the terminal decoder's finite-only percentile rule.
		if compiled.Timechart.ValueKind == TimechartValueKindPercentile {
			invalid = "toUInt8(NOT isFinite(ifNull(" + physicalValue + ", 0)))"
		}
	}
	if compiled.Timechart.Mode != TimechartModeFixedCount {
		present = q(TimechartInputPresentColumn) + " != 0"
	}
	bucket := q(TimechartBucketColumn)
	if !compiled.Timechart.Calendar && !compiled.Timechart.ExactGrid {
		bucket = "fromUnixTimestamp64Nano(toInt64(toInt128(" + strconv.FormatInt(compiled.Timechart.FirstBucket.UnixNano(), 10) + ") + toInt128(" + ordinal + ") * toInt128(" + strconv.FormatInt(int64(compiled.Timechart.Span), 10) + ")), 'UTC')"
	}
	ends := make([]int64, len(compiled.Timechart.Boundaries)-1)
	for i := range ends {
		ends[i] = compiled.Timechart.Boundaries[i+1].UnixNano()
	}
	endColumn := q(fmt.Sprintf("__os_timechart_end_%d", stage))
	endSQL := "fromUnixTimestamp64Nano(arrayElement(?, " + ordinal + " + 1), 'UTC')"
	if compiled.Timechart.ExactGrid && !compiled.Timechart.Continuous {
		present = "(" + present + ") AND " + q(TimechartBucketPresentColumn) + " != 0"
	}
	if compiled.Timechart.ExactGrid && !compiled.Timechart.IncludePartial {
		present = "(" + present + ") AND toUnixTimestamp64Nano(" + bucket + ") >= " + strconv.FormatInt(compiled.Timechart.SearchEarliest.UnixNano(), 10) + " AND " + endColumn + " <= fromUnixTimestamp64Nano(" + strconv.FormatInt(compiled.Timechart.SearchLatest.UnixNano(), 10) + ")"
	}
	invalidColumn := q(fmt.Sprintf("__os_timechart_invalid_%d", stage))
	presentColumn := q(fmt.Sprintf("__os_timechart_present_%d", stage))
	physical := strings.TrimSuffix(compiled.SQL, materializedCTESettingsSQL)
	workProjection := ""
	if compiled.timechartWorkReceipt {
		workProjection = ", " + q(TimechartWorkRowsColumn)
	}
	sql := "SELECT " + endSQL + " AS " + endColumn + ", " + bucket + " AS " + timeColumn + ", " + physicalValue + " AS " + valueColumn + ", " + invalid + " AS " + invalidColumn + ", toUInt8(" + present + ") AS " + presentColumn + workProjection + " FROM (" + physical + ")"
	state := compileState{visible: map[string]fieldState{
		"_time":                 {valueSQL: timeColumn, kind: fieldKindTime, canonicalTime: true, existsSQL: "1", timeBucketEndSQL: endColumn},
		operator.Measure.Output: {valueSQL: valueColumn, kind: kind, numberType: numberType, numericIntegral: numberType == "UInt64", numericSort: true, existsSQL: "1"},
	}, publicOrder: []string{"_time", operator.Measure.Output}, privateColumns: []string{endColumn}, context: previous.context, chronologicalBarriers: previous.chronologicalBarriers, order: []compiledSortKey{{valueSQL: timeColumn}}}
	if numberType == "Float64" {
		field := state.visible[operator.Measure.Output]
		field.existsSQL = "isNotNull(" + valueColumn + ")"
		state.visible[operator.Measure.Output] = field
	}
	if compiled.timechartWorkReceipt {
		workColumn := q(TimechartWorkRowsColumn)
		// The validated private scalar survives projection and reaggregation.
		state.mvExpandQueryRowsSQL = workColumn
		state.privateColumns = append(state.privateColumns, workColumn)
	}
	barrier := &pendingChronologicalBarrier{name: name, sql: sql, validationColumns: []string{invalidColumn}, fanout: 2, depth: compiled.relationalDepth + 1, ownerRange: operator.Range}
	state, args := bindChronologicalBarrier(state, barrier, append([]any{ends}, compiled.Args...))
	state.context.atomicResult = true
	state.context.requiresMaterializedValidationSettings = true
	relation := compiledRelation{sql: "SELECT " + timeColumn + ", " + valueColumn + ", " + endColumn + workProjection + " FROM " + name + " WHERE " + presentColumn + " != 0", depth: compiled.relationalDepth + 2, ownerRange: operator.Range}
	return relation, state, args
}
