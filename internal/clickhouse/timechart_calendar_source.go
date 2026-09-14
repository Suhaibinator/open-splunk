package clickhouse

import "strconv"

// Assign each source row to the predecessor in the sealed boundary grid once.
// Native ASOF builds a bounded ordered lookup; its work per event does not grow
// linearly with the grid size. The outer timechart source remains materialized
// and supplies both occupancy and aggregates from this single event scan.
func (spec timechartGridSpec) assignCalendarSource(relation compiledRelation, args []any, eventTime string) (compiledRelation, []any, string) {
	q := quoteIdentifier
	source := q("__os_tc_calendar_input")
	grid := q("__os_tc_calendar_lookup")
	key := q("__os_tc_calendar_key")
	matched := q("__os_tc_calendar_matched")
	joinKey := q("__os_tc_calendar_join")
	ticks := make([]int64, len(spec.boundaries)-1)
	for index := range ticks {
		ticks[index] = spec.boundaries[index].UnixNano()
	}
	relation.sql = "SELECT " + source + ".*, " + grid + "." + key + " AS " + key + ", " + grid + "." + matched + " AS " + matched + " FROM (SELECT *, toUInt8(1) AS " + joinKey + " FROM (" + relation.sql + ")) AS " + source +
		" ASOF LEFT JOIN (SELECT toUInt8(1) AS " + matched + ", arrayJoin(?) AS " + key + ") AS " + grid +
		" ON " + source + "." + joinKey + " = " + grid + "." + matched + " AND toUnixTimestamp64Nano(" + eventTime + ") >= " + grid + "." + key
	relation.depth = relationalNodeDepth(relationalNodeDepth(relation.depth), relationalNodeDepth())
	// Preserve fixedrange clipping for valid transformed timestamps outside
	// the visible grid. Keep them as distinct out-of-grid groups so upstream
	// aggregate validation still sees them; never alias an unmatched row to
	// ClickHouse's default zero key. The timestamp expression retains the
	// independent missing/null validation installed by compileTimechart.
	first := strconv.FormatInt(spec.firstBucket.UnixNano()-1, 10)
	end := strconv.FormatInt(spec.boundaries[len(spec.boundaries)-1].UnixNano(), 10)
	clock := "fromUnixTimestamp64Nano(multiIf(toUnixTimestamp64Nano(" + eventTime + ") >= " + end + ", toInt64(" + end + "), " + matched + " = 0, toInt64(" + first + "), " + key + "), 'UTC')"
	return relation, append(args, ticks), clock
}
