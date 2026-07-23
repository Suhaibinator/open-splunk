package clickhouse

import "math"

// epochFloorBucketNumberSQL returns a signed epoch-bucket expression for a
// DateTime64 tick count. ClickHouse integer division truncates toward zero, so
// negative non-multiples need one additional step toward negative infinity.
func epochFloorBucketNumberSQL(ticks string) string {
	return "intDiv(" + ticks + ", ?) - if(" + ticks + " < 0 AND " + ticks + " % ? != 0, 1, 0)"
}

// ordinalGridSQL returns the common zero-based ordinal grid used by timeline
// and timechart. The signed bucket number is private join state; only the
// bounded UInt64 ordinal crosses the ClickHouse executor boundary.
func ordinalGridSQL(ordinal, bucketNumber string) string {
	return "SELECT toUInt64(number) AS " + ordinal + ", toInt64(?) + toInt64(number) AS " + bucketNumber + " FROM numbers(?)"
}

func ordinalGridFirstBucketNumber(firstUnix, spanSeconds int64, bucketCount uint64) (int64, bool) {
	if spanSeconds <= 0 || bucketCount == 0 || firstUnix%spanSeconds != 0 || bucketCount-1 > math.MaxInt64 {
		return 0, false
	}
	if bucketCount > uint64(math.MaxInt64)/uint64(spanSeconds) {
		return 0, false
	}
	coveredSeconds := int64(bucketCount * uint64(spanSeconds))
	if firstUnix > math.MaxInt64-coveredSeconds {
		return 0, false
	}
	firstBucketNumber := firstUnix / spanSeconds
	lastBucketOffset := int64(bucketCount - 1)
	if firstBucketNumber > math.MaxInt64-lastBucketOffset {
		return 0, false
	}
	return firstBucketNumber, true
}

func appendOrdinalGridArgs(args []any, spanNanoseconds, firstBucketNumber int64, bucketCount uint64) []any {
	return append(args, spanNanoseconds, spanNanoseconds, firstBucketNumber, bucketCount)
}
