package searchanalysis

import (
	"errors"
	"math"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

const (
	absoluteMaximumTimelineBuckets = uint32(10_000)
	// Timeline grouping uses ClickHouse DateTime64 nanosecond ticks. Keep the
	// fixed width representable as signed nanoseconds while all range selection
	// itself continues to avoid time.Duration overflow.
	maximumTimelineSpanSeconds = int64(9_223_372_036)
)

var ErrInvalidTimelineRequest = errors.New("invalid search timeline request")

type timelineGeometry struct {
	FirstBucket time.Time
	SpanSeconds int64
	BucketCount uint64
}

func selectTimelineGeometry(earliest, latest time.Time, maxBuckets uint32, preferredSeconds int64) (timelineGeometry, error) {
	earliest = earliest.Round(0).UTC()
	latest = latest.Round(0).UTC()
	if earliest.IsZero() || latest.IsZero() || !earliest.Before(latest) || maxBuckets == 0 ||
		maxBuckets > absoluteMaximumTimelineBuckets || preferredSeconds < 0 || preferredSeconds > maximumTimelineSpanSeconds ||
		!clickhouse.SupportsSearchTimeRange(earliest, latest) {
		return timelineGeometry{}, ErrInvalidTimelineRequest
	}
	// No epoch-aligned fixed interval can cover timestamps on both sides of
	// zero with one bucket: the boundary at the Unix epoch always splits them.
	if maxBuckets == 1 && earliest.Unix() < 0 && (latest.Unix() > 0 || latest.Unix() == 0 && latest.Nanosecond() > 0) {
		return timelineGeometry{}, ErrInvalidTimelineRequest
	}

	if preferredSeconds > 0 {
		preferred, err := geometryForSpan(earliest, latest, preferredSeconds)
		if err == nil && preferred.BucketCount <= uint64(maxBuckets) {
			return preferred, nil
		}
	}

	rangeSeconds, ok := timelineRangeSeconds(earliest, latest)
	if !ok {
		return timelineGeometry{}, ErrInvalidTimelineRequest
	}
	minimum := ceilPositive(rangeSeconds, int64(maxBuckets))
	if preferredSeconds >= minimum {
		if preferredSeconds == math.MaxInt64 {
			return timelineGeometry{}, ErrInvalidTimelineRequest
		}
		minimum = preferredSeconds + 1
	}
	candidate, ok := niceTimelineSpanAtLeast(minimum)
	for ok {
		geometry, err := geometryForSpan(earliest, latest, candidate)
		if err == nil && geometry.BucketCount <= uint64(maxBuckets) {
			return geometry, nil
		}
		if candidate == math.MaxInt64 {
			break
		}
		candidate, ok = niceTimelineSpanAtLeast(candidate + 1)
	}
	// The 1/2/5 progression ends at five billion seconds because its next
	// candidate exceeds the signed-nanosecond span ceiling. Try that exact hard
	// ceiling as the final deterministic width: it is needed to cover the
	// supported epoch-to-2262 range in one bucket and the full supported range
	// in two buckets.
	if minimum <= maximumTimelineSpanSeconds {
		geometry, err := geometryForSpan(earliest, latest, maximumTimelineSpanSeconds)
		if err == nil && geometry.BucketCount <= uint64(maxBuckets) {
			return geometry, nil
		}
	}
	return timelineGeometry{}, ErrInvalidTimelineRequest
}

func geometryForSpan(earliest, latest time.Time, spanSeconds int64) (timelineGeometry, error) {
	if spanSeconds <= 0 || spanSeconds > maximumTimelineSpanSeconds {
		return timelineGeometry{}, ErrInvalidTimelineRequest
	}
	firstSeconds := floorTimelineSeconds(earliest.Unix(), spanSeconds) * spanSeconds
	deltaSeconds := latest.Unix() - firstSeconds
	if deltaSeconds < 0 {
		return timelineGeometry{}, ErrInvalidTimelineRequest
	}
	bucketCount := uint64(deltaSeconds / spanSeconds)
	if deltaSeconds%spanSeconds != 0 || latest.Nanosecond() != 0 {
		bucketCount++
	}
	if bucketCount == 0 || bucketCount > uint64(absoluteMaximumTimelineBuckets) {
		return timelineGeometry{}, ErrInvalidTimelineRequest
	}
	return timelineGeometry{
		FirstBucket: time.Unix(firstSeconds, 0).UTC(),
		SpanSeconds: spanSeconds,
		BucketCount: bucketCount,
	}, nil
}

func timelineRangeSeconds(earliest, latest time.Time) (int64, bool) {
	seconds := latest.Unix() - earliest.Unix()
	if latest.Nanosecond() > earliest.Nanosecond() {
		seconds++
	}
	return seconds, seconds > 0
}

func floorTimelineSeconds(value, divisor int64) int64 {
	quotient := value / divisor
	if value%divisor < 0 {
		quotient--
	}
	return quotient
}

func ceilPositive(value, divisor int64) int64 {
	return value/divisor + boolInt64(value%divisor != 0)
}

func boolInt64(value bool) int64 {
	if value {
		return 1
	}
	return 0
}

func niceTimelineSpanAtLeast(minimum int64) (int64, bool) {
	if minimum <= 0 {
		minimum = 1
	}
	for power := int64(1); power <= maximumTimelineSpanSeconds; {
		for _, multiplier := range [...]int64{1, 2, 5} {
			if power > maximumTimelineSpanSeconds/multiplier {
				continue
			}
			candidate := power * multiplier
			if candidate >= minimum {
				return candidate, true
			}
		}
		if power > maximumTimelineSpanSeconds/10 {
			break
		}
		power *= 10
	}
	return 0, false
}
