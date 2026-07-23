package searchanalysis

import (
	"errors"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestSelectTimelineGeometryUsesEpochAlignedMinimalCover(t *testing.T) {
	tests := []struct {
		name             string
		earliest         time.Time
		latest           time.Time
		maxBuckets       uint32
		preferredSeconds int64
		wantFirst        time.Time
		wantSpan         int64
		wantBuckets      uint64
	}{
		{
			name:     "aligned",
			earliest: time.Date(2026, 7, 21, 8, 0, 0, 0, time.UTC), latest: time.Date(2026, 7, 21, 8, 5, 0, 0, time.UTC),
			maxBuckets: 10, preferredSeconds: 60,
			wantFirst: time.Date(2026, 7, 21, 8, 0, 0, 0, time.UTC), wantSpan: 60, wantBuckets: 5,
		},
		{
			name:     "fractional clipped edges",
			earliest: time.Date(2026, 7, 21, 8, 0, 30, 123, time.UTC), latest: time.Date(2026, 7, 21, 8, 2, 0, 1, time.UTC),
			maxBuckets: 10, preferredSeconds: 60,
			wantFirst: time.Date(2026, 7, 21, 8, 0, 0, 0, time.UTC), wantSpan: 60, wantBuckets: 3,
		},
		{
			name:     "pre epoch mathematical floor",
			earliest: time.Unix(-30, 0).UTC(), latest: time.Unix(30, 0).UTC(),
			maxBuckets: 2, preferredSeconds: 60,
			wantFirst: time.Unix(-60, 0).UTC(), wantSpan: 60, wantBuckets: 2,
		},
		{
			name:     "exact preferred width",
			earliest: time.Unix(34, 0).UTC(), latest: time.Unix(101, 0).UTC(),
			maxBuckets: 8, preferredSeconds: 17,
			wantFirst: time.Unix(34, 0).UTC(), wantSpan: 17, wantBuckets: 4,
		},
		{
			name:     "alignment widens automatic width",
			earliest: time.Unix(1, 0).UTC(), latest: time.Unix(101, 0).UTC(),
			maxBuckets: 1,
			wantFirst:  time.Unix(0, 0).UTC(), wantSpan: 200, wantBuckets: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			geometry, err := selectTimelineGeometry(test.earliest, test.latest, test.maxBuckets, test.preferredSeconds)
			if err != nil {
				t.Fatalf("selectTimelineGeometry() error = %v", err)
			}
			if !geometry.FirstBucket.Equal(test.wantFirst) || geometry.SpanSeconds != test.wantSpan || geometry.BucketCount != test.wantBuckets {
				t.Fatalf("geometry = %+v, want first=%v span=%d count=%d", geometry, test.wantFirst, test.wantSpan, test.wantBuckets)
			}
			assertMinimalCover(t, geometry, test.earliest, test.latest)
		})
	}
}

func TestSelectTimelineGeometryHandlesFullSupportedRangeWithoutDurationOverflow(t *testing.T) {
	earliest := clickhouse.MinimumSearchTime().Add(time.Nanosecond)
	latest := clickhouse.MaximumSearchTime()
	geometry, err := selectTimelineGeometry(earliest, latest, 100, 0)
	if err != nil {
		t.Fatalf("selectTimelineGeometry() error = %v", err)
	}
	if geometry.BucketCount == 0 || geometry.BucketCount > 100 || geometry.SpanSeconds <= 0 {
		t.Fatalf("geometry = %+v", geometry)
	}
	if !geometry.FirstBucket.Before(clickhouse.MinimumSearchTime()) {
		t.Fatalf("full-range geometry did not exercise a pre-1900 aligned ordinal: %+v", geometry)
	}
	assertMinimalCover(t, geometry, earliest, latest)
}

func TestSelectTimelineGeometryUsesHardSpanCeilingAfterNiceWidths(t *testing.T) {
	t.Parallel()

	earliest := time.Unix(0, 0).UTC()
	latest := clickhouse.MaximumSearchTime()
	geometry, err := selectTimelineGeometry(earliest, latest, 1, 0)
	if err != nil {
		t.Fatalf("selectTimelineGeometry() error = %v", err)
	}
	if !geometry.FirstBucket.Equal(earliest) || geometry.SpanSeconds != maximumTimelineSpanSeconds || geometry.BucketCount != 1 {
		t.Fatalf("geometry = %+v, want epoch origin, span %d, one bucket", geometry, maximumTimelineSpanSeconds)
	}
	assertMinimalCover(t, geometry, earliest, latest)
}

func TestSelectTimelineGeometryRejectsInvalidOrImpossibleRequests(t *testing.T) {
	validEarliest := time.Unix(1, 0).UTC()
	validLatest := time.Unix(2, 0).UTC()
	tests := []struct {
		name       string
		earliest   time.Time
		latest     time.Time
		maxBuckets uint32
		preferred  int64
	}{
		{name: "empty range", earliest: validEarliest, latest: validEarliest, maxBuckets: 1},
		{name: "zero buckets", earliest: validEarliest, latest: validLatest},
		{name: "above absolute buckets", earliest: validEarliest, latest: validLatest, maxBuckets: absoluteMaximumTimelineBuckets + 1},
		{name: "negative preferred", earliest: validEarliest, latest: validLatest, maxBuckets: 1, preferred: -1},
		{name: "excessive preferred", earliest: validEarliest, latest: validLatest, maxBuckets: 1, preferred: maximumTimelineSpanSeconds + 1},
		{name: "one bucket crossing epoch", earliest: time.Unix(-1, 0).UTC(), latest: time.Unix(1, 0).UTC(), maxBuckets: 1},
		{name: "before DateTime64 minimum", earliest: clickhouse.MinimumSearchTime().Add(-time.Nanosecond), latest: validLatest, maxBuckets: 10},
		{name: "after DateTime64 maximum", earliest: validEarliest, latest: clickhouse.MaximumSearchTime().Add(time.Nanosecond), maxBuckets: 10},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			geometry, err := selectTimelineGeometry(test.earliest, test.latest, test.maxBuckets, test.preferred)
			if !errors.Is(err, ErrInvalidTimelineRequest) {
				t.Fatalf("selectTimelineGeometry() = (%+v, %v), want ErrInvalidTimelineRequest", geometry, err)
			}
		})
	}
}

func assertMinimalCover(t *testing.T, geometry timelineGeometry, earliest, latest time.Time) {
	t.Helper()
	first := geometry.FirstBucket
	second := time.Unix(first.Unix()+geometry.SpanSeconds, 0).UTC()
	last := time.Unix(first.Unix()+int64(geometry.BucketCount-1)*geometry.SpanSeconds, 0).UTC()
	end := time.Unix(last.Unix()+geometry.SpanSeconds, 0).UTC()
	if first.After(earliest) || !earliest.Before(second) || !last.Before(latest) || end.Before(latest) {
		t.Fatalf("geometry %+v is not the minimal cover of [%v,%v)", geometry, earliest, latest)
	}
}
