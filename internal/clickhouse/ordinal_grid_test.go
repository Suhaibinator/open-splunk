package clickhouse

import (
	"math"
	"testing"
)

func TestOrdinalGridFirstBucketNumber(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		first       int64
		span        int64
		bucketCount uint64
		want        int64
		ok          bool
	}{
		{name: "positive", first: 600, span: 300, bucketCount: 3, want: 2, ok: true},
		{name: "pre epoch", first: -300, span: 300, bucketCount: 2, want: -1, ok: true},
		{name: "unaligned", first: -1, span: 300, bucketCount: 1},
		{name: "zero span", first: 0, bucketCount: 1},
		{name: "zero buckets", first: 0, span: 1},
		{name: "offset exceeds signed range", first: 0, span: 1, bucketCount: uint64(math.MaxInt64) + 2},
		{name: "signed bucket overflow", first: math.MaxInt64, span: 1, bucketCount: 2},
		{
			name:        "unix grid end overflow",
			first:       math.MaxInt64 - math.MaxInt64%300,
			span:        300,
			bucketCount: 2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := ordinalGridFirstBucketNumber(test.first, test.span, test.bucketCount)
			if got != test.want || ok != test.ok {
				t.Fatalf("ordinalGridFirstBucketNumber(%d, %d, %d) = %d, %v; want %d, %v",
					test.first, test.span, test.bucketCount, got, ok, test.want, test.ok)
			}
		})
	}
}
