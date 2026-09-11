package clickhouse

import (
	"bytes"
	"crypto/sha256"
	"math"
	"slices"
	"testing"
)

func TestCalendarInt64ArgumentFastPathMatchesCanonicalEncoding(t *testing.T) {
	for _, values := range [][]int64{nil, {}, {math.MinInt64, -1, 0, 1, math.MaxInt64}, make([]int64, 10000)} {
		want := sha256.New()
		writeTokenPart(want, "")
		writeTokenPart(want, "[]int64")
		writeBool(want, values == nil)
		writeUint64(want, uint64(len(values)))
		for _, value := range values {
			if !writeCompiledArgument(want, value, 1) {
				t.Fatal("scalar canonical encoding failed")
			}
		}
		got := sha256.New()
		if !writeCompiledArgument(got, values, 0) || !bytes.Equal(got.Sum(nil), want.Sum(nil)) {
			t.Fatalf("canonical encoding differs for %d values", len(values))
		}
		cloned, ok := cloneCompiledArgument(values)
		if !ok {
			t.Fatal("clone failed")
		}
		clone := cloned.([]int64)
		if (clone == nil) != (values == nil) || !slices.Equal(clone, values) {
			t.Fatalf("clone changed nil or values: %v", clone)
		}
		if len(clone) > 0 {
			old := values[0]
			clone[0]++
			if values[0] != old {
				t.Fatal("clone aliases original")
			}
		}
	}
}
