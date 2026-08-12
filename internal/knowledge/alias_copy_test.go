package knowledge

import (
	"math"
	"testing"
)

func TestAliasCopyAccountingConstantsAreFrozen(t *testing.T) {
	t.Parallel()

	if MaximumAliasCopyRuntimeEventBytes != 4<<20 ||
		MaximumAliasCopyRuntimeQueryUnits != 1<<30 ||
		AliasCopyWorkUnits != 1 ||
		AliasCopyDescendantWorkUnits != 1 {
		t.Fatal("alias-copy accounting constants changed")
	}
}

func TestAliasCopyCheckedChargeBoundariesAndOverflow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		write AliasCopyWrite
		want  AliasCopyCharge
		ok    bool
	}{
		{
			name: "zero-length value and metadata arrays",
			want: AliasCopyCharge{PayloadBytes: 1, WorkUnits: 2},
			ok:   true,
		},
		{
			name: "ordinary container",
			write: AliasCopyWrite{
				ValueBytes:        10,
				RelativeNameBytes: 3,
				RelativeTypeBytes: 2,
				DescendantCount:   4,
			},
			want: AliasCopyCharge{PayloadBytes: 16, WorkUnits: 21},
			ok:   true,
		},
		{
			name:  "exact event-byte ceiling",
			write: AliasCopyWrite{ValueBytes: MaximumAliasCopyRuntimeEventBytes - 1},
			want: AliasCopyCharge{
				PayloadBytes: MaximumAliasCopyRuntimeEventBytes,
				WorkUnits:    MaximumAliasCopyRuntimeEventBytes + 1,
			},
			ok: true,
		},
		{
			name: "exact query-work ceiling",
			write: AliasCopyWrite{
				DescendantCount: MaximumAliasCopyRuntimeQueryUnits - 2,
			},
			want: AliasCopyCharge{
				PayloadBytes: 1,
				WorkUnits:    MaximumAliasCopyRuntimeQueryUnits,
			},
			ok: true,
		},
		{
			name:  "largest exact charge",
			write: AliasCopyWrite{ValueBytes: math.MaxUint64 - 2},
			want: AliasCopyCharge{
				PayloadBytes: math.MaxUint64 - 1,
				WorkUnits:    math.MaxUint64,
			},
			ok: true,
		},
		{
			name:  "payload overflow",
			write: AliasCopyWrite{ValueBytes: math.MaxUint64},
		},
		{
			name: "metadata sum overflow",
			write: AliasCopyWrite{
				ValueBytes:        math.MaxUint64 - 1,
				RelativeNameBytes: 1,
			},
		},
		{
			name:  "copy work overflow",
			write: AliasCopyWrite{ValueBytes: math.MaxUint64 - 1},
		},
		{
			name: "descendant work overflow",
			write: AliasCopyWrite{
				DescendantCount: math.MaxUint64,
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, ok := CheckedAliasCopyCharge(test.write)
			if ok != test.ok || got != test.want {
				t.Fatalf(
					"CheckedAliasCopyCharge(%+v) = (%+v, %t), want (%+v, %t)",
					test.write,
					got,
					ok,
					test.want,
					test.ok,
				)
			}
		})
	}
}

func TestAliasCopyRowAggregationSaturatesAtCeilingPlusOne(t *testing.T) {
	t.Parallel()

	if got := SaturatingAliasCopyRow(nil); got != (AliasCopyCharge{}) {
		t.Fatalf("empty row charge = %+v", got)
	}
	exact := SaturatingAliasCopyRow([]AliasCopyCharge{{
		PayloadBytes: MaximumAliasCopyRuntimeEventBytes,
		WorkUnits:    MaximumAliasCopyRuntimeQueryUnits,
	}})
	if exact != (AliasCopyCharge{
		PayloadBytes: MaximumAliasCopyRuntimeEventBytes,
		WorkUnits:    MaximumAliasCopyRuntimeQueryUnits,
	}) {
		t.Fatalf("exact ceiling charge = %+v", exact)
	}

	saturated := SaturatingAliasCopyRow([]AliasCopyCharge{
		{PayloadBytes: MaximumAliasCopyRuntimeEventBytes, WorkUnits: MaximumAliasCopyRuntimeQueryUnits},
		{PayloadBytes: 1, WorkUnits: 1},
		{PayloadBytes: math.MaxUint64, WorkUnits: math.MaxUint64},
	})
	if saturated != (AliasCopyCharge{
		PayloadBytes: MaximumAliasCopyRuntimeEventBytes + 1,
		WorkUnits:    MaximumAliasCopyRuntimeQueryUnits + 1,
	}) {
		t.Fatalf("saturated row charge = %+v", saturated)
	}
}

func TestAliasCopyRowAggregationSumsWinningDestinations(t *testing.T) {
	t.Parallel()

	writes := []AliasCopyWrite{
		{ValueBytes: 10, RelativeNameBytes: 2, RelativeTypeBytes: 1, DescendantCount: 3},
		{ValueBytes: 20, RelativeNameBytes: 4, RelativeTypeBytes: 2, DescendantCount: 5},
		{},
	}
	charges := make([]AliasCopyCharge, len(writes))
	for index, write := range writes {
		charge, ok := CheckedAliasCopyCharge(write)
		if !ok {
			t.Fatalf("destination %d charge overflowed", index)
		}
		charges[index] = charge
	}
	if got, want := SaturatingAliasCopyRow(charges), (AliasCopyCharge{
		PayloadBytes: 42,
		WorkUnits:    53,
	}); got != want {
		t.Fatalf("multi-destination row charge = %+v, want %+v", got, want)
	}
}
