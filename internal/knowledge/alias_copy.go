package knowledge

import "math"

const (
	// MaximumAliasCopyRuntimeEventBytes bounds the aggregate payload copied by all
	// winning alias destinations for one event.
	MaximumAliasCopyRuntimeEventBytes uint64 = 4 << 20
	// MaximumAliasCopyRuntimeQueryUnits bounds cumulative alias-copy work across one
	// query.
	MaximumAliasCopyRuntimeQueryUnits uint64 = 1 << 30
	// AliasCopyWorkUnits is charged once for each winning alias write.
	AliasCopyWorkUnits uint64 = 1
	// AliasCopyDescendantWorkUnits is charged for each copied descendant.
	AliasCopyDescendantWorkUnits uint64 = 1

	aliasCopyMetadataVersionBytes uint64 = 1
)

// AliasCopyWrite describes the byte and descendant counts for one already-
// resolved winning alias write. Losing candidates and no-write rows must not
// be supplied to the accounting oracle.
type AliasCopyWrite struct {
	ValueBytes        uint64
	RelativeNameBytes uint64
	RelativeTypeBytes uint64
	DescendantCount   uint64
}

// AliasCopyCharge is the deterministic payload and work charged for one write
// or for the bounded aggregation of all winning destinations in one row.
type AliasCopyCharge struct {
	PayloadBytes uint64
	WorkUnits    uint64
}

// CheckedAliasCopyCharge computes the exact charge for one winning write.
// Payload is V+N+T+1 metadata-version byte. Work is payload plus one unit per
// descendant and one unit for the copy itself. The boolean is false, with a
// zero charge, if any uint64 addition or multiplication would overflow.
func CheckedAliasCopyCharge(write AliasCopyWrite) (AliasCopyCharge, bool) {
	payload, ok := checkedAliasCopyAdd(write.ValueBytes, write.RelativeNameBytes)
	if !ok {
		return AliasCopyCharge{}, false
	}
	payload, ok = checkedAliasCopyAdd(payload, write.RelativeTypeBytes)
	if !ok {
		return AliasCopyCharge{}, false
	}
	payload, ok = checkedAliasCopyAdd(payload, aliasCopyMetadataVersionBytes)
	if !ok || write.DescendantCount > math.MaxUint64/AliasCopyDescendantWorkUnits {
		return AliasCopyCharge{}, false
	}
	descendantWork := write.DescendantCount * AliasCopyDescendantWorkUnits
	work, ok := checkedAliasCopyAdd(payload, descendantWork)
	if !ok {
		return AliasCopyCharge{}, false
	}
	work, ok = checkedAliasCopyAdd(work, AliasCopyWorkUnits)
	if !ok {
		return AliasCopyCharge{}, false
	}
	return AliasCopyCharge{PayloadBytes: payload, WorkUnits: work}, true
}

// SaturatingAliasCopyRow aggregates one checked charge per winning destination
// in a row. Each dimension saturates independently at its hard ceiling plus
// one, preserving an exact, overflow-safe sentinel for a rejected row/query.
func SaturatingAliasCopyRow(charges []AliasCopyCharge) AliasCopyCharge {
	result := AliasCopyCharge{}
	for _, charge := range charges {
		result.PayloadBytes = saturatingAliasCopyAdd(
			result.PayloadBytes,
			charge.PayloadBytes,
			MaximumAliasCopyRuntimeEventBytes+1,
		)
		result.WorkUnits = saturatingAliasCopyAdd(
			result.WorkUnits,
			charge.WorkUnits,
			MaximumAliasCopyRuntimeQueryUnits+1,
		)
	}
	return result
}

func checkedAliasCopyAdd(left, right uint64) (uint64, bool) {
	if right > math.MaxUint64-left {
		return 0, false
	}
	return left + right, true
}

func saturatingAliasCopyAdd(current, increment, saturation uint64) uint64 {
	if current >= saturation || increment >= saturation ||
		increment > saturation-current {
		return saturation
	}
	return current + increment
}
