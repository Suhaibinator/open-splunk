package clickhouse

import (
	"math"
	"slices"
)

// NearbyEventOutput binds the four unchanged physical event fields needed to
// prepare a nearby-event search to their final public result ordinals. A
// caller can trust this descriptor only through CompiledQuery.NearbyEventOutput.
type NearbyEventOutput struct {
	TimeIndex   uint16
	IndexIndex  uint16
	HostIndex   uint16
	SourceIndex uint16
}

type originalEventField uint8

const (
	originalEventFieldNone originalEventField = iota
	originalEventFieldTime
	originalEventFieldIndex
	originalEventFieldHost
	originalEventFieldSource
)

var nearbyEventFieldNames = [...]string{"_time", "index", "host", "source"}

func originalEventFieldForName(name string) originalEventField {
	switch name {
	case "_time":
		return originalEventFieldTime
	case "index":
		return originalEventFieldIndex
	case "host":
		return originalEventFieldHost
	case "source":
		return originalEventFieldSource
	default:
		return originalEventFieldNone
	}
}

func nearbyEventOutput(state compileState, outputFields []string) *NearbyEventOutput {
	if !state.eventRows || len(outputFields) > math.MaxUint16 {
		return nil
	}
	indexes := [len(nearbyEventFieldNames)]int{-1, -1, -1, -1}
	for requiredIndex, requiredName := range nearbyEventFieldNames {
		outputIndex := slices.Index(outputFields, requiredName)
		field, ok := state.visible[requiredName]
		if outputIndex >= 0 && ok &&
			field.originalEventField == originalEventFieldForName(requiredName) {
			indexes[requiredIndex] = outputIndex
		}
	}
	if slices.Contains(indexes[:], -1) {
		return nil
	}
	return &NearbyEventOutput{
		TimeIndex:   uint16(indexes[0]),
		IndexIndex:  uint16(indexes[1]),
		HostIndex:   uint16(indexes[2]),
		SourceIndex: uint16(indexes[3]),
	}
}

func validNearbyEventOutput(compiled CompiledQuery) bool {
	if compiled.NearbyEvent == nil {
		return true
	}
	indexes := [...]uint16{
		compiled.NearbyEvent.TimeIndex,
		compiled.NearbyEvent.IndexIndex,
		compiled.NearbyEvent.HostIndex,
		compiled.NearbyEvent.SourceIndex,
	}
	seen := make(map[uint16]struct{}, len(indexes))
	for index, outputIndex := range indexes {
		if int(outputIndex) >= len(compiled.OutputFields) ||
			compiled.OutputFields[outputIndex] != nearbyEventFieldNames[index] {
			return false
		}
		if _, duplicate := seen[outputIndex]; duplicate {
			return false
		}
		seen[outputIndex] = struct{}{}
	}
	return true
}

// NearbyEventOutput returns detached compiler-authenticated source-field
// ordinals. False means the final result is transformed, required fields were
// removed or overwritten, or the compiled query was changed after sealing.
func (compiled CompiledQuery) NearbyEventOutput() (NearbyEventOutput, bool) {
	if compiled.NearbyEvent == nil || !compiled.HasValidExecutionSeal() {
		return NearbyEventOutput{}, false
	}
	return *compiled.NearbyEvent, true
}
