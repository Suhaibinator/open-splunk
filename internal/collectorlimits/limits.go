// Package collectorlimits defines resource and wire-snapshot bounds shared by
// collector clients and the server-side fleet registry.
package collectorlimits

const (
	// A validated build identity can contain a 64-byte semantic version, a
	// 64-byte source revision, and the three-byte " (...)" wrapper.
	MaximumCollectorVersionBytes          = 131
	MaximumHostnameBytes                  = 255
	MaximumOperatingSystemBytes           = 128
	MaximumArchitectureBytes              = 128
	MaximumCapabilities                   = 64
	MaximumAuthorizedIndexes              = 256
	MaximumInputs                         = 256
	MaximumSourceBytes                    = 4096
	MaximumSourcetypeBytes                = 255
	MaximumInputStatusMessageBytes        = 8 << 10
	MaximumSnapshotBytes                  = 1 << 20
	MaximumCheckpointGuardBytes           = 1 << 20
	MaximumFleetCounter            uint64 = 1<<63 - 1
)

// ClampFleetCounter keeps unsigned runtime counters representable in the
// fleet store's signed 64-bit columns and validation boundary.
func ClampFleetCounter(value uint64) uint64 {
	return min(value, MaximumFleetCounter)
}

// SaturatingAddFleetCounters sums counters without allowing an intermediate
// uint64 wrap to turn a monotonic fleet counter into a small value.
func SaturatingAddFleetCounters(values ...uint64) uint64 {
	var total uint64
	for _, value := range values {
		value = ClampFleetCounter(value)
		if value > MaximumFleetCounter-total {
			return MaximumFleetCounter
		}
		total += value
	}
	return total
}
