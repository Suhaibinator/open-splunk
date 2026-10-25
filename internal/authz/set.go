package authz

import "math/bits"

// CapabilitySet is an immutable set of capabilities. It is a value type:
// copies are independent and no method mutates the receiver. The zero value
// is the empty set.
type CapabilitySet struct {
	bits uint32
}

// NewCapabilitySet returns the set of the given capabilities. It reports
// false, and returns the empty set, if any capability is not canonical.
func NewCapabilitySet(capabilities ...Capability) (CapabilitySet, bool) {
	var result CapabilitySet
	for _, capability := range capabilities {
		bit, ok := capability.bit()
		if !ok {
			return CapabilitySet{}, false
		}
		result.bits |= bit
	}
	return result, true
}

// with returns a copy of set extended by canonical capabilities. It panics
// on a non-canonical capability, which only package-internal tables pass.
func (set CapabilitySet) with(capabilities ...Capability) CapabilitySet {
	for _, capability := range capabilities {
		bit, ok := capability.bit()
		if !ok {
			panic("authz: non-canonical capability " + string(capability))
		}
		set.bits |= bit
	}
	return set
}

// Has reports whether set contains capability. A non-canonical capability is
// never held.
func (set CapabilitySet) Has(capability Capability) bool {
	bit, ok := capability.bit()
	return ok && set.bits&bit != 0
}

// Len returns the number of capabilities in set.
func (set CapabilitySet) Len() int {
	return bits.OnesCount32(set.bits)
}

// Empty reports whether set holds no capability.
func (set CapabilitySet) Empty() bool {
	return set.bits == 0
}

// IsSubsetOf reports whether every capability in set is also in other.
func (set CapabilitySet) IsSubsetOf(other CapabilitySet) bool {
	return set.bits&^other.bits == 0
}

// Capabilities returns the members of set in canonical order as a fresh
// slice.
func (set CapabilitySet) Capabilities() []Capability {
	result := make([]Capability, 0, set.Len())
	for index, capability := range allCapabilities {
		if set.bits&(1<<uint(index)) != 0 {
			result = append(result, capability)
		}
	}
	return result
}

// Strings returns the canonical ids of set in canonical order, ready for the
// wire.
func (set CapabilitySet) Strings() []string {
	capabilities := set.Capabilities()
	result := make([]string, len(capabilities))
	for index, capability := range capabilities {
		result[index] = string(capability)
	}
	return result
}
