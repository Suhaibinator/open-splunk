package common_util

import "maps"

// CloneMapOrNil returns a shallow copy of input, or nil when input is empty.
// It differs from maps.Clone by collapsing an empty non-nil map to nil, which
// is what the proto and persistence paths want: an absent optional map and an
// explicitly empty one must both serialize as unset.
func CloneMapOrNil[K comparable, V any](input map[K]V) map[K]V {
	if len(input) == 0 {
		return nil
	}
	return maps.Clone(input)
}
