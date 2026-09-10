package searchjobs

// ListValueFromItems builds a list into private storage. Value children are
// immutable, so their private backing can be shared without recursively
// copying lists that an adapter just constructed. No caller-owned slice is
// retained or exposed.
func ListValueFromItems(length int, item func(int) (Value, error)) (Value, error) {
	return buildListValue(nil, length, item)
}
