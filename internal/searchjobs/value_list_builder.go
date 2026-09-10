package searchjobs

import "errors"

// ListValueFromItems builds a list into private storage. Value children are
// immutable, so their private backing can be shared without recursively
// copying lists that an adapter just constructed. No caller-owned slice is
// retained or exposed.
func ListValueFromItems(length int, item func(int) (Value, error)) (Value, error) {
	if length < 0 || item == nil {
		return Value{}, errors.New("search result list builder is invalid")
	}
	values := make([]Value, length)
	for index := range values {
		value, err := item(index)
		if err != nil {
			return Value{}, err
		}
		if err := validateValue(value, 1); err != nil {
			return Value{}, err
		}
		values[index] = value
	}
	candidate := Value{kind: ValueKindList, listValue: values}
	return candidate, nil
}
