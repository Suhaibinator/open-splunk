package common_util

// Ternary returns ifTrue when condition is true and ifFalse otherwise.
func Ternary[T any](condition bool, ifTrue, ifFalse T) T {
	if condition {
		return ifTrue
	}
	return ifFalse
}
