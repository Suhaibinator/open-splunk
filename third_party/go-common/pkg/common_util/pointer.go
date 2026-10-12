package common_util

import "reflect"

// IsNil reports whether value is a nil interface, or an interface holding a nil
// pointer, map, slice, channel or function. A plain `value == nil` misses the
// second case, which is how a typed-nil configuration or policy view slips past
// a required-argument check and only fails later at the first dereference.
func IsNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

// Deref returns the value p points at, or the zero value of T when p is nil.
// It is the read half of the nullable-column and optional-proto-field idiom;
// use Go's new(v) builtin for the write half.
func Deref[T any](p *T) T {
	var zero T
	return DerefOr(p, zero)
}

// DerefOr returns the value p points at, or fallback when p is nil. Prefer it
// over Deref wherever "unset" and "explicitly zero" must resolve differently.
func DerefOr[T any](p *T, fallback T) T {
	if p == nil {
		return fallback
	}
	return *p
}
