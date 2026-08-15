// Package nilcheck recognizes both nil interfaces and interfaces containing
// typed nil values.
package nilcheck

import "reflect"

// IsNil reports whether value is nil or contains a nil value of a nil-capable
// dynamic type.
func IsNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
