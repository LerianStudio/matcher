// Copyright 2025 Lerian Studio.

package domain

import "reflect"

// IsNilValue reports whether value is nil or a typed-nil value stored behind an interface.
func IsNilValue(value any) bool {
	if value == nil {
		return true
	}

	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return true
	}

	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}
