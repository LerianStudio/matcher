// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"math"
	"reflect"
)

// floatEqualityTolerance is the maximum absolute difference for two floating-point
// numbers to be considered equivalent in config value comparisons.
const floatEqualityTolerance = 1e-9

// valuesEquivalent compares two any-typed values for semantic equality,
// handling numeric type coercion (int vs float64 from JSON deserialization)
// and falling back to reflect.DeepEqual for all other types.
func valuesEquivalent(left, right any) bool {
	leftNumber, leftIsNumber := toFloat64(left)
	rightNumber, rightIsNumber := toFloat64(right)

	if leftIsNumber && rightIsNumber {
		return math.Abs(leftNumber-rightNumber) < floatEqualityTolerance
	}

	return reflect.DeepEqual(left, right)
}

// toFloat64 converts any numeric value to float64 using reflect.Kind grouping.
func toFloat64(value any) (float64, bool) {
	if value == nil {
		return 0, false
	}

	reflected := reflect.ValueOf(value)

	switch reflected.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(reflected.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(reflected.Uint()), true
	case reflect.Float32, reflect.Float64:
		return reflected.Float(), true
	default:
		return 0, false
	}
}
