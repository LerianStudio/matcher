// Copyright 2025 Lerian Studio.

package registry

import (
	"fmt"
	"math"
	"reflect"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
)

// validateValue checks a value against a key definition's type and custom
// validator. It assumes the value is non-nil; callers must handle nil (reset)
// before calling this function.
func validateValue(def domain.KeyDef, value any) error {
	if err := checkValueType(value, def.ValueType); err != nil {
		return fmt.Errorf("key %q: %w", def.Key, err)
	}

	if def.Validator != nil {
		if err := def.Validator(value); err != nil {
			return fmt.Errorf("key %q: %w", def.Key, err)
		}
	}

	return nil
}

// checkValueType verifies that a value matches the expected ValueType.
//
// JSON coercion is handled explicitly: when values arrive from JSON
// unmarshalling, all numbers are represented as float64. For ValueTypeInt we
// accept float64 values that have no fractional part (e.g. 42.0). For
// ValueTypeFloat we accept int and int64 (widened to float).
func checkValueType(value any, expected domain.ValueType) error {
	if value == nil {
		return nil
	}

	switch expected {
	case domain.ValueTypeString:
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected %s, got %T: %w", expected, value, domain.ErrValueInvalid)
		}
	case domain.ValueTypeInt:
		if !isIntCompatible(value) {
			return fmt.Errorf("expected %s, got %T: %w", expected, value, domain.ErrValueInvalid)
		}
	case domain.ValueTypeBool:
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("expected %s, got %T: %w", expected, value, domain.ErrValueInvalid)
		}
	case domain.ValueTypeFloat:
		if !isFloatCompatible(value) {
			return fmt.Errorf("expected %s, got %T: %w", expected, value, domain.ErrValueInvalid)
		}
	case domain.ValueTypeObject:
		if !isObjectCompatible(value) {
			return fmt.Errorf("expected %s, got %T: %w", expected, value, domain.ErrValueInvalid)
		}
	case domain.ValueTypeArray:
		if !isArrayCompatible(value) {
			return fmt.Errorf("expected %s, got %T: %w", expected, value, domain.ErrValueInvalid)
		}
	default:
		return fmt.Errorf("unsupported value type %q: %w", expected, domain.ErrValueInvalid)
	}

	return nil
}

// isIntCompatible reports whether value can be treated as an integer.
// Accepts int, int64, and float64 without a fractional part (JSON coercion).
func isIntCompatible(value any) bool {
	switch v := value.(type) {
	case int:
		return true
	case int64:
		return true
	case float64:
		return math.Trunc(v) == v && !math.IsInf(v, 0) && !math.IsNaN(v)
	default:
		return false
	}
}

// isFloatCompatible reports whether value can be treated as a float.
// Accepts float64, float32, int, and int64 (widened to float).
func isFloatCompatible(value any) bool {
	switch value.(type) {
	case float64, float32, int, int64:
		return true
	default:
		return false
	}
}

func isObjectCompatible(value any) bool {
	rv := reflect.ValueOf(value)
	return rv.IsValid() && rv.Kind() == reflect.Map
}

func isArrayCompatible(value any) bool {
	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return false
	}

	return rv.Kind() == reflect.Array || rv.Kind() == reflect.Slice
}
