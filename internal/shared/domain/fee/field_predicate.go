package fee

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// PredicateOperator defines the comparison operation for a field predicate.
type PredicateOperator string

const (
	// PredicateOperatorEquals matches when metadata[Field] string-equals Value (case-insensitive).
	PredicateOperatorEquals PredicateOperator = "EQUALS"
	// PredicateOperatorIn matches when metadata[Field] is one of Values (case-insensitive).
	PredicateOperatorIn PredicateOperator = "IN"
	// PredicateOperatorExists matches when metadata[Field] key is present (any value).
	PredicateOperatorExists PredicateOperator = "EXISTS"
)

// Length and count limits for FieldPredicate fields.
const (
	maxPredicateFieldLength = 255
	maxPredicateValueLength = 1024
	maxPredicateValuesCount = 100
)

// IsValid returns true if the predicate operator is a recognized value.
func (o PredicateOperator) IsValid() bool {
	switch o {
	case PredicateOperatorEquals, PredicateOperatorIn, PredicateOperatorExists:
		return true
	default:
		return false
	}
}

// FieldPredicate represents a single condition evaluated against transaction metadata.
type FieldPredicate struct {
	Field    string            `json:"field"`
	Operator PredicateOperator `json:"operator"`
	Value    string            `json:"value,omitempty"`
	Values   []string          `json:"values,omitempty"`
}

// Validate checks that the predicate has all required fields for its operator.
func (pred FieldPredicate) Validate(_ context.Context) error {
	if err := pred.validateField(); err != nil {
		return err
	}

	if err := pred.validateOperator(); err != nil {
		return err
	}

	return pred.validateOperatorRequirements()
}

func (pred FieldPredicate) validateField() error {
	if strings.TrimSpace(pred.Field) == "" {
		return fmt.Errorf("field predicate: %w", ErrPredicateFieldRequired)
	}

	if len(pred.Field) > maxPredicateFieldLength {
		return fmt.Errorf("field predicate: %w", ErrPredicateFieldTooLong)
	}

	return nil
}

func (pred FieldPredicate) validateOperator() error {
	if !pred.Operator.IsValid() {
		return fmt.Errorf("field predicate operator %q: %w", pred.Operator, ErrInvalidPredicateOperator)
	}

	return nil
}

func (pred FieldPredicate) validateOperatorRequirements() error {
	switch pred.Operator {
	case PredicateOperatorEquals:
		return pred.validateEquals()
	case PredicateOperatorIn:
		return pred.validateIn()
	case PredicateOperatorExists:
		return pred.validateExists()
	}

	return nil
}

func (pred FieldPredicate) validateEquals() error {
	if pred.Value == "" {
		return fmt.Errorf("field predicate %q: %w", pred.Field, ErrPredicateValueRequired)
	}

	if len(pred.Values) > 0 {
		return fmt.Errorf("field predicate %q: %w", pred.Field, ErrPredicateValuesForbidden)
	}

	if len(pred.Value) > maxPredicateValueLength {
		return fmt.Errorf("field predicate: %w", ErrPredicateValueTooLong)
	}

	return nil
}

func (pred FieldPredicate) validateIn() error {
	if pred.Value != "" {
		return fmt.Errorf("field predicate %q: %w", pred.Field, ErrPredicateValueForbidden)
	}

	if len(pred.Values) == 0 {
		return fmt.Errorf("field predicate %q: %w", pred.Field, ErrPredicateValuesRequired)
	}

	if len(pred.Values) > maxPredicateValuesCount {
		return fmt.Errorf("field predicate: %w", ErrPredicateValuesTooMany)
	}

	for _, value := range pred.Values {
		if len(value) > maxPredicateValueLength {
			return fmt.Errorf("field predicate: %w", ErrPredicateValueTooLong)
		}
	}

	return nil
}

func (pred FieldPredicate) validateExists() error {
	if pred.Value != "" {
		return fmt.Errorf("field predicate %q: %w", pred.Field, ErrPredicateValueForbidden)
	}

	if len(pred.Values) > 0 {
		return fmt.Errorf("field predicate %q: %w", pred.Field, ErrPredicateValuesForbidden)
	}

	return nil
}

// Evaluate checks whether the predicate matches the given metadata.
// Returns false if metadata is nil or the key is missing (for EQUALS/IN/EXISTS).
func (pred FieldPredicate) Evaluate(metadata map[string]any) bool {
	if metadata == nil {
		return false
	}

	rawVal, exists := metadata[pred.Field]

	switch pred.Operator {
	case PredicateOperatorEquals:
		if !exists {
			return false
		}

		return strings.EqualFold(stringifyPredicateValue(rawVal), pred.Value)

	case PredicateOperatorIn:
		if !exists {
			return false
		}

		strVal := stringifyPredicateValue(rawVal)

		for _, candidate := range pred.Values {
			if strings.EqualFold(strVal, candidate) {
				return true
			}
		}

		return false

	case PredicateOperatorExists:
		return exists

	default:
		return false
	}
}

func stringifyPredicateValue(raw any) string {
	switch value := raw.(type) {
	case string:
		return value
	case []byte:
		return string(value)
	case bool:
		return strconv.FormatBool(value)
	case fmt.Stringer:
		// Guard against typed-nil interface values (e.g. (*time.Time)(nil) satisfies
		// fmt.Stringer but calling String() on it panics). We check all nil-able kinds
		// (pointer, map, slice, chan, func, interface) to be fully defensive.
		rv := reflect.ValueOf(value)
		switch rv.Kind() {
		case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func, reflect.Interface:
			if rv.IsNil() {
				return ""
			}
		default:
			// Non-nilable kinds (Int, String, Struct, …) — no guard needed.
		}

		return value.String()
	}

	return stringifyPredicateScalar(raw)
}

func stringifyPredicateScalar(raw any) string {
	rv := reflect.ValueOf(raw)
	if !rv.IsValid() {
		return fmt.Sprintf("%v", raw)
	}

	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(rv.Uint(), 10)
	case reflect.Float32:
		return strconv.FormatFloat(rv.Float(), 'f', -1, 32)
	case reflect.Float64:
		return strconv.FormatFloat(rv.Float(), 'f', -1, 64)
	default:
		return fmt.Sprintf("%v", raw)
	}
}
