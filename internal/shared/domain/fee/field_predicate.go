package fee

import (
	"context"
	"fmt"
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
	if pred.Field == "" {
		return fmt.Errorf("field predicate: %w", ErrPredicateFieldRequired)
	}

	if !pred.Operator.IsValid() {
		return fmt.Errorf("field predicate operator %q: %w", pred.Operator, ErrInvalidPredicateOperator)
	}

	switch pred.Operator {
	case PredicateOperatorEquals:
		if pred.Value == "" {
			return fmt.Errorf("field predicate %q: %w", pred.Field, ErrPredicateValueRequired)
		}
	case PredicateOperatorIn:
		if len(pred.Values) == 0 {
			return fmt.Errorf("field predicate %q: %w", pred.Field, ErrPredicateValuesRequired)
		}
	case PredicateOperatorExists:
		// No additional fields required.
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

		return strings.EqualFold(fmt.Sprintf("%v", rawVal), pred.Value)

	case PredicateOperatorIn:
		if !exists {
			return false
		}

		strVal := fmt.Sprintf("%v", rawVal)

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
