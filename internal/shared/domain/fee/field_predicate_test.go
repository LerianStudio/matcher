//go:build unit

package fee

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPredicateOperator_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		op    PredicateOperator
		valid bool
	}{
		{"EQUALS valid", PredicateOperatorEquals, true},
		{"IN valid", PredicateOperatorIn, true},
		{"EXISTS valid", PredicateOperatorExists, true},
		{"empty invalid", PredicateOperator(""), false},
		{"unknown invalid", PredicateOperator("LIKE"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.valid, tt.op.IsValid())
		})
	}
}

func TestFieldPredicate_Validate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name      string
		predicate FieldPredicate
		wantErr   error
	}{
		{
			name:      "valid EQUALS",
			predicate: FieldPredicate{Field: "institution", Operator: PredicateOperatorEquals, Value: "Itau"},
			wantErr:   nil,
		},
		{
			name:      "valid IN",
			predicate: FieldPredicate{Field: "card_brand", Operator: PredicateOperatorIn, Values: []string{"Visa", "Mastercard"}},
			wantErr:   nil,
		},
		{
			name:      "valid EXISTS",
			predicate: FieldPredicate{Field: "institution", Operator: PredicateOperatorExists},
			wantErr:   nil,
		},
		{
			name:      "empty field",
			predicate: FieldPredicate{Field: "", Operator: PredicateOperatorEquals, Value: "x"},
			wantErr:   ErrPredicateFieldRequired,
		},
		{
			name:      "whitespace field",
			predicate: FieldPredicate{Field: "   ", Operator: PredicateOperatorEquals, Value: "x"},
			wantErr:   ErrPredicateFieldRequired,
		},
		{
			name:      "invalid operator",
			predicate: FieldPredicate{Field: "f", Operator: PredicateOperator("LIKE")},
			wantErr:   ErrInvalidPredicateOperator,
		},
		{
			name:      "EQUALS missing value",
			predicate: FieldPredicate{Field: "f", Operator: PredicateOperatorEquals, Value: ""},
			wantErr:   ErrPredicateValueRequired,
		},
		{
			name:      "IN missing values",
			predicate: FieldPredicate{Field: "f", Operator: PredicateOperatorIn, Values: nil},
			wantErr:   ErrPredicateValuesRequired,
		},
		{
			name:      "IN empty values slice",
			predicate: FieldPredicate{Field: "f", Operator: PredicateOperatorIn, Values: []string{}},
			wantErr:   ErrPredicateValuesRequired,
		},
		{
			name:      "EQUALS rejects values slice",
			predicate: FieldPredicate{Field: "f", Operator: PredicateOperatorEquals, Value: "x", Values: []string{"y"}},
			wantErr:   ErrPredicateValuesForbidden,
		},
		{
			name:      "IN rejects singular value",
			predicate: FieldPredicate{Field: "f", Operator: PredicateOperatorIn, Value: "x", Values: []string{"y"}},
			wantErr:   ErrPredicateValueForbidden,
		},
		{
			name:      "EXISTS rejects singular value",
			predicate: FieldPredicate{Field: "f", Operator: PredicateOperatorExists, Value: "x"},
			wantErr:   ErrPredicateValueForbidden,
		},
		{
			name:      "EXISTS rejects values slice",
			predicate: FieldPredicate{Field: "f", Operator: PredicateOperatorExists, Values: []string{"x"}},
			wantErr:   ErrPredicateValuesForbidden,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.predicate.Validate(ctx)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestFieldPredicate_Evaluate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		predicate FieldPredicate
		metadata  map[string]any
		matches   bool
	}{
		{
			name:      "EQUALS match case-insensitive",
			predicate: FieldPredicate{Field: "institution", Operator: PredicateOperatorEquals, Value: "Itau"},
			metadata:  map[string]any{"institution": "itau"},
			matches:   true,
		},
		{
			name:      "EQUALS no match",
			predicate: FieldPredicate{Field: "institution", Operator: PredicateOperatorEquals, Value: "Itau"},
			metadata:  map[string]any{"institution": "Santander"},
			matches:   false,
		},
		{
			name:      "EQUALS missing key",
			predicate: FieldPredicate{Field: "institution", Operator: PredicateOperatorEquals, Value: "Itau"},
			metadata:  map[string]any{"other": "value"},
			matches:   false,
		},
		{
			name:      "EQUALS numeric value coercion",
			predicate: FieldPredicate{Field: "code", Operator: PredicateOperatorEquals, Value: "42"},
			metadata:  map[string]any{"code": 42},
			matches:   true,
		},
		{
			name:      "IN match",
			predicate: FieldPredicate{Field: "brand", Operator: PredicateOperatorIn, Values: []string{"Visa", "Mastercard"}},
			metadata:  map[string]any{"brand": "visa"},
			matches:   true,
		},
		{
			name:      "IN no match",
			predicate: FieldPredicate{Field: "brand", Operator: PredicateOperatorIn, Values: []string{"Visa", "Mastercard"}},
			metadata:  map[string]any{"brand": "Elo"},
			matches:   false,
		},
		{
			name:      "IN missing key",
			predicate: FieldPredicate{Field: "brand", Operator: PredicateOperatorIn, Values: []string{"Visa"}},
			metadata:  map[string]any{},
			matches:   false,
		},
		{
			name:      "EXISTS key present",
			predicate: FieldPredicate{Field: "institution", Operator: PredicateOperatorExists},
			metadata:  map[string]any{"institution": ""},
			matches:   true,
		},
		{
			name:      "EXISTS key absent",
			predicate: FieldPredicate{Field: "institution", Operator: PredicateOperatorExists},
			metadata:  map[string]any{"other": "value"},
			matches:   false,
		},
		{
			name:      "nil metadata",
			predicate: FieldPredicate{Field: "institution", Operator: PredicateOperatorEquals, Value: "X"},
			metadata:  nil,
			matches:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.predicate.Evaluate(tt.metadata)
			assert.Equal(t, tt.matches, got)
		})
	}
}
