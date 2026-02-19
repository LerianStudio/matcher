//go:build unit

package dispute_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/domain/dispute"
)

func TestDisputeCategory_Constants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		category dispute.DisputeCategory
		expected string
	}{
		{
			name:     "BankFeeError",
			category: dispute.DisputeCategoryBankFeeError,
			expected: "BANK_FEE_ERROR",
		},
		{
			name:     "UnrecognizedCharge",
			category: dispute.DisputeCategoryUnrecognizedCharge,
			expected: "UNRECOGNIZED_CHARGE",
		},
		{
			name:     "DuplicateTransaction",
			category: dispute.DisputeCategoryDuplicateTransaction,
			expected: "DUPLICATE_TRANSACTION",
		},
		{name: "Other", category: dispute.DisputeCategoryOther, expected: "OTHER"},
		{
			name:     "AmountMismatch",
			category: dispute.DisputeCategoryAmountMismatch,
			expected: "AMOUNT_MISMATCH",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, string(tt.category))
		})
	}
}

func TestDisputeCategory_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		category dispute.DisputeCategory
		expected bool
	}{
		{name: "BankFeeError valid", category: dispute.DisputeCategoryBankFeeError, expected: true},
		{
			name:     "UnrecognizedCharge valid",
			category: dispute.DisputeCategoryUnrecognizedCharge,
			expected: true,
		},
		{
			name:     "DuplicateTransaction valid",
			category: dispute.DisputeCategoryDuplicateTransaction,
			expected: true,
		},
		{
			name:     "AmountMismatch valid",
			category: dispute.DisputeCategoryAmountMismatch,
			expected: true,
		},
		{name: "Other valid", category: dispute.DisputeCategoryOther, expected: true},
		{name: "Empty invalid", category: dispute.DisputeCategory(""), expected: false},
		{
			name:     "Lowercase invalid",
			category: dispute.DisputeCategory("bank_fee_error"),
			expected: false,
		},
		{name: "Unknown invalid", category: dispute.DisputeCategory("UNKNOWN"), expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.category.IsValid())
		})
	}
}

func TestDisputeCategory_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		category dispute.DisputeCategory
		expected string
	}{
		{
			name:     "BankFeeError",
			category: dispute.DisputeCategoryBankFeeError,
			expected: "BANK_FEE_ERROR",
		},
		{
			name:     "UnrecognizedCharge",
			category: dispute.DisputeCategoryUnrecognizedCharge,
			expected: "UNRECOGNIZED_CHARGE",
		},
		{
			name:     "DuplicateTransaction",
			category: dispute.DisputeCategoryDuplicateTransaction,
			expected: "DUPLICATE_TRANSACTION",
		},
		{name: "Other", category: dispute.DisputeCategoryOther, expected: "OTHER"},
		{
			name:     "AmountMismatch",
			category: dispute.DisputeCategoryAmountMismatch,
			expected: "AMOUNT_MISMATCH",
		},
		{name: "Custom", category: dispute.DisputeCategory("CUSTOM"), expected: "CUSTOM"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.category.String())
		})
	}
}

func TestParseDisputeCategory(t *testing.T) {
	t.Parallel()

	valid := []string{"BANK_FEE_ERROR", "UNRECOGNIZED_CHARGE", "DUPLICATE_TRANSACTION", "AMOUNT_MISMATCH", "OTHER"}
	for _, category := range valid {
		t.Run("Valid "+category, func(t *testing.T) {
			t.Parallel()

			parsed, err := dispute.ParseDisputeCategory(category)
			require.NoError(t, err)
			assert.Equal(t, dispute.DisputeCategory(category), parsed)
		})
	}

	// Test case-insensitive parsing (lowercase should now work due to ToUpper normalization)
	t.Run("Valid lowercase bank_fee_error", func(t *testing.T) {
		t.Parallel()

		parsed, err := dispute.ParseDisputeCategory("bank_fee_error")
		require.NoError(t, err)
		assert.Equal(t, dispute.DisputeCategoryBankFeeError, parsed)
	})

	// Invalid cases - empty, unknown, and trailing spaces should still fail
	invalid := []string{"", "UNKNOWN", "BANK_FEE_ERROR "}
	for _, category := range invalid {
		t.Run("Invalid "+category, func(t *testing.T) {
			t.Parallel()

			parsed, err := dispute.ParseDisputeCategory(category)
			require.Error(t, err)
			require.ErrorIs(t, err, dispute.ErrInvalidDisputeCategory)
			assert.Equal(t, dispute.DisputeCategory(""), parsed)
		})
	}
}
