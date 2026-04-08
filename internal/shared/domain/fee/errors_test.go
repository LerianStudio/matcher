//go:build unit

package fee

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errTestContext = errors.New("context")

func TestErrorVariables(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "ErrNilFeeStructure",
			err:      ErrNilFeeStructure,
			expected: "fee structure is nil",
		},
		{
			name:     "ErrInvalidCurrency",
			err:      ErrInvalidCurrency,
			expected: "invalid currency",
		},
		{
			name:     "ErrCurrencyMismatch",
			err:      ErrCurrencyMismatch,
			expected: "currency mismatch",
		},
		{
			name:     "ErrNegativeAmount",
			err:      ErrNegativeAmount,
			expected: "amount must be non-negative",
		},
		{
			name:     "ErrInvalidPercentageRate",
			err:      ErrInvalidPercentageRate,
			expected: "percentage rate must be between 0 and 1 inclusive",
		},
		{
			name:     "ErrInvalidTieredDefinition",
			err:      ErrInvalidTieredDefinition,
			expected: "invalid tiered fee definition",
		},
		{
			name:     "ErrToleranceNegative",
			err:      ErrToleranceNegative,
			expected: "tolerance must be non-negative",
		},
		{
			name:     "ErrScheduleNameRequired",
			err:      ErrScheduleNameRequired,
			expected: "fee schedule name is required",
		},
		{
			name:     "ErrScheduleNameTooLong",
			err:      ErrScheduleNameTooLong,
			expected: "fee schedule name exceeds 100 characters",
		},
		{
			name:     "ErrScheduleItemsRequired",
			err:      ErrScheduleItemsRequired,
			expected: "fee schedule must have at least one item",
		},
		{
			name:     "ErrDuplicateItemPriority",
			err:      ErrDuplicateItemPriority,
			expected: "duplicate item priority in fee schedule",
		},
		{
			name:     "ErrInvalidApplicationOrder",
			err:      ErrInvalidApplicationOrder,
			expected: "invalid application order",
		},
		{
			name:     "ErrInvalidRoundingScale",
			err:      ErrInvalidRoundingScale,
			expected: "rounding scale must be between 0 and 10",
		},
		{
			name:     "ErrInvalidRoundingMode",
			err:      ErrInvalidRoundingMode,
			expected: "invalid rounding mode",
		},
		{
			name:     "ErrItemNameRequired",
			err:      ErrItemNameRequired,
			expected: "fee schedule item name is required",
		},
		{
			name:     "ErrNilSchedule",
			err:      ErrNilSchedule,
			expected: "fee schedule is nil",
		},
		{
			name:     "ErrGrossConvergenceFailed",
			err:      ErrGrossConvergenceFailed,
			expected: "gross calculation failed to converge",
		},
		{
			name:     "ErrFeeScheduleNotFound",
			err:      ErrFeeScheduleNotFound,
			expected: "fee schedule not found",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Error(t, tc.err)
			assert.Equal(t, tc.expected, tc.err.Error())
		})
	}
}

func TestErrorsIs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		err    error
		target error
		match  bool
	}{
		{
			name:   "ErrInvalidCurrency matches itself",
			err:    ErrInvalidCurrency,
			target: ErrInvalidCurrency,
			match:  true,
		},
		{
			name:   "ErrCurrencyMismatch does not match ErrInvalidCurrency",
			err:    ErrCurrencyMismatch,
			target: ErrInvalidCurrency,
			match:  false,
		},
		{
			name:   "ErrNegativeAmount matches itself",
			err:    ErrNegativeAmount,
			target: ErrNegativeAmount,
			match:  true,
		},
		{
			name:   "ErrToleranceNegative matches itself",
			err:    ErrToleranceNegative,
			target: ErrToleranceNegative,
			match:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := errors.Is(tc.err, tc.target)
			assert.Equal(t, tc.match, result)
		})
	}
}

func TestErrorsAreDistinct(t *testing.T) {
	t.Parallel()

	allErrors := []error{
		ErrNilFeeStructure,
		ErrInvalidCurrency,
		ErrCurrencyMismatch,
		ErrNegativeAmount,
		ErrInvalidPercentageRate,
		ErrInvalidTieredDefinition,
		ErrToleranceNegative,
		ErrScheduleNameRequired,
		ErrScheduleNameTooLong,
		ErrScheduleItemsRequired,
		ErrDuplicateItemPriority,
		ErrInvalidApplicationOrder,
		ErrInvalidRoundingScale,
		ErrInvalidRoundingMode,
		ErrItemNameRequired,
		ErrNilSchedule,
		ErrGrossConvergenceFailed,
		ErrFeeScheduleNotFound,
	}

	for i, err1 := range allErrors {
		for j, err2 := range allErrors {
			if i != j {
				assert.NotEqual(t, err1, err2, "errors at index %d and %d should be distinct", i, j)
			}
		}
	}
}
