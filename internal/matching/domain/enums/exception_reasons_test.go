//go:build unit

package enums

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitizeReason(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty string returns UNMATCHED",
			input:    "",
			expected: ReasonUnmatched,
		},
		{
			name:     "valid UNMATCHED reason",
			input:    ReasonUnmatched,
			expected: ReasonUnmatched,
		},
		{
			name:     "valid FX_RATE_UNAVAILABLE reason",
			input:    ReasonFXRateUnavailable,
			expected: ReasonFXRateUnavailable,
		},
		{
			name:     "valid MISSING_BASE_AMOUNT reason",
			input:    ReasonMissingBaseAmount,
			expected: ReasonMissingBaseAmount,
		},
		{
			name:     "valid MISSING_BASE_CURRENCY reason",
			input:    ReasonMissingBaseCurrency,
			expected: ReasonMissingBaseCurrency,
		},
		{
			name:     "valid SPLIT_INCOMPLETE reason",
			input:    ReasonSplitIncomplete,
			expected: ReasonSplitIncomplete,
		},
		{
			name:     "valid VALIDATION_FAILED reason",
			input:    ReasonValidationFailed,
			expected: ReasonValidationFailed,
		},
		{
			name:     "valid SOURCE_MISMATCH reason",
			input:    ReasonSourceMismatch,
			expected: ReasonSourceMismatch,
		},
		{
			name:     "valid DUPLICATE_TRANSACTION reason",
			input:    ReasonDuplicateTransaction,
			expected: ReasonDuplicateTransaction,
		},
		{
			name:     "valid FEE_VARIANCE reason",
			input:    ReasonFeeVariance,
			expected: ReasonFeeVariance,
		},
		{
			name:     "valid FEE_DATA_MISSING reason",
			input:    ReasonFeeDataMissing,
			expected: ReasonFeeDataMissing,
		},
		{
			name:     "valid FEE_CURRENCY_MISMATCH reason",
			input:    ReasonFeeCurrencyMismatch,
			expected: ReasonFeeCurrencyMismatch,
		},
		{
			name:     "invalid reason returns UNMATCHED",
			input:    "INVALID_REASON",
			expected: ReasonUnmatched,
		},
		{
			name:     "SQL injection attempt returns UNMATCHED",
			input:    "'; DROP TABLE exceptions; --",
			expected: ReasonUnmatched,
		},
		{
			name:     "reason exceeding max length is truncated then validated",
			input:    "THIS_IS_A_VERY_LONG_REASON_THAT_EXCEEDS_THE_MAXIMUM_ALLOWED_LENGTH_OF_64_CHARS",
			expected: ReasonUnmatched,
		},
		{
			name:     "XSS attempt returns UNMATCHED",
			input:    "<script>alert('xss')</script>",
			expected: ReasonUnmatched,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := SanitizeReason(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSanitizeReason_LengthBounds(t *testing.T) {
	t.Parallel()

	// Test that long strings are handled (truncated before validation)
	longInput := make([]byte, 200)
	for i := range longInput {
		longInput[i] = 'A'
	}

	result := SanitizeReason(string(longInput))
	assert.Equal(t, ReasonUnmatched, result)
}

func TestReasonConstants(t *testing.T) {
	t.Parallel()

	// Verify constants are properly defined
	assert.Equal(t, "UNMATCHED", ReasonUnmatched)
	assert.Equal(t, "FX_RATE_UNAVAILABLE", ReasonFXRateUnavailable)
	assert.Equal(t, "MISSING_BASE_AMOUNT", ReasonMissingBaseAmount)
	assert.Equal(t, "MISSING_BASE_CURRENCY", ReasonMissingBaseCurrency)
	assert.Equal(t, "SPLIT_INCOMPLETE", ReasonSplitIncomplete)
	assert.Equal(t, "VALIDATION_FAILED", ReasonValidationFailed)
	assert.Equal(t, "SOURCE_MISMATCH", ReasonSourceMismatch)
	assert.Equal(t, "DUPLICATE_TRANSACTION", ReasonDuplicateTransaction)
	assert.Equal(t, "FEE_VARIANCE", ReasonFeeVariance)
	assert.Equal(t, "FEE_DATA_MISSING", ReasonFeeDataMissing)
	assert.Equal(t, "FEE_CURRENCY_MISMATCH", ReasonFeeCurrencyMismatch)
	assert.Equal(t, 64, MaxReasonLength)
}

func TestValidReasonsAllowlist(t *testing.T) {
	t.Parallel()

	// Verify all reason constants are in the allowlist
	expectedReasons := []string{
		ReasonUnmatched,
		ReasonFXRateUnavailable,
		ReasonMissingBaseAmount,
		ReasonMissingBaseCurrency,
		ReasonSplitIncomplete,
		ReasonValidationFailed,
		ReasonSourceMismatch,
		ReasonDuplicateTransaction,
		ReasonFeeVariance,
		ReasonFeeDataMissing,
		ReasonFeeCurrencyMismatch,
	}

	for _, reason := range expectedReasons {
		result := SanitizeReason(reason)
		assert.Equal(t, reason, result, "Reason %s should be in allowlist", reason)
	}
}
