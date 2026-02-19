//go:build unit

package value_objects

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRuleType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    RuleType
		wantErr bool
	}{
		{"exact_valid", "EXACT", RuleTypeExact, false},
		{"tolerance_valid", "TOLERANCE", RuleTypeTolerance, false},
		{"date_lag_valid", "DATE_LAG", RuleTypeDateLag, false},
		{"invalid_type", "INVALID", "", true},
		{"empty_string", "", "", true},
		{"lowercase_exact", "exact", RuleTypeExact, false},
		{"lowercase_tolerance", "tolerance", RuleTypeTolerance, false},
		{"lowercase_date_lag", "date_lag", RuleTypeDateLag, false},
		{"mixed_case_exact", "Exact", RuleTypeExact, false},
		{"mixed_case_tolerance", "Tolerance", RuleTypeTolerance, false},
		{"with_spaces", " EXACT ", RuleTypeExact, false},
		{"fuzzy", "FUZZY", "", true},
		{"partial_match", "EXA", "", true},
		{"numeric", "123", "", true},
		{"special_chars", "EXACT!", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseRuleType(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "invalid rule type")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			assert.True(t, got.Valid())
		})
	}
}

func TestRuleType_Valid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		rule  RuleType
		valid bool
	}{
		{"exact", RuleTypeExact, true},
		{"tolerance", RuleTypeTolerance, true},
		{"date_lag", RuleTypeDateLag, true},
		{"invalid", RuleType("FUZZY"), false},
		{"empty", RuleType(""), false},
		{"lowercase_exact", RuleType("exact"), false},
		{"lowercase_tolerance", RuleType("tolerance"), false},
		{"mixed_case", RuleType("Exact"), false},
		{"with_spaces", RuleType(" EXACT "), false},
		{"numeric", RuleType("123"), false},
		{"special_chars", RuleType("EXACT!"), false},
		{"partial_match", RuleType("EXA"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.valid, tt.rule.Valid())
		})
	}
}

func TestRuleType_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		rule  RuleType
		valid bool
	}{
		{"exact_is_valid", RuleTypeExact, true},
		{"tolerance_is_valid", RuleTypeTolerance, true},
		{"date_lag_is_valid", RuleTypeDateLag, true},
		{"invalid_is_not_valid", RuleType("UNKNOWN"), false},
		{"empty_is_not_valid", RuleType(""), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.valid, tt.rule.IsValid())
			assert.Equal(t, tt.rule.Valid(), tt.rule.IsValid())
		})
	}
}

func TestRuleType_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		rule     RuleType
		expected string
	}{
		{"exact_string", RuleTypeExact, "EXACT"},
		{"tolerance_string", RuleTypeTolerance, "TOLERANCE"},
		{"date_lag_string", RuleTypeDateLag, "DATE_LAG"},
		{"empty_string", RuleType(""), ""},
		{"custom_value", RuleType("CUSTOM"), "CUSTOM"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, tt.rule.String())
		})
	}
}

func TestRuleType_Constants(t *testing.T) {
	t.Parallel()

	t.Run("exact_constant_value", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, RuleType("EXACT"), RuleTypeExact)
	})

	t.Run("tolerance_constant_value", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, RuleType("TOLERANCE"), RuleTypeTolerance)
	})

	t.Run("date_lag_constant_value", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, RuleType("DATE_LAG"), RuleTypeDateLag)
	})
}

func TestErrInvalidRuleType(t *testing.T) {
	t.Parallel()

	t.Run("error_is_not_nil", func(t *testing.T) {
		t.Parallel()

		assert.NotNil(t, ErrInvalidRuleType)
	})

	t.Run("error_message", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "invalid rule type", ErrInvalidRuleType.Error())
	})

	t.Run("wrapped_error_can_be_unwrapped", func(t *testing.T) {
		t.Parallel()

		_, err := ParseRuleType("INVALID")
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrInvalidRuleType))
	})
}
