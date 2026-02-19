//go:build unit

package value_objects_test

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
)

func TestExceptionSeverity_Constants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		severity value_objects.ExceptionSeverity
		expected string
	}{
		{name: "Low", severity: value_objects.ExceptionSeverityLow, expected: "LOW"},
		{name: "Medium", severity: value_objects.ExceptionSeverityMedium, expected: "MEDIUM"},
		{name: "High", severity: value_objects.ExceptionSeverityHigh, expected: "HIGH"},
		{name: "Critical", severity: value_objects.ExceptionSeverityCritical, expected: "CRITICAL"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, string(tt.severity))
		})
	}
}

func TestExceptionSeverity_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		severity value_objects.ExceptionSeverity
		expected bool
	}{
		{name: "Low valid", severity: value_objects.ExceptionSeverityLow, expected: true},
		{name: "Medium valid", severity: value_objects.ExceptionSeverityMedium, expected: true},
		{name: "High valid", severity: value_objects.ExceptionSeverityHigh, expected: true},
		{name: "Critical valid", severity: value_objects.ExceptionSeverityCritical, expected: true},
		{name: "Empty invalid", severity: value_objects.ExceptionSeverity(""), expected: false},
		{
			name:     "Random invalid",
			severity: value_objects.ExceptionSeverity("RANDOM"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.severity.IsValid())
		})
	}
}

func TestParseExceptionSeverity(t *testing.T) {
	t.Parallel()

	valid := []struct {
		input    string
		expected value_objects.ExceptionSeverity
	}{
		{"LOW", value_objects.ExceptionSeverityLow},
		{"MEDIUM", value_objects.ExceptionSeverityMedium},
		{"HIGH", value_objects.ExceptionSeverityHigh},
		{"CRITICAL", value_objects.ExceptionSeverityCritical},
		{"low", value_objects.ExceptionSeverityLow},
		{"high", value_objects.ExceptionSeverityHigh},
		{"  MEDIUM  ", value_objects.ExceptionSeverityMedium},
		{"Critical", value_objects.ExceptionSeverityCritical},
	}
	for _, tt := range valid {
		t.Run("Valid_"+tt.input, func(t *testing.T) {
			t.Parallel()

			parsed, err := value_objects.ParseExceptionSeverity(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, parsed)
		})
	}

	invalid := []string{"", "UNKNOWN", "invalid", "  "}
	for _, value := range invalid {
		t.Run("Invalid_"+value, func(t *testing.T) {
			t.Parallel()

			parsed, err := value_objects.ParseExceptionSeverity(value)
			require.Error(t, err)
			require.ErrorIs(t, err, value_objects.ErrInvalidExceptionSeverity)
			assert.Equal(t, value_objects.ExceptionSeverity(""), parsed)
		})
	}
}

func TestClassifyExceptionSeverity_EmptyRules(t *testing.T) {
	t.Parallel()

	_, err := value_objects.ClassifyExceptionSeverity(
		value_objects.SeverityClassificationInput{},
		nil,
	)
	require.ErrorIs(t, err, value_objects.ErrEmptySeverityRules)
}

func TestClassifyExceptionSeverity_DefaultRules(t *testing.T) {
	t.Parallel()

	rules := value_objects.DefaultSeverityRules([]string{"REGULATORY"})

	tests := []struct {
		name         string
		amount       decimal.Decimal
		ageHours     int
		sourceType   string
		fxMissing    bool
		expected     value_objects.ExceptionSeverity
		expectFXNote bool
	}{
		{
			name:     "Critical by amount threshold",
			amount:   decimal.NewFromInt(100000),
			ageHours: 1,
			expected: value_objects.ExceptionSeverityCritical,
		},
		{
			name:     "Critical by age threshold",
			amount:   decimal.NewFromInt(10),
			ageHours: 120,
			expected: value_objects.ExceptionSeverityCritical,
		},
		{
			name:       "Critical by regulatory source",
			amount:     decimal.NewFromInt(10),
			ageHours:   1,
			sourceType: "REGULATORY",
			expected:   value_objects.ExceptionSeverityCritical,
		},
		{
			name:     "High by amount threshold",
			amount:   decimal.NewFromInt(10000),
			ageHours: 1,
			expected: value_objects.ExceptionSeverityHigh,
		},
		{
			name:     "High by age threshold",
			amount:   decimal.NewFromInt(9999),
			ageHours: 72,
			expected: value_objects.ExceptionSeverityHigh,
		},
		{
			name:     "Medium by amount threshold",
			amount:   decimal.NewFromInt(1000),
			ageHours: 1,
			expected: value_objects.ExceptionSeverityMedium,
		},
		{
			name:     "Medium by age threshold",
			amount:   decimal.NewFromInt(999),
			ageHours: 24,
			expected: value_objects.ExceptionSeverityMedium,
		},
		{
			name:     "Low default",
			amount:   decimal.NewFromInt(999),
			ageHours: 23,
			expected: value_objects.ExceptionSeverityLow,
		},
		{
			name:         "FX missing adds reason",
			amount:       decimal.NewFromInt(1),
			ageHours:     1,
			fxMissing:    true,
			expected:     value_objects.ExceptionSeverityLow,
			expectFXNote: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := value_objects.ClassifyExceptionSeverity(
				value_objects.SeverityClassificationInput{
					AmountAbsBase: tt.amount,
					AgeHours:      tt.ageHours,
					SourceType:    tt.sourceType,
					FXMissing:     tt.fxMissing,
				},
				rules,
			)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result.Severity)

			if tt.expectFXNote {
				assert.Contains(t, result.Reasons, value_objects.ReasonFXRateUnavailable)
			} else {
				assert.NotContains(t, result.Reasons, value_objects.ReasonFXRateUnavailable)
			}
		})
	}
}

func TestClassifyExceptionSeverity_InvalidRuleSeverity(t *testing.T) {
	t.Parallel()

	rules := []value_objects.SeverityRule{
		{Severity: value_objects.ExceptionSeverity("BAD")},
	}

	_, err := value_objects.ClassifyExceptionSeverity(value_objects.SeverityClassificationInput{
		AmountAbsBase: decimal.NewFromInt(1),
		AgeHours:      1,
	}, rules)
	require.ErrorIs(t, err, value_objects.ErrInvalidExceptionSeverity)
}

func TestClassifyExceptionSeverity_BoundaryValues(t *testing.T) {
	t.Parallel()

	rules := value_objects.DefaultSeverityRules(nil)

	tests := []struct {
		name     string
		amount   decimal.Decimal
		ageHours int
		expected value_objects.ExceptionSeverity
	}{
		// Amount boundaries - verify >= comparison
		{
			name:     "Amount 999 -> LOW",
			amount:   decimal.NewFromInt(999),
			ageHours: 0,
			expected: value_objects.ExceptionSeverityLow,
		},
		{
			name:     "Amount 1000 -> MEDIUM",
			amount:   decimal.NewFromInt(1000),
			ageHours: 0,
			expected: value_objects.ExceptionSeverityMedium,
		},
		{
			name:     "Amount 1001 -> MEDIUM",
			amount:   decimal.NewFromInt(1001),
			ageHours: 0,
			expected: value_objects.ExceptionSeverityMedium,
		},
		{
			name:     "Amount 9999 -> MEDIUM",
			amount:   decimal.NewFromInt(9999),
			ageHours: 0,
			expected: value_objects.ExceptionSeverityMedium,
		},
		{
			name:     "Amount 10000 -> HIGH",
			amount:   decimal.NewFromInt(10000),
			ageHours: 0,
			expected: value_objects.ExceptionSeverityHigh,
		},
		{
			name:     "Amount 10001 -> HIGH",
			amount:   decimal.NewFromInt(10001),
			ageHours: 0,
			expected: value_objects.ExceptionSeverityHigh,
		},
		{
			name:     "Amount 99999 -> HIGH",
			amount:   decimal.NewFromInt(99999),
			ageHours: 0,
			expected: value_objects.ExceptionSeverityHigh,
		},
		{
			name:     "Amount 100000 -> CRITICAL",
			amount:   decimal.NewFromInt(100000),
			ageHours: 0,
			expected: value_objects.ExceptionSeverityCritical,
		},
		{
			name:     "Amount 100001 -> CRITICAL",
			amount:   decimal.NewFromInt(100001),
			ageHours: 0,
			expected: value_objects.ExceptionSeverityCritical,
		},

		// Age boundaries - verify >= comparison
		{
			name:     "Age 23h -> LOW",
			amount:   decimal.NewFromInt(1),
			ageHours: 23,
			expected: value_objects.ExceptionSeverityLow,
		},
		{
			name:     "Age 24h -> MEDIUM",
			amount:   decimal.NewFromInt(1),
			ageHours: 24,
			expected: value_objects.ExceptionSeverityMedium,
		},
		{
			name:     "Age 25h -> MEDIUM",
			amount:   decimal.NewFromInt(1),
			ageHours: 25,
			expected: value_objects.ExceptionSeverityMedium,
		},
		{
			name:     "Age 71h -> MEDIUM",
			amount:   decimal.NewFromInt(1),
			ageHours: 71,
			expected: value_objects.ExceptionSeverityMedium,
		},
		{
			name:     "Age 72h -> HIGH",
			amount:   decimal.NewFromInt(1),
			ageHours: 72,
			expected: value_objects.ExceptionSeverityHigh,
		},
		{
			name:     "Age 73h -> HIGH",
			amount:   decimal.NewFromInt(1),
			ageHours: 73,
			expected: value_objects.ExceptionSeverityHigh,
		},
		{
			name:     "Age 119h -> HIGH",
			amount:   decimal.NewFromInt(1),
			ageHours: 119,
			expected: value_objects.ExceptionSeverityHigh,
		},
		{
			name:     "Age 120h -> CRITICAL",
			amount:   decimal.NewFromInt(1),
			ageHours: 120,
			expected: value_objects.ExceptionSeverityCritical,
		},
		{
			name:     "Age 121h -> CRITICAL",
			amount:   decimal.NewFromInt(1),
			ageHours: 121,
			expected: value_objects.ExceptionSeverityCritical,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := value_objects.ClassifyExceptionSeverity(
				value_objects.SeverityClassificationInput{
					AmountAbsBase: tt.amount,
					AgeHours:      tt.ageHours,
				},
				rules,
			)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result.Severity)
		})
	}
}

func TestClassifyExceptionSeverity_NegativeAmount(t *testing.T) {
	t.Parallel()

	rules := value_objects.DefaultSeverityRules(nil)

	tests := []struct {
		name     string
		amount   decimal.Decimal
		expected value_objects.ExceptionSeverity
	}{
		{
			name:     "Negative -100000 -> CRITICAL",
			amount:   decimal.NewFromInt(-100000),
			expected: value_objects.ExceptionSeverityCritical,
		},
		{
			name:     "Negative -10000 -> HIGH",
			amount:   decimal.NewFromInt(-10000),
			expected: value_objects.ExceptionSeverityHigh,
		},
		{
			name:     "Negative -1000 -> MEDIUM",
			amount:   decimal.NewFromInt(-1000),
			expected: value_objects.ExceptionSeverityMedium,
		},
		{
			name:     "Negative -999 -> LOW",
			amount:   decimal.NewFromInt(-999),
			expected: value_objects.ExceptionSeverityLow,
		},
		{
			name:     "Negative -1 -> LOW",
			amount:   decimal.NewFromInt(-1),
			expected: value_objects.ExceptionSeverityLow,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := value_objects.ClassifyExceptionSeverity(
				value_objects.SeverityClassificationInput{
					AmountAbsBase: tt.amount,
					AgeHours:      0,
				},
				rules,
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				tt.expected,
				result.Severity,
				"negative amounts should use absolute value",
			)
		})
	}
}
