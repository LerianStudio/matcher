// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package exception_test

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/domain/exception"
)

func TestExceptionSeverity_Constants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		severity exception.ExceptionSeverity
		expected string
	}{
		{name: "Low", severity: exception.ExceptionSeverityLow, expected: "LOW"},
		{name: "Medium", severity: exception.ExceptionSeverityMedium, expected: "MEDIUM"},
		{name: "High", severity: exception.ExceptionSeverityHigh, expected: "HIGH"},
		{name: "Critical", severity: exception.ExceptionSeverityCritical, expected: "CRITICAL"},
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
		severity exception.ExceptionSeverity
		expected bool
	}{
		{name: "Low valid", severity: exception.ExceptionSeverityLow, expected: true},
		{name: "Medium valid", severity: exception.ExceptionSeverityMedium, expected: true},
		{name: "High valid", severity: exception.ExceptionSeverityHigh, expected: true},
		{name: "Critical valid", severity: exception.ExceptionSeverityCritical, expected: true},
		{name: "Empty invalid", severity: exception.ExceptionSeverity(""), expected: false},
		{
			name:     "Random invalid",
			severity: exception.ExceptionSeverity("RANDOM"),
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
		expected exception.ExceptionSeverity
	}{
		{"LOW", exception.ExceptionSeverityLow},
		{"MEDIUM", exception.ExceptionSeverityMedium},
		{"HIGH", exception.ExceptionSeverityHigh},
		{"CRITICAL", exception.ExceptionSeverityCritical},
		{"low", exception.ExceptionSeverityLow},
		{"high", exception.ExceptionSeverityHigh},
		{"  MEDIUM  ", exception.ExceptionSeverityMedium},
		{"Critical", exception.ExceptionSeverityCritical},
	}
	for _, tt := range valid {
		t.Run("Valid_"+tt.input, func(t *testing.T) {
			t.Parallel()

			parsed, err := exception.ParseExceptionSeverity(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, parsed)
		})
	}

	invalid := []string{"", "UNKNOWN", "invalid", "  "}
	for _, value := range invalid {
		t.Run("Invalid_"+value, func(t *testing.T) {
			t.Parallel()

			parsed, err := exception.ParseExceptionSeverity(value)
			require.Error(t, err)
			require.ErrorIs(t, err, exception.ErrInvalidExceptionSeverity)
			assert.Equal(t, exception.ExceptionSeverity(""), parsed)
		})
	}
}

func TestClassifyExceptionSeverity_EmptyRules(t *testing.T) {
	t.Parallel()

	_, err := exception.ClassifyExceptionSeverity(
		exception.SeverityClassificationInput{},
		nil,
	)
	require.ErrorIs(t, err, exception.ErrEmptySeverityRules)
}

func TestClassifyExceptionSeverity_DefaultRules(t *testing.T) {
	t.Parallel()

	rules := exception.DefaultSeverityRules([]string{"REGULATORY"})

	tests := []struct {
		name         string
		amount       decimal.Decimal
		ageHours     int
		sourceType   string
		fxMissing    bool
		expected     exception.ExceptionSeverity
		expectFXNote bool
	}{
		{
			name:     "Critical by amount threshold",
			amount:   decimal.NewFromInt(100000),
			ageHours: 1,
			expected: exception.ExceptionSeverityCritical,
		},
		{
			name:     "Critical by age threshold",
			amount:   decimal.NewFromInt(10),
			ageHours: 120,
			expected: exception.ExceptionSeverityCritical,
		},
		{
			name:       "Critical by regulatory source",
			amount:     decimal.NewFromInt(10),
			ageHours:   1,
			sourceType: "REGULATORY",
			expected:   exception.ExceptionSeverityCritical,
		},
		{
			name:     "High by amount threshold",
			amount:   decimal.NewFromInt(10000),
			ageHours: 1,
			expected: exception.ExceptionSeverityHigh,
		},
		{
			name:     "High by age threshold",
			amount:   decimal.NewFromInt(9999),
			ageHours: 72,
			expected: exception.ExceptionSeverityHigh,
		},
		{
			name:     "Medium by amount threshold",
			amount:   decimal.NewFromInt(1000),
			ageHours: 1,
			expected: exception.ExceptionSeverityMedium,
		},
		{
			name:     "Medium by age threshold",
			amount:   decimal.NewFromInt(999),
			ageHours: 24,
			expected: exception.ExceptionSeverityMedium,
		},
		{
			name:     "Low default",
			amount:   decimal.NewFromInt(999),
			ageHours: 23,
			expected: exception.ExceptionSeverityLow,
		},
		{
			name:         "FX missing adds reason",
			amount:       decimal.NewFromInt(1),
			ageHours:     1,
			fxMissing:    true,
			expected:     exception.ExceptionSeverityLow,
			expectFXNote: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := exception.ClassifyExceptionSeverity(
				exception.SeverityClassificationInput{
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
				assert.Contains(t, result.Reasons, exception.ReasonFXRateUnavailable)
			} else {
				assert.NotContains(t, result.Reasons, exception.ReasonFXRateUnavailable)
			}
		})
	}
}

func TestClassifyExceptionSeverity_InvalidRuleSeverity(t *testing.T) {
	t.Parallel()

	rules := []exception.SeverityRule{
		{Severity: exception.ExceptionSeverity("BAD")},
	}

	_, err := exception.ClassifyExceptionSeverity(exception.SeverityClassificationInput{
		AmountAbsBase: decimal.NewFromInt(1),
		AgeHours:      1,
	}, rules)
	require.ErrorIs(t, err, exception.ErrInvalidExceptionSeverity)
}

func TestClassifyExceptionSeverity_BoundaryValues(t *testing.T) {
	t.Parallel()

	rules := exception.DefaultSeverityRules(nil)

	tests := []struct {
		name     string
		amount   decimal.Decimal
		ageHours int
		expected exception.ExceptionSeverity
	}{
		// Amount boundaries - verify >= comparison
		{
			name:     "Amount 999 -> LOW",
			amount:   decimal.NewFromInt(999),
			ageHours: 0,
			expected: exception.ExceptionSeverityLow,
		},
		{
			name:     "Amount 1000 -> MEDIUM",
			amount:   decimal.NewFromInt(1000),
			ageHours: 0,
			expected: exception.ExceptionSeverityMedium,
		},
		{
			name:     "Amount 1001 -> MEDIUM",
			amount:   decimal.NewFromInt(1001),
			ageHours: 0,
			expected: exception.ExceptionSeverityMedium,
		},
		{
			name:     "Amount 9999 -> MEDIUM",
			amount:   decimal.NewFromInt(9999),
			ageHours: 0,
			expected: exception.ExceptionSeverityMedium,
		},
		{
			name:     "Amount 10000 -> HIGH",
			amount:   decimal.NewFromInt(10000),
			ageHours: 0,
			expected: exception.ExceptionSeverityHigh,
		},
		{
			name:     "Amount 10001 -> HIGH",
			amount:   decimal.NewFromInt(10001),
			ageHours: 0,
			expected: exception.ExceptionSeverityHigh,
		},
		{
			name:     "Amount 99999 -> HIGH",
			amount:   decimal.NewFromInt(99999),
			ageHours: 0,
			expected: exception.ExceptionSeverityHigh,
		},
		{
			name:     "Amount 100000 -> CRITICAL",
			amount:   decimal.NewFromInt(100000),
			ageHours: 0,
			expected: exception.ExceptionSeverityCritical,
		},
		{
			name:     "Amount 100001 -> CRITICAL",
			amount:   decimal.NewFromInt(100001),
			ageHours: 0,
			expected: exception.ExceptionSeverityCritical,
		},

		// Age boundaries - verify >= comparison
		{
			name:     "Age 23h -> LOW",
			amount:   decimal.NewFromInt(1),
			ageHours: 23,
			expected: exception.ExceptionSeverityLow,
		},
		{
			name:     "Age 24h -> MEDIUM",
			amount:   decimal.NewFromInt(1),
			ageHours: 24,
			expected: exception.ExceptionSeverityMedium,
		},
		{
			name:     "Age 25h -> MEDIUM",
			amount:   decimal.NewFromInt(1),
			ageHours: 25,
			expected: exception.ExceptionSeverityMedium,
		},
		{
			name:     "Age 71h -> MEDIUM",
			amount:   decimal.NewFromInt(1),
			ageHours: 71,
			expected: exception.ExceptionSeverityMedium,
		},
		{
			name:     "Age 72h -> HIGH",
			amount:   decimal.NewFromInt(1),
			ageHours: 72,
			expected: exception.ExceptionSeverityHigh,
		},
		{
			name:     "Age 73h -> HIGH",
			amount:   decimal.NewFromInt(1),
			ageHours: 73,
			expected: exception.ExceptionSeverityHigh,
		},
		{
			name:     "Age 119h -> HIGH",
			amount:   decimal.NewFromInt(1),
			ageHours: 119,
			expected: exception.ExceptionSeverityHigh,
		},
		{
			name:     "Age 120h -> CRITICAL",
			amount:   decimal.NewFromInt(1),
			ageHours: 120,
			expected: exception.ExceptionSeverityCritical,
		},
		{
			name:     "Age 121h -> CRITICAL",
			amount:   decimal.NewFromInt(1),
			ageHours: 121,
			expected: exception.ExceptionSeverityCritical,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := exception.ClassifyExceptionSeverity(
				exception.SeverityClassificationInput{
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

	rules := exception.DefaultSeverityRules(nil)

	tests := []struct {
		name     string
		amount   decimal.Decimal
		expected exception.ExceptionSeverity
	}{
		{
			name:     "Negative -100000 -> CRITICAL",
			amount:   decimal.NewFromInt(-100000),
			expected: exception.ExceptionSeverityCritical,
		},
		{
			name:     "Negative -10000 -> HIGH",
			amount:   decimal.NewFromInt(-10000),
			expected: exception.ExceptionSeverityHigh,
		},
		{
			name:     "Negative -1000 -> MEDIUM",
			amount:   decimal.NewFromInt(-1000),
			expected: exception.ExceptionSeverityMedium,
		},
		{
			name:     "Negative -999 -> LOW",
			amount:   decimal.NewFromInt(-999),
			expected: exception.ExceptionSeverityLow,
		},
		{
			name:     "Negative -1 -> LOW",
			amount:   decimal.NewFromInt(-1),
			expected: exception.ExceptionSeverityLow,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := exception.ClassifyExceptionSeverity(
				exception.SeverityClassificationInput{
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
