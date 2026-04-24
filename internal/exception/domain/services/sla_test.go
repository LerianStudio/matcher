// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package services

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestComputeSLADueAt_DefaultRules(t *testing.T) {
	t.Parallel()

	reference := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rules := DefaultSLARules()

	tests := []struct {
		name          string
		input         SLAInput
		expectedRule  string
		expectedHours int
	}{
		{
			name: "critical amount",
			input: SLAInput{
				AmountAbsBase: decimal.NewFromInt(200000),
				AgeHours:      1,
				ReferenceTime: reference,
			},
			expectedRule:  "CRITICAL",
			expectedHours: slaCriticalDueHours,
		},
		{
			name: "high amount",
			input: SLAInput{
				AmountAbsBase: decimal.NewFromInt(20000),
				AgeHours:      1,
				ReferenceTime: reference,
			},
			expectedRule:  "HIGH",
			expectedHours: slaHighDueHours,
		},
		{
			name: "medium amount",
			input: SLAInput{
				AmountAbsBase: decimal.NewFromInt(2000),
				AgeHours:      1,
				ReferenceTime: reference,
			},
			expectedRule:  "MEDIUM",
			expectedHours: slaMediumDueHours,
		},
		{
			name: "low fallback",
			input: SLAInput{
				AmountAbsBase: decimal.NewFromInt(100),
				AgeHours:      1,
				ReferenceTime: reference,
			},
			expectedRule:  "LOW",
			expectedHours: slaLowDueHours,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := ComputeSLADueAt(tt.input, rules)
			require.NoError(t, err)
			require.Equal(t, tt.expectedRule, result.RuleName)
			require.Equal(t, reference.Add(time.Duration(tt.expectedHours)*time.Hour), result.DueAt)
		})
	}
}

func TestComputeSLADueAt_InvalidRules(t *testing.T) {
	t.Parallel()

	_, err := ComputeSLADueAt(SLAInput{}, nil)
	require.ErrorIs(t, err, ErrEmptySLARules)

	_, err = ComputeSLADueAt(SLAInput{}, []SLARule{})
	require.ErrorIs(t, err, ErrEmptySLARules)

	rules := []SLARule{{Name: "BAD", DueIn: 0}}
	_, err = ComputeSLADueAt(SLAInput{}, rules)
	require.ErrorIs(t, err, ErrInvalidSLARule)
}

func TestComputeSLADueAt_NormalizesInput(t *testing.T) {
	t.Parallel()

	reference := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	rules := []SLARule{{Name: "RULE", MinAmountAbsBase: decimalPtr(10), DueIn: 2 * time.Hour}}

	result, err := ComputeSLADueAt(
		SLAInput{AmountAbsBase: decimal.NewFromInt(-20), AgeHours: -5, ReferenceTime: reference},
		rules,
	)
	require.NoError(t, err)
	require.Equal(t, reference.Add(2*time.Hour), result.DueAt)
}

func TestComputeSLADueAt_AgeThresholds(t *testing.T) {
	t.Parallel()

	reference := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rules := DefaultSLARules()

	result, err := ComputeSLADueAt(
		SLAInput{
			AmountAbsBase: decimal.NewFromInt(1),
			AgeHours:      slaCriticalAgeHours,
			ReferenceTime: reference,
		},
		rules,
	)
	require.NoError(t, err)
	require.Equal(t, "CRITICAL", result.RuleName)

	result, err = ComputeSLADueAt(
		SLAInput{
			AmountAbsBase: decimal.NewFromInt(1),
			AgeHours:      slaHighAgeHours,
			ReferenceTime: reference,
		},
		rules,
	)
	require.NoError(t, err)
	require.Equal(t, "HIGH", result.RuleName)

	result, err = ComputeSLADueAt(
		SLAInput{
			AmountAbsBase: decimal.NewFromInt(1),
			AgeHours:      slaMediumAgeHours,
			ReferenceTime: reference,
		},
		rules,
	)
	require.NoError(t, err)
	require.Equal(t, "MEDIUM", result.RuleName)

	result, err = ComputeSLADueAt(
		SLAInput{
			AmountAbsBase: decimal.NewFromInt(1),
			AgeHours:      slaCriticalAgeHours - 1,
			ReferenceTime: reference,
		},
		rules,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		"HIGH",
		result.RuleName,
		"AgeHours just below slaCriticalAgeHours should be HIGH",
	)

	result, err = ComputeSLADueAt(
		SLAInput{
			AmountAbsBase: decimal.NewFromInt(1),
			AgeHours:      slaHighAgeHours - 1,
			ReferenceTime: reference,
		},
		rules,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		"MEDIUM",
		result.RuleName,
		"AgeHours just below slaHighAgeHours should be MEDIUM",
	)

	result, err = ComputeSLADueAt(
		SLAInput{
			AmountAbsBase: decimal.NewFromInt(1),
			AgeHours:      slaMediumAgeHours - 1,
			ReferenceTime: reference,
		},
		rules,
	)
	require.NoError(t, err)
	require.Equal(t, "LOW", result.RuleName, "AgeHours just below slaMediumAgeHours should be LOW")
}

func decimalPtr(value int64) *decimal.Decimal {
	amount := decimal.NewFromInt(value)
	return &amount
}
