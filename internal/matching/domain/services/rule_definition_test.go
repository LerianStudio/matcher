// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package services

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestSentinelErrors(t *testing.T) {
	t.Parallel()

	t.Run("ErrNilMatchRule", func(t *testing.T) {
		t.Parallel()
		require.Error(t, ErrNilMatchRule)
		require.Equal(t, "match rule is nil", ErrNilMatchRule.Error())
	})

	t.Run("ErrUnsupportedRuleType", func(t *testing.T) {
		t.Parallel()
		require.Error(t, ErrUnsupportedRuleType)
		require.Equal(t, "unsupported rule type", ErrUnsupportedRuleType.Error())
	})
}

func TestDatePrecisionConstants(t *testing.T) {
	t.Parallel()

	t.Run("DAY", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, DatePrecisionDay, DatePrecision("DAY"))
	})

	t.Run("TIMESTAMP", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, DatePrecisionTimestamp, DatePrecision("TIMESTAMP"))
	})
}

func TestRoundingModeConstants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		constant RoundingMode
		expected string
	}{
		{"HALF_UP", RoundingHalfUp, "HALF_UP"},
		{"BANKERS", RoundingBankers, "BANKERS"},
		{"FLOOR", RoundingFloor, "FLOOR"},
		{"CEIL", RoundingCeil, "CEIL"},
		{"TRUNCATE", RoundingTruncate, "TRUNCATE"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.constant, RoundingMode(tc.expected))
		})
	}
}

func TestDateLagDirectionConstants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		constant DateLagDirection
		expected string
	}{
		{"ABS", DateLagDirectionAbs, "ABS"},
		{"LEFT_BEFORE_RIGHT", DateLagDirectionLeftBeforeRight, "LEFT_BEFORE_RIGHT"},
		{"RIGHT_BEFORE_LEFT", DateLagDirectionRightBeforeLeft, "RIGHT_BEFORE_LEFT"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.constant, DateLagDirection(tc.expected))
		})
	}
}

func TestAllocationDirectionConstants(t *testing.T) {
	t.Parallel()

	t.Run("LEFT_TO_RIGHT", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, AllocationDirectionLeftToRight, AllocationDirection("LEFT_TO_RIGHT"))
	})

	t.Run("RIGHT_TO_LEFT", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, AllocationDirectionRightToLeft, AllocationDirection("RIGHT_TO_LEFT"))
	})
}

func TestAllocationToleranceModeConstants(t *testing.T) {
	t.Parallel()

	t.Run("ABS", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, AllocationToleranceAbsolute, AllocationToleranceMode("ABS"))
	})

	t.Run("PERCENT", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, AllocationTolerancePercent, AllocationToleranceMode("PERCENT"))
	})
}

func TestExactConfig_Instantiation(t *testing.T) {
	t.Parallel()

	cfg := ExactConfig{
		MatchAmount:       true,
		MatchCurrency:     true,
		MatchDate:         true,
		DatePrecision:     DatePrecisionDay,
		MatchReference:    true,
		CaseInsensitive:   true,
		ReferenceMustSet:  false,
		MatchBaseAmount:   true,
		MatchBaseCurrency: true,
		MatchScore:        100,
		MatchBaseScore:    80,
	}

	require.True(t, cfg.MatchAmount)
	require.True(t, cfg.MatchCurrency)
	require.True(t, cfg.MatchDate)
	require.Equal(t, DatePrecisionDay, cfg.DatePrecision)
	require.True(t, cfg.MatchReference)
	require.True(t, cfg.CaseInsensitive)
	require.False(t, cfg.ReferenceMustSet)
	require.True(t, cfg.MatchBaseAmount)
	require.True(t, cfg.MatchBaseCurrency)
	require.Equal(t, 100, cfg.MatchScore)
	require.Equal(t, 80, cfg.MatchBaseScore)
}

func TestToleranceConfig_Instantiation(t *testing.T) {
	t.Parallel()

	cfg := ToleranceConfig{
		MatchCurrency:      true,
		DateWindowDays:     5,
		AbsAmountTolerance: decimal.RequireFromString("0.01"),
		PercentTolerance:   decimal.RequireFromString("0.05"),
		RoundingScale:      2,
		RoundingMode:       RoundingHalfUp,
		MatchBaseAmount:    true,
		MatchBaseCurrency:  true,
		MatchScore:         90,
		MatchBaseScore:     75,
	}

	require.True(t, cfg.MatchCurrency)
	require.Equal(t, 5, cfg.DateWindowDays)
	require.True(t, decimal.RequireFromString("0.01").Equal(cfg.AbsAmountTolerance))
	require.True(t, decimal.RequireFromString("0.05").Equal(cfg.PercentTolerance))
	require.Equal(t, 2, cfg.RoundingScale)
	require.Equal(t, RoundingHalfUp, cfg.RoundingMode)
	require.True(t, cfg.MatchBaseAmount)
	require.True(t, cfg.MatchBaseCurrency)
	require.Equal(t, 90, cfg.MatchScore)
	require.Equal(t, 75, cfg.MatchBaseScore)
}

func TestDateLagConfig_Instantiation(t *testing.T) {
	t.Parallel()

	cfg := DateLagConfig{
		MinDays:       1,
		MaxDays:       7,
		Inclusive:     true,
		Direction:     DateLagDirectionLeftBeforeRight,
		FeeTolerance:  decimal.RequireFromString("0.50"),
		MatchScore:    85,
		MatchCurrency: true,
	}

	require.Equal(t, 1, cfg.MinDays)
	require.Equal(t, 7, cfg.MaxDays)
	require.True(t, cfg.Inclusive)
	require.Equal(t, DateLagDirectionLeftBeforeRight, cfg.Direction)
	require.True(t, decimal.RequireFromString("0.50").Equal(cfg.FeeTolerance))
	require.Equal(t, 85, cfg.MatchScore)
	require.True(t, cfg.MatchCurrency)
}

func TestAllocationConfig_Instantiation(t *testing.T) {
	t.Parallel()

	cfg := AllocationConfig{
		AllowPartial:   true,
		Direction:      AllocationDirectionLeftToRight,
		ToleranceMode:  AllocationTolerancePercent,
		ToleranceValue: decimal.RequireFromString("0.02"),
		UseBaseAmount:  true,
	}

	require.True(t, cfg.AllowPartial)
	require.Equal(t, AllocationDirectionLeftToRight, cfg.Direction)
	require.Equal(t, AllocationTolerancePercent, cfg.ToleranceMode)
	require.True(t, decimal.RequireFromString("0.02").Equal(cfg.ToleranceValue))
	require.True(t, cfg.UseBaseAmount)
}

func TestRuleDefinition_Instantiation(t *testing.T) {
	t.Parallel()

	id := uuid.New()
	exactCfg := &ExactConfig{MatchAmount: true}
	toleranceCfg := &ToleranceConfig{DateWindowDays: 3}
	dateLagCfg := &DateLagConfig{MinDays: 1, MaxDays: 5}
	allocCfg := &AllocationConfig{AllowPartial: true}

	rd := RuleDefinition{
		ID:         id,
		Priority:   10,
		Type:       shared.RuleTypeExact,
		Exact:      exactCfg,
		Tolerance:  toleranceCfg,
		DateLag:    dateLagCfg,
		Allocation: allocCfg,
	}

	require.Equal(t, id, rd.ID)
	require.Equal(t, 10, rd.Priority)
	require.Equal(t, shared.RuleTypeExact, rd.Type)
	require.NotNil(t, rd.Exact)
	require.True(t, rd.Exact.MatchAmount)
	require.NotNil(t, rd.Tolerance)
	require.Equal(t, 3, rd.Tolerance.DateWindowDays)
	require.NotNil(t, rd.DateLag)
	require.Equal(t, 1, rd.DateLag.MinDays)
	require.NotNil(t, rd.Allocation)
	require.True(t, rd.Allocation.AllowPartial)
}

func TestRuleDefinitionFromMatchRule_NilRule(t *testing.T) {
	t.Parallel()

	rd, err := RuleDefinitionFromMatchRule(nil)

	require.ErrorIs(t, err, ErrNilMatchRule)
	require.Equal(t, RuleDefinition{}, rd)
}

func TestRuleDefinitionFromMatchRule_InvalidRuleType(t *testing.T) {
	t.Parallel()

	rule := &shared.MatchRule{
		ID:       uuid.New(),
		Priority: 1,
		Type:     shared.RuleType("INVALID_TYPE"),
	}

	rd, err := RuleDefinitionFromMatchRule(rule)

	require.ErrorIs(t, err, ErrUnsupportedRuleType)
	require.Equal(t, RuleDefinition{}, rd)
}

func TestRuleDefinitionFromMatchRule_ValidRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ruleType shared.RuleType
	}{
		{"EXACT", shared.RuleTypeExact},
		{"TOLERANCE", shared.RuleTypeTolerance},
		{"DATE_LAG", shared.RuleTypeDateLag},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			id := uuid.New()
			contextID := uuid.New()
			now := time.Now().UTC()

			rule := &shared.MatchRule{
				ID:        id,
				ContextID: contextID,
				Priority:  5,
				Type:      tc.ruleType,
				Config:    map[string]any{"key": "value"},
				CreatedAt: now,
				UpdatedAt: now,
			}

			rd, err := RuleDefinitionFromMatchRule(rule)

			require.NoError(t, err)
			require.Equal(t, id, rd.ID)
			require.Equal(t, 5, rd.Priority)
			require.Equal(t, tc.ruleType, rd.Type)
		})
	}
}
