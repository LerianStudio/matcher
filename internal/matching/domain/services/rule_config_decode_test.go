// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package services

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestRuleDefinitionFromMatchRule_Nil(t *testing.T) {
	t.Parallel()

	_, err := RuleDefinitionFromMatchRule(nil)
	require.ErrorIs(t, err, ErrNilMatchRule)
}

func TestRuleDefinitionFromMatchRule_InvalidType(t *testing.T) {
	t.Parallel()

	rule := newMatchRule(shared.RuleType("UNKNOWN"), nil)
	_, err := RuleDefinitionFromMatchRule(rule)
	require.ErrorIs(t, err, ErrUnsupportedRuleType)
}

func TestDecodeRuleDefinition_ExactDefaults(t *testing.T) {
	t.Parallel()

	rule := newMatchRule(shared.RuleTypeExact, nil)
	def, err := DecodeRuleDefinition(rule)
	require.NoError(t, err)
	require.NotNil(t, def.Exact)
	require.Equal(t, DatePrecisionDay, def.Exact.DatePrecision)
	require.True(t, def.Exact.MatchAmount)
	require.True(t, def.Exact.MatchCurrency)
	require.True(t, def.Exact.MatchDate)
	require.True(t, def.Exact.MatchReference)
	require.True(t, def.Exact.CaseInsensitive)
	require.Equal(t, defaultExactScore, def.Exact.MatchScore)
	require.Equal(t, defaultExactBaseScore, def.Exact.MatchBaseScore)
	require.NotNil(t, def.Allocation)
	require.Equal(t, AllocationDirectionLeftToRight, def.Allocation.Direction)
	require.Equal(t, AllocationToleranceAbsolute, def.Allocation.ToleranceMode)
}

func TestDecodeRuleDefinition_Tolerance(t *testing.T) {
	t.Parallel()

	rule := newMatchRule(shared.RuleTypeTolerance, map[string]any{
		"absTolerance":     "1.25",
		"percentTolerance": "0.10",
	})
	def, err := DecodeRuleDefinition(rule)
	require.NoError(t, err)
	require.NotNil(t, def.Tolerance)
	require.Equal(t, decimal.RequireFromString("1.25"), def.Tolerance.AbsAmountTolerance)
	require.Equal(t, decimal.RequireFromString("0.10"), def.Tolerance.PercentTolerance)
	require.Equal(t, defaultToleranceScore, def.Tolerance.MatchScore)
	require.Equal(t, defaultToleranceBase, def.Tolerance.MatchBaseScore)
	require.NotNil(t, def.Allocation)
	require.Equal(t, AllocationDirectionLeftToRight, def.Allocation.Direction)
}

func TestDecodeRuleDefinition_Tolerance_AllocationUseBaseAmount(t *testing.T) {
	t.Parallel()

	rule := newMatchRule(shared.RuleTypeTolerance, map[string]any{
		"allocationUseBaseAmount": true,
		"absTolerance":            "1.00",
	})
	def, err := DecodeRuleDefinition(rule)
	require.NoError(t, err)
	require.NotNil(t, def.Allocation)
	require.True(t, def.Allocation.UseBaseAmount)
	require.True(t, def.Tolerance.MatchBaseAmount)
	require.True(t, def.Tolerance.MatchBaseCurrency)
}

func TestDecodeRuleDefinition_Tolerance_BaseMatchingFlags(t *testing.T) {
	t.Parallel()

	rule := newMatchRule(shared.RuleTypeTolerance, map[string]any{
		"matchBaseAmount":   true,
		"matchBaseCurrency": true,
		"matchCurrency":     true,
		"absTolerance":      "0.10",
		"percentTolerance":  "0.00",
		"roundingScale":     2,
		"roundingMode":      "HALF_UP",
		"dateWindowDays":    0,
	})

	def, err := DecodeRuleDefinition(rule)
	require.NoError(t, err)
	require.NotNil(t, def.Tolerance)
	require.True(t, def.Tolerance.MatchBaseAmount)
	require.True(t, def.Tolerance.MatchBaseCurrency)
	require.True(t, def.Tolerance.MatchCurrency)
	require.Equal(t, decimal.RequireFromString("0.10"), def.Tolerance.AbsAmountTolerance)
	require.Equal(t, decimal.RequireFromString("0.00"), def.Tolerance.PercentTolerance)
	require.Equal(t, 2, def.Tolerance.RoundingScale)
	require.Equal(t, RoundingHalfUp, def.Tolerance.RoundingMode)
	require.Equal(t, 0, def.Tolerance.DateWindowDays)
	// When matchBaseAmount is true and allocationUseBaseAmount is not explicitly false,
	// allocation.UseBaseAmount should be aligned to true for consistency
	require.True(
		t,
		def.Allocation.UseBaseAmount,
		"allocation should use base amount when base matching is enabled",
	)
}

func TestDecodeRuleDefinition_Tolerance_ExplicitAllocationOverride(t *testing.T) {
	t.Parallel()

	// When allocationUseBaseAmount is explicitly set to false, it should not be overridden
	rule := newMatchRule(shared.RuleTypeTolerance, map[string]any{
		"matchBaseAmount":         true,
		"matchBaseCurrency":       true,
		"allocationUseBaseAmount": false,
		"absTolerance":            "0.10",
	})

	def, err := DecodeRuleDefinition(rule)
	require.NoError(t, err)
	require.NotNil(t, def.Tolerance)
	require.True(t, def.Tolerance.MatchBaseAmount)
	require.True(t, def.Tolerance.MatchBaseCurrency)
	require.False(t, def.Allocation.UseBaseAmount, "explicit false should not be overridden")
}

func TestDecodeRuleDefinition_Tolerance_ReferenceFieldsDefaults(t *testing.T) {
	t.Parallel()

	// When no reference fields are specified, defaults should match ExactConfig for consistency
	rule := newMatchRule(shared.RuleTypeTolerance, map[string]any{
		"absTolerance": "1.00",
	})

	def, err := DecodeRuleDefinition(rule)
	require.NoError(t, err)
	require.NotNil(t, def.Tolerance)
	require.True(
		t,
		def.Tolerance.MatchReference,
		"matchReference should default to true (consistent with ExactConfig)",
	)
	require.True(t, def.Tolerance.CaseInsensitive, "caseInsensitive should default to true")
	require.False(t, def.Tolerance.ReferenceMustSet, "referenceMustSet should default to false")
}

func TestDecodeRuleDefinition_Tolerance_ReferenceFieldsEnabled(t *testing.T) {
	t.Parallel()

	rule := newMatchRule(shared.RuleTypeTolerance, map[string]any{
		"absTolerance":     "1.00",
		"matchReference":   true,
		"caseInsensitive":  false,
		"referenceMustSet": true,
	})

	def, err := DecodeRuleDefinition(rule)
	require.NoError(t, err)
	require.NotNil(t, def.Tolerance)
	require.True(t, def.Tolerance.MatchReference, "matchReference should be true when set")
	require.False(t, def.Tolerance.CaseInsensitive, "caseInsensitive should be false when set")
	require.True(t, def.Tolerance.ReferenceMustSet, "referenceMustSet should be true when set")
}

func TestDecodeRuleDefinition_DateLag(t *testing.T) {
	t.Parallel()

	rule := newMatchRule(shared.RuleTypeDateLag, map[string]any{
		"minDays":   1,
		"maxDays":   3,
		"direction": string(DateLagDirectionLeftBeforeRight),
	})
	def, err := DecodeRuleDefinition(rule)
	require.NoError(t, err)
	require.NotNil(t, def.DateLag)
	require.Equal(t, 1, def.DateLag.MinDays)
	require.Equal(t, 3, def.DateLag.MaxDays)
	require.Equal(t, DateLagDirectionLeftBeforeRight, def.DateLag.Direction)
	require.Equal(t, decimal.Zero, def.DateLag.FeeTolerance)
	require.Equal(t, defaultDateLagScore, def.DateLag.MatchScore)
	require.NotNil(t, def.Allocation)
}

func TestDecodeExactConfig_InvalidDatePrecision(t *testing.T) {
	t.Parallel()

	_, err := decodeExactConfig(map[string]any{"datePrecision": "BAD"})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRuleConfigDecode)

	cfg, err := decodeExactConfig(map[string]any{"matchScore": 0})
	require.NoError(t, err)
	require.Equal(t, 0, cfg.MatchScore)
}

func TestDecodeExactConfig_Overrides(t *testing.T) {
	t.Parallel()

	cfg, err := decodeExactConfig(map[string]any{
		"matchCurrency":   false,
		"matchReference":  false,
		"caseInsensitive": false,
		"datePrecision":   "TIMESTAMP",
		"matchScore":      75,
	})
	require.NoError(t, err)
	require.False(t, cfg.MatchCurrency)
	require.False(t, cfg.MatchReference)
	require.False(t, cfg.CaseInsensitive)
	require.Equal(t, DatePrecisionTimestamp, cfg.DatePrecision)
	require.Equal(t, 75, cfg.MatchScore)
}

func TestDecodeToleranceConfig_Defaults(t *testing.T) {
	t.Parallel()

	cfg, err := decodeToleranceConfig(nil)
	require.NoError(t, err)
	require.True(t, cfg.MatchCurrency)
	require.Equal(t, 0, cfg.DateWindowDays)
	require.Equal(t, defaultRoundingScale, cfg.RoundingScale)
	require.Equal(t, RoundingHalfUp, cfg.RoundingMode)
	require.Equal(t, decimal.RequireFromString("0.50"), cfg.AbsAmountTolerance)
	require.Equal(t, decimal.RequireFromString("0.005"), cfg.PercentTolerance)
	require.Equal(t, defaultToleranceScore, cfg.MatchScore)
	require.Equal(t, defaultToleranceBase, cfg.MatchBaseScore)
}

func TestDecodeToleranceConfig_InvalidRoundingMode(t *testing.T) {
	t.Parallel()

	_, err := decodeToleranceConfig(map[string]any{"roundingMode": "BAD"})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRuleConfigDecode)
}

func TestDecodeToleranceConfig_RoundingScaleTooLarge(t *testing.T) {
	t.Parallel()

	_, err := decodeToleranceConfig(map[string]any{"roundingScale": 11})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRuleConfigDecode)
}

func TestDecodeToleranceConfig_DateWindowTooLarge(t *testing.T) {
	t.Parallel()

	_, err := decodeToleranceConfig(map[string]any{"dateWindowDays": 3651})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRuleConfigDecode)
}

func TestDecodeToleranceConfig_NegativeDateWindowDays(t *testing.T) {
	t.Parallel()

	_, err := decodeToleranceConfig(map[string]any{"dateWindowDays": -1})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRuleConfigDecode)
}

func TestDecodeToleranceConfig_NegativeRoundingScale(t *testing.T) {
	t.Parallel()

	_, err := decodeToleranceConfig(map[string]any{"roundingScale": -1})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRuleConfigDecode)
}

func TestDecodeToleranceConfig_InvalidBool(t *testing.T) {
	t.Parallel()

	_, err := decodeToleranceConfig(map[string]any{"matchCurrency": "yes"})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRuleConfigDecode)
}

func TestDecodeToleranceConfig_InvalidAbsTolerance(t *testing.T) {
	t.Parallel()

	_, err := decodeToleranceConfig(map[string]any{"absTolerance": "-1"})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRuleConfigDecode)

	_, err = decodeToleranceConfig(map[string]any{"percentTolerance": "-0.1"})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRuleConfigDecode)

	cfg, err := decodeToleranceConfig(map[string]any{"matchScore": 0})
	require.NoError(t, err)
	require.Equal(t, 0, cfg.MatchScore)

	cfg, err = decodeToleranceConfig(map[string]any{"matchBaseAmount": true, "matchBaseScore": 0})
	require.NoError(t, err)
	require.Equal(t, 0, cfg.MatchBaseScore)
	require.True(t, cfg.MatchBaseAmount)
}

func TestDecodeToleranceConfig_NegativePercentTolerance(t *testing.T) {
	t.Parallel()

	_, err := decodeToleranceConfig(map[string]any{"percentTolerance": "-0.1"})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRuleConfigDecode)
}

func TestDecodeToleranceConfig_FloatTolerance(t *testing.T) {
	t.Parallel()

	cfg, err := decodeToleranceConfig(map[string]any{
		"absTolerance":     0.5,
		"percentTolerance": 0.1,
	})
	require.NoError(t, err)
	require.Equal(t, decimal.RequireFromString("0.5"), cfg.AbsAmountTolerance)
	require.Equal(t, decimal.RequireFromString("0.1"), cfg.PercentTolerance)
}

func TestDecodeToleranceConfig_PercentageBaseDefault(t *testing.T) {
	t.Parallel()

	cfg, err := decodeToleranceConfig(map[string]any{})
	require.NoError(t, err)
	require.Equal(t, TolerancePercentageBaseMax, cfg.PercentageBase, "default percentageBase should be MAX")
}

func TestDecodeToleranceConfig_PercentageBaseValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		value    string
		expected TolerancePercentageBase
	}{
		{name: "MAX", value: "MAX", expected: TolerancePercentageBaseMax},
		{name: "MIN", value: "MIN", expected: TolerancePercentageBaseMin},
		{name: "AVERAGE", value: "AVERAGE", expected: TolerancePercentageBaseAvg},
		{name: "LEFT", value: "LEFT", expected: TolerancePercentageBaseLeft},
		{name: "RIGHT", value: "RIGHT", expected: TolerancePercentageBaseRight},
		{name: "lowercase max", value: "max", expected: TolerancePercentageBaseMax},
		{name: "mixed case Min", value: "Min", expected: TolerancePercentageBaseMin},
		{name: "lowercase average", value: "average", expected: TolerancePercentageBaseAvg},
		{name: "mixed case Left", value: "Left", expected: TolerancePercentageBaseLeft},
		{name: "lowercase right", value: "right", expected: TolerancePercentageBaseRight},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := decodeToleranceConfig(map[string]any{"percentageBase": tt.value})
			require.NoError(t, err)
			require.Equal(t, tt.expected, cfg.PercentageBase)
		})
	}
}

func TestDecodeToleranceConfig_PercentageBaseInvalid(t *testing.T) {
	t.Parallel()

	_, err := decodeToleranceConfig(map[string]any{"percentageBase": "BAD"})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRuleConfigDecode)
}

func TestDecodeToleranceConfig_PercentageBaseNonString(t *testing.T) {
	t.Parallel()

	_, err := decodeToleranceConfig(map[string]any{"percentageBase": 123})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRuleConfigDecode)
}

func TestDecodeToleranceConfig_FractionalDateWindowDays(t *testing.T) {
	t.Parallel()

	_, err := decodeToleranceConfig(map[string]any{"dateWindowDays": 1.5})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRuleConfigDecode)
}

func TestDecodeDateLagConfig_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config map[string]any
	}{
		{name: "invalid direction", config: map[string]any{"direction": "BAD"}},
		{name: "negative minDays", config: map[string]any{"minDays": -1}},
		{name: "minDays greater than maxDays", config: map[string]any{"minDays": 5, "maxDays": 2}},
		{name: "negative feeTolerance", config: map[string]any{"feeTolerance": "-1"}},
		{name: "exclusive bounds with minDays zero excludes same-day", config: map[string]any{"inclusive": false, "minDays": 0, "maxDays": 5}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := decodeDateLagConfig(tt.config)
			require.Error(t, err)
			require.ErrorIs(t, err, ErrRuleConfigDecode)
		})
	}
}

func TestDecodeDateLagConfig_Valid(t *testing.T) {
	t.Parallel()

	cfg, err := decodeDateLagConfig(map[string]any{
		"minDays":       1,
		"maxDays":       4,
		"inclusive":     false,
		"direction":     string(DateLagDirectionRightBeforeLeft),
		"feeTolerance":  "1.25",
		"matchCurrency": false,
		"matchScore":    70,
	})
	require.NoError(t, err)
	require.Equal(t, 1, cfg.MinDays)
	require.Equal(t, 4, cfg.MaxDays)
	require.False(t, cfg.Inclusive)
	require.Equal(t, DateLagDirectionRightBeforeLeft, cfg.Direction)
	require.Equal(t, decimal.RequireFromString("1.25"), cfg.FeeTolerance)
	require.False(t, cfg.MatchCurrency)
	require.Equal(t, 70, cfg.MatchScore)
}

func TestDecodeDateLagConfig_InclusiveBoundsWithMinDaysZero(t *testing.T) {
	t.Parallel()

	cfg, err := decodeDateLagConfig(map[string]any{
		"inclusive": true,
		"minDays":   0,
		"maxDays":   5,
	})
	require.NoError(t, err)
	require.True(t, cfg.Inclusive)
	require.Equal(t, 0, cfg.MinDays)
	require.Equal(t, 5, cfg.MaxDays)
}

func TestDecodeDateLagConfig_Defaults(t *testing.T) {
	t.Parallel()

	cfg, err := decodeDateLagConfig(nil)
	require.NoError(t, err)
	require.Equal(t, 0, cfg.MinDays)
	require.Equal(t, 0, cfg.MaxDays)
	require.True(t, cfg.Inclusive)
	require.Equal(t, DateLagDirectionAbs, cfg.Direction)
	require.Equal(t, decimal.Zero, cfg.FeeTolerance)
	require.True(t, cfg.MatchCurrency)
	require.Equal(t, defaultDateLagScore, cfg.MatchScore)
}

func TestDecodeDateLagConfig_MaxDaysTooLarge(t *testing.T) {
	t.Parallel()

	_, err := decodeDateLagConfig(map[string]any{"minDays": 0, "maxDays": 4000})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRuleConfigDecode)
}

func newMatchRule(ruleType shared.RuleType, config map[string]any) *shared.MatchRule {
	return &shared.MatchRule{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Priority:  1,
		Type:      ruleType,
		Config:    config,
	}
}

func TestDecodeAllocationConfig(t *testing.T) {
	t.Parallel()

	cfg, err := decodeAllocationConfig(map[string]any{
		"allowPartial":             true,
		"allocationUseBaseAmount":  true,
		"allocationToleranceValue": "1.5",
		"allocationDirection":      string(AllocationDirectionRightToLeft),
		"allocationToleranceMode":  string(AllocationTolerancePercent),
	})
	require.NoError(t, err)
	require.True(t, cfg.AllowPartial)
	require.True(t, cfg.UseBaseAmount)
	require.Equal(t, decimal.RequireFromString("1.5"), cfg.ToleranceValue)
	require.Equal(t, AllocationDirectionRightToLeft, cfg.Direction)
	require.Equal(t, AllocationTolerancePercent, cfg.ToleranceMode)

	_, err = decodeAllocationConfig(map[string]any{"allocationToleranceValue": "-1"})
	require.ErrorIs(t, err, ErrRuleConfigDecode)

	_, err = decodeAllocationConfig(map[string]any{"allocationDirection": "BAD"})
	require.ErrorIs(t, err, ErrRuleConfigDecode)

	_, err = decodeAllocationConfig(map[string]any{"allocationToleranceMode": "BAD"})
	require.ErrorIs(t, err, ErrRuleConfigDecode)
}

func TestGetIntSupportsJSONNumber(t *testing.T) {
	t.Parallel()

	m := map[string]any{"value": json.Number("42")}
	value, err := getInt(m, "value", 0)
	require.NoError(t, err)
	require.Equal(t, 42, value)
}

func TestValidateRuleConfig_Valid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ruleType shared.RuleType
		config   map[string]any
	}{
		{
			name:     "exact with match amount",
			ruleType: shared.RuleTypeExact,
			config:   map[string]any{"matchAmount": true},
		},
		{
			name:     "tolerance with date window",
			ruleType: shared.RuleTypeTolerance,
			config:   map[string]any{"dateWindowDays": 5},
		},
		{
			name:     "date_lag with min/max days",
			ruleType: shared.RuleTypeDateLag,
			config:   map[string]any{"minDays": 1, "maxDays": 3},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateRuleConfig(tc.ruleType, tc.config)
			require.NoError(t, err)
		})
	}
}

func TestValidateRuleConfig_Invalid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		ruleType    shared.RuleType
		config      map[string]any
		expectError bool
	}{
		{
			name:        "nil config",
			ruleType:    shared.RuleTypeExact,
			config:      nil,
			expectError: true,
		},
		{
			name:        "empty config",
			ruleType:    shared.RuleTypeExact,
			config:      map[string]any{},
			expectError: true,
		},
		{
			name:        "tolerance with negative date window",
			ruleType:    shared.RuleTypeTolerance,
			config:      map[string]any{"dateWindowDays": -1},
			expectError: true,
		},
		{
			name:        "date_lag with max less than min",
			ruleType:    shared.RuleTypeDateLag,
			config:      map[string]any{"minDays": 5, "maxDays": 2},
			expectError: true,
		},
		{
			name:        "exact with invalid score",
			ruleType:    shared.RuleTypeExact,
			config:      map[string]any{"matchScore": 150},
			expectError: true,
		},
		{
			name:        "unsupported rule type",
			ruleType:    shared.RuleType("INVALID"),
			config:      map[string]any{"anyKey": "anyValue"},
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateRuleConfig(tc.ruleType, tc.config)
			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
