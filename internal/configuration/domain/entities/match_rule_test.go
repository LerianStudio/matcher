//go:build unit

package entities

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func testNewMatchRuleError(
	ctx context.Context,
	t *testing.T,
	contextID uuid.UUID,
	input CreateMatchRuleInput,
	expectedErr error,
) {
	t.Helper()

	_, err := NewMatchRule(ctx, contextID, input)
	require.Error(t, err)
	require.ErrorIs(t, err, expectedErr)
}

func testNewMatchRuleSuccess(
	ctx context.Context,
	t *testing.T,
	contextID uuid.UUID,
	input CreateMatchRuleInput,
) *MatchRule {
	t.Helper()

	rule, err := NewMatchRule(ctx, contextID, input)
	require.NoError(t, err)
	assert.Equal(t, input.Config, rule.Config)

	return rule
}

func testMatchRuleUpdateError(
	ctx context.Context,
	t *testing.T,
	rule *MatchRule,
	input UpdateMatchRuleInput,
	expectedErr error,
) {
	t.Helper()

	originalPriority := rule.Priority
	originalType := rule.Type
	originalConfig := rule.Config
	originalUpdatedAt := rule.UpdatedAt

	err := rule.Update(ctx, input)
	require.Error(t, err)
	require.ErrorIs(t, err, expectedErr)
	assert.Equal(t, originalPriority, rule.Priority)
	assert.Equal(t, originalType, rule.Type)
	assert.Equal(t, originalConfig, rule.Config)
	assert.Equal(t, originalUpdatedAt, rule.UpdatedAt)
}

func runBasicValidationTests(ctx context.Context, t *testing.T, contextID uuid.UUID) {
	t.Helper()

	t.Run("creates valid match rule", func(t *testing.T) {
		t.Parallel()

		input := CreateMatchRuleInput{
			Priority: 1,
			Type:     shared.RuleTypeExact,
			Config:   map[string]any{"matchCurrency": true},
		}
		rule, err := NewMatchRule(ctx, contextID, input)
		require.NoError(t, err)
		assert.NotEqual(t, uuid.Nil, rule.ID)
		assert.Equal(t, contextID, rule.ContextID)
		assert.Equal(t, 1, rule.Priority)
		assert.Equal(t, shared.RuleTypeExact, rule.Type)
	})

	t.Run("fails with nil context", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleError(
			ctx,
			t,
			uuid.Nil,
			CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleTypeExact,
				Config:   map[string]any{"matchCurrency": true},
			},
			ErrRuleContextRequired,
		)
	})

	t.Run("fails with invalid priority", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleError(
			ctx,
			t,
			contextID,
			CreateMatchRuleInput{
				Priority: 0,
				Type:     shared.RuleTypeExact,
				Config:   map[string]any{"matchCurrency": true},
			},
			ErrRulePriorityInvalid,
		)
	})

	t.Run("fails with negative priority", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleError(
			ctx,
			t,
			contextID,
			CreateMatchRuleInput{
				Priority: -1,
				Type:     shared.RuleTypeExact,
				Config:   map[string]any{"matchCurrency": true},
			},
			ErrRulePriorityInvalid,
		)
	})

	t.Run("fails with priority exceeding max", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleError(
			ctx,
			t,
			contextID,
			CreateMatchRuleInput{
				Priority: 1001,
				Type:     shared.RuleTypeExact,
				Config:   map[string]any{"matchCurrency": true},
			},
			ErrRulePriorityInvalid,
		)
	})

	t.Run("fails with invalid type", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleError(
			ctx,
			t,
			contextID,
			CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleType("INVALID"),
				Config:   map[string]any{"matchCurrency": true},
			},
			ErrRuleTypeInvalid,
		)
	})

	t.Run("fails with empty config", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleError(
			ctx,
			t,
			contextID,
			CreateMatchRuleInput{Priority: 1, Type: shared.RuleTypeExact, Config: map[string]any{}},
			ErrRuleConfigRequired,
		)
	})

	t.Run("fails with nil config", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleError(
			ctx,
			t,
			contextID,
			CreateMatchRuleInput{Priority: 1, Type: shared.RuleTypeExact, Config: nil},
			ErrRuleConfigRequired,
		)
	})
}

func runExactRuleConfigTests(ctx context.Context, t *testing.T, contextID uuid.UUID) {
	t.Helper()

	t.Run("accepts arbitrary exact config", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleSuccess(
			ctx,
			t,
			contextID,
			CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleTypeExact,
				Config:   map[string]any{"unexpected": true},
			},
		)
	})

	t.Run("accepts exact config with mixed types", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleSuccess(
			ctx,
			t,
			contextID,
			CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleTypeExact,
				Config:   map[string]any{"matchCurrency": "true"},
			},
		)
	})

	t.Run("accepts exact matchScore out of range", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleSuccess(
			ctx,
			t,
			contextID,
			CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleTypeExact,
				Config:   map[string]any{"matchScore": 101},
			},
		)
	})

	t.Run("accepts exact invalid date precision", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleSuccess(
			ctx,
			t,
			contextID,
			CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleTypeExact,
				Config:   map[string]any{"datePrecision": "MONTH"},
			},
		)
	})

	t.Run("accepts exact rule allocation config", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleSuccess(
			ctx,
			t,
			contextID,
			CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleTypeExact,
				Config: map[string]any{
					"allowPartial":             true,
					"allocationDirection":      "LEFT_TO_RIGHT",
					"allocationToleranceMode":  "ABS",
					"allocationToleranceValue": "0.25",
					"allocationUseBaseAmount":  true,
				},
			},
		)
	})
}

func runToleranceRuleConfigTests(ctx context.Context, t *testing.T, contextID uuid.UUID) {
	t.Helper()

	t.Run("accepts arbitrary tolerance config", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleSuccess(
			ctx,
			t,
			contextID,
			CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleTypeTolerance,
				Config:   map[string]any{"absTolerance": true},
			},
		)
	})

	t.Run("accepts tolerance config with negative values", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleSuccess(
			ctx,
			t,
			contextID,
			CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleTypeTolerance,
				Config: map[string]any{
					"absTolerance":     "1.00",
					"percentTolerance": -1,
					"dateWindowDays":   0,
				},
			},
		)
	})

	t.Run("accepts tolerance config without tolerances", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleSuccess(
			ctx,
			t,
			contextID,
			CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleTypeTolerance,
				Config:   map[string]any{"matchCurrency": true},
			},
		)
	})

	t.Run("accepts tolerance rounding mode", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleSuccess(
			ctx,
			t,
			contextID,
			CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleTypeTolerance,
				Config:   map[string]any{"roundingMode": "BAD"},
			},
		)
	})

	t.Run("accepts tolerance matchScore out of range", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleSuccess(
			ctx,
			t,
			contextID,
			CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleTypeTolerance,
				Config:   map[string]any{"matchScore": 101},
			},
		)
	})

	t.Run("accepts NaN tolerance config", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleSuccess(
			ctx,
			t,
			contextID,
			CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleTypeTolerance,
				Config: map[string]any{
					"absTolerance":     "NaN",
					"percentTolerance": 1,
					"dateWindowDays":   0,
				},
			},
		)
	})

	t.Run("accepts Inf tolerance config", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleSuccess(
			ctx,
			t,
			contextID,
			CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleTypeTolerance,
				Config:   map[string]any{"absTolerance": "Inf", "percentTolerance": 1},
			},
		)
	})

	t.Run("accepts negative Inf tolerance config", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleSuccess(
			ctx,
			t,
			contextID,
			CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleTypeTolerance,
				Config:   map[string]any{"absTolerance": "-Inf", "percentTolerance": 1},
			},
		)
	})

	t.Run("accepts json.Number tolerance", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleSuccess(
			ctx,
			t,
			contextID,
			CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleTypeTolerance,
				Config:   map[string]any{"absTolerance": "1e309", "percentTolerance": 1},
			},
		)
	})

	t.Run("accepts tolerance allocation base amount", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleSuccess(
			ctx,
			t,
			contextID,
			CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleTypeTolerance,
				Config:   map[string]any{"absTolerance": "0.10", "allocationUseBaseAmount": true},
			},
		)
	})

	t.Run("accepts negative tolerance config", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleSuccess(
			ctx,
			t,
			contextID,
			CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleTypeTolerance,
				Config:   map[string]any{"absTolerance": "-1", "percentTolerance": "1"},
			},
		)
	})
}

func runDateLagRuleConfigTests(ctx context.Context, t *testing.T, contextID uuid.UUID) {
	t.Helper()

	t.Run("accepts date lag config with minDays > maxDays", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleSuccess(
			ctx,
			t,
			contextID,
			CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleTypeDateLag,
				Config:   map[string]any{"minDays": 5, "maxDays": 1, "direction": "ABS"},
			},
		)
	})

	t.Run("accepts date lag inclusive type", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleSuccess(
			ctx,
			t,
			contextID,
			CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleTypeDateLag,
				Config:   map[string]any{"inclusive": "true"},
			},
		)
	})

	t.Run("accepts date lag direction", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleSuccess(
			ctx,
			t,
			contextID,
			CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleTypeDateLag,
				Config:   map[string]any{"minDays": 1, "maxDays": 2, "direction": "BAD"},
			},
		)
	})

	t.Run("accepts date lag default direction", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleSuccess(
			ctx,
			t,
			contextID,
			CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleTypeDateLag,
				Config:   map[string]any{"minDays": 1, "maxDays": 2},
			},
		)
	})

	t.Run("accepts date lag maxDays too large", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleSuccess(
			ctx,
			t,
			contextID,
			CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleTypeDateLag,
				Config:   map[string]any{"minDays": 0, "maxDays": 4000, "direction": "ABS"},
			},
		)
	})

	t.Run("accepts date lag negative days", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleSuccess(
			ctx,
			t,
			contextID,
			CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleTypeDateLag,
				Config:   map[string]any{"minDays": -1, "maxDays": 2, "direction": "ABS"},
			},
		)
	})

	t.Run("accepts date lag non-integer maxDays", func(t *testing.T) {
		t.Parallel()
		testNewMatchRuleSuccess(
			ctx,
			t,
			contextID,
			CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleTypeDateLag,
				Config:   map[string]any{"minDays": 1, "maxDays": 1.5, "direction": "ABS"},
			},
		)
	})
}

func TestNewMatchRule(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()

	runBasicValidationTests(ctx, t, contextID)
	runExactRuleConfigTests(ctx, t, contextID)
	runToleranceRuleConfigTests(ctx, t, contextID)
	runDateLagRuleConfigTests(ctx, t, contextID)
}

func TestMatchRule_Update(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	createRule := func(t *testing.T) *MatchRule {
		t.Helper()

		input := CreateMatchRuleInput{
			Priority: 2,
			Type:     shared.RuleTypeTolerance,
			Config:   map[string]any{"absTolerance": "1.00"},
		}
		rule, err := NewMatchRule(ctx, contextID, input)
		require.NoError(t, err)

		return rule
	}

	t.Run("nil receiver", func(t *testing.T) {
		t.Parallel()

		priority := 1
		err := (*MatchRule)(nil).Update(ctx, UpdateMatchRuleInput{Priority: &priority})
		require.Error(t, err)
		require.ErrorIs(t, err, ErrMatchRuleNil)
	})

	t.Run("updates priority", func(t *testing.T) {
		t.Parallel()

		rule := createRule(t)
		newPriority := 3
		err := rule.Update(ctx, UpdateMatchRuleInput{Priority: &newPriority})
		require.NoError(t, err)
		assert.Equal(t, 3, rule.Priority)
	})

	t.Run("updates type", func(t *testing.T) {
		t.Parallel()

		rule := createRule(t)
		newType := shared.RuleTypeDateLag
		newConfig := map[string]any{"minDays": 1, "maxDays": 2, "direction": "ABS"}
		err := rule.Update(ctx, UpdateMatchRuleInput{Type: &newType, Config: newConfig})
		require.NoError(t, err)
		assert.Equal(t, shared.RuleTypeDateLag, rule.Type)
		assert.Equal(t, newConfig, rule.Config)
	})

	t.Run("updates config", func(t *testing.T) {
		t.Parallel()

		rule := createRule(t)
		newConfig := map[string]any{"absTolerance": "2.00"}
		err := rule.Update(ctx, UpdateMatchRuleInput{Config: newConfig})
		require.NoError(t, err)
		assert.Equal(t, newConfig, rule.Config)
	})

	t.Run("updates type without config", func(t *testing.T) {
		t.Parallel()

		rule := createRule(t)
		newType := shared.RuleTypeDateLag
		err := rule.Update(ctx, UpdateMatchRuleInput{Type: &newType})
		require.NoError(t, err)
		assert.Equal(t, newType, rule.Type)
	})

	t.Run("fails with invalid priority", func(t *testing.T) {
		t.Parallel()
		rule := createRule(t)
		invalidPriority := 0
		testMatchRuleUpdateError(
			ctx,
			t,
			rule,
			UpdateMatchRuleInput{Priority: &invalidPriority},
			ErrRulePriorityInvalid,
		)
	})

	t.Run("fails with invalid type", func(t *testing.T) {
		t.Parallel()
		rule := createRule(t)
		invalidType := shared.RuleType("INVALID")
		testMatchRuleUpdateError(
			ctx,
			t,
			rule,
			UpdateMatchRuleInput{
				Type:   &invalidType,
				Config: map[string]any{"absTolerance": "1.00"},
			},
			ErrRuleTypeInvalid,
		)
	})

	t.Run("fails with empty config", func(t *testing.T) {
		t.Parallel()
		rule := createRule(t)
		testMatchRuleUpdateError(
			ctx,
			t,
			rule,
			UpdateMatchRuleInput{Config: map[string]any{}},
			ErrRuleConfigRequired,
		)
	})
}

func TestValidateMatchRuleConfig(t *testing.T) {
	t.Parallel()

	t.Run("returns error for empty config", func(t *testing.T) {
		t.Parallel()

		err := ValidateMatchRuleConfig(shared.RuleTypeExact, map[string]any{})
		require.ErrorIs(t, err, ErrRuleConfigRequired)
	})

	t.Run("returns error for nil config", func(t *testing.T) {
		t.Parallel()

		err := ValidateMatchRuleConfig(shared.RuleTypeExact, nil)
		require.ErrorIs(t, err, ErrRuleConfigRequired)
	})

	t.Run("returns error for invalid rule type", func(t *testing.T) {
		t.Parallel()

		err := ValidateMatchRuleConfig(shared.RuleType("INVALID"), map[string]any{"key": "val"})
		require.ErrorIs(t, err, ErrRuleTypeInvalid)
	})

	t.Run("accepts exact config with recognized keys", func(t *testing.T) {
		t.Parallel()

		err := ValidateMatchRuleConfig(shared.RuleTypeExact, map[string]any{"matchCurrency": true})
		require.NoError(t, err)
	})

	t.Run("accepts exact config with allocation keys", func(t *testing.T) {
		t.Parallel()

		err := ValidateMatchRuleConfig(shared.RuleTypeExact, map[string]any{
			"allowPartial":        true,
			"allocationDirection": "LEFT_TO_RIGHT",
		})
		require.NoError(t, err)
	})

	t.Run("rejects exact config with only unrecognized keys", func(t *testing.T) {
		t.Parallel()

		err := ValidateMatchRuleConfig(shared.RuleTypeExact, map[string]any{"unexpected": true})
		require.Error(t, err)
		require.ErrorIs(t, err, ErrRuleConfigMissingRequiredKeys)
		assert.Contains(t, err.Error(), "EXACT")
	})

	t.Run("accepts exact config with mix of recognized and unrecognized keys", func(t *testing.T) {
		t.Parallel()

		err := ValidateMatchRuleConfig(shared.RuleTypeExact, map[string]any{
			"matchAmount": true,
			"custom_key":  "value",
		})
		require.NoError(t, err)
	})

	t.Run("accepts tolerance config with recognized keys", func(t *testing.T) {
		t.Parallel()

		err := ValidateMatchRuleConfig(shared.RuleTypeTolerance, map[string]any{"absTolerance": "1.00"})
		require.NoError(t, err)
	})

	t.Run("rejects tolerance config with only unrecognized keys", func(t *testing.T) {
		t.Parallel()

		err := ValidateMatchRuleConfig(shared.RuleTypeTolerance, map[string]any{"field": "amount"})
		require.Error(t, err)
		require.ErrorIs(t, err, ErrRuleConfigMissingRequiredKeys)
		assert.Contains(t, err.Error(), "TOLERANCE")
	})

	t.Run("accepts tolerance config with matchCurrency", func(t *testing.T) {
		t.Parallel()

		err := ValidateMatchRuleConfig(shared.RuleTypeTolerance, map[string]any{"matchCurrency": true})
		require.NoError(t, err)
	})

	t.Run("accepts date lag config with recognized keys", func(t *testing.T) {
		t.Parallel()

		err := ValidateMatchRuleConfig(shared.RuleTypeDateLag, map[string]any{"minDays": 1, "maxDays": 5})
		require.NoError(t, err)
	})

	t.Run("rejects date lag config with only unrecognized keys", func(t *testing.T) {
		t.Parallel()

		err := ValidateMatchRuleConfig(shared.RuleTypeDateLag, map[string]any{"field": "date", "max_lag_days": 5})
		require.Error(t, err)
		require.ErrorIs(t, err, ErrRuleConfigMissingRequiredKeys)
		assert.Contains(t, err.Error(), "DATE_LAG")
	})

	t.Run("accepts date lag config with direction key", func(t *testing.T) {
		t.Parallel()

		err := ValidateMatchRuleConfig(shared.RuleTypeDateLag, map[string]any{"direction": "ABS"})
		require.NoError(t, err)
	})

	t.Run("accepts date lag config with inclusive key", func(t *testing.T) {
		t.Parallel()

		err := ValidateMatchRuleConfig(shared.RuleTypeDateLag, map[string]any{"inclusive": true})
		require.NoError(t, err)
	})
}
