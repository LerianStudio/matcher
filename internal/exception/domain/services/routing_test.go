//go:build unit

package services

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
)

func TestEvaluateRouting_FirstMatchWins(t *testing.T) {
	t.Parallel()

	rules := []RoutingRule{
		{
			Name:       "high",
			Priority:   1,
			Severities: []value_objects.ExceptionSeverity{value_objects.ExceptionSeverityHigh},
			Target:     RoutingTargetManual,
			Queue:      "queue-high",
		},
		{
			Name:     "fallback",
			Priority: 2,
			MatchAll: true,
			Target:   RoutingTargetManual,
			Queue:    "queue-default",
		},
	}

	decision, err := EvaluateRouting(
		RoutingInput{
			Severity:      value_objects.ExceptionSeverityHigh,
			AmountAbsBase: decimal.NewFromInt(10),
		},
		rules,
	)
	require.NoError(t, err)
	require.Equal(t, "high", decision.RuleName)
	require.Equal(t, "queue-high", decision.Queue)
}

func TestEvaluateRouting_OverrideRequiresReason(t *testing.T) {
	t.Parallel()

	severity := value_objects.ExceptionSeverityCritical
	rules := []RoutingRule{
		{
			Name:             "override",
			Priority:         1,
			MatchAll:         true,
			Target:           RoutingTargetManual,
			OverrideSeverity: &severity,
		},
	}

	_, err := EvaluateRouting(
		RoutingInput{
			Severity:      value_objects.ExceptionSeverityHigh,
			AmountAbsBase: decimal.NewFromInt(10),
		},
		rules,
	)
	require.ErrorIs(t, err, ErrOverrideReasonRequired)
}

func TestEvaluateRouting_InvalidTarget(t *testing.T) {
	t.Parallel()

	rules := []RoutingRule{
		{
			Name:     "bad",
			Priority: 1,
			MatchAll: true,
			Target:   RoutingTarget("BAD"),
		},
	}

	_, err := EvaluateRouting(
		RoutingInput{
			Severity:      value_objects.ExceptionSeverityLow,
			AmountAbsBase: decimal.NewFromInt(10),
		},
		rules,
	)
	require.ErrorIs(t, err, ErrInvalidRoutingRule)
}

func TestEvaluateRouting_EmptyRules(t *testing.T) {
	t.Parallel()

	_, err := EvaluateRouting(RoutingInput{Severity: value_objects.ExceptionSeverityLow}, nil)
	require.ErrorIs(t, err, ErrEmptyRoutingRules)

	_, err = EvaluateRouting(
		RoutingInput{Severity: value_objects.ExceptionSeverityLow},
		[]RoutingRule{},
	)
	require.ErrorIs(t, err, ErrEmptyRoutingRules)
}

func TestEvaluateRouting_PriorityOrdering(t *testing.T) {
	t.Parallel()

	rules := []RoutingRule{
		{
			Name:     "second",
			Priority: 2,
			MatchAll: true,
			Target:   RoutingTargetManual,
			Queue:    "second-queue",
		},
		{
			Name:     "first",
			Priority: 1,
			MatchAll: true,
			Target:   RoutingTargetManual,
			Queue:    "first-queue",
		},
	}

	decision, err := EvaluateRouting(
		RoutingInput{
			Severity:      value_objects.ExceptionSeverityLow,
			AmountAbsBase: decimal.NewFromInt(10),
		},
		rules,
	)
	require.NoError(t, err)
	require.Equal(t, "first", decision.RuleName)
	require.Equal(t, "first-queue", decision.Queue)
}

func TestEvaluateRouting_OverrideSeverityWithReason(t *testing.T) {
	t.Parallel()

	severity := value_objects.ExceptionSeverityCritical
	reason := value_objects.OverrideReasonPolicyException
	rules := []RoutingRule{
		{
			Name:             "override",
			Priority:         1,
			MatchAll:         true,
			Target:           RoutingTargetManual,
			OverrideSeverity: &severity,
			OverrideReason:   &reason,
		},
	}

	decision, err := EvaluateRouting(
		RoutingInput{
			Severity:      value_objects.ExceptionSeverityHigh,
			AmountAbsBase: decimal.NewFromInt(10),
		},
		rules,
	)
	require.NoError(t, err)
	require.NotNil(t, decision.OverrideSeverity)
	require.Equal(t, value_objects.ExceptionSeverityCritical, *decision.OverrideSeverity)
	require.NotNil(t, decision.OverrideReason)
	require.Equal(t, value_objects.OverrideReasonPolicyException, *decision.OverrideReason)
}

func TestEvaluateRouting_AmountThreshold(t *testing.T) {
	t.Parallel()

	minAmount := decimal.NewFromInt(1000)
	rules := []RoutingRule{
		{
			Name:             "high-value",
			Priority:         1,
			MinAmountAbsBase: &minAmount,
			Target:           RoutingTargetJira,
			Queue:            "high-value-queue",
		},
		{
			Name:     "fallback",
			Priority: 2,
			MatchAll: true,
			Target:   RoutingTargetManual,
			Queue:    "default-queue",
		},
	}

	decision, err := EvaluateRouting(
		RoutingInput{
			Severity:      value_objects.ExceptionSeverityLow,
			AmountAbsBase: decimal.NewFromInt(500),
		},
		rules,
	)
	require.NoError(t, err)
	require.Equal(t, "fallback", decision.RuleName)

	decision, err = EvaluateRouting(
		RoutingInput{
			Severity:      value_objects.ExceptionSeverityLow,
			AmountAbsBase: decimal.NewFromInt(2000),
		},
		rules,
	)
	require.NoError(t, err)
	require.Equal(t, "high-value", decision.RuleName)
}

func TestEvaluateRouting_SourceTypeFilter(t *testing.T) {
	t.Parallel()

	rules := []RoutingRule{
		{
			Name:        "regulatory",
			Priority:    1,
			SourceTypes: []string{"REGULATORY"},
			Target:      RoutingTargetServiceNow,
			Queue:       "regulatory-queue",
		},
		{
			Name:     "fallback",
			Priority: 2,
			MatchAll: true,
			Target:   RoutingTargetManual,
		},
	}

	decision, err := EvaluateRouting(
		RoutingInput{Severity: value_objects.ExceptionSeverityLow, SourceType: "regulatory"},
		rules,
	)
	require.NoError(t, err)
	require.Equal(t, "regulatory", decision.RuleName)

	decision, err = EvaluateRouting(
		RoutingInput{Severity: value_objects.ExceptionSeverityLow, SourceType: "other"},
		rules,
	)
	require.NoError(t, err)
	require.Equal(t, "fallback", decision.RuleName)
}

func TestSortRoutingRules(t *testing.T) {
	t.Parallel()

	rules := []RoutingRule{
		{Name: "c", Priority: 2, MatchAll: true, Target: RoutingTargetManual},
		{Name: "a", Priority: 1, MatchAll: true, Target: RoutingTargetManual},
		{Name: "b", Priority: 1, MatchAll: true, Target: RoutingTargetManual},
	}

	SortRoutingRules(rules)

	require.Equal(t, "a", rules[0].Name)
	require.Equal(t, "b", rules[1].Name)
	require.Equal(t, "c", rules[2].Name)
}
