//go:build integration

package exception

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/domain/services"
	sharedexception "github.com/LerianStudio/matcher/internal/shared/domain/exception"
	exceptionVO "github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/testutil"
)

func TestIntegration_Exception_IntegrationSLARouting_CriticalAmount(t *testing.T) {
	t.Parallel()

	input := services.SLAInput{
		AmountAbsBase: decimal.NewFromInt(150000),
		AgeHours:      0,
		ReferenceTime: time.Now().UTC(),
	}

	rules := services.DefaultSLARules()

	result, err := services.ComputeSLADueAt(input, rules)
	require.NoError(t, err)
	require.Equal(t, "CRITICAL", result.RuleName)
	require.Equal(t, 0, result.RuleIndex)

	expectedDue := input.ReferenceTime.Add(24 * time.Hour)
	require.WithinDuration(t, expectedDue, result.DueAt, time.Second)
}

func TestIntegration_Exception_IntegrationSLARouting_CriticalAge(t *testing.T) {
	t.Parallel()

	input := services.SLAInput{
		AmountAbsBase: decimal.NewFromInt(50),
		AgeHours:      130,
		ReferenceTime: time.Now().UTC(),
	}

	rules := services.DefaultSLARules()

	result, err := services.ComputeSLADueAt(input, rules)
	require.NoError(t, err)
	require.Equal(t, "CRITICAL", result.RuleName)
}

func TestIntegration_Exception_IntegrationSLARouting_HighAmount(t *testing.T) {
	t.Parallel()

	input := services.SLAInput{
		AmountAbsBase: decimal.NewFromInt(25000),
		AgeHours:      0,
		ReferenceTime: time.Now().UTC(),
	}

	rules := services.DefaultSLARules()

	result, err := services.ComputeSLADueAt(input, rules)
	require.NoError(t, err)
	require.Equal(t, "HIGH", result.RuleName)
	require.Equal(t, 1, result.RuleIndex)

	expectedDue := input.ReferenceTime.Add(72 * time.Hour)
	require.WithinDuration(t, expectedDue, result.DueAt, time.Second)
}

func TestIntegration_Exception_IntegrationSLARouting_HighAge(t *testing.T) {
	t.Parallel()

	input := services.SLAInput{
		AmountAbsBase: decimal.NewFromInt(100),
		AgeHours:      80,
		ReferenceTime: time.Now().UTC(),
	}

	rules := services.DefaultSLARules()

	result, err := services.ComputeSLADueAt(input, rules)
	require.NoError(t, err)
	require.Equal(t, "HIGH", result.RuleName)
}

func TestIntegration_Exception_IntegrationSLARouting_MediumAmount(t *testing.T) {
	t.Parallel()

	input := services.SLAInput{
		AmountAbsBase: decimal.NewFromInt(5000),
		AgeHours:      0,
		ReferenceTime: time.Now().UTC(),
	}

	rules := services.DefaultSLARules()

	result, err := services.ComputeSLADueAt(input, rules)
	require.NoError(t, err)
	require.Equal(t, "MEDIUM", result.RuleName)
	require.Equal(t, 2, result.RuleIndex)

	expectedDue := input.ReferenceTime.Add(120 * time.Hour)
	require.WithinDuration(t, expectedDue, result.DueAt, time.Second)
}

func TestIntegration_Exception_IntegrationSLARouting_MediumAge(t *testing.T) {
	t.Parallel()

	input := services.SLAInput{
		AmountAbsBase: decimal.NewFromInt(50),
		AgeHours:      30,
		ReferenceTime: time.Now().UTC(),
	}

	rules := services.DefaultSLARules()

	result, err := services.ComputeSLADueAt(input, rules)
	require.NoError(t, err)
	require.Equal(t, "MEDIUM", result.RuleName)
}

func TestIntegration_Exception_IntegrationSLARouting_LowDefault(t *testing.T) {
	t.Parallel()

	input := services.SLAInput{
		AmountAbsBase: decimal.NewFromInt(100),
		AgeHours:      0,
		ReferenceTime: time.Now().UTC(),
	}

	rules := services.DefaultSLARules()

	result, err := services.ComputeSLADueAt(input, rules)
	require.NoError(t, err)
	require.Equal(t, "LOW", result.RuleName)
	require.Equal(t, 3, result.RuleIndex)

	expectedDue := input.ReferenceTime.Add(168 * time.Hour)
	require.WithinDuration(t, expectedDue, result.DueAt, time.Second)
}

func TestIntegration_Exception_IntegrationSLARouting_DeterministicOrdering(t *testing.T) {
	t.Parallel()

	referenceTime := time.Date(2026, 1, 20, 12, 0, 0, 0, time.UTC)

	testCases := []struct {
		name         string
		amount       int64
		ageHours     int
		expectedRule string
	}{
		{"critical_amount", 100000, 0, "CRITICAL"},
		{"critical_age", 50, 120, "CRITICAL"},
		{"high_amount", 10000, 0, "HIGH"},
		{"high_age", 50, 72, "HIGH"},
		{"medium_amount", 1000, 0, "MEDIUM"},
		{"medium_age", 50, 24, "MEDIUM"},
		{"low_default", 500, 12, "LOW"},
	}

	rules := services.DefaultSLARules()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			input := services.SLAInput{
				AmountAbsBase: decimal.NewFromInt(tc.amount),
				AgeHours:      tc.ageHours,
				ReferenceTime: referenceTime,
			}

			result1, err := services.ComputeSLADueAt(input, rules)
			require.NoError(t, err)

			result2, err := services.ComputeSLADueAt(input, rules)
			require.NoError(t, err)

			require.Equal(t, tc.expectedRule, result1.RuleName)
			require.Equal(
				t,
				result1.RuleName,
				result2.RuleName,
				"SLA routing should be deterministic",
			)
			require.Equal(t, result1.DueAt, result2.DueAt, "Due date should be deterministic")
		})
	}
}

func TestIntegration_Exception_IntegrationSLARouting_RoutingDecisions(t *testing.T) {
	t.Parallel()

	criticalSeverity := sharedexception.ExceptionSeverityCritical
	highSeverity := sharedexception.ExceptionSeverityHigh

	testCases := []struct {
		name           string
		input          services.RoutingInput
		rules          []services.RoutingRule
		expectedTarget services.RoutingTarget
		expectedRule   string
	}{
		{
			name: "critical_severity_routes_to_jira",
			input: services.RoutingInput{
				Severity:      sharedexception.ExceptionSeverityCritical,
				AmountAbsBase: decimal.NewFromInt(50000),
				AgeHours:      10,
			},
			rules: []services.RoutingRule{
				{
					Name:     "critical-to-jira",
					Priority: 1,
					Severities: []sharedexception.ExceptionSeverity{
						sharedexception.ExceptionSeverityCritical,
					},
					Target: services.RoutingTargetJira,
					Queue:  "OPS",
				},
				{
					Name:     "default-manual",
					Priority: 100,
					MatchAll: true,
					Target:   services.RoutingTargetManual,
				},
			},
			expectedTarget: services.RoutingTargetJira,
			expectedRule:   "critical-to-jira",
		},
		{
			name: "high_amount_routes_to_webhook",
			input: services.RoutingInput{
				Severity:      sharedexception.ExceptionSeverityHigh,
				AmountAbsBase: decimal.NewFromInt(75000),
				AgeHours:      5,
			},
			rules: []services.RoutingRule{
				{
					Name:             "high-amount-webhook",
					Priority:         1,
					MinAmountAbsBase: testutil.DecimalFromInt(50000),
					Target:           services.RoutingTargetWebhook,
				},
				{
					Name:     "fallback",
					Priority: 100,
					MatchAll: true,
					Target:   services.RoutingTargetManual,
				},
			},
			expectedTarget: services.RoutingTargetWebhook,
			expectedRule:   "high-amount-webhook",
		},
		{
			name: "severity_override_applied",
			input: services.RoutingInput{
				Severity:      sharedexception.ExceptionSeverityMedium,
				AmountAbsBase: decimal.NewFromInt(200000),
				AgeHours:      150,
			},
			rules: []services.RoutingRule{
				{
					Name:             "escalate-old-high-amount",
					Priority:         1,
					MinAmountAbsBase: testutil.DecimalFromInt(100000),
					MinAgeHours:      testutil.IntPtr(100),
					Target:           services.RoutingTargetJira,
					OverrideSeverity: &criticalSeverity,
					OverrideReason:   testutil.Ptr(exceptionVO.OverrideReasonOpsApproval),
				},
				{
					Name:     "default",
					Priority: 100,
					MatchAll: true,
					Target:   services.RoutingTargetManual,
				},
			},
			expectedTarget: services.RoutingTargetJira,
			expectedRule:   "escalate-old-high-amount",
		},
		{
			name: "priority_ordering",
			input: services.RoutingInput{
				Severity:      sharedexception.ExceptionSeverityHigh,
				AmountAbsBase: decimal.NewFromInt(20000),
				AgeHours:      50,
			},
			rules: []services.RoutingRule{
				{
					Name:     "low-priority-webhook",
					Priority: 10,
					Severities: []sharedexception.ExceptionSeverity{
						sharedexception.ExceptionSeverityHigh,
					},
					Target: services.RoutingTargetWebhook,
				},
				{
					Name:     "high-priority-jira",
					Priority: 1,
					Severities: []sharedexception.ExceptionSeverity{
						sharedexception.ExceptionSeverityHigh,
					},
					Target:           services.RoutingTargetJira,
					OverrideSeverity: &highSeverity,
					OverrideReason: testutil.Ptr(
						exceptionVO.OverrideReasonDataCorrection,
					),
				},
			},
			expectedTarget: services.RoutingTargetJira,
			expectedRule:   "high-priority-jira",
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			decision, err := services.EvaluateRouting(tc.input, tc.rules)
			require.NoError(t, err)
			require.Equal(t, tc.expectedTarget, decision.Target)
			require.Equal(t, tc.expectedRule, decision.RuleName)
		})
	}
}

func TestIntegration_Exception_IntegrationSLARouting_NoMatchingRule(t *testing.T) {
	t.Parallel()

	input := services.RoutingInput{
		Severity:      sharedexception.ExceptionSeverityLow,
		AmountAbsBase: decimal.NewFromInt(100),
		AgeHours:      1,
	}

	rules := []services.RoutingRule{
		{
			Name:       "critical-only",
			Priority:   1,
			Severities: []sharedexception.ExceptionSeverity{sharedexception.ExceptionSeverityCritical},
			Target:     services.RoutingTargetJira,
		},
	}

	_, err := services.EvaluateRouting(input, rules)
	require.Error(t, err)
	require.ErrorIs(t, err, services.ErrNoMatchingRoutingRule)
}

func TestIntegration_Exception_IntegrationSLARouting_EmptyRules(t *testing.T) {
	t.Parallel()

	input := services.SLAInput{
		AmountAbsBase: decimal.NewFromInt(100),
		AgeHours:      0,
		ReferenceTime: time.Now().UTC(),
	}

	_, err := services.ComputeSLADueAt(input, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, services.ErrEmptySLARules)

	routingInput := services.RoutingInput{
		Severity:      sharedexception.ExceptionSeverityMedium,
		AmountAbsBase: decimal.NewFromInt(100),
	}

	_, err = services.EvaluateRouting(routingInput, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, services.ErrEmptyRoutingRules)
}
