//go:build unit

package services

import (
	"math/rand"
	"testing"
	"testing/quick"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
)

func TestProperty_RoutingDeterministic(t *testing.T) {
	t.Parallel()

	cfg := quick.Config{
		MaxCount: 100,
		Rand:     rand.New(rand.NewSource(99)),
	}

	property := func(priorityA, priorityB uint8, amount int64) bool {
		rules := []RoutingRule{
			{
				Name:     "first",
				Priority: int(priorityA),
				MatchAll: true,
				Target:   RoutingTargetManual,
				Queue:    "q1",
			},
			{
				Name:     "second",
				Priority: int(priorityB),
				MatchAll: true,
				Target:   RoutingTargetManual,
				Queue:    "q2",
			},
		}

		input := RoutingInput{
			Severity:      value_objects.ExceptionSeverityHigh,
			AmountAbsBase: decimal.NewFromInt(amount),
		}
		first, err1 := EvaluateRouting(input, rules)
		second, err2 := EvaluateRouting(input, rules)

		if (err1 == nil) != (err2 == nil) {
			return false
		}

		if err1 != nil {
			return true
		}

		return first.RuleName == second.RuleName && first.Queue == second.Queue
	}

	require.NoError(t, quick.Check(property, &cfg))
}

func TestProperty_RoutingPriorityOrder(t *testing.T) {
	t.Parallel()

	cfg := quick.Config{
		MaxCount: 50,
		Rand:     rand.New(rand.NewSource(42)),
	}

	property := func(priorityA, priorityB uint8) bool {
		if priorityA == priorityB {
			return true
		}

		rules := []RoutingRule{
			{
				Name:     "a",
				Priority: int(priorityA),
				MatchAll: true,
				Target:   RoutingTargetManual,
				Queue:    "qa",
			},
			{
				Name:     "b",
				Priority: int(priorityB),
				MatchAll: true,
				Target:   RoutingTargetManual,
				Queue:    "qb",
			},
		}

		input := RoutingInput{
			Severity:      value_objects.ExceptionSeverityLow,
			AmountAbsBase: decimal.NewFromInt(100),
		}

		decision, err := EvaluateRouting(input, rules)
		if err != nil {
			return false
		}

		if priorityA < priorityB {
			return decision.RuleName == "a"
		}

		return decision.RuleName == "b"
	}

	require.NoError(t, quick.Check(property, &cfg))
}

func TestProperty_Routing_TargetAlwaysValid(t *testing.T) {
	t.Parallel()

	cfg := quick.Config{
		MaxCount: 100,
		Rand:     rand.New(rand.NewSource(123)),
	}

	validTargets := []RoutingTarget{
		RoutingTargetManual,
		RoutingTargetJira,
		RoutingTargetServiceNow,
		RoutingTargetWebhook,
	}

	validSeverities := []value_objects.ExceptionSeverity{
		value_objects.ExceptionSeverityLow,
		value_objects.ExceptionSeverityMedium,
		value_objects.ExceptionSeverityHigh,
		value_objects.ExceptionSeverityCritical,
	}

	property := func(targetIndex, severityIndex uint8, amount int64) bool {
		target := validTargets[int(targetIndex)%len(validTargets)]
		severity := validSeverities[int(severityIndex)%len(validSeverities)]

		rules := []RoutingRule{
			{
				Name:     "test-rule",
				Priority: 1,
				MatchAll: true,
				Target:   target,
				Queue:    "test-queue",
			},
		}

		input := RoutingInput{
			Severity:      severity,
			AmountAbsBase: decimal.NewFromInt(amount),
		}

		decision, err := EvaluateRouting(input, rules)
		if err != nil {
			return false
		}

		return decision.Target.IsValid()
	}

	require.NoError(t, quick.Check(property, &cfg))
}

func TestProperty_Routing_MatchAllCatchesAll(t *testing.T) {
	t.Parallel()

	cfg := quick.Config{
		MaxCount: 100,
		Rand:     rand.New(rand.NewSource(456)),
	}

	validSeverities := []value_objects.ExceptionSeverity{
		value_objects.ExceptionSeverityLow,
		value_objects.ExceptionSeverityMedium,
		value_objects.ExceptionSeverityHigh,
		value_objects.ExceptionSeverityCritical,
	}

	property := func(severityIndex uint8, amount int64, ageHours uint16) bool {
		severity := validSeverities[int(severityIndex)%len(validSeverities)]

		rules := []RoutingRule{
			{
				Name:     "specific-rule",
				Priority: 1,
				Severities: []value_objects.ExceptionSeverity{
					value_objects.ExceptionSeverityCritical,
				},
				Target: RoutingTargetJira,
				Queue:  "jira-queue",
			},
			{
				Name:     "catchall",
				Priority: 100,
				MatchAll: true,
				Target:   RoutingTargetManual,
				Queue:    "manual-queue",
			},
		}

		input := RoutingInput{
			Severity:      severity,
			AmountAbsBase: decimal.NewFromInt(amount),
			AgeHours:      int(ageHours),
		}

		decision, err := EvaluateRouting(input, rules)
		if err != nil {
			return false
		}

		if severity == value_objects.ExceptionSeverityCritical {
			return decision.RuleName == "specific-rule"
		}

		return decision.RuleName == "catchall"
	}

	require.NoError(t, quick.Check(property, &cfg))
}
