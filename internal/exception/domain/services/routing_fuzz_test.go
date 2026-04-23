//go:build unit

package services

import (
	"math"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	sharedexception "github.com/LerianStudio/matcher/internal/shared/domain/exception"
)

func FuzzEvaluateRouting_NoPanic(f *testing.F) {
	f.Add(int64(100), 1)
	f.Add(int64(-500), 2)
	f.Add(int64(0), 0)
	f.Add(int64(1), 1)
	f.Add(int64(-1), -1)
	f.Add(int64(math.MinInt64), math.MinInt32)
	f.Add(int64(math.MaxInt64), math.MaxInt32)

	f.Fuzz(func(t *testing.T, amount int64, priority int) {
		rules := []RoutingRule{
			{
				Name:     "fallback",
				Priority: priority,
				MatchAll: true,
				Target:   RoutingTargetManual,
				Queue:    "queue",
			},
		}

		input := RoutingInput{
			Severity:      sharedexception.ExceptionSeverityLow,
			AmountAbsBase: decimal.NewFromInt(amount),
		}

		decision, err := EvaluateRouting(input, rules)
		if err == nil {
			require.True(t, decision.Target.IsValid())
		}
	})
}
