//go:build unit

package value_objects

import (
	"math/rand"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/require"
)

func TestProperty_Status_ParseRoundTrip(t *testing.T) {
	t.Parallel()

	cfg := quick.Config{
		MaxCount: 100,
		Rand:     rand.New(rand.NewSource(42)),
	}

	validStatuses := []ExceptionStatus{
		ExceptionStatusOpen,
		ExceptionStatusAssigned,
		ExceptionStatusResolved,
	}

	property := func(index uint8) bool {
		status := validStatuses[int(index)%len(validStatuses)]

		parsed, err := ParseExceptionStatus(status.String())
		if err != nil {
			return false
		}

		return parsed == status
	}

	require.NoError(t, quick.Check(property, &cfg))
}

func TestProperty_Status_TransitionMatchesDomainLogic(t *testing.T) {
	t.Parallel()

	cfg := quick.Config{
		MaxCount: 100,
		Rand:     rand.New(rand.NewSource(7)),
	}

	validStatuses := []ExceptionStatus{
		ExceptionStatusOpen,
		ExceptionStatusAssigned,
		ExceptionStatusResolved,
	}

	// Mirror of AllowedResolutionTransitions() to verify domain logic against.
	validTransitions := map[ExceptionStatus][]ExceptionStatus{
		ExceptionStatusOpen:     {ExceptionStatusAssigned, ExceptionStatusResolved},
		ExceptionStatusAssigned: {ExceptionStatusResolved},
		ExceptionStatusResolved: {},
	}

	isExpectedValid := func(from, to ExceptionStatus) bool {
		allowed, exists := validTransitions[from]
		if !exists {
			return false
		}

		for _, valid := range allowed {
			if valid == to {
				return true
			}
		}

		return false
	}

	property := func(fromIndex, toIndex uint8) bool {
		from := validStatuses[int(fromIndex)%len(validStatuses)]
		to := validStatuses[int(toIndex)%len(validStatuses)]

		err := ValidateResolutionTransition(from, to)
		domainAllows := err == nil

		return domainAllows == isExpectedValid(from, to)
	}

	require.NoError(t, quick.Check(property, &cfg))
}
