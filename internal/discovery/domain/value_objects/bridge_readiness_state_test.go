//go:build unit

package value_objects_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
)

func TestBridgeReadinessState_Constants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "pending", vo.BridgeReadinessPending.String())
	assert.Equal(t, "ready", vo.BridgeReadinessReady.String())
	assert.Equal(t, "stale", vo.BridgeReadinessStale.String())
	assert.Equal(t, "failed", vo.BridgeReadinessFailed.String())
	assert.Equal(t, "in_flight", vo.BridgeReadinessInFlight.String())
}

func TestBridgeReadinessState_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		state vo.BridgeReadinessState
		want  bool
	}{
		{"pending valid", vo.BridgeReadinessPending, true},
		{"ready valid", vo.BridgeReadinessReady, true},
		{"stale valid", vo.BridgeReadinessStale, true},
		{"failed valid", vo.BridgeReadinessFailed, true},
		{"in_flight valid", vo.BridgeReadinessInFlight, true},
		{"empty invalid", vo.BridgeReadinessState(""), false},
		{"unknown invalid", vo.BridgeReadinessState("nope"), false},
		{"upper-case invalid (constants are lower)", vo.BridgeReadinessState("PENDING"), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.state.IsValid())
		})
	}
}

func TestParseBridgeReadinessState_Valid(t *testing.T) {
	t.Parallel()

	tests := map[string]vo.BridgeReadinessState{
		"pending":     vo.BridgeReadinessPending,
		"PENDING":     vo.BridgeReadinessPending,
		"  Ready  ":   vo.BridgeReadinessReady,
		"stale":       vo.BridgeReadinessStale,
		"FAILED":      vo.BridgeReadinessFailed,
		"in_flight":   vo.BridgeReadinessInFlight,
		"IN_FLIGHT":   vo.BridgeReadinessInFlight,
		"  in_flight": vo.BridgeReadinessInFlight,
	}

	for input, want := range tests {
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			got, err := vo.ParseBridgeReadinessState(input)
			require.NoError(t, err)
			assert.Equal(t, want, got)
		})
	}
}

func TestParseBridgeReadinessState_Invalid(t *testing.T) {
	t.Parallel()

	tests := []string{"", "unknown", "completed", "READY!"}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			got, err := vo.ParseBridgeReadinessState(input)
			require.Error(t, err)
			assert.True(t, errors.Is(err, vo.ErrInvalidBridgeReadinessState))
			assert.Empty(t, got)
		})
	}
}
