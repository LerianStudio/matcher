//go:build unit

// Copyright 2025 Lerian Studio.

package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReconcilerPhase_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		phase ReconcilerPhase
		want  string
	}{
		{name: "state-sync", phase: PhaseStateSync, want: "state-sync"},
		{name: "validation", phase: PhaseValidation, want: "validation"},
		{name: "side-effect", phase: PhaseSideEffect, want: "side-effect"},
		{name: "unknown", phase: ReconcilerPhase(99), want: "unknown(99)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.phase.String())
		})
	}
}

func TestReconcilerPhase_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		phase ReconcilerPhase
		want  bool
	}{
		{name: "state-sync is valid", phase: PhaseStateSync, want: true},
		{name: "validation is valid", phase: PhaseValidation, want: true},
		{name: "side-effect is valid", phase: PhaseSideEffect, want: true},
		{name: "negative is invalid", phase: ReconcilerPhase(-1), want: false},
		{name: "out of range is invalid", phase: ReconcilerPhase(99), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.phase.IsValid())
		})
	}
}

func TestReconcilerPhase_Ordering(t *testing.T) {
	t.Parallel()

	// Verify phases sort in the correct execution order.
	assert.Less(t, PhaseStateSync, PhaseValidation)
	assert.Less(t, PhaseValidation, PhaseSideEffect)
}

func TestParseReconcilerPhase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    ReconcilerPhase
		wantErr bool
	}{
		{name: "state-sync", input: "state-sync", want: PhaseStateSync},
		{name: "validation", input: "validation", want: PhaseValidation},
		{name: "side-effect", input: "side-effect", want: PhaseSideEffect},
		{name: "case insensitive", input: "State-Sync", want: PhaseStateSync},
		{name: "with whitespace", input: "  validation  ", want: PhaseValidation},
		{name: "invalid", input: "bogus", wantErr: true},
		{name: "empty", input: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseReconcilerPhase(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ErrInvalidReconcilerPhase)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
