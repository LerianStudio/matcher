// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package value_objects

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateResolutionTransition(t *testing.T) {
	t.Parallel()

	// Valid transitions from OPEN.
	require.NoError(t, ValidateResolutionTransition(ExceptionStatusOpen, ExceptionStatusResolved))
	require.NoError(t, ValidateResolutionTransition(ExceptionStatusOpen, ExceptionStatusAssigned))
	require.NoError(t, ValidateResolutionTransition(ExceptionStatusOpen, ExceptionStatusPendingResolution))

	// Valid transitions from ASSIGNED.
	require.NoError(
		t,
		ValidateResolutionTransition(ExceptionStatusAssigned, ExceptionStatusResolved),
	)
	require.NoError(
		t,
		ValidateResolutionTransition(ExceptionStatusAssigned, ExceptionStatusPendingResolution),
	)

	// Valid transitions from PENDING_RESOLUTION.
	require.NoError(
		t,
		ValidateResolutionTransition(ExceptionStatusPendingResolution, ExceptionStatusResolved),
	)
	require.NoError(
		t,
		ValidateResolutionTransition(ExceptionStatusPendingResolution, ExceptionStatusOpen),
	)
	require.NoError(
		t,
		ValidateResolutionTransition(ExceptionStatusPendingResolution, ExceptionStatusAssigned),
	)

	// Invalid transitions.
	require.ErrorIs(
		t,
		ValidateResolutionTransition(ExceptionStatusResolved, ExceptionStatusResolved),
		ErrInvalidResolutionTransition,
	)
	require.ErrorIs(
		t,
		ValidateResolutionTransition(ExceptionStatusResolved, ExceptionStatusOpen),
		ErrInvalidResolutionTransition,
	)
	require.ErrorIs(
		t,
		ValidateResolutionTransition(ExceptionStatusResolved, ExceptionStatusPendingResolution),
		ErrInvalidResolutionTransition,
	)
}

func TestValidateResolutionTransition_InvalidStatus(t *testing.T) {
	t.Parallel()

	require.ErrorIs(
		t,
		ValidateResolutionTransition(ExceptionStatus("BAD"), ExceptionStatusResolved),
		ErrInvalidExceptionStatus,
	)
	require.ErrorIs(
		t,
		ValidateResolutionTransition(ExceptionStatusOpen, ExceptionStatus("BAD")),
		ErrInvalidExceptionStatus,
	)
}

func TestAllowedResolutionTransitions(t *testing.T) {
	t.Parallel()

	transitions := AllowedResolutionTransitions()

	require.Len(t, transitions[ExceptionStatusOpen], 3)
	require.Contains(t, transitions[ExceptionStatusOpen], ExceptionStatusAssigned)
	require.Contains(t, transitions[ExceptionStatusOpen], ExceptionStatusResolved)
	require.Contains(t, transitions[ExceptionStatusOpen], ExceptionStatusPendingResolution)

	require.Len(t, transitions[ExceptionStatusAssigned], 2)
	require.Contains(t, transitions[ExceptionStatusAssigned], ExceptionStatusResolved)
	require.Contains(t, transitions[ExceptionStatusAssigned], ExceptionStatusPendingResolution)

	require.Len(t, transitions[ExceptionStatusPendingResolution], 3)
	require.Contains(t, transitions[ExceptionStatusPendingResolution], ExceptionStatusResolved)
	require.Contains(t, transitions[ExceptionStatusPendingResolution], ExceptionStatusOpen)
	require.Contains(t, transitions[ExceptionStatusPendingResolution], ExceptionStatusAssigned)

	require.Empty(t, transitions[ExceptionStatusResolved])
}
