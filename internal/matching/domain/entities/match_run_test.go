// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package entities_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/matching/domain/entities"
	"github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
)

func TestMatchRunLifecycle(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()

	run, err := entities.NewMatchRun(
		context.Background(),
		contextID,
		value_objects.MatchRunModeDryRun,
	)
	require.NoError(t, err)
	require.Equal(t, value_objects.MatchRunStatusProcessing, run.Status)
	require.Nil(t, run.CompletedAt)

	require.NoError(t, run.Complete(context.Background(), map[string]int{"matches_found": 2}))
	require.Equal(t, value_objects.MatchRunStatusCompleted, run.Status)
	require.NotNil(t, run.CompletedAt)
	require.Equal(t, 2, run.Stats["matches_found"])
	require.Nil(t, run.FailureReason)

	run, err = entities.NewMatchRun(
		context.Background(),
		contextID,
		value_objects.MatchRunModeDryRun,
	)
	require.NoError(t, err)
	require.NoError(t, run.Complete(context.Background(), nil))
	require.NotNil(t, run.Stats)
	require.Empty(t, run.Stats)

	previousStatus := run.Status
	previousStats := run.Stats
	previousCompletedAt := run.CompletedAt
	previousFailureReason := run.FailureReason

	require.Error(t, run.Complete(context.Background(), map[string]int{"matches_found": 3}))
	require.Error(t, run.Fail(context.Background(), "failure"))
	require.Equal(t, previousStatus, run.Status)
	require.Equal(t, previousStats, run.Stats)
	require.Equal(t, previousCompletedAt, run.CompletedAt)
	require.Equal(t, previousFailureReason, run.FailureReason)
}

func TestMatchRunFailure(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()

	run, err := entities.NewMatchRun(
		context.Background(),
		contextID,
		value_objects.MatchRunModeCommit,
	)
	require.NoError(t, err)

	require.NoError(t, run.Fail(context.Background(), "timeout"))
	require.Equal(t, value_objects.MatchRunStatusFailed, run.Status)
	require.NotNil(t, run.CompletedAt)
	require.NotNil(t, run.FailureReason)
	require.Equal(t, "timeout", *run.FailureReason)

	previousFailureReason := run.FailureReason
	require.Error(t, run.Fail(context.Background(), "again"))
	require.Equal(t, previousFailureReason, run.FailureReason)
}

func TestMatchRunValidation(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()

	_, err := entities.NewMatchRun(
		context.Background(),
		contextID,
		value_objects.MatchRunMode("INVALID"),
	)
	require.Error(t, err)

	_, err = entities.NewMatchRun(context.Background(), uuid.Nil, value_objects.MatchRunModeDryRun)
	require.Error(t, err)
}

func TestMatchRunCompleteRequiresProcessing(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()

	run, err := entities.NewMatchRun(
		context.Background(),
		contextID,
		value_objects.MatchRunModeCommit,
	)
	require.NoError(t, err)
	require.NoError(t, run.Fail(context.Background(), "failure"))

	previousStatus := run.Status
	previousStats := run.Stats
	previousCompletedAt := run.CompletedAt
	previousFailureReason := run.FailureReason

	require.Error(t, run.Complete(context.Background(), map[string]int{"matches_found": 1}))
	require.Equal(t, previousStatus, run.Status)
	require.Equal(t, previousStats, run.Stats)
	require.Equal(t, previousCompletedAt, run.CompletedAt)
	require.Equal(t, previousFailureReason, run.FailureReason)
}

func TestMatchRunFailValidation(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()

	run, err := entities.NewMatchRun(
		context.Background(),
		contextID,
		value_objects.MatchRunModeCommit,
	)
	require.NoError(t, err)

	previousStatus := run.Status
	previousCompletedAt := run.CompletedAt
	previousFailureReason := run.FailureReason

	require.Error(t, run.Fail(context.Background(), ""))
	require.Equal(t, previousStatus, run.Status)
	require.Equal(t, previousCompletedAt, run.CompletedAt)
	require.Equal(t, previousFailureReason, run.FailureReason)
}

func TestMatchRunNilReceiver(t *testing.T) {
	t.Parallel()

	var run *entities.MatchRun

	require.Error(t, run.Fail(context.Background(), "reason"))
	require.Error(t, run.Complete(context.Background(), nil))
}
