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

func TestMatchRun_Complete_DefensiveCopyStats(t *testing.T) {
	t.Parallel()

	run, err := entities.NewMatchRun(
		context.Background(),
		uuid.New(),
		value_objects.MatchRunModeDryRun,
	)
	require.NoError(t, err)

	stats := map[string]int{"matches": 1}
	require.NoError(t, run.Complete(context.Background(), stats))

	stats["matches"] = 999
	stats["new_key"] = 123

	require.Equal(t, 1, run.Stats["matches"], "entity must defensively copy caller-owned maps")
	_, exists := run.Stats["new_key"]
	require.False(t, exists)
}

func TestMatchRun_Complete_TransitionsStatusAndSetsCompletedAt(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	run, err := entities.NewMatchRun(ctx, uuid.New(), value_objects.MatchRunModeDryRun)
	require.NoError(t, err)

	require.Equal(t, value_objects.MatchRunStatusProcessing, run.Status)
	require.Nil(t, run.CompletedAt)

	stats := map[string]int{"matches": 5, "unmatched": 2}
	require.NoError(t, run.Complete(ctx, stats))

	require.Equal(t, value_objects.MatchRunStatusCompleted, run.Status)
	require.NotNil(t, run.CompletedAt)
	require.Equal(t, 5, run.Stats["matches"])
	require.Equal(t, 2, run.Stats["unmatched"])
	require.Nil(t, run.FailureReason)
}

func TestMatchRun_Complete_ErrorOnAlreadyCompleted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	run, err := entities.NewMatchRun(ctx, uuid.New(), value_objects.MatchRunModeDryRun)
	require.NoError(t, err)

	require.NoError(t, run.Complete(ctx, nil))

	err = run.Complete(ctx, map[string]int{"matches": 10})
	require.ErrorIs(t, err, entities.ErrMatchRunMustBeProcessingToComplete)
}

func TestMatchRun_Complete_ErrorOnFailedRun(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	run, err := entities.NewMatchRun(ctx, uuid.New(), value_objects.MatchRunModeDryRun)
	require.NoError(t, err)

	require.NoError(t, run.Fail(ctx, "database error"))

	err = run.Complete(ctx, nil)
	require.ErrorIs(t, err, entities.ErrMatchRunMustBeProcessingToComplete)
}

func TestMatchRun_Complete_AcceptsNilStats(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	run, err := entities.NewMatchRun(ctx, uuid.New(), value_objects.MatchRunModeDryRun)
	require.NoError(t, err)

	require.NoError(t, run.Complete(ctx, nil))

	require.NotNil(t, run.Stats)
	require.Empty(t, run.Stats)
}

func TestMatchRun_Fail_TransitionsStatusAndSetsReason(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	run, err := entities.NewMatchRun(ctx, uuid.New(), value_objects.MatchRunModeDryRun)
	require.NoError(t, err)

	require.Equal(t, value_objects.MatchRunStatusProcessing, run.Status)
	require.Nil(t, run.FailureReason)

	reason := "database connection lost"
	require.NoError(t, run.Fail(ctx, reason))

	require.Equal(t, value_objects.MatchRunStatusFailed, run.Status)
	require.NotNil(t, run.FailureReason)
	require.Equal(t, reason, *run.FailureReason)
	require.NotNil(t, run.CompletedAt)
}

func TestMatchRun_Fail_ErrorOnAlreadyFailed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	run, err := entities.NewMatchRun(ctx, uuid.New(), value_objects.MatchRunModeDryRun)
	require.NoError(t, err)

	require.NoError(t, run.Fail(ctx, "first failure"))

	err = run.Fail(ctx, "second failure")
	require.ErrorIs(t, err, entities.ErrMatchRunMustBeProcessingToFail)
}

func TestMatchRun_Fail_ErrorOnCompletedRun(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	run, err := entities.NewMatchRun(ctx, uuid.New(), value_objects.MatchRunModeDryRun)
	require.NoError(t, err)

	require.NoError(t, run.Complete(ctx, nil))

	err = run.Fail(ctx, "too late to fail")
	require.ErrorIs(t, err, entities.ErrMatchRunMustBeProcessingToFail)
}

func TestMatchRun_Fail_ErrorOnEmptyReason(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	run, err := entities.NewMatchRun(ctx, uuid.New(), value_objects.MatchRunModeDryRun)
	require.NoError(t, err)

	err = run.Fail(ctx, "")
	require.ErrorIs(t, err, entities.ErrFailureReasonRequired)
}
