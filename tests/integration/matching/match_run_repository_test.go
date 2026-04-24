//go:build integration

package matching

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	matchRunRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/match_run"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepositories "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestIntegration_Matching_MatchRunRepository_CreateUpdateAndFind(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := matchRunRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		run, err := matchingEntities.NewMatchRun(
			ctx,
			h.Seed.ContextID,
			matchingVO.MatchRunModeCommit,
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, run)
		require.NoError(t, err)
		require.NotEqual(t, uuid.Nil, created.ID)
		require.Equal(t, matchingVO.MatchRunStatusProcessing, created.Status)

		reason := "test failure"
		require.NoError(t, created.Fail(ctx, reason))

		updated, err := repo.Update(ctx, created)
		require.NoError(t, err)
		require.Equal(t, matchingVO.MatchRunStatusFailed, updated.Status)
		require.NotNil(t, updated.FailureReason)
		require.Equal(t, reason, *updated.FailureReason)

		fetched, err := repo.FindByID(ctx, h.Seed.ContextID, updated.ID)
		require.NoError(t, err)
		require.Equal(t, updated.ID, fetched.ID)
		require.Equal(t, updated.ContextID, fetched.ContextID)
		require.Equal(t, matchingVO.MatchRunStatusFailed, fetched.Status)
	})
}

func TestIntegration_Matching_MatchRunRepository_Complete(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := matchRunRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		run, err := matchingEntities.NewMatchRun(
			ctx,
			h.Seed.ContextID,
			matchingVO.MatchRunModeDryRun,
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, run)
		require.NoError(t, err)
		require.Equal(t, matchingVO.MatchRunStatusProcessing, created.Status)

		stats := map[string]int{
			"transactions_processed": 10,
			"groups_created":         5,
			"exceptions_created":     3,
		}
		require.NoError(t, created.Complete(ctx, stats))
		completed, err := repo.Update(ctx, created)
		require.NoError(t, err)
		require.Equal(t, matchingVO.MatchRunStatusCompleted, completed.Status)
		require.Equal(t, 10, completed.Stats["transactions_processed"])
		require.Equal(t, 5, completed.Stats["groups_created"])
		require.Equal(t, 3, completed.Stats["exceptions_created"])
	})
}

func TestIntegration_Matching_MatchRunRepository_ListByContextID(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := matchRunRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		for i := 0; i < 3; i++ {
			run, err := matchingEntities.NewMatchRun(
				ctx,
				h.Seed.ContextID,
				matchingVO.MatchRunModeCommit,
			)
			require.NoError(t, err)
			_, err = repo.Create(ctx, run)
			require.NoError(t, err)
		}

		runs, _, err := repo.ListByContextID(
			ctx,
			h.Seed.ContextID,
			matchingRepositories.CursorFilter{Limit: 10},
		)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(runs), 3)
	})
}
