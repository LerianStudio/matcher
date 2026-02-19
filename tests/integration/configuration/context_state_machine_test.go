//go:build integration

package configuration

import (
	"testing"

	"github.com/stretchr/testify/require"

	contextRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/context"
	fieldMapRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/field_map"
	matchRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/match_rule"
	sourceRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/source"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	configCommand "github.com/LerianStudio/matcher/internal/configuration/services/command"
	"github.com/LerianStudio/matcher/tests/integration"
)

// newCommandUseCase builds a configCommand.UseCase wired to the test harness database.
// Every state machine test shares this setup; factoring it avoids boilerplate drift.
func newCommandUseCase(t *testing.T, h *integration.TestHarness) *configCommand.UseCase {
	t.Helper()

	ctxRepo := contextRepo.NewRepository(h.Provider())
	srcRepo, err := sourceRepo.NewRepository(h.Provider())
	require.NoError(t, err)

	fmRepo := fieldMapRepo.NewRepository(h.Provider())
	mrRepo := matchRuleRepo.NewRepository(h.Provider())

	uc, err := configCommand.NewUseCase(ctxRepo, srcRepo, fmRepo, mrRepo)
	require.NoError(t, err)

	return uc
}

// createDraftContext is a test helper that creates a fresh DRAFT context via the
// use case and asserts the invariant that new contexts always start in DRAFT.
func createDraftContext(
	t *testing.T,
	h *integration.TestHarness,
	uc *configCommand.UseCase,
	name string,
) *entities.ReconciliationContext {
	t.Helper()

	ctx := h.Ctx()

	created, err := uc.CreateContext(ctx, h.Seed.TenantID, entities.CreateReconciliationContextInput{
		Name:     name,
		Type:     value_objects.ContextTypeOneToOne,
		Interval: "0 0 * * *",
	})
	require.NoError(t, err)
	require.Equal(t, value_objects.ContextStatusDraft, created.Status)

	return created
}

func TestContextStateMachine_NewContextStartsInDraft(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		uc := newCommandUseCase(t, h)

		created := createDraftContext(t, h, uc, "SM Draft Check")

		require.Equal(t, value_objects.ContextStatusDraft, created.Status,
			"newly created context must be in DRAFT status")
	})
}

func TestContextStateMachine_DraftToActive(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		uc := newCommandUseCase(t, h)
		ctx := h.Ctx()

		created := createDraftContext(t, h, uc, "SM Draft To Active")

		status := value_objects.ContextStatusActive
		updated, err := uc.UpdateContext(ctx, created.ID, entities.UpdateReconciliationContextInput{
			Status: &status,
		})
		require.NoError(t, err)
		require.Equal(t, value_objects.ContextStatusActive, updated.Status,
			"context must transition from DRAFT to ACTIVE")
	})
}

func TestContextStateMachine_ActiveToPaused(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		uc := newCommandUseCase(t, h)
		ctx := h.Ctx()

		created := createDraftContext(t, h, uc, "SM Active To Paused")

		// DRAFT -> ACTIVE
		activeStatus := value_objects.ContextStatusActive
		activated, err := uc.UpdateContext(ctx, created.ID, entities.UpdateReconciliationContextInput{
			Status: &activeStatus,
		})
		require.NoError(t, err)
		require.Equal(t, value_objects.ContextStatusActive, activated.Status)

		// ACTIVE -> PAUSED
		pausedStatus := value_objects.ContextStatusPaused
		paused, err := uc.UpdateContext(ctx, activated.ID, entities.UpdateReconciliationContextInput{
			Status: &pausedStatus,
		})
		require.NoError(t, err)
		require.Equal(t, value_objects.ContextStatusPaused, paused.Status,
			"context must transition from ACTIVE to PAUSED")
	})
}

func TestContextStateMachine_PausedToActive(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		uc := newCommandUseCase(t, h)
		ctx := h.Ctx()

		created := createDraftContext(t, h, uc, "SM Paused To Active")

		// DRAFT -> ACTIVE
		activeStatus := value_objects.ContextStatusActive
		activated, err := uc.UpdateContext(ctx, created.ID, entities.UpdateReconciliationContextInput{
			Status: &activeStatus,
		})
		require.NoError(t, err)
		require.Equal(t, value_objects.ContextStatusActive, activated.Status)

		// ACTIVE -> PAUSED
		pausedStatus := value_objects.ContextStatusPaused
		paused, err := uc.UpdateContext(ctx, activated.ID, entities.UpdateReconciliationContextInput{
			Status: &pausedStatus,
		})
		require.NoError(t, err)
		require.Equal(t, value_objects.ContextStatusPaused, paused.Status)

		// PAUSED -> ACTIVE (resume)
		resumed, err := uc.UpdateContext(ctx, paused.ID, entities.UpdateReconciliationContextInput{
			Status: &activeStatus,
		})
		require.NoError(t, err)
		require.Equal(t, value_objects.ContextStatusActive, resumed.Status,
			"context must transition from PAUSED back to ACTIVE (resume)")
	})
}

func TestContextStateMachine_ActiveToArchived(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		uc := newCommandUseCase(t, h)
		ctx := h.Ctx()

		created := createDraftContext(t, h, uc, "SM Active To Archived")

		// DRAFT -> ACTIVE
		activeStatus := value_objects.ContextStatusActive
		activated, err := uc.UpdateContext(ctx, created.ID, entities.UpdateReconciliationContextInput{
			Status: &activeStatus,
		})
		require.NoError(t, err)
		require.Equal(t, value_objects.ContextStatusActive, activated.Status)

		// ACTIVE -> ARCHIVED
		archivedStatus := value_objects.ContextStatusArchived
		archived, err := uc.UpdateContext(ctx, activated.ID, entities.UpdateReconciliationContextInput{
			Status: &archivedStatus,
		})
		require.NoError(t, err)
		require.Equal(t, value_objects.ContextStatusArchived, archived.Status,
			"context must transition from ACTIVE to ARCHIVED")
	})
}

func TestContextStateMachine_PausedToArchived(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		uc := newCommandUseCase(t, h)
		ctx := h.Ctx()

		created := createDraftContext(t, h, uc, "SM Paused To Archived")

		// DRAFT -> ACTIVE
		activeStatus := value_objects.ContextStatusActive
		activated, err := uc.UpdateContext(ctx, created.ID, entities.UpdateReconciliationContextInput{
			Status: &activeStatus,
		})
		require.NoError(t, err)
		require.Equal(t, value_objects.ContextStatusActive, activated.Status)

		// ACTIVE -> PAUSED
		pausedStatus := value_objects.ContextStatusPaused
		paused, err := uc.UpdateContext(ctx, activated.ID, entities.UpdateReconciliationContextInput{
			Status: &pausedStatus,
		})
		require.NoError(t, err)
		require.Equal(t, value_objects.ContextStatusPaused, paused.Status)

		// PAUSED -> ARCHIVED
		archivedStatus := value_objects.ContextStatusArchived
		archived, err := uc.UpdateContext(ctx, paused.ID, entities.UpdateReconciliationContextInput{
			Status: &archivedStatus,
		})
		require.NoError(t, err)
		require.Equal(t, value_objects.ContextStatusArchived, archived.Status,
			"context must transition from PAUSED to ARCHIVED")
	})
}

func TestContextStateMachine_InvalidTransitions(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		uc := newCommandUseCase(t, h)
		ctx := h.Ctx()

		t.Run("DRAFT_to_PAUSED", func(t *testing.T) {
			t.Parallel()

			created := createDraftContext(t, h, uc, "SM Invalid Draft Paused")

			pausedStatus := value_objects.ContextStatusPaused
			_, err := uc.UpdateContext(ctx, created.ID, entities.UpdateReconciliationContextInput{
				Status: &pausedStatus,
			})
			require.Error(t, err)
			require.ErrorIs(t, err, entities.ErrInvalidStateTransition,
				"DRAFT -> PAUSED must be rejected")
		})

		t.Run("DRAFT_to_ARCHIVED", func(t *testing.T) {
			t.Parallel()

			created := createDraftContext(t, h, uc, "SM Invalid Draft Archived")

			archivedStatus := value_objects.ContextStatusArchived
			_, err := uc.UpdateContext(ctx, created.ID, entities.UpdateReconciliationContextInput{
				Status: &archivedStatus,
			})
			require.Error(t, err)
			require.ErrorIs(t, err, entities.ErrInvalidStateTransition,
				"DRAFT -> ARCHIVED must be rejected")
		})

		t.Run("ARCHIVED_to_ACTIVE", func(t *testing.T) {
			t.Parallel()

			created := createDraftContext(t, h, uc, "SM Invalid Archived Active")

			// Move to ACTIVE then ARCHIVED
			activeStatus := value_objects.ContextStatusActive
			_, err := uc.UpdateContext(ctx, created.ID, entities.UpdateReconciliationContextInput{
				Status: &activeStatus,
			})
			require.NoError(t, err)

			archivedStatus := value_objects.ContextStatusArchived
			_, err = uc.UpdateContext(ctx, created.ID, entities.UpdateReconciliationContextInput{
				Status: &archivedStatus,
			})
			require.NoError(t, err)

			// ARCHIVED -> ACTIVE must fail
			_, err = uc.UpdateContext(ctx, created.ID, entities.UpdateReconciliationContextInput{
				Status: &activeStatus,
			})
			require.Error(t, err)
			require.ErrorIs(t, err, entities.ErrArchivedContextCannotBeModified,
				"ARCHIVED -> ACTIVE must be rejected (archived contexts are immutable)")
		})

		t.Run("ARCHIVED_to_PAUSED", func(t *testing.T) {
			t.Parallel()

			created := createDraftContext(t, h, uc, "SM Invalid Archived Paused")

			// Move to ACTIVE then ARCHIVED
			activeStatus := value_objects.ContextStatusActive
			_, err := uc.UpdateContext(ctx, created.ID, entities.UpdateReconciliationContextInput{
				Status: &activeStatus,
			})
			require.NoError(t, err)

			archivedStatus := value_objects.ContextStatusArchived
			_, err = uc.UpdateContext(ctx, created.ID, entities.UpdateReconciliationContextInput{
				Status: &archivedStatus,
			})
			require.NoError(t, err)

			// ARCHIVED -> PAUSED must fail
			pausedStatus := value_objects.ContextStatusPaused
			_, err = uc.UpdateContext(ctx, created.ID, entities.UpdateReconciliationContextInput{
				Status: &pausedStatus,
			})
			require.Error(t, err)
			require.ErrorIs(t, err, entities.ErrArchivedContextCannotBeModified,
				"ARCHIVED -> PAUSED must be rejected (archived contexts are immutable)")
		})
	})
}

func TestContextStateMachine_ArchivedContextRejectsUpdate(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		uc := newCommandUseCase(t, h)
		ctx := h.Ctx()

		created := createDraftContext(t, h, uc, "SM Archived Rejects Update")

		// DRAFT -> ACTIVE -> ARCHIVED
		activeStatus := value_objects.ContextStatusActive
		_, err := uc.UpdateContext(ctx, created.ID, entities.UpdateReconciliationContextInput{
			Status: &activeStatus,
		})
		require.NoError(t, err)

		archivedStatus := value_objects.ContextStatusArchived
		_, err = uc.UpdateContext(ctx, created.ID, entities.UpdateReconciliationContextInput{
			Status: &archivedStatus,
		})
		require.NoError(t, err)

		// Attempting a non-status field update on an archived context must fail.
		name := "New Name"
		_, err = uc.UpdateContext(ctx, created.ID, entities.UpdateReconciliationContextInput{
			Name: &name,
		})
		require.Error(t, err)
		require.ErrorIs(t, err, entities.ErrArchivedContextCannotBeModified,
			"any update to an archived context must be rejected")
	})
}
