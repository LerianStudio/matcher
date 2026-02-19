//go:build integration

package configuration

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	scheduleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/schedule"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/tests/integration"
)

// boolPtr returns a pointer to the given bool value.
func boolPtr(v bool) *bool { return &v }

func TestSchedule_CreateAndFindByID(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := scheduleRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		entity, err := entities.NewReconciliationSchedule(
			ctx,
			h.Seed.ContextID,
			entities.CreateScheduleInput{
				CronExpression: "0 0 * * *",
				Enabled:        boolPtr(true),
			},
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, entity)
		require.NoError(t, err)
		require.NotEqual(t, uuid.Nil, created.ID)
		require.Equal(t, h.Seed.ContextID, created.ContextID)
		require.Equal(t, "0 0 * * *", created.CronExpression)
		require.True(t, created.Enabled)
		require.NotNil(t, created.NextRunAt, "enabled schedule should have NextRunAt set")
		require.False(t, created.CreatedAt.IsZero())
		require.False(t, created.UpdatedAt.IsZero())

		fetched, err := repo.FindByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, created.ID, fetched.ID)
		require.Equal(t, created.ContextID, fetched.ContextID)
		require.Equal(t, created.CronExpression, fetched.CronExpression)
		require.Equal(t, created.Enabled, fetched.Enabled)
		require.False(t, fetched.CreatedAt.IsZero())
		require.False(t, fetched.UpdatedAt.IsZero())
	})
}

func TestSchedule_FindByContextID(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := scheduleRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		schedule1, err := entities.NewReconciliationSchedule(
			ctx,
			h.Seed.ContextID,
			entities.CreateScheduleInput{
				CronExpression: "0 6 * * *",
				Enabled:        boolPtr(true),
			},
		)
		require.NoError(t, err)

		_, err = repo.Create(ctx, schedule1)
		require.NoError(t, err)

		schedule2, err := entities.NewReconciliationSchedule(
			ctx,
			h.Seed.ContextID,
			entities.CreateScheduleInput{
				CronExpression: "0 18 * * *",
				Enabled:        boolPtr(true),
			},
		)
		require.NoError(t, err)

		_, err = repo.Create(ctx, schedule2)
		require.NoError(t, err)

		results, err := repo.FindByContextID(ctx, h.Seed.ContextID)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(results), 2, "should return at least the 2 schedules we created")

		// Verify both our schedules are present in the result set.
		foundIDs := make(map[uuid.UUID]bool, len(results))
		for _, s := range results {
			foundIDs[s.ID] = true
			require.Equal(t, h.Seed.ContextID, s.ContextID,
				"all returned schedules must belong to the queried context")
		}

		require.True(t, foundIDs[schedule1.ID], "schedule1 should be in results")
		require.True(t, foundIDs[schedule2.ID], "schedule2 should be in results")
	})
}

func TestSchedule_FindDueSchedules(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := scheduleRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		// Create an enabled schedule with NextRunAt firmly in the past.
		entity, err := entities.NewReconciliationSchedule(
			ctx,
			h.Seed.ContextID,
			entities.CreateScheduleInput{
				CronExpression: "0 0 * * *",
				Enabled:        boolPtr(true),
			},
		)
		require.NoError(t, err)

		// Force NextRunAt to 1 hour ago so it qualifies as "due".
		pastTime := time.Now().UTC().Add(-time.Hour)
		entity.NextRunAt = &pastTime

		created, err := repo.Create(ctx, entity)
		require.NoError(t, err)
		require.NotEqual(t, uuid.Nil, created.ID)

		// Query for due schedules using current time.
		now := time.Now().UTC()
		dueSchedules, err := repo.FindDueSchedules(ctx, now)
		require.NoError(t, err)

		found := false

		for _, s := range dueSchedules {
			if s.ID == created.ID {
				found = true
				require.True(t, s.Enabled, "due schedule must be enabled")
				require.NotEqual(t, uuid.Nil, s.TenantID,
					"FindDueSchedules JOIN should populate TenantID")

				break
			}
		}

		require.True(t, found, "schedule with past NextRunAt should be returned by FindDueSchedules")
	})
}

func TestSchedule_UpdateLastRunAndNextRun(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := scheduleRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		entity, err := entities.NewReconciliationSchedule(
			ctx,
			h.Seed.ContextID,
			entities.CreateScheduleInput{
				CronExpression: "0 12 * * *",
				Enabled:        boolPtr(true),
			},
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, entity)
		require.NoError(t, err)
		require.Nil(t, created.LastRunAt, "new schedule should not have LastRunAt set")

		// Simulate a run via MarkRun: sets LastRunAt and recalculates NextRunAt.
		runTime := time.Now().UTC()
		created.MarkRun(runTime)

		require.NotNil(t, created.LastRunAt, "MarkRun should set LastRunAt")
		require.NotNil(t, created.NextRunAt, "MarkRun should recalculate NextRunAt")

		updated, err := repo.Update(ctx, created)
		require.NoError(t, err)

		// Verify the persisted values reflect the update.
		require.NotNil(t, updated.LastRunAt)
		require.WithinDuration(t, runTime, *updated.LastRunAt, time.Second,
			"LastRunAt should match the run time")
		require.NotNil(t, updated.NextRunAt)
		require.True(t, updated.NextRunAt.After(runTime),
			"NextRunAt should be after the run time")

		// Re-fetch from DB to confirm persistence.
		fetched, err := repo.FindByID(ctx, created.ID)
		require.NoError(t, err)
		require.NotNil(t, fetched.LastRunAt)
		require.WithinDuration(t, runTime, *fetched.LastRunAt, time.Second)
		require.NotNil(t, fetched.NextRunAt)
		require.True(t, fetched.NextRunAt.After(runTime))
	})
}

func TestSchedule_DisabledScheduleNotDue(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := scheduleRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		// Create a disabled schedule.
		entity, err := entities.NewReconciliationSchedule(
			ctx,
			h.Seed.ContextID,
			entities.CreateScheduleInput{
				CronExpression: "0 0 * * *",
				Enabled:        boolPtr(false),
			},
		)
		require.NoError(t, err)
		require.False(t, entity.Enabled, "entity should be disabled")

		// The constructor sets NextRunAt to nil for disabled schedules.
		// Force NextRunAt to the past to prove that the "enabled = TRUE"
		// filter in FindDueSchedules is what excludes it, not the nil NextRunAt.
		pastTime := time.Now().UTC().Add(-time.Hour)
		entity.NextRunAt = &pastTime

		created, err := repo.Create(ctx, entity)
		require.NoError(t, err)
		require.NotEqual(t, uuid.Nil, created.ID)

		now := time.Now().UTC()
		dueSchedules, err := repo.FindDueSchedules(ctx, now)
		require.NoError(t, err)

		for _, s := range dueSchedules {
			require.NotEqual(t, created.ID, s.ID,
				"disabled schedule must NOT appear in due schedules even if NextRunAt is in the past")
		}
	})
}
