//go:build integration

package ingestion

import (
	"database/sql"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	jobRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/repositories"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/value_objects"
	"github.com/LerianStudio/matcher/tests/integration"
)

var errTestRollback = errors.New("rollback")

func TestIntegration_Ingestion_JobRepository_CreateAndFindByID(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := jobRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		job, err := entities.NewIngestionJob(ctx, h.Seed.ContextID, h.Seed.SourceID, "file.csv", 10)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))

		created, err := repo.Create(ctx, job)
		require.NoError(t, err)
		require.NotEqual(t, uuid.Nil, created.ID)

		fetched, err := repo.FindByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, created.ID, fetched.ID)
		require.Equal(t, created.ContextID, fetched.ContextID)
		require.Equal(t, created.SourceID, fetched.SourceID)
		require.Equal(t, created.Status, fetched.Status)
		require.Equal(t, created.Metadata.FileName, fetched.Metadata.FileName)
		require.Equal(t, created.Metadata.FileSize, fetched.Metadata.FileSize)
		require.True(t, created.StartedAt.Equal(fetched.StartedAt))
		require.True(t, created.CreatedAt.Equal(fetched.CreatedAt))
		require.True(t, created.UpdatedAt.Equal(fetched.UpdatedAt))
	})
}

func TestIntegration_Ingestion_JobRepository_Update(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := jobRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		job, err := entities.NewIngestionJob(ctx, h.Seed.ContextID, h.Seed.SourceID, "file.csv", 10)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))

		created, err := repo.Create(ctx, job)
		require.NoError(t, err)

		require.NoError(t, created.Complete(ctx, 2, 1))
		updated, err := repo.Update(ctx, created)
		require.NoError(t, err)
		require.Equal(t, value_objects.JobStatusCompleted, updated.Status)

		data, err := updated.MetadataJSON()
		require.NoError(t, err)

		var metadata entities.JobMetadata
		require.NoError(t, json.Unmarshal(data, &metadata))
		require.Equal(t, 2, metadata.TotalRows)
		require.Equal(t, 1, metadata.FailedRows)
	})
}

func TestIntegration_Ingestion_JobRepository_FindByID_NotFound(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := jobRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		_, err := repo.FindByID(ctx, uuid.New())
		require.ErrorIs(t, err, sql.ErrNoRows)
	})
}

func TestIntegration_Ingestion_JobRepository_FindByContextID_Empty(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := jobRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		jobs, pagination, err := repo.FindByContextID(
			ctx,
			uuid.New(),
			repositories.CursorFilter{Limit: 1, SortBy: "id"},
		)
		require.NoError(t, err)
		require.Empty(t, jobs)
		require.Empty(t, pagination.Next)
	})
}

func TestIntegration_Ingestion_JobRepository_Create_DuplicateKey(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := jobRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		job, err := entities.NewIngestionJob(ctx, h.Seed.ContextID, h.Seed.SourceID, "file.csv", 10)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))

		created, err := repo.Create(ctx, job)
		require.NoError(t, err)

		job.ID = created.ID
		_, err = repo.Create(ctx, job)
		require.Error(t, err)
	})
}

func TestIntegration_Ingestion_JobRepository_FindByContextID_Pagination(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := jobRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		job1, err := entities.NewIngestionJob(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			"file1.csv",
			10,
		)
		require.NoError(t, err)
		require.NoError(t, job1.Start(ctx))
		job1.ID = uuid.MustParse("00000000-0000-0000-0000-000000000010")
		created1, err := repo.Create(ctx, job1)
		require.NoError(t, err)

		job2, err := entities.NewIngestionJob(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			"file2.csv",
			11,
		)
		require.NoError(t, err)
		require.NoError(t, job2.Start(ctx))
		job2.ID = uuid.MustParse("00000000-0000-0000-0000-000000000020")
		created2, err := repo.Create(ctx, job2)
		require.NoError(t, err)

		jobs, pagination, err := repo.FindByContextID(
			ctx,
			h.Seed.ContextID,
			repositories.CursorFilter{Limit: 1, SortBy: "id"},
		)
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		require.Equal(t, created1.ID, jobs[0].ID)
		require.NotEmpty(t, pagination.Next)

		nextJobs, nextPagination, err := repo.FindByContextID(
			ctx,
			h.Seed.ContextID,
			repositories.CursorFilter{Limit: 1, SortBy: "id", Cursor: pagination.Next},
		)
		require.NoError(t, err)
		require.Len(t, nextJobs, 1)
		require.Equal(t, created2.ID, nextJobs[0].ID)
		require.NotEmpty(t, nextPagination.Prev)
	})
}

func TestIntegration_Ingestion_JobRepository_WithTx(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := jobRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		job, err := entities.NewIngestionJob(ctx, h.Seed.ContextID, h.Seed.SourceID, "file.csv", 10)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))

		var createdJob *entities.IngestionJob
		err = repo.WithTx(ctx, func(tx *sql.Tx) error {
			var txErr error
			createdJob, txErr = repo.CreateWithTx(ctx, tx, job)

			return txErr
		})
		require.NoError(t, err)
		require.NotNil(t, createdJob)

		fetched, err := repo.FindByID(ctx, createdJob.ID)
		require.NoError(t, err)
		require.NotNil(t, fetched)
		require.Equal(t, createdJob.ID, fetched.ID)
	})
}

func TestIntegration_Ingestion_JobRepository_WithTxRollback(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := jobRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		job, err := entities.NewIngestionJob(ctx, h.Seed.ContextID, h.Seed.SourceID, "file.csv", 10)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))

		var createdJob *entities.IngestionJob
		err = repo.WithTx(ctx, func(tx *sql.Tx) error {
			var txErr error
			createdJob, txErr = repo.CreateWithTx(ctx, tx, job)
			if txErr != nil {
				return txErr
			}

			return errTestRollback
		})
		require.ErrorIs(t, err, errTestRollback)
		require.NotNil(t, createdJob)

		_, err = repo.FindByID(ctx, createdJob.ID)
		require.Error(t, err)
	})
}
