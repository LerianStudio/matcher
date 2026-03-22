//go:build integration

package reporting

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestExportJob_CreateAndRetrieve(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := newExportJobRepo(h)

		job := createTestExportJob(
			t, ctx, repo,
			h.Seed.TenantID, h.Seed.ContextID,
			entities.ExportReportTypeMatched, entities.ExportFormatCSV,
		)

		// Retrieve by ID
		fetched, err := repo.GetByID(ctx, job.ID)
		require.NoError(t, err)
		require.Equal(t, job.ID, fetched.ID)
		require.Equal(t, h.Seed.TenantID, fetched.TenantID)
		require.Equal(t, h.Seed.ContextID, fetched.ContextID)
		require.Equal(t, entities.ExportReportTypeMatched, fetched.ReportType)
		require.Equal(t, entities.ExportFormatCSV, fetched.Format)
		require.Equal(t, entities.ExportJobStatusQueued, fetched.Status)
		require.Zero(t, fetched.RecordsWritten)
		require.Zero(t, fetched.BytesWritten)
		require.Empty(t, fetched.FileKey)
		require.Empty(t, fetched.Error)
		require.Equal(t, 0, fetched.Attempts)
		require.Nil(t, fetched.StartedAt)
		require.Nil(t, fetched.FinishedAt)
		require.False(t, fetched.ExpiresAt.IsZero())
	})
}

func TestExportJob_GetByID_NotFound(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := newExportJobRepo(h)

		_, err := repo.GetByID(ctx, uuid.New())
		require.Error(t, err)
		require.ErrorIs(t, err, repositories.ErrExportJobNotFound)
	})
}

func TestExportJob_ListByContext(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := newExportJobRepo(h)

		// Create 3 jobs for the seeded context
		formats := []entities.ExportFormat{entities.ExportFormatCSV, entities.ExportFormatJSON, entities.ExportFormatPDF}
		createdIDs := make([]uuid.UUID, 0, len(formats))

		for _, format := range formats {
			job := createTestExportJob(
				t, ctx, repo,
				h.Seed.TenantID, h.Seed.ContextID,
				entities.ExportReportTypeSummary, format,
			)
			createdIDs = append(createdIDs, job.ID)

			// Small sleep to ensure distinct created_at ordering
			time.Sleep(10 * time.Millisecond)
		}

		// List by context
		jobs, err := repo.ListByContext(ctx, h.Seed.ContextID, 10)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(jobs), 3)

		// Verify ordering: newest first (created_at DESC)
		for i := 1; i < len(jobs); i++ {
			require.True(t,
				jobs[i-1].CreatedAt.After(jobs[i].CreatedAt) ||
					jobs[i-1].CreatedAt.Equal(jobs[i].CreatedAt),
				"jobs should be ordered by created_at DESC",
			)
		}

		// All created jobs should appear in results
		jobIDSet := make(map[uuid.UUID]bool)
		for _, j := range jobs {
			jobIDSet[j.ID] = true
		}

		for _, id := range createdIDs {
			require.True(t, jobIDSet[id], "created job %s should appear in list", id)
		}
	})
}

func TestExportJob_CancelQueuedJob(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := newExportJobRepo(h)

		job := createTestExportJob(
			t, ctx, repo,
			h.Seed.TenantID, h.Seed.ContextID,
			entities.ExportReportTypeUnmatched, entities.ExportFormatCSV,
		)
		require.Equal(t, entities.ExportJobStatusQueued, job.Status)

		// Cancel the queued job
		job.MarkCanceled()
		err := repo.UpdateStatus(ctx, job)
		require.NoError(t, err)

		// Verify persisted state
		fetched, err := repo.GetByID(ctx, job.ID)
		require.NoError(t, err)
		require.Equal(t, entities.ExportJobStatusCanceled, fetched.Status)
		require.NotNil(t, fetched.FinishedAt)
		require.True(t, fetched.IsTerminal())
	})
}

func TestExportJob_StatusTransitions(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := newExportJobRepo(h)

		// Create job in QUEUED status
		job := createTestExportJob(
			t, ctx, repo,
			h.Seed.TenantID, h.Seed.ContextID,
			entities.ExportReportTypeVariance, entities.ExportFormatJSON,
		)
		require.Equal(t, entities.ExportJobStatusQueued, job.Status)
		require.False(t, job.IsTerminal())

		// Transition: QUEUED -> RUNNING
		job.MarkRunning()
		require.Equal(t, entities.ExportJobStatusRunning, job.Status)
		require.NotNil(t, job.StartedAt)
		require.Equal(t, 1, job.Attempts)
		require.False(t, job.IsTerminal())

		err := repo.Update(ctx, job)
		require.NoError(t, err)

		fetched, err := repo.GetByID(ctx, job.ID)
		require.NoError(t, err)
		require.Equal(t, entities.ExportJobStatusRunning, fetched.Status)
		require.NotNil(t, fetched.StartedAt)
		require.Equal(t, 1, fetched.Attempts)

		// Update progress mid-run
		err = repo.UpdateProgress(ctx, job.ID, 500, 102400)
		require.NoError(t, err)

		fetched, err = repo.GetByID(ctx, job.ID)
		require.NoError(t, err)
		require.Equal(t, int64(500), fetched.RecordsWritten)
		require.Equal(t, int64(102400), fetched.BytesWritten)

		// Transition: RUNNING -> SUCCEEDED
		job.MarkSucceeded(
			"exports/test-file-key.csv",
			"VARIANCE_report.csv",
			"abc123sha256hash",
			1000, 204800,
		)
		require.Equal(t, entities.ExportJobStatusSucceeded, job.Status)
		require.True(t, job.IsTerminal())
		require.True(t, job.IsDownloadable())

		err = repo.Update(ctx, job)
		require.NoError(t, err)

		fetched, err = repo.GetByID(ctx, job.ID)
		require.NoError(t, err)
		require.Equal(t, entities.ExportJobStatusSucceeded, fetched.Status)
		require.Equal(t, "exports/test-file-key.csv", fetched.FileKey)
		require.Equal(t, "VARIANCE_report.csv", fetched.FileName)
		require.Equal(t, "abc123sha256hash", fetched.SHA256)
		require.Equal(t, int64(1000), fetched.RecordsWritten)
		require.Equal(t, int64(204800), fetched.BytesWritten)
		require.NotNil(t, fetched.FinishedAt)
	})
}

func TestExportJob_DownloadableOnlyWhenSucceeded(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := newExportJobRepo(h)

		job := createTestExportJob(
			t, ctx, repo,
			h.Seed.TenantID, h.Seed.ContextID,
			entities.ExportReportTypeMatched, entities.ExportFormatPDF,
		)

		// QUEUED: not downloadable
		fetched, err := repo.GetByID(ctx, job.ID)
		require.NoError(t, err)
		require.False(t, fetched.IsDownloadable(), "QUEUED job should not be downloadable")

		// RUNNING: not downloadable
		job.MarkRunning()
		err = repo.Update(ctx, job)
		require.NoError(t, err)

		fetched, err = repo.GetByID(ctx, job.ID)
		require.NoError(t, err)
		require.False(t, fetched.IsDownloadable(), "RUNNING job should not be downloadable")

		// FAILED: not downloadable
		job.MarkFailed("simulated error")
		err = repo.Update(ctx, job)
		require.NoError(t, err)

		fetched, err = repo.GetByID(ctx, job.ID)
		require.NoError(t, err)
		require.False(t, fetched.IsDownloadable(), "FAILED job should not be downloadable")
		require.Equal(t, "simulated error", fetched.Error)
		require.True(t, fetched.IsTerminal())
	})
}

func TestExportJob_DeleteRemovesRecord(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := newExportJobRepo(h)

		job := createTestExportJob(
			t, ctx, repo,
			h.Seed.TenantID, h.Seed.ContextID,
			entities.ExportReportTypeSummary, entities.ExportFormatXML,
		)

		// Verify it exists
		_, err := repo.GetByID(ctx, job.ID)
		require.NoError(t, err)

		// Delete it
		err = repo.Delete(ctx, job.ID)
		require.NoError(t, err)

		// Verify it's gone
		_, err = repo.GetByID(ctx, job.ID)
		require.Error(t, err)
		require.ErrorIs(t, err, repositories.ErrExportJobNotFound)
	})
}
