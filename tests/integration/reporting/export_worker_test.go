//go:build integration

package reporting

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/services/command"
	"github.com/LerianStudio/matcher/tests/integration"
)

// TestExportJobLifecycle_QueuedToRunning verifies that a QUEUED export job can
// transition to RUNNING with StartedAt set and Attempts incremented.
func TestExportJobLifecycle_QueuedToRunning(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := newExportJobRepo(h)

		// Create a job — starts in QUEUED.
		job := createTestExportJob(
			t, ctx, repo,
			h.Seed.TenantID, h.Seed.ContextID,
			entities.ExportReportTypeMatched, entities.ExportFormatCSV,
		)
		require.Equal(t, entities.ExportJobStatusQueued, job.Status)
		require.Nil(t, job.StartedAt)
		require.Equal(t, 0, job.Attempts)

		// Transition QUEUED → RUNNING.
		job.MarkRunning()
		require.Equal(t, entities.ExportJobStatusRunning, job.Status)
		require.NotNil(t, job.StartedAt)
		require.Equal(t, 1, job.Attempts)
		require.False(t, job.IsTerminal())

		// Persist and verify round-trip.
		err := repo.Update(ctx, job)
		require.NoError(t, err)

		fetched, err := repo.GetByID(ctx, job.ID)
		require.NoError(t, err)
		require.Equal(t, entities.ExportJobStatusRunning, fetched.Status)
		require.NotNil(t, fetched.StartedAt)
		require.Equal(t, 1, fetched.Attempts)
		require.WithinDuration(t, *job.StartedAt, *fetched.StartedAt, 0)
	})
}

// TestExportJobLifecycle_RunningToSucceeded exercises the full happy-path:
// QUEUED → RUNNING → SUCCEEDED with file metadata persisted.
func TestExportJobLifecycle_RunningToSucceeded(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := newExportJobRepo(h)

		job := createTestExportJob(
			t, ctx, repo,
			h.Seed.TenantID, h.Seed.ContextID,
			entities.ExportReportTypeUnmatched, entities.ExportFormatJSON,
		)

		// QUEUED → RUNNING.
		job.MarkRunning()

		err := repo.Update(ctx, job)
		require.NoError(t, err)

		// RUNNING → SUCCEEDED with file details.
		const (
			fileKey        = "exports/tenant/ctx/report.json"
			fileName       = "UNMATCHED_report.json"
			sha256Hash     = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
			recordsWritten = int64(2500)
			bytesWritten   = int64(512000)
		)

		job.MarkSucceeded(fileKey, fileName, sha256Hash, recordsWritten, bytesWritten)
		require.Equal(t, entities.ExportJobStatusSucceeded, job.Status)
		require.True(t, job.IsTerminal())
		require.True(t, job.IsDownloadable())
		require.NotNil(t, job.FinishedAt)

		err = repo.Update(ctx, job)
		require.NoError(t, err)

		// Verify full round-trip.
		fetched, err := repo.GetByID(ctx, job.ID)
		require.NoError(t, err)
		require.Equal(t, entities.ExportJobStatusSucceeded, fetched.Status)
		require.Equal(t, fileKey, fetched.FileKey)
		require.Equal(t, fileName, fetched.FileName)
		require.Equal(t, sha256Hash, fetched.SHA256)
		require.Equal(t, recordsWritten, fetched.RecordsWritten)
		require.Equal(t, bytesWritten, fetched.BytesWritten)
		require.NotNil(t, fetched.StartedAt)
		require.NotNil(t, fetched.FinishedAt)
		require.True(t, fetched.IsDownloadable())
		require.Empty(t, fetched.Error)
	})
}

// TestExportJobLifecycle_RunningToFailed exercises the failure path:
// QUEUED → RUNNING → FAILED with error message persisted.
func TestExportJobLifecycle_RunningToFailed(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := newExportJobRepo(h)

		job := createTestExportJob(
			t, ctx, repo,
			h.Seed.TenantID, h.Seed.ContextID,
			entities.ExportReportTypeVariance, entities.ExportFormatCSV,
		)

		// QUEUED → RUNNING.
		job.MarkRunning()

		err := repo.Update(ctx, job)
		require.NoError(t, err)

		// RUNNING → FAILED with error message.
		const errMsg = "storage upload failed: connection refused"

		job.MarkFailed(errMsg)
		require.Equal(t, entities.ExportJobStatusFailed, job.Status)
		require.True(t, job.IsTerminal())
		require.False(t, job.IsDownloadable())
		require.NotNil(t, job.FinishedAt)
		require.Equal(t, errMsg, job.Error)

		err = repo.Update(ctx, job)
		require.NoError(t, err)

		// Verify round-trip.
		fetched, err := repo.GetByID(ctx, job.ID)
		require.NoError(t, err)
		require.Equal(t, entities.ExportJobStatusFailed, fetched.Status)
		require.Equal(t, errMsg, fetched.Error)
		require.NotNil(t, fetched.StartedAt)
		require.NotNil(t, fetched.FinishedAt)
		require.False(t, fetched.IsDownloadable())
		// File metadata should be empty on failure.
		require.Empty(t, fetched.FileKey)
		require.Empty(t, fetched.FileName)
		require.Empty(t, fetched.SHA256)
		require.Zero(t, fetched.RecordsWritten)
	})
}

// TestExportJobLifecycle_CancelQueuedJob verifies that a QUEUED job can be
// cancelled through the use case, persisting CANCELED status and FinishedAt.
func TestExportJobLifecycle_CancelQueuedJob(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := newExportJobRepo(h)

		job := createTestExportJob(
			t, ctx, repo,
			h.Seed.TenantID, h.Seed.ContextID,
			entities.ExportReportTypeSummary, entities.ExportFormatCSV,
		)
		require.Equal(t, entities.ExportJobStatusQueued, job.Status)

		// Cancel via use case.
		uc, err := command.NewExportJobUseCase(repo)
		require.NoError(t, err)

		err = uc.CancelExportJob(ctx, job.ID)
		require.NoError(t, err)

		// Verify persisted state.
		fetched, err := repo.GetByID(ctx, job.ID)
		require.NoError(t, err)
		require.Equal(t, entities.ExportJobStatusCanceled, fetched.Status)
		require.NotNil(t, fetched.FinishedAt)
		require.True(t, fetched.IsTerminal())
		require.False(t, fetched.IsDownloadable())
	})
}

// TestExportJobLifecycle_CancelRunningJobSucceeds verifies that a RUNNING job
// can be cancelled. The CancelExportJob use case permits cancellation from any
// non-terminal state (QUEUED or RUNNING). IsTerminal() returns false for RUNNING,
// so the guard passes and the job transitions to CANCELED.
func TestExportJobLifecycle_CancelRunningJobSucceeds(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := newExportJobRepo(h)

		job := createTestExportJob(
			t, ctx, repo,
			h.Seed.TenantID, h.Seed.ContextID,
			entities.ExportReportTypeMatched, entities.ExportFormatJSON,
		)

		// Transition to RUNNING (simulates worker claiming the job).
		job.MarkRunning()

		err := repo.Update(ctx, job)
		require.NoError(t, err)

		// Cancel a RUNNING job — the implementation allows this.
		uc, err := command.NewExportJobUseCase(repo)
		require.NoError(t, err)

		err = uc.CancelExportJob(ctx, job.ID)
		require.NoError(t, err, "cancelling a RUNNING job should succeed")

		// Verify the job transitioned to CANCELED.
		fetched, err := repo.GetByID(ctx, job.ID)
		require.NoError(t, err)
		require.Equal(t, entities.ExportJobStatusCanceled, fetched.Status,
			"RUNNING job should be CANCELED after cancel")
		require.True(t, fetched.IsTerminal(), "CANCELED job should be terminal")
		require.NotNil(t, fetched.FinishedAt, "FinishedAt should be set on cancel")
	})
}
