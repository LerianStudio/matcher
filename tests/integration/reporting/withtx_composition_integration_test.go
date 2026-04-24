//go:build integration

// Package reporting contains integration tests for the reporting context's
// WithTx repository surface under multi-aggregate composition.
//
// ExportJob state machine transitions (QUEUED → RUNNING → SUCCEEDED/FAILED)
// compose Create + UpdateStatus in the same tx during worker checkpointing.
// A partial rollback mid-transition would leave the job visible to the
// dashboard in an inconsistent state — a "running" job with no real worker,
// or a completed job with no recorded output.
//
// Covers FINDING-042 (REFACTOR-051).
package reporting

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/tests/integration"
)

// errDeliberateRollback forces the composition callback to abort, triggering
// tx rollback so the test can verify atomic undo across all repos involved.
var errDeliberateRollback = errors.New("deliberate rollback for composition test")

// TestIntegration_Reporting_WithTxComposition_ExportJobCreateAndUpdateStatus_Rollback
// asserts that ExportJob.CreateWithTx + UpdateStatusWithTx roll back atomically.
// The worker pattern claims a queued job and marks it RUNNING in one tx; if
// that tx aborts the job must remain QUEUED (or not exist) — never stuck in
// RUNNING with no worker heartbeat.
func TestIntegration_Reporting_WithTxComposition_ExportJobCreateAndUpdateStatus_Rollback(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := newExportJobRepo(h)

		filter := entities.ExportJobFilter{
			DateFrom: time.Now().UTC().AddDate(0, -1, 0),
			DateTo:   time.Now().UTC(),
		}
		job, err := entities.NewExportJob(
			ctx,
			h.Seed.TenantID,
			h.Seed.ContextID,
			entities.ExportReportTypeMatched,
			entities.ExportFormatCSV,
			filter,
		)
		require.NoError(t, err)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if err := repo.CreateWithTx(ctx, tx, job); err != nil {
				return struct{}{}, err
			}

			if err := job.MarkRunning(); err != nil {
				return struct{}{}, err
			}

			if err := repo.UpdateStatusWithTx(ctx, tx, job); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, errDeliberateRollback
		})
		require.ErrorIs(t, txErr, errDeliberateRollback)

		_, err = repo.GetByID(ctx, job.ID)
		require.Error(t, err,
			"ExportJob must not persist after Create+UpdateStatus+rollback")
	})
}

// TestIntegration_Reporting_WithTxComposition_ExportJobCreateAndUpdateStatus_Commit
// is the commit counterpart — job visible in RUNNING status.
func TestIntegration_Reporting_WithTxComposition_ExportJobCreateAndUpdateStatus_Commit(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := newExportJobRepo(h)

		filter := entities.ExportJobFilter{
			DateFrom: time.Now().UTC().AddDate(0, -1, 0),
			DateTo:   time.Now().UTC(),
		}
		job, err := entities.NewExportJob(
			ctx,
			h.Seed.TenantID,
			h.Seed.ContextID,
			entities.ExportReportTypeMatched,
			entities.ExportFormatCSV,
			filter,
		)
		require.NoError(t, err)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if err := repo.CreateWithTx(ctx, tx, job); err != nil {
				return struct{}{}, err
			}

			if err := job.MarkRunning(); err != nil {
				return struct{}{}, err
			}

			if err := repo.UpdateStatusWithTx(ctx, tx, job); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, nil
		})
		require.NoError(t, txErr)

		found, err := repo.GetByID(ctx, job.ID)
		require.NoError(t, err)
		require.NotNil(t, found)
		require.Equal(t, entities.ExportJobStatusRunning, found.Status)
	})
}

// TestIntegration_Reporting_WithTxComposition_ExportJobCreateAndUpdateProgress_Rollback
// asserts that Create + UpdateProgress compose atomically. Progress counters
// (records_written, bytes_written) are auditor-visible — a partial rollback
// would make a freshly-created job appear to have already processed rows.
func TestIntegration_Reporting_WithTxComposition_ExportJobCreateAndUpdateProgress_Rollback(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := newExportJobRepo(h)

		filter := entities.ExportJobFilter{
			DateFrom: time.Now().UTC().AddDate(0, -1, 0),
			DateTo:   time.Now().UTC(),
		}
		job, err := entities.NewExportJob(
			ctx,
			h.Seed.TenantID,
			h.Seed.ContextID,
			entities.ExportReportTypeMatched,
			entities.ExportFormatCSV,
			filter,
		)
		require.NoError(t, err)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if err := repo.CreateWithTx(ctx, tx, job); err != nil {
				return struct{}{}, err
			}

			if err := repo.UpdateProgressWithTx(ctx, tx, job.ID, 100, 2048); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, errDeliberateRollback
		})
		require.ErrorIs(t, txErr, errDeliberateRollback)

		_, err = repo.GetByID(ctx, job.ID)
		require.Error(t, err,
			"ExportJob must not persist after Create+UpdateProgress+rollback")
	})
}

// TestIntegration_Reporting_WithTxComposition_ExportJobCreateAndUpdateProgress_Commit
// is the commit counterpart — job visible with advanced progress counters.
func TestIntegration_Reporting_WithTxComposition_ExportJobCreateAndUpdateProgress_Commit(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := newExportJobRepo(h)

		filter := entities.ExportJobFilter{
			DateFrom: time.Now().UTC().AddDate(0, -1, 0),
			DateTo:   time.Now().UTC(),
		}
		job, err := entities.NewExportJob(
			ctx,
			h.Seed.TenantID,
			h.Seed.ContextID,
			entities.ExportReportTypeMatched,
			entities.ExportFormatCSV,
			filter,
		)
		require.NoError(t, err)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if err := repo.CreateWithTx(ctx, tx, job); err != nil {
				return struct{}{}, err
			}

			if err := repo.UpdateProgressWithTx(ctx, tx, job.ID, 100, 2048); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, nil
		})
		require.NoError(t, txErr)

		found, err := repo.GetByID(ctx, job.ID)
		require.NoError(t, err)
		require.NotNil(t, found)
		require.EqualValues(t, 100, found.RecordsWritten)
		require.EqualValues(t, 2048, found.BytesWritten)
	})
}

// TestIntegration_Reporting_WithTxComposition_ExportJobCreateAndDelete_Rollback
// asserts that Create + Delete compose atomically. Admins sometimes queue a
// job and immediately cancel/delete it as part of a rollback flow; a partial
// failure here would leak half-deleted jobs.
func TestIntegration_Reporting_WithTxComposition_ExportJobCreateAndDelete_Rollback(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := newExportJobRepo(h)

		// Pre-create an unrelated job to delete inside the composition, and a
		// second job to create inside the composition. Rollback must preserve
		// the first (Delete undone) and remove the second (Create undone).
		seedJob := createTestExportJob(
			t, ctx, repo,
			h.Seed.TenantID, h.Seed.ContextID,
			entities.ExportReportTypeMatched, entities.ExportFormatCSV,
		)

		filter := entities.ExportJobFilter{
			DateFrom: time.Now().UTC().AddDate(0, -1, 0),
			DateTo:   time.Now().UTC(),
		}
		newJob, err := entities.NewExportJob(
			ctx,
			h.Seed.TenantID,
			h.Seed.ContextID,
			entities.ExportReportTypeMatched,
			entities.ExportFormatJSON,
			filter,
		)
		require.NoError(t, err)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if err := repo.CreateWithTx(ctx, tx, newJob); err != nil {
				return struct{}{}, err
			}

			if err := repo.DeleteWithTx(ctx, tx, seedJob.ID); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, errDeliberateRollback
		})
		require.ErrorIs(t, txErr, errDeliberateRollback)

		// The newly-created job must not exist (Create rolled back).
		_, err = repo.GetByID(ctx, newJob.ID)
		require.Error(t, err, "new ExportJob must not persist after Create+rollback")

		// The pre-existing job must still exist (Delete rolled back).
		foundSeed, err := repo.GetByID(ctx, seedJob.ID)
		require.NoError(t, err,
			"pre-existing ExportJob must survive Delete+rollback")
		require.NotNil(t, foundSeed)
		require.Equal(t, seedJob.ID, foundSeed.ID)
	})
}

// TestIntegration_Reporting_WithTxComposition_ExportJobCreateAndDelete_Commit
// is the commit counterpart — new job visible, old job gone.
func TestIntegration_Reporting_WithTxComposition_ExportJobCreateAndDelete_Commit(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := newExportJobRepo(h)

		seedJob := createTestExportJob(
			t, ctx, repo,
			h.Seed.TenantID, h.Seed.ContextID,
			entities.ExportReportTypeMatched, entities.ExportFormatCSV,
		)

		filter := entities.ExportJobFilter{
			DateFrom: time.Now().UTC().AddDate(0, -1, 0),
			DateTo:   time.Now().UTC(),
		}
		newJob, err := entities.NewExportJob(
			ctx,
			h.Seed.TenantID,
			h.Seed.ContextID,
			entities.ExportReportTypeMatched,
			entities.ExportFormatJSON,
			filter,
		)
		require.NoError(t, err)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if err := repo.CreateWithTx(ctx, tx, newJob); err != nil {
				return struct{}{}, err
			}

			if err := repo.DeleteWithTx(ctx, tx, seedJob.ID); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, nil
		})
		require.NoError(t, txErr)

		foundNew, err := repo.GetByID(ctx, newJob.ID)
		require.NoError(t, err)
		require.NotNil(t, foundNew)

		_, err = repo.GetByID(ctx, seedJob.ID)
		require.Error(t, err,
			"pre-existing ExportJob must be deleted after commit")
	})
}
