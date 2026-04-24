//go:build integration

package reporting

import (
	"database/sql"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/tests/integration"
)

// TestIntegration_Reporting_ClaimNextQueued_NoJobs verifies that ClaimNextQueued returns nil, nil
// when the export_jobs queue is empty.
func TestIntegration_Reporting_ClaimNextQueued_NoJobs(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := newExportJobRepo(h)

		claimed, err := repo.ClaimNextQueued(ctx)
		require.NoError(t, err, "empty queue should not return an error")
		require.Nil(t, claimed, "empty queue should return nil job")
	})
}

// TestIntegration_Reporting_ClaimNextQueued_ClaimsSingleJob verifies that ClaimNextQueued atomically
// transitions a single QUEUED job to RUNNING, sets StartedAt, and increments
// Attempts from 0 to 1.
func TestIntegration_Reporting_ClaimNextQueued_ClaimsSingleJob(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := newExportJobRepo(h)

		original := createTestExportJob(
			t, ctx, repo,
			h.Seed.TenantID, h.Seed.ContextID,
			entities.ExportReportTypeMatched, entities.ExportFormatCSV,
		)
		require.Equal(t, entities.ExportJobStatusQueued, original.Status)
		require.Equal(t, 0, original.Attempts)

		claimed, err := repo.ClaimNextQueued(ctx)
		require.NoError(t, err)
		require.NotNil(t, claimed, "should claim the queued job")

		require.Equal(t, original.ID, claimed.ID, "claimed job ID must match created job")
		require.Equal(t, entities.ExportJobStatusRunning, claimed.Status,
			"claimed job must be RUNNING")
		require.NotNil(t, claimed.StartedAt, "StartedAt must be set after claim")
		require.Equal(t, 1, claimed.Attempts,
			"Attempts must be incremented from 0 to 1")

		// Verify the queue is now empty.
		next, err := repo.ClaimNextQueued(ctx)
		require.NoError(t, err)
		require.Nil(t, next, "queue should be empty after single job claimed")
	})
}

// TestIntegration_Reporting_ClaimNextQueued_FIFO_Order verifies that ClaimNextQueued returns jobs
// in created_at ascending order (FIFO), honouring the ORDER BY created_at ASC
// in the SELECT ... FOR UPDATE SKIP LOCKED sub-query.
func TestIntegration_Reporting_ClaimNextQueued_FIFO_Order(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := newExportJobRepo(h)

		const jobCount = 3

		createdIDs := make([]uuid.UUID, 0, jobCount)
		for range jobCount {
			job := createTestExportJob(
				t, ctx, repo,
				h.Seed.TenantID, h.Seed.ContextID,
				entities.ExportReportTypeSummary, entities.ExportFormatJSON,
			)
			createdIDs = append(createdIDs, job.ID)

			// Sleep to guarantee distinct created_at timestamps so ORDER BY
			// created_at ASC is deterministic.
			time.Sleep(15 * time.Millisecond)
		}

		// Claim all 3 jobs sequentially and verify FIFO ordering.
		for i := range jobCount {
			claimed, err := repo.ClaimNextQueued(ctx)
			require.NoError(t, err)
			require.NotNil(t, claimed, "claim %d should return a job", i+1)
			require.Equal(t, createdIDs[i], claimed.ID,
				"claim %d: expected job %s but got %s (FIFO violated)",
				i+1, createdIDs[i], claimed.ID)
			require.Equal(t, entities.ExportJobStatusRunning, claimed.Status)
		}

		// Queue must be exhausted.
		extra, err := repo.ClaimNextQueued(ctx)
		require.NoError(t, err)
		require.Nil(t, extra, "queue should be empty after all jobs claimed")
	})
}

// TestIntegration_Reporting_ClaimNextQueued_ConcurrentWorkers_NoDuplicates is the core test for the
// SELECT FOR UPDATE SKIP LOCKED semantics. Five concurrent goroutines race to
// claim five QUEUED jobs. Each job must be claimed exactly once — no duplicates,
// no missed jobs.
func TestIntegration_Reporting_ClaimNextQueued_ConcurrentWorkers_NoDuplicates(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := newExportJobRepo(h)

		const workerCount = 5

		expectedIDs := make(map[uuid.UUID]struct{}, workerCount)
		for range workerCount {
			job := createTestExportJob(
				t, ctx, repo,
				h.Seed.TenantID, h.Seed.ContextID,
				entities.ExportReportTypeVariance, entities.ExportFormatCSV,
			)
			expectedIDs[job.ID] = struct{}{}

			// Small delay so created_at differs (makes debugging easier).
			time.Sleep(5 * time.Millisecond)
		}

		// Launch workers concurrently. Every goroutine shares the same repo
		// (and underlying connection pool) — exactly how production workers
		// operate.
		var wg sync.WaitGroup

		results := make(chan *entities.ExportJob, workerCount)

		var errCount int64

		for range workerCount {
			wg.Add(1)

			go func() {
				defer wg.Done()

				claimed, err := repo.ClaimNextQueued(ctx)
				if err != nil {
					atomic.AddInt64(&errCount, 1)
					return
				}

				if claimed != nil {
					results <- claimed
				}
			}()
		}

		wg.Wait()
		close(results)

		require.Equal(t, int64(0), atomic.LoadInt64(&errCount),
			"no worker should encounter an error")

		// Collect results and assert uniqueness.
		seen := make(map[uuid.UUID]bool, workerCount)
		for job := range results {
			require.False(t, seen[job.ID],
				"duplicate claim detected for job %s — FOR UPDATE SKIP LOCKED violated", job.ID)
			seen[job.ID] = true

			require.Equal(t, entities.ExportJobStatusRunning, job.Status,
				"claimed job %s must be RUNNING", job.ID)
		}

		require.Equal(t, workerCount, len(seen),
			"all %d jobs must be claimed exactly once", workerCount)

		// Verify every created job was claimed.
		for id := range expectedIDs {
			require.True(t, seen[id],
				"job %s was created but never claimed", id)
		}

		// Queue must be empty.
		leftover, err := repo.ClaimNextQueued(ctx)
		require.NoError(t, err)
		require.Nil(t, leftover, "queue should be empty after all concurrent claims")
	})
}

// TestIntegration_Reporting_ClaimNextQueued_SkipsNonQueued verifies that ClaimNextQueued only picks
// up jobs in QUEUED status, ignoring RUNNING and COMPLETED jobs.
func TestIntegration_Reporting_ClaimNextQueued_SkipsNonQueued(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := newExportJobRepo(h)

		// Create 3 jobs — all start as QUEUED.
		queuedJob := createTestExportJob(
			t, ctx, repo,
			h.Seed.TenantID, h.Seed.ContextID,
			entities.ExportReportTypeMatched, entities.ExportFormatCSV,
		)

		runningJob := createTestExportJob(
			t, ctx, repo,
			h.Seed.TenantID, h.Seed.ContextID,
			entities.ExportReportTypeUnmatched, entities.ExportFormatJSON,
		)

		completedJob := createTestExportJob(
			t, ctx, repo,
			h.Seed.TenantID, h.Seed.ContextID,
			entities.ExportReportTypeSummary, entities.ExportFormatPDF,
		)

		// Force the second job to RUNNING via raw SQL.
		_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			_, execErr := tx.ExecContext(ctx,
				`UPDATE export_jobs SET status = $1, started_at = NOW(), attempts = 1, updated_at = NOW() WHERE id = $2`,
				entities.ExportJobStatusRunning, runningJob.ID.String(),
			)
			return struct{}{}, execErr
		})
		require.NoError(t, err, "failed to force job to RUNNING")

		// Force the third job to SUCCEEDED via raw SQL.
		_, err = pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			_, execErr := tx.ExecContext(ctx,
				`UPDATE export_jobs SET status = $1, finished_at = NOW(), updated_at = NOW() WHERE id = $2`,
				entities.ExportJobStatusSucceeded, completedJob.ID.String(),
			)
			return struct{}{}, execErr
		})
		require.NoError(t, err, "failed to force job to SUCCEEDED")

		// ClaimNextQueued must return only the QUEUED job.
		claimed, err := repo.ClaimNextQueued(ctx)
		require.NoError(t, err)
		require.NotNil(t, claimed, "should claim the QUEUED job")
		require.Equal(t, queuedJob.ID, claimed.ID,
			"should claim the QUEUED job, not RUNNING or SUCCEEDED")
		require.Equal(t, entities.ExportJobStatusRunning, claimed.Status,
			"claimed job must transition to RUNNING")

		// Queue should now be empty — RUNNING and SUCCEEDED are not eligible.
		next, err := repo.ClaimNextQueued(ctx)
		require.NoError(t, err)
		require.Nil(t, next,
			"no more QUEUED jobs — RUNNING and SUCCEEDED must be skipped")
	})
}
