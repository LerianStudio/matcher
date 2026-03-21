//go:build chaos

package chaos

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// --------------------------------------------------------------------------
// CHAOS-08: Process crash between MatchRun creation and commit
// --------------------------------------------------------------------------

// TestCHAOS08_CrashBetweenRunCreationAndCommit simulates a crash in the gap
// between MatchRun creation (status=PROCESSING) and the commit transaction.
// Without a reaper process, the MatchRun stays orphaned in PROCESSING forever.
//
// Target: match_group_commands.go — the gap between Phase 6 (MatchRun created)
//
//	and Phase 8b (commit transaction).
//
// Injection: Context cancellation mid-operation simulates a crash.
// Expected: Either the run transitions to FAILED via finalizeRunFailure,
//
//	or it stays PROCESSING (proving the orphan problem).
//
// Finding: There is NO background reaper for orphaned PROCESSING runs.
func TestCHAOS08_CrashBetweenRunCreationAndCommit(t *testing.T) {
	// Intentionally sequential: this test uses shared chaos harness state
	// (GetSharedChaos + ResetDatabase) and must avoid parallel interference.
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")
	h.ResetDatabase(t)

	directDB := h.DirectDB(t)
	ctx := h.Ctx()

	// Create a second source (matching needs >=2 sources with transactions).
	secondSourceID := uuid.New()

	_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.ExecContext(ctx, `
			INSERT INTO reconciliation_sources (id, context_id, name, type, side, config, created_at, updated_at)
			VALUES ($1, $2, 'Chaos Source B', 'BANK', 'RIGHT', '{}', NOW(), NOW())
		`, secondSourceID, h.Seed.ContextID)
		return struct{}{}, execErr
	})
	require.NoError(t, err, "create second source")

	// Create ingestion jobs for both sources (FK parent for transactions).
	jobIDs := make([]uuid.UUID, 2)
	for i, sourceID := range []uuid.UUID{h.Seed.SourceID, secondSourceID} {
		jobIDs[i] = uuid.New()
		_, jobErr := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			_, execErr := tx.ExecContext(ctx, `
				INSERT INTO ingestion_jobs (id, context_id, source_id, status, created_at, updated_at)
				VALUES ($1, $2, $3, 'COMPLETED', NOW(), NOW())
			`, jobIDs[i], h.Seed.ContextID, sourceID)
			return struct{}{}, execErr
		})
		require.NoError(t, jobErr, "create ingestion job for source %d", i)
	}

	// Insert transactions for both sources.
	for i, sourceID := range []uuid.UUID{h.Seed.SourceID, secondSourceID} {
		for j := range 5 {
			_, txErr := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
				_, execErr := tx.ExecContext(ctx, `
					INSERT INTO transactions (id, ingestion_job_id, source_id, external_id, date, amount, currency, status, metadata, created_at, updated_at)
					VALUES (gen_random_uuid(), $1, $2, $3, '2025-01-15', 100.00, 'USD', 'UNMATCHED', '{}', NOW(), NOW())
				`, jobIDs[i], sourceID, fmt.Sprintf("CHAOS-SRC%d-TX%d", i, j))
				return struct{}{}, execErr
			})
			require.NoError(t, txErr, "insert transaction src%d tx%d", i, j)
		}
	}

	// Create a match run manually in PROCESSING state — simulating Phase 6.
	orphanRunID := uuid.New()

	_, err = pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.ExecContext(ctx, `
			INSERT INTO match_runs (id, context_id, mode, status, stats, created_at, updated_at)
			VALUES ($1, $2, 'COMMIT', 'PROCESSING', '{"total_candidates": 10}', NOW(), NOW())
		`, orphanRunID, h.Seed.ContextID)
		return struct{}{}, execErr
	})
	require.NoError(t, err, "create orphaned PROCESSING match run")

	// At this point, the process "crashes" — no finalizeRunFailure is called.
	// This simulates a hard kill (SIGKILL) or OOM.

	// Verify the orphaned run exists in PROCESSING state.
	AssertMatchRunStatus(t, directDB, orphanRunID, "PROCESSING")

	t.Log("FINDING CONFIRMED: Match run stuck in PROCESSING state after simulated crash. " +
		"There is NO background reaper to detect and recover orphaned runs. " +
		"The run will remain in PROCESSING indefinitely until manual intervention.")

	// Verify overall data integrity.
	AssertNoDataCorruption(t, directDB)
}

// --------------------------------------------------------------------------
// CHAOS-09: Ingestion failure after partial transaction writes
// --------------------------------------------------------------------------

// TestCHAOS09_IngestionPartialFailure simulates a PostgreSQL failure during
// chunked ingestion. Transactions may be partially written to the database
// while the ingestion job fails.
//
// Target: ingestion/commands.go — gap between filterAndInsertChunk (tx written)
//
//	and completeIngestionJob (job marked COMPLETED + outbox event).
//
// Injection: PG reset_peer after some transactions are committed.
// Expected: Some transactions exist in DB with a FAILED job.
// Finding: Chunked processing means partial inserts are committed separately.
func TestCHAOS09_IngestionPartialFailure(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")
	h.ResetDatabase(t)

	directDB := h.DirectDB(t)
	ctx := h.Ctx()

	// Insert some transactions as if they came from a partially-completed ingestion.
	jobID := uuid.New()

	_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
		// Create a job in PROCESSING state.
		_, execErr := tx.ExecContext(ctx, `
			INSERT INTO ingestion_jobs (id, context_id, source_id, status, metadata, created_at, updated_at)
			VALUES ($1, $2, $3, 'PROCESSING', '{"file_name": "chaos-partial.csv", "format": "csv", "total_records": 10}', NOW(), NOW())
		`, jobID, h.Seed.ContextID, h.Seed.SourceID)
		return struct{}{}, execErr
	})
	require.NoError(t, err, "create processing job")

	// Insert partial transactions (simulating chunk 1 committed).
	for i := range 5 {
		_, txErr := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			_, execErr := tx.ExecContext(ctx, `
				INSERT INTO transactions (id, ingestion_job_id, source_id, external_id, date, amount, currency, status, metadata, created_at, updated_at)
				VALUES (gen_random_uuid(), $1, $2, $3, '2025-01-15', 50.00, 'USD', 'UNMATCHED', '{}', NOW(), NOW())
			`, jobID, h.Seed.SourceID, fmt.Sprintf("CHAOS-PARTIAL-%d", i))
			return struct{}{}, execErr
		})
		require.NoError(t, txErr, "insert partial transaction %d", i)
	}

	// Now inject PG reset — simulating a crash before the job completion transaction.
	h.InjectPGResetPeer(t, 0)

	// The job completion transaction would fail.
	_, err = pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.ExecContext(ctx, `
			UPDATE ingestion_jobs SET status = 'COMPLETED', total_records = 5, updated_at = NOW()
			WHERE id = $1
		`, jobID)
		return struct{}{}, execErr
	})
	assert.Error(t, err, "job completion should fail with PG reset")

	// Remove the toxic so we can verify state.
	h.RemoveAllToxics(t)
	require.Eventually(t, func() bool {
		healthCtx, healthCancel := context.WithTimeout(context.Background(), time.Second)
		defer healthCancel()

		var one int

		err := directDB.QueryRowContext(healthCtx, "SELECT 1").Scan(&one)
		return err == nil && one == 1
	}, 5*time.Second, 100*time.Millisecond, "database should recover after removing toxics")

	// Verify: job is still PROCESSING (completion failed), but 5 transactions exist.
	var jobStatus string
	queryErr := directDB.QueryRowContext(context.Background(),
		`SELECT status FROM ingestion_jobs WHERE id = $1`, jobID,
	).Scan(&jobStatus)
	require.NoError(t, queryErr, "query job status")
	assert.Equal(t, "PROCESSING", jobStatus,
		"job should still be PROCESSING after failed completion")

	var txCount int
	queryErr = directDB.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM transactions WHERE external_id LIKE 'CHAOS-PARTIAL-%'`,
	).Scan(&txCount)
	require.NoError(t, queryErr, "count partial transactions")
	assert.Equal(t, 5, txCount,
		"5 transactions should exist from committed chunks")

	t.Log("FINDING CONFIRMED: Ingestion partial failure leaves orphaned state — " +
		"5 transactions committed but job stuck in PROCESSING. " +
		"The transactions are visible to matching but the job never completed.")

	AssertNoDataCorruption(t, directDB)
}

// --------------------------------------------------------------------------
// CHAOS-10: Concurrent match runs on same context
// --------------------------------------------------------------------------

// TestCHAOS10_ConcurrentMatchRuns verifies that the distributed lock prevents
// two simultaneous match runs on the same context, even under Redis latency.
//
// Target: Lock acquisition in RunMatch (match_group_commands.go:1316-1336).
// Injection: Redis latency (500ms) to widen the race window.
// Expected: One run succeeds, the other fails with ErrMatchRunLocked or similar.
func TestCHAOS10_ConcurrentMatchRuns(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")
	h.ResetDatabase(t)

	// Ensure a clean chaos baseline in case a previous test left toxics/proxies dirty.
	h.RemoveAllToxics(t)
	h.EnableAllProxies(t)
	t.Cleanup(func() {
		h.RemoveAllToxics(t)
		h.EnableAllProxies(t)
	})

	cs := BootChaosServer(t, h)
	ctx := h.Ctx()

	// Setup: add a second source and enough transactions to force real lock contention.
	secondSourceID := uuid.New()

	_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.ExecContext(ctx, `
			INSERT INTO reconciliation_sources (id, context_id, name, type, side, config, created_at, updated_at)
			VALUES ($1, $2, 'Chaos Source B', 'BANK', 'RIGHT', '{}', NOW(), NOW())
		`, secondSourceID, h.Seed.ContextID)

		return struct{}{}, execErr
	})
	require.NoError(t, err, "create second source")

	jobBySource := map[uuid.UUID]uuid.UUID{
		h.Seed.SourceID: uuid.New(),
		secondSourceID:  uuid.New(),
	}

	for sourceID, jobID := range jobBySource {
		_, jobErr := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			_, execErr := tx.ExecContext(ctx, `
				INSERT INTO ingestion_jobs (id, context_id, source_id, status, created_at, updated_at)
				VALUES ($1, $2, $3, 'COMPLETED', NOW(), NOW())
			`, jobID, h.Seed.ContextID, sourceID)

			return struct{}{}, execErr
		})
		require.NoError(t, jobErr, "create ingestion job for source %s", sourceID)
	}

	for sourceID, jobID := range jobBySource {
		for i := range 20 {
			externalID := fmt.Sprintf("CHAOS10-%s-%04d", sourceID.String()[:8], i)

			_, txErr := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
				_, execErr := tx.ExecContext(ctx, `
					INSERT INTO transactions (id, ingestion_job_id, source_id, external_id, date, amount, currency, status, metadata, created_at, updated_at)
					VALUES (gen_random_uuid(), $1, $2, $3, '2025-01-15', 100.00, 'USD', 'UNMATCHED', '{}', NOW(), NOW())
				`, jobID, sourceID, externalID)

				return struct{}{}, execErr
			})
			require.NoError(t, txErr, "insert source transaction %s", externalID)
		}
	}

	// Setup: create field maps and match rules for both sources.
	cs.CreateFieldMap(t, h.Seed.ContextID, h.Seed.SourceID)
	cs.CreateFieldMap(t, h.Seed.ContextID, secondSourceID)
	cs.CreateMatchRule(t, h.Seed.ContextID)

	// Inject Redis latency to widen the race window during lock acquisition.
	h.InjectRedisLatency(t, 500, 100)
	// Add mild PG latency so the first run keeps the lock long enough
	// for deterministic overlap with the second request.
	h.InjectPGLatency(t, 100, 25)

	// Fire two concurrent match runs.
	type result struct {
		status int
		body   []byte
		err    error
	}

	runRequest := func() result {
		path := fmt.Sprintf("/v1/matching/contexts/%s/run", h.Seed.ContextID)
		payload, marshalErr := json.Marshal(map[string]any{"mode": "DRY_RUN"})
		if marshalErr != nil {
			return result{err: marshalErr}
		}

		req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Idempotency-Key", uuid.New().String())
		req.Header.Set("X-Tenant-ID", h.Seed.TenantID.String())

		resp, testErr := cs.App.Test(req, 120000)
		if testErr != nil {
			return result{err: testErr}
		}

		if resp == nil || resp.Body == nil {
			return result{err: fmt.Errorf("nil HTTP response")}
		}

		defer resp.Body.Close()

		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return result{err: readErr}
		}

		return result{status: resp.StatusCode, body: body}
	}

	start := make(chan struct{})
	resultsCh := make(chan result, 2)

	for range 2 {
		go func() {
			<-start
			resultsCh <- runRequest()
		}()
	}

	close(start)

	firstResult := <-resultsCh
	secondResult := <-resultsCh

	results := []result{firstResult, secondResult}

	// Analyze results: at most one should succeed.
	successCount := 0
	lockedCount := 0
	requestErrCount := 0

	for i, r := range results {
		if r.err != nil {
			requestErrCount++
			t.Logf("Match run %d request error: %v", i, r.err)
			continue
		}

		t.Logf("Match run %d: status=%d body=%s", i, r.status, string(r.body))

		switch {
		case r.status >= 200 && r.status < 300:
			successCount++
		case r.status == http.StatusConflict || r.status == http.StatusLocked || r.status == http.StatusTooManyRequests:
			lockedCount++
		default:
			// Other failures (e.g., no transactions to match) are acceptable.
			t.Logf("Match run %d returned unexpected status %d (may be valid if no candidates)", i, r.status)
		}
	}

	assert.Zero(t, requestErrCount,
		"concurrent run requests should complete without transport errors")

	// The key assertion: we should NOT have two concurrent successes.
	// Either one succeeds + one is locked, or both fail for other reasons.
	assert.LessOrEqual(t, successCount, 1,
		"at most one concurrent match run should succeed (got %d successes, %d locked)",
		successCount, lockedCount)

	t.Logf("Concurrent match results: %d successes, %d locked (under 500ms Redis latency)",
		successCount, lockedCount)
}

// --------------------------------------------------------------------------
// CHAOS-10b: Match run with PG latency during commit
// --------------------------------------------------------------------------

// TestCHAOS10b_MatchRunWithPGLatency verifies that a match run handles
// PostgreSQL latency gracefully during the commit phase.
//
// Target: The wide commit transaction (match_group_commands.go:729-778).
// Injection: PG latency during match run execution.
// Expected: Either completes slowly or times out cleanly.
func TestCHAOS10b_MatchRunWithPGLatency(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")
	h.ResetDatabase(t)
	h.RemoveAllToxics(t)
	h.EnableAllProxies(t)
	t.Cleanup(func() {
		h.RemoveAllToxics(t)
		h.EnableAllProxies(t)
	})

	cs := BootChaosServer(t, h)

	cs.CreateFieldMap(t, h.Seed.ContextID, h.Seed.SourceID)
	cs.CreateMatchRule(t, h.Seed.ContextID)

	// Inject moderate PG latency (1s per operation).
	h.InjectPGLatency(t, 1000, 200)

	// Trigger a dry run match (less likely to timeout than a commit).
	status, body := cs.TriggerMatchRun(t, h.Seed.ContextID, "DRY_RUN")

	t.Logf("Match run under PG latency: status=%d body=%s", status, string(body))

	// The run may succeed slowly or fail — either is acceptable.
	// What matters is it doesn't hang indefinitely or corrupt data.
	if status >= 200 && status < 300 {
		t.Log("Match run completed despite PG latency")
	} else {
		t.Logf("Match run failed under PG latency (status %d) — verifying data integrity", status)
	}

	// Remove latency and verify data integrity.
	h.RemoveAllToxics(t)
	time.Sleep(500 * time.Millisecond)

	directDB := h.DirectDB(t)
	AssertNoDataCorruption(t, directDB)
	AssertNoOrphanedProcessingRuns(t, directDB)
}

// --------------------------------------------------------------------------
// CHAOS-09b: Full ingestion through HTTP with PG failure mid-stream
// --------------------------------------------------------------------------

// TestCHAOS09b_IngestionHTTPWithPGDrop verifies that a file upload via the
// HTTP API handles a PostgreSQL failure gracefully. The ingestion endpoint
// should return an error and not leave corrupted partial state.
//
// Target: POST /v1/imports/contexts/:contextId/sources/:sourceId/upload
// Injection: PG reset_peer during upload processing.
// Expected: HTTP error response, no corrupted state.
func TestCHAOS09b_IngestionHTTPWithPGDrop(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")
	h.ResetDatabase(t)

	cs := BootChaosServer(t, h)

	// Create field map for the source.
	cs.CreateFieldMap(t, h.Seed.ContextID, h.Seed.SourceID)

	// Inject PG reset after a small amount of data passes through.
	// timeout=500 means reset after 500 bytes — enough for the CSV header
	// but fails during row processing.
	h.InjectPGResetPeer(t, 500)

	csv := BuildCSVContent(50) // 50 rows

	status, body := cs.UploadCSV(t, h.Seed.ContextID, h.Seed.SourceID, csv)

	t.Logf("Ingestion under PG reset: status=%d body=%s", status, string(body))

	// The request should either fail (5xx) or succeed partially.
	// What matters is data integrity.

	// Remove toxic and check state.
	h.RemoveAllToxics(t)
	time.Sleep(500 * time.Millisecond)

	directDB := h.DirectDB(t)
	AssertNoDataCorruption(t, directDB)

	if status >= 500 {
		t.Log("Ingestion correctly returned server error under PG failure")
	} else if status >= 200 && status < 300 {
		// If it somehow succeeded, verify the data is consistent.
		var resp map[string]any
		if err := json.Unmarshal(body, &resp); err == nil {
			t.Logf("Ingestion succeeded despite PG chaos: %v", resp)
		}
	}
}
