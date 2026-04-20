//go:build chaos

package chaos

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// --------------------------------------------------------------------------
// CHAOS-15: Sync export under pool exhaustion
// --------------------------------------------------------------------------

// TestCHAOS15_SyncExportPoolExhaustion verifies that concurrent synchronous
// export requests don't exhaust the database connection pool and leave the
// system unable to serve other requests.
//
// Target: GET /v1/reports/contexts/:contextId/matched/export
//
//	(loads entire dataset into memory via ListMatchedForExport).
//
// Setup: POSTGRES_MAX_OPEN_CONNS=5 (set by ChaosHarness env).
// Injection: Fire N concurrent export requests where N > MaxOpenConns.
// Expected: Either requests queue and eventually complete, or some fail
//
//	with connection timeout errors. The key assertion is that
//	the system RECOVERS — subsequent simple queries still work.
//
// Finding: No circuit breaker on sync exports; the rate limiter only caps
//
//	request frequency, not concurrent connections held.
func TestCHAOS15_SyncExportPoolExhaustion(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")
	h.ResetDatabase(t)

	cs := BootChaosServer(t, h)
	ctx := h.Ctx()

	// Insert seed data: some matched transactions so the export has work to do.
	seedSecondSourceWithTransactions(t, h, 20)

	// Inject PG latency to make each export hold a connection longer.
	h.InjectPGLatency(t, 500, 100)

	// Fire 10 concurrent export requests with MaxOpenConns=5.
	// This should saturate the pool.
	const concurrency = 10

	var (
		wg          sync.WaitGroup
		successes   atomic.Int32
		failures    atomic.Int32
		statusCodes [concurrency]int
	)

	for i := range concurrency {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			path := fmt.Sprintf("/v1/reports/contexts/%s/matched/export?date_from=2024-01-01&date_to=2025-12-31&format=csv",
				h.Seed.ContextID)
			req := httptest.NewRequest(http.MethodGet, path, nil)
			req.Header.Set("X-Tenant-ID", h.Seed.TenantID.String())

			resp, err := cs.App.Test(req, 30000) // 30s timeout
			if err != nil {
				failures.Add(1)
				statusCodes[idx] = -1

				return
			}

			if resp != nil && resp.Body != nil {
				defer resp.Body.Close()
			}

			statusCodes[idx] = resp.StatusCode

			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				successes.Add(1)
			} else {
				failures.Add(1)
			}
		}(i)
	}

	wg.Wait()

	h.RemoveAllToxics(t)

	t.Logf("Concurrent exports (%d): %d successes, %d failures (pool size=5)",
		concurrency, successes.Load(), failures.Load())

	for i, code := range statusCodes {
		t.Logf("  export[%d] status=%d", i, code)
	}

	// The CRITICAL assertion: the system RECOVERS after the storm.
	// Wait a moment for connections to return to the pool.
	time.Sleep(1 * time.Second)

	_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.ExecContext(ctx, "SELECT 1")
		return struct{}{}, execErr
	})
	assert.NoError(t, err,
		"database should be responsive after concurrent export storm subsides")
}

// --------------------------------------------------------------------------
// CHAOS-16: Auto-match goroutine bomb under bulk ingestion
// --------------------------------------------------------------------------

// TestCHAOS16_BulkIngestionGoroutineBomb verifies the system's behavior when
// many ingestion completions fire simultaneously, each spawning a goroutine
// to trigger auto-match.
//
// Target: auto_match_adapters.go:98 — SafeGoWithContextAndComponent per completion.
// Setup: Upload multiple CSV files rapidly to the same context.
// Expected: Goroutines should be bounded (or at least not crash).
//
// Finding: No semaphore/backpressure on auto-match goroutines.
//
//nolint:tparallel // goroutine bomb test must run serially
func TestCHAOS16_BulkIngestionGoroutineBomb(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")
	h.ResetDatabase(t)

	cs := BootChaosServer(t, h)

	// Setup: create field map and match rule for the context.
	cs.CreateFieldMap(t, h.Seed.ContextID, h.Seed.SourceID)
	cs.CreateMatchRule(t, h.Seed.ContextID)

	// Record baseline goroutine count.
	baselineGoroutines := runtime.NumGoroutine()
	t.Logf("Baseline goroutine count: %d", baselineGoroutines)

	// Fire 20 concurrent CSV uploads to the same source.
	// Each upload completion may trigger an auto-match goroutine.
	const uploadCount = 20

	var (
		wg        sync.WaitGroup
		successes atomic.Int32
		failures  atomic.Int32
	)

	for i := range uploadCount {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			csv := fmt.Sprintf("external_id,date,amount,currency\nBOMB-%05d,2025-01-15,100.00,USD\n", idx)
			status, _ := cs.UploadCSV(t, h.Seed.ContextID, h.Seed.SourceID, csv)

			if status >= 200 && status < 300 {
				successes.Add(1)
			} else {
				failures.Add(1)
			}
		}(i)
	}

	wg.Wait()

	// Measure goroutine count after the storm.
	peakGoroutines := runtime.NumGoroutine()
	goroutineGrowth := peakGoroutines - baselineGoroutines

	t.Logf("After %d concurrent uploads: %d successes, %d failures",
		uploadCount, successes.Load(), failures.Load())
	t.Logf("Goroutine count: baseline=%d peak=%d growth=%d",
		baselineGoroutines, peakGoroutines, goroutineGrowth)

	// Allow goroutines time to settle (match runs complete or fail).
	time.Sleep(5 * time.Second)

	settledGoroutines := runtime.NumGoroutine()
	t.Logf("Goroutine count after 5s settle: %d (leaked: %d)",
		settledGoroutines, settledGoroutines-baselineGoroutines)

	// The goroutine count should eventually return to near baseline.
	// A growth of >50 goroutines after settling is a leak indicator.
	leaked := settledGoroutines - baselineGoroutines
	assert.Less(t, leaked, 50,
		"goroutine leak detected: %d goroutines above baseline after settling "+
			"(baseline=%d, settled=%d). This suggests auto-match goroutines are "+
			"not being cleaned up.", leaked, baselineGoroutines, settledGoroutines)

	// System should still be responsive.
	resp, _ := cs.DoGet(t, "/health")
	assert.Equal(t, http.StatusOK, resp.StatusCode, "system should be responsive after goroutine storm")

	// Log finding about backpressure.
	if goroutineGrowth > uploadCount {
		t.Logf("FINDING: Goroutine growth (%d) exceeds upload count (%d). "+
			"Each ingestion completion spawns unbounded goroutines "+
			"(auto_match_adapters.go:98). No semaphore or queue.", goroutineGrowth, uploadCount)
	}
}

// --------------------------------------------------------------------------
// CHAOS-17: Connection pool deadlock under concurrent operations
// --------------------------------------------------------------------------

// TestCHAOS17_PoolDeadlockUnderConcurrency verifies that with a very small
// connection pool, concurrent operations don't deadlock. Each operation type
// (ingestion, matching, outbox dispatch, health check) competes for the
// same limited pool.
//
// Target: The shared PG connection pool with MaxOpenConns=5.
// Setup: POSTGRES_MAX_OPEN_CONNS=5 (set by ChaosHarness).
// Injection: Concurrent operations from multiple subsystems simultaneously.
// Expected: Operations either succeed or timeout, but NEVER deadlock.
//
//nolint:tparallel // pool contention test must run serially
func TestCHAOS17_PoolDeadlockUnderConcurrency(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")
	h.ResetDatabase(t)

	cs := BootChaosServer(t, h)
	ctx := h.Ctx()

	// Inject moderate PG latency to increase connection hold time.
	h.InjectPGLatency(t, 200, 50)

	// Launch concurrent operations of different types.
	type opResult struct {
		name    string
		err     error
		elapsed time.Duration
	}

	results := make(chan opResult, 20)

	var wg sync.WaitGroup

	// 1. Database transactions (5 concurrent)
	for i := range 5 {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			start := time.Now()

			opCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			_, err := pgcommon.WithTenantTx(opCtx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
				_, execErr := tx.ExecContext(opCtx, "SELECT pg_sleep(0.1)")
				return struct{}{}, execErr
			})

			results <- opResult{
				name:    fmt.Sprintf("tx-%d", idx),
				err:     err,
				elapsed: time.Since(start),
			}
		}(i)
	}

	// 2. Health check probes (3 concurrent)
	for i := range 3 {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			start := time.Now()

			req := httptest.NewRequest(http.MethodGet, "/readyz", nil)

			resp, err := cs.App.Test(req, 10000)
			if resp != nil && resp.Body != nil {
				defer resp.Body.Close()
			}

			results <- opResult{
				name:    fmt.Sprintf("ready-%d", idx),
				err:     err,
				elapsed: time.Since(start),
			}
		}(i)
	}

	// 3. Outbox dispatch (2 concurrent)
	for i := range 2 {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			start := time.Now()

			cs.DispatchOutbox(t)

			results <- opResult{
				name:    fmt.Sprintf("outbox-%d", idx),
				err:     nil,
				elapsed: time.Since(start),
			}
		}(i)
	}

	// Wait for all operations (with a hard deadline).
	done := make(chan struct{})

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All operations completed.
	case <-time.After(30 * time.Second):
		t.Fatal("DEADLOCK DETECTED: concurrent operations did not complete within 30s. " +
			"This indicates a connection pool deadlock where all connections are held " +
			"by operations waiting for connections.")
	}

	close(results)

	// Analyze results.
	var (
		completed int
		errored   int
		maxTime   time.Duration
	)

	for r := range results {
		completed++

		if r.err != nil {
			errored++
			t.Logf("  %s: FAILED in %v — %v", r.name, r.elapsed, r.err)
		} else {
			t.Logf("  %s: OK in %v", r.name, r.elapsed)
		}

		if r.elapsed > maxTime {
			maxTime = r.elapsed
		}
	}

	t.Logf("Pool contention results: %d completed, %d errors, max latency %v",
		completed, errored, maxTime)

	// No operation should take more than 15s (the per-check timeout + pool wait).
	assert.Less(t, maxTime, 20*time.Second,
		"max operation time should be bounded (got %v). Pool contention may be too severe.", maxTime)

	// System must recover.
	h.RemoveAllToxics(t)
	time.Sleep(500 * time.Millisecond)

	_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.ExecContext(ctx, "SELECT 1")
		return struct{}{}, execErr
	})
	assert.NoError(t, err, "database should recover after pool contention subsides")
}

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

// seedSecondSourceWithTransactions creates a second source and inserts
// transactions into both sources for match testing.
func seedSecondSourceWithTransactions(t *testing.T, h *ChaosHarness, count int) string {
	t.Helper()

	ctx := h.Ctx()
	secondSourceID := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

	_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.ExecContext(ctx, `
			INSERT INTO reconciliation_sources (id, context_id, name, type, side, config, created_at, updated_at)
			VALUES ($1, $2, 'Chaos Source B', 'BANK', 'RIGHT', '{}', NOW(), NOW())
			ON CONFLICT (id) DO NOTHING
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

	for i, sourceID := range []uuid.UUID{h.Seed.SourceID, secondSourceID} {
		for j := range count {
			_, txErr := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
				_, execErr := tx.ExecContext(ctx, `
					INSERT INTO transactions (id, ingestion_job_id, source_id, external_id, date, amount, currency, status, metadata, created_at, updated_at)
					VALUES (gen_random_uuid(), $1, $2, $3, '2025-01-15', 100.00, 'USD', 'UNMATCHED', '{}', NOW(), NOW())
				`, jobBySource[sourceID], sourceID, fmt.Sprintf("EXHAUST-SRC%d-TX%d", i, j))
				return struct{}{}, execErr
			})
			require.NoError(t, txErr, "insert transaction")
		}
	}

	return secondSourceID.String()
}
