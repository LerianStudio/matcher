//go:build chaos

package chaos

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// --------------------------------------------------------------------------
// CHAOS-01: PostgreSQL connection drop during transaction
// --------------------------------------------------------------------------

// TestCHAOS01_PGDropDuringTransaction verifies that an abrupt PostgreSQL
// connection reset mid-transaction causes a clean rollback with no partial writes.
//
// Target: pgcommon.WithTenantTx — the wrapper used by all repository write operations.
// Injection: reset_peer toxic on PostgreSQL proxy.
// Expected: Transaction rolls back, error is returned, no data committed.
func TestCHAOS01_PGDropDuringTransaction(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")
	h.ResetDatabase(t)

	directDB := h.DirectDB(t)

	// Verify we can write through the proxy under normal conditions.
	ctx := h.Ctx()
	_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.ExecContext(ctx, "SELECT 1")
		return struct{}{}, execErr
	})
	require.NoError(t, err, "baseline transaction should succeed")

	// Inject: reset all TCP connections through the PG proxy immediately.
	// timeout=0 means reset after 0 bytes (immediate).
	h.InjectPGResetPeer(t, 0)

	// Attempt a transaction through the poisoned proxy.
	_, err = pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.ExecContext(ctx, `
			INSERT INTO reconciliation_contexts (id, tenant_id, name, type, status, interval, created_at, updated_at)
			VALUES (gen_random_uuid(), $1, 'chaos-phantom', 'ONE_TO_ONE', 'ACTIVE', '0 0 * * *', NOW(), NOW())
		`, h.Seed.TenantID)
		return struct{}{}, execErr
	})

	// The transaction MUST fail — connection was reset.
	assert.Error(t, err, "transaction through poisoned proxy should fail")

	// Verify no phantom data was committed.
	var count int
	queryErr := directDB.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM reconciliation_contexts WHERE name = 'chaos-phantom'`,
	).Scan(&count)
	require.NoError(t, queryErr, "direct DB query should work")
	assert.Equal(t, 0, count,
		"no phantom data should be committed after connection reset (found %d rows)", count)

	// Verify data integrity after chaos.
	AssertNoDataCorruption(t, directDB)
}

// --------------------------------------------------------------------------
// CHAOS-02: PostgreSQL latency spike causing query timeout
// --------------------------------------------------------------------------

// TestCHAOS02_PGLatencySpike verifies that high PostgreSQL latency causes
// queries to timeout gracefully rather than hanging indefinitely.
//
// Target: Any database operation with a context deadline.
// Injection: 5-second latency toxic on PostgreSQL proxy.
// Expected: Context deadline exceeded error; no hung goroutines.
func TestCHAOS02_PGLatencySpike(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")
	h.ResetDatabase(t)

	// Inject 5 seconds of latency on every PostgreSQL response.
	h.InjectPGLatency(t, 5000, 0)

	// Set a tight deadline (2 seconds) — should timeout before PG responds.
	ctx, cancel := context.WithTimeout(h.Ctx(), 2*time.Second)
	defer cancel()

	start := time.Now()
	_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.ExecContext(ctx, "SELECT pg_sleep(0.1)")
		return struct{}{}, execErr
	})
	elapsed := time.Since(start)

	// Must fail with context deadline.
	assert.Error(t, err, "query should timeout under latency injection")
	// Should fail in ~2s (deadline), not ~5s+ (latency).
	assert.Less(t, elapsed, 4*time.Second,
		"timeout should respect context deadline (%v elapsed), not wait for full latency", elapsed)
}

// --------------------------------------------------------------------------
// CHAOS-03: PostgreSQL proxy disabled (complete outage)
// --------------------------------------------------------------------------

// TestCHAOS03_PGCompleteOutage verifies that disabling the PostgreSQL proxy
// causes all database operations to fail fast, and that recovery is seamless
// when the proxy is re-enabled.
//
// Target: All repository operations.
// Injection: Disable PG proxy entirely (connection refused).
// Expected: Operations fail fast, recovery works without restart.
func TestCHAOS03_PGCompleteOutage(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")
	h.ResetDatabase(t)

	ctx := h.Ctx()

	// Baseline: normal operation works.
	_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.ExecContext(ctx, "SELECT 1")
		return struct{}{}, execErr
	})
	require.NoError(t, err, "baseline should work")

	// Inject: disable PG proxy entirely.
	h.DisablePGProxy(t)

	// Operations should fail. Use a short timeout to avoid hanging.
	failCtx, failCancel := context.WithTimeout(ctx, 5*time.Second)
	defer failCancel()

	_, err = pgcommon.WithTenantTx(failCtx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.ExecContext(failCtx, "SELECT 1")
		return struct{}{}, execErr
	})
	assert.Error(t, err, "operations should fail when PG proxy is disabled")

	// Recovery: re-enable the proxy.
	h.EnablePGProxy(t)

	// Give the connection pool a moment to recover.
	time.Sleep(500 * time.Millisecond)

	// Operations should work again — Go's sql.DB pool handles reconnection.
	require.Eventually(t, func() bool {
		recoveryCtx, recoveryCancel := context.WithTimeout(ctx, 3*time.Second)
		defer recoveryCancel()

		_, txErr := pgcommon.WithTenantTx(recoveryCtx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			_, execErr := tx.ExecContext(recoveryCtx, "SELECT 1")
			return struct{}{}, execErr
		})

		return txErr == nil
	}, 15*time.Second, 500*time.Millisecond,
		"database operations should recover after proxy re-enable")
}

// --------------------------------------------------------------------------
// CHAOS-02b: PostgreSQL bandwidth throttle on large batch
// --------------------------------------------------------------------------

// TestCHAOS02b_PGBandwidthThrottle verifies that severely throttled bandwidth
// on PostgreSQL causes large operations to timeout without data corruption.
//
// Target: Batch INSERT operations (match group creation, transaction ingestion).
// Injection: 1 KB/s bandwidth limit on PostgreSQL proxy.
// Expected: Operation timeout or slow completion, no partial commits.
func TestCHAOS02b_PGBandwidthThrottle(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")
	h.ResetDatabase(t)

	directDB := h.DirectDB(t)

	// Inject: severe bandwidth limit (1 KB/s).
	h.InjectPGBandwidth(t, 1)

	ctx, cancel := context.WithTimeout(h.Ctx(), 5*time.Second)
	defer cancel()

	// Attempt a moderately large write that will exceed the bandwidth budget.
	_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
		for i := range 50 {
			_, execErr := tx.ExecContext(ctx, `
				INSERT INTO reconciliation_contexts (id, tenant_id, name, type, status, interval, created_at, updated_at)
				VALUES (gen_random_uuid(), $1, $2, 'ONE_TO_ONE', 'ACTIVE', '0 0 * * *', NOW(), NOW())
			`, h.Seed.TenantID, fmt.Sprintf("chaos-bandwidth-%02d", i))
			if execErr != nil {
				return struct{}{}, execErr
			}
		}

		return struct{}{}, nil
	})

	// Either times out or succeeds slowly — either way, data integrity must hold.
	if err != nil {
		t.Logf("bandwidth-throttled batch write failed as expected: %v", err)

		// Verify no partial data committed.
		var count int
		queryErr := directDB.QueryRowContext(context.Background(),
			`SELECT COUNT(*) FROM reconciliation_contexts WHERE name LIKE 'chaos-bandwidth-%'`,
		).Scan(&count)
		require.NoError(t, queryErr, "direct query should work")
		assert.Equal(t, 0, count,
			"no partial data should be committed after throttled timeout (found %d rows)", count)
	} else {
		t.Log("bandwidth-throttled batch write succeeded (within deadline)")
	}

	AssertNoDataCorruption(t, directDB)
}
