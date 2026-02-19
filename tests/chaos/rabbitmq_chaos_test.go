//go:build chaos

package chaos

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// --------------------------------------------------------------------------
// CHAOS-06: RabbitMQ connection drop during outbox dispatch
// --------------------------------------------------------------------------

// TestCHAOS06_RabbitDropDuringOutboxWrite verifies that when RabbitMQ is
// unreachable, outbox events are still safely persisted in the database
// as part of the business transaction.
//
// Target: Outbox pattern — events created in the same DB transaction as business data.
// Injection: Disable RabbitMQ proxy entirely.
// Expected: Business transaction succeeds, outbox events accumulate in DB,
//
//	dispatcher cannot publish but events are safe.
//
// Key insight: The outbox pattern's entire purpose is to survive this scenario.
func TestCHAOS06_RabbitDropDuringOutboxWrite(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")
	h.ResetDatabase(t)

	directDB := h.DirectDB(t)
	ctx := h.Ctx()

	// Inject: Kill RabbitMQ connection.
	h.DisableRabbitProxy(t)

	// Business operations that create outbox events should still succeed
	// because outbox events are written to the same PostgreSQL transaction —
	// they don't depend on RabbitMQ being available at write time.
	//
	// We verify this by writing directly to the outbox table through the proxy.
	_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
		// Simulate what the matching service does: write business data + outbox event
		// in the same transaction.
		_, execErr := tx.ExecContext(ctx, `
			INSERT INTO outbox_events (id, event_type, aggregate_id, payload, status, attempts, created_at, updated_at)
			VALUES (gen_random_uuid(), 'test.chaos_event', gen_random_uuid(), '{"test": true}', 'PENDING', 0, NOW(), NOW())
		`)
		return struct{}{}, execErr
	})
	require.NoError(t, err,
		"business transaction with outbox event should succeed even with RabbitMQ down "+
			"(outbox events go to PostgreSQL, not RabbitMQ)")

	// Verify the outbox event is safely persisted.
	stats := GetOutboxStats(t, directDB)
	assert.GreaterOrEqual(t, stats.Pending+stats.Failed, 1,
		"at least one outbox event should be pending/failed in DB: %s", stats)

	t.Logf("VERIFIED: Outbox event safely persisted with RabbitMQ down: %s", stats)
}

// --------------------------------------------------------------------------
// CHAOS-07: RabbitMQ unavailable for extended period (outbox backlog)
// --------------------------------------------------------------------------

// TestCHAOS07_RabbitExtendedOutage_OutboxAccumulates verifies that during an
// extended RabbitMQ outage, outbox events accumulate safely in PostgreSQL
// and the database doesn't degrade.
//
// Target: Outbox table growth, PostgreSQL performance under outbox backlog.
// Injection: Disable RabbitMQ proxy for duration of event generation.
// Expected: Events accumulate, DB stays responsive, events are recoverable.
func TestCHAOS07_RabbitExtendedOutage_OutboxAccumulates(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")
	h.ResetDatabase(t)

	directDB := h.DirectDB(t)
	ctx := h.Ctx()

	// Inject: Kill RabbitMQ.
	h.DisableRabbitProxy(t)

	// Generate multiple outbox events while RabbitMQ is down.
	eventCount := 25

	for i := range eventCount {
		_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			payload := fmt.Sprintf(`{"event_number": %d}`, i)

			_, execErr := tx.ExecContext(ctx, `
				INSERT INTO outbox_events (id, event_type, aggregate_id, payload, status, attempts, created_at, updated_at)
				VALUES (gen_random_uuid(), 'test.chaos_backlog', gen_random_uuid(), $1, 'PENDING', 0, NOW(), NOW())
			`, payload)
			return struct{}{}, execErr
		})
		require.NoError(t, err, "outbox write %d should succeed", i)
	}

	// Verify all events are in the database.
	stats := GetOutboxStats(t, directDB)
	assert.GreaterOrEqual(t, stats.Total, eventCount,
		"expected at least %d outbox events, got %d: %s", eventCount, stats.Total, stats)

	// Verify database is still responsive under the outbox backlog.
	start := time.Now()

	_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.ExecContext(ctx, "SELECT 1")
		return struct{}{}, execErr
	})

	queryTime := time.Since(start)
	require.NoError(t, err, "DB should stay responsive with outbox backlog")
	assert.Less(t, queryTime, 2*time.Second,
		"DB query should be fast even with %d pending outbox events (%v)", eventCount, queryTime)

	t.Logf("VERIFIED: %d outbox events accumulated safely with RabbitMQ down. "+
		"DB query time: %v. Stats: %s", eventCount, queryTime, stats)

	// Recovery: Re-enable RabbitMQ.
	h.EnableRabbitProxy(t)

	// Note: Actual event delivery would require running the outbox dispatcher,
	// which is tested at the full-service level. Here we verify the data layer.
}

// --------------------------------------------------------------------------
// CHAOS-06b: RabbitMQ reset during active connection
// --------------------------------------------------------------------------

// TestCHAOS06b_RabbitResetPeer verifies that a TCP reset on the RabbitMQ
// connection is detected (or at least doesn't corrupt the outbox state).
//
// Target: AMQP channel staleness detection.
// Injection: reset_peer toxic on RabbitMQ proxy.
// Expected: Connection failure is detected; outbox events stay safe in DB.
func TestCHAOS06b_RabbitResetPeer(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")
	h.ResetDatabase(t)

	directDB := h.DirectDB(t)
	ctx := h.Ctx()

	// First, create some outbox events while RabbitMQ is healthy.
	for range 5 {
		_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			_, execErr := tx.ExecContext(ctx, `
				INSERT INTO outbox_events (id, event_type, aggregate_id, payload, status, attempts, created_at, updated_at)
				VALUES (gen_random_uuid(), 'test.pre_reset', gen_random_uuid(), '{"pre_reset": true}', 'PENDING', 0, NOW(), NOW())
			`)
			return struct{}{}, execErr
		})
		require.NoError(t, err, "pre-reset outbox write should succeed")
	}

	// Inject: Reset TCP connections to RabbitMQ.
	h.InjectRabbitResetPeer(t, 0)

	// Write more outbox events — these should still succeed (they go to PG, not RabbitMQ).
	for range 5 {
		_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			_, execErr := tx.ExecContext(ctx, `
				INSERT INTO outbox_events (id, event_type, aggregate_id, payload, status, attempts, created_at, updated_at)
				VALUES (gen_random_uuid(), 'test.post_reset', gen_random_uuid(), '{"post_reset": true}', 'PENDING', 0, NOW(), NOW())
			`)
			return struct{}{}, execErr
		})
		require.NoError(t, err,
			"post-reset outbox write should succeed (outbox is in PostgreSQL)")
	}

	// All 10 events should be safely persisted.
	stats := GetOutboxStats(t, directDB)
	assert.GreaterOrEqual(t, stats.Total, 10,
		"all outbox events should be persisted regardless of RabbitMQ state: %s", stats)

	AssertNoDataCorruption(t, directDB)
}
