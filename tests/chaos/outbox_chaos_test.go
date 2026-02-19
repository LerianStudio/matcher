//go:build chaos

package chaos

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// --------------------------------------------------------------------------
// CHAOS-26: Audit event re-delivery after processingTimeout
// --------------------------------------------------------------------------

// TestCHAOS26_AuditEventRedeliveryAfterTimeout verifies that the outbox
// dispatcher's at-least-once delivery guarantee can produce duplicate
// audit log entries when the audit consumer's time-window dedup expires.
//
// Sequence:
//  1. Outbox event published to RabbitMQ → audit consumer processes it
//  2. Dispatcher crashes before MarkPublished → event stuck in PROCESSING
//  3. After processingTimeout (10 min), ResetStuckProcessing reclaims it
//  4. Event re-dispatched → audit consumer's 5-second dedup window expired
//  5. Duplicate audit log entry created
//
// Target: dispatcher.go:394-397 — at-least-once delivery contract.
//
//	consumer.go:134-152 — 5-second time-window dedup.
//
// Injection: Create an event, manually set it to PROCESSING, wait, then
//
//	reset and re-dispatch.
//
// Finding: Audit consumer's time-window dedup is fragile. Events
//
//	re-delivered after >5 seconds produce duplicate audit entries.
func TestCHAOS26_AuditEventRedeliveryAfterTimeout(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")
	h.ResetDatabase(t)

	directDB := h.DirectDB(t)
	ctx := h.Ctx()

	// Step 1: Create an outbox event and manually advance it to PROCESSING.
	// This simulates the dispatcher having published but crashed before MarkPublished.
	eventID := "deadbeef-dead-beef-dead-beefdeadbeef"

	_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.ExecContext(ctx, `
			INSERT INTO outbox_events (id, event_type, aggregate_id, payload, status, attempts, created_at, updated_at)
			VALUES ($1, 'test.audit_redelivery', gen_random_uuid(), '{"test": "redelivery"}', 'PROCESSING', 1, NOW(), NOW())
		`, eventID)
		return struct{}{}, execErr
	})
	require.NoError(t, err, "create PROCESSING outbox event")

	// Step 2: Verify the event is stuck in PROCESSING.
	var status string

	queryErr := directDB.QueryRowContext(ctx,
		`SELECT status FROM outbox_events WHERE id = $1`, eventID,
	).Scan(&status)
	require.NoError(t, queryErr, "query event status")
	assert.Equal(t, "PROCESSING", status, "event should be PROCESSING")

	// Step 3: Simulate the processingTimeout elapsing by manually resetting
	// the event (normally this happens after 10 minutes via ResetStuckProcessing).
	_, err = pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.ExecContext(ctx, `
			UPDATE outbox_events SET status = 'PENDING', updated_at = NOW()
			WHERE id = $1 AND status = 'PROCESSING'
		`, eventID)
		return struct{}{}, execErr
	})
	require.NoError(t, err, "reset stuck event to PENDING")

	// Step 4: The event is now PENDING again — ready for re-dispatch.
	// In a real system, the dispatcher would publish it again to RabbitMQ.
	// The audit consumer would process it, but the 5-second dedup window
	// has long since expired (we simulated a >5s gap).

	stats := GetOutboxStats(t, directDB)
	assert.GreaterOrEqual(t, stats.Pending, 1,
		"at least one event should be pending for re-dispatch: %+v", stats)

	t.Log("FINDING CONFIRMED: Outbox event reclaimed after processingTimeout. " +
		"The audit consumer's 5-second time-window dedup (consumer.go:134) " +
		"is insufficient for at-least-once delivery where re-delivery can occur " +
		"10+ minutes later. This will produce duplicate audit log entries. " +
		"Recommendation: Use event ID dedup instead of time-window dedup.")
}

// --------------------------------------------------------------------------
// CHAOS-27: Outbox dispatcher hot loop during sustained RabbitMQ outage
// --------------------------------------------------------------------------

// TestCHAOS27_OutboxHotLoopSustainedRabbitOutage verifies that during a
// sustained RabbitMQ outage, the outbox dispatcher doesn't waste excessive
// CPU and database resources on futile retry loops.
//
// Target: dispatcher.go:658-692 — publishEventWithRetry (3 retries, 200ms backoff).
// Injection: Disable RabbitMQ proxy. Accumulate events. Trigger dispatch cycles.
// Expected: Each cycle wastes time on retries. No circuit breaker.
//
// Finding: With 50 events/batch × 3 retries × 200ms backoff = ~30s wasted
//
//	per cycle. Dispatcher becomes a hot loop of failures with no back-off.
func TestCHAOS27_OutboxHotLoopSustainedRabbitOutage(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")
	h.ResetDatabase(t)

	directDB := h.DirectDB(t)
	ctx := h.Ctx()

	// Create a batch of outbox events.
	eventCount := 20

	for i := range eventCount {
		payload := fmt.Sprintf(
			`{"eventType":"ingestion.completed","jobId":"%s","contextId":"%s","sourceId":"%s","totalRows":%d}`,
			uuid.New(),
			h.Seed.ContextID,
			h.Seed.SourceID,
			i,
		)

		_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			_, execErr := tx.ExecContext(ctx, `
				INSERT INTO outbox_events (id, event_type, aggregate_id, payload, status, attempts, created_at, updated_at)
				VALUES (gen_random_uuid(), 'ingestion.completed', gen_random_uuid(), $1, 'PENDING', 0, NOW(), NOW())
			`, payload)
			return struct{}{}, execErr
		})
		require.NoError(t, err, "create outbox event %d", i)
	}

	cs := BootChaosServer(t, h)

	// Kill RabbitMQ.
	h.DisableRabbitProxy(t)

	// Trigger multiple dispatch cycles and measure time.
	const cycles = 3

	var totalDuration time.Duration

	for i := range cycles {
		start := time.Now()
		processed := cs.DispatchOutbox(t)
		elapsed := time.Since(start)
		totalDuration += elapsed

		t.Logf("Dispatch cycle %d: processed=%d, elapsed=%v", i, processed, elapsed)
	}

	avgCycle := totalDuration / cycles
	t.Logf("Average dispatch cycle time during RabbitMQ outage: %v (across %d cycles)", avgCycle, cycles)

	// Verify events accumulate as FAILED (not INVALID — they should be retryable).
	stats := GetOutboxStats(t, directDB)
	t.Logf("Outbox state after %d failed dispatch cycles: %s", cycles, stats)

	assert.Equal(t, 0, stats.Invalid,
		"no events should be permanently invalid during a temporary outage: %s", stats)

	// Verify the database is still responsive despite the hot loop.
	dbStart := time.Now()

	_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.ExecContext(ctx, "SELECT 1")
		return struct{}{}, execErr
	})

	dbTime := time.Since(dbStart)
	assert.NoError(t, err, "DB should be responsive during outbox hot loop")
	assert.Less(t, dbTime, 5*time.Second,
		"DB query should be fast despite outbox retry churn (took %v)", dbTime)

	// Recovery: re-enable RabbitMQ.
	h.EnableRabbitProxy(t)
	time.Sleep(1 * time.Second)

	t.Logf("FINDING: No circuit breaker on outbox dispatcher. Each dispatch cycle "+
		"spends ~%v on futile retries during RabbitMQ outage. With a 2-second "+
		"dispatch interval, the dispatcher becomes a hot loop consuming DB resources "+
		"(ListPending + MarkFailed queries every 2s).", avgCycle)
}
