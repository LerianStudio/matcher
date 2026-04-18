//go:build chaos

package chaos

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/bootstrap"
	outboxServices "github.com/LerianStudio/lib-commons/v5/commons/outbox"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// --------------------------------------------------------------------------
// CHAOS-11: Graceful shutdown during active operations
// --------------------------------------------------------------------------

// TestCHAOS11_ShutdownDuringActiveOperations verifies that calling Shutdown
// while database operations are in-flight completes or cancels them cleanly,
// with no data corruption.
//
// Target: service.go:157-192 — shutdown sequence.
// Injection: Context cancellation simulating SIGTERM.
// Expected: In-flight operations cancel via context; data remains consistent.
func TestCHAOS11_ShutdownDuringActiveOperations(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")
	h.ResetDatabase(t)

	h.SetEnvForBootstrap(t)

	svc, err := bootstrap.InitServersWithOptions(&bootstrap.Options{
		Logger: &libLog.NopLogger{},
	})
	require.NoError(t, err, "bootstrap service for shutdown test")

	app := svc.GetApp()

	// Start a long-running operation in the background.
	operationStarted := make(chan struct{})
	operationDone := make(chan error, 1)

	go func() {
		close(operationStarted)

		ctx := h.Ctx()

		// Simulate a long-running transaction.
		_, txErr := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			// Insert some data, then sleep to simulate work.
			_, execErr := tx.ExecContext(ctx, `
				INSERT INTO reconciliation_contexts (id, tenant_id, name, type, status, interval, created_at, updated_at)
				VALUES (gen_random_uuid(), $1, 'shutdown-test', 'ONE_TO_ONE', 'ACTIVE', '0 0 * * *', NOW(), NOW())
			`, h.Seed.TenantID)
			if execErr != nil {
				return struct{}{}, execErr
			}

			// Simulate work in progress.
			time.Sleep(2 * time.Second)

			return struct{}{}, nil
		})

		operationDone <- txErr
	}()

	<-operationStarted
	// Give the transaction time to start.
	time.Sleep(200 * time.Millisecond)

	// Trigger shutdown while the operation is in-flight.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	shutdownStart := time.Now()
	shutdownErr := app.ShutdownWithContext(shutdownCtx)
	shutdownDuration := time.Since(shutdownStart)

	t.Logf("Shutdown completed in %v (error: %v)", shutdownDuration, shutdownErr)

	// Wait for the operation to complete (it may have been cancelled by shutdown).
	select {
	case opErr := <-operationDone:
		if opErr != nil {
			t.Logf("In-flight operation result after shutdown: %v (expected — context was cancelled)", opErr)
		} else {
			t.Log("In-flight operation completed before shutdown took effect")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("In-flight operation hung after shutdown (15s timeout)")
	}

	// Verify data integrity via direct connection.
	directDB := h.DirectDB(t)
	AssertNoDataCorruption(t, directDB)
}

// --------------------------------------------------------------------------
// CHAOS-12: Outbox dispatch during shutdown
// --------------------------------------------------------------------------

// TestCHAOS12_OutboxDuringShutdown verifies that outbox events being
// dispatched during shutdown are handled cleanly — either completing
// the current batch or leaving events in a recoverable state.
//
// Target: dispatcher.go:346-366 — Shutdown with WaitGroup.
// Injection: Create pending events, trigger dispatch, then shutdown.
// Expected: Events either complete dispatch or remain in PENDING/PROCESSING.
func TestCHAOS12_OutboxDuringShutdown(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")
	h.ResetDatabase(t)

	directDB := h.DirectDB(t)
	ctx := h.Ctx()

	// Inject RabbitMQ latency to slow down event publishing.
	h.InjectRabbitLatency(t, 2000, 500)

	// Create some outbox events.
	for i := range 10 {
		payload := fmt.Sprintf(
			`{"eventType":"ingestion.completed","jobId":"%s","contextId":"%s","sourceId":"%s"}`,
			uuid.New(),
			h.Seed.ContextID,
			h.Seed.SourceID,
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

	// Boot service (with slow RabbitMQ).
	h.SetEnvForBootstrap(t)

	svc, err := bootstrap.InitServersWithOptions(&bootstrap.Options{
		Logger: &libLog.NopLogger{},
	})
	require.NoError(t, err, "bootstrap service for outbox shutdown test")

	// Trigger one dispatch cycle (may be slow due to RabbitMQ latency).
	cs := &ChaosServer{Service: svc, App: svc.GetApp(), Harness: h}

	if runner := svc.GetOutboxRunner(); runner != nil {
		if d, ok := runner.(interface{ DispatchOnce(context.Context) int }); ok {
			if dispatcher, ok := d.(*outboxServices.Dispatcher); ok {
				cs.Dispatcher = dispatcher
			}
		}
	}

	go func() { cs.DispatchOutbox(t) }() // fire and forget for shutdown race

	// Give dispatch a moment to start.
	time.Sleep(200 * time.Millisecond)

	// Shutdown while dispatch is (possibly) in flight.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	_ = svc.GetApp().ShutdownWithContext(shutdownCtx)

	// Wait for things to settle.
	time.Sleep(1 * time.Second)

	// Verify outbox state — events should be in a recoverable state.
	stats := GetOutboxStats(t, directDB)

	t.Logf("Outbox state after shutdown-during-dispatch: %s", stats)

	// No events should be permanently lost.
	assert.Equal(t, 0, stats.Invalid,
		"no events should be permanently invalid after shutdown: %s", stats)

	// All events should be in a recoverable state
	// (PENDING, PROCESSING that will be reset by processingTimeout, or PUBLISHED).
	recoverableOrComplete := stats.Pending + stats.Processing + stats.Published + stats.Failed
	assert.Equal(t, stats.Total, recoverableOrComplete,
		"all events should be in recoverable or completed state: %s", stats)
}

// --------------------------------------------------------------------------
// CHAOS-13: Slow PostgreSQL during startup
// --------------------------------------------------------------------------

// TestCHAOS13_SlowPGDuringStartup verifies the shared infrastructure timeout
// budget behavior. A slow PostgreSQL connection consumes time from the shared
// 30-second budget, potentially starving Redis and RabbitMQ connections.
//
// Target: init.go:300-301 — single 30s context.WithTimeout for ALL infra.
// Injection: PG latency set before bootstrap.
// Expected: If PG takes too long, startup fails entirely.
// Finding: There is no per-service timeout budget.
func TestCHAOS13_SlowPGDuringStartup(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")
	h.ResetDatabase(t)

	// Inject moderate PG latency (3s) — shouldn't kill startup but will slow it.
	h.InjectPGLatency(t, 3000, 500)

	h.SetEnvForBootstrap(t)
	// Set a short infra timeout to make the test faster.
	t.Setenv("INFRA_CONNECT_TIMEOUT_SEC", "15")

	start := time.Now()

	svc, err := bootstrap.InitServersWithOptions(&bootstrap.Options{
		Logger: &libLog.NopLogger{},
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Logf("FINDING: Startup failed after %v with PG latency: %v", elapsed, err)
		t.Log("This confirms the shared timeout budget issue — " +
			"PG latency consumed the entire infrastructure connect budget.")

		// This is an expected finding, not a test failure.
		// The infrastructure connect timeout is shared across all services.
		return
	}

	defer func() { _ = svc.GetApp().Shutdown() }()

	t.Logf("Startup succeeded in %v despite 3s PG latency", elapsed)

	// Verify the service is actually functional.
	req := httptest.NewRequest(http.MethodGet, "/health", nil)

	resp, testErr := svc.GetApp().Test(req, 10000)
	require.NoError(t, testErr, "health check after slow startup")

	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}

	assert.Equal(t, http.StatusOK, resp.StatusCode, "service should be healthy after slow startup")
}

// --------------------------------------------------------------------------
// CHAOS-14: Tenant schema discovery failure in outbox dispatcher
// --------------------------------------------------------------------------

// TestCHAOS14_TenantDiscoveryFailure verifies that when PostgreSQL is
// temporarily unavailable during the outbox dispatcher's tenant discovery
// phase, the dispatcher gracefully skips the cycle and recovers.
//
// Target: outbox.postgresql.go:224-283 — ListTenants queries pg_namespace.
// Injection: Brief PG outage during dispatch.
// Expected: Dispatch cycle fails gracefully, next cycle succeeds.
func TestCHAOS14_TenantDiscoveryFailure(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")
	h.ResetDatabase(t)

	directDB := h.DirectDB(t)
	ctx := h.Ctx()

	// Create outbox events that need dispatching.
	for i := range 5 {
		payload := fmt.Sprintf(
			`{"id":"%s","eventType":"governance.audit_log_created","tenantId":"%s","entityType":"reconciliation_context","entityId":"%s","action":"CHAOS_TENANT_DISCOVERY","occurredAt":"2026-01-01T00:00:00Z","timestamp":"2026-01-01T00:00:00Z"}`,
			uuid.New(),
			h.Seed.TenantID,
			h.Seed.ContextID,
		)

		_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			_, execErr := tx.ExecContext(ctx, `
				INSERT INTO outbox_events (id, event_type, aggregate_id, payload, status, attempts, created_at, updated_at)
				VALUES (gen_random_uuid(), 'governance.audit_log_created', gen_random_uuid(), $1, 'PENDING', 0, NOW(), NOW())
			`, payload)
			return struct{}{}, execErr
		})
		require.NoError(t, err, "create outbox event %d", i)
	}

	initialStats := GetOutboxStats(t, directDB)
	require.Equal(t, 5, initialStats.Pending,
		"expected 5 pending outbox events before outage; got %s", initialStats)

	// Boot service.
	cs := BootChaosServer(t, h)
	require.NotNil(t, cs.Dispatcher, "outbox dispatcher should be available")

	dispatchOnceWithTimeout := func(timeout time.Duration) int {
		dispatchCtx, cancel := context.WithTimeout(h.Ctx(), timeout)
		defer cancel()

		return cs.Dispatcher.DispatchOnce(dispatchCtx)
	}

	// Briefly disable PG during dispatch.
	h.DisablePGProxy(t)

	// Attempt dispatch — should fail gracefully (no panic, no corruption).
	processed := dispatchOnceWithTimeout(3 * time.Second)
	t.Logf("Dispatch during PG outage: processed=%d (expected 0)", processed)
	assert.Equal(t, 0, processed,
		"dispatch should process 0 events during PG outage")

	// Re-enable PG and allow connection pool to recover.
	// The pool holds stale/dead connections from the outage; it needs time
	// to detect them and establish fresh ones.
	h.EnablePGProxy(t)
	time.Sleep(3 * time.Second)

	// Next dispatch cycle should succeed. Use a short per-attempt timeout so
	// stale pooled connections fail fast and Eventually can retry.
	var totalProcessed int

	require.Eventually(t, func() bool {
		p := dispatchOnceWithTimeout(3 * time.Second)
		totalProcessed += p

		return totalProcessed > 0
	}, 30*time.Second, 1*time.Second,
		"dispatch should recover after PG re-enable")

	statsAfterRecovery := GetOutboxStats(t, directDB)
	t.Logf("VERIFIED: Dispatcher recovered after PG outage. Processed=%d, stats=%s",
		totalProcessed,
		statsAfterRecovery,
	)

	// Verify no events were lost.
	assert.Equal(t, 0, statsAfterRecovery.Invalid,
		"no events should be permanently invalid: %s", statsAfterRecovery)
}
