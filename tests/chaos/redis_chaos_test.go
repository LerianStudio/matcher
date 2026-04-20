//go:build chaos

package chaos

import (
	"database/sql"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/bootstrap"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// --------------------------------------------------------------------------
// CHAOS-04: Redis disappears at runtime (silent security degradation)
// --------------------------------------------------------------------------

// TestCHAOS04_RedisDisappears_ServiceDegraded verifies that when Redis disappears,
// the service correctly reports degraded readiness (503). This was previously a
// security gap where /ready returned 200 even with Redis down, silently disabling
// rate limiting. The fix ensures Redis health is reflected in readiness checks.
//
// Target: Rate limiting, idempotency, /ready endpoint.
// Injection: Disable Redis proxy entirely.
// Expected: /ready returns 503 (degraded), /health returns 200 (process alive).
func TestCHAOS04_RedisDisappears_ServiceDegraded(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")
	h.ResetDatabase(t)

	// Boot a full service through proxied infrastructure.
	h.SetEnvForBootstrap(t)

	svc, err := bootstrap.InitServersWithOptions(&bootstrap.Options{
		Logger: &libLog.NopLogger{},
	})
	require.NoError(t, err, "bootstrap service")

	app := svc.GetApp()

	t.Cleanup(func() {
		_ = app.Shutdown()
	})

	// Baseline: /ready should be OK and /health should be OK.
	assertHealthEndpoint(t, app, "/health", http.StatusOK)
	assertReadyEndpoint(t, app, http.StatusOK)

	// Baseline: database operations still work through proxy.
	ctx := h.Ctx()
	_, err = pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.ExecContext(ctx, "SELECT 1")
		return struct{}{}, execErr
	})
	require.NoError(t, err, "baseline DB operation should work")

	// ---------------------------------------------------------------
	// INJECT CHAOS: Kill Redis
	// ---------------------------------------------------------------
	h.DisableRedisProxy(t)

	// Give connections a moment to fail.
	time.Sleep(500 * time.Millisecond)

	// /health should still return 200 (liveness = process alive).
	assertHealthEndpoint(t, app, "/health", http.StatusOK)

	// /ready — Redis is now reflected in readiness checks.
	// The service correctly reports degraded (503) when Redis is down,
	// preventing load balancers from routing traffic to an instance
	// that cannot enforce rate limiting or idempotency.
	assertReadyEndpoint(t, app, http.StatusServiceUnavailable)

	t.Log("VERIFIED: Service correctly reports degraded (503) with Redis down. " +
		"Rate limiting and idempotency depend on Redis, so the service " +
		"should not accept traffic without them.")

	// Verify: database operations still work (PG is unaffected).
	_, err = pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.ExecContext(ctx, "SELECT 1")
		return struct{}{}, execErr
	})
	assert.NoError(t, err, "database operations should work even with Redis down")

	// ---------------------------------------------------------------
	// RECOVERY: Re-enable Redis
	// ---------------------------------------------------------------
	h.EnableRedisProxy(t)
	time.Sleep(1 * time.Second)

	// Service should recover without restart.
	assertReadyEndpoint(t, app, http.StatusOK)
}

// --------------------------------------------------------------------------
// CHAOS-05: Redis latency spike
// --------------------------------------------------------------------------

// TestCHAOS05_RedisLatencySpike verifies that Redis latency doesn't cascade
// into blocking HTTP request processing (rate limiter should timeout gracefully).
//
// Target: Rate limiter middleware, idempotency checks.
// Injection: 3-second latency on Redis proxy.
// Expected: Requests may be slower but should not hang indefinitely.
func TestCHAOS05_RedisLatencySpike(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")
	h.ResetDatabase(t)

	// Boot service.
	h.SetEnvForBootstrap(t)

	svc, err := bootstrap.InitServersWithOptions(&bootstrap.Options{
		Logger: &libLog.NopLogger{},
	})
	require.NoError(t, err, "bootstrap service")

	app := svc.GetApp()

	t.Cleanup(func() {
		_ = app.Shutdown()
	})

	// Inject: 3 seconds of latency on every Redis operation.
	h.InjectRedisLatency(t, 3000, 0)

	// Make a request to a rate-limited endpoint.
	// It should either succeed (rate limiter fails open) or fail with a timeout,
	// but NOT hang indefinitely.
	start := time.Now()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)

	resp, err := app.Test(req, 10000) // 10 second test timeout
	elapsed := time.Since(start)

	require.NoError(t, err, "request should complete (not hang)")
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}

	assert.Less(t, elapsed, 8*time.Second,
		"request should complete within reasonable time, not hang on Redis latency (%v)", elapsed)

	t.Logf("Request completed in %v with status %d under Redis latency injection",
		elapsed, resp.StatusCode)
}

// --------------------------------------------------------------------------
// Test helpers
// --------------------------------------------------------------------------

func assertHealthEndpoint(t *testing.T, app interface {
	Test(*http.Request, ...int) (*http.Response, error)
}, path string, expectedStatus int,
) {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, path, nil)

	resp, err := app.Test(req, 5000)
	require.NoError(t, err, "health request to %s", path)

	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}

	assert.Equal(t, expectedStatus, resp.StatusCode,
		"GET %s: expected %d, got %d", path, expectedStatus, resp.StatusCode)
}

func assertReadyEndpoint(t *testing.T, app interface {
	Test(*http.Request, ...int) (*http.Response, error)
}, expectedStatus int,
) {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)

	resp, err := app.Test(req, 10000)
	require.NoError(t, err, "readiness request")

	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)

		t.Logf("/ready response [%d]: %s", resp.StatusCode, string(body))
	}

	assert.Equal(t, expectedStatus, resp.StatusCode,
		"GET /ready: expected %d, got %d", expectedStatus, resp.StatusCode)
}
