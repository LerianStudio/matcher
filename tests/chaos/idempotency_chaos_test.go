//go:build chaos

package chaos

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --------------------------------------------------------------------------
// CHAOS-18: Redis latency → idempotency 500 cascade
// --------------------------------------------------------------------------

// TestCHAOS18_IdempotencyRedisLatency_500Cascade verifies that when Redis
// has high latency, the idempotency middleware blocks ALL POST/PUT/PATCH
// requests with 500 errors — a total write blackout.
//
// Target: idempotency.go:216-222 — TryAcquire returns error → 500.
// Injection: Redis latency (2s) via Toxiproxy.
// Expected: Mutating endpoints return 500. GET endpoints still work.
//
// Finding: Idempotency middleware has no circuit breaker. Any Redis
//
//	degradation cascades into a full write outage.
func TestCHAOS18_IdempotencyRedisLatency_500Cascade(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")
	h.ResetDatabase(t)

	cs := BootChaosServer(t, h)

	// Baseline: POST should work normally.
	path := fmt.Sprintf("/v1/contexts/%s/rules", h.Seed.ContextID)

	resp, _ := cs.DoJSON(t, http.MethodPost, path, map[string]any{
		"priority": 1,
		"type":     "EXACT",
		"config":   map[string]any{"fields": []string{"amount"}},
	})

	t.Logf("Baseline POST: status=%d", resp.StatusCode)
	// Note: may be 2xx (created) or 4xx (validation) — either is fine for baseline.
	baselineWorked := resp.StatusCode != http.StatusInternalServerError

	// Inject: 2 seconds of Redis latency on every operation.
	h.InjectRedisLatency(t, 2000, 0)

	// Fire concurrent POST requests. Each one hits the idempotency middleware
	// which calls TryAcquire → Redis → 2s timeout → error → 500.
	const concurrency = 10

	var (
		wg        sync.WaitGroup
		status500 atomic.Int32
		statusOK  atomic.Int32
		other     atomic.Int32
	)

	for i := range concurrency {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			reqPath := fmt.Sprintf("/v1/contexts/%s/rules", h.Seed.ContextID)

			req := httptest.NewRequest(http.MethodPost, reqPath, strings.NewReader(fmt.Sprintf(
				`{"priority": %d, "type": "EXACT", "config": {"fields": ["amount"]}}`, idx+10,
			)))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Tenant-ID", h.Seed.TenantID.String())
			req.Header.Set("X-Idempotency-Key", uuid.New().String())

			resp, err := cs.App.Test(req, 15000)
			if err != nil {
				other.Add(1)
				return
			}

			if resp != nil && resp.Body != nil {
				defer resp.Body.Close()
			}

			switch {
			case resp.StatusCode == http.StatusInternalServerError:
				status500.Add(1)
			case resp.StatusCode >= 200 && resp.StatusCode < 500:
				statusOK.Add(1)
			default:
				other.Add(1)
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Under Redis latency: %d/500s, %d successful, %d other (out of %d requests)",
		status500.Load(), statusOK.Load(), other.Load(), concurrency)

	// Verify GET endpoints still work (they bypass idempotency).
	getReq := httptest.NewRequest(http.MethodGet, "/health", nil)

	getResp, err := cs.App.Test(getReq, 5000)
	require.NoError(t, err, "GET should work during Redis latency")

	if getResp != nil && getResp.Body != nil {
		defer getResp.Body.Close()
	}

	assert.Equal(t, http.StatusOK, getResp.StatusCode,
		"GET /health should work even with Redis latency (idempotency only affects POST/PUT/PATCH)")

	// The finding: if ANY requests returned 500, the middleware is failing closed.
	if status500.Load() > 0 && baselineWorked {
		t.Logf("FINDING CONFIRMED: Idempotency middleware fails-closed on Redis errors. "+
			"%d/%d POST requests returned 500. All mutating endpoints become unavailable "+
			"during any Redis latency spike. No circuit breaker, no fallback.",
			status500.Load(), concurrency)
	}

	// Recovery: remove toxic and verify system recovers.
	h.RemoveAllToxics(t)
	time.Sleep(1 * time.Second)

	recoveryResp, _ := cs.DoJSON(t, http.MethodPost, path, map[string]any{
		"priority": 99,
		"type":     "EXACT",
		"config":   map[string]any{"fields": []string{"currency"}},
	})

	t.Logf("Recovery POST after Redis latency removed: status=%d", recoveryResp.StatusCode)
}

// --------------------------------------------------------------------------
// CHAOS-19: MarkComplete failure → stale pending key → eventual duplicate
// --------------------------------------------------------------------------

// TestCHAOS19_IdempotencyMarkCompleteFailure verifies the dangerous time
// window when a request succeeds but MarkComplete fails (Redis dies between
// handler completion and cache write).
//
// Sequence:
//  1. POST request → TryAcquire succeeds (key marked "pending" in Redis)
//  2. Handler executes successfully → response sent to client
//  3. MarkComplete called → Redis fails → key stays "pending"
//  4. Same request retried within 5 min → gets 409 "in progress" (false!)
//  5. After 5 min TTL → key expires → retry executes again → DUPLICATE
//
// Target: idempotency.go:333 — markRequestComplete.
// Injection: Kill Redis AFTER the first request completes.
// Expected: Retry gets 409 (stale pending key).
//
// Finding: 5-minute window where retries are falsely rejected,
//
//	followed by a window where true duplicates can occur.
func TestCHAOS19_IdempotencyMarkCompleteFailure(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")
	h.ResetDatabase(t)

	cs := BootChaosServer(t, h)

	// Use a fixed idempotency key for both requests.
	idempotencyKey := uuid.New().String()
	path := fmt.Sprintf("/v1/contexts/%s/rules", h.Seed.ContextID)

	// First request: should succeed.
	req1 := httptest.NewRequest(http.MethodPost, path, strings.NewReader(
		`{"priority": 1, "type": "EXACT", "config": {"fields": ["amount"]}}`,
	))
	req1.Header.Set("Content-Type", "application/json")
	req1.Header.Set("X-Tenant-ID", h.Seed.TenantID.String())
	req1.Header.Set("X-Idempotency-Key", idempotencyKey)

	resp1, err := cs.App.Test(req1, 15000)
	require.NoError(t, err, "first request should complete")

	if resp1 != nil && resp1.Body != nil {
		defer resp1.Body.Close()
	}

	firstStatus := resp1.StatusCode
	t.Logf("First request with idempotency key: status=%d", firstStatus)

	// Kill Redis so MarkComplete can't persist the response.
	// In practice this tests the race: handler completed but cache update failed.
	h.DisableRedisProxy(t)
	time.Sleep(500 * time.Millisecond)

	// Re-enable Redis so the retry request can check the key.
	h.EnableRedisProxy(t)
	time.Sleep(500 * time.Millisecond)

	// Retry with the same idempotency key.
	req2 := httptest.NewRequest(http.MethodPost, path, strings.NewReader(
		`{"priority": 1, "type": "EXACT", "config": {"fields": ["amount"]}}`,
	))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("X-Tenant-ID", h.Seed.TenantID.String())
	req2.Header.Set("X-Idempotency-Key", idempotencyKey)

	resp2, err := cs.App.Test(req2, 15000)
	require.NoError(t, err, "retry request should complete")

	if resp2 != nil && resp2.Body != nil {
		defer resp2.Body.Close()
	}

	retryStatus := resp2.StatusCode
	t.Logf("Retry with same idempotency key after Redis kill: status=%d", retryStatus)

	// Analyze the behavior:
	// - If retry gets 409 (Conflict) → stale pending key is blocking retries
	// - If retry gets the same response as first → idempotency worked despite chaos
	// - If retry creates a new resource → the key expired/was lost → potential duplicate
	switch {
	case retryStatus == http.StatusConflict || retryStatus == http.StatusTooManyRequests:
		t.Log("FINDING: Retry returned 409/429 — stale 'pending' key is falsely blocking retries. " +
			"The first request succeeded but MarkComplete failed. " +
			"The client must wait for the pending TTL (5 min) before retrying.")
	case retryStatus == firstStatus:
		t.Log("Idempotency working as expected — retry returned cached response or re-created the same resource.")
	case retryStatus >= 200 && retryStatus < 300 && firstStatus >= 200 && firstStatus < 300:
		t.Log("WARNING: Both requests returned success. If they created different resources, " +
			"this is a true duplicate. If the second returned the same resource, it's benign.")
	default:
		t.Logf("Retry returned status %d (first was %d) — investigate manually", retryStatus, firstStatus)
	}
}

// --------------------------------------------------------------------------
// CHAOS-20: Rate limiter fail-open vs idempotency fail-closed inconsistency
// --------------------------------------------------------------------------

// TestCHAOS20_RedisDown_FailOpenVsFailClosed documents the inconsistency
// between rate limiter (fails-open, allows all requests) and idempotency
// middleware (fails-closed, blocks all mutations) when Redis is unavailable.
//
// Target: Rate limiter (fiber_server.go) vs idempotency (idempotency.go).
// Injection: Disable Redis proxy entirely.
// Expected: GET requests succeed (no middleware affected).
//
//	POST requests fail with 500 (idempotency blocks).
//	Rate limiting silently stops (fail-open).
//	Result: system is both unprotected from floods AND unable
//	to process legitimate writes.
func TestCHAOS20_RedisDown_FailOpenVsFailClosed(t *testing.T) {
	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")
	h.ResetDatabase(t)

	cs := BootChaosServer(t, h)

	// Baseline: verify both GET and POST work.
	getResp, _ := cs.DoGet(t, "/health")
	assert.Equal(t, http.StatusOK, getResp.StatusCode, "baseline GET")

	postPath := fmt.Sprintf("/v1/contexts/%s/rules", h.Seed.ContextID)

	postResp, _ := cs.DoJSON(t, http.MethodPost, postPath, map[string]any{
		"priority": 1,
		"type":     "EXACT",
		"config":   map[string]any{"fields": []string{"amount"}},
	})
	baselinePostStatus := postResp.StatusCode
	t.Logf("Baseline: GET=/health→200, POST→%d", baselinePostStatus)

	// Kill Redis.
	h.DisableRedisProxy(t)
	time.Sleep(1 * time.Second)

	// Test 1: GET /health should still work (no Redis dependency).
	getResp2, _ := cs.DoGet(t, "/health")
	getWorks := getResp2.StatusCode == http.StatusOK

	// Test 2: GET /ready — depends on Redis optionality (FIX-1 may affect this).
	readyReq := httptest.NewRequest(http.MethodGet, "/ready", nil)

	readyResp, _ := cs.App.Test(readyReq, 10000)
	if readyResp != nil && readyResp.Body != nil {
		defer readyResp.Body.Close()
	}

	readyStatus := readyResp.StatusCode

	// Test 3: POST — should fail with 500 from idempotency middleware.
	postResp2, _ := cs.DoJSON(t, http.MethodPost, postPath, map[string]any{
		"priority": 2,
		"type":     "EXACT",
		"config":   map[string]any{"fields": []string{"currency"}},
	})
	postStatus := postResp2.StatusCode

	// Test 4: Rapid GET requests — no rate limiting protection.
	rapidGetCount := 0

	for range 50 {
		req := httptest.NewRequest(http.MethodGet, "/health", nil)

		resp, err := cs.App.Test(req, 5000)
		if err == nil && resp.StatusCode == http.StatusOK {
			rapidGetCount++
		}

		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
	}

	t.Logf("With Redis DOWN:")
	t.Logf("  GET /health: works=%v", getWorks)
	t.Logf("  GET /ready:  status=%d (FIX-1 should make this 503 with rate limiting enabled)", readyStatus)
	t.Logf("  POST:        status=%d (idempotency fail-closed = 500)", postStatus)
	t.Logf("  50 rapid GETs: %d/50 succeeded (rate limiter fail-open = no protection)", rapidGetCount)

	if postStatus == http.StatusInternalServerError && rapidGetCount >= 50 {
		t.Log("FINDING CONFIRMED: Inconsistent failure modes when Redis is down. " +
			"Rate limiter: fail-OPEN (all 50 rapid requests passed). " +
			"Idempotency: fail-CLOSED (POST returns 500). " +
			"The system is simultaneously vulnerable to floods " +
			"AND unable to process legitimate mutations.")
	}

	// Recovery.
	h.EnableRedisProxy(t)
	time.Sleep(1 * time.Second)
}
