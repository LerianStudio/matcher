//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/LerianStudio/lib-uncommons/v2/uncommons/runtime"
)

// RunE2E is the standard entry point for e2e tests.
// It creates an isolated TestContext and provides the API client.
func RunE2E(t *testing.T, testFn func(t *testing.T, tc *TestContext, client *Client)) {
	t.Helper()

	tc := NewTestContext(t, GetConfig())
	client := GetClient()

	testFn(t, tc, client)
}

// RunE2EWithTimeout runs a test with a specific timeout.
func RunE2EWithTimeout(
	t *testing.T,
	timeout time.Duration,
	testFn func(t *testing.T, tc *TestContext, client *Client),
) {
	t.Helper()

	tc := NewTestContext(t, GetConfig())
	client := GetClient()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	done := make(chan struct{})
	runtime.SafeGo(nil, "e2e-test-timeout", runtime.KeepRunning, func() {
		defer close(done)
		testFn(t, tc, client)
	})

	select {
	case <-done:
		// Test completed
	case <-ctx.Done():
		t.Fatalf("test timed out after %v", timeout)
	}
}
