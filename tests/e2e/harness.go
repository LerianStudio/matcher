//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// RunE2E is the standard entry point for e2e tests.
// It creates an isolated TestContext and provides the API client.
func RunE2E(t *testing.T, testFn func(t *testing.T, tc *TestContext, client *Client)) {
	t.Helper()

	tc := NewTestContext(t, GetConfig())
	client := GetClient()

	testFn(t, tc, client)
}

// RunE2EWithTimeout runs a test with a wall-clock timeout enforced by an
// external watchdog. The watchdog fires even if testFn blocks on a synchronous
// HTTP call that ignored ctx. Most journeys still pass context.Background()
// to client calls, so without the watchdog the timeout was silently dead.
func RunE2EWithTimeout(
	t *testing.T,
	timeout time.Duration,
	testFn func(t *testing.T, tc *TestContext, client *Client),
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	tc := newTestContext(t, GetConfig(), ctx)
	client := GetClient()

	done := make(chan struct{})
	panicCh := make(chan any, 1)

	runtime.SafeGoWithContextAndComponent(
		ctx,
		&libLog.NopLogger{},
		constants.ApplicationName,
		"e2e-test-watchdog",
		runtime.KeepRunning,
		func(_ context.Context) {
			defer close(done)
			defer func() {
				if recovered := recover(); recovered != nil {
					panicCh <- recovered
				}
			}()

			testFn(t, tc, client)
		},
	)

	select {
	case <-done:
		select {
		case recovered := <-panicCh:
			panic(recovered)
		default:
		}
	case <-ctx.Done():
		t.Fatalf("test timed out after %v", timeout)
	}
}
