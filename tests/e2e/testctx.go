//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestContext provides per-test isolation with unique identifiers and cleanup registration.
type TestContext struct {
	t          *testing.T
	cfg        *E2EConfig
	runID      string
	tenantID   string
	namePrefix string

	mu       sync.Mutex
	cleanups []func() error
}

// NewTestContext creates a new test context with unique isolation identifiers.
func NewTestContext(t *testing.T, cfg *E2EConfig) *TestContext {
	t.Helper()

	runID := uuid.New().String()[:8]
	tc := &TestContext{
		t:          t,
		cfg:        cfg,
		runID:      runID,
		tenantID:   cfg.DefaultTenantID,
		namePrefix: fmt.Sprintf("e2e-%s", runID),
		cleanups:   make([]func() error, 0),
	}

	t.Cleanup(func() {
		tc.runCleanups()
	})

	return tc
}

// RunID returns the unique run identifier for this test.
func (tc *TestContext) RunID() string {
	return tc.runID
}

// TenantID returns the tenant ID for this test.
func (tc *TestContext) TenantID() string {
	return tc.tenantID
}

// NamePrefix returns a prefix for naming test entities.
func (tc *TestContext) NamePrefix() string {
	return tc.namePrefix
}

// UniqueName generates a unique name for a test entity.
func (tc *TestContext) UniqueName(base string) string {
	return fmt.Sprintf("%s-%s", tc.namePrefix, base)
}

// Config returns the e2e configuration.
func (tc *TestContext) Config() *E2EConfig {
	return tc.cfg
}

// Context returns a background context with test timeout.
func (tc *TestContext) Context() context.Context {
	return context.Background()
}

// ContextWithTimeout returns a context with the specified timeout.
func (tc *TestContext) ContextWithTimeout(
	timeout time.Duration,
) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), timeout)
}

// RegisterCleanup adds a cleanup function to be called in LIFO order when the test ends.
func (tc *TestContext) RegisterCleanup(fn func() error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.cleanups = append(tc.cleanups, fn)
}

// runCleanups executes all registered cleanup functions in LIFO order.
// If E2E_KEEP_DATA environment variable is set, cleanup is skipped to preserve test data.
func (tc *TestContext) runCleanups() {
	if os.Getenv("E2E_KEEP_DATA") != "" {
		tc.t.Logf("E2E_KEEP_DATA is set - skipping cleanup to preserve test data")
		return
	}

	tc.mu.Lock()
	cleanups := make([]func() error, len(tc.cleanups))
	copy(cleanups, tc.cleanups)
	tc.mu.Unlock()

	// Execute in reverse order (LIFO)
	for i := len(cleanups) - 1; i >= 0; i-- {
		if err := cleanups[i](); err != nil {
			tc.t.Logf("cleanup error: %v", err)
		}
	}
}

// Logf logs a message with the test context prefix.
func (tc *TestContext) Logf(format string, args ...any) {
	tc.t.Helper()
	tc.t.Logf("[%s] "+format, append([]any{tc.runID}, args...)...)
}
