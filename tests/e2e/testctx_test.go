//go:build e2e

package e2e

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTestContext(t *testing.T) {
	cfg := &E2EConfig{
		DefaultTenantID: "test-tenant-123",
	}

	tc := NewTestContext(t, cfg)

	require.NotNil(t, tc)
	assert.NotEmpty(t, tc.RunID())
	assert.Len(t, tc.RunID(), 8)
	assert.Equal(t, "test-tenant-123", tc.TenantID())
	assert.Contains(t, tc.NamePrefix(), "e2e-")
	assert.Contains(t, tc.NamePrefix(), tc.RunID())
}

func TestTestContext_UniqueName(t *testing.T) {
	cfg := &E2EConfig{
		DefaultTenantID: "test-tenant",
	}

	tc := NewTestContext(t, cfg)
	name := tc.UniqueName("my-entity")

	assert.Contains(t, name, tc.NamePrefix())
	assert.Contains(t, name, "my-entity")
	assert.True(t, len(name) > len("my-entity"))
}

func TestTestContext_UniqueNames_AreDifferent(t *testing.T) {
	cfg := &E2EConfig{
		DefaultTenantID: "test-tenant",
	}

	tc1 := NewTestContext(t, cfg)
	tc2 := NewTestContext(t, cfg)

	name1 := tc1.UniqueName("entity")
	name2 := tc2.UniqueName("entity")

	assert.NotEqual(t, name1, name2)
}

func TestTestContext_Config(t *testing.T) {
	cfg := &E2EConfig{
		AppBaseURL:      "http://test:4018",
		DefaultTenantID: "config-test",
	}

	tc := NewTestContext(t, cfg)

	retrievedCfg := tc.Config()
	require.NotNil(t, retrievedCfg)
	assert.Equal(t, cfg, retrievedCfg)
	assert.Equal(t, "http://test:4018", retrievedCfg.AppBaseURL)
}

func TestTestContext_Context_ReturnsBackground(t *testing.T) {
	cfg := &E2EConfig{
		DefaultTenantID: "test-tenant",
	}

	tc := NewTestContext(t, cfg)
	ctx := tc.Context()

	require.NotNil(t, ctx)
	assert.Equal(t, context.Background(), ctx)
}

func TestTestContext_ContextWithTimeout(t *testing.T) {
	cfg := &E2EConfig{
		DefaultTenantID: "test-tenant",
	}

	tc := NewTestContext(t, cfg)
	ctx, cancel := tc.ContextWithTimeout(100 * time.Millisecond)
	defer cancel()

	require.NotNil(t, ctx)

	deadline, ok := ctx.Deadline()
	assert.True(t, ok)
	assert.True(t, deadline.After(time.Now()))
}

func TestTestContext_RegisterCleanup(t *testing.T) {
	cfg := &E2EConfig{
		DefaultTenantID: "test-tenant",
	}

	var cleanupOrder []int

	// Cleanup handlers run when the subtest completes, so verifying cleanupOrder
	// after t.Run confirms RegisterCleanup executes in LIFO order.
	t.Run("registers and runs cleanups in LIFO order", func(t *testing.T) {
		tc := NewTestContext(t, cfg)

		tc.RegisterCleanup(func() error {
			cleanupOrder = append(cleanupOrder, 1)
			return nil
		})
		tc.RegisterCleanup(func() error {
			cleanupOrder = append(cleanupOrder, 2)
			return nil
		})
		tc.RegisterCleanup(func() error {
			cleanupOrder = append(cleanupOrder, 3)
			return nil
		})
	})

	assert.Equal(t, []int{3, 2, 1}, cleanupOrder)
}

func TestTestContext_RegisterCleanup_HandlesErrors(t *testing.T) {
	cfg := &E2EConfig{
		DefaultTenantID: "test-tenant",
	}

	var cleanupsCalled int

	t.Run("continues cleanup even on error", func(t *testing.T) {
		tc := NewTestContext(t, cfg)

		tc.RegisterCleanup(func() error {
			cleanupsCalled++
			return nil
		})
		tc.RegisterCleanup(func() error {
			cleanupsCalled++
			return errors.New("cleanup error")
		})
		tc.RegisterCleanup(func() error {
			cleanupsCalled++
			return nil
		})
	})

	assert.Equal(t, 3, cleanupsCalled)
}

func TestTestContext_ConcurrentCleanupRegistration(t *testing.T) {
	cfg := &E2EConfig{
		DefaultTenantID: "test-tenant",
	}

	var mu sync.Mutex
	cleanupCount := 0

	t.Run("handles concurrent registrations", func(t *testing.T) {
		tc := NewTestContext(t, cfg)

		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				tc.RegisterCleanup(func() error {
					mu.Lock()
					cleanupCount++
					mu.Unlock()
					return nil
				})
			}()
		}
		wg.Wait()
	})

	assert.Equal(t, 50, cleanupCount)
}

func TestTestContext_Logf(t *testing.T) {
	cfg := &E2EConfig{
		DefaultTenantID: "test-tenant",
	}

	tc := NewTestContext(t, cfg)

	tc.Logf("test message %d", 42)
}

func TestTestContext_RunID_IsConsistent(t *testing.T) {
	cfg := &E2EConfig{
		DefaultTenantID: "test-tenant",
	}

	tc := NewTestContext(t, cfg)

	runID1 := tc.RunID()
	runID2 := tc.RunID()

	assert.Equal(t, runID1, runID2)
}
