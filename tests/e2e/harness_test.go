//go:build e2e

package e2e

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunE2E_CreatesTestContext(t *testing.T) {
	globalMu.Lock()
	originalConfig := globalConfig
	originalClient := globalClient
	globalMu.Unlock()
	defer func() {
		globalMu.Lock()
		globalConfig = originalConfig
		globalClient = originalClient
		globalMu.Unlock()
	}()

	cfg := &E2EConfig{
		DefaultTenantID:   "harness-test-tenant",
		DefaultTenantSlug: "harness-test",
	}
	SetGlobals(cfg, nil)

	var capturedTC *TestContext
	var capturedClient *Client

	RunE2E(t, func(t *testing.T, tc *TestContext, client *Client) {
		capturedTC = tc
		capturedClient = client
	})

	require.NotNil(t, capturedTC)
	assert.NotEmpty(t, capturedTC.RunID())
	assert.Equal(t, "harness-test-tenant", capturedTC.TenantID())
	assert.Nil(t, capturedClient)
}

func TestRunE2E_UsesGlobalConfig(t *testing.T) {
	globalMu.Lock()
	originalConfig := globalConfig
	originalClient := globalClient
	globalMu.Unlock()
	defer func() {
		globalMu.Lock()
		globalConfig = originalConfig
		globalClient = originalClient
		globalMu.Unlock()
	}()

	cfg := &E2EConfig{
		AppBaseURL:      "http://harness-test:9999",
		DefaultTenantID: "config-tenant",
	}
	SetGlobals(cfg, nil)

	RunE2E(t, func(t *testing.T, tc *TestContext, client *Client) {
		require.NotNil(t, tc.Config())
		assert.Equal(t, "http://harness-test:9999", tc.Config().AppBaseURL)
	})
}

func TestRunE2E_MultipleRuns(t *testing.T) {
	globalMu.Lock()
	originalConfig := globalConfig
	originalClient := globalClient
	globalMu.Unlock()
	defer func() {
		globalMu.Lock()
		globalConfig = originalConfig
		globalClient = originalClient
		globalMu.Unlock()
	}()

	cfg := &E2EConfig{
		DefaultTenantID: "multi-run-tenant",
	}
	SetGlobals(cfg, nil)

	var runIDs []string
	var mu sync.Mutex

	for i := 0; i < 3; i++ {
		i := i
		t.Run(fmt.Sprintf("run-%d", i), func(t *testing.T) {
			RunE2E(t, func(t *testing.T, tc *TestContext, client *Client) {
				mu.Lock()
				runIDs = append(runIDs, tc.RunID())
				mu.Unlock()
			})
		})
	}

	assert.Len(t, runIDs, 3)
	assert.NotEqual(t, runIDs[0], runIDs[1])
	assert.NotEqual(t, runIDs[1], runIDs[2])
	assert.NotEqual(t, runIDs[0], runIDs[2])
}

func TestRunE2E_TestContextIsolation(t *testing.T) {
	globalMu.Lock()
	originalConfig := globalConfig
	originalClient := globalClient
	globalMu.Unlock()
	defer func() {
		globalMu.Lock()
		globalConfig = originalConfig
		globalClient = originalClient
		globalMu.Unlock()
	}()

	cfg := &E2EConfig{
		DefaultTenantID: "isolation-tenant",
	}
	SetGlobals(cfg, nil)

	var tc1Name, tc2Name string

	t.Run("first", func(t *testing.T) {
		RunE2E(t, func(t *testing.T, tc *TestContext, client *Client) {
			tc1Name = tc.UniqueName("entity")
		})
	})

	t.Run("second", func(t *testing.T) {
		RunE2E(t, func(t *testing.T, tc *TestContext, client *Client) {
			tc2Name = tc.UniqueName("entity")
		})
	})

	assert.NotEqual(t, tc1Name, tc2Name)
}
