//go:build e2e

package e2e

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetGlobals(t *testing.T) {
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
		AppBaseURL:      "http://test:4018",
		DefaultTenantID: "test-tenant",
	}

	SetGlobals(cfg, nil)

	retrieved := GetConfig()
	require.NotNil(t, retrieved)
	assert.Equal(t, "http://test:4018", retrieved.AppBaseURL)
	assert.Equal(t, "test-tenant", retrieved.DefaultTenantID)
}

func TestGetConfig_ReturnsNilWhenNotSet(t *testing.T) {
	var originalConfig *E2EConfig
	globalMu.RLock()
	originalConfig = globalConfig
	globalMu.RUnlock()
	defer func() {
		globalMu.Lock()
		globalConfig = originalConfig
		globalMu.Unlock()
	}()

	globalMu.Lock()
	globalConfig = nil
	globalMu.Unlock()

	result := GetConfig()
	assert.Nil(t, result)
}

func TestGetClient_ReturnsNilWhenNotSet(t *testing.T) {
	originalClient := globalClient
	defer func() {
		globalMu.Lock()
		globalClient = originalClient
		globalMu.Unlock()
	}()

	globalMu.Lock()
	globalClient = nil
	globalMu.Unlock()

	result := GetClient()
	assert.Nil(t, result)
}

func TestGlobals_ConcurrentAccess(t *testing.T) {
	originalConfig := globalConfig
	originalClient := globalClient
	defer func() {
		globalMu.Lock()
		globalConfig = originalConfig
		globalClient = originalClient
		globalMu.Unlock()
	}()

	cfg := &E2EConfig{
		AppBaseURL:      "http://concurrent:4018",
		DefaultTenantID: "concurrent-tenant",
	}
	SetGlobals(cfg, nil)

	var wg sync.WaitGroup
	numGoroutines := 100
	results := make(chan *E2EConfig, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- GetConfig()
		}()
	}

	wg.Wait()
	close(results)

	for retrieved := range results {
		assert.NotNil(t, retrieved)
		if retrieved != nil {
			assert.Equal(t, "http://concurrent:4018", retrieved.AppBaseURL)
		}
	}
}
