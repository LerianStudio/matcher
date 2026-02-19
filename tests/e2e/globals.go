//go:build e2e

package e2e

import "sync"

var (
	globalMu     sync.RWMutex
	globalConfig *E2EConfig
	globalClient *Client
)

// SetGlobals sets the global config and client (called from TestMain).
func SetGlobals(cfg *E2EConfig, client *Client) {
	globalMu.Lock()
	defer globalMu.Unlock()
	globalConfig = cfg
	globalClient = client
}

// GetConfig returns the global e2e configuration.
func GetConfig() *E2EConfig {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return globalConfig
}

// GetClient returns the global API client.
func GetClient() *Client {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return globalClient
}
