//go:build unit

package bootstrap

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFetcherHTTPClientConfig_SetsFieldsFromConfig(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Fetcher: FetcherConfig{
			URL:               "http://localhost:9090",
			AllowPrivateIPs:   true,
			HealthTimeoutSec:  5,
			RequestTimeoutSec: 30,
		},
	}

	clientCfg := fetcherHTTPClientConfig(cfg)

	assert.Equal(t, "http://localhost:9090", clientCfg.BaseURL)
	assert.True(t, clientCfg.AllowPrivateIPs)
	assert.Equal(t, 5*time.Second, clientCfg.HealthTimeout)
	assert.Equal(t, 30*time.Second, clientCfg.RequestTimeout)
	assert.Equal(t, defaultFetcherClientMaxRetries, clientCfg.MaxRetries)
	assert.Equal(t, defaultFetcherClientRetryBaseDelay, clientCfg.RetryBaseDelay)
}

func TestFetcherHTTPClientConfig_DefaultConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 3, defaultFetcherClientMaxRetries)
	assert.Equal(t, 500*time.Millisecond, defaultFetcherClientRetryBaseDelay)
}

func TestInitOptionalDiscoveryWorker_DisabledConfig_ReturnsNil(t *testing.T) {
	t.Parallel()

	cfg := &Config{Fetcher: FetcherConfig{Enabled: false}}

	worker, err := initOptionalDiscoveryWorker(
		t.Context(), nil, cfg, nil, nil, nil, nil,
	)

	assert.NoError(t, err)
	assert.Nil(t, worker)
}

func TestInitOptionalDiscoveryWorker_NilConfig_ReturnsNil(t *testing.T) {
	t.Parallel()

	worker, err := initOptionalDiscoveryWorker(
		t.Context(), nil, nil, nil, nil, nil, nil,
	)

	assert.NoError(t, err)
	assert.Nil(t, worker)
}
