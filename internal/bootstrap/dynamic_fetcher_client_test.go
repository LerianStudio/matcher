//go:build unit

package bootstrap

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	discoveryWorker "github.com/LerianStudio/matcher/internal/discovery/services/worker"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func TestDynamicFetcherClient_Current_DisabledReturnsUnavailable(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Fetcher.Enabled = false
	client := newDynamicFetcherClient(cfg, nil)

	_, err := client.(*dynamicFetcherClient).current()
	require.Error(t, err)
	assert.ErrorIs(t, err, sharedPorts.ErrFetcherUnavailable)
}

func TestDynamicFetcherClient_Current_ReusesUntilConfigChanges(t *testing.T) {
	t.Parallel()

	activeCfg := defaultConfig()
	activeCfg.Fetcher.Enabled = true
	activeCfg.Fetcher.URL = "https://fetcher-a.example"
	client := newDynamicFetcherClient(activeCfg, func() *Config { return activeCfg }).(*dynamicFetcherClient)

	first, err := client.current()
	require.NoError(t, err)
	second, err := client.current()
	require.NoError(t, err)
	assert.Same(t, first, second)

	updated := *activeCfg
	updated.Fetcher.URL = "https://fetcher-b.example"
	activeCfg = &updated

	third, err := client.current()
	require.NoError(t, err)
	assert.NotSame(t, first, third)
}

func TestDynamicExtractionPoller_ReportsFailureWhenDelegateCannotBeBuilt(t *testing.T) {
	t.Parallel()

	poller := newDynamicExtractionPoller(nil, nil, func() discoveryWorker.ExtractionPollerConfig { return discoveryWorker.ExtractionPollerConfig{} }, nil)
	failed := false
	poller.PollUntilComplete(context.Background(), uuid.UUID{}, nil, func(context.Context, string) { failed = true })
	assert.True(t, failed)
}
