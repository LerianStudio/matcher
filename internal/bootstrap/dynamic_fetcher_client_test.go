//go:build unit

package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func TestDynamicFetcherClient_Current_DisabledReturnsUnavailable(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Fetcher.Enabled = false
	client := newDynamicFetcherClient(cfg, nil, &libLog.NopLogger{})

	_, err := client.(*dynamicFetcherClient).current()
	require.Error(t, err)
	assert.ErrorIs(t, err, sharedPorts.ErrFetcherUnavailable)
}

func TestDynamicFetcherClient_Current_ReusesUntilConfigChanges(t *testing.T) {
	t.Parallel()

	activeCfg := defaultConfig()
	activeCfg.Fetcher.Enabled = true
	activeCfg.Fetcher.URL = "https://fetcher-a.example"
	client := newDynamicFetcherClient(activeCfg, func() *Config { return activeCfg }, &libLog.NopLogger{}).(*dynamicFetcherClient)

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

