//go:build chaos

package chaos

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsolateService_UnknownService(t *testing.T) {
	t.Parallel()

	h := &ChaosHarness{}

	err := h.IsolateService(t, "unknown-service")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown service")
}

func TestIsolateService_KnownServiceNilProxy(t *testing.T) {
	t.Parallel()

	h := &ChaosHarness{}
	err := h.IsolateService(t, "postgres")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "postgres proxy is nil")
}

func TestRemoveAllToxics_NilProxies(t *testing.T) {
	t.Parallel()

	h := &ChaosHarness{
		PGProxy:     nil,
		RedisProxy:  nil,
		RabbitProxy: nil,
	}

	// Should not panic with nil proxies.
	assert.NotPanics(t, func() {
		h.RemoveAllToxics(t)
	})
}

func TestEnableAllProxies_NilProxies(t *testing.T) {
	t.Parallel()

	h := &ChaosHarness{
		PGProxy:     nil,
		RedisProxy:  nil,
		RabbitProxy: nil,
	}

	// Should not panic with nil proxies.
	assert.NotPanics(t, func() {
		h.EnableAllProxies(t)
	})
}
