//go:build unit

package chaos

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeProxyController struct {
	name       string
	toxicNames []string
	listErr    error
	removeErrs map[string]error
	enabled    bool
	saveErr    error
}

func (proxy *fakeProxyController) Name() string { return proxy.name }

func (proxy *fakeProxyController) ListToxicNames() ([]string, error) {
	if proxy.listErr != nil {
		return nil, proxy.listErr
	}
	return append([]string(nil), proxy.toxicNames...), nil
}

func (proxy *fakeProxyController) RemoveToxicByName(name string) error {
	if err, ok := proxy.removeErrs[name]; ok {
		return err
	}
	return nil
}

func (proxy *fakeProxyController) SetEnabled(enabled bool) error {
	proxy.enabled = enabled
	return proxy.saveErr
}

func TestIsolateServiceProxy_UnknownService(t *testing.T) {
	t.Parallel()

	_, err := isolateServiceProxy("unknown-service", map[string]proxyController{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown service")
}

func TestIsolateServiceProxy_KnownServiceNilProxy(t *testing.T) {
	t.Parallel()

	_, err := isolateServiceProxy("postgres", map[string]proxyController{"postgres": nil})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "postgres proxy is nil")
}

func TestRemoveAllToxicsFromProxies_PositivePath(t *testing.T) {
	t.Parallel()

	proxy := &fakeProxyController{
		name:       "postgres",
		toxicNames: []string{"latency", "timeout"},
	}
	assert.NoError(t, removeAllToxicsFromProxies([]proxyController{proxy}))
}

func TestRemoveAllToxicsFromProxies_SurfacesFailures(t *testing.T) {
	t.Parallel()

	proxy := &fakeProxyController{
		name:       "redis",
		toxicNames: []string{"latency"},
		removeErrs: map[string]error{"latency": errors.New("remove failed")},
	}
	err := removeAllToxicsFromProxies([]proxyController{proxy})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "remove toxic latency from redis")
}

func TestEnableAllProxyControllers_PositivePath(t *testing.T) {
	t.Parallel()

	proxy := &fakeProxyController{name: "rabbitmq"}
	assert.NoError(t, enableAllProxyControllers([]proxyController{proxy}))
	assert.True(t, proxy.enabled)
}

func TestEnableAllProxyControllers_SurfacesFailures(t *testing.T) {
	t.Parallel()

	proxy := &fakeProxyController{name: "rabbitmq", saveErr: errors.New("save failed")}
	err := enableAllProxyControllers([]proxyController{proxy})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "re-enable proxy rabbitmq")
}

func TestIsolateServiceProxy_PositivePathAndRestore(t *testing.T) {
	t.Parallel()

	proxy := &fakeProxyController{name: "postgres", enabled: true}
	restore, err := isolateServiceProxy("postgres", map[string]proxyController{"postgres": proxy})
	require.NoError(t, err)
	assert.False(t, proxy.enabled)
	require.NoError(t, restore())
	assert.True(t, proxy.enabled)
}
