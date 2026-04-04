//go:build unit

package bootstrap

import (
	"testing"

	spregistry "github.com/LerianStudio/lib-commons/v4/commons/systemplane/registry"
	spservice "github.com/LerianStudio/lib-commons/v4/commons/systemplane/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMatcherSystemplaneRuntimeManager_ProvidesRuntimeAccessors(t *testing.T) {
	t.Parallel()

	reg := spregistry.New()
	store := &runtimeHandlerStoreStub{}
	supervisor := &runtimeHandlerSupervisorStub{}
	delegate := &runtimeSettingsManagerStub{}

	manager := newMatcherSystemplaneRuntimeManager(delegate, reg, store, supervisor)
	require.NotNil(t, manager)

	runtimeManager, ok := manager.(systemplaneRuntimeManager)
	require.True(t, ok)
	assert.Same(t, reg, runtimeManager.registry())
	assert.Same(t, store, runtimeManager.store())
	assert.Same(t, supervisor, runtimeManager.supervisor())
}

func TestNewMatcherSystemplaneRuntimeManager_NilDelegate(t *testing.T) {
	t.Parallel()

	var delegate spservice.Manager

	manager := newMatcherSystemplaneRuntimeManager(delegate, nil, nil, nil)
	assert.Nil(t, manager)
}

func TestNewMatcherSystemplaneRuntimeManager_IncompleteRuntimeDepsFallsBackToDelegate(t *testing.T) {
	t.Parallel()

	delegate := &runtimeSettingsManagerStub{}

	manager := newMatcherSystemplaneRuntimeManager(delegate, nil, nil, nil)
	require.NotNil(t, manager)
	assert.Same(t, delegate, manager)

	_, ok := manager.(systemplaneRuntimeManager)
	assert.False(t, ok)
}
