//go:build unit

package ports

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
)

func TestDispatcherInterfaceCompiles(t *testing.T) {
	t.Parallel()

	var _ Dispatcher = (*mockDispatcher)(nil)
}

type mockDispatcher struct {
	running bool
	stopped bool
}

func (m *mockDispatcher) Run(_ *libCommons.Launcher) error {
	m.running = true
	return nil
}

func (m *mockDispatcher) Stop() {
	m.stopped = true
	m.running = false
}

func TestMockDispatcherRun(t *testing.T) {
	t.Parallel()

	dispatcher := &mockDispatcher{}

	err := dispatcher.Run(nil)

	require.NoError(t, err)
	assert.True(t, dispatcher.running)
}

func TestMockDispatcherStop(t *testing.T) {
	t.Parallel()

	dispatcher := &mockDispatcher{running: true}

	dispatcher.Stop()

	assert.True(t, dispatcher.stopped)
	assert.False(t, dispatcher.running)
}

func TestMockDispatcherLifecycle(t *testing.T) {
	t.Parallel()

	dispatcher := &mockDispatcher{}

	assert.False(t, dispatcher.running)
	assert.False(t, dispatcher.stopped)

	_ = dispatcher.Run(nil)
	assert.True(t, dispatcher.running)

	dispatcher.Stop()
	assert.False(t, dispatcher.running)
	assert.True(t, dispatcher.stopped)
}
