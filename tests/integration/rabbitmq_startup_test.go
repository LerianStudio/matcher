//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeRabbitMQStartupContainer struct {
	hostResults   []hostResult
	hostCalls     int
	mappedPorts   map[string][]portResult
	mappedCalls   map[string]int
	inspectResult *types.ContainerJSON
	inspectErr    error
}

type hostResult struct {
	host string
	err  error
}

type portResult struct {
	port nat.Port
	err  error
}

func (container *fakeRabbitMQStartupContainer) Host(_ context.Context) (string, error) {
	if container.hostCalls >= len(container.hostResults) {
		return "", errors.New("unexpected host call")
	}

	result := container.hostResults[container.hostCalls]
	container.hostCalls++

	return result.host, result.err
}

func (container *fakeRabbitMQStartupContainer) MappedPort(_ context.Context, port nat.Port) (nat.Port, error) {
	if container.mappedCalls == nil {
		container.mappedCalls = make(map[string]int)
	}

	key := string(port)
	results := container.mappedPorts[key]
	callIndex := container.mappedCalls[key]
	container.mappedCalls[key] = callIndex + 1

	if callIndex >= len(results) {
		return "", errors.New("unexpected mapped port call")
	}

	return results[callIndex].port, results[callIndex].err
}

func (container *fakeRabbitMQStartupContainer) Inspect(_ context.Context) (*types.ContainerJSON, error) {
	return container.inspectResult, container.inspectErr
}

func TestRabbitMQContainerRequest(t *testing.T) {
	t.Parallel()

	req := rabbitMQContainerRequest()

	assert.Equal(t, "rabbitmq:4.1.3-management-alpine", req.Image)
	assert.Contains(t, req.ExposedPorts, "5672/tcp")
	assert.Contains(t, req.ExposedPorts, "15672/tcp")
	assert.NotNil(t, req.WaitingFor)
}

func TestContainerHostWithRetry_NilContainer(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := containerHostWithRetry(ctx, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "container is nil")
}

func TestMappedPortWithRetry_NilContainer(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := mappedPortWithRetry(ctx, nil, "5672/tcp")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "container is nil")
}

func TestContainerHostWithRetry_RetriesUntilSuccess(t *testing.T) {
	t.Parallel()

	fake := &fakeRabbitMQStartupContainer{
		hostResults: []hostResult{
			{err: errors.New("temporary host failure")},
			{err: errors.New("temporary host failure")},
			{host: "127.0.0.1"},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	host, err := containerHostWithRetry(ctx, fake)
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1", host)
	assert.Equal(t, 3, fake.hostCalls)
}

func TestResolveMappedPort_FallsBackToRawPortLookup(t *testing.T) {
	t.Parallel()

	fake := &fakeRabbitMQStartupContainer{
		mappedPorts: map[string][]portResult{
			"5672/tcp": {{err: errors.New("protocol lookup failed")}},
			"5672":     {{port: nat.Port("5673/tcp")}},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	port, err := resolveMappedPort(ctx, fake, "5672/tcp")
	require.NoError(t, err)
	assert.Equal(t, "5673", port)
	assert.Equal(t, 1, fake.mappedCalls["5672/tcp"])
	assert.Equal(t, 1, fake.mappedCalls["5672"])
}

func TestWaitWithContext_ImmediateReturn(t *testing.T) {
	t.Parallel()

	err := waitWithContext(context.Background(), 1*time.Millisecond)
	assert.NoError(t, err)
}

func TestWaitWithContext_CancelledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := waitWithContext(ctx, 10*time.Second)
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}
