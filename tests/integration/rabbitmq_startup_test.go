//go:build integration

package integration

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

type fakeRabbitMQStartupContainer struct {
	hostResults   []hostResult
	hostCalls     int
	mappedPorts   map[string][]portResult
	mappedCalls   map[string]int
	inspectResult *container.InspectResponse
	inspectErr    error
}

type fakeGenericContainerResult struct {
	container testcontainers.Container
	err       error
}

type hostResult struct {
	host string
	err  error
}

type portResult struct {
	port network.Port
	err  error
}

func (c *fakeRabbitMQStartupContainer) Host(_ context.Context) (string, error) {
	if c.hostCalls >= len(c.hostResults) {
		return "", errors.New("unexpected host call")
	}

	result := c.hostResults[c.hostCalls]
	c.hostCalls++

	return result.host, result.err
}

func (c *fakeRabbitMQStartupContainer) MappedPort(_ context.Context, port string) (network.Port, error) {
	if c.mappedCalls == nil {
		c.mappedCalls = make(map[string]int)
	}

	results := c.mappedPorts[port]
	callIndex := c.mappedCalls[port]
	c.mappedCalls[port] = callIndex + 1

	if callIndex >= len(results) {
		return network.Port{}, errors.New("unexpected mapped port call")
	}

	return results[callIndex].port, results[callIndex].err
}

func (c *fakeRabbitMQStartupContainer) Inspect(_ context.Context) (*container.InspectResponse, error) {
	return c.inspectResult, c.inspectErr
}

// mustParsePort is a test helper that parses a port string, fataling on error.
func mustParsePort(t *testing.T, s string) network.Port {
	t.Helper()

	p, err := network.ParsePort(s)
	require.NoError(t, err, "parsing port %q", s)

	return p
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

func TestContainerHostWithRetry_TypedNilContainer(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var typedNil *fakeRabbitMQStartupContainer

	_, err := containerHostWithRetry(ctx, typedNil)
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

func TestStartRabbitMQContainer_TypedNilContainerRejected(t *testing.T) {
	// Not parallel: overrides package-level container factory seam.

	originalFactory := rabbitMQContainerFactory
	rabbitMQContainerFactory = func(context.Context, testcontainers.GenericContainerRequest) (testcontainers.Container, error) {
		var typedNil *fakeStartedContainer
		return typedNil, nil
	}
	t.Cleanup(func() { rabbitMQContainerFactory = originalFactory })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := startRabbitMQContainer(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil container")
}

func TestContainerHostWithRetry_RetriesUntilSuccess(t *testing.T) {
	// Not parallel: overrides package-level retry wait seam.

	originalWait := rabbitMQRetryWait
	var waits []time.Duration
	rabbitMQRetryWait = func(_ context.Context, d time.Duration) error {
		waits = append(waits, d)
		return nil
	}
	t.Cleanup(func() { rabbitMQRetryWait = originalWait })

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
	assert.Equal(t, []time.Duration{rabbitMQRetryDelay, rabbitMQRetryDelay}, waits)
}

func TestContainerHostWithRetry_EmptyHostExhaustsRetries(t *testing.T) {
	// Not parallel: overrides package-level retry wait seam.

	originalWait := rabbitMQRetryWait
	rabbitMQRetryWait = func(context.Context, time.Duration) error { return nil }
	t.Cleanup(func() { rabbitMQRetryWait = originalWait })

	fake := &fakeRabbitMQStartupContainer{
		hostResults: make([]hostResult, rabbitMQRetryCount),
	}

	_, err := containerHostWithRetry(context.Background(), fake)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty container host")
	assert.Equal(t, rabbitMQRetryCount, fake.hostCalls)
}

func TestMappedPortWithRetry_EmptyPortExhaustsRetries(t *testing.T) {
	// Not parallel: overrides package-level retry wait seam.

	originalWait := rabbitMQRetryWait
	rabbitMQRetryWait = func(context.Context, time.Duration) error { return nil }
	t.Cleanup(func() { rabbitMQRetryWait = originalWait })

	fake := &fakeRabbitMQStartupContainer{
		mappedPorts: map[string][]portResult{
			"5672/tcp": make([]portResult, rabbitMQRetryCount),
		},
	}

	_, err := mappedPortWithRetry(context.Background(), fake, "5672/tcp")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty mapped port")
}

func TestResolveMappedPort_FallsBackToRawPortLookup(t *testing.T) {
	t.Parallel()

	fake := &fakeRabbitMQStartupContainer{
		mappedPorts: map[string][]portResult{
			"5672/tcp": {{err: errors.New("protocol lookup failed")}},
			"5672":     {{port: mustParsePort(t, "5673/tcp")}},
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

func TestResolveMappedPort_ReturnsErrorWhenInspectDetailsAreNil(t *testing.T) {
	t.Parallel()

	fake := &fakeRabbitMQStartupContainer{
		mappedPorts: map[string][]portResult{
			"5672/tcp": {{err: errors.New("protocol lookup failed")}},
			"5672":     {{err: errors.New("raw lookup failed")}},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := resolveMappedPort(ctx, fake, "5672/tcp")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "inspect returned nil container details")
}

func TestResolveMappedPort_FallsBackToInspectBindings(t *testing.T) {
	t.Parallel()

	fake := &fakeRabbitMQStartupContainer{
		mappedPorts: map[string][]portResult{
			"5672/tcp": {{err: errors.New("protocol lookup failed")}},
			"5672":     {{err: errors.New("raw lookup failed")}},
		},
		inspectResult: &container.InspectResponse{
			NetworkSettings: &container.NetworkSettings{
				Ports: network.PortMap{
					mustParsePort(t, "5672/tcp"): {{HostPort: "5673"}},
				},
			},
		},
	}

	port, err := resolveMappedPort(context.Background(), fake, "5672/tcp")
	require.NoError(t, err)
	assert.Equal(t, "5673", port)
}

func TestResolveMappedPort_FallsBackToHostNetworkMode(t *testing.T) {
	t.Parallel()

	fake := &fakeRabbitMQStartupContainer{
		mappedPorts: map[string][]portResult{
			"5672/tcp": {{err: errors.New("protocol lookup failed")}},
			"5672":     {{err: errors.New("raw lookup failed")}},
		},
		inspectResult: &container.InspectResponse{
			HostConfig: &container.HostConfig{NetworkMode: "host"},
		},
	}

	port, err := resolveMappedPort(context.Background(), fake, "5672/tcp")
	require.NoError(t, err)
	assert.Equal(t, "5672", port)
}

func TestStartRabbitMQContainer_RetriesUntilSuccess(t *testing.T) {
	// Not parallel: overrides package-level container factory and retry wait seams.

	originalFactory := rabbitMQContainerFactory
	originalWait := rabbitMQRetryWait
	results := []fakeGenericContainerResult{{err: errors.New("fail-1")}, {err: errors.New("fail-2")}, {container: &fakeStartedContainer{}}}
	callCount := 0
	var waits []time.Duration
	rabbitMQContainerFactory = func(_ context.Context, _ testcontainers.GenericContainerRequest) (testcontainers.Container, error) {
		result := results[callCount]
		callCount++
		return result.container, result.err
	}
	rabbitMQRetryWait = func(_ context.Context, d time.Duration) error {
		waits = append(waits, d)
		return nil
	}
	t.Cleanup(func() {
		rabbitMQContainerFactory = originalFactory
		rabbitMQRetryWait = originalWait
	})

	c, err := startRabbitMQContainer(context.Background())
	require.NoError(t, err)
	assert.NotNil(t, c)
	assert.Equal(t, 3, callCount)
	assert.Equal(t, []time.Duration{2 * time.Second, 4 * time.Second}, waits)
}

func TestStartRabbitMQContainer_FailsAfterMaxAttempts(t *testing.T) {
	// Not parallel: overrides package-level container factory and retry wait seams.

	originalFactory := rabbitMQContainerFactory
	originalWait := rabbitMQRetryWait
	callCount := 0
	rabbitMQContainerFactory = func(_ context.Context, _ testcontainers.GenericContainerRequest) (testcontainers.Container, error) {
		callCount++
		return nil, errors.New("always fails")
	}
	rabbitMQRetryWait = func(context.Context, time.Duration) error { return nil }
	t.Cleanup(func() {
		rabbitMQContainerFactory = originalFactory
		rabbitMQRetryWait = originalWait
	})

	_, err := startRabbitMQContainer(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "start rabbitmq after 3 attempts")
	assert.Equal(t, rabbitMQStartAttempts, callCount)
}

func TestStartRabbitMQContainer_ReportsCleanupFailure(t *testing.T) {
	// Not parallel: overrides package-level container factory seam.

	originalFactory := rabbitMQContainerFactory
	rabbitMQContainerFactory = func(_ context.Context, _ testcontainers.GenericContainerRequest) (testcontainers.Container, error) {
		return &fakeStartedContainer{terminateErr: errors.New("terminate failed")}, errors.New("start failed")
	}
	t.Cleanup(func() { rabbitMQContainerFactory = originalFactory })

	_, err := startRabbitMQContainer(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cleanup failed after rabbitmq startup error")
	assert.Contains(t, err.Error(), "terminate failed")
}

func TestStartRabbitMQContainer_AbortsWhenRetryWaitIsCancelled(t *testing.T) {
	// Not parallel: overrides package-level container factory and retry wait seams.

	originalFactory := rabbitMQContainerFactory
	originalWait := rabbitMQRetryWait
	rabbitMQContainerFactory = func(_ context.Context, _ testcontainers.GenericContainerRequest) (testcontainers.Container, error) {
		return nil, errors.New("start failed")
	}
	rabbitMQRetryWait = func(_ context.Context, _ time.Duration) error { return context.Canceled }
	t.Cleanup(func() {
		rabbitMQContainerFactory = originalFactory
		rabbitMQRetryWait = originalWait
	})

	_, err := startRabbitMQContainer(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "wait before retrying rabbitmq startup")
	assert.ErrorIs(t, err, context.Canceled)
}

func TestContainerHostWithRetry_AbortsWhenRetryWaitIsCancelled(t *testing.T) {
	// Not parallel: overrides package-level retry wait seam.
	originalWait := rabbitMQRetryWait
	rabbitMQRetryWait = func(_ context.Context, _ time.Duration) error { return context.Canceled }
	t.Cleanup(func() { rabbitMQRetryWait = originalWait })

	fake := &fakeRabbitMQStartupContainer{
		hostResults: []hostResult{{err: errors.New("temporary host failure")}},
	}

	_, err := containerHostWithRetry(context.Background(), fake)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "wait before retrying rabbitmq host lookup")
	assert.ErrorIs(t, err, context.Canceled)
}

func TestMappedPortWithRetry_AbortsWhenRetryWaitIsCancelled(t *testing.T) {
	// Not parallel: overrides package-level retry wait seam.
	originalWait := rabbitMQRetryWait
	rabbitMQRetryWait = func(_ context.Context, _ time.Duration) error { return context.Canceled }
	t.Cleanup(func() { rabbitMQRetryWait = originalWait })

	fake := &fakeRabbitMQStartupContainer{
		mappedPorts: map[string][]portResult{
			"5672/tcp": {{err: errors.New("temporary mapped port failure")}},
		},
	}

	_, err := mappedPortWithRetry(context.Background(), fake, "5672/tcp")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "wait before retrying mapped port lookup")
	assert.ErrorIs(t, err, context.Canceled)
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
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

type fakeStartedContainer struct{ terminateErr error }

func (*fakeStartedContainer) GetContainerID() string                           { return "" }
func (*fakeStartedContainer) Endpoint(context.Context, string) (string, error) { return "", nil }
func (*fakeStartedContainer) PortEndpoint(context.Context, string, string) (string, error) {
	return "", nil
}
func (*fakeStartedContainer) Host(context.Context) (string, error) { return "", nil }
func (*fakeStartedContainer) Inspect(context.Context) (*container.InspectResponse, error) {
	return &container.InspectResponse{}, nil
}
func (*fakeStartedContainer) MappedPort(context.Context, string) (network.Port, error) {
	return network.Port{}, nil
}
func (*fakeStartedContainer) Ports(context.Context) (network.PortMap, error) { return nil, nil }
func (*fakeStartedContainer) SessionID() string                              { return "" }
func (*fakeStartedContainer) IsRunning() bool                                { return true }
func (*fakeStartedContainer) Start(context.Context) error                    { return nil }
func (*fakeStartedContainer) Stop(context.Context, *time.Duration) error     { return nil }
func (c *fakeStartedContainer) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	return c.terminateErr
}
func (*fakeStartedContainer) Logs(context.Context) (io.ReadCloser, error) { return nil, nil }
func (*fakeStartedContainer) FollowOutput(testcontainers.LogConsumer)     {}
func (*fakeStartedContainer) StartLogProducer(context.Context, ...testcontainers.LogProductionOption) error {
	return nil
}
func (*fakeStartedContainer) StopLogProducer() error                               { return nil }
func (*fakeStartedContainer) Name(context.Context) (string, error)                 { return "", nil }
func (*fakeStartedContainer) State(context.Context) (*container.State, error)       { return nil, nil }
func (*fakeStartedContainer) Networks(context.Context) ([]string, error)            { return nil, nil }
func (*fakeStartedContainer) NetworkAliases(context.Context) (map[string][]string, error) {
	return nil, nil
}

func (*fakeStartedContainer) Exec(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
	return 0, nil, nil
}
func (*fakeStartedContainer) ContainerIP(context.Context) (string, error)    { return "", nil }
func (*fakeStartedContainer) ContainerIPs(context.Context) ([]string, error) { return nil, nil }
func (*fakeStartedContainer) CopyToContainer(context.Context, []byte, string, int64) error {
	return nil
}

func (*fakeStartedContainer) CopyDirToContainer(context.Context, string, string, int64) error {
	return nil
}

func (*fakeStartedContainer) CopyFileToContainer(context.Context, string, string, int64) error {
	return nil
}

func (*fakeStartedContainer) CopyFileFromContainer(context.Context, string) (io.ReadCloser, error) {
	return nil, nil
}

func (*fakeStartedContainer) GetLogProductionErrorChannel() <-chan error { return nil }
