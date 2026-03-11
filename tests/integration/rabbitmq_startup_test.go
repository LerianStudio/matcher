//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
