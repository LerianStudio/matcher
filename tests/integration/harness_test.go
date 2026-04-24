//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestIntegration_Flow_TestHarnessStructure(t *testing.T) {
	t.Parallel()

	harness := &TestHarness{
		PostgresDSN:       "postgres://user:pass@host:5432/db",
		RedisAddr:         "redis://host:6379",
		RabbitMQHost:      "localhost",
		RabbitMQPort:      "5672",
		RabbitMQHealthURL: "http://localhost:15672",
	}

	require.NotEmpty(t, harness.PostgresDSN)
	require.NotEmpty(t, harness.RedisAddr)
	require.NotEmpty(t, harness.RabbitMQHost)
	require.NotEmpty(t, harness.RabbitMQPort)
	require.NotEmpty(t, harness.RabbitMQHealthURL)
}

func TestIntegration_Flow_TerminateContainerNil(t *testing.T) {
	t.Parallel()

	err := terminateContainer(context.Background(), nil)
	require.NoError(t, err)
}

func TestIntegration_Flow_RunWithDatabase(t *testing.T) {
	RunWithDatabase(t, func(t *testing.T, h *TestHarness) {
		require.NotNil(t, h.Connection)
		require.NotEqual(t, uuid.Nil, h.Seed.TenantID)
		require.NotEqual(t, uuid.Nil, h.Seed.ContextID)
		require.NotEqual(t, uuid.Nil, h.Seed.SourceID)

		ctx := h.Ctx()
		require.NotNil(t, ctx)
	})
}

func TestIntegration_Flow_SeedDataIsValid(t *testing.T) {
	RunWithDatabase(t, func(t *testing.T, h *TestHarness) {
		require.NotEmpty(t, h.PostgresDSN)
		require.NotNil(t, h.Connection)
		connected, err := h.Connection.IsConnected()
		require.NoError(t, err)
		require.True(t, connected)
	})
}
