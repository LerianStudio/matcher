//go:build unit

package ports

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/domain/services"
)

func TestExternalConnector_InterfaceCompliance(t *testing.T) {
	t.Parallel()

	var _ ExternalConnector = (*mockExternalConnector)(nil)
}

type mockExternalConnector struct {
	dispatchResult DispatchResult
	dispatchErr    error
}

func (m *mockExternalConnector) Dispatch(
	_ context.Context,
	_ string,
	_ services.RoutingDecision,
	_ []byte,
) (DispatchResult, error) {
	return m.dispatchResult, m.dispatchErr
}

func TestExternalConnector_MockImplementation(t *testing.T) {
	t.Parallel()

	t.Run("dispatches successfully", func(t *testing.T) {
		t.Parallel()

		expectedResult := DispatchResult{
			Target:            services.RoutingTargetJira,
			ExternalReference: "JIRA-123",
			Acknowledged:      true,
		}

		connector := &mockExternalConnector{dispatchResult: expectedResult}
		ctx := t.Context()

		decision := services.RoutingDecision{
			Target:   services.RoutingTargetJira,
			Queue:    "support",
			Assignee: "team-lead",
		}
		payload := []byte(`{"title": "Exception"}`)

		result, err := connector.Dispatch(ctx, "exc-123", decision, payload)

		assert.NoError(t, err)
		assert.Equal(t, expectedResult.Target, result.Target)
		assert.Equal(t, expectedResult.ExternalReference, result.ExternalReference)
		assert.True(t, result.Acknowledged)
	})

	t.Run("returns error on dispatch failure", func(t *testing.T) {
		t.Parallel()

		connector := &mockExternalConnector{
			dispatchErr: assert.AnError,
		}
		ctx := t.Context()

		decision := services.RoutingDecision{Target: services.RoutingTargetWebhook}

		_, err := connector.Dispatch(ctx, "exc-456", decision, nil)

		require.Error(t, err)
	})
}

func TestDispatchResult_Fields(t *testing.T) {
	t.Parallel()

	t.Run("creates result with all fields", func(t *testing.T) {
		t.Parallel()

		result := DispatchResult{
			Target:            services.RoutingTargetServiceNow,
			ExternalReference: "INC0012345",
			Acknowledged:      true,
		}

		assert.Equal(t, services.RoutingTargetServiceNow, result.Target)
		assert.Equal(t, "INC0012345", result.ExternalReference)
		assert.True(t, result.Acknowledged)
	})

	t.Run("creates result with unacknowledged", func(t *testing.T) {
		t.Parallel()

		result := DispatchResult{
			Target:       services.RoutingTargetWebhook,
			Acknowledged: false,
		}

		assert.Equal(t, services.RoutingTargetWebhook, result.Target)
		assert.Empty(t, result.ExternalReference)
		assert.False(t, result.Acknowledged)
	})

	t.Run("creates result for manual routing", func(t *testing.T) {
		t.Parallel()

		result := DispatchResult{
			Target:       services.RoutingTargetManual,
			Acknowledged: true,
		}

		assert.Equal(t, services.RoutingTargetManual, result.Target)
		assert.True(t, result.Acknowledged)
	})
}
