//go:build unit

package rabbitmq

import (
	"context"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
)

// mockMultiTenantManager is a test mock for RabbitMQMultiTenantManager.
type mockMultiTenantManager struct {
	getChannelFn func(ctx context.Context, tenantID string) (*amqp.Channel, error)
}

func (m *mockMultiTenantManager) GetChannel(ctx context.Context, tenantID string) (*amqp.Channel, error) {
	if m.getChannelFn != nil {
		return m.getChannelFn(ctx, tenantID)
	}
	return nil, nil
}

func TestRabbitMQMultiTenantManager_InterfaceCompileCheck(t *testing.T) {
	t.Parallel()
	var _ RabbitMQMultiTenantManager = (*mockMultiTenantManager)(nil)
	assert.NotNil(t, &mockMultiTenantManager{})
}
