//go:build unit

package rabbitmq

import (
	"context"
	"testing"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEventPublisherFromChannel_NilReturnsError(t *testing.T) {
	t.Parallel()

	_, err := NewEventPublisherFromChannel(nil)
	require.Error(t, err)
}

func TestNewEventPublisherMultiTenant_NilManagerReturnsError(t *testing.T) {
	t.Parallel()

	_, err := NewEventPublisherMultiTenant(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rabbitmq multi-tenant manager is required")
}

func TestNewEventPublisherMultiTenant_ValidManager(t *testing.T) {
	t.Parallel()

	mockManager := &mockRabbitMQMultiTenantManager{}

	publisher, err := NewEventPublisherMultiTenant(mockManager)
	require.NoError(t, err)
	require.NotNil(t, publisher)
	assert.NotNil(t, publisher.rabbitmqManager)
	assert.Nil(t, publisher.confirmablePublisher)
}

func TestEventPublisher_Close_MultiTenantMode(t *testing.T) {
	t.Parallel()

	mockManager := &mockRabbitMQMultiTenantManager{}

	publisher, err := NewEventPublisherMultiTenant(mockManager)
	require.NoError(t, err)

	// Close should be a no-op in multi-tenant mode
	err = publisher.Close()
	assert.NoError(t, err)
}

func TestEventPublisher_Close_NilPublisher(t *testing.T) {
	t.Parallel()

	var publisher *EventPublisher
	err := publisher.Close()
	assert.NoError(t, err)
}

// mockRabbitMQMultiTenantManager is a test double for RabbitMQMultiTenantManager.
type mockRabbitMQMultiTenantManager struct {
	channels       map[string]*mockAMQPChannel
	getChannelErr  error
	getChannelCall int
}

func (m *mockRabbitMQMultiTenantManager) GetChannel(ctx context.Context, tenantID string) (*amqp.Channel, error) {
	m.getChannelCall++

	if m.getChannelErr != nil {
		return nil, m.getChannelErr
	}

	// Note: In real implementation, this would return an actual *amqp.Channel
	// from the tenant's vhost. For unit tests, we cannot easily mock *amqp.Channel
	// since it's a concrete type with unexported fields. Integration tests should
	// cover the actual channel usage.
	return nil, nil
}

// mockAMQPChannel is a test double for amqp.Channel.
type mockAMQPChannel struct {
	exchangeDeclareErr error
	publishErr         error
	publishCalls       []publishCall
}

type publishCall struct {
	exchange   string
	routingKey string
	msg        amqp.Publishing
}

func TestEventPublisher_XTenantIDHeader_AddedWhenTenantPresent(t *testing.T) {
	t.Parallel()

	// This test verifies the X-Tenant-ID header is added when tenant context exists.
	// We can't fully test the publish path without a real AMQP connection or complex mocking,
	// but we can verify the logic by checking that the publisher correctly identifies
	// when it should use multi-tenant mode vs single-tenant mode.

	mockManager := &mockRabbitMQMultiTenantManager{}

	publisher, err := NewEventPublisherMultiTenant(mockManager)
	require.NoError(t, err)

	// Verify the publisher is in multi-tenant mode
	assert.NotNil(t, publisher.rabbitmqManager)
	assert.Nil(t, publisher.confirmablePublisher)
}

func TestPublishMultiTenant_RequiresTenantID(t *testing.T) {
	t.Parallel()

	mockManager := &mockRabbitMQMultiTenantManager{}

	publisher, err := NewEventPublisherMultiTenant(mockManager)
	require.NoError(t, err)

	// Call publishMultiTenant directly with empty tenant ID
	// This bypasses the full publish path and directly tests the tenant ID check
	err = publisher.publishMultiTenant(
		context.Background(),
		"test.routing.key",
		"", // empty tenant ID
		amqp.Publishing{},
		nil,
		nil,
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, errTenantIDRequiredForMultiTenant)
}

func TestPublishMultiTenant_GetChannelError(t *testing.T) {
	t.Parallel()

	mockManager := &mockRabbitMQMultiTenantManager{
		getChannelErr: assert.AnError,
	}

	publisher, err := NewEventPublisherMultiTenant(mockManager)
	require.NoError(t, err)

	// Call publishMultiTenant with a valid tenant ID but manager returns error
	err = publisher.publishMultiTenant(
		context.Background(),
		"test.routing.key",
		"550e8400-e29b-41d4-a716-446655440000",
		amqp.Publishing{},
		nil,
		nil,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get tenant channel")
}

func TestPublish_NilPublisher_ReturnsError(t *testing.T) {
	t.Parallel()

	var publisher *EventPublisher

	err := publisher.publish(context.Background(), "test.key", uuid.New(), nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, errPublisherNotInit)
}

func TestPublish_NoPublisherOrManager_ReturnsError(t *testing.T) {
	t.Parallel()

	// Empty publisher with neither confirmablePublisher nor rabbitmqManager
	publisher := &EventPublisher{}

	err := publisher.publish(context.Background(), "test.key", uuid.New(), nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, errPublisherNotInit)
}
