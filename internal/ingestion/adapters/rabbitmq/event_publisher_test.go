//go:build unit

package rabbitmq

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEventPublisherFromChannel_NilReturnsError(t *testing.T) {
	t.Parallel()

	_, err := NewEventPublisherFromChannel(nil)
	require.Error(t, err)
}

func TestNewMultiTenantEventPublisher_NilManagerReturnsError(t *testing.T) {
	t.Parallel()

	_, err := NewMultiTenantEventPublisher(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, errRabbitMQManagerRequired)
}

func TestEventPublisher_Close_MultiTenantMode(t *testing.T) {
	t.Parallel()

	// Directly construct a multi-tenant publisher (bypasses NewMultiTenantEventPublisher
	// which requires a real *tmrabbitmq.Manager).
	publisher := &EventPublisher{
		multiTenant: true,
	}

	// Close should be a no-op in multi-tenant mode
	err := publisher.Close()
	assert.NoError(t, err)
}

func TestEventPublisher_Close_NilPublisher(t *testing.T) {
	t.Parallel()

	var publisher *EventPublisher
	err := publisher.Close()
	assert.NoError(t, err)
}

func TestEventPublisher_MultiTenantMode_FieldsSet(t *testing.T) {
	t.Parallel()

	// Verify a multi-tenant publisher has the expected field state.
	publisher := &EventPublisher{
		multiTenant: true,
	}

	assert.True(t, publisher.multiTenant)
	assert.Nil(t, publisher.confirmablePublisher)
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

	// Empty publisher with neither confirmablePublisher nor rmqManager
	publisher := &EventPublisher{}

	err := publisher.publish(context.Background(), "test.key", uuid.New(), nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, errPublisherNotInit)
}
