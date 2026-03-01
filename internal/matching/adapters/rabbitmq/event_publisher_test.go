//go:build unit

package rabbitmq

import (
	"context"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
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
	getChannelErr  error
	getChannelCall int
}

func (m *mockRabbitMQMultiTenantManager) GetChannel(_ context.Context, _ string) (*amqp.Channel, error) {
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

func TestPublishMatchConfirmed_NilPublisher_ReturnsError(t *testing.T) {
	t.Parallel()

	var publisher *EventPublisher

	err := publisher.PublishMatchConfirmed(context.Background(), &sharedDomain.MatchConfirmedEvent{})
	require.Error(t, err)
	assert.ErrorIs(t, err, errPublisherNotInit)
}

func TestPublishMatchConfirmed_NoPublisherOrManager_ReturnsError(t *testing.T) {
	t.Parallel()

	// Empty publisher with neither confirmablePublisher nor rabbitmqManager
	publisher := &EventPublisher{}

	err := publisher.PublishMatchConfirmed(context.Background(), &sharedDomain.MatchConfirmedEvent{})
	require.Error(t, err)
	assert.ErrorIs(t, err, errPublisherNotInit)
}

func TestPublishMatchConfirmed_NilEvent_ReturnsError(t *testing.T) {
	t.Parallel()

	mockManager := &mockRabbitMQMultiTenantManager{}
	publisher, err := NewEventPublisherMultiTenant(mockManager)
	require.NoError(t, err)

	err = publisher.PublishMatchConfirmed(context.Background(), nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, errEventRequired)
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
		"match-123",
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
		"match-123",
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get tenant channel")
}

func TestPublishMatchUnmatched_NilPublisher_ReturnsError(t *testing.T) {
	t.Parallel()

	var publisher *EventPublisher

	err := publisher.PublishMatchUnmatched(context.Background(), &sharedDomain.MatchUnmatchedEvent{})
	require.Error(t, err)
	assert.ErrorIs(t, err, errPublisherNotInit)
}

func TestPublishMatchUnmatched_NoPublisherOrManager_ReturnsError(t *testing.T) {
	t.Parallel()

	// Empty publisher with neither confirmablePublisher nor rabbitmqManager
	publisher := &EventPublisher{}

	err := publisher.PublishMatchUnmatched(context.Background(), &sharedDomain.MatchUnmatchedEvent{})
	require.Error(t, err)
	assert.ErrorIs(t, err, errPublisherNotInit)
}

func TestPublishMatchUnmatched_NilEvent_ReturnsError(t *testing.T) {
	t.Parallel()

	mockManager := &mockRabbitMQMultiTenantManager{}
	publisher, err := NewEventPublisherMultiTenant(mockManager)
	require.NoError(t, err)

	err = publisher.PublishMatchUnmatched(context.Background(), nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, errUnmatchedEventRequired)
}

// Note: Full publish tests for PublishMatchConfirmed and PublishMatchUnmatched with multi-tenant mode
// require integration tests with actual AMQP channels since *amqp.Channel cannot be easily mocked.
// The publishMultiTenant method is tested directly above for error handling paths.
