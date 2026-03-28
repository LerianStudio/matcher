//go:build unit

package rabbitmq

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
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

func TestPublishMatchConfirmed_NilPublisher_ReturnsError(t *testing.T) {
	t.Parallel()

	var publisher *EventPublisher

	err := publisher.PublishMatchConfirmed(context.Background(), &sharedDomain.MatchConfirmedEvent{})
	require.Error(t, err)
	assert.ErrorIs(t, err, errPublisherNotInit)
}

func TestPublishMatchConfirmed_NoPublisherOrManager_ReturnsError(t *testing.T) {
	t.Parallel()

	// Empty publisher with neither confirmablePublisher nor rmqManager
	publisher := &EventPublisher{}

	err := publisher.PublishMatchConfirmed(context.Background(), &sharedDomain.MatchConfirmedEvent{})
	require.Error(t, err)
	assert.ErrorIs(t, err, errPublisherNotInit)
}

func TestPublishMatchConfirmed_NilEvent_ReturnsError(t *testing.T) {
	t.Parallel()

	// Multi-tenant publisher constructed directly
	publisher := &EventPublisher{
		multiTenant: true,
	}

	err := publisher.PublishMatchConfirmed(context.Background(), nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, errEventRequired)
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

	// Empty publisher with neither confirmablePublisher nor rmqManager
	publisher := &EventPublisher{}

	err := publisher.PublishMatchUnmatched(context.Background(), &sharedDomain.MatchUnmatchedEvent{})
	require.Error(t, err)
	assert.ErrorIs(t, err, errPublisherNotInit)
}

func TestPublishMatchUnmatched_NilEvent_ReturnsError(t *testing.T) {
	t.Parallel()

	// Multi-tenant publisher constructed directly
	publisher := &EventPublisher{
		multiTenant: true,
	}

	err := publisher.PublishMatchUnmatched(context.Background(), nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, errUnmatchedEventRequired)
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

// Note: Full publish tests for PublishMatchConfirmed and PublishMatchUnmatched with multi-tenant mode
// require integration tests with actual AMQP channels since *amqp.Channel cannot be easily mocked.
// The publishMultiTenant method is tested in integration tests for error handling paths.
