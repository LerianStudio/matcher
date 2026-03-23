//go:build unit

package rabbitmq

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	tmrabbitmq "github.com/LerianStudio/lib-commons/v4/commons/tenant-manager/rabbitmq"

	"github.com/LerianStudio/matcher/internal/auth"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingPorts "github.com/LerianStudio/matcher/internal/matching/ports"
)

func TestNewMultiTenantEventPublisher_NilManager(t *testing.T) {
	t.Parallel()

	pub, err := NewMultiTenantEventPublisher(nil)

	require.Nil(t, pub)
	require.ErrorIs(t, err, errRabbitMQManagerRequired)
}

func TestNewMultiTenantEventPublisher_Success(t *testing.T) {
	t.Parallel()

	// tmrabbitmq.NewManager(nil, "test") creates a manager without a client.
	// This is sufficient to test the constructor sets fields correctly.
	mgr := tmrabbitmq.NewManager(nil, "test")

	pub, err := NewMultiTenantEventPublisher(mgr)

	require.NoError(t, err)
	require.NotNil(t, pub)
	assert.True(t, pub.multiTenant)
	assert.NotNil(t, pub.rmqManager)
	assert.NotNil(t, pub.propagator)
	// Single-tenant fields should be nil/zero
	assert.Nil(t, pub.conn)
	assert.Nil(t, pub.ch)
	assert.Nil(t, pub.confirmablePublisher)
}

func TestMultiTenantEventPublisher_Close_IsNoop(t *testing.T) {
	t.Parallel()

	mgr := tmrabbitmq.NewManager(nil, "test")
	pub, err := NewMultiTenantEventPublisher(mgr)
	require.NoError(t, err)

	// Close should be a no-op in multi-tenant mode (manager is closed by bootstrap)
	require.NoError(t, pub.Close())
}

func TestMultiTenantEventPublisher_PublishMatchConfirmed_RequiresTenantID(t *testing.T) {
	t.Parallel()

	mgr := tmrabbitmq.NewManager(nil, "test")
	pub, err := NewMultiTenantEventPublisher(mgr)
	require.NoError(t, err)

	event := &matchingEntities.MatchConfirmedEvent{
		MatchID:   uuid.New(),
		Timestamp: time.Now().UTC(),
	}

	// No tenant ID in context => should fail with errTenantIDRequired
	err = pub.PublishMatchConfirmed(context.Background(), event)
	require.ErrorIs(t, err, errTenantIDRequired)
}

func TestMultiTenantEventPublisher_PublishMatchUnmatched_RequiresTenantID(t *testing.T) {
	t.Parallel()

	mgr := tmrabbitmq.NewManager(nil, "test")
	pub, err := NewMultiTenantEventPublisher(mgr)
	require.NoError(t, err)

	event := &matchingEntities.MatchUnmatchedEvent{
		MatchID:   uuid.New(),
		RunID:     uuid.New(),
		ContextID: uuid.New(),
		Timestamp: time.Now().UTC(),
	}

	// No tenant ID in context => should fail with errTenantIDRequired
	err = pub.PublishMatchUnmatched(context.Background(), event)
	require.ErrorIs(t, err, errTenantIDRequired)
}

func TestMultiTenantEventPublisher_PublishMatchConfirmed_NilEvent(t *testing.T) {
	t.Parallel()

	mgr := tmrabbitmq.NewManager(nil, "test")
	pub, err := NewMultiTenantEventPublisher(mgr)
	require.NoError(t, err)

	err = pub.PublishMatchConfirmed(context.Background(), nil)
	require.ErrorIs(t, err, errEventRequired)
}

func TestMultiTenantEventPublisher_PublishMatchUnmatched_NilEvent(t *testing.T) {
	t.Parallel()

	mgr := tmrabbitmq.NewManager(nil, "test")
	pub, err := NewMultiTenantEventPublisher(mgr)
	require.NoError(t, err)

	err = pub.PublishMatchUnmatched(context.Background(), nil)
	require.ErrorIs(t, err, errUnmatchedEventRequired)
}

func TestMultiTenantEventPublisher_PublishMatchConfirmed_WithTenantID_FailsOnGetChannel(t *testing.T) {
	t.Parallel()

	// Manager without a client => GetChannel will fail trying to get tenant config
	mgr := tmrabbitmq.NewManager(nil, "test")
	pub, err := NewMultiTenantEventPublisher(mgr)
	require.NoError(t, err)

	event := &matchingEntities.MatchConfirmedEvent{
		MatchID:   uuid.New(),
		Timestamp: time.Now().UTC(),
	}

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-123")

	err = pub.PublishMatchConfirmed(ctx, event)
	require.Error(t, err)
	require.ErrorContains(t, err, "tenant vhost")
}

func TestMultiTenantEventPublisher_SatisfiesMatchEventPublisherInterface(t *testing.T) {
	t.Parallel()

	mgr := tmrabbitmq.NewManager(nil, "test")
	pub, err := NewMultiTenantEventPublisher(mgr)
	require.NoError(t, err)

	// Verify the publisher satisfies the port interface
	var _ matchingPorts.MatchEventPublisher = pub
}
