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
	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
)

func TestNewMultiTenantEventPublisher_NilManager(t *testing.T) {
	t.Parallel()

	pub, err := NewMultiTenantEventPublisher(nil)

	require.Nil(t, pub)
	require.ErrorIs(t, err, errRabbitMQManagerRequired)
}

func TestNewMultiTenantEventPublisher_Success(t *testing.T) {
	t.Parallel()

	mgr := tmrabbitmq.NewManager(nil, "test")

	pub, err := NewMultiTenantEventPublisher(mgr)

	require.NoError(t, err)
	require.NotNil(t, pub)
	assert.True(t, pub.multiTenant)
	assert.NotNil(t, pub.rmqManager)
	assert.NotNil(t, pub.propagator)
	// Single-tenant field should be nil
	assert.Nil(t, pub.confirmablePublisher)
}

func TestMultiTenantEventPublisher_Close_IsNoop(t *testing.T) {
	t.Parallel()

	mgr := tmrabbitmq.NewManager(nil, "test")
	pub, err := NewMultiTenantEventPublisher(mgr)
	require.NoError(t, err)

	require.NoError(t, pub.Close())
}

func TestMultiTenantEventPublisher_PublishIngestionCompleted_RequiresTenantID(t *testing.T) {
	t.Parallel()

	mgr := tmrabbitmq.NewManager(nil, "test")
	pub, err := NewMultiTenantEventPublisher(mgr)
	require.NoError(t, err)

	event := &entities.IngestionCompletedEvent{
		JobID:     uuid.New(),
		Timestamp: time.Now().UTC(),
	}

	// No tenant ID in context => should fail
	err = pub.PublishIngestionCompleted(context.Background(), event)
	require.ErrorIs(t, err, errTenantIDRequired)
}

func TestMultiTenantEventPublisher_PublishIngestionFailed_RequiresTenantID(t *testing.T) {
	t.Parallel()

	mgr := tmrabbitmq.NewManager(nil, "test")
	pub, err := NewMultiTenantEventPublisher(mgr)
	require.NoError(t, err)

	event := &entities.IngestionFailedEvent{
		JobID:     uuid.New(),
		Timestamp: time.Now().UTC(),
	}

	// No tenant ID in context => should fail
	err = pub.PublishIngestionFailed(context.Background(), event)
	require.ErrorIs(t, err, errTenantIDRequired)
}

func TestMultiTenantEventPublisher_PublishIngestionCompleted_NilEvent(t *testing.T) {
	t.Parallel()

	mgr := tmrabbitmq.NewManager(nil, "test")
	pub, err := NewMultiTenantEventPublisher(mgr)
	require.NoError(t, err)

	err = pub.PublishIngestionCompleted(context.Background(), nil)
	require.ErrorIs(t, err, errNilEvent)
}

func TestMultiTenantEventPublisher_PublishIngestionFailed_NilEvent(t *testing.T) {
	t.Parallel()

	mgr := tmrabbitmq.NewManager(nil, "test")
	pub, err := NewMultiTenantEventPublisher(mgr)
	require.NoError(t, err)

	err = pub.PublishIngestionFailed(context.Background(), nil)
	require.ErrorIs(t, err, errNilEvent)
}

func TestMultiTenantEventPublisher_PublishIngestionCompleted_WithTenantID_FailsOnGetChannel(t *testing.T) {
	t.Parallel()

	// Manager without a client => GetChannel will fail trying to get tenant config
	mgr := tmrabbitmq.NewManager(nil, "test")
	pub, err := NewMultiTenantEventPublisher(mgr)
	require.NoError(t, err)

	event := &entities.IngestionCompletedEvent{
		JobID:     uuid.New(),
		Timestamp: time.Now().UTC(),
	}

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-456")

	err = pub.PublishIngestionCompleted(ctx, event)
	require.Error(t, err)
	require.ErrorContains(t, err, "tenant vhost")
}
