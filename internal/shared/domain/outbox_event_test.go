//go:build unit

package shared

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/lib-commons/v5/commons/outbox"
)

func TestOutboxEventStatus_Constants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, outbox.OutboxStatusPending, OutboxStatusPending)
	assert.Equal(t, outbox.OutboxStatusProcessing, OutboxStatusProcessing)
	assert.Equal(t, outbox.OutboxStatusPublished, OutboxStatusPublished)
	assert.Equal(t, outbox.OutboxStatusFailed, OutboxStatusFailed)
	assert.Equal(t, outbox.OutboxStatusInvalid, OutboxStatusInvalid)
}

func TestNewOutboxEvent_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	aggregateID := uuid.New()
	payload := []byte(`{"key":"value"}`)

	event, err := NewOutboxEvent(ctx, "test.event", aggregateID, payload)

	require.NoError(t, err)
	require.NotNil(t, event)
	assert.NotEqual(t, uuid.Nil, event.ID)
	assert.Equal(t, "test.event", event.EventType)
	assert.Equal(t, aggregateID, event.AggregateID)
	assert.Equal(t, payload, event.Payload)
	assert.Equal(t, OutboxStatusPending, event.Status)
	assert.Equal(t, 0, event.Attempts)
	assert.Nil(t, event.PublishedAt)
	assert.Empty(t, event.LastError)
	assert.False(t, event.CreatedAt.IsZero())
	assert.False(t, event.UpdatedAt.IsZero())
}

func TestNewOutboxEvent_EmptyEventType(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	event, err := NewOutboxEvent(ctx, "", uuid.New(), []byte(`{}`))

	assert.Error(t, err)
	assert.Nil(t, event)
}

func TestNewOutboxEvent_NilAggregateID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	event, err := NewOutboxEvent(ctx, "test.event", uuid.Nil, []byte(`{}`))

	assert.Error(t, err)
	assert.Nil(t, event)
}

func TestNewOutboxEvent_EmptyPayload(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	event, err := NewOutboxEvent(ctx, "test.event", uuid.New(), []byte{})

	assert.Error(t, err)
	assert.Nil(t, event)
}

func TestNewOutboxEvent_NilPayload(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	event, err := NewOutboxEvent(ctx, "test.event", uuid.New(), nil)

	assert.Error(t, err)
	assert.Nil(t, event)
}

func TestNewOutboxEvent_InvalidJSONPayload(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	event, err := NewOutboxEvent(ctx, "test.event", uuid.New(), []byte("not-json"))

	assert.Error(t, err)
	assert.Nil(t, event)
}

func TestErrOutboxEventRequired(t *testing.T) {
	t.Parallel()

	assert.EqualError(t, ErrOutboxEventRequired, "outbox event is required")
}
