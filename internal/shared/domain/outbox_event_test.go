//go:build unit

package shared

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOutboxEventStatus_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status OutboxEventStatus
		want   bool
	}{
		{name: "PENDING is valid", status: OutboxStatusPending, want: true},
		{name: "PROCESSING is valid", status: OutboxStatusProcessing, want: true},
		{name: "PUBLISHED is valid", status: OutboxStatusPublished, want: true},
		{name: "FAILED is valid", status: OutboxStatusFailed, want: true},
		{name: "INVALID is valid", status: OutboxStatusInvalid, want: true},
		{name: "empty string is invalid", status: OutboxEventStatus(""), want: false},
		{name: "lowercase pending is invalid", status: OutboxEventStatus("pending"), want: false},
		{name: "arbitrary string is invalid", status: OutboxEventStatus("UNKNOWN"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.status.IsValid())
		})
	}
}

func TestOutboxEventStatus_Constants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, OutboxEventStatus("PENDING"), OutboxStatusPending)
	assert.Equal(t, OutboxEventStatus("PROCESSING"), OutboxStatusProcessing)
	assert.Equal(t, OutboxEventStatus("PUBLISHED"), OutboxStatusPublished)
	assert.Equal(t, OutboxEventStatus("FAILED"), OutboxStatusFailed)
	assert.Equal(t, OutboxEventStatus("INVALID"), OutboxStatusInvalid)
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
	assert.Contains(t, err.Error(), "outbox event type")
}

func TestNewOutboxEvent_NilAggregateID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	event, err := NewOutboxEvent(ctx, "test.event", uuid.Nil, []byte(`{}`))

	assert.Error(t, err)
	assert.Nil(t, event)
	assert.Contains(t, err.Error(), "outbox event aggregate id")
}

func TestNewOutboxEvent_EmptyPayload(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	event, err := NewOutboxEvent(ctx, "test.event", uuid.New(), []byte{})

	assert.Error(t, err)
	assert.Nil(t, event)
	assert.Contains(t, err.Error(), "outbox event payload")
}

func TestNewOutboxEvent_NilPayload(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	event, err := NewOutboxEvent(ctx, "test.event", uuid.New(), nil)

	assert.Error(t, err)
	assert.Nil(t, event)
	assert.Contains(t, err.Error(), "outbox event payload")
}

func TestNewOutboxEvent_InvalidJSONPayload(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	event, err := NewOutboxEvent(ctx, "test.event", uuid.New(), []byte("not-json"))

	assert.ErrorIs(t, err, ErrOutboxEventPayloadInvalidJSON)
	assert.Nil(t, event)
}

func TestErrOutboxEventRequired(t *testing.T) {
	t.Parallel()

	assert.EqualError(t, ErrOutboxEventRequired, "outbox event is required")
}

func TestErrOutboxEventPayloadInvalidJSON(t *testing.T) {
	t.Parallel()

	assert.EqualError(t, ErrOutboxEventPayloadInvalidJSON, "outbox event payload must be valid JSON")
}
