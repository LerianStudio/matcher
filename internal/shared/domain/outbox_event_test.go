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

	assert.Equal(t, "PENDING", OutboxStatusPending)
	assert.Equal(t, "PROCESSING", OutboxStatusProcessing)
	assert.Equal(t, "PUBLISHED", OutboxStatusPublished)
	assert.Equal(t, "FAILED", OutboxStatusFailed)
	assert.Equal(t, "INVALID", OutboxStatusInvalid)
}

func TestOutboxEventStatus_IsValid(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		value string
		valid bool
	}{
		{name: "empty string", value: "", valid: false},
		{name: "lowercase pending", value: "pending", valid: false},
		{name: "canonical pending", value: OutboxStatusPending, valid: true},
		{name: "canonical processing", value: OutboxStatusProcessing, valid: true},
		{name: "canonical published", value: OutboxStatusPublished, valid: true},
		{name: "canonical failed", value: OutboxStatusFailed, valid: true},
		{name: "canonical invalid", value: OutboxStatusInvalid, valid: true},
		{name: "arbitrary string", value: "RANDOM_GARBAGE", valid: false},
		{name: "trailing space", value: "PENDING ", valid: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.valid, OutboxEventStatus(tc.value).IsValid())
		})
	}
}

func TestParseOutboxEventStatus(t *testing.T) {
	t.Parallel()

	status, err := ParseOutboxEventStatus(OutboxStatusPublished)
	require.NoError(t, err)
	assert.Equal(t, OutboxEventStatus(OutboxStatusPublished), status)

	_, err = ParseOutboxEventStatus("BROKEN")
	require.Error(t, err)
}

func TestValidateOutboxTransition(t *testing.T) {
	t.Parallel()

	require.NoError(t, ValidateOutboxTransition(OutboxStatusPending, OutboxStatusProcessing))
	require.Error(t, ValidateOutboxTransition(OutboxStatusPublished, OutboxStatusFailed))
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
	status, statusErr := OutboxStatusOf(event)
	require.NoError(t, statusErr)
	assert.Equal(t, OutboxEventStatus(OutboxStatusPending), status)
	assert.Equal(t, 0, event.Attempts)
	assert.Nil(t, event.PublishedAt)
	assert.Empty(t, event.LastError)
	assert.False(t, event.CreatedAt.IsZero())
	assert.False(t, event.UpdatedAt.IsZero())
}

func TestOutboxStatusOf_NilEvent(t *testing.T) {
	t.Parallel()

	_, err := OutboxStatusOf(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrOutboxEventRequired)
}

func TestOutboxStatusOf_InvalidStatus(t *testing.T) {
	t.Parallel()

	event := &OutboxEvent{Status: "BROKEN"}
	_, err := OutboxStatusOf(event)
	require.Error(t, err)
}

func TestNewOutboxEvent_EmptyEventType(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	event, err := NewOutboxEvent(ctx, "", uuid.New(), []byte(`{}`))

	require.Error(t, err)
	assert.Nil(t, event)
	assert.Contains(t, err.Error(), "outbox event type")
}

func TestNewOutboxEvent_NilAggregateID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	event, err := NewOutboxEvent(ctx, "test.event", uuid.Nil, []byte(`{}`))

	require.Error(t, err)
	assert.Nil(t, event)
	assert.Contains(t, err.Error(), "aggregate id")
}

func TestNewOutboxEvent_EmptyPayload(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	event, err := NewOutboxEvent(ctx, "test.event", uuid.New(), []byte{})

	require.Error(t, err)
	assert.Nil(t, event)
	assert.Contains(t, err.Error(), "payload")
}

func TestNewOutboxEvent_NilPayload(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	event, err := NewOutboxEvent(ctx, "test.event", uuid.New(), nil)

	require.Error(t, err)
	assert.Nil(t, event)
	assert.Contains(t, err.Error(), "payload")
}

func TestNewOutboxEvent_InvalidJSONPayload(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	event, err := NewOutboxEvent(ctx, "test.event", uuid.New(), []byte("not-json"))

	require.Error(t, err)
	assert.Nil(t, event)
	assert.ErrorIs(t, err, ErrOutboxEventPayloadInvalidJSON)
}

func TestErrOutboxEventRequired(t *testing.T) {
	t.Parallel()

	assert.EqualError(t, ErrOutboxEventRequired, "outbox event is required")
}

func TestErrOutboxEventPayloadInvalidJSON(t *testing.T) {
	t.Parallel()

	assert.EqualError(t, ErrOutboxEventPayloadInvalidJSON, "outbox event payload must be valid JSON (stored as JSONB)")
}
