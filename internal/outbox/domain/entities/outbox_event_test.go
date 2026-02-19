//go:build unit

package entities

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestNewOutboxEvent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	payload := []byte("payload")
	event, err := NewOutboxEvent(ctx, "type", uuid.New(), payload)
	require.NoError(t, err)
	require.Equal(t, OutboxStatusPending, event.Status)
	require.Equal(t, 0, event.Attempts)
	require.NotEqual(t, uuid.Nil, event.ID)
	require.Equal(t, payload, event.Payload)
}

func TestNewOutboxEventValidation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, err := NewOutboxEvent(ctx, "", uuid.New(), []byte("x"))
	require.Error(t, err)
	_, err = NewOutboxEvent(ctx, "type", uuid.Nil, []byte("x"))
	require.Error(t, err)
	_, err = NewOutboxEvent(ctx, "type", uuid.New(), nil)
	require.Error(t, err)
}
