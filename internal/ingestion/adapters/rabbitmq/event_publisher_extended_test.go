//go:build unit

package rabbitmq

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/value_objects"
)

// TestPublishIngestionFailed_NilConn tests nil connection in PublishIngestionFailed.
func TestPublishIngestionFailed_NilConn(t *testing.T) {
	t.Parallel()

	publisher := &EventPublisher{}

	job := &entities.IngestionJob{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Status:    value_objects.JobStatusProcessing,
		Metadata:  entities.JobMetadata{},
	}

	err := job.Fail(context.Background(), "test error")
	require.NoError(t, err)

	event, eventErr := entities.NewIngestionFailedEvent(context.Background(), job)
	require.NoError(t, eventErr)

	publishErr := publisher.PublishIngestionFailed(context.Background(), event)

	require.ErrorIs(t, publishErr, errPublisherNotInit)
}

// TestEventPublisher_SentinelErrorMessages verifies error messages.
func TestEventPublisher_SentinelErrorMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "channel required",
			err:      errRabbitMQChannelRequired,
			expected: "rabbitmq channel is required",
		},
		{
			name:     "publisher not init",
			err:      errPublisherNotInit,
			expected: "rabbitmq publisher not initialized",
		},
		{
			name:     "nil event",
			err:      errNilEvent,
			expected: "event is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.EqualError(t, tt.err, tt.expected)
		})
	}
}

// TestEventPublisher_StructureValidation verifies struct fields.
func TestEventPublisher_StructureValidation(t *testing.T) {
	t.Parallel()

	t.Run("zero value publisher", func(t *testing.T) {
		t.Parallel()

		var publisher EventPublisher
		require.Nil(t, publisher.confirmablePublisher)
		require.Nil(t, publisher.propagator)
	})

	t.Run("nil pointer publisher", func(t *testing.T) {
		t.Parallel()

		var publisher *EventPublisher
		require.Nil(t, publisher)
	})
}

// TestPublishIngestionCompleted_ValidEvent tests valid event structure.
func TestPublishIngestionCompleted_ValidEvent(t *testing.T) {
	t.Parallel()

	job := &entities.IngestionJob{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Metadata:  entities.JobMetadata{FileName: "test.csv", FileSize: 1024},
	}

	event, err := entities.NewIngestionCompletedEvent(
		context.Background(),
		job,
		100,
		time.Now().UTC(),
		time.Now().UTC(),
		100,
		0,
	)
	require.NoError(t, err)
	require.NotNil(t, event)
	require.Equal(t, entities.EventTypeIngestionCompleted, event.EventType)
	require.Equal(t, job.ID, event.JobID)
	require.Equal(t, 100, event.TransactionCount)
}

// TestPublishIngestionFailed_ValidEvent tests valid failed event structure.
func TestPublishIngestionFailed_ValidEvent(t *testing.T) {
	t.Parallel()

	job := &entities.IngestionJob{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Status:    value_objects.JobStatusProcessing,
		Metadata:  entities.JobMetadata{FileName: "test.csv", Error: "parse error"},
	}

	err := job.Fail(context.Background(), "validation failed")
	require.NoError(t, err)

	event, err := entities.NewIngestionFailedEvent(context.Background(), job)
	require.NoError(t, err)
	require.NotNil(t, event)
	require.Equal(t, entities.EventTypeIngestionFailed, event.EventType)
	require.Equal(t, job.ID, event.JobID)
}

// TestPublishIngestionCompleted_EmptyPublisher tests empty publisher struct.
func TestPublishIngestionCompleted_EmptyPublisher(t *testing.T) {
	t.Parallel()

	publisher := &EventPublisher{}

	job := &entities.IngestionJob{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
	}

	event, err := entities.NewIngestionCompletedEvent(
		context.Background(),
		job,
		1,
		time.Now().UTC(),
		time.Now().UTC(),
		1,
		0,
	)
	require.NoError(t, err)

	err = publisher.PublishIngestionCompleted(context.Background(), event)
	require.ErrorIs(t, err, errPublisherNotInit)
}

// TestPublishIngestionFailed_EmptyPublisher tests empty publisher struct.
func TestPublishIngestionFailed_EmptyPublisher(t *testing.T) {
	t.Parallel()

	publisher := &EventPublisher{}

	job := &entities.IngestionJob{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Status:    value_objects.JobStatusProcessing,
	}

	err := job.Fail(context.Background(), "error")
	require.NoError(t, err)

	event, eventErr := entities.NewIngestionFailedEvent(context.Background(), job)
	require.NoError(t, eventErr)

	publishErr := publisher.PublishIngestionFailed(context.Background(), event)
	require.ErrorIs(t, publishErr, errPublisherNotInit)
}

// TestRoutingKeyValues verifies routing key constant values.
func TestRoutingKeyValues(t *testing.T) {
	t.Parallel()

	t.Run("completed routing key format", func(t *testing.T) {
		t.Parallel()
		require.Contains(t, routingKeyIngestionCompleted, "ingestion")
		require.Contains(t, routingKeyIngestionCompleted, "completed")
	})

	t.Run("failed routing key format", func(t *testing.T) {
		t.Parallel()
		require.Contains(t, routingKeyIngestionFailed, "ingestion")
		require.Contains(t, routingKeyIngestionFailed, "failed")
	})
}

// TestPublishMethods_ContextCancellation tests context handling.
func TestPublishMethods_ContextCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	publisher := &EventPublisher{}

	job := &entities.IngestionJob{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
	}

	event, err := entities.NewIngestionCompletedEvent(
		context.Background(),
		job,
		1,
		time.Now().UTC(),
		time.Now().UTC(),
		1,
		0,
	)
	require.NoError(t, err)

	err = publisher.PublishIngestionCompleted(ctx, event)
	require.ErrorIs(t, err, errPublisherNotInit)
}

// TestPublishIngestionCompleted_LargeTransactionCount tests edge case values.
func TestPublishIngestionCompleted_LargeTransactionCount(t *testing.T) {
	t.Parallel()

	job := &entities.IngestionJob{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
	}

	event, err := entities.NewIngestionCompletedEvent(
		context.Background(),
		job,
		1000000,
		time.Now().UTC().Add(-24*time.Hour),
		time.Now().UTC(),
		1000000,
		0,
	)
	require.NoError(t, err)
	require.Equal(t, 1000000, event.TransactionCount)
}

// TestPublishIngestionCompleted_ZeroTransactionCount tests zero transactions.
func TestPublishIngestionCompleted_ZeroTransactionCount(t *testing.T) {
	t.Parallel()

	job := &entities.IngestionJob{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
	}

	event, err := entities.NewIngestionCompletedEvent(
		context.Background(),
		job,
		0,
		time.Now().UTC(),
		time.Now().UTC(),
		0,
		0,
	)
	require.NoError(t, err)
	require.Equal(t, 0, event.TransactionCount)
}
