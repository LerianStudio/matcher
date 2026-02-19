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

func TestPublishIngestionCompleted_NilEvent(t *testing.T) {
	t.Parallel()

	publisher := &EventPublisher{}

	err := publisher.PublishIngestionCompleted(context.Background(), nil)

	require.ErrorIs(t, err, errNilEvent)
}

func TestPublishIngestionFailed_NilEvent(t *testing.T) {
	t.Parallel()

	publisher := &EventPublisher{}

	err := publisher.PublishIngestionFailed(context.Background(), nil)

	require.ErrorIs(t, err, errNilEvent)
}

func TestPublish_NilPublisher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		publish func(t *testing.T, publisher *EventPublisher) error
	}{
		{
			name: "PublishIngestionCompleted",
			publish: func(t *testing.T, publisher *EventPublisher) error {
				t.Helper()

				job := &entities.IngestionJob{
					ID:        uuid.New(),
					ContextID: uuid.New(),
					SourceID:  uuid.New(),
					Metadata:  entities.JobMetadata{},
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

				return publisher.PublishIngestionCompleted(context.Background(), event)
			},
		},
		{
			name: "PublishIngestionFailed",
			publish: func(t *testing.T, publisher *EventPublisher) error {
				t.Helper()

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

				return publisher.PublishIngestionFailed(context.Background(), event)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var publisher *EventPublisher

			err := tt.publish(t, publisher)
			require.ErrorIs(t, err, errPublisherNotInit)
		})
	}
}

func TestPublishIngestionCompleted_UninitializedPublisher(t *testing.T) {
	t.Parallel()

	publisher := &EventPublisher{}

	job := &entities.IngestionJob{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Metadata:  entities.JobMetadata{},
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

func TestSentinelErrors(t *testing.T) {
	t.Parallel()

	require.EqualError(t, errRabbitMQChannelRequired, "rabbitmq channel is required")
	require.EqualError(t, errPublisherNotInit, "rabbitmq publisher not initialized")
	require.EqualError(t, errNilEvent, "event is required")
}

func TestRoutingKeyConstants(t *testing.T) {
	t.Parallel()

	require.Equal(t, "ingestion.completed", routingKeyIngestionCompleted)
	require.Equal(t, "ingestion.failed", routingKeyIngestionFailed)
}
