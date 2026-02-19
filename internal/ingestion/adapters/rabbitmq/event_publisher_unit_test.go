//go:build unit

package rabbitmq

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/value_objects"
	sharedRabbitmq "github.com/LerianStudio/matcher/internal/shared/adapters/rabbitmq"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

type mockConfirmableChannel struct {
	mu          sync.Mutex
	confirms    chan amqp.Confirmation
	closeNotify chan *amqp.Error
}

func newMockConfirmableChannel() *mockConfirmableChannel {
	return &mockConfirmableChannel{closeNotify: make(chan *amqp.Error, 1)}
}

func (*mockConfirmableChannel) Confirm(bool) error {
	return nil
}

func (m *mockConfirmableChannel) NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.confirms = confirm

	return confirm
}

func (m *mockConfirmableChannel) NotifyClose(chan *amqp.Error) chan *amqp.Error {
	return m.closeNotify
}

func (*mockConfirmableChannel) PublishWithContext(context.Context, string, string, bool, bool, amqp.Publishing) error {
	return nil
}

func (m *mockConfirmableChannel) Close() error {
	return nil
}

func TestPublish_NilPublisherPointer(t *testing.T) {
	t.Parallel()

	var publisher *EventPublisher

	err := publisher.publish(context.Background(), "test.key", uuid.New(), map[string]string{"test": "data"})

	require.ErrorIs(t, err, errPublisherNotInit)
}

func TestPublish_NilConfirmablePublisher(t *testing.T) {
	t.Parallel()

	publisher := &EventPublisher{
		confirmablePublisher: nil,
	}

	err := publisher.publish(context.Background(), "test.key", uuid.New(), map[string]string{"test": "data"})

	require.ErrorIs(t, err, errPublisherNotInit)
}

func TestPublishIngestionCompleted_NilEvent_WithNilPublisher(t *testing.T) {
	t.Parallel()

	var publisher *EventPublisher

	err := publisher.PublishIngestionCompleted(context.Background(), nil)
	require.ErrorIs(t, err, errNilEvent)
}

func TestPublishIngestionFailed_NilEvent_WithNilPublisher(t *testing.T) {
	t.Parallel()

	var publisher *EventPublisher

	err := publisher.PublishIngestionFailed(context.Background(), nil)
	require.ErrorIs(t, err, errNilEvent)
}

func TestPublishIngestionCompleted_WithVariousMetadata(t *testing.T) {
	t.Parallel()

	publisher := &EventPublisher{}

	tests := []struct {
		name     string
		metadata entities.JobMetadata
	}{
		{
			name:     "empty metadata",
			metadata: entities.JobMetadata{},
		},
		{
			name:     "with file info",
			metadata: entities.JobMetadata{FileName: "transactions.csv", FileSize: 1024},
		},
		{
			name:     "with error",
			metadata: entities.JobMetadata{Error: "parse error"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			job := &entities.IngestionJob{
				ID:        testutil.DeterministicUUID("job-id"),
				ContextID: testutil.DeterministicUUID("context-id"),
				SourceID:  testutil.DeterministicUUID("source-id"),
				Metadata:  tt.metadata,
			}

			fixedTime := testutil.FixedTime()

			event, err := entities.NewIngestionCompletedEvent(
				context.Background(),
				job,
				1,
				fixedTime,
				fixedTime,
				1,
				0,
			)
			require.NoError(t, err)

			err = publisher.PublishIngestionCompleted(context.Background(), event)

			// Should fail because publisher has no confirmable publisher
			require.ErrorIs(t, err, errPublisherNotInit)
		})
	}
}

func TestPublishIngestionFailed_WithVariousErrors(t *testing.T) {
	t.Parallel()

	publisher := &EventPublisher{}

	tests := []struct {
		name     string
		errorMsg string
	}{
		{"short error", "fail"},
		{"long error", "a very detailed error message explaining what happened during ingestion processing"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			job := &entities.IngestionJob{
				ID:        uuid.New(),
				ContextID: uuid.New(),
				SourceID:  uuid.New(),
				Status:    value_objects.JobStatusProcessing,
			}

			err := job.Fail(context.Background(), tt.errorMsg)
			require.NoError(t, err)

			event, eventErr := entities.NewIngestionFailedEvent(context.Background(), job)
			require.NoError(t, eventErr)

			publishErr := publisher.PublishIngestionFailed(context.Background(), event)

			require.ErrorIs(t, publishErr, errPublisherNotInit)
		})
	}

	t.Run("empty error", func(t *testing.T) {
		t.Parallel()

		job := &entities.IngestionJob{
			ID:        uuid.New(),
			ContextID: uuid.New(),
			SourceID:  uuid.New(),
			Status:    value_objects.JobStatusProcessing,
		}

		err := job.Fail(context.Background(), "")
		require.Error(t, err, "empty error message should be rejected by domain validation")
	})
}

func TestNewEventPublisherFromChannel_NilChannel(t *testing.T) {
	t.Parallel()

	publisher, err := NewEventPublisherFromChannel(nil)

	require.Nil(t, publisher)
	require.ErrorIs(t, err, errRabbitMQChannelRequired)
}

func TestEventPublisher_ConfirmableSetupFailedError(t *testing.T) {
	t.Parallel()

	assert.EqualError(t, errConfirmableSetupFailed, "failed to setup confirmable publisher")
}

func TestEventPublisher_Close_NilSafe(t *testing.T) {
	t.Parallel()

	var nilPublisher *EventPublisher
	require.NoError(t, nilPublisher.Close())

	publisher := &EventPublisher{}
	require.NoError(t, publisher.Close())
}

func TestEventPublisher_Close_ClosesConfirmablePublisher(t *testing.T) {
	t.Parallel()

	ch := newMockConfirmableChannel()
	confirmablePublisher, err := sharedRabbitmq.NewConfirmablePublisherFromChannel(ch)
	require.NoError(t, err)

	publisher := &EventPublisher{confirmablePublisher: confirmablePublisher, propagator: otel.GetTextMapPropagator()}

	require.NoError(t, publisher.Close())

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
	require.ErrorIs(t, err, sharedRabbitmq.ErrPublisherClosed)
}
