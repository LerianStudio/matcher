//go:build integration

package ingestion

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"
	libRabbitmq "github.com/LerianStudio/lib-uncommons/v2/uncommons/rabbitmq"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"

	ingestionRabbitmq "github.com/LerianStudio/matcher/internal/ingestion/adapters/rabbitmq"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	sharedRabbitmq "github.com/LerianStudio/matcher/internal/shared/adapters/rabbitmq"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestEventPublisherPublishesMessage(t *testing.T) {
	integration.RunWithHarness(t, func(t *testing.T, harness *integration.TestHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		conn := &libRabbitmq.RabbitMQConnection{
			ConnectionStringSource:   "amqp://guest:guest@" + harness.RabbitMQHost + ":" + harness.RabbitMQPort + "/",
			HealthCheckURL:           harness.RabbitMQHealthURL,
			User:                     "guest",
			Pass:                     "guest",
			Logger:                   &libLog.NopLogger{},
			AllowInsecureHealthCheck: true,
		}
		require.NoError(t, conn.Connect())
		t.Cleanup(func() {
			if conn.Channel != nil {
				_ = conn.Channel.Close()
			}
			if conn.Connection != nil {
				_ = conn.Connection.Close()
			}
		})

		// Each publisher must own a dedicated channel (AMQP confirms are channel-scoped).
		pubCh, err := conn.Connection.Channel()
		require.NoError(t, err)
		t.Cleanup(func() { _ = pubCh.Close() })

		publisher, err := ingestionRabbitmq.NewEventPublisherFromChannel(pubCh)
		require.NoError(t, err)

		_, err = conn.Channel.QueueDeclare(
			"test.ingestion",
			true,
			false,
			false,
			false,
			nil,
		)
		require.NoError(t, err)
		require.NoError(
			t,
			conn.Channel.QueueBind(
				"test.ingestion",
				"ingestion.*",
				sharedRabbitmq.ExchangeName,
				false,
				nil,
			),
		)

		job := &entities.IngestionJob{
			ID:        uuid.New(),
			ContextID: uuid.New(),
			SourceID:  uuid.New(),
			Metadata:  entities.JobMetadata{},
		}
		event, err := entities.NewIngestionCompletedEvent(
			ctx,
			job,
			1,
			time.Now().UTC(),
			time.Now().UTC(),
			1,
			0,
		)
		require.NoError(t, err)

		require.NoError(t, publisher.PublishIngestionCompleted(ctx, event))

		var delivery amqp.Delivery
		var ok bool
		require.Eventually(t, func() bool {
			delivery, ok, err = conn.Channel.Get("test.ingestion", true)
			return err == nil && ok
		}, 5*time.Second, 100*time.Millisecond, "message not received")

		require.Equal(t, "application/json", delivery.ContentType)
		require.NotEmpty(t, delivery.Body)

		idempotencyKey, ok := delivery.Headers["idempotency_key"].(string)
		require.True(t, ok)
		require.NotEmpty(t, idempotencyKey)

		var receivedEvent entities.IngestionCompletedEvent
		require.NoError(t, json.Unmarshal(delivery.Body, &receivedEvent))
		require.Equal(t, job.ID, receivedEvent.JobID)
		require.Equal(t, entities.EventTypeIngestionCompleted, receivedEvent.EventType)
	})
}
