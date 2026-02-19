//go:build integration

package matching

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

	matchingRabbitmq "github.com/LerianStudio/matcher/internal/matching/adapters/rabbitmq"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	sharedRabbitmq "github.com/LerianStudio/matcher/internal/shared/adapters/rabbitmq"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestEventPublisherPublishesMatchConfirmed(t *testing.T) {
	integration.RunWithHarness(t, func(t *testing.T, harness *integration.TestHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		conn := &libRabbitmq.RabbitMQConnection{
			ConnectionStringSource: "amqp://guest:guest@" + harness.RabbitMQHost + ":" + harness.RabbitMQPort + "/",
			HealthCheckURL:         harness.RabbitMQHealthURL,
			User:                   "guest",
			Pass:                   "guest",
			Logger:                 &libLog.NopLogger{},
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

		publisher, err := matchingRabbitmq.NewEventPublisherFromChannel(pubCh)
		require.NoError(t, err)

		_, err = conn.Channel.QueueDeclare("test.matching", true, false, false, false, nil)
		require.NoError(t, err)
		require.NoError(
			t,
			conn.Channel.QueueBind(
				"test.matching",
				"matching.*",
				sharedRabbitmq.ExchangeName,
				false,
				nil,
			),
		)

		now := time.Now().UTC()
		event := &matchingEntities.MatchConfirmedEvent{
			EventType:      matchingEntities.EventTypeMatchConfirmed,
			TenantID:       uuid.New(),
			TenantSlug:     "default",
			ContextID:      uuid.New(),
			RunID:          uuid.New(),
			MatchID:        uuid.New(),
			RuleID:         uuid.New(),
			TransactionIDs: []uuid.UUID{uuid.New()},
			Confidence:     90,
			ConfirmedAt:    now,
			Timestamp:      now,
		}

		require.NoError(t, publisher.PublishMatchConfirmed(ctx, event))

		var delivery amqp.Delivery
		var ok bool
		require.Eventually(t, func() bool {
			delivery, ok, err = conn.Channel.Get("test.matching", true)
			return err == nil && ok
		}, 5*time.Second, 100*time.Millisecond, "message not received")

		require.Equal(t, "application/json", delivery.ContentType)
		require.NotEmpty(t, delivery.Body)

		idempotencyKey, ok := delivery.Headers["idempotency_key"].(string)
		require.True(t, ok)
		require.Equal(t, event.MatchID.String(), idempotencyKey)

		var received matchingEntities.MatchConfirmedEvent
		require.NoError(t, json.Unmarshal(delivery.Body, &received))
		require.Equal(t, matchingEntities.EventTypeMatchConfirmed, received.EventType)
		require.Equal(t, event.MatchID, received.MatchID)
	})
}
