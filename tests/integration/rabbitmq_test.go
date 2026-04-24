//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libRabbitmq "github.com/LerianStudio/lib-commons/v5/commons/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_Flow_RabbitMQ_ConnectionEstablishment(t *testing.T) {
	RunWithHarness(t, func(t *testing.T, h *TestHarness) {
		conn := createRabbitMQConnectionFromHarness(t, h)

		err := conn.EnsureChannel()
		require.NoError(t, err, "EnsureChannel should succeed")

		require.NotNil(t, conn.Connection, "RabbitMQ connection should be established")
		require.NotNil(t, conn.Channel, "RabbitMQ channel should be established")

		t.Cleanup(func() {
			cleanupRabbitMQConnection(conn)
		})
	})
}

func TestIntegration_Flow_RabbitMQ_ChannelManagement(t *testing.T) {
	RunWithHarness(t, func(t *testing.T, h *TestHarness) {
		conn := createRabbitMQConnectionFromHarness(t, h)

		err := conn.EnsureChannel()
		require.NoError(t, err, "first EnsureChannel should succeed")

		firstChannel := conn.Channel

		err = conn.EnsureChannel()
		require.NoError(t, err, "second EnsureChannel should succeed")

		assert.Equal(t, firstChannel, conn.Channel, "EnsureChannel should reuse existing channel")

		t.Cleanup(func() {
			cleanupRabbitMQConnection(conn)
		})
	})
}

func TestIntegration_Flow_RabbitMQ_ExchangeDeclare(t *testing.T) {
	RunWithHarness(t, func(t *testing.T, h *TestHarness) {
		conn := createRabbitMQConnectionFromHarness(t, h)

		err := conn.EnsureChannel()
		require.NoError(t, err)

		exchangeName := fmt.Sprintf("test.exchange.%d", time.Now().UnixNano())

		err = conn.Channel.ExchangeDeclare(
			exchangeName,
			"topic",
			true,
			false,
			false,
			false,
			nil,
		)
		require.NoError(t, err, "exchange declaration should succeed")

		t.Cleanup(func() {
			if conn.Channel != nil {
				_ = conn.Channel.ExchangeDelete(exchangeName, false, false)
			}
			cleanupRabbitMQConnection(conn)
		})
	})
}

func TestIntegration_Flow_RabbitMQ_QueueDeclareAndBind(t *testing.T) {
	RunWithHarness(t, func(t *testing.T, h *TestHarness) {
		conn := createRabbitMQConnectionFromHarness(t, h)

		err := conn.EnsureChannel()
		require.NoError(t, err)

		exchangeName := fmt.Sprintf("test.exchange.%d", time.Now().UnixNano())
		queueName := fmt.Sprintf("test.queue.%d", time.Now().UnixNano())
		routingKey := "test.routing.key"

		err = conn.Channel.ExchangeDeclare(exchangeName, "topic", true, false, false, false, nil)
		require.NoError(t, err)

		q, err := conn.Channel.QueueDeclare(queueName, true, false, false, false, nil)
		require.NoError(t, err, "queue declaration should succeed")
		assert.Equal(t, queueName, q.Name)

		err = conn.Channel.QueueBind(queueName, routingKey, exchangeName, false, nil)
		require.NoError(t, err, "queue binding should succeed")

		t.Cleanup(func() {
			if conn.Channel != nil {
				_, _ = conn.Channel.QueueDelete(queueName, false, false, false)
				_ = conn.Channel.ExchangeDelete(exchangeName, false, false)
			}
			cleanupRabbitMQConnection(conn)
		})
	})
}

func TestIntegration_Flow_RabbitMQ_PublishMessage(t *testing.T) {
	RunWithHarness(t, func(t *testing.T, h *TestHarness) {
		conn := createRabbitMQConnectionFromHarness(t, h)

		err := conn.EnsureChannel()
		require.NoError(t, err)

		exchangeName := fmt.Sprintf("test.exchange.%d", time.Now().UnixNano())
		routingKey := "test.publish.key"

		err = conn.Channel.ExchangeDeclare(exchangeName, "topic", true, false, false, false, nil)
		require.NoError(t, err)

		testMessage := map[string]string{
			"test_key": "test_value",
			"id":       "12345",
		}
		body, err := json.Marshal(testMessage)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err = conn.Channel.PublishWithContext(
			ctx,
			exchangeName,
			routingKey,
			false,
			false,
			amqp.Publishing{
				ContentType:  "application/json",
				Body:         body,
				DeliveryMode: amqp.Persistent,
				MessageId:    "test-message-id",
			},
		)
		require.NoError(t, err, "message publishing should succeed")

		t.Cleanup(func() {
			if conn.Channel != nil {
				_ = conn.Channel.ExchangeDelete(exchangeName, false, false)
			}
			cleanupRabbitMQConnection(conn)
		})
	})
}

func TestIntegration_Flow_RabbitMQ_ConsumeMessage(t *testing.T) {
	RunWithHarness(t, func(t *testing.T, h *TestHarness) {
		conn := createRabbitMQConnectionFromHarness(t, h)

		err := conn.EnsureChannel()
		require.NoError(t, err)

		exchangeName := fmt.Sprintf("test.exchange.%d", time.Now().UnixNano())
		queueName := fmt.Sprintf("test.queue.%d", time.Now().UnixNano())
		routingKey := "test.consume.key"

		err = conn.Channel.ExchangeDeclare(exchangeName, "topic", true, false, false, false, nil)
		require.NoError(t, err)

		_, err = conn.Channel.QueueDeclare(queueName, true, false, false, false, nil)
		require.NoError(t, err)

		err = conn.Channel.QueueBind(queueName, routingKey, exchangeName, false, nil)
		require.NoError(t, err)

		deliveries, err := conn.Channel.Consume(
			queueName,
			"",
			false,
			false,
			false,
			false,
			nil,
		)
		require.NoError(t, err, "consume should succeed")

		testMessage := map[string]string{"message": "hello world"}
		body, err := json.Marshal(testMessage)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err = conn.Channel.PublishWithContext(
			ctx,
			exchangeName,
			routingKey,
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        body,
				MessageId:   "test-consume-message-id",
			},
		)
		require.NoError(t, err)

		select {
		case msg := <-deliveries:
			assert.Equal(t, body, msg.Body, "received message body should match")
			assert.Equal(t, "application/json", msg.ContentType)

			err = msg.Ack(false)
			require.NoError(t, err, "message acknowledgment should succeed")
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for message")
		}

		t.Cleanup(func() {
			if conn.Channel != nil {
				_, _ = conn.Channel.QueueDelete(queueName, false, false, false)
				_ = conn.Channel.ExchangeDelete(exchangeName, false, false)
			}
			cleanupRabbitMQConnection(conn)
		})
	})
}

func TestIntegration_Flow_RabbitMQ_MessageHeaders(t *testing.T) {
	RunWithHarness(t, func(t *testing.T, h *TestHarness) {
		conn := createRabbitMQConnectionFromHarness(t, h)

		err := conn.EnsureChannel()
		require.NoError(t, err)

		exchangeName := fmt.Sprintf("test.exchange.%d", time.Now().UnixNano())
		queueName := fmt.Sprintf("test.queue.%d", time.Now().UnixNano())
		routingKey := "test.headers.key"

		err = conn.Channel.ExchangeDeclare(exchangeName, "topic", true, false, false, false, nil)
		require.NoError(t, err)

		_, err = conn.Channel.QueueDeclare(queueName, true, false, false, false, nil)
		require.NoError(t, err)

		err = conn.Channel.QueueBind(queueName, routingKey, exchangeName, false, nil)
		require.NoError(t, err)

		deliveries, err := conn.Channel.Consume(queueName, "", false, false, false, false, nil)
		require.NoError(t, err)

		headers := amqp.Table{
			"idempotency_key": "test-idempotency-key-12345",
			"trace_id":        "trace-abc-123",
			"span_id":         "span-xyz-456",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err = conn.Channel.PublishWithContext(
			ctx,
			exchangeName,
			routingKey,
			false,
			false,
			amqp.Publishing{
				ContentType:  "application/json",
				Body:         []byte(`{"test": true}`),
				Headers:      headers,
				DeliveryMode: amqp.Persistent,
				MessageId:    "headers-test-message",
			},
		)
		require.NoError(t, err)

		select {
		case msg := <-deliveries:
			assert.Equal(t, "test-idempotency-key-12345", msg.Headers["idempotency_key"])
			assert.Equal(t, "trace-abc-123", msg.Headers["trace_id"])
			assert.Equal(t, "span-xyz-456", msg.Headers["span_id"])

			err = msg.Ack(false)
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for message with headers")
		}

		t.Cleanup(func() {
			if conn.Channel != nil {
				_, _ = conn.Channel.QueueDelete(queueName, false, false, false)
				_ = conn.Channel.ExchangeDelete(exchangeName, false, false)
			}
			cleanupRabbitMQConnection(conn)
		})
	})
}

func TestIntegration_Flow_RabbitMQ_MultipleConsumers(t *testing.T) {
	RunWithHarness(t, func(t *testing.T, h *TestHarness) {
		conn := createRabbitMQConnectionFromHarness(t, h)

		err := conn.EnsureChannel()
		require.NoError(t, err)

		exchangeName := fmt.Sprintf("test.exchange.%d", time.Now().UnixNano())
		queueName := fmt.Sprintf("test.queue.%d", time.Now().UnixNano())
		routingKey := "test.multi.key"

		err = conn.Channel.ExchangeDeclare(exchangeName, "topic", true, false, false, false, nil)
		require.NoError(t, err)

		_, err = conn.Channel.QueueDeclare(queueName, true, false, false, false, nil)
		require.NoError(t, err)

		err = conn.Channel.QueueBind(queueName, routingKey, exchangeName, false, nil)
		require.NoError(t, err)

		deliveries1, err := conn.Channel.Consume(
			queueName,
			"consumer-1",
			false,
			false,
			false,
			false,
			nil,
		)
		require.NoError(t, err)

		deliveries2, err := conn.Channel.Consume(
			queueName,
			"consumer-2",
			false,
			false,
			false,
			false,
			nil,
		)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		const messageCount = 4
		for i := range messageCount {
			err = conn.Channel.PublishWithContext(
				ctx,
				exchangeName,
				routingKey,
				false,
				false,
				amqp.Publishing{
					ContentType: "application/json",
					Body:        []byte(fmt.Sprintf(`{"seq": %d}`, i)),
				},
			)
			require.NoError(t, err)
		}

		received := 0
		timeout := time.After(10 * time.Second)

		for received < messageCount {
			select {
			case msg := <-deliveries1:
				err = msg.Ack(false)
				require.NoError(t, err)
				received++
			case msg := <-deliveries2:
				err = msg.Ack(false)
				require.NoError(t, err)
				received++
			case <-timeout:
				t.Fatalf("timeout: received only %d/%d messages", received, messageCount)
			}
		}

		assert.Equal(t, messageCount, received, "all messages should be received")

		t.Cleanup(func() {
			if conn.Channel != nil {
				_, _ = conn.Channel.QueueDelete(queueName, false, false, false)
				_ = conn.Channel.ExchangeDelete(exchangeName, false, false)
			}
			cleanupRabbitMQConnection(conn)
		})
	})
}

func TestIntegration_Flow_RabbitMQ_MessageRequeue(t *testing.T) {
	RunWithHarness(t, func(t *testing.T, h *TestHarness) {
		conn := createRabbitMQConnectionFromHarness(t, h)

		err := conn.EnsureChannel()
		require.NoError(t, err)

		exchangeName := fmt.Sprintf("test.exchange.%d", time.Now().UnixNano())
		queueName := fmt.Sprintf("test.queue.%d", time.Now().UnixNano())
		routingKey := "test.requeue.key"

		err = conn.Channel.ExchangeDeclare(exchangeName, "topic", true, false, false, false, nil)
		require.NoError(t, err)

		_, err = conn.Channel.QueueDeclare(queueName, true, false, false, false, nil)
		require.NoError(t, err)

		err = conn.Channel.QueueBind(queueName, routingKey, exchangeName, false, nil)
		require.NoError(t, err)

		deliveries, err := conn.Channel.Consume(queueName, "", false, false, false, false, nil)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err = conn.Channel.PublishWithContext(
			ctx,
			exchangeName,
			routingKey,
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        []byte(`{"requeue": "test"}`),
			},
		)
		require.NoError(t, err)

		select {
		case msg := <-deliveries:
			err = msg.Nack(false, true)
			require.NoError(t, err, "Nack with requeue should succeed")
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for first delivery")
		}

		select {
		case msg := <-deliveries:
			assert.True(t, msg.Redelivered, "message should be marked as redelivered")
			err = msg.Ack(false)
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for requeued message")
		}

		t.Cleanup(func() {
			if conn.Channel != nil {
				_, _ = conn.Channel.QueueDelete(queueName, false, false, false)
				_ = conn.Channel.ExchangeDelete(exchangeName, false, false)
			}
			cleanupRabbitMQConnection(conn)
		})
	})
}

func TestIntegration_Flow_RabbitMQ_TopicRouting(t *testing.T) {
	RunWithHarness(t, func(t *testing.T, h *TestHarness) {
		conn := createRabbitMQConnectionFromHarness(t, h)

		err := conn.EnsureChannel()
		require.NoError(t, err)

		exchangeName := fmt.Sprintf("test.exchange.%d", time.Now().UnixNano())
		queueMatch := fmt.Sprintf("test.queue.match.%d", time.Now().UnixNano())
		queueWild := fmt.Sprintf("test.queue.wild.%d", time.Now().UnixNano())

		err = conn.Channel.ExchangeDeclare(exchangeName, "topic", true, false, false, false, nil)
		require.NoError(t, err)

		_, err = conn.Channel.QueueDeclare(queueMatch, true, false, false, false, nil)
		require.NoError(t, err)
		_, err = conn.Channel.QueueDeclare(queueWild, true, false, false, false, nil)
		require.NoError(t, err)

		err = conn.Channel.QueueBind(
			queueMatch,
			"matcher.events.completed",
			exchangeName,
			false,
			nil,
		)
		require.NoError(t, err)
		err = conn.Channel.QueueBind(queueWild, "matcher.events.*", exchangeName, false, nil)
		require.NoError(t, err)

		deliveriesMatch, err := conn.Channel.Consume(
			queueMatch,
			"",
			false,
			false,
			false,
			false,
			nil,
		)
		require.NoError(t, err)
		deliveriesWild, err := conn.Channel.Consume(queueWild, "", false, false, false, false, nil)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err = conn.Channel.PublishWithContext(
			ctx,
			exchangeName,
			"matcher.events.completed",
			false,
			false,
			amqp.Publishing{ContentType: "application/json", Body: []byte(`{"type": "completed"}`)},
		)
		require.NoError(t, err)

		matchReceived := false
		wildReceived := false
		timeout := time.After(5 * time.Second)

		for !matchReceived || !wildReceived {
			select {
			case msg := <-deliveriesMatch:
				matchReceived = true
				_ = msg.Ack(false)
			case msg := <-deliveriesWild:
				wildReceived = true
				_ = msg.Ack(false)
			case <-timeout:
				t.Fatalf("timeout: matchReceived=%v, wildReceived=%v", matchReceived, wildReceived)
			}
		}

		assert.True(t, matchReceived, "exact match queue should receive message")
		assert.True(t, wildReceived, "wildcard queue should receive message")

		t.Cleanup(func() {
			if conn.Channel != nil {
				_, _ = conn.Channel.QueueDelete(queueMatch, false, false, false)
				_, _ = conn.Channel.QueueDelete(queueWild, false, false, false)
				_ = conn.Channel.ExchangeDelete(exchangeName, false, false)
			}
			cleanupRabbitMQConnection(conn)
		})
	})
}

func TestIntegration_Flow_RabbitMQ_ConnectionReconnect(t *testing.T) {
	RunWithHarness(t, func(t *testing.T, h *TestHarness) {
		conn := createRabbitMQConnectionFromHarness(t, h)

		err := conn.EnsureChannel()
		require.NoError(t, err)

		if conn.Channel != nil {
			_ = conn.Channel.Close()
			conn.Channel = nil
		}

		err = conn.EnsureChannel()
		require.NoError(t, err, "EnsureChannel should reconnect after channel close")
		require.NotNil(t, conn.Channel, "new channel should be created")

		t.Cleanup(func() {
			cleanupRabbitMQConnection(conn)
		})
	})
}

func createRabbitMQConnectionFromHarness(
	t *testing.T,
	harness *TestHarness,
) *libRabbitmq.RabbitMQConnection {
	t.Helper()

	connStr := "amqp://guest:guest@" + harness.RabbitMQHost + ":" + harness.RabbitMQPort + "/"

	return &libRabbitmq.RabbitMQConnection{
		ConnectionStringSource:   connStr,
		HealthCheckURL:           harness.RabbitMQHealthURL,
		Host:                     harness.RabbitMQHost,
		Port:                     harness.RabbitMQPort,
		User:                     "guest",
		Pass:                     "guest",
		Logger:                   &libLog.NopLogger{},
		AllowInsecureHealthCheck: true,
	}
}

func cleanupRabbitMQConnection(conn *libRabbitmq.RabbitMQConnection) {
	if conn == nil {
		return
	}

	if conn.Channel != nil {
		_ = conn.Channel.Close()
	}

	if conn.Connection != nil {
		_ = conn.Connection.Close()
	}
}
