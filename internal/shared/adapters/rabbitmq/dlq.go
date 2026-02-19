// Package rabbitmq provides shared RabbitMQ configuration and utilities.
package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// AMQPChannel defines the interface for AMQP channel operations needed for DLQ setup.
type AMQPChannel interface {
	ExchangeDeclare(
		name, kind string,
		durable, autoDelete, internal, noWait bool,
		args amqp.Table,
	) error
	QueueDeclare(
		name string,
		durable, autoDelete, exclusive, noWait bool,
		args amqp.Table,
	) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
}

// DeclareDLQTopology declares the Dead Letter Exchange and Queue for handling failed messages.
// This should be called during publisher/consumer initialization.
//
// Topology created:
// - DLX exchange (topic, durable): matcher.events.dlx
// - DLQ queue (durable): matcher.events.dlq
// - Binding: DLQ bound to DLX with routing key "#" (all messages).
func DeclareDLQTopology(ch AMQPChannel) error {
	if ch == nil {
		return fmt.Errorf("declare dlq topology: %w", ErrChannelRequired)
	}

	if err := ch.ExchangeDeclare(
		DLXExchangeName,
		ExchangeType,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare dlx exchange: %w", err)
	}

	if _, err := ch.QueueDeclare(
		DLQName,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare dlq queue: %w", err)
	}

	if err := ch.QueueBind(
		DLQName,
		"#",
		DLXExchangeName,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind dlq to dlx: %w", err)
	}

	return nil
}

// GetDLXArgs returns the AMQP table arguments needed to configure a queue
// to dead-letter rejected/expired messages to the DLX.
// Use these args when declaring consumer queues.
//
// Note: Only x-dead-letter-exchange is set here; x-dead-letter-routing-key is
// intentionally omitted. This means dead-lettered messages retain their original
// routing key (e.g., "ingestion.completed", "matching.match_confirmed"). The DLQ
// is bound to the DLX with the "#" (catch-all) pattern, so all dead-lettered
// messages arrive in a single queue regardless of their original routing key.
//
// Operators inspecting the DLQ should examine the x-death header on each message
// to determine the originating queue, exchange, and routing key. This header is
// automatically populated by RabbitMQ when a message is dead-lettered.
func GetDLXArgs() amqp.Table {
	return amqp.Table{
		"x-dead-letter-exchange": DLXExchangeName,
	}
}
