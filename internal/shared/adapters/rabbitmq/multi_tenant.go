package rabbitmq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQMultiTenantManager provides per-tenant RabbitMQ channels via vhost isolation.
// In multi-tenant mode, each tenant has its own RabbitMQ vhost and this interface
// provides access to tenant-specific channels.
type RabbitMQMultiTenantManager interface {
	// GetChannel returns an AMQP channel for the specified tenant's vhost.
	// The caller should NOT close the channel; it is managed by the pool.
	GetChannel(ctx context.Context, tenantID string) (*amqp.Channel, error)
}
