// Package rabbitmq provides shared RabbitMQ configuration and utilities.
package rabbitmq

// Known routing keys used across the matcher service:
//
//   - "ingestion.completed"           — Published by ingestion context when a file/batch
//     is successfully parsed and normalized. Triggers downstream matching.
//     (see internal/ingestion/adapters/rabbitmq/event_publisher.go)
//
//   - "ingestion.failed"              — Published by ingestion context when a file/batch
//     fails parsing or validation. Used for alerting and retry workflows.
//     (see internal/ingestion/adapters/rabbitmq/event_publisher.go)
//
//   - "matching.match_confirmed"      — Published by matching context when transactions
//     are successfully matched/reconciled. Consumed by external services
//     (e.g., Midaz ledger, settlement, webhooks).
//     Defined as shared/domain.EventTypeMatchConfirmed.
//     (see internal/matching/adapters/rabbitmq/event_publisher.go)
//
//   - "governance.audit_log_created"  — Published when an immutable audit log entry is
//     created. Defined as shared/domain.EventTypeAuditLogCreated.
//     (see internal/shared/domain/events.go)
//
// All routing keys follow the "<context>.<event>" convention and are published to
// the ExchangeName topic exchange. The DLX catch-all binding ("#") ensures that
// dead-lettered messages from any routing key land in the DLQ.

const (
	// ExchangeName is the main event exchange.
	ExchangeName = "matcher.events"
	// ExchangeType is the exchange type (topic for routing flexibility).
	ExchangeType = "topic"

	// DLXExchangeName is the dead letter exchange for failed messages.
	DLXExchangeName = "matcher.events.dlx"
	// DLQName is the dead letter queue for failed messages.
	DLQName = "matcher.events.dlq"
)
