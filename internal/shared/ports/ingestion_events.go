package ports

import (
	"context"

	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
)

// IngestionEventPublisher publishes ingestion events to the message broker.
// This shared interface allows the outbox dispatcher to publish ingestion events
// without importing ingestion/ports directly, breaking the outbox->ingestion coupling.
type IngestionEventPublisher interface {
	// PublishIngestionCompleted publishes a completion event with idempotency.
	PublishIngestionCompleted(ctx context.Context, event *sharedDomain.IngestionCompletedEvent) error

	// PublishIngestionFailed publishes a failure event with idempotency.
	PublishIngestionFailed(ctx context.Context, event *sharedDomain.IngestionFailedEvent) error
}
