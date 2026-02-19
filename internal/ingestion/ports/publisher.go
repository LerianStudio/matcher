package ports

import (
	"context"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
)

// EventPublisher publishes ingestion events to message broker.
type EventPublisher interface {
	// PublishIngestionCompleted publishes completion event with idempotency
	PublishIngestionCompleted(ctx context.Context, event *entities.IngestionCompletedEvent) error

	// PublishIngestionFailed publishes failure event with idempotency
	PublishIngestionFailed(ctx context.Context, event *entities.IngestionFailedEvent) error
}
