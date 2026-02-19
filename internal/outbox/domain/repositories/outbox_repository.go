// Package repositories provides outbox persistence contracts.
package repositories

//go:generate mockgen -destination=mocks/outbox_repository_mock.go -package=mocks . OutboxRepository

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/outbox/domain/entities"
)

// OutboxRepository defines persistence operations for outbox events.
type OutboxRepository interface {
	Create(ctx context.Context, event *entities.OutboxEvent) (*entities.OutboxEvent, error)
	CreateWithTx(
		ctx context.Context,
		tx Tx,
		event *entities.OutboxEvent,
	) (*entities.OutboxEvent, error)
	ListPending(ctx context.Context, limit int) ([]*entities.OutboxEvent, error)
	ListPendingByType(ctx context.Context, eventType string, limit int) ([]*entities.OutboxEvent, error)
	ListTenants(ctx context.Context) ([]string, error)
	GetByID(ctx context.Context, id uuid.UUID) (*entities.OutboxEvent, error)
	MarkPublished(ctx context.Context, id uuid.UUID, publishedAt time.Time) error
	MarkFailed(ctx context.Context, id uuid.UUID, errMsg string, maxAttempts int) error
	ListFailedForRetry(
		ctx context.Context,
		limit int,
		failedBefore time.Time,
		maxAttempts int,
	) ([]*entities.OutboxEvent, error)
	ResetForRetry(
		ctx context.Context,
		limit int,
		failedBefore time.Time,
		maxAttempts int,
	) ([]*entities.OutboxEvent, error)
	ResetStuckProcessing(
		ctx context.Context,
		limit int,
		processingBefore time.Time,
		maxAttempts int,
	) ([]*entities.OutboxEvent, error)
	MarkInvalid(ctx context.Context, id uuid.UUID, errMsg string) error
}
