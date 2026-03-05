// Package ports defines shared interfaces (ports) for infrastructure abstraction.
package ports

import (
	"context"
	"database/sql"
	"time"

	"github.com/google/uuid"

	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
)

// OutboxRepository defines persistence operations for outbox events.
// This is the shared kernel interface used by all bounded contexts that interact
// with the outbox pattern, avoiding direct imports into outbox/domain/repositories.
type OutboxRepository interface {
	Create(ctx context.Context, event *sharedDomain.OutboxEvent) (*sharedDomain.OutboxEvent, error)
	CreateWithTx(
		ctx context.Context,
		tx *sql.Tx,
		event *sharedDomain.OutboxEvent,
	) (*sharedDomain.OutboxEvent, error)
	ListPending(ctx context.Context, limit int) ([]*sharedDomain.OutboxEvent, error)
	ListPendingByType(ctx context.Context, eventType string, limit int) ([]*sharedDomain.OutboxEvent, error)
	ListTenants(ctx context.Context) ([]string, error)
	GetByID(ctx context.Context, id uuid.UUID) (*sharedDomain.OutboxEvent, error)
	MarkPublished(ctx context.Context, id uuid.UUID, publishedAt time.Time) error
	MarkFailed(ctx context.Context, id uuid.UUID, errMsg string, maxAttempts int) error
	ListFailedForRetry(
		ctx context.Context,
		limit int,
		failedBefore time.Time,
		maxAttempts int,
	) ([]*sharedDomain.OutboxEvent, error)
	ResetForRetry(
		ctx context.Context,
		limit int,
		failedBefore time.Time,
		maxAttempts int,
	) ([]*sharedDomain.OutboxEvent, error)
	ResetStuckProcessing(
		ctx context.Context,
		limit int,
		processingBefore time.Time,
		maxAttempts int,
	) ([]*sharedDomain.OutboxEvent, error)
	MarkInvalid(ctx context.Context, id uuid.UUID, errMsg string) error
}
