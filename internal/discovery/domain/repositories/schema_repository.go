package repositories

import (
	"context"
	"database/sql"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
)

// SchemaRepository defines persistence operations for DiscoveredSchema entities.
type SchemaRepository interface {
	// UpsertBatch creates or updates multiple DiscoveredSchema entries atomically.
	UpsertBatch(ctx context.Context, schemas []*entities.DiscoveredSchema) error
	// UpsertBatchWithTx creates or updates multiple schemas within an existing transaction.
	UpsertBatchWithTx(ctx context.Context, tx *sql.Tx, schemas []*entities.DiscoveredSchema) error
	// FindByConnectionID retrieves all schemas discovered for a given connection.
	FindByConnectionID(ctx context.Context, connectionID uuid.UUID) ([]*entities.DiscoveredSchema, error)
	// DeleteByConnectionID removes all schemas associated with a connection.
	DeleteByConnectionID(ctx context.Context, connectionID uuid.UUID) error
	// DeleteByConnectionIDWithTx removes schemas for a connection within an existing transaction.
	DeleteByConnectionIDWithTx(ctx context.Context, tx *sql.Tx, connectionID uuid.UUID) error
}
