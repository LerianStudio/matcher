package repositories

import (
	"context"
	"database/sql"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
)

// ExtractionRepository defines persistence operations for ExtractionRequest entities.
type ExtractionRepository interface {
	// Create persists a new ExtractionRequest.
	Create(ctx context.Context, req *entities.ExtractionRequest) error
	// CreateWithTx persists a new ExtractionRequest within an existing transaction.
	CreateWithTx(ctx context.Context, tx *sql.Tx, req *entities.ExtractionRequest) error
	// Update persists changes to an existing ExtractionRequest.
	Update(ctx context.Context, req *entities.ExtractionRequest) error
	// UpdateWithTx persists changes within an existing transaction.
	UpdateWithTx(ctx context.Context, tx *sql.Tx, req *entities.ExtractionRequest) error
	// FindByID retrieves an ExtractionRequest by its internal ID.
	FindByID(ctx context.Context, id uuid.UUID) (*entities.ExtractionRequest, error)
	// FindByIngestionJobID retrieves the ExtractionRequest associated with an ingestion job.
	FindByIngestionJobID(ctx context.Context, jobID uuid.UUID) (*entities.ExtractionRequest, error)
}
