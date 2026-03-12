package repositories

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Domain-level sentinel errors for extraction repository operations.
var (
	ErrExtractionNotFound = errors.New("extraction request not found")
	ErrExtractionConflict = errors.New("extraction request changed concurrently")
)

// ExtractionRepository defines persistence operations for ExtractionRequest entities.
type ExtractionRepository interface {
	// Create persists a new ExtractionRequest.
	Create(ctx context.Context, req *entities.ExtractionRequest) error
	// CreateWithTx persists a new ExtractionRequest within an existing transaction.
	CreateWithTx(ctx context.Context, tx sharedPorts.Tx, req *entities.ExtractionRequest) error
	// Update persists changes to an existing ExtractionRequest.
	Update(ctx context.Context, req *entities.ExtractionRequest) error
	// UpdateIfUnchanged persists changes only if the stored row still has the
	// expected updated_at value, preventing stale writers from overwriting newer state.
	UpdateIfUnchanged(ctx context.Context, req *entities.ExtractionRequest, expectedUpdatedAt time.Time) error
	// UpdateIfUnchangedWithTx persists changes conditionally within an existing
	// transaction, preventing stale writers from overwriting newer state.
	UpdateIfUnchangedWithTx(ctx context.Context, tx sharedPorts.Tx, req *entities.ExtractionRequest, expectedUpdatedAt time.Time) error
	// UpdateWithTx persists changes within an existing transaction.
	UpdateWithTx(ctx context.Context, tx sharedPorts.Tx, req *entities.ExtractionRequest) error
	// FindByID retrieves an ExtractionRequest by its internal ID.
	FindByID(ctx context.Context, id uuid.UUID) (*entities.ExtractionRequest, error)
}
