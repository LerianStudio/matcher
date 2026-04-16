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
	// LinkIfUnlinked atomically sets ingestion_job_id on the extraction row
	// identified by id, but only when the existing ingestion_job_id is NULL.
	// Returns sharedPorts.ErrExtractionAlreadyLinked when the row is already
	// linked; returns ErrExtractionNotFound when no row matches the id.
	//
	// Implementations MUST use a single atomic SQL UPDATE with the
	// ingestion_job_id IS NULL predicate so concurrent callers cannot both
	// succeed. See internal/shared/adapters/cross/fetcher_bridge_adapters.go
	// for the canonical consumer.
	LinkIfUnlinked(ctx context.Context, id, ingestionJobID uuid.UUID) error
	// FindEligibleForBridge returns up to limit completed extractions that
	// have no ingestion_job_id yet. Results are ordered by updated_at (oldest
	// first) to drain the backlog fairly. The query runs inside a
	// tenant-scoped transaction so it only sees rows for the tenant resolved
	// from ctx (plus the default tenant when auth is disabled).
	FindEligibleForBridge(ctx context.Context, limit int) ([]*entities.ExtractionRequest, error)
}
