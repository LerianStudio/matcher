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
	// CountBridgeReadiness returns aggregate counts of extractions partitioned
	// by bridge readiness state for the tenant resolved from ctx.
	//
	// staleThreshold partitions COMPLETE+unlinked rows into "pending" (created
	// recently, worker is expected to drain) versus "stale" (created longer
	// ago, operator should investigate). Implementations evaluate the
	// threshold against NOW() - created_at, not against updated_at, so a
	// stuck-but-recently-retried row still counts as stale.
	CountBridgeReadiness(ctx context.Context, staleThreshold time.Duration) (BridgeReadinessCounts, error)
	// ListBridgeCandidates returns extractions in the requested readiness
	// state ordered by created_at ASC (oldest first), supporting cursor
	// pagination via the opaque createdAfter timestamp + idAfter UUID. When
	// createdAfter is zero, paging starts from the beginning. limit is
	// clamped to a sensible maximum by the implementation.
	//
	// staleThreshold partitions COMPLETE+unlinked rows the same way
	// CountBridgeReadiness does so call sites see a consistent picture.
	ListBridgeCandidates(
		ctx context.Context,
		state string,
		staleThreshold time.Duration,
		createdAfter time.Time,
		idAfter uuid.UUID,
		limit int,
	) ([]*entities.ExtractionRequest, error)
}

// BridgeReadinessCounts captures the five-way partition of extractions for a
// tenant. Counts are mutually exclusive and Total() sums to the total
// extraction count for that tenant. InFlightCount covers upstream extractions
// not yet COMPLETE (PENDING/SUBMITTED/EXTRACTING) so the dashboard can
// distinguish "nothing happening" from "Fetcher is actively working".
type BridgeReadinessCounts struct {
	Ready         int64
	Pending       int64
	Stale         int64
	Failed        int64
	InFlightCount int64
}

// Total returns the sum of all five readiness buckets. Useful for sanity
// checking that the partition is complete (covers every extraction in the
// tenant). Reserved for partition invariants and dashboard sanity probes;
// T-005 will rely on this when adding bridge-failure semantics so the
// partition remains exhaustive.
func (c BridgeReadinessCounts) Total() int64 {
	return c.Ready + c.Pending + c.Stale + c.Failed + c.InFlightCount
}
