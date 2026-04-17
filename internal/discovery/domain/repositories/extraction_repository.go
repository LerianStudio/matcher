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
	// MarkBridgeFailed persists a terminal bridge failure on the extraction
	// row identified by req.ID. Updates bridge_attempts, bridge_last_error,
	// bridge_last_error_message, bridge_failed_at, and updated_at — leaves
	// the discovery-side status untouched. Implementations MUST be
	// idempotent on (id, bridge_last_error): calling twice with the same
	// class persists the latest message+timestamp without churning the row
	// out of its current eligibility state. Once persisted, the row is
	// excluded from FindEligibleForBridge by virtue of bridge_last_error
	// being non-NULL.
	MarkBridgeFailed(ctx context.Context, req *entities.ExtractionRequest) error
	// MarkBridgeFailedWithTx is the WithTx variant of MarkBridgeFailed,
	// for callers that need to coordinate the failure write with other
	// state updates inside one transaction.
	MarkBridgeFailedWithTx(ctx context.Context, tx sharedPorts.Tx, req *entities.ExtractionRequest) error
	// IncrementBridgeAttempts narrowly persists the bumped attempts counter
	// + updated_at on the extraction row identified by id, gated by the
	// `ingestion_job_id IS NULL` predicate (Polish Fix 3).
	//
	// Implementations MUST use a SQL UPDATE that touches ONLY bridge_attempts
	// and updated_at — never the wide column list — so a concurrent link
	// write is never clobbered by a transient-retry attempt under a
	// lock-TTL-expiry edge case. Returns sharedPorts.ErrExtractionAlreadyLinked
	// when the WHERE clause filters the row out (already linked); returns
	// nil on the happy path.
	IncrementBridgeAttempts(ctx context.Context, id uuid.UUID, attempts int) error
	// IncrementBridgeAttemptsWithTx is the WithTx variant for callers that
	// need to coordinate the increment with other writes inside a single
	// transaction.
	IncrementBridgeAttemptsWithTx(ctx context.Context, tx sharedPorts.Tx, id uuid.UUID, attempts int) error
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
	// MarkCustodyDeleted persists the terminal "custody object is gone" marker
	// on the extraction row identified by id (T-006 polish, migration 000027).
	// Narrow UPDATE touches ONLY custody_deleted_at — the discovery status,
	// bridge_* columns, ingestion link, and updated_at are all left alone so
	// this write is race-safe against every other path that may touch the
	// same row.
	//
	// Used by both the bridge orchestrator's happy-path cleanupCustody hook
	// and the custody retention worker's sweep. Once persisted, the row drops
	// out of FindBridgeRetentionCandidates (which gates on custody_deleted_at
	// IS NULL) so the sweep converges to idle instead of re-scanning cleaned
	// rows forever. Returns ErrExtractionNotFound when no row matches the id.
	MarkCustodyDeleted(ctx context.Context, id uuid.UUID, deletedAt time.Time) error
	// MarkCustodyDeletedWithTx is the WithTx variant of MarkCustodyDeleted
	// (repositorytx linter requirement).
	MarkCustodyDeletedWithTx(ctx context.Context, tx sharedPorts.Tx, id uuid.UUID, deletedAt time.Time) error
	// FindBridgeRetentionCandidates returns extraction rows whose custody
	// object MAY still be sitting in object storage despite the extraction
	// no longer needing it (T-006 retention sweep).
	//
	// Two populations are returned:
	//
	//  1. TERMINAL: rows with bridge_last_error IS NOT NULL — these never
	//     went through the happy-path cleanupCustody hook in
	//     BridgeExtractionOrchestrator, so their custody object is
	//     guaranteed orphaned.
	//
	//  2. LATE-LINKED: rows with ingestion_job_id IS NOT NULL whose
	//     updated_at is older than gracePeriod — these SHOULD have had
	//     custody deleted by the orchestrator's happy-path hook, but if
	//     that delete failed (network blip, S3 outage), the custody object
	//     leaks. The grace period prevents the sweep from racing the
	//     orchestrator on freshly-linked rows.
	//
	// Results are ordered by updated_at ASC so older orphans get processed
	// first (drain-the-backlog fairness). limit is clamped by the caller.
	FindBridgeRetentionCandidates(
		ctx context.Context,
		gracePeriod time.Duration,
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
