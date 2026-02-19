// Package cross provides adapters for cross-context dependencies.
// These adapters bridge bounded contexts while keeping domain types isolated.
package cross

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	ingestionEntities "github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// Sentinel errors for context lookup operations.
var (
	ErrContextLookupNotInitialized = errors.New("transaction context lookup not initialized")
	ErrTransactionNotFound         = errors.New("transaction not found")
	ErrIngestionJobNotFound        = errors.New("ingestion job not found")
	ErrSourceNotFound              = errors.New("source not found for context lookup")
	ErrTransactionFinderRequired   = errors.New("transaction finder is required")
	ErrIngestionJobFinderRequired  = errors.New("ingestion job finder is required")
)

// TransactionFinder is an interface for finding transactions by ID.
type TransactionFinder interface {
	FindByID(ctx context.Context, transactionID uuid.UUID) (*shared.Transaction, error)
}

// JobFinder is an interface for finding ingestion jobs by ID.
type JobFinder interface {
	FindByID(ctx context.Context, jobID uuid.UUID) (*ingestionEntities.IngestionJob, error)
}

// SourceContextFinder is an optional interface for resolving context IDs via the source path.
// When the primary ingestion-job lookup fails, this provides a fallback:
// Transaction.SourceID -> reconciliation_sources.context_id.
type SourceContextFinder interface {
	GetContextIDBySourceID(ctx context.Context, sourceID uuid.UUID) (uuid.UUID, error)
}

// TransactionContextLookup implements ExceptionContextLookup by looking up transactions and jobs.
// It supports an optional source-based fallback for resilience when ingestion job lookups fail.
type TransactionContextLookup struct {
	transactionFinder TransactionFinder
	jobFinder         JobFinder
	sourceFinder      SourceContextFinder // optional fallback
}

// NewTransactionContextLookup creates a new TransactionContextLookup.
func NewTransactionContextLookup(
	transactionFinder TransactionFinder,
	jobFinder JobFinder,
) (*TransactionContextLookup, error) {
	if transactionFinder == nil {
		return nil, ErrTransactionFinderRequired
	}

	if jobFinder == nil {
		return nil, ErrIngestionJobFinderRequired
	}

	return &TransactionContextLookup{
		transactionFinder: transactionFinder,
		jobFinder:         jobFinder,
	}, nil
}

// WithSourceFinder sets an optional source-based fallback for context ID resolution.
// When set, if the ingestion job lookup fails, the lookup will attempt to resolve
// the context ID via the transaction's SourceID -> reconciliation_sources.context_id.
func (lookup *TransactionContextLookup) WithSourceFinder(sf SourceContextFinder) {
	if lookup != nil {
		lookup.sourceFinder = sf
	}
}

// GetContextIDByTransactionID retrieves the context ID for a given transaction.
// It first finds the transaction to get the IngestionJobID, then looks up the
// ingestion job to get the ContextID. If the ingestion job lookup fails and a
// SourceContextFinder is configured, it falls back to resolving via the source path.
func (lookup *TransactionContextLookup) GetContextIDByTransactionID(
	ctx context.Context,
	transactionID uuid.UUID,
) (uuid.UUID, error) {
	if lookup == nil || lookup.transactionFinder == nil || lookup.jobFinder == nil {
		return uuid.Nil, ErrContextLookupNotInitialized
	}

	// Step 1: Find the transaction to get its IngestionJobID and SourceID
	tx, err := lookup.transactionFinder.FindByID(ctx, transactionID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("find transaction: %w", err)
	}

	if tx == nil {
		return uuid.Nil, fmt.Errorf("%w: %s", ErrTransactionNotFound, transactionID)
	}

	// Step 2: Try the primary path — find the ingestion job to get its ContextID
	contextID, jobErr := lookup.resolveViaIngestionJob(ctx, tx.IngestionJobID)
	if jobErr == nil {
		return contextID, nil
	}

	// Step 3: If the primary path failed and a source finder is available,
	// try the fallback path via SourceID -> reconciliation_sources.context_id
	if lookup.sourceFinder != nil && tx.SourceID != uuid.Nil {
		var sourceErr error

		contextID, sourceErr = lookup.sourceFinder.GetContextIDBySourceID(ctx, tx.SourceID)
		if sourceErr == nil {
			return contextID, nil
		}
		// Both paths failed; return the original ingestion job error
		// since it's the primary path and more informative
	}

	return uuid.Nil, jobErr
}

// resolveViaIngestionJob looks up the context ID through the ingestion job.
func (lookup *TransactionContextLookup) resolveViaIngestionJob(
	ctx context.Context,
	ingestionJobID uuid.UUID,
) (uuid.UUID, error) {
	job, err := lookup.jobFinder.FindByID(ctx, ingestionJobID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("find ingestion job: %w", err)
	}

	if job == nil {
		return uuid.Nil, fmt.Errorf("%w: %s", ErrIngestionJobNotFound, ingestionJobID)
	}

	return job.ContextID, nil
}

var _ ExceptionContextLookup = (*TransactionContextLookup)(nil)
