package cross

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"

	ingestionEntities "github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// ExceptionContextLookup resolves a reconciliation context ID for a transaction.
type ExceptionContextLookup interface {
	GetContextIDByTransactionID(ctx context.Context, transactionID uuid.UUID) (uuid.UUID, error)
}

// TransactionFinder is an interface for finding transactions by ID.
type TransactionFinder interface {
	FindByID(ctx context.Context, transactionID uuid.UUID) (*shared.Transaction, error)
}

// JobFinder is an interface for finding ingestion jobs by ID.
type JobFinder interface {
	FindByID(ctx context.Context, jobID uuid.UUID) (*ingestionEntities.IngestionJob, error)
}

// SourceContextFinder is an optional interface for resolving context IDs via the source path.
type SourceContextFinder interface {
	GetContextIDBySourceID(ctx context.Context, sourceID uuid.UUID) (uuid.UUID, error)
}

// TransactionContextLookup resolves context IDs using transaction, ingestion job,
// and optional source fallback lookups.
type TransactionContextLookup struct {
	transactionFinder TransactionFinder
	jobFinder         JobFinder
	sourceFinder      SourceContextFinder
}

// NewTransactionContextLookup creates a context resolver that derives context IDs from transaction metadata.
func NewTransactionContextLookup(
	transactionFinder TransactionFinder,
	jobFinder JobFinder,
	sourceFinder SourceContextFinder,
) (*TransactionContextLookup, error) {
	if transactionFinder == nil {
		return nil, ErrNilTransactionRepository
	}

	if jobFinder == nil {
		return nil, ErrNilJobFinder
	}

	return &TransactionContextLookup{
		transactionFinder: transactionFinder,
		jobFinder:         jobFinder,
		sourceFinder:      sourceFinder,
	}, nil
}

// GetContextIDByTransactionID resolves the reconciliation context for a transaction ID.
func (lookup *TransactionContextLookup) GetContextIDByTransactionID(
	ctx context.Context,
	transactionID uuid.UUID,
) (uuid.UUID, error) {
	if lookup == nil || lookup.transactionFinder == nil || lookup.jobFinder == nil {
		return uuid.Nil, ErrContextLookupNotInitialized
	}

	tx, err := lookup.transactionFinder.FindByID(ctx, transactionID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return uuid.Nil, fmt.Errorf("%w: %s", ErrTransactionNotFound, transactionID)
		}

		return uuid.Nil, fmt.Errorf("find transaction: %w", err)
	}

	if tx == nil {
		return uuid.Nil, fmt.Errorf("%w: %s", ErrTransactionNotFound, transactionID)
	}

	contextID, jobErr := lookup.resolveViaIngestionJob(ctx, tx.IngestionJobID)
	if jobErr == nil {
		return contextID, nil
	}

	if lookup.sourceFinder == nil || tx.SourceID == uuid.Nil {
		return uuid.Nil, jobErr
	}

	fallbackContextID, sourceErr := lookup.resolveViaSource(ctx, tx.SourceID)
	if sourceErr == nil {
		return fallbackContextID, nil
	}

	return uuid.Nil, mapSourceLookupError(jobErr, sourceErr)
}

func (lookup *TransactionContextLookup) resolveViaSource(
	ctx context.Context,
	sourceID uuid.UUID,
) (uuid.UUID, error) {
	contextID, err := lookup.sourceFinder.GetContextIDBySourceID(ctx, sourceID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("resolve context via source: %w", err)
	}

	return contextID, nil
}

func (lookup *TransactionContextLookup) resolveViaIngestionJob(
	ctx context.Context,
	ingestionJobID uuid.UUID,
) (uuid.UUID, error) {
	job, err := lookup.jobFinder.FindByID(ctx, ingestionJobID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return uuid.Nil, fmt.Errorf("%w: %s", ErrIngestionJobNotFound, ingestionJobID)
		}

		return uuid.Nil, fmt.Errorf("find ingestion job: %w", err)
	}

	if job == nil {
		return uuid.Nil, fmt.Errorf("%w: %s", ErrIngestionJobNotFound, ingestionJobID)
	}

	return job.ContextID, nil
}

var _ ExceptionContextLookup = (*TransactionContextLookup)(nil)
