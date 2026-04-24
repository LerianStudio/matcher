// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package cross

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"

	configSourceRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/source"
	ingestionJobRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	ingestionTxRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/transaction"
)

// TransactionContextLookup resolves context IDs using transaction, ingestion job,
// and optional source fallback lookups.
type TransactionContextLookup struct {
	transactionFinder *ingestionTxRepo.Repository
	jobFinder         *ingestionJobRepo.Repository
	sourceFinder      *configSourceRepo.Repository
}

// NewTransactionContextLookup creates a context resolver that derives context IDs from transaction metadata.
// The sourceFinder is optional; pass nil to disable source-based fallback.
func NewTransactionContextLookup(
	transactionFinder *ingestionTxRepo.Repository,
	jobFinder *ingestionJobRepo.Repository,
	sourceFinder *configSourceRepo.Repository,
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
