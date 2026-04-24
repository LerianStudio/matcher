// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package command

import (
	"context"
	"fmt"
	"time"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	ingestionRepositories "github.com/LerianStudio/matcher/internal/ingestion/domain/repositories"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// clearDedupKeys removes dedup keys after successful ingestion.
// This allows legitimate re-uploads once the ingestion job completes.
// Errors are logged but do not affect the result since the job already succeeded.
func (uc *UseCase) clearDedupKeys(ctx context.Context, state *ingestionState) {
	if state == nil || state.job == nil || len(state.markedHashes) == 0 {
		return
	}

	if clearErr := uc.dedupe.ClearBatch(ctx, state.job.ContextID, state.markedHashes); clearErr != nil {
		logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)
		logger.With(libLog.Err(clearErr)).Log(ctx, libLog.LevelWarn, "failed to clear dedup keys after successful ingestion")
	}
}

// filterAndInsertChunk performs bulk deduplication check and inserts a chunk of transactions.
// Returns the number of inserted transactions, marked hashes for cleanup, and any error.
func (uc *UseCase) filterAndInsertChunk(
	ctx context.Context,
	job *entities.IngestionJob,
	transactions []*shared.Transaction,
) (int, []string, error) {
	if len(transactions) == 0 {
		return 0, nil, nil
	}

	keys := make([]ingestionRepositories.ExternalIDKey, 0, len(transactions))
	hashes := make([]string, 0, len(transactions))

	for _, tx := range transactions {
		keys = append(keys, ingestionRepositories.ExternalIDKey{
			SourceID:   tx.SourceID,
			ExternalID: tx.ExternalID,
		})
		hashes = append(hashes, uc.dedupe.CalculateHash(tx.SourceID, tx.ExternalID))
	}

	existsMap, err := uc.transactionRepo.ExistsBulkBySourceAndExternalID(ctx, keys)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to check existing transactions: %w", err)
	}

	markedByHash, err := uc.dedupe.MarkSeenBulk(ctx, job.ContextID, hashes, uc.currentDedupeTTL(ctx))
	if err != nil {
		return 0, nil, fmt.Errorf("failed to mark transactions seen: %w", err)
	}

	filtered := make([]*shared.Transaction, 0, len(transactions))
	markedHashes := make([]string, 0, len(transactions))

	for i, tx := range transactions {
		hash := hashes[i]
		if !markedByHash[hash] {
			// Already present in Redis — treat as duplicate.
			continue
		}

		markedHashes = append(markedHashes, hash)

		key := ingestionRepositories.ExternalIDKey{SourceID: tx.SourceID, ExternalID: tx.ExternalID}
		if existsMap[key] {
			continue
		}

		filtered = append(filtered, tx)
	}

	if len(filtered) > 0 {
		if _, err := uc.transactionRepo.CreateBatch(ctx, filtered); err != nil {
			return 0, markedHashes, fmt.Errorf("failed to insert transactions: %w", err)
		}
	}

	return len(filtered), markedHashes, nil
}

func (uc *UseCase) currentDedupeTTL(ctx context.Context) time.Duration {
	if uc == nil {
		return 0
	}

	if uc.dedupeTTLResolver != nil {
		if ttl := uc.dedupeTTLResolver(ctx); ttl > 0 {
			return ttl
		}
	}

	if uc.dedupeTTLGetter != nil {
		if ttl := uc.dedupeTTLGetter(); ttl > 0 {
			return ttl
		}
	}

	return uc.dedupeTTL
}
