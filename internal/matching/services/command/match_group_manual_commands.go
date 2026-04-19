package command

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	"github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// TODO(telemetry): matching/adapters/http/handlers.go — logSpanError uses HandleSpanError for
// business outcomes (badRequest, writeNotFound, forbidden). Add logSpanBusinessEvent using
// HandleSpanBusinessErrorEvent and create business-aware variants for 400/404/409 responses.
// See reporting/adapters/http/handlers_export_job.go for the reference implementation.

const (
	minManualMatchTransactions = 2
	manualMatchConfidence      = 100
)

// ManualMatchInput contains the input parameters for creating a manual match.
type ManualMatchInput struct {
	TenantID       uuid.UUID
	ContextID      uuid.UUID
	TransactionIDs []uuid.UUID
	Notes          string
}

// Manual-match sentinel errors.
var (
	ErrMinimumTransactionsRequired  = errors.New("at least two transactions are required")
	ErrDuplicateTransactionIDs      = errors.New("duplicate transaction IDs provided")
	ErrTransactionNotFound          = errors.New("one or more transactions not found")
	ErrTransactionNotUnmatched      = errors.New("one or more transactions are not unmatched")
	ErrManualMatchCreatingRun       = errors.New("failed to create manual match run")
	ErrManualMatchNoGroupCreated    = errors.New("no match group created")
	ErrManualMatchSourcesNotDiverse = errors.New("transactions must come from at least two different sources for reconciliation")
)

// ManualMatch creates a manual match group for the given transactions.
//
//nolint:gocognit,gocyclo,cyclop // transactional operation requires sequential steps
func (uc *UseCase) ManualMatch(
	ctx context.Context,
	in ManualMatchInput,
) (*matchingEntities.MatchGroup, error) {
	if err := uc.validateManualMatchInput(in); err != nil {
		return nil, err
	}

	if err := validateTenantFromContext(ctx, in.TenantID); err != nil {
		return nil, err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.matching.manual_match")
	defer span.End()

	_ = libOpentelemetry.SetSpanAttributesFromValue(span, "matcher", in, nil)

	ctxInfo, err := uc.contextProvider.FindByID(ctx, in.TenantID, in.ContextID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to find context", err)
		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to find context")

		return nil, fmt.Errorf("find context: %w", ErrContextNotFound)
	}

	if ctxInfo == nil {
		return nil, ErrContextNotFound
	}

	if !ctxInfo.Active {
		return nil, ErrContextNotActive
	}

	transactions, err := uc.txRepo.FindByContextAndIDs(ctx, in.ContextID, in.TransactionIDs)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to find transactions", err)
		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to find transactions")

		return nil, fmt.Errorf("find transactions: %w", err)
	}

	if len(transactions) != len(in.TransactionIDs) {
		return nil, ErrTransactionNotFound
	}

	for _, txn := range transactions {
		if txn == nil {
			return nil, ErrTransactionNotFound
		}

		if txn.Status != shared.TransactionStatusUnmatched {
			return nil, fmt.Errorf(
				"%w: transaction %s has status %s",
				ErrTransactionNotUnmatched,
				txn.ID,
				txn.Status,
			)
		}
	}

	uniqueSources := make(map[uuid.UUID]struct{}, len(transactions))
	for _, txn := range transactions {
		uniqueSources[txn.SourceID] = struct{}{}
	}

	if len(uniqueSources) < minManualMatchTransactions {
		return nil, ErrManualMatchSourcesNotDiverse
	}

	run, err := matchingEntities.NewMatchRun(ctx, in.ContextID, matchingVO.MatchRunModeCommit)
	if err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "failed to create match run entity", err)
		return nil, fmt.Errorf("%w: %w", ErrManualMatchCreatingRun, err)
	}

	var createdGroup *matchingEntities.MatchGroup

	err = uc.txRepo.WithTx(ctx, func(tx repositories.Tx) error {
		createdRun, txErr := uc.matchRunRepo.CreateWithTx(ctx, tx, run)
		if txErr != nil {
			return fmt.Errorf("create match run: %w", txErr)
		}

		if createdRun == nil {
			return ErrMatchRunPersistedNil
		}

		items := make([]*matchingEntities.MatchItem, 0, len(transactions))
		for _, txn := range transactions {
			amount := txn.Amount
			currency := txn.Currency

			if txn.AmountBase != nil && !txn.AmountBase.IsZero() {
				amount = *txn.AmountBase
			}

			if txn.BaseCurrency != nil && *txn.BaseCurrency != "" {
				currency = *txn.BaseCurrency
			}

			item, itemErr := matchingEntities.NewMatchItem(ctx, txn.ID, amount, currency, amount)
			if itemErr != nil {
				return fmt.Errorf("create match item for transaction %s: %w", txn.ID, itemErr)
			}

			items = append(items, item)
		}

		confidence, confErr := matchingVO.ParseConfidenceScore(manualMatchConfidence)
		if confErr != nil {
			return fmt.Errorf("parse confidence score: %w", confErr)
		}

		group, groupErr := matchingEntities.NewMatchGroup(
			ctx,
			in.ContextID,
			createdRun.ID,
			uuid.Nil,
			confidence,
			items,
		)
		if groupErr != nil {
			return fmt.Errorf("create match group: %w", groupErr)
		}

		if confirmErr := group.Confirm(ctx); confirmErr != nil {
			return fmt.Errorf("confirm match group: %w", confirmErr)
		}

		createdGroups, batchErr := uc.matchGroupRepo.CreateBatchWithTx(
			ctx,
			tx,
			[]*matchingEntities.MatchGroup{group},
		)
		if batchErr != nil {
			return fmt.Errorf("persist match group: %w", batchErr)
		}

		if len(createdGroups) == 0 || createdGroups[0] == nil {
			return ErrManualMatchNoGroupCreated
		}

		createdGroup = createdGroups[0]
		createdGroup.Items = items

		if len(items) > 0 {
			if _, itemsErr := uc.matchItemRepo.CreateBatchWithTx(ctx, tx, items); itemsErr != nil {
				return fmt.Errorf("persist match items: %w", itemsErr)
			}
		}

		if markErr := uc.txRepo.MarkMatchedWithTx(ctx, tx, in.ContextID, in.TransactionIDs); markErr != nil {
			return fmt.Errorf("mark transactions matched: %w", markErr)
		}

		stats := map[string]int{
			"matched_transactions": len(in.TransactionIDs),
			"match_groups":         1,
		}

		if completeErr := createdRun.Complete(ctx, stats); completeErr != nil {
			return fmt.Errorf("complete match run: %w", completeErr)
		}

		if _, updateErr := uc.matchRunRepo.UpdateWithTx(ctx, tx, createdRun); updateErr != nil {
			return fmt.Errorf("update match run: %w", updateErr)
		}

		if outboxErr := uc.enqueueMatchConfirmedEvents(
			ctx,
			tx,
			[]*matchingEntities.MatchGroup{createdGroup},
		); outboxErr != nil {
			return fmt.Errorf("enqueue match confirmed event: %w", outboxErr)
		}

		return nil
	})
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create manual match", err)
		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to create manual match")

		return nil, fmt.Errorf("create manual match: %w", err)
	}

	logger.With(
		libLog.String("group_id", createdGroup.ID.String()),
		libLog.Any("transactions", len(in.TransactionIDs)),
	).Log(ctx, libLog.LevelInfo, "manual match created")

	return createdGroup, nil
}

func (uc *UseCase) validateManualMatchInput(in ManualMatchInput) error {
	if in.TenantID == uuid.Nil {
		return ErrTenantIDRequired
	}

	if in.ContextID == uuid.Nil {
		return ErrRunMatchContextIDRequired
	}

	if len(in.TransactionIDs) < minManualMatchTransactions {
		return ErrMinimumTransactionsRequired
	}

	seen := make(map[uuid.UUID]bool, len(in.TransactionIDs))
	for _, id := range in.TransactionIDs {
		if seen[id] {
			return ErrDuplicateTransactionIDs
		}

		seen[id] = true
	}

	return nil
}
