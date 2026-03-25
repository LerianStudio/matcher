// Package cross provides adapters for cross-context dependencies.
// These adapters bridge different bounded contexts while keeping ports isolated.
package cross

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	ingestionTxRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/transaction"
	matchingRepos "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	matchingPorts "github.com/LerianStudio/matcher/internal/matching/ports"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// BaseTransactionRepository defines the methods required from the base repository.
// This interface enables mocking for unit tests.
type BaseTransactionRepository interface {
	ListUnmatchedByContext(
		ctx context.Context,
		contextID uuid.UUID,
		startInclusive, endInclusive *time.Time,
		limit, offset int,
	) ([]*shared.Transaction, error)
	FindByContextAndIDs(
		ctx context.Context,
		contextID uuid.UUID,
		transactionIDs []uuid.UUID,
	) ([]*shared.Transaction, error)
	MarkMatched(ctx context.Context, contextID uuid.UUID, transactionIDs []uuid.UUID) error
	MarkMatchedWithTx(ctx context.Context, tx *sql.Tx, contextID uuid.UUID, transactionIDs []uuid.UUID) error
	MarkPendingReview(ctx context.Context, contextID uuid.UUID, transactionIDs []uuid.UUID) error
	MarkPendingReviewWithTx(ctx context.Context, tx *sql.Tx, contextID uuid.UUID, transactionIDs []uuid.UUID) error
	MarkUnmatched(ctx context.Context, contextID uuid.UUID, transactionIDs []uuid.UUID) error
	MarkUnmatchedWithTx(ctx context.Context, tx *sql.Tx, contextID uuid.UUID, transactionIDs []uuid.UUID) error
}

// TransactionRepositoryAdapter bridges ingestion transactions to matching ports.
type TransactionRepositoryAdapter struct {
	provider ports.InfrastructureProvider
	baseRepo BaseTransactionRepository
}

// Sentinel errors for transaction repository adapter.
var (
	ErrAdapterNotInitialized = errors.New("transaction repository adapter not initialized")
	ErrContextIDRequired     = errors.New("context id is required")
	ErrNilProvider           = errors.New("infrastructure provider is required")
	ErrNilBaseRepo           = errors.New("base transaction repository is required")
)

// NewTransactionRepositoryAdapterFromRepo creates a new adapter using the concrete ingestion repository.
// This is the primary constructor for production use.
func NewTransactionRepositoryAdapterFromRepo(
	provider ports.InfrastructureProvider,
	baseRepo *ingestionTxRepo.Repository,
) (*TransactionRepositoryAdapter, error) {
	if provider == nil {
		return nil, ErrNilProvider
	}

	if baseRepo == nil {
		return nil, ErrNilBaseRepo
	}

	return &TransactionRepositoryAdapter{provider: provider, baseRepo: baseRepo}, nil
}

// ListUnmatchedByContext proxies to the ingestion transaction repository.
func (adapter *TransactionRepositoryAdapter) ListUnmatchedByContext(
	ctx context.Context,
	contextID uuid.UUID,
	startInclusive, endInclusive *time.Time,
	limit, offset int,
) ([]*shared.Transaction, error) {
	if adapter == nil || adapter.baseRepo == nil {
		return nil, ErrAdapterNotInitialized
	}

	if contextID == uuid.Nil {
		return nil, ErrContextIDRequired
	}

	transactions, err := adapter.baseRepo.ListUnmatchedByContext(
		ctx,
		contextID,
		startInclusive,
		endInclusive,
		limit,
		offset,
	)
	if err != nil {
		return nil, fmt.Errorf("list unmatched by context: %w", err)
	}

	return transactions, nil
}

// FindByContextAndIDs proxies to the ingestion transaction repository.
func (adapter *TransactionRepositoryAdapter) FindByContextAndIDs(
	ctx context.Context,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) ([]*shared.Transaction, error) {
	if adapter == nil || adapter.baseRepo == nil {
		return nil, ErrAdapterNotInitialized
	}

	if contextID == uuid.Nil {
		return nil, ErrContextIDRequired
	}

	transactions, err := adapter.baseRepo.FindByContextAndIDs(ctx, contextID, transactionIDs)
	if err != nil {
		return nil, fmt.Errorf("find by context and ids: %w", err)
	}

	return transactions, nil
}

// MarkMatched updates transactions as matched within its own transaction.
func (adapter *TransactionRepositoryAdapter) MarkMatched(
	ctx context.Context,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) error {
	if adapter == nil || adapter.baseRepo == nil {
		return ErrAdapterNotInitialized
	}

	if contextID == uuid.Nil {
		return ErrContextIDRequired
	}

	if err := adapter.baseRepo.MarkMatched(ctx, contextID, transactionIDs); err != nil {
		return fmt.Errorf("mark matched: %w", err)
	}

	return nil
}

// MarkPendingReview updates transactions as pending review within its own transaction.
func (adapter *TransactionRepositoryAdapter) MarkPendingReview(
	ctx context.Context,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) error {
	if adapter == nil || adapter.baseRepo == nil {
		return ErrAdapterNotInitialized
	}

	if contextID == uuid.Nil {
		return ErrContextIDRequired
	}

	if err := adapter.baseRepo.MarkPendingReview(ctx, contextID, transactionIDs); err != nil {
		return fmt.Errorf("mark pending review: %w", err)
	}

	return nil
}

// MarkMatchedWithTx updates transactions as matched within a provided transaction.
func (adapter *TransactionRepositoryAdapter) MarkMatchedWithTx(
	ctx context.Context,
	tx matchingRepos.Tx,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) error {
	if adapter == nil || adapter.baseRepo == nil || adapter.provider == nil {
		return ErrAdapterNotInitialized
	}

	if contextID == uuid.Nil {
		return ErrContextIDRequired
	}

	if len(transactionIDs) == 0 {
		return nil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	if tracer == nil {
		tracer = otel.Tracer("commons.default")
	}

	ctx, span := tracer.Start(ctx, "postgres.mark_transactions_matched_with_tx")
	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		adapter.provider,
		tx,
		func(execTx *sql.Tx) (struct{}, error) {
			if err := adapter.baseRepo.MarkMatchedWithTx(ctx, execTx, contextID, transactionIDs); err != nil {
				return struct{}{}, fmt.Errorf("delegate mark matched: %w", err)
			}

			return struct{}{}, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("mark matched with transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to mark transactions matched", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to mark transactions matched: %v", wrappedErr))

		return wrappedErr
	}

	return nil
}

// MarkPendingReviewWithTx updates transactions as pending review within a provided transaction.
func (adapter *TransactionRepositoryAdapter) MarkPendingReviewWithTx(
	ctx context.Context,
	tx matchingRepos.Tx,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) error {
	if adapter == nil || adapter.baseRepo == nil || adapter.provider == nil {
		return ErrAdapterNotInitialized
	}

	if contextID == uuid.Nil {
		return ErrContextIDRequired
	}

	if len(transactionIDs) == 0 {
		return nil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	if tracer == nil {
		tracer = otel.Tracer("commons.default")
	}

	ctx, span := tracer.Start(ctx, "postgres.mark_transactions_pending_review_with_tx")
	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		adapter.provider,
		tx,
		func(execTx *sql.Tx) (struct{}, error) {
			if err := adapter.baseRepo.MarkPendingReviewWithTx(ctx, execTx, contextID, transactionIDs); err != nil {
				return struct{}{}, fmt.Errorf("delegate mark pending review: %w", err)
			}

			return struct{}{}, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("mark pending review with transaction: %w", err)
		libOpentelemetry.HandleSpanError(
			span,
			"failed to mark transactions pending review",
			wrappedErr,
		)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to mark transactions pending review: %v", wrappedErr))

		return wrappedErr
	}

	return nil
}

// WithTx runs fn inside a tenant-scoped transaction.
func (adapter *TransactionRepositoryAdapter) WithTx(
	ctx context.Context,
	fn func(matchingRepos.Tx) error,
) error {
	if adapter == nil || adapter.provider == nil {
		return ErrAdapterNotInitialized
	}

	if fn == nil {
		return nil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	if tracer == nil {
		tracer = otel.Tracer("commons.default")
	}

	ctx, span := tracer.Start(ctx, "postgres.matching_transaction_with_tx")
	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(
		ctx,
		adapter.provider,
		func(tx *sql.Tx) (struct{}, error) {
			if err := fn(tx); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, nil
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to run transaction wrapper", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to run transaction wrapper: %v", err))

		return fmt.Errorf("run transaction wrapper: %w", err)
	}

	return nil
}

// MarkUnmatched marks transactions as unmatched by their IDs.
func (adapter *TransactionRepositoryAdapter) MarkUnmatched(
	ctx context.Context,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) error {
	if adapter == nil || adapter.baseRepo == nil {
		return ErrAdapterNotInitialized
	}

	if contextID == uuid.Nil {
		return ErrContextIDRequired
	}

	if err := adapter.baseRepo.MarkUnmatched(ctx, contextID, transactionIDs); err != nil {
		return fmt.Errorf("mark unmatched: %w", err)
	}

	return nil
}

// MarkUnmatchedWithTx marks transactions as unmatched within a provided transaction.
func (adapter *TransactionRepositoryAdapter) MarkUnmatchedWithTx(
	ctx context.Context,
	tx matchingRepos.Tx,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) error {
	if adapter == nil || adapter.baseRepo == nil || adapter.provider == nil {
		return ErrAdapterNotInitialized
	}

	if contextID == uuid.Nil {
		return ErrContextIDRequired
	}

	if len(transactionIDs) == 0 {
		return nil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	if tracer == nil {
		tracer = otel.Tracer("commons.default")
	}

	ctx, span := tracer.Start(ctx, "postgres.mark_transactions_unmatched_with_tx")
	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		adapter.provider,
		tx,
		func(execTx *sql.Tx) (struct{}, error) {
			if err := adapter.baseRepo.MarkUnmatchedWithTx(ctx, execTx, contextID, transactionIDs); err != nil {
				return struct{}{}, fmt.Errorf("delegate mark unmatched: %w", err)
			}

			return struct{}{}, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("mark unmatched with transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to mark transactions unmatched", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to mark transactions unmatched: %v", wrappedErr))

		return wrappedErr
	}

	return nil
}

var _ matchingPorts.TransactionRepository = (*TransactionRepositoryAdapter)(nil)
