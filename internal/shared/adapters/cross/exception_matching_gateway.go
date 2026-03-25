// Package cross provides adapters for cross-context dependencies.
// These adapters bridge bounded contexts while keeping domain types isolated.
package cross

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	exceptionPorts "github.com/LerianStudio/matcher/internal/exception/ports"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepos "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// Sentinel errors for exception matching gateway operations.
var (
	ErrNilAdjustmentRepository     = errors.New("adjustment repository is required")
	ErrNilTransactionRepository    = errors.New("transaction repository is required")
	ErrNilJobFinder                = errors.New("ingestion job finder is required")
	ErrContextLookupNotInitialized = errors.New("transaction context lookup not initialized")
	ErrTransactionNotFound         = errors.New("transaction not found")
	ErrIngestionJobNotFound        = errors.New("ingestion job not found")
	ErrSourceNotFound              = errors.New("source not found for context lookup")
	ErrContextNotFound             = errors.New("context not found for transaction")
	ErrInvalidDirection            = errors.New("invalid adjustment direction")
)

// ExceptionTransactionRepository contains the minimal transaction operations needed
// for exception resolution: force-match status updates.
type ExceptionTransactionRepository interface {
	FindByID(ctx context.Context, transactionID uuid.UUID) (*shared.Transaction, error)
	MarkMatched(ctx context.Context, contextID uuid.UUID, transactionIDs []uuid.UUID) error
}

// ExceptionMatchingGateway implements exception.ports.MatchingGateway by coordinating
// with matching and ingestion repositories directly through one bridge.
type ExceptionMatchingGateway struct {
	adjustmentRepo  matchingRepos.AdjustmentRepository
	transactionRepo ExceptionTransactionRepository
	contextLookup   ExceptionContextLookup
}

// NewExceptionMatchingGateway creates a new matching gateway for exception resolution.
func NewExceptionMatchingGateway(
	adjustmentRepo matchingRepos.AdjustmentRepository,
	transactionRepo ExceptionTransactionRepository,
	jobFinder JobFinder,
	sourceFinder SourceContextFinder,
) (*ExceptionMatchingGateway, error) {
	if adjustmentRepo == nil {
		return nil, ErrNilAdjustmentRepository
	}

	if transactionRepo == nil {
		return nil, ErrNilTransactionRepository
	}

	contextLookup, err := NewTransactionContextLookup(transactionRepo, jobFinder, sourceFinder)
	if err != nil {
		return nil, err
	}

	return &ExceptionMatchingGateway{
		adjustmentRepo:  adjustmentRepo,
		transactionRepo: transactionRepo,
		contextLookup:   contextLookup,
	}, nil
}

// CreateForceMatch creates a force match by marking the transaction as matched.
func (gateway *ExceptionMatchingGateway) CreateForceMatch(
	ctx context.Context,
	input exceptionPorts.ForceMatchInput,
) error {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "cross.exception_matching_gateway.create_force_match")
	defer span.End()

	contextID, err := gateway.resolveContextIDByTransactionID(ctx, input.TransactionID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to resolve context ID", err)
		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("force match: failed to resolve context ID for transaction %s: %v",
			input.TransactionID, err))

		return fmt.Errorf("resolve context ID: %w", err)
	}

	if err := gateway.transactionRepo.MarkMatched(ctx, contextID, []uuid.UUID{input.TransactionID}); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to mark transaction as matched", err)
		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("force match: failed to mark transaction %s as matched: %v",
			input.TransactionID, err))

		return fmt.Errorf("mark transaction matched: %w", err)
	}

	logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("force match created: transaction=%s, context=%s, reason=%s, actor=%s",
		input.TransactionID, contextID, input.OverrideReason, input.Actor))

	return nil
}

// CreateAdjustment creates an adjustment record for a transaction.
func (gateway *ExceptionMatchingGateway) CreateAdjustment(
	ctx context.Context,
	input exceptionPorts.CreateAdjustmentInput,
) error {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "cross.exception_matching_gateway.create_adjustment")
	defer span.End()

	contextID, err := gateway.resolveContextIDByTransactionID(ctx, input.TransactionID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to resolve context ID", err)
		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("adjust entry: failed to resolve context ID for transaction %s: %v",
			input.TransactionID, err))

		return fmt.Errorf("resolve context ID: %w", err)
	}

	adjustmentType := mapReasonToAdjustmentType(input.Reason)
	transactionID := input.TransactionID

	direction := matchingEntities.AdjustmentDirection(input.Direction)
	if !direction.IsValid() {
		libOpentelemetry.HandleSpanError(span, "invalid adjustment direction", ErrInvalidDirection)
		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("adjust entry: invalid direction %q for transaction %s",
			input.Direction, input.TransactionID))

		return fmt.Errorf("validate direction: %w", ErrInvalidDirection)
	}

	adjustment, err := matchingEntities.NewAdjustment(
		ctx,
		contextID,
		nil,
		&transactionID,
		adjustmentType,
		direction,
		input.Amount,
		input.Currency,
		input.Notes,
		input.Reason,
		input.Actor,
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create adjustment entity", err)
		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("adjust entry: failed to create adjustment entity: %v", err))

		return fmt.Errorf("create adjustment entity: %w", err)
	}

	if _, err := gateway.adjustmentRepo.Create(ctx, adjustment); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to persist adjustment", err)
		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("adjust entry: failed to persist adjustment: %v", err))

		return fmt.Errorf("persist adjustment: %w", err)
	}

	logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("adjustment created: adjustment=%s, transaction=%s, amount=%s %s, reason=%s",
		adjustment.ID, input.TransactionID, input.Amount.String(), input.Currency, input.Reason))

	return nil
}

func (gateway *ExceptionMatchingGateway) resolveContextIDByTransactionID(
	ctx context.Context,
	transactionID uuid.UUID,
) (uuid.UUID, error) {
	if gateway == nil || gateway.contextLookup == nil {
		return uuid.Nil, ErrContextLookupNotInitialized
	}

	contextID, err := gateway.contextLookup.GetContextIDByTransactionID(ctx, transactionID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("resolve context by transaction id: %w", err)
	}

	return contextID, nil
}

func mapSourceLookupError(jobErr, sourceErr error) error {
	if errors.Is(jobErr, ErrIngestionJobNotFound) && errors.Is(sourceErr, sql.ErrNoRows) {
		return ErrSourceNotFound
	}

	if errors.Is(sourceErr, ErrSourceNotFound) || errors.Is(sourceErr, ErrContextNotFound) {
		return sourceErr
	}

	return jobErr
}

// mapReasonToAdjustmentType maps exception adjustment reasons to matching adjustment types.
func mapReasonToAdjustmentType(reason string) matchingEntities.AdjustmentType {
	switch reason {
	case "CURRENCY_CORRECTION":
		return matchingEntities.AdjustmentTypeFXDifference
	default:
		return matchingEntities.AdjustmentTypeMiscellaneous
	}
}

var _ exceptionPorts.MatchingGateway = (*ExceptionMatchingGateway)(nil)
