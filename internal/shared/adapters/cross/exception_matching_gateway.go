// Package cross provides adapters for cross-context dependencies.
// These adapters bridge bounded contexts while keeping domain types isolated.
package cross

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"
	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"
	libOpentelemetry "github.com/LerianStudio/lib-uncommons/v2/uncommons/opentelemetry"

	exceptionPorts "github.com/LerianStudio/matcher/internal/exception/ports"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepos "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	matchingPorts "github.com/LerianStudio/matcher/internal/matching/ports"
)

// Sentinel errors for exception matching gateway operations.
var (
	ErrNilAdjustmentRepository  = errors.New("adjustment repository is required")
	ErrNilTransactionRepository = errors.New("transaction repository is required")
	ErrNilContextLookup         = errors.New("context lookup is required")
	ErrContextNotFound          = errors.New("context not found for transaction")
	ErrInvalidDirection         = errors.New("invalid adjustment direction")
)

// ExceptionContextLookup abstracts the operation of finding the context ID for a transaction.
type ExceptionContextLookup interface {
	// GetContextIDByTransactionID returns the context ID for a given transaction.
	GetContextIDByTransactionID(ctx context.Context, transactionID uuid.UUID) (uuid.UUID, error)
}

// ExceptionMatchingGateway implements exception.ports.MatchingGateway by coordinating
// with matching context repositories.
type ExceptionMatchingGateway struct {
	adjustmentRepo  matchingRepos.AdjustmentRepository
	transactionRepo matchingPorts.TransactionRepository
	contextLookup   ExceptionContextLookup
}

// NewExceptionMatchingGateway creates a new matching gateway for exception resolution.
func NewExceptionMatchingGateway(
	adjustmentRepo matchingRepos.AdjustmentRepository,
	transactionRepo matchingPorts.TransactionRepository,
	contextLookup ExceptionContextLookup,
) (*ExceptionMatchingGateway, error) {
	if adjustmentRepo == nil {
		return nil, ErrNilAdjustmentRepository
	}

	if transactionRepo == nil {
		return nil, ErrNilTransactionRepository
	}

	if contextLookup == nil {
		return nil, ErrNilContextLookup
	}

	return &ExceptionMatchingGateway{
		adjustmentRepo:  adjustmentRepo,
		transactionRepo: transactionRepo,
		contextLookup:   contextLookup,
	}, nil
}

// CreateForceMatch creates a force match by marking the transaction as matched.
// Unlike normal matching which creates match groups linking multiple transactions,
// force match directly transitions the transaction to MATCHED status as a manual override.
func (gateway *ExceptionMatchingGateway) CreateForceMatch(
	ctx context.Context,
	input exceptionPorts.ForceMatchInput,
) error {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "cross.exception_matching_gateway.create_force_match")

	defer span.End()

	// Resolve the context ID from the transaction
	contextID, err := gateway.contextLookup.GetContextIDByTransactionID(ctx, input.TransactionID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to resolve context ID", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("force match: failed to resolve context ID for transaction %s: %v",
			input.TransactionID, err))

		return fmt.Errorf("resolve context ID: %w", err)
	}

	// Mark the transaction as matched
	// This is a direct status change without creating a match group,
	// which is appropriate for manual force matches
	err = gateway.transactionRepo.MarkMatched(ctx, contextID, []uuid.UUID{input.TransactionID})
	if err != nil {
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

	// Resolve the context ID from the transaction
	contextID, err := gateway.contextLookup.GetContextIDByTransactionID(ctx, input.TransactionID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to resolve context ID", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("adjust entry: failed to resolve context ID for transaction %s: %v",
			input.TransactionID, err))

		return fmt.Errorf("resolve context ID: %w", err)
	}

	// Map the reason to an adjustment type
	adjustmentType := mapReasonToAdjustmentType(input.Reason)

	// Create the adjustment entity
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
		nil,            // No match group ID for exception-based adjustments
		&transactionID, // Link to the transaction
		adjustmentType,
		direction,
		input.Amount,
		input.Currency,
		input.Notes, // Use notes as description
		input.Reason,
		input.Actor,
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create adjustment entity", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("adjust entry: failed to create adjustment entity: %v", err))

		return fmt.Errorf("create adjustment entity: %w", err)
	}

	// Persist the adjustment
	_, err = gateway.adjustmentRepo.Create(ctx, adjustment)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to persist adjustment", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("adjust entry: failed to persist adjustment: %v", err))

		return fmt.Errorf("persist adjustment: %w", err)
	}

	logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("adjustment created: adjustment=%s, transaction=%s, amount=%s %s, reason=%s",
		adjustment.ID, input.TransactionID, input.Amount.String(), input.Currency, input.Reason))

	return nil
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
