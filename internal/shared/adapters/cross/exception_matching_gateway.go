// Package cross provides adapters for cross-context dependencies.
// These adapters bridge bounded contexts while keeping domain types isolated.
package cross

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	configSourceRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/source"
	exceptionPorts "github.com/LerianStudio/matcher/internal/exception/ports"
	ingestionJobRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	ingestionTxRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/transaction"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepos "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
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

// ExceptionMatchingGateway implements exception.ports.MatchingGateway by coordinating
// with matching and ingestion repositories directly through one bridge.
type ExceptionMatchingGateway struct {
	adjustmentRepo  matchingRepos.AdjustmentRepository
	transactionRepo *ingestionTxRepo.Repository
	contextLookup   *TransactionContextLookup
}

// NewExceptionMatchingGateway creates a new matching gateway for exception resolution.
// sourceFinder is optional; pass nil to disable source-based context resolution fallback.
func NewExceptionMatchingGateway(
	adjustmentRepo matchingRepos.AdjustmentRepository,
	transactionRepo *ingestionTxRepo.Repository,
	jobFinder *ingestionJobRepo.Repository,
	sourceFinder *configSourceRepo.Repository,
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
	if gateway == nil || gateway.transactionRepo == nil {
		return ErrNilTransactionRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "cross.exception_matching_gateway.create_force_match")
	defer span.End()

	contextID, err := gateway.resolveContextIDByTransactionID(ctx, input.TransactionID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to resolve context ID", err)
		logger.With(
			libLog.String("operation", "force_match"),
			libLog.String("transaction_id", input.TransactionID.String()),
		).Log(ctx, libLog.LevelError, "failed to resolve context ID")

		return fmt.Errorf("resolve context ID: %w", err)
	}

	if err := gateway.transactionRepo.MarkMatched(ctx, contextID, []uuid.UUID{input.TransactionID}); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to mark transaction as matched", err)
		logger.With(
			libLog.String("operation", "force_match"),
			libLog.String("transaction_id", input.TransactionID.String()),
			libLog.String("context_id", contextID.String()),
		).Log(ctx, libLog.LevelError, "failed to mark transaction as matched")

		return fmt.Errorf("mark transaction matched: %w", err)
	}

	// input.OverrideReason and input.Actor are user-controlled; they must
	// never be interpolated into the log message template. The structured
	// fields are escaped by the logger and safe against CRLF injection.
	logger.With(
		libLog.String("operation", "force_match"),
		libLog.String("transaction_id", input.TransactionID.String()),
		libLog.String("context_id", contextID.String()),
		libLog.String("override_reason", input.OverrideReason),
		libLog.String("actor", input.Actor),
	).Log(ctx, libLog.LevelInfo, "force match created")

	return nil
}

// CreateAdjustment creates an adjustment record for a transaction.
func (gateway *ExceptionMatchingGateway) CreateAdjustment(
	ctx context.Context,
	input exceptionPorts.CreateAdjustmentInput,
) error {
	if gateway == nil || gateway.adjustmentRepo == nil {
		return ErrNilAdjustmentRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "cross.exception_matching_gateway.create_adjustment")
	defer span.End()

	contextID, err := gateway.resolveContextIDByTransactionID(ctx, input.TransactionID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to resolve context ID", err)
		logger.With(
			libLog.String("operation", "adjust_entry"),
			libLog.String("transaction_id", input.TransactionID.String()),
		).Log(ctx, libLog.LevelError, "failed to resolve context ID")

		return fmt.Errorf("resolve context ID: %w", err)
	}

	adjustmentType := mapReasonToAdjustmentType(ctx, logger, input.Reason)
	transactionID := input.TransactionID

	direction := matchingEntities.AdjustmentDirection(input.Direction)
	if !direction.IsValid() {
		libOpentelemetry.HandleSpanError(span, "invalid adjustment direction", ErrInvalidDirection)
		logger.With(
			libLog.String("operation", "adjust_entry"),
			libLog.String("transaction_id", input.TransactionID.String()),
			libLog.String("direction", input.Direction),
		).Log(ctx, libLog.LevelError, "invalid adjustment direction")

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
		logger.With(
			libLog.String("operation", "adjust_entry"),
			libLog.String("transaction_id", input.TransactionID.String()),
		).Log(ctx, libLog.LevelError, "failed to create adjustment entity")

		return fmt.Errorf("create adjustment entity: %w", err)
	}

	if _, err := gateway.adjustmentRepo.Create(ctx, adjustment); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to persist adjustment", err)
		logger.With(
			libLog.String("operation", "adjust_entry"),
			libLog.String("transaction_id", input.TransactionID.String()),
			libLog.String("adjustment_id", adjustment.ID.String()),
		).Log(ctx, libLog.LevelError, "failed to persist adjustment")

		return fmt.Errorf("persist adjustment: %w", err)
	}

	// input.Reason, input.Notes and input.Actor are user-controlled; pass
	// them only as structured fields so the logger escapes them and they
	// cannot forge new log lines via CRLF injection.
	logger.With(
		libLog.String("operation", "adjust_entry"),
		libLog.String("adjustment_id", adjustment.ID.String()),
		libLog.String("transaction_id", input.TransactionID.String()),
		libLog.String("amount", input.Amount.String()),
		libLog.String("currency", input.Currency),
		libLog.String("reason", input.Reason),
		libLog.String("actor", input.Actor),
	).Log(ctx, libLog.LevelInfo, "adjustment created")

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

// mapReasonToAdjustmentType maps exception adjustment reasons to matching
// adjustment types. Unknown reasons fall back to Miscellaneous but emit a
// WARN so operators can spot unexpected reason codes reaching the gateway
// (typically a config/console drift).
func mapReasonToAdjustmentType(
	ctx context.Context,
	logger libLog.Logger,
	reason string,
) matchingEntities.AdjustmentType {
	switch reason {
	case "CURRENCY_CORRECTION":
		return matchingEntities.AdjustmentTypeFXDifference
	default:
		if logger != nil {
			logger.With(
				libLog.String("reason", reason),
				libLog.String("mapped_to", string(matchingEntities.AdjustmentTypeMiscellaneous)),
			).Log(ctx, libLog.LevelWarn, "adjustment: unmapped reason, defaulting to miscellaneous")
		}

		return matchingEntities.AdjustmentTypeMiscellaneous
	}
}

var _ exceptionPorts.MatchingGateway = (*ExceptionMatchingGateway)(nil)
