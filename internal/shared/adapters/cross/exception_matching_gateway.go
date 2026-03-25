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
	ingestionEntities "github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
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
// for exception resolution: lookup for context resolution and force-match status updates.
type ExceptionTransactionRepository interface {
	FindByID(ctx context.Context, transactionID uuid.UUID) (*shared.Transaction, error)
	MarkMatched(ctx context.Context, contextID uuid.UUID, transactionIDs []uuid.UUID) error
}

// JobFinder is an interface for finding ingestion jobs by ID.
type JobFinder interface {
	FindByID(ctx context.Context, jobID uuid.UUID) (*ingestionEntities.IngestionJob, error)
}

// SourceContextFinder is an optional interface for resolving context IDs via the source path.
type SourceContextFinder interface {
	GetContextIDBySourceID(ctx context.Context, sourceID uuid.UUID) (uuid.UUID, error)
}

// ExceptionMatchingGateway implements exception.ports.MatchingGateway by coordinating
// with matching and ingestion repositories directly through one bridge.
type ExceptionMatchingGateway struct {
	adjustmentRepo  matchingRepos.AdjustmentRepository
	transactionRepo ExceptionTransactionRepository
	jobFinder       JobFinder
	sourceFinder    SourceContextFinder
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

	if jobFinder == nil {
		return nil, ErrNilJobFinder
	}

	return &ExceptionMatchingGateway{
		adjustmentRepo:  adjustmentRepo,
		transactionRepo: transactionRepo,
		jobFinder:       jobFinder,
		sourceFinder:    sourceFinder,
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
	if gateway == nil || gateway.transactionRepo == nil || gateway.jobFinder == nil {
		return uuid.Nil, ErrContextLookupNotInitialized
	}

	tx, err := gateway.transactionRepo.FindByID(ctx, transactionID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return uuid.Nil, fmt.Errorf("%w: %s", ErrTransactionNotFound, transactionID)
		}

		return uuid.Nil, fmt.Errorf("find transaction: %w", err)
	}

	if tx == nil {
		return uuid.Nil, fmt.Errorf("%w: %s", ErrTransactionNotFound, transactionID)
	}

	return gateway.resolveContextIDFromTransaction(ctx, tx)
}

func (gateway *ExceptionMatchingGateway) resolveContextIDFromTransaction(
	ctx context.Context,
	tx *shared.Transaction,
) (uuid.UUID, error) {
	if tx == nil {
		return uuid.Nil, ErrTransactionNotFound
	}

	contextID, jobErr := gateway.resolveViaIngestionJob(ctx, tx.IngestionJobID)
	if jobErr == nil {
		return contextID, nil
	}

	if gateway.sourceFinder == nil || tx.SourceID == uuid.Nil {
		return uuid.Nil, jobErr
	}

	fallbackContextID, sourceErr := gateway.resolveViaSource(ctx, tx.SourceID)
	if sourceErr == nil {
		return fallbackContextID, nil
	}

	return uuid.Nil, mapSourceLookupError(jobErr, sourceErr)
}

func (gateway *ExceptionMatchingGateway) resolveViaSource(
	ctx context.Context,
	sourceID uuid.UUID,
) (uuid.UUID, error) {
	contextID, err := gateway.sourceFinder.GetContextIDBySourceID(ctx, sourceID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("resolve context via source: %w", err)
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

func (gateway *ExceptionMatchingGateway) resolveViaIngestionJob(
	ctx context.Context,
	ingestionJobID uuid.UUID,
) (uuid.UUID, error) {
	job, err := gateway.jobFinder.FindByID(ctx, ingestionJobID)
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
