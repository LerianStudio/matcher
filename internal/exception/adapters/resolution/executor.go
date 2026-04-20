// Package resolution provides adapters for executing exception resolution actions.
//
// NOTE: This executor straddles the adapter/service boundary by design. It orchestrates
// between domain repositories and the matching gateway, and co-locates direction
// determination logic (deriving DEBIT/CREDIT from amount sign) for cohesion with the
// gateway call that consumes it. Future refactoring could extract this into services/
// or domain/services/ if the business logic grows beyond simple direction mapping.
package resolution

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/exception/ports"
)

// Sentinel errors for executor operations.
var (
	ErrNilExceptionRepository = errors.New("exception repository is required")
	ErrNilMatchingGateway     = errors.New("matching gateway is required")
	ErrNilActorExtractor      = errors.New("actor extractor is required")
)

// Executor implements ResolutionExecutor by coordinating with the matching context.
type Executor struct {
	exceptionRepo   repositories.ExceptionRepository
	matchingGateway ports.MatchingGateway
	actorExtractor  ports.ActorExtractor
}

// NewExecutor creates a new resolution executor with required dependencies.
func NewExecutor(
	exceptionRepo repositories.ExceptionRepository,
	matchingGateway ports.MatchingGateway,
	actorExtractor ports.ActorExtractor,
) (*Executor, error) {
	if exceptionRepo == nil {
		return nil, ErrNilExceptionRepository
	}

	if matchingGateway == nil {
		return nil, ErrNilMatchingGateway
	}

	if actorExtractor == nil {
		return nil, ErrNilActorExtractor
	}

	return &Executor{
		exceptionRepo:   exceptionRepo,
		matchingGateway: matchingGateway,
		actorExtractor:  actorExtractor,
	}, nil
}

// ForceMatch executes a force match resolution for an exception.
// This bypasses normal matching rules and marks the transaction as matched.
func (executor *Executor) ForceMatch(
	ctx context.Context,
	exceptionID uuid.UUID,
	notes string,
	overrideReason value_objects.OverrideReason,
) error {
	logger, tracer := trackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "resolution.executor.force_match")

	defer span.End()

	// Look up the exception to get the transaction ID
	exception, err := executor.exceptionRepo.FindByID(ctx, exceptionID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to find exception", err)

		logger.Log(ctx, libLog.LevelError, "force match: failed to find exception",
			libLog.String("exception_id", exceptionID.String()),
		)

		return fmt.Errorf("find exception: %w", err)
	}

	if exception == nil {
		return entities.ErrExceptionNotFound
	}

	// Get actor from context
	actor := executor.actorExtractor.GetActor(ctx)

	// Create force match in matching context
	input := ports.ForceMatchInput{
		ExceptionID:    exceptionID,
		TransactionID:  exception.TransactionID,
		Notes:          notes,
		OverrideReason: string(overrideReason),
		Actor:          actor,
	}

	if err := executor.matchingGateway.CreateForceMatch(ctx, input); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create force match", err)

		logger.Log(ctx, libLog.LevelError, "force match: failed to create force match",
			libLog.String("exception_id", exceptionID.String()),
		)

		return fmt.Errorf("create force match: %w", err)
	}

	logger.Log(ctx, libLog.LevelInfo, "force match executed",
		libLog.String("exception_id", exceptionID.String()),
		libLog.String("transaction_id", exception.TransactionID.String()),
		libLog.String("reason", string(overrideReason)),
	)

	return nil
}

// AdjustEntry executes an adjustment entry resolution for an exception.
// This creates a compensating entry to balance the discrepancy.
func (executor *Executor) AdjustEntry(
	ctx context.Context,
	exceptionID uuid.UUID,
	input ports.AdjustmentInput,
) error {
	logger, tracer := trackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "resolution.executor.adjust_entry")

	defer span.End()

	// Validate input
	if input.Amount.IsZero() {
		return fmt.Errorf("%w: amount cannot be zero", ErrInvalidAdjustment)
	}

	// Look up the exception to get the transaction ID
	exception, err := executor.exceptionRepo.FindByID(ctx, exceptionID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to find exception", err)

		logger.Log(ctx, libLog.LevelError, "adjust entry: failed to find exception",
			libLog.String("exception_id", exceptionID.String()),
		)

		return fmt.Errorf("find exception: %w", err)
	}

	if exception == nil {
		return entities.ErrExceptionNotFound
	}

	// Get actor from context
	actor := executor.actorExtractor.GetActor(ctx)

	// Determine adjustment direction based on amount sign:
	// - Positive amounts are debits (increasing the balance)
	// - Negative amounts are credits (decreasing the balance)
	direction := "DEBIT"
	if input.Amount.IsNegative() {
		direction = "CREDIT"
	}

	// Create adjustment in matching context
	adjustmentInput := ports.CreateAdjustmentInput{
		ExceptionID:   exceptionID,
		TransactionID: exception.TransactionID,
		Direction:     direction,
		Amount:        input.Amount,
		Currency:      input.Currency,
		Reason:        string(input.Reason),
		Notes:         input.Notes,
		Actor:         actor,
	}

	if err := executor.matchingGateway.CreateAdjustment(ctx, adjustmentInput); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create adjustment", err)

		logger.Log(ctx, libLog.LevelError, "adjust entry: failed to create adjustment",
			libLog.String("exception_id", exceptionID.String()),
		)

		return fmt.Errorf("create adjustment: %w", err)
	}

	logger.Log(ctx, libLog.LevelInfo, "adjust entry executed",
		libLog.String("exception_id", exceptionID.String()),
		libLog.String("transaction_id", exception.TransactionID.String()),
		libLog.String("amount", input.Amount.String()),
		libLog.String("currency", input.Currency),
		libLog.String("reason", string(input.Reason)),
	)

	return nil
}

func trackingFromContext(ctx context.Context) (libLog.Logger, trace.Tracer) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	if tracer == nil {
		tracer = noop.NewTracerProvider().Tracer("commons.noop")
	}

	return logger, tracer
}

var _ ports.ResolutionExecutor = (*Executor)(nil)
