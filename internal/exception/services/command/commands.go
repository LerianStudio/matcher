// Package command provides exception resolution command use cases.
package command

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/exception/ports"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Command use case errors.
var (
	ErrNilExceptionRepository   = errors.New("exception repository is required")
	ErrNilResolutionExecutor    = errors.New("resolution executor is required")
	ErrNilAuditPublisher        = errors.New("audit publisher is required")
	ErrNilActorExtractor        = errors.New("actor extractor is required")
	ErrNilInfraProvider         = errors.New("infrastructure provider is required")
	ErrNilIdempotencyRepository = errors.New(
		"idempotency repository is required for callback operations",
	)
	ErrExceptionIDRequired        = errors.New("exception id is required")
	ErrActorRequired              = errors.New("actor is required")
	ErrZeroAdjustmentAmount       = errors.New("adjustment amount cannot be zero")
	ErrNegativeAdjustmentAmount   = errors.New("adjustment amount cannot be negative")
	ErrInvalidCurrency            = errors.New("invalid currency code")
	ErrNilDisputeRepository       = errors.New("dispute repository is required")
	ErrDisputeIDRequired          = errors.New("dispute id is required")
	ErrDisputeCategoryRequired    = errors.New("dispute category is required")
	ErrDisputeDescriptionRequired = errors.New("dispute description is required")
	ErrDisputeCommentRequired     = errors.New("evidence comment is required")
	ErrDisputeResolutionRequired  = errors.New("dispute resolution is required")
	ErrCallbackExternalSystem     = errors.New("external system is required")
	ErrCallbackExternalIssueID    = errors.New("external issue id is required")
	ErrCallbackStatusRequired     = errors.New("callback status is required")
	ErrCallbackAssigneeRequired   = errors.New("callback assignee is required")
	ErrCallbackStatusUnsupported  = errors.New("callback status is unsupported")
	ErrCallbackOpenNotValidTarget = errors.New(
		"OPEN is not a valid callback resolution target: use ASSIGNED or RESOLVED",
	)
	ErrCallbackRateLimitExceeded = errors.New("callback rate limit exceeded")
	ErrNilCallbackRateLimiter    = errors.New("callback rate limiter is required")
	ErrCallbackInProgress        = errors.New("callback is already being processed")
	ErrCallbackRetryable         = errors.New("callback can be retried")
)

// UseCase implements exception resolution commands.
type UseCase struct {
	exceptionRepo      repositories.ExceptionRepository
	resolutionExecutor ports.ResolutionExecutor
	auditPublisher     ports.AuditPublisher
	actorExtractor     ports.ActorExtractor
	infraProvider      sharedPorts.InfrastructureProvider

	// OTel metrics (initialized once at construction time)
	revertFailedCounter metric.Int64Counter
}

// NewUseCase creates a new UseCase with the required dependencies.
func NewUseCase(
	repo repositories.ExceptionRepository,
	executor ports.ResolutionExecutor,
	audit ports.AuditPublisher,
	actor ports.ActorExtractor,
	infraProvider sharedPorts.InfrastructureProvider,
) (*UseCase, error) {
	if repo == nil {
		return nil, ErrNilExceptionRepository
	}

	if executor == nil {
		return nil, ErrNilResolutionExecutor
	}

	if audit == nil {
		return nil, ErrNilAuditPublisher
	}

	if actor == nil {
		return nil, ErrNilActorExtractor
	}

	if infraProvider == nil {
		return nil, ErrNilInfraProvider
	}

	uc := &UseCase{
		exceptionRepo:      repo,
		resolutionExecutor: executor,
		auditPublisher:     audit,
		actorExtractor:     actor,
		infraProvider:      infraProvider,
	}

	if err := uc.initMetrics(); err != nil {
		return nil, fmt.Errorf("init exception command metrics: %w", err)
	}

	return uc, nil
}

// initMetrics creates the OTel metric instruments for exception command operations.
func (uc *UseCase) initMetrics() error {
	meter := otel.Meter("matcher.exception.command")

	var err error

	uc.revertFailedCounter, err = meter.Int64Counter(
		"exception.status_revert_failed_total",
		metric.WithDescription("Count of failed exception status reverts from PENDING_RESOLUTION"),
	)
	if err != nil {
		return fmt.Errorf("create exception.status_revert_failed_total counter: %w", err)
	}

	return nil
}

// executeWithRevert executes a gateway operation and reverts exception status on failure.
// This helper reduces cyclomatic complexity and nested if blocks in resolution commands.
func (uc *UseCase) executeWithRevert(
	ctx context.Context,
	exception *entities.Exception,
	previousStatus value_objects.ExceptionStatus,
	gatewayFn func() error,
	logger libLog.Logger,
) error {
	if err := gatewayFn(); err != nil {
		// Attempt to revert status on gateway failure.
		uc.revertExceptionStatus(ctx, exception, previousStatus, logger)

		return err
	}

	return nil
}

// revertExceptionStatus attempts to abort resolution and update exception status.
// Logs errors but does not return them - revert is best-effort.
// Failures are also recorded on the active span for observability.
func (uc *UseCase) revertExceptionStatus(
	ctx context.Context,
	exception *entities.Exception,
	previousStatus value_objects.ExceptionStatus,
	logger libLog.Logger,
) {
	span := trace.SpanFromContext(ctx)

	if abortErr := exception.AbortResolution(ctx, previousStatus); abortErr != nil {
		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to abort resolution: %v", abortErr))

		libOpentelemetry.HandleSpanError(
			span,
			fmt.Sprintf("status revert failed: abort resolution from %s", previousStatus),
			abortErr,
		)

		uc.revertFailedCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("exception_id", exception.ID.String()),
			attribute.String("target_status", string(previousStatus)),
			attribute.String("failure_phase", "abort_resolution"),
		))

		return
	}

	if _, updateErr := uc.exceptionRepo.Update(ctx, exception); updateErr != nil {
		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to revert exception status: %v", updateErr))

		libOpentelemetry.HandleSpanError(
			span,
			fmt.Sprintf("status revert failed: persist revert to %s", previousStatus),
			updateErr,
		)

		uc.revertFailedCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("exception_id", exception.ID.String()),
			attribute.String("target_status", string(previousStatus)),
			attribute.String("failure_phase", "persist_revert"),
		))
	}
}
