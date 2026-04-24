// Package command provides exception command use cases (resolution, disputes,
// dispatch, comments, callbacks). The merged ExceptionUseCase groups all
// command operations on the exception bounded context behind a single entry
// point. Required dependencies (exception repo, actor extractor, audit
// publisher, infra provider) are constructor arguments. Specialised
// dependencies used only by a subset of operations (resolution executor,
// dispute repo, comment repo, external connector, idempotency repo, rate
// limiter) are wired via UseCaseOption.
package command

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

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
	ErrUnexpectedNilResult       = errors.New("unexpected nil result")
	ErrNilExternalConnector      = errors.New("external connector is required")
)

// ExceptionUseCase groups every write operation on the exception bounded
// context: resolution (ForceMatch/AdjustEntry/BulkAssign/BulkResolve),
// disputes (Open/Close/SubmitEvidence), external dispatch
// (Dispatch/BulkDispatch), comments (AddComment/DeleteComment), and
// inbound callbacks (ProcessCallback). Required dependencies are set via
// the constructor; specialised dependencies used only by a subset of
// operations are wired through UseCaseOption.
type ExceptionUseCase struct {
	// Required dependencies shared by most command paths.
	exceptionRepo  repositories.ExceptionRepository
	actorExtractor ports.ActorExtractor
	auditPublisher ports.AuditPublisher
	infraProvider  sharedPorts.InfrastructureProvider

	// Optional dependencies: each operation that needs one checks for nil
	// and returns the matching sentinel so callers receive a clear error
	// when a required option was not wired.
	resolutionExecutor ports.ResolutionExecutor
	disputeRepo        repositories.DisputeRepository
	commentRepo        repositories.CommentRepository
	connector          ports.ExternalConnector
	idempotencyRepo    sharedPorts.IdempotencyRepository
	rateLimiter        ports.CallbackRateLimiter

	// OTel metrics initialised once at construction time.
	revertFailedCounter metric.Int64Counter
}

// UseCaseOption configures optional dependencies on the merged
// ExceptionUseCase. Nil values are ignored so callers can pass results of
// conditional setup without guarding at the call site.
type UseCaseOption func(*ExceptionUseCase)

// WithResolutionExecutor sets the resolution executor used by ForceMatch,
// AdjustEntry, BulkAssign and BulkResolve.
func WithResolutionExecutor(executor ports.ResolutionExecutor) UseCaseOption {
	return func(uc *ExceptionUseCase) {
		if executor != nil {
			uc.resolutionExecutor = executor
		}
	}
}

// WithDisputeRepository sets the dispute repository used by OpenDispute,
// CloseDispute and SubmitEvidence.
func WithDisputeRepository(repo repositories.DisputeRepository) UseCaseOption {
	return func(uc *ExceptionUseCase) {
		if repo != nil {
			uc.disputeRepo = repo
		}
	}
}

// WithCommentRepository sets the comment repository used by AddComment and
// DeleteComment.
func WithCommentRepository(repo repositories.CommentRepository) UseCaseOption {
	return func(uc *ExceptionUseCase) {
		if repo != nil {
			uc.commentRepo = repo
		}
	}
}

// WithExternalConnector sets the external connector used by Dispatch and
// BulkDispatch.
func WithExternalConnector(connector ports.ExternalConnector) UseCaseOption {
	return func(uc *ExceptionUseCase) {
		if connector != nil {
			uc.connector = connector
		}
	}
}

// WithIdempotencyRepository sets the idempotency repository used by
// ProcessCallback.
func WithIdempotencyRepository(repo sharedPorts.IdempotencyRepository) UseCaseOption {
	return func(uc *ExceptionUseCase) {
		if !sharedPorts.IsNilValue(repo) {
			uc.idempotencyRepo = repo
		}
	}
}

// WithCallbackRateLimiter sets the rate limiter used by ProcessCallback.
func WithCallbackRateLimiter(limiter ports.CallbackRateLimiter) UseCaseOption {
	return func(uc *ExceptionUseCase) {
		if !sharedPorts.IsNilValue(limiter) {
			uc.rateLimiter = limiter
		}
	}
}

// NewExceptionUseCase creates a new ExceptionUseCase with the required
// dependencies. Specialised dependencies are wired via UseCaseOption.
// Methods that need a specialised dependency not wired by the caller will
// return the matching ErrNil* sentinel.
func NewExceptionUseCase(
	repo repositories.ExceptionRepository,
	actor ports.ActorExtractor,
	audit ports.AuditPublisher,
	infraProvider sharedPorts.InfrastructureProvider,
	opts ...UseCaseOption,
) (*ExceptionUseCase, error) {
	if repo == nil {
		return nil, ErrNilExceptionRepository
	}

	if actor == nil {
		return nil, ErrNilActorExtractor
	}

	if audit == nil {
		return nil, ErrNilAuditPublisher
	}

	if infraProvider == nil {
		return nil, ErrNilInfraProvider
	}

	uc := &ExceptionUseCase{
		exceptionRepo:  repo,
		actorExtractor: actor,
		auditPublisher: audit,
		infraProvider:  infraProvider,
	}

	for _, opt := range opts {
		if opt != nil {
			opt(uc)
		}
	}

	if err := uc.initMetrics(); err != nil {
		return nil, fmt.Errorf("init exception command metrics: %w", err)
	}

	return uc, nil
}

// initMetrics creates the OTel metric instruments for exception command operations.
func (uc *ExceptionUseCase) initMetrics() error {
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

// isBusinessError returns true when the error originates from a domain/business
// rule violation (not-found, nil entity, invalid state transition, pending state).
// Infrastructure failures (database, transaction, network) are NOT business errors
// and should be reported with HandleSpanError for proper telemetry classification.
func isBusinessError(err error) bool {
	return errors.Is(err, entities.ErrExceptionNotFound) ||
		errors.Is(err, entities.ErrExceptionNil) ||
		errors.Is(err, entities.ErrExceptionMustBeOpenToAssign) ||
		errors.Is(err, entities.ErrExceptionMustBeOpenOrAssignedToResolve) ||
		errors.Is(err, entities.ErrExceptionPendingResolution) ||
		errors.Is(err, entities.ErrAssigneeRequired) ||
		errors.Is(err, entities.ErrResolutionNotesRequired) ||
		errors.Is(err, value_objects.ErrInvalidResolutionTransition) ||
		errors.Is(err, value_objects.ErrInvalidExceptionStatus)
}

// executeWithRevert executes a gateway operation and reverts exception status on failure.
// This helper reduces cyclomatic complexity and nested if blocks in resolution commands.
func (uc *ExceptionUseCase) executeWithRevert(
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
func (uc *ExceptionUseCase) revertExceptionStatus(
	ctx context.Context,
	exception *entities.Exception,
	previousStatus value_objects.ExceptionStatus,
	logger libLog.Logger,
) {
	span := trace.SpanFromContext(ctx)

	if abortErr := exception.AbortResolution(ctx, previousStatus); abortErr != nil {
		libLog.SafeError(logger, ctx, "failed to abort resolution", abortErr, runtime.IsProductionMode())

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
		libLog.SafeError(logger, ctx, "failed to revert exception status", updateErr, runtime.IsProductionMode())

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
