// Package http provides HTTP handlers for configuration management.
package http

import (
	"context"
	"database/sql"
	"errors"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/configuration/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/repositories"
	configPorts "github.com/LerianStudio/matcher/internal/configuration/ports"
	"github.com/LerianStudio/matcher/internal/configuration/services/command"
	"github.com/LerianStudio/matcher/internal/configuration/services/query"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Configuration handler errors.
var (
	// ErrNilCommandUseCase is returned when the command use case is nil.
	ErrNilCommandUseCase = errors.New("command use case is required")
	// ErrNilQueryUseCase is returned when the query use case is nil.
	ErrNilQueryUseCase = errors.New("query use case is required")
	// ErrNilContextRepository is returned when the context repository is nil.
	ErrNilContextRepository = errors.New("context repository is required")
	// ErrNilSourceRepository is returned when the source repository is nil.
	ErrNilSourceRepository = errors.New("source repository is required")
	// ErrNilMatchRuleRepository is returned when the match rule repository is nil.
	ErrNilMatchRuleRepository = errors.New("match rule repository is required")
	// ErrNilFieldMapRepository is returned when the field map repository is nil.
	ErrNilFieldMapRepository = errors.New("field map repository is required")
	// ErrNilFeeRuleRepository is returned when the fee rule repository is nil.
	ErrNilFeeRuleRepository = errors.New("fee rule repository is required")
	// ErrNilFeeScheduleRepository is returned when the fee schedule repository is nil.
	ErrNilFeeScheduleRepository = errors.New("fee schedule repository is required")
	// ErrNilScheduleRepository is returned when the schedule repository is nil.
	ErrNilScheduleRepository = errors.New("schedule repository is required")
	// ErrRuleIDsRequired is returned when rule IDs are not provided.
	ErrRuleIDsRequired = errors.New("rule IDs are required")
)

// Handler handles HTTP requests for configuration operations.
//
// productionMode governs SafeError behavior (suppresses internal error
// details in client responses when true). Stored as a per-handler bool
// rather than a package-level atomic.Bool — the previous shared-global
// state coupled every test in the package to whichever test last
// constructed a handler, regardless of the production flag each test
// wanted to exercise.
//
// The seven repository fields back single-aggregate GET handlers, list/count
// handlers, and the tenant ownership verifier directly. They bypass the query
// UseCase because the corresponding query methods were span-only wrappers
// around the repo; calling the repositories here removes a redundant layer
// without losing observability (the postgres adapter still emits its own span).
type Handler struct {
	command         *command.UseCase
	query           *query.UseCase
	contextRepo     repositories.ContextRepository
	sourceRepo      repositories.SourceRepository
	matchRuleRepo   repositories.MatchRuleRepository
	fieldMapRepo    repositories.FieldMapRepository
	feeRuleRepo     repositories.FeeRuleRepository
	feeScheduleRepo sharedPorts.FeeScheduleRepository
	scheduleRepo    configPorts.ScheduleRepository
	contextVerifier libHTTP.TenantOwnershipVerifier
	productionMode  bool
}

func startHandlerSpan(c *fiber.Ctx, name string) (context.Context, trace.Span, libLog.Logger) {
	return sharedhttp.StartHandlerSpan(c, name)
}

func (handler *Handler) logSpanError(ctx context.Context, span trace.Span, logger libLog.Logger, message string, err error) {
	sharedhttp.LogSpanError(ctx, span, logger, handler.productionMode, message, err)
}

//nolint:wrapcheck // HTTP transport response is the terminal error boundary.
func (handler *Handler) badRequest(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	message string,
	err error,
) error {
	return sharedhttp.BadRequest(ctx, fiberCtx, span, logger, handler.productionMode, safeClientMessage(message, err), err)
}

func (handler *Handler) unauthorized(ctx context.Context, c *fiber.Ctx, span trace.Span, logger libLog.Logger, err error) error {
	handler.logSpanError(ctx, span, logger, "invalid tenant id", err)
	return respondError(c, fiber.StatusUnauthorized, "unauthorized", "unauthorized")
}

func writeNotFound(c *fiber.Ctx, slug, message string) error {
	return respondError(c, fiber.StatusNotFound, slug, message)
}

//nolint:wrapcheck // HTTP transport response is the terminal error boundary.
func respondError(fiberCtx *fiber.Ctx, status int, slug, message string) error {
	return sharedhttp.RespondError(fiberCtx, status, slug, message)
}

//nolint:wrapcheck // HTTP transport response is the terminal error boundary.
func respondContextVerificationError(fiberCtx *fiber.Ctx, err error) error {
	return sharedhttp.RespondProductError(
		fiberCtx,
		sharedhttp.ValidateContextVerificationError(
			err,
			sharedhttp.WithContextNotFound("configuration_context_not_found", "context not found"),
		),
	)
}

//nolint:wrapcheck // HTTP transport response is the terminal error boundary.
func respondOwnershipVerificationError(fiberCtx *fiber.Ctx, err error, notFoundSlug, notFoundMessage string) error {
	return sharedhttp.RespondProductError(
		fiberCtx,
		sharedhttp.ValidateContextVerificationError(
			err,
			sharedhttp.WithContextNotFound(notFoundSlug, notFoundMessage),
			sharedhttp.WithHiddenContextOwnershipAsNotFound(notFoundSlug, notFoundMessage),
		),
	)
}

func safeClientMessage(defaultMsg string, err error) string {
	if err == nil {
		return defaultMsg
	}

	if isClientSafeError(err) {
		return err.Error()
	}

	return defaultMsg
}

func (handler *Handler) ensureSourceAccess(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	contextID, sourceID uuid.UUID,
) error {
	_, err := handler.sourceRepo.FindByID(ctx, contextID, sourceID)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "failed to load source", err)

		if errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "configuration_source_not_found", "source not found")
		}

		return writeServiceError(fiberCtx, err)
	}

	return nil
}

func (handler *Handler) handleContextVerificationError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) error {
	handler.logSpanError(ctx, span, logger, "context verification failed", err)

	if errors.Is(err, libHTTP.ErrContextNotFound) {
		return writeNotFound(fiberCtx, "configuration_context_not_found", "context not found")
	}

	return respondContextVerificationError(fiberCtx, err)
}

func (handler *Handler) handleOwnershipVerificationError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
	notFoundSlug, notFoundMessage string,
) error {
	handler.logSpanError(ctx, span, logger, "context ownership verification failed", err)

	return respondOwnershipVerificationError(fiberCtx, err, notFoundSlug, notFoundMessage)
}

// NewHandler creates a new configuration handler.
//
// The seven repositories are required in addition to the command/query UseCases
// because the tenant ownership verifier, the single-aggregate GET handlers,
// and the list/count handlers read through the repositories directly. See the
// Handler doc comment for the rationale.
func NewHandler(
	commandUseCase *command.UseCase,
	queryUseCase *query.UseCase,
	contextRepo repositories.ContextRepository,
	sourceRepo repositories.SourceRepository,
	matchRuleRepo repositories.MatchRuleRepository,
	fieldMapRepo repositories.FieldMapRepository,
	feeRuleRepo repositories.FeeRuleRepository,
	feeScheduleRepo sharedPorts.FeeScheduleRepository,
	scheduleRepo configPorts.ScheduleRepository,
	production bool,
) (*Handler, error) {
	if commandUseCase == nil {
		return nil, ErrNilCommandUseCase
	}

	if queryUseCase == nil {
		return nil, ErrNilQueryUseCase
	}

	if contextRepo == nil {
		return nil, ErrNilContextRepository
	}

	if sourceRepo == nil {
		return nil, ErrNilSourceRepository
	}

	if matchRuleRepo == nil {
		return nil, ErrNilMatchRuleRepository
	}

	if fieldMapRepo == nil {
		return nil, ErrNilFieldMapRepository
	}

	if feeRuleRepo == nil {
		return nil, ErrNilFeeRuleRepository
	}

	if feeScheduleRepo == nil {
		return nil, ErrNilFeeScheduleRepository
	}

	if scheduleRepo == nil {
		return nil, ErrNilScheduleRepository
	}

	return &Handler{
		command:         commandUseCase,
		query:           queryUseCase,
		contextRepo:     contextRepo,
		sourceRepo:      sourceRepo,
		matchRuleRepo:   matchRuleRepo,
		fieldMapRepo:    fieldMapRepo,
		feeRuleRepo:     feeRuleRepo,
		feeScheduleRepo: feeScheduleRepo,
		scheduleRepo:    scheduleRepo,
		contextVerifier: NewTenantOwnershipVerifier(contextRepo),
		productionMode:  production,
	}, nil
}

// boolDefault returns the value of b if non-nil, or the default value otherwise.
func boolDefault(b *bool, defaultVal bool) bool {
	if b == nil {
		return defaultVal
	}

	return *b
}

func mapUpdateContextError(fiberCtx *fiber.Ctx, err error) error {
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return writeNotFound(fiberCtx, "configuration_context_not_found", "context not found")
	case errors.Is(err, command.ErrContextNameAlreadyExists):
		return respondError(fiberCtx, fiber.StatusConflict, "duplicate_name", err.Error())
	case errors.Is(err, entities.ErrInvalidStateTransition):
		return respondError(fiberCtx, fiber.StatusConflict, "invalid_state_transition", err.Error())
	case errors.Is(err, entities.ErrArchivedContextCannotBeModified):
		return respondError(fiberCtx, fiber.StatusConflict, "archived_context", err.Error())
	default:
		return writeServiceError(fiberCtx, err)
	}
}

func writeServiceError(fiberCtx *fiber.Ctx, err error) error {
	message := clientErrorMessage(err)
	if isClientSafeError(err) {
		return respondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", message)
	}

	return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
}

func clientErrorMessage(err error) string {
	return safeClientMessage("request_failed", err)
}

func isClientSafeError(err error) bool {
	safeErrors := []error{
		dto.ErrDeprecatedRateID,
		entities.ErrNilReconciliationContext,
		entities.ErrContextNameRequired,
		entities.ErrContextNameTooLong,
		entities.ErrContextTypeInvalid,
		entities.ErrContextStatusInvalid,
		entities.ErrContextIntervalRequired,
		entities.ErrContextTenantRequired,
		entities.ErrSourceNameRequired,
		entities.ErrSourceNameTooLong,
		entities.ErrSourceTypeInvalid,
		entities.ErrSourceContextRequired,
		entities.ErrSourceSideRequired,
		entities.ErrSourceSideInvalid,
		shared.ErrFieldMapNil,
		shared.ErrFieldMapContextRequired,
		shared.ErrFieldMapSourceRequired,
		shared.ErrFieldMapMappingRequired,
		shared.ErrFieldMapMappingValueEmpty,
		entities.ErrMatchRuleNil,
		entities.ErrRuleContextRequired,
		entities.ErrRulePriorityInvalid,
		entities.ErrRuleTypeInvalid,
		entities.ErrRuleConfigRequired,
		entities.ErrRuleConfigMissingRequiredKeys,
		entities.ErrRulePriorityConflict,
	}
	for _, safeErr := range safeErrors {
		if errors.Is(err, safeErr) {
			return true
		}
	}

	return false
}
