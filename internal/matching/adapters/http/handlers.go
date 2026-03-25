// Package http provides HTTP handlers for the matching domain.
package http

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/matching/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	"github.com/LerianStudio/matcher/internal/matching/services/command"
	matchingQuery "github.com/LerianStudio/matcher/internal/matching/services/query"
)

// Pagination constants.
const (
	sortOrderDesc                   = "desc"
	minTransactionIDsForManualMatch = 2
)

// productionMode indicates whether the application is running in production.
// Set once during handler construction via NewHandler; governs SafeError behavior
// (suppresses internal error details in client responses when true).
// Uses atomic.Bool because parallel tests construct handlers concurrently.
var productionMode atomic.Bool

// Handler handles HTTP requests for matching operations.
type Handler struct {
	command                 *command.UseCase
	query                   *matchingQuery.UseCase
	contextProvider         contextProvider
	contextVerifier         libHTTP.TenantOwnershipVerifier
	resourceContextVerifier libHTTP.ResourceOwnershipVerifier
}

type contextProvider interface {
	FindByID(
		ctx context.Context,
		tenantID, contextID uuid.UUID,
	) (*ports.ReconciliationContextInfo, error)
}

// ErrNilCommandUseCase indicates a missing command use case.
var ErrNilCommandUseCase = errors.New("command use case is required")

// ErrNilQueryUseCase indicates a missing query use case.
var ErrNilQueryUseCase = errors.New("query use case is required")

// ErrNilContextProvider indicates a missing context provider.
var ErrNilContextProvider = errors.New("context provider is required")

var (
	// ErrMatchRunResponseNil indicates a missing match run response.
	ErrMatchRunResponseNil = errors.New("match run response is nil")
	// ErrInvalidSortOrder indicates sort order parameter is invalid.
	ErrInvalidSortOrder = errors.New("invalid sort_order")
	// ErrInvalidSortBy indicates sort by parameter is invalid for cursor pagination.
	ErrInvalidSortBy    = errors.New("invalid sort_by")
	errMissingParameter = errors.New("missing parameter")
	errForbidden        = errors.New("forbidden")
)

// RunMatchRequest defines the payload to trigger a matching run.
type RunMatchRequest struct {
	Mode string `json:"mode" validate:"required,oneof=DRY_RUN COMMIT" example:"DRY_RUN" enums:"DRY_RUN,COMMIT"`
}

// RunMatchResponse defines the response payload for a matching run.
type RunMatchResponse struct {
	RunID  uuid.UUID `json:"runId"  format:"uuid" example:"550e8400-e29b-41d4-a716-446655440000"`
	Status string    `json:"status"               example:"PROCESSING"                           enums:"PROCESSING,COMPLETED,FAILED"`
}

// UnmatchRequest defines the payload to break/unmatch a match group.
type UnmatchRequest struct {
	Reason string `json:"reason" validate:"required" example:"incorrect match - amounts do not match"`
}

// CreateManualMatchRequest defines the payload to create a manual match.
type CreateManualMatchRequest struct {
	TransactionIDs []string `json:"transactionIds"  validate:"required,min=2,max=50,dive,uuid4" minItems:"2" maxItems:"50" example:"550e8400-e29b-41d4-a716-446655440000"`
	Notes          string   `json:"notes,omitempty"                                      example:"Manual match for Q4 reconciliation"`
}

// ManualMatchResponse defines the response for a manual match creation.
type ManualMatchResponse struct {
	MatchGroup *dto.MatchGroupResponse `json:"matchGroup"`
}

// CreateAdjustmentRequest defines the payload to create a balancing adjustment.
type CreateAdjustmentRequest struct {
	MatchGroupID  string `json:"matchGroupId,omitempty"  format:"uuid" example:"550e8400-e29b-41d4-a716-446655440000"`
	TransactionID string `json:"transactionId,omitempty" format:"uuid" example:"550e8400-e29b-41d4-a716-446655440000"`
	Type          string `json:"type"                                  example:"BANK_FEE"                             validate:"required,oneof=BANK_FEE FX_DIFFERENCE ROUNDING WRITE_OFF MISCELLANEOUS" enums:"BANK_FEE,FX_DIFFERENCE,ROUNDING,WRITE_OFF,MISCELLANEOUS"`
	Direction     string `json:"direction"                             example:"DEBIT"                                validate:"required,oneof=DEBIT CREDIT"                                            enums:"DEBIT,CREDIT"`
	Amount        string `json:"amount"                                example:"10.50"                                validate:"required,positive_amount"`
	Currency      string `json:"currency"                              example:"USD"                                  validate:"required,len=3"`
	Description   string `json:"description"                           example:"Bank wire fee adjustment"             validate:"required"`
	Reason        string `json:"reason"                                example:"Variance due to bank processing fee"  validate:"required"`
}

// AdjustmentResponse defines the response for an adjustment creation.
type AdjustmentResponse struct {
	Adjustment *dto.AdjustmentResponse `json:"adjustment"`
}

// ErrReasonRequired indicates the reason field is missing.
var ErrReasonRequired = errors.New("reason is required")

// NewHandler builds a matching HTTP handler.
func NewHandler(
	commandUseCase *command.UseCase,
	queryUseCase *matchingQuery.UseCase,
	ctxProvider contextProvider,
	production bool,
) (*Handler, error) {
	if commandUseCase == nil {
		return nil, ErrNilCommandUseCase
	}

	if queryUseCase == nil {
		return nil, ErrNilQueryUseCase
	}

	if ctxProvider == nil {
		return nil, ErrNilContextProvider
	}

	productionMode.Store(production)

	verifier := NewTenantOwnershipVerifier(ctxProvider)
	resourceVerifier := NewResourceContextVerifier(ctxProvider, auth.GetTenantID)

	return &Handler{
		command:                 commandUseCase,
		query:                   queryUseCase,
		contextProvider:         ctxProvider,
		contextVerifier:         verifier,
		resourceContextVerifier: resourceVerifier,
	}, nil
}

func startHandlerSpan(c *fiber.Ctx, name string) (context.Context, trace.Span, libLog.Logger) {
	ctx := c.UserContext()
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	if tracer == nil {
		tracer = otel.Tracer("commons.default")
	}

	ctx, span := tracer.Start(ctx, name)

	return ctx, span, logger
}

func logSpanError(ctx context.Context, span trace.Span, logger libLog.Logger, message string, err error) {
	libOpentelemetry.HandleSpanError(span, message, err)
	libLog.SafeError(logger, ctx, message, err, productionMode.Load())
}

func badRequest(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	message string,
	err error,
) error {
	logSpanError(ctx, span, logger, message, err)

	return libHTTP.RespondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", message)
}

func writeServiceError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	message string,
	err error,
) error {
	logSpanError(ctx, span, logger, message, err)

	return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
}

func writeNotFound(fiberCtx *fiber.Ctx, message string) error {
	return libHTTP.RespondError(fiberCtx, fiber.StatusNotFound, "not_found", message)
}

func forbidden(ctx context.Context, fiberCtx *fiber.Ctx, span trace.Span, logger libLog.Logger, err error) error {
	const message = "access denied"

	if err == nil {
		err = fmt.Errorf("%w: %s", errForbidden, message)
	}

	libOpentelemetry.HandleSpanError(span, message, err)

	logger.Log(ctx, libLog.LevelWarn, "access denied: "+message)

	return libHTTP.RespondError(fiberCtx, fiber.StatusForbidden, "forbidden", message)
}

// handleContextVerificationError handles errors from ParseAndVerifyTenantScopedID.
// Returns (shouldReturn, error) where shouldReturn indicates if the caller should return.
func handleContextVerificationError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) (bool, error) {
	if err == nil {
		return false, nil
	}

	switch {
	case errors.Is(err, libHTTP.ErrMissingContextID),
		errors.Is(err, libHTTP.ErrInvalidContextID):
		return true, badRequest(ctx, fiberCtx, span, logger, "invalid context id", err)

	case errors.Is(err, libHTTP.ErrTenantIDNotFound),
		errors.Is(err, libHTTP.ErrInvalidTenantID):
		return true, libHTTP.RespondError(fiberCtx, fiber.StatusUnauthorized, "unauthorized", "unauthorized")

	case errors.Is(err, libHTTP.ErrContextNotFound):
		return true, writeNotFound(fiberCtx, "context not found")

	case errors.Is(err, libHTTP.ErrContextNotActive):
		libOpentelemetry.HandleSpanError(span, "context not active", err)

		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("context not active: %v", err))

		return true, libHTTP.RespondError(
			fiberCtx,
			fiber.StatusForbidden,
			"context_not_active",
			"context is not active",
		)

	case errors.Is(err, libHTTP.ErrContextNotOwned),
		errors.Is(err, libHTTP.ErrContextAccessDenied):
		return true, forbidden(ctx, fiberCtx, span, logger, err)

	// Infrastructure lookup failures (e.g. database errors during ownership check) → 500
	case errors.Is(err, libHTTP.ErrContextLookupFailed):
		return true, writeServiceError(ctx, fiberCtx, span, logger, "context verification failed", err)

	default:
		return true, writeServiceError(ctx, fiberCtx, span, logger, "context verification failed", err)
	}
}

// handleContextQueryVerificationError handles errors from ParseAndVerifyResourceScopedID.
// Returns (shouldReturn, error) where shouldReturn indicates if the caller should return.
func handleContextQueryVerificationError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) (bool, error) {
	if err == nil {
		return false, nil
	}

	switch {
	case errors.Is(err, libHTTP.ErrMissingContextID),
		errors.Is(err, libHTTP.ErrInvalidContextID):
		return true, badRequest(ctx, fiberCtx, span, logger, "invalid context id", err)

	case errors.Is(err, libHTTP.ErrContextNotActive):
		libOpentelemetry.HandleSpanError(span, "context not active", err)

		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("context not active: %v", err))

		return true, libHTTP.RespondError(
			fiberCtx,
			fiber.StatusForbidden,
			"context_not_active",
			"context is not active",
		)

	case errors.Is(err, libHTTP.ErrTenantIDNotFound),
		errors.Is(err, libHTTP.ErrInvalidTenantID):
		return true, libHTTP.RespondError(fiberCtx, fiber.StatusUnauthorized, "unauthorized", "unauthorized")

	case errors.Is(err, libHTTP.ErrContextNotFound):
		return true, writeNotFound(fiberCtx, "context not found")

	case errors.Is(err, libHTTP.ErrContextNotOwned),
		errors.Is(err, libHTTP.ErrContextAccessDenied):
		return true, forbidden(ctx, fiberCtx, span, logger, err)

	// Infrastructure lookup failures (e.g. database errors during ownership check) → 500
	case errors.Is(err, libHTTP.ErrLookupFailed),
		errors.Is(err, libHTTP.ErrContextLookupFailed):
		return true, writeServiceError(ctx, fiberCtx, span, logger, "context query verification failed", err)

	default:
		return true, writeServiceError(
			ctx,
			fiberCtx,
			span,
			logger,
			"context query verification failed",
			err,
		)
	}
}

func parseUUIDParam(c *fiber.Ctx, name string) (uuid.UUID, error) {
	value := c.Params(name)
	if value == "" {
		return uuid.Nil, errMissingParameter
	}

	id, err := uuid.Parse(value)
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid %s param: %w", name, err)
	}

	return id, nil
}

func parseOptionalUUID(s string) (*uuid.UUID, error) {
	if s == "" {
		return nil, nil
	}

	id, err := uuid.Parse(s)
	if err != nil {
		return nil, fmt.Errorf("parse uuid: %w", err)
	}

	return &id, nil
}
