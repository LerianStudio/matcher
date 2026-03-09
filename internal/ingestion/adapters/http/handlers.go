// Package http provides HTTP handlers for ingestion operations.
package http

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/ingestion/adapters/http/dto"
	ingestionRepositories "github.com/LerianStudio/matcher/internal/ingestion/domain/repositories"
	"github.com/LerianStudio/matcher/internal/ingestion/services/command"
	"github.com/LerianStudio/matcher/internal/ingestion/services/query"
	sharedpagination "github.com/LerianStudio/matcher/internal/shared/adapters/http"
)

// maxUploadSize is the handler-level file size limit (100MB).
// Note: Fiber's HTTP_BODY_LIMIT_BYTES (configured via bootstrap) may also
// limit request size at the framework level. The stricter of the two limits
// takes effect: Fiber rejects oversized requests before they reach this handler.
const maxUploadSize = 100 * 1024 * 1024

const (
	sortOrderDesc = "desc"
)

var (
	// ErrNilCommandUseCase indicates command use case is nil.
	ErrNilCommandUseCase = errors.New("command use case is required")
	// ErrNilQueryUseCase indicates query use case is nil.
	ErrNilQueryUseCase = errors.New("query use case is required")
	// ErrNilContextProvider indicates context provider is nil.
	ErrNilContextProvider = errors.New("context provider is required")
	// ErrFormatRequired indicates format parameter is missing.
	ErrFormatRequired = errors.New("format is required")
	// ErrInvalidFormat indicates format parameter is invalid.
	ErrInvalidFormat = errors.New("invalid format")
	// ErrInvalidSortOrder indicates sort order parameter is invalid.
	ErrInvalidSortOrder = errors.New("invalid sort_order")
	// ErrInvalidSortBy indicates sort by parameter is invalid for cursor pagination.
	ErrInvalidSortBy = errors.New("invalid sort_by")
	// ErrEmptyFile indicates the uploaded file has zero bytes.
	ErrEmptyFile = errors.New("uploaded file has zero bytes")
	// ErrInvalidContentType indicates the file content type doesn't match declared format.
	ErrInvalidContentType = errors.New("invalid content type")
	// errForbidden indicates access denied.
	errForbidden = errors.New("forbidden")
)

// validJobSortColumns defines allowed sort_by values for job listing endpoints.
var validJobSortColumns = map[string]bool{
	"id": true, "created_at": true, "started_at": true, "completed_at": true, "status": true,
}

// validTransactionSortColumns defines allowed sort_by values for transaction listing endpoints.
var validTransactionSortColumns = map[string]bool{
	"id": true, "created_at": true, "date": true, "status": true, "extraction_status": true,
}

// parseSortOrder extracts and validates the sort_order query parameter.
// Returns the normalized sort order string or an error response.
func parseSortOrder(fiberCtx *fiber.Ctx) (string, error) {
	sortOrder := strings.TrimSpace(fiberCtx.Query("sort_order"))
	if sortOrder == "" {
		return sortOrderDesc, nil
	}

	sortOrder = strings.ToLower(sortOrder)
	if sortOrder != "asc" && sortOrder != sortOrderDesc {
		return "", ErrInvalidSortOrder
	}

	return sortOrder, nil
}

// ReconciliationContextInfo contains the context information needed by ingestion.
type ReconciliationContextInfo struct {
	ID     uuid.UUID
	Active bool
}

type contextProvider interface {
	FindByID(ctx context.Context, tenantID, contextID uuid.UUID) (*ReconciliationContextInfo, error)
}

// productionMode indicates whether the application is running in production.
// Set once during handler construction via NewHandler; governs SafeError behavior
// (suppresses internal error details in client responses when true).
// Uses atomic.Bool because parallel tests construct handlers concurrently.
var productionMode atomic.Bool

// Handlers provides HTTP handlers for ingestion operations.
type Handlers struct {
	commandUC       *command.UseCase
	queryUC         *query.UseCase
	contextProvider contextProvider
	contextVerifier libHTTP.TenantOwnershipVerifier
}

// NewHandlers creates a new Handlers instance with the given use cases.
func NewHandlers(
	commandUC *command.UseCase,
	queryUC *query.UseCase,
	ctxProvider contextProvider,
	production bool,
) (*Handlers, error) {
	if commandUC == nil {
		return nil, ErrNilCommandUseCase
	}

	if queryUC == nil {
		return nil, ErrNilQueryUseCase
	}

	if ctxProvider == nil {
		return nil, ErrNilContextProvider
	}

	productionMode.Store(production)

	verifier := NewTenantOwnershipVerifier(ctxProvider)

	return &Handlers{
		commandUC:       commandUC,
		queryUC:         queryUC,
		contextProvider: ctxProvider,
		contextVerifier: verifier,
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

// validateFileContentType checks if the file's content type is valid for the declared format.
// Returns true if valid, false otherwise. Unknown content types pass validation.
// Handles media type parameters (e.g., "application/json; charset=utf-8").
func validateFileContentType(contentType, format string) bool {
	contentType = strings.ToLower(strings.TrimSpace(contentType))
	if contentType == "" || contentType == "application/octet-stream" || contentType == "text/plain" {
		return true
	}

	// Strip media type parameters (e.g., "; charset=utf-8")
	if idx := strings.IndexByte(contentType, ';'); idx != -1 {
		contentType = strings.TrimSpace(contentType[:idx])
	}

	validTypes := map[string][]string{
		"csv":  {"text/csv", "application/csv"},
		"json": {"application/json"},
		"xml":  {"application/xml", "text/xml"},
	}

	allowed, ok := validTypes[format]
	if !ok {
		return true
	}

	return slices.Contains(allowed, contentType)
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

func notFound(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	message string,
	err error,
) error {
	logSpanError(ctx, span, logger, message, err)

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

// handleContextVerificationError maps errors from ParseAndVerifyTenantScopedID to HTTP responses.
func handleContextVerificationError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) error {
	// Invalid or missing context ID → bad request
	if errors.Is(err, libHTTP.ErrMissingContextID) ||
		errors.Is(err, libHTTP.ErrInvalidContextID) {
		return badRequest(ctx, fiberCtx, span, logger, "invalid context_id", err)
	}

	// Missing or invalid tenant ID → unauthorized
	if errors.Is(err, libHTTP.ErrTenantIDNotFound) ||
		errors.Is(err, libHTTP.ErrInvalidTenantID) {
		logSpanError(ctx, span, logger, "invalid tenant id", err)
		return libHTTP.RespondError(fiberCtx, fiber.StatusUnauthorized, "unauthorized", "unauthorized")
	}

	// Context not found → 404
	if errors.Is(err, libHTTP.ErrContextNotFound) {
		return notFound(ctx, fiberCtx, span, logger, "context not found", err)
	}

	// Context not active → forbidden with specific message
	if errors.Is(err, libHTTP.ErrContextNotActive) {
		logSpanError(ctx, span, logger, "context not active", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusForbidden, "context_not_active", "context is not active")
	}

	// Infrastructure lookup failures (e.g. database errors during ownership check) → 500
	if errors.Is(err, libHTTP.ErrContextLookupFailed) {
		logSpanError(ctx, span, logger, "context lookup failed", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	// Tenant or ownership issues → forbidden
	return forbidden(ctx, fiberCtx, span, logger, err)
}

// handleIngestionError maps errors from StartIngestion to appropriate HTTP responses.
func handleIngestionError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) error {
	// Source not found → 404
	if errors.Is(err, command.ErrSourceNotFound) {
		return notFound(ctx, fiberCtx, span, logger, "source not found", err)
	}

	// Field map not found → 404
	if errors.Is(err, command.ErrFieldMapNotFound) {
		return notFound(ctx, fiberCtx, span, logger, "field mapping not found for source", err)
	}

	// Empty file (EOF reading headers) → 400
	if errors.Is(err, io.EOF) || strings.Contains(err.Error(), "failed to read csv headers: EOF") ||
		strings.Contains(err.Error(), "failed to read json: EOF") ||
		strings.Contains(err.Error(), "failed to decode xml: EOF") {
		return badRequest(ctx, fiberCtx, span, logger, "file is empty or has no content", err)
	}

	// Format required → 400
	if errors.Is(err, command.ErrFormatRequiredUC) {
		return badRequest(ctx, fiberCtx, span, logger, "format is required", err)
	}

	// Empty file (no data rows) → 400
	if errors.Is(err, command.ErrEmptyFile) {
		return badRequest(ctx, fiberCtx, span, logger, "file contains no data rows", err)
	}

	// Generic server error
	logSpanError(ctx, span, logger, "failed to start ingestion", err)

	return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
}

// UploadFile handles POST /v1/imports/contexts/:contextId/sources/:sourceId/upload
// @Summary Upload transaction file
// @Description Uploads a transaction file (CSV, JSON, or XML) for ingestion into a reconciliation context. The file is parsed, validated, and transactions are extracted for matching.
// @ID uploadFile
// @Tags Ingestion
// @Accept multipart/form-data
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param sourceId path string true "Source ID" format(uuid)
// @Param file formData file true "Transaction file (CSV, JSON, or XML)"
// @Param format formData string true "File format" Enums(csv, json, xml)
// @Success 202 {object} dto.JobResponse
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Source not found"
// @Failure 409 {object} ErrorResponse "Conflict: duplicate resource or idempotency key in progress"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/imports/contexts/{contextId}/sources/{sourceId}/upload [post]
//
//nolint:cyclop // HTTP handler with multiple validations
func (handler *Handlers) UploadFile(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.ingestion.upload")
	defer span.End()

	contextID, tenantID, err := libHTTP.ParseAndVerifyTenantScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationParam,
		handler.contextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
	)
	if err != nil {
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	sourceID, err := uuid.Parse(fiberCtx.Params("sourceId"))
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid source_id", err)
	}

	file, err := fiberCtx.FormFile("file")
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "file is required", err)
	}

	if file.Size == 0 {
		return badRequest(ctx, fiberCtx, span, logger, "file is empty", ErrEmptyFile)
	}

	if file.Size > maxUploadSize {
		return libHTTP.RespondError(
			fiberCtx,
			fiber.StatusRequestEntityTooLarge,
			"payload_too_large",
			"file exceeds 100MB limit",
		)
	}

	format := strings.TrimSpace(fiberCtx.FormValue("format"))

	if format == "" {
		return badRequest(ctx, fiberCtx, span, logger, "format is required", ErrFormatRequired)
	}

	format = strings.ToLower(format)
	if format != "csv" && format != "json" && format != "xml" {
		return badRequest(
			ctx,
			fiberCtx,
			span,
			logger,
			"invalid format: must be one of csv, json, xml",
			ErrInvalidFormat,
		)
	}

	if !validateFileContentType(file.Header.Get("Content-Type"), format) {
		return badRequest(ctx, fiberCtx, span, logger, "file content type does not match declared format", ErrInvalidContentType)
	}

	fileReader, err := file.Open()
	if err != nil {
		logSpanError(ctx, span, logger, "failed to open file", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}
	defer fileReader.Close()

	job, err := handler.commandUC.StartIngestion(
		ctx,
		contextID,
		sourceID,
		file.Filename,
		file.Size,
		format,
		fileReader,
	)
	if err != nil {
		return handleIngestionError(ctx, fiberCtx, span, logger, err)
	}

	if writeErr := libHTTP.Respond(fiberCtx, fiber.StatusAccepted, dto.JobToResponse(job)); writeErr != nil {
		return fmt.Errorf("write accepted response: %w", writeErr)
	}

	return nil
}

// GetJob handles GET /v1/imports/contexts/:contextId/jobs/:jobId
// @Summary Get ingestion job status
// @Description Retrieves the status and details of an ingestion job by its ID, including progress metrics and error information.
// @ID getIngestionJob
// @Tags Ingestion
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param jobId path string true "Job ID" format(uuid)
// @Success 200 {object} dto.JobResponse
// @Failure 400 {object} ErrorResponse "Invalid query parameters"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Job not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/imports/contexts/{contextId}/jobs/{jobId} [get]
func (handler *Handlers) GetJob(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.ingestion.get_job")
	defer span.End()

	contextID, tenantID, err := libHTTP.ParseAndVerifyTenantScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationParam,
		handler.contextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
	)
	if err != nil {
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	jobID, err := uuid.Parse(fiberCtx.Params("jobId"))
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid job_id", err)
	}

	job, err := handler.queryUC.GetJobByContext(ctx, contextID, jobID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) || errors.Is(err, query.ErrJobNotFound) {
			return notFound(ctx, fiberCtx, span, logger, "job not found", err)
		}

		logSpanError(ctx, span, logger, "failed to get job", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.JobToResponse(job))
}

// ListJobsByContext handles GET /v1/imports/contexts/:contextId/jobs
// @Summary List ingestion jobs for a context
// @Description Returns a cursor-paginated list of ingestion jobs for a reconciliation context, with optional sorting.
// @ID listIngestionJobs
// @Tags Ingestion
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param limit query int false "Maximum number of records to return" default(20) minimum(1) maximum(200)
// @Param cursor query string false "Cursor for pagination (opaque)"
// @Param sort_order query string false "Sort order" Enums(asc,desc)
// @Param sort_by query string false "Sort field" Enums(id,created_at,started_at,completed_at,status)
// @Success 200 {object} dto.ListJobsResponse
// @Failure 400 {object} ErrorResponse "Invalid query parameters"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/imports/contexts/{contextId}/jobs [get]
func (handler *Handlers) ListJobsByContext(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.ingestion.list_jobs")
	defer span.End()

	contextID, tenantID, err := libHTTP.ParseAndVerifyTenantScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationParam,
		handler.contextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
	)
	if err != nil {
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	cursor, limit, err := libHTTP.ParseOpaqueCursorPagination(fiberCtx)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid pagination parameters", err)
	}

	cursor = strings.TrimSpace(cursor)

	sortOrder := strings.TrimSpace(fiberCtx.Query("sort_order"))
	if sortOrder == "" {
		sortOrder = sortOrderDesc
	}

	sortOrder = strings.ToLower(sortOrder)
	if sortOrder != "asc" && sortOrder != sortOrderDesc {
		return badRequest(
			ctx,
			fiberCtx,
			span,
			logger,
			"invalid sort_order: must be asc or desc",
			ErrInvalidSortOrder,
		)
	}

	sortBy := strings.TrimSpace(fiberCtx.Query("sort_by"))
	if sortBy != "" && !validJobSortColumns[sortBy] {
		return badRequest(
			ctx,
			fiberCtx,
			span,
			logger,
			"invalid sort_by: must be one of id, created_at, started_at, completed_at, status",
			ErrInvalidSortBy,
		)
	}

	jobs, pagination, err := handler.queryUC.ListJobsByContext(
		ctx,
		contextID,
		ingestionRepositories.CursorFilter{
			Limit:     limit,
			Cursor:    cursor,
			SortBy:    sortBy,
			SortOrder: sortOrder,
		},
	)
	if err != nil {
		if errors.Is(err, libHTTP.ErrInvalidCursor) {
			return badRequest(ctx, fiberCtx, span, logger, "invalid pagination parameters", err)
		}

		logSpanError(ctx, span, logger, "failed to list jobs", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	items := dto.JobsToResponse(jobs)

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ListJobsResponse{
		Items: items,
		CursorResponse: sharedpagination.CursorResponse{
			NextCursor: pagination.Next,
			PrevCursor: pagination.Prev,
			Limit:      limit,
			HasMore:    pagination.Next != "",
		},
	})
}

// ListTransactionsByJob handles GET /v1/imports/contexts/:contextId/jobs/:jobId/transactions
// @Summary List transactions for a job
// @Description Returns a cursor-paginated list of transactions extracted from an ingestion job, with optional sorting.
// @ID listJobTransactions
// @Tags Ingestion
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param jobId path string true "Job ID" format(uuid)
// @Param limit query int false "Maximum number of records to return" default(20) minimum(1) maximum(200)
// @Param cursor query string false "Cursor for pagination (opaque)"
// @Param sort_order query string false "Sort order" Enums(asc,desc)
// @Param sort_by query string false "Sort field" Enums(id,created_at,date,status,extraction_status)
// @Success 200 {object} dto.ListTransactionsResponse
// @Failure 400 {object} ErrorResponse "Invalid query parameters"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Job not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/imports/contexts/{contextId}/jobs/{jobId}/transactions [get]
func (handler *Handlers) ListTransactionsByJob(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.ingestion.list_transactions")
	defer span.End()

	contextID, tenantID, err := libHTTP.ParseAndVerifyTenantScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationParam,
		handler.contextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
	)
	if err != nil {
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	jobID, err := uuid.Parse(fiberCtx.Params("jobId"))
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid job_id", err)
	}

	cursor, limit, err := libHTTP.ParseOpaqueCursorPagination(fiberCtx)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid pagination parameters", err)
	}

	cursor = strings.TrimSpace(cursor)

	sortOrder, err := parseSortOrder(fiberCtx)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid sort_order: must be asc or desc", err)
	}

	sortBy := strings.TrimSpace(fiberCtx.Query("sort_by"))
	if sortBy != "" && !validTransactionSortColumns[sortBy] {
		return badRequest(
			ctx,
			fiberCtx,
			span,
			logger,
			"invalid sort_by: must be one of id, created_at, date, status, extraction_status",
			ErrInvalidSortBy,
		)
	}

	job, err := handler.queryUC.GetJobByContext(ctx, contextID, jobID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) || errors.Is(err, query.ErrJobNotFound) {
			return notFound(ctx, fiberCtx, span, logger, "job not found", err)
		}

		logSpanError(ctx, span, logger, "failed to get job", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	transactions, pagination, err := handler.queryUC.ListTransactionsByJobContext(
		ctx,
		job.ID,
		contextID,
		ingestionRepositories.CursorFilter{
			Limit:     limit,
			Cursor:    cursor,
			SortBy:    sortBy,
			SortOrder: sortOrder,
		},
	)
	if err != nil {
		if errors.Is(err, libHTTP.ErrInvalidCursor) {
			return badRequest(ctx, fiberCtx, span, logger, "invalid pagination parameters", err)
		}

		logSpanError(ctx, span, logger, "failed to list transactions", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	items := dto.TransactionsToResponse(transactions, jobID, contextID)

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ListTransactionsResponse{
		Items: items,
		CursorResponse: sharedpagination.CursorResponse{
			NextCursor: pagination.Next,
			PrevCursor: pagination.Prev,
			Limit:      limit,
			HasMore:    pagination.Next != "",
		},
	})
}

// IgnoreTransaction handles POST /v1/imports/contexts/:contextId/transactions/:transactionId/ignore
// @Summary Ignore a transaction
// @Description Marks a transaction as "Do Not Match" with a required reason. Only UNMATCHED transactions can be ignored.
// @ID ignoreTransaction
// @Tags Ingestion
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param transactionId path string true "Transaction ID" format(uuid)
// @Param request body dto.IgnoreTransactionRequest true "Ignore transaction request"
// @Success 200 {object} dto.IgnoreTransactionResponse
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Transaction not found"
// @Failure 409 {object} ErrorResponse "Transaction already matched/ignored or idempotency conflict"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/imports/contexts/{contextId}/transactions/{transactionId}/ignore [post]
func (handler *Handlers) IgnoreTransaction(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.ingestion.ignore_transaction")
	defer span.End()

	contextID, tenantID, err := libHTTP.ParseAndVerifyTenantScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationParam,
		handler.contextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
	)
	if err != nil {
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	transactionID, err := uuid.Parse(fiberCtx.Params("transactionId"))
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid transaction_id", err)
	}

	var req dto.IgnoreTransactionRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid request body", err)
	}

	tx, err := handler.commandUC.IgnoreTransaction(ctx, command.IgnoreTransactionInput{
		TransactionID: transactionID,
		ContextID:     contextID,
		Reason:        req.Reason,
	})
	if err != nil {
		return handleIgnoreTransactionError(ctx, fiberCtx, span, logger, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.IgnoreTransactionResponse{
		TransactionResponse: dto.TransactionToResponse(tx, tx.IngestionJobID, contextID),
	})
}

// SearchTransactions handles GET /v1/imports/contexts/:contextId/transactions/search
// @Summary Search transactions
// @Description Searches transactions within a reconciliation context by text, amount range, date range, currency, source, and status. Returns offset-paginated results with total count.
// @ID searchTransactions
// @Tags Ingestion
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param q query string false "Free text search across external ID and description"
// @Param amount_min query string false "Minimum transaction amount"
// @Param amount_max query string false "Maximum transaction amount"
// @Param date_from query string false "Start date filter (RFC3339)" format(date-time)
// @Param date_to query string false "End date filter (RFC3339)" format(date-time)
// @Param reference query string false "Exact external ID match"
// @Param currency query string false "Currency code filter"
// @Param source_id query string false "Source ID filter"
// @Param status query string false "Transaction status filter" Enums(UNMATCHED,MATCHED,IGNORED,PENDING_REVIEW)
// @Param limit query int false "Maximum number of records to return" default(20) minimum(1) maximum(50)
// @Param offset query int false "Number of records to skip" default(0) minimum(0)
// @Success 200 {object} dto.SearchTransactionsResponse
// @Failure 400 {object} ErrorResponse "Invalid query parameters"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/imports/contexts/{contextId}/transactions/search [get]
func (handler *Handlers) SearchTransactions(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.ingestion.search_transactions")
	defer span.End()

	contextID, tenantID, err := libHTTP.ParseAndVerifyTenantScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationParam,
		handler.contextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
	)
	if err != nil {
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	var req dto.SearchTransactionsRequest
	if err := fiberCtx.QueryParser(&req); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid query parameters", err)
	}

	searchParams, err := parseSearchParams(req)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid search parameters", err)
	}

	transactions, total, err := handler.queryUC.SearchTransactions(ctx, contextID, searchParams)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to search transactions", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	items := dto.SearchTransactionsToResponse(transactions, contextID)

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.SearchTransactionsResponse{
		Items:  items,
		Total:  total,
		Limit:  searchParams.Limit,
		Offset: searchParams.Offset,
	})
}

//nolint:cyclop // parsing multiple optional search parameters
func parseSearchParams(
	req dto.SearchTransactionsRequest,
) (ingestionRepositories.TransactionSearchParams, error) {
	params := ingestionRepositories.TransactionSearchParams{
		Query:     strings.TrimSpace(req.Query),
		Reference: strings.TrimSpace(req.Reference),
		Currency:  strings.TrimSpace(req.Currency),
		Status:    strings.TrimSpace(req.Status),
		Limit:     req.Limit,
		Offset:    req.Offset,
	}

	if params.Limit <= 0 {
		params.Limit = 20
	}

	const maxSearchLimit = 50
	if params.Limit > maxSearchLimit {
		params.Limit = maxSearchLimit
	}

	if params.Offset < 0 {
		params.Offset = 0
	}

	if req.AmountMin != "" {
		val, err := parseDecimal(req.AmountMin)
		if err != nil {
			return params, fmt.Errorf("invalid amount_min: %w", err)
		}

		params.AmountMin = &val
	}

	if req.AmountMax != "" {
		val, err := parseDecimal(req.AmountMax)
		if err != nil {
			return params, fmt.Errorf("invalid amount_max: %w", err)
		}

		params.AmountMax = &val
	}

	if req.DateFrom != "" {
		t, err := parseRFC3339(req.DateFrom)
		if err != nil {
			return params, fmt.Errorf("invalid date_from: %w", err)
		}

		params.DateFrom = &t
	}

	if req.DateTo != "" {
		t, err := parseRFC3339(req.DateTo)
		if err != nil {
			return params, fmt.Errorf("invalid date_to: %w", err)
		}

		params.DateTo = &t
	}

	if req.SourceID != "" {
		id, err := uuid.Parse(req.SourceID)
		if err != nil {
			return params, fmt.Errorf("invalid source_id: %w", err)
		}

		params.SourceID = &id
	}

	return params, nil
}

func parseDecimal(s string) (decimal.Decimal, error) {
	d, err := decimal.NewFromString(s)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("parse decimal: %w", err)
	}

	return d, nil
}

func parseRFC3339(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse time: %w", err)
	}

	return t.UTC(), nil
}

// PreviewFile handles POST /v1/imports/contexts/:contextId/sources/:sourceId/preview
// @Summary Preview uploaded file
// @Description Parses a sample file and returns detected column headers and sample rows. Used for field mapping configuration. Does not persist any data.
// @ID previewFile
// @Tags Ingestion
// @Accept multipart/form-data
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param sourceId path string true "Source ID" format(uuid)
// @Param file formData file true "Sample file to preview (CSV, JSON, or XML)"
// @Param format formData string false "File format (auto-detected from extension if omitted)" Enums(csv, json, xml)
// @Param max_rows formData int false "Maximum sample rows to return (default 5, max 20)" default(5) minimum(1) maximum(20)
// @Param max_rows query int false "Maximum sample rows to return (default 5, max 20)" default(5) minimum(1) maximum(20)
// @Success 200 {object} dto.FilePreviewResponse
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Source not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/imports/contexts/{contextId}/sources/{sourceId}/preview [post]
func (handler *Handlers) PreviewFile(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.ingestion.preview_file")
	defer span.End()

	contextID, tenantID, err := libHTTP.ParseAndVerifyTenantScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationParam,
		handler.contextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
	)
	if err != nil {
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	sourceID, err := uuid.Parse(fiberCtx.Params("sourceId"))
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid source_id", err)
	}

	span.SetAttributes(attribute.String("source_id", sourceID.String()))

	file, err := fiberCtx.FormFile("file")
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "file is required", err)
	}

	if file.Size == 0 {
		return badRequest(ctx, fiberCtx, span, logger, "file is empty", ErrEmptyFile)
	}

	format := strings.TrimSpace(fiberCtx.FormValue("format"))
	if format == "" {
		format = detectFormatFromFilename(file.Filename)
	}

	if format == "" {
		return badRequest(ctx, fiberCtx, span, logger, "unsupported file format; allowed: csv, json, xml", ErrFormatRequired)
	}

	maxRows := parseMaxRows(fiberCtx, logger)

	fileReader, err := file.Open()
	if err != nil {
		logSpanError(ctx, span, logger, "failed to open file", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	defer func() {
		if closeErr := fileReader.Close(); closeErr != nil {
			logSpanError(ctx, span, logger, "failed to close preview file", closeErr)
		}
	}()

	preview, err := handler.queryUC.PreviewFile(ctx, fileReader, format, maxRows)
	if err != nil {
		return handlePreviewError(ctx, fiberCtx, span, logger, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.FilePreviewResponse{
		Columns:    preview.Columns,
		SampleRows: preview.SampleRows,
		RowCount:   preview.RowCount,
		Format:     preview.Format,
	})
}

// parseMaxRows extracts and clamps the max_rows parameter from query string
// or form data, returning a value between 1 and 20 (default 5).
func parseMaxRows(fiberCtx *fiber.Ctx, logger libLog.Logger) int {
	const (
		defaultPreviewRows = 5
		maxPreviewRows     = 20
	)

	maxRows := fiberCtx.QueryInt("max_rows", 0)
	if maxRows == 0 {
		maxRows = parseMaxRowsFromForm(fiberCtx, logger)
	}

	if maxRows <= 0 {
		maxRows = defaultPreviewRows
	}

	if maxRows > maxPreviewRows {
		maxRows = maxPreviewRows
	}

	return maxRows
}

// parseMaxRowsFromForm attempts to parse max_rows from form data.
func parseMaxRowsFromForm(fiberCtx *fiber.Ctx, logger libLog.Logger) int {
	maxRowsForm := strings.TrimSpace(fiberCtx.FormValue("max_rows"))
	if maxRowsForm == "" {
		return 0
	}

	parsed, err := strconv.Atoi(maxRowsForm)
	if err != nil {
		ctx := fiberCtx.UserContext()
		logger.Log(ctx, libLog.LevelDebug, fmt.Sprintf("invalid max_rows form value %q: %v", maxRowsForm, err))

		return 0
	}

	return parsed
}

// detectFormatFromFilename infers file format from the filename extension.
func detectFormatFromFilename(filename string) string {
	lower := strings.ToLower(filename)

	switch {
	case strings.HasSuffix(lower, ".csv"):
		return "csv"
	case strings.HasSuffix(lower, ".json"):
		return "json"
	case strings.HasSuffix(lower, ".xml"):
		return "xml"
	default:
		return ""
	}
}

func handlePreviewError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) error {
	if errors.Is(err, query.ErrPreviewReaderRequired) ||
		errors.Is(err, query.ErrPreviewFormatRequired) ||
		errors.Is(err, query.ErrPreviewInvalidFormat) ||
		errors.Is(err, query.ErrPreviewEmptyFile) {
		return badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	logSpanError(ctx, span, logger, "failed to preview file", err)

	return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
}

func handleIgnoreTransactionError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) error {
	if errors.Is(err, command.ErrTransactionNotFound) {
		return notFound(ctx, fiberCtx, span, logger, "transaction not found", err)
	}

	if errors.Is(err, command.ErrTransactionNotIgnorable) {
		logSpanError(ctx, span, logger, "transaction cannot be ignored", err)

		return libHTTP.RespondError(
			fiberCtx,
			fiber.StatusConflict,
			"invalid_state",
			"transaction cannot be ignored: only UNMATCHED transactions can be ignored",
		)
	}

	if errors.Is(err, command.ErrReasonRequired) {
		return badRequest(ctx, fiberCtx, span, logger, "reason is required", err)
	}

	logSpanError(ctx, span, logger, "failed to ignore transaction", err)

	return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
}
