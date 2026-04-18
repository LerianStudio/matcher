package http

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/pointers"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/services/command"
	"github.com/LerianStudio/matcher/internal/reporting/services/query"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

const (
	// maxAsyncExportDateRangeDays defines the maximum date range permitted for async export jobs.
	maxAsyncExportDateRangeDays = 365
)

var (
	// ErrNilExportJobUseCase indicates export job use case is nil.
	ErrNilExportJobUseCase = errors.New("export job use case is required")
	// ErrNilExportJobQueryService indicates export job query service is nil.
	ErrNilExportJobQueryService = errors.New("export job query service is required")
	// ErrNilStorageClientHandler indicates storage client is nil.
	ErrNilStorageClientHandler = errors.New("storage client is required")
	// ErrInvalidJobID indicates job ID is invalid.
	ErrInvalidJobID = errors.New("invalid job ID")
	// ErrJobNotDownloadable indicates job is not ready for download.
	ErrJobNotDownloadable = errors.New("job is not ready for download")
	// ErrPDFNotSupportedAsync indicates PDF format is not supported for async export.
	ErrPDFNotSupportedAsync = errors.New("use synchronous export for PDF")
	// ErrSummaryNotSupportedAsync indicates SUMMARY report type is not supported for async export.
	ErrSummaryNotSupportedAsync = errors.New("SUMMARY report type is not supported for async export; use synchronous export")
	// ErrExceptionsNotSupportedAsync indicates EXCEPTIONS report type is not yet supported for async export.
	ErrExceptionsNotSupportedAsync = errors.New("EXCEPTIONS report type is not yet supported for async export")
	// ErrTenantIDNotFound indicates tenant ID is not in context.
	ErrTenantIDNotFound = errors.New("tenant ID not found in context")
	// ErrTenantMismatch indicates the job does not belong to the requesting tenant.
	ErrTenantMismatch = errors.New("export job does not belong to this tenant")
	// ErrDateRangeInvalid indicates dateFrom is after dateTo.
	ErrDateRangeInvalid = errors.New("dateFrom must be before or equal to dateTo")
	// ErrAsyncExportDateRangeExceeded indicates the date range exceeds the maximum for async export jobs.
	ErrAsyncExportDateRangeExceeded = errors.New("date range exceeds maximum allowed for export jobs")
	// ErrExportWorkerDisabled indicates async export job creation is unavailable.
	ErrExportWorkerDisabled = errors.New("export worker is disabled")
)

// logSpanBusinessEvent records a business-outcome event on the span without marking
// the span as errored. Use for expected outcomes (validation, not-found, conflict)
// that are not infrastructure failures.
func logSpanBusinessEvent(ctx context.Context, span trace.Span, logger libLog.Logger, message string, err error) {
	libOpentelemetry.HandleSpanBusinessErrorEvent(span, message, err)
	libLog.SafeError(logger, ctx, message, err, productionMode.Load())
}

// badRequestBiz responds with 400 and records a business event (not a span error).
// Use for validation failures and malformed input — expected client behaviour.
func badRequestBiz(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	message string,
	err error,
) error {
	return badRequestBizWithSlug(ctx, fiberCtx, span, logger, "invalid_request", message, err)
}

func badRequestBizWithSlug(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	slug, message string,
	err error,
) error {
	logSpanBusinessEvent(ctx, span, logger, message, err)

	return respondError(fiberCtx, fiber.StatusBadRequest, slug, message)
}

// notFoundBiz responds with 404 and records a business event (not a span error).
// Use for "entity does not exist" responses — expected business outcome.
func notFoundBiz(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) error {
	const message = "export job not found"

	logSpanBusinessEvent(ctx, span, logger, message, err)

	return respondError(fiberCtx, fiber.StatusNotFound, "reporting_export_job_not_found", message)
}

// ExportJobRuntimeConfig controls runtime-sensitive handler behavior without
// coupling the reporting package to bootstrap internals.
type ExportJobRuntimeConfig struct {
	Enabled       *bool
	PresignExpiry time.Duration
}

// ExportJobHandlers provides HTTP handlers for export job operations.
type ExportJobHandlers struct {
	exportJobUC           *command.ExportJobUseCase
	querySvc              *query.ExportJobQueryService
	storage               sharedPorts.ObjectStorageClient
	contextVerifier       libHTTP.TenantOwnershipVerifier
	enabled               bool
	presignExpiry         time.Duration
	runtimeConfigResolver func(context.Context) ExportJobRuntimeConfig
}

// NewExportJobHandlers creates a new ExportJobHandlers instance.
// presignExpiry configures how long download URLs remain valid.
// If zero or negative, defaults to 1 hour.
func NewExportJobHandlers(
	exportJobUC *command.ExportJobUseCase,
	querySvc *query.ExportJobQueryService,
	storage sharedPorts.ObjectStorageClient,
	ctxProvider contextProvider,
	presignExpiry time.Duration,
) (*ExportJobHandlers, error) {
	if exportJobUC == nil {
		return nil, ErrNilExportJobUseCase
	}

	if querySvc == nil {
		return nil, ErrNilExportJobQueryService
	}

	if sharedPorts.IsNilValue(storage) {
		return nil, ErrNilStorageClientHandler
	}

	if ctxProvider == nil {
		return nil, ErrNilContextProvider
	}

	if presignExpiry <= 0 {
		presignExpiry = entities.DefaultPresignExpiry
	}

	verifier := NewTenantOwnershipVerifier(ctxProvider)

	return &ExportJobHandlers{
		exportJobUC:     exportJobUC,
		querySvc:        querySvc,
		storage:         storage,
		contextVerifier: verifier,
		enabled:         true,
		presignExpiry:   presignExpiry,
	}, nil
}

// SetRuntimeEnabled configures the startup-time enabled state for async export creation.
func (handler *ExportJobHandlers) SetRuntimeEnabled(enabled bool) {
	if handler != nil {
		handler.enabled = enabled
	}
}

// SetRuntimeConfigResolver allows bootstrap to inject context-aware runtime settings.
func (handler *ExportJobHandlers) SetRuntimeConfigResolver(resolver func(context.Context) ExportJobRuntimeConfig) {
	if handler != nil {
		handler.runtimeConfigResolver = resolver
	}
}

func (handler *ExportJobHandlers) currentRuntimeConfigForContext(ctx context.Context) ExportJobRuntimeConfig {
	enabled := true
	config := ExportJobRuntimeConfig{
		Enabled:       &enabled,
		PresignExpiry: entities.DefaultPresignExpiry,
	}

	if handler == nil {
		return config
	}

	config.Enabled = pointers.Bool(handler.enabled)

	if handler.presignExpiry > 0 {
		config.PresignExpiry = handler.presignExpiry
	}

	if handler.runtimeConfigResolver != nil {
		runtimeConfig := handler.runtimeConfigResolver(ctx)
		if runtimeConfig.Enabled == nil {
			runtimeConfig.Enabled = config.Enabled
		}

		if runtimeConfig.PresignExpiry <= 0 {
			runtimeConfig.PresignExpiry = config.PresignExpiry
		}

		return runtimeConfig
	}

	return config
}

// CreateExportJobRequest represents the request body for creating an export job.
type CreateExportJobRequest struct {
	// ReportType specifies the type of report to export
	// @enum MATCHED,UNMATCHED,SUMMARY,VARIANCE,EXCEPTIONS,MATCHES,UNMATCHED_TRANSACTIONS
	ReportType string `json:"reportType" validate:"required,oneof=MATCHED UNMATCHED SUMMARY VARIANCE EXCEPTIONS MATCHES UNMATCHED_TRANSACTIONS matched unmatched summary variance exceptions matches unmatched_transactions" example:"MATCHED"`
	// Format specifies the export file format (server normalizes to uppercase)
	Format string `json:"format" validate:"required,oneof=CSV JSON XML csv json xml" enums:"CSV,JSON,XML,csv,json,xml" example:"CSV"`
	// DateFrom is the start date for the report (YYYY-MM-DD)
	DateFrom string `json:"dateFrom" validate:"required" example:"2025-01-01"`
	// DateTo is the end date for the report (YYYY-MM-DD)
	DateTo string `json:"dateTo" validate:"required" example:"2025-01-31"`
	// SourceID optionally filters to a specific source
	SourceID *string `json:"sourceId,omitempty" validate:"omitempty,uuid" example:"550e8400-e29b-41d4-a716-446655440000"`
}

// CreateExportJobResponse represents the response for creating an export job.
type CreateExportJobResponse struct {
	JobID     string `json:"jobId"     example:"550e8400-e29b-41d4-a716-446655440000"`
	Status    string `json:"status"    example:"QUEUED"    enums:"QUEUED"`
	StatusURL string `json:"statusUrl" example:"/v1/contexts/550e8400-e29b-41d4-a716-446655440000/export-jobs/550e8400-e29b-41d4-a716-446655440001"`
}

// parsedExportJobRequest holds validated and parsed request data.
type parsedExportJobRequest struct {
	reportType entities.ExportReportType
	format     entities.ExportFormat
	dateFrom   time.Time
	dateTo     time.Time
	sourceID   *uuid.UUID
}

// parseExportJobRequest parses and applies business rules to the request.
// Note: Struct validation (required, oneof, uuid) is done by libHTTP.ParseBodyAndValidate.
func parseExportJobRequest(req *CreateExportJobRequest) (*parsedExportJobRequest, string, error) {
	normalizedReportType, ok := normalizeReportTypeAlias(req.ReportType)
	if !ok {
		return nil, "invalid report_type: must be MATCHED, UNMATCHED, SUMMARY, VARIANCE, EXCEPTIONS, MATCHES, or UNMATCHED_TRANSACTIONS", entities.ErrInvalidReportType
	}

	format := entities.ExportFormat(strings.ToUpper(strings.TrimSpace(req.Format)))

	if !format.IsValid() {
		return nil, "invalid format: must be CSV, JSON, XML, or PDF", entities.ErrInvalidExportFormat
	}

	if format == entities.ExportFormatPDF {
		return nil, "PDF format not supported for async export", ErrPDFNotSupportedAsync
	}

	if normalizedReportType == entities.ExportReportTypeSummary {
		return nil, "SUMMARY report type not supported for async export", ErrSummaryNotSupportedAsync
	}

	if normalizedReportType == entities.ExportReportTypeExceptions {
		return nil, "EXCEPTIONS report type is not yet supported for async export", ErrExceptionsNotSupportedAsync
	}

	dateFrom, err := time.Parse(time.DateOnly, req.DateFrom)
	if err != nil {
		return nil, "invalid date_from format", fmt.Errorf("invalid date_from format: %w", err)
	}

	dateTo, err := time.Parse(time.DateOnly, req.DateTo)
	if err != nil {
		return nil, "invalid date_to format", fmt.Errorf("invalid date_to format: %w", err)
	}

	if dateFrom.After(dateTo) {
		return nil, "dateFrom must be before or equal to dateTo", ErrDateRangeInvalid
	}

	if dateTo.Sub(dateFrom).Hours()/hoursPerDay > float64(maxAsyncExportDateRangeDays) {
		return nil, fmt.Sprintf("date range exceeds maximum of %d days for export jobs", maxAsyncExportDateRangeDays), ErrAsyncExportDateRangeExceeded
	}

	dateTo = dateTo.Add(hoursPerDay*time.Hour - time.Nanosecond)

	var sourceID *uuid.UUID

	if req.SourceID != nil && *req.SourceID != "" {
		parsed, err := uuid.Parse(*req.SourceID)
		if err != nil {
			return nil, "invalid source_id", fmt.Errorf("invalid source_id: %w", err)
		}

		sourceID = &parsed
	}

	return &parsedExportJobRequest{
		reportType: normalizedReportType,
		format:     format,
		dateFrom:   dateFrom,
		dateTo:     dateTo,
		sourceID:   sourceID,
	}, "", nil
}

func normalizeReportTypeAlias(reportType string) (entities.ExportReportType, bool) {
	switch strings.ToUpper(strings.TrimSpace(reportType)) {
	case string(entities.ExportReportTypeMatched), "MATCHES":
		return entities.ExportReportTypeMatched, true
	case string(entities.ExportReportTypeUnmatched), "UNMATCHED_TRANSACTIONS":
		return entities.ExportReportTypeUnmatched, true
	case string(entities.ExportReportTypeSummary):
		return entities.ExportReportTypeSummary, true
	case string(entities.ExportReportTypeVariance):
		return entities.ExportReportTypeVariance, true
	case string(entities.ExportReportTypeExceptions):
		return entities.ExportReportTypeExceptions, true
	default:
		return "", false
	}
}

// ExportJobResponse represents an export job in API responses.
type ExportJobResponse struct {
	ID             string  `json:"id"                   example:"550e8400-e29b-41d4-a716-446655440000"`
	ReportType     string  `json:"reportType"           example:"MATCHED"    enums:"MATCHED,UNMATCHED,SUMMARY,VARIANCE"`
	Format         string  `json:"format"               example:"CSV"        enums:"CSV,JSON,XML,PDF"`
	Status         string  `json:"status"               example:"SUCCEEDED"  enums:"QUEUED,RUNNING,SUCCEEDED,FAILED,EXPIRED,CANCELED"`
	RecordsWritten int64   `json:"recordsWritten"       example:"4250"`
	BytesWritten   int64   `json:"bytesWritten"         example:"524288"`
	FileName       *string `json:"fileName,omitempty"   example:"matched_report_2025-01-31.csv"`
	Error          *string `json:"error,omitempty"       example:"timeout exceeded"`
	CreatedAt      string  `json:"createdAt"            example:"2025-01-15T10:30:00Z"`
	StartedAt      *string `json:"startedAt,omitempty"  example:"2025-01-15T10:30:05Z"`
	FinishedAt     *string `json:"finishedAt,omitempty" example:"2025-01-15T10:35:00Z"`
	ExpiresAt      string  `json:"expiresAt"            example:"2025-01-16T10:30:00Z"`
	DownloadURL    *string `json:"downloadUrl,omitempty" example:"https://storage.example.com/exports/matched_report.csv?token=abc"`
}

// ExportJobListResponse represents a list of export jobs.
type ExportJobListResponse struct {
	Items []*ExportJobResponse `json:"items" validate:"omitempty,max=200" maxItems:"200"`
	sharedhttp.CursorResponse
}

// DownloadExportJobResponse represents the response for downloading an export file.
type DownloadExportJobResponse struct {
	// Presigned URL to download the export file
	DownloadURL string `json:"downloadUrl"  example:"https://storage.example.com/exports/report.csv?token=abc"`
	// Original file name of the export
	FileName string `json:"fileName"     example:"matched_report.csv"`
	// SHA-256 checksum of the file for integrity verification
	SHA256 string `json:"sha256"       example:"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"`
	// Duration in seconds until the download URL expires
	ExpiresIn int `json:"expiresIn"    example:"3600"`
}

func startExportJobSpan(c *fiber.Ctx, name string) (context.Context, trace.Span, libLog.Logger) {
	return startHandlerSpan(c, name)
}

// verifyJobTenantOwnership checks that the export job belongs to the tenant in context.
func verifyJobTenantOwnership(ctx context.Context, job *entities.ExportJob) error {
	tenantIDStr := auth.GetTenantID(ctx)

	tenantID, err := uuid.Parse(tenantIDStr)
	if err != nil {
		return ErrTenantIDNotFound
	}

	if job.TenantID != tenantID {
		return ErrTenantMismatch
	}

	return nil
}

// CreateExportJob handles POST /v1/contexts/:contextId/export-jobs
// @ID createExportJob
// @Summary Create an export job
// @Description Creates an async export job for large report exports (CSV, JSON, XML).
// @Tags Export Jobs
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param request body CreateExportJobRequest true "Export job parameters"
// @Success 202 {object} CreateExportJobResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 409 {object} sharedhttp.ErrorResponse "Conflict: duplicate resource or idempotency key in progress"
// @Failure 503 {object} sharedhttp.ErrorResponse "Export worker disabled"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/contexts/{contextId}/export-jobs [post]
func (handler *ExportJobHandlers) CreateExportJob(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startExportJobSpan(fiberCtx, "handler.export_job.create")

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

	runtimeConfig := handler.currentRuntimeConfigForContext(ctx)
	if runtimeConfig.Enabled != nil && !*runtimeConfig.Enabled {
		return respondError(
			fiberCtx,
			fiber.StatusServiceUnavailable,
			"export_worker_disabled",
			ErrExportWorkerDisabled.Error(),
		)
	}

	var req CreateExportJobRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return badRequestBiz(ctx, fiberCtx, span, logger, "invalid request body", err)
	}

	parsed, msg, err := parseExportJobRequest(&req)
	if err != nil {
		if errors.Is(err, entities.ErrInvalidExportFormat) ||
			errors.Is(err, ErrPDFNotSupportedAsync) ||
			errors.Is(err, ErrSummaryNotSupportedAsync) ||
			errors.Is(err, ErrExceptionsNotSupportedAsync) {
			return badRequestBizWithSlug(ctx, fiberCtx, span, logger, "reporting_invalid_export_format", msg, err)
		}

		return badRequestBiz(ctx, fiberCtx, span, logger, msg, err)
	}

	input := command.CreateExportJobInput{
		TenantID:   tenantID,
		ContextID:  contextID,
		ReportType: parsed.reportType,
		Format:     parsed.format,
		Filter: entities.ExportJobFilter{
			DateFrom: parsed.dateFrom,
			DateTo:   parsed.dateTo,
			SourceID: parsed.sourceID,
		},
	}

	output, err := handler.exportJobUC.CreateExportJob(ctx, input)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to create export job", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	response := CreateExportJobResponse{
		JobID:     output.JobID.String(),
		Status:    string(output.Status),
		StatusURL: output.StatusURL,
	}

	if writeErr := libHTTP.Respond(fiberCtx, fiber.StatusAccepted, response); writeErr != nil {
		return fmt.Errorf("write accepted response: %w", writeErr)
	}

	return nil
}

// GetExportJob handles GET /v1/export-jobs/:jobId
// @ID getExportJob
// @Summary Get export job status
// @Description Retrieves the status of an export job.
// @Tags Export Jobs
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param jobId path string true "Export Job ID" format(uuid)
// @Success 200 {object} ExportJobResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Export job not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/export-jobs/{jobId} [get]
func (handler *ExportJobHandlers) GetExportJob(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startExportJobSpan(fiberCtx, "handler.export_job.get")

	defer span.End()

	jobIDStr := fiberCtx.Params("jobId")

	jobID, err := uuid.Parse(jobIDStr)
	if err != nil {
		return badRequestBiz(ctx, fiberCtx, span, logger, "invalid job ID", ErrInvalidJobID)
	}

	job, err := handler.querySvc.GetByID(ctx, jobID)
	if err != nil {
		if errors.Is(err, query.ErrExportJobNotFound) {
			return notFoundBiz(ctx, fiberCtx, span, logger, err)
		}

		logSpanError(ctx, span, logger, "failed to get export job", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	if job == nil {
		logSpanError(ctx, span, logger, "export job unexpectedly nil", nil)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	if err := verifyJobTenantOwnership(ctx, job); err != nil {
		return notFoundBiz(ctx, fiberCtx, span, logger, err)
	}

	response := handler.mapJobToResponse(ctx, job)

	if writeErr := libHTTP.Respond(fiberCtx, fiber.StatusOK, response); writeErr != nil {
		return fmt.Errorf("write ok response: %w", writeErr)
	}

	return nil
}

// ListExportJobs handles GET /v1/export-jobs
// @ID listExportJobs
// @Summary List export jobs
// @Description Lists export jobs for the authenticated tenant using forward-only cursor-based pagination.
// @Description Use the nextCursor value from the response to fetch subsequent pages.
// @Tags Export Jobs
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param status query string false "Filter by status (QUEUED, RUNNING, SUCCEEDED, FAILED, EXPIRED, CANCELED)"
// @Param cursor query string false "Cursor for pagination (opaque)"
// @Param limit query int false "Maximum number of records to return" default(20) minimum(1) maximum(200)
// @Success 200 {object} ExportJobListResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/export-jobs [get]
func (handler *ExportJobHandlers) ListExportJobs(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startExportJobSpan(fiberCtx, "handler.export_job.list")

	defer span.End()

	cursor, limit, err := parseTimestampCursorPagination(fiberCtx)
	if err != nil {
		return badRequestBiz(ctx, fiberCtx, span, logger, "invalid pagination parameters", err)
	}

	var status *string
	if s := fiberCtx.Query("status"); s != "" {
		status = pointers.String(s)
	}

	jobs, pagination, err := handler.querySvc.List(ctx, query.ListExportJobsInput{
		Status: status,
		Cursor: cursor,
		Limit:  limit,
	})
	if err != nil {
		logSpanError(ctx, span, logger, "failed to list export jobs", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	responses := make([]*ExportJobResponse, len(jobs))
	for i, job := range jobs {
		responses[i] = handler.mapJobToResponse(ctx, job)
	}

	response := ExportJobListResponse{
		Items: responses,
		CursorResponse: sharedhttp.CursorResponse{
			Limit:      limit,
			HasMore:    pagination.Next != "",
			NextCursor: pagination.Next,
		},
	}

	if writeErr := libHTTP.Respond(fiberCtx, fiber.StatusOK, response); writeErr != nil {
		return fmt.Errorf("write ok response: %w", writeErr)
	}

	return nil
}

func parseTimestampCursorPagination(fiberCtx *fiber.Ctx) (*libHTTP.TimestampCursor, int, error) {
	cursor, limit, err := libHTTP.ParseTimestampCursorPagination(fiberCtx)
	if err != nil {
		return nil, 0, fmt.Errorf("parse timestamp cursor pagination: %w", err)
	}

	limit = libHTTP.ValidateLimit(limit, constants.DefaultPaginationLimit, constants.MaximumPaginationLimit)

	return cursor, limit, nil
}

// CancelExportJob handles POST /v1/export-jobs/:jobId/cancel
// @ID cancelExportJob
// @Summary Cancel an export job
// @Description Cancels a queued or running export job.
// @Tags Export Jobs
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param jobId path string true "Export Job ID" format(uuid)
// @Success 200 {object} ExportJobResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Export job not found"
// @Failure 409 {object} sharedhttp.ErrorResponse "Job in terminal state or idempotency conflict"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/export-jobs/{jobId}/cancel [post]
func (handler *ExportJobHandlers) CancelExportJob(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startExportJobSpan(fiberCtx, "handler.export_job.cancel")

	defer span.End()

	jobIDStr := fiberCtx.Params("jobId")

	jobID, err := uuid.Parse(jobIDStr)
	if err != nil {
		return badRequestBiz(ctx, fiberCtx, span, logger, "invalid job ID", ErrInvalidJobID)
	}

	existingJob, err := handler.querySvc.GetByID(ctx, jobID)
	if err != nil {
		if errors.Is(err, query.ErrExportJobNotFound) {
			return notFoundBiz(ctx, fiberCtx, span, logger, err)
		}

		logSpanError(ctx, span, logger, "failed to get export job for cancel", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	if existingJob == nil {
		return notFoundBiz(ctx, fiberCtx, span, logger, query.ErrExportJobNotFound)
	}

	if err := verifyJobTenantOwnership(ctx, existingJob); err != nil {
		return notFoundBiz(ctx, fiberCtx, span, logger, err)
	}

	if err := handler.exportJobUC.CancelExportJob(ctx, jobID); err != nil {
		if errors.Is(err, command.ErrExportJobNotFound) {
			return notFoundBiz(ctx, fiberCtx, span, logger, err)
		}

		if errors.Is(err, command.ErrJobInTerminalState) {
			logSpanBusinessEvent(ctx, span, logger, "job already in terminal state", err)

			return respondError(
				fiberCtx,
				fiber.StatusConflict,
				"conflict",
				"job is already in a terminal state",
			)
		}

		logSpanError(ctx, span, logger, "failed to cancel export job", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	job, err := handler.querySvc.GetByID(ctx, jobID)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to get cancelled job", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	if job == nil {
		logSpanError(ctx, span, logger, "cancelled job unexpectedly nil", nil)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	response := handler.mapJobToResponse(ctx, job)

	if writeErr := libHTTP.Respond(fiberCtx, fiber.StatusOK, response); writeErr != nil {
		return fmt.Errorf("write ok response: %w", writeErr)
	}

	return nil
}

// DownloadExportJob handles GET /v1/export-jobs/:jobId/download
// @ID downloadExportJob
// @Summary Download export file
// @Description Returns a presigned URL or redirects to download the export file.
// @Tags Export Jobs
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param jobId path string true "Export Job ID" format(uuid)
// @Success 200 {object} DownloadExportJobResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Export job not found"
// @Failure 409 {object} sharedhttp.ErrorResponse "Job not ready for download"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/export-jobs/{jobId}/download [get]
func (handler *ExportJobHandlers) DownloadExportJob(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startExportJobSpan(fiberCtx, "handler.export_job.download")

	defer span.End()

	jobIDStr := fiberCtx.Params("jobId")

	jobID, err := uuid.Parse(jobIDStr)
	if err != nil {
		return badRequestBiz(ctx, fiberCtx, span, logger, "invalid job ID", ErrInvalidJobID)
	}

	job, err := handler.querySvc.GetByID(ctx, jobID)
	if err != nil {
		if errors.Is(err, query.ErrExportJobNotFound) {
			return notFoundBiz(ctx, fiberCtx, span, logger, err)
		}

		logSpanError(ctx, span, logger, "failed to get export job", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	if job == nil {
		logSpanError(ctx, span, logger, "export job unexpectedly nil", nil)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	if err := verifyJobTenantOwnership(ctx, job); err != nil {
		return notFoundBiz(ctx, fiberCtx, span, logger, err)
	}

	if !job.IsDownloadable() {
		logSpanBusinessEvent(ctx, span, logger, "job not downloadable", ErrJobNotDownloadable)

		return respondError(
			fiberCtx,
			fiber.StatusConflict,
			"not_ready",
			"export job is not ready for download",
		)
	}

	if time.Now().After(job.ExpiresAt) {
		return respondError(
			fiberCtx,
			fiber.StatusGone,
			"expired",
			"export file has expired",
		)
	}

	runtimeConfig := handler.currentRuntimeConfigForContext(ctx)

	downloadURL, err := handler.storage.GeneratePresignedURL(ctx, job.FileKey, runtimeConfig.PresignExpiry)
	if err != nil {
		if errors.Is(err, sharedPorts.ErrObjectStorageUnavailable) {
			return respondError(fiberCtx, fiber.StatusServiceUnavailable, "object_storage_unavailable", "export storage is unavailable")
		}

		logSpanError(ctx, span, logger, "failed to generate download URL", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, DownloadExportJobResponse{
		DownloadURL: downloadURL,
		FileName:    job.FileName,
		SHA256:      job.SHA256,
		ExpiresIn:   int(runtimeConfig.PresignExpiry.Seconds()),
	}); err != nil {
		return fmt.Errorf("respond download export job: %w", err)
	}

	return nil
}

// ListExportJobsByContext handles GET /v1/contexts/:contextId/export-jobs
// @ID listExportJobsByContext
// @Summary List export jobs by context
// @Description Lists export jobs for a specific reconciliation context.
// @Tags Export Jobs
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param cursor query string false "Pagination cursor from previous response"
// @Param limit query int false "Maximum number of records to return" default(20) minimum(1) maximum(200)
// @Success 200 {object} ExportJobListResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/contexts/{contextId}/export-jobs [get]
func (handler *ExportJobHandlers) ListExportJobsByContext(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startExportJobSpan(fiberCtx, "handler.export_job.list_by_context")

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

	_, limit, err := libHTTP.ParseOpaqueCursorPagination(fiberCtx)
	if err != nil {
		return badRequestBiz(ctx, fiberCtx, span, logger, "invalid pagination parameters", err)
	}

	// Fetch limit+1 to determine if more pages exist without an extra COUNT query.
	jobs, err := handler.querySvc.ListByContext(ctx, contextID, limit+1)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to list export jobs by context", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	hasMore := len(jobs) > limit
	if hasMore {
		jobs = jobs[:limit]
	}

	responses := make([]*ExportJobResponse, len(jobs))
	for i, job := range jobs {
		responses[i] = handler.mapJobToResponse(ctx, job)
	}

	response := ExportJobListResponse{
		Items: responses,
		CursorResponse: sharedhttp.CursorResponse{
			Limit:   limit,
			HasMore: hasMore,
		},
	}

	if writeErr := libHTTP.Respond(fiberCtx, fiber.StatusOK, response); writeErr != nil {
		return fmt.Errorf("write ok response: %w", writeErr)
	}

	return nil
}

func (handler *ExportJobHandlers) mapJobToResponse(
	_ context.Context,
	job *entities.ExportJob,
) *ExportJobResponse {
	if job == nil {
		return &ExportJobResponse{}
	}

	response := &ExportJobResponse{
		ID:             job.ID.String(),
		ReportType:     string(job.ReportType),
		Format:         string(job.Format),
		Status:         string(job.Status),
		RecordsWritten: job.RecordsWritten,
		BytesWritten:   job.BytesWritten,
		CreatedAt:      job.CreatedAt.Format(time.RFC3339),
		ExpiresAt:      job.ExpiresAt.Format(time.RFC3339),
	}

	if job.FileName != "" {
		response.FileName = &job.FileName
	}

	if job.Error != "" {
		response.Error = &job.Error
	}

	if job.StartedAt != nil {
		response.StartedAt = pointers.String(job.StartedAt.Format(time.RFC3339))
	}

	if job.FinishedAt != nil {
		response.FinishedAt = pointers.String(job.FinishedAt.Format(time.RFC3339))
	}

	if job.IsDownloadable() && time.Now().Before(job.ExpiresAt) {
		response.DownloadURL = pointers.String("/v1/export-jobs/" + job.ID.String() + "/download")
	}

	return response
}
