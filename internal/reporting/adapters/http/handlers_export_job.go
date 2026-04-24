// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package http

import (
	"context"
	"errors"
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
	reportingRepos "github.com/LerianStudio/matcher/internal/reporting/domain/repositories"
	"github.com/LerianStudio/matcher/internal/reporting/services/command"
	"github.com/LerianStudio/matcher/internal/reporting/services/query"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
	"github.com/LerianStudio/matcher/internal/shared/objectstorage"
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
	// ErrNilExportJobRepository indicates export job repository is nil.
	ErrNilExportJobRepository = errors.New("export job repository is required")
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

// ExportJobRuntimeConfig controls runtime-sensitive handler behavior without
// coupling the reporting package to bootstrap internals.
type ExportJobRuntimeConfig struct {
	Enabled       *bool
	PresignExpiry time.Duration
}

// ExportJobHandlers provides HTTP handlers for export job operations.
//
// productionMode governs SafeError behavior (suppresses internal error
// details in client responses when true). Stored per-handler rather than
// on a package-level atomic.Bool to avoid cross-test coupling via shared
// global state.
type ExportJobHandlers struct {
	exportJobUC           *command.ExportJobUseCase
	querySvc              *query.ExportJobQueryService
	exportJobRepo         reportingRepos.ExportJobRepository
	storage               objectstorage.Backend
	contextVerifier       libHTTP.TenantOwnershipVerifier
	enabled               bool
	presignExpiry         time.Duration
	runtimeConfigResolver func(context.Context) ExportJobRuntimeConfig
	productionMode        bool
}

// logSpanBusinessEvent records a business-outcome event on the span without marking
// the span as errored. Use for expected outcomes (validation, not-found, conflict)
// that are not infrastructure failures.
//
// Defined as a method so it reads productionMode from the receiver rather
// than a package-global atomic.Bool.
func (handler *ExportJobHandlers) logSpanBusinessEvent(ctx context.Context, span trace.Span, logger libLog.Logger, message string, err error) {
	libOpentelemetry.HandleSpanBusinessErrorEvent(span, message, err)
	libLog.SafeError(logger, ctx, message, err, handler.productionMode)
}

// logSpanError wraps sharedhttp.LogSpanError; reads productionMode from the receiver.
func (handler *ExportJobHandlers) logSpanError(ctx context.Context, span trace.Span, logger libLog.Logger, message string, err error) {
	sharedhttp.LogSpanError(ctx, span, logger, handler.productionMode, message, err)
}

// badRequestBiz responds with 400 and records a business event (not a span error).
// Use for validation failures and malformed input — expected client behaviour.
func (handler *ExportJobHandlers) badRequestBiz(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	message string,
	err error,
) error {
	return handler.badRequestBizWithSlug(ctx, fiberCtx, span, logger, "invalid_request", message, err)
}

func (handler *ExportJobHandlers) badRequestBizWithSlug(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	slug, message string,
	err error,
) error {
	handler.logSpanBusinessEvent(ctx, span, logger, message, err)

	return respondError(fiberCtx, fiber.StatusBadRequest, slug, message)
}

// notFoundBiz responds with 404 and records a business event (not a span error).
// Use for "entity does not exist" responses — expected business outcome.
func (handler *ExportJobHandlers) notFoundBiz(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) error {
	const message = "export job not found"

	handler.logSpanBusinessEvent(ctx, span, logger, message, err)

	return respondError(fiberCtx, fiber.StatusNotFound, "reporting_export_job_not_found", message)
}

// handleContextVerificationError maps errors from ParseAndVerifyTenantScopedID to HTTP responses.
func (handler *ExportJobHandlers) handleContextVerificationError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) error {
	handler.logSpanError(ctx, span, logger, "context verification failed", err)

	return respondContextVerificationError(fiberCtx, err)
}

// NewExportJobHandlers creates a new ExportJobHandlers instance.
// presignExpiry configures how long download URLs remain valid.
// If zero or negative, defaults to 1 hour.
func NewExportJobHandlers(
	exportJobUC *command.ExportJobUseCase,
	querySvc *query.ExportJobQueryService,
	exportJobRepo reportingRepos.ExportJobRepository,
	storage objectstorage.Backend,
	ctxProvider contextProvider,
	presignExpiry time.Duration,
	production bool,
) (*ExportJobHandlers, error) {
	if exportJobUC == nil {
		return nil, ErrNilExportJobUseCase
	}

	if querySvc == nil {
		return nil, ErrNilExportJobQueryService
	}

	if exportJobRepo == nil {
		return nil, ErrNilExportJobRepository
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
		exportJobRepo:   exportJobRepo,
		storage:         storage,
		contextVerifier: verifier,
		enabled:         true,
		presignExpiry:   presignExpiry,
		productionMode:  production,
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
