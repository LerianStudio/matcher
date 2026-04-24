// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package http provides HTTP handlers for ingestion operations.
package http

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"

	"github.com/gofiber/fiber/v2"
	"go.opentelemetry.io/otel/trace"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	ingestionRepositories "github.com/LerianStudio/matcher/internal/ingestion/domain/repositories"
	"github.com/LerianStudio/matcher/internal/ingestion/services/command"
	"github.com/LerianStudio/matcher/internal/ingestion/services/query"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
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
	// ErrNilJobRepository indicates job repository is nil.
	ErrNilJobRepository = errors.New("job repository is required")
	// ErrNilTransactionRepository indicates transaction repository is nil.
	ErrNilTransactionRepository = errors.New("transaction repository is required")
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
type ReconciliationContextInfo = sharedPorts.ContextAccessInfo

type contextProvider = sharedPorts.ContextAccessProvider

// Handlers provides HTTP handlers for ingestion operations.
//
// productionMode governs SafeError behavior (per-handler bool — see
// matching/http for the same pattern and rationale).
//
// The jobRepo and transactionRepo fields back the list/search handlers
// directly. The corresponding query UseCase methods (ListJobsByContext,
// ListTransactionsByJobContext, SearchTransactions) were span-only
// wrappers around the repos.
type Handlers struct {
	commandUC       *command.UseCase
	queryUC         *query.UseCase
	jobRepo         ingestionRepositories.JobRepository
	transactionRepo ingestionRepositories.TransactionRepository
	contextProvider contextProvider
	contextVerifier libHTTP.TenantOwnershipVerifier
	productionMode  bool
}

// NewHandlers creates a new Handlers instance with the given use cases.
func NewHandlers(
	commandUC *command.UseCase,
	queryUC *query.UseCase,
	jobRepo ingestionRepositories.JobRepository,
	transactionRepo ingestionRepositories.TransactionRepository,
	ctxProvider contextProvider,
	production bool,
) (*Handlers, error) {
	if commandUC == nil {
		return nil, ErrNilCommandUseCase
	}

	if queryUC == nil {
		return nil, ErrNilQueryUseCase
	}

	if jobRepo == nil {
		return nil, ErrNilJobRepository
	}

	if transactionRepo == nil {
		return nil, ErrNilTransactionRepository
	}

	if ctxProvider == nil {
		return nil, ErrNilContextProvider
	}

	verifier := NewTenantOwnershipVerifier(ctxProvider)

	return &Handlers{
		commandUC:       commandUC,
		queryUC:         queryUC,
		jobRepo:         jobRepo,
		transactionRepo: transactionRepo,
		contextProvider: ctxProvider,
		contextVerifier: verifier,
		productionMode:  production,
	}, nil
}

func startHandlerSpan(c *fiber.Ctx, name string) (context.Context, trace.Span, libLog.Logger) {
	return sharedhttp.StartHandlerSpan(c, name)
}

func (handler *Handlers) logSpanError(ctx context.Context, span trace.Span, logger libLog.Logger, message string, err error) {
	sharedhttp.LogSpanError(ctx, span, logger, handler.productionMode, message, err)
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

//nolint:wrapcheck // HTTP transport response is the terminal error boundary.
func (handler *Handlers) badRequest(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	message string,
	err error,
) error {
	return sharedhttp.BadRequest(ctx, fiberCtx, span, logger, handler.productionMode, message, err)
}

//nolint:wrapcheck // HTTP transport response is the terminal error boundary.
func respondError(fiberCtx *fiber.Ctx, status int, slug, message string) error {
	return sharedhttp.RespondError(fiberCtx, status, slug, message)
}

//nolint:wrapcheck // HTTP transport response is the terminal error boundary.
func respondContextVerificationError(fiberCtx *fiber.Ctx, err error) error {
	return sharedhttp.RespondProductError(fiberCtx, sharedhttp.ValidateContextVerificationError(err))
}

func (handler *Handlers) notFound(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	slug,
	message string,
	err error,
) error {
	sharedhttp.LogSpanError(ctx, span, logger, handler.productionMode, message, err)

	return respondError(fiberCtx, fiber.StatusNotFound, slug, message)
}

// handleContextVerificationError maps errors from ParseAndVerifyTenantScopedID to HTTP responses.
func (handler *Handlers) handleContextVerificationError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) error {
	if errors.Is(err, libHTTP.ErrMissingContextID) || errors.Is(err, libHTTP.ErrInvalidContextID) {
		handler.logSpanError(ctx, span, logger, "context verification failed", err)

		return respondContextVerificationError(fiberCtx, err)
	}

	handler.logSpanError(ctx, span, logger, "context verification failed", err)

	return respondContextVerificationError(fiberCtx, err)
}

// handleIngestionError maps errors from StartIngestion to appropriate HTTP responses.
func (handler *Handlers) handleIngestionError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) error {
	// Source not found → 404
	if errors.Is(err, command.ErrSourceNotFound) {
		return handler.notFound(ctx, fiberCtx, span, logger, "ingestion_source_not_found", "source not found", err)
	}

	// Field map not found → 404
	if errors.Is(err, command.ErrFieldMapNotFound) {
		return handler.notFound(ctx, fiberCtx, span, logger, "ingestion_field_map_not_found", "field mapping not found for source", err)
	}

	// Empty file (EOF reading headers) → 400
	if errors.Is(err, io.EOF) || strings.Contains(err.Error(), "failed to read csv headers: EOF") ||
		strings.Contains(err.Error(), "failed to read json: EOF") ||
		strings.Contains(err.Error(), "failed to decode xml: EOF") {
		handler.logSpanError(ctx, span, logger, "file is empty or has no content", err)

		return respondError(fiberCtx, fiber.StatusBadRequest, "ingestion_empty_file", "file is empty or has no content")
	}

	// Format required → 400
	if errors.Is(err, command.ErrFormatRequiredUC) {
		handler.logSpanError(ctx, span, logger, "format is required", err)

		return respondError(fiberCtx, fiber.StatusBadRequest, "ingestion_format_required", "format is required")
	}

	// Empty file (no data rows) → 400
	if errors.Is(err, command.ErrEmptyFile) {
		handler.logSpanError(ctx, span, logger, "file contains no data rows", err)

		return respondError(fiberCtx, fiber.StatusBadRequest, "ingestion_empty_file", "file contains no data rows")
	}

	// Generic server error
	handler.logSpanError(ctx, span, logger, "failed to start ingestion", err)

	return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
}
