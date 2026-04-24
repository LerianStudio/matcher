// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package http provides HTTP handlers for the reporting context.
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

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	reportingRepos "github.com/LerianStudio/matcher/internal/reporting/domain/repositories"
	"github.com/LerianStudio/matcher/internal/reporting/services/query"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

const (
	maxDateRangeDays = 90
	hoursPerDay      = 24

	formatCSV       = "csv"
	formatPDF       = "pdf"
	contentTypeCSV  = "text/csv"
	contentTypePDF  = "application/pdf"
	contentDispoFmt = "attachment; filename=\""
)

var (
	// ErrNilDashboardUseCase indicates dashboard use case is nil.
	ErrNilDashboardUseCase = errors.New("dashboard use case is required")
	// ErrNilContextProvider indicates context provider is nil.
	ErrNilContextProvider = errors.New("context provider is required")
	// ErrInvalidDateRange indicates invalid date range.
	ErrInvalidDateRange = errors.New("invalid date range")
	// ErrDateFromRequired indicates date_from parameter is missing.
	ErrDateFromRequired = errors.New("date_from is required")
	// ErrDateToRequired indicates date_to parameter is missing.
	ErrDateToRequired = errors.New("date_to is required")
	// ErrInvalidSourceID indicates source_id parameter is invalid.
	ErrInvalidSourceID = errors.New("source_id must be a valid UUID")
	// ErrDateRangeExceeded indicates the date range exceeds the maximum allowed.
	ErrDateRangeExceeded = errors.New("date range cannot exceed 90 days")
	// ErrInvalidExportFormat indicates export format is invalid.
	ErrInvalidExportFormat = errors.New("format must be csv or pdf")
	// ErrInvalidSortOrder indicates sort_order parameter is invalid.
	ErrInvalidSortOrder = errors.New("sort_order must be asc or desc")
	// ErrNilExportUseCase indicates export use case is nil.
	ErrNilExportUseCase = errors.New("export use case is required")
	// ErrNilReportRepository indicates report repository is nil.
	ErrNilReportRepository = errors.New("report repository is required")
)

// ReconciliationContextInfo contains the context information needed by reporting.
type ReconciliationContextInfo = sharedPorts.ContextAccessInfo

type contextProvider = sharedPorts.ContextAccessProvider

// Handlers provides HTTP handlers for reporting operations.
//
// productionMode governs SafeError behavior (suppresses internal error
// details in client responses when true). Stored as a per-handler bool
// rather than a package-level atomic.Bool — the previous shared-global
// state coupled every test in the package to whichever test last
// constructed a handler, regardless of the production flag each test
// wanted to exercise.
type Handlers struct {
	dashboardUC     *query.DashboardUseCase
	exportUC        *query.UseCase
	reportRepo      reportingRepos.ReportRepository
	contextProvider contextProvider
	contextVerifier libHTTP.TenantOwnershipVerifier
	productionMode  bool
}

// NewHandlers creates a new Handlers instance with the given use cases.
//
// reportRepo backs the span-only Get*/Count* handlers directly. The
// corresponding query UseCase methods were span-only wrappers around
// the repo — see T-009b handoff for context.
func NewHandlers(
	dashboardUC *query.DashboardUseCase,
	ctxProvider contextProvider,
	exportUC *query.UseCase,
	reportRepo reportingRepos.ReportRepository,
	production bool,
) (*Handlers, error) {
	if dashboardUC == nil {
		return nil, ErrNilDashboardUseCase
	}

	if ctxProvider == nil {
		return nil, ErrNilContextProvider
	}

	if exportUC == nil {
		return nil, ErrNilExportUseCase
	}

	if reportRepo == nil {
		return nil, ErrNilReportRepository
	}

	verifier := NewTenantOwnershipVerifier(ctxProvider)

	return &Handlers{
		dashboardUC:     dashboardUC,
		exportUC:        exportUC,
		reportRepo:      reportRepo,
		contextProvider: ctxProvider,
		contextVerifier: verifier,
		productionMode:  production,
	}, nil
}

func startHandlerSpan(c *fiber.Ctx, name string) (context.Context, trace.Span, libLog.Logger) {
	return sharedhttp.StartHandlerSpan(c, name)
}

// The helpers below (logSpanError, badRequest) are defined as methods on
// every handler type in the reporting package so they can read
// productionMode from the receiver. Previously they were package-level
// free functions reading a shared atomic.Bool, which coupled every test
// in the package to whichever test last constructed a handler.

func (handler *Handlers) logSpanError(ctx context.Context, span trace.Span, logger libLog.Logger, message string, err error) {
	sharedhttp.LogSpanError(ctx, span, logger, handler.productionMode, message, err)
}

//nolint:wrapcheck // HTTP transport response is the terminal error boundary.
func respondError(fiberCtx *fiber.Ctx, status int, slug, message string) error {
	return sharedhttp.RespondError(fiberCtx, status, slug, message)
}

//nolint:wrapcheck // HTTP transport response is the terminal error boundary.
func respondContextVerificationError(fiberCtx *fiber.Ctx, err error) error {
	return sharedhttp.RespondProductError(fiberCtx, sharedhttp.ValidateContextVerificationError(err))
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

func parseDashboardFilter(
	fiberCtx *fiber.Ctx,
	contextID uuid.UUID,
) (entities.DashboardFilter, error) {
	dateFrom, dateTo, err := parseDateFilter(fiberCtx)
	if err != nil {
		return entities.DashboardFilter{}, err
	}

	sourceID, err := parseSourceIDFilter(fiberCtx)
	if err != nil {
		return entities.DashboardFilter{}, err
	}

	return entities.DashboardFilter{
		ContextID: contextID,
		DateFrom:  dateFrom,
		DateTo:    dateTo,
		SourceID:  sourceID,
	}, nil
}

func parseDateFilter(fiberCtx *fiber.Ctx) (time.Time, time.Time, error) {
	dateFromStr := fiberCtx.Query("date_from")
	if dateFromStr == "" {
		return time.Time{}, time.Time{}, ErrDateFromRequired
	}

	dateToStr := fiberCtx.Query("date_to")
	if dateToStr == "" {
		return time.Time{}, time.Time{}, ErrDateToRequired
	}

	dateFrom, err := time.Parse(time.DateOnly, dateFromStr)
	if err != nil {
		return time.Time{}, time.Time{}, ErrInvalidDateRange
	}

	dateTo, err := time.Parse(time.DateOnly, dateToStr)
	if err != nil {
		return time.Time{}, time.Time{}, ErrInvalidDateRange
	}

	dateTo = dateTo.Add(hoursPerDay*time.Hour - time.Nanosecond)

	if dateFrom.After(dateTo) {
		return time.Time{}, time.Time{}, ErrInvalidDateRange
	}

	if dateTo.Sub(dateFrom).Hours() > float64(maxDateRangeDays*hoursPerDay) {
		return time.Time{}, time.Time{}, ErrDateRangeExceeded
	}

	return dateFrom, dateTo, nil
}

func parseSourceIDFilter(fiberCtx *fiber.Ctx) (*uuid.UUID, error) {
	sourceIDStr := fiberCtx.Query("source_id")
	if sourceIDStr == "" {
		return nil, nil
	}

	sourceID, err := uuid.Parse(sourceIDStr)
	if err != nil {
		return nil, ErrInvalidSourceID
	}

	return &sourceID, nil
}

func parseReportFilter(fiberCtx *fiber.Ctx, contextID uuid.UUID) (entities.ReportFilter, error) {
	dateFrom, dateTo, err := parseDateFilter(fiberCtx)
	if err != nil {
		return entities.ReportFilter{}, err
	}

	sourceID, err := parseSourceIDFilter(fiberCtx)
	if err != nil {
		return entities.ReportFilter{}, err
	}

	cursor, limit, err := libHTTP.ParseOpaqueCursorPagination(fiberCtx)
	if err != nil {
		return entities.ReportFilter{}, fmt.Errorf("invalid pagination: %w", err)
	}

	sortOrder, err := parseSortOrder(fiberCtx)
	if err != nil {
		return entities.ReportFilter{}, err
	}

	return entities.ReportFilter{
		ContextID: contextID,
		DateFrom:  dateFrom,
		DateTo:    dateTo,
		SourceID:  sourceID,
		Limit:     limit,
		Cursor:    cursor,
		SortOrder: sortOrder,
	}, nil
}

func parseVarianceReportFilter(
	fiberCtx *fiber.Ctx,
	contextID uuid.UUID,
) (entities.VarianceReportFilter, error) {
	dateFrom, dateTo, err := parseDateFilter(fiberCtx)
	if err != nil {
		return entities.VarianceReportFilter{}, err
	}

	sourceID, err := parseSourceIDFilter(fiberCtx)
	if err != nil {
		return entities.VarianceReportFilter{}, err
	}

	cursor, limit, err := libHTTP.ParseOpaqueCursorPagination(fiberCtx)
	if err != nil {
		return entities.VarianceReportFilter{}, fmt.Errorf("invalid pagination: %w", err)
	}

	sortOrder, err := parseSortOrder(fiberCtx)
	if err != nil {
		return entities.VarianceReportFilter{}, err
	}

	return entities.VarianceReportFilter{
		ContextID: contextID,
		DateFrom:  dateFrom,
		DateTo:    dateTo,
		SourceID:  sourceID,
		Limit:     limit,
		Cursor:    cursor,
		SortOrder: sortOrder,
	}, nil
}

// parseSortOrder validates and normalizes the sort_order query parameter.
// Accepts "asc" or "desc" (case-insensitive), defaults to "desc" when empty.
func parseSortOrder(fiberCtx *fiber.Ctx) (string, error) {
	raw := fiberCtx.Query("sort_order", "desc")
	normalized := strings.ToLower(raw)

	if normalized != "asc" && normalized != "desc" {
		return "", ErrInvalidSortOrder
	}

	return normalized, nil
}
