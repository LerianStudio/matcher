// Package http provides HTTP handlers for governance operations.
package http

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/governance/adapters/http/dto"
	governanceEntities "github.com/LerianStudio/matcher/internal/governance/domain/entities"
	governanceErrors "github.com/LerianStudio/matcher/internal/governance/domain/errors"
	"github.com/LerianStudio/matcher/internal/governance/domain/repositories"
	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// Sentinel errors for handler validation.
var (
	ErrRepoRequired       = errors.New("audit log repository is required")
	ErrMissingAuditLogID  = errors.New("audit log id is required")
	ErrInvalidAuditLogID  = errors.New("audit log id must be a valid UUID")
	ErrMissingEntityType  = errors.New("entity type is required")
	ErrMissingEntityID    = errors.New("entity id is required")
	ErrInvalidEntityID    = errors.New("entity id must be a valid UUID")
	ErrInvalidDateFromFmt = errors.New("date_from must be a valid date (YYYY-MM-DD or RFC3339)")
	ErrInvalidDateToFmt   = errors.New("date_to must be a valid date (YYYY-MM-DD or RFC3339)")
	ErrInvalidDateFormat  = errors.New("invalid date format")
)

// productionMode indicates whether the application is running in production.
// Set once during handler construction via NewHandler; governs SafeError behavior
// (suppresses internal error details in client responses when true).
var productionMode bool

// Handler handles HTTP requests for governance audit logs.
// It instruments each operation with OpenTelemetry metrics for observability:
// audit_log_created_total, audit_log_queries_total, and audit_log_query_latency_seconds.
type Handler struct {
	repo repositories.AuditLogRepository

	// OpenTelemetry metric instruments
	createdTotal     metric.Int64Counter
	queriesTotal     metric.Int64Counter
	queryLatencyHist metric.Float64Histogram
}

// NewHandler creates a new governance HTTP handler.
func NewHandler(repo repositories.AuditLogRepository, production bool) (*Handler, error) {
	if repo == nil {
		return nil, ErrRepoRequired
	}

	productionMode = production

	handler := &Handler{repo: repo}

	if err := handler.initMetrics(); err != nil {
		return nil, fmt.Errorf("init governance handler metrics: %w", err)
	}

	return handler, nil
}

// initMetrics creates the OpenTelemetry metric instruments for audit log operations.
func (handler *Handler) initMetrics() error {
	meter := otel.Meter("governance.http")

	var err error

	handler.createdTotal, err = meter.Int64Counter("audit_log_created_total",
		metric.WithDescription("Total number of audit log entries created"),
		metric.WithUnit("{entry}"))
	if err != nil {
		return fmt.Errorf("create audit_log_created_total counter: %w", err)
	}

	handler.queriesTotal, err = meter.Int64Counter("audit_log_queries_total",
		metric.WithDescription("Total number of audit log query operations"),
		metric.WithUnit("{query}"))
	if err != nil {
		return fmt.Errorf("create audit_log_queries_total counter: %w", err)
	}

	handler.queryLatencyHist, err = meter.Float64Histogram("audit_log_query_latency_seconds",
		metric.WithDescription("Latency of audit log query operations in seconds"),
		metric.WithUnit("s"))
	if err != nil {
		return fmt.Errorf("create audit_log_query_latency_seconds histogram: %w", err)
	}

	return nil
}

func startHandlerSpan(c *fiber.Ctx, name string) (context.Context, trace.Span, libLog.Logger) {
	ctx := c.UserContext()
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	if tracer == nil {
		tracer = otel.Tracer("governance.http")
	}

	ctx, span := tracer.Start(ctx, name)

	return ctx, span, logger
}

func logSpanError(ctx context.Context, span trace.Span, logger libLog.Logger, message string, err error) {
	libOpentelemetry.HandleSpanError(span, message, err)
	libLog.SafeError(logger, ctx, message, err, productionMode)
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

func writeNotFound(
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

// GetAuditLog retrieves a single audit log by ID.
// @Summary Get audit log by ID
// @Description Retrieves a single audit log entry by its ID. Audit logs contain immutable records of all system operations for compliance and debugging purposes.
// @ID getAuditLog
// @Tags Governance
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param id path string true "Audit Log ID" format(uuid)
// @Success 200 {object} dto.AuditLogResponse
// @Failure 400 {object} libHTTP.ErrorResponse "Invalid request payload"
// @Failure 401 {object} libHTTP.ErrorResponse "Unauthorized"
// @Failure 403 {object} libHTTP.ErrorResponse "Forbidden"
// @Failure 404 {object} libHTTP.ErrorResponse "Audit log not found"
// @Failure 500 {object} libHTTP.ErrorResponse "Internal server error"
// @Router /v1/governance/audit-logs/{id} [get]
func (handler *Handler) GetAuditLog(
	fiberCtx *fiber.Ctx,
) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.governance.get_audit_log")
	defer span.End()

	queryStart := time.Now()

	handler.queriesTotal.Add(ctx, 1)

	idStr := fiberCtx.Params("id")
	if idStr == "" {
		return badRequest(ctx, fiberCtx, span, logger, "audit log id is required", ErrMissingAuditLogID)
	}

	id, err := uuid.Parse(idStr)
	if err != nil {
		return badRequest(
			ctx,
			fiberCtx,
			span,
			logger,
			"invalid audit log id",
			fmt.Errorf("%w: %s", ErrInvalidAuditLogID, idStr),
		)
	}

	auditLog, err := handler.repo.GetByID(ctx, id)

	handler.queryLatencyHist.Record(ctx, time.Since(queryStart).Seconds())

	if err != nil {
		if errors.Is(err, governanceErrors.ErrAuditLogNotFound) {
			return writeNotFound(ctx, fiberCtx, span, logger, "audit log not found", err)
		}

		return writeServiceError(ctx, fiberCtx, span, logger, "failed to get audit log", err)
	}

	if auditLog == nil {
		return writeNotFound(
			ctx,
			fiberCtx,
			span,
			logger,
			"audit log not found",
			governanceErrors.ErrAuditLogNotFound,
		)
	}

	if writeErr := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.AuditLogToResponse(auditLog)); writeErr != nil {
		return fmt.Errorf("write ok response: %w", writeErr)
	}

	return nil
}

// ListAuditLogsByEntity retrieves audit logs for a specific entity.
// @Summary List audit logs by entity
// @Description Returns a cursor-paginated list of audit log entries for a specific entity, ordered by creation time descending. Use this to trace the complete change history of any entity in the system. Pagination is forward-only (no prevCursor); use the nextCursor value to fetch subsequent pages.
// @ID listAuditLogsByEntity
// @Tags Governance
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param entityType path string true "Entity type" example(context)
// @Param entityId path string true "Entity ID" format(uuid)
// @Param limit query int false "Maximum number of records to return" default(20) minimum(1) maximum(200)
// @Param cursor query string false "Cursor for pagination (opaque)"
// @Success 200 {object} dto.ListAuditLogsResponse
// @Failure 400 {object} libHTTP.ErrorResponse "Invalid request payload"
// @Failure 401 {object} libHTTP.ErrorResponse "Unauthorized"
// @Failure 403 {object} libHTTP.ErrorResponse "Forbidden"
// @Failure 500 {object} libHTTP.ErrorResponse "Internal server error"
// @Router /v1/governance/entities/{entityType}/{entityId}/audit-logs [get]
func (handler *Handler) ListAuditLogsByEntity(
	fiberCtx *fiber.Ctx,
) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.governance.list_audit_logs_by_entity")
	defer span.End()

	queryStart := time.Now()

	handler.queriesTotal.Add(ctx, 1)

	entityType := fiberCtx.Params("entityType")
	if entityType == "" {
		return badRequest(ctx, fiberCtx, span, logger, "entity type is required", ErrMissingEntityType)
	}

	entityIDStr := fiberCtx.Params("entityId")
	if entityIDStr == "" {
		return badRequest(ctx, fiberCtx, span, logger, "entity id is required", ErrMissingEntityID)
	}

	entityID, err := uuid.Parse(entityIDStr)
	if err != nil {
		return badRequest(
			ctx,
			fiberCtx,
			span,
			logger,
			"invalid entity id",
			fmt.Errorf("%w: %s", ErrInvalidEntityID, entityIDStr),
		)
	}

	cursor, limit, err := parseTimestampCursorPagination(fiberCtx)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid pagination parameters", err)
	}

	logs, nextCursor, err := handler.repo.ListByEntity(ctx, entityType, entityID, cursor, limit)

	handler.queryLatencyHist.Record(ctx, time.Since(queryStart).Seconds())

	if err != nil {
		return writeServiceError(ctx, fiberCtx, span, logger, "failed to list audit logs", err)
	}

	response := dto.ListAuditLogsResponse{
		Items: dto.AuditLogsToResponse(logs),
		CursorResponse: dto.CursorResponse{
			Limit:      limit,
			NextCursor: nextCursor,
			HasMore:    nextCursor != "",
		},
	}

	if writeErr := libHTTP.Respond(fiberCtx, fiber.StatusOK, response); writeErr != nil {
		return fmt.Errorf("write ok response: %w", writeErr)
	}

	return nil
}

// ListAuditLogs retrieves audit logs with optional filters.
// @Summary List audit logs
// @Description Returns a cursor-paginated list of audit log entries with optional filters. Use this to search across all audit logs in the system. Pagination is forward-only (no prevCursor); use the nextCursor value to fetch subsequent pages.
// @ID listAuditLogs
// @Tags Governance
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param actor query string false "Filter by actor ID"
// @Param date_from query string false "Filter logs from this date (YYYY-MM-DD or RFC3339)"
// @Param date_to query string false "Filter logs until this date (YYYY-MM-DD or RFC3339)"
// @Param action query string false "Filter by action type" example(CREATE)
// @Param entity_type query string false "Filter by entity type" example(context)
// @Param limit query int false "Maximum number of records to return" default(20) minimum(1) maximum(200)
// @Param cursor query string false "Cursor for pagination (opaque)"
// @Success 200 {object} dto.ListAuditLogsResponse
// @Failure 400 {object} libHTTP.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} libHTTP.ErrorResponse "Unauthorized"
// @Failure 403 {object} libHTTP.ErrorResponse "Forbidden"
// @Failure 500 {object} libHTTP.ErrorResponse "Internal server error"
// @Router /v1/governance/audit-logs [get]
func (handler *Handler) ListAuditLogs(
	fiberCtx *fiber.Ctx,
) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.governance.list_audit_logs")
	defer span.End()

	queryStart := time.Now()

	handler.queriesTotal.Add(ctx, 1)

	filter, err := parseAuditLogFilter(fiberCtx)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	cursor, limit, err := parseTimestampCursorPagination(fiberCtx)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid pagination parameters", err)
	}

	logs, nextCursor, err := handler.repo.List(ctx, filter, cursor, limit)

	handler.queryLatencyHist.Record(ctx, time.Since(queryStart).Seconds())

	if err != nil {
		return writeServiceError(ctx, fiberCtx, span, logger, "failed to list audit logs", err)
	}

	response := dto.ListAuditLogsResponse{
		Items: dto.AuditLogsToResponse(logs),
		CursorResponse: dto.CursorResponse{
			Limit:      limit,
			NextCursor: nextCursor,
			HasMore:    nextCursor != "",
		},
	}

	if writeErr := libHTTP.Respond(fiberCtx, fiber.StatusOK, response); writeErr != nil {
		return fmt.Errorf("write ok response: %w", writeErr)
	}

	return nil
}

func parseAuditLogFilter(fiberCtx *fiber.Ctx) (governanceEntities.AuditLogFilter, error) {
	var filter governanceEntities.AuditLogFilter

	if actor := fiberCtx.Query("actor"); actor != "" {
		if err := libHTTP.ValidateQueryParamLength(actor, "actor", libHTTP.MaxQueryParamLengthLong); err != nil {
			return filter, fmt.Errorf("validate actor query param: %w", err)
		}

		filter.Actor = &actor
	}

	if action := fiberCtx.Query("action"); action != "" {
		if err := libHTTP.ValidateQueryParamLength(action, "action", libHTTP.MaxQueryParamLengthShort); err != nil {
			return filter, fmt.Errorf("validate action query param: %w", err)
		}

		filter.Action = &action
	}

	if entityType := fiberCtx.Query("entity_type"); entityType != "" {
		if err := libHTTP.ValidateQueryParamLength(entityType, "entity_type", libHTTP.MaxQueryParamLengthShort); err != nil {
			return filter, fmt.Errorf("validate entity_type query param: %w", err)
		}

		filter.EntityType = &entityType
	}

	if dateFrom := fiberCtx.Query("date_from"); dateFrom != "" {
		t, err := parseDate(dateFrom)
		if err != nil {
			return filter, ErrInvalidDateFromFmt
		}

		filter.DateFrom = &t
	}

	if dateTo := fiberCtx.Query("date_to"); dateTo != "" {
		t, err := parseDateTo(dateTo)
		if err != nil {
			return filter, ErrInvalidDateToFmt
		}

		filter.DateTo = &t
	}

	return filter, nil
}

func parseTimestampCursorPagination(fiberCtx *fiber.Ctx) (*libHTTP.TimestampCursor, int, error) {
	cursor, limit, err := libHTTP.ParseTimestampCursorPagination(fiberCtx)
	if err != nil {
		return nil, 0, fmt.Errorf("parse timestamp cursor pagination: %w", err)
	}

	limit = libHTTP.ValidateLimit(limit, constants.DefaultPaginationLimit, constants.MaximumPaginationLimit)

	return cursor, limit, nil
}

func parseDate(dateStr string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, dateStr); err == nil {
		return t, nil
	}

	if t, err := time.Parse(time.DateOnly, dateStr); err == nil {
		return t, nil
	}

	return time.Time{}, fmt.Errorf("%w: %s", ErrInvalidDateFormat, dateStr)
}

// parseDateTo parses a date string for the "date_to" filter.
// For RFC3339 timestamps (e.g., "2025-01-15T23:59:59Z"), it uses the exact time.
// For DateOnly format (e.g., "2025-01-15"), it returns 23:59:59.999999999 of that day
// to include all records from the entire day (end-of-day inclusive).
func parseDateTo(dateStr string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, dateStr); err == nil {
		return t, nil
	}

	if t, err := time.Parse(time.DateOnly, dateStr); err == nil {
		return t.Add(24*time.Hour - time.Nanosecond), nil
	}

	return time.Time{}, fmt.Errorf("%w: %s", ErrInvalidDateFormat, dateStr)
}
