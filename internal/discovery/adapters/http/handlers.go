// Package http provides HTTP handlers for discovery operations.
package http

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/discovery/adapters/http/dto"
	discoveryCommand "github.com/LerianStudio/matcher/internal/discovery/services/command"
	discoveryQuery "github.com/LerianStudio/matcher/internal/discovery/services/query"
)

// Discovery handler errors.
var (
	// ErrNilCommandUseCase is returned when the command use case is nil.
	ErrNilCommandUseCase = errors.New("command use case is required")
	// ErrNilQueryUseCase is returned when the query use case is nil.
	ErrNilQueryUseCase = errors.New("query use case is required")
)

// productionMode indicates whether the application is running in production.
// Set once during handler construction via NewHandler; governs SafeError behavior
// when logging internal failures.
var productionMode atomic.Bool

// Handler handles discovery HTTP requests.
type Handler struct {
	command *discoveryCommand.UseCase
	query   *discoveryQuery.UseCase
}

// NewHandler creates a new discovery HTTP handler.
func NewHandler(
	command *discoveryCommand.UseCase,
	query *discoveryQuery.UseCase,
	production bool,
) (*Handler, error) {
	if command == nil {
		return nil, ErrNilCommandUseCase
	}

	if query == nil {
		return nil, ErrNilQueryUseCase
	}

	productionMode.Store(production)

	return &Handler{
		command: command,
		query:   query,
	}, nil
}

func startHandlerSpan(fiberCtx *fiber.Ctx, name string) (context.Context, trace.Span, libLog.Logger) {
	ctx := fiberCtx.UserContext()
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

// GetDiscoveryStatus handles GET /v1/discovery/status.
//
// @ID getDiscoveryStatus
// @Summary Get discovery status
// @Description Returns the current Fetcher integration status including health and connection count.
// @Tags Discovery
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Success 200 {object} dto.DiscoveryStatusResponse "Discovery status"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/discovery/status [get]
func (handler *Handler) GetDiscoveryStatus(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "discovery.http.get_status")
	defer span.End()

	status, err := handler.query.GetDiscoveryStatus(ctx)
	if err != nil {
		logSpanError(ctx, span, logger, "get discovery status", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_error", "failed to get discovery status")
	}

	response := dto.DiscoveryStatusResponse{
		FetcherHealthy:  status.FetcherHealthy,
		ConnectionCount: status.ConnectionCount,
	}

	if !status.LastSyncAt.IsZero() {
		lastSyncAt := status.LastSyncAt
		response.LastSyncAt = &lastSyncAt
	}

	return fiberCtx.JSON(response)
}

// ListConnections handles GET /v1/discovery/connections.
//
// @ID listDiscoveryConnections
// @Summary List discovered connections
// @Description Returns all discovered Fetcher database connections.
// @Tags Discovery
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Success 200 {object} dto.ConnectionListResponse "List of connections"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/discovery/connections [get]
func (handler *Handler) ListConnections(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "discovery.http.list_connections")
	defer span.End()

	conns, err := handler.query.ListConnections(ctx)
	if err != nil {
		logSpanError(ctx, span, logger, "list connections", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_error", "failed to list connections")
	}

	responses := make([]dto.ConnectionResponse, 0, len(conns))
	for _, conn := range conns {
		responses = append(responses, dto.ConnectionFromEntity(conn))
	}

	return fiberCtx.JSON(dto.ConnectionListResponse{Connections: responses})
}

// GetConnection handles GET /v1/discovery/connections/:connectionId.
//
// @ID getDiscoveryConnection
// @Summary Get a discovered connection
// @Description Returns a single discovered Fetcher connection by its internal ID.
// @Tags Discovery
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param connectionId path string true "Connection ID (UUID)"
// @Success 200 {object} dto.ConnectionResponse "Connection details"
// @Failure 400 {object} ErrorResponse "Invalid connection ID"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 404 {object} ErrorResponse "Connection not found"
// @Router /v1/discovery/connections/{connectionId} [get]
func (handler *Handler) GetConnection(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "discovery.http.get_connection")
	defer span.End()

	connIDStr := fiberCtx.Params("connectionId")

	connID, err := uuid.Parse(connIDStr)
	if err != nil {
		logSpanError(ctx, span, logger, "invalid connection id", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", "invalid connection ID")
	}

	conn, err := handler.query.GetConnection(ctx, connID)
	if err != nil {
		logSpanError(ctx, span, logger, "get connection", err)

		// Check if it's actually a not-found error
		if errors.Is(err, discoveryQuery.ErrConnectionNotFound) {
			return libHTTP.RespondError(fiberCtx, fiber.StatusNotFound, "not_found", "connection not found")
		}

		return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_error", "failed to get connection")
	}

	return fiberCtx.JSON(dto.ConnectionFromEntity(conn))
}

// GetConnectionSchema handles GET /v1/discovery/connections/:connectionId/schema.
//
// @ID getDiscoveryConnectionSchema
// @Summary Get connection schema
// @Description Returns all discovered table schemas for a connection.
// @Tags Discovery
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param connectionId path string true "Connection ID (UUID)"
// @Success 200 {object} dto.ConnectionSchemaResponse "Schema for the connection"
// @Failure 400 {object} ErrorResponse "Invalid connection ID"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 404 {object} ErrorResponse "Connection not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/discovery/connections/{connectionId}/schema [get]
func (handler *Handler) GetConnectionSchema(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "discovery.http.get_connection_schema")
	defer span.End()

	connIDStr := fiberCtx.Params("connectionId")

	connID, err := uuid.Parse(connIDStr)
	if err != nil {
		logSpanError(ctx, span, logger, "invalid connection id", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", "invalid connection ID")
	}

	_, err = handler.query.GetConnection(ctx, connID)
	if err != nil {
		logSpanError(ctx, span, logger, "get connection for schema", err)

		if errors.Is(err, discoveryQuery.ErrConnectionNotFound) {
			return libHTTP.RespondError(fiberCtx, fiber.StatusNotFound, "not_found", "connection not found")
		}

		return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_error", "failed to get connection")
	}

	schemas, err := handler.query.GetConnectionSchema(ctx, connID)
	if err != nil {
		logSpanError(ctx, span, logger, "get schema", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_error", "failed to get schema")
	}

	tables := make([]dto.SchemaTableResponse, 0, len(schemas))
	for _, sch := range schemas {
		if sch == nil {
			continue
		}

		cols := make([]dto.SchemaColumnResponse, 0, len(sch.Columns))
		for _, col := range sch.Columns {
			cols = append(cols, dto.SchemaColumnResponse{
				Name:     col.Name,
				Type:     col.Type,
				Nullable: col.Nullable,
			})
		}

		tables = append(tables, dto.SchemaTableResponse{
			TableName: sch.TableName,
			Columns:   cols,
		})
	}

	return fiberCtx.JSON(dto.ConnectionSchemaResponse{
		ConnectionID: connID,
		Tables:       tables,
	})
}

// TestConnection handles POST /v1/discovery/connections/:connectionId/test.
//
// @ID testDiscoveryConnection
// @Summary Test a connection
// @Description Tests connectivity for a specific discovered connection owned by the current tenant.
// @Tags Discovery
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param connectionId path string true "Connection ID (UUID)"
// @Failure 400 {object} ErrorResponse "Invalid connection ID"
// @Success 200 {object} dto.TestConnectionResponse "Test result"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 404 {object} ErrorResponse "Connection not found"
// @Failure 503 {object} ErrorResponse "Fetcher service unavailable"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/discovery/connections/{connectionId}/test [post]
func (handler *Handler) TestConnection(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "discovery.http.test_connection")
	defer span.End()

	connIDStr := fiberCtx.Params("connectionId")

	connID, err := uuid.Parse(connIDStr)
	if err != nil {
		logSpanError(ctx, span, logger, "invalid connection id", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", "invalid connection ID")
	}

	result, err := handler.command.TestConnection(ctx, connID)
	if err != nil {
		if errors.Is(err, discoveryCommand.ErrConnectionNotFound) {
			logSpanError(ctx, span, logger, "connection not found", err)

			return libHTTP.RespondError(fiberCtx, fiber.StatusNotFound, "not_found", "connection not found")
		}

		if errors.Is(err, discoveryCommand.ErrFetcherUnavailable) {
			logSpanError(ctx, span, logger, "fetcher unavailable", err)

			return libHTTP.RespondError(fiberCtx, fiber.StatusServiceUnavailable, "service_unavailable", "fetcher service unavailable")
		}

		logSpanError(ctx, span, logger, "test connection", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_error", "failed to test connection")
	}

	return fiberCtx.JSON(dto.TestConnectionResponse{
		ConnectionID:  result.ConnectionID,
		FetcherConnID: result.FetcherConnID,
		Healthy:       result.Healthy,
		LatencyMs:     result.LatencyMs,
		ErrorMessage:  sanitizedConnectionTestError(result),
	})
}

func sanitizedConnectionTestError(result *discoveryCommand.ConnectionTestResult) string {
	if result == nil || result.Healthy || result.ErrorMessage == "" {
		return ""
	}

	return "connection test failed"
}

// RefreshDiscovery handles POST /v1/discovery/refresh.
//
// @ID refreshDiscovery
// @Summary Refresh discovery
// @Description Forces an immediate sync with the Fetcher service, updating connections and schemas.
// @Tags Discovery
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Success 200 {object} dto.RefreshDiscoveryResponse "Refresh result"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 503 {object} ErrorResponse "Fetcher service unavailable"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/discovery/refresh [post]
func (handler *Handler) RefreshDiscovery(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "discovery.http.refresh_discovery")
	defer span.End()

	synced, err := handler.command.RefreshDiscovery(ctx)
	if err != nil {
		if errors.Is(err, discoveryCommand.ErrFetcherUnavailable) {
			logSpanError(ctx, span, logger, "fetcher unavailable", err)

			return libHTTP.RespondError(fiberCtx, fiber.StatusServiceUnavailable, "service_unavailable", "fetcher service unavailable")
		}

		logSpanError(ctx, span, logger, "refresh discovery", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_error", "failed to refresh discovery")
	}

	return fiberCtx.JSON(dto.RefreshDiscoveryResponse{ConnectionsSynced: synced})
}

// ErrorResponse is a placeholder for Swagger documentation.
// The actual error response type is defined in lib-commons.
type ErrorResponse struct {
	Code    int    `json:"code"`
	Type    string `json:"type"`
	Message string `json:"message"`
}
