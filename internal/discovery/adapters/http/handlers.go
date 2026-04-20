// Package http provides HTTP handlers for discovery operations.
package http

import (
	"context"
	"errors"
	"fmt"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/discovery/adapters/http/dto"
	discoveryCommand "github.com/LerianStudio/matcher/internal/discovery/services/command"
	discoveryQuery "github.com/LerianStudio/matcher/internal/discovery/services/query"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
)

// Discovery handler errors.
var (
	// ErrNilCommandUseCase is returned when the command use case is nil.
	ErrNilCommandUseCase = errors.New("command use case is required")
	// ErrNilQueryUseCase is returned when the query use case is nil.
	ErrNilQueryUseCase = errors.New("query use case is required")
)

// Handler handles discovery HTTP requests.
//
// productionMode is captured as a per-handler bool rather than a package-
// level atomic.Bool. The previous shared-global state was fine at runtime
// (the flag is written once in NewHandler) but leaked across tests: any
// test that constructed a handler with production=true silently changed
// the observable error-message behavior for every other test in the
// package. Per-handler state eliminates that cross-test coupling.
type Handler struct {
	command        *discoveryCommand.UseCase
	query          *discoveryQuery.UseCase
	staleness      stalenessProvider
	productionMode bool
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

	return &Handler{
		command:        command,
		query:          query,
		productionMode: production,
	}, nil
}

func startHandlerSpan(fiberCtx *fiber.Ctx, name string) (context.Context, trace.Span, libLog.Logger) {
	return sharedhttp.StartHandlerSpan(fiberCtx, name)
}

// logSpanError forwards to sharedhttp.LogSpanError using this handler's
// productionMode. Unexported method — call sites read `handler.logSpanError`.
func (handler *Handler) logSpanError(ctx context.Context, span trace.Span, logger libLog.Logger, message string, err error) {
	sharedhttp.LogSpanError(ctx, span, logger, handler.productionMode, message, err)
}

//nolint:wrapcheck // HTTP transport response is the terminal error boundary.
func respondError(fiberCtx *fiber.Ctx, status int, slug, message string) error {
	return sharedhttp.RespondError(fiberCtx, status, slug, message)
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
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/discovery/status [get]
func (handler *Handler) GetDiscoveryStatus(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "discovery.http.get_status")
	defer span.End()

	status, err := handler.query.GetDiscoveryStatus(ctx)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "get discovery status", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "failed to get discovery status")
	}

	response := dto.DiscoveryStatusResponse{
		FetcherHealthy:  status.FetcherHealthy,
		ConnectionCount: status.ConnectionCount,
	}

	if !status.LastSyncAt.IsZero() {
		lastSyncAt := status.LastSyncAt
		response.LastSyncAt = &lastSyncAt
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, response); err != nil {
		return fmt.Errorf("respond get discovery status: %w", err)
	}

	return nil
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
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/discovery/connections [get]
func (handler *Handler) ListConnections(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "discovery.http.list_connections")
	defer span.End()

	conns, err := handler.query.ListConnections(ctx)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "list connections", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "failed to list connections")
	}

	responses := make([]dto.ConnectionResponse, 0, len(conns))
	for _, conn := range conns {
		responses = append(responses, dto.ConnectionFromEntity(conn))
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ConnectionListResponse{Connections: responses}); err != nil {
		return fmt.Errorf("respond list connections: %w", err)
	}

	return nil
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
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid connection ID"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Connection not found"
// @Router /v1/discovery/connections/{connectionId} [get]
func (handler *Handler) GetConnection(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "discovery.http.get_connection")
	defer span.End()

	connIDStr := fiberCtx.Params("connectionId")

	connID, err := uuid.Parse(connIDStr)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "invalid connection id", err)

		return respondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", "invalid connection ID")
	}

	conn, err := handler.query.GetConnection(ctx, connID)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "get connection", err)

		// Check if it's actually a not-found error
		if errors.Is(err, discoveryQuery.ErrConnectionNotFound) {
			return respondError(fiberCtx, fiber.StatusNotFound, "discovery_connection_not_found", "connection not found")
		}

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "failed to get connection")
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ConnectionFromEntity(conn)); err != nil {
		return fmt.Errorf("respond get connection: %w", err)
	}

	return nil
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
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid connection ID"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Connection not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/discovery/connections/{connectionId}/schema [get]
func (handler *Handler) GetConnectionSchema(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "discovery.http.get_connection_schema")
	defer span.End()

	connIDStr := fiberCtx.Params("connectionId")

	connID, err := uuid.Parse(connIDStr)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "invalid connection id", err)

		return respondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", "invalid connection ID")
	}

	_, err = handler.query.GetConnection(ctx, connID)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "get connection for schema", err)

		if errors.Is(err, discoveryQuery.ErrConnectionNotFound) {
			return respondError(fiberCtx, fiber.StatusNotFound, "discovery_connection_not_found", "connection not found")
		}

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "failed to get connection")
	}

	schemas, err := handler.query.GetConnectionSchema(ctx, connID)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "get schema", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "failed to get schema")
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

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ConnectionSchemaResponse{
		ConnectionID: connID,
		Tables:       tables,
	}); err != nil {
		return fmt.Errorf("respond get connection schema: %w", err)
	}

	return nil
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
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid connection ID"
// @Success 200 {object} dto.TestConnectionResponse "Test result"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Connection not found"
// @Failure 503 {object} sharedhttp.ErrorResponse "Fetcher service unavailable"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/discovery/connections/{connectionId}/test [post]
func (handler *Handler) TestConnection(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "discovery.http.test_connection")
	defer span.End()

	connIDStr := fiberCtx.Params("connectionId")

	connID, err := uuid.Parse(connIDStr)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "invalid connection id", err)

		return respondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", "invalid connection ID")
	}

	result, err := handler.command.TestConnection(ctx, connID)
	if err != nil {
		if errors.Is(err, discoveryCommand.ErrConnectionNotFound) {
			handler.logSpanError(ctx, span, logger, "connection not found", err)

			return respondError(fiberCtx, fiber.StatusNotFound, "discovery_connection_not_found", "connection not found")
		}

		if errors.Is(err, discoveryCommand.ErrFetcherUnavailable) {
			handler.logSpanError(ctx, span, logger, "fetcher unavailable", err)

			return respondError(fiberCtx, fiber.StatusServiceUnavailable, "discovery_fetcher_unavailable", "fetcher service unavailable")
		}

		handler.logSpanError(ctx, span, logger, "test connection", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "failed to test connection")
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.TestConnectionResponse{
		ConnectionID: result.ConnectionID,
		Healthy:      result.Healthy,
		LatencyMs:    result.LatencyMs,
		ErrorMessage: sanitizedConnectionTestError(result),
	}); err != nil {
		return fmt.Errorf("respond test connection: %w", err)
	}

	return nil
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
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 409 {object} sharedhttp.ErrorResponse "Refresh already in progress"
// @Failure 503 {object} sharedhttp.ErrorResponse "Fetcher service unavailable"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/discovery/refresh [post]
func (handler *Handler) RefreshDiscovery(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "discovery.http.refresh_discovery")
	defer span.End()

	synced, err := handler.command.RefreshDiscovery(ctx)
	if err != nil {
		if errors.Is(err, discoveryCommand.ErrDiscoveryRefreshInProgress) {
			handler.logSpanError(ctx, span, logger, "refresh discovery in progress", err)

			return respondError(fiberCtx, fiber.StatusConflict, "refresh_in_progress", "discovery refresh already in progress")
		}

		if errors.Is(err, discoveryCommand.ErrFetcherUnavailable) {
			handler.logSpanError(ctx, span, logger, "fetcher unavailable", err)

			return respondError(fiberCtx, fiber.StatusServiceUnavailable, "discovery_fetcher_unavailable", "fetcher service unavailable")
		}

		handler.logSpanError(ctx, span, logger, "refresh discovery", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "failed to refresh discovery")
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.RefreshDiscoveryResponse{ConnectionsSynced: synced}); err != nil {
		return fmt.Errorf("respond refresh discovery: %w", err)
	}

	return nil
}
