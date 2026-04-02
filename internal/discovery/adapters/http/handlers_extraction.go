package http

import (
	"errors"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	"github.com/LerianStudio/matcher/internal/discovery/adapters/http/dto"
	discoveryCommand "github.com/LerianStudio/matcher/internal/discovery/services/command"
	discoveryQuery "github.com/LerianStudio/matcher/internal/discovery/services/query"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

var _ = sharedhttp.ErrorResponse{}

// StartExtraction handles POST /v1/discovery/connections/:connectionId/extractions.
//
// @ID startDiscoveryExtraction
// @Summary Start a connection extraction
// @Description Creates a tenant-scoped extraction request for a discovered connection and submits it to Fetcher.
// @Tags Discovery
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param connectionId path string true "Connection ID (UUID)"
// @Param request body dto.StartExtractionRequest true "Extraction request"
// @Success 201 {object} dto.ExtractionRequestResponse "Extraction request"
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Connection not found"
// @Failure 503 {object} sharedhttp.ErrorResponse "Fetcher service unavailable"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/discovery/connections/{connectionId}/extractions [post]
func (handler *Handler) StartExtraction(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "discovery.http.start_extraction")
	defer span.End()

	connectionID, err := uuid.Parse(fiberCtx.Params("connectionId"))
	if err != nil {
		logSpanError(ctx, span, logger, "invalid connection id", err)

		return respondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", "invalid connection ID")
	}

	var request dto.StartExtractionRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &request); err != nil {
		logSpanError(ctx, span, logger, "invalid extraction payload", err)

		return respondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", "invalid extraction request body")
	}

	extraction, err := handler.command.StartExtraction(
		ctx,
		connectionID,
		rawExtractionTables(request.Tables),
		sharedPorts.ExtractionParams{
			StartDate: request.StartDate,
			EndDate:   request.EndDate,
			Filters:   request.Filters,
		},
	)
	if err != nil {
		switch {
		case errors.Is(err, discoveryCommand.ErrInvalidExtractionRequest):
			logSpanError(ctx, span, logger, "invalid extraction request", err)
			return respondError(fiberCtx, fiber.StatusBadRequest, "discovery_invalid_extraction", err.Error())
		case errors.Is(err, discoveryCommand.ErrConnectionNotFound):
			logSpanError(ctx, span, logger, "connection not found", err)
			return respondError(fiberCtx, fiber.StatusNotFound, "discovery_connection_not_found", "connection not found")
		case errors.Is(err, discoveryCommand.ErrFetcherUnavailable):
			logSpanError(ctx, span, logger, "fetcher unavailable", err)
			return respondError(fiberCtx, fiber.StatusServiceUnavailable, "discovery_fetcher_unavailable", "fetcher service unavailable")
		default:
			logSpanError(ctx, span, logger, "start extraction", err)
			return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "failed to start extraction")
		}
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusCreated, dto.ExtractionRequestFromEntity(extraction))
}

// GetExtraction handles GET /v1/discovery/extractions/:extractionId.
//
// @ID getDiscoveryExtraction
// @Summary Get an extraction request
// @Description Returns a single tenant-scoped extraction request by its internal ID.
// @Tags Discovery
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param extractionId path string true "Extraction ID (UUID)"
// @Success 200 {object} dto.ExtractionRequestResponse "Extraction request"
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid extraction ID"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Extraction not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/discovery/extractions/{extractionId} [get]
func (handler *Handler) GetExtraction(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "discovery.http.get_extraction")
	defer span.End()

	extractionID, err := uuid.Parse(fiberCtx.Params("extractionId"))
	if err != nil {
		logSpanError(ctx, span, logger, "invalid extraction id", err)

		return respondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", "invalid extraction ID")
	}

	extraction, err := handler.query.GetExtraction(ctx, extractionID)
	if err != nil {
		logSpanError(ctx, span, logger, "get extraction", err)

		if errors.Is(err, discoveryQuery.ErrExtractionNotFound) {
			return respondError(fiberCtx, fiber.StatusNotFound, "discovery_extraction_not_found", "extraction not found")
		}

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "failed to get extraction")
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ExtractionRequestFromEntity(extraction))
}

func rawExtractionTables(tables map[string]dto.ExtractionTableRequest) map[string]any {
	if len(tables) == 0 {
		return map[string]any{}
	}

	raw := make(map[string]any, len(tables))
	for tableName, cfg := range tables {
		tableCfg := make(map[string]any, 1)
		if len(cfg.Columns) > 0 {
			tableCfg["columns"] = cfg.Columns
		}

		raw[tableName] = tableCfg
	}

	return raw
}

// PollExtraction handles POST /v1/discovery/extractions/:extractionId/poll.
//
// @ID pollDiscoveryExtraction
// @Summary Poll an extraction request
// @Description Polls Fetcher for the latest extraction status and persists any lifecycle transition.
// @Tags Discovery
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param extractionId path string true "Extraction ID (UUID)"
// @Success 200 {object} dto.ExtractionRequestResponse "Updated extraction request"
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid extraction ID"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Extraction not found"
// @Failure 503 {object} sharedhttp.ErrorResponse "Fetcher service unavailable"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/discovery/extractions/{extractionId}/poll [post]
func (handler *Handler) PollExtraction(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "discovery.http.poll_extraction")
	defer span.End()

	extractionID, err := uuid.Parse(fiberCtx.Params("extractionId"))
	if err != nil {
		logSpanError(ctx, span, logger, "invalid extraction id", err)

		return respondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", "invalid extraction ID")
	}

	extraction, err := handler.command.PollExtractionStatus(ctx, extractionID)
	if err != nil {
		logSpanError(ctx, span, logger, "poll extraction", err)

		if errors.Is(err, discoveryCommand.ErrExtractionNotFound) {
			return respondError(fiberCtx, fiber.StatusNotFound, "discovery_extraction_not_found", "extraction not found")
		}

		if errors.Is(err, discoveryCommand.ErrFetcherUnavailable) {
			return respondError(fiberCtx, fiber.StatusServiceUnavailable, "discovery_fetcher_unavailable", "fetcher service unavailable")
		}

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "failed to poll extraction")
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ExtractionRequestFromEntity(extraction))
}
