// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package http

import (
	"fmt"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/ingestion/adapters/http/dto"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
)

var _ = sharedhttp.ErrorResponse{}

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
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Source not found"
// @Failure 409 {object} sharedhttp.ErrorResponse "Conflict: duplicate resource or idempotency key in progress"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
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
		return handler.handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	sourceID, err := uuid.Parse(fiberCtx.Params("sourceId"))
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid source_id", err)
	}

	file, err := fiberCtx.FormFile("file")
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "file is required", err)
	}

	if file.Size == 0 {
		handler.logSpanError(ctx, span, logger, "file is empty", ErrEmptyFile)

		return respondError(fiberCtx, fiber.StatusBadRequest, "ingestion_empty_file", "file is empty")
	}

	if file.Size > maxUploadSize {
		return respondError(
			fiberCtx,
			fiber.StatusRequestEntityTooLarge,
			"request_entity_too_large",
			"file exceeds 100MB limit",
		)
	}

	format := strings.TrimSpace(fiberCtx.FormValue("format"))

	if format == "" {
		handler.logSpanError(ctx, span, logger, "format is required", ErrFormatRequired)

		return respondError(fiberCtx, fiber.StatusBadRequest, "ingestion_format_required", "format is required")
	}

	format = strings.ToLower(format)
	if format != "csv" && format != "json" && format != "xml" {
		return handler.badRequest(
			ctx,
			fiberCtx,
			span,
			logger,
			"invalid format: must be one of csv, json, xml",
			ErrInvalidFormat,
		)
	}

	if !validateFileContentType(file.Header.Get("Content-Type"), format) {
		return handler.badRequest(ctx, fiberCtx, span, logger, "file content type does not match declared format", ErrInvalidContentType)
	}

	fileReader, err := file.Open()
	if err != nil {
		handler.logSpanError(ctx, span, logger, "failed to open file", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
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
		return handler.handleIngestionError(ctx, fiberCtx, span, logger, err)
	}

	if writeErr := libHTTP.Respond(fiberCtx, fiber.StatusAccepted, dto.JobToResponse(job)); writeErr != nil {
		return fmt.Errorf("write accepted response: %w", writeErr)
	}

	return nil
}
