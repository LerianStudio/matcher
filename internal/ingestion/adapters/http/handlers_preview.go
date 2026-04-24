// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package http

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/ingestion/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/ingestion/services/query"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
)

var _ = sharedhttp.ErrorResponse{}

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
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Source not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
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
		return handler.handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	sourceID, err := uuid.Parse(fiberCtx.Params("sourceId"))
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid source_id", err)
	}

	span.SetAttributes(attribute.String("source_id", sourceID.String()))

	file, err := fiberCtx.FormFile("file")
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "file is required", err)
	}

	if file.Size == 0 {
		return handler.badRequest(ctx, fiberCtx, span, logger, "file is empty", ErrEmptyFile)
	}

	format := strings.TrimSpace(fiberCtx.FormValue("format"))
	if format == "" {
		format = detectFormatFromFilename(file.Filename)
	}

	if format == "" {
		return handler.badRequest(ctx, fiberCtx, span, logger, "unsupported file format; allowed: csv, json, xml", ErrFormatRequired)
	}

	maxRows := parseMaxRows(fiberCtx, logger)

	fileReader, err := file.Open()
	if err != nil {
		handler.logSpanError(ctx, span, logger, "failed to open file", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	defer func() {
		if closeErr := fileReader.Close(); closeErr != nil {
			handler.logSpanError(ctx, span, logger, "failed to close preview file", closeErr)
		}
	}()

	preview, err := handler.queryUC.PreviewFile(ctx, fileReader, format, maxRows)
	if err != nil {
		return handler.handlePreviewError(ctx, fiberCtx, span, logger, err)
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.FilePreviewResponse{
		Columns:    preview.Columns,
		SampleRows: preview.SampleRows,
		RowCount:   preview.RowCount,
		Format:     preview.Format,
	}); err != nil {
		return fmt.Errorf("respond file preview: %w", err)
	}

	return nil
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

func (handler *Handlers) handlePreviewError(
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
		return handler.badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	handler.logSpanError(ctx, span, logger, "failed to preview file", err)

	return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
}
