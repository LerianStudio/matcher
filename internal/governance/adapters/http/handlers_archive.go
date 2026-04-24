// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package http provides HTTP handlers for governance operations.
package http

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/governance/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	governanceErrors "github.com/LerianStudio/matcher/internal/governance/domain/errors"
	"github.com/LerianStudio/matcher/internal/governance/domain/repositories"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
	"github.com/LerianStudio/matcher/internal/shared/objectstorage"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

var _ = sharedhttp.ErrorResponse{}

// Sentinel errors for archive handler validation.
var (
	ErrArchiveRepoRequired    = errors.New("archive metadata repository is required")
	ErrStorageClientRequired  = errors.New("object storage client is required")
	ErrPresignExpiryRequired  = errors.New("presign expiry must be positive")
	ErrMissingArchiveID       = errors.New("archive id is required")
	ErrInvalidArchiveID       = errors.New("archive id must be a valid UUID")
	ErrArchiveDateFromInvalid = errors.New("from must be a valid date (YYYY-MM-DD or RFC3339)")
	ErrArchiveDateToInvalid   = errors.New("to must be a valid date (YYYY-MM-DD or RFC3339)")
)

// ArchiveHandler handles HTTP requests for archive retrieval.
//
// productionMode governs SafeError behavior (suppresses internal error
// details in client responses when true). Stored per-handler rather than
// on a package-level atomic.Bool to avoid cross-test coupling via shared
// global state.
type ArchiveHandler struct {
	archiveRepo           repositories.ArchiveMetadataRepository
	storage               objectstorage.Backend
	presignExpiry         time.Duration
	presignExpiryResolver func(context.Context) time.Duration
	productionMode        bool
}

type (
	// ArchiveMetadataResponse represents archive metadata in API responses.
	ArchiveMetadataResponse = dto.ArchiveMetadataResponse
	// ArchiveDownloadResponse contains a presigned URL for archive downloads.
	ArchiveDownloadResponse = dto.ArchiveDownloadResponse
	// ListArchivesResponse contains paginated archive metadata items.
	ListArchivesResponse = dto.ListArchivesResponse
)

// NewArchiveHandler creates a new archive HTTP handler.
func NewArchiveHandler(
	repo repositories.ArchiveMetadataRepository,
	storage objectstorage.Backend,
	presignExpiry time.Duration,
	production bool,
) (*ArchiveHandler, error) {
	if repo == nil {
		return nil, ErrArchiveRepoRequired
	}

	if storage == nil {
		return nil, ErrStorageClientRequired
	}

	if presignExpiry <= 0 {
		return nil, ErrPresignExpiryRequired
	}

	return &ArchiveHandler{
		archiveRepo:    repo,
		storage:        storage,
		presignExpiry:  presignExpiry,
		productionMode: production,
	}, nil
}

// SetRuntimePresignExpiryResolver injects a context-aware presign expiry source.
func (ah *ArchiveHandler) SetRuntimePresignExpiryResolver(resolver func(context.Context) time.Duration) {
	if ah == nil {
		return
	}

	ah.presignExpiryResolver = resolver
}

func (ah *ArchiveHandler) currentPresignExpiryForContext(ctx context.Context) time.Duration {
	if ah == nil {
		return 0
	}

	if ah.presignExpiryResolver != nil {
		if runtimeExpiry := ah.presignExpiryResolver(ctx); runtimeExpiry > 0 {
			return runtimeExpiry
		}
	}

	return ah.presignExpiry
}

// Response helpers — see note on *Handler methods in handlers.go for why
// these live on the receiver rather than in package-global state.

func (ah *ArchiveHandler) logSpanError(ctx context.Context, span trace.Span, logger libLog.Logger, message string, err error) {
	sharedhttp.LogSpanError(ctx, span, logger, ah.productionMode, message, err)
}

//nolint:wrapcheck // HTTP transport response is the terminal error boundary.
func (ah *ArchiveHandler) badRequest(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	message string,
	err error,
) error {
	return sharedhttp.BadRequest(ctx, fiberCtx, span, logger, ah.productionMode, message, err)
}

//nolint:wrapcheck // HTTP transport response is the terminal error boundary.
func (ah *ArchiveHandler) writeServiceError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	message string,
	err error,
) error {
	return sharedhttp.InternalError(ctx, fiberCtx, span, logger, ah.productionMode, message, err)
}

func (ah *ArchiveHandler) writeNotFound(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	slug string,
	message string,
	err error,
) error {
	sharedhttp.LogSpanError(ctx, span, logger, ah.productionMode, message, err)

	return respondError(fiberCtx, fiber.StatusNotFound, slug, message)
}

// ListArchives retrieves completed audit log archives for the tenant.
// @Summary List audit log archives
// @Description Returns a paginated list of completed audit log archives for the tenant. Only archives with status COMPLETE are returned.
// @Description This endpoint uses offset-based pagination (limit/offset) rather than cursor-based pagination.
// @Description Use optional date filters (YYYY-MM-DD or RFC3339 format) to narrow results by archive date range.
// @ID listArchives
// @Tags Governance
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param from query string false "Filter archives from this date (YYYY-MM-DD or RFC3339 format, e.g. 2024-01-01 or 2024-01-01T00:00:00Z)"
// @Param to query string false "Filter archives until this date (YYYY-MM-DD or RFC3339 format, e.g. 2024-12-31 or 2024-12-31T23:59:59Z)"
// @Param limit query int false "Maximum number of records to return" default(20) minimum(1) maximum(200)
// @Param offset query int false "Number of records to skip" default(0) minimum(0)
// @Success 200 {object} ListArchivesResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/governance/archives [get]
func (ah *ArchiveHandler) ListArchives(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.governance.list_archives")
	defer span.End()

	tenantIDStr := auth.GetTenantID(ctx)

	tenantID, err := uuid.Parse(tenantIDStr)
	if err != nil {
		return ah.badRequest(ctx, fiberCtx, span, logger, "invalid tenant id", err)
	}

	limit, offset, err := libHTTP.ParsePagination(fiberCtx)
	if err != nil {
		return ah.badRequest(ctx, fiberCtx, span, logger, "invalid pagination parameters", err)
	}

	var from, to *time.Time

	if fromStr := fiberCtx.Query("from"); fromStr != "" {
		parsed, parseErr := parseDate(fromStr)
		if parseErr != nil {
			return ah.badRequest(ctx, fiberCtx, span, logger, ErrArchiveDateFromInvalid.Error(), ErrArchiveDateFromInvalid)
		}

		from = &parsed
	}

	if toStr := fiberCtx.Query("to"); toStr != "" {
		parsed, parseErr := parseDateTo(toStr)
		if parseErr != nil {
			return ah.badRequest(ctx, fiberCtx, span, logger, ErrArchiveDateToInvalid.Error(), ErrArchiveDateToInvalid)
		}

		to = &parsed
	}

	// Fetch limit+1 to determine if more pages exist without an extra COUNT query.
	archives, err := ah.archiveRepo.ListByTenant(ctx, tenantID, entities.StatusComplete, from, to, limit+1, offset)
	if err != nil {
		return ah.writeServiceError(ctx, fiberCtx, span, logger, "failed to list archives", err)
	}

	hasMore := len(archives) > limit
	if hasMore {
		archives = archives[:limit]
	}

	items := dto.ArchiveMetadataToResponses(archives)

	response := dto.ListArchivesResponse{
		Items:   items,
		Limit:   limit,
		HasMore: hasMore,
	}

	if writeErr := libHTTP.Respond(fiberCtx, fiber.StatusOK, response); writeErr != nil {
		return fmt.Errorf("write ok response: %w", writeErr)
	}

	return nil
}

// DownloadArchive generates a presigned download URL for a specific archive.
// @Summary Download audit log archive
// @Description Generates a time-limited presigned URL for downloading a specific completed audit log archive. The URL expires after the configured presign duration.
// @ID downloadArchive
// @Tags Governance
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param id path string true "Archive ID" format(uuid)
// @Success 200 {object} ArchiveDownloadResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Archive not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/governance/archives/{id}/download [get]
func (ah *ArchiveHandler) DownloadArchive(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.governance.download_archive")
	defer span.End()

	idStr := fiberCtx.Params("id")
	if idStr == "" {
		return ah.badRequest(ctx, fiberCtx, span, logger, "archive id is required", ErrMissingArchiveID)
	}

	archiveID, err := uuid.Parse(idStr)
	if err != nil {
		return ah.badRequest(
			ctx,
			fiberCtx,
			span,
			logger,
			"invalid archive id",
			fmt.Errorf("%w: %s", ErrInvalidArchiveID, idStr),
		)
	}

	archive, err := ah.archiveRepo.GetByID(ctx, archiveID)
	if err != nil {
		if errors.Is(err, governanceErrors.ErrMetadataNotFound) {
			return ah.writeNotFound(ctx, fiberCtx, span, logger, "governance_archive_not_found", "archive not found", err)
		}

		return ah.writeServiceError(ctx, fiberCtx, span, logger, "failed to get archive", err)
	}

	if archive == nil {
		return ah.writeNotFound(ctx, fiberCtx, span, logger, "governance_archive_not_found", "archive not found", governanceErrors.ErrMetadataNotFound)
	}

	// Verify tenant ownership
	tenantIDStr := auth.GetTenantID(ctx)

	tenantID, err := uuid.Parse(tenantIDStr)
	if err != nil {
		return ah.badRequest(ctx, fiberCtx, span, logger, "invalid tenant id", err)
	}

	if archive.TenantID != tenantID {
		return ah.writeNotFound(ctx, fiberCtx, span, logger, "governance_archive_not_found", "archive not found", governanceErrors.ErrMetadataNotFound)
	}

	presignExpiry := ah.currentPresignExpiryForContext(ctx)

	downloadURL, err := ah.storage.GeneratePresignedURL(ctx, archive.ArchiveKey, presignExpiry)
	if err != nil {
		if errors.Is(err, sharedPorts.ErrObjectStorageUnavailable) {
			ah.logSpanError(ctx, span, logger, "archive storage unavailable", err)

			return respondError(fiberCtx, fiber.StatusServiceUnavailable, "object_storage_unavailable", "archive storage is unavailable")
		}

		return ah.writeServiceError(ctx, fiberCtx, span, logger, "failed to generate download url", err)
	}

	expiresAt := time.Now().UTC().Add(presignExpiry)

	response := dto.ArchiveDownloadResponse{
		DownloadURL: downloadURL,
		ExpiresAt:   expiresAt.Format(time.RFC3339),
		Checksum:    archive.Checksum,
	}

	if writeErr := libHTTP.Respond(fiberCtx, fiber.StatusOK, response); writeErr != nil {
		return fmt.Errorf("write ok response: %w", writeErr)
	}

	return nil
}
