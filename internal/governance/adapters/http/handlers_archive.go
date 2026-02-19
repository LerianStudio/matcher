// Package http provides HTTP handlers for governance operations.
package http

import (
	"errors"
	"fmt"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"

	sharedhttp "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	archivePostgres "github.com/LerianStudio/matcher/internal/governance/adapters/postgres/archive_metadata"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/internal/governance/domain/repositories"
	reportingPorts "github.com/LerianStudio/matcher/internal/reporting/ports"
)

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
type ArchiveHandler struct {
	archiveRepo   repositories.ArchiveMetadataRepository
	storage       reportingPorts.ObjectStorageClient
	presignExpiry time.Duration
}

// NewArchiveHandler creates a new archive HTTP handler.
func NewArchiveHandler(
	repo repositories.ArchiveMetadataRepository,
	storage reportingPorts.ObjectStorageClient,
	presignExpiry time.Duration,
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
		archiveRepo:   repo,
		storage:       storage,
		presignExpiry: presignExpiry,
	}, nil
}

// ArchiveMetadataResponse represents an archive metadata entry in API responses.
// @Description Completed audit log archive metadata
type ArchiveMetadataResponse struct {
	// Unique identifier for the archive
	ID string `json:"id"                    example:"550e8400-e29b-41d4-a716-446655440000"`
	// Name of the database partition that was archived
	PartitionName string `json:"partition_name"         example:"audit_logs_2024_q1"`
	// Start of the archived date range (RFC3339)
	DateRangeStart string `json:"date_range_start"       example:"2024-01-01T00:00:00Z"`
	// End of the archived date range (RFC3339)
	DateRangeEnd string `json:"date_range_end"         example:"2024-03-31T23:59:59Z"`
	// Number of rows archived
	RowCount int64 `json:"row_count"              example:"150000"`
	// Compressed file size in bytes
	CompressedSizeBytes int64 `json:"compressed_size_bytes"  example:"10485760"`
	// Object storage class
	StorageClass string `json:"storage_class"          example:"GLACIER"  enums:"STANDARD,GLACIER,DEEP_ARCHIVE"`
	// Archive status
	Status string `json:"status"                 example:"COMPLETE" enums:"COMPLETE"`
	// Timestamp when archival completed (RFC3339)
	ArchivedAt *string `json:"archived_at,omitempty"   example:"2024-04-01T02:30:00Z"`
}

// ArchiveDownloadResponse provides a time-limited download URL for an archive.
// @Description Presigned download URL for an archived audit log
type ArchiveDownloadResponse struct {
	// Presigned URL for downloading the archive
	DownloadURL string `json:"download_url" example:"https://s3.amazonaws.com/bucket/archive.gz?X-Amz-Signature=..."`
	// Expiration time of the download URL (RFC3339)
	ExpiresAt string `json:"expires_at"   example:"2026-02-05T13:00:00Z"`
	// SHA-256 checksum of the archive file
	Checksum string `json:"checksum"     example:"sha256:abc123def456..."`
}

// ListArchivesResponse represents the paginated list of archives.
// @Description Paginated list of completed audit log archives
type ListArchivesResponse struct {
	// List of archive metadata entries
	Items []ArchiveMetadataResponse `json:"items" validate:"omitempty,max=200" maxItems:"200"`
	Limit int                       `json:"limit"   example:"20" minimum:"1" maximum:"200"`
	// Indicates whether more pages exist
	HasMore bool `json:"hasMore" example:"true"`
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
		return badRequest(ctx, fiberCtx, span, logger, "invalid tenant id", err)
	}

	limit, offset, err := sharedhttp.ParsePagination(fiberCtx)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid pagination parameters", err)
	}

	var from, to *time.Time

	if fromStr := fiberCtx.Query("from"); fromStr != "" {
		parsed, parseErr := parseDate(fromStr)
		if parseErr != nil {
			return badRequest(ctx, fiberCtx, span, logger, ErrArchiveDateFromInvalid.Error(), ErrArchiveDateFromInvalid)
		}

		from = &parsed
	}

	if toStr := fiberCtx.Query("to"); toStr != "" {
		parsed, parseErr := parseDateTo(toStr)
		if parseErr != nil {
			return badRequest(ctx, fiberCtx, span, logger, ErrArchiveDateToInvalid.Error(), ErrArchiveDateToInvalid)
		}

		to = &parsed
	}

	// Fetch limit+1 to determine if more pages exist without an extra COUNT query.
	archives, err := ah.archiveRepo.ListByTenant(ctx, tenantID, entities.StatusComplete, from, to, limit+1, offset)
	if err != nil {
		return writeServiceError(ctx, fiberCtx, span, logger, "failed to list archives", err)
	}

	hasMore := len(archives) > limit
	if hasMore {
		archives = archives[:limit]
	}

	items := archiveMetadataToResponses(archives)

	response := ListArchivesResponse{
		Items:   items,
		Limit:   limit,
		HasMore: hasMore,
	}

	if writeErr := sharedhttp.Respond(fiberCtx, fiber.StatusOK, response); writeErr != nil {
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
		return badRequest(ctx, fiberCtx, span, logger, "archive id is required", ErrMissingArchiveID)
	}

	archiveID, err := uuid.Parse(idStr)
	if err != nil {
		return badRequest(
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
		if errors.Is(err, archivePostgres.ErrMetadataNotFound) {
			return writeNotFound(ctx, fiberCtx, span, logger, "archive not found", err)
		}

		return writeServiceError(ctx, fiberCtx, span, logger, "failed to get archive", err)
	}

	if archive == nil {
		return writeNotFound(ctx, fiberCtx, span, logger, "archive not found", archivePostgres.ErrMetadataNotFound)
	}

	// Verify tenant ownership
	tenantIDStr := auth.GetTenantID(ctx)

	tenantID, err := uuid.Parse(tenantIDStr)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid tenant id", err)
	}

	if archive.TenantID != tenantID {
		return writeNotFound(ctx, fiberCtx, span, logger, "archive not found", archivePostgres.ErrMetadataNotFound)
	}

	downloadURL, err := ah.storage.GeneratePresignedURL(ctx, archive.ArchiveKey, ah.presignExpiry)
	if err != nil {
		return writeServiceError(ctx, fiberCtx, span, logger, "failed to generate download url", err)
	}

	expiresAt := time.Now().UTC().Add(ah.presignExpiry)

	response := ArchiveDownloadResponse{
		DownloadURL: downloadURL,
		ExpiresAt:   expiresAt.Format(time.RFC3339),
		Checksum:    archive.Checksum,
	}

	if writeErr := sharedhttp.Respond(fiberCtx, fiber.StatusOK, response); writeErr != nil {
		return fmt.Errorf("write ok response: %w", writeErr)
	}

	return nil
}

func archiveMetadataToResponse(am *entities.ArchiveMetadata) ArchiveMetadataResponse {
	if am == nil {
		return ArchiveMetadataResponse{}
	}

	resp := ArchiveMetadataResponse{
		ID:                  am.ID.String(),
		PartitionName:       am.PartitionName,
		DateRangeStart:      am.DateRangeStart.Format(time.RFC3339),
		DateRangeEnd:        am.DateRangeEnd.Format(time.RFC3339),
		RowCount:            am.RowCount,
		CompressedSizeBytes: am.CompressedSizeBytes,
		StorageClass:        am.StorageClass,
		Status:              am.Status,
	}

	if am.ArchivedAt != nil {
		formatted := am.ArchivedAt.Format(time.RFC3339)
		resp.ArchivedAt = &formatted
	}

	return resp
}

func archiveMetadataToResponses(archives []*entities.ArchiveMetadata) []ArchiveMetadataResponse {
	result := make([]ArchiveMetadataResponse, 0, len(archives))

	for _, am := range archives {
		if am != nil {
			result = append(result, archiveMetadataToResponse(am))
		}
	}

	return result
}
