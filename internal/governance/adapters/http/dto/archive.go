package dto

import (
	"time"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
)

// ArchiveMetadataResponse represents an archive metadata entry in API responses.
// @Description Completed audit log archive metadata
type ArchiveMetadataResponse struct {
	// Unique identifier for the archive
	ID string `json:"id"                   example:"550e8400-e29b-41d4-a716-446655440000"`
	// Name of the database partition that was archived
	PartitionName string `json:"partitionName"        example:"audit_logs_2024_q1"`
	// Start of the archived date range (RFC3339)
	DateRangeStart string `json:"dateRangeStart"       example:"2024-01-01T00:00:00Z"`
	// End of the archived date range (RFC3339)
	DateRangeEnd string `json:"dateRangeEnd"         example:"2024-03-31T23:59:59Z"`
	// Number of rows archived
	RowCount int64 `json:"rowCount"             example:"150000"`
	// Compressed file size in bytes
	CompressedSizeBytes int64 `json:"compressedSizeBytes"  example:"10485760"`
	// Object storage class
	StorageClass string `json:"storageClass"         example:"GLACIER"  enums:"STANDARD,GLACIER,DEEP_ARCHIVE"`
	// Archive status
	Status string `json:"status"               example:"COMPLETE" enums:"COMPLETE"`
	// Timestamp when archival completed (RFC3339)
	ArchivedAt *string `json:"archivedAt,omitempty" example:"2024-04-01T02:30:00Z"`
}

// ArchiveDownloadResponse provides a time-limited download URL for an archive.
// @Description Presigned download URL for an archived audit log
type ArchiveDownloadResponse struct {
	// Presigned URL for downloading the archive
	DownloadURL string `json:"downloadUrl" example:"https://s3.amazonaws.com/bucket/archive.gz?X-Amz-Signature=..."`
	// Expiration time of the download URL (RFC3339)
	ExpiresAt string `json:"expiresAt"   example:"2026-02-05T13:00:00Z"`
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

// ArchiveMetadataToResponse converts archive metadata to an API response DTO.
func ArchiveMetadataToResponse(am *entities.ArchiveMetadata) ArchiveMetadataResponse {
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
		Status:              string(am.Status),
	}

	if am.ArchivedAt != nil {
		formatted := am.ArchivedAt.Format(time.RFC3339)
		resp.ArchivedAt = &formatted
	}

	return resp
}

// ArchiveMetadataToResponses converts archive metadata entities to response DTOs.
func ArchiveMetadataToResponses(archives []*entities.ArchiveMetadata) []ArchiveMetadataResponse {
	result := make([]ArchiveMetadataResponse, 0, len(archives))

	for _, am := range archives {
		if am != nil {
			result = append(result, ArchiveMetadataToResponse(am))
		}
	}

	return result
}
