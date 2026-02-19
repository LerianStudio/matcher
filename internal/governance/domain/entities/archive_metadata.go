// Package entities defines governance domain types and validation logic.
package entities

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-uncommons/v2/uncommons/assert"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// Archive status constants representing the lifecycle of a partition archival.
const (
	StatusPending   = "PENDING"
	StatusExporting = "EXPORTING"
	StatusExported  = "EXPORTED"
	StatusUploading = "UPLOADING"
	StatusUploaded  = "UPLOADED"
	StatusVerifying = "VERIFYING"
	StatusVerified  = "VERIFIED"
	StatusDetaching = "DETACHING"
	StatusComplete  = "COMPLETE"
)

// Sentinel errors for archive metadata validation.
var (
	ErrInvalidStateTransition    = errors.New("invalid state transition")
	ErrArchiveTenantIDRequired   = errors.New("tenant id is required")
	ErrPartitionNameRequired     = errors.New("partition name is required")
	ErrDateRangeStartRequired    = errors.New("date range start is required")
	ErrDateRangeEndRequired      = errors.New("date range end is required")
	ErrDateRangeEndBeforeStart   = errors.New("date range end must be after start")
	ErrRowCountMustBeNonNegative = errors.New("row count must be non-negative")
	ErrArchiveKeyRequired        = errors.New("archive key is required")
	ErrChecksumRequired          = errors.New("checksum is required")
	ErrCompressedSizeNonNegative = errors.New("compressed size must be non-negative")
	ErrStorageClassRequired      = errors.New("storage class is required")
)

// ArchiveMetadata tracks the lifecycle state of a partitioned audit log archival.
type ArchiveMetadata struct {
	ID                  uuid.UUID
	TenantID            uuid.UUID
	PartitionName       string
	DateRangeStart      time.Time
	DateRangeEnd        time.Time
	RowCount            int64
	ArchiveKey          string
	Checksum            string
	CompressedSizeBytes int64
	StorageClass        string
	Status              string
	ErrorMessage        string
	ArchivedAt          *time.Time
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// NewArchiveMetadata validates inputs and returns a new archive metadata in PENDING status.
func NewArchiveMetadata(
	ctx context.Context,
	tenantID uuid.UUID,
	partitionName string,
	rangeStart, rangeEnd time.Time,
) (*ArchiveMetadata, error) {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "governance.archive_metadata.new")

	if err := asserter.That(ctx, tenantID != uuid.Nil, "tenant id is required"); err != nil {
		return nil, ErrArchiveTenantIDRequired
	}

	if err := asserter.NotEmpty(ctx, partitionName, "partition name is required"); err != nil {
		return nil, ErrPartitionNameRequired
	}

	if err := asserter.That(ctx, !rangeStart.IsZero(), "date range start is required"); err != nil {
		return nil, ErrDateRangeStartRequired
	}

	if err := asserter.That(ctx, !rangeEnd.IsZero(), "date range end is required"); err != nil {
		return nil, ErrDateRangeEndRequired
	}

	if err := asserter.That(ctx, rangeEnd.After(rangeStart), "date range end must be after start"); err != nil {
		return nil, ErrDateRangeEndBeforeStart
	}

	now := time.Now().UTC()

	return &ArchiveMetadata{
		ID:             uuid.New(),
		TenantID:       tenantID,
		PartitionName:  partitionName,
		DateRangeStart: rangeStart,
		DateRangeEnd:   rangeEnd,
		Status:         StatusPending,
		CreatedAt:      now,
		UpdatedAt:      now,
	}, nil
}

// MarkExporting transitions from PENDING to EXPORTING.
func (am *ArchiveMetadata) MarkExporting() error {
	if am.Status != StatusPending {
		return fmt.Errorf("%w: cannot transition from %s to %s", ErrInvalidStateTransition, am.Status, StatusExporting)
	}

	am.Status = StatusExporting
	am.UpdatedAt = time.Now().UTC()

	return nil
}

// MarkExported transitions from EXPORTING to EXPORTED with the exported row count.
func (am *ArchiveMetadata) MarkExported(rowCount int64) error {
	if am.Status != StatusExporting {
		return fmt.Errorf("%w: cannot transition from %s to %s", ErrInvalidStateTransition, am.Status, StatusExported)
	}

	if rowCount < 0 {
		return ErrRowCountMustBeNonNegative
	}

	am.Status = StatusExported
	am.RowCount = rowCount
	am.UpdatedAt = time.Now().UTC()

	return nil
}

// MarkUploading transitions from EXPORTED to UPLOADING.
func (am *ArchiveMetadata) MarkUploading() error {
	if am.Status != StatusExported {
		return fmt.Errorf("%w: cannot transition from %s to %s", ErrInvalidStateTransition, am.Status, StatusUploading)
	}

	am.Status = StatusUploading
	am.UpdatedAt = time.Now().UTC()

	return nil
}

// MarkUploaded transitions from UPLOADING to UPLOADED with archive details.
func (am *ArchiveMetadata) MarkUploaded(archiveKey, checksum string, compressedSize int64, storageClass string) error {
	if am.Status != StatusUploading {
		return fmt.Errorf("%w: cannot transition from %s to %s", ErrInvalidStateTransition, am.Status, StatusUploaded)
	}

	if archiveKey == "" {
		return ErrArchiveKeyRequired
	}

	if checksum == "" {
		return ErrChecksumRequired
	}

	if compressedSize < 0 {
		return ErrCompressedSizeNonNegative
	}

	if storageClass == "" {
		return ErrStorageClassRequired
	}

	am.Status = StatusUploaded
	am.ArchiveKey = archiveKey
	am.Checksum = checksum
	am.CompressedSizeBytes = compressedSize
	am.StorageClass = storageClass
	am.UpdatedAt = time.Now().UTC()

	return nil
}

// MarkVerifying transitions from UPLOADED to VERIFYING.
func (am *ArchiveMetadata) MarkVerifying() error {
	if am.Status != StatusUploaded {
		return fmt.Errorf("%w: cannot transition from %s to %s", ErrInvalidStateTransition, am.Status, StatusVerifying)
	}

	am.Status = StatusVerifying
	am.UpdatedAt = time.Now().UTC()

	return nil
}

// MarkVerified transitions from VERIFYING to VERIFIED.
func (am *ArchiveMetadata) MarkVerified() error {
	if am.Status != StatusVerifying {
		return fmt.Errorf("%w: cannot transition from %s to %s", ErrInvalidStateTransition, am.Status, StatusVerified)
	}

	am.Status = StatusVerified
	am.UpdatedAt = time.Now().UTC()

	return nil
}

// MarkDetaching transitions from VERIFIED to DETACHING.
func (am *ArchiveMetadata) MarkDetaching() error {
	if am.Status != StatusVerified {
		return fmt.Errorf("%w: cannot transition from %s to %s", ErrInvalidStateTransition, am.Status, StatusDetaching)
	}

	am.Status = StatusDetaching
	am.UpdatedAt = time.Now().UTC()

	return nil
}

// MarkComplete transitions from DETACHING to COMPLETE.
func (am *ArchiveMetadata) MarkComplete() error {
	if am.Status != StatusDetaching {
		return fmt.Errorf("%w: cannot transition from %s to %s", ErrInvalidStateTransition, am.Status, StatusComplete)
	}

	now := time.Now().UTC()
	am.Status = StatusComplete
	am.ArchivedAt = &now
	am.UpdatedAt = now

	return nil
}

// MarkError records an error message while preserving the current status for retry.
func (am *ArchiveMetadata) MarkError(msg string) {
	am.ErrorMessage = msg
	am.UpdatedAt = time.Now().UTC()
}
