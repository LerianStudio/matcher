// Package repositories provides governance persistence contracts.
package repositories

//go:generate mockgen -source=archive_metadata_repository.go -destination=mocks/archive_metadata_repository_mock.go -package=mocks

import (
	"context"
	"database/sql"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
)

// ArchiveMetadataRepository defines persistence operations for archive metadata.
type ArchiveMetadataRepository interface {
	// Create inserts a new archive metadata record.
	Create(ctx context.Context, metadata *entities.ArchiveMetadata) error

	// CreateWithTx inserts a new archive metadata record using the provided transaction.
	CreateWithTx(ctx context.Context, tx *sql.Tx, metadata *entities.ArchiveMetadata) error

	// Update persists changes to an existing archive metadata record.
	Update(ctx context.Context, metadata *entities.ArchiveMetadata) error

	// UpdateWithTx persists changes to an existing archive metadata record using the provided transaction.
	UpdateWithTx(ctx context.Context, tx *sql.Tx, metadata *entities.ArchiveMetadata) error

	// GetByID retrieves a single archive metadata by its ID.
	GetByID(ctx context.Context, id uuid.UUID) (*entities.ArchiveMetadata, error)

	// GetByPartition retrieves archive metadata for a specific partition within a tenant.
	GetByPartition(ctx context.Context, tenantID uuid.UUID, partitionName string) (*entities.ArchiveMetadata, error)

	// ListByTenant retrieves archive metadata for a tenant, optionally filtered by status and date bounds.
	// Returns the matching metadata records and any error.
	ListByTenant(ctx context.Context, tenantID uuid.UUID, status entities.ArchiveStatus, from, to *time.Time, limit, offset int) ([]*entities.ArchiveMetadata, error)

	// ListPending retrieves all archive metadata records with PENDING status.
	ListPending(ctx context.Context) ([]*entities.ArchiveMetadata, error)

	// ListIncomplete retrieves all archive metadata records that are not yet COMPLETE.
	// Used for crash recovery to resume interrupted archival processes.
	ListIncomplete(ctx context.Context) ([]*entities.ArchiveMetadata, error)
}
