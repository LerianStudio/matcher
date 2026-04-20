// Package repositories provides reporting persistence contracts.
package repositories

//go:generate mockgen -destination=mocks/export_job_repository_mock.go -package=mocks . ExportJobRepository

import (
	"context"

	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
)

// ExportJobRepository defines operations for export job persistence.
type ExportJobRepository interface {
	// Create persists a new export job.
	Create(ctx context.Context, job *entities.ExportJob) error

	// GetByID retrieves an export job by its ID.
	GetByID(ctx context.Context, id uuid.UUID) (*entities.ExportJob, error)

	// Update persists changes to an existing export job.
	Update(ctx context.Context, job *entities.ExportJob) error

	// UpdateStatus updates only the status and related fields of a job.
	UpdateStatus(ctx context.Context, job *entities.ExportJob) error

	// UpdateProgress updates the progress counters of a running job.
	UpdateProgress(ctx context.Context, id uuid.UUID, recordsWritten, bytesWritten int64) error

	// List retrieves export jobs for the tenant in context with optional status filter.
	// Tenant is extracted from context using auth.GetTenantID(ctx).
	// cursor is the timestamp+ID of the last item from the previous page (nil for first page).
	// Results are ordered by created_at DESC and cursor-based pagination uses (created_at, id) keyset.
	List(
		ctx context.Context,
		status *string,
		cursor *libHTTP.TimestampCursor,
		limit int,
	) ([]*entities.ExportJob, libHTTP.CursorPagination, error)

	// ListByContext retrieves export jobs for a specific context using forward-only
	// cursor-based pagination. cursor is the timestamp+ID of the last item from the
	// previous page (nil for first page). Results are ordered by created_at DESC and
	// use the same (created_at, id) keyset as List for consistent ordering semantics.
	ListByContext(
		ctx context.Context,
		contextID uuid.UUID,
		cursor *libHTTP.TimestampCursor,
		limit int,
	) ([]*entities.ExportJob, libHTTP.CursorPagination, error)

	// ListExpired retrieves jobs that have passed their expiration time.
	ListExpired(ctx context.Context, limit int) ([]*entities.ExportJob, error)

	// ClaimNextQueued atomically claims the next queued job for processing.
	// Only claims jobs where NextRetryAt is nil or in the past.
	// Returns nil if no jobs are available.
	ClaimNextQueued(ctx context.Context) (*entities.ExportJob, error)

	// Delete removes an export job by ID.
	Delete(ctx context.Context, id uuid.UUID) error

	// RequeueForRetry updates a job for retry with a scheduled next attempt time.
	RequeueForRetry(ctx context.Context, job *entities.ExportJob) error
}
