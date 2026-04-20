// Package query provides read operations for reporting.
package query

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories"
)

// ListExportJobsInput contains parameters for listing export jobs.
type ListExportJobsInput struct {
	Status *string
	Cursor *libHTTP.TimestampCursor
	Limit  int
}

// ErrNilExportJobRepository is returned when a nil repository is provided.
var ErrNilExportJobRepository = errors.New("export job repository is required")

// ErrExportJobNotFound is returned when an export job is not found.
var ErrExportJobNotFound = repositories.ErrExportJobNotFound

// ExportJobQueryService provides read operations for export jobs.
type ExportJobQueryService struct {
	repo repositories.ExportJobRepository
}

// NewExportJobQueryService creates a new query service for export jobs.
func NewExportJobQueryService(
	repo repositories.ExportJobRepository,
) (*ExportJobQueryService, error) {
	if repo == nil {
		return nil, ErrNilExportJobRepository
	}

	return &ExportJobQueryService{repo: repo}, nil
}

// GetByID retrieves an export job by its ID.
func (svc *ExportJobQueryService) GetByID(
	ctx context.Context,
	id uuid.UUID,
) (*entities.ExportJob, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "reporting.query.export_job.get_by_id")
	defer span.End()

	job, err := svc.repo.GetByID(ctx, id)
	if err != nil {
		if errors.Is(err, repositories.ErrExportJobNotFound) {
			libOpentelemetry.HandleSpanBusinessErrorEvent(span, "export job not found", err)
		} else {
			libOpentelemetry.HandleSpanError(span, "failed to get export job", err)
		}

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to get export job by ID %s: %v", id.String(), err))

		return nil, fmt.Errorf("getting export job: %w", err)
	}

	return job, nil
}

// List retrieves export jobs for the tenant with optional status filter and cursor-based pagination.
// cursor is the ID of the last item from the previous page (nil for first page).
func (svc *ExportJobQueryService) List(
	ctx context.Context,
	input ListExportJobsInput,
) ([]*entities.ExportJob, libHTTP.CursorPagination, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "reporting.query.export_job.list")
	defer span.End()

	jobs, pagination, err := svc.repo.List(ctx, input.Status, input.Cursor, input.Limit)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list export jobs", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to list export jobs (status=%v, cursor=%v, limit=%d): %v",
			input.Status, input.Cursor, input.Limit, err))

		return nil, libHTTP.CursorPagination{}, fmt.Errorf("listing export jobs: %w", err)
	}

	return jobs, pagination, nil
}

// ListByContextInput contains parameters for listing export jobs scoped to a context.
type ListByContextInput struct {
	ContextID uuid.UUID
	Cursor    *libHTTP.TimestampCursor
	Limit     int
}

// ListByContext retrieves export jobs for a specific reconciliation context with
// forward-only cursor-based pagination. The returned CursorPagination.Next is
// empty when there are no more pages.
func (svc *ExportJobQueryService) ListByContext(
	ctx context.Context,
	input ListByContextInput,
) ([]*entities.ExportJob, libHTTP.CursorPagination, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "reporting.query.export_job.list_by_context")
	defer span.End()

	jobs, pagination, err := svc.repo.ListByContext(ctx, input.ContextID, input.Cursor, input.Limit)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list export jobs by context", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf(
			"failed to list export jobs by context %s (limit=%d): %v",
			input.ContextID.String(),
			input.Limit,
			err,
		))

		return nil, libHTTP.CursorPagination{}, fmt.Errorf("listing export jobs by context: %w", err)
	}

	return jobs, pagination, nil
}
