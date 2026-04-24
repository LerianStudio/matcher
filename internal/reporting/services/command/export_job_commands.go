// Package command provides write operations for reporting.
package command

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories"
)

const exportJobResourcePathFmt = "/v1/export-jobs/%s"

// ErrNilExportJobRepository is returned when a nil repository is provided.
var ErrNilExportJobRepository = errors.New("export job repository is required")

// ErrExportJobNotFound is returned when an export job is not found.
var ErrExportJobNotFound = repositories.ErrExportJobNotFound

// ErrJobInTerminalState is returned when trying to cancel a job that's already in a terminal state.
var ErrJobInTerminalState = errors.New("cannot cancel job in terminal state")

// ExportJobUseCase orchestrates export job commands.
type ExportJobUseCase struct {
	repo repositories.ExportJobRepository
}

// NewExportJobUseCase creates a new export job use case.
func NewExportJobUseCase(repo repositories.ExportJobRepository) (*ExportJobUseCase, error) {
	if repo == nil {
		return nil, ErrNilExportJobRepository
	}

	return &ExportJobUseCase{repo: repo}, nil
}

// CreateExportJobInput contains parameters for creating an export job.
type CreateExportJobInput struct {
	TenantID   uuid.UUID
	ContextID  uuid.UUID
	ReportType entities.ExportReportType
	Format     entities.ExportFormat
	Filter     entities.ExportJobFilter
}

// CreateExportJobOutput contains the result of creating an export job.
type CreateExportJobOutput struct {
	JobID     uuid.UUID
	Status    entities.ExportJobStatus
	StatusURL string
}

// CreateExportJob creates a new export job and queues it for processing.
func (uc *ExportJobUseCase) CreateExportJob(
	ctx context.Context,
	input CreateExportJobInput,
) (*CreateExportJobOutput, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "reporting.command.create_export_job")
	defer span.End()

	_ = libOpentelemetry.SetSpanAttributesFromValue(span, "export_job", struct {
		TenantID   string `json:"tenantId"`
		ContextID  string `json:"contextId"`
		ReportType string `json:"reportType"`
		Format     string `json:"format"`
	}{
		TenantID:   input.TenantID.String(),
		ContextID:  input.ContextID.String(),
		ReportType: string(input.ReportType),
		Format:     string(input.Format),
	}, nil)

	job, err := entities.NewExportJob(
		ctx,
		input.TenantID,
		input.ContextID,
		input.ReportType,
		input.Format,
		input.Filter,
	)
	if err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "failed to create export job entity", err)

		libLog.SafeError(logger, ctx, "failed to create export job entity", err, runtime.IsProductionMode())

		return nil, fmt.Errorf("creating export job entity: %w", err)
	}

	if err := uc.repo.Create(ctx, job); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to persist export job", err)

		libLog.SafeError(logger, ctx, "failed to persist export job", err, runtime.IsProductionMode())

		return nil, fmt.Errorf("persisting export job: %w", err)
	}

	logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("created export job %s for context %s", job.ID, job.ContextID))

	return &CreateExportJobOutput{
		JobID:     job.ID,
		Status:    job.Status,
		StatusURL: fmt.Sprintf(exportJobResourcePathFmt, job.ID),
	}, nil
}

// CancelExportJob cancels a queued or running export job.
func (uc *ExportJobUseCase) CancelExportJob(ctx context.Context, id uuid.UUID) error {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "reporting.command.cancel_export_job")
	defer span.End()

	_ = libOpentelemetry.SetSpanAttributesFromValue(span, "export_job", struct {
		ID string `json:"id"`
	}{
		ID: id.String(),
	}, nil)

	job, err := uc.repo.GetByID(ctx, id)
	if err != nil {
		if errors.Is(err, repositories.ErrExportJobNotFound) {
			libOpentelemetry.HandleSpanBusinessErrorEvent(span, "export job not found for cancellation", err)

			logger.Log(ctx, libLog.LevelWarn, "export job not found for cancellation",
				libLog.String("exportJobId", id.String()))

			return ErrExportJobNotFound
		}

		libOpentelemetry.HandleSpanError(span, "failed to get export job for cancellation", err)

		libLog.SafeError(logger, ctx, "failed to get export job for cancellation", err, runtime.IsProductionMode())

		return fmt.Errorf("getting export job: %w", err)
	}

	if job.IsTerminal() {
		return fmt.Errorf("%w: %s", ErrJobInTerminalState, string(job.Status))
	}

	if err := job.MarkCanceled(); err != nil {
		return fmt.Errorf("mark export job canceled: %w", err)
	}

	if err := uc.repo.UpdateStatus(ctx, job); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to cancel export job", err)

		libLog.SafeError(logger, ctx, "failed to cancel export job", err, runtime.IsProductionMode())

		return fmt.Errorf("canceling export job: %w", err)
	}

	logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("canceled export job %s", job.ID))

	return nil
}
