// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package command provides write operations for reporting.
package command

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"
	streaming "github.com/LerianStudio/lib-streaming/v2"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories"
	reportingMetrics "github.com/LerianStudio/matcher/internal/reporting/services/metrics"
	reportingStreamingPayload "github.com/LerianStudio/matcher/internal/reporting/services/streamingpayload"
	sharedObservability "github.com/LerianStudio/matcher/internal/shared/observability"
	"github.com/LerianStudio/matcher/internal/streaming/emission"
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
	repo          repositories.ExportJobRepository
	streamEmitter streaming.Emitter
}

// NewExportJobUseCase creates a new export job use case.
func NewExportJobUseCase(repo repositories.ExportJobRepository, options ...ExportJobUseCaseOption) (*ExportJobUseCase, error) {
	if repo == nil {
		return nil, ErrNilExportJobRepository
	}

	uc := &ExportJobUseCase{repo: repo}

	for _, option := range options {
		if option != nil {
			option(uc)
		}
	}

	return uc, nil
}

// Functional options for streaming.Emitter injection follow the convention:
// - Bare WithStreamingEmitter when this package owns one emitter consumer
// - With<ReceiverName>StreamingEmitter when multiple consumers coexist in the same package.

// ExportJobUseCaseOption configures optional export job command dependencies.
type ExportJobUseCaseOption func(*ExportJobUseCase)

// WithStreamingEmitter sets the emitter used for export job streaming events.
// Use emission.IsNilEmitter() to defend against typed-nil interface values
// (e.g., a (*MockEmitter)(nil) hiding behind a streaming.Emitter interface).
func WithStreamingEmitter(emitter streaming.Emitter) ExportJobUseCaseOption {
	return func(uc *ExportJobUseCase) {
		if !emission.IsNilEmitter(emitter) {
			uc.streamEmitter = emitter
		}
	}
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
	}, sharedObservability.NewMatcherRedactor())

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

	reportingMetrics.RecordExportJobTransition(
		ctx,
		string(job.Format),
		string(entities.ExportJobStatusQueued),
	)
	uc.emitExportJobEvent(ctx, span, "export_job.created", job)

	return &CreateExportJobOutput{
		JobID:     job.ID,
		Status:    job.Status,
		StatusURL: fmt.Sprintf(exportJobResourcePathFmt, job.ID),
	}, nil
}

func (uc *ExportJobUseCase) emitExportJobEvent(ctx context.Context, span trace.Span, definitionKey string, job *entities.ExportJob) {
	if job == nil {
		return
	}

	if err := emission.Emit(ctx, uc.streamEmitter, definitionKey, job.ID.String(), exportJobPayload(definitionKey, job)); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to emit streaming event "+definitionKey, err)
	}
}

func exportJobPayload(definitionKey string, job *entities.ExportJob) map[string]any {
	return reportingStreamingPayload.ExportJob(definitionKey, job)
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
	}, sharedObservability.NewMatcherRedactor())

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

	reportingMetrics.RecordExportJobTransition(
		ctx,
		string(job.Format),
		string(entities.ExportJobStatusCanceled),
	)

	return nil
}
