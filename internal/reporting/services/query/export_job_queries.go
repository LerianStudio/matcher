// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package query provides read operations for reporting.
package query

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

		libLog.SafeError(logger, ctx, "failed to get export job by ID "+id.String(), err, runtime.IsProductionMode())

		return nil, fmt.Errorf("getting export job: %w", err)
	}

	return job, nil
}
