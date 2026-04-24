// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package repositories defines repository interfaces for the ingestion domain.
package repositories

import (
	"context"

	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
)

// CursorFilter contains pagination and sorting parameters.
type CursorFilter struct {
	Limit     int
	Cursor    string
	SortBy    string
	SortOrder string
}

//go:generate mockgen -source=job_repository.go -destination=mocks/job_repository_mock.go -package=mocks

// JobRepository defines the interface for ingestion job persistence.
type JobRepository interface {
	Create(ctx context.Context, job *entities.IngestionJob) (*entities.IngestionJob, error)
	FindByID(ctx context.Context, id uuid.UUID) (*entities.IngestionJob, error)
	FindByContextID(
		ctx context.Context,
		contextID uuid.UUID,
		filter CursorFilter,
	) ([]*entities.IngestionJob, libHTTP.CursorPagination, error)
	Update(ctx context.Context, job *entities.IngestionJob) (*entities.IngestionJob, error)
	// FindLatestByExtractionID returns the most recent ingestion job that was
	// stamped with the given extraction id in metadata (T-005 P1). Used by
	// IngestFromTrustedStream to short-circuit duplicate intake when the
	// bridge orchestrator retries after a transient link failure. Returns
	// nil + nil (NOT a sentinel) when no job has been stamped with this
	// extraction id; this keeps the call site cheap because "no prior job"
	// is the common case on first attempt.
	FindLatestByExtractionID(ctx context.Context, extractionID uuid.UUID) (*entities.IngestionJob, error)
}
