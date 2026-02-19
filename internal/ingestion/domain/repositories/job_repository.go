// Package repositories defines repository interfaces for the ingestion domain.
package repositories

import (
	"context"

	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
)

// CursorFilter contains pagination and sorting parameters.
type CursorFilter struct {
	Limit     int
	Cursor    string
	SortBy    string
	SortOrder string
}

//go:generate mockgen -source=job_repository.go -destination=mock/job_repository_mock.go -package=mock

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
}
